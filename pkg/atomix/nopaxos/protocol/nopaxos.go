// Copyright 2019-present Open Networking Foundation.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package protocol

import (
	"github.com/atomix/atomix-go-node/pkg/atomix/node"
	"github.com/atomix/atomix-go-node/pkg/atomix/service"
	"github.com/atomix/atomix-nopaxos-node/pkg/atomix/nopaxos/config"
	"github.com/atomix/atomix-nopaxos-node/pkg/atomix/nopaxos/util"
	"io"
	"sync"
	"time"
)

const bloomFilterHashFunctions = 5

// NewNOPaxos returns a new NOPaxos protocol state struct
func NewNOPaxos(cluster Cluster, registry *node.Registry, config *config.ProtocolConfig) *NOPaxos {
	nopaxos := &NOPaxos{
		logger:  util.NewNodeLogger(string(cluster.Member())),
		config:  config,
		cluster: cluster,
		state:   newStateMachine(string(cluster.Member()), registry),
		viewID: &ViewId{
			SessionNum: 1,
			LeaderNum:  1,
		},
		lastNormView: &ViewId{
			SessionNum: 1,
			LeaderNum:  1,
		},
		sessionMessageNum: 1,
		log:               newLog(1),
		viewChanges:       make(map[MemberID]*ViewChange),
		gapCommitReps:     make(map[MemberID]*GapCommitReply),
		syncReps:          make(map[MemberID]*SyncReply),
	}
	nopaxos.start()
	return nopaxos
}

// MemberID is the ID of a NOPaxos cluster member
type MemberID string

// LeaderID is the leader identifier
type LeaderID uint64

// SessionID is a sequencer session ID
type SessionID uint64

// MessageID is a sequencer message ID
type MessageID uint64

// LogSlotID is a log slot number
type LogSlotID uint64

// Status is the protocol status
type Status int

const (
	// StatusNormal is the normal status
	StatusNormal Status = iota
	// StatusViewChange is the view change status
	StatusViewChange
	// StatusGapCommit is the gap commit status
	StatusGapCommit
)

// NOPaxos is an interface for managing the state of the NOPaxos consensus protocol
type NOPaxos struct {
	logger               util.Logger
	config               *config.ProtocolConfig
	cluster              Cluster
	state                *stateMachine
	applied              LogSlotID
	sequencer            ClientService_ClientStreamServer
	log                  *Log
	viewChangeLog        *Log
	viewLog              *Log
	status               Status
	sessionMessageNum    MessageID
	viewID               *ViewId
	lastNormView         *ViewId
	viewChanges          map[MemberID]*ViewChange
	viewChangeRepairs    map[MemberID]*ViewChangeRepair
	viewChangeRepairReps map[MemberID]*ViewChangeRepairReply
	viewRepair           *ViewRepair
	currentGapSlot       LogSlotID
	gapCommitReps        map[MemberID]*GapCommitReply
	tentativeSync        LogSlotID
	syncPoint            LogSlotID
	syncRepair           *SyncRepair
	syncReps             map[MemberID]*SyncReply
	syncLog              *Log
	pingTicker           *time.Ticker
	timeoutTimer         *time.Timer
	stateMu              sync.RWMutex
}

func (s *NOPaxos) start() {
	s.stateMu.Lock()
	s.resetTimeout()
	s.setPingTicker()
	s.stateMu.Unlock()
}

func (s *NOPaxos) resetTimeout() {
	s.logger.Debug("Resetting leader timeout")
	s.timeoutTimer = time.NewTimer(s.config.GetLeaderTimeoutOrDefault())
	go func() {
		select {
		case _, ok := <-s.timeoutTimer.C:
			if !ok {
				return
			}
			s.timeout()
		}
	}()
}

func (s *NOPaxos) setPingTicker() {
	s.logger.Debug("Setting ping ticker")
	s.pingTicker = time.NewTicker(s.config.GetPingIntervalOrDefault())
	go func() {
		for {
			select {
			case _, ok := <-s.pingTicker.C:
				if !ok {
					return
				}
				s.sendPing()
			}
		}
	}()
}

func (s *NOPaxos) ClientStream(stream ClientService_ClientStreamServer) error {
	s.stateMu.Lock()
	s.sequencer = stream
	s.stateMu.Unlock()
	for {
		message, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		go s.handleClient(message, stream)
	}
}

func (s *NOPaxos) handleClient(message *ClientMessage, stream ClientService_ClientStreamServer) {
	switch m := message.Message.(type) {
	case *ClientMessage_Command:
		s.command(m.Command, stream)
	case *ClientMessage_Query:
		s.query(m.Query, stream)
	}
}

func (s *NOPaxos) ReplicaStream(stream ReplicaService_ReplicaStreamServer) error {
	for {
		message, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		go s.handleReplica(message)
	}
}

func (s *NOPaxos) handleReplica(message *ReplicaMessage) {
	switch m := message.Message.(type) {
	case *ReplicaMessage_Command:
		s.handleSlot(m.Command)
	case *ReplicaMessage_SlotLookup:
		s.handleSlotLookup(m.SlotLookup)
	case *ReplicaMessage_GapCommit:
		s.handleGapCommit(m.GapCommit)
	case *ReplicaMessage_GapCommitReply:
		s.handleGapCommitReply(m.GapCommitReply)
	case *ReplicaMessage_ViewChangeRequest:
		s.handleViewChangeRequest(m.ViewChangeRequest)
	case *ReplicaMessage_ViewChange:
		s.handleViewChange(m.ViewChange)
	case *ReplicaMessage_ViewChangeRepair:
		s.handleViewChangeRepair(m.ViewChangeRepair)
	case *ReplicaMessage_ViewChangeRepairReply:
		s.handleViewChangeRepairReply(m.ViewChangeRepairReply)
	case *ReplicaMessage_StartView:
		s.handleStartView(m.StartView)
	case *ReplicaMessage_ViewRepair:
		s.handleViewRepair(m.ViewRepair)
	case *ReplicaMessage_ViewRepairReply:
		s.handleViewRepairReply(m.ViewRepairReply)
	case *ReplicaMessage_SyncPrepare:
		s.handleSyncPrepare(m.SyncPrepare)
	case *ReplicaMessage_SyncRepair:
		s.handleSyncRepair(m.SyncRepair)
	case *ReplicaMessage_SyncRepairReply:
		s.handleSyncRepairReply(m.SyncRepairReply)
	case *ReplicaMessage_SyncReply:
		s.handleSyncReply(m.SyncReply)
	case *ReplicaMessage_SyncCommit:
		s.handleSyncCommit(m.SyncCommit)
	case *ReplicaMessage_Ping:
		s.handlePing(m.Ping)
	}
}

func (s *NOPaxos) getLeader(viewID *ViewId) MemberID {
	members := s.cluster.Members()
	return members[int(uint64(viewID.LeaderNum)%uint64(len(members)))]
}

func newStateMachine(node string, registry *node.Registry) *stateMachine {
	sm := &stateMachine{
		node:     node,
		registry: registry,
	}
	sm.reset()
	return sm
}

// stateMachine is the replica state machine
type stateMachine struct {
	node     string
	registry *node.Registry
	state    node.StateMachine
	context  *stateMachineContext
}

// applyCommand applies a command to the state machine
func (s *stateMachine) applyCommand(entry *LogEntry, ch chan<- node.Output) {
	s.context.index++
	s.context.op = service.OpTypeCommand
	if entry.Timestamp.After(s.context.timestamp) {
		s.context.timestamp = entry.Timestamp
	}
	s.state.Command(entry.Value, ch)
}

// applyQuery applies a query to the state machine
func (s *stateMachine) applyQuery(request *QueryRequest, ch chan<- node.Output) {
	s.context.op = service.OpTypeQuery
	s.state.Query(request.Value, ch)
}

// reset resets the state machine
func (s *stateMachine) reset() {
	s.context = &stateMachineContext{}
	s.state = node.NewPrimitiveStateMachine(s.registry, s.context)
}

// stateMachineContext is a replica state machine context
type stateMachineContext struct {
	node      string
	index     uint64
	timestamp time.Time
	op        service.OperationType
}

func (c *stateMachineContext) Node() string {
	return c.node
}

func (c *stateMachineContext) Index() uint64 {
	return c.index
}

func (c *stateMachineContext) Timestamp() time.Time {
	return c.timestamp
}

func (c *stateMachineContext) OperationType() service.OperationType {
	return c.op
}
