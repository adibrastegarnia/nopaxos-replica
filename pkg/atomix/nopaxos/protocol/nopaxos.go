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
	"github.com/google/uuid"
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
		recoveryID:        uuid.New().String(),
		recoverReps:       make(map[MemberID]*RecoverReply),
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
	// StatusViewChange is used to ignore certain messages during view changes
	StatusViewChange
	// StatusGapCommit indicates the replica is undergoing a gap commit
	StatusGapCommit
	// StatusRecovering is used by a recovering replica to avoid operating on old state
	StatusRecovering
)

// NOPaxos is an interface for managing the state of the NOPaxos consensus protocol
type NOPaxos struct {
	logger               util.Logger
	config               *config.ProtocolConfig
	cluster              Cluster                             // The cluster state
	state                *stateMachine                       // The replica's state machine
	applied              LogSlotID                           // The highest slot applied to the state machine
	sequencer            ClientService_ClientStreamServer    // The stream to the sequencer
	log                  *Log                                // The primary log
	status               Status                              // The status of the replica TODO: Ensure statuses are correctly filtered
	recoveryID           string                              // A nonce indicating the recovery attempt
	recoverReps          map[MemberID]*RecoverReply          // The set of recover replies received
	sessionMessageNum    MessageID                           // The latest message num for the sequencer session
	viewID               *ViewId                             // The current view ID
	lastNormView         *ViewId                             // The last normal view ID TODO: Ensure this is used correctly
	viewChanges          map[MemberID]*ViewChange            // The set of view changes received TODO: Ensure this is cleared
	viewChangeRepairs    map[MemberID]*ViewChangeRepair      // The set of view change repairs received TODO: Ensure this is cleared
	viewChangeRepairReps map[MemberID]*ViewChangeRepairReply // The set of view change repair replies received TODO: Ensure this is cleared
	viewRepair           *ViewRepair                         // The last view repair requested TODO: Ensure this is cleared
	viewChangeLog        *Log                                // A temporary log for view changes TODO: Ensure this is garbage collected
	viewLog              *Log                                // A temporary log for view starts TODO: Ensure this is garbage collected
	currentGapSlot       LogSlotID                           // The current slot for which a gap is being committed TODO: Ensure this is used correctly
	gapCommitReps        map[MemberID]*GapCommitReply        // The set of gap commit replies TODO: Ensure this is cleared
	tentativeSync        LogSlotID                           // The tentative sync point TODO: Ensure this is used correctly
	syncPoint            LogSlotID                           // The last known sync point TODO: Ensure this is used correctly
	syncRepair           *SyncRepair                         // The last sent sync repair
	syncReps             map[MemberID]*SyncReply             // The set of sync replies received TODO: Ensure this is cleared
	syncLog              *Log                                // A temporary log for synchronization TODO: Ensure this is garbage collected
	pingTicker           *time.Ticker
	timeoutTimer         *time.Timer
	mu                   sync.RWMutex
}

func (s *NOPaxos) start() {
	s.mu.Lock()
	s.resetTimeout()
	s.setPingTicker()
	s.mu.Unlock()
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
			s.Timeout()
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
	s.mu.Lock()
	s.sequencer = stream
	s.mu.Unlock()
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
	case *ReplicaMessage_Recover:
		s.handleRecover(m.Recover)
	case *ReplicaMessage_RecoverReply:
		s.handleRecoverReply(m.RecoverReply)
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
