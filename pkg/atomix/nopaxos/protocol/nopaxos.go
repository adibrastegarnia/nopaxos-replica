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
	"github.com/atomix/go-framework/pkg/atomix/node"
	"github.com/atomix/go-framework/pkg/atomix/service"
	streams "github.com/atomix/go-framework/pkg/atomix/stream"
	"github.com/atomix/nopaxos-replica/pkg/atomix/nopaxos/config"
	"github.com/atomix/nopaxos-replica/pkg/atomix/nopaxos/util"
	"github.com/gogo/protobuf/proto"
	"github.com/google/uuid"
	"io"
	"sync"
	"time"
)

const bloomFilterHashFunctions = 5

// NewNOPaxos returns a new NOPaxos protocol state struct
func NewNOPaxos(cluster Cluster, registry *node.Registry, config *config.ProtocolConfig) *NOPaxos {
	nopaxos := &NOPaxos{
		logger:   util.NewNodeLogger(string(cluster.Member())),
		config:   config,
		cluster:  cluster,
		state:    newStateMachine(string(cluster.Member()), registry),
		status:   StatusRecovering,
		watchers: make([]func(Status), 0),
		viewID: &ViewId{
			SessionNum: 1,
			LeaderNum:  1,
		},
		lastNormView: &ViewId{
			SessionNum: 1,
			LeaderNum:  1,
		},
		sessionMessageNum:    1,
		log:                  newLog(1),
		recoveryID:           uuid.New().String(),
		recoverReps:          make(map[MemberID]*RecoverReply),
		viewChanges:          make(map[MemberID]*ViewChange),
		viewChangeRepairs:    make(map[MemberID]*ViewChangeRepair),
		viewChangeRepairReps: make(map[MemberID]*ViewChangeRepairReply),
		gapCommitReps:        make(map[MemberID]*GapCommitReply),
		syncReps:             make(map[MemberID]*SyncReply),
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

// Status is the protocol status
type Status string

const (
	// StatusRecovering is used by a recovering replica to avoid operating on old state
	StatusRecovering Status = "Recovering"
	// StatusNormal is the normal status
	StatusNormal Status = "Normal"
	// StatusViewChange is used to ignore certain messages during view changes
	StatusViewChange Status = "ViewChange"
	// StatusGapCommit indicates the replica is undergoing a gap commit
	StatusGapCommit Status = "GapCommit"
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
	status               Status                              // The status of the replica
	watchers             []func(Status)                      // A list of status watchers
	recoveryID           string                              // A nonce indicating the recovery attempt
	recoverReps          map[MemberID]*RecoverReply          // The set of recover replies received
	currentCheckpoint    *Checkpoint                         // The current checkpoint (if any)
	sessionMessageNum    MessageID                           // The latest message num for the sequencer session
	viewID               *ViewId                             // The current view ID
	lastNormView         *ViewId                             // The last normal view ID
	viewChanges          map[MemberID]*ViewChange            // The set of view changes received
	viewChangeRepairs    map[MemberID]*ViewChangeRepair      // The set of view change repairs received
	viewChangeRepairReps map[MemberID]*ViewChangeRepairReply // The set of view change repair replies received
	viewRepair           *ViewRepair                         // The last view repair requested
	viewLog              *Log                                // A temporary log for view starts
	currentGapSlot       LogSlotID                           // The current slot for which a gap is being committed
	gapCommitReps        map[MemberID]*GapCommitReply        // The set of gap commit replies
	tentativeSync        LogSlotID                           // The tentative sync point
	syncPoint            LogSlotID                           // The last known sync point
	syncRepair           *SyncRepair                         // The last sent sync repair
	syncReps             map[MemberID]*SyncReply             // The set of sync replies received
	syncLog              *Log                                // A temporary log for synchronization
	pingTicker           *time.Ticker
	checkpointTicker     *time.Ticker
	syncTicker           *time.Ticker
	timeoutTimer         *time.Timer
	mu                   sync.RWMutex
}

func (s *NOPaxos) start() {
	s.mu.Lock()
	s.setPingTicker()
	s.setCheckpointTicker()
	go s.resetTimeout()
	s.mu.Unlock()
}

func (s *NOPaxos) Watch(watcher func(Status)) {
	s.watchers = append(s.watchers, watcher)
}

func (s *NOPaxos) setStatus(status Status) {
	if s.status != status {
		s.logger.Debug("Replica status changed: %s", status)
		s.status = status
		for _, watcher := range s.watchers {
			watcher(status)
		}
	}
}

func (s *NOPaxos) resetTimeout() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.timeoutTimer != nil {
		s.timeoutTimer.Stop()
	}
	s.timeoutTimer = time.NewTimer(s.config.GetLeaderTimeoutOrDefault())
	go func() {
		select {
		case _, ok := <-s.timeoutTimer.C:
			if ok {
				s.Timeout()
			}
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

func (s *NOPaxos) setCheckpointTicker() {
	s.logger.Debug("Setting checkpoint ticker")
	s.checkpointTicker = time.NewTicker(s.config.GetCheckpointIntervalOrDefault())
	go func() {
		for {
			select {
			case _, ok := <-s.checkpointTicker.C:
				if !ok {
					return
				}
				s.checkpoint()
			}
		}
	}()
}

func (s *NOPaxos) setSyncTicker() {
	s.logger.Debug("Setting sync ticker")
	s.checkpointTicker = time.NewTicker(s.config.GetSyncIntervalOrDefault())
	go func() {
		for {
			select {
			case _, ok := <-s.syncTicker.C:
				if !ok {
					return
				}
				s.startSync()
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
		s.handleClient(message, stream)
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

func (s *NOPaxos) send(message *ReplicaMessage, member MemberID) {
	if stream, err := s.cluster.GetStream(member); err == nil {
		err := stream.Send(message)
		if err != nil {
			s.logger.Error("Failed to send to %s: %v", member, err)
		}
	} else {
		s.logger.Error("Failed to open stream to %s: %v", member, err)
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

// checkpoint takes a checkpoint
func (s *stateMachine) checkpoint(checkpoint *Checkpoint) {
	writer := checkpoint.Writer()
	defer writer.Close()
	_ = s.state.Snapshot(writer)
}

// restore restores the state from a checkpoint
func (s *stateMachine) restore(checkpoint *Checkpoint) {
	reader := checkpoint.Reader()
	defer reader.Close()
	_ = s.state.Install(reader)
}

// applyCommand applies a command to the state machine
func (s *stateMachine) applyCommand(entry *LogEntry, stream streams.WriteStream) {
	s.context.index = uint64(entry.SlotNum)
	s.context.op = service.OpTypeCommand
	if entry.Timestamp.After(s.context.timestamp) {
		s.context.timestamp = entry.Timestamp
	}
	s.state.Command(entry.Value, streams.NewEncodingStream(stream, func(value interface{}) (interface{}, error) {
		return proto.Marshal(&Indexed{
			Index: s.context.index,
			Value: value.([]byte),
		})
	}))
}

// applyQuery applies a query to the state machine
func (s *stateMachine) applyQuery(request *QueryRequest, stream streams.WriteStream) {
	s.context.op = service.OpTypeQuery
	s.state.Query(request.Value, stream)
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
