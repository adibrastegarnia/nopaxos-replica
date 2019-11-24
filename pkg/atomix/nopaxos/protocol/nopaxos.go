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
	"github.com/atomix/atomix-nopaxos-node/pkg/atomix/nopaxos/config"
	"github.com/atomix/atomix-nopaxos-node/pkg/atomix/nopaxos/state"
	"github.com/atomix/atomix-nopaxos-node/pkg/atomix/nopaxos/store"
	"github.com/atomix/atomix-nopaxos-node/pkg/atomix/nopaxos/store/log"
	"github.com/atomix/atomix-nopaxos-node/pkg/atomix/nopaxos/util"
	"io"
	"math"
	"sync"
	"time"
)

// NewNOPaxos returns a new NOPaxos protocol state struct
func NewNOPaxos(cluster Cluster, state state.Manager, store store.Store, config *config.ProtocolConfig) *NOPaxos {
	return &NOPaxos{
		log:     util.NewNodeLogger(string(cluster.Member())),
		state:   state,
		store:   store,
		config:  config,
		cluster: cluster,
	}
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
	log              util.Logger
	config           *config.ProtocolConfig
	cluster          Cluster
	store            store.Store
	state            state.Manager
	reader           log.Reader
	writer           log.Writer
	status           Status
	sessionMessageID MessageID
	viewID           *ViewId
	lastNormView     *ViewId
	viewChanges      map[MemberID]*ViewChange
	currentGapSlot   LogSlotID
	gapCommitReps    []*GapCommitReply
	tentativeSync    LogSlotID
	syncReps         LogSlotID
	mu               sync.RWMutex
}

func (s *NOPaxos) Stream(stream NOPaxosService_StreamServer) error {
	for {
		message, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		go s.handle(message, stream)
	}
}

func (s *NOPaxos) handle(message *Message, stream NOPaxosService_StreamServer) {
	switch m := message.Message.(type) {
	case *Message_Command:
		s.command(m.Command)
	case *Message_Query:
		s.query(m.Query)
	case *Message_SlotLookup:
		s.slotLookup(m.SlotLookup)
	case *Message_GapCommit:
		s.gapCommit(m.GapCommit, stream)
	case *Message_GapCommitReply:
		s.gapCommitReply(m.GapCommitReply)
	case *Message_ViewChangeRequest:
		s.viewChangeRequest(m.ViewChangeRequest)
	case *Message_ViewChange:
		s.viewChange(m.ViewChange)
	case *Message_StartView:
		s.startView(m.StartView)
	case *Message_SyncPrepare:
		s.syncPrepare(m.SyncPrepare)
	case *Message_SyncReply:
		s.syncReply(m.SyncReply)
	case *Message_SyncCommit:
		s.syncCommit(m.SyncCommit)
	}
}

func (s *NOPaxos) getLeader(viewID *ViewId) MemberID {
	members := s.cluster.Members()
	return members[int(uint64(viewID.LeaderId)%uint64(len(members)))]
}

func (s *NOPaxos) command(request *CommandRequest) {

}

func (s *NOPaxos) query(request *QueryRequest) {

}

func (s *NOPaxos) slotLookup(request *SlotLookup) {
	// If the view ID does not match the sender's view ID, skip the message
	if s.viewID == nil || s.viewID.LeaderId != request.ViewId.LeaderId || s.viewID.SessionId != request.ViewId.SessionId {
		return
	}

	// If this replica is not the leader, skip the message
	if s.getLeader(s.viewID) != s.cluster.Member() {
		return
	}

	// If the replica's status is not Normal, skip the message
	if s.status != StatusNormal {
		return
	}

	slotID := s.writer.LastSlotID() + 1 - LogSlotID(s.sessionMessageID-request.MessageId)

	// TODO
}

func (s *NOPaxos) gapCommit(request *GapCommitRequest, stream NOPaxosService_StreamServer) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// If the view ID does not match the sender's view ID, skip the message
	if s.viewID == nil || s.viewID.LeaderId != request.ViewId.LeaderId || s.viewID.SessionId != request.ViewId.SessionId {
		return
	}

	// If the replica's status is not Normal or GapCommit, skip the message
	if s.status != StatusNormal && s.status != StatusGapCommit {
		return
	}

	// If the request slot ID is not the next slot in the replica's log, skip the message
	lastSlotID := s.writer.LastSlotID()
	if request.SlotId > lastSlotID+1 {
		return
	}

	// TODO: This operation should be a replace rather than truncate/append
	// TODO: This can be done trivially by flipping a bit in the written entry
	s.writer.Truncate(request.SlotId)
	s.writer.Append(&LogEntry{
		LogSlotId: request.SlotId,
		Timestamp: time.Now(),
		Entry:     &LogEntry_NoOp{},
	})

	// Increment the session message ID if neccessary
	if request.SlotId > s.writer.LastSlotID() {
		s.sessionMessageID++
	}

	stream.SendMsg(&GapCommitReply{
		SenderId: s.cluster.Member(),
		ViewId:   s.viewID,
		SlotId:   request.SlotId,
	})
}

func (s *NOPaxos) gapCommitReply(reply *GapCommitReply) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// If the view ID does not match the sender's view ID, skip the message
	if s.viewID == nil || s.viewID.LeaderId != reply.ViewId.LeaderId || s.viewID.SessionId != reply.ViewId.SessionId {
		return
	}

	// If this replica is not the leader, skip the message
	if s.getLeader(s.viewID) != s.cluster.Member() {
		return
	}

	// If the replica's status is not Normal or GapCommit, skip the message
	if s.status != StatusGapCommit {
		return
	}

	// If the gap commit slot does not match the current gap slot, skip the message
	if reply.SlotId != s.currentGapSlot {
		return
	}

	s.gapCommitReps = append(s.gapCommitReps, reply)

	// TODO
}

func (s *NOPaxos) viewChangeRequest(request *ViewChangeRequest) error {
	newLeaderID := LeaderID(math.Max(float64(s.viewID.LeaderId), float64(request.ViewId.LeaderId)))
	newSessionID := SessionID(math.Max(float64(s.viewID.SessionId), float64(request.ViewId.SessionId)))
	newViewID := &ViewId{
		LeaderId:  newLeaderID,
		SessionId: newSessionID,
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// If the view IDs match, ignore the request
	if s.viewID != nil && s.viewID.LeaderId == newViewID.LeaderId && s.viewID.SessionId == newViewID.SessionId {
		return nil
	}

	// Set the replica's status to ViewChange
	s.status = StatusViewChange

	// Set the replica's view ID to the new view ID
	s.viewID = newViewID

	// Reset the view changes
	s.viewChanges = make(map[MemberID]*ViewChange)

	// Send a ViewChange to the new leader
	stream, err := s.cluster.GetStream(s.getLeader(newViewID))
	if err != nil {
		return err
	}

	// TODO: The spec sends the replica's log in the ViewChange message
	// Obviously we don't want to do that in a production system
	// Additionally, the spec sends the ViewChangeRequest to all replicas in case this is an entirely new view
	stream.Send(&Message{
		Message: &Message_ViewChange{
			ViewChange: &ViewChange{
				MemberId:   s.cluster.Member(),
				ViewId:     newViewID,
				LastNormal: s.lastNormView,
				MessageId:  s.sessionMessageID,
				Log:        log,
			},
		},
	})
}

func (s *NOPaxos) viewChange(request *ViewChange) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// If the view IDs do not match, ignore the request
	if s.viewID == nil || s.viewID.LeaderId != request.ViewId.LeaderId || s.viewID.SessionId != request.ViewId.SessionId {
		return
	}

	// If the replica's status is not ViewChange, ignore the request
	if s.status != StatusViewChange {
		return
	}

	// If this replica is not the leader of the view, ignore the request
	if s.getLeader(request.ViewId) != s.cluster.Member() {
		return
	}

	// Add the view change to the set of view changes
	s.viewChanges[request.MemberId] = request

	// If the view change has received enough acknowlegements, start the new view
	// TODO
}

func (s *NOPaxos) startView(request *StartView) {

}

func (s *NOPaxos) syncPrepare(request *SyncPrepare) {

}

func (s *NOPaxos) syncReply(reply *SyncReply) {

}

func (s *NOPaxos) syncCommit(request *SyncCommit) {

}
