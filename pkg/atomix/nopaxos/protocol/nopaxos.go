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
	"io"
	"math"
	"sync"
	"time"
)

// NewNOPaxos returns a new NOPaxos protocol state struct
func NewNOPaxos(cluster Cluster, state state.Manager, store store.Store, config *config.ProtocolConfig) *NOPaxos {
	nopaxos := &NOPaxos{
		state:   state,
		store:   store,
		config:  config,
		cluster: cluster,
	}
	nopaxos.setLog(store.NewLog())
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
	config              *config.ProtocolConfig
	cluster             Cluster
	store               store.Store
	state               state.Manager
	sequencer           ClientService_ClientStreamServer
	log                 log.Log
	reader              log.Reader
	writer              log.Writer
	status              Status
	sessionMessageCount MessageID
	viewID              *ViewId
	lastNormView        *ViewId
	viewChanges         map[MemberID]*ViewChange
	currentGapSlot      LogSlotID
	gapCommitReps       map[MemberID]*GapCommitReply
	tentativeSync       LogSlotID
	syncPoint           LogSlotID
	syncReps            map[MemberID]*SyncReply
	mu                  sync.RWMutex
}

func (s *NOPaxos) setLog(log log.Log) {
	s.log = log
	s.reader = log.OpenReader(0)
	s.writer = log.Writer()
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
		s.query(m.Query)
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
		go s.handleReplica(message, stream)
	}
}

func (s *NOPaxos) handleReplica(message *ReplicaMessage, stream ReplicaService_ReplicaStreamServer) {
	switch m := message.Message.(type) {
	case *ReplicaMessage_Command:
		s.slot(m.Command)
	case *ReplicaMessage_SlotLookup:
		s.slotLookup(m.SlotLookup, stream)
	case *ReplicaMessage_GapCommit:
		s.gapCommit(m.GapCommit, stream)
	case *ReplicaMessage_GapCommitReply:
		s.gapCommitReply(m.GapCommitReply)
	case *ReplicaMessage_ViewChangeRequest:
		s.viewChangeRequest(m.ViewChangeRequest)
	case *ReplicaMessage_ViewChange:
		s.viewChange(m.ViewChange)
	case *ReplicaMessage_StartView:
		s.startView(m.StartView)
	case *ReplicaMessage_SyncPrepare:
		s.syncPrepare(m.SyncPrepare, stream)
	case *ReplicaMessage_SyncReply:
		s.syncReply(m.SyncReply)
	case *ReplicaMessage_SyncCommit:
		s.syncCommit(m.SyncCommit)
	}
}

func (s *NOPaxos) getLeader(viewID *ViewId) MemberID {
	members := s.cluster.Members()
	return members[int(uint64(viewID.LeaderId)%uint64(len(members)))]
}

func (s *NOPaxos) slot(request *CommandRequest) {
	s.command(request, nil)
}

func (s *NOPaxos) command(request *CommandRequest, stream ClientService_ClientStreamServer) {
	// If the replica's status is not Normal, skip the commit
	if s.status != StatusNormal {
		return
	}

	if request.SessionId == s.viewID.SessionId && request.MessageId == s.sessionMessageCount {
		// Command received in the normal case
		slotID := s.writer.LastSlotID() + 1
		s.writer.Append(&LogEntry{
			LogSlotId: slotID,
			Timestamp: time.Now(),
			Entry: &LogEntry_Command{
				Command: &CommandEntry{
					MessageId: request.MessageId,
					Value:     request.Value,
				},
			},
		})

		// TODO: Apply the command to the state machine before responding if leader
		if stream != nil {
			_ = stream.Send(&ClientMessage{
				Message: &ClientMessage_CommandReply{
					CommandReply: &CommandReply{
						MessageId: request.MessageId,
						SenderId:  s.cluster.Member(),
						ViewId:    s.viewID,
						SlotId:    slotID,
						Value:     nil, // TODO
					},
				},
			})
		}
	} else if request.SessionId > s.viewID.SessionId {
		// Command received in the session terminated case
		newViewID := &ViewId{
			SessionId: request.SessionId,
			LeaderId:  s.viewID.LeaderId,
		}
		for _, member := range s.cluster.Members() {
			if stream, err := s.cluster.GetStream(member); err == nil {
				_ = stream.Send(&ReplicaMessage{
					Message: &ReplicaMessage_ViewChangeRequest{
						ViewChangeRequest: &ViewChangeRequest{
							ViewId: newViewID,
						},
					},
				})
			}
		}
	} else if request.SessionId == s.viewID.SessionId && request.MessageId > s.sessionMessageCount {
		// Drop notification. If leader commit a gap, otherwise ask the leader for the slot
		if s.getLeader(s.viewID) == s.cluster.Member() {
			s.sendGapCommit()
		} else {
			stream, err := s.cluster.GetStream(s.getLeader(s.viewID))
			if err != nil {
				return
			}
			_ = stream.Send(&ReplicaMessage{
				Message: &ReplicaMessage_SlotLookup{
					SlotLookup: &SlotLookup{
						SenderId:  s.cluster.Member(),
						ViewId:    s.viewID,
						MessageId: request.MessageId,
					},
				},
			})
		}
	}
}

func (s *NOPaxos) query(request *QueryRequest) {

}

func (s *NOPaxos) slotLookup(request *SlotLookup, stream ReplicaService_ReplicaStreamServer) {
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

	slotID := s.writer.LastSlotID() + 1 - LogSlotID(s.sessionMessageCount-request.MessageId)

	if slotID <= s.writer.LastSlotID() {
		s.reader.Reset(slotID)
		entry := s.reader.NextEntry()
		if e, ok := entry.Entry.Entry.(*LogEntry_Command); ok {
			_ = stream.Send(&ReplicaMessage{
				Message: &ReplicaMessage_Command{
					Command: &CommandRequest{
						SessionId: s.viewID.SessionId,
						MessageId: e.Command.MessageId,
						Value:     e.Command.Value,
					},
				},
			})
		}
	} else if slotID == s.writer.LastSlotID()+1 {
		s.sendGapCommit()
	}
}

func (s *NOPaxos) sendGapCommit() {
	// If this replica is not the leader, skip the commit
	if s.getLeader(s.viewID) != s.cluster.Member() {
		return
	}

	// If the replica's status is not Normal, skip the commit
	if s.status != StatusNormal {
		return
	}

	slotID := s.writer.LastSlotID() + 1

	// Set the replica's status to GapCommit
	s.status = StatusGapCommit

	// Set the current gap slot
	s.currentGapSlot = slotID

	// Send a GapCommit to each replica
	for _, member := range s.cluster.Members() {
		if stream, err := s.cluster.GetStream(member); err == nil {
			_ = stream.Send(&ReplicaMessage{
				Message: &ReplicaMessage_GapCommit{
					GapCommit: &GapCommitRequest{
						ViewId: s.viewID,
						SlotId: slotID,
					},
				},
			})
		}
	}
}

func (s *NOPaxos) gapCommit(request *GapCommitRequest, stream ReplicaService_ReplicaStreamServer) {
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
		s.sessionMessageCount++
	}

	_ = stream.SendMsg(&GapCommitReply{
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

	s.gapCommitReps[reply.SenderId] = reply

	// Get the set of gap commit replies for the current slot
	gapCommits := make([]*GapCommitReply, 0, len(s.gapCommitReps))
	for _, gapCommit := range s.gapCommitReps {
		if gapCommit.ViewId.SessionId == s.viewID.SessionId && gapCommit.ViewId.LeaderId == s.viewID.LeaderId && gapCommit.SlotId == s.currentGapSlot {
			gapCommits = append(gapCommits, gapCommit)
		}
	}

	// If a quorum of gap commits has been received for the slot, return the status to normal
	if len(gapCommits) >= s.cluster.QuorumSize() {
		s.status = StatusNormal
	}
}

func (s *NOPaxos) startLeaderChange() {
	newViewID := &ViewId{
		SessionId: s.viewID.SessionId,
		LeaderId:  s.viewID.LeaderId + 1,
	}

	for _, member := range s.cluster.Members() {
		if member != s.getLeader(newViewID) {
			if stream, err := s.cluster.GetStream(member); err == nil {
				_ = stream.Send(&ReplicaMessage{
					Message: &ReplicaMessage_ViewChangeRequest{
						ViewChangeRequest: &ViewChangeRequest{
							ViewId: newViewID,
						},
					},
				})
			}
		}
	}
}

func (s *NOPaxos) viewChangeRequest(request *ViewChangeRequest) {
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
		return
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
		return
	}

	// TODO: We probably can't send the entire log in a single message here
	reader := s.log.OpenReader(0)
	log := make([]*LogEntry, 0, reader.LastSlotID()-reader.FirstSlotID()+1)
	reader.Reset(0)
	entry := reader.NextEntry()
	for entry != nil {
		log = append(log, entry.Entry)
		entry = reader.NextEntry()
	}
	_ = stream.Send(&ReplicaMessage{
		Message: &ReplicaMessage_ViewChange{
			ViewChange: &ViewChange{
				MemberId:   s.cluster.Member(),
				ViewId:     newViewID,
				LastNormal: s.lastNormView,
				MessageId:  s.sessionMessageCount,
				Log:        log,
			},
		},
	})

	// Send a view change request to all replicas other than the leader
	for _, member := range s.cluster.Members() {
		if member != s.getLeader(newViewID) {
			if stream, err := s.cluster.GetStream(member); err == nil {
				_ = stream.Send(&ReplicaMessage{
					Message: &ReplicaMessage_ViewChangeRequest{
						ViewChangeRequest: &ViewChangeRequest{
							ViewId: newViewID,
						},
					},
				})
			}
		}
	}
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

	// Aggregate the view changes for the current view
	viewChanges := make([]*ViewChange, 0, len(s.viewChanges))
	for _, viewChange := range s.viewChanges {
		if viewChange.ViewId.SessionId == s.viewID.SessionId && viewChange.ViewId.LeaderId == s.viewID.LeaderId {
			viewChanges = append(viewChanges, viewChange)
		}
	}

	// If the view changes have reached a quorum, start the new view
	if len(viewChanges) >= s.cluster.QuorumSize() {
		// Create the state for the new view
		var lastNormal *ViewChange
		for _, viewChange := range viewChanges {
			if viewChange.ViewId.SessionId <= s.viewID.SessionId && viewChange.ViewId.LeaderId <= s.viewID.LeaderId {
				lastNormal = viewChange
			}
		}

		var newMessageID MessageID
		goodLogs := make([][]*LogEntry, 0, len(viewChanges))
		for _, viewChange := range viewChanges {
			if viewChange.LastNormal.SessionId == lastNormal.ViewId.SessionId && viewChange.LastNormal.LeaderId == lastNormal.ViewId.LeaderId {
				goodLogs = append(goodLogs, viewChange.Log)

				// If the session has changed, take the maximum message ID
				if lastNormal.ViewId.SessionId == s.viewID.SessionId && viewChange.MessageId > newMessageID {
					newMessageID = viewChange.MessageId
				}
			}
		}

		var firstSlotID, lastSlotID LogSlotID
		newLog := make(map[LogSlotID]*LogEntry)
		for _, goodLog := range goodLogs {
			for _, entry := range goodLog {
				newEntry := newLog[entry.LogSlotId]
				if _, ok := entry.Entry.(*LogEntry_NoOp); ok || newEntry == nil {
					newLog[entry.LogSlotId] = entry
					if firstSlotID == 0 || entry.LogSlotId < firstSlotID {
						firstSlotID = entry.LogSlotId
					}
					if lastSlotID == 0 || entry.LogSlotId > lastSlotID {
						lastSlotID = entry.LogSlotId
					}
				}
			}
		}

		log := make([]*LogEntry, 0, len(newLog))
		for slotID := firstSlotID; slotID <= lastSlotID; slotID++ {
			entry := newLog[slotID]
			if entry == nil {
				entry = &LogEntry{
					LogSlotId: slotID,
					Timestamp: time.Now(),
					Entry:     &LogEntry_NoOp{},
				}
			}
			log = append(log, entry)
		}

		// Send a StartView to each replica
		for _, member := range s.cluster.Members() {
			if stream, err := s.cluster.GetStream(member); err == nil {
				_ = stream.Send(&ReplicaMessage{
					Message: &ReplicaMessage_StartView{
						StartView: &StartView{
							ViewId:    s.viewID,
							MessageId: newMessageID,
							Log:       log,
						},
					},
				})
			}
		}
	}
}

func (s *NOPaxos) startView(request *StartView) {

}

func (s *NOPaxos) startSync() {
	// If this replica is not the leader of the view, ignore the request
	if s.getLeader(s.viewID) != s.cluster.Member() {
		return
	}

	// If the replica's status is not Normal, do not attempt the sync
	if s.status != StatusNormal {
		return
	}

	s.syncReps = make(map[MemberID]*SyncReply)
	s.tentativeSync = s.writer.LastSlotID()

	reader := s.log.OpenReader(0)
	log := make([]*LogEntry, 0, reader.LastSlotID()-reader.FirstSlotID()+1)
	reader.Reset(0)
	entry := reader.NextEntry()
	for entry != nil {
		log = append(log, entry.Entry)
		entry = reader.NextEntry()
	}

	for _, member := range s.cluster.Members() {
		if member != s.cluster.Member() {
			if stream, err := s.cluster.GetStream(member); err == nil {
				_ = stream.Send(&ReplicaMessage{
					Message: &ReplicaMessage_SyncPrepare{
						SyncPrepare: &SyncPrepare{
							SenderId:  s.cluster.Member(),
							ViewId:    s.viewID,
							MessageId: s.sessionMessageCount,
							Log:       log,
						},
					},
				})
			}
		}
	}
}

func (s *NOPaxos) syncPrepare(request *SyncPrepare, stream ReplicaService_ReplicaStreamServer) {
	// If the replica's status is not Normal, ignore the request
	if s.status != StatusNormal {
		return
	}

	// If the view IDs do not match, ignore the request
	if s.viewID == nil || s.viewID.LeaderId != request.ViewId.LeaderId || s.viewID.SessionId != request.ViewId.SessionId {
		return
	}

	// If the sender is not the leader for the current view, ignore the request
	if request.SenderId != s.getLeader(request.ViewId) {
		return
	}

	newLog := s.store.NewLog()
	var firstSlotID LogSlotID
	if len(request.Log) > 0 {
		firstSlotID = request.Log[0].LogSlotId
	}
	newReader := newLog.OpenReader(0)
	newWriter := newLog.Writer()
	newWriter.Reset(firstSlotID)
	for _, entry := range request.Log {
		newWriter.Append(entry)
	}
	reader := s.log.OpenReader(newWriter.LastSlotID() + 1)
	entry := reader.NextEntry()
	for entry != nil {
		newWriter.Append(entry.Entry)
	}
	s.sessionMessageCount = s.sessionMessageCount + MessageID(newWriter.LastSlotID()-s.writer.LastSlotID())
	s.setLog(newLog)

	// Send a SyncReply back to the leader
	_ = stream.Send(&ReplicaMessage{
		Message: &ReplicaMessage_SyncReply{
			SyncReply: &SyncReply{
				SenderId: s.cluster.Member(),
				ViewId:   s.viewID,
				SlotId:   newWriter.LastSlotID(),
			},
		},
	})

	// Send a RequestReply for all entries in the new log
	s.mu.RLock()
	sequencer := s.sequencer
	s.mu.RUnlock()

	if sequencer != nil {
		entry = newReader.NextEntry()
		for entry != nil {
			if e, ok := entry.Entry.Entry.(*LogEntry_Command); ok {
				_ = sequencer.Send(&ClientMessage{
					Message: &ClientMessage_CommandReply{
						CommandReply: &CommandReply{
							MessageId: e.Command.MessageId,
							SenderId:  s.cluster.Member(),
							ViewId:    s.viewID,
							SlotId:    entry.SlotID,
						},
					},
				})
			}
			entry = newReader.NextEntry()
		}
	}
}

func (s *NOPaxos) syncReply(reply *SyncReply) {
	// If the view IDs do not match, ignore the request
	if s.viewID == nil || s.viewID.LeaderId != reply.ViewId.LeaderId || s.viewID.SessionId != reply.ViewId.SessionId {
		return
	}

	// If the replica's status is not Normal, ignore the request
	if s.status != StatusNormal {
		return
	}

	// Add the reply to the set of sync replies
	s.syncReps[reply.SenderId] = reply

	syncReps := make([]*SyncReply, 0, len(s.syncReps))
	for _, syncRep := range s.syncReps {
		if syncRep.ViewId.LeaderId == s.viewID.LeaderId && syncRep.ViewId.SessionId == s.viewID.SessionId && syncRep.SlotId == s.tentativeSync {
			syncReps = append(syncReps, syncRep)
		}
	}

	if len(syncReps) >= s.cluster.QuorumSize() {
		reader := s.log.OpenReader(0)
		log := make([]*LogEntry, 0, reader.LastSlotID()-reader.FirstSlotID()+1)
		reader.Reset(0)
		entry := reader.NextEntry()
		for entry != nil {
			log = append(log, entry.Entry)
			entry = reader.NextEntry()
		}

		for _, member := range s.cluster.Members() {
			if member != s.cluster.Member() {
				if stream, err := s.cluster.GetStream(member); err == nil {
					_ = stream.Send(&ReplicaMessage{
						Message: &ReplicaMessage_SyncCommit{
							SyncCommit: &SyncCommit{
								SenderId:  s.cluster.Member(),
								ViewId:    s.viewID,
								MessageId: s.sessionMessageCount,
								Log:       log,
							},
						},
					})
				}
			}
		}
	}
}

func (s *NOPaxos) syncCommit(request *SyncCommit) {
	// If the replica's status is not Normal, ignore the request
	if s.status != StatusNormal {
		return
	}

	// If the view IDs do not match, ignore the request
	if s.viewID == nil || s.viewID.LeaderId != request.ViewId.LeaderId || s.viewID.SessionId != request.ViewId.SessionId {
		return
	}

	// If the sender is not the leader for the current view, ignore the request
	if request.SenderId != s.getLeader(request.ViewId) {
		return
	}

	newLog := s.store.NewLog()
	var firstSlotID LogSlotID
	if len(request.Log) > 0 {
		firstSlotID = request.Log[0].LogSlotId
	}
	newWriter := newLog.Writer()
	newWriter.Reset(firstSlotID)
	for _, entry := range request.Log {
		newWriter.Append(entry)
	}
	reader := s.log.OpenReader(newWriter.LastSlotID() + 1)
	entry := reader.NextEntry()
	for entry != nil {
		newWriter.Append(entry.Entry)
	}
	s.sessionMessageCount = s.sessionMessageCount + MessageID(newWriter.LastSlotID()-s.writer.LastSlotID())
	s.syncPoint = newWriter.LastSlotID()
	s.setLog(newLog)
}
