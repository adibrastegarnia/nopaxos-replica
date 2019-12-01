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

func (s *NOPaxos) sendGapCommit() {
	s.mu.Lock()
	defer s.mu.Unlock()

	// If this replica is not the leader, skip the commit
	if s.getLeader(s.viewID) != s.cluster.Member() {
		return
	}

	// If the replica's status is not Normal, skip the commit
	if s.status != StatusNormal {
		return
	}

	// Add a no-op entry to the log
	s.log.Extend(s.log.LastSlot() + 1)
	slotID := s.log.LastSlot()

	// Set the replica's status to GapCommit
	s.setStatus(StatusGapCommit)

	// Set the current gap slot
	s.currentGapSlot = slotID

	gapCommit := &GapCommitRequest{
		Sender:  s.cluster.Member(),
		ViewID:  s.viewID,
		SlotNum: slotID,
	}
	message := &ReplicaMessage{
		Message: &ReplicaMessage_GapCommit{
			GapCommit: gapCommit,
		},
	}

	// Send a GapCommit to each replica
	for _, member := range s.cluster.Members() {
		s.logger.SendTo("GapCommit", gapCommit, member)
		go s.send(message, member)
	}
}

func (s *NOPaxos) handleGapCommit(request *GapCommitRequest) {
	s.logger.ReceiveFrom("GapCommitRequest", request, request.Sender)

	s.mu.Lock()
	defer s.mu.Unlock()

	// If the view ID does not match the sender's view ID, skip the message
	if s.viewID.LeaderNum != request.ViewID.LeaderNum || s.viewID.SessionNum != request.ViewID.SessionNum {
		return
	}

	// If the replica's status is not Normal or GapCommit, skip the message
	if s.status != StatusNormal && s.status != StatusGapCommit {
		return
	}

	// If the request slot ID is not the next slot in the replica's log, skip the message
	lastSlotID := s.log.LastSlot()
	if request.SlotNum > lastSlotID+1 {
		return
	}

	// A no-op entry is represented as a missing entry
	s.log.Delete(request.SlotNum)

	// Increment the session message ID if necessary
	if request.SlotNum > s.log.LastSlot() {
		s.sessionMessageNum++
	}

	gapCommitReply := &GapCommitReply{
		Sender:  s.cluster.Member(),
		ViewID:  s.viewID,
		SlotNum: request.SlotNum,
	}
	message := &ReplicaMessage{
		Message: &ReplicaMessage_GapCommitReply{
			GapCommitReply: gapCommitReply,
		},
	}

	s.logger.SendTo("GapCommitReply", gapCommitReply, request.Sender)
	go s.send(message, request.Sender)
}

func (s *NOPaxos) handleGapCommitReply(reply *GapCommitReply) {
	s.logger.ReceiveFrom("GapCommitReply", reply, reply.Sender)

	s.mu.Lock()
	defer s.mu.Unlock()

	// If the view ID does not match the sender's view ID, skip the message
	if s.viewID.LeaderNum != reply.ViewID.LeaderNum || s.viewID.SessionNum != reply.ViewID.SessionNum {
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
	if reply.SlotNum != s.currentGapSlot {
		return
	}

	s.gapCommitReps[reply.Sender] = reply

	// Get the set of gap commit replies for the current slot
	gapCommits := make([]*GapCommitReply, 0, len(s.gapCommitReps))
	for _, gapCommit := range s.gapCommitReps {
		if gapCommit.ViewID.SessionNum == s.viewID.SessionNum && gapCommit.ViewID.LeaderNum == s.viewID.LeaderNum && gapCommit.SlotNum == s.currentGapSlot {
			gapCommits = append(gapCommits, gapCommit)
		}
	}

	// If a quorum of gap commits has been received for the slot, return the status to normal
	if len(gapCommits) >= s.cluster.QuorumSize() {
		s.setStatus(StatusNormal)
	}
}

func (s *NOPaxos) handleSlotLookup(request *SlotLookup) {
	s.logger.ReceiveFrom("SlotLookup", request, request.Sender)

	s.mu.RLock()
	defer s.mu.RUnlock()

	// If the view ID does not match the sender's view ID, skip the message
	if s.viewID.LeaderNum != request.ViewID.LeaderNum || s.viewID.SessionNum != request.ViewID.SessionNum {
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

	slotNum := s.log.LastSlot() + 1 - LogSlotID(s.sessionMessageNum-request.MessageNum)

	if slotNum <= s.log.LastSlot() {
		for i := slotNum; i <= s.log.LastSlot(); i++ {
			entry := s.log.Get(i)
			if entry != nil {
				commandRequest := &CommandRequest{
					SessionNum: s.viewID.SessionNum,
					MessageNum: entry.MessageNum,
					Value:      entry.Value,
				}
				message := &ReplicaMessage{
					Message: &ReplicaMessage_Command{
						Command: commandRequest,
					},
				}
				s.logger.SendTo("CommandRequest", commandRequest, request.Sender)
				go s.send(message, request.Sender)
			}
		}
	} else if slotNum == s.log.LastSlot()+1 {
		go s.sendGapCommit()
	}
}
