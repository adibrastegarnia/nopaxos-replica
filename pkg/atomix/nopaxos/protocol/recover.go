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

func (s *NOPaxos) startRecovery() {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Set the replica's status to Recovering
	s.setStatus(StatusRecovering)

	// Request a recovery from all other replicas
	recover := &Recover{
		Sender:     s.cluster.Member(),
		RecoveryID: s.recoveryID,
	}
	for _, member := range s.cluster.Members() {
		if stream, err := s.cluster.GetStream(member); err == nil {
			s.logger.SendTo("Recover", recover, member)
			_ = stream.Send(&ReplicaMessage{
				Message: &ReplicaMessage_Recover{
					Recover: recover,
				},
			})
		}
	}
}

func (s *NOPaxos) handleRecover(request *Recover) {
	s.logger.ReceiveFrom("Recover", request, request.Sender)

	s.mu.RLock()
	defer s.mu.RUnlock()

	// If this node is recovering, return an empty view
	if s.status == StatusRecovering {
		if stream, err := s.cluster.GetStream(request.Sender); err == nil {
			reply := &RecoverReply{
				Sender:     s.cluster.Member(),
				RecoveryID: request.RecoveryID,
			}
			s.logger.SendTo("RecoverReply", reply, request.Sender)
			_ = stream.Send(&ReplicaMessage{
				Message: &ReplicaMessage_RecoverReply{
					RecoverReply: reply,
				},
			})
		}
	}

	// If this node is not in the normal state, ignore the recovery
	if s.status != StatusNormal {
		return
	}

	// If this node is the leader for its view, send the checkpoint and log
	var log []*LogEntry
	var checkpointSlotNum LogSlotID
	var checkpointData []byte
	if s.getLeader(s.viewID) == s.cluster.Member() {
		log := make([]*LogEntry, 0, s.log.LastSlot()-s.log.FirstSlot()+1)
		for slotNum := s.log.FirstSlot(); slotNum <= s.log.LastSlot(); slotNum++ {
			if entry := s.log.Get(slotNum); entry != nil {
				log = append(log, entry)
			}
		}
		if s.currentCheckpoint != nil {
			checkpointData = s.currentCheckpoint.Data
		}
	}

	// Send a RecoverReply back to the recovering node
	if stream, err := s.cluster.GetStream(request.Sender); err == nil {
		reply := &RecoverReply{
			Sender:            s.cluster.Member(),
			RecoveryID:        request.RecoveryID,
			ViewID:            s.viewID,
			MessageNum:        s.sessionMessageNum,
			CheckpointSlotNum: checkpointSlotNum,
			Checkpoint:        checkpointData,
			Log:               log,
		}
		s.logger.SendTo("RecoverReply", reply, request.Sender)
		_ = stream.Send(&ReplicaMessage{
			Message: &ReplicaMessage_RecoverReply{
				RecoverReply: reply,
			},
		})
	}
}

func (s *NOPaxos) handleRecoverReply(reply *RecoverReply) {
	s.logger.ReceiveFrom("RecoverReply", reply, reply.Sender)

	s.mu.Lock()
	defer s.mu.Unlock()

	// If this node is not recovering, ignore the reply
	if s.status != StatusRecovering {
		return
	}

	// If the recovery ID does not match, ignore the reply
	if reply.RecoveryID != s.recoveryID {
		return
	}

	// Add the reply to the set of replies
	s.recoverReps[reply.Sender] = reply

	// Determine the most recent view in the replies and count the number of recovering nodes
	recovering := 0
	for _, recoverReply := range s.recoverReps {
		if recoverReply.ViewID == nil {
			recovering++
		} else if recoverReply.ViewID.SessionNum > s.viewID.SessionNum || (recoverReply.ViewID.SessionNum == s.viewID.SessionNum && recoverReply.ViewID.LeaderNum > s.viewID.LeaderNum) {
			s.viewID = recoverReply.ViewID
		}
	}

	// Ensure we've received a reply from the leader of the view
	leaderReply := s.recoverReps[s.getLeader(s.viewID)]
	if leaderReply == nil {
		return
	}

	// Aggregate the replies changes for the most recent view
	viewReplies := make([]*RecoverReply, 0, len(s.recoverReps))
	for _, recoverReply := range s.recoverReps {
		if recoverReply.ViewID != nil && recoverReply.ViewID.SessionNum == s.viewID.SessionNum && recoverReply.ViewID.LeaderNum == s.viewID.LeaderNum {
			viewReplies = append(viewReplies, recoverReply)
		}
	}

	// If a quorum of nodes are recovering, start in the initial state
	// Otherwise, if a quorum of views has been received, initialize the state from the leader
	if recovering >= s.cluster.QuorumSize() {
		s.setStatus(StatusNormal)
	} else if len(viewReplies) >= s.cluster.QuorumSize() {
		if len(leaderReply.Log) > 0 {
			newLog := newLog(leaderReply.Log[0].SlotNum)
			for _, entry := range leaderReply.Log {
				newLog.Set(entry)
			}
			s.log = newLog
		}

		if leaderReply.CheckpointSlotNum > 0 {
			s.currentCheckpoint = newCheckpoint(leaderReply.CheckpointSlotNum)
			s.currentCheckpoint.Data = leaderReply.Checkpoint
		}
		s.sessionMessageNum = leaderReply.MessageNum
		s.setStatus(StatusNormal)
	}
}
