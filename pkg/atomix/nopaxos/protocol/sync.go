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

func (s *NOPaxos) startSync() {
	s.mu.Lock()
	defer s.mu.Unlock()

	// If this replica is not the leader of the view, ignore the request
	if s.getLeader(s.viewID) != s.cluster.Member() {
		return
	}

	// If the replica's status is not Normal, do not attempt the sync
	if s.status != StatusNormal {
		return
	}

	s.syncReps = make(map[MemberID]*SyncReply)
	s.tentativeSync = s.log.LastSlot()

	// Create a no-op filter
	noOpFilter := newNoOpFilter(s.log.FirstSlot(), s.log.LastSlot())
	for slotNum := s.log.FirstSlot(); slotNum <= s.log.LastSlot(); slotNum++ {
		if entry := s.log.Get(slotNum); entry == nil {
			noOpFilter.add(slotNum)
		}
	}

	// Marshall the bloom filter to bytes
	noOpFilterBytes, err := noOpFilter.marshal()
	if err != nil {
		s.logger.Error("Failed to marshal bloom filter", err)
		return
	}

	syncPrepare := &SyncPrepare{
		Sender:          s.cluster.Member(),
		ViewID:          s.viewID,
		MessageNum:      s.sessionMessageNum,
		NoOpFilter:      noOpFilterBytes,
		FirstLogSlotNum: s.log.FirstSlot(),
		LastLogSlotNum:  s.log.LastSlot(),
	}
	message := &ReplicaMessage{
		Message: &ReplicaMessage_SyncPrepare{
			SyncPrepare: syncPrepare,
		},
	}

	for _, member := range s.cluster.Members() {
		if member != s.cluster.Member() {
			s.logger.SendTo("SyncPrepare", syncPrepare, member)
			go s.send(message, member)
		}
	}
}

func (s *NOPaxos) handleSyncPrepare(request *SyncPrepare) {
	s.logger.ReceiveFrom("SyncPrepare", request, request.Sender)

	s.mu.Lock()
	defer s.mu.Unlock()

	// If the replica's status is not Normal, ignore the request
	if s.status != StatusNormal {
		return
	}

	// If the view IDs do not match, ignore the request
	if s.viewID.LeaderNum != request.ViewID.LeaderNum || s.viewID.SessionNum != request.ViewID.SessionNum {
		return
	}

	// If the sender is not the leader for the current view, ignore the request
	if request.Sender != s.getLeader(request.ViewID) {
		return
	}

	// Unmarshal the leader's no-op filter
	noOpFilter := newNoOpFilter(request.FirstLogSlotNum, request.LastLogSlotNum)
	if err := noOpFilter.unmarshal(request.NoOpFilter); err != nil {
		s.logger.Error("Failed to decode bloom filter", err)
		return
	}

	newLog := newLog(request.FirstLogSlotNum)
	entrySlots := make([]LogSlotID, 0)
	for slotNum := request.FirstLogSlotNum; slotNum <= request.LastLogSlotNum; slotNum++ {
		// If the entry is greater than the last in the replica's log, request it.
		if entry := s.log.Get(slotNum); entry != nil {
			// If the entry is missing from the leader's log, request it. Otherwise add it to the new log.
			if noOpFilter.isMaybeNoOp(slotNum) {
				entrySlots = append(entrySlots, slotNum)
			} else {
				newLog.Set(entry)
			}
		} else if slotNum > s.log.LastSlot() {
			entrySlots = append(entrySlots, slotNum)
		}
	}

	// If the view's checkpoint is newer than the local checkpoint, request a checkpoint
	var checkpoint LogSlotID
	if request.FirstLogSlotNum > 1 && (s.currentCheckpoint == nil || request.FirstLogSlotNum > s.currentCheckpoint.SlotNum+1) {
		checkpoint = request.FirstLogSlotNum - 1
	}

	// If any entries need to be requested from the leader, request them. Otherwise, send a SyncReply
	if len(entrySlots) > 0 || checkpoint > 0 {
		leader := s.getLeader(s.viewID)
		syncRepair := &SyncRepair{
			Sender:     s.cluster.Member(),
			ViewID:     s.viewID,
			Checkpoint: checkpoint,
			SlotNums:   entrySlots,
		}
		message := &ReplicaMessage{
			Message: &ReplicaMessage_SyncRepair{
				SyncRepair: syncRepair,
			},
		}
		s.logger.SendTo("SyncRepair", syncRepair, leader)
		go s.send(message, leader)
	} else {
		s.sessionMessageNum = s.sessionMessageNum + MessageID(newLog.LastSlot()-s.log.LastSlot())
		s.log = newLog

		// Send a SyncReply back to the leader
		syncReply := &SyncReply{
			Sender:  s.cluster.Member(),
			ViewID:  s.viewID,
			SlotNum: s.log.LastSlot(),
		}
		message := &ReplicaMessage{
			Message: &ReplicaMessage_SyncReply{
				SyncReply: syncReply,
			},
		}
		s.logger.SendTo("SyncReply", syncReply, request.Sender)
		go s.send(message, request.Sender)

		// Send a RequestReply for all entries in the new log
		sequencer := s.sequencer

		if sequencer != nil {
			for slotNum := s.log.FirstSlot(); slotNum <= s.log.LastSlot(); slotNum++ {
				entry := s.log.Get(slotNum)
				if entry != nil {
					_ = sequencer.Send(&ClientMessage{
						Message: &ClientMessage_CommandReply{
							CommandReply: &CommandReply{
								MessageNum: entry.MessageNum,
								Sender:     s.cluster.Member(),
								ViewID:     s.viewID,
								SlotNum:    slotNum,
							},
						},
					})
				}
			}
		}
	}
}

func (s *NOPaxos) handleSyncRepair(request *SyncRepair) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// If the request views do not match, ignore the request
	if s.viewID.SessionNum != request.ViewID.SessionNum || s.viewID.LeaderNum != request.ViewID.LeaderNum {
		return
	}

	// Lookup entries for the requested slots
	entries := make([]*LogEntry, 0, len(request.SlotNums))
	for _, slotNum := range request.SlotNums {
		if entry := s.log.Get(slotNum); entry != nil {
			entries = append(entries, entry)
		}
	}

	// If a checkpoint was requested, return the checkpoint
	var checkpointSlotNum LogSlotID
	var checkpointData []byte
	if request.Checkpoint > 0 && s.currentCheckpoint != nil && request.Checkpoint <= s.currentCheckpoint.SlotNum {
		checkpointSlotNum = s.currentCheckpoint.SlotNum
		checkpointData = s.currentCheckpoint.Data
	}

	// Send non-nil entries back to the sender
	syncRepairReply := &SyncRepairReply{
		Sender:            s.cluster.Member(),
		ViewID:            s.viewID,
		CheckpointSlotNum: checkpointSlotNum,
		Checkpoint:        checkpointData,
		Entries:           entries,
	}
	message := &ReplicaMessage{
		Message: &ReplicaMessage_SyncRepairReply{
			SyncRepairReply: syncRepairReply,
		},
	}
	s.logger.SendTo("SyncRepairReply", syncRepairReply, request.Sender)
	go s.send(message, request.Sender)
}

func (s *NOPaxos) handleSyncRepairReply(reply *SyncRepairReply) {
	// If the request views do not match, ignore the reply
	if s.viewID.SessionNum != reply.ViewID.SessionNum || s.viewID.LeaderNum != reply.ViewID.LeaderNum {
		return
	}

	// If no sync repair request is stored, ignore the reply
	request := s.syncRepair
	if request == nil || s.syncLog == nil {
		return
	}

	// If a checkpoint was returned and the checkpoint is newer than the local checkpoint, store the checkpoint
	if reply.CheckpointSlotNum > 0 && (s.currentCheckpoint == nil || reply.CheckpointSlotNum > s.currentCheckpoint.SlotNum) {
		s.currentCheckpoint = newCheckpoint(reply.CheckpointSlotNum)
		s.currentCheckpoint.Data = reply.Checkpoint
	}

	// Create a map of log entries
	entries := make(map[LogSlotID]*LogEntry)
	for _, entry := range reply.Entries {
		entries[entry.SlotNum] = entry
	}

	// For each requested slot, store the entry if one was returned. Otherwise, remove the entry
	for _, slotNum := range request.SlotNums {
		if entry := entries[slotNum]; entry != nil {
			s.syncLog.Set(entry)
		} else {
			s.syncLog.Delete(slotNum)
		}
	}

	// Once the repair is complete, send a SyncReply
	s.sessionMessageNum = s.sessionMessageNum + MessageID(s.syncLog.LastSlot()-s.log.LastSlot())
	s.log = s.syncLog
	s.syncLog = nil

	// Send a SyncReply back to the leader
	syncReply := &SyncReply{
		Sender:  s.cluster.Member(),
		ViewID:  s.viewID,
		SlotNum: s.log.LastSlot(),
	}
	message := &ReplicaMessage{
		Message: &ReplicaMessage_SyncReply{
			SyncReply: syncReply,
		},
	}
	s.logger.SendTo("SyncReply", syncReply, reply.Sender)
	go s.send(message, reply.Sender)

	// Send a RequestReply for all entries in the new log
	sequencer := s.sequencer

	if sequencer != nil {
		for slotNum := s.log.FirstSlot(); slotNum <= s.log.LastSlot(); slotNum++ {
			entry := s.log.Get(slotNum)
			if entry != nil {
				_ = sequencer.Send(&ClientMessage{
					Message: &ClientMessage_CommandReply{
						CommandReply: &CommandReply{
							MessageNum: entry.MessageNum,
							Sender:     s.cluster.Member(),
							ViewID:     s.viewID,
							SlotNum:    slotNum,
						},
					},
				})
			}
		}
	}
}

func (s *NOPaxos) handleSyncReply(reply *SyncReply) {
	s.logger.ReceiveFrom("SyncReply", reply, reply.Sender)

	s.mu.RLock()
	defer s.mu.RUnlock()

	// If the view IDs do not match, ignore the request
	if s.viewID.LeaderNum != reply.ViewID.LeaderNum || s.viewID.SessionNum != reply.ViewID.SessionNum {
		return
	}

	// If the replica's status is not Normal, ignore the request
	if s.status != StatusNormal {
		return
	}

	// Add the reply to the set of sync replies
	s.syncReps[reply.Sender] = reply

	localSynced := false
	syncReps := make([]*SyncReply, 0, len(s.syncReps))
	for _, syncRep := range s.syncReps {
		if syncRep.ViewID.LeaderNum == s.viewID.LeaderNum && syncRep.ViewID.SessionNum == s.viewID.SessionNum && syncRep.SlotNum == s.tentativeSync {
			syncReps = append(syncReps, syncRep)
			if syncRep.Sender == s.cluster.Member() {
				localSynced = true
			}
		}
	}

	sessionMessageNum := s.sessionMessageNum - MessageID(s.log.LastSlot()-s.tentativeSync)

	if localSynced && len(syncReps) >= s.cluster.QuorumSize() {
		commit := &SyncCommit{
			Sender:     s.cluster.Member(),
			ViewID:     s.viewID,
			MessageNum: sessionMessageNum,
			SyncPoint:  s.tentativeSync,
		}
		message := &ReplicaMessage{
			Message: &ReplicaMessage_SyncCommit{
				SyncCommit: commit,
			},
		}

		for _, member := range s.cluster.Members() {
			if member != s.cluster.Member() {
				s.logger.SendTo("SyncCommit", commit, member)
				go s.send(message, member)
			}
		}
	}
}

func (s *NOPaxos) handleSyncCommit(request *SyncCommit) {
	s.logger.ReceiveFrom("SyncCommit", request, request.Sender)

	s.mu.RLock()
	defer s.mu.RUnlock()

	// If the replica's status is not Normal, ignore the request
	if s.status != StatusNormal {
		return
	}

	// If the view IDs do not match, ignore the request
	if s.viewID.LeaderNum != request.ViewID.LeaderNum || s.viewID.SessionNum != request.ViewID.SessionNum {
		return
	}

	// If the sender is not the leader for the current view, ignore the request
	if request.Sender != s.getLeader(request.ViewID) {
		return
	}

	// If a checkpoint exists and is less than the sync point, restore the checkpoint
	if s.currentCheckpoint != nil && s.currentCheckpoint.SlotNum <= request.SyncPoint && s.currentCheckpoint.SlotNum > s.applied {
		s.state.restore(s.currentCheckpoint)
		s.applied = s.currentCheckpoint.SlotNum
	}

	for slotNum := s.applied + 1; slotNum <= request.SyncPoint; slotNum++ {
		entry := s.log.Get(slotNum)
		if entry != nil {
			s.state.applyCommand(entry, nil)
		}
		s.applied = slotNum
	}
}
