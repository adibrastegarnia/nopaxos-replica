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
	"encoding/binary"
	"encoding/json"
	"github.com/willf/bloom"
)

func (s *NOPaxos) handleStartView(request *StartView) {
	s.logger.ReceiveFrom("StartView", request, request.Sender)

	s.mu.Lock()
	defer s.mu.Unlock()

	// If the local view is newer than the request view, skip the view
	if s.viewID.SessionNum > request.ViewID.SessionNum && s.viewID.LeaderNum > request.ViewID.LeaderNum {
		return
	}

	// If the views match and the replica is not in the ViewChange state, skip the view
	if s.viewID.SessionNum == request.ViewID.SessionNum && s.viewID.LeaderNum == request.ViewID.LeaderNum && s.status != StatusViewChange {
		return
	}

	// Unmarshal the leader's no-op filter
	noOpFilter := &bloom.BloomFilter{}
	if err := json.Unmarshal(request.NoOpFilter, noOpFilter); err != nil {
		s.logger.Error("Failed to decode bloom filter", err)
		return
	}

	newLog := newLog(request.FirstLogSlotNum)
	entrySlots := make([]LogSlotID, 0)
	for slotNum := request.FirstLogSlotNum; slotNum <= request.LastLogSlotNum; slotNum++ {
		// If the entry is greater than the last in the replica's log, request it.
		if entry := s.log.Get(slotNum); entry != nil {
			// If the entry is missing from the leader's log, request it. Otherwise add it to the new log.
			key := make([]byte, 8)
			binary.BigEndian.PutUint64(key, uint64(slotNum))
			if noOpFilter.Test(key) {
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

	// If any entries need to be requested from the leader, request them
	if len(entrySlots) > 0 {
		leader := s.getLeader(s.viewID)
		if stream, err := s.cluster.GetStream(leader); err == nil {
			repair := &ViewRepair{
				Sender:     s.cluster.Member(),
				ViewID:     s.viewID,
				MessageNum: request.MessageNum,
				Checkpoint: checkpoint,
				SlotNums:   entrySlots,
			}
			message := &ReplicaMessage{
				Message: &ReplicaMessage_ViewRepair{
					ViewRepair: repair,
				},
			}
			s.viewRepair = repair
			s.logger.SendTo("ViewRepair", repair, leader)
			_ = stream.Send(message)
		}
	} else {
		s.log = newLog
		s.sessionMessageNum = request.MessageNum
		s.setStatus(StatusNormal)
		s.viewID = request.ViewID
		s.lastNormView = request.ViewID

		// Send a reply for all commands in the log
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

	s.resetTimeout()
}

func (s *NOPaxos) handleViewRepair(request *ViewRepair) {
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
	if stream, err := s.cluster.GetStream(request.Sender); err == nil {
		viewRepairReply := &ViewRepairReply{
			Sender:            s.cluster.Member(),
			ViewID:            s.viewID,
			CheckpointSlotNum: checkpointSlotNum,
			Checkpoint:        checkpointData,
			Entries:           entries,
		}
		message := &ReplicaMessage{
			Message: &ReplicaMessage_ViewRepairReply{
				ViewRepairReply: viewRepairReply,
			},
		}
		s.logger.SendTo("ViewRepairReply", viewRepairReply, request.Sender)
		_ = stream.Send(message)
	}
}

func (s *NOPaxos) handleViewRepairReply(reply *ViewRepairReply) {
	// If the request views do not match, ignore the reply
	if s.viewID.SessionNum != reply.ViewID.SessionNum || s.viewID.LeaderNum != reply.ViewID.LeaderNum {
		return
	}

	// If no view repair request is stored, ignore the reply
	request := s.viewRepair
	if request == nil || s.viewLog == nil {
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
			s.viewLog.Set(entry)
		} else {
			s.viewLog.Delete(slotNum)
		}
	}

	s.log = s.viewLog
	s.sessionMessageNum = request.MessageNum
	s.setStatus(StatusNormal)
	s.viewID = request.ViewID
	s.lastNormView = request.ViewID

	// If a checkpoint exists and is less than the sync point, restore the checkpoint
	if s.currentCheckpoint != nil && s.currentCheckpoint.SlotNum <= s.log.LastSlot() && s.currentCheckpoint.SlotNum > s.applied {
		s.state.restore(s.currentCheckpoint)
		s.applied = s.currentCheckpoint.SlotNum
	}

	sequencer := s.sequencer
	for slotNum := s.applied + 1; slotNum <= s.log.LastSlot(); slotNum++ {
		entry := s.log.Get(slotNum)
		if entry != nil {
			s.state.applyCommand(entry, nil)
			if sequencer != nil {
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
		s.applied = slotNum
	}
}
