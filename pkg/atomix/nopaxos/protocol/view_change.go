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
	"math"
)

func (s *NOPaxos) startLeaderChange() {
	s.mu.RLock()
	newViewID := &ViewId{
		SessionNum: s.viewID.SessionNum,
		LeaderNum:  s.viewID.LeaderNum + 1,
	}
	s.mu.RUnlock()

	viewChangeRequest := &ViewChangeRequest{
		Sender: s.cluster.Member(),
		ViewID: newViewID,
	}
	message := &ReplicaMessage{
		Message: &ReplicaMessage_ViewChangeRequest{
			ViewChangeRequest: viewChangeRequest,
		},
	}

	for _, member := range s.cluster.Members() {
		if member != s.getLeader(newViewID) {
			if stream, err := s.cluster.GetStream(member); err == nil {
				s.logger.SendTo("ViewChangeRequest", viewChangeRequest, member)
				_ = stream.Send(message)
			}
		}
	}
}

func (s *NOPaxos) handleViewChangeRequest(request *ViewChangeRequest) {
	s.logger.ReceiveFrom("ViewChangeRequest", request, request.Sender)

	s.mu.RLock()
	newLeaderID := LeaderID(math.Max(float64(s.viewID.LeaderNum), float64(request.ViewID.LeaderNum)))
	newSessionID := SessionID(math.Max(float64(s.viewID.SessionNum), float64(request.ViewID.SessionNum)))
	newViewID := &ViewId{
		LeaderNum:  newLeaderID,
		SessionNum: newSessionID,
	}

	// If the view IDs match, ignore the request
	if s.viewID.LeaderNum == newViewID.LeaderNum && s.viewID.SessionNum == newViewID.SessionNum {
		s.mu.RUnlock()
		return
	}
	s.mu.RUnlock()
	s.mu.Lock()

	// Set the replica's status to ViewChange
	s.status = StatusViewChange

	// Set the replica's view ID to the new view ID
	s.viewID = newViewID

	// Reset the view changes
	s.viewChanges = make(map[MemberID]*ViewChange)

	s.mu.Unlock()
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Send a ViewChange to the new leader
	leader := s.getLeader(newViewID)
	stream, err := s.cluster.GetStream(leader)
	if err != nil {
		return
	}

	// Create a bloom filter of the log and add non-empty entries
	noOpFilter := bloom.New(uint(s.log.LastSlot()-s.log.FirstSlot()+1), bloomFilterHashFunctions)
	for slotNum := s.log.FirstSlot(); slotNum <= s.log.LastSlot(); slotNum++ {
		if entry := s.log.Get(slotNum); entry == nil {
			key := make([]byte, 8)
			binary.BigEndian.PutUint64(key, uint64(slotNum))
			noOpFilter.Add(key)
		}
	}

	// Marshall the bloom filter to bytes
	noOpFilterBytes, err := json.Marshal(noOpFilter)
	if err != nil {
		s.logger.Error("Failed to encode bloom filter", err)
		return
	}

	// Send a ViewChange message to the leader
	message := &ReplicaMessage{
		Message: &ReplicaMessage_ViewChange{
			ViewChange: &ViewChange{
				Sender:          s.cluster.Member(),
				ViewID:          newViewID,
				LastNormal:      s.lastNormView,
				MessageNum:      s.sessionMessageNum,
				NoOpFilter:      noOpFilterBytes,
				FirstLogSlotNum: s.log.FirstSlot(),
				LastLogSlotNum:  s.log.LastSlot(),
			},
		},
	}
	s.logger.SendTo("ViewChange", message, leader)
	_ = stream.Send(message)

	// Send a ViewChangeRequest to all other replicas
	viewChangeRequest := &ViewChangeRequest{
		Sender: s.cluster.Member(),
		ViewID: newViewID,
	}
	message = &ReplicaMessage{
		Message: &ReplicaMessage_ViewChangeRequest{
			ViewChangeRequest: viewChangeRequest,
		},
	}

	// Send a view change request to all replicas other than the leader
	for _, member := range s.cluster.Members() {
		if stream, err := s.cluster.GetStream(member); err == nil {
			s.logger.SendTo("ViewChangeRequest", viewChangeRequest, member)
			_ = stream.Send(message)
		}
	}
}

func (s *NOPaxos) handleViewChange(request *ViewChange) {
	s.logger.ReceiveFrom("ViewChange", request, request.Sender)

	s.mu.Lock()
	defer s.mu.Unlock()

	// If the view IDs do not match, ignore the request
	if s.viewID.LeaderNum != request.ViewID.LeaderNum || s.viewID.SessionNum != request.ViewID.SessionNum {
		return
	}

	// If the replica's status is not ViewChange, ignore the request
	if s.status != StatusViewChange {
		return
	}

	// If this replica is not the leader of the view, ignore the request
	if s.getLeader(request.ViewID) != s.cluster.Member() {
		return
	}

	// Add the view change to the set of view changes
	s.viewChanges[request.Sender] = request

	// Aggregate the view changes for the current view
	localViewChanged := false
	viewChanges := make([]*ViewChange, 0, len(s.viewChanges))
	for _, viewChange := range s.viewChanges {
		if viewChange.ViewID.SessionNum == s.viewID.SessionNum && viewChange.ViewID.LeaderNum == s.viewID.LeaderNum {
			viewChanges = append(viewChanges, viewChange)
			if viewChange.Sender == s.cluster.Member() {
				localViewChanged = true
			}
		}
	}

	// If the view changes have reached a quorum, start the new view
	if localViewChanged && len(viewChanges) >= s.cluster.QuorumSize() {
		// Create the state for the new view
		var lastNormal *ViewId
		for _, viewChange1 := range viewChanges {
			normal := true
			for _, viewChange2 := range viewChanges {
				if viewChange2.LastNormal.SessionNum > viewChange1.LastNormal.SessionNum || viewChange2.LastNormal.LeaderNum > viewChange1.LastNormal.LeaderNum {
					normal = false
					break
				}
			}
			if normal {
				lastNormal = viewChange1.LastNormal
			}
		}

		var newMessageID MessageID
		var minSlotNum, maxSlotNum LogSlotID

		noOpFilters := make(map[MemberID]*bloom.BloomFilter)
		for _, viewChange := range viewChanges {
			if viewChange.LastNormal.SessionNum == lastNormal.SessionNum && viewChange.LastNormal.LeaderNum == lastNormal.LeaderNum {
				noOpFilter := &bloom.BloomFilter{}
				if err := json.Unmarshal(viewChange.NoOpFilter, noOpFilter); err != nil {
					s.logger.Error("Failed to decode bloom filter", err)
					return
				}

				noOpFilters[viewChange.Sender] = noOpFilter

				// Record the min and max slot for all view changes
				if minSlotNum == 0 || viewChange.FirstLogSlotNum < minSlotNum {
					minSlotNum = viewChange.FirstLogSlotNum
				}
				if maxSlotNum == 0 || viewChange.LastLogSlotNum > maxSlotNum {
					maxSlotNum = viewChange.LastLogSlotNum
				}

				// If the session has changed, take the maximum message ID
				if lastNormal.SessionNum == s.viewID.SessionNum && viewChange.MessageNum > newMessageID {
					newMessageID = viewChange.MessageNum
				}
			}
		}

		var firstSlotNum, lastSlotNum LogSlotID
		newLog := newLog(minSlotNum)
		noOpSlots := make(map[MemberID][]LogSlotID)
		for slotNum := minSlotNum; slotNum <= maxSlotNum; slotNum++ {
			// If the entry is missing from the local log, it's a no-op.
			// If the entry is present, check no-op filters to determine whether to request
			// a repair from peers.
			if entry := s.log.Get(slotNum); entry != nil {
				newLog.Set(entry)

				// For each member, if the no-op is present in the member's filter request a repair.
				for member, noOpFilter := range noOpFilters {
					key := make([]byte, 8)
					binary.BigEndian.PutUint64(key, uint64(slotNum))
					if noOpFilter.Test(key) {
						slots := noOpSlots[member]
						if slots == nil {
							slots = make([]LogSlotID, 0)
						}
						noOpSlots[member] = append(slots, slotNum)
					}
				}
			}
		}

		// Set the view change log. Note this log is maintained separate from the primary log until the view is started.
		s.viewChangeLog = newLog

		// If there are any missing slots in the log, store the new log and send LogRepair requests to peers to
		// determine whether a no-op entry should be written to the log. Otherwise, send a StartView.
		if len(noOpSlots) > 0 {
			s.viewChangeRepairs = make(map[MemberID]*ViewChangeRepair)
			for member, slots := range noOpSlots {
				if stream, err := s.cluster.GetStream(member); err == nil {
					repair := &ViewChangeRepair{
						Sender:     s.cluster.Member(),
						ViewID:     s.viewID,
						MessageNum: newMessageID,
						SlotNums:   slots,
					}
					message := &ReplicaMessage{
						Message: &ReplicaMessage_ViewChangeRepair{
							ViewChangeRepair: repair,
						},
					}
					s.viewChangeRepairs[member] = repair
					s.logger.SendTo("ViewChangeRepair", repair, member)
					_ = stream.Send(message)
				}
			}
		} else {
			// Create a new no-op filter and add no-op entries
			newNoOpFilter := bloom.New(uint(lastSlotNum-firstSlotNum+1), bloomFilterHashFunctions)
			for slotNum := firstSlotNum; slotNum <= lastSlotNum; slotNum++ {
				if entry := newLog.Get(slotNum); entry == nil {
					key := make([]byte, 8)
					binary.BigEndian.PutUint64(key, uint64(slotNum))
					newNoOpFilter.Add(key)
				}
			}

			// Marshal the no-op filter to JSON
			newNoOpFilterJson, err := json.Marshal(newNoOpFilter)
			if err != nil {
				s.logger.Error("Failed to marshal bloom filter", err)
				return
			}

			// Create and send a StartView message to each replica with the no-op filter
			startView := &StartView{
				Sender:          s.cluster.Member(),
				ViewID:          s.viewID,
				MessageNum:      newMessageID,
				NoOpFilter:      newNoOpFilterJson,
				FirstLogSlotNum: firstSlotNum,
				LastLogSlotNum:  lastSlotNum,
			}
			message := &ReplicaMessage{
				Message: &ReplicaMessage_StartView{
					StartView: startView,
				},
			}

			// Send a StartView to each replica
			for _, member := range s.cluster.Members() {
				if stream, err := s.cluster.GetStream(member); err == nil {
					s.logger.SendTo("StartView", startView, member)
					_ = stream.Send(message)
				}
			}
		}
	}
}

func (s *NOPaxos) handleViewChangeRepair(request *ViewChangeRepair) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// If the request views do not match, ignore the request
	if s.viewID.SessionNum != request.ViewID.SessionNum || s.viewID.LeaderNum != request.ViewID.LeaderNum {
		return
	}

	// Lookup entries for the requested slots. For any entry that's present in the log,
	// append the slot num.
	slots := make([]LogSlotID, 0, len(request.SlotNums))
	for _, slotNum := range request.SlotNums {
		if entry := s.log.Get(slotNum); entry != nil {
			slots = append(slots, slotNum)
		}
	}

	// Send non-nil entries back to the sender
	if stream, err := s.cluster.GetStream(request.Sender); err == nil {
		viewChangeReply := &ViewChangeRepairReply{
			Sender:     s.cluster.Member(),
			ViewID:     s.viewID,
			MessageNum: request.MessageNum,
			SlotNums:   slots,
		}
		message := &ReplicaMessage{
			Message: &ReplicaMessage_ViewChangeRepairReply{
				ViewChangeRepairReply: viewChangeReply,
			},
		}
		s.logger.SendTo("ViewChangeRepairReply", viewChangeReply, request.Sender)
		_ = stream.Send(message)
	}
}

func (s *NOPaxos) handleViewChangeRepairReply(reply *ViewChangeRepairReply) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// If the reply view does not match the current view, skip the reply
	if s.viewID.SessionNum != reply.ViewID.SessionNum || s.viewID.LeaderNum != reply.ViewID.LeaderNum {
		return
	}

	// Add the reply to the SlotRepair replies list
	s.viewChangeRepairReps[reply.Sender] = reply

	// If all view repairs have been responded to, remove entries where any slot is empty
	// and populate slots where all entries have been returned
	if len(s.viewChangeRepairs) == len(s.viewChangeRepairReps) {
		// Compute the number of requests for each slot
		slots := make(map[LogSlotID]*repairState)
		for _, slotRepair := range s.viewChangeRepairs {
			for _, slotNum := range slotRepair.SlotNums {
				state := slots[slotNum]
				if state == nil {
					state = &repairState{}
					slots[slotNum] = state
				}
				state.requests++
			}
		}

		// For each repair reply, add the replies to each slot
		for _, viewRepairRep := range s.viewChangeRepairReps {
			for _, slotNum := range viewRepairRep.SlotNums {
				state := slots[slotNum]
				if state != nil {
					state.replies++
				}
			}
		}

		// For each slot, remove entries where the replies do not equal the requests
		for slotNum, slot := range slots {
			if slot.requests != slot.replies {
				s.viewChangeLog.Delete(slotNum)
			}
		}

		// Create a new no-op filter and add no-op entries
		newNoOpFilter := bloom.New(uint(s.log.LastSlot()-s.log.FirstSlot()+1), bloomFilterHashFunctions)
		for slotNum := s.log.FirstSlot(); slotNum <= s.log.LastSlot(); slotNum++ {
			if entry := s.log.Get(slotNum); entry == nil {
				key := make([]byte, 8)
				binary.BigEndian.PutUint64(key, uint64(slotNum))
				newNoOpFilter.Add(key)
			}
		}

		// Marshal the no-op filter to JSON
		newNoOpFilterJson, err := json.Marshal(newNoOpFilter)
		if err != nil {
			s.logger.Error("Failed to marshal bloom filter", err)
			return
		}

		// Create and send a StartView message to each replica with the no-op filter
		startView := &StartView{
			Sender:          s.cluster.Member(),
			ViewID:          s.viewID,
			MessageNum:      reply.MessageNum,
			NoOpFilter:      newNoOpFilterJson,
			FirstLogSlotNum: s.log.FirstSlot(),
			LastLogSlotNum:  s.log.LastSlot(),
		}
		message := &ReplicaMessage{
			Message: &ReplicaMessage_StartView{
				StartView: startView,
			},
		}

		// Send a StartView to each replica
		for _, member := range s.cluster.Members() {
			if stream, err := s.cluster.GetStream(member); err == nil {
				s.logger.SendTo("StartView", startView, member)
				_ = stream.Send(message)
			}
		}

		// Unset repair fields
		s.viewChangeRepairs = make(map[MemberID]*ViewChangeRepair)
		s.viewChangeRepairReps = make(map[MemberID]*ViewChangeRepairReply)
	}
}

// repairState holds the state of a slot repair
type repairState struct {
	requests int
	replies  int
}
