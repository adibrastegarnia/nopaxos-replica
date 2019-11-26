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

func (s *NOPaxos) sendPing() {
	s.stateMu.RLock()

	// If this replica is not the leader of the view, do not send the ping
	if s.getLeader(s.viewID) != s.cluster.Member() {
		s.stateMu.RUnlock()
		return
	}

	message := &ReplicaMessage{
		Message: &ReplicaMessage_Ping{
			Ping: &Ping{
				Sender: s.cluster.Member(),
				ViewID: s.viewID,
			},
		},
	}
	s.stateMu.RUnlock()

	for _, member := range s.cluster.Members() {
		if member != s.cluster.Member() {
			if stream, err := s.cluster.GetStream(member); err == nil {
				s.logger.SendTo("Ping", message, member)
				_ = stream.Send(message)
			}
		}
	}
}

func (s *NOPaxos) handlePing(request *Ping) {
	s.logger.ReceiveFrom("Ping", request, request.Sender)

	s.stateMu.Lock()
	defer s.stateMu.Unlock()

	// If the view IDs do not match, ignore the request
	if s.viewID.LeaderNum != request.ViewID.LeaderNum || s.viewID.SessionNum != request.ViewID.SessionNum {
		return
	}

	// If the replica's status is not Normal, ignore the request
	if s.status != StatusNormal {
		return
	}

	s.timeoutTimer.Reset(s.config.GetLeaderTimeoutOrDefault())
}

func (s *NOPaxos) Timeout() {
	s.stateMu.RLock()
	if s.getLeader(s.viewID) != s.cluster.Member() {
		s.stateMu.RUnlock()
		s.logger.Debug("Leader ping timed out")
		s.startLeaderChange()
	} else {
		s.stateMu.RUnlock()
	}
}
