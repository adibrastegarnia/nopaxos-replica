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

// LogSlotID is a log slot number
type LogSlotID uint64

// newLog returns a new NOPaxos log
func newLog(firstSlot LogSlotID) *Log {
	return &Log{
		firstSlotNum: firstSlot,
		entries:      make(map[LogSlotID]*LogEntry),
	}
}

// Log is a NOPaxos log
type Log struct {
	firstSlotNum LogSlotID
	lastSlotNum  LogSlotID
	entries      map[LogSlotID]*LogEntry
}

// Len returns the length of the log
func (l *Log) Len() int {
	return len(l.entries)
}

// FirstSlot returns the first slot in the log
func (l *Log) FirstSlot() LogSlotID {
	return l.firstSlotNum
}

// LastSlot returns the last slot in the log
func (l *Log) LastSlot() LogSlotID {
	return l.lastSlotNum
}

// Get gets a log entry
func (l *Log) Get(slotNum LogSlotID) *LogEntry {
	return l.entries[slotNum]
}

// Set sets a log entry
func (l *Log) Set(entry *LogEntry) {
	l.entries[entry.SlotNum] = entry
	if entry.SlotNum < l.firstSlotNum {
		l.firstSlotNum = entry.SlotNum
	}
	if entry.SlotNum > l.lastSlotNum {
		l.lastSlotNum = entry.SlotNum
	}
}

// Delete deletes a log entry
func (l *Log) Delete(slotNum LogSlotID) {
	delete(l.entries, slotNum)
}

// Truncate removes entries from the head of the log
func (l *Log) Truncate(slotNum LogSlotID) {
	for slot := l.firstSlotNum; slot < slotNum; slot++ {
		l.Delete(slot)
	}
	l.firstSlotNum = slotNum
}

// Extend extends the tail of the log
func (l *Log) Extend(slotNum LogSlotID) {
	if slotNum > l.lastSlotNum {
		l.lastSlotNum = slotNum
	}
}
