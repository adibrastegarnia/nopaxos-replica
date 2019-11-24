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

package state

import (
	"github.com/atomix/atomix-go-node/pkg/atomix/node"
	"github.com/atomix/atomix-go-node/pkg/atomix/service"
	nopaxos "github.com/atomix/atomix-nopaxos-node/pkg/atomix/nopaxos/protocol"
	"github.com/atomix/atomix-nopaxos-node/pkg/atomix/nopaxos/store"
	"github.com/atomix/atomix-nopaxos-node/pkg/atomix/nopaxos/store/log"
	"github.com/atomix/atomix-nopaxos-node/pkg/atomix/nopaxos/util"
	"time"
)

// NewManager returns a new NOPaxos state manager
func NewManager(member nopaxos.MemberID, store store.Store, registry *node.Registry) Manager {
	sm := &manager{
		member: member,
		log:    util.NewNodeLogger(string(member)),
		reader: store.Log().OpenReader(0),
		ch:     make(chan *change, stateBufferSize),
	}
	sm.state = node.NewPrimitiveStateMachine(registry, sm)
	go sm.start()
	return sm
}

// Manager provides a state machine to which to apply NOPaxos log entries
type Manager interface {
	// ApplyIndex reads and applies the given slot to the state machine
	ApplyIndex(slotID nopaxos.LogSlotID)

	// Apply applies a committed entry to the state machine
	ApplyEntry(entry *log.Entry, ch chan<- node.Output)

	// Close closes the state manager
	Close() error
}

const (
	// stateBufferSize is the size of the state manager's change channel buffer
	stateBufferSize = 1024
)

// manager manages the NOPaxos state machine
type manager struct {
	member       nopaxos.MemberID
	state        node.StateMachine
	log          util.Logger
	currentIndex nopaxos.LogSlotID
	currentTime  time.Time
	lastApplied  nopaxos.LogSlotID
	reader       log.Reader
	operation    service.OperationType
	ch           chan *change
}

// Node returns the local node identifier
func (m *manager) Node() string {
	return string(m.member)
}

// applyIndex applies entries up to the given slot
func (m *manager) ApplyIndex(slotID nopaxos.LogSlotID) {
	m.ch <- &change{
		entry: &log.Entry{
			SlotID: slotID,
		},
	}
}

// ApplyEntry enqueues the given entry to be applied to the state machine, returning output on the given channel
func (m *manager) ApplyEntry(entry *log.Entry, ch chan<- node.Output) {
	m.ch <- &change{
		entry:  entry,
		result: ch,
	}
}

func (m *manager) updateClock(slotID nopaxos.LogSlotID, timestamp time.Time) {
	m.currentIndex = slotID
	if timestamp.UnixNano() > m.currentTime.UnixNano() {
		m.currentTime = timestamp
	}
}

// start begins applying entries to the state machine
func (m *manager) start() {
	for change := range m.ch {
		m.execChange(change)
	}
}

// execChange executes the given change on the state machine
func (m *manager) execChange(change *change) {
	defer func() {
		err := recover()
		if err != nil {
			m.log.Error("Recovered from panic %v", err)
		}
	}()
	if change.entry.Entry != nil {
		// If the entry is a query, apply it without incrementing the lastApplied slot
		if query, ok := change.entry.Entry.Entry.(*nopaxos.LogEntry_Query); ok {
			m.execQuery(change.entry.SlotID, change.entry.Entry.Timestamp, query.Query, change.result)
		} else {
			m.execPendingChanges(change.entry.SlotID - 1)
			m.execEntry(change.entry, change.result)
			m.lastApplied = change.entry.SlotID
		}
	} else if change.entry.SlotID > m.lastApplied {
		m.execPendingChanges(change.entry.SlotID - 1)
		m.execEntry(change.entry, change.result)
		m.lastApplied = change.entry.SlotID
	}
}

// execPendingChanges reads and executes changes up to the given slot
func (m *manager) execPendingChanges(slotID nopaxos.LogSlotID) {
	if m.lastApplied < slotID {
		for m.lastApplied < slotID {
			entry := m.reader.NextEntry()
			if entry != nil {
				m.execEntry(entry, nil)
				m.lastApplied = entry.SlotID
			} else {
				return
			}
		}
	}
}

// execEntry applies the given entry to the state machine and returns the result(s) on the given channel
func (m *manager) execEntry(entry *log.Entry, ch chan<- node.Output) {
	if entry.Entry == nil {
		m.reader.Reset(entry.SlotID)
		entry = m.reader.NextEntry()
	}

	switch e := entry.Entry.Entry.(type) {
	case *nopaxos.LogEntry_Query:
		m.execQuery(entry.SlotID, entry.Entry.Timestamp, e.Query, ch)
	case *nopaxos.LogEntry_Command:
		m.log.Trace("Applying command %d", entry.SlotID)
		m.execCommand(entry.SlotID, entry.Entry.Timestamp, e.Command, ch)
	}
}

func (m *manager) execQuery(slotID nopaxos.LogSlotID, timestamp time.Time, query *nopaxos.QueryEntry, ch chan<- node.Output) {
	m.log.Trace("Applying query %d", slotID)
	m.operation = service.OpTypeQuery
	m.state.Query(query.Value, ch)
}

func (m *manager) execCommand(slotID nopaxos.LogSlotID, timestamp time.Time, command *nopaxos.CommandEntry, ch chan<- node.Output) {
	m.updateClock(slotID, timestamp)
	m.operation = service.OpTypeCommand
	m.state.Command(command.Value, ch)
}

type change struct {
	entry  *log.Entry
	result chan<- node.Output
}

func (m *manager) Index() uint64 {
	return uint64(m.currentIndex)
}

func (m *manager) Timestamp() time.Time {
	return m.currentTime
}

func (m *manager) OperationType() service.OperationType {
	return m.operation
}

func (m *manager) Close() error {
	return nil
}
