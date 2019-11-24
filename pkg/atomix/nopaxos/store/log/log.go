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

package log

import (
	nopaxos "github.com/atomix/atomix-nopaxos-node/pkg/atomix/nopaxos/protocol"
	"io"
)

// NewMemoryLog creates a new in-memory Log
func NewMemoryLog() Log {
	log := &memoryLog{
		entries:    make([]*Entry, 0, 1024),
		firstSlotID: 1,
		readers:    make([]*memoryReader, 0, 10),
	}
	log.writer = &memoryWriter{
		log: log,
	}
	return log
}

// Log provides for reading and writing entries in the NOPaxos log
type Log interface {
	io.Closer

	// Writer returns the NOPaxos log writer
	Writer() Writer

	// OpenReader opens a NOPaxos log reader
	OpenReader(index nopaxos.LogSlotID) Reader
}

// Writer supports writing entries to the NOPaxos log
type Writer interface {
	io.Closer

	// LastSlotID returns the last index written to the log
	LastSlotID() nopaxos.LogSlotID

	// LastEntry returns the last entry written to the log
	LastEntry() *Entry

	// Append appends the given entry to the log
	Append(entry *nopaxos.LogEntry) *Entry

	// Reset resets the log writer to the given index
	Reset(index nopaxos.LogSlotID)

	// Truncate truncates the tail of the log to the given index
	Truncate(index nopaxos.LogSlotID)
}

// Reader supports reading of entries from the NOPaxos log
type Reader interface {
	io.Closer

	// FirstSlotID returns the first index in the log
	FirstSlotID() nopaxos.LogSlotID

	// LastSlotID returns the last index in the log
	LastSlotID() nopaxos.LogSlotID

	// CurrentSlotID returns the current index of the reader
	CurrentSlotID() nopaxos.LogSlotID

	// CurrentEntry returns the current Entry
	CurrentEntry() *Entry

	// NextSlotID returns the next index in the log
	NextSlotID() nopaxos.LogSlotID

	// NextEntry advances the log index and returns the next entry in the log
	NextEntry() *Entry

	// Reset resets the log reader to the given index
	Reset(index nopaxos.LogSlotID)
}

// Entry is an indexed NOPaxos log entry
type Entry struct {
	SlotID nopaxos.LogSlotID
	Entry *nopaxos.LogEntry
}

type memoryLog struct {
	entries    []*Entry
	firstSlotID nopaxos.LogSlotID
	writer     *memoryWriter
	readers    []*memoryReader
}

func (l *memoryLog) Writer() Writer {
	return l.writer
}

func (l *memoryLog) OpenReader(index nopaxos.LogSlotID) Reader {
	readerSlotID := -1
	for i := 0; i < len(l.entries); i++ {
		if l.entries[i].SlotID == index {
			readerSlotID = i - 1
			break
		}
	}
	reader := &memoryReader{
		log:   l,
		index: readerSlotID,
	}
	l.readers = append(l.readers, reader)
	return reader
}

func (l *memoryLog) Close() error {
	return nil
}

type memoryWriter struct {
	log *memoryLog
}

func (w *memoryWriter) LastSlotID() nopaxos.LogSlotID {
	if entry := w.LastEntry(); entry != nil {
		return entry.SlotID
	}
	return w.log.firstSlotID - 1
}

func (w *memoryWriter) LastEntry() *Entry {
	if len(w.log.entries) == 0 {
		return nil
	}
	return w.log.entries[len(w.log.entries)-1]
}

func (w *memoryWriter) nextSlotID() nopaxos.LogSlotID {
	if len(w.log.entries) == 0 {
		return w.log.firstSlotID
	}
	return w.log.entries[len(w.log.entries)-1].SlotID + 1
}

func (w *memoryWriter) Append(entry *nopaxos.LogEntry) *Entry {
	indexed := &Entry{
		SlotID: w.nextSlotID(),
		Entry: entry,
	}
	w.log.entries = append(w.log.entries, indexed)
	return indexed
}

func (w *memoryWriter) Reset(index nopaxos.LogSlotID) {
	w.log.entries = w.log.entries[:0]
	w.log.firstSlotID = index
	for _, reader := range w.log.readers {
		reader.maybeReset()
	}
}

func (w *memoryWriter) Truncate(index nopaxos.LogSlotID) {
	for i := 0; i < len(w.log.entries); i++ {
		if w.log.entries[i].SlotID > index {
			w.log.entries = w.log.entries[:i]
			break
		}
	}
	for _, reader := range w.log.readers {
		reader.maybeReset()
	}
}

func (w *memoryWriter) Close() error {
	panic("implement me")
}

type memoryReader struct {
	log   *memoryLog
	index int
}

func (r *memoryReader) FirstSlotID() nopaxos.LogSlotID {
	return r.log.firstSlotID
}

func (r *memoryReader) LastSlotID() nopaxos.LogSlotID {
	if len(r.log.entries) == 0 {
		return r.log.firstSlotID - 1
	}
	return r.log.entries[len(r.log.entries)-1].SlotID
}

func (r *memoryReader) CurrentSlotID() nopaxos.LogSlotID {
	if r.index == -1 || len(r.log.entries) == 0 {
		return r.log.firstSlotID - 1
	}
	return r.log.entries[r.index].SlotID
}

func (r *memoryReader) CurrentEntry() *Entry {
	if r.index == -1 || len(r.log.entries) == 0 {
		return nil
	}
	return r.log.entries[r.index]
}

func (r *memoryReader) NextSlotID() nopaxos.LogSlotID {
	if r.index == -1 || len(r.log.entries) == 0 {
		return r.log.firstSlotID
	}
	return r.log.entries[r.index].SlotID + 1
}

func (r *memoryReader) NextEntry() *Entry {
	if len(r.log.entries) > r.index+1 {
		r.index++
		return r.log.entries[r.index]
	}
	return nil
}

func (r *memoryReader) Reset(index nopaxos.LogSlotID) {
	for i := 0; i < len(r.log.entries); i++ {
		if r.log.entries[i].SlotID >= index {
			r.index = i - 1
			break
		}
	}
}

func (r *memoryReader) maybeReset() {
	if r.index >= 0 && len(r.log.entries) <= r.index {
		r.index = len(r.log.entries) - 1
	}
}

func (r *memoryReader) Close() error {
	return nil
}
