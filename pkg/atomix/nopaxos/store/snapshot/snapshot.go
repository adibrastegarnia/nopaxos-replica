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

package snapshot

import (
	"bytes"
	nopaxos "github.com/atomix/atomix-nopaxos-node/pkg/atomix/nopaxos/protocol"
	"io"
	"time"
)

// NewMemoryStore creates a new in-memory snapshot store
func NewMemoryStore() Store {
	return &memorySnapshotStore{
		snapshots: make(map[nopaxos.LogSlotID]Snapshot),
	}
}

// Store is an interface for managing snapshots
type Store interface {
	// NewSnapshot creates a new snapshot
	NewSnapshot(index nopaxos.LogSlotID, timestamp time.Time) Snapshot

	// CurrentSnapshot returns the current snapshot
	CurrentSnapshot() Snapshot

	// Close closes the store
	Close() error
}

// Snapshot is a single state machine snapshot
type Snapshot interface {
	// LogSlotID is the log slot at which the snapshot was taken
	LogSlotID() nopaxos.LogSlotID

	// Timestamp is the time at which the snapshot was taken
	Timestamp() time.Time

	// Reader returns a new snapshot reader
	Reader() io.ReadCloser

	// Writer returns a new snapshot writer
	Writer() io.WriteCloser
}

// memorySnapshotStore is an in-memory Store
type memorySnapshotStore struct {
	snapshots       map[nopaxos.LogSlotID]Snapshot
	currentSnapshot Snapshot
}

func (s *memorySnapshotStore) NewSnapshot(slotID nopaxos.LogSlotID, timestamp time.Time) Snapshot {
	snapshot := &memorySnapshot{
		slotID:    slotID,
		timestamp: timestamp,
		bytes:     make([]byte, 0, 1024*1024),
	}
	s.snapshots[slotID] = snapshot
	s.currentSnapshot = snapshot
	return snapshot
}

func (s *memorySnapshotStore) CurrentSnapshot() Snapshot {
	return s.currentSnapshot
}

func (s *memorySnapshotStore) Close() error {
	return nil
}

type memorySnapshot struct {
	slotID    nopaxos.LogSlotID
	timestamp time.Time
	bytes     []byte
}

func (s *memorySnapshot) LogSlotID() nopaxos.LogSlotID {
	return s.slotID
}

func (s *memorySnapshot) Timestamp() time.Time {
	return s.timestamp
}

func (s *memorySnapshot) Reader() io.ReadCloser {
	return &memoryReader{
		reader: bytes.NewReader(s.bytes),
	}
}

func (s *memorySnapshot) Writer() io.WriteCloser {
	return &memoryWriter{
		snapshot: s,
		buf:      bytes.NewBuffer(s.bytes),
	}
}

type memoryReader struct {
	reader io.Reader
}

func (r *memoryReader) Read(p []byte) (n int, err error) {
	return r.reader.Read(p)
}

func (r *memoryReader) Close() error {
	return nil
}

type memoryWriter struct {
	snapshot *memorySnapshot
	buf      *bytes.Buffer
}

func (w *memoryWriter) Write(p []byte) (n int, err error) {
	return w.buf.Write(p)
}

func (w *memoryWriter) Close() error {
	w.snapshot.bytes = w.buf.Bytes()
	return nil
}
