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
	"bytes"
	"crypto/sha256"
	"io"
)

// newSnapshot creates a new snapshot
func newSnapshot(slotNum LogSlotID) *Snapshot {
	return &Snapshot{
		SlotNum: slotNum,
		Data:    make([]byte, 0),
	}
}

// Snapshot is a snapshot of the state machine
type Snapshot struct {
	SlotNum  LogSlotID
	Data     []byte
	Checksum []byte
}

func (s *Snapshot) Writer() io.WriteCloser {
	return &snapshotWriteCloser{snapshot: s}
}

func (s *Snapshot) Reader() io.ReadCloser {
	return &snapshotReadCloser{reader: bytes.NewBuffer(s.Data)}
}

type snapshotWriteCloser struct {
	snapshot *Snapshot
}

func (w *snapshotWriteCloser) Write(p []byte) (n int, err error) {
	w.snapshot.Data = append(w.snapshot.Data, p...)
	return len(p), nil
}

func (w *snapshotWriteCloser) Close() error {
	checksum := sha256.Sum256([]byte(w.snapshot.Data))
	w.snapshot.Checksum = checksum[:]
	return nil
}

type snapshotReadCloser struct {
	reader io.Reader
}

func (r *snapshotReadCloser) Read(p []byte) (n int, err error) {
	return r.reader.Read(p)
}

func (r *snapshotReadCloser) Close() error {
	return nil
}
