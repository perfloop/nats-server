// Copyright 2026 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package server

import (
	"bytes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	iofs "io/fs"
	"os"
)

const (
	consumerJournalVersion      = 1
	consumerJournalHeaderSize   = 4 + 1 + sha256.Size
	consumerJournalFrameHdrSize = 8
	consumerJournalMaxBytes     = 1 << 20
)

var (
	consumerJournalMagic       = [4]byte{'C', 'J', 'R', '1'}
	consumerJournalFlushMarker = [4]byte{'C', 'J', 'F', '1'}
)

type consumerDeliveryDelta struct {
	dseq uint64
	sseq uint64
	dc   uint64
	ts   int64
}

type consumerJournalState struct {
	generation      [sha256.Size]byte
	bytes           int64
	ready           bool
	snapshot        bool
	configBarrier   bool
	snapshotPending bool
	snapshotVersion uint64
	version         uint64
	deltas          []consumerDeliveryDelta
}

// journalState is protected by o.mu after it has been looked up.
func (o *consumerFileStore) journalState() *consumerJournalState {
	if journal, ok := o.fs.cj.Load(o); ok {
		return journal.(*consumerJournalState)
	}
	journal := &consumerJournalState{}
	actual, _ := o.fs.cj.LoadOrStore(o, journal)
	return actual.(*consumerJournalState)
}

func consumerJournalFile(o *consumerFileStore) string {
	return o.ifn + ".journal"
}

func (o *consumerFileStore) markSnapshotLocked() {
	journal := o.journalState()
	journal.version++
	journal.snapshot = true
	journal.deltas = nil
}

func (o *consumerFileStore) recordDeliveredLocked(dseq, sseq, dc uint64, ts int64) {
	journal := o.journalState()
	journal.version++
	if journal.ready && !journal.snapshot {
		journal.deltas = append(journal.deltas, consumerDeliveryDelta{dseq, sseq, dc, ts})
		return
	}
	journal.snapshot = true
	journal.deltas = nil
}

func (o *consumerFileStore) journalFlushPendingLocked() bool {
	journal := o.journalState()
	return journal.ready && !journal.snapshot && len(journal.deltas) > 0
}

func isConsumerJournalFlushMarker(buf []byte) bool {
	return bytes.Equal(buf, consumerJournalFlushMarker[:])
}

func (o *consumerFileStore) writeJournalCheckpoint() error {
	o.mu.Lock()
	if o.closed {
		o.mu.Unlock()
		return ErrStoreClosed
	}
	if o.writing {
		// A synchronous snapshot is in progress. Keep the pending journal
		// state dirty and queue another pass once that write releases it.
		o.kickFlusher()
		o.mu.Unlock()
		return nil
	}

	journal := o.journalState()
	version := journal.version
	aek := o.aek
	writeSnapshot := journal.snapshot || !journal.ready
	if !writeSnapshot && len(journal.deltas) == 0 {
		o.dirty = false
		o.mu.Unlock()
		return nil
	}
	if !writeSnapshot && journal.bytes+int64(consumerJournalFrameMaxSize(len(journal.deltas), aek)) > consumerJournalMaxBytes {
		writeSnapshot = true
	}
	if writeSnapshot {
		buf, err := o.encodeFullStateLocked()
		o.mu.Unlock()
		if err != nil {
			return err
		}
		return o.writeSnapshot(buf, version)
	}

	deltas := journal.deltas
	journal.deltas = nil
	o.writing = true
	journalBytes := journal.bytes
	generation := journal.generation
	jfn := consumerJournalFile(o)
	o.mu.Unlock()

	frame, err := encodeConsumerJournalFrame(deltas, aek)
	if err != nil {
		o.mu.Lock()
		o.writing = false
		journal := o.journalState()
		journal.snapshot = true
		journal.deltas = nil
		o.dirty = true
		o.mu.Unlock()
		return err
	}
	if journalBytes == 0 {
		data := makeConsumerJournalHeader(generation)
		data = append(data, frame...)
		err = o.fs.writeFileWithOptionalSync(jfn, data, defaultFilePerms)
	} else {
		err = o.fs.appendFileWithOptionalSync(jfn, frame, defaultFilePerms)
	}

	o.mu.Lock()
	defer o.mu.Unlock()
	o.writing = false
	journal = o.journalState()
	if err != nil {
		// A failed append may have left a partial frame. The next flush writes a
		// complete snapshot instead of extending a journal whose tail is unknown.
		journal.snapshot = true
		journal.deltas = nil
		o.dirty = true
		return err
	}
	if journalBytes == 0 {
		journal.bytes = int64(consumerJournalHeaderSize + len(frame))
	} else {
		journal.bytes += int64(len(frame))
	}
	if journal.version == version && len(journal.deltas) == 0 && !journal.snapshot {
		o.dirty = false
	} else {
		o.dirty = true
	}
	return nil
}

func (o *consumerFileStore) writeSnapshot(buf []byte, version uint64) error {
	for {
		err := o.writeRawState(buf)
		if err == nil {
			break
		}
		if err != errConsumerStateWriteInProgress {
			return err
		}
		// A full snapshot is a barrier for journal interpretation, so it must
		// follow an in-flight state write rather than being silently deferred.
		o.waitForStateWrite()
	}

	o.mu.Lock()
	defer o.mu.Unlock()
	journal := o.journalState()
	journal.generation = consumerStateGeneration(buf, &o.cfg.ConsumerConfig)
	journal.ready = true
	journal.bytes = 0
	if !journal.snapshotPending || journal.snapshotVersion == version {
		journal.snapshotPending = false
	}
	if journal.version == version && !journal.configBarrier {
		journal.snapshot = false
		journal.deltas = nil
		o.dirty = false
	} else {
		journal.snapshot = true
		o.dirty = true
	}
	return nil
}

func (o *consumerFileStore) loadJournalLocked(state *ConsumerState, generation [sha256.Size]byte) error {
	journal := o.journalState()
	journal.generation = generation
	journal.ready = true
	journal.bytes = 0
	jfn := consumerJournalFile(o)
	o.fs.dios.acquire()
	data, err := os.ReadFile(jfn)
	o.fs.dios.release()
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	if len(data) < consumerJournalHeaderSize {
		return o.truncateJournalLocked(jfn, 0)
	}
	if len(data) > consumerJournalHeaderSize+consumerJournalMaxBytes {
		return fmt.Errorf("consumer state journal exceeds maximum size")
	}
	if string(data[:4]) != string(consumerJournalMagic[:]) || data[4] != consumerJournalVersion || !bytes.Equal(data[5:consumerJournalHeaderSize], generation[:]) {
		return nil
	}

	offset := consumerJournalHeaderSize
	for offset < len(data) {
		if len(data)-offset < consumerJournalFrameHdrSize {
			if err := o.truncateJournalLocked(jfn, int64(offset)); err != nil {
				return err
			}
			break
		}
		size := int(binary.BigEndian.Uint32(data[offset:]))
		checksum := binary.BigEndian.Uint32(data[offset+4:])
		offset += consumerJournalFrameHdrSize
		if size < 0 || size > len(data)-offset {
			offset -= consumerJournalFrameHdrSize
			if err := o.truncateJournalLocked(jfn, int64(offset)); err != nil {
				return err
			}
			break
		}
		payload := data[offset : offset+size]
		if crc32.ChecksumIEEE(payload) != checksum {
			// An incomplete append can leave a complete-looking final frame
			// whose payload has not reached disk. Keep all earlier validated
			// frames and discard only that torn tail.
			if offset+size == len(data) {
				offset -= consumerJournalFrameHdrSize
				if err := o.truncateJournalLocked(jfn, int64(offset)); err != nil {
					return err
				}
				break
			}
			return fmt.Errorf("consumer state journal checksum mismatch")
		}
		if o.aek != nil {
			ns := o.aek.NonceSize()
			if len(payload) < ns {
				return fmt.Errorf("invalid encrypted consumer state journal frame")
			}
			payload, err = o.aek.Open(nil, payload[:ns], payload[ns:], nil)
			if err != nil {
				return err
			}
		}
		deltas, err := decodeConsumerJournalFrame(payload)
		if err != nil {
			return err
		}
		for _, delta := range deltas {
			o.applyConsumerDeliveryDelta(state, delta)
		}
		offset += size
	}
	journal.bytes = int64(offset)
	return nil
}

func (o *consumerFileStore) truncateJournalLocked(jfn string, size int64) error {
	o.fs.dios.acquire()
	err := os.Truncate(jfn, size)
	o.fs.dios.release()
	return err
}

// consumerStateGeneration binds journal frames to both their snapshot and the
// consumer settings that control replay. Keep this in sync with the settings
// consulted by applyConsumerDeliveryDelta.
func consumerStateGeneration(buf []byte, cfg *ConsumerConfig) [sha256.Size]byte {
	h := sha256.New()
	_, _ = h.Write(buf)
	_, _ = h.Write([]byte{byte(cfg.AckPolicy)})
	var maxDeliver [8]byte
	binary.LittleEndian.PutUint64(maxDeliver[:], uint64(cfg.MaxDeliver))
	_, _ = h.Write(maxDeliver[:])
	var generation [sha256.Size]byte
	copy(generation[:], h.Sum(nil))
	return generation
}

func makeConsumerJournalHeader(generation [sha256.Size]byte) []byte {
	header := make([]byte, consumerJournalHeaderSize)
	copy(header, consumerJournalMagic[:])
	header[4] = consumerJournalVersion
	copy(header[5:], generation[:])
	return header
}

func encodeConsumerJournalFrame(deltas []consumerDeliveryDelta, aek cipher.AEAD) ([]byte, error) {
	payload := make([]byte, 0, consumerJournalFrameMaxSize(len(deltas), nil)-consumerJournalFrameHdrSize)
	payload = binary.AppendUvarint(payload, uint64(len(deltas)))
	for _, delta := range deltas {
		payload = binary.AppendUvarint(payload, delta.dseq)
		payload = binary.AppendUvarint(payload, delta.sseq)
		payload = binary.AppendUvarint(payload, delta.dc)
		payload = binary.AppendVarint(payload, delta.ts)
	}
	if aek != nil {
		nonce := make([]byte, aek.NonceSize(), aek.NonceSize()+len(payload)+aek.Overhead())
		if n, err := rand.Read(nonce); err != nil {
			return nil, err
		} else if n != len(nonce) {
			return nil, fmt.Errorf("not enough nonce bytes read (%d != %d)", n, len(nonce))
		}
		payload = aek.Seal(nonce, nonce, payload, nil)
	}
	frame := make([]byte, consumerJournalFrameHdrSize+len(payload))
	binary.BigEndian.PutUint32(frame[:4], uint32(len(payload)))
	binary.BigEndian.PutUint32(frame[4:8], crc32.ChecksumIEEE(payload))
	copy(frame[consumerJournalFrameHdrSize:], payload)
	return frame, nil
}

func consumerJournalFrameMaxSize(deltas int, aek cipher.AEAD) int {
	size := consumerJournalFrameHdrSize + binary.MaxVarintLen64 + deltas*(3*binary.MaxVarintLen64+binary.MaxVarintLen64)
	if aek != nil {
		size += aek.NonceSize() + aek.Overhead()
	}
	return size
}

func decodeConsumerJournalFrame(payload []byte) ([]consumerDeliveryDelta, error) {
	count, n := binary.Uvarint(payload)
	if n <= 0 || count > uint64(len(payload)) {
		return nil, fmt.Errorf("invalid consumer state journal frame")
	}
	offset := n
	deltas := make([]consumerDeliveryDelta, 0, count)
	for range count {
		dseq, n := binary.Uvarint(payload[offset:])
		if n <= 0 {
			return nil, fmt.Errorf("invalid consumer state journal delivery sequence")
		}
		offset += n
		sseq, n := binary.Uvarint(payload[offset:])
		if n <= 0 {
			return nil, fmt.Errorf("invalid consumer state journal stream sequence")
		}
		offset += n
		dc, n := binary.Uvarint(payload[offset:])
		if n <= 0 {
			return nil, fmt.Errorf("invalid consumer state journal delivery count")
		}
		offset += n
		ts, n := binary.Varint(payload[offset:])
		if n <= 0 {
			return nil, fmt.Errorf("invalid consumer state journal timestamp")
		}
		offset += n
		deltas = append(deltas, consumerDeliveryDelta{dseq, sseq, dc, ts})
	}
	if offset != len(payload) {
		return nil, fmt.Errorf("invalid consumer state journal frame length")
	}
	return deltas, nil
}

func (o *consumerFileStore) applyConsumerDeliveryDelta(state *ConsumerState, delta consumerDeliveryDelta) {
	if delta.dseq <= state.AckFloor.Consumer {
		return
	}
	if o.cfg.AckPolicy == AckNone {
		if delta.dseq > state.Delivered.Consumer {
			state.Delivered.Consumer = delta.dseq
			state.AckFloor.Consumer = delta.dseq
		}
		if delta.sseq > state.Delivered.Stream {
			state.Delivered.Stream = delta.sseq
			state.AckFloor.Stream = delta.sseq
		}
		return
	}

	if state.Pending == nil {
		state.Pending = make(map[uint64]*Pending)
	}
	if delta.sseq <= state.Delivered.Stream {
		if pending := state.Pending[delta.sseq]; pending != nil {
			pending.Timestamp = delta.ts
		}
	} else {
		state.Pending[delta.sseq] = &Pending{Sequence: delta.dseq, Timestamp: delta.ts}
	}
	if delta.dseq > state.Delivered.Consumer {
		state.Delivered.Consumer = delta.dseq
	}
	if delta.sseq > state.Delivered.Stream {
		state.Delivered.Stream = delta.sseq
	}
	if delta.dc > 1 {
		if maxdc := uint64(o.cfg.MaxDeliver); maxdc > 0 && delta.dc > maxdc {
			delete(state.Pending, delta.sseq)
		}
		if state.Redelivered == nil {
			state.Redelivered = make(map[uint64]uint64)
		}
		if state.Redelivered[delta.sseq] < delta.dc-1 {
			state.Redelivered[delta.sseq] = delta.dc - 1
		}
	}
}

func (fs *fileStore) appendFileWithOptionalSync(name string, data []byte, perm iofs.FileMode) error {
	flags := os.O_CREATE | os.O_WRONLY | os.O_APPEND
	if fs.syncAlways.Load() {
		flags |= os.O_SYNC
	}
	fs.dios.acquire()
	defer fs.dios.release()

	f, err := os.OpenFile(name, flags, perm)
	if err != nil {
		return err
	}
	for len(data) > 0 {
		n, err := f.Write(data)
		if err != nil {
			_ = f.Close()
			return err
		}
		if n == 0 {
			_ = f.Close()
			return io.ErrShortWrite
		}
		data = data[n:]
	}
	return f.Close()
}
