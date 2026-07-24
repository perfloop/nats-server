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
	consumerJournalVersion        = 1
	consumerJournalHeaderSize     = 4 + 1 + sha256.Size
	consumerJournalFrameHdrSize   = 8
	consumerJournalMaxBytes       = 1 << 20
	consumerJournalSmallPayload   = binary.MaxVarintLen64 + 4*binary.MaxVarintLen64
	consumerJournalSmallCryptoPad = 64
	consumerJournalSmallFrame     = consumerJournalHeaderSize + consumerJournalFrameHdrSize + consumerJournalSmallPayload + consumerJournalSmallCryptoPad
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
	file            string
	// flusherDone lets shutdown wait for an in-progress checkpoint without polling.
	flusherDone chan struct{}

	// Most flushes contain one delivery. Keep that delta and its plaintext
	// frame inline so the ordinary checkpoint path does not allocate.
	delta    consumerDeliveryDelta
	hasDelta bool
	deltas   []consumerDeliveryDelta // Additional deltas in a coalesced flush.
	payload  [consumerJournalSmallPayload]byte
	frame    [consumerJournalSmallFrame]byte
}

func (j *consumerJournalState) clearDeltas() {
	j.hasDelta = false
	j.deltas = nil
}

func (j *consumerJournalState) pendingDeltas() int {
	if !j.hasDelta {
		return 0
	}
	return 1 + len(j.deltas)
}

// journalState is protected by o.mu.
func (o *consumerFileStore) journalState() *consumerJournalState {
	return &o.cfg.journal
}

func consumerJournalFile(o *consumerFileStore) string {
	return o.ifn + ".journal"
}

func (o *consumerFileStore) markSnapshotLocked() {
	journal := o.journalState()
	journal.version++
	journal.snapshot = true
	journal.clearDeltas()
}

func (o *consumerFileStore) recordDeliveredLocked(dseq, sseq, dc uint64, ts int64) {
	journal := o.journalState()
	journal.version++
	if journal.ready && !journal.snapshot {
		delta := consumerDeliveryDelta{dseq, sseq, dc, ts}
		if !journal.hasDelta {
			journal.delta = delta
			journal.hasDelta = true
		} else {
			journal.deltas = append(journal.deltas, delta)
		}
		return
	}
	journal.snapshot = true
	journal.clearDeltas()
}

func (o *consumerFileStore) journalFlushPendingLocked() bool {
	journal := o.journalState()
	return journal.ready && !journal.snapshot && journal.hasDelta
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
	if !writeSnapshot && !journal.hasDelta {
		o.dirty = false
		o.mu.Unlock()
		return nil
	}
	if !writeSnapshot && journal.bytes+int64(consumerJournalFrameMaxSize(journal.pendingDeltas(), aek)) > consumerJournalMaxBytes {
		// Bound restart replay and journal growth with a deliberate full-state
		// compaction. Ordinary frame appends below this threshold stay O(delta).
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

	first := journal.delta
	deltas := journal.deltas
	journal.clearDeltas()
	o.writing = true
	journalBytes := journal.bytes
	generation := journal.generation
	jfn := journal.file
	if jfn == _EMPTY_ {
		jfn = consumerJournalFile(o)
		journal.file = jfn
	}
	o.mu.Unlock()

	// Reuse the inline frame prefix as associated data. This keeps the ordinary
	// encrypted one-delta checkpoint allocation-free while authenticating the
	// journal header with every frame.
	header := journal.frame[:consumerJournalHeaderSize]
	writeConsumerJournalHeader(header, generation)
	var frame []byte
	var err error
	if len(deltas) == 0 && (aek == nil || aek.NonceSize()+aek.Overhead() <= consumerJournalSmallCryptoPad) {
		base := journal.frame[:consumerJournalHeaderSize]
		if aek == nil {
			data := appendSingleConsumerJournalFrame(base, first)
			frame = data[consumerJournalHeaderSize:]
			if journalBytes == 0 {
				err = o.fs.writeFileWithOptionalSync(jfn, data, defaultFilePerms)
			} else {
				err = o.fs.appendFileWithOptionalSync(jfn, frame, defaultFilePerms)
			}
		} else {
			data, e := appendSingleEncryptedConsumerJournalFrame(base, journal.payload[:0], first, aek, header)
			if e != nil {
				err = e
			} else {
				frame = data[consumerJournalHeaderSize:]
				if journalBytes == 0 {
					err = o.fs.writeFileWithOptionalSync(jfn, data, defaultFilePerms)
				} else {
					err = o.fs.appendFileWithOptionalSync(jfn, frame, defaultFilePerms)
				}
			}
		}
	} else {
		frame, err = encodeConsumerJournalFrame(first, deltas, aek, header)
		if err == nil {
			if journalBytes == 0 {
				data := append(header, frame...)
				err = o.fs.writeFileWithOptionalSync(jfn, data, defaultFilePerms)
			} else {
				err = o.fs.appendFileWithOptionalSync(jfn, frame, defaultFilePerms)
			}
		}
	}

	o.mu.Lock()
	defer o.mu.Unlock()
	o.writing = false
	journal = o.journalState()
	if err != nil {
		// A failed append may have left a partial frame. The next flush writes a
		// complete snapshot instead of extending a journal whose tail is unknown.
		journal.snapshot = true
		journal.clearDeltas()
		o.dirty = true
		return err
	}
	if journalBytes == 0 {
		journal.bytes = int64(consumerJournalHeaderSize + len(frame))
	} else {
		journal.bytes += int64(len(frame))
	}
	if journal.version == version && !journal.hasDelta && !journal.snapshot {
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
	journal.file = consumerJournalFile(o)
	if !journal.snapshotPending || journal.snapshotVersion == version {
		journal.snapshotPending = false
	}
	if journal.version == version && !journal.configBarrier {
		journal.snapshot = false
		journal.clearDeltas()
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
	journal.file = consumerJournalFile(o)
	jfn := journal.file
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
			payload, err = o.aek.Open(nil, payload[:ns], payload[ns:], data[:consumerJournalHeaderSize])
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
	writeConsumerJournalHeader(header, generation)
	return header
}

func writeConsumerJournalHeader(header []byte, generation [sha256.Size]byte) {
	copy(header, consumerJournalMagic[:])
	header[4] = consumerJournalVersion
	copy(header[5:], generation[:])
}

func appendSingleConsumerJournalFrame(dst []byte, delta consumerDeliveryDelta) []byte {
	start := len(dst)
	dst = append(dst, 0, 0, 0, 0, 0, 0, 0, 0)
	dst = binary.AppendUvarint(dst, 1)
	dst = binary.AppendUvarint(dst, delta.dseq)
	dst = binary.AppendUvarint(dst, delta.sseq)
	dst = binary.AppendUvarint(dst, delta.dc)
	dst = binary.AppendVarint(dst, delta.ts)
	payload := dst[start+consumerJournalFrameHdrSize:]
	binary.BigEndian.PutUint32(dst[start:], uint32(len(payload)))
	binary.BigEndian.PutUint32(dst[start+4:], crc32.ChecksumIEEE(payload))
	return dst
}

func appendSingleEncryptedConsumerJournalFrame(dst, plaintext []byte, delta consumerDeliveryDelta, aek cipher.AEAD, aad []byte) ([]byte, error) {
	start := len(dst)
	dst = append(dst, 0, 0, 0, 0, 0, 0, 0, 0)
	plaintext = binary.AppendUvarint(plaintext, 1)
	plaintext = appendConsumerJournalDelta(plaintext, delta)

	nonceStart := len(dst)
	dst = dst[:nonceStart+aek.NonceSize()]
	nonce := dst[nonceStart:]
	if n, err := rand.Read(nonce); err != nil {
		return nil, err
	} else if n != len(nonce) {
		return nil, fmt.Errorf("not enough nonce bytes read (%d != %d)", n, len(nonce))
	}
	payloadStart := start + consumerJournalFrameHdrSize
	payload := aek.Seal(dst[payloadStart:payloadStart+aek.NonceSize()], nonce, plaintext, aad)
	dst = dst[:payloadStart+len(payload)]
	binary.BigEndian.PutUint32(dst[start:], uint32(len(payload)))
	binary.BigEndian.PutUint32(dst[start+4:], crc32.ChecksumIEEE(payload))
	return dst, nil
}

func encodeConsumerJournalFrame(first consumerDeliveryDelta, deltas []consumerDeliveryDelta, aek cipher.AEAD, aad []byte) ([]byte, error) {
	payload := make([]byte, 0, consumerJournalFrameMaxSize(1+len(deltas), nil)-consumerJournalFrameHdrSize)
	payload = binary.AppendUvarint(payload, uint64(1+len(deltas)))
	payload = appendConsumerJournalDelta(payload, first)
	for _, delta := range deltas {
		payload = appendConsumerJournalDelta(payload, delta)
	}
	if aek != nil {
		nonce := make([]byte, aek.NonceSize(), aek.NonceSize()+len(payload)+aek.Overhead())
		if n, err := rand.Read(nonce); err != nil {
			return nil, err
		} else if n != len(nonce) {
			return nil, fmt.Errorf("not enough nonce bytes read (%d != %d)", n, len(nonce))
		}
		payload = aek.Seal(nonce, nonce, payload, aad)
	}
	frame := make([]byte, consumerJournalFrameHdrSize+len(payload))
	binary.BigEndian.PutUint32(frame[:4], uint32(len(payload)))
	binary.BigEndian.PutUint32(frame[4:8], crc32.ChecksumIEEE(payload))
	copy(frame[consumerJournalFrameHdrSize:], payload)
	return frame, nil
}

func appendConsumerJournalDelta(dst []byte, delta consumerDeliveryDelta) []byte {
	dst = binary.AppendUvarint(dst, delta.dseq)
	dst = binary.AppendUvarint(dst, delta.sseq)
	dst = binary.AppendUvarint(dst, delta.dc)
	return binary.AppendVarint(dst, delta.ts)
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
