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
	"reflect"
	"testing"
	"time"
)

func consumerCheckpointState(pending int) *ConsumerState {
	state := &ConsumerState{
		AckFloor:  SequencePair{Consumer: 1, Stream: 1},
		Delivered: SequencePair{Consumer: uint64(pending + 1), Stream: uint64(pending + 1)},
		Pending:   make(map[uint64]*Pending, pending),
	}
	started := time.Now().Add(-time.Duration(pending) * time.Second).Round(time.Second).UnixNano()
	for i := 0; i < pending; i++ {
		seq := uint64(i + 2)
		state.Pending[seq] = &Pending{Sequence: seq, Timestamp: started + int64(i)*int64(time.Second)}
	}
	return state
}

func newConsumerCheckpointStore(tb testing.TB, fcfg FileStoreConfig) (*fileStore, *consumerFileStore) {
	tb.Helper()
	fs, err := newFileStoreWithCreated(fcfg, StreamConfig{Name: "CHECKPOINT", Storage: FileStorage}, time.Now(), prf(&fcfg), nil)
	if err != nil {
		tb.Fatalf("creating file store: %v", err)
	}
	store, err := fs.ConsumerStore("CHECKPOINT", time.Time{}, &ConsumerConfig{AckPolicy: AckExplicit})
	if err != nil {
		_ = fs.Stop()
		tb.Fatalf("creating consumer store: %v", err)
	}
	return fs, store.(*consumerFileStore)
}

func waitForConsumerCheckpoint(tb testing.TB, o *consumerFileStore) {
	tb.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		o.mu.Lock()
		complete := !o.dirty && !o.writing
		o.mu.Unlock()
		if complete {
			return
		}
		time.Sleep(time.Millisecond)
	}
	tb.Fatal("consumer checkpoint did not complete")
}

func checkpointConsumerState(tb testing.TB, o *consumerFileStore, pending int) *ConsumerState {
	tb.Helper()
	if err := o.ForceUpdate(consumerCheckpointState(pending)); err != nil {
		tb.Fatalf("seeding consumer state: %v", err)
	}
	next := uint64(pending + 2)
	if err := o.UpdateDelivered(next, next, 1, time.Now().Round(time.Second).UnixNano()); err != nil {
		tb.Fatalf("updating consumer state: %v", err)
	}
	waitForConsumerCheckpoint(tb, o)
	state, err := o.State()
	if err != nil {
		tb.Fatalf("reading consumer state: %v", err)
	}
	return state
}

func TestConsumerFileStoreCheckpointPersistsHighCardinalityState(t *testing.T) {
	fcfg := FileStoreConfig{StoreDir: t.TempDir(), Cipher: AES, SyncAlways: true}
	fs, o := newConsumerCheckpointStore(t, fcfg)
	expected := checkpointConsumerState(t, o, 8192)

	encoded, err := o.EncodedState()
	if err != nil {
		t.Fatalf("encoding state for export: %v", err)
	}
	fromExport, err := decodeConsumerState(encoded)
	if err != nil {
		t.Fatalf("decoding exported state: %v", err)
	}
	if !reflect.DeepEqual(expected, fromExport) {
		t.Fatalf("exported state differs from memory: got %+v, want %+v", fromExport, expected)
	}

	if err := o.Stop(); err != nil {
		t.Fatalf("stopping consumer store: %v", err)
	}
	if err := fs.Stop(); err != nil {
		t.Fatalf("stopping file store: %v", err)
	}

	fs, o = newConsumerCheckpointStore(t, fcfg)
	defer fs.Stop()
	defer o.Stop()
	recovered, err := o.State()
	if err != nil {
		t.Fatalf("reading recovered state: %v", err)
	}
	if !reflect.DeepEqual(expected, recovered) {
		t.Fatalf("recovered state differs from checkpoint: got %+v, want %+v", recovered, expected)
	}
}

func benchmarkConsumerFileStoreCheckpoint(b *testing.B, pending int) {
	fcfg := FileStoreConfig{StoreDir: b.TempDir()}
	fs, o := newConsumerCheckpointStore(b, fcfg)
	defer fs.Stop()
	defer o.Stop()

	if err := o.ForceUpdate(consumerCheckpointState(pending)); err != nil {
		b.Fatalf("seeding consumer state: %v", err)
	}

	next := uint64(pending + 2)
	b.ResetTimer()
	for b.Loop() {
		if err := o.UpdateDelivered(next, next, 1, time.Now().Round(time.Second).UnixNano()); err != nil {
			b.Fatalf("updating consumer state: %v", err)
		}
		waitForConsumerCheckpoint(b, o)
		next++
	}
	b.StopTimer()

	state, err := o.State()
	if err != nil {
		b.Fatalf("reading consumer state: %v", err)
	}
	if state.Delivered.Stream != next-1 || len(state.Pending) != pending+int(next-uint64(pending+2)) {
		b.Fatalf("unexpected final state: %+v", state)
	}
}

func BenchmarkConsumerFileStoreCheckpointPending16(b *testing.B) {
	benchmarkConsumerFileStoreCheckpoint(b, 16)
}

func BenchmarkConsumerFileStoreCheckpointPending32768(b *testing.B) {
	benchmarkConsumerFileStoreCheckpoint(b, 32768)
}

func BenchmarkConsumerFileStoreCheckpointEncodeProfile(b *testing.B) {
	state := consumerCheckpointState(32768)
	var total int
	for b.Loop() {
		total += len(encodeConsumerState(state))
	}
	if total == 0 {
		b.Fatal("consumer state was not encoded")
	}
}
