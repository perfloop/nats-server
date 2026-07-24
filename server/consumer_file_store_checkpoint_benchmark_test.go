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

func armConsumerCheckpoint(o *consumerFileStore) (<-chan error, func()) {
	done := make(chan error, 1)
	o.mu.Lock()
	o.testFlushDone = func(err error) {
		select {
		case done <- err:
		default:
		}
	}
	o.mu.Unlock()
	return done, func() {
		o.mu.Lock()
		o.testFlushDone = nil
		o.mu.Unlock()
	}
}

func checkpointConsumerState(tb testing.TB, o *consumerFileStore, pending int) *ConsumerState {
	tb.Helper()
	if err := o.ForceUpdate(consumerCheckpointState(pending)); err != nil {
		tb.Fatalf("seeding consumer state: %v", err)
	}
	done, disarm := armConsumerCheckpoint(o)
	defer disarm()
	next := uint64(pending + 2)
	if err := o.UpdateDelivered(next, next, 1, time.Now().Round(time.Second).UnixNano()); err != nil {
		tb.Fatalf("updating consumer state: %v", err)
	}
	if err := <-done; err != nil {
		tb.Fatalf("flushing consumer state: %v", err)
	}
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

const consumerCheckpointSamples = 128

func benchmarkConsumerFileStoreCheckpoint(b *testing.B, pending int) {
	type benchmarkStore struct {
		fs     *fileStore
		o      *consumerFileStore
		done   <-chan error
		disarm func()
	}

	// Each store has a fresh flush loop, so its first checkpoint takes the same
	// immediate-flush branch without including the production coalescing delay.
	// Creating and seeding them is setup; the sealed -benchtime=128x command then
	// averages independent high-cardinality checkpoints instead of one
	// scheduler-sensitive wakeup.
	stores := make([]benchmarkStore, consumerCheckpointSamples)
	for i := range stores {
		fcfg := FileStoreConfig{StoreDir: b.TempDir()}
		fs, o := newConsumerCheckpointStore(b, fcfg)
		if err := o.ForceUpdate(consumerCheckpointState(pending)); err != nil {
			b.Fatalf("seeding consumer state: %v", err)
		}
		done, disarm := armConsumerCheckpoint(o)
		stores[i] = benchmarkStore{fs: fs, o: o, done: done, disarm: disarm}
	}
	defer func() {
		for _, store := range stores {
			_ = store.o.Stop()
			_ = store.fs.Stop()
		}
	}()

	for i := 0; b.Loop(); i++ {
		if i == len(stores) {
			b.Fatal("consumer checkpoint benchmark requires -benchtime=128x")
		}
		store := stores[i]
		next := uint64(pending + 2)
		if err := store.o.UpdateDelivered(next, next, 1, time.Now().Round(time.Second).UnixNano()); err != nil {
			b.Fatalf("updating consumer state: %v", err)
		}
		if err := <-store.done; err != nil {
			b.Fatalf("flushing consumer state: %v", err)
		}
	}

	for _, store := range stores {
		store.disarm()
		state, err := store.o.State()
		if err != nil {
			b.Fatalf("reading consumer state: %v", err)
		}
		if state.Delivered.Stream != uint64(pending+2) || len(state.Pending) != pending+1 {
			b.Fatalf("unexpected final state: %+v", state)
		}
	}
}

func BenchmarkConsumerFileStoreCheckpointPending16(b *testing.B) {
	benchmarkConsumerFileStoreCheckpoint(b, 16)
}

func BenchmarkConsumerFileStoreCheckpointPending32768(b *testing.B) {
	benchmarkConsumerFileStoreCheckpoint(b, 32768)
}
