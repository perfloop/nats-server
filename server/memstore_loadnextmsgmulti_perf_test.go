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
	"fmt"
	"testing"

	"github.com/nats-io/nats-server/v2/server/gsl"
)

func TestMemStoreLoadNextMsgMultiDeletedBounds(t *testing.T) {
	newStore := func(t *testing.T) *memStore {
		t.Helper()

		ms, err := newMemStore(&StreamConfig{Name: "MULTI", Subjects: []string{">"}, Storage: MemoryStorage})
		require_NoError(t, err)

		for _, subj := range []string{
			"noise.one",
			"orders.eu.a",
			"noise.two",
			"orders.us.a",
			"orders.eu.b",
			"orders.us.b",
			"noise.three",
			"orders.eu.c",
			"orders.us.c",
			"noise.four",
		} {
			_, _, err := ms.StoreMsg(subj, nil, nil, 0)
			require_NoError(t, err)
		}

		for _, seq := range []uint64{2, 3, 8} {
			removed, err := ms.RemoveMsg(seq)
			require_NoError(t, err)
			require_True(t, removed)
		}
		return ms
	}

	next := func(ms *memStore, sl *gsl.SimpleSublist, start uint64) (uint64, string, error) {
		ms.mu.RLock()
		defer ms.mu.RUnlock()

		if start < ms.state.FirstSeq {
			start = ms.state.FirstSeq
		}
		if start > ms.state.LastSeq || ms.state.Msgs == 0 {
			return ms.state.LastSeq, _EMPTY_, ErrStoreEOF
		}
		for seq := start; seq <= ms.state.LastSeq; seq++ {
			if sm := ms.msgs[seq]; sm != nil && sl.HasInterest(sm.subj) {
				return seq, sm.subj, nil
			}
		}
		return ms.state.LastSeq, _EMPTY_, ErrStoreEOF
	}

	for _, tc := range []struct {
		name    string
		filters []string
	}{
		{
			name:    "multi_literal_and_wildcard",
			filters: []string{"orders.eu.*", "orders.us.b", "payments.>"},
		},
		{
			name:    "single_filter",
			filters: []string{"orders.eu.*"},
		},
		{
			name:    "full_wildcard",
			filters: []string{">"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ms := newStore(t)
			defer ms.Stop()

			sl := gsl.NewSimpleSublist()
			for _, filter := range tc.filters {
				require_NoError(t, sl.Insert(filter, struct{}{}))
			}

			for start := uint64(0); start <= 12; start++ {
				wantSeq, wantSubj, wantErr := next(ms, sl, start)
				var smv StoreMsg
				got, gotSeq, gotErr := ms.LoadNextMsgMulti(sl, start, &smv)

				require_Equal(t, gotErr, wantErr)
				require_Equal(t, gotSeq, wantSeq)
				if wantSubj == _EMPTY_ {
					require_True(t, got == nil)
				} else {
					require_NotNil(t, got)
					require_Equal(t, got.subj, wantSubj)
				}
			}
		})
	}
}

func Benchmark_MemStoreLoadNextMsgMultiSparse(b *testing.B) {
	const (
		totalMessages = 100_000
		firstMatch    = 90_001
	)

	ms, err := newMemStore(&StreamConfig{Name: "MULTI", Subjects: []string{">"}, Storage: MemoryStorage})
	require_NoError(b, err)
	defer ms.Stop()

	for seq := 1; seq <= totalMessages; seq++ {
		subj := fmt.Sprintf("noise.%06d", seq)
		switch seq {
		case firstMatch:
			subj = "orders.eu"
		case 95_001:
			subj = "orders.us"
		}
		_, _, err = ms.StoreMsg(subj, nil, nil, 0)
		require_NoError(b, err)
	}

	sl := gsl.NewSimpleSublist()
	require_NoError(b, sl.Insert("orders.eu", struct{}{}))
	require_NoError(b, sl.Insert("orders.us", struct{}{}))

	var smv StoreMsg
	for _, start := range []uint64{1, 2} {
		sm, seq, err := ms.LoadNextMsgMulti(sl, start, &smv)
		if err != nil || seq != firstMatch || sm == nil || sm.subj != "orders.eu" {
			b.Fatalf("start %d: got message=%v sequence=%d error=%v", start, sm, seq, err)
		}
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; b.Loop(); i++ {
		start := uint64(i&1) + 1
		sm, seq, err := ms.LoadNextMsgMulti(sl, start, &smv)
		if err != nil || seq != firstMatch || sm == nil || sm.subj != "orders.eu" {
			b.Fatalf("start %d: got message=%v sequence=%d error=%v", start, sm, seq, err)
		}
	}
}

func Benchmark_MemStoreLoadNextMsgMultiBroad(b *testing.B) {
	const (
		totalMessages = 20_000
		matchEvery    = 32
	)

	ms, err := newMemStore(&StreamConfig{Name: "MULTI", Subjects: []string{">"}, Storage: MemoryStorage})
	require_NoError(b, err)
	defer ms.Stop()

	for seq := 1; seq <= totalMessages; seq++ {
		subj := fmt.Sprintf("noise.%05d", seq)
		if seq%matchEvery == 0 {
			if seq%(2*matchEvery) == 0 {
				subj = fmt.Sprintf("orders.%05d", seq)
			} else {
				subj = fmt.Sprintf("events.%05d", seq)
			}
		}
		_, _, err = ms.StoreMsg(subj, nil, nil, 0)
		require_NoError(b, err)
	}

	sl := gsl.NewSimpleSublist()
	require_NoError(b, sl.Insert("orders.>", struct{}{}))
	require_NoError(b, sl.Insert("events.>", struct{}{}))

	var smv StoreMsg
	for _, start := range []uint64{1, 2} {
		sm, seq, err := ms.LoadNextMsgMulti(sl, start, &smv)
		if err != nil || seq != matchEvery || sm == nil || sm.subj != "events.00032" {
			b.Fatalf("start %d: got message=%v sequence=%d error=%v", start, sm, seq, err)
		}
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; b.Loop(); i++ {
		start := uint64(i&1) + 1
		sm, seq, err := ms.LoadNextMsgMulti(sl, start, &smv)
		if err != nil || seq != matchEvery || sm == nil || sm.subj != "events.00032" {
			b.Fatalf("start %d: got message=%v sequence=%d error=%v", start, sm, seq, err)
		}
	}
}
