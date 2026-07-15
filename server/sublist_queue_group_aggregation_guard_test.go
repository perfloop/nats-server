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
	"sync/atomic"
	"testing"
)

func checkQueueGroupAggregationBenchmarkResult(b *testing.B, r *SublistResult, queueGroups, matchingNodes int) {
	b.Helper()
	if len(r.qsubs) != queueGroups || len(r.qsubs[0]) != matchingNodes {
		b.Fatalf("unexpected result shape: %d queue groups, %d subscriptions in first group", len(r.qsubs), len(r.qsubs[0]))
	}
}

func BenchmarkSublistQueueGroupAggregationBoundary(b *testing.B) {
	cases := []struct {
		queueGroups   int
		matchingNodes int
	}{
		{16, 1},
		{16, 2},
		{16, 3},
		{63, 1},
		{63, 2},
		{63, 3},
		{64, 1},
		{64, 2},
	}

	for _, tc := range cases {
		b.Run(fmt.Sprintf("groups=%d/nodes=%d", tc.queueGroups, tc.matchingNodes), func(b *testing.B) {
			f := newQueueGroupAggregationFixture(false, tc.queueGroups, tc.matchingNodes, false)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				checkQueueGroupAggregationBenchmarkResult(b, f.sl.Match(f.subjects[i%len(f.subjects)]), tc.queueGroups, tc.matchingNodes)
			}
		})
	}
}

func BenchmarkSublistQueueGroupAggregationCacheHit(b *testing.B) {
	const (
		queueGroups   = 64
		matchingNodes = 3
	)

	f := newQueueGroupAggregationFixture(true, queueGroups, matchingNodes, false)
	subject := f.subjects[0]
	checkQueueGroupAggregationBenchmarkResult(b, f.sl.Match(subject), queueGroups, matchingNodes)
	hits := atomic.LoadUint64(&f.sl.cacheHits)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		checkQueueGroupAggregationBenchmarkResult(b, f.sl.Match(subject), queueGroups, matchingNodes)
	}
	b.StopTimer()
	if got := atomic.LoadUint64(&f.sl.cacheHits) - hits; got != uint64(b.N) {
		b.Fatalf("cache hits = %d, want %d", got, b.N)
	}
}

func BenchmarkSublistQueueGroupAggregationCacheChurn(b *testing.B) {
	const (
		queueGroups   = 64
		matchingNodes = 3
	)

	s := NewSublistWithCache()
	for group := 0; group < queueGroups; group++ {
		queue := fmt.Sprintf("perfqga-q-%03d", group)
		for _, filter := range []string{">", "*.*", "*.tail"} {
			if err := s.Insert(newQSub(filter, queue)); err != nil {
				b.Fatal(err)
			}
		}
	}

	subjects := make([]string, b.N)
	for i := range subjects {
		subjects[i] = fmt.Sprintf("perfqga%d.tail", i)
	}
	hits := atomic.LoadUint64(&s.cacheHits)
	b.ReportAllocs()
	b.ResetTimer()
	for _, subject := range subjects {
		checkQueueGroupAggregationBenchmarkResult(b, s.Match(subject), queueGroups, matchingNodes)
	}
	b.StopTimer()
	if got := atomic.LoadUint64(&s.cacheHits) - hits; got != 0 {
		b.Fatalf("cache hits = %d, want 0", got)
	}
}
