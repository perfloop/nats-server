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
	"strconv"
	"sync/atomic"
	"testing"
)

func checkQueueGroupAggregationBenchmarkResult(b *testing.B, r *SublistResult, queueGroups, matchingNodes int) {
	b.Helper()
	if len(r.qsubs) != queueGroups || len(r.qsubs[0]) != matchingNodes {
		b.Fatalf("unexpected result shape: %d queue groups, %d subscriptions in first group", len(r.qsubs), len(r.qsubs[0]))
	}
}

func BenchmarkSublistQueueGroupAggregation(b *testing.B) {
	cases := []struct {
		queueGroups   int
		matchingNodes int
	}{
		{1, 3},
		{4, 3},
		{8, 3},
		{64, 3},
		{256, 3},
	}

	for _, tc := range cases {
		b.Run(fmt.Sprintf("groups=%d/nodes=%d", tc.queueGroups, tc.matchingNodes), func(b *testing.B) {
			f := newQueueGroupAggregationFixture(false, tc.queueGroups, tc.matchingNodes, false)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				r := f.sl.Match(f.subjects[i%len(f.subjects)])
				if len(r.qsubs) != tc.queueGroups || len(r.qsubs[0]) != tc.matchingNodes {
					b.Fatalf("unexpected result shape: %d queue groups, %d subscriptions in first group", len(r.qsubs), len(r.qsubs[0]))
				}
			}
		})
	}
}

func BenchmarkSublistQueueGroupAggregation257(b *testing.B) {
	const (
		queueGroups   = 257
		matchingNodes = 3
	)

	f := newQueueGroupAggregationFixture(false, queueGroups, matchingNodes, false)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		checkQueueGroupAggregationBenchmarkResult(b, f.sl.Match(f.subjects[i%len(f.subjects)]), queueGroups, matchingNodes)
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

func BenchmarkSublistQueueGroupAggregationPromotion(b *testing.B) {
	const (
		groupsPerNode = 32
		queueGroups   = 80
	)

	s := NewSublistNoCache()
	subjects := make([]string, 8)
	for i := range subjects {
		subjects[i] = "perfqga." + strconv.Itoa(i)
	}
	insert := func(subject, queue string) {
		if err := s.Insert(newQSub(subject, queue)); err != nil {
			b.Fatal(err)
		}
	}
	for group := 0; group < groupsPerNode; group++ {
		insert("perfqga.>", "perfqga-q-"+strconv.Itoa(group))
	}
	for _, subject := range subjects {
		for group := groupsPerNode; group < 64; group++ {
			insert(subject, "perfqga-q-"+strconv.Itoa(group))
		}
	}
	for group := 0; group < groupsPerNode/2; group++ {
		insert("perfqga.*", "perfqga-q-"+strconv.Itoa(group))
	}
	for group := 64; group < queueGroups; group++ {
		insert("perfqga.*", "perfqga-q-"+strconv.Itoa(group))
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r := s.Match(subjects[i%len(subjects)])
		if len(r.qsubs) != queueGroups {
			b.Fatalf("unexpected result shape: %d queue groups, want %d", len(r.qsubs), queueGroups)
		}
	}
}

func benchmarkSublistQueueGroupAggregationCacheMiss(b *testing.B, queueGroups int) {
	const matchingNodes = 3

	s := NewSublistWithCache()
	for group := 0; group < queueGroups; group++ {
		queue := "perfqga-q-" + strconv.Itoa(group)
		for _, filter := range []string{">", "*.*", "*.tail"} {
			if err := s.Insert(newQSub(filter, queue)); err != nil {
				b.Fatal(err)
			}
		}
	}

	hits := atomic.LoadUint64(&s.cacheHits)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		subject := "perfqga" + strconv.Itoa(i) + ".tail"
		checkQueueGroupAggregationBenchmarkResult(b, s.Match(subject), queueGroups, matchingNodes)
	}
	b.StopTimer()
	if got := atomic.LoadUint64(&s.cacheHits) - hits; got != 0 {
		b.Fatalf("cache hits = %d, want 0", got)
	}
}

func BenchmarkSublistQueueGroupAggregationCacheChurn(b *testing.B) {
	benchmarkSublistQueueGroupAggregationCacheMiss(b, 64)
}

func BenchmarkSublistQueueGroupAggregationCacheMiss(b *testing.B) {
	for _, queueGroups := range []int{64, 257} {
		b.Run("groups="+strconv.Itoa(queueGroups), func(b *testing.B) {
			benchmarkSublistQueueGroupAggregationCacheMiss(b, queueGroups)
		})
	}
}

func BenchmarkSublistQueueGroupAggregationPoolBoundary(b *testing.B) {
	for _, queueGroups := range []int{512, 513} {
		b.Run(fmt.Sprintf("groups=%d/nodes=3", queueGroups), func(b *testing.B) {
			f := newQueueGroupAggregationFixture(false, queueGroups, 3, false)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				checkQueueGroupAggregationBenchmarkResult(b, f.sl.Match(f.subjects[i%len(f.subjects)]), queueGroups, 3)
			}
		})
	}
}

func BenchmarkSublistQueueGroupAggregationCacheMissPoolBoundary(b *testing.B) {
	for _, queueGroups := range []int{512, 513} {
		b.Run(fmt.Sprintf("groups=%d", queueGroups), func(b *testing.B) {
			benchmarkSublistQueueGroupAggregationCacheMiss(b, queueGroups)
		})
	}
}

func BenchmarkSublistQueueGroupAggregationConcurrent(b *testing.B) {
	const (
		queueGroups   = 64
		matchingNodes = 3
	)

	f := newQueueGroupAggregationFixture(false, queueGroups, matchingNodes, false)
	b.ReportAllocs()
	b.SetParallelism(4)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		subject := 0
		for pb.Next() {
			r := f.sl.Match(f.subjects[subject%len(f.subjects)])
			checkQueueGroupAggregationBenchmarkResult(b, r, queueGroups, matchingNodes)
			subject++
		}
	})
}
