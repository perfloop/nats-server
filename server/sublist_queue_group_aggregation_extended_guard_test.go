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
	"strconv"
	"sync/atomic"
	"testing"
)

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

func BenchmarkSublistQueueGroupAggregationCacheMiss(b *testing.B) {
	for _, queueGroups := range []int{64, 257} {
		b.Run("groups="+strconv.Itoa(queueGroups), func(b *testing.B) {
			benchmarkSublistQueueGroupAggregationCacheMiss(b, queueGroups)
		})
	}
}
