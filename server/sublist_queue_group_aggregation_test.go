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
	"sync"
	"testing"
)

type queueGroupAggregationFixture struct {
	sl       *Sublist
	subjects []string
	expected map[string]map[string][]*subscription
}

func newQueueGroupAggregationFixtureBase(cache bool, queueGroups int) *queueGroupAggregationFixture {
	f := &queueGroupAggregationFixture{
		sl:       NewSublist(cache),
		subjects: make([]string, 8),
		expected: make(map[string]map[string][]*subscription),
	}
	for i := range f.subjects {
		subject := fmt.Sprintf("perfqga.%d", i)
		f.subjects[i] = subject
		f.expected[subject] = make(map[string][]*subscription, queueGroups)
	}
	return f
}

func (f *queueGroupAggregationFixture) insert(subject, queue string, matches []string, weighted bool) {
	var sub *subscription
	if weighted {
		sub = newRemoteQSub(subject, queue, 2)
	} else {
		sub = newQSub(subject, queue)
	}
	if err := f.sl.Insert(sub); err != nil {
		panic(err)
	}
	copies := 1
	if weighted {
		copies = int(sub.qw)
	}
	for _, match := range matches {
		for range copies {
			f.expected[match][queue] = append(f.expected[match][queue], sub)
		}
	}
}

func newQueueGroupAggregationFixture(cache bool, queueGroups, matchingNodes int, remote bool) *queueGroupAggregationFixture {
	if matchingNodes < 1 || matchingNodes > 3 {
		panic("invalid matching node count")
	}

	f := newQueueGroupAggregationFixtureBase(cache, queueGroups)
	for group := 0; group < queueGroups; group++ {
		queue := fmt.Sprintf("perfqga-q-%03d", group)
		f.insert("perfqga.>", queue, f.subjects, false)
		if matchingNodes >= 2 {
			f.insert("perfqga.*", queue, f.subjects, false)
		}
		if matchingNodes >= 3 {
			for i, subject := range f.subjects {
				f.insert(subject, queue, []string{subject}, remote && group == 0 && i == 0)
			}
		}
	}
	return f
}

func newQueueGroupAggregationPromotionFixture(cache bool) *queueGroupAggregationFixture {
	const groupsPerNode = qslotMapMin / 2

	// matchLevel adds the full wildcard, literal, then partial wildcard node.
	// The first two create 64 disjoint groups; the final 32-group node exercises
	// promotion from accumulated results with both existing and new queue names.
	f := newQueueGroupAggregationFixtureBase(cache, qslotMapMin+groupsPerNode/2)
	for group := 0; group < groupsPerNode; group++ {
		f.insert("perfqga.>", fmt.Sprintf("perfqga-q-%03d", group), f.subjects, false)
	}
	for _, subject := range f.subjects {
		for group := groupsPerNode; group < qslotMapMin; group++ {
			f.insert(subject, fmt.Sprintf("perfqga-q-%03d", group), []string{subject}, false)
		}
	}
	for group := 0; group < groupsPerNode/2; group++ {
		f.insert("perfqga.*", fmt.Sprintf("perfqga-q-%03d", group), f.subjects, false)
	}
	for group := qslotMapMin; group < qslotMapMin+groupsPerNode/2; group++ {
		f.insert("perfqga.*", fmt.Sprintf("perfqga-q-%03d", group), f.subjects, false)
	}
	return f
}

func sameSubscriptions(got, want []*subscription) bool {
	if len(got) != len(want) {
		return false
	}
	counts := make(map[*subscription]int, len(want))
	for _, sub := range want {
		counts[sub]++
	}
	for _, sub := range got {
		if counts[sub] == 0 {
			return false
		}
		counts[sub]--
	}
	for _, count := range counts {
		if count != 0 {
			return false
		}
	}
	return true
}

func checkQueueGroupAggregationResult(f *queueGroupAggregationFixture, subject string, r *SublistResult) error {
	if got, want := len(r.qsubs), len(f.expected[subject]); got != want {
		return fmt.Errorf("Match(%q) returned %d queue groups, want %d", subject, got, want)
	}
	for queue, want := range f.expected[subject] {
		slot := findQSlot([]byte(queue), r.qsubs)
		if slot < 0 {
			return fmt.Errorf("Match(%q) omitted queue group %q", subject, queue)
		}
		if got := r.qsubs[slot]; !sameSubscriptions(got, want) {
			return fmt.Errorf("Match(%q) queue group %q differs from expected membership", subject, queue)
		}
	}
	return nil
}

func checkQueueGroupAggregationFixture(t *testing.T, f *queueGroupAggregationFixture) {
	t.Helper()
	for pass := 0; pass < 2; pass++ {
		for _, subject := range f.subjects {
			if err := checkQueueGroupAggregationResult(f, subject, f.sl.Match(subject)); err != nil {
				t.Fatal(err)
			}
		}
	}
}

func TestSublistQueueGroupAggregationAcrossMatchedNodes(t *testing.T) {
	for _, cache := range []bool{false, true} {
		t.Run(fmt.Sprintf("cache=%t/per-node-index", cache), func(t *testing.T) {
			checkQueueGroupAggregationFixture(t, newQueueGroupAggregationFixture(cache, qslotMapMin, 3, true))
		})
		t.Run(fmt.Sprintf("cache=%t/accumulated-index", cache), func(t *testing.T) {
			checkQueueGroupAggregationFixture(t, newQueueGroupAggregationPromotionFixture(cache))
		})
	}

	t.Run("uncached/above-pool-limit", func(t *testing.T) {
		checkQueueGroupAggregationFixture(t, newQueueGroupAggregationFixture(false, qslotMapPoolMax+1, 3, false))
	})

	t.Run("concurrent-uncached", func(t *testing.T) {
		f := newQueueGroupAggregationFixture(false, qslotMapMin, 3, true)
		const workers = 8
		start := make(chan struct{})
		errs := make(chan error, workers)
		var wg sync.WaitGroup
		for worker := 0; worker < workers; worker++ {
			wg.Add(1)
			go func(worker int) {
				defer wg.Done()
				<-start
				for match := 0; match < len(f.subjects)*2; match++ {
					subject := f.subjects[(worker+match)%len(f.subjects)]
					if err := checkQueueGroupAggregationResult(f, subject, f.sl.Match(subject)); err != nil {
						errs <- err
						return
					}
				}
			}(worker)
		}
		close(start)
		wg.Wait()
		close(errs)
		for err := range errs {
			t.Fatal(err)
		}
	})
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
