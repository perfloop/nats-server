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
)

type queueGroupAggregationFixture struct {
	sl       *Sublist
	subjects []string
	expected map[string]map[string][]*subscription
}

func newQueueGroupAggregationFixture(cache bool, queueGroups, matchingNodes int, remote bool) *queueGroupAggregationFixture {
	if matchingNodes < 1 || matchingNodes > 3 {
		panic("invalid matching node count")
	}

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

	insert := func(subject, queue string, matches []string, weighted bool) {
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

	for group := 0; group < queueGroups; group++ {
		queue := fmt.Sprintf("perfqga-q-%03d", group)
		insert("perfqga.>", queue, f.subjects, false)
		if matchingNodes >= 2 {
			insert("perfqga.*", queue, f.subjects, false)
		}
		if matchingNodes >= 3 {
			for i, subject := range f.subjects {
				insert(subject, queue, []string{subject}, remote && group == 0 && i == 0)
			}
		}
	}

	return f
}

func TestSublistQueueGroupAggregationAcrossMatchedNodes(t *testing.T) {
	const (
		queueGroups   = 16
		matchingNodes = 3
	)

	for _, cache := range []bool{false, true} {
		t.Run(fmt.Sprintf("cache=%t", cache), func(t *testing.T) {
			f := newQueueGroupAggregationFixture(cache, queueGroups, matchingNodes, true)
			for pass := 0; pass < 2; pass++ {
				for _, subject := range f.subjects {
					r := f.sl.Match(subject)
					if got := len(r.qsubs); got != queueGroups {
						t.Fatalf("Match(%q) returned %d queue groups, want %d", subject, got, queueGroups)
					}
					for queue, want := range f.expected[subject] {
						slot := findQSlot([]byte(queue), r.qsubs)
						if slot < 0 {
							t.Fatalf("Match(%q) omitted queue group %q", subject, queue)
						}
						got := r.qsubs[slot]
						if len(got) != len(want) {
							t.Fatalf("Match(%q) queue group %q has %d subscriptions, want %d", subject, queue, len(got), len(want))
						}
						for _, sub := range want {
							verifyMember(got, sub, t)
						}
					}
				}
			}
		})
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
		{16, 3},
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
