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
	"runtime"
	"sync/atomic"
	"testing"
)

const perAccountCacheCapacityTestSize = 32

type perAccountCacheCapacityProbe struct {
	entriesAfterRefresh int
	entriesAfterReplay  int
	bL1Hits             int
	bL1Reloads          int
	bSublistMatches     int
}

func newPerAccountCacheCapacityFixture(tb testing.TB) (*client, *Account, *Account, perAccountCacheLookup, []perAccountCacheLookup) {
	tb.Helper()

	previousSize := maxPerAccountCacheSize
	maxPerAccountCacheSize = perAccountCacheCapacityTestSize
	tb.Cleanup(func() {
		maxPerAccountCacheSize = previousSize
	})

	c, accA, accB, aLookup, bLookups := newPerAccountCacheFixture(tb, perAccountCacheCapacityTestSize-1)
	primePerAccountCache(tb, c, accA, accB, aLookup, bLookups)
	if len(c.in.pacache) != maxPerAccountCacheSize {
		tb.Fatalf("expected full per-account cache, got %d entries", len(c.in.pacache))
	}
	return c, accA, accB, aLookup, bLookups
}

func probePerAccountCacheAtCapacity(tb testing.TB) perAccountCacheCapacityProbe {
	tb.Helper()

	c, accA, accB, aLookup, bLookups := newPerAccountCacheCapacityFixture(tb)
	beforeBMatches := atomic.LoadUint64(&accB.sl.matches)
	if err := accA.sl.Insert(&subscription{subject: []byte("a.subject")}); err != nil {
		tb.Fatalf("error changing account A subscriptions: %v", err)
	}
	if acc, results := lookupPerAccountCache(c, aLookup); acc != accA || len(results.psubs) != 2 {
		tb.Fatalf("stale A cache result: account=%p results=%d", acc, len(results.psubs))
	}

	probe := perAccountCacheCapacityProbe{entriesAfterRefresh: len(c.in.pacache)}
	for _, lookup := range bLookups {
		if _, ok := c.in.pacache[string(lookup.cacheKey)]; ok {
			probe.bL1Hits++
		} else {
			probe.bL1Reloads++
		}
		if acc, results := lookupPerAccountCache(c, lookup); acc != accB || len(results.psubs) != 1 {
			tb.Fatalf("unexpected B cache result for %q: account=%p results=%d", lookup.subject, acc, len(results.psubs))
		}
	}
	probe.bSublistMatches = int(atomic.LoadUint64(&accB.sl.matches) - beforeBMatches)
	probe.entriesAfterReplay = len(c.in.pacache)
	return probe
}

func TestPerAccountCacheAtCapacityRefreshKeepsBEntries(t *testing.T) {
	c, accA, accB, aLookup, bLookups := newPerAccountCacheCapacityFixture(t)

	// Exercise enough real A-only mutations that an implementation which prunes
	// on an in-place refresh cannot pass by repeatedly choosing A as the victim.
	for i := 0; i < perAccountCacheCapacityTestSize; i++ {
		if err := accA.sl.Insert(&subscription{subject: []byte("a.subject")}); err != nil {
			t.Fatalf("error changing account A subscriptions: %v", err)
		}
		if acc, results := lookupPerAccountCache(c, aLookup); acc != accA || len(results.psubs) != i+2 {
			t.Fatalf("stale A cache result after mutation %d: account=%p results=%d", i, acc, len(results.psubs))
		}
		if entries := len(c.in.pacache); entries != maxPerAccountCacheSize {
			t.Fatalf("cache size after mutation %d = %d, want %d", i, entries, maxPerAccountCacheSize)
		}

		beforeBMatches := atomic.LoadUint64(&accB.sl.matches)
		for _, lookup := range bLookups {
			if _, ok := c.in.pacache[string(lookup.cacheKey)]; !ok {
				t.Fatalf("B cache entry %q was evicted by A refresh %d", lookup.cacheKey, i)
			}
			if acc, results := lookupPerAccountCache(c, lookup); acc != accB || len(results.psubs) != 1 {
				t.Fatalf("unexpected B result for %q after A refresh %d: account=%p results=%d", lookup.subject, i, acc, len(results.psubs))
			}
		}
		if matches := atomic.LoadUint64(&accB.sl.matches); matches != beforeBMatches {
			t.Fatalf("B performed %d Sublist matches after A refresh %d", matches-beforeBMatches, i)
		}
		if entries := len(c.in.pacache); entries != maxPerAccountCacheSize {
			t.Fatalf("cache size after B replay %d = %d, want %d", i, entries, maxPerAccountCacheSize)
		}
	}
}

func BenchmarkPerAccountCacheAtCapacityRefresh(b *testing.B) {
	probe := probePerAccountCacheAtCapacity(b)
	if probe.entriesAfterRefresh < 1 || probe.entriesAfterRefresh > perAccountCacheCapacityTestSize || probe.entriesAfterReplay != perAccountCacheCapacityTestSize || probe.bL1Hits+probe.bL1Reloads != perAccountCacheCapacityTestSize-1 || probe.bSublistMatches < 0 {
		b.Fatalf("unexpected at-capacity cache probe: %+v", probe)
	}

	c, accA, _, aLookup, bLookups := newPerAccountCacheCapacityFixture(b)
	var result *SublistResult
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		atomic.AddUint64(&accA.sl.genid, 1)
		_, result = lookupPerAccountCache(c, aLookup)
		for _, lookup := range bLookups {
			_, result = lookupPerAccountCache(c, lookup)
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N), "at_capacity_mixed_refresh_ns/op")
	runtime.KeepAlive(result)
	if entries := len(c.in.pacache); entries != maxPerAccountCacheSize {
		b.Fatalf("cache size after benchmark = %d, want %d", entries, maxPerAccountCacheSize)
	}
	b.ReportMetric(float64(probe.entriesAfterRefresh), "cache_entries_after_refresh/stale_refresh")
	b.ReportMetric(float64(probe.entriesAfterReplay), "cache_entries_after_replay/stale_refresh")
	b.ReportMetric(float64(probe.bL1Hits), "b_l1_hits/stale_refresh")
	b.ReportMetric(float64(probe.bL1Reloads), "b_l1_reloads/stale_refresh")
	b.ReportMetric(float64(probe.bSublistMatches), "b_sublist_matches/stale_refresh")
}
