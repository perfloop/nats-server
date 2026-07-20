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

import "testing"

const perAccountCacheCapacityGuardSize = 2

// prepareSaturatedPerAccountCache fills the route cache with one A and one B
// entry, advances only A's generation, and refreshes A through the routed
// message handler. The next B key is deliberately cold.
func prepareSaturatedPerAccountCache(tb testing.TB, f *perAccountCacheFixture) (preColdEntries int) {
	tb.Helper()

	clear(f.c.in.pacache)
	f.route(f.bKeys[0])
	f.route(f.aKeys[0])
	if got := len(f.c.in.pacache); got != maxPerAccountCacheSize {
		tb.Fatalf("primed per-account cache entries: got %d, want %d", got, maxPerAccountCacheSize)
	}
	if _, ok := f.c.in.pacache[string(f.bKeys[1].cache)]; ok {
		tb.Fatal("cold B key was present before the generation refresh")
	}

	f.toggleAccountAGeneration(tb)
	f.route(f.aKeys[0])
	f.requireCachedResult(tb, f.aKeys[0], f.a, f.aSubs[0])

	preColdEntries = len(f.c.in.pacache)
	if preColdEntries != 1 && preColdEntries != maxPerAccountCacheSize {
		tb.Fatalf("entries after refreshing stale A key: got %d, want 1 or %d", preColdEntries, maxPerAccountCacheSize)
	}
	return preColdEntries
}

func verifySaturatedColdKey(tb testing.TB, f *perAccountCacheFixture, preColdEntries int) (pruned bool) {
	tb.Helper()

	f.requireCachedResult(tb, f.bKeys[1], f.b, f.bSubs[1])
	postColdEntries := len(f.c.in.pacache)
	if postColdEntries != maxPerAccountCacheSize {
		tb.Fatalf("entries after cold B key: got %d, want %d", postColdEntries, maxPerAccountCacheSize)
	}
	if preColdEntries == maxPerAccountCacheSize {
		// The cold key was absent and the map stayed full, so the capacity
		// pruning path had to remove an existing entry before insertion.
		return true
	}
	if postColdEntries != preColdEntries+1 {
		tb.Fatalf("cold B key changed entries from %d to %d, want %d", preColdEntries, postColdEntries, preColdEntries+1)
	}
	return false
}

func TestPerAccountCacheColdKeyAfterGenerationRefresh(t *testing.T) {
	oldMax := maxPerAccountCacheSize
	maxPerAccountCacheSize = perAccountCacheCapacityGuardSize
	t.Cleanup(func() { maxPerAccountCacheSize = oldMax })

	f := newPerAccountCacheFixture(t)
	preColdEntries := prepareSaturatedPerAccountCache(t, f)
	beforeA, beforeB := f.matchCounts()
	beforeDeliveries := f.deliveries
	f.route(f.bKeys[1])
	pruned := verifySaturatedColdKey(t, f, preColdEntries)
	if got := f.deliveries - beforeDeliveries; got != 1 {
		t.Fatalf("cold routed-message deliveries: got %d, want 1", got)
	}
	afterA, afterB := f.matchCounts()
	if got := afterA - beforeA; got != 0 {
		t.Fatalf("A sublist matches for cold B key: got %d, want 0", got)
	}
	if got := afterB - beforeB; got != 1 {
		t.Fatalf("B sublist matches for cold B key: got %d, want 1", got)
	}
	t.Logf("saturated cache cold-key scenario: pre_cold_entries=%d pruned=%t", preColdEntries, pruned)
}

func BenchmarkPerAccountCacheColdKeyAfterGenerationRefresh(b *testing.B) {
	oldMax := maxPerAccountCacheSize
	maxPerAccountCacheSize = perAccountCacheCapacityGuardSize
	b.Cleanup(func() { maxPerAccountCacheSize = oldMax })

	f := newPerAccountCacheFixture(b)
	var prunes, preColdEntriesTotal, coldBMatches uint64

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		b.StopTimer()
		preColdEntries := prepareSaturatedPerAccountCache(b, f)
		beforeA, beforeB := f.matchCounts()
		beforeDeliveries := f.deliveries
		b.StartTimer()

		// This is the destination operation that may prune after a stale A
		// refresh retained the rest of a full per-account cache.
		f.route(f.bKeys[1])

		b.StopTimer()
		if got := f.deliveries - beforeDeliveries; got != 1 {
			b.Fatalf("cold routed-message deliveries: got %d, want 1", got)
		}
		afterA, afterB := f.matchCounts()
		if got := afterA - beforeA; got != 0 {
			b.Fatalf("A sublist matches for cold B key: got %d, want 0", got)
		}
		coldMatches := afterB - beforeB
		if coldMatches != 1 {
			b.Fatalf("B sublist matches for cold B key: got %d, want 1", coldMatches)
		}
		if verifySaturatedColdKey(b, f, preColdEntries) {
			prunes++
		}
		preColdEntriesTotal += uint64(preColdEntries)
		coldBMatches += coldMatches
		b.StartTimer()
	}
	b.StopTimer()

	b.ReportMetric(float64(prunes)/float64(b.N), "prune_events/op")
	b.ReportMetric(float64(preColdEntriesTotal)/float64(b.N), "pre_cold_entries/op")
	b.ReportMetric(float64(coldBMatches)/float64(b.N), "cold_b_sublist_matches/op")
	b.ReportMetric(1, "routed_messages/op")
}
