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

type perAccountCacheLookup struct {
	account  []byte
	subject  []byte
	cacheKey []byte
}

func newPerAccountCacheFixture(tb testing.TB, unaffectedKeys int) (*client, *Account, *Account, perAccountCacheLookup, []perAccountCacheLookup) {
	tb.Helper()

	srv := &Server{}
	accA := NewAccount("A")
	accB := NewAccount("B")
	accA.sl = NewSublistWithCache()
	accB.sl = NewSublistWithCache()
	srv.accounts.Store(accA.Name, accA)
	srv.accounts.Store(accB.Name, accB)

	addSub := func(acc *Account, subject string) {
		tb.Helper()
		if err := acc.sl.Insert(&subscription{subject: []byte(subject)}); err != nil {
			tb.Fatalf("error adding subscription for %q: %v", subject, err)
		}
	}

	const changedSubject = "a.subject"
	addSub(accA, changedSubject)
	aLookup := perAccountCacheLookup{
		account:  []byte(accA.Name),
		subject:  []byte(changedSubject),
		cacheKey: []byte(accA.Name + " " + changedSubject),
	}

	bLookups := make([]perAccountCacheLookup, 0, unaffectedKeys)
	for i := 0; i < unaffectedKeys; i++ {
		subject := fmt.Sprintf("b.%03d", i)
		addSub(accB, subject)
		bLookups = append(bLookups, perAccountCacheLookup{
			account:  []byte(accB.Name),
			subject:  []byte(subject),
			cacheKey: []byte(accB.Name + " " + subject),
		})
	}

	c := &client{
		kind:  ROUTER,
		srv:   srv,
		route: &route{},
		in: readCache{
			pacache: make(map[string]*perAccountCache, unaffectedKeys+1),
		},
	}
	return c, accA, accB, aLookup, bLookups
}

func lookupPerAccountCache(c *client, lookup perAccountCacheLookup) (*Account, *SublistResult) {
	c.pa.account = lookup.account
	c.pa.subject = lookup.subject
	c.pa.pacache = lookup.cacheKey
	return c.getAccAndResultFromCache()
}

func primePerAccountCache(tb testing.TB, c *client, accA, accB *Account, aLookup perAccountCacheLookup, bLookups []perAccountCacheLookup) {
	tb.Helper()

	if acc, results := lookupPerAccountCache(c, aLookup); acc != accA || len(results.psubs) != 1 {
		tb.Fatalf("unexpected A cache result: account=%p results=%d", acc, len(results.psubs))
	}
	for _, lookup := range bLookups {
		if acc, results := lookupPerAccountCache(c, lookup); acc != accB || len(results.psubs) != 1 {
			tb.Fatalf("unexpected B cache result for %q: account=%p results=%d", lookup.subject, acc, len(results.psubs))
		}
	}
}

func TestPerAccountCacheRefreshesGeneration(t *testing.T) {
	c, accA, accB, aLookup, bLookups := newPerAccountCacheFixture(t, 1)
	primePerAccountCache(t, c, accA, accB, aLookup, bLookups)

	// A second subscription changes only A's generation and makes A's cached
	// result stale. B must remain routable after A refreshes.
	if err := accA.sl.Insert(&subscription{subject: []byte("a.subject")}); err != nil {
		t.Fatalf("error changing account A subscriptions: %v", err)
	}
	if acc, results := lookupPerAccountCache(c, aLookup); acc != accA || len(results.psubs) != 2 {
		t.Fatalf("stale A cache result: account=%p results=%d", acc, len(results.psubs))
	}
	if acc, results := lookupPerAccountCache(c, bLookups[0]); acc != accB || len(results.psubs) != 1 {
		t.Fatalf("unexpected B result after A refresh: account=%p results=%d", acc, len(results.psubs))
	}
}

var perAccountCacheRouteMessage = []byte("\r\n")

func newPerAccountRouteFixture(tb testing.TB, staleAccountKeys, unaffectedKeys int) (*client, *Account, *Account, []perAccountCacheLookup, []perAccountCacheLookup) {
	tb.Helper()

	srv := &Server{gateway: &srvGateway{}}
	accA := NewAccount("A")
	accB := NewAccount("B")
	accA.sl = NewSublistWithCache()
	accB.sl = NewSublistWithCache()
	srv.accounts.Store(accA.Name, accA)
	srv.accounts.Store(accB.Name, accB)

	aLookups := make([]perAccountCacheLookup, 0, staleAccountKeys)
	for i := 0; i < staleAccountKeys; i++ {
		subject := fmt.Sprintf("a.%03d", i)
		aLookups = append(aLookups, perAccountCacheLookup{
			account:  []byte(accA.Name),
			subject:  []byte(subject),
			cacheKey: []byte(accA.Name + " " + subject),
		})
	}
	bLookups := make([]perAccountCacheLookup, 0, unaffectedKeys)
	for i := 0; i < unaffectedKeys; i++ {
		subject := fmt.Sprintf("b.%03d", i)
		bLookups = append(bLookups, perAccountCacheLookup{
			account:  []byte(accB.Name),
			subject:  []byte(subject),
			cacheKey: []byte(accB.Name + " " + subject),
		})
	}

	c := &client{
		kind:  ROUTER,
		srv:   srv,
		route: &route{},
		in: readCache{
			pacache: make(map[string]*perAccountCache, staleAccountKeys+unaffectedKeys),
		},
	}
	return c, accA, accB, aLookups, bLookups
}

func processRoutedCacheMessage(c *client, lookup perAccountCacheLookup) {
	c.pa.account = lookup.account
	c.pa.subject = lookup.subject
	c.pa.pacache = lookup.cacheKey
	c.processInboundRoutedMsg(perAccountCacheRouteMessage)
}

func checkPerAccountRouteCache(tb testing.TB, c *client, lookup perAccountCacheLookup, expected *Account) {
	tb.Helper()
	pac := c.in.pacache[string(lookup.cacheKey)]
	if pac == nil || pac.acc != expected {
		tb.Fatalf("unexpected route cache entry for %q: %#v", lookup.cacheKey, pac)
	}
}

func primePerAccountRouteCache(tb testing.TB, c *client, accA, accB *Account, aLookups, bLookups []perAccountCacheLookup) {
	tb.Helper()

	for _, lookup := range aLookups {
		processRoutedCacheMessage(c, lookup)
		checkPerAccountRouteCache(tb, c, lookup, accA)
	}
	for _, lookup := range bLookups {
		processRoutedCacheMessage(c, lookup)
		checkPerAccountRouteCache(tb, c, lookup, accB)
	}
}

func TestPerAccountRouteCacheRefreshesGeneration(t *testing.T) {
	c, accA, accB, aLookups, bLookups := newPerAccountRouteFixture(t, 2, 1)
	primePerAccountRouteCache(t, c, accA, accB, aLookups, bLookups)

	if err := accA.sl.Insert(&subscription{subject: []byte("a.changed")}); err != nil {
		t.Fatalf("error changing account A subscriptions: %v", err)
	}
	processRoutedCacheMessage(c, aLookups[0])
	checkPerAccountRouteCache(t, c, aLookups[0], accA)
	processRoutedCacheMessage(c, bLookups[0])
	checkPerAccountRouteCache(t, c, bLookups[0], accB)
}

type perAccountCacheProbe struct {
	aL1StaleReloads int
	aStaleEntries   int
	bL1Hits         int
	bL1Reloads      int
	aSublistMatches int
	bSublistMatches int
}

func probePerAccountRouteCache(tb testing.TB, staleAccountKeys, unaffectedKeys int) perAccountCacheProbe {
	tb.Helper()

	c, accA, accB, aLookups, bLookups := newPerAccountRouteFixture(tb, staleAccountKeys, unaffectedKeys)
	primePerAccountRouteCache(tb, c, accA, accB, aLookups, bLookups)
	beforeAMatches := atomic.LoadUint64(&accA.sl.matches)
	beforeBMatches := atomic.LoadUint64(&accB.sl.matches)

	// This real sublist mutation changes only A's generation. Its subject is
	// different from the measured key, so the timed cost remains the cache
	// refresh and subsequent routed-message processing rather than delivery.
	if err := accA.sl.Insert(&subscription{subject: []byte("a.changed")}); err != nil {
		tb.Fatalf("error changing account A subscriptions: %v", err)
	}

	probe := perAccountCacheProbe{}
	generation := atomic.LoadUint64(&accA.sl.genid)
	for _, lookup := range aLookups {
		pac := c.in.pacache[string(lookup.cacheKey)]
		if pac == nil {
			tb.Fatalf("missing primed A route cache entry for %q", lookup.cacheKey)
		}
		if pac.genid != generation {
			probe.aStaleEntries++
		}
	}

	aLookup := aLookups[0]
	processRoutedCacheMessage(c, aLookup)
	checkPerAccountRouteCache(tb, c, aLookup, accA)
	probe.aL1StaleReloads = int(atomic.LoadUint64(&accA.sl.matches) - beforeAMatches)

	for _, lookup := range bLookups {
		if _, ok := c.in.pacache[string(lookup.cacheKey)]; ok {
			probe.bL1Hits++
		} else {
			probe.bL1Reloads++
		}
		processRoutedCacheMessage(c, lookup)
		checkPerAccountRouteCache(tb, c, lookup, accB)
	}
	probe.aSublistMatches = int(atomic.LoadUint64(&accA.sl.matches) - beforeAMatches)
	probe.bSublistMatches = int(atomic.LoadUint64(&accB.sl.matches) - beforeBMatches)
	return probe
}

func BenchmarkPerAccountCacheBReplayAfterARefresh(b *testing.B) {
	const (
		staleAccountKeys = 8
		unaffectedKeys   = 64
	)

	// This probe binds B's cache state after a real A-only mutation. The timed
	// loop then measures the same stale-A-plus-B-replay cycle, amortized over
	// the B messages whose residency the guard protects.
	probe := probePerAccountRouteCache(b, staleAccountKeys, unaffectedKeys)
	if probe.aStaleEntries != staleAccountKeys || probe.aL1StaleReloads != 1 || probe.aSublistMatches != 1 || probe.bL1Hits+probe.bL1Reloads != unaffectedKeys {
		b.Fatalf("unexpected cache probe: %+v", probe)
	}

	c, accA, accB, aLookups, bLookups := newPerAccountRouteFixture(b, staleAccountKeys, unaffectedKeys)
	primePerAccountRouteCache(b, c, accA, accB, aLookups, bLookups)
	aLookup := aLookups[0]

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		atomic.AddUint64(&accA.sl.genid, 1)
		processRoutedCacheMessage(c, aLookup)
		for _, lookup := range bLookups {
			processRoutedCacheMessage(c, lookup)
		}
	}
	b.StopTimer()

	checkPerAccountRouteCache(b, c, aLookup, accA)
	checkPerAccountRouteCache(b, c, bLookups[len(bLookups)-1], accB)
	b.ReportMetric(float64(probe.aL1StaleReloads), "a_l1_stale_reloads/stale_refresh")
	b.ReportMetric(float64(probe.aStaleEntries), "a_stale_entries/stale_refresh")
	b.ReportMetric(float64(probe.bL1Hits), "b_l1_hits/stale_refresh")
	b.ReportMetric(float64(probe.bL1Reloads), "b_l1_reloads/stale_refresh")
	if probe.bL1Hits == unaffectedKeys && probe.bL1Reloads == 0 {
		b.ReportMetric(1, "b_l1_residency/stale_refresh")
	} else {
		b.ReportMetric(0, "b_l1_residency/stale_refresh")
	}
	b.ReportMetric(float64(probe.aSublistMatches), "a_sublist_matches/stale_refresh")
	b.ReportMetric(float64(probe.bSublistMatches), "b_sublist_matches/stale_refresh")
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*unaffectedKeys), "b_replay_after_a_refresh_ns/op")
}

func BenchmarkPerAccountCacheMixedGenerationRefresh(b *testing.B) {
	const (
		staleAccountKeys = 8
		unaffectedKeys   = 64
	)

	// The probe is outside the timed section. It primes several A and B keys,
	// then records the per-account L1 and Sublist.MatchBytes effects of one
	// real A-only mutation and one stale A routed message.
	probe := probePerAccountRouteCache(b, staleAccountKeys, unaffectedKeys)
	if probe.aStaleEntries != staleAccountKeys || probe.aL1StaleReloads != 1 || probe.aSublistMatches != 1 || probe.bL1Hits+probe.bL1Reloads != unaffectedKeys {
		b.Fatalf("unexpected cache probe: %+v", probe)
	}

	c, accA, accB, aLookups, bLookups := newPerAccountRouteFixture(b, staleAccountKeys, unaffectedKeys)
	primePerAccountRouteCache(b, c, accA, accB, aLookups, bLookups)
	aLookup := aLookups[0]

	b.ReportAllocs()
	b.ResetTimer()
	cpuBefore, err := perAccountCacheCPUSeconds()
	if err != nil {
		b.Fatal(err)
	}
	for b.Loop() {
		// The atomic generation increment isolates the post-mutation cost. The
		// probe above demonstrates that a real A subscription mutation reaches
		// this same stale-entry branch.
		atomic.AddUint64(&accA.sl.genid, 1)
		processRoutedCacheMessage(c, aLookup)
		for _, lookup := range bLookups {
			processRoutedCacheMessage(c, lookup)
		}
	}
	b.StopTimer()
	cpuAfter, err := perAccountCacheCPUSeconds()
	if err != nil {
		b.Fatal(err)
	}

	checkPerAccountRouteCache(b, c, aLookup, accA)
	checkPerAccountRouteCache(b, c, bLookups[len(bLookups)-1], accB)
	b.ReportMetric(float64(probe.aL1StaleReloads), "a_l1_stale_reloads/stale_refresh")
	b.ReportMetric(float64(probe.aStaleEntries), "a_stale_entries/stale_refresh")
	b.ReportMetric(float64(probe.bL1Hits), "b_l1_hits/stale_refresh")
	b.ReportMetric(float64(probe.bL1Reloads), "b_l1_reloads/stale_refresh")
	if probe.bL1Hits == unaffectedKeys && probe.bL1Reloads == 0 {
		b.ReportMetric(1, "b_l1_residency/stale_refresh")
	} else {
		b.ReportMetric(0, "b_l1_residency/stale_refresh")
	}
	b.ReportMetric(float64(probe.aSublistMatches), "a_sublist_matches/stale_refresh")
	b.ReportMetric(float64(probe.bSublistMatches), "b_sublist_matches/stale_refresh")
	messages := float64(b.N * (unaffectedKeys + 1))
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/messages, "routed_message_ns/op")
	b.ReportMetric((cpuAfter-cpuBefore)*1e9/messages, "cpu_ns/routed_message")
}
