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

// BenchmarkPerAccountCacheRoutedMixedRefresh64 measures a routed-message cycle
// with one stale A entry and 64 reusable B entries. Each cycle mutates A through
// the normal subscription add/remove path, refreshes A through the route handler,
// and then routes every B subject to an internal subscriber. The controls report
// whether the B entries were still in the route L1 before their replay and how
// many account sublist matches the cycle required.
func BenchmarkPerAccountCacheRoutedMixedRefresh64(b *testing.B) {
	const bKeys = 64

	s := New(DefaultOptions())
	a, err := s.RegisterAccount("A")
	if err != nil {
		b.Fatal(err)
	}
	bacc, err := s.RegisterAccount("B")
	if err != nil {
		b.Fatal(err)
	}

	var deliveries int
	newSubscriber := func(acc *Account) *client {
		return &client{
			kind:  ACCOUNT,
			srv:   s,
			acc:   acc,
			subs:  make(map[string]*subscription),
			msubs: -1,
			mpay:  -1,
		}
	}
	addSubscriber := func(c *client, subject, sid []byte) *subscription {
		sub, err := c.processSub(subject, nil, sid,
			func(*subscription, *client, *Account, string, string, []byte) { deliveries++ }, true)
		if err != nil {
			b.Fatal(err)
		}
		return sub
	}

	aName, aSubject := []byte("A"), []byte("a.hot")
	aCacheKey := []byte("A a.hot")
	mutationSubject := []byte("a.mutation")
	aClient := newSubscriber(a)
	addSubscriber(aClient, aSubject, aSubject)
	bClient := newSubscriber(bacc)
	bSubjects := make([][]byte, bKeys)
	bCacheKeys := make([]string, bKeys)
	bPACAKeys := make([][]byte, bKeys)
	for i := 0; i < bKeys; i++ {
		subject := fmt.Sprintf("b.%d", i)
		bSubjects[i] = []byte(subject)
		bCacheKeys[i] = "B " + subject
		bPACAKeys[i] = []byte(bCacheKeys[i])
		addSubscriber(bClient, bSubjects[i], bSubjects[i])
	}

	route := &client{
		kind:  ROUTER,
		srv:   s,
		route: &route{},
		in: readCache{
			pacache: make(map[string]*perAccountCache, bKeys+1),
		},
	}
	msg := []byte(_CRLF_)
	bName := []byte("B")
	receive := func(account, subject, pacache []byte) {
		route.pa.account = account
		route.pa.subject = subject
		route.pa.pacache = pacache
		route.processInboundRoutedMsg(msg)
	}

	// Prime one A and all B route-cache entries before timing.
	receive(aName, aSubject, aCacheKey)
	for i := 0; i < bKeys; i++ {
		receive(bName, bSubjects[i], bPACAKeys[i])
	}
	if got, want := len(route.in.pacache), bKeys+1; got != want {
		b.Fatalf("primed cache has %d entries, want %d", got, want)
	}
	matchesBefore := a.sl.Stats().NumMatches + bacc.sl.Stats().NumMatches

	b.ReportAllocs()
	b.ResetTimer()
	bL1Hits, bReloads, cycles := 0, 0, 0
	for b.Loop() {
		mutation, err := aClient.processSub(mutationSubject, nil, mutationSubject, nil, true)
		if err != nil {
			b.Fatal(err)
		}

		// This is the stale A refresh on the actual routed-message handler.
		receive(aName, aSubject, aCacheKey)
		for i := 0; i < bKeys; i++ {
			if _, ok := route.in.pacache[bCacheKeys[i]]; ok {
				bL1Hits++
			} else {
				bReloads++
			}
			receive(bName, bSubjects[i], bPACAKeys[i])
		}

		// Restore A's subscription state before the next stale-generation cycle.
		aClient.unsubscribe(a, mutation, true, true)
		cycles++
	}

	if deliveries != (cycles+1)*(bKeys+1) {
		b.Fatalf("routed deliveries = %d, want %d", deliveries, (cycles+1)*(bKeys+1))
	}
	if bL1Hits+bReloads != cycles*bKeys {
		b.Fatalf("B replays = %d, want %d", bL1Hits+bReloads, cycles*bKeys)
	}
	matchesAfter := a.sl.Stats().NumMatches + bacc.sl.Stats().NumMatches
	b.ReportMetric(float64(bL1Hits)/float64(cycles), "b_l1_hits/op")
	b.ReportMetric(float64(bReloads)/float64(cycles), "b_reloads/op")
	b.ReportMetric(float64(matchesAfter-matchesBefore)/float64(cycles), "sublist_matches/op")
}
