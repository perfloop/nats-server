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
	"sync/atomic"
	"testing"
)

const perAccountCacheSubjectCount = 8

type perAccountCacheKey struct {
	account []byte
	subject []byte
	cache   []byte
}

type perAccountCacheFixture struct {
	c        *client
	delivery *client
	a        *Account
	b        *Account
	msg      []byte

	aKeys []perAccountCacheKey
	bKeys []perAccountCacheKey
	aSubs []*subscription
	bSubs []*subscription

	mutation         *subscription
	mutationInserted bool
	deliveries       uint64
}

func newPerAccountCacheKey(account, subject string) perAccountCacheKey {
	cache := make([]byte, 0, len(account)+1+len(subject))
	cache = append(cache, account...)
	cache = append(cache, ' ')
	cache = append(cache, subject...)
	return perAccountCacheKey{
		account: []byte(account),
		subject: []byte(subject),
		cache:   cache,
	}
}

func (f *perAccountCacheFixture) newSub(subject []byte) *subscription {
	return &subscription{
		client:  f.delivery,
		subject: append([]byte(nil), subject...),
		icb: func(_ *subscription, _ *client, _ *Account, _ string, _ string, _ []byte) {
			f.deliveries++
		},
	}
}

func newPerAccountCacheFixture(tb testing.TB) *perAccountCacheFixture {
	tb.Helper()

	accountA, accountB := NewAccount("A"), NewAccount("B")
	opts := DefaultOptions()
	opts.NoSystemAccount = true
	opts.Accounts = []*Account{accountA, accountB}
	s, err := NewServer(opts)
	if err != nil {
		tb.Fatalf("create server: %v", err)
	}

	accountA, err = s.LookupAccount("A")
	if err != nil {
		tb.Fatalf("lookup account A: %v", err)
	}
	accountB, err = s.LookupAccount("B")
	if err != nil {
		tb.Fatalf("lookup account B: %v", err)
	}

	f := &perAccountCacheFixture{
		a:   accountA,
		b:   accountB,
		msg: []byte("x\r\n"),
	}
	f.c = &client{
		srv:   s,
		kind:  ROUTER,
		route: &route{},
		in: readCache{
			pacache: make(map[string]*perAccountCache),
		},
	}
	f.c.initClient()
	f.delivery = &client{srv: s, kind: ACCOUNT}
	f.delivery.initClient()

	for _, suffix := range [...]string{"one", "two", "three", "four", "five", "six", "seven", "eight"} {
		ak := newPerAccountCacheKey("A", "a."+suffix)
		bk := newPerAccountCacheKey("B", "b."+suffix)
		as := f.newSub(ak.subject)
		bs := f.newSub(bk.subject)
		if err := f.a.sl.Insert(as); err != nil {
			tb.Fatalf("insert account A subscription %q: %v", ak.subject, err)
		}
		if err := f.b.sl.Insert(bs); err != nil {
			tb.Fatalf("insert account B subscription %q: %v", bk.subject, err)
		}
		f.aKeys = append(f.aKeys, ak)
		f.bKeys = append(f.bKeys, bk)
		f.aSubs = append(f.aSubs, as)
		f.bSubs = append(f.bSubs, bs)
	}
	f.mutation = f.newSub([]byte("a.refresh"))
	f.prime(tb)
	return f
}

func (f *perAccountCacheFixture) route(key perAccountCacheKey) {
	f.c.pa.account = key.account
	f.c.pa.subject = key.subject
	f.c.pa.pacache = key.cache
	f.c.pa.reply = nil
	f.c.pa.queues = nil
	f.c.pa.hdr = 0
	f.c.processInboundRoutedMsg(f.msg)
}

func (f *perAccountCacheFixture) cached(key perAccountCacheKey) (*Account, *SublistResult) {
	pac := f.c.in.pacache[string(key.cache)]
	if pac == nil {
		return nil, nil
	}
	return pac.acc, pac.results
}

func containsPerAccountCacheSub(r *SublistResult, want *subscription) bool {
	if r == nil {
		return false
	}
	for _, sub := range r.psubs {
		if sub == want {
			return true
		}
	}
	return false
}

func (f *perAccountCacheFixture) requireCachedResult(tb testing.TB, key perAccountCacheKey, account *Account, want *subscription) {
	tb.Helper()
	gotAccount, gotResult := f.cached(key)
	if gotAccount != account {
		tb.Fatalf("cached account for %q: got %p (%v), want %p (%v)", key.cache, gotAccount, gotAccount, account, account)
	}
	if !containsPerAccountCacheSub(gotResult, want) {
		tb.Fatalf("cached result for account %q does not contain subscription %q", account.Name, want.subject)
	}
}

func (f *perAccountCacheFixture) prime(tb testing.TB) {
	tb.Helper()
	before := f.deliveries
	for i, key := range f.aKeys {
		f.route(key)
		f.requireCachedResult(tb, key, f.a, f.aSubs[i])
	}
	for i, key := range f.bKeys {
		f.route(key)
		f.requireCachedResult(tb, key, f.b, f.bSubs[i])
	}
	if got, want := f.deliveries-before, uint64(len(f.aKeys)+len(f.bKeys)); got != want {
		tb.Fatalf("primed routed-message deliveries: got %d, want %d", got, want)
	}
}

func (f *perAccountCacheFixture) toggleAccountAGeneration(tb testing.TB) {
	tb.Helper()
	var err error
	if f.mutationInserted {
		err = f.a.sl.Remove(f.mutation)
	} else {
		err = f.a.sl.Insert(f.mutation)
	}
	if err != nil {
		tb.Fatalf("change account A subscriptions: %v", err)
	}
	f.mutationInserted = !f.mutationInserted
}

func (f *perAccountCacheFixture) matchCounts() (a, b uint64) {
	return atomic.LoadUint64(&f.a.sl.matches), atomic.LoadUint64(&f.b.sl.matches)
}

func TestPerAccountCacheGenerationRefresh(t *testing.T) {
	f := newPerAccountCacheFixture(t)

	// Change the cached subject itself so that a stale A cache entry must be
	// refreshed, not merely invalidated by an unrelated generation change.
	freshA := f.newSub(f.aKeys[0].subject)
	if err := f.a.sl.Insert(freshA); err != nil {
		t.Fatalf("insert fresh account A subscription: %v", err)
	}

	beforeA, beforeB := f.matchCounts()
	beforeDeliveries := f.deliveries
	f.route(f.aKeys[0])
	f.requireCachedResult(t, f.aKeys[0], f.a, f.aSubs[0])
	_, result := f.cached(f.aKeys[0])
	if !containsPerAccountCacheSub(result, freshA) {
		t.Fatal("stale account A cache result did not include the new subscription")
	}
	// The refreshed A entry itself must be usable without another sublist reload.
	f.route(f.aKeys[0])
	f.requireCachedResult(t, f.aKeys[0], f.a, freshA)
	afterA, _ := f.matchCounts()
	if got := afterA - beforeA; got != 1 {
		t.Fatalf("account A reloads after its generation change: got %d, want 1", got)
	}

	for replay := 0; replay < 2; replay++ {
		for i, key := range f.bKeys {
			f.route(key)
			f.requireCachedResult(t, key, f.b, f.bSubs[i])
		}
	}
	_, afterB := f.matchCounts()
	bReloads := afterB - beforeB
	if bReloads > uint64(len(f.bKeys)) {
		t.Fatalf("account B reloads after account A changed: got %d, want at most %d", bReloads, len(f.bKeys))
	}
	if got, want := f.deliveries-beforeDeliveries, uint64(4+2*len(f.bKeys)); got != want {
		t.Fatalf("routed-message deliveries after account A changed: got %d, want %d", got, want)
	}
	t.Logf("mixed-account cache generation scenario: b_sublist_matches=%d a_sublist_matches=%d", bReloads, afterA-beforeA)
}

func BenchmarkPerAccountCacheMixedAccountGeneration(b *testing.B) {
	f := newPerAccountCacheFixture(b)

	var aMisses, aHits, bMisses, bHits uint64
	const replays = 2
	const routedMessagesPerOp = 2 + replays*perAccountCacheSubjectCount

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		// The subscription change is the invalidation trigger, not the routed
		// message work whose cache behavior is being timed.
		b.StopTimer()
		f.toggleAccountAGeneration(b)
		beforeA, beforeB := f.matchCounts()
		beforeDeliveries := f.deliveries
		b.StartTimer()

		f.route(f.aKeys[0])
		f.route(f.aKeys[0])
		for replay := 0; replay < replays; replay++ {
			for _, key := range f.bKeys {
				f.route(key)
			}
		}

		b.StopTimer()
		afterA, afterB := f.matchCounts()
		aReloads, bReloads := afterA-beforeA, afterB-beforeB
		if aReloads != 1 {
			b.Fatalf("account A reloads after its generation change: got %d, want 1", aReloads)
		}
		if bReloads > uint64(len(f.bKeys)*replays) {
			b.Fatalf("account B reloads after account A changed: got %d", bReloads)
		}
		if got, want := f.deliveries-beforeDeliveries, uint64(routedMessagesPerOp); got != want {
			b.Fatalf("routed-message deliveries: got %d, want %d", got, want)
		}
		f.requireCachedResult(b, f.aKeys[0], f.a, f.aSubs[0])
		for i, key := range f.bKeys {
			f.requireCachedResult(b, key, f.b, f.bSubs[i])
		}

		aMisses += aReloads
		aHits += 2 - aReloads
		bMisses += bReloads
		bHits += uint64(len(f.bKeys)*replays) - bReloads
		b.StartTimer()
	}
	b.StopTimer()

	ops := float64(b.N)
	b.ReportMetric(float64(routedMessagesPerOp), "routed_messages/op")
	b.ReportMetric(float64(aMisses)/ops, "a_l1_misses/op")
	b.ReportMetric(float64(aHits)/ops, "a_l1_hits/op")
	b.ReportMetric(float64(aMisses)/ops, "a_sublist_matches/op")
	b.ReportMetric(float64(bMisses)/ops, "b_l1_misses/op")
	b.ReportMetric(float64(bHits)/ops, "b_l1_hits/op")
	b.ReportMetric(float64(bMisses)/ops, "b_sublist_matches/op")
}
