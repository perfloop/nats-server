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
	"bytes"
	"fmt"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

const (
	perAccountCacheNetworkAUser    = "per-account-a"
	perAccountCacheNetworkBUser    = "per-account-b"
	perAccountCacheNetworkPassword = "pwd"
	perAccountCacheNetworkReplays  = 4
	perAccountCacheNetworkFanout   = 4
)

var perAccountCacheNetworkPayload = bytes.Repeat([]byte("NATS route and gateway cache payload. "), 32)

type perAccountCacheNetworkFixture struct {
	source  *Server
	target  *Server
	inbound *client

	pubA     *nats.Conn
	pubB     *nats.Conn
	mutateA  *nats.Conn
	aReceive *nats.Conn
	bReceive *nats.Conn

	aAccount *Account
	bAccount *Account

	aSubject     string
	bLowSubject  string
	bHighSubject string
	bKeys        []string

	aDeliveries     atomic.Int64
	bLowDeliveries  atomic.Int64
	bHighDeliveries atomic.Int64
}

type perAccountCacheNetworkProbe struct {
	aL1StaleReloads     int
	aStaleEntries       int
	bL1Hits             int
	bL1Reloads          int
	bSublistMatches     int
	bSublistCacheHits   int
	bSublistCacheMisses int
	bLowDeliveries      int
	bHighDeliveries     int
}

func addPerAccountCacheNetworkAccounts(o *Options) {
	a := NewAccount("A")
	b := NewAccount("B")
	o.Accounts = []*Account{a, b}
	o.Users = []*User{
		{Username: perAccountCacheNetworkAUser, Password: perAccountCacheNetworkPassword, Account: a},
		{Username: perAccountCacheNetworkBUser, Password: perAccountCacheNetworkPassword, Account: b},
	}
}

func perAccountCacheNetworkConnect(tb testing.TB, s *Server, user string) *nats.Conn {
	tb.Helper()
	nc, err := nats.Connect(s.ClientURL(), nats.UserInfo(user, perAccountCacheNetworkPassword))
	if err != nil {
		tb.Fatalf("error connecting %q client: %v", user, err)
	}
	tb.Cleanup(nc.Close)
	return nc
}

func newPerAccountCacheRouteNetworkFixture(tb testing.TB) *perAccountCacheNetworkFixture {
	tb.Helper()

	sourceOpts := DefaultOptions()
	sourceOpts.NoSystemAccount = true
	sourceOpts.Trace = false
	sourceOpts.Debug = false
	addPerAccountCacheNetworkAccounts(sourceOpts)
	source := RunServer(sourceOpts)
	tb.Cleanup(source.Shutdown)

	targetOpts := DefaultOptions()
	targetOpts.NoSystemAccount = true
	targetOpts.Trace = false
	targetOpts.Debug = false
	targetOpts.Cluster.Name = sourceOpts.Cluster.Name
	targetOpts.Routes = RoutesFromStr(fmt.Sprintf("nats://127.0.0.1:%d", source.ClusterAddr().Port))
	addPerAccountCacheNetworkAccounts(targetOpts)
	target := RunServer(targetOpts)
	tb.Cleanup(target.Shutdown)
	checkClusterFormed(tb, source, target)

	inbound := getFirstRoute(target)
	if inbound == nil {
		tb.Fatal("missing route connection on target")
	}
	return newPerAccountCacheNetworkFixture(tb, source, target, inbound)
}

func newPerAccountCacheGatewayNetworkFixture(tb testing.TB) *perAccountCacheNetworkFixture {
	tb.Helper()

	targetOpts := testDefaultOptionsForGateway("B")
	targetOpts.Trace = false
	targetOpts.Debug = false
	addPerAccountCacheNetworkAccounts(targetOpts)
	target := runGatewayServer(targetOpts)
	tb.Cleanup(target.Shutdown)

	sourceOpts := testDefaultOptionsForGateway("A")
	gatewayURL, err := url.Parse(fmt.Sprintf("nats://127.0.0.1:%d", target.GatewayAddr().Port))
	if err != nil {
		tb.Fatalf("error parsing gateway URL: %v", err)
	}
	sourceOpts.Gateway.Gateways = []*RemoteGatewayOpts{{Name: "B", URLs: []*url.URL{gatewayURL}}}
	sourceOpts.Trace = false
	sourceOpts.Debug = false
	addPerAccountCacheNetworkAccounts(sourceOpts)
	source := runGatewayServer(sourceOpts)
	tb.Cleanup(source.Shutdown)
	waitForOutboundGatewaysForTB(tb, source, 1)
	waitForInboundGatewaysForTB(tb, target, 1)

	inbound := getInboundGatewayConnection(target, "A")
	if inbound == nil {
		tb.Fatal("missing inbound gateway connection on target")
	}
	return newPerAccountCacheNetworkFixture(tb, source, target, inbound)
}

func waitForOutboundGatewaysForTB(tb testing.TB, s *Server, expected int) {
	tb.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for s.numOutboundGateways() != expected {
		if time.Now().After(deadline) {
			tb.Fatalf("timed out waiting for %d outbound gateways", expected)
		}
		time.Sleep(time.Millisecond)
	}
}

func waitForInboundGatewaysForTB(tb testing.TB, s *Server, expected int) {
	tb.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for s.numInboundGateways() != expected {
		if time.Now().After(deadline) {
			tb.Fatalf("timed out waiting for %d inbound gateways", expected)
		}
		time.Sleep(time.Millisecond)
	}
}

func newPerAccountCacheNetworkFixture(tb testing.TB, source, target *Server, inbound *client) *perAccountCacheNetworkFixture {
	tb.Helper()

	f := &perAccountCacheNetworkFixture{
		source:       source,
		target:       target,
		inbound:      inbound,
		pubA:         perAccountCacheNetworkConnect(tb, source, perAccountCacheNetworkAUser),
		pubB:         perAccountCacheNetworkConnect(tb, source, perAccountCacheNetworkBUser),
		mutateA:      perAccountCacheNetworkConnect(tb, target, perAccountCacheNetworkAUser),
		aReceive:     perAccountCacheNetworkConnect(tb, target, perAccountCacheNetworkAUser),
		bReceive:     perAccountCacheNetworkConnect(tb, target, perAccountCacheNetworkBUser),
		aSubject:     "a.cache.payload",
		bLowSubject:  "b.cache.low",
		bHighSubject: "b.cache.high",
	}
	var err error
	if f.aAccount, err = target.LookupAccount("A"); err != nil {
		tb.Fatalf("error looking up target account A: %v", err)
	}
	if f.bAccount, err = target.LookupAccount("B"); err != nil {
		tb.Fatalf("error looking up target account B: %v", err)
	}
	f.bKeys = []string{"B " + f.bLowSubject, "B " + f.bHighSubject}

	if _, err := f.aReceive.Subscribe(f.aSubject, func(*nats.Msg) {
		f.aDeliveries.Add(1)
	}); err != nil {
		tb.Fatalf("error subscribing to A subject: %v", err)
	}
	if _, err := f.bReceive.Subscribe(f.bLowSubject, func(*nats.Msg) {
		f.bLowDeliveries.Add(1)
	}); err != nil {
		tb.Fatalf("error subscribing to low-fanout B subject: %v", err)
	}
	for i := 0; i < perAccountCacheNetworkFanout; i++ {
		if _, err := f.bReceive.Subscribe(f.bHighSubject, func(*nats.Msg) {
			f.bHighDeliveries.Add(1)
		}); err != nil {
			tb.Fatalf("error subscribing to high-fanout B subject: %v", err)
		}
	}
	if _, err := f.bReceive.QueueSubscribe(f.bHighSubject, "per-account-cache", func(*nats.Msg) {
		f.bHighDeliveries.Add(1)
	}); err != nil {
		tb.Fatalf("error queue-subscribing to high-fanout B subject: %v", err)
	}
	if err := f.aReceive.Flush(); err != nil {
		tb.Fatalf("error flushing A subscriptions: %v", err)
	}
	if err := f.bReceive.Flush(); err != nil {
		tb.Fatalf("error flushing B subscriptions: %v", err)
	}
	waitForPerAccountCacheNetworkInterest(tb, source, "A", f.aSubject)
	waitForPerAccountCacheNetworkInterest(tb, source, "B", f.bLowSubject)
	waitForPerAccountCacheNetworkInterest(tb, source, "B", f.bHighSubject)
	return f
}

func waitForPerAccountCacheNetworkInterest(tb testing.TB, s *Server, account, subject string) {
	tb.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		if gwc := s.getOutboundGatewayConnection("B"); gwc != nil {
			if outsiei, ok := gwc.gw.outsim.Load(account); ok && outsiei != nil {
				outsie := outsiei.(*outsie)
				r := outsie.sl.Match(subject)
				if len(r.psubs) > 0 || len(r.qsubs) > 0 {
					return
				}
			}
		} else if acc, err := s.LookupAccount(account); err == nil && acc.SubscriptionInterest(subject) {
			return
		}
		if time.Now().After(deadline) {
			tb.Fatalf("timed out waiting for interest in account %q on %q", account, subject)
		}
		time.Sleep(time.Millisecond)
	}
}

func (f *perAccountCacheNetworkFixture) waitFor(tb testing.TB, counter *atomic.Int64, expected int64, description string) {
	tb.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for counter.Load() < expected {
		if time.Now().After(deadline) {
			tb.Fatalf("timed out waiting for %s: got %d, expected %d", description, counter.Load(), expected)
		}
		time.Sleep(time.Millisecond)
	}
}

func (f *perAccountCacheNetworkFixture) publishA(tb testing.TB) {
	tb.Helper()
	before := f.aDeliveries.Load()
	if err := f.pubA.Publish(f.aSubject, perAccountCacheNetworkPayload); err != nil {
		tb.Fatalf("error publishing A message: %v", err)
	}
	if err := f.pubA.Flush(); err != nil {
		tb.Fatalf("error flushing A message: %v", err)
	}
	f.waitFor(tb, &f.aDeliveries, before+1, "A delivery")
}

func (f *perAccountCacheNetworkFixture) publishB(tb testing.TB) (lowDeliveries, highDeliveries int) {
	tb.Helper()
	beforeLow := f.bLowDeliveries.Load()
	beforeHigh := f.bHighDeliveries.Load()
	for i := 0; i < perAccountCacheNetworkReplays; i++ {
		if err := f.pubB.Publish(f.bLowSubject, perAccountCacheNetworkPayload); err != nil {
			tb.Fatalf("error publishing low-fanout B message: %v", err)
		}
		if err := f.pubB.Publish(f.bHighSubject, perAccountCacheNetworkPayload); err != nil {
			tb.Fatalf("error publishing high-fanout B message: %v", err)
		}
	}
	if err := f.pubB.Flush(); err != nil {
		tb.Fatalf("error flushing B messages: %v", err)
	}
	f.waitFor(tb, &f.bLowDeliveries, beforeLow+perAccountCacheNetworkReplays, "low-fanout B deliveries")
	f.waitFor(tb, &f.bHighDeliveries, beforeHigh+int64(perAccountCacheNetworkReplays*(perAccountCacheNetworkFanout+1)), "high-fanout B deliveries")
	return int(f.bLowDeliveries.Load() - beforeLow), int(f.bHighDeliveries.Load() - beforeHigh)
}

func (f *perAccountCacheNetworkFixture) addAMutation(tb testing.TB) *nats.Subscription {
	tb.Helper()
	sub, err := f.mutateA.SubscribeSync("a.cache.mutation")
	if err != nil {
		tb.Fatalf("error adding A mutation subscription: %v", err)
	}
	if err := f.mutateA.Flush(); err != nil {
		tb.Fatalf("error flushing A mutation subscription: %v", err)
	}
	return sub
}

func (f *perAccountCacheNetworkFixture) removeAMutation(tb testing.TB, sub *nats.Subscription) {
	tb.Helper()
	if err := sub.Unsubscribe(); err != nil {
		tb.Fatalf("error removing A mutation subscription: %v", err)
	}
	if err := f.mutateA.Flush(); err != nil {
		tb.Fatalf("error flushing A mutation removal: %v", err)
	}
}

func (f *perAccountCacheNetworkFixture) cacheState() (hits, reloads int) {
	f.inbound.mu.Lock()
	defer f.inbound.mu.Unlock()
	for _, key := range f.bKeys {
		if _, ok := f.inbound.in.pacache[key]; ok {
			hits++
		} else {
			reloads++
		}
	}
	return hits, reloads
}

func (f *perAccountCacheNetworkFixture) countStaleAEntries() int {
	f.inbound.mu.Lock()
	defer f.inbound.mu.Unlock()
	generation := atomic.LoadUint64(&f.aAccount.sl.genid)
	stale := 0
	for key, pac := range f.inbound.in.pacache {
		if len(key) >= 2 && key[0] == 'A' && key[1] == ' ' && pac.genid != generation {
			stale++
		}
	}
	return stale
}

func (f *perAccountCacheNetworkFixture) prime(tb testing.TB) {
	tb.Helper()
	f.publishA(tb)
	_, _ = f.publishB(tb)
	if hits, reloads := f.cacheState(); hits != len(f.bKeys) || reloads != 0 {
		tb.Fatalf("failed to prime B route cache: hits=%d reloads=%d", hits, reloads)
	}
}

func (f *perAccountCacheNetworkFixture) probe(tb testing.TB) perAccountCacheNetworkProbe {
	tb.Helper()
	f.prime(tb)
	beforeAMatches := atomic.LoadUint64(&f.aAccount.sl.matches)
	beforeBMatches := atomic.LoadUint64(&f.bAccount.sl.matches)
	beforeBCacheHits := atomic.LoadUint64(&f.bAccount.sl.cacheHits)

	mutation := f.addAMutation(tb)
	staleEntries := f.countStaleAEntries()
	if staleEntries < 1 {
		tb.Fatal("A mutation did not make the inbound route cache stale")
	}
	f.publishA(tb)
	probe := perAccountCacheNetworkProbe{
		aL1StaleReloads: int(atomic.LoadUint64(&f.aAccount.sl.matches) - beforeAMatches),
		aStaleEntries:   staleEntries,
	}
	probe.bL1Hits, probe.bL1Reloads = f.cacheState()
	probe.bLowDeliveries, probe.bHighDeliveries = f.publishB(tb)
	probe.bSublistMatches = int(atomic.LoadUint64(&f.bAccount.sl.matches) - beforeBMatches)
	probe.bSublistCacheHits = int(atomic.LoadUint64(&f.bAccount.sl.cacheHits) - beforeBCacheHits)
	probe.bSublistCacheMisses = probe.bSublistMatches - probe.bSublistCacheHits
	f.removeAMutation(tb, mutation)
	return probe
}

func TestPerAccountCacheNetworkMixedFanoutDelivery(t *testing.T) {
	for _, test := range []struct {
		name       string
		newFixture func(testing.TB) *perAccountCacheNetworkFixture
	}{
		{"route", newPerAccountCacheRouteNetworkFixture},
		{"gateway", newPerAccountCacheGatewayNetworkFixture},
	} {
		t.Run(test.name, func(t *testing.T) {
			probe := test.newFixture(t).probe(t)
			if probe.aL1StaleReloads != 1 || probe.aStaleEntries < 1 {
				t.Fatalf("unexpected stale A refresh: %+v", probe)
			}
			if probe.bL1Hits+probe.bL1Reloads != 2 {
				t.Fatalf("unexpected B L1 state: %+v", probe)
			}
			if probe.bSublistMatches != probe.bSublistCacheHits+probe.bSublistCacheMisses || probe.bSublistCacheMisses != 0 {
				t.Fatalf("unexpected B Sublist cache state: %+v", probe)
			}
			if probe.bLowDeliveries != perAccountCacheNetworkReplays || probe.bHighDeliveries != perAccountCacheNetworkReplays*(perAccountCacheNetworkFanout+1) {
				t.Fatalf("unexpected B delivery fanout: %+v", probe)
			}
		})
	}
}

func benchmarkPerAccountCacheNetworkMixedFanout(b *testing.B, f *perAccountCacheNetworkFixture, prefix string) {
	probe := f.probe(b)
	if probe.aL1StaleReloads != 1 || probe.aStaleEntries < 1 || probe.bL1Hits+probe.bL1Reloads != len(f.bKeys) || probe.bLowDeliveries != perAccountCacheNetworkReplays || probe.bHighDeliveries != perAccountCacheNetworkReplays*(perAccountCacheNetworkFanout+1) {
		b.Fatalf("unexpected %s cache probe: %+v", prefix, probe)
	}

	b.ReportAllocs()
	for b.Loop() {
		b.StopTimer()
		mutation := f.addAMutation(b)
		b.StartTimer()
		f.publishA(b)
		_, _ = f.publishB(b)
		b.StopTimer()
		f.removeAMutation(b, mutation)
		b.StartTimer()
	}
	b.StopTimer()
	b.ReportMetric(float64(probe.aL1StaleReloads), prefix+"_a_l1_stale_reloads/stale_refresh")
	b.ReportMetric(float64(probe.aStaleEntries), prefix+"_a_stale_entries/stale_refresh")
	b.ReportMetric(float64(probe.bL1Hits), prefix+"_b_l1_hits/stale_refresh")
	b.ReportMetric(float64(probe.bL1Reloads), prefix+"_b_l1_reloads/stale_refresh")
	b.ReportMetric(float64(probe.bSublistMatches), prefix+"_b_sublist_matches/stale_refresh")
	b.ReportMetric(float64(probe.bSublistCacheHits), prefix+"_b_sublist_cache_hits/stale_refresh")
	b.ReportMetric(float64(probe.bSublistCacheMisses), prefix+"_b_sublist_cache_misses/stale_refresh")
	b.ReportMetric(float64(probe.bLowDeliveries), prefix+"_b_low_fanout_deliveries/stale_refresh")
	b.ReportMetric(float64(probe.bHighDeliveries), prefix+"_b_high_fanout_deliveries/stale_refresh")
	b.ReportMetric(float64(perAccountCacheNetworkReplays), prefix+"_b_replays_per_key/stale_refresh")
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N), prefix+"_end_to_end_mixed_fanout_ns/op")
}

func BenchmarkPerAccountCacheRouteEndToEndMixedFanout(b *testing.B) {
	benchmarkPerAccountCacheNetworkMixedFanout(b, newPerAccountCacheRouteNetworkFixture(b), "route")
}

func BenchmarkPerAccountCacheGatewayEndToEndMixedFanout(b *testing.B) {
	benchmarkPerAccountCacheNetworkMixedFanout(b, newPerAccountCacheGatewayNetworkFixture(b), "gateway")
}
