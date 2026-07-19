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
	"time"

	"github.com/nats-io/nats.go"
)

func TestPerAccountRouteCacheRejectsRemovedAccountAfterOtherRefresh(t *testing.T) {
	const targetConfig = `
		server_name: "cache-target"
		port: -1
		accounts {
			A { users: [{user: "per-account-a", password: "pwd"}] }
			B { users: [{user: "per-account-b", password: "pwd"}] }
		}
		cluster {
			name: "per-account-cache-removal"
			listen: "127.0.0.1:-1"
		}
	`
	const targetConfigWithoutA = `
		server_name: "cache-target"
		port: -1
		accounts {
			B { users: [{user: "per-account-b", password: "pwd"}] }
		}
		cluster {
			name: "per-account-cache-removal"
			listen: "127.0.0.1:-1"
		}
	`

	conf := createConfFile(t, []byte(targetConfig))
	target, targetOpts := RunServerWithConfig(conf)
	defer target.Shutdown()

	sourceOpts := DefaultOptions()
	sourceOpts.Cluster.Name = targetOpts.Cluster.Name
	sourceOpts.Routes = RoutesFromStr(fmt.Sprintf("nats://127.0.0.1:%d", target.ClusterAddr().Port))
	addPerAccountCacheNetworkAccounts(sourceOpts)
	source := RunServer(sourceOpts)
	defer source.Shutdown()
	checkClusterFormed(t, source, target)

	inbound := getFirstRoute(target)
	if inbound == nil {
		t.Fatal("missing inbound route on target")
	}

	connect := func(s *Server, user string) *nats.Conn {
		t.Helper()
		nc, err := nats.Connect(s.ClientURL(), nats.UserInfo(user, perAccountCacheNetworkPassword))
		if err != nil {
			t.Fatalf("error connecting %q: %v", user, err)
		}
		t.Cleanup(nc.Close)
		return nc
	}
	sourceB := connect(source, perAccountCacheNetworkBUser)
	targetB := connect(target, perAccountCacheNetworkBUser)

	const (
		aSubject = "a.cache.removed"
		bSubject = "b.cache.refresh"
	)
	bSub := natsSubSync(t, targetB, bSubject)
	natsFlush(t, targetB)
	checkSubInterest(t, source, "B", bSubject, time.Second)

	// A delayed RMSG can still arrive after the target no longer advertises
	// interest. Prime that account's route L1 without a local A client, so the
	// removal path itself cannot change A's sublist generation via client cleanup.
	processRoutedCacheMessage(inbound, perAccountCacheLookup{
		account:  []byte("A"),
		subject:  []byte(aSubject),
		cacheKey: []byte("A " + aSubject),
	})
	natsPub(t, sourceB, bSubject, []byte("prime B"))
	natsFlush(t, sourceB)
	natsNexMsg(t, bSub, time.Second)

	cacheAKey := "A " + aSubject
	cacheBKey := "B " + bSubject
	checkFor(t, time.Second, 10*time.Millisecond, func() error {
		inbound.mu.Lock()
		_, hasA := inbound.in.pacache[cacheAKey]
		_, hasB := inbound.in.pacache[cacheBKey]
		inbound.mu.Unlock()
		if !hasA || !hasB {
			return fmt.Errorf("route cache not primed: A=%v B=%v", hasA, hasB)
		}
		return nil
	})

	oldA, err := target.LookupAccount("A")
	if err != nil {
		t.Fatalf("error looking up A before reload: %v", err)
	}
	reloadUpdateConfig(t, target, conf, targetConfigWithoutA)
	if _, err := target.LookupAccount("A"); err == nil {
		t.Fatal("removed account A remained registered after reload")
	}
	checkClusterFormed(t, source, target)

	bAccount, err := target.LookupAccount("B")
	if err != nil {
		t.Fatalf("error looking up B after reload: %v", err)
	}
	if err := bAccount.sl.Insert(&subscription{subject: []byte("b.cache.mutation")}); err != nil {
		t.Fatalf("error mutating B subscriptions: %v", err)
	}
	natsPub(t, sourceB, bSubject, []byte("refresh B"))
	natsFlush(t, sourceB)
	natsNexMsg(t, bSub, time.Second)

	inbound.mu.Lock()
	_, retainedA := inbound.in.pacache[cacheAKey]
	inbound.mu.Unlock()
	if !retainedA {
		t.Fatal("B refresh unexpectedly discarded the delayed A cache entry")
	}

	oldA.stats.Lock()
	beforeRouteMessages := oldA.stats.rt.inMsgs
	oldA.stats.Unlock()
	// Model a delayed RMSG received on the still-connected route after A was
	// removed and B refreshed its own stale entry.
	processRoutedCacheMessage(inbound, perAccountCacheLookup{
		account:  []byte("A"),
		subject:  []byte(aSubject),
		cacheKey: []byte(cacheAKey),
	})
	oldA.stats.Lock()
	afterRouteMessages := oldA.stats.rt.inMsgs
	oldA.stats.Unlock()
	if afterRouteMessages != beforeRouteMessages {
		t.Fatalf("removed A handled delayed route message: before=%d after=%d", beforeRouteMessages, afterRouteMessages)
	}
}

func TestPerAccountCacheRejectsExpiredAccountAfterOtherRefresh(t *testing.T) {
	c, accA, accB, aLookup, bLookups := newPerAccountCacheFixture(t, 1)
	primePerAccountCache(t, c, accA, accB, aLookup, bLookups)

	// Account-resolver removal marks the account expired while route connections
	// can retain their account-aware L1 entries. Refresh B after that removal,
	// then verify an old A entry cannot be used for a delayed message.
	removeCb(c.srv, accA.Name)
	if !accA.IsExpired() {
		t.Fatal("removed account A was not marked expired")
	}
	if err := accB.sl.Insert(&subscription{subject: []byte("b.cache.mutation")}); err != nil {
		t.Fatalf("error changing B subscriptions: %v", err)
	}
	if acc, results := lookupPerAccountCache(c, bLookups[0]); acc != accB || len(results.psubs) != 1 {
		t.Fatalf("stale B cache result: account=%p results=%d", acc, len(results.psubs))
	}
	if _, ok := c.in.pacache[string(aLookup.cacheKey)]; !ok {
		t.Fatal("B refresh unexpectedly discarded the expired A cache entry")
	}
	if acc, results := lookupPerAccountCache(c, aLookup); acc != nil || results != nil {
		t.Fatalf("expired A cache entry was reused: account=%p results=%#v", acc, results)
	}
}
