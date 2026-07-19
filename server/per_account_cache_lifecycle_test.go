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

const (
	perAccountCacheLifecycleAUser    = "per-account-a"
	perAccountCacheLifecycleBUser    = "per-account-b"
	perAccountCacheLifecyclePassword = "pwd"
)

var perAccountCacheLifecycleRouteMessage = []byte("\r\n")

// processPerAccountCacheLifecycleRoutedMessage supplies the cache fields that
// route parsing has already decoded before processInboundRoutedMsg runs. This
// models a delayed RMSG without depending on a peer that has correctly removed
// interest for a deleted account.
func processPerAccountCacheLifecycleRoutedMessage(c *client, account, subject string) {
	c.pa.account = []byte(account)
	c.pa.subject = []byte(subject)
	if c.kind == ROUTER && c.route != nil && len(c.route.accName) > 0 {
		c.pa.pacache = []byte(subject)
	} else {
		c.pa.pacache = []byte(account + " " + subject)
	}
	c.processInboundRoutedMsg(perAccountCacheLifecycleRouteMessage)
}

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
	sourceA := NewAccount("A")
	sourceB := NewAccount("B")
	sourceOpts.Accounts = []*Account{sourceA, sourceB}
	sourceOpts.Users = []*User{
		{Username: perAccountCacheLifecycleAUser, Password: perAccountCacheLifecyclePassword, Account: sourceA},
		{Username: perAccountCacheLifecycleBUser, Password: perAccountCacheLifecyclePassword, Account: sourceB},
	}
	source := RunServer(sourceOpts)
	defer source.Shutdown()
	checkClusterFormed(t, source, target)

	inbound := getFirstRoute(target)
	if inbound == nil {
		t.Fatal("missing inbound route on target")
	}

	connect := func(s *Server, user string) *nats.Conn {
		t.Helper()
		nc, err := nats.Connect(s.ClientURL(), nats.UserInfo(user, perAccountCacheLifecyclePassword))
		if err != nil {
			t.Fatalf("error connecting %q: %v", user, err)
		}
		t.Cleanup(nc.Close)
		return nc
	}
	sourceBConnection := connect(source, perAccountCacheLifecycleBUser)
	targetB := connect(target, perAccountCacheLifecycleBUser)

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
	processPerAccountCacheLifecycleRoutedMessage(inbound, "A", aSubject)
	natsPub(t, sourceBConnection, bSubject, []byte("prime B"))
	natsFlush(t, sourceBConnection)
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
	natsPub(t, sourceBConnection, bSubject, []byte("refresh B"))
	natsFlush(t, sourceBConnection)
	natsNexMsg(t, bSub, time.Second)

	inbound.mu.Lock()
	_, retainedA := inbound.in.pacache[cacheAKey]
	_, retainedB := inbound.in.pacache[cacheBKey]
	inbound.mu.Unlock()
	if !retainedA {
		t.Fatal("B refresh unexpectedly discarded the delayed A cache entry")
	}
	if !retainedB {
		t.Fatal("B refresh unexpectedly discarded B's cache entry")
	}

	oldA.stats.Lock()
	beforeRouteMessages := oldA.stats.rt.inMsgs
	oldA.stats.Unlock()
	// Model a delayed RMSG received on the still-connected route after A was
	// removed and B refreshed its own stale entry.
	processPerAccountCacheLifecycleRoutedMessage(inbound, "A", aSubject)
	oldA.stats.Lock()
	afterRouteMessages := oldA.stats.rt.inMsgs
	oldA.stats.Unlock()
	if afterRouteMessages != beforeRouteMessages {
		t.Fatalf("removed A handled delayed route message: before=%d after=%d", beforeRouteMessages, afterRouteMessages)
	}

	inbound.mu.Lock()
	_, retainedA = inbound.in.pacache[cacheAKey]
	_, retainedB = inbound.in.pacache[cacheBKey]
	inbound.mu.Unlock()
	if retainedA {
		t.Fatal("removed A cache entry survived its failed delayed lookup")
	}
	if !retainedB {
		t.Fatal("removing A's cache entry discarded B's cache entry")
	}
	natsPub(t, sourceBConnection, bSubject, []byte("replay B"))
	natsFlush(t, sourceBConnection)
	natsNexMsg(t, bSub, time.Second)
}

func TestPerAccountCacheRejectsExpiredDedicatedRouteAccount(t *testing.T) {
	const targetConfig = `
		server_name: "cache-dedicated-target"
		port: -1
		accounts {
			A { users: [{user: "per-account-a", password: "pwd"}] }
		}
		cluster {
			name: "per-account-cache-dedicated-removal"
			listen: "127.0.0.1:-1"
			accounts: ["A"]
		}
	`

	conf := createConfFile(t, []byte(targetConfig))
	target, targetOpts := RunServerWithConfig(conf)
	defer target.Shutdown()

	sourceConfig := fmt.Sprintf(`
		server_name: "cache-dedicated-source"
		port: -1
		accounts {
			A { users: [{user: "per-account-a", password: "pwd"}] }
		}
		cluster {
			name: "per-account-cache-dedicated-removal"
			listen: "127.0.0.1:-1"
			routes: ["nats://127.0.0.1:%d"]
			accounts: ["A"]
		}
	`, targetOpts.Cluster.Port)
	source, _ := RunServerWithConfig(createConfFile(t, []byte(sourceConfig)))
	defer source.Shutdown()
	checkClusterFormed(t, source, target)

	var inbound *client
	checkFor(t, time.Second, 10*time.Millisecond, func() error {
		target.mu.RLock()
		for _, r := range target.accRoutes["A"] {
			inbound = r
			break
		}
		target.mu.RUnlock()
		if inbound == nil {
			return fmt.Errorf("missing dedicated route for A")
		}
		inbound.mu.Lock()
		accountName := string(inbound.route.accName)
		account := inbound.acc
		inbound.mu.Unlock()
		if accountName != "A" || account == nil {
			return fmt.Errorf("dedicated route is not bound to A")
		}
		return nil
	})

	const subject = "a.cache.dedicated.removed"
	processPerAccountCacheLifecycleRoutedMessage(inbound, "A", subject)
	inbound.mu.Lock()
	cached := inbound.in.pacache[subject]
	inbound.mu.Unlock()
	if cached == nil {
		t.Fatal("dedicated route did not prime its account cache")
	}

	acc, err := target.LookupAccount("A")
	if err != nil {
		t.Fatalf("error looking up A before removal: %v", err)
	}
	removeCb(target, acc.Name)
	if !acc.IsExpired() {
		t.Fatal("removed dedicated-route account was not marked expired")
	}

	acc.stats.Lock()
	beforeRouteMessages := acc.stats.rt.inMsgs
	acc.stats.Unlock()
	// The dedicated route retains c.acc directly, so this is the branch that
	// must reject a delayed RMSG after resolver removal.
	processPerAccountCacheLifecycleRoutedMessage(inbound, "A", subject)
	acc.stats.Lock()
	afterRouteMessages := acc.stats.rt.inMsgs
	acc.stats.Unlock()
	if afterRouteMessages != beforeRouteMessages {
		t.Fatalf("expired dedicated-route account handled delayed message: before=%d after=%d", beforeRouteMessages, afterRouteMessages)
	}
	inbound.mu.Lock()
	cached = inbound.in.pacache[subject]
	inbound.mu.Unlock()
	if cached != nil {
		t.Fatal("expired dedicated-route cache entry survived its failed lookup")
	}
}

func TestPerAccountCacheRejectsExpiredAccountAfterOtherRefresh(t *testing.T) {
	const (
		aSubject = "a.cache.expired"
		bSubject = "b.cache.refresh"
	)

	srv := &Server{}
	accA := NewAccount("A")
	accB := NewAccount("B")
	accA.sl = NewSublistWithCache()
	accB.sl = NewSublistWithCache()
	srv.accounts.Store(accA.Name, accA)
	srv.accounts.Store(accB.Name, accB)
	if err := accA.sl.Insert(&subscription{subject: []byte(aSubject)}); err != nil {
		t.Fatalf("error adding A subscription: %v", err)
	}
	if err := accB.sl.Insert(&subscription{subject: []byte(bSubject)}); err != nil {
		t.Fatalf("error adding B subscription: %v", err)
	}

	c := &client{
		kind:  ROUTER,
		srv:   srv,
		route: &route{},
		in: readCache{
			pacache: make(map[string]*perAccountCache, 2),
		},
	}
	lookup := func(account, subject string) (*Account, *SublistResult) {
		c.pa.account = []byte(account)
		c.pa.subject = []byte(subject)
		c.pa.pacache = []byte(account + " " + subject)
		return c.getAccAndResultFromCache()
	}

	if acc, results := lookup("A", aSubject); acc != accA || len(results.psubs) != 1 {
		t.Fatalf("unexpected A cache result: account=%p results=%d", acc, len(results.psubs))
	}
	if acc, results := lookup("B", bSubject); acc != accB || len(results.psubs) != 1 {
		t.Fatalf("unexpected B cache result: account=%p results=%d", acc, len(results.psubs))
	}

	// Account-resolver removal marks the account expired while route connections
	// can retain their account-aware L1 entries. Refresh B after that removal,
	// then verify an old A entry cannot be used for a delayed message.
	removeCb(srv, accA.Name)
	if !accA.IsExpired() {
		t.Fatal("removed account A was not marked expired")
	}
	if err := accB.sl.Insert(&subscription{subject: []byte("b.cache.mutation")}); err != nil {
		t.Fatalf("error changing B subscriptions: %v", err)
	}
	if acc, results := lookup("B", bSubject); acc != accB || len(results.psubs) != 1 {
		t.Fatalf("stale B cache result: account=%p results=%d", acc, len(results.psubs))
	}

	if acc, results := lookup("A", aSubject); acc != nil || results != nil {
		t.Fatalf("expired A cache entry was reused: account=%p results=%#v", acc, results)
	}
	if _, ok := c.in.pacache["A "+aSubject]; ok {
		t.Fatal("expired A cache entry survived its failed lookup")
	}
	if _, ok := c.in.pacache["B "+bSubject]; !ok {
		t.Fatal("removing A's cache entry discarded B's cache entry")
	}
	if acc, results := lookup("B", bSubject); acc != accB || len(results.psubs) != 1 {
		t.Fatalf("B cache entry was not routable after removing A: account=%p results=%d", acc, len(results.psubs))
	}
}
