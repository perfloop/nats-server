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
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

// dropRouteWritesConn holds route-control traffic in one direction while
// allowing public routed messages from the remote server to keep flowing.
type dropRouteWritesConn struct {
	net.Conn
}

func (*dropRouteWritesConn) Write(p []byte) (int, error) {
	return len(p), nil
}

func TestRoutePerAccountCacheDropsRemovedAccountAfterOtherAccountRefresh(t *testing.T) {
	const configAWithBothAccounts = `
		server_name: "A"
		port: -1
		accounts {
			A { users: [{user: "a", password: "pwd"}] }
			B { users: [{user: "b", password: "pwd"}] }
		}
		cluster {
			name: "cache-reload"
			listen: "127.0.0.1:-1"
		}
	`
	const configAWithoutB = `
		server_name: "A"
		port: -1
		accounts {
			A { users: [{user: "a", password: "pwd"}] }
		}
		cluster {
			name: "cache-reload"
			listen: "127.0.0.1:-1"
		}
	`

	confA := createConfFile(t, []byte(configAWithBothAccounts))
	srvA, optsA := RunServerWithConfig(confA)
	defer srvA.Shutdown()

	confB := createConfFile(t, []byte(fmt.Sprintf(`
		server_name: "B"
		port: -1
		accounts {
			A { users: [{user: "a", password: "pwd"}] }
			B { users: [{user: "b", password: "pwd"}] }
		}
		cluster {
			name: "cache-reload"
			listen: "127.0.0.1:-1"
			routes: ["nats://127.0.0.1:%d"]
		}
	`, optsA.Cluster.Port)))
	srvB, _ := RunServerWithConfig(confB)
	defer srvB.Shutdown()

	checkClusterFormed(t, srvA, srvB)

	aSubConn := natsConnect(t, srvA.ClientURL(), nats.UserInfo("a", "pwd"))
	defer aSubConn.Close()
	bSubConn := natsConnect(t, srvA.ClientURL(), nats.UserInfo("b", "pwd"))
	defer bSubConn.Close()
	aSub := natsSubSync(t, aSubConn, "a")
	bSub := natsSubSync(t, bSubConn, "b")
	natsFlush(t, aSubConn)
	natsFlush(t, bSubConn)

	aPub := natsConnect(t, srvB.ClientURL(), nats.UserInfo("a", "pwd"))
	defer aPub.Close()
	bPub := natsConnect(t, srvB.ClientURL(), nats.UserInfo("b", "pwd"))
	defer bPub.Close()

	checkSubInterest(t, srvB, "A", "a", time.Second)
	checkSubInterest(t, srvB, "B", "b", time.Second)

	// Prime one non-account-bound route cache with entries for both accounts.
	natsPub(t, aPub, "a", []byte("prime-a"))
	natsFlush(t, aPub)
	natsNexMsg(t, aSub, time.Second)
	natsPub(t, bPub, "b", []byte("prime-b"))
	natsFlush(t, bPub)
	natsNexMsg(t, bSub, time.Second)

	const aCacheKey, bCacheKey = "A a", "B b"
	var route *client
	srvA.mu.RLock()
	srvA.forEachRoute(func(r *client) {
		if r.route != nil && r.route.remoteID == srvB.ID() && len(r.route.accName) == 0 &&
			r.in.pacache[aCacheKey] != nil && r.in.pacache[bCacheKey] != nil {
			route = r
		}
	})
	srvA.mu.RUnlock()
	if route == nil {
		t.Fatal("missing non-account-bound route cache primed for both accounts")
	}
	bPac := route.in.pacache[bCacheKey]

	// Delay route-control traffic from A to B so B retains its already-known
	// interest in b long enough to send a public routed message after A reloads.
	// The route cache under test is on A's inbound side, so B-to-A data still
	// exercises its normal route message path.
	srvA.mu.RLock()
	srvA.forEachRoute(func(r *client) {
		if r.route != nil && r.route.remoteID == srvB.ID() && len(r.route.accName) == 0 {
			r.mu.Lock()
			r.nc = &dropRouteWritesConn{Conn: r.nc}
			r.mu.Unlock()
		}
	})
	srvA.mu.RUnlock()

	// Remove B from the authoritative catalog before making A's cached result
	// stale. The following A message must not make B's old cache entry usable.
	reloadUpdateConfig(t, srvA, confA, configAWithoutB)
	if _, err := srvA.LookupAccount("B"); err == nil {
		t.Fatal("removed account B remained in the account catalog")
	}
	if currentBGen := atomic.LoadUint64(&bPac.acc.sl.genid); currentBGen <= bPac.genid {
		t.Fatalf("removed account B did not invalidate its cached result: cached=%d current=%d", bPac.genid, currentBGen)
	}

	aAcc, err := srvA.LookupAccount("A")
	if err != nil {
		t.Fatalf("lookup account A after reload: %v", err)
	}
	beforeGen := atomic.LoadUint64(&aAcc.sl.genid)
	aRefreshSub := natsSubSync(t, aSubConn, "a.refresh")
	defer aRefreshSub.Unsubscribe()
	natsFlush(t, aSubConn)
	afterGen := atomic.LoadUint64(&aAcc.sl.genid)
	if afterGen <= beforeGen {
		t.Fatalf("account A generation did not advance: before=%d after=%d", beforeGen, afterGen)
	}

	natsPub(t, aPub, "a", []byte("refresh-a"))
	natsFlush(t, aPub)
	natsNexMsg(t, aSub, time.Second)
	if pac := route.in.pacache[aCacheKey]; pac == nil || pac.genid != afterGen {
		t.Fatalf("account A cache entry was not refreshed to generation %d: %#v", afterGen, pac)
	}

	// B still has remote interest from the pre-reload subscription, so this
	// public publish reaches the same route. It must not reuse B's detached
	// Account/Sublist after the catalog has removed that account.
	beforeBRouteMessage := atomic.LoadInt64(&route.inMsgs)
	natsPub(t, bPub, "b", []byte("removed-b"))
	natsFlush(t, bPub)
	checkFor(t, time.Second, 15*time.Millisecond, func() error {
		if atomic.LoadInt64(&route.inMsgs) > beforeBRouteMessage {
			return nil
		}
		return fmt.Errorf("route did not receive the post-reload B message")
	})
	if msg, err := bSub.NextMsg(250 * time.Millisecond); err == nil {
		t.Fatalf("removed account B received routed message %q", msg.Data)
	}
	if _, ok := route.in.pacache[bCacheKey]; ok {
		t.Fatal("removed account B remained in the route per-account cache")
	}
}
