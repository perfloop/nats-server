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

// BenchmarkPerAccountCacheSteadyRouteHit prices the unchanged-generation route
// cache-hit path. The fixture primes B before timing and verifies that the
// timed routes neither reload the shared sublist nor lose their delivery.
func BenchmarkPerAccountCacheSteadyRouteHit(b *testing.B) {
	f := newPerAccountCacheFixture(b)
	key := f.bKeys[0]
	f.requireCachedResult(b, key, f.b, f.bSubs[0])

	_, beforeMatches := f.matchCounts()
	beforeDeliveries := f.deliveries
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		f.route(key)
	}
	b.StopTimer()

	_, afterMatches := f.matchCounts()
	if got := afterMatches - beforeMatches; got != 0 {
		b.Fatalf("steady B cache-hit sublist matches: got %d, want 0", got)
	}
	if got, want := f.deliveries-beforeDeliveries, uint64(b.N); got != want {
		b.Fatalf("steady B routed-message deliveries: got %d, want %d", got, want)
	}
	ops := float64(b.N)
	b.ReportMetric(float64(afterMatches-beforeMatches)/ops, "sublist_matches/op")
	b.ReportMetric(float64(f.deliveries-beforeDeliveries)/ops, "routed_messages/op")
}
