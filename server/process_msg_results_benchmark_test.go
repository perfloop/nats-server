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
	"net"
	"testing"
)

func TestStreamImportMappedSubjectRepeatedDelivery(t *testing.T) {
	s, fooAcc, barAcc := simpleAccountServer(t)
	defer s.Shutdown()

	cfoo, _, _ := newClientForServer(s)
	defer cfoo.close()
	if err := cfoo.registerWithAccount(fooAcc); err != nil {
		t.Fatalf("Error registering client with foo account: %v", err)
	}

	cbar, crBar, _ := newClientForServer(s)
	defer cbar.close()
	if err := cbar.registerWithAccount(barAcc); err != nil {
		t.Fatalf("Error registering client with bar account: %v", err)
	}

	if err := fooAcc.AddStreamExport(">", []*Account{barAcc}); err != nil {
		t.Fatalf("Error adding stream export: %v", err)
	}
	if err := barAcc.AddStreamImport(fooAcc, "*", "pub.imports."); err != nil {
		t.Fatalf("Error adding stream import: %v", err)
	}

	cbar.parseAsync("SUB pub.imports.* 1\r\nPING\r\n")
	if _, err := crBar.ReadString('\n'); err != nil {
		t.Fatalf("Error waiting for subscription: %v", err)
	}

	for _, tc := range []struct {
		publish string
		subject string
		payload []byte
	}{
		{"PUB foo 5\r\nfirst\r\n", "pub.imports.foo", []byte("first\r\n")},
		{"PUB bar 6\r\nsecond\r\n", "pub.imports.bar", []byte("second\r\n")},
	} {
		cfoo.parseAsync(tc.publish)
		line, err := crBar.ReadString('\n')
		if err != nil {
			t.Fatalf("Error reading message header: %v", err)
		}
		matches := msgPat.FindAllStringSubmatch(line, -1)
		if len(matches) == 0 {
			t.Fatalf("No message received for %q", tc.subject)
		}
		if got := matches[0][SUB_INDEX]; got != tc.subject {
			t.Fatalf("Expected mapped subject %q, got %q", tc.subject, got)
		}
		checkPayload(crBar, tc.payload, t)
	}
}

func BenchmarkProcessMsgResultsStreamImport(b *testing.B) {
	targetConn, peerConn := net.Pipe()
	defer targetConn.Close()
	defer peerConn.Close()

	producer := &client{
		kind: CLIENT,
		msgb: [msgScratchSize]byte{'R', 'M', 'S', 'G', ' '},
		pcd:  make(map[*client]struct{}, 1),
		parseState: parseState{
			pa: pubArg{hdr: -1, size: 1, szb: []byte("1")},
		},
	}
	target := &client{
		kind: CLIENT,
		nc:   targetConn,
		out: outbound{
			nb: net.Buffers{nbPoolGet(nbPoolSizeSmall)},
			mp: 1 << 62,
		},
	}
	sub := &subscription{
		client: target,
		im:     &streamImport{to: "pub.imports.foo"},
		sid:    []byte("1"),
	}
	result := &SublistResult{psubs: []*subscription{sub}}
	messages := [2][]byte{[]byte("a\r\n"), []byte("b\r\n")}
	subject := []byte("foo")

	b.ReportAllocs()
	b.SetBytes(1)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		delivered, _ := producer.processMsgResults(nil, result, messages[i&1], nil, subject, nil, 0)
		if !delivered {
			b.Fatal("expected mapped stream import delivery")
		}
		if i+1 < b.N {
			target.out.nb[0] = target.out.nb[0][:0]
			target.out.pb = 0
		}
	}
	b.StopTimer()

	if !bytes.Contains(target.out.nb[0], []byte("MSG pub.imports.foo 1 1\r\n")) {
		b.Fatalf("Expected mapped message header, got %q", target.out.nb[0])
	}
}
