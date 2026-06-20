// Copyright © 2026 Mochi OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

// Mochi server: e.header("local") unit tests
//
// "local" must be true ONLY for an in-process self-loop stream (e.peer ==
// net_id) and false for any remote peer — it gates serving restricted apps to
// a host's own app-update loopback, so a remote peer must never read true.

package main

import (
	"testing"

	sl "go.starlark.net/starlark"
)

func event_header_local(t *testing.T, peer string) bool {
	t.Helper()
	e := &Event{peer: peer}
	v, err := e.sl_header(nil, sl.NewBuiltin("header", e.sl_header), sl.Tuple{sl.String("local")}, nil)
	if err != nil {
		t.Fatalf("sl_header(\"local\"): %v", err)
	}
	b, ok := v.(sl.Bool)
	if !ok {
		t.Fatalf("sl_header(\"local\") returned %T, want sl.Bool", v)
	}
	return bool(b)
}

func TestEventHeaderLocal(t *testing.T) {
	saved := net_id
	defer func() { net_id = saved }()

	net_id = "12D3KooWSelfPeerIdentityForTest"

	// In-process self-loop: peer == net_id -> local.
	if !event_header_local(t, net_id) {
		t.Errorf("self-loop (peer==net_id): local = false, want true")
	}

	// Remote peer: peer != net_id -> not local.
	if event_header_local(t, "12D3KooWRemotePeerIdentity") {
		t.Errorf("remote peer: local = true, want false")
	}

	// Empty peer (e.g. non-stream event) is not local.
	if event_header_local(t, "") {
		t.Errorf("empty peer: local = true, want false")
	}

	// Guard: when net_id is unset, nothing is local — even an empty peer must
	// not match an empty net_id.
	net_id = ""
	if event_header_local(t, "") {
		t.Errorf("empty net_id with empty peer: local = true, want false")
	}
}
