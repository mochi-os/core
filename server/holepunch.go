// Mochi server: hole-punch (DCUtR) observability.
//
// Two NAT'd servers meet over a circuit relay, then DCUtR coordinates
// simultaneous dials to upgrade to a direct connection. Relayed connections are
// throttled by duration and bytes, so without a successful punch two NAT'd
// servers cannot hold a sustained connection. Hole punching is enabled in
// net_start; this tracer logs its outcomes and counts them for the status page.
//
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the Mochi
// Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"sync/atomic"

	holepunch "github.com/libp2p/go-libp2p/p2p/protocol/holepunch"
)

var (
	holepunch_success atomic.Int64
	holepunch_failure atomic.Int64
)

// holepunch_tracer implements holepunch.EventTracer.
type holepunch_tracer struct{}

// Trace records a DCUtR event. The decisive signal is EndHolePunchEvt:
// Success means a direct connection was established through both NATs.
// A successful DirectDialEvt is not counted — it means the peer was
// directly reachable and no punch was needed.
func (holepunch_tracer) Trace(e *holepunch.Event) {
	switch v := e.Evt.(type) {
	case *holepunch.StartHolePunchEvt:
		debug("Hole punch starting to peer %q (round-trip %s)", e.Remote, v.RTT)
	case *holepunch.HolePunchAttemptEvt:
		debug("Hole punch attempt %d to peer %q", v.Attempt, e.Remote)
	case *holepunch.EndHolePunchEvt:
		if v.Success {
			holepunch_success.Add(1)
			info("Hole punch to peer %q succeeded in %s: direct connection established", e.Remote, v.EllapsedTime)
		} else {
			holepunch_failure.Add(1)
			info("Hole punch to peer %q failed after %s: %s", e.Remote, v.EllapsedTime, v.Error)
		}
	case *holepunch.DirectDialEvt:
		if !v.Success {
			debug("Direct dial to peer %q failed (%s); falling back to hole punch", e.Remote, v.Error)
		}
	case *holepunch.ProtocolErrorEvt:
		debug("Hole punch protocol error with peer %q: %s", e.Remote, v.Error)
	}
}
