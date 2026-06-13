// Mochi server: hole-punch (DCUtR) observability.
//
// Two NAT'd servers cannot dial each other directly; they meet over a
// circuit relay, then DCUtR (Direct Connection Upgrade through Relay)
// coordinates simultaneous dials to punch through both NATs and upgrade
// to a direct connection. This matters because relayed connections are
// deliberately throttled (the relay caps duration and bytes per
// circuit), so without a successful punch two NAT'd servers cannot hold
// a sustained connection — replication between two home servers would
// stall. Hole punching is enabled in net_start; this tracer makes its
// outcomes visible (logs plus success/failure counters for the status
// page), since otherwise it is enabled but entirely unobserved.
//
// Copyright Alistair Cunningham 2026

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
