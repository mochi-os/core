// Mochi server: Protocol 2 — operator-tunable settings.
//
// All /mochi/2 timing and buffering defaults live in the matching
// `*_default` constants in protocol2_sender.go / protocol2_worker.go.
// This file exposes those defaults as ini-tunable getters under the
// [peer] section, so an operator can adjust them via mochi.conf
// without recompiling. Names match the plan's "peer.*" namespace.
//
// Unset settings fall back to the compile-time default.
//
// Copyright Alistair Cunningham 2026

package main

// peer_window returns the per-peer inflight cap. Local memory bound on
// the Sender.inflight map; wire-level back-pressure rides on libp2p.
func peer_window() int { return ini_int("peer", "window", sender_window_default) }

// peer_outbox returns the per-Sender outbox channel depth.
func peer_outbox() int { return ini_int("peer", "outbox", sender_outbox_default) }

// peer_inflight_timeout returns the per-message ack timeout (seconds).
// Stale inflight entries past this are queue_failed and retried.
func peer_inflight_timeout() int {
	return ini_int("peer", "timeout", sender_inflight_timeout)
}

// peer_ping_interval_seconds is the idle-period before a Sender emits
// a ping frame. Active streams (any inbound frame within the period)
// don't ping — the receive-side activity resets the timer.
func peer_ping_interval_seconds() int {
	return ini_int("peer", "ping_interval", int(sender_ping_interval.Seconds()))
}

// peer_ping_timeout_seconds is the pong wait. No pong within this
// window → stream treated as dead and standard cleanup runs.
func peer_ping_timeout_seconds() int {
	return ini_int("peer", "ping_timeout", sender_ping_timeout)
}

// peer_worker_idle_seconds is the no-activity window after which an
// idle (user, app) worker is reaped. Active workers stay alive.
func peer_worker_idle_seconds() int {
	return ini_int("peer", "worker_idle", worker_idle_default)
}

// peer_worker_inbox is the per-(user, app) channel depth. Smaller =
// back-pressure propagates into libp2p faster; larger = more memory
// and more head-of-line tolerance.
func peer_worker_inbox() int {
	return ini_int("peer", "worker_inbox", worker_inbox_default)
}

// peer_rate is the per-Sender outbound message rate cap in
// messages/second. Default 0 (unlimited) per claude/plans/protocol2.md
// Decision points — operators set this only after observing a runaway
// producer, since the inflight cap (peer_window) is the natural
// back-pressure mechanism.
func peer_rate() int { return ini_int("peer", "rate", 0) }
