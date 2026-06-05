// Mochi server: Peer send-progress (app-level "stalled") cache.
//
// peer_reachability.go tracks libp2p CONNECT failures. This tracks the
// distinct failure mode that cache deliberately ignores ("the peer IS
// reachable; the failure is application- or protocol-level, handled
// separately"): a peer whose /mochi/2/messages stream OPENS fine but
// whose inflight frames repeatedly time out without an ack — e.g. a wiped
// or unbootstrapped replica that receives ops but can never apply them,
// so it never acks.
//
// Without this, queue_process keeps surfacing that target's backlog every
// tick, the manager re-enters immediately (queue.go drain loop), and
// queue_select re-scans the whole pending set each pass — the 2026-06-02
// incident where 90k undeliverable replication rows to a wiped mochi2
// pinned a core at ~87%.
//
// When a target crosses the threshold it is "stalled" for
// peer_stall_window; queue_process defers its entire backlog in one shot
// (queue_defer_target) so queue_select stops scanning it. After the
// window the deferred rows come due again and get one trial send: an ack
// clears the stall and resurrects the backlog; another timeout re-stalls.
// A trial is a cheap frame send (the stream is already open), so unlike
// the connect-level silent cache there is no libp2p-dial cost to a
// periodic trial — the time-windowed model the silent cache rejects is
// safe here.
//
// Signals, fed from the per-peer Sender where the peer is known:
//   - ack frame resolved an inflight entry  -> peer_mark_progress
//   - inflight entry timed out without ack   -> peer_mark_no_progress
//
// Copyright Alistair Cunningham 2026

package main

import "sync"

const (
	// Consecutive sweep-observed inflight timeouts (no intervening ack)
	// before a target is treated as stalled. Mirrors
	// peer_silent_failure_threshold; conservative so a single slow ack
	// doesn't stall a working peer.
	peer_stall_threshold = 3

	// How long a stalled target's backlog is deferred before the next
	// trial send. A genuinely-dead replica retries at most this often
	// (no spin); a recovered one resumes on the next trial.
	peer_stall_window = 3600 // 1 hour
)

type PeerProgress struct {
	Timeouts     int
	StalledUntil int64
}

var (
	peer_progress      = map[string]PeerProgress{}
	peer_progress_lock = &sync.Mutex{}
)

// peer_is_stalled reports whether sends to this peer are timing out
// without acks and the trial window hasn't reopened yet. Bootstrap and
// self are never stalled.
func peer_is_stalled(id string) bool {
	if id == "" || id == net_id || peer_is_bootstrap(id) {
		return false
	}
	peer_progress_lock.Lock()
	defer peer_progress_lock.Unlock()
	p, ok := peer_progress[id]
	return ok && p.Timeouts >= peer_stall_threshold && now() < p.StalledUntil
}

// peer_stall_until returns the time a stalled target's backlog should be
// deferred to (its current trial-window end), or 0 if not stalled.
func peer_stall_until(id string) int64 {
	peer_progress_lock.Lock()
	defer peer_progress_lock.Unlock()
	p, ok := peer_progress[id]
	if !ok || p.Timeouts < peer_stall_threshold {
		return 0
	}
	return p.StalledUntil
}

// peer_mark_progress clears any stall — an ack arrived, so the peer is
// applying and acking. Resurrects the deferred backlog only on the
// stalled->recovered transition (a cheap no-op for the common,
// never-stalled peer). Called per ack frame from the Sender read loop.
func peer_mark_progress(id string) {
	if id == "" || id == net_id {
		return
	}
	peer_progress_lock.Lock()
	p, ok := peer_progress[id]
	stalled := ok && p.Timeouts >= peer_stall_threshold
	if ok {
		delete(peer_progress, id)
	}
	peer_progress_lock.Unlock()
	// The peer just acked, so it is reachable again: clear the offline mark.
	replication_member_reachable(id)
	if stalled {
		queue_resurrect_peer(id)
	}
}

// peer_mark_no_progress records that an inflight frame to this peer timed
// out without an ack. On crossing the threshold it opens a stall window.
// Called once per sweep per peer that had stale inflight.
func peer_mark_no_progress(id string) {
	if id == "" || id == net_id || peer_is_bootstrap(id) {
		return
	}
	peer_progress_lock.Lock()
	p := peer_progress[id]
	p.Timeouts++
	if p.Timeouts >= peer_stall_threshold {
		p.StalledUntil = now() + peer_stall_window
	}
	crossed := p.Timeouts == peer_stall_threshold
	peer_progress[id] = p
	peer_progress_lock.Unlock()
	// On the first crossing into stalled, stamp the member as unreachable: a
	// replication member that connects but never acks (wiped replica). An ack
	// clears it.
	if crossed {
		replication_member_unreachable(id)
	}
}
