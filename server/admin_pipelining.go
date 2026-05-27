// Mochi server: /_/admin/pipelining/status handler.
//
// Operator visibility into /mochi/2 transport state. Reports per-peer
// protocol-support cache, active Senders + their inflight depth,
// per-host worker pool size, and pending replies. Used by
// `mochictl pipelining status` for both day-to-day observability and
// Phase 8 readiness ("can we deprecate /mochi/1?").
//
// Copyright Alistair Cunningham 2026

package main

import (
	"net/http"
	"sort"

	"github.com/gin-gonic/gin"
)

// PipeliningPeer is one row of the per-peer summary.
type PipeliningPeer struct {
	Peer     string `json:"peer"`
	Messages string `json:"messages"` // "supported" | "unsupported" | "unknown"
	Stream   string `json:"stream"`   // ditto
	Sender   bool   `json:"sender"`   // true if a /mochi/2/messages Sender is open
	Inflight int    `json:"inflight"` // queued or awaiting ack on the open Sender
	Session  string `json:"session,omitempty"`
}

// PipeliningStatus is the response body.
type PipeliningStatus struct {
	Workers          int              `json:"workers"`
	WorkersPending   int              `json:"workers_pending"`
	Senders          int              `json:"senders"`
	Peers            []PipeliningPeer `json:"peers"`
}

// admin_pipelining_status is GET /_/admin/pipelining/status.
func admin_pipelining_status(c *gin.Context) {
	workers, pending := worker_count()
	out := PipeliningStatus{
		Workers:        workers,
		WorkersPending: pending,
	}

	// Snapshot the per-peer state under the relevant locks. Each
	// snapshot is short — copy maps to local slices then release.
	peer_states := map[string]*PipeliningPeer{}

	protocol_known_lock.RLock()
	for peer, m := range protocol_known {
		p := peer_states[peer]
		if p == nil {
			p = &PipeliningPeer{Peer: peer}
			peer_states[peer] = p
		}
		p.Messages = protocol_state_string(m[protocol_messages])
		p.Stream = protocol_state_string(m[protocol_stream])
	}
	protocol_known_lock.RUnlock()

	senders_lock.Lock()
	out.Senders = len(senders)
	for peer, s := range senders {
		p := peer_states[peer]
		if p == nil {
			p = &PipeliningPeer{Peer: peer}
			peer_states[peer] = p
		}
		p.Sender = true
		s.lock.Lock()
		p.Inflight = len(s.inflight)
		s.lock.Unlock()
		p.Session = s.session
	}
	senders_lock.Unlock()

	for _, p := range peer_states {
		if p.Messages == "" {
			p.Messages = "unknown"
		}
		if p.Stream == "" {
			p.Stream = "unknown"
		}
		out.Peers = append(out.Peers, *p)
	}
	sort.Slice(out.Peers, func(i, j int) bool { return out.Peers[i].Peer < out.Peers[j].Peer })

	c.JSON(http.StatusOK, out)
}

func protocol_state_string(s protocol_state) string {
	switch s {
	case protocol_state_supported:
		return "supported"
	case protocol_state_unsupported:
		return "unsupported"
	}
	return "unknown"
}
