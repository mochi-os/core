// Mochi server: /_/admin/pipelining/status handler.
//
// Operator visibility into /mochi/2 transport state. Reports the open
// /mochi/2/messages Senders with their inflight depth and session, plus
// the per-host worker pool size. Used by `mochictl pipelining status`.
//
// Copyright © 2026 Mochi OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"net/http"
	"sort"

	"github.com/gin-gonic/gin"
)

// PipeliningPeer is one row of the per-peer summary.
type PipeliningPeer struct {
	Peer     string `json:"peer"`
	Sender   bool   `json:"sender"`   // true if a /mochi/2/messages Sender is open
	Inflight int    `json:"inflight"` // queued or awaiting ack on the open Sender
	Session  string `json:"session,omitempty"`
}

// PipeliningStatus is the response body.
type PipeliningStatus struct {
	Workers        int              `json:"workers"`
	WorkersPending int              `json:"workers_pending"`
	Senders        int              `json:"senders"`
	Peers          []PipeliningPeer `json:"peers"`
}

// admin_pipelining_status is GET /_/admin/pipelining/status.
func admin_pipelining_status(c *gin.Context) {
	workers, pending := worker_count()
	out := PipeliningStatus{
		Workers:        workers,
		WorkersPending: pending,
	}

	senders_lock.Lock()
	out.Senders = len(senders)
	for peer, s := range senders {
		p := PipeliningPeer{Peer: peer, Sender: true}
		s.lock.Lock()
		p.Inflight = len(s.inflight)
		s.lock.Unlock()
		p.Session = s.session
		out.Peers = append(out.Peers, p)
	}
	senders_lock.Unlock()

	sort.Slice(out.Peers, func(i, j int) bool { return out.Peers[i].Peer < out.Peers[j].Peer })

	c.JSON(http.StatusOK, out)
}
