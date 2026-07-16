// mochictl: pipelining subcommands (operator visibility into /mochi/2).
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.
//
// `mochictl pipelining status` -> GET /_/admin/pipelining/status
//   Open /mochi/2/messages Senders + their inflight depth and session,
//   plus the per-host worker pool size. See claude/plans/protocol2.md.

package main

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
)

// cmd_pipelining_status handles `mochictl pipelining status`.
//
// With -j / -t the response is dumped raw for scripted consumption.
// Default output is a per-peer table plus a one-line summary of worker
// + sender counts.
func cmd_pipelining_status(args []string) error {
	path := "/_/admin/pipelining/status"
	if flag_json || flag_tabs {
		return get_dump(path, "peers")
	}

	resp, err := client().Get(path)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode/100 != 2 {
		return http_error(resp.StatusCode, body)
	}

	var payload struct {
		Workers        int `json:"workers"`
		WorkersPending int `json:"workers_pending"`
		Senders        int `json:"senders"`
		Peers          []struct {
			Peer     string `json:"peer"`
			Sender   bool   `json:"sender"`
			Inflight int    `json:"inflight"`
			Session  string `json:"session"`
		} `json:"peers"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		os.Stdout.Write(body)
		return nil
	}

	fmt.Printf("Workers: %d (%d pending frames)   Senders: %d\n",
		payload.Workers, payload.WorkersPending, payload.Senders)

	if len(payload.Peers) == 0 {
		fmt.Println("No peers seen — server hasn't talked to anyone over /mochi/2 yet.")
		return nil
	}

	peer_w := 4
	for _, p := range payload.Peers {
		if len(p.Peer) > peer_w {
			peer_w = len(p.Peer)
		}
	}
	if peer_w > 52 {
		peer_w = 52
	}
	fmt.Printf("\n%-*s  %-8s  %8s  %s\n",
		peer_w, "PEER", "SENDER", "INFLIGHT", "SESSION")
	for _, p := range payload.Peers {
		peer := p.Peer
		if len(peer) > peer_w {
			peer = peer[:peer_w-1] + "…"
		}
		sender := "no"
		if p.Sender {
			sender = "yes"
		}
		fmt.Printf("%-*s  %-8s  %8d  %s\n",
			peer_w, peer, sender, p.Inflight, p.Session)
	}
	return nil
}
