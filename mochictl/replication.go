// mochictl: replication subcommands (operator inspection / mgmt).
// Copyright © 2026 Mochi OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.
//
// Sibling to replica.go. The `mochictl replica` family is for the
// joining replica's perspective (join / leave / status of THIS server's
// pair attempt). The `mochictl replication` family below is for ongoing
// operator inspection / management of the pair set as it exists right
// now — list members, kick a member, summary view.
//
// `mochictl replication status`        → GET /_/admin/replication/status
// `mochictl replication pair list`     → GET /_/admin/replication/pair
// `mochictl replication pair remove`   → POST /_/admin/replication/pair/remove
// `mochictl replication resync`        → POST /_/admin/replication/resync
// `mochictl replication resume`        → POST /_/admin/replication/resume
// `mochictl replication backfill`      → POST /_/admin/replication/backfill

package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
)

// cmd_replication_resync handles `mochictl replication resync <peer-id>`.
// POSTs the peer-id to the admin endpoint; the server seeds bootstrap
// rows for every scope and re-emits manifest-requests, forcing a
// re-walk that fetches anything missing on the local side. Idempotent
// — files whose local copy already matches by size + sha256 are
// skipped at manifest-diff time.
//
// Usage:
//   mochictl replication resync <peer-id>
func cmd_replication_resync(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: mochictl replication resync <peer-id>")
	}
	peer := args[0]

	body, err := json.Marshal(map[string]string{"peer": peer})
	if err != nil {
		return err
	}

	resp, err := client().Post("/_/admin/replication/resync", "application/json", bytes.NewReader(body))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode/100 != 2 {
		return http_error(resp.StatusCode, raw)
	}

	if flag_json {
		fmt.Println(string(raw))
		return nil
	}
	if !flag_verbose {
		return nil
	}
	fmt.Printf("Re-bootstrap initiated against peer %s. Track progress with 'mochictl replication status'.\n", peer)
	return nil
}

// cmd_replication_resume handles `mochictl replication resume <peer-id>`.
// POSTs the peer-id to the admin endpoint, which re-drives every scope
// that hasn't finished bootstrapping from that peer. Unlike `resync`,
// this is safe on a populated, running server — it only re-fires
// not-yet-done scopes, so it never rename-replaces a DB the daemon holds
// open. Use it when a bootstrap completed some scopes but left others
// stuck (e.g. files / apps queued behind a long userdbs transfer) and
// resync refuses with "This server has users".
//
// Usage:
//   mochictl replication resume <peer-id>
func cmd_replication_resume(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: mochictl replication resume <peer-id>")
	}
	peer := args[0]

	body, err := json.Marshal(map[string]string{"peer": peer})
	if err != nil {
		return err
	}

	resp, err := client().Post("/_/admin/replication/resume", "application/json", bytes.NewReader(body))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode/100 != 2 {
		return http_error(resp.StatusCode, raw)
	}

	if flag_json {
		fmt.Println(string(raw))
		return nil
	}
	if !flag_verbose {
		return nil
	}
	fmt.Printf("Resume initiated against peer %s. Track progress with 'mochictl replication progress'.\n", peer)
	return nil
}

// cmd_replication_backfill handles `mochictl replication backfill <peer-id>`.
// Re-runs the pair-join system-row backfill against `peer`. Safe on
// populated hosts: every row goes through the live op channel
// (REPLACE INTO on the receiver), never rename-replacing an open DB
// file. Use this after extending pair-backfill coverage with a new
// table, or as an escape hatch when per-event ops missed a window.
//
// Usage:
//   mochictl replication backfill <peer-id>
func cmd_replication_backfill(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: mochictl replication backfill <peer-id>")
	}
	peer := args[0]

	body, err := json.Marshal(map[string]string{"peer": peer})
	if err != nil {
		return err
	}

	resp, err := client().Post("/_/admin/replication/backfill", "application/json", bytes.NewReader(body))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode/100 != 2 {
		return http_error(resp.StatusCode, raw)
	}

	if flag_json {
		fmt.Println(string(raw))
		return nil
	}
	if !flag_verbose {
		return nil
	}
	fmt.Printf("Pair-backfill dispatched against peer %s.\n", peer)
	return nil
}

// cmd_replication_reseed handles
// `mochictl replication reseed <peer-id> <db-path> [--force]`.
// Re-seeds ONE stalled stream's DB from `peer` on a live, populated
// replica — the targeted alternative to a full `replica reset` when a
// single stream has wedged on an anchored gap. `db-path` is the stream's
// DB relative to the data dir, e.g.
// users/<user>/<app>/db/feeds.db. The server fetches a fresh snapshot,
// lands it, and re-anchors the cursor. It refuses if the local DB has
// un-shipped local writes (non-empty journal) unless --force is given.
//
// Usage:
//
//	mochictl replication reseed <peer-id> <db-path> [--force]
func cmd_replication_reseed(args []string) error {
	var peer, path string
	force := false
	for _, a := range args {
		switch {
		case a == "--force":
			force = true
		case peer == "":
			peer = a
		case path == "":
			path = a
		}
	}
	if peer == "" || path == "" {
		return fmt.Errorf("usage: mochictl replication reseed <peer-id> <db-path> [--force]")
	}

	body, err := json.Marshal(map[string]any{"peer": peer, "path": path, "force": force})
	if err != nil {
		return err
	}

	resp, err := client().Post("/_/admin/replication/reseed", "application/json", bytes.NewReader(body))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode/100 != 2 {
		return http_error(resp.StatusCode, raw)
	}

	if flag_json {
		fmt.Println(string(raw))
		return nil
	}
	if !flag_verbose {
		return nil
	}
	fmt.Printf("Re-seed dispatched: %s from peer %s.\n", path, peer)
	return nil
}

// cmd_replication_pending_gc handles `mochictl replication pending gc`.
// Runs the unfillable-pending GC on demand and reports the number of
// rows dropped. No arguments. Use after cleaning up a removed-and-
// rejoined peer to flush the now-orphaned pending rows immediately
// instead of waiting for the next hourly pass.
func cmd_replication_pending_gc(args []string) error {
	resp, err := client().Post("/_/admin/replication/pending/gc", "application/json", nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode/100 != 2 {
		return http_error(resp.StatusCode, raw)
	}
	if flag_json {
		fmt.Println(string(raw))
		return nil
	}
	var result struct {
		Dropped int `json:"dropped"`
	}
	if err := json.Unmarshal(raw, &result); err != nil {
		return err
	}
	if flag_verbose || result.Dropped > 0 {
		fmt.Printf("Dropped %d unfillable pending row(s).\n", result.Dropped)
	}
	return nil
}

// cmd_replication_pair_remove handles `mochictl replication pair remove <peer-id>`.
// POSTs the peer-id to the admin endpoint; the server kicks that member
// from the local pair table and announces the new set to the rest.
func cmd_replication_pair_remove(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: mochictl replication pair remove <peer-id>")
	}
	peer := args[0]

	body, err := json.Marshal(map[string]string{"peer": peer})
	if err != nil {
		return err
	}

	resp, err := client().Post("/_/admin/replication/pair/remove", "application/json", bytes.NewReader(body))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode/100 != 2 {
		return http_error(resp.StatusCode, raw)
	}

	if flag_json {
		fmt.Println(string(raw))
		return nil
	}
	if !flag_verbose {
		return nil
	}
	var result struct {
		Removed string   `json:"removed"`
		Members []string `json:"members"`
	}
	if err := json.Unmarshal(raw, &result); err == nil {
		fmt.Printf("Removed %s. Remaining pair members: %s\n", result.Removed, strings.Join(result.Members, ", "))
	}
	return nil
}

// cmd_replication_progress handles `mochictl replication progress [peer-id]`.
// Renders the inbound bulk-bootstrap state grouped by peer with one
// line per scope. With -j or -t, falls through to the generic dump for
// scripted consumption. The optional peer-id arg filters server-side.
//
// scopeOrder lists scopes in the natural transfer order — sysdbs go
// first (operator-supplied), then files, apps, userdbs in dependency
// order. Anything else falls to the end alphabetically.
func cmd_replication_progress(args []string) error {
	path := "/_/admin/replication/progress"
	if len(args) > 0 && args[0] != "" {
		path = path + "?peer=" + args[0]
	}
	if flag_json || flag_tabs {
		return get_dump(path, "rows")
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
		Rows []struct {
			Peer     string `json:"peer"`
			Scope    string `json:"scope"`
			State    string `json:"state"`
			Position string `json:"position"`
		} `json:"rows"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		os.Stdout.Write(body)
		return nil
	}

	if len(payload.Rows) == 0 {
		fmt.Println("No inbound bootstrap activity on this server.")
		fmt.Println("If you expected progress here, you may have queried the wrong instance — bootstrap state is tracked on the joining server, not the source.")
		return nil
	}

	// Group by peer in the order they first appear.
	type entry struct{ scope, state, position string }
	byPeer := map[string][]entry{}
	var peers []string
	for _, r := range payload.Rows {
		if _, seen := byPeer[r.Peer]; !seen {
			peers = append(peers, r.Peer)
		}
		byPeer[r.Peer] = append(byPeer[r.Peer], entry{r.Scope, r.State, r.Position})
	}

	scopeRank := map[string]int{"sysdbs": 0, "files": 1, "apps": 2, "userdbs": 3}
	scopeLabel := map[string]string{
		"sysdbs":  "system databases",
		"userdbs": "user databases",
	}
	label := func(scope string) string {
		if l, ok := scopeLabel[scope]; ok {
			return l
		}
		return scope
	}

	// Column widths sized to the widest rendered scope label across all
	// rows so alignment doesn't lose its grid when a "user databases"
	// row shares a peer with a 5-char "files" row.
	scopeCol := 0
	for _, rows := range byPeer {
		for _, r := range rows {
			if l := label(r.scope); len(l) > scopeCol {
				scopeCol = len(l)
			}
		}
	}
	scopeCol += 2 // gap before the state column

	// stateCol must fit the widest state value — "incomplete" is 10 chars.
	// pad() clamps the fill to zero so a value wider than its column can never
	// make strings.Repeat panic on a negative count — which it did the moment
	// a scope settled to 'incomplete' (stateCol was 8).
	const stateCol = 12
	pad := func(width, n int) string {
		if width > n {
			return strings.Repeat(" ", width-n)
		}
		return ""
	}

	for i, peer := range peers {
		if i > 0 {
			fmt.Println()
		}
		fmt.Println(peer)
		rows := byPeer[peer]
		sort.SliceStable(rows, func(a, b int) bool {
			ra, oka := scopeRank[rows[a].scope]
			rb, okb := scopeRank[rows[b].scope]
			if oka && okb {
				return ra < rb
			}
			if oka {
				return true
			}
			if okb {
				return false
			}
			return rows[a].scope < rows[b].scope
		})
		for _, r := range rows {
			scopeStr := label(r.scope) + pad(scopeCol, len(label(r.scope)))
			stateStr := r.state + pad(stateCol, len(r.state))
			tail := ""
			if r.state == "active" && r.position != "" {
				tail = r.position + " items remaining"
			}
			fmt.Printf("  %s%s%s\n", scopeStr, stateStr, tail)
		}
	}
	return nil
}
