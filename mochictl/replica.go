// mochictl: replica subcommands (whole-server pair join / leave / status).
// Copyright Alistair Cunningham 2026
//
// `mochictl replica join <source-id>` is the operator-side entry point
// for adding a fresh server to an existing pair set. The server-side
// admin handler (POST /_/admin/replica/join) refuses if the local
// users.db is non-empty (the empty-replica rule), records the pending
// join in settings.db, and emits a P2P join-request to the source
// peer. This command then polls /_/admin/replica/status until the
// state flips to approved (the source admin clicked Approve) or
// denied / errored. Bootstrap progress display is part of #66 — until
// then, "approved" is the terminal state from this command's POV.
//
// `mochictl replica leave` clears the local pair table and announces
// the departure via pair-membership-change ops. Per the plan, leave
// stops sync without wiping local data — admins delete users via the
// normal action if they want full cleanup.
//
// `mochictl replica status` is a one-shot read of the current state
// for diagnostics and scripting.

//go:build linux

package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"time"
)

// cmd_replica_join handles `mochictl replica join <source-id>`. POSTs
// the source-id to the server admin endpoint, then polls status with a
// short backoff until a terminal state.
func cmd_replica_join(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: mochictl replica join <source-peer-id>")
	}
	source := args[0]

	body, err := json.Marshal(map[string]string{"source": source})
	if err != nil {
		return err
	}

	resp, err := client().Post("/_/admin/replica/join", "application/json", bytes.NewReader(body))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(raw)))
	}

	var start struct {
		State  string `json:"state"`
		Peer   string `json:"peer"`
		Source string `json:"source"`
	}
	if err := json.Unmarshal(raw, &start); err != nil {
		return fmt.Errorf("bad start response: %w", err)
	}

	if !flag_json {
		fmt.Printf("Replica peer-id: %s\n", start.Peer)
		fmt.Printf("Waiting for approval from %s ...\n", start.Source)
	}

	// Poll status. Short backoff because approval is a human-driven
	// action — the operator may be clicking right now, or could take
	// a couple of minutes. We don't time out client-side; mochictl
	// runs until the server reports a terminal state. Ctrl+C is the
	// operator's escape hatch; the pending state survives in
	// settings.db and is observable via `mochictl replica status`.
	for {
		time.Sleep(2 * time.Second)
		state, source, reason, members, err := replica_status_read()
		if err != nil {
			return err
		}
		switch state {
		case "approved":
			if flag_json {
				fmt.Println(string(must_marshal(map[string]any{
					"state":   "approved",
					"source":  source,
					"members": members,
				})))
			} else {
				fmt.Printf("Approved. Pair set: %s\n", strings.Join(members, ", "))
				fmt.Printf("Bootstrap started against %s. Track with: mochictl replication progress %s\n", source, source)
			}
			return nil
		case "denied":
			if flag_json {
				fmt.Println(string(must_marshal(map[string]any{
					"state":  "denied",
					"source": source,
					"reason": reason,
				})))
			} else {
				if reason != "" {
					fmt.Fprintf(os.Stderr, "Denied: %s\n", reason)
				} else {
					fmt.Fprintln(os.Stderr, "Denied.")
				}
			}
			return fmt.Errorf("join denied")
		case "waiting":
			// Keep polling.
		case "idle":
			// State was lost server-side (admin cleared it?) — bail
			// rather than poll forever.
			return fmt.Errorf("server reports no pending join (state=idle)")
		default:
			return fmt.Errorf("unexpected state %q", state)
		}
	}
}

// cmd_replica_leave handles `mochictl replica leave`. POSTs to the
// admin endpoint and prints the server's response.
func cmd_replica_leave(args []string) error {
	resp, err := client().Post("/_/admin/replica/leave", "", nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	if flag_json {
		fmt.Println(string(body))
		return nil
	}
	if !flag_verbose {
		return nil
	}
	var result struct {
		State          string   `json:"state"`
		FormerMembers  []string `json:"former_members"`
	}
	if err := json.Unmarshal(body, &result); err == nil {
		fmt.Printf("%s. Former pair members: %s\n", result.State, strings.Join(result.FormerMembers, ", "))
	}
	return nil
}

// cmd_replica_status is the one-shot diagnostic read.
func cmd_replica_status(args []string) error {
	return get_dump("/_/admin/replica/status", "state", "peer", "source", "members", "reason")
}

// replica_status_read is the polling-loop helper used by cmd_replica_join.
// Returns (state, source, reason, members, err).
func replica_status_read() (string, string, string, []string, error) {
	resp, err := client().Get("/_/admin/replica/status")
	if err != nil {
		return "", "", "", nil, err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode/100 != 2 {
		return "", "", "", nil, fmt.Errorf("HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	var s struct {
		State   string   `json:"state"`
		Source  string   `json:"source"`
		Reason  string   `json:"reason"`
		Members []string `json:"members"`
	}
	if err := json.Unmarshal(body, &s); err != nil {
		return "", "", "", nil, err
	}
	return s.State, s.Source, s.Reason, s.Members, nil
}

func must_marshal(v any) []byte {
	b, _ := json.Marshal(v)
	return b
}
