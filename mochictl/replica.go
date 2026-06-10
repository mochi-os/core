// mochictl: replica subcommands (whole-server pair join / leave / status).
// Copyright Alistair Cunningham 2026
//
// `mochictl replica join <source-id>` is the operator-side entry point
// for adding a fresh server to an existing pair set. The server-side
// admin handler (POST /_/admin/replica/join) refuses if the local
// users.db is non-empty (the empty-replica rule), records the pending
// join in settings.db, and emits a Net join-request to the source
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

package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"core/common/ini"
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
		return http_error(resp.StatusCode, raw)
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
				fmt.Printf("Bootstrap started against %s. Track with: %s replication progress\n", source, self_invocation())
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
		return http_error(resp.StatusCode, body)
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

// cmd_replica_reset handles `mochictl replica reset --from=<peer-id> --confirm`.
// The escape hatch for a replica that's too far behind to catch up via
// incremental replication: wipes the local replicated state and prints
// the rejoin command. Preserves the libp2p host key so the peer identity
// stays stable - rejoin reuses the same identity, the partner sees the
// same peer-id.
//
// Refuses without --confirm (destructive), without --from (the reset
// only makes sense paired with a partner to rejoin against), and if
// the admin socket is reachable (server must be stopped first).
//
// Backs up replication.db to db/replication.db.pre-reset so the operator
// can inspect the prior pair / cursor / leadership state before it's
// gone. Local-only DBs (queue, peers, external, directory, identities)
// are preserved - they're not part of the replication contract and may
// hold in-flight messages or libp2p discovery cache that the operator
// would rather not lose.
func cmd_replica_reset(args []string) error {
	var from string
	confirm := false
	for _, arg := range args {
		switch {
		case arg == "--confirm":
			confirm = true
		case strings.HasPrefix(arg, "--from="):
			from = strings.TrimPrefix(arg, "--from=")
		default:
			return fmt.Errorf("unknown argument %q", arg)
		}
	}
	if from == "" {
		return fmt.Errorf("usage: mochictl replica reset --from=<peer-id> --confirm")
	}
	if !confirm {
		return fmt.Errorf("--confirm required: reset is destructive (wipes all replicated DBs and the per-user trees)")
	}

	// Server must be stopped. Probe the admin socket: if it responds,
	// the server is alive and reset would race file-mutation with live
	// writes.
	if replica_server_alive() {
		return fmt.Errorf("server is running on the admin socket; stop it before resetting")
	}

	data := ini.String("directories", "data", "/var/lib/mochi")
	if _, err := os.Stat(data); err != nil {
		return fmt.Errorf("data directory %q not accessible: %w", data, err)
	}

	// Back up replication.db so the operator can inspect pre-reset state.
	src := filepath.Join(data, "db", "replication.db")
	if _, err := os.Stat(src); err == nil {
		dst := src + ".pre-reset"
		if err := replica_reset_copy(src, dst); err != nil {
			return fmt.Errorf("backup replication.db -> %s: %w", dst, err)
		}
		fmt.Printf("Backed up %s to %s\n", src, dst)
	}

	// Wipe the replicated DBs (per the core DB audit, task #44) plus
	// their WAL / SHM siblings. Preserves local-only DBs and the
	// libp2p host key.
	replicated := []string{
		"db/users.db",
		"db/sessions.db",
		"db/settings.db",
		"db/apps.db",
		"db/domains.db",
		"db/schedule.db",
		"db/replication.db",
	}
	wiped := 0
	for _, t := range replicated {
		for _, suffix := range []string{"", "-wal", "-shm"} {
			p := filepath.Join(data, t+suffix)
			if _, err := os.Stat(p); err != nil {
				continue
			}
			if err := os.Remove(p); err != nil {
				return fmt.Errorf("wipe %s: %w", p, err)
			}
			wiped++
		}
	}

	// Per-user trees hold every user's app DBs and uploaded files;
	// all replicated by user replication.
	users_dir := filepath.Join(data, "users")
	if _, err := os.Stat(users_dir); err == nil {
		if err := os.RemoveAll(users_dir); err != nil {
			return fmt.Errorf("wipe users tree %s: %w", users_dir, err)
		}
	}

	fmt.Printf("Wiped %d file(s) under %s/db plus the users/ tree. Local-only state (queue.db, peers.db, external.db, directory.db, identities.db) and the libp2p host key (p2p/private.key) preserved.\n",
		wiped, data)
	fmt.Println()
	fmt.Println("Next steps:")
	fmt.Println("  1. Start the server (e.g. systemctl --user start mochi1).")
	fmt.Printf("  2. mochictl replica join %s\n", from)
	fmt.Println()
	fmt.Println("The partner's admin will see the fresh join request and can approve.")
	return nil
}

// replica_server_alive probes the admin socket with a short-timeout
// GET to /_/admin/replica/status. A success or any HTTP response means
// the server is running; only a transport error counts as "stopped".
func replica_server_alive() bool {
	resp, err := client(2 * time.Second).Get("/_/admin/replica/status")
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	return true
}

// replica_reset_copy copies src to dst. Used to back up replication.db
// before wipe. Simple read-all + write-all - replication.db is on the
// order of MBs at most, no need for streaming.
func replica_reset_copy(src, dst string) error {
	data, err := os.ReadFile(src)
	if err != nil {
		return err
	}
	return os.WriteFile(dst, data, 0644)
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
		return "", "", "", nil, http_error(resp.StatusCode, body)
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
