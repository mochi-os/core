// mochictl: subcommand implementations.
// Copyright Alistair Cunningham 2026
//
// Each subcommand is a function value in the `commands` map (declared in
// main.go). Every server-talking subcommand uses the UDS admin client —
// including `health`, which hits /_/admin/health rather than the public
// /_/health, because TLS-only deploys reject 127.0.0.1 handshakes on SNI
// mismatch. External monitors keep using the public /_/health endpoint.

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
)

// self_invocation reconstructs the `mochictl [global-flags]` prefix that
// would target the same admin socket as the current run. Used when we
// print a follow-up command for the user to copy — without the flag the
// hint would default to /etc/mochi/mochi.conf and silently miss the
// instance they're actually managing.
func self_invocation() string {
	if socket != "" {
		return fmt.Sprintf("mochictl -s %s", socket)
	}
	if file != "" && file != default_config {
		return fmt.Sprintf("mochictl -f %s", file)
	}
	return "mochictl"
}

// http_error formats a non-2xx admin-socket response as a user-friendly
// error string. Tries the JSON `message` field first (server-side
// translated text from respond_error), then the `error` code, and
// finally falls back to the raw trimmed body. Drops the HTTP status —
// CLI users care about the cause, not the code; use -v if you need it.
func http_error(status int, body []byte) error {
	trimmed := strings.TrimSpace(string(body))
	if len(trimmed) == 0 {
		return fmt.Errorf("HTTP %d", status)
	}
	var parsed map[string]any
	if err := json.Unmarshal([]byte(trimmed), &parsed); err == nil {
		if m, ok := parsed["message"].(string); ok && m != "" {
			return fmt.Errorf("%s", m)
		}
		if e, ok := parsed["error"].(string); ok && e != "" {
			return fmt.Errorf("%s", e)
		}
	}
	return fmt.Errorf("%s", trimmed)
}

func init() {
	commands = map[string]command{
		"health": {
			help: "Check server health (UDS probe to /_/admin/health)",
			run:  cmd_health,
		},
		"status": {
			help: "Show uptime, version, peers, apps",
			run: func(args []string) error {
				return get_dump("/_/admin/status",
					"status", "version", "uptime",
					"peers_connected", "peers_known", "apps")
			},
		},
		"version": {
			help: "Show server and CLI versions",
			run:  cmd_version,
		},
		"config show": {
			help: "Show effective config (file + env, secrets redacted)",
			run: func(args []string) error {
				return get_dump("/_/admin/config")
			},
		},
		"identity": {
			help: "Show server peer ID and data dir",
			run: func(args []string) error {
				return get_dump("/_/admin/identity")
			},
		},
		"backup": {
			help: "Stream a tar.gz backup to stdout or a file path",
			run:  cmd_backup,
		},
		"snapshot": {
			help: "Write *.db.backup siblings of every live DB in the data dir",
			run: func(args []string) error {
				return post_silent("/_/admin/snapshot")
			},
		},
		"vacuum": {
			help: "Reclaim free pages from every open DB now (the periodic pass, on demand)",
			run: func(args []string) error {
				return post_dump("/_/admin/vacuum",
					"databases_reclaimed", "bytes_reclaimed", "duration_ms")
			},
		},
		"rsync-filter": {
			help: "Print rsync filter rules for backing up the data dir",
			run:  cmd_rsync_filter,
		},
		"restore": {
			help: "Walk a directory and rename *.db.backup (or legacy *.db.snap) to *.db (server stopped)",
			run:  cmd_restore,
		},
		"stop": {
			help: "Graceful shutdown; supervisor decides whether to restart",
			run: func(args []string) error {
				return post_action("/_/admin/stop", "Stopping server")
			},
		},
		"start": {
			help: "Start the server via systemctl (native installs only)",
			run:  cmd_start,
		},
		"restart": {
			help: "Graceful shutdown with restart-hint exit code",
			run: func(args []string) error {
				return post_action("/_/admin/restart", "Restarting server")
			},
		},
		"replica reset": {
			help: "Wipe replicated DBs + per-user trees and print the rejoin command. Server must be stopped. Use when a replica is too far behind to catch up via incremental replication. Args: --from=<peer-id> --confirm",
			run:  cmd_replica_reset,
		},
		"replica join": {
			help: "Join an existing server as a pair replica (fresh installs only). Args: <source-peer-id> [--address=<multiaddr>]... to supply the source's address when discovery cannot find it",
			run:  cmd_replica_join,
		},
		"replica leave": {
			help: "Leave the pair set (stops sync; does not wipe local data)",
			run:  cmd_replica_leave,
		},
		"replica status": {
			help: "Show current pair / pending-join state",
			run:  cmd_replica_status,
		},
		"replication status": {
			help: "Summarise pair set + per-user host counts + pending requests",
			run: func(args []string) error {
				return get_dump("/_/admin/replication/status",
					"peer", "pair", "hosts_count", "links_pending", "joins_pending", "bootstrap_pending")
			},
		},
		"replication pair list": {
			help: "List current pair members",
			run: func(args []string) error {
				return get_dump("/_/admin/replication/pair", "members")
			},
		},
		"replication pairs": {
			help: "Per-pair health rollup (bootstrap, cursors, pending, leases) - what each pair partner looks like from here",
			run: func(args []string) error {
				return get_dump("/_/admin/replication/pairs", "host", "now", "pairs")
			},
		},
		"replication progress": {
			help: "Per-(peer, scope) bulk-bootstrap progress",
			run:  cmd_replication_progress,
		},
		"replication stalled": {
			help: "Per-stream pending-buffer stalls (replication drift not healing on its own)",
			run: func(args []string) error {
				return get_dump("/_/admin/replication/stalled", "stalled")
			},
		},
		"replication irreparable": {
			help: "Run the irreparable scan on demand (same logic the manager runs hourly) and list relationships broken past T_forget. Reason is 'stalled' (unfillable gap) or 'offline' (member unreachable too long).",
			run: func(args []string) error {
				return get_dump("/_/admin/replication/irreparable", "irreparable")
			},
		},
		"replication pending gc": {
			help: "On-demand purge of aged unfillable pending rows (same logic the manager runs hourly). Reports count dropped.",
			run:  cmd_replication_pending_gc,
		},
		"replication pair remove": {
			help: "Kick a specific peer from the pair set",
			run:  cmd_replication_pair_remove,
		},
		"replication resync": {
			help: "Force a bulk-bootstrap re-run against the given peer",
			run:  cmd_replication_resync,
		},
		"replication resume": {
			help: "Re-drive only the not-yet-done bootstrap scopes for a peer (safe on a populated server, unlike resync)",
			run:  cmd_replication_resume,
		},
		"replication backfill": {
			help: "Re-run the pair-join system-row backfill against the given peer",
			run:  cmd_replication_backfill,
		},
		"replication reseed": {
			help: "Re-seed ONE stalled stream's DB from a source peer on a live populated replica (targeted alternative to a full 'replica reset'). Refuses if the local DB has un-shipped local writes unless --force. Args: <peer-id> <db-path> [--force]",
			run:  cmd_replication_reseed,
		},
		"replication audit": {
			help: "Convergence audit findings: apps running stale on-disk code (apps.db claims a version not on disk, e.g. a restricted app frozen on a replica) + cross-host content divergences confirmed stable across rounds",
			run: func(args []string) error {
				return get_dump("/_/admin/replication/audit", "stale", "divergences", "stuck")
			},
		},
		"broadcast lag": {
			help: "Per-(user, app, peer, key) broadcast subscriber lag (received_last vs owner _log.max when this host owns the stream)",
			run:  cmd_broadcast_lag,
		},
		"pipelining status": {
			help: "/mochi/2 transport state: open Senders + inflight, per-host worker pool.",
			run:  cmd_pipelining_status,
		},
		"pubsub status": {
			help: "Per-topic GossipSub mesh peer count + published/received counters during the /mochi/2 migration.",
			run:  cmd_pubsub_status,
		},
		"check starlark": {
			help: "Parse every .star file under <path> using the server's go.starlark.net parser. Non-zero exit + file:line:col on the first parse error. Use in deploy.sh before zipping the bundle.",
			run:  cmd_check_starlark,
		},
	}
}

// get_dump issues a GET to the given admin path and renders the response
// body in the format selected by global flags. Optional `order` controls
// the top-level field ordering for human and tab-separated output. Non-2xx
// status codes return an error so the CLI exits non-zero.
func get_dump(path string, order ...string) error {
	resp, err := client().Get(path)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode/100 != 2 {
		return http_error(resp.StatusCode, body)
	}
	return render(body, order...)
}

// post_action is for lifecycle endpoints (stop/restart) where the JSON
// response body is just `{"status": "..."}`. Silent on success by default;
// prints `human_msg` (e.g. "Stopping server") only with -v. In -t / -j
// mode it falls through to post_dump and renders the raw response so
// scripts can parse it.
func post_action(path, human_msg string) error {
	if flag_tabs || flag_json {
		return post_dump(path)
	}
	resp, err := client().Post(path, "", nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode/100 != 2 {
		return http_error(resp.StatusCode, body)
	}
	if flag_verbose {
		fmt.Println(human_msg)
	}
	return nil
}

// post_silent POSTs to path and returns the response body verbatim only when
// the caller has asked for output (-v, -t, or -j); otherwise it succeeds
// without printing anything. Used for routine maintenance commands like
// `mochictl snapshot` that are typically run from cron — the operator only
// cares about the exit code unless they passed -v.
func post_silent(path string, order ...string) error {
	if flag_verbose || flag_tabs || flag_json {
		return post_dump(path, order...)
	}
	resp, err := client().Post(path, "", nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode/100 != 2 {
		return http_error(resp.StatusCode, body)
	}
	return nil
}

// post_dump is the POST equivalent of get_dump.
func post_dump(path string, order ...string) error {
	resp, err := client().Post(path, "", nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode/100 != 2 {
		return http_error(resp.StatusCode, body)
	}
	return render(body, order...)
}

// cmd_health probes /_/admin/health over the UDS. Used by Docker HEALTHCHECK
// (which runs inside the container and has socket access) and by operators
// running mochictl on the host. Mirrors the field set returned by the public
// /_/health route so external monitors and HEALTHCHECK see the same shape.
func cmd_health(args []string) error {
	resp, err := client().Get("/_/admin/health")
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if err := render(body, "status", "version", "uptime", "database", "network"); err != nil {
		return err
	}
	if resp.StatusCode/100 != 2 {
		return http_error(resp.StatusCode, body)
	}
	return nil
}

// cmd_version queries the server for its build, then renders alongside the
// mochictl version. Output respects the global format flags. Field order:
// server version, schema version, mochictl version.
func cmd_version(args []string) error {
	order := []string{"server_version", "schema_version", "mochictl_version"}

	resp, err := client().Get("/_/admin/version")
	if err != nil {
		// Server unreachable — print only the client version.
		out, _ := json.Marshal(map[string]string{"mochictl_version": build_version})
		return render(out, order...)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)

	// Decode the server response so we can attach the mochictl version.
	var server map[string]any
	if err := json.Unmarshal(body, &server); err != nil {
		return render(body, order...)
	}
	server["mochictl_version"] = build_version
	out, _ := json.Marshal(server)
	return render(out, order...)
}

// cmd_backup streams the admin tarball to either a file path or stdout.
//
//	mochictl backup                 -> ./mochi-backup_YYYYMMDD_HHMMSS.tar.gz
//	mochictl backup -               -> stdout
//	mochictl backup /path/to/file   -> /path/to/file
//
// Backups can take many minutes on a large data dir, so we use a no-timeout
// client (0 disables http.Client.Timeout). The user can ctrl-C if needed.
func cmd_backup(args []string) error {
	resp, err := client(0).Get("/_/admin/backup")
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		body, _ := io.ReadAll(resp.Body)
		return http_error(resp.StatusCode, body)
	}

	var out io.Writer
	var path string
	auto := false
	switch {
	case len(args) == 0:
		// mochi-backup_YYYYMMDD_HHMMSS.tar.gz — sortable, shell-safe.
		path = fmt.Sprintf("mochi-backup_%s.tar.gz", time.Now().Format("20060102_150405"))
		auto = true
	case args[0] == "-":
		// Stream to stdout — no path, no confirmation message.
		out = os.Stdout
	default:
		path = args[0]
	}

	if out == nil {
		f, err := os.Create(path)
		if err != nil {
			return err
		}
		defer f.Close()
		out = f
	}

	if _, err := io.Copy(out, resp.Body); err != nil {
		return err
	}
	// Auto-named path: print so the user knows where the file landed.
	// Explicit path: silent on success unless -v.
	if path != "" && (auto || flag_verbose) {
		fmt.Fprintf(os.Stderr, "mochictl: wrote %s\n", path)
	}
	return nil
}

// rsync_filter_rules is the canonical filter set for backing up the data dir
// with rsync (or restic / borg / S3 sync). Live SQLite files, in-flight
// snapshot temps, and the runtime state directory are excluded; *.db.backup
// siblings (and legacy *.db.snap from before the 2026-05-27 rename) produced
// by `mochictl snapshot` are kept.
var rsync_filter_rules = []string{
	"- *.db",
	"- *.db-wal",
	"- *.db-shm",
	"- *.db-journal",
	"- *.backup.tmp",
	"- *.snap.tmp",
	"- run/",
}

// cmd_rsync_filter prints the filter rules to stdout, one per line. Suitable
// for piping into `rsync --filter='merge <(mochictl rsync-filter)'`.
func cmd_rsync_filter(args []string) error {
	for _, line := range rsync_filter_rules {
		fmt.Println(line)
	}
	return nil
}

// cmd_restore walks the given directory tree and renames every *.db.backup
// file (and any legacy *.db.snap from before the 2026-05-27 rename) to its
// sibling with the suffix stripped. Run after rsync brings a backup to a
// destination, before starting the server there. The server must be stopped
// during this operation.
//
// Replication state (db/replication.db) is stripped after the rename
// pass: the restored host comes back unpaired so it won't auto-reconnect
// to its previous pair partner with stale cursors and silently lose
// self-emitted ops between the snapshot and the crash. Re-establishing
// the pair is an explicit operator step after restore; see the
// backup-restore wiki page for the documented procedure.
func cmd_restore(args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("restore <dir>: directory argument required")
	}
	root := args[0]
	count := 0
	err := filepath.WalkDir(root, func(p string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		name := d.Name()
		var live string
		switch {
		case strings.HasSuffix(name, ".db.backup"):
			live = strings.TrimSuffix(p, ".backup")
		case strings.HasSuffix(name, ".db.snap"):
			live = strings.TrimSuffix(p, ".snap")
		default:
			return nil
		}
		if err := os.Rename(p, live); err != nil {
			return fmt.Errorf("rename %s -> %s: %w", p, live, err)
		}
		count++
		return nil
	})
	if err != nil {
		return err
	}

	// Strip replication.db (and its WAL / SHM siblings if present).
	// The server creates a fresh one on startup. Any previously-paired
	// state - pair members, cursors, seen dedup table, sequence
	// counters, leadership rows - is gone. The host comes back
	// unpaired.
	replication_path := filepath.Join(root, "db", "replication.db")
	stripped := false
	for _, suffix := range []string{"", "-wal", "-shm"} {
		path := replication_path + suffix
		if _, err := os.Stat(path); err == nil {
			if err := os.Remove(path); err != nil {
				return fmt.Errorf("strip replication state %s: %w", path, err)
			}
			stripped = true
		}
	}

	fmt.Printf("Renamed %d snapshot file(s) under %s\n", count, root)
	if stripped {
		fmt.Println()
		fmt.Println("Replication state stripped: the restored host will come back unpaired.")
		fmt.Println("To re-establish replication, see the backup-restore wiki page for the")
		fmt.Println("post-restore procedure (decide which side is canonical, reinstall the")
		fmt.Println("others as fresh replicas, mochictl replica join against the canonical).")
	}
	return nil
}

// cmd_start shells to systemctl to start the mochi-server unit. Errors if
// systemd isn't detected (e.g. running inside Docker or with no init system).
func cmd_start(args []string) error {
	return supervisor_start()
}

// post_with_body is a helper for sending a JSON body. Currently unused but
// kept for future v2 endpoints (users.create, apps.install, etc).
//
//nolint:unused
func post_with_body(path string, payload any) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	resp, err := client().Post(path, "application/json", bytes.NewReader(body))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	out, _ := io.ReadAll(resp.Body)
	if resp.StatusCode/100 != 2 {
		return http_error(resp.StatusCode, out)
	}
	os.Stdout.Write(out)
	if len(out) > 0 && out[len(out)-1] != '\n' {
		fmt.Println()
	}
	return nil
}
