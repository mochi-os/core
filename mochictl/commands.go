// mochictl: subcommand implementations.
// Copyright Alistair Cunningham 2026
//
// Each subcommand is a function value in the `commands` map (declared in
// main.go). Every server-talking subcommand uses the UDS admin client —
// including `health`, which hits /_/admin/health rather than the public
// /_/health, because TLS-only deploys reject 127.0.0.1 handshakes on SNI
// mismatch. External monitors keep using the public /_/health endpoint.

//go:build linux

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
			help: "Write *.db.snap siblings of every live DB in the data dir",
			run: func(args []string) error {
				return post_silent("/_/admin/snapshot")
			},
		},
		"rsync-filter": {
			help: "Print rsync filter rules for backing up the data dir",
			run:  cmd_rsync_filter,
		},
		"restore": {
			help: "Walk a directory and rename *.db.snap -> strip .snap (server stopped)",
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
		"replica join": {
			help: "Join an existing server as a pair replica (fresh installs only)",
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
		"replication progress": {
			help: "Per-(peer, scope) bulk-bootstrap progress",
			run: func(args []string) error {
				path := "/_/admin/replication/progress"
				if len(args) > 0 && args[0] != "" {
					path = path + "?peer=" + args[0]
				}
				return get_dump(path, "rows")
			},
		},
		"replication pair remove": {
			help: "Kick a specific peer from the pair set",
			run:  cmd_replication_pair_remove,
		},
		"replication resync": {
			help: "Force a bulk-bootstrap re-run against the given peer",
			run:  cmd_replication_resync,
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
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
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
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
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
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
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
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
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
		return fmt.Errorf("HTTP %d", resp.StatusCode)
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
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
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
// snapshot temps, and the runtime state directory are excluded; *.db.snap
// siblings produced by `mochictl snapshot` are kept.
var rsync_filter_rules = []string{
	"- *.db",
	"- *.db-wal",
	"- *.db-shm",
	"- *.db-journal",
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

// cmd_restore walks the given directory tree and renames every *.db.snap
// file to its sibling without the .snap suffix. Run after rsync brings a
// backup to a destination, before starting the server there. The server
// must be stopped during this operation.
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
		if d.IsDir() || !strings.HasSuffix(d.Name(), ".db.snap") {
			return nil
		}
		live := strings.TrimSuffix(p, ".snap")
		if err := os.Rename(p, live); err != nil {
			return fmt.Errorf("rename %s -> %s: %w", p, live, err)
		}
		count++
		return nil
	})
	if err != nil {
		return err
	}
	fmt.Printf("Renamed %d snapshot file(s) under %s\n", count, root)
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
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(out)))
	}
	os.Stdout.Write(out)
	if len(out) > 0 && out[len(out)-1] != '\n' {
		fmt.Println()
	}
	return nil
}
