// Mochi server: Main
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"flag"
	"os"
	"os/signal"
	"runtime"
	"time"

	"core/common/paths"
)

type Map map[string]any

var (
	build_version    string
	build_platform   string
	cache_dir        string
	config_file      string
	data_dir         string
	dev_apps_dir     string
	dev_reload       bool
	web_cache        bool
	web_compress     string
	web_gzip_level   int
	web_brotli_level int
	email_host       string
	email_port       int
	email_tls        bool

	server_started_at time.Time
)

func main() {
	server_started_at = time.Now()
	if windows_service_run() {
		// Ran as a Windows service via the SCM. service_windows.go drove
		// main_serve() to completion already.
		return
	}
	code := main_serve(nil)
	if code != 0 {
		os.Exit(code)
	}
}

// main_serve runs the full server lifecycle: parse flags, load config, start
// managers, wait for a shutdown trigger, drain, exit. Returns the exit code.
//
// The optional ready callback is invoked once initialisation is complete and
// the server has started serving requests — used by the Windows service
// handler to transition from StartPending to Running at the right moment.
// Pass nil in interactive mode.
func main_serve(ready func()) int {
	if build_platform != "" {
		info("Mochi %s starting on %s", build_version, build_platform)
	} else {
		info("Mochi %s starting", build_version)
	}

	// Platform-aware default paths, shared with mochictl
	default_config := paths.Config()
	default_cache := paths.Cache()
	default_data := paths.Data()

	flag.StringVar(&config_file, "f", default_config, "Configuration file")
	flag.Parse()
	err := ini_load(config_file)
	if err != nil {
		warn("Unable to read configuration file: %v", err)
		return 1
	}

	cache_dir = ini_string("directories", "cache", default_cache)
	data_dir = ini_string("directories", "data", default_data)
	if err := directories_ensure(); err != nil {
		warn("directories.ensure failed: %v", err)
		return 1
	}
	if err := run_dir_create(); err != nil {
		warn("Unable to create runtime state directory %s: %v", run_dir(), err)
	}
	temporary_configure()
	// Confirm the data directory is writable. On Windows, the MSI
	// installer creates %ProgramData%\Mochi\data owned by SYSTEM with
	// restrictive ACLs so the auto-installed mochi-server service
	// (running as LocalSystem) can write to it. Running
	// mochi-server.exe interactively from a non-admin shell hits a
	// permission wall that previously surfaced as a panic from deep
	// inside setting_set; bail early with a clear message instead.
	if err := data_dir_writable_check(); err != nil {
		warn("Data directory %q is not writable: %v.", data_dir, err)
		switch runtime.GOOS {
		case "windows":
			warn("On Windows, either let the auto-installed mochi-server service handle it (Services.msc → 'Mochi Server') or run mochi-server.exe from an elevated (Run as administrator) command prompt.")
		case "darwin":
			warn("On macOS, run mochi-server with sudo or adjust ownership of the data directory.")
		default:
			warn("Run mochi-server as a user with write permission on the data directory, or adjust the directory's ownership.")
		}
		return 1
	}

	// Redirect stdout/stderr to a file when running as a Windows service —
	// the SCM doesn't expose a console, so log.Print would otherwise vanish.
	// No-op on other platforms and in interactive mode.
	windows_service_redirect_logs()

	// Load [email] before audit_init so any warn() emitted during early
	// startup (e.g. audit_init failing on a host with no syslog) can reach
	// the admin via email_send.
	email_host = ini_string("email", "host", "127.0.0.1")
	email_port = ini_int("email", "port", 25)
	email_tls = ini_bool("email", "tls", true)

	audit_init()
	audit_server_start(build_version)

	dev_apps_dir = ini_string("development", "apps", "")
	dev_reload = ini_bool("development", "reload", false)
	web_cache = ini_bool("web", "cache", true)
	web_compress = ini_string("web", "compress", "auto")
	web_gzip_level = ini_int("web", "gzip", 6)
	web_brotli_level = ini_int("web", "brotli", 4)
	switch web_compress {
	case "none", "gzip", "br", "auto":
	default:
		warn("Invalid web.compress value %q; disabling compression", web_compress)
		web_compress = "none"
	}
	if web_gzip_level < 1 || web_gzip_level > 9 {
		warn("Invalid web.gzip level %d; using default (6)", web_gzip_level)
		web_gzip_level = 6
	}
	if web_brotli_level < 0 || web_brotli_level > 11 {
		warn("Invalid web.brotli level %d; using default (4)", web_brotli_level)
		web_brotli_level = 4
	}

	load_core_labels()
	starlark_configure()
	db_start()
	passkey_init()
	if err := domains_load_certs(); err != nil {
		warn("Failed to load domain certificates: %v", err)
	}
	domains_init_acme()
	apps_start()
	net_start()
	// setting_set replicates to every pair member via system-set ops
	// (#68). Must run after net_start so the spawned send_peer
	// goroutines don't dereference a nil net_me on a server that
	// already has pair members from a prior run.
	setting_set("server_started", itoa(int(now())))
	if err := admin_start(); err != nil {
		warn("admin listener disabled: %v", err)
	}
	go cache_manager()
	go closure_manager()
	go entities_manager()
	go directory_manager()
	go directory_cleanup_manager()
	go peers_manager()
	go peer_reconnect_manager()
	go peers_publish()
	go queue_manager()
	go queue_ack_batcher()
	go self_loop_drain()
	go ratelimit_manager()
	go broadcast_manager()
	go restore_cleanup_orphans()
	go db_app_system_sweep()
	go sessions_manager()
	go update_manager()
	// Register the configured [web] domain (if any) before the web server
	// starts, so a fresh server can serve HTTPS on first boot.
	domains_seed_config()
	go web_start()
	go apps_manager()
	go schedule_start()

	if ready != nil {
		ready()
	}

	// Wait for a shutdown trigger. Sources:
	//   - os.Interrupt (Ctrl-C, cross-platform)
	//   - SIGTERM (docker stop, systemctl stop)
	//   - shutdown_request channel (mochictl stop / restart, exit code carried;
	//     also driven by the Windows service handler when the SCM sends Stop)
	// SIGHUP is registered too but ignored — config reload was dropped, and
	// not registering it would let kill -HUP terminate the process via the
	// default signal action.
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, append([]os.Signal{os.Interrupt}, extra_signals()...)...)

	exit_code := 0
loop:
	for {
		select {
		case s := <-sig:
			if is_ignorable_signal(s) {
				info("Signal %v received, ignoring (restart for config changes)", s)
				continue
			}
			info("Shutdown signal %v received, stopping gracefully...", s)
			break loop
		case code := <-shutdown_request:
			info("Operator-initiated shutdown (exit code %d)", code)
			exit_code = code
			break loop
		}
	}

	audit_server_stop()

	// Run the drain + close sequence under an overall deadline. queue_drain
	// and peers_shutdown are individually bounded, but on a busy PUBLIC host
	// libp2p's host Close (and the relay/transport shutdown beneath it) can
	// block indefinitely when a connection or listener won't quiesce —
	// observed on yuzu hanging the full systemd TimeoutStopSec (90s) before
	// SIGKILL, while an idle NAT'd instance shuts down in milliseconds. Cap
	// the whole phase well under 90s so we exit cleanly on our own terms
	// instead of being force-killed mid-write. SQLite is crash-safe (WAL
	// recovery on next open), so a forced exit here loses no committed data,
	// and peers treat the dropped connection like any other and reconnect.
	const shutdown_grace = 30 * time.Second
	done := make(chan struct{})
	go func() {
		queue_drain(10 * time.Second) // outbound queue (bounded)
		peers_shutdown()              // bye to connected peers (bounded)
		// relay_shutdown (relay_service.Close) and net_me.Close (the libp2p host
		// close) are BOTH unbounded libp2p teardowns, and on a busy PUBLIC host
		// they reliably never quiesce: the web server is still accepting on the
		// shared :443 listener and the QUIC/relay connections won't drain, so
		// this phase WAS the entire 30s hang-then-kill on every restart (an idle
		// NAT'd host closes in milliseconds). Give it a brief window for a clean
		// close, then move on — peers already got goodbye, the OS reclaims the
		// sockets on exit, and peers treat the dropped connection like any other.
		netdone := make(chan struct{})
		go func() {
			relay_shutdown() // stop the circuit-relay service
			if net_me != nil {
				net_me.Close() // close the libp2p host
			}
			close(netdone)
		}()
		select {
		case <-netdone:
		case <-time.After(2 * time.Second):
			info("libp2p teardown did not quiesce within 2s; proceeding to exit")
		}
		audit_close()
		close(done)
	}()

	select {
	case <-done:
		info("Shutdown complete")
	case <-time.After(shutdown_grace):
		// Backstop only: the libp2p teardown is individually bounded above, so
		// reaching here means queue_drain / peers_shutdown / audit_close itself
		// overran — rare. info, not warn: the forced exit is the designed, safe
		// fallback (SQLite is crash-safe; the alternative was the 90s SIGKILL),
		// so it's not operator-actionable — log it, don't email a "Mochi error".
		info("Shutdown exceeded %s; forcing exit", shutdown_grace)
		os.Exit(exit_code)
	}
	return exit_code
}
