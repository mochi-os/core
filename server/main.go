// Mochi server: Main
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"flag"
	"fmt"
	"io"
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

// server_arguments parses a command line into the configuration path. Reporting
// the version is mochictl's job, so there is no -version flag. Its own FlagSet
// with ContinueOnError: flag.CommandLine's ExitOnError would kill a test
// process.
func server_arguments(arguments []string, default_config string, output io.Writer) (config string, err error) {
	set := flag.NewFlagSet("mochi-server", flag.ContinueOnError)
	set.SetOutput(output)
	set.StringVar(&config, "f", default_config, "Configuration file")
	if err = set.Parse(arguments); err != nil {
		return config, err
	}
	// The flag package stops at the first non-flag argument and reports no error,
	// so `mochi-server version` would silently start a server.
	if set.NArg() > 0 {
		err = fmt.Errorf("unexpected argument %q: mochi-server takes no positional arguments", set.Arg(0))
		fmt.Fprintln(output, err)
		set.Usage()
	}
	return config, err
}

// main_serve runs the full server lifecycle and returns the exit code. The
// optional ready callback fires once the server is serving; the Windows service
// handler uses it to reach Running. Pass nil in interactive mode.
func main_serve(ready func()) int {
	// Platform-aware default paths, shared with mochictl
	default_config := paths.Config()
	default_cache := paths.Cache()
	default_data := paths.Data()

	config, err := server_arguments(os.Args[1:], default_config, os.Stderr)
	if err != nil {
		// server_arguments has already written the error and the usage. Exit 2 is the
		// conventional flag-parse status.
		return 2
	}
	config_file = config

	// Announced only once the arguments are known good, so a rejected invocation
	// leaves no start record without a matching shutdown.
	if build_platform != "" {
		info("Mochi %s starting on %s", build_version, build_platform)
	} else {
		info("Mochi %s starting", build_version)
	}

	err = ini_load(config_file)
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
	if err := apps_dir_create(); err != nil {
		warn("Unable to create app directory %s: %v", apps_dir(), err)
	}
	temporary_configure()
	// Build the API table and register built-in apps and hooks before anything can
	// evaluate a script, route an event, or open a stream. log.go and streams.go
	// keep their init()s: they must precede package-level variables.
	api_init()
	events_init()
	directory_init()
	world_init()
	peers_init()
	senders_init()
	// Confirm the data directory is writable: on Windows %ProgramData%\Mochi\data
	// is SYSTEM-owned, so an interactive non-admin run cannot write there. Bail
	// here rather than panicking inside setting_set.
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
	cache_configure()
	db_start()
	// Before anything can open an app database: an app's migration reads the
	// export this writes, and takes its absence as "no rows".
	attachment_export_sweep()
	if err := domains_load_certs(); err != nil {
		warn("Failed to load domain certificates: %v", err)
	}
	domains_init_acme()
	apps_start()
	go git_placeholder_sweep()
	net_start()
	setting_set("server_started", itoa(int(now())))
	if err := admin_start(); err != nil {
		warn("admin listener disabled: %v", err)
	}
	if err := world_start(); err != nil {
		warn("World listener failed to start: %v", err)
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
	go push_manager()
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

	// Wait for a shutdown trigger: os.Interrupt, SIGTERM, or shutdown_request
	// (mochictl stop / restart, carrying the exit code). SIGHUP is registered only
	// so its default action does not terminate the process.
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

	// Cap the whole drain and close under a deadline: libp2p's host Close can
	// block indefinitely on a busy public host, and systemd SIGKILLs at 90s.
	// SQLite is crash-safe, so a forced exit loses no committed data.
	const shutdown_grace = 30 * time.Second
	done := make(chan struct{})
	go func() {
		queue_drain(10 * time.Second) // outbound queue (bounded)
		peers_shutdown()              // bye to connected peers (bounded)
		// relay_shutdown and net_me.Close are both unbounded libp2p teardowns and on
		// a busy public host never quiesce. Give them a brief window, then move on -
		// peers already got goodbye and the OS reclaims sockets.
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
		// Backstop: reaching here means the drain itself overran. info, not warn -
		// the forced exit is the designed fallback, not operator-actionable.
		info("Shutdown exceeded %s; forcing exit", shutdown_grace)
		os.Exit(exit_code)
	}
	return exit_code
}
