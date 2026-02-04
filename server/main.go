// Mochi server: Main
// Copyright Alistair Cunningham 2024-2025

package main

import (
	"flag"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"time"
)

type Map map[string]any

var (
	build_version string
	cache_dir     string
	data_dir      string
	dev_apps_dir  string
	dev_reload    bool
	web_cache     bool
	email_host    string
	email_port    int
)

func main() {
	info("Mochi %s starting", build_version)

	// Platform-aware default paths
	default_config := "/etc/mochi/mochi.conf"
	default_cache := "/var/cache/mochi"
	default_data := "/var/lib/mochi"
	if runtime.GOOS == "windows" {
		local_app_data := os.Getenv("LOCALAPPDATA")
		if local_app_data == "" {
			local_app_data = filepath.Join(os.Getenv("USERPROFILE"), "AppData", "Local")
		}
		default_config = filepath.Join(local_app_data, "mochi", "mochi.conf")
		default_cache = filepath.Join(local_app_data, "mochi", "cache")
		default_data = filepath.Join(local_app_data, "mochi", "data")
	}

	var file string
	flag.StringVar(&file, "f", default_config, "Configuration file")
	flag.Parse()
	err := ini_load(file)
	if err != nil {
		warn("Unable to read configuration file: %v", err)
		os.Exit(1)
	}

	audit_init()
	audit_server_start(build_version)

	cache_dir = ini_string("directories", "cache", default_cache)
	data_dir = ini_string("directories", "data", default_data)
	dev_apps_dir = ini_string("development", "apps", "")
	dev_reload = ini_bool("development", "reload", false)
	web_cache = ini_bool("web", "cache", true)
	email_host = ini_string("email", "host", "127.0.0.1")
	email_port = ini_int("email", "port", 25)

	starlark_configure()
	db_start()
	passkey_init()
	if err := domains_load_certs(); err != nil {
		warn("Failed to load domain certificates: %v", err)
	}
	domains_init_acme()
	setting_set("server_started", itoa(int(now())))
	apps_start()
	p2p_start()
	go cache_manager()
	go entities_manager()
	go directory_manager()
	go peers_manager()
	go peers_publish()
	go queue_manager()
	go ratelimit_manager()
	go sessions_manager()
	go web_start()
	go apps_manager()
	go schedule_start()

	// Wait for shutdown signal (os.Interrupt works cross-platform)
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt)
	<-sig

	info("Shutdown signal received, stopping gracefully...")
	audit_server_stop()

	// Wait for queue to drain (with timeout)
	queue_drain(10 * time.Second)

	// Notify connected peers
	peers_shutdown()

	// Close P2P host
	if p2p_me != nil {
		p2p_me.Close()
	}

	audit_close()
	info("Shutdown complete")
}
