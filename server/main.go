// Mochi server: Main
// Copyright Alistair Cunningham 2024-2025

package main

import (
	"flag"
	"os"
	"os/signal"
	"syscall"
	"time"
)

type Map map[string]any

var (
	build_version string
	cache_dir     string
	data_dir      string
	email_host    string
	email_port    int
)

func main() {
	info("Mochi %s starting", build_version)

	var file string
	var migrate bool
	flag.StringVar(&file, "f", "/etc/mochi/mochi.conf", "Configuration file")
	flag.BoolVar(&migrate, "migrate-attachments", false, "Migrate attachments from old system to new per-app system")
	flag.Parse()
	err := ini_load(file)
	if err != nil {
		warn("Unable to read configuration file: %v", err)
		os.Exit(1)
	}

	cache_dir = ini_string("directories", "cache", "/var/cache/mochi")
	data_dir = ini_string("directories", "data", "/var/lib/mochi")
	email_host = ini_string("email", "host", "127.0.0.1")
	email_port = ini_int("email", "port", 25)

	if migrate {
		migrate_attachments()
		return
	}

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
	go web_start()
	//go apps_manager()

	// Wait for shutdown signal
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig

	info("Shutdown signal received, stopping gracefully...")

	// Wait for queue to drain (with timeout)
	queue_drain(10 * time.Second)

	// Notify connected peers
	peers_shutdown()

	// Close P2P host
	if p2p_me != nil {
		p2p_me.Close()
	}

	info("Shutdown complete")
}
