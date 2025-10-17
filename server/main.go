// Mochi server: Main
// Copyright Alistair Cunningham 2024-2025

package main

import (
	"flag"
	"os"
)

type Map map[string]any

var (
	build_version string
	cache_dir     string
	data_dir      string
	email_from    string
	email_host    string
	email_port    int
)

func main() {
	info("Mochi %s starting", build_version)

	var file string
	flag.StringVar(&file, "f", "/etc/mochi/mochi.conf", "Configuration file")
	flag.Parse()
	err := ini_load(file)
	if err != nil {
		warn("Unable to read configuration file: %v", err)
		os.Exit(1)
	}

	cache_dir = ini_string("directories", "cache", "/var/cache/mochi")
	data_dir = ini_string("directories", "data", "/var/lib/mochi")
	email_from = ini_string("email", "from", "mochi-server@localhost")
	email_host = ini_string("email", "host", "127.0.0.1")
	email_port = ini_int("email", "port", 25)

	new_install := db_start()
	apps_start()
	p2p_start()
	go cache_manager()
	go entities_manager()
	go peers_manager()
	go peers_publish()
	go queue_manager()
	go web_start()
	//TODO Enable and test apps manager once wasabi is running 0.2.
	//go apps_manager()

	if new_install {
		go directory_download()
	}

	select {}
}
