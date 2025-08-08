// Mochi server: Main
// Copyright Alistair Cunningham 2024-2025

package main

import (
	"flag"
)

type Map map[string]any

var (
	cache_dir  string
	data_dir   string
	email_from string
	email_host string
	email_port int
)

func main() {
	log_info("Starting")

	var file string
	flag.StringVar(&file, "f", "/etc/mochi/mochi.conf", "Configuration file")
	flag.Parse()
	err := ini_load(file)
	if err != nil {
		log_error("Unable to read configuration file: %v", err)
		return
	}

	cache_dir = ini_string("directories", "cache", "/var/cache/mochi")
	data_dir = ini_string("directories", "data", "/var/lib/mochi")
	email_from = ini_string("email", "from", "mochi-server@localhost")
	email_host = ini_string("email", "host", "127.0.0.1")
	email_port = ini_int("email", "port", 25)

	new_install := db_start()
	libp2p_start()
	go attachments_manager()
	go entities_manager()
	go events_manager()
	go cache_manager()
	go web_start()

	if new_install {
		go directory_download()
	}

	select {}
}
