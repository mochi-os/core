// Comms server: Main
// Copyright Alistair Cunningham 2024

package main

import (
	"flag"
	"gopkg.in/ini.v1"
)

type Map map[string]any

var (
	cache_dir string
	data_dir  string
)

func main() {
	log_info("Starting")

	var file string
	flag.StringVar(&file, "f", "/etc/comms/comms.conf", "Configuration file")
	flag.Parse()
	i, err := ini.Load(file)
	if err != nil {
		log_error("Unable to read configuration file: %v", err)
		return
	}

	cache_dir = i.Section("directories").Key("cache").MustString("/var/cache/comms")
	data_dir = i.Section("directories").Key("data").MustString("/var/lib/comms")

	new_install := db_start()
	go peers_manager()
	libp2p_start(i.Section("libp2p").Key("listen").MustString("0.0.0.0"), i.Section("libp2p").Key("port").MustInt(1443))
	go attachments_manager()
	go identities_manager()
	go events_manager()
	go cache_manager()
	go web_start(i.Section("web").Key("listen").MustString("0.0.0.0"), i.Section("web").Key("port").MustInt(8080))

	if new_install {
		go directory_download()
	}

	select {}
}
