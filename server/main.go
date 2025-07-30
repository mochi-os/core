// Mochi server: Main
// Copyright Alistair Cunningham 2024

package main

import (
	"flag"
	"gopkg.in/ini.v1"
	"strings"
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
	c, err := ini.Load(file)
	if err != nil {
		log_error("Unable to read configuration file: %v", err)
		return
	}

	cache_dir = c.Section("directories").Key("cache").MustString("/var/cache/mochi")
	data_dir = c.Section("directories").Key("data").MustString("/var/lib/mochi")
	email_from = c.Section("email").Key("from").MustString("mochi-server@localhost")
	email_host = c.Section("email").Key("host").MustString("127.0.0.1")
	email_port = c.Section("email").Key("port").MustInt(25)

	new_install := db_start()
	go peers_manager()
	libp2p_start(c.Section("libp2p").Key("listen").MustString("0.0.0.0"), c.Section("libp2p").Key("port").MustInt(1443))
	go attachments_manager()
	go entities_manager()
	go events_manager()
	go cache_manager()
	go web_start(c.Section("web").Key("listen").MustString("0.0.0.0"), c.Section("web").Key("port").MustInt(8080), strings.Split(c.Section("web").Key("domains").MustString(""), ","), c.Section("web").Key("debug").MustBool(false))

	if new_install {
		go directory_download()
	}

	select {}
}
