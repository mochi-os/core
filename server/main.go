// Comms server: Main
// Copyright Alistair Cunningham 2024

package main

import (
	"flag"
)

type Map map[string]any

var data_dir string

func main() {
	var port int
	log_info("Starting")
	flag.StringVar(&data_dir, "data", "/var/lib/comms", "Directory to store data in")
	flag.StringVar(&libp2p_listen, "listen", "0.0.0.0", "libp2p IP address to listen on")
	flag.IntVar(&libp2p_port, "port", 1443, "libp2p port to listen on")
	flag.IntVar(&port, "web", 8080, "Web port to listen on")
	flag.Parse()
	domains := flag.Args()

	new_install := db_start()
	//apps_start()
	go web_start(port, domains)
	go peers_manager()
	libp2p_start()
	go identities_manager()
	go events_manager()
	if new_install {
		go directory_download()
	}

	select {}
}
