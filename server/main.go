// Comms server: Main
// Copyright Alistair Cunningham 2024

package main

import (
	"flag"
)

var data_dir string

func main() {
	log_info("Starting")
	flag.StringVar(&data_dir, "directory", "/var/lib/comms", "Directory to store data in")
	flag.StringVar(&libp2p_listen, "listen", "0.0.0.0", "libp2p IP address to listen on")
	flag.IntVar(&libp2p_port, "port", 1443, "libp2p port to listen on")
	flag.IntVar(&web_port, "web", 8080, "Web port to listen on")
	flag.Parse()

	db_init()
	go peers_manager()
	libp2p_start()
	log_info("Web listening on ':%d'", web_port)
	go web_start()
	go queue_helper()

	log_info("Ready")
	select {}
	log_info("Terminating")
}
