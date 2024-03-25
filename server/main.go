// Comms server: Main
// Copyright Alistair Cunningham 2024

package main

import (
	"flag"
)

func main() {
	log_info("Starting")
	listen := flag.String("listen", "0.0.0.0", "libp2p IP address to listen on")
	port := flag.Int("port", 1443, "libp2p port to listen on")
	flag.Parse()

	db_init()

	libp2p_start(*listen, *port)
	log_info("Web listening on ':8080'")
	go web_start(":8080")

	log_info("Ready")
	select {}
	log_info("Terminating")
}
