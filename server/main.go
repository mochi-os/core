// Comms server: Main
// Copyright Alistair Cunningham 2024

package main

import (
	"flag"
)

func main() {
	log_info("Starting")
	port := flag.Int("libp2p", 20443, "libp2p port to listen on")
	flag.Parse()

	db_init()

	libp2p_start(*port)
	log_info("Web listening on ':8080'")
	go web_start(":8080")

	log_info("Ready")
	select {}
	log_info("Terminating")
}
