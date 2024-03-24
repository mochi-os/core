// Comms server: Logging
// Copyright Alistair Cunningham 2024

package main

import (
	"log"
)

func log_debug(message string, values ...any) {
	log.Printf(message+"\n", values...)
}

func log_error(message string, values ...any) {
	log.Fatalf(message+"\n", values...)
}

func log_info(message string, values ...any) {
	log.Printf(message+"\n", values...)
}

func log_warn(message string, values ...any) {
	log.Printf(message+"\n", values...)
}
