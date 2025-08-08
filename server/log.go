// Mochi server: Logging
// Copyright Alistair Cunningham 2024-2025

package main

import (
	"fmt"
	"log"
)

func log_debug(message string, values ...any) {
	s := fmt.Sprintf(message, values...)
	if len(s) > 1000 {
		log.Print(s[:1000] + "...\n")
	} else {
		log.Print(s + "\n")
	}
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
