// Mochi server: Logging
// Copyright Alistair Cunningham 2024-2025

package main

import (
	"fmt"
	"log"
	"time"
)

func init() {
	log.SetFlags(0)
	log.SetOutput(new(logWriter))
}

type logWriter struct {
}

func (writer logWriter) Write(bytes []byte) (int, error) {
	return fmt.Print(time.Now().Format("2006-01-02 15:04:05.000000") + " " + string(bytes))
}

func debug(message string, values ...any) {
	out := fmt.Sprintf(message, values...)
	if len(out) > 1000 {
		log.Print(out[:1000] + "...\n")
	} else {
		log.Print(out + "\n")
	}
}

func info(message string, values ...any) {
	log.Printf(message+"\n", values...)
}

func warn(message string, values ...any) {
	out := fmt.Sprintf(message, values...)
	log.Print(out + "\n")

	admin := ini_string("email", "admin", "")
	if admin != "" {
		email_send(admin, "Mochi error", out)
	}
}
