// Mochi server: signals on Windows (interrupt only — no SIGHUP).
// Copyright Alistair Cunningham 2026

//go:build windows

package main

import "os"

func extra_signals() []os.Signal { return nil }

func is_ignorable_signal(s os.Signal) bool { return false }
