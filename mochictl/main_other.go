// mochictl: stub for non-Linux platforms.
// Copyright Alistair Cunningham 2026
//
// mochictl is Linux-only by design (UDS peer credentials, systemd lifecycle,
// supplementary group lookup via /proc). On other platforms the binary
// prints a clear message and exits non-zero so package builds and `go vet`
// don't fail, but the tool isn't usable.

//go:build !linux

package main

import (
	"fmt"
	"os"
)

func main() {
	fmt.Fprintln(os.Stderr, "mochictl is only supported on Linux. To manage a remote Mochi server, SSH to the host and run mochictl there.")
	os.Exit(1)
}
