// Mochi server: admin stubs for non-Linux platforms.
// Copyright Alistair Cunningham 2026
//
// mochictl and the UDS admin listener are Linux-only by design. See
// claude/plans/mochictl.md → "Platform support" for the rationale and the
// future cross-platform escape hatch (opt-in HTTP-with-token admin).

//go:build !linux

package main

// admin_start is a no-op on non-Linux. The admin endpoints are intentionally
// unreachable on Windows / macOS / BSD.
func admin_start() error {
	return nil
}
