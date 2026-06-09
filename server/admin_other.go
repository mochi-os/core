// Mochi server: admin stubs for platforms without a UDS admin listener.
// Copyright Alistair Cunningham 2026
//
// The UDS admin listener and mochictl are supported on Linux and macOS, whose
// Unix-socket peer credentials (SO_PEERCRED / LOCAL_PEERCRED) authenticate the
// caller. Windows and other platforms have no equivalent peer-credential
// mechanism, so admin_start is a no-op there; supporting them would need a
// different transport and auth model (e.g. a named pipe with a security
// descriptor, or HTTP with a token).

//go:build !linux && !darwin

package main

// admin_start is a no-op on platforms without UDS peer-credential auth. The
// admin endpoints are intentionally unreachable there (e.g. Windows).
func admin_start() error {
	return nil
}
