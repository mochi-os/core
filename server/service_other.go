// Mochi server: Service stubs for non-Windows platforms.
// Copyright Alistair Cunningham 2026

//go:build !windows

package main

// windows_service_run is a no-op outside Windows. Returns false so main()
// falls through to the interactive / Unix daemon path.
func windows_service_run() bool { return false }

// windows_service_redirect_logs is a no-op outside Windows.
func windows_service_redirect_logs() {}
