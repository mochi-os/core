// Mochi server: Windows Service Control Manager integration.
// Copyright Alistair Cunningham 2026
//
// When mochi-server is started by the SCM (systemctl-equivalent on Windows),
// IsWindowsService returns true and we hand control to svc.Run, which calls
// our Execute method below. Execute starts main_serve in a goroutine and
// translates SCM Stop/Shutdown commands into pushes on shutdown_request,
// the same channel mochictl already uses for graceful shutdown.
//
// In interactive (developer console) mode IsWindowsService returns false
// and main() falls through to the normal main_serve path — same as Unix.

//go:build windows

package main

import (
	"log"
	"os"
	"path/filepath"
	"time"

	"golang.org/x/sys/windows/svc"
)

const windows_service_name = "mochi-server"

type mochi_service struct{}

// Execute is the SCM control loop. Returns when the service is fully stopped.
func (m *mochi_service) Execute(args []string, r <-chan svc.ChangeRequest, status chan<- svc.Status) (svcSpecificEC bool, exitCode uint32) {
	const accept = svc.AcceptStop | svc.AcceptShutdown

	status <- svc.Status{State: svc.StartPending}

	done := make(chan int, 1)
	ready := func() {
		status <- svc.Status{State: svc.Running, Accepts: accept}
	}
	go func() {
		done <- main_serve(ready)
	}()

	for {
		select {
		case c := <-r:
			switch c.Cmd {
			case svc.Interrogate:
				// Repeating the status twice is the standard pattern — some SCM
				// versions miss the first send right after Interrogate.
				status <- c.CurrentStatus
				time.Sleep(100 * time.Millisecond)
				status <- c.CurrentStatus
			case svc.Stop, svc.Shutdown:
				status <- svc.Status{State: svc.StopPending}
				select {
				case shutdown_request <- 0:
				default:
					// already requested
				}
				code := <-done
				status <- svc.Status{State: svc.Stopped}
				return false, uint32(code)
			}
		case code := <-done:
			// main_serve exited on its own (e.g. config error during startup).
			status <- svc.Status{State: svc.Stopped}
			return false, uint32(code)
		}
	}
}

// windows_service_run hands off to the SCM if we were launched as a service.
// Returns true after svc.Run returns (whether successfully or with an error)
// so main() knows not to fall through to the interactive path.
func windows_service_run() bool {
	is_service, err := svc.IsWindowsService()
	if err != nil || !is_service {
		return false
	}
	if err := svc.Run(windows_service_name, &mochi_service{}); err != nil {
		// SCM logs are inaccessible here; write to stderr so a manual
		// `mochi-server.exe` invocation surfaces the problem.
		log.Printf("Windows service handler exited with error: %v", err)
	}
	return true
}

// windows_service_redirect_logs sends stdout/stderr to a file so the SCM
// (which has no console) can still produce a log. No-op when running
// interactively (a console is attached) — preserves the dev experience.
func windows_service_redirect_logs() {
	is_service, err := svc.IsWindowsService()
	if err != nil || !is_service {
		return
	}
	log_dir := filepath.Join(data_dir, "logs")
	if err := os.MkdirAll(log_dir, 0o755); err != nil {
		return
	}
	path := filepath.Join(log_dir, "mochi-server.log")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return
	}
	os.Stdout = f
	os.Stderr = f
	log.SetOutput(f)
}
