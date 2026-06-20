// mochictl: supervisor detection + shell-out for `mochictl start`.
// Copyright © 2026 Mochi OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.
//
// `mochictl start` is awkward by definition — if the server isn't running,
// the UDS socket doesn't exist and we have nothing to talk to. Best we can
// do is detect the supervisor (systemd, Docker, or none) and shell to it,
// or error with a useful hint.

//go:build linux

package main

import (
	"fmt"
	"os"
	"os/exec"
	"strings"
)

// supervisor_start attempts to start the mochi-server unit via systemctl.
// Falls through with a helpful error if systemd isn't the supervisor.
func supervisor_start() error {
	switch detect() {
	case "systemd":
		cmd := exec.Command("systemctl", "start", "mochi-server")
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		if err := cmd.Run(); err != nil {
			return fmt.Errorf("systemctl start mochi-server: %w", err)
		}
		return nil
	case "docker":
		return fmt.Errorf("running inside a Docker container — use `docker start <container>` from the host instead")
	default:
		return fmt.Errorf("no supervisor detected; run `mochi-server -f /etc/mochi/mochi.conf` directly")
	}
}

// detect reports the supervisor environment mochictl is running in.
// Returns "systemd", "docker", or "" (none / unknown).
func detect() string {
	// Docker check first: if PID 1's cgroup mentions docker / containerd /
	// kubepods, we're in a container even if systemctl happens to exist.
	if data, err := os.ReadFile("/proc/1/cgroup"); err == nil {
		s := string(data)
		if strings.Contains(s, "docker") || strings.Contains(s, "containerd") || strings.Contains(s, "kubepods") {
			return "docker"
		}
	}

	// systemd check: systemctl present AND PID 1 is "systemd".
	if _, err := exec.LookPath("systemctl"); err == nil {
		if comm, err := os.ReadFile("/proc/1/comm"); err == nil && strings.TrimSpace(string(comm)) == "systemd" {
			return "systemd"
		}
	}
	return ""
}
