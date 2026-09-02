// mochictl: supervisor detection + shell-out for `mochictl start`.
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the Mochi
// Application Interface Exception - see license.txt and license-exception.md.
//
// The server is not running, so there is no admin socket to talk to: detect the
// supervisor (systemd, Docker, or none) and shell to it.

//go:build linux

package main

import (
	"fmt"
	"os"
	"os/exec"
	"strings"
)

// systemctl_path returns the absolute path to systemctl, or "" when it is not
// where a systemd host puts it. Not exec.LookPath: mochictl runs as root and
// inherits PATH, so a writable directory in it would choose the binary.
func systemctl_path() string {
	for _, candidate := range []string{"/usr/bin/systemctl", "/bin/systemctl", "/usr/sbin/systemctl", "/sbin/systemctl"} {
		if information, err := os.Stat(candidate); err == nil && !information.IsDir() {
			return candidate
		}
	}
	return ""
}

// supervisor_start attempts to start the mochi-server unit via systemctl.
// Falls through with a helpful error if systemd isn't the supervisor.
func supervisor_start() error {
	switch detect() {
	case "systemd":
		systemctl := systemctl_path()
		if systemctl == "" {
			return fmt.Errorf("systemd is the supervisor but systemctl is not at any expected absolute path")
		}
		cmd := exec.Command(systemctl, "start", "mochi-server")
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
	cgroup, _ := os.ReadFile("/proc/1/cgroup")
	comm, _ := os.ReadFile("/proc/1/comm")
	_, err := os.Stat("/.dockerenv")
	return supervisor(string(cgroup), err == nil, strings.TrimSpace(string(comm)), systemctl_path() != "")
}

// supervisor decides from what PID 1 looks like. A container is recognised
// by the marker file Docker plants at the root, by a cgroup v1 path naming
// the runtime, or by the bare `0::/` a cgroup v2 container shows with a PID 1
// that is not systemd; that last form is what every current host presents,
// and it matched nothing. A systemd host shows `0::/init.scope`.
func supervisor(cgroup string, dockerenv bool, comm string, systemctl bool) string {
	if dockerenv || strings.Contains(cgroup, "docker") || strings.Contains(cgroup, "containerd") || strings.Contains(cgroup, "kubepods") {
		return "docker"
	}
	if strings.TrimSpace(cgroup) == "0::/" && comm != "systemd" {
		return "docker"
	}
	if systemctl && comm == "systemd" {
		return "systemd"
	}
	return ""
}
