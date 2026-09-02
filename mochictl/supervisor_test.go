// mochictl: which supervisor `mochictl start` should talk to.
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

//go:build linux

package main

import (
	"os"
	"strings"
	"testing"
)

// TestSupervisorRecognisesContainers. On cgroup v2 a container's PID 1 shows
// `0::/` and none of the runtime names the old check looked for, so every
// current Docker host was told to run the server by hand. The shapes below
// are what this host and a container on it presented.
func TestSupervisorRecognisesContainers(t *testing.T) {
	cases := []struct {
		name      string
		cgroup    string
		dockerenv bool
		comm      string
		systemctl bool
		want      string
	}{
		{"cgroup v2 container", "0::/\n", false, "mochi-server", false, "docker"},
		{"cgroup v2 container with the marker", "0::/\n", true, "mochi-server", false, "docker"},
		{"marker alone", "", true, "mochi-server", true, "docker"},
		{"cgroup v1 container", "12:memory:/docker/abc\n", false, "mochi-server", true, "docker"},
		{"kubernetes pod", "0::/kubepods/burstable/pod1\n", false, "mochi-server", false, "docker"},
		{"systemd host", "0::/init.scope\n", false, "systemd", true, "systemd"},
		{"systemd inside a system container", "0::/\n", false, "systemd", true, "systemd"},
		{"no supervisor", "0::/init.scope\n", false, "init", false, ""},
		{"systemd without systemctl", "0::/init.scope\n", false, "systemd", false, ""},
	}
	for _, c := range cases {
		if got := supervisor(c.cgroup, c.dockerenv, c.comm, c.systemctl); got != c.want {
			t.Errorf("%s: got %q, want %q", c.name, got, c.want)
		}
	}
}

// TestSystemctlIsNotResolvedThroughPath is #76.
func TestSystemctlIsNotResolvedThroughPath(t *testing.T) {
	data, err := os.ReadFile("supervisor.go")
	if err != nil {
		t.Fatalf("reading supervisor.go: %v", err)
	}
	source := string(data)

	if strings.Contains(source, `exec.LookPath("systemctl")`) {
		t.Error("systemctl is still resolved through PATH; mochictl runs as root and inherits PATH from its caller")
	}
	if strings.Contains(source, `exec.Command("systemctl"`) {
		t.Error("systemctl is still executed by bare name, which searches PATH")
	}
	if !strings.Contains(source, `"/usr/bin/systemctl"`) {
		t.Error("systemctl_path does not try an absolute path")
	}
}

// TestSystemctlPathAcceptsOnlyAbsolutePaths: whatever it returns is executed
// as root, so it must be a path, not a name.
func TestSystemctlPathAcceptsOnlyAbsolutePaths(t *testing.T) {
	got := systemctl_path()
	if got == "" {
		t.Skip("no systemctl on this host; nothing to check")
	}
	if !strings.HasPrefix(got, "/") {
		t.Errorf("systemctl_path returned %q, which is not absolute", got)
	}
	if information, err := os.Stat(got); err != nil || information.IsDir() {
		t.Errorf("systemctl_path returned %q, which is not a file: %v", got, err)
	}
}
