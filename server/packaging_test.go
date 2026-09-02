// Mochi server: what the packages install and how the service is supervised.
//
// The unit file, the package scripts and the compose example are plain text
// the Go suite would otherwise never look at. Each test here pins one
// property of them: the unit is sandboxed without the two directives this
// binary cannot survive, it grants the bind capability to the process, and it
// carries no dead lines; every package post-install restores the group
// traverse bit the recursive chmod removes; the rpm starts and restarts the
// service and names the real licence; the deb declares what its scripts need
// and stages its directories at sane modes; and the compose example follows
// the restart contract mochictl relies on, from a track tag.
//
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// packaging_read returns a file under the core repository root.
func packaging_read(t *testing.T, parts ...string) string {
	t.Helper()
	path := filepath.Join(append([]string{".."}, parts...)...)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("%s: %v", path, err)
	}
	return string(data)
}

// packaging_unit returns the systemd unit as a map of directive to values.
func packaging_unit(t *testing.T) map[string][]string {
	t.Helper()
	directives := map[string][]string{}
	for _, line := range strings.Split(packaging_read(t, "install", "etc", "systemd", "system", "mochi-server.service"), "\n") {
		if key, value, found := strings.Cut(line, "="); found && !strings.HasPrefix(line, "#") {
			directives[key] = append(directives[key], value)
		}
	}
	return directives
}

// packaging_section returns the body of one rpm scriptlet, from its directive
// to the next one.
func packaging_section(spec, name string) string {
	_, after, found := strings.Cut(spec, "\n"+name+"\n")
	if !found {
		return ""
	}
	if end := regexp.MustCompile(`(?m)^%`).FindStringIndex(after); end != nil {
		return after[:end[0]]
	}
	return after
}

// TestUnitIsSandboxed. The unit ran the marketplace-app runtime and the public
// libp2p listener with no sandbox at all. The world unit's set is the model,
// minus MemoryDenyWriteExecute (the UPX-packed binary and wazero cannot run
// under it) and ProtectProc (the admin socket reads the client's
// /proc/<pid>/status), plus AF_NETLINK, without which interface enumeration
// fails and the server silently discovers no peers.
func TestUnitIsSandboxed(t *testing.T) {
	unit := packaging_unit(t)
	for _, directive := range []string{"NoNewPrivileges", "ProtectSystem", "ProtectHome", "PrivateTmp", "ProtectKernelTunables", "ProtectControlGroups", "RestrictNamespaces", "RestrictSUIDSGID"} {
		if len(unit[directive]) == 0 {
			t.Errorf("unit carries no %s=", directive)
		}
	}
	if got := unit["ProtectSystem"]; len(got) != 1 || got[0] != "strict" {
		t.Errorf("ProtectSystem = %v, want strict", got)
	}
	if !strings.Contains(strings.Join(unit["ReadWritePaths"], " "), "/var/lib/mochi") {
		t.Errorf("ReadWritePaths %v does not include the data directory", unit["ReadWritePaths"])
	}
	families := strings.Join(unit["RestrictAddressFamilies"], " ")
	for _, family := range []string{"AF_UNIX", "AF_INET", "AF_INET6", "AF_NETLINK"} {
		if !strings.Contains(families, family) {
			t.Errorf("RestrictAddressFamilies %q lacks %s", families, family)
		}
	}
	for _, forbidden := range []string{"MemoryDenyWriteExecute", "ProtectProc", "ProcSubset"} {
		if len(unit[forbidden]) != 0 {
			t.Errorf("unit carries %s=, which this binary cannot run under", forbidden)
		}
	}
}

// TestUnitGrantsTheBindCapabilityToTheProcess. The unit wrote the capability
// onto the binary with setcap before every start, which drifted the installed
// file, needed root and xattrs, and gave the capability to anyone executing
// the binary.
func TestUnitGrantsTheBindCapabilityToTheProcess(t *testing.T) {
	unit := packaging_unit(t)
	if got := unit["AmbientCapabilities"]; len(got) != 1 || got[0] != "CAP_NET_BIND_SERVICE" {
		t.Errorf("AmbientCapabilities = %v, want CAP_NET_BIND_SERVICE", got)
	}
	if got := unit["CapabilityBoundingSet"]; len(got) != 1 || got[0] != "CAP_NET_BIND_SERVICE" {
		t.Errorf("CapabilityBoundingSet = %v, want CAP_NET_BIND_SERVICE", got)
	}
	if len(unit["ExecStartPre"]) != 0 {
		t.Errorf("unit still runs ExecStartPre=%v", unit["ExecStartPre"])
	}
}

// TestUnitCarriesNoDeadLines. PIDFile= on a Type=simple unit for a server that
// writes no pid file, and LimitSTACK=infinity, which switches the process to
// the legacy bottom-up address-space layout for no measured reason.
func TestUnitCarriesNoDeadLines(t *testing.T) {
	unit := packaging_unit(t)
	for _, dead := range []string{"PIDFile", "LimitSTACK"} {
		if len(unit[dead]) != 0 {
			t.Errorf("unit still carries %s=%v", dead, unit[dead])
		}
	}
}

// TestPackagesRestoreGroupTraverse. Every post-install closes the data
// directory with a recursive go-rwx, which left it 0700: a member of the mochi
// group could never reach the admin socket the documentation tells them to
// use. The traverse bit must be restored after the recursive chmod, in every
// package.
func TestPackagesRestoreGroupTraverse(t *testing.T) {
	for name, script := range map[string]string{
		"deb postinst":    packaging_read(t, "build", "deb", "DEBIAN", "postinst"),
		"rpm %post":       packaging_section(packaging_read(t, "build", "rpm", "mochi-server.spec"), "%post"),
		"pkg postinstall": packaging_read(t, "build", "pkg", "scripts", "postinstall"),
	} {
		closing := strings.Index(script, "chmod -R go-rwx /var/lib/mochi")
		opening := strings.Index(script, "chmod 0710 /var/lib/mochi")
		if closing < 0 {
			t.Errorf("%s no longer closes the data directory", name)
			continue
		}
		if opening < 0 {
			t.Errorf("%s never restores group traverse on the data directory", name)
			continue
		}
		if opening < closing {
			t.Errorf("%s restores group traverse before the recursive chmod removes it", name)
		}
	}
}

// TestPackagesClearTheFileCapability. Installs that ran the old unit carry a
// file capability on the binary; the Linux post-installs remove it once.
func TestPackagesClearTheFileCapability(t *testing.T) {
	for name, script := range map[string]string{
		"deb postinst": packaging_read(t, "build", "deb", "DEBIAN", "postinst"),
		"rpm %post":    packaging_section(packaging_read(t, "build", "rpm", "mochi-server.spec"), "%post"),
	} {
		if !strings.Contains(script, "setcap -r /usr/sbin/mochi-server") {
			t.Errorf("%s does not clear the file capability", name)
		}
	}
}

// TestRpmStartsAndRestarts. The rpm enabled the unit but never started it, and
// an upgrade left the old binary running until reboot.
func TestRpmStartsAndRestarts(t *testing.T) {
	spec := packaging_read(t, "build", "rpm", "mochi-server.spec")
	if post := packaging_section(spec, "%post"); !strings.Contains(post, "systemctl start mochi-server") {
		t.Errorf("%%post never starts the service:\n%s", post)
	}
	if postun := packaging_section(spec, "%postun"); !strings.Contains(postun, "systemctl try-restart mochi-server") {
		t.Errorf("%%postun never restarts the service on upgrade:\n%s", postun)
	}
}

// TestRpmNamesTheLicence. The spec said Proprietary for AGPL-3.0-only code.
func TestRpmNamesTheLicence(t *testing.T) {
	spec := packaging_read(t, "build", "rpm", "mochi-server.spec")
	match := regexp.MustCompile(`(?m)^License:\s*(.*)$`).FindStringSubmatch(spec)
	if match == nil || !strings.Contains(match[1], "AGPL-3.0-only") {
		t.Errorf("License = %v, want AGPL-3.0-only", match)
	}
}

// TestDebDeclaresItsDependencies. preinst needs adduser, postinst needs
// systemctl and setcap, and nothing was declared.
func TestDebDeclaresItsDependencies(t *testing.T) {
	control := packaging_read(t, "build", "deb", "DEBIAN", "control")
	fields := map[string]string{}
	for _, line := range strings.Split(control, "\n") {
		if key, value, found := strings.Cut(line, ":"); found && !strings.HasPrefix(line, " ") {
			fields[key] = strings.TrimSpace(value)
		}
	}
	for _, need := range []string{"adduser", "systemd", "libcap2-bin"} {
		if !strings.Contains(fields["Depends"], need) {
			t.Errorf("Depends %q lacks %s", fields["Depends"], need)
		}
	}
	for _, field := range []string{"Section", "Priority", "Homepage"} {
		if fields[field] == "" {
			t.Errorf("control has no %s field", field)
		}
	}
}

// TestDebStagingModes. The staging tree was created with -m 0775, so the deb
// shipped /usr/bin and /usr/sbin group-writable.
func TestDebStagingModes(t *testing.T) {
	makefile := packaging_read(t, "Makefile")
	if strings.Contains(makefile, "mkdir -p -m 0775") {
		t.Errorf("a deb recipe still creates its staging tree with -m 0775")
	}
	for _, build := range []string{"build_linux_amd64", "build_linux_arm64", "build_linux_armhf"} {
		if !strings.Contains(makefile, "chmod 0755 $("+build+") $("+build+")/usr $("+build+")/usr/bin $("+build+")/usr/sbin") {
			t.Errorf("%s recipe does not set the system directories to 0755", build)
		}
		if !strings.Contains(makefile, "chmod 0750 $("+build+")/var/cache/mochi $("+build+")/var/lib/mochi") {
			t.Errorf("%s recipe does not set the mochi directories to 0750", build)
		}
	}
}

// TestComposeExampleFollowsTheRestartContract. mochictl stop exits 0 to stay
// down and restart exits 75 to come back; only on-failure honours that.
func TestComposeExampleFollowsTheRestartContract(t *testing.T) {
	compose := packaging_read(t, "build", "docker", "docker-compose.example.yml")
	if !strings.Contains(compose, "restart: on-failure") {
		t.Errorf("compose example does not restart on-failure")
	}
	if strings.Contains(compose, "unless-stopped") {
		t.Errorf("compose example still restarts unless-stopped")
	}
}

// TestComposeExamplePinsATrackTag. latest moves on every push; production is
// the track name the update manifest uses.
func TestComposeExamplePinsATrackTag(t *testing.T) {
	compose := packaging_read(t, "build", "docker", "docker-compose.example.yml")
	if strings.Contains(compose, "mochi-server:latest") {
		t.Errorf("compose example pins :latest")
	}
	if !strings.Contains(compose, "mochi-server:production") {
		t.Errorf("compose example does not pin the production tag")
	}
}
