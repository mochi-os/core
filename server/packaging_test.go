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
	"os/exec"
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

// packaging_stage is the staging root the dry runs are handed, so no make -n
// here ever creates one.
const packaging_stage = "/tmp/mochi-stage-probe"

// packaging_dry runs `make -n` on the core Makefile with a fixed staging root
// and returns what it would run.
func packaging_dry(t *testing.T, arguments ...string) string {
	t.Helper()
	command := exec.Command("make", append(append([]string{"-n", "-C", ".."}, arguments...), "STAGE="+packaging_stage)...)
	command.Env = append(os.Environ(), "MAKEFLAGS=")
	out, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("make -n %v: %v\n%s", arguments, err, out)
	}
	return string(out)
}

// TestReleaseStagesUnderAPrivateRoot. Every package was staged under a fixed
// name in /tmp, where any local account could pre-create the directory and
// plant files that dpkg-deb would pack and gpg would sign. Staging now happens
// under one private mktemp root the release hands to its sub-makes, and a
// staging directory that already exists is an error.
func TestReleaseStagesUnderAPrivateRoot(t *testing.T) {
	makefile := packaging_read(t, "Makefile")
	if fixed := regexp.MustCompile(`(?m)^\w+\s*=\s*/tmp/`).FindString(makefile); fixed != "" {
		t.Errorf("a release path is still fixed under /tmp: %q", strings.TrimSpace(fixed))
	}
	if !strings.Contains(makefile, "mktemp -d /tmp/mochi-release.") {
		t.Errorf("no private staging root is made")
	}
	for _, phase := range []string{"release-build STAGE=$(stage)", "release-publish STAGE=$(stage)", "release-clean STAGE=$(stage)"} {
		if !strings.Contains(makefile, phase) {
			t.Errorf("release does not hand its root to the sub-make: %s", phase)
		}
	}
	out := packaging_dry(t, "-B", "deb-amd64")
	if !strings.Contains(out, "mkdir "+packaging_stage+"/mochi-server_") {
		t.Errorf("the deb staging directory is not created with a plain mkdir under the root:\n%s", out)
	}
	if strings.Contains(out, " /tmp/mochi-server_") {
		t.Errorf("the deb recipe still names a fixed /tmp path")
	}
	// A rule named by its output path expands the root when the Makefile is
	// read, so a build that stages nothing would make a root on every call.
	before, _ := filepath.Glob("/tmp/mochi-release.*")
	command := exec.Command("make", "-n", "-B", "-C", "..", "../bin/mochi-server")
	command.Env = append(os.Environ(), "MAKEFLAGS=")
	if out, err := command.CombinedOutput(); err != nil {
		t.Fatalf("make -n ../bin/mochi-server: %v\n%s", err, out)
	}
	after, _ := filepath.Glob("/tmp/mochi-release.*")
	if len(after) > len(before) {
		t.Errorf("a build that stages nothing made a staging root: %v", after)
		for _, root := range after[len(before):] {
			os.RemoveAll(root)
		}
	}
}

// TestManPagesInstallOnlyOnRequest. Building a man page copied it into the
// invoking user's home directory, and every package rule depends on the man
// pages, so a release wrote outside the tree. The copy is its own target.
func TestManPagesInstallOnlyOnRequest(t *testing.T) {
	if out := packaging_dry(t, "-B", "../bin/mochictl.1"); strings.Contains(out, ".local/share/man") {
		t.Errorf("building the man page still installs it:\n%s", out)
	}
	out := packaging_dry(t, "-B", "man-install")
	for _, page := range []string{"man1/", "man5/", "man7/", "man8/"} {
		if !strings.Contains(out, ".local/share/man/"+page) {
			t.Errorf("man-install does not copy into %s", page)
		}
	}
}

// TestAptIndexNeverListsItself. The index script hashed every file under the
// suite, including the previous run's InRelease, and then signed that list into
// the new InRelease: a signed statement about itself that could never be true.
func TestAptIndexNeverListsItself(t *testing.T) {
	script := packaging_read(t, "build", "scripts", "apt-repository-update")
	if !strings.Contains(script, "rm -f Release InRelease Release.gpg") {
		t.Errorf("the previous signed outputs are not removed before hashing")
	}
	start := strings.Index(script, "do_hash() {")
	end := strings.Index(script[start:], "\n}\n")
	if start < 0 || end < 0 {
		t.Fatalf("do_hash not found in the script")
	}
	function := script[start : start+end+3]
	suite := t.TempDir()
	for _, name := range []string{"Packages", "InRelease", "Release.gpg", "Release.base"} {
		if err := os.WriteFile(filepath.Join(suite, name), []byte(name), 0644); err != nil {
			t.Fatal(err)
		}
	}
	command := exec.Command("sh", "-c", function+"\ndo_hash SHA256 sha256sum")
	command.Dir = suite
	if out, err := command.CombinedOutput(); err != nil {
		t.Fatalf("do_hash: %v\n%s", err, out)
	}
	release, err := os.ReadFile(filepath.Join(suite, "Release"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(release), " Packages") {
		t.Errorf("Release does not list Packages:\n%s", release)
	}
	for _, signed := range []string{"InRelease", "Release.gpg"} {
		if strings.Contains(string(release), " "+signed) {
			t.Errorf("Release lists %s, a signed output of the same run:\n%s", signed, release)
		}
	}
}

// TestReleasePublishesTheAptClientFiles. The apt list, sources and keyring
// lived only in the untracked packages tree, so a tree wipe or a key rotation
// left the apt channel stale with nothing to diff.
func TestReleasePublishesTheAptClientFiles(t *testing.T) {
	list := packaging_read(t, "build", "apt", "mochi.list")
	if !strings.Contains(list, "signed-by=/etc/apt/keyrings/mochi.gpg") || !strings.Contains(list, "https://packages.mochi-os.org/apt stable main") {
		t.Errorf("mochi.list does not name the keyring and the suite:\n%s", list)
	}
	sources := packaging_read(t, "build", "apt", "mochi.sources")
	if !strings.Contains(sources, "Signed-By: /etc/apt/keyrings/mochi.gpg") || !strings.Contains(sources, "Suites: stable") {
		t.Errorf("mochi.sources does not name the keyring and the suite:\n%s", sources)
	}
	out := packaging_dry(t, "release-publish")
	if !strings.Contains(out, "cp build/apt/mochi.list build/apt/mochi.sources ../packages/apt/") {
		t.Errorf("release-publish does not copy the client files")
	}
	if !strings.Contains(out, "> ../packages/apt/mochi.gpg") {
		t.Errorf("release-publish does not export the binary keyring")
	}
}

// TestPkgBuilderFailsOnArchiveErrors. The cpio pipelines discarded stderr
// under a set -e with no pipefail, so an unreadable file produced a short
// payload that was signed and shipped.
func TestPkgBuilderFailsOnArchiveErrors(t *testing.T) {
	script := packaging_read(t, "build", "scripts", "build-pkg")
	if !strings.Contains(script, "set -euo pipefail") {
		t.Errorf("build-pkg does not fail on a pipeline error")
	}
	if strings.Contains(script, "cpio -o --format odc 2>/dev/null") {
		t.Errorf("build-pkg still silences cpio")
	}
}

// TestUninstallerMatchesTheProcessByName. pkill -f matched any command line
// mentioning the binary's path and killed it as root.
func TestUninstallerMatchesTheProcessByName(t *testing.T) {
	script := packaging_read(t, "build", "pkg", "mochi-uninstall")
	for _, pattern := range []string{"pkill -f", "pkill -CONT -f", "pkill -KILL -f", "pgrep -f"} {
		if strings.Contains(script, pattern) {
			t.Errorf("uninstaller still matches by command line: %s", pattern)
		}
	}
	if !strings.Contains(script, "pgrep -x mochi-server") {
		t.Errorf("uninstaller does not match the process by name")
	}
}

// TestPreinstallProbesIdsByAttribute. The free-id loops read a path that is
// not a record, so they never iterated.
func TestPreinstallProbesIdsByAttribute(t *testing.T) {
	script := packaging_read(t, "build", "pkg", "scripts", "preinstall")
	for _, probe := range []string{"dscl . -search /Groups PrimaryGroupID", "dscl . -search /Users UniqueID"} {
		if !strings.Contains(script, probe) {
			t.Errorf("preinstall does not probe with %q", probe)
		}
	}
	for _, path := range []string{"/Groups/gid/", "/Users/uid/"} {
		if strings.Contains(script, path) {
			t.Errorf("preinstall still reads the non-record path %s", path)
		}
	}
}

// TestUninstallerKeepsTheConfiguration. The uninstaller preserved the data
// and deleted the configuration the data is useless without.
func TestUninstallerKeepsTheConfiguration(t *testing.T) {
	script := packaging_read(t, "build", "pkg", "mochi-uninstall")
	if strings.Contains(script, "rm -f /etc/mochi/mochi.conf\n") {
		t.Errorf("uninstaller still deletes /etc/mochi/mochi.conf")
	}
	if !strings.Contains(script, "kept at /etc/mochi/mochi.conf") {
		t.Errorf("uninstaller does not tell the operator the configuration was kept")
	}
}

// TestPkgPreservesTheInstalledConfiguration. The payload carried the live
// configuration file, so every upgrade replaced the operator's settings with
// the defaults. The default now ships beside the live file and postinstall
// copies it in only when nothing is there.
func TestPkgPreservesTheInstalledConfiguration(t *testing.T) {
	builder := packaging_read(t, "build", "scripts", "build-pkg")
	if strings.Contains(builder, "/private/etc/mochi/mochi.conf\"") {
		t.Errorf("the payload still carries the live configuration file")
	}
	if !strings.Contains(builder, "/private/etc/mochi/mochi.conf.default\"") {
		t.Errorf("the payload does not carry the default beside the live file")
	}
	script := packaging_read(t, "build", "pkg", "scripts", "postinstall")
	start := strings.Index(script, "if [ ! -f /etc/mochi/mochi.conf ]")
	if start < 0 {
		t.Fatalf("postinstall does not guard the configuration copy")
	}
	block := script[start : start+strings.Index(script[start:], "fi\n")+3]
	root := t.TempDir()
	block = strings.ReplaceAll(block, "/etc/mochi", root)
	if err := os.WriteFile(filepath.Join(root, "mochi.conf.default"), []byte("default"), 0644); err != nil {
		t.Fatal(err)
	}
	for _, round := range []struct{ round, want string }{{"first install", "default"}, {"upgrade", "edited"}} {
		round, want := round.round, round.want
		if round == "upgrade" {
			if err := os.WriteFile(filepath.Join(root, "mochi.conf"), []byte("edited"), 0644); err != nil {
				t.Fatal(err)
			}
		}
		if out, err := exec.Command("sh", "-c", block).CombinedOutput(); err != nil {
			t.Fatalf("%s: %v\n%s", round, err, out)
		}
		got, err := os.ReadFile(filepath.Join(root, "mochi.conf"))
		if err != nil {
			t.Fatal(err)
		}
		if string(got) != want {
			t.Errorf("%s: configuration is %q, want %q", round, got, want)
		}
	}
}

// TestDeployScriptNamesNoRetiredHost. The header still described a retired
// server as a standing backup.
func TestDeployScriptNamesNoRetiredHost(t *testing.T) {
	script := packaging_read(t, "build", "scripts", "deploy")
	if strings.Contains(strings.ToLower(script), "wasabi") {
		t.Errorf("deploy still names the retired server")
	}
}

// TestManifestsCarryGenerationTimeAndPlatform. The signature bound the bytes
// to the release key and nothing bound them to a date, so a replayed old
// manifest froze every server on the release before it. Every manifest now
// says when it was generated and for which subtree, with one time per publish.
func TestManifestsCarryGenerationTimeAndPlatform(t *testing.T) {
	out := packaging_dry(t, "release-publish")
	for _, platform := range []string{"apt", "rpm", "macos", "docker"} {
		if !regexp.MustCompile(`"generated": \d+, "platform": "` + platform + `", "tracks"`).MatchString(out) {
			t.Errorf("the %s manifest carries no generation time or platform", platform)
		}
	}
	if !strings.Contains(out, `"generated": %s, "platform": "windows", "tracks"`) {
		t.Errorf("the windows manifest carries no generation time or platform")
	}
	times := map[string]bool{}
	for _, match := range regexp.MustCompile(`"generated": (\d+)`).FindAllStringSubmatch(out, -1) {
		times[match[1]] = true
	}
	if len(times) != 1 {
		t.Errorf("the manifests of one publish disagree on the generation time: %v", times)
	}
	for value := range times {
		if !strings.Contains(out, "'"+value+"' '") {
			t.Errorf("the windows manifest is not stamped with the same generation time %s", value)
		}
	}
}
