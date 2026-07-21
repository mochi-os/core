// Mochi server: Update check
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.
//
// Daily HTTPS poll of packages.mochi-os.org/<platform>/versions.json. When a
// newer release for the running track is detected, send a Mochi notification
// to every administrator. The settings system status page renders a banner
// with platform-specific upgrade instructions.

package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	update_track            = "production"
	update_initial_lag      = 5 * time.Minute
	update_interval         = 24 * time.Hour
	update_timeout          = 30 * time.Second
	update_install_timeout  = 10 * time.Minute
	update_install_pre_wait = 5 // seconds before msiexec runs, lets the service exit cleanly

	// Ceilings on what a manifest can make us read. The manifest is a few
	// hundred bytes and the MSI is tens of megabytes; these bound the damage
	// if the package host is compromised or simply wrong, since a download
	// that fills the data volume takes the server down with it.
	update_manifest_maximum = 1 << 20   // 1 MiB
	update_artifact_maximum = 256 << 20 // 256 MiB
)

// update_install_lock guards against concurrent install attempts (e.g. an
// admin clicking the button twice).
var update_install_lock sync.Mutex

// update_versions is the per-platform versions.json. Releases is keyed by
// version string and carries the integrity data the self-installer verifies
// before it hands an artifact to the system installer.
type update_versions struct {
	Tracks   map[string]string         `json:"tracks"`
	Releases map[string]update_release `json:"releases"`
}

// update_release describes one downloadable artifact. File is relative to the
// platform directory, so the manifest names the exact file rather than the
// self-installer guessing it.
type update_release struct {
	File   string `json:"file"`
	Size   int64  `json:"size"`
	Sha256 string `json:"sha256"`
}

// update_base is the platform's directory on the package host, without a
// trailing slash. Empty when the running build has no known package subtree.
func update_base() string {
	path := update_url_path()
	if path == "" {
		return ""
	}
	return "https://packages.mochi-os.org/" + path
}

// update_manifest fetches and parses the running platform's versions.json.
func update_manifest() (*update_versions, error) {
	base := update_base()
	if base == "" {
		return nil, fmt.Errorf("no URL path for build_platform=%q", build_platform)
	}
	url := base + "/versions.json"

	ctx, cancel := context.WithTimeout(context.Background(), update_timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("build request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetch %s: %v", url, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("fetch %s: status %d", url, resp.StatusCode)
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, update_manifest_maximum))
	if err != nil {
		return nil, fmt.Errorf("read body: %v", err)
	}

	var v update_versions
	if err := json.Unmarshal(body, &v); err != nil {
		return nil, fmt.Errorf("parse %s: %v", url, err)
	}
	return &v, nil
}

// update_manager polls packages.mochi-os.org once a day and notifies admins
// when a newer version is available on the configured track. Also clears any
// stale update_pending / update_notified state left over from a previous
// install attempt — see update_install_clear_on_match for the success path.
func update_manager() {
	update_install_clear_on_match()

	if build_version == "" || build_platform == "" {
		debug("Server update: skipped (build_version=%q build_platform=%q)", build_version, build_platform)
		return
	}
	if !ini_bool("update", "check", true) {
		info("Server update: daily check disabled by config")
		return
	}

	time.Sleep(update_initial_lag)
	update_check()
	for range time.Tick(update_interval) {
		update_check()
	}
}

// update_install_clear_on_match runs at startup. If update_notified or
// update_pending equals build_version, the install we triggered last time
// succeeded — clear both so the banner disappears. update_pending is also
// cleared if it points to a version that's no longer the latest, so a stale
// "Installing…" indicator can't outlive a server crash.
func update_install_clear_on_match() {
	notified := setting_get("update_notified", "")
	if notified != "" && version_compare(notified, build_version) <= 0 {
		setting_set("update_notified", "")
	}
	pending := setting_get("update_pending", "")
	if pending != "" {
		setting_set("update_pending", "")
	}
}

// update_check fetches the per-platform versions.json, compares the
// production track to build_version, and dispatches notifications when newer.
func update_check() {
	v, err := update_manifest()
	if err != nil {
		info("Server update: %v", err)
		return
	}

	latest, ok := v.Tracks[update_track]
	if !ok || latest == "" {
		info("Server update: no %q track in the %s manifest", update_track, build_platform)
		return
	}

	setting_set("update_checked", strconv.FormatInt(time.Now().Unix(), 10))

	if version_compare(latest, build_version) <= 0 {
		debug("Server update: running %s, latest %s, nothing to do", build_version, latest)
		return
	}

	notified := setting_get("update_notified", "")
	if notified != "" && version_compare(latest, notified) <= 0 {
		debug("Server update: already notified about %s (running %s)", notified, build_version)
		return
	}

	info("Server update: new version %s available (running %s)", latest, build_version)
	update_notify_admins(latest)
	setting_set("update_notified", latest)
}

// update_url_path maps the running build to the packages.mochi-os.org subtree
// that holds its versions.json. Linux releases ship as both deb and rpm from
// the same binary so the format is detected at runtime.
func update_url_path() string {
	switch build_platform {
	case "linux":
		switch update_linux_format() {
		case "deb":
			return "apt"
		case "rpm":
			return "rpm"
		}
		return ""
	case "macos":
		return "macos"
	case "windows":
		return "windows"
	case "docker":
		return "docker"
	}
	return ""
}

// update_platform_full returns the build platform with format / arch
// disambiguators that the UI uses to render platform-specific upgrade
// instructions. One of: "linux-deb", "linux-rpm", "macos-arm64",
// "macos-amd64", "windows", "docker", or "" for dev / unknown.
func update_platform_full() string {
	switch build_platform {
	case "linux":
		return "linux-" + update_linux_format()
	case "macos":
		return "macos-" + runtime.GOARCH
	case "windows", "docker":
		return build_platform
	}
	return ""
}

// update_linux_format returns "deb" or "rpm" based on which package manager
// owns this host. Falls back to "deb" if neither marker is found — most
// development hosts are Debian-family.
func update_linux_format() string {
	if exists("/etc/debian_version") {
		return "deb"
	}
	if exists("/etc/redhat-release") || exists("/etc/fedora-release") || exists("/etc/centos-release") {
		return "rpm"
	}
	return "deb"
}

func exists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

// update_install_start kicks off an unattended self-install of the named
// version on Windows. Writes the in-flight indicator update_pending=<version>
// and spawns update_install_run in a goroutine. Returns an error if the
// platform doesn't support self-install or if no upgrade is currently
// available. Safe to call concurrently — the install lock prevents overlap.
func update_install_start(version string) error {
	if runtime.GOOS != "windows" || build_platform != "windows" {
		return fmt.Errorf("self-install not supported on %s", build_platform)
	}
	if version == "" {
		return fmt.Errorf("no version to install")
	}
	if version_compare(version, build_version) <= 0 {
		return fmt.Errorf("requested version %q is not newer than %q", version, build_version)
	}
	if !update_install_lock.TryLock() {
		return fmt.Errorf("install already in progress")
	}
	setting_set("update_pending", version)
	go func() {
		defer update_install_lock.Unlock()
		if err := update_install_run(version); err != nil {
			warn("Server update: install %s failed: %v", version, err)
			setting_set("update_pending", "")
		}
	}()
	return nil
}

// update_install_run downloads the new MSI to %ProgramData%\Mochi\tmp and
// hands it off to a detached msiexec invocation. Returns once msiexec has
// been launched; the running service is then expected to receive a Stop from
// the SCM (driven by msiexec's ServiceControl Stop=both rule via the WiX
// MajorUpgrade) shortly after.
//
// We push to shutdown_request defensively so the service exits cleanly even
// if msiexec's Stop arrives slowly. The 5-second pre-wait inside the cmd
// invocation ensures we're stopped before msiexec tries to replace files.
func update_install_run(version string) error {
	// Re-read the manifest rather than trusting the daily check's cached
	// version: it names the exact artifact for this version and carries the
	// digest we verify below, and it may have moved on since the last poll.
	v, err := update_manifest()
	if err != nil {
		return fmt.Errorf("manifest: %v", err)
	}
	release, ok := v.Releases[version]
	if !ok {
		return fmt.Errorf("manifest has no release entry for %q", version)
	}
	if release.File == "" || release.Sha256 == "" || release.Size <= 0 {
		return fmt.Errorf("manifest entry for %q is incomplete", version)
	}
	if strings.ContainsAny(release.File, "/\\") {
		return fmt.Errorf("manifest entry for %q names a path, not a file: %q", version, release.File)
	}

	url := update_base() + "/" + release.File
	dir := filepath.Join(data_dir, "tmp")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("mkdir %s: %v", dir, err)
	}
	path := filepath.Join(dir, "mochi-server-"+version+".msi")

	info("Server update: downloading %s to %s", url, path)
	if err := update_install_download(url, path, release); err != nil {
		return fmt.Errorf("download: %v", err)
	}

	// Write the MSI install log alongside the .msi so a failed upgrade
	// leaves a forensic trail on disk that survives the service exit.
	// %ProgramData%\Mochi\data\tmp\mochi-server-<version>.msi.log
	msi_log := path + ".log"

	info("Server update: launching msiexec for %s (log: %s)", path, msi_log)
	if err := update_install_spawn(path, msi_log); err != nil {
		return fmt.Errorf("spawn msiexec: %v", err)
	}

	info("Server update: shutting down for self-install")
	select {
	case shutdown_request <- 0:
	default:
	}
	return nil
}

// update_install_download streams a URL to disk with a generous timeout — MSIs
// are tens of megabytes and corporate links can be slow — and refuses to
// publish the result unless it is exactly the artifact the manifest described.
//
// The digest is what makes the whole path safe to hand to msiexec, which runs
// the file as LocalSystem with no verification of its own. It also catches the
// benign case: a release landing between the version check and this download
// leaves the manifest and the artifact briefly out of step, and installing
// whatever bytes arrive would silently install a version nobody chose.
func update_install_download(url, dest string, release update_release) error {
	if release.Size > update_artifact_maximum {
		return fmt.Errorf("manifest size %d exceeds maximum %d", release.Size, update_artifact_maximum)
	}

	ctx, cancel := context.WithTimeout(context.Background(), update_install_timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("status %d", resp.StatusCode)
	}

	tmp := dest + ".part"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	// Remove the partial on every failure path, including the verification
	// failures below — a rejected artifact must not survive on disk where a
	// later attempt might mistake it for a good download.
	complete := false
	defer func() {
		if !complete {
			os.Remove(tmp)
		}
	}()

	// One byte past the declared size, so a body that is longer than the
	// manifest claims is detected rather than silently truncated to a
	// plausible-looking file.
	digest := sha256.New()
	written, err := io.Copy(io.MultiWriter(f, digest), io.LimitReader(resp.Body, release.Size+1))
	if err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if written != release.Size {
		return fmt.Errorf("size %d does not match manifest size %d", written, release.Size)
	}
	if sum := hex.EncodeToString(digest.Sum(nil)); !strings.EqualFold(sum, release.Sha256) {
		return fmt.Errorf("sha256 %s does not match manifest sha256 %s", sum, release.Sha256)
	}

	if err := os.Rename(tmp, dest); err != nil {
		return err
	}
	complete = true
	return nil
}

// update_notify_admins dispatches one Mochi notification per administrator
// for the new version. The notifications service is invoked with app="" so
// the receiving user sees the sender as "Mochi server". Title and body are
// resolved per-recipient against core labels so each admin sees the
// notification in their own language.
func update_notify_admins(latest string) {
	db := db_open("db/users.db")
	rows, err := db.rows("select uid, username from users where role = ?", "administrator")
	if err != nil {
		warn("Server update: list admins: %v", err)
		return
	}
	if len(rows) == 0 {
		info("Server update: no administrators to notify")
		return
	}

	link := "/settings/system/status"

	for _, row := range rows {
		id, _ := row["uid"].(string)
		if id == "" {
			continue
		}
		user := user_by_uid(id)
		if user == nil {
			continue
		}
		lang := user_language(user)
		title := resolve_core_label(lang, "update.notification.title", map[string]any{"version": latest})
		body := resolve_core_label(lang, "update.notification.body", map[string]any{"current": build_version})
		topic_label := resolve_core_label(lang, "update.notification.topic", nil)

		args := Map{
			"topic":  "upgrade/available",
			"object": "",
			"title":  title,
			"body":   body,
			"url":    link,
			"label":  topic_label,
			"count":  int64(1),
		}
		if err := service_call_as_server(id, "notifications", "send", args); err != nil {
			info("Server update: notify user %q: %v", id, err)
		}
	}
}
