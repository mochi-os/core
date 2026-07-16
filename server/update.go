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
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
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
)

// update_install_lock guards against concurrent install attempts (e.g. an
// admin clicking the button twice).
var update_install_lock sync.Mutex

type update_versions struct {
	Tracks map[string]string `json:"tracks"`
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
	path := update_url_path()
	if path == "" {
		warn("Server update: no URL path for build_platform=%q", build_platform)
		return
	}

	url := "https://packages.mochi-os.org/" + path + "/versions.json"

	ctx, cancel := context.WithTimeout(context.Background(), update_timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		warn("Server update: build request: %v", err)
		return
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		info("Server update: fetch %s: %v", url, err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		info("Server update: fetch %s: status %d", url, resp.StatusCode)
		return
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		info("Server update: read body: %v", err)
		return
	}

	var v update_versions
	if err := json.Unmarshal(body, &v); err != nil {
		info("Server update: parse %s: %v", url, err)
		return
	}

	latest, ok := v.Tracks[update_track]
	if !ok || latest == "" {
		info("Server update: no %q track in %s", update_track, url)
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
	url := "https://packages.mochi-os.org/windows/mochi-server.msi"
	dir := filepath.Join(data_dir, "tmp")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("mkdir %s: %v", dir, err)
	}
	path := filepath.Join(dir, "mochi-server-"+version+".msi")

	info("Server update: downloading %s to %s", url, path)
	if err := update_install_download(url, path); err != nil {
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

// update_install_download streams a URL to disk with a generous timeout —
// MSIs are tens of megabytes and corporate links can be slow.
func update_install_download(url, dest string) error {
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
	if _, err := io.Copy(f, resp.Body); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return err
	}
	return os.Rename(tmp, dest)
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
