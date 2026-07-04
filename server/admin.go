// Mochi server: cross-platform /_/admin/* handlers (read-only and lifecycle).
// Copyright © 2026 Mochi OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.
//
// Handlers run only on the UDS admin listener (registered in admin_linux.go).
// Read-only handlers (status, version, config, identity) read internal state
// without mutating it. Lifecycle handlers (stop, restart, reload) trigger
// graceful shutdown via shutdown_request or re-read the config in place.

package main

import (
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"core/common/ini"

	"github.com/gin-gonic/gin"
)

// shutdown_request carries the exit code for an operator-initiated shutdown.
// Buffered so a handler that pushes onto it doesn't block on the main loop.
var shutdown_request = make(chan int, 1)

// admin_status returns runtime state — uptime, version, peer counts, app count.
func admin_status(c *gin.Context) {
	peers_connected := 0
	peers_known := 0
	if net_me != nil {
		peers_connected = len(net_me.Network().Peers())
		// Peerstore tracks every peer libp2p has heard about this run
		// (including currently-connected, past connections, and DHT/relay
		// referrals), so peers_known is always >= peers_connected.
		peers_known = len(net_me.Peerstore().Peers())
	}

	apps_lock.Lock()
	apps_count := len(apps)
	apps_lock.Unlock()

	c.JSON(http.StatusOK, gin.H{
		"status":          "ok",
		"version":         build_version,
		"uptime":          int(time.Since(server_started_at).Seconds()),
		"peers_connected": peers_connected,
		"peers_known":     peers_known,
		"apps":            apps_count,
	})
}

// admin_version returns the server build version. Equivalent information is
// already in /_/health and /_/admin/status, but a dedicated endpoint is
// convenient for `mochictl version` to call.
func admin_version(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"server_version": build_version,
		"schema_version": schema_version,
	})
}

// admin_config returns the merged effective config (file values + MOCHI_*
// env overrides) with sensitive keys redacted.
func admin_config(c *gin.Context) {
	c.JSON(http.StatusOK, ini.Effective())
}

// admin_health returns the same liveness body as the public /_/health route.
// Exposed over the UDS so mochictl can probe a server bound to TLS-only ports
// (where a 127.0.0.1 HTTPS handshake fails on SNI mismatch).
func admin_health(c *gin.Context) {
	body, code := health_status()
	c.JSON(code, body)
}

// admin_identity returns the libp2p peer ID that identifies this server
// to the rest of the Mochi network.
func admin_identity(c *gin.Context) {
	if net_me == nil {
		respond_error(c, http.StatusServiceUnavailable, "net_not_started", "errors.net_not_started", nil)
		return
	}
	c.JSON(http.StatusOK, gin.H{
		"peer_id":     net_id,
		"fingerprint": fingerprint_hyphens(fingerprint(net_id)),
		"data_dir":    data_dir,
	})
}

// admin_stop initiates graceful shutdown. Server exits 0; the supervisor
// decides whether to restart based on its policy.
func admin_stop(c *gin.Context) {
	select {
	case shutdown_request <- 0:
		c.JSON(http.StatusOK, gin.H{"status": "stopping"})
	default:
		respond_error(c, http.StatusConflict, "shutdown_in_progress", "errors.shutdown_in_progress", nil)
	}
}

// admin_restart initiates graceful shutdown with exit code 75 so a supervisor
// configured with Restart=on-failure (systemd) or --restart=on-failure
// (Docker) brings the server back up.
func admin_restart(c *gin.Context) {
	select {
	case shutdown_request <- 75:
		c.JSON(http.StatusOK, gin.H{"status": "restarting"})
	default:
		respond_error(c, http.StatusConflict, "shutdown_in_progress", "errors.shutdown_in_progress", nil)
	}
}

// admin_migrate walks every user and opens every installed app's data DB,
// which runs any pending database migrations (per-user app DBs migrate on
// demand, so rarely-touched users lag behind). Run before a schema baseline
// change so every database reaches the current schema while the migration
// code still exists. Synchronous; reports per-user counts.
func admin_migrate(c *gin.Context) {
	users := db_open("db/users.db")
	rows, err := users.rows("select uid from users")
	if err != nil {
		c.JSON(500, gin.H{"error": err.Error()})
		return
	}
	apps_lock.Lock()
	names := make([]string, 0, len(apps))
	for id := range apps {
		names = append(names, id)
	}
	apps_lock.Unlock()
	opened := 0
	failed := 0
	for _, r := range rows {
		uid, _ := r["uid"].(string)
		u := user_by_uid(uid)
		if u == nil {
			continue
		}
		for _, id := range names {
			a := app_by_id(id)
			if a == nil {
				continue
			}
			av := a.active(u)
			if av == nil || av.Database.File == "" {
				continue
			}
			// Only migrate databases that already exist - opening creates
			// on demand, and we don't want to mint empty DBs for every
			// (user, app) pair.
			path := fmt.Sprintf("users/%s/%s/db/%s", u.UID, a.id, av.Database.File)
			if _, err := os.Stat(filepath.Join(data_dir, path)); err != nil {
				continue
			}
			if db := db_app(u, a); db != nil {
				opened++
			} else {
				failed++
			}
		}
	}
	info("Admin migrate: %d databases opened/migrated, %d failed", opened, failed)
	c.JSON(200, gin.H{"opened": opened, "failed": failed})
}
