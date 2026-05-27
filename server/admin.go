// Mochi server: cross-platform /_/admin/* handlers (read-only and lifecycle).
// Copyright Alistair Cunningham 2026
//
// Handlers run only on the UDS admin listener (registered in admin_linux.go).
// Read-only handlers (status, version, config, identity) read internal state
// without mutating it. Lifecycle handlers (stop, restart, reload) trigger
// graceful shutdown via shutdown_request or re-read the config in place.

package main

import (
	"net/http"
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
		c.JSON(http.StatusServiceUnavailable, gin.H{
			"error": "net not started",
		})
		return
	}
	c.JSON(http.StatusOK, gin.H{
		"peer_id":  net_id,
		"data_dir": data_dir,
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
