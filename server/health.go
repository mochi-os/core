// Mochi server: /_/health endpoint (liveness probe for Docker / Kubernetes / monitors).
// Copyright Alistair Cunningham 2026

package main

import (
	"net/http"
	"path/filepath"
	"time"

	"github.com/gin-gonic/gin"
)

// web_health returns a JSON liveness summary. Public, unauthenticated.
// Returns HTTP 200 if all subsystems are healthy, 503 with detail otherwise.
func web_health(c *gin.Context) {
	database_status := "ok"
	network_status := "ok"
	overall := http.StatusOK

	// Database liveness — users.db is opened during db_start. The databases
	// map keys by absolute path (filepath.Join(data_dir, file)), so we look
	// up using the same key db_open_work uses.
	users_key := filepath.Join(data_dir, "db", "users.db")
	databases_lock.Lock()
	db, ok := databases[users_key]
	databases_lock.Unlock()
	if !ok || db == nil || db.handle == nil {
		database_status = "not started"
		overall = http.StatusServiceUnavailable
	} else if err := db.handle.Ping(); err != nil {
		database_status = "error: " + err.Error()
		overall = http.StatusServiceUnavailable
	}

	// Network (libp2p) liveness — p2p_me is set by p2p_start.
	if p2p_me == nil {
		network_status = "not started"
		overall = http.StatusServiceUnavailable
	}

	status := "ok"
	if overall != http.StatusOK {
		status = "degraded"
	}

	uptime := int(time.Since(server_started_at).Seconds())

	c.JSON(overall, gin.H{
		"status":   status,
		"version":  build_version,
		"uptime":   uptime,
		"database": database_status,
		"network":  network_status,
	})
}
