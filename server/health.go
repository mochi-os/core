// Mochi server: /_/health endpoint (liveness probe for Docker / Kubernetes / monitors).
// Copyright Alistair Cunningham 2026

package main

import (
	"net/http"
	"path/filepath"
	"time"

	"github.com/gin-gonic/gin"
)

// health_status assembles the liveness summary returned by both the public
// /_/health route and the UDS /_/admin/health route. Returns the JSON body
// and the HTTP status code (200 if healthy, 503 if any subsystem is degraded).
func health_status() (gin.H, int) {
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
	if !ok || db == nil || db.internal == nil {
		database_status = "not started"
		overall = http.StatusServiceUnavailable
	} else if err := db.internal.Ping(); err != nil {
		database_status = "error: " + err.Error()
		overall = http.StatusServiceUnavailable
	}

	// Network (libp2p) liveness — net_me is set by net_start.
	if net_me == nil {
		network_status = "not started"
		overall = http.StatusServiceUnavailable
	}

	status := "ok"
	if overall != http.StatusOK {
		status = "degraded"
	}

	// Replication health is reported but does NOT flip the liveness code: a
	// dead/irreparable PEER is an operator-attention issue, not a dead
	// server, and 503 here would wrongly make load balancers / k8s restart a
	// perfectly live host. Monitors alert on replication != "ok".
	replication_status := "ok"
	irreparable := replication_irreparable_count()
	if irreparable > 0 {
		replication_status = "degraded"
	}

	return gin.H{
		"status":      status,
		"version":     build_version,
		"uptime":      int(time.Since(server_started_at).Seconds()),
		"database":    database_status,
		"network":     network_status,
		"replication": replication_status,
		"irreparable": irreparable,
	}, overall
}

// web_health returns a JSON liveness summary. Public, unauthenticated.
func web_health(c *gin.Context) {
	body, code := health_status()
	c.JSON(code, body)
}
