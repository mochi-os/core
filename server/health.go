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

	// Manager liveness (dead-man's-switch): the replication manager ticks every
	// 30s and runs every replication health scan + alert. A stale heartbeat means
	// it has hung — alerting itself is dead and can't report its own death — so
	// an external monitor catches it via manager_age. -1 = not yet started.
	manager_hung, manager_age := replication_manager_hung()
	uptime := int(time.Since(server_started_at).Seconds())

	// Report degraded for ANY replication problem so an external poll of
	// /_/health sees it independent of the server's own email path: a dead peer
	// (irreparable), a hung manager, or any active alert (stall / not-advancing /
	// divergence / stale app).
	if irreparable > 0 || manager_hung || replication_active_alerts() > 0 {
		replication_status = "degraded"
	}

	return gin.H{
		"status":      status,
		"version":     build_version,
		"uptime":      uptime,
		"database":    database_status,
		"network":     network_status,
		"replication": replication_status,
		"irreparable": irreparable,
		"manager_age": manager_age,
	}, overall
}

// web_health returns a JSON liveness summary. Public, unauthenticated.
func web_health(c *gin.Context) {
	body, code := health_status()
	c.JSON(code, body)
}
