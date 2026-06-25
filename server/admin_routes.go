// Mochi server: admin route registration, audit middleware, and the
// platform-neutral peer-credential type.
// Copyright © 2026 Mochi OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.
//
// These pieces are transport-agnostic: the same Gin router and audit
// middleware are served over a Unix domain socket (Linux/macOS, admin_unix.go)
// or a named pipe (Windows, admin_windows.go). The per-OS transport file owns
// the listener, the connection-level authorization (UDS peer credentials or
// the pipe's security descriptor), and admin_start.

package main

import (
	"context"
	"fmt"
	"net/http/pprof"

	"github.com/gin-gonic/gin"
)

// admin_router is the shared Gin engine; the per-OS admin_start builds it via
// admin_register_routes and serves it over that platform's listener.
var admin_router *gin.Engine

// admin_cred is the platform-neutral peer identity for an accepted admin
// connection. On Linux/macOS the transport fills it from the socket's peer
// credentials (SO_PEERCRED / LOCAL_PEERCRED); on Windows the pipe's security
// descriptor gates access at connect time, so no per-connection cred is
// attached and admin_peer_cred returns nil. pid is 0 when unknown.
type admin_cred struct {
	uid uint32
	gid uint32
	pid int32
}

// peer_credential_key is the context key used to attach the peer's admin_cred
// to the request context so handlers and middleware can read it.
type peer_credential_key struct{}

// admin_peer_cred extracts the peer credentials attached by the transport's
// ConnContext, or nil when none were attached (e.g. on Windows).
func admin_peer_cred(ctx context.Context) *admin_cred {
	if cred, ok := ctx.Value(peer_credential_key{}).(*admin_cred); ok {
		return cred
	}
	return nil
}

// admin_register_routes wires every /_/admin/* handler.
func admin_register_routes(r *gin.Engine) {
	admin := r.Group("/_/admin")
	admin.Use(admin_audit_middleware())
	admin.GET("/status", admin_status)
	admin.GET("/version", admin_version)
	admin.GET("/config", admin_config)
	admin.GET("/identity", admin_identity)
	admin.GET("/health", admin_health)
	admin.POST("/snapshot", admin_snapshot)
	admin.POST("/vacuum", admin_vacuum)
	admin.GET("/backup", admin_backup)
	admin.POST("/stop", admin_stop)
	admin.POST("/restart", admin_restart)
	admin.POST("/replica/join", admin_replica_join)
	admin.POST("/replica/approve", admin_replica_approve)
	admin.POST("/replica/leave", admin_replica_leave)
	admin.GET("/replica/status", admin_replica_status)
	admin.GET("/replication/status", admin_replication_status)
	admin.GET("/replication/pair", admin_replication_pair)
	admin.GET("/replication/pairs", admin_replication_pairs)
	admin.GET("/replication/progress", admin_replication_progress)
	admin.GET("/replication/ops", admin_replication_ops)
	admin.GET("/replication/stalled", admin_replication_stalled)
	admin.GET("/replication/irreparable", admin_replication_irreparable)
	admin.POST("/replication/pair/remove", admin_replication_pair_remove)
	admin.POST("/replication/pending/gc", admin_replication_pending_gc)
	admin.POST("/replication/resync", admin_replication_resync)
	admin.POST("/replication/resume", admin_replication_resume)
	admin.POST("/replication/backfill", admin_replication_backfill)
	admin.POST("/replication/reseed", admin_replication_reseed)
	admin.GET("/replication/audit", admin_replication_audit)
	admin.GET("/broadcast/lag", admin_broadcast_lag)
	admin.POST("/broadcast/pending/gc", admin_broadcast_pending_gc)
	admin.GET("/pipelining/status", admin_pipelining_status)
	admin.GET("/pubsub/status", admin_pubsub_status)

	// pprof endpoints — admin-socket only, no separate port. The transport's
	// connection-level auth gates access. Useful for diagnosing memory bloat /
	// goroutine leaks during replication tests:
	//   mochictl -s admin.sock raw GET /_/admin/debug/pprof/heap > heap.pb.gz
	//   go tool pprof heap.pb.gz
	// curl -s --unix-socket admin.sock http://x/_/admin/debug/pprof/<profile>
	// is the lower-level form for ad-hoc captures.
	debug := r.Group("/_/admin/debug/pprof")
	debug.GET("/", gin.WrapF(pprof.Index))
	debug.GET("/cmdline", gin.WrapF(pprof.Cmdline))
	debug.GET("/profile", gin.WrapF(pprof.Profile))
	debug.GET("/symbol", gin.WrapF(pprof.Symbol))
	debug.POST("/symbol", gin.WrapF(pprof.Symbol))
	debug.GET("/trace", gin.WrapF(pprof.Trace))
	debug.GET("/allocs", gin.WrapH(pprof.Handler("allocs")))
	debug.GET("/heap", gin.WrapH(pprof.Handler("heap")))
	debug.GET("/goroutine", gin.WrapH(pprof.Handler("goroutine")))
	debug.GET("/threadcreate", gin.WrapH(pprof.Handler("threadcreate")))
	debug.GET("/block", gin.WrapH(pprof.Handler("block")))
	debug.GET("/mutex", gin.WrapH(pprof.Handler("mutex")))
}

// -- Audit middleware ------------------------------------------------------

// admin_audited_routes maps "<METHOD> <fullPath>" to the subcommand label
// to record. Anything not in this map is not audited.
var admin_audited_routes = map[string]string{
	"POST /_/admin/snapshot":                "admin.snapshot",
	"POST /_/admin/vacuum":                  "admin.vacuum",
	"POST /_/admin/stop":                    "admin.stop",
	"POST /_/admin/restart":                 "admin.restart",
	"POST /_/admin/replica/join":            "admin.replica.join",
	"POST /_/admin/replica/approve":         "admin.replica.approve",
	"POST /_/admin/replica/leave":           "admin.replica.leave",
	"POST /_/admin/replication/pair/remove": "admin.replication.pair.remove",
	"POST /_/admin/replication/resync":      "admin.replication.resync",
	"POST /_/admin/replication/resume":      "admin.replication.resume",
	"POST /_/admin/replication/backfill":    "admin.replication.backfill",
	"POST /_/admin/replication/reseed":      "admin.replication.reseed",
	"GET /_/admin/replication/audit":        "admin.replication.audit",
}

// admin_audit_middleware records a daemon-facility audit row after each
// request to a state-changing admin route.
func admin_audit_middleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Next()

		key := c.Request.Method + " " + c.FullPath()
		op, ok := admin_audited_routes[key]
		if !ok {
			return
		}
		cred := admin_peer_cred(c.Request.Context())
		uid := -1
		gid := -1
		if cred != nil {
			uid = int(cred.uid)
			gid = int(cred.gid)
		}
		audit_log_daemon(fmt.Sprintf("%s peer_uid=%d peer_gid=%d status=%d",
			op, uid, gid, c.Writer.Status()))
	}
}
