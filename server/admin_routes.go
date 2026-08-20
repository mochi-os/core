// Mochi server: admin route registration, audit middleware, and the
// platform-neutral peer-credential type.
// Copyright © 2026 Mochisoft OÜ
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

// admin_credential is the platform-neutral peer identity for an accepted admin
// connection. On Linux/macOS the transport fills it from the socket's peer
// credentials (SO_PEERCRED / LOCAL_PEERCRED); on Windows the pipe's security
// descriptor gates access at connect time, so no per-connection credential is
// attached and admin_peer_credential returns nil. pid is 0 when unknown.
type admin_credential struct {
	uid uint32
	gid uint32
	pid int32
}

// peer_credential_key is the context key used to attach the peer's admin_credential
// to the request context so handlers and middleware can read it.
type peer_credential_key struct{}

// admin_peer_credential extracts the peer credentials attached by the transport's
// ConnContext, or nil when none were attached (e.g. on Windows).
func admin_peer_credential(ctx context.Context) *admin_credential {
	if credential, ok := ctx.Value(peer_credential_key{}).(*admin_credential); ok {
		return credential
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
	admin.POST("/migrate", admin_migrate)
	admin.POST("/snapshot", admin_snapshot)
	admin.POST("/vacuum", admin_vacuum)
	admin.GET("/backup", admin_backup)
	admin.POST("/stop", admin_stop)
	admin.POST("/restart", admin_restart)
	admin.GET("/broadcast/lag", admin_broadcast_lag)
	admin.POST("/broadcast/pending/gc", admin_broadcast_pending_gc)
	admin.GET("/pipelining/status", admin_pipelining_status)
	admin.GET("/pubsub/status", admin_pubsub_status)
	admin.GET("/worlds", admin_worlds)

	// pprof endpoints — admin-socket only, no separate port. The transport's
	// connection-level auth gates access. Useful for diagnosing memory bloat /
	// goroutine leaks:
	//   mochictl -s admin.sock raw GET /_/admin/debug/pprof/heap > heap.pb.gz
	//   go tool pprof heap.pb.gz
	// curl -s --unix-socket admin.sock http://x/_/admin/debug/pprof/<profile>
	// is the lower-level form for ad-hoc captures.
	profiling := r.Group("/_/admin/debug/pprof")
	profiling.GET("/", gin.WrapF(pprof.Index))
	profiling.GET("/cmdline", gin.WrapF(pprof.Cmdline))
	profiling.GET("/profile", gin.WrapF(pprof.Profile))
	profiling.GET("/symbol", gin.WrapF(pprof.Symbol))
	profiling.POST("/symbol", gin.WrapF(pprof.Symbol))
	profiling.GET("/trace", gin.WrapF(pprof.Trace))
	profiling.GET("/allocs", gin.WrapH(pprof.Handler("allocs")))
	profiling.GET("/heap", gin.WrapH(pprof.Handler("heap")))
	profiling.GET("/goroutine", gin.WrapH(pprof.Handler("goroutine")))
	profiling.GET("/threadcreate", gin.WrapH(pprof.Handler("threadcreate")))
	profiling.GET("/block", gin.WrapH(pprof.Handler("block")))
	profiling.GET("/mutex", gin.WrapH(pprof.Handler("mutex")))
}

// -- Audit middleware ------------------------------------------------------

// admin_audited_routes maps "<METHOD> <fullPath>" to the subcommand label
// to record. Anything not in this map is not audited.
var admin_audited_routes = map[string]string{
	"POST /_/admin/snapshot": "admin.snapshot",
	"POST /_/admin/vacuum":   "admin.vacuum",
	"POST /_/admin/stop":     "admin.stop",
	"POST /_/admin/restart":  "admin.restart",
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
		credential := admin_peer_credential(c.Request.Context())
		uid := -1
		gid := -1
		if credential != nil {
			uid = int(credential.uid)
			gid = int(credential.gid)
		}
		audit_log_daemon(fmt.Sprintf("%s peer_uid=%d peer_gid=%d status=%d",
			op, uid, gid, c.Writer.Status()))
	}
}
