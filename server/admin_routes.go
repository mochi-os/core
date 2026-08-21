// Mochi server: admin route registration, audit middleware, and the
// platform-neutral peer-credential type.
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the Mochi
// Application Interface Exception - see license.txt and license-exception.md.
//
// Transport-agnostic: the per-OS file (admin_unix.go, admin_windows.go) owns
// the listener, the connection-level authorization, and admin_start.

package main

import (
	"context"
	"fmt"
	"net/http/pprof"
	"strings"

	"github.com/gin-gonic/gin"
)

// admin_router is the shared Gin engine; the per-OS admin_start builds it via
// admin_register_routes and serves it over that platform's listener.
var admin_router *gin.Engine

// admin_credential is the platform-neutral peer identity for an admin
// connection. Windows gates at connect time through the pipe's security
// descriptor and attaches none, so admin_peer_credential returns nil there.
type admin_credential struct {
	uid uint32
	gid uint32
	pid int32
}

// peer_credential_key is the context key used to attach the peer's admin_credential
// to the request context so handlers and middleware can read it.
type peer_credential_key struct{}

// audit_peer_identity returns the peer's uid, gid and pid for an audit line, or
// -1 each where the transport attached no credential.
func audit_peer_identity(ctx context.Context) (uid, gid, pid int) {
	credential := admin_peer_credential(ctx)
	if credential == nil {
		return -1, -1, -1
	}
	return int(credential.uid), int(credential.gid), int(credential.pid)
}

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
	profiling := admin.Group("/debug/pprof")
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

// admin_audited_routes maps "<METHOD> <fullPath>" to the subcommand label to
// record; anything absent, and not under the profiling prefix, is not audited.
// Reads belong here too: an export or config dump is what left this server.
var admin_audited_routes = map[string]string{
	"POST /_/admin/snapshot": "admin.snapshot",
	"POST /_/admin/vacuum":   "admin.vacuum",
	"POST /_/admin/stop":     "admin.stop",
	"POST /_/admin/restart":  "admin.restart",
	"POST /_/admin/migrate":  "admin.migrate",
	"GET /_/admin/backup":    "admin.backup",
	"GET /_/admin/config":    "admin.config",
}

// admin_profiling_prefix is audited by prefix rather than by name: the twelve
// pprof endpoints hand out process memory, stacks and goroutine state, and
// naming each one in the map above would be a list to forget to extend.
const admin_profiling_prefix = "/_/admin/debug/pprof"

// admin_audited_operation returns the label to record for one request, and
// whether it is audited at all.
func admin_audited_operation(method, path string) (string, bool) {
	if operation, ok := admin_audited_routes[method+" "+path]; ok {
		return operation, true
	}
	if path == admin_profiling_prefix || strings.HasPrefix(path, admin_profiling_prefix+"/") {
		profile := strings.Trim(strings.TrimPrefix(path, admin_profiling_prefix), "/")
		if profile == "" {
			profile = "index"
		}
		return "admin.pprof." + profile, true
	}
	return "", false
}

// admin_audit_middleware records a daemon-facility audit row after each
// request to a state-changing admin route.
func admin_audit_middleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Next()

		operation, ok := admin_audited_operation(c.Request.Method, c.FullPath())
		if !ok {
			return
		}
		uid, gid, _ := audit_peer_identity(c.Request.Context())
		audit_log_daemon(fmt.Sprintf("%s peer_uid=%d peer_gid=%d status=%d",
			operation, uid, gid, c.Writer.Status()))
	}
}
