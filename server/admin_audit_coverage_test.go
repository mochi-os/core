// Mochi server: the local sockets record who did what - every admin route in
// admin_audited_routes plus the pprof group, and the world socket's peer
// credential.
//
// Copyright © 2026 Mochisoft OU
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the Mochi
// Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"context"
	"os"
	"strings"
	"testing"
)

// TestAuditedOperationNamesTheWrites is the set the middleware must cover.
func TestAuditedOperationNamesTheWrites(t *testing.T) {
	for _, c := range []struct{ method, path, want string }{
		{"POST", "/_/admin/snapshot", "admin.snapshot"},
		{"POST", "/_/admin/vacuum", "admin.vacuum"},
		{"POST", "/_/admin/stop", "admin.stop"},
		{"POST", "/_/admin/restart", "admin.restart"},
		{"POST", "/_/admin/migrate", "admin.migrate"},
		{"GET", "/_/admin/backup", "admin.backup"},
		{"GET", "/_/admin/config", "admin.config"},
	} {
		got, ok := admin_audited_operation(c.method, c.path)
		if !ok {
			t.Errorf("%s %s is not audited", c.method, c.path)
			continue
		}
		if got != c.want {
			t.Errorf("%s %s recorded as %q, want %q", c.method, c.path, got, c.want)
		}
	}
}

// TestAuditedOperationCoversEveryProfile: the pprof endpoints are audited by
// prefix, so a profile added later is covered without editing a list.
func TestAuditedOperationCoversEveryProfile(t *testing.T) {
	for _, c := range []struct{ path, want string }{
		{"/_/admin/debug/pprof", "admin.pprof.index"},
		{"/_/admin/debug/pprof/", "admin.pprof.index"},
		{"/_/admin/debug/pprof/heap", "admin.pprof.heap"},
		{"/_/admin/debug/pprof/goroutine", "admin.pprof.goroutine"},
		{"/_/admin/debug/pprof/profile", "admin.pprof.profile"},
		{"/_/admin/debug/pprof/somethingnew", "admin.pprof.somethingnew"},
	} {
		got, ok := admin_audited_operation("GET", c.path)
		if !ok {
			t.Errorf("%s is not audited; it hands out process memory or stacks", c.path)
			continue
		}
		if got != c.want {
			t.Errorf("%s recorded as %q, want %q", c.path, got, c.want)
		}
	}
}

// TestAuditedOperationIgnoresTheRest. Auditing everything would be its own
// failure: a log where every read of /_/admin/status appears buries the rows
// that matter.
func TestAuditedOperationIgnoresTheRest(t *testing.T) {
	for _, c := range []struct{ method, path string }{
		{"GET", "/_/admin/status"},
		{"GET", "/_/admin/version"},
		{"GET", "/_/admin/health"},
		{"GET", "/_/admin/identity"},
		{"GET", "/_/admin/worlds"},
		{"GET", "/_/health"},
		{"GET", "/_/admin/debugpprof"}, // near the prefix, not under it
		{"GET", ""},                    // no matched route
	} {
		if operation, ok := admin_audited_operation(c.method, c.path); ok {
			t.Errorf("%s %s is audited as %q; it changes nothing and discloses nothing", c.method, c.path, operation)
		}
	}
}

// TestProfilingIsRegisteredUnderTheAuditedGroup is the structural half. The
// prefix rule above is only reachable if pprof requests pass through the
// middleware at all, which they did not when the group hung off the router.
func TestProfilingIsRegisteredUnderTheAuditedGroup(t *testing.T) {
	source, err := os.ReadFile("admin_routes.go")
	if err != nil {
		t.Fatalf("reading admin_routes.go: %v", err)
	}
	text := string(source)

	if strings.Contains(text, `r.Group("/_/admin/debug/pprof")`) {
		t.Error("the pprof group hangs off the router again, so it inherits none of the admin group's middleware; it cannot be audited from admin_audited_routes no matter what the map says")
	}
	if !strings.Contains(text, `admin.Group("/debug/pprof")`) {
		t.Error("the pprof group is no longer a child of the admin group")
	}
	if !strings.Contains(text, "admin.Use(admin_audit_middleware())") {
		t.Error("the admin group no longer installs the audit middleware")
	}
}

// TestPeerIdentityReportsAnUnknownPeerConsistently. Windows attaches no
// per-connection credential - the pipe's security descriptor gates at connect
// time - so both sockets have to render that the same way, or an operator
// reading the log has to know which transport wrote each line.
func TestPeerIdentityReportsAnUnknownPeerConsistently(t *testing.T) {
	uid, gid, pid := audit_peer_identity(context.Background())
	if uid != -1 || gid != -1 || pid != -1 {
		t.Errorf("audit_peer_identity with no credential = (%d, %d, %d), want (-1, -1, -1)", uid, gid, pid)
	}

	credential := &admin_credential{uid: 1000, gid: 1001, pid: 4242}
	ctx := context.WithValue(context.Background(), peer_credential_key{}, credential)
	uid, gid, pid = audit_peer_identity(ctx)
	if uid != 1000 || gid != 1001 || pid != 4242 {
		t.Errorf("audit_peer_identity = (%d, %d, %d), want (1000, 1001, 4242)", uid, gid, pid)
	}
}

// TestWorldPushIsAudited is the world half. The handler records the push
// itself rather than a middleware, because the identifying detail is the world
// id and only the handler has parsed it.
func TestWorldPushIsAudited(t *testing.T) {
	source, err := os.ReadFile("world.go")
	if err != nil {
		t.Fatalf("reading world.go: %v", err)
	}
	text := string(source)
	at := strings.Index(text, "func world_status_handler(")
	if at < 0 {
		t.Fatal("world.go no longer defines world_status_handler")
	}
	body := text[at:]
	if end := strings.Index(body, "\n}\n"); end > 0 {
		body = body[:end]
	}

	if !strings.Contains(body, "audit_log_daemon(") {
		t.Error("world_status_handler records nothing; the pushed name and address gossip on to other hosts, and the socket's group is looser than the admin socket's, so which member pushed is exactly what is worth knowing")
	}
	if !strings.Contains(body, "audit_peer_identity(") {
		t.Error("world_status_handler does not read the peer identity, so its audit line cannot say who pushed")
	}
	if !strings.Contains(body, "input.World.ID") || !strings.Contains(body, "world=") {
		t.Error("the world audit line does not name the world; \"a listing changed\" without which listing is not an audit trail")
	}

	// The record has to follow the store, not precede the validation that may
	// still reject the push.
	store := strings.Index(body, "world_store(")
	audit := strings.Index(body, "audit_log_daemon(")
	if store < 0 || audit < store {
		t.Error("world_status_handler audits before it stores; a rejected push would be recorded as though it had taken effect")
	}
}

// Reads the source rather than dialling: world_unix.go is behind a build tag
// and needs a real UDS and a mochi-world group.
func TestWorldSocketAttachesItsCredential(t *testing.T) {
	source, err := os.ReadFile("world_unix.go")
	if err != nil {
		t.Fatalf("reading world_unix.go: %v", err)
	}
	text := string(source)

	if strings.Contains(text, "\t\treturn c, nil\n") {
		t.Error("world_conn_listener.Accept returns the bare connection again, so the credential it just verified is dropped and no handler can learn who connected")
	}
	if !strings.Contains(text, "&world_conn{Conn: c, credential: credential}") {
		t.Error("Accept no longer wraps the connection with its credential")
	}
	if !strings.Contains(text, "ConnContext:") {
		t.Error("the world server sets no ConnContext, so a credential attached to the connection never reaches the request context")
	}
	if !strings.Contains(text, "peer_credential_key{}") {
		t.Error("the world server promotes the credential under a different key than admin_peer_credential reads")
	}
}
