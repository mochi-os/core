// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"crypto/ed25519"
	"errors"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"

	sl "go.starlark.net/starlark"
)

// TestEntityReadersNeedAPermission. entity.owned enumerates every object the
// user owns across every app with no argument at all, and entity.get hands over
// the data blob; info leaks the same shape for any local entity.
func TestEntityReadersNeedAPermission(t *testing.T) {
	create_test_routing_env(t)

	user := &User{UID: "u1", Username: "user1@example.com"}
	app := create_external_app("reader")
	apps[app.id] = app
	thread := create_test_thread(user, app)

	for _, c := range []struct {
		name string
		call func() (sl.Value, error)
	}{
		{"mochi.entity.owned", func() (sl.Value, error) {
			return api_entity_owned(thread, sl.NewBuiltin("mochi.entity.owned", nil), nil, nil)
		}},
		{"mochi.entity.get", func() (sl.Value, error) {
			return api_entity_get(thread, sl.NewBuiltin("mochi.entity.get", nil), sl.Tuple{sl.String("x")}, nil)
		}},
		{"mochi.entity.info", func() (sl.Value, error) {
			return api_entity_info(thread, sl.NewBuiltin("mochi.entity.info", nil), sl.Tuple{sl.String("x")}, nil)
		}},
	} {
		t.Run(c.name, func(t *testing.T) {
			_, err := c.call()
			var denied *PermissionError
			if !errors.As(err, &denied) || denied.Permission != "entity/read" {
				t.Errorf("%s returned %v, want an entity/read PermissionError", c.name, err)
			}
		})
	}

	// Granted, the call fails on the missing entities table instead - the point
	// being that the refusal is no longer the permission.
	db := db_user(user, "user")
	db.permissions_setup()
	db.permissions_upsert(app.id, "entity/read", "", 1)
	_, err := api_entity_owned(thread, sl.NewBuiltin("mochi.entity.owned", nil), nil, nil)
	var still_denied *PermissionError
	if errors.As(err, &still_denied) {
		t.Errorf("a granted app was still refused on the permission: %v", err)
	}
}

// TestEntityNameStaysUngated. name resolves one display string for an id the
// caller already holds, and entity ids are unguessable. It is also the only one
// an inbound P2P handler reaches - comptroller is not a default app and cannot
// hold a grant.
func TestEntityNameStaysUngated(t *testing.T) {
	source, err := os.ReadFile("entities.go")
	if err != nil {
		t.Fatalf("read entities.go: %v", err)
	}
	text := string(source)
	fn := text[strings.Index(text, "func api_entity_name("):]
	fn = fn[:strings.Index(fn, "\n}")]
	if strings.Contains(fn, "require_permission") {
		t.Error("api_entity_name is gated; comptroller's inbound P2P handler cannot hold the grant")
	}
	for _, name := range []string{"api_entity_owned", "api_entity_get", "api_entity_info"} {
		other := text[strings.Index(text, "func "+name+"("):]
		other = other[:strings.Index(other, "\n}")]
		if !strings.Contains(other, `entity/read`) {
			t.Errorf("%s is no longer gated; enumeration and data reads are the ones that need the grant", name)
		}
	}
}

// TestEntityReadIsStandard. Twelve first-party apps need it, and letting the
// user pick one of their own objects is an ordinary thing for an app to ask -
// the argument friends/read is standard for. Restricted would render the
// consent dialog with no Allow button and send the user hunting in settings.
func TestEntityReadIsStandard(t *testing.T) {
	if permission_restricted("entity/read") {
		t.Error("entity/read is restricted, so no consent dialog can grant it to a third-party app")
	}
	if permission_restricted("access/read") {
		t.Error("access/read is restricted; the apps that use it read their own ACL tables")
	}
}

// TestAccessCheckNeedsAPermission. The other seven mochi.access.* builtins
// touch only the app's own table; check resolves the subject's role out of
// core's users.db, which accounts/read otherwise gates.
func TestAccessCheckNeedsAPermission(t *testing.T) {
	create_test_routing_env(t)

	user := &User{UID: "u1", Username: "user1@example.com"}
	app := create_external_app("prober")
	apps[app.id] = app
	thread := create_test_thread(user, app)

	_, err := api_access_check(thread, sl.NewBuiltin("mochi.access.check", nil),
		sl.Tuple{sl.String("someone"), sl.String("probe"), sl.String("*")}, nil)
	var denied *PermissionError
	if !errors.As(err, &denied) || denied.Permission != "access/read" {
		t.Fatalf("api_access_check returned %v, want an access/read PermissionError", err)
	}

	source, err := os.ReadFile("access.go")
	if err != nil {
		t.Fatalf("read access.go: %v", err)
	}
	// Both call shapes: check is on the _acting variant because a public
	// action reaches it with no caller at all, and strict require_permission
	// refuses that before it ever looks at a grant.
	text := string(source)
	calls := strings.Count(text, "require_permission(t, fn,") +
		strings.Count(text, "require_permission_acting(t, fn,")
	if calls != 1 {
		t.Errorf("access.go has %d permission checks, want exactly 1 - the other seven APIs read only the app's own table and are meant to stay ungated", calls)
	}
}

// TestQidReachesOnlyTheFixedEndpoint. No url: grant, because the endpoint is a
// constant and the app supplies only query parameters - and url:<domain> is
// shared with mochi.url.*, so requiring it would let the app reach any path on
// the host. If the endpoint ever becomes app-supplied, the gate has to come
// back.
func TestQidReachesOnlyTheFixedEndpoint(t *testing.T) {
	if !strings.HasPrefix(qid_endpoint, "https://") {
		t.Errorf("qid_endpoint = %q, want an https URL", qid_endpoint)
	}

	body, err := os.ReadFile("qid.go")
	if err != nil {
		t.Fatalf("read qid.go: %v", err)
	}
	text := string(body)

	// Every outbound request is built from the constant. A second literal host
	// would mean a request nobody reviewing the constant would know about.
	if n := strings.Count(text, "http.NewRequest("); n != 2 {
		t.Errorf("qid.go makes %d outbound requests, want 2; a new one needs the same fixed-endpoint argument", n)
	}
	for _, line := range strings.Split(text, "\n") {
		if !strings.Contains(line, "http.NewRequest(") && !strings.Contains(line, "Sprintf") {
			continue
		}
		if strings.Contains(line, "https://") {
			t.Errorf("qid.go builds a request from a literal URL rather than qid_endpoint: %s", strings.TrimSpace(line))
		}
	}

	// And no app input reaches the host: the URL is the constant plus a query
	// string, so the only way an app could redirect the request is by having
	// its input concatenated ahead of the "?".
	for _, m := range regexp.MustCompile(`(?m)^\s*u :?= fmt\.Sprintf\("([^"]*)"`).FindAllStringSubmatch(text, -1) {
		if !strings.HasPrefix(m[1], "%s?") {
			t.Errorf("a qid request URL is built as %q; it must be the endpoint constant followed immediately by the query string", m[1])
		}
	}

	// The gate is gone and must not come back by habit.
	if strings.Contains(text, "require_permission_url") {
		t.Error("qid.go requires a url: grant again; the host is fixed in core, and the grant it would need also confers arbitrary mochi.url access to that host")
	}
}

// TestQidLanguageIsValidated. lang is half the primary key of both cache
// tables in external.db, which every app shares, so free text there lets one
// app mint unbounded rows for everyone.
func TestQidLanguageIsValidated(t *testing.T) {
	for _, bad := range []string{"", "en; DROP", "../../etc", strings.Repeat("x", 200), "EN_US!"} {
		if qid_language_regex.MatchString(bad) {
			t.Errorf("qid_language_regex accepts %q", bad)
		}
	}
	for _, good := range []string{"en", "fr", "pt-BR", "zh-Hans", "ckb"} {
		if !qid_language_regex.MatchString(good) {
			t.Errorf("qid_language_regex rejects %q", good)
		}
	}
}

// TestPeerConnectUrlCannotReachAnArbitraryPath. The URL is rebuilt from scheme
// and host rather than concatenated, or a trailing "?x=" or "#" lets the app
// choose the GET. No url: grant - the host is a Mochi server the user picked.
func TestPeerConnectUrlCannotReachAnArbitraryPath(t *testing.T) {
	source, err := os.ReadFile("remote.go")
	if err != nil {
		t.Fatalf("read remote.go: %v", err)
	}
	text := string(source)
	fn := text[strings.Index(text, "func peer_connect_url("):]
	fn = fn[:strings.Index(fn, "\n}")]

	if strings.Contains(fn, `strings.TrimSuffix(url, "/") + "/_/p2p/info"`) {
		t.Error("peer_connect_url still concatenates the app's string onto the path")
	}
	if !strings.Contains(fn, `parsed.Scheme + "://" + parsed.Host + "/_/p2p/info"`) {
		t.Error("peer_connect_url no longer rebuilds the request from scheme and host alone")
	}
	if !strings.Contains(fn, `parsed.Scheme != "http" && parsed.Scheme != "https"`) {
		t.Error("peer_connect_url does not restrict the scheme, so file:// and friends are reachable")
	}
}

// TestAppSignaturesAreDomainSeparated. Core signs export manifests and pubsub
// frames with the same entity keys and no tag, so app signatures carry the tag.
// Tagging what core signs changes what remote peers verify: a wire break.
func TestAppSignaturesAreDomainSeparated(t *testing.T) {
	create_test_routing_env(t)

	// An entity whose id is its public key, with the private half stored.
	public, private, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("keygen: %v", err)
	}
	id := base58_encode(public)
	db := db_open("db/users.db")
	db.exec("create table if not exists entities (id text not null primary key, private text not null, fingerprint text not null, user text not null, parent text not null default '', class text not null, name text not null, privacy text not null default 'public', data text not null default '', published integer not null default 0)")
	db.exec("insert into entities (id, private, fingerprint, user, class, name) values (?, ?, ?, 'u1', 'person', 'Test')",
		id, base58_encode(private), fingerprint(id))

	payload := "an export manifest, or a pubsub frame"

	// What core produces for the same bytes, and what an app now produces.
	core_signature := entity_sign(id, payload)
	app_signature := entity_sign(id, entity_domain_application+payload)
	if core_signature == "" || app_signature == "" {
		t.Fatal("signing produced nothing; the fixture entity is wrong")
	}
	if core_signature == app_signature {
		t.Fatal("an app signature over the payload equals core's, so they are still interchangeable")
	}

	// THE POINT: core verifies its own payload untagged, and the app's
	// signature must not satisfy that check.
	if entity_verify(id, payload, app_signature) {
		t.Error("an app-minted signature validates where core verifies an untagged payload - the separation does not hold")
	}
	if !entity_verify(id, payload, core_signature) {
		t.Error("core can no longer verify its own untagged signature")
	}

	// mochi.entity.verify accepts both while pre-tag signatures are still
	// around: an app's own new signature, and one made before the tag existed.
	user := &User{UID: "u1", Username: "user1@example.com"}
	app := create_external_app("wikis")
	apps[app.id] = app
	thread := create_test_thread(user, app)
	fn := sl.NewBuiltin("mochi.entity.verify", nil)
	for _, c := range []struct {
		name      string
		signature string
	}{
		{"a signature minted after the tag", app_signature},
		{"a signature minted before the tag", core_signature},
	} {
		value, err := api_entity_verify(thread, fn,
			sl.Tuple{sl.String(id), sl.String(payload), sl.String(c.signature)}, nil)
		if err != nil {
			t.Fatalf("%s: api_entity_verify: %v", c.name, err)
		}
		if value != sl.True {
			t.Errorf("%s did not verify", c.name)
		}
	}
}

// TestStreamWriterPreservesItsInterfaces. streams.go type-asserts s.writer for
// CloseWrite and SetWriteDeadline. A wrapper that drops them fails silently:
// half-close becomes a full close and write deadlines vanish.
func TestStreamWriterPreservesItsInterfaces(t *testing.T) {
	underlying := &stream_writer_probe{}
	w := &stream_writer{inner: underlying, app: "prober"}

	if _, ok := any(w).(interface{ CloseWrite() error }); !ok {
		t.Fatal("stream_writer does not satisfy CloseWrite, so close_write falls back to a full close")
	}
	if _, ok := any(w).(interface{ SetWriteDeadline(time.Time) error }); !ok {
		t.Fatal("stream_writer does not satisfy SetWriteDeadline, so write deadlines are dropped")
	}

	if err := w.CloseWrite(); err != nil {
		t.Errorf("CloseWrite: %v", err)
	}
	if !underlying.half_closed {
		t.Error("CloseWrite did not reach the underlying writer's CloseWrite")
	}
	if underlying.closed {
		t.Error("CloseWrite fully closed a writer that supports half-close")
	}
	if err := w.SetWriteDeadline(time.Now()); err != nil {
		t.Errorf("SetWriteDeadline: %v", err)
	}
	if !underlying.deadlined {
		t.Error("SetWriteDeadline did not reach the underlying writer")
	}
}

// TestStreamWriterMetersAndRefuses: bytes are charged as they are written, and
// once the budget is gone the write is refused rather than truncated.
func TestStreamWriterMetersAndRefuses(t *testing.T) {
	app := "meter-test-app"
	rate_limit_stream_outbound.reset(app)
	defer rate_limit_stream_outbound.reset(app)

	w := &stream_writer{inner: &stream_writer_probe{}, app: app}
	if n, err := w.Write(make([]byte, 4096)); err != nil || n != 4096 {
		t.Fatalf("Write returned (%d, %v), want (4096, nil)", n, err)
	}

	// Spend the budget, then the next write must be refused, not short.
	stream_outbound_charge(app, 11*1024*1024*1024)
	n, err := w.Write([]byte("more"))
	if err == nil {
		t.Error("a write past the budget was allowed")
	}
	if n != 0 {
		t.Errorf("a refused write reported %d bytes; it must write none", n)
	}
}

// stream_writer_probe stands in for a libp2p stream: it supports half-close and
// write deadlines, which is what the wrapper has to forward.
type stream_writer_probe struct {
	half_closed bool
	closed      bool
	deadlined   bool
}

func (p *stream_writer_probe) Write(b []byte) (int, error)      { return len(b), nil }
func (p *stream_writer_probe) Close() error                     { p.closed = true; return nil }
func (p *stream_writer_probe) CloseWrite() error                { p.half_closed = true; return nil }
func (p *stream_writer_probe) SetWriteDeadline(time.Time) error { p.deadlined = true; return nil }
