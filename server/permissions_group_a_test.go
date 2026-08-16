// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"crypto/ed25519"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	sl "go.starlark.net/starlark"
)

// TestEntityReadersNeedAPermission. entity.owned enumerates every object the
// user owns across every app with no argument at all - the disclosure
// mochi.group.list is gated to prevent, in that function's own words - and
// entity.get hands over the data blob with it. info and name leak the same
// shape for any local entity.
func TestEntityReadersNeedAPermission(t *testing.T) {
	cleanup := create_test_routing_env(t)
	defer cleanup()

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

	// Granted, the call gets past the permission and on to the query. The test
	// environment has no entities table, so it fails there instead - which is
	// the point: the refusal is no longer the permission.
	db := db_user(user, "user")
	db.permissions_setup()
	db.permissions_upsert(app.id, "entity/read", "", 1)
	_, err := api_entity_owned(thread, sl.NewBuiltin("mochi.entity.owned", nil), nil, nil)
	var stillDenied *PermissionError
	if errors.As(err, &stillDenied) {
		t.Errorf("a granted app was still refused on the permission: %v", err)
	}
}

// TestEntityNameStaysUngated. name resolves one display string for an id the
// caller already holds, and entity ids are unguessable public keys - the
// exemption mochi.group.get relies on. It is also the only one of the four an
// inbound P2P handler reaches: comptroller calls it from
// event_staff_accounts_list and, not being a default app, has no way to hold a
// grant at all (task #135). Gating it took comptroller's staff endpoints down
// with a 500 on 2026-08-15.
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
// touch only the calling app's own table. check is different: it resolves the
// subject's role out of core's users.db, so an app that writes
// allow("#administrator", ...) into its own table can then ask, for any
// identity it holds, whether that identity is a local account and an
// administrator - which accounts/read otherwise gates.
func TestAccessCheckNeedsAPermission(t *testing.T) {
	cleanup := create_test_routing_env(t)
	defer cleanup()

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
	if n := strings.Count(string(source), "require_permission(t, fn,"); n != 1 {
		t.Errorf("access.go has %d permission checks, want exactly 1 - the other seven APIs read only the app's own table and are meant to stay ungated", n)
	}
}

// TestQidRequiresTheWikidataGrant. Both APIs reach Wikidata, and every other
// outbound call in core requires the url: grant. The host is fixed in the
// source, so the grant is exact rather than the url:* wildcard.
func TestQidRequiresTheWikidataGrant(t *testing.T) {
	cleanup := create_test_routing_env(t)
	defer cleanup()

	user := &User{UID: "u1", Username: "user1@example.com"}
	app := create_external_app("looker")
	apps[app.id] = app
	thread := create_test_thread(user, app)

	for _, c := range []struct {
		name string
		call func() (sl.Value, error)
	}{
		{"mochi.qid.lookup", func() (sl.Value, error) {
			return api_qid_lookup(thread, sl.NewBuiltin("mochi.qid.lookup", nil),
				sl.Tuple{sl.String("Q42"), sl.String("en")}, nil)
		}},
		{"mochi.qid.search", func() (sl.Value, error) {
			return api_qid_search(thread, sl.NewBuiltin("mochi.qid.search", nil),
				sl.Tuple{sl.String("douglas"), sl.String("en")}, nil)
		}},
	} {
		t.Run(c.name, func(t *testing.T) {
			_, err := c.call()
			var denied *PermissionError
			if !errors.As(err, &denied) {
				t.Fatalf("%s returned %v, want a PermissionError", c.name, err)
			}
			if denied.Permission != "url:www.wikidata.org" {
				t.Errorf("refused on %q, want url:www.wikidata.org - the host is fixed, so the grant should not need the wildcard", denied.Permission)
			}
		})
	}

	// Feeds and Forums are the only callers and must arrive holding it.
	for _, name := range []string{"Feeds", "Forums"} {
		found := false
		for _, app := range apps_default {
			if app.Name != name {
				continue
			}
			for _, g := range app.Permissions {
				if g.Permission == "url" && g.Object == "www.wikidata.org" {
					found = true
				}
			}
		}
		if !found {
			t.Errorf("default app %q calls mochi.qid but has no url:www.wikidata.org grant", name)
		}
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

// TestPeerConnectUrlCannotReachAnArbitraryPath. The app supplies this string
// and it used to be concatenated onto "/_/p2p/info": a trailing "?x=" carried
// a path and query through, and a trailing "#" discarded the suffix, so either
// way the app chose the GET. Rebuilding from scheme and host is what closes it,
// and it is why this needs no url: grant - unlike the sibling APIs, the host
// here is a Mochi server the user picked, not a domain known in advance.
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
// frames with the same entity keys and no tag, and an app can emit those exact
// bytes - so without separation an app-minted signature is indistinguishable
// from a core-issued one. The tag goes on the APP side: tagging what core signs
// would change what remote peers verify, which is a wire break.
func TestAppSignaturesAreDomainSeparated(t *testing.T) {
	cleanup := create_test_routing_env(t)
	defer cleanup()

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

// TestStreamWriterPreservesItsInterfaces. streams.go type-asserts on s.writer
// in three places - close_write reaches for CloseWrite, and write and write_raw
// both reach for SetWriteDeadline. A metering wrapper that did not carry those
// across would fail the assertions silently: half-close would become a full
// close, and every write deadline would vanish. That is the hazard that makes
// this wrapper worth a test rather than a glance.
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
