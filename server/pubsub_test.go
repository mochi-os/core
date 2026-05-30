// Mochi server: /mochi/2 pubsub unit tests
// Copyright Alistair Cunningham 2026

package main

import (
	"bytes"
	"crypto/ed25519"
	"sync"
	"sync/atomic"
	"testing"
)

// TestMessageSeenMarkAtomic: under concurrent receivers sharing the dedup
// map — the pubsub manager and the direct-stream workers — exactly one
// caller may win "not seen" and process; the rest must dedup. Guards the
// check-then-mark race that separate message_seen / message_mark_seen
// calls lose (first observed live as a directory/delete processed twice
// during the /mochi/1 + /mochi/2 dual-run).
func TestMessageSeenMarkAtomic(t *testing.T) {
	id := uid()
	const n = 64
	var processed atomic.Int64
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			if !message_seen_mark(id) {
				processed.Add(1)
			}
		}()
	}
	close(start)
	wg.Wait()
	if got := processed.Load(); got != 1 {
		t.Errorf("message_seen_mark let %d concurrent callers process the same id, want exactly 1", got)
	}
}

// canonical_encoder is initialised by protocol2_test.go's package init();
// the signing tests below also call protocol2_init() defensively (it's
// idempotent) so they pass regardless of test file ordering.

// TestPubsubExpiresTTLExceedsMaxRetry: the freshness TTL must outlive the
// longest queue-broadcast retry interval, so a queue-held re-flood is
// never already expired by the time it lands (and a peer announcement
// stays valid past the hourly peers_publish cadence). The Expires +
// signature are recomputed per flood, but the invariant is the cheap
// guard against a future TTL change dipping below the retry ceiling.
func TestPubsubExpiresTTLExceedsMaxRetry(t *testing.T) {
	var max int64
	for _, d := range retry_delays {
		if d > max {
			max = d
		}
	}
	if pubsub_expires_ttl <= max {
		t.Errorf("pubsub_expires_ttl (%d) must exceed the max retry interval (%d)", pubsub_expires_ttl, max)
	}
}

// TestPubsubFresh: the freshness window accepts a now()+ttl stamp and
// rejects missing, zero, expired, and absurdly-far-future ones.
func TestPubsubFresh(t *testing.T) {
	base := now()
	cases := []struct {
		name    string
		expires string
		want    bool
	}{
		{"full-ttl", i64toa(base + pubsub_expires_ttl), true},
		{"near-future", i64toa(base + 60), true},
		{"missing", "", false},
		{"zero", "0", false},
		{"expired", i64toa(base - 1), false},
		{"far-future", i64toa(base + pubsub_expires_max + 60), false},
	}
	for _, c := range cases {
		if got := pubsub_fresh(c.expires); got != c.want {
			t.Errorf("%s: pubsub_fresh(%q) = %v, want %v", c.name, c.expires, got, c.want)
		}
	}
}

// TestPubsubStringContent: all-string content projects fully; any
// non-string value fails the projection so the caller rejects the frame.
func TestPubsubStringContent(t *testing.T) {
	if out, ok := pubsub_string_content(map[string]any{"a": "1", "b": "two"}); !ok || out["a"] != "1" || out["b"] != "two" {
		t.Errorf("all-string content: out=%v ok=%v", out, ok)
	}
	if _, ok := pubsub_string_content(map[string]any{"a": "1", "n": int64(2)}); ok {
		t.Error("non-string value should fail projection")
	}
	if out, ok := pubsub_string_content(nil); !ok || len(out) != 0 {
		t.Errorf("nil content: out=%v ok=%v", out, ok)
	}
}

// TestPubsubSignableDeterministic: the canonical signable is byte-identical
// whether the content arrives as a freshly-built map[string]string or as
// the map[string]any a frame decodes to — the property the all-string
// rule exists to guarantee. If it ever diverges, signature verification
// silently breaks on the receiver.
func TestPubsubSignableDeterministic(t *testing.T) {
	protocol2_init()
	from := test_entity_id('a')
	expires := i64toa(now() + pubsub_expires_ttl)
	content := map[string]string{"id": from, "name": "Alice", "version": "100", "created": "1000"}

	direct, err := pubsub_signable(from, "directory", "publish", expires, content)
	if err != nil {
		t.Fatalf("signable (direct): %v", err)
	}

	// Simulate the receiver: encode content as a frame would carry it,
	// decode to map[string]any, project back to map[string]string.
	var decoded map[string]any
	if err := cbor_decode_mode.Unmarshal(cbor_encode(content), &decoded); err != nil {
		t.Fatalf("content round-trip decode: %v", err)
	}
	proj, ok := pubsub_string_content(decoded)
	if !ok {
		t.Fatal("projection of round-tripped content failed")
	}
	roundtrip, err := pubsub_signable(from, "directory", "publish", expires, proj)
	if err != nil {
		t.Fatalf("signable (round-trip): %v", err)
	}

	if !bytes.Equal(direct, roundtrip) {
		t.Error("canonical signable differs between map[string]string and decoded map[string]any — determinism broken")
	}
}

// TestPubsubSignVerify: a signature produced by the entity verifies, and a
// tamper to any signed field (content, expires, service, event) or the
// wrong verify key fails.
func TestPubsubSignVerify(t *testing.T) {
	protocol2_init()
	cleanup := setup_replication_test(t)
	defer cleanup()
	setup_users_test_schema()
	id, _ := new_entity_keys(t)

	expires := i64toa(now() + pubsub_expires_ttl)
	content := map[string]string{"id": id, "name": "Alice", "version": "100"}

	sig := pubsub_sign(id, "directory", "publish", expires, content)
	if len(sig) != ed25519.SignatureSize {
		t.Fatalf("pubsub_sign returned %d-byte signature, want %d", len(sig), ed25519.SignatureSize)
	}

	if err := pubsub_verify(id, "directory", "publish", expires, content, sig); err != nil {
		t.Errorf("verify of untampered signature failed: %v", err)
	}

	tampered := map[string]string{"id": id, "name": "Mallory", "version": "100"}
	if err := pubsub_verify(id, "directory", "publish", expires, tampered, sig); err == nil {
		t.Error("verify accepted tampered content")
	}
	// Rewriting expires to extend the window must fail (expires is signed).
	if err := pubsub_verify(id, "directory", "publish", i64toa(now()+pubsub_expires_max), content, sig); err == nil {
		t.Error("verify accepted tampered expires")
	}
	if err := pubsub_verify(id, "directory", "delete", expires, content, sig); err == nil {
		t.Error("verify accepted tampered event")
	}
	other, _ := new_entity_keys(t)
	if err := pubsub_verify(other, "directory", "publish", expires, content, sig); err == nil {
		t.Error("verify accepted signature under the wrong entity")
	}
}

// signed_directory_frame builds the wire bytes of a signed directory/publish
// Frame at the given version, with a fresh in-window Expires.
func signed_directory_frame(t *testing.T, id, name string, version int64) []byte {
	t.Helper()
	return directory_frame(t, id, name, version, i64toa(now()+pubsub_expires_ttl))
}

// directory_frame is signed_directory_frame with an explicit Expires, for
// the freshness cases.
func directory_frame(t *testing.T, id, name string, version int64, expires string) []byte {
	t.Helper()
	content := map[string]string{
		"id": id, "name": name, "class": "person",
		"location": "p2p/peerY", "data": "x",
		"created": "1000", "version": i64toa(version),
	}
	sig := pubsub_sign(id, "directory", "publish", expires, content)
	cmap := make(map[string]any, len(content))
	for k, v := range content {
		cmap[k] = v
	}
	f := &Frame{
		Type: frame_type_message, From: id,
		Service: "directory", Event: "publish", ID: uid(),
		Expires: expires, Content: cmap, Signature: sig,
	}
	var buf bytes.Buffer
	if err := frame_write(&buf, f); err != nil {
		t.Fatalf("frame_write: %v", err)
	}
	return buf.Bytes()
}

// TestPubsubReceiveRoutesDirectory: a valid signed directory/publish
// Frame decodes, verifies, and routes through to a directory.db write; a
// lower-version Frame is dropped by version-LWW; an expired Frame is
// dropped before routing. Exercises the whole /mochi/2 receive path.
func TestPubsubReceiveRoutesDirectory(t *testing.T) {
	protocol2_init()
	cleanup := setup_replication_test(t)
	defer cleanup()
	setup_users_test_schema()
	ddb := db_open("db/directory.db")
	ddb.exec("create table entities ( id text not null primary key, name text not null, class text not null, location text not null default '', data text not null default '', fingerprint text not null default '', created integer not null, updated integer not null, version integer not null default 0 )")
	ddb.exec("create table locations ( entity text not null, peer text not null, seen integer not null, primary key (entity, peer) )")

	id, _ := new_entity_keys(t)

	// Newer announce (version 200) routes and writes.
	pubsub_receive(signed_directory_frame(t, id, "Alice Smith", 200), "peerY")
	if name, ver := dir_entity(t, ddb, id); name != "Alice Smith" || ver != 200 {
		t.Fatalf("after v200 frame: name=%q version=%d, want Alice Smith/200", name, ver)
	}

	// Older announce (version 100) is dropped by version-LWW.
	pubsub_receive(signed_directory_frame(t, id, "Alice", 100), "peerY")
	if name, ver := dir_entity(t, ddb, id); name != "Alice Smith" || ver != 200 {
		t.Errorf("stale v100 frame clobbered record: name=%q version=%d, want Alice Smith/200", name, ver)
	}

	// Expired frame is dropped at the freshness check, before routing.
	pubsub_receive(directory_frame(t, id, "Expired", 300, i64toa(now()-1)), "peerY")
	if name, ver := dir_entity(t, ddb, id); name != "Alice Smith" || ver != 200 {
		t.Errorf("expired v300 frame was applied: name=%q version=%d, want Alice Smith/200", name, ver)
	}
}
