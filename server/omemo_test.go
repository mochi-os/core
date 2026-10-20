// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"encoding/base64"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	sl "go.starlark.net/starlark"
	"mellium.im/xmpp/jid"
)

// omemo_published waits until an account's device is in its device lists.
func omemo_published(t *testing.T, user *User, account string) *omemo_device {
	t.Helper()
	d, err := omemo_device_of(user, account)
	if err != nil {
		t.Fatal(err)
	}
	if err := d.announce(context.Background(), false); err != nil {
		t.Fatalf("device not published: %v", err)
	}
	return d
}

// omemo_send sends a message of a conversation from Mochi's own sessions on a
// hosted account, as chat does.
func omemo_send(t *testing.T, user *User, account string, to string, text string, key string) map[string]any {
	t.Helper()
	return sl_decode(xmpp_call(t, user, api_account_object_create, sl.Tuple{sl.String(account), xmpp_object("conversation", "xmpp:"+to, "content", text)}, sl.Tuple{sl.String("key"), sl.String(key)})).(map[string]any)
}

// TestOmemoHosted: an account's device is published in its device lists and
// bundles of both versions, open to anyone; a message to someone with no
// devices goes in the clear, and once they have one it goes encrypted to
// their device and the sender's other devices in both versions, with the
// text other clients show, and is read by their device before chat hears
// it; the answer reads the other way, the key exchange stops once answered,
// and a message the other side's device began a session with is answered
// with keys only, which chat never hears. The archive gives an encrypted
// message as it came.
func TestOmemoHosted(t *testing.T) {
	env := create_xmpp_server_env(t)
	env.both()
	b := env.bob_online(t)

	// Bob has no device yet: the message goes in the clear.
	sent := omemo_send(t, env.alice, "x1", "bob@example.com", "In the clear", "chat|k0")
	if sent["encrypted"] == true {
		t.Errorf("THE POINT: a message to someone with no devices encrypted: %v", sent)
	}
	b.expect(t, "the clear message", ">In the clear</body>")

	alice := omemo_published(t, env.alice, "x1")
	bob := omemo_published(t, env.bob, "x2")
	a := &xmpp_account{user: env.bob, id: "x2", address: jid.MustParse("bob@example.com")}
	if config := xmpp_node_of(a, omemo_legacy_list); config == nil || config.access != "open" {
		t.Fatalf("THE POINT: bob's device list open to anyone: %v", config)
	}
	listed, _ := bob.listed(context.Background(), jid.MustParse("bob@example.com"), omemo_current, true)
	if len(listed) != 1 || listed[0] != bob.id {
		t.Fatalf("THE POINT: bob's device in his list: %v", listed)
	}
	items, _ := alice.fetch(context.Background(), jid.MustParse("bob@example.com"), omemo_current_nodes, strconv.FormatUint(uint64(bob.id), 10))
	if len(items) != 1 || strings.Count(items[0].Inner, "<pk ") != omemo_prekeys {
		t.Fatalf("THE POINT: bob's OMEMO 2 bundle with its pre-keys: %v", items)
	}

	sent = omemo_send(t, env.alice, "x1", "bob@example.com", "Secret", "chat|k1")
	if sent["encrypted"] != true || !strings.Contains(sent["payload"].(string), omemo_current) {
		t.Fatalf("THE POINT: the message sent encrypted, its payload answered: %v", sent)
	}
	got := env.log.find(t, "message", func(e map[string]any) bool { return e["account"] == "x2" && e["id"] == "chat|k1" })
	if got["body"] != "Secret" || got["encrypted"] != true {
		t.Fatalf("THE POINT: bob's device read it before chat heard it: %v", got)
	}
	if payload, _ := got["payload"].(string); !strings.Contains(payload, `<encrypted xmlns="`+omemo_current+`">`) {
		t.Errorf("the encrypted content kept for the archive: %v", got["payload"])
	}
	stanza := b.expect(t, "the encrypted message", `id="chat|k1"`)
	if strings.Contains(stanza, "Secret") || !strings.Contains(stanza, omemo_fallback[:20]) || !strings.Contains(stanza, `xmlns="urn:xmpp:eme:0"`) {
		t.Errorf("THE POINT: bob's client given it encrypted, with the fallback text: %s", stanza)
	}

	// Bob's device answers the key exchange with keys alone, which alice's
	// device takes and chat never hears.
	wait(t, "alice's session acknowledged", func() bool {
		s := alice.session(omemo_current, jid.MustParse("bob@example.com"), bob.id)
		return s != nil && s.Exchange == nil
	})
	if prekeys, _ := omemo_database(env.bob).row("select count(*) as n from prekeys where account='x2'"); row_int(prekeys, "n") != int64(omemo_prekeys) {
		t.Errorf("THE POINT: bob's used pre-key replaced: %v", prekeys)
	}

	omemo_send(t, env.bob, "x2", "alice@example.com", "Reply", "chat|k2")
	reply := env.log.find(t, "message", func(e map[string]any) bool { return e["account"] == "x1" && e["id"] == "chat|k2" })
	if reply["body"] != "Reply" || reply["encrypted"] != true {
		t.Fatalf("THE POINT: the reply read by alice's device: %v", reply)
	}
	env.log.lock.Lock()
	for _, e := range env.log.events {
		if e["kind"] == "message" && e["account"] == "x1" && e["body"] == "" {
			t.Errorf("THE POINT: a key transport heard by chat: %v", e)
		}
	}
	env.log.lock.Unlock()

	// The archive gives an encrypted message as it came, never as read.
	entries := xmpp_archive_items(&xmpp_account{user: env.bob, id: "x2", address: jid.MustParse("bob@example.com")}, map[string]any{"archive": "A1", "id": "chat|k1", "person": "xmpp:alice@example.com", "body": "Secret", "created": int64(1), "payload": got["payload"]})
	if len(entries) != 1 || strings.Contains(entries[0].text, "Secret") || !strings.Contains(entries[0].text, omemo_current) {
		t.Errorf("THE POINT: the archive's encrypted message: %v", entries)
	}
}

// TestOmemoTrust: a person's devices are used blindly until one of theirs is
// verified, then only those verified; a distrusted device is neither
// encrypted to nor read, and a message only for it is refused.
func TestOmemoTrust(t *testing.T) {
	env := create_xmpp_server_env(t)
	env.both()
	alice := omemo_published(t, env.alice, "x1")
	bob := omemo_published(t, env.bob, "x2")
	person := jid.MustParse("bob@example.com")
	omemo_send(t, env.alice, "x1", "bob@example.com", "First", "chat|t1")
	env.log.find(t, "message", func(e map[string]any) bool { return e["id"] == "chat|t1" && e["body"] == "First" })

	db := omemo_database(env.alice)
	db.exec("insert into identities (account, person, device, key, trust, created) values ('x1', 'bob@example.com', 7, 'a2V5', '', 1)")
	if !alice.usable(person, bob.id) || !alice.usable(person, 7) {
		t.Fatal("THE POINT: devices used blindly before any is verified")
	}
	if err := alice.trust(person, bob.id, "verified"); err != nil {
		t.Fatal(err)
	}
	if !alice.usable(person, bob.id) || alice.usable(person, 7) {
		t.Errorf("THE POINT: once one is verified, only those verified")
	}
	if err := alice.trust(person, 7, "verified"); err != nil || !alice.usable(person, 7) {
		t.Errorf("a second device verified: %v", err)
	}
	if err := alice.trust(person, 99, "verified"); err == nil {
		t.Error("an unknown device's trust recorded")
	}

	if err := bob.trust(jid.MustParse("alice@example.com"), alice.id, "distrusted"); err != nil {
		t.Fatal(err)
	}
	omemo_send(t, env.alice, "x1", "bob@example.com", "Unread", "chat|t2")
	got := env.log.find(t, "message", func(e map[string]any) bool { return e["id"] == "chat|t2" })
	if got["body"] != "" || got["encrypted"] != true {
		t.Errorf("THE POINT: a distrusted device's message not read: %v", got)
	}
	if value, _ := api_account_object_create(linked_thread(env.bob), sl.NewBuiltin("test", nil), sl.Tuple{sl.String("x2"), xmpp_object("conversation", "xmpp:alice@example.com", "content", "Nobody")}, nil); value != sl.None {
		t.Errorf("THE POINT: a message whose only device is distrusted sent: %v", value)
	}
}

// TestOmemoBundle: a bundle is read in each version with one of its
// one-time pre-keys; one missing its parts is refused.
func TestOmemoBundle(t *testing.T) {
	identity, signed, once := omemo_generate(), omemo_generate(), omemo_generate()
	legacy := `<bundle xmlns="` + omemo_legacy + `"><signedPreKeyPublic signedPreKeyId="4">` + base64.StdEncoding.EncodeToString(omemo_typed(signed.Public)) + `</signedPreKeyPublic><signedPreKeySignature>` + base64.StdEncoding.EncodeToString(omemo_sign(identity.Private, omemo_typed(signed.Public))) + `</signedPreKeySignature><identityKey>` + base64.StdEncoding.EncodeToString(omemo_typed(identity.Public)) + `</identityKey><prekeys><preKeyPublic preKeyId="9">` + base64.StdEncoding.EncodeToString(omemo_typed(once.Public)) + `</preKeyPublic></prekeys></bundle>`
	b, err := omemo_bundle_parse(omemo_legacy, legacy)
	if err != nil || b.signed != 4 || b.prekey != 9 || !bytes.Equal(b.identity, identity.Public) || !bytes.Equal(b.once, once.Public) {
		t.Fatalf("THE POINT: the original version's bundle: %v %+v", err, b)
	}
	if _, err := omemo_begin(omemo_legacy, omemo_generate(), b); err != nil {
		t.Errorf("a session begun from it: %v", err)
	}
	current := `<bundle xmlns="` + omemo_current + `"><spk id="4">` + base64.StdEncoding.EncodeToString(signed.Public) + `</spk><spks>` + base64.StdEncoding.EncodeToString(omemo_sign(identity.Private, signed.Public)) + `</spks><ik>` + base64.StdEncoding.EncodeToString(omemo_identity(identity.Public)) + `</ik><prekeys><pk id="9">` + base64.StdEncoding.EncodeToString(once.Public) + `</pk></prekeys></bundle>`
	b, err = omemo_bundle_parse(omemo_current, current)
	if err != nil || !bytes.Equal(b.identity, identity.Public) || !bytes.Equal(b.edwards, omemo_identity(identity.Public)) || b.prekey != 9 {
		t.Fatalf("THE POINT: OMEMO 2's bundle: %v %+v", err, b)
	}
	if _, err := omemo_begin(omemo_current, omemo_generate(), b); err != nil {
		t.Errorf("a session begun from it: %v", err)
	}
	if _, err := omemo_bundle_parse(omemo_current, `<bundle xmlns="`+omemo_current+`"><spk id="4">AAAA</spk></bundle>`); err == nil {
		t.Error("THE POINT: a bundle missing its parts read")
	}
}

// TestOmemoContent: the original version's payload is read with its tag after
// the key or after the payload; OMEMO 2's envelope is read where it came from
// the sender and, in a room, was for it; and what it protected is given to
// the message.
func TestOmemoContent(t *testing.T) {
	material, payload := omemo_envelope(`<body xmlns="jabber:client">Hi &amp; bye</body><reply xmlns="urn:xmpp:reply:0" id="r1"/>`, "alice@example.com", "club@rooms.example.com")
	sealed, _ := base64.StdEncoding.DecodeString(payload)
	content, err := omemo_unenvelope(material, sealed, "alice@example.com", "club@rooms.example.com", "bob@example.com")
	if err != nil {
		t.Fatalf("THE POINT: the envelope read: %v", err)
	}
	if _, err := omemo_unenvelope(material, sealed, "mallory@example.com", "", "bob@example.com"); err == nil {
		t.Error("THE POINT: an envelope naming another sender read")
	}
	if _, err := omemo_unenvelope(material, sealed, "alice@example.com", "other@rooms.example.com", "bob@example.com"); err == nil {
		t.Error("THE POINT: an envelope for another room read")
	}
	changed := bytes.Clone(sealed)
	changed[0] ^= 1
	if _, err := omemo_unenvelope(material, changed, "alice@example.com", "", "bob@example.com"); err == nil {
		t.Error("THE POINT: a changed payload read")
	}
	var m xmpp_message
	omemo_apply(&m, omemo_current, content)
	if m.opened == nil || *m.opened != "Hi & bye" || m.Reply == nil || m.Reply.ID != "r1" {
		t.Errorf("THE POINT: what the envelope protected given to the message: %+v", m)
	}

	// The original version: AES-GCM, its tag with the key or the payload.
	for _, tagged := range []bool{true, false} {
		key, iv := bytes.Repeat([]byte{1}, 16), bytes.Repeat([]byte{2}, 12)
		sealed := omemo_test_gcm(t, key, iv, "Legacy")
		material, body := append(key, sealed[len(sealed)-16:]...), sealed[:len(sealed)-16]
		if !tagged {
			material, body = key, sealed
		}
		if text, err := omemo_gcm_open(material, iv, body); err != nil || text != "Legacy" {
			t.Errorf("THE POINT: the original version's payload, tag with the key %v: %q %v", tagged, text, err)
		}
	}
	if _, err := omemo_gcm_open(make([]byte, 20), make([]byte, 12), make([]byte, 20)); err == nil {
		t.Error("key material of another length read")
	}
}

// TestOmemoFiles: a file shared encrypted is named by an aesgcm address
// carrying its key, fetched at the https address it stands for and decrypted
// with the key kept for it; one with no key kept is given as fetched.
func TestOmemoFiles(t *testing.T) {
	create_signin_test_env(t)
	user := user_by_uid("u-alice")
	sealed, key := omemo_file_seal([]byte("picture"))
	address, found := omemo_shared("aesgcm://files.example.com/a/photo.jpg#" + key)
	if address != "https://files.example.com/a/photo.jpg" || found != key {
		t.Fatalf("THE POINT: the address and key: %q %q", address, found)
	}
	for _, bad := range []string{"https://files.example.com/a#" + key, "aesgcm://files.example.com/a#00", "aesgcm://files.example.com/a b#" + key} {
		if address, _ := omemo_shared(bad); address != "" {
			t.Errorf("%q read as a shared file", bad)
		}
	}
	encryption_database(user).exec("insert into files (account, url, file, created) values ('x1', ?, ?, 1)", address, json_encode(map[string]string{"aesgcm": key}))
	if plain, err := encryption_opened(user, "x1", address, sealed); err != nil || string(plain) != "picture" {
		t.Errorf("THE POINT: the file decrypted: %q %v", plain, err)
	}
	if plain, _ := encryption_opened(user, "x1", "https://files.example.com/other", []byte("as is")); string(plain) != "as is" {
		t.Errorf("a file with no key changed: %q", plain)
	}
	if _, err := encryption_opened(user, "x1", address, []byte("not sealed")); err == nil {
		t.Error("THE POINT: a file that does not decrypt given")
	}
}

// omemo_test_gcm seals text with AES-GCM, its tag after it.
func omemo_test_gcm(t *testing.T, key []byte, iv []byte, text string) []byte {
	t.Helper()
	block, err := aes.NewCipher(key)
	if err != nil {
		t.Fatal(err)
	}
	gcm, _ := cipher.NewGCM(block)
	return gcm.Seal(nil, iv, []byte(text), nil)
}

// TestOmemoRoom: in a room that shows who its people are, each of whom has a
// device, a message goes encrypted to their devices, OMEMO 2's envelope
// naming the room, and the other's device reads it from the person the room
// shows the sender to be. A room where someone has no device takes it in
// the clear.
func TestOmemoRoom(t *testing.T) {
	env := create_xmpp_server_env(t)
	env.both()
	omemo_published(t, env.alice, "x1")
	omemo_published(t, env.bob, "x2")
	made := sl_decode(xmpp_call(t, env.alice, api_account_room_create, sl.Tuple{sl.String("x1"), sl.NewList([]sl.Value{sl.String("bob@example.com"), sl.String("carol@remote.test")})})).(map[string]any)
	conversation := made["conversation"].(string)
	room := strings.TrimPrefix(conversation, "xmpp:")
	xmpp_call(t, env.bob, api_account_room_join, sl.Tuple{sl.String("x2"), sl.String(room)})
	// A second device of alice's, which a room's message goes to as one of
	// the sender's own.
	other := omemo_test_new()
	a := &xmpp_account{user: env.alice, id: "x1", address: jid.MustParse("alice@example.com")}
	b := other.bundle(omemo_current)
	xmpp_pep_publish(a, omemo_current_nodes, "4242", `<bundle xmlns="`+omemo_current+`"><spk id="7">`+base64.StdEncoding.EncodeToString(b.key)+`</spk><spks>`+base64.StdEncoding.EncodeToString(b.signature)+`</spks><ik>`+base64.StdEncoding.EncodeToString(b.edwards)+`</ik><prekeys><pk id="9">`+base64.StdEncoding.EncodeToString(b.once)+`</pk></prekeys></bundle>`, map[string]string{"pubsub#access_model": "open", "pubsub#max_items": "max"})
	alice := omemo_devices["u-alice|x1"]
	alice.list_publish(context.Background(), omemo_current, []uint32{alice.id, 4242})
	wait(t, "each known to the other by address", func() bool {
		a, b := omemo_people("x1", jid.MustParse(room), true), omemo_people("x2", jid.MustParse(room), true)
		return len(a) == 1 && a[0].String() == "bob@example.com" && len(b) == 1 && b[0].String() == "alice@example.com"
	})
	// Another client of alice's in the room, by her own address.
	omemo_occupant("x1", room, "alice-phone", "alice@example.com", false)
	sent := omemo_send(t, env.alice, "x1", room, "In the room", "chat|r1")
	if payload, _ := sent["payload"].(string); strings.Count(payload, `<keys jid="alice@example.com"`) != 1 || strings.Count(payload, `rid="4242"`) != 1 {
		t.Errorf("THE POINT: the sender's other device named once: %s", payload)
	}
	got := env.log.find(t, "message", func(e map[string]any) bool { return e["account"] == "x2" && e["body"] == "In the room" })
	if got["encrypted"] != true || got["conversation"] != conversation {
		t.Errorf("THE POINT: the room's message read by bob's device: %v %v", got, sent)
	}

	// Someone with no device joins: the room takes messages in the clear.
	omemo_occupant("x1", room, "carol", "carol@example.com", false)
	omemo_send(t, env.alice, "x1", room, "To everyone", "chat|r2")
	clear := env.log.find(t, "message", func(e map[string]any) bool { return e["account"] == "x2" && e["body"] == "To everyone" })
	if clear["encrypted"] != false {
		t.Errorf("THE POINT: a room with someone of no device takes it in the clear: %v", clear)
	}
	// One shown only by nick: nothing can be encrypted to them.
	omemo_occupant("x1", room, "carol", "", false)
	if people := omemo_people("x1", jid.MustParse(room), true); people != nil {
		t.Errorf("THE POINT: a room hiding someone's address encrypted to: %v", people)
	}
}

// TestOmemoFilesHosted: a file for an encrypted conversation is uploaded
// encrypted and shared by its aesgcm address inside the encrypted message;
// the other side's apps are given its https address, whose key their device
// keeps, and the stored file decrypts with it.
func TestOmemoFilesHosted(t *testing.T) {
	env := create_xmpp_server_env(t)
	env.both()
	omemo_published(t, env.alice, "x1")
	omemo_published(t, env.bob, "x2")
	fn := sl.NewBuiltin("test", nil)
	uploaded := decoded(t)(api_account_upload(linked_thread(env.bob), fn, sl.Tuple{sl.String("x2"), sl.Bytes("a picture"), sl.String("image/png")}, []sl.Tuple{{sl.String("name"), sl.String("p.png")}, {sl.String("conversation"), sl.String("xmpp:alice@example.com")}}))
	address := uploaded["url"].(string)
	row, _ := xmpp_database().row("select id from uploads where user='u-bob' order by created desc limit 1")
	stored, _ := os.ReadFile(media_path(env.bob, row_string(row, "id")))
	if bytes.Contains(stored, []byte("a picture")) || uploaded["type"] != "image/png" || encryption_shared(env.bob, "x2", address) == "" {
		t.Fatalf("THE POINT: the file uploaded encrypted, its key kept: %v %q", uploaded, stored)
	}
	sent := sl_decode(xmpp_call(t, env.bob, api_account_object_create, sl.Tuple{sl.String("x2"), sl.Value(sl_encode(map[string]any{"conversation": "xmpp:alice@example.com", "file": uploaded}))}, sl.Tuple{sl.String("key"), sl.String("chat|f1")})).(map[string]any)
	if sent["encrypted"] != true || strings.Contains(sent["payload"].(string), "aesgcm") {
		t.Fatalf("THE POINT: the file's message encrypted: %v", sent)
	}
	got := env.log.find(t, "message", func(e map[string]any) bool { return e["account"] == "x1" && e["id"] == "chat|f1" })
	files, _ := got["files"].([]any)
	if len(files) != 1 || files[0].(map[string]any)["url"] != address || got["body"] != "" {
		t.Fatalf("THE POINT: alice's apps given the file's address: %v", got)
	}
	if plain, err := encryption_opened(env.alice, "x1", address, stored); err != nil || string(plain) != "a picture" {
		t.Errorf("THE POINT: the file decrypted with the key alice's device kept: %q %v", plain, err)
	}
}

// TestOmemoNotices: a device list someone's server tells of as it changes is
// kept without asking; the account's own list without its device has it
// published again.
func TestOmemoNotices(t *testing.T) {
	env := create_xmpp_server_env(t)
	env.both()
	alice := omemo_published(t, env.alice, "x1")
	bob := omemo_published(t, env.bob, "x2")
	b := &xmpp_account{user: env.bob, id: "x2", address: jid.MustParse("bob@example.com")}
	xmpp_pep_publish(b, omemo_current_list, "current", `<devices xmlns="`+omemo_current+`"><device id="`+strconv.FormatUint(uint64(bob.id), 10)+`"/><device id="77"/></devices>`, nil)
	wait(t, "alice told of bob's new list", func() bool {
		row, _ := omemo_database(env.alice).row("select devices from lists where account='x1' and person='bob@example.com' and version=?", omemo_current)
		return strings.Contains(row_string(row, "devices"), "77")
	})
	a := &xmpp_account{user: env.alice, id: "x1", address: jid.MustParse("alice@example.com")}
	xmpp_pep_publish(a, omemo_legacy_list, "current", `<list xmlns="`+omemo_legacy+`"><device id="5"/></list>`, nil)
	alice.heard(context.Background(), jid.MustParse("alice@example.com"), omemo_legacy_list, `<list xmlns="`+omemo_legacy+`"><device id="5"/></list>`)
	wait(t, "alice's device published again", func() bool {
		items, _ := alice.fetch(context.Background(), jid.MustParse("alice@example.com"), omemo_legacy_list, "")
		return len(items) == 1 && strings.Contains(items[0].Inner, strconv.FormatUint(uint64(alice.id), 10)) && strings.Contains(items[0].Inner, `id="5"`)
	})
}

// TestOmemoFunctions: the apps' functions show an XMPP account's device, a
// fingerprint for each version, and a person's devices with the user's word
// on each, a person in a room named as the room shows them; Matrix's alone
// are refused for an XMPP account.
func TestOmemoFunctions(t *testing.T) {
	env := create_xmpp_server_env(t)
	env.both()
	bob := omemo_published(t, env.bob, "x2")
	thread, fn := linked_thread(env.alice), sl.NewBuiltin("test", nil)
	xmpp := decoded(t)(api_account_encryption_status(thread, fn, sl.Tuple{sl.String("x1")}, nil))
	if fingerprints, _ := xmpp["fingerprints"].([]any); xmpp["network"] != "xmpp" || len(fingerprints) != 2 {
		t.Errorf("THE POINT: the XMPP device, a fingerprint for each version: %v", xmpp)
	}
	if _, err := api_account_encryption_setup(thread, fn, sl.Tuple{sl.String("x1")}, nil); err == nil {
		t.Error("THE POINT: Matrix's recovery set up for an XMPP account")
	}
	if value, err := api_account_encryption_verifications(thread, fn, sl.Tuple{sl.String("x1")}, nil); err != nil || value.(*sl.List).Len() != 0 {
		t.Errorf("an XMPP account's verifications: %v %v", value, err)
	}

	value, err := api_account_encryption_devices(thread, fn, sl.Tuple{sl.String("x1"), sl.String("xmpp:bob@example.com")}, nil)
	devices, _ := sl_decode(value).([]any)
	if err != nil || len(devices) == 0 || devices[0].(map[string]any)["id"] != strconv.FormatUint(uint64(bob.id), 10) || devices[0].(map[string]any)["trust"] != "undecided" || devices[0].(map[string]any)["fingerprint"] == "" {
		t.Fatalf("THE POINT: bob's devices: %v %v", devices, err)
	}
	if _, err := api_account_encryption_trust(thread, fn, sl.Tuple{sl.String("x1"), sl.String("xmpp:bob@example.com"), sl.String(strconv.FormatUint(uint64(bob.id), 10)), sl.String("distrusted")}, nil); err != nil {
		t.Fatal(err)
	}
	value, _ = api_account_encryption_devices(thread, fn, sl.Tuple{sl.String("x1"), sl.String("xmpp:bob@example.com")}, nil)
	if trust := sl_decode(value).([]any)[0].(map[string]any)["trust"]; trust != "distrusted" {
		t.Errorf("THE POINT: the user's word kept: %v", trust)
	}
	// Someone in a room, by the address the room shows.
	omemo_occupant("x1", "club@rooms.example.com", "Bobby", "bob@example.com", false)
	value, _ = api_account_encryption_devices(thread, fn, sl.Tuple{sl.String("x1"), sl.String("xmpp:club@rooms.example.com/Bobby")}, nil)
	if room := sl_decode(value).([]any); len(room) == 0 || room[0].(map[string]any)["trust"] != "distrusted" {
		t.Errorf("THE POINT: someone in a room as the room shows them: %v", room)
	}
}

// TestOmemoLinkedNotHeld: a linked account's device waiting on its server's
// answer to publishing it does not keep a message that came meanwhile from
// being opened. The answer is read by the same session that opens the
// message, so a device held across the request would wait out its timeout.
func TestOmemoLinkedNotHeld(t *testing.T) {
	alice := create_linked_test_env(t)
	saved := xmpp_query_timeout
	xmpp_query_timeout = 5 * time.Second
	t.Cleanup(func() { xmpp_query_timeout = saved })
	fake := create_xmpp_fake(t, "secret")
	// The publication is held unanswered until the test ends: a stanza the
	// fake does not answer is refused at once.
	held := make(chan struct{})
	t.Cleanup(func() { close(held) })
	fake.answer = func(text string) []string {
		if !strings.HasPrefix(text, "<iq") {
			return nil
		}
		if strings.Contains(text, "<publish ") {
			<-held
		}
		return []string{`<iq xmlns="jabber:client" type="result" id="` + xml_attribute(text, "id") + `"/>`}
	}
	log := &inbound_log{}
	log.install(t)
	account := decoded(t)(api_account_link(linked_thread(alice), sl.NewBuiltin("test", nil), sl.Tuple{sl.String("xmpp")}, linked_kwargs("address", "alice@example.org", "password", "secret")))
	id := account["id"].(string)
	wait(t, "the connection", func() bool { connected, _ := linked_connected(alice.UID, id); return connected })
	fake.await(t, "the device being published", "<publish ")
	fake.push(t, `<message xmlns="jabber:client" type="chat" id="o1" from="bob@example.org/phone"><encrypted xmlns="eu.siacs.conversations.axolotl"><payload>abc</payload></encrypted></message>`)
	if message := log.find(t, "message", func(e map[string]any) bool { return e["id"] == "o1" }); message["encrypted"] != true {
		t.Errorf("THE POINT: the message opened while the device is published: %v", message)
	}
}

// TestOmemoLinkedNotHeldSealing: neither does a message being sealed, its
// recipient's devices asked of the network.
func TestOmemoLinkedNotHeldSealing(t *testing.T) {
	alice := create_linked_test_env(t)
	saved := xmpp_query_timeout
	xmpp_query_timeout = 5 * time.Second
	t.Cleanup(func() { xmpp_query_timeout = saved })
	fake := create_xmpp_fake(t, "secret")
	held := make(chan struct{})
	t.Cleanup(func() { close(held) })
	fake.answer = func(text string) []string {
		if !strings.HasPrefix(text, "<iq") {
			return nil
		}
		if strings.Contains(text, `to="bob@example.org"`) {
			<-held
		}
		return []string{`<iq xmlns="jabber:client" type="result" id="` + xml_attribute(text, "id") + `"/>`}
	}
	log := &inbound_log{}
	log.install(t)
	account := decoded(t)(api_account_link(linked_thread(alice), sl.NewBuiltin("test", nil), sl.Tuple{sl.String("xmpp")}, linked_kwargs("address", "alice@example.org", "password", "secret")))
	id := account["id"].(string)
	wait(t, "the connection", func() bool { connected, _ := linked_connected(alice.UID, id); return connected })
	d, err := omemo_device_of(alice, id)
	if err != nil {
		t.Fatal(err)
	}
	wait(t, "the device published", func() bool { return d.published.Load() })
	encryption_running.Go(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*xmpp_query_timeout)
		defer cancel()
		d.seal(ctx, []jid.JID{jid.MustParse("bob@example.org")}, "", "Hello")
	})
	fake.await(t, "bob's devices asked for", `to="bob@example.org"`)
	fake.push(t, `<message xmlns="jabber:client" type="chat" id="o2" from="carol@example.org/phone"><encrypted xmlns="eu.siacs.conversations.axolotl"><payload>abc</payload></encrypted></message>`)
	if message := log.find(t, "message", func(e map[string]any) bool { return e["id"] == "o2" }); message["encrypted"] != true {
		t.Errorf("THE POINT: the message opened while another is sealed: %v", message)
	}
}

// TestOmemoLinkedLists: a linked account's device reads the device lists its
// server answers with, its own and other people's, items and all: it adds
// itself to the account's list beside the devices of the user's other
// clients, and encrypts to the devices a person lists.
func TestOmemoLinkedLists(t *testing.T) {
	alice := create_linked_test_env(t)
	fake := create_xmpp_fake(t, "secret")
	var unreachable atomic.Bool
	fake.answer = func(text string) []string {
		if !strings.HasPrefix(text, "<iq") {
			return nil
		}
		id := xml_attribute(text, "id")
		switch {
		case strings.Contains(text, `to="erin@example.org"`) || strings.Contains(text, `to="bob@example.org"`) && unreachable.Load():
			return []string{`<iq xmlns="jabber:client" type="error" id="` + id + `"><error type="wait"><remote-server-timeout xmlns="urn:ietf:params:xml:ns:xmpp-stanzas"/></error></iq>`}
		case strings.Contains(text, `to="dave@example.org"`):
			return []string{`<iq xmlns="jabber:client" type="error" id="` + id + `"><error type="auth"><forbidden xmlns="urn:ietf:params:xml:ns:xmpp-stanzas"/></error></iq>`}
		case strings.Contains(text, `to="frank@example.org"`):
			return []string{`<iq xmlns="jabber:client" type="error" id="` + id + `"><error type="cancel"><feature-not-implemented xmlns="urn:ietf:params:xml:ns:xmpp-stanzas"/></error></iq>`}
		case strings.Contains(text, `to="gina@example.org"`):
			return []string{`<iq xmlns="jabber:client" type="error" id="` + id + `"><error type="cancel"><not-allowed xmlns="urn:ietf:params:xml:ns:xmpp-stanzas"/></error></iq>`}
		case strings.Contains(text, "<items ") && strings.Contains(text, `node="urn:xmpp:omemo:2:devices"`):
			return []string{`<iq xmlns="jabber:client" type="result" id="` + id + `"><pubsub xmlns="http://jabber.org/protocol/pubsub"><items node="urn:xmpp:omemo:2:devices"><item id="current"><devices xmlns="urn:xmpp:omemo:2"><device id="77"/></devices></item></items></pubsub></iq>`}
		case strings.Contains(text, "<items ") && strings.Contains(text, `node="eu.siacs.conversations.axolotl.devicelist"`):
			return []string{`<iq xmlns="jabber:client" type="result" id="` + id + `"><pubsub xmlns="http://jabber.org/protocol/pubsub"><items node="eu.siacs.conversations.axolotl.devicelist"><item id="current"><list xmlns="eu.siacs.conversations.axolotl"><device id="77"/></list></item></items></pubsub></iq>`}
		}
		return []string{`<iq xmlns="jabber:client" type="result" id="` + id + `"/>`}
	}
	account := decoded(t)(api_account_link(linked_thread(alice), sl.NewBuiltin("test", nil), sl.Tuple{sl.String("xmpp")}, linked_kwargs("address", "alice@example.org", "password", "secret")))
	id := account["id"].(string)
	wait(t, "the connection", func() bool { connected, _ := linked_connected(alice.UID, id); return connected })
	d, err := omemo_device_of(alice, id)
	if err != nil {
		t.Fatal(err)
	}
	wait(t, "the device published", func() bool { return d.published.Load() })
	own := strconv.FormatUint(uint64(d.id), 10)
	for _, node := range []string{"urn:xmpp:omemo:2:devices", "eu.siacs.conversations.axolotl.devicelist"} {
		if published := fake.heard("<publish ", `node="`+node+`"`, `id="77"`, `id="`+own+`"`); len(published) == 0 {
			t.Errorf("THE POINT: the account's %s list keeps its other clients' device beside this one: %v", node, fake.heard("<publish ", `node="`+node+`"`))
		}
	}
	listed, err := d.listed(context.Background(), jid.MustParse("bob@example.org"), omemo_current, true)
	if err != nil || len(listed) != 1 || listed[0] != 77 {
		t.Errorf("THE POINT: bob's device read from his list: %v %v", listed, err)
	}
	if listed, err := d.listed(context.Background(), jid.JID{}, omemo_current, true); listed != nil || err != nil || len(fake.heard(`to=""`)) > 0 {
		t.Errorf("THE POINT: nobody's devices are not asked for: %v %v", listed, err)
	}
	// Dave's server does not show his lists to the account.
	dave := jid.MustParse("dave@example.org")
	for range 2 {
		if listed, err := d.listed(context.Background(), dave, omemo_current, false); len(listed) != 0 || err != nil {
			t.Errorf("THE POINT: lists not shown list nothing: %v %v", listed, err)
		}
	}
	if asked := fake.heard(`to="dave@example.org"`); len(asked) != 1 {
		t.Errorf("THE POINT: the refusal kept, not asked again: %d", len(asked))
	}
	// Frank's server publishes nothing; Gina's does not allow it read.
	for _, person := range []string{"frank@example.org", "gina@example.org"} {
		if listed, err := d.listed(context.Background(), jid.MustParse(person), omemo_current, true); len(listed) != 0 || err != nil {
			t.Errorf("THE POINT: %s lists nothing to encrypt to: %v %v", person, listed, err)
		}
	}
	// Erin's server does not answer, and her lists were never read: nothing
	// goes to her in the clear, and a file for her goes encrypted.
	erin := jid.MustParse("erin@example.org")
	if sealed, err := omemo_seal_message(context.Background(), alice, id, erin, false, "Hello", ""); err == nil {
		t.Errorf("THE POINT: a message refused where her devices cannot be read: %q", sealed)
	}
	if !omemo_encrypted(context.Background(), alice, id, erin, false) {
		t.Error("THE POINT: a file for her goes encrypted")
	}
	// Bob's stops answering after his lists were read: they stand in, old
	// as they are, but not where a list read now is asked for.
	unreachable.Store(true)
	omemo_database(alice).exec("update lists set fetched=1 where person='bob@example.org'")
	if listed, err := d.listed(context.Background(), jid.MustParse("bob@example.org"), omemo_current, false); err != nil || len(listed) != 1 || listed[0] != 77 {
		t.Errorf("THE POINT: bob's last list stands in: %v %v", listed, err)
	}
	if _, err := d.listed(context.Background(), jid.MustParse("bob@example.org"), omemo_current, true); err == nil {
		t.Error("THE POINT: a list read now fails while it cannot be read")
	}
}

// omemo_lists_fake is an XMPP server for one account whose device lists
// keep what is published to them, starting with another client's device 77.
func omemo_lists_fake(t *testing.T) *xmpp_fake {
	t.Helper()
	fake := create_xmpp_fake(t, "secret")
	var lock sync.Mutex
	lists := map[string]string{
		"urn:xmpp:omemo:2:devices":                  `<devices xmlns="urn:xmpp:omemo:2"><device id="77"/></devices>`,
		"eu.siacs.conversations.axolotl.devicelist": `<list xmlns="eu.siacs.conversations.axolotl"><device id="77"/></list>`,
	}
	payload := regexp.MustCompile(`(?s)<item[^>]*>(.*)</item>`)
	fake.answer = func(text string) []string {
		if !strings.HasPrefix(text, "<iq") {
			return nil
		}
		id := xml_attribute(text, "id")
		lock.Lock()
		defer lock.Unlock()
		for node := range lists {
			if !strings.Contains(text, `node="`+node+`"`) {
				continue
			}
			if strings.Contains(text, "<publish ") {
				if found := payload.FindStringSubmatch(text); found != nil {
					lists[node] = found[1]
				}
				break
			}
			if strings.Contains(text, "<items ") {
				return []string{`<iq xmlns="jabber:client" type="result" id="` + id + `"><pubsub xmlns="http://jabber.org/protocol/pubsub"><items node="` + node + `"><item id="current">` + lists[node] + `</item></items></pubsub></iq>`}
			}
		}
		return []string{`<iq xmlns="jabber:client" type="result" id="` + id + `"/>`}
	}
	return fake
}

// omemo_withdrawn checks that the latest list published for each node no
// longer names the device, and still names the other client's.
func omemo_withdrawn(t *testing.T, fake *xmpp_fake, device uint32) {
	t.Helper()
	own := `id="` + strconv.FormatUint(uint64(device), 10) + `"`
	for _, node := range []string{"urn:xmpp:omemo:2:devices", "eu.siacs.conversations.axolotl.devicelist"} {
		published := fake.heard("<publish ", `node="`+node+`"`)
		if len(published) < 2 {
			t.Fatalf("the %s list was published %d times", node, len(published))
		}
		if latest := published[len(published)-1]; strings.Contains(latest, own) || !strings.Contains(latest, `id="77"`) {
			t.Errorf("THE POINT: the device withdrawn from the account's %s list, the other client's kept: %s", node, latest)
		}
	}
	if len(fake.heard("<retract ", "urn:xmpp:omemo:2:bundles")) == 0 || len(fake.heard("<delete ", "eu.siacs.conversations.axolotl.bundles:")) == 0 {
		t.Error("THE POINT: the device's bundles taken off the account's server")
	}
}

// TestOmemoLinkedWithdrawn: a linked XMPP account removed takes core's
// device out of the account's device lists and its bundles off its server,
// so its contacts stop encrypting to it; so does its user purged.
func TestOmemoLinkedWithdrawn(t *testing.T) {
	for _, way := range []string{"removed", "purged"} {
		t.Run(way, func(t *testing.T) {
			alice := create_linked_test_env(t)
			fake := omemo_lists_fake(t)
			thread, fn := linked_thread(alice), sl.NewBuiltin("test", nil)
			account := decoded(t)(api_account_link(thread, fn, sl.Tuple{sl.String("xmpp")}, linked_kwargs("address", "alice@example.org", "password", "secret")))
			id := account["id"].(string)
			wait(t, "the connection", func() bool { connected, _ := linked_connected(alice.UID, id); return connected })
			d, err := omemo_device_of(alice, id)
			if err != nil {
				t.Fatal(err)
			}
			wait(t, "the device published", func() bool { return d.published.Load() })
			if way == "removed" {
				if _, err := api_account_remove(thread, fn, sl.Tuple{sl.String(id)}, nil); err != nil {
					t.Fatal(err)
				}
			} else {
				// The rest of core's schema, which a purge goes through.
				db_create()
				if _, err := user_purge_local(alice.UID); err != nil {
					t.Fatal(err)
				}
			}
			omemo_withdrawn(t, fake, d.id)
		})
	}
}

// TestOmemoOpenedTogether: a new account's device opened from several places
// at once - a session's first message and its publication - is made once
// and is the same device to each.
func TestOmemoOpenedTogether(t *testing.T) {
	env := create_xmpp_server_env(t)
	env.both()
	devices := make([]*omemo_device, 20)
	var group sync.WaitGroup
	for i := range devices {
		group.Go(func() {
			d, err := omemo_device_of(env.alice, "x1")
			if err != nil {
				t.Error(err)
			}
			devices[i] = d
		})
	}
	group.Wait()
	for _, d := range devices {
		if d == nil || d != devices[0] {
			t.Fatal("THE POINT: one device, made once, for each that opened it")
		}
	}
}
