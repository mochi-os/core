// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	sl "go.starlark.net/starlark"
	"maunium.net/go/mautrix"
	"maunium.net/go/mautrix/id"
)

// encryption_pair is alice's and root's hosted Matrix accounts here in an
// encrypted room, with core's device for each opened.
func encryption_pair(t *testing.T) (alice *User, root *User, log *inbound_log, room string, ad *encryption_matrix, rd *encryption_matrix) {
	t.Helper()
	alice, log = create_matrix_test_env(t)
	root = user_by_uid("u-root")
	db_user(root, "user").exec("insert into accounts (id, type, label, identifier, data, created, verified, enabled) values ('m2', 'matrix', 'Root', '@root:example.com', ?, 1, 1, 1)", json_encode(map[string]any{"hosted": true, "handle": "root", "domain": "example.com"}))
	actor_register(root, "m2", "matrix", "example.com", "root")
	fn := sl.NewBuiltin("test", nil)
	made := decoded(t)(api_account_room_create(linked_thread(alice), fn, sl.Tuple{sl.String("m1"), sl.NewList([]sl.Value{sl.String("@root:example.com")})}, nil))
	room = made["conversation"].(string)
	decoded(t)(api_account_room_join(linked_thread(root), fn, sl.Tuple{sl.String("m2"), sl.String(room)}, nil))
	empty := ""
	if _, err := rooms_send(context.Background(), room, "@alice:example.com", "m.room.encryption", &empty, map[string]any{"algorithm": "m.megolm.v1.aes-sha2"}); err != nil {
		t.Fatal(err)
	}
	var err error
	if ad, err = encryption_matrix_of(alice, "m1"); err != nil {
		t.Fatalf("alice's device: %v", err)
	}
	if rd, err = encryption_matrix_of(root, "m2"); err != nil {
		t.Fatalf("root's device: %v", err)
	}
	return
}

// encryption_sent sends a message of a conversation through a hosted Matrix
// account, as chat does.
func encryption_sent(t *testing.T, user *User, account string, object map[string]any, key string) map[string]any {
	t.Helper()
	return decoded(t)(api_account_object_create(linked_thread(user), sl.NewBuiltin("test", nil), sl.Tuple{sl.String(account), sl.Value(sl_encode(object))}, []sl.Tuple{{sl.String("key"), sl.String(key)}}))
}

// encryption_heard waits for a message of a room's to reach an account's
// apps with a body, as a message or once its keys came.
func encryption_heard(t *testing.T, log *inbound_log, account string, room string, body string) map[string]any {
	t.Helper()
	var found map[string]any
	wait(t, "the message "+body, func() bool {
		log.lock.Lock()
		defer log.lock.Unlock()
		for _, e := range log.events {
			if e["account"] == account && e["conversation"] == room && (e["kind"] == "message" || e["kind"] == "decrypted") && e["body"] == body {
				found = e
				return true
			}
		}
		return false
	})
	return found
}

// TestEncryptionMatrixHosted: core's device of a hosted Matrix account is a
// device of the engine's key store, with its keys uploaded; what an app
// sends to an encrypted room goes encrypted, its relation in the clear, and
// is read by the other account's device before chat hears it: the message,
// the answer, a correction and a reaction. Chat is never handed ciphertext.
func TestEncryptionMatrixHosted(t *testing.T) {
	_, root, log, room, ad, _ := encryption_pair(t)
	if !keyring_known("@alice:example.com", ad.device) || keyring_name("@alice:example.com", ad.device) != "Mochi" {
		t.Fatalf("THE POINT: alice's device in the key store: %s", ad.device)
	}
	if keys := keyring_devices("@alice:example.com", []string{ad.device})[ad.device]; !strings.Contains(string(keys), ad.machine.OwnIdentity().SigningKey.String()) {
		t.Fatalf("THE POINT: the device's keys uploaded: %s", keys)
	}

	sent := encryption_sent(t, ad.user, "m1", map[string]any{"conversation": room, "content": "Secret"}, "chat|e1")
	event := rooms_event(sent["id"].(string))
	if event == nil || event.Type() != "m.room.encrypted" || strings.Contains(string(event.Content()), "Secret") {
		t.Fatalf("THE POINT: the message sent encrypted: %v", event)
	}
	heard := encryption_heard(t, log, "m2", room, "Secret")
	if heard["from"] != "@alice:example.com" || heard["encrypted"] != true {
		t.Errorf("the message as root's apps heard it: %v", heard)
	}

	encryption_sent(t, root, "m2", map[string]any{"conversation": room, "content": "Reply"}, "chat|e2")
	encryption_heard(t, log, "m1", room, "Reply")

	// A correction, its relation in the clear for servers to follow.
	decoded(t)(api_account_object_update(linked_thread(ad.user), sl.NewBuiltin("test", nil), sl.Tuple{sl.String("m1"), sl.String(sent["id"].(string)), sl.Value(sl_encode(map[string]any{"conversation": room, "content": "Secret, corrected"}))}, nil))
	edit := log.find(t, "edit", func(e map[string]any) bool { return e["account"] == "m2" && e["body"] == "Secret, corrected" })
	if edit["id"] != sent["id"] {
		t.Errorf("THE POINT: the correction read: %v", edit)
	}
	reaction := encryption_sent(t, root, "m2", map[string]any{"conversation": room, "reaction": "👍", "message": sent["id"]}, "chat|e3")
	if e := rooms_event(reaction["id"].(string)); e == nil || e.Type() != "m.room.encrypted" || !strings.Contains(string(e.Content()), `"m.relates_to"`) {
		t.Errorf("THE POINT: the reaction encrypted, its relation in the clear: %v", e)
	}
	log.find(t, "reaction", func(e map[string]any) bool { return e["account"] == "m1" && e["reaction"] == "👍" })

	log.lock.Lock()
	for _, e := range log.events {
		if body, _ := e["body"].(string); strings.Contains(body, "m.megolm") || e["kind"] == "message" && e["encrypted"] == true && body == "" && e["account"] != "" {
			t.Errorf("THE POINT: chat handed what it cannot read: %v", e)
		}
	}
	log.lock.Unlock()
}

// TestEncryptionMatrixLate: a message whose room key has not come is handed
// to chat as encrypted with no text, and once the key comes it is handed on
// again, read.
func TestEncryptionMatrixLate(t *testing.T) {
	_, root, log, room, ad, rd := encryption_pair(t)
	ctx := context.Background()
	// Root's device is not told the key came, which waits on its way.
	encryption_database(root).exec("delete from devices where account='m2'")
	members, _ := ad.rooms.GetRoomJoinedOrInvitedMembers(ctx, id.RoomID(room))
	if err := ad.machine.ShareGroupSession(ctx, id.RoomID(room), members); err != nil {
		t.Fatal(err)
	}
	// The key waits on the way to root's device.
	db := federation_database()
	held, _ := db.rows("select * from deliveries where user='@root:example.com' and device=?", rd.device)
	if len(held) == 0 {
		t.Fatal("no room key on its way to root's device")
	}
	db.exec("delete from deliveries where user='@root:example.com' and device=?", rd.device)
	sent := encryption_sent(t, ad.user, "m1", map[string]any{"conversation": room, "content": "Late"}, "chat|l1")
	waiting := log.find(t, "message", func(e map[string]any) bool { return e["account"] == "m2" && e["id"] == sent["id"] })
	if waiting["body"] != "" || waiting["encrypted"] != true {
		t.Fatalf("THE POINT: the message shown waiting for its key: %v", waiting)
	}
	for _, row := range held {
		db.exec("insert into deliveries (user, device, sender, type, content, created) values (?, ?, ?, ?, ?, ?)", row["user"], row["device"], row["sender"], row["type"], row["content"], row["created"])
	}
	encryption_database(root).exec("insert into devices (account, device, created) values ('m2', ?, 1)", rd.device)
	encryption_delivered("@root:example.com", rd.device)
	read := log.find(t, "decrypted", func(e map[string]any) bool { return e["account"] == "m2" && e["id"] == sent["id"] })
	if read["body"] != "Late" {
		t.Errorf("THE POINT: the message read once its key came: %v", read)
	}
	if left, _ := encryption_database(root).exists("select 1 from pending where account='m2'"); left {
		t.Error("THE POINT: the message still waits once read")
	}
}

// TestEncryptionMatrixFiles: a file for an encrypted room is uploaded
// encrypted, its key going in the message; the other account's apps are
// given its address, and fetching it there decrypts it.
func TestEncryptionMatrixFiles(t *testing.T) {
	saved := cache_dir
	cache_dir = t.TempDir()
	t.Cleanup(func() { cache_dir = saved })
	_, root, log, room, ad, _ := encryption_pair(t)
	fn := sl.NewBuiltin("test", nil)
	uploaded := decoded(t)(api_account_upload(linked_thread(ad.user), fn, sl.Tuple{sl.String("m1"), sl.Bytes("a picture"), sl.String("image/png")}, []sl.Tuple{{sl.String("name"), sl.String("p.png")}, {sl.String("conversation"), sl.String(room)}}))
	stored, _ := os.ReadFile(media_path(ad.user, strings.TrimPrefix(uploaded["url"].(string), "mxc://example.com/")))
	if bytes.Contains(stored, []byte("a picture")) || uploaded["type"] != "image/png" || as_int64(uploaded["size"]) != 9 {
		t.Fatalf("THE POINT: the file uploaded encrypted: %v %q", uploaded, stored)
	}
	sent := encryption_sent(t, ad.user, "m1", map[string]any{"conversation": room, "file": uploaded}, "chat|f1")
	if e := rooms_event(sent["id"].(string)); e == nil || e.Type() != "m.room.encrypted" {
		t.Fatalf("the message sent encrypted: %v", e)
	}
	got := log.find(t, "message", func(e map[string]any) bool { return e["account"] == "m2" && e["id"] == sent["id"] && e["files"] != nil })
	file := got["files"].([]any)[0].(map[string]any)
	fetched := decoded(t)(api_account_media(linked_thread(root), fn, sl.Tuple{sl.String("m2"), sl.String(file["url"].(string))}, nil))
	if string(fetched["data"].([]byte)) != "a picture" || fetched["type"] != "" || file["type"] != "image/png" {
		t.Errorf("THE POINT: the file decrypted as fetched: %v", fetched)
	}
}

// TestEncryptionMatrixRecovery: setting up recovery makes the account's
// cross-signing keys, signs the device and backs its room keys up - its own
// and those it was sent - the recovery key answered once; the recovery key
// is taken again and another refused.
func TestEncryptionMatrixRecovery(t *testing.T) {
	_, root, log, room, ad, _ := encryption_pair(t)
	ctx := context.Background()
	encryption_sent(t, ad.user, "m1", map[string]any{"conversation": room, "content": "Before"}, "chat|r1")
	encryption_sent(t, root, "m2", map[string]any{"conversation": room, "content": "Answer"}, "chat|r2")
	encryption_heard(t, log, "m1", room, "Answer")
	recovery, err := ad.setup(ctx, "")
	if err != nil || recovery == "" {
		t.Fatalf("THE POINT: recovery set up: %v", err)
	}
	status := ad.status(ctx)
	if status["signing"] != true || status["verified"] != true || status["recovery"] != true || status["backup"] != true {
		t.Errorf("THE POINT: the device signed and backed up: %v", status)
	}
	version := ad.machine.KeyBackupVersion()
	held, _ := encryption_database(ad.user).row("select count(*) as n from crypto_megolm_inbound_session where account_id=?", ad.account)
	backed, _ := federation_database().row("select count(*) as n from sessions where user='@alice:example.com' and version=?", version.String())
	if held["n"] != int64(2) || backed["n"] != held["n"] {
		t.Errorf("THE POINT: each of the room's keys in the backup: %v held, %v backed up", held["n"], backed["n"])
	}
	if _, err := ad.recover(ctx, recovery); err != nil {
		t.Errorf("THE POINT: the recovery key taken: %v", err)
	}
	if _, err := ad.recover(ctx, "EsT0 notr eaLk ey00"); !errors.Is(err, encryption_recovery_refused) {
		t.Errorf("THE POINT: another key refused: %v", err)
	}
}

// TestEncryptionMatrixVerification: a device without the account's
// cross-signing keys verifies nobody, asking or accepting, until recovery is
// set up; then alice asks root to compare emoji in their room, root is
// asked, accepts, and both are shown the same emoji; once both say they
// match, each has the other's device verified, its master key signed. The
// request is never a message to chat.
func TestEncryptionMatrixVerification(t *testing.T) {
	alice, root, log, room, ad, rd := encryption_pair(t)
	ctx := context.Background()
	if _, err := ad.verify(ctx, "@root:example.com", room); !errors.Is(err, encryption_signing_needed) {
		t.Fatalf("THE POINT: a verification asked without the cross-signing keys: %v", err)
	}
	asking := decoded(t)(api_account_encryption_verify(linked_thread(alice), sl.NewBuiltin("test", nil), sl.Tuple{sl.String("m1"), sl.String("@root:example.com")}, []sl.Tuple{{sl.String("room"), sl.String(room)}}))
	if asking["recovery"] != true {
		t.Errorf("THE POINT: the app told recovery comes first: %v", asking)
	}
	setup := func(d *encryption_matrix) {
		t.Helper()
		if _, err := d.setup(ctx, ""); err != nil {
			t.Fatal(err)
		}
		if d.status(ctx)["keys"] != true {
			t.Fatalf("the device holds the cross-signing keys once set up")
		}
	}
	setup(ad)
	asked, err := ad.verify(ctx, "@root:example.com", room)
	if err != nil {
		t.Fatalf("verification not asked: %v", err)
	}
	var theirs *encryption_verification
	wait(t, "root asked", func() bool {
		for _, v := range rd.verifications() {
			if v.Phase == "requested" && !v.Started {
				theirs = v
				return true
			}
		}
		return false
	})
	if theirs.Person != "@alice:example.com" {
		t.Errorf("the request as root sees it: %+v", theirs)
	}
	if err := rd.act(ctx, theirs.Flow, "accept"); !errors.Is(err, encryption_signing_needed) {
		t.Fatalf("THE POINT: a verification accepted without the cross-signing keys: %v", err)
	}
	setup(rd)
	if err := rd.act(ctx, theirs.Flow, "accept"); err != nil {
		t.Fatal(err)
	}
	phase := func(d *encryption_matrix, flow string, want string) *encryption_verification {
		t.Helper()
		var found *encryption_verification
		wait(t, d.address+" at "+want, func() bool {
			for _, v := range d.verifications() {
				if v.Flow == flow && v.Phase == want {
					found = v
					return true
				}
			}
			return false
		})
		return found
	}
	phase(ad, asked.Flow, "ready")
	if err := ad.act(ctx, asked.Flow, "start"); err != nil {
		t.Fatal(err)
	}
	mine, other := phase(ad, asked.Flow, "compare"), phase(rd, theirs.Flow, "compare")
	if len(mine.Emoji) != 7 || strings.Join(mine.Emoji, "") != strings.Join(other.Emoji, "") {
		t.Fatalf("THE POINT: the same emoji shown to both: %v %v", mine.Emoji, other.Emoji)
	}
	// As the apps are shown them.
	value, err := api_account_encryption_verifications(linked_thread(root), sl.NewBuiltin("test", nil), sl.Tuple{sl.String("m2")}, nil)
	listed, _ := sl_decode(value).([]any)
	if err != nil || len(listed) != 1 || listed[0].(map[string]any)["phase"] != "compare" || len(listed[0].(map[string]any)["emoji"].([]any)) != 7 {
		t.Fatalf("THE POINT: root's verifications as the apps are given them: %v %v", listed, err)
	}
	if err := ad.act(ctx, asked.Flow, "confirm"); err != nil {
		t.Fatal(err)
	}
	if err := rd.act(ctx, theirs.Flow, "confirm"); err != nil {
		t.Fatal(err)
	}
	phase(ad, asked.Flow, "done")
	phase(rd, theirs.Flow, "done")
	for _, side := range []struct {
		d      *encryption_matrix
		person string
	}{{ad, "@root:example.com"}, {rd, "@alice:example.com"}} {
		devices := side.d.devices(ctx, side.person)
		if len(devices) != 1 || devices[0]["verified"] != true {
			t.Errorf("THE POINT: %s's device verified by the other: %v", side.person, devices)
		}
	}
	log.lock.Lock()
	for _, e := range log.events {
		if body, _ := e["body"].(string); strings.Contains(body, "verif") {
			t.Errorf("THE POINT: a verification handed to chat: %v", e)
		}
	}
	log.lock.Unlock()
	_ = root
}

// TestEncryptionDelivered: a room key sent to a hosted account's device here
// is taken at once, before any of the room's events is read.
func TestEncryptionDelivered(t *testing.T) {
	_, _, _, room, ad, rd := encryption_pair(t)
	ctx := context.Background()
	members, _ := ad.rooms.GetRoomJoinedOrInvitedMembers(ctx, id.RoomID(room))
	if err := ad.machine.ShareGroupSession(ctx, id.RoomID(room), members); err != nil {
		t.Fatal(err)
	}
	outbound, _ := ad.machine.CryptoStore.GetOutboundGroupSession(ctx, id.RoomID(room))
	wait(t, "THE POINT: root's device took the room key as it came", func() bool {
		session, _ := rd.machine.CryptoStore.GetGroupSession(ctx, id.RoomID(room), outbound.ID())
		return session != nil
	})
}

// TestEncryptionFunctions: the apps' functions show a Matrix account's
// device and a person's devices and take the user's word on them, behind the
// account's permission.
func TestEncryptionFunctions(t *testing.T) {
	alice, _, _, _, ad, rd := encryption_pair(t)
	thread, fn := linked_thread(alice), sl.NewBuiltin("test", nil)
	status := decoded(t)(api_account_encryption_status(thread, fn, sl.Tuple{sl.String("m1")}, nil))
	if status["network"] != "matrix" || status["device"] != ad.device || status["address"] != "@alice:example.com" || status["fingerprint"] == "" {
		t.Errorf("THE POINT: the Matrix device: %v", status)
	}
	value, err := api_account_encryption_devices(thread, fn, sl.Tuple{sl.String("m1"), sl.String("@root:example.com")}, nil)
	devices, _ := sl_decode(value).([]any)
	if err != nil || len(devices) != 1 || devices[0].(map[string]any)["id"] != rd.device {
		t.Fatalf("THE POINT: root's devices: %v %v", devices, err)
	}
	if _, err := api_account_encryption_trust(thread, fn, sl.Tuple{sl.String("m1"), sl.String("@root:example.com"), sl.String(rd.device), sl.String("verified")}, nil); err != nil {
		t.Fatal(err)
	}
	value, _ = api_account_encryption_devices(thread, fn, sl.Tuple{sl.String("m1"), sl.String("@root:example.com")}, nil)
	if verified := sl_decode(value).([]any)[0].(map[string]any)["verified"]; verified != true {
		t.Errorf("THE POINT: root's device verified by the user's word: %v", verified)
	}
	if _, err := api_account_encryption_trust(thread, fn, sl.Tuple{sl.String("m1"), sl.String("@root:example.com"), sl.String(rd.device), sl.String("sure")}, nil); err == nil {
		t.Error("THE POINT: a word that is none taken")
	}

	// An app without the account's permission is refused.
	other := create_external_app("app2")
	permission_grant(alice, other.id, "accounts/read")
	if _, err := api_account_encryption_status(create_test_thread(alice, other), fn, sl.Tuple{sl.String("m1")}, nil); err == nil {
		t.Error("THE POINT: an app without the account's permission shown its device")
	}
}

// TestEncryptionForget: an account removed takes its encryption device with
// it: the keys and sessions kept for it, and a hosted Matrix account's device
// in the key store.
func TestEncryptionForget(t *testing.T) {
	alice, _, _, room, ad, _ := encryption_pair(t)
	encryption_sent(t, alice, "m1", map[string]any{"conversation": room, "content": "Before"}, "chat|g1")
	db := encryption_database(alice)
	if kept, _ := db.exists("select 1 from crypto_megolm_inbound_session where account_id='m1'"); !kept {
		t.Fatal("no room key kept to forget")
	}
	if _, err := api_account_remove(linked_thread(alice), sl.NewBuiltin("test", nil), sl.Tuple{sl.String("m1")}, nil); err != nil {
		t.Fatal(err)
	}
	for _, query := range []string{"select 1 from crypto_megolm_inbound_session where account_id='m1'", "select 1 from crypto_account where account_id='m1'", "select 1 from devices where account='m1'"} {
		if left, _ := db.exists(query); left {
			t.Errorf("THE POINT: the account's encryption kept: %s", query)
		}
	}
	if keyring_known("@alice:example.com", ad.device) {
		t.Error("THE POINT: the account's device kept in the key store")
	}
}

// TestEncryptionMatrixOtherDevice: when another device of the user's accepts
// a verification in a room, this device shows it taken there and leaves its
// steps to that device, rather than cancelling it for everyone.
func TestEncryptionMatrixOtherDevice(t *testing.T) {
	_, _, _, room, ad, rd := encryption_pair(t)
	ctx := context.Background()
	if _, err := ad.setup(ctx, ""); err != nil {
		t.Fatal(err)
	}
	asked, err := ad.verify(ctx, "@root:example.com", room)
	if err != nil {
		t.Fatal(err)
	}
	wait(t, "root asked", func() bool {
		v, ok := rd.verifying.Load(asked.Flow)
		return ok && v.(*encryption_verification).Phase == "requested"
	})
	step := func(kind string, sender string, content map[string]any) {
		content["m.relates_to"] = map[string]any{"rel_type": "m.reference", "event_id": asked.Flow}
		rd.dispatch(ctx, room, &matrix_event{Type: kind, Sender: sender, ID: "$" + kind, Content: content})
	}
	step("m.key.verification.ready", "@root:example.com", map[string]any{"from_device": "OTHER", "methods": []any{"m.sas.v1"}})
	if v, _ := rd.verifying.Load(asked.Flow); v.(*encryption_verification).Phase != "cancelled" || v.(*encryption_verification).Reason != "m.accepted" {
		t.Errorf("THE POINT: shown taken on the other device: %+v", v)
	}
	step("m.key.verification.start", "@alice:example.com", map[string]any{"from_device": ad.device, "method": "m.sas.v1"})
	time.Sleep(500 * time.Millisecond)
	if v, _ := ad.verifying.Load(asked.Flow); v.(*encryption_verification).Phase == "cancelled" {
		t.Errorf("THE POINT: the verification cancelled for everyone by a device not in it: %+v", v)
	}
}

// TestEncryptionUserPurged: a user purged has their linked accounts'
// sessions ended and their encryption devices dropped, which would
// otherwise go on working against databases that are gone.
func TestEncryptionUserPurged(t *testing.T) {
	purge_test_env(t, "u-gone")
	ended := false
	linked_lock.Lock()
	linked_sessions[linked_session_key("u-gone", "l1")] = &linked_session{cancel: func() { ended = true }}
	linked_lock.Unlock()
	encryption_lock.Lock()
	encryption_matrices["u-gone|m1"] = &encryption_matrix{}
	encryption_lock.Unlock()
	omemo_lock.Lock()
	omemo_devices["u-gone|x1"] = &omemo_device{}
	omemo_lock.Unlock()
	t.Cleanup(encryption_reset)
	if _, err := user_purge_local("u-gone"); err != nil {
		t.Fatal(err)
	}
	linked_lock.Lock()
	_, running := linked_sessions[linked_session_key("u-gone", "l1")]
	linked_lock.Unlock()
	if running || !ended {
		t.Error("THE POINT: the purged user's linked session ended")
	}
	encryption_lock.Lock()
	_, matrix := encryption_matrices["u-gone|m1"]
	encryption_lock.Unlock()
	omemo_lock.Lock()
	_, omemo := omemo_devices["u-gone|x1"]
	omemo_lock.Unlock()
	if matrix || omemo {
		t.Error("THE POINT: the purged user's devices dropped")
	}
}

// TestEncryptionAuthenticate: a cross-signing upload refused for want of the
// password is told from another failure, as mautrix gives it: a value,
// wrapped.
func TestEncryptionAuthenticate(t *testing.T) {
	refused := fmt.Errorf("failed to publish cross-signing keys: %w", mautrix.HTTPError{Response: &http.Response{StatusCode: 401}})
	if !encryption_authenticate(refused) {
		t.Error("THE POINT: a 401 is the password wanted")
	}
	failed := fmt.Errorf("failed to publish cross-signing keys: %w", mautrix.HTTPError{Response: &http.Response{StatusCode: 500}})
	if encryption_authenticate(failed) || encryption_authenticate(nil) {
		t.Error("another failure, or none, is not")
	}
}

// TestEncryptionWaitsForBudget: a linked account's device, over the
// account's budget on its server, waits for the next window rather than
// failing the request, which would leave a verification half taken.
func TestEncryptionWaitsForBudget(t *testing.T) {
	alice := create_linked_test_env(t)
	fake := create_matrix_fake(t)
	account := decoded(t)(api_account_link(linked_thread(alice), sl.NewBuiltin("test", nil), sl.Tuple{sl.String("matrix")}, linked_kwargs("username", "alice", "server", fake_host(fake.server), "password", "secret")))
	saved := rate_limit_host
	rate_limit_host = &rate_limiter{entries: make(map[string]*rate_limit_entry), limit: 1, window: 1}
	t.Cleanup(func() { rate_limit_host = saved })
	d := &encryption_matrix{linked: linked_get(alice, account["id"].(string))}
	address, _ := url.Parse(fake.server.URL + "/_matrix/client/v3/account/whoami")
	for i := range 2 {
		if status, _, err := d.request(context.Background(), "GET", address, nil); err != nil || status != 200 {
			t.Fatalf("THE POINT: request %d, over the budget, waited for it: %d %v", i, status, err)
		}
	}
}

// TestEncryptionOpenedTogether: a new Matrix device opened from several
// places at once is made once, one device in the key store, and is the same
// device to each.
func TestEncryptionOpenedTogether(t *testing.T) {
	_, root, _, _, _, _ := encryption_pair(t)
	db_user(root, "user").exec("insert into accounts (id, type, label, identifier, data, created, verified, enabled) values ('m3', 'matrix', 'Root', '@other:example.com', ?, 1, 1, 1)", json_encode(map[string]any{"hosted": true, "handle": "other", "domain": "example.com"}))
	actor_register(root, "m3", "matrix", "example.com", "other")
	devices := make([]*encryption_matrix, 10)
	var group sync.WaitGroup
	for i := range devices {
		group.Go(func() {
			d, err := encryption_matrix_of(root, "m3")
			if err != nil {
				t.Error(err)
			}
			devices[i] = d
		})
	}
	group.Wait()
	for _, d := range devices {
		if d == nil || d != devices[0] {
			t.Fatal("THE POINT: one device for each that opened it")
		}
	}
	if made, _ := federation_database().row("select count(*) as n from clients where user='@other:example.com'"); made["n"] != int64(1) {
		t.Errorf("THE POINT: one device made in the key store: %v", made["n"])
	}
}
