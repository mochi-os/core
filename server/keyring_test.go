// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/matrix-org/gomatrixserverlib"
	sl "go.starlark.net/starlark"
)

// An olm_key is an ed25519 key pair a test signs Matrix key objects with, as
// a device or a cross-signing key would.
type olm_key struct {
	private ed25519.PrivateKey
	public  string
}

func create_olm_key(t *testing.T) *olm_key {
	t.Helper()
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	return &olm_key{private: private, public: base64.RawStdEncoding.EncodeToString(public)}
}

// sign signs a key object as signer by the key's id, answering it signed.
func (k *olm_key) sign(t *testing.T, object map[string]any, signer string, id string) map[string]any {
	t.Helper()
	signed, err := gomatrixserverlib.SignJSON(signer, gomatrixserverlib.KeyID(id), k.private, mustjson(object))
	if err != nil {
		t.Fatal(err)
	}
	var out map[string]any
	json.Unmarshal(signed, &out)
	return out
}

// device_keys is a device's own keys, signed by it.
func (k *olm_key) device_keys(t *testing.T, user string, device string) map[string]any {
	return k.sign(t, map[string]any{"user_id": user, "device_id": device, "algorithms": []any{"m.olm.v1.curve25519-aes-sha2", "m.megolm.v1.aes-sha2"},
		"keys": map[string]any{"curve25519:" + device: "curve" + device, "ed25519:" + device: k.public}}, user, "ed25519:"+device)
}

// signing_key is a cross-signing key object of a purpose.
func (k *olm_key) signing_key(user string, purpose string) map[string]any {
	return map[string]any{"user_id": user, "usage": []any{purpose}, "keys": map[string]any{"ed25519:" + k.public: k.public}}
}

// keys_request sends a request of a device of an account's to the key
// store, answering the status and the body.
func keys_request(t *testing.T, account string, user string, device string, method string, path string, body any) (int, map[string]any) {
	t.Helper()
	var raw json.RawMessage
	if body != nil {
		raw = mustjson(body)
	}
	answer := keyring_request(context.Background(), account, user, device, method, path, raw)
	var decoded map[string]any
	json.Unmarshal(mustjson(answer.body), &decoded)
	return answer.status, decoded
}

// keys_sync reads what arrived for a device since a position.
func keys_sync(t *testing.T, user string, device string, since string) map[string]any {
	t.Helper()
	return sl_decode(sl_encode(keyring_sync(user, device, since))).(map[string]any)
}

// keys_hosted_pair is alice's and root's hosted Matrix accounts here, in a
// room together, with a device each.
func keys_hosted_pair(t *testing.T) (alice *User, root *User, ad string, rd string, room string) {
	alice, _ = create_matrix_test_env(t)
	root = user_by_uid("u-root")
	db_user(root, "user").exec("insert into accounts (id, type, label, identifier, data, created, verified, enabled) values ('m2', 'matrix', 'Root', '@root:example.com', ?, 1, 1, 1)", json_encode(map[string]any{"hosted": true, "handle": "root", "domain": "example.com"}))
	actor_register(root, "m2", "matrix", "example.com", "root")
	fn := sl.NewBuiltin("test", nil)
	thread, rooted := linked_thread(alice), linked_thread(root)
	made := decoded(t)(api_account_room_create(thread, fn, sl.Tuple{sl.String("m1"), sl.NewList([]sl.Value{sl.String("@root:example.com")})}, nil))
	room = made["conversation"].(string)
	decoded(t)(api_account_room_join(rooted, fn, sl.Tuple{sl.String("m2"), sl.String(room)}, nil))
	ad = keyring_device("@alice:example.com", "Mochi")
	rd = keyring_device("@root:example.com", "Mochi")
	return
}

// TestKeyringDevices: a device of a hosted Matrix account, named as made,
// uploads its keys, signed by itself and never changed, and its one-time and fallback
// keys; another account's device in a room with it learns it changed, reads
// its keys with its name, and claims its one-time keys until only the
// fallback key is left; what one device sends another waits for it, once,
// until it says it has it; the device's keys go with it.
func TestKeyringDevices(t *testing.T) {
	_, _, ad, rd, _ := keys_hosted_pair(t)
	if name := keyring_name("@alice:example.com", ad); name != "Mochi" {
		t.Errorf("the device's name = %q", name)
	}
	root_position := fmt.Sprint(keys_sync(t, "@root:example.com", rd, "")["next"])

	device := create_olm_key(t)
	if status, body := keys_request(t, "m1", "@alice:example.com", ad, "POST", "keys/upload", map[string]any{"device_keys": device.device_keys(t, "@root:example.com", ad)}); status != 400 {
		t.Errorf("THE POINT: keys for another user taken: %d %v", status, body)
	}
	// Signed by the device itself, under alice's name, yet naming root.
	naming := device.sign(t, map[string]any{"user_id": "@root:example.com", "device_id": ad, "algorithms": []any{"m.olm.v1.curve25519-aes-sha2"},
		"keys": map[string]any{"curve25519:" + ad: "curve" + ad, "ed25519:" + ad: device.public}}, "@alice:example.com", "ed25519:"+ad)
	if status, _ := keys_request(t, "m1", "@alice:example.com", ad, "POST", "keys/upload", map[string]any{"device_keys": naming}); status != 400 {
		t.Errorf("THE POINT: keys signed by the device but naming another user taken: %d", status)
	}
	forged := device.device_keys(t, "@alice:example.com", ad)
	forged["algorithms"] = []any{"m.olm.v1.curve25519-aes-sha2"}
	if status, _ := keys_request(t, "m1", "@alice:example.com", ad, "POST", "keys/upload", map[string]any{"device_keys": forged}); status != 400 {
		t.Errorf("THE POINT: keys whose signature does not check taken: %d", status)
	}
	onetime := map[string]any{}
	for _, id := range []string{"AAAAAQ", "AAAAAg"} {
		onetime["signed_curve25519:"+id] = device.sign(t, map[string]any{"key": "otk" + id}, "@alice:example.com", "ed25519:"+ad)
	}
	status, body := keys_request(t, "m1", "@alice:example.com", ad, "POST", "keys/upload", map[string]any{
		"device_keys":   device.device_keys(t, "@alice:example.com", ad),
		"one_time_keys": onetime,
		"fallback_keys": map[string]any{"signed_curve25519:AAAAAw": device.sign(t, map[string]any{"key": "fallback", "fallback": true}, "@alice:example.com", "ed25519:"+ad)},
	})
	if status != 200 || fmt.Sprint(body["one_time_key_counts"]) != "map[signed_curve25519:2]" {
		t.Fatalf("THE POINT: the upload: %d %v", status, body)
	}
	changed := create_olm_key(t)
	if status, _ := keys_request(t, "m1", "@alice:example.com", ad, "POST", "keys/upload", map[string]any{"device_keys": changed.device_keys(t, "@alice:example.com", ad)}); status != 400 {
		t.Errorf("THE POINT: a device's keys changed: %d", status)
	}

	synced := keys_sync(t, "@root:example.com", rd, root_position)
	if fmt.Sprint(synced["changed"]) != "[@alice:example.com]" {
		t.Errorf("THE POINT: the room told alice's device changed: %v", synced["changed"])
	}
	status, body = keys_request(t, "m2", "@root:example.com", rd, "POST", "keys/query", map[string]any{"device_keys": map[string]any{"@alice:example.com": []any{}}})
	keys, _ := body["device_keys"].(map[string]any)["@alice:example.com"].(map[string]any)[ad].(map[string]any)
	if status != 200 || keys["user_id"] != "@alice:example.com" || keys["unsigned"].(map[string]any)["device_display_name"] != "Mochi" {
		t.Fatalf("THE POINT: alice's keys read: %d %v", status, body)
	}
	delete(keys, "unsigned")
	if !keyring_verify(mustjson(keys), "@alice:example.com", "ed25519:"+ad, device.public) {
		t.Error("THE POINT: the keys read no longer carry the device's signature")
	}

	claim := map[string]any{"one_time_keys": map[string]any{"@alice:example.com": map[string]any{ad: "signed_curve25519"}}}
	var claimed []string
	for range 4 {
		_, body := keys_request(t, "m2", "@root:example.com", rd, "POST", "keys/claim", claim)
		got, _ := body["one_time_keys"].(map[string]any)["@alice:example.com"].(map[string]any)[ad].(map[string]any)
		for id := range got {
			claimed = append(claimed, id)
		}
	}
	if strings.Join(claimed, " ") != "signed_curve25519:AAAAAQ signed_curve25519:AAAAAg signed_curve25519:AAAAAw signed_curve25519:AAAAAw" {
		t.Errorf("THE POINT: the one-time keys once each, then the fallback key: %v", claimed)
	}
	position := fmt.Sprint(keys_sync(t, "@alice:example.com", ad, "")["next"])
	if counted := keys_sync(t, "@alice:example.com", ad, position); fmt.Sprint(counted["counts"]) != "map[signed_curve25519:0]" || fmt.Sprint(counted["fallback"]) != "[]" {
		t.Errorf("the device's counts: %v %v", counted["counts"], counted["fallback"])
	}

	message := map[string]any{"messages": map[string]any{"@alice:example.com": map[string]any{ad: map[string]any{"algorithm": "m.olm.v1.curve25519-aes-sha2", "ciphertext": "c1"}}}}
	for range 2 {
		if status, _ := keys_request(t, "m2", "@root:example.com", rd, "PUT", "sendToDevice/m.room.encrypted/t1", message); status != 200 {
			t.Fatalf("send = %d", status)
		}
	}
	got := keys_sync(t, "@alice:example.com", ad, position)
	events, _ := got["events"].([]any)
	if len(events) != 1 || events[0].(map[string]any)["sender"] != "@root:example.com" || events[0].(map[string]any)["type"] != "m.room.encrypted" {
		t.Fatalf("THE POINT: what root sent alice's device, once: %v", events)
	}
	if again := keys_sync(t, "@alice:example.com", ad, position)["events"].([]any); len(again) != 1 {
		t.Errorf("what the device has not said it has is given again: %v", again)
	}
	if after := keys_sync(t, "@alice:example.com", ad, fmt.Sprint(got["next"]))["events"].([]any); len(after) != 0 {
		t.Errorf("THE POINT: what the device said it has is let go: %v", after)
	}
	if kept, _ := federation_database().exists("select 1 from deliveries where user=? and device=?", "@alice:example.com", ad); kept {
		t.Error("THE POINT: what the device said it has is still kept")
	}

	keyring_delete("@alice:example.com", ad)
	if keyring_known("@alice:example.com", ad) {
		t.Error("THE POINT: the device outlived its holder")
	}
	if _, body := keys_request(t, "m2", "@root:example.com", rd, "POST", "keys/query", map[string]any{"device_keys": map[string]any{"@alice:example.com": []any{}}}); len(body["device_keys"].(map[string]any)["@alice:example.com"].(map[string]any)) != 0 {
		t.Errorf("the deleted device's keys are still read: %v", body)
	}
}

// TestKeyringSigning: the account's cross-signing keys are kept, a
// self-signing key only where the master key signs it; the self-signing
// key's signature on a device of the account's is read with the device's
// keys, a forged one refused; a signature of the user-signing key on someone
// else's master key is shown to the signer alone; and the room is told the
// keys changed.
func TestKeyringSigning(t *testing.T) {
	_, _, ad, rd, _ := keys_hosted_pair(t)
	device := create_olm_key(t)
	keys_request(t, "m1", "@alice:example.com", ad, "POST", "keys/upload", map[string]any{"device_keys": device.device_keys(t, "@alice:example.com", ad)})
	position := fmt.Sprint(keys_sync(t, "@root:example.com", rd, "")["next"])

	master, self, users := create_olm_key(t), create_olm_key(t), create_olm_key(t)
	unsigned := map[string]any{"master_key": master.signing_key("@alice:example.com", "master"), "self_signing_key": self.signing_key("@alice:example.com", "self_signing")}
	if status, _ := keys_request(t, "m1", "@alice:example.com", ad, "POST", "keys/device_signing/upload", unsigned); status != 400 {
		t.Errorf("THE POINT: a self-signing key the master key did not sign taken: %d", status)
	}
	upload := map[string]any{
		"master_key":       master.signing_key("@alice:example.com", "master"),
		"self_signing_key": master.sign(t, self.signing_key("@alice:example.com", "self_signing"), "@alice:example.com", "ed25519:"+master.public),
		"user_signing_key": master.sign(t, users.signing_key("@alice:example.com", "user_signing"), "@alice:example.com", "ed25519:"+master.public),
	}
	if status, body := keys_request(t, "m1", "@alice:example.com", ad, "POST", "keys/device_signing/upload", upload); status != 200 {
		t.Fatalf("the cross-signing keys: %d %v", status, body)
	}
	synced := keys_sync(t, "@root:example.com", rd, position)
	if changed := synced["changed"]; fmt.Sprint(changed) != "[@alice:example.com]" {
		t.Errorf("THE POINT: the room told alice's keys changed: %v", changed)
	}
	position = fmt.Sprint(synced["next"])

	stored := keyring_devices("@alice:example.com", []string{ad})[ad]
	var object map[string]any
	json.Unmarshal(stored, &object)
	delete(object, "unsigned")
	forged := create_olm_key(t).sign(t, object, "@alice:example.com", "ed25519:"+self.public)
	if _, body := keys_request(t, "m1", "@alice:example.com", ad, "POST", "keys/signatures/upload", map[string]any{"@alice:example.com": map[string]any{ad: forged}}); len(body["failures"].(map[string]any)) == 0 {
		t.Errorf("THE POINT: a forged signature taken: %v", body)
	}
	signed := self.sign(t, object, "@alice:example.com", "ed25519:"+self.public)
	if _, body := keys_request(t, "m1", "@alice:example.com", ad, "POST", "keys/signatures/upload", map[string]any{"@alice:example.com": map[string]any{ad: signed}}); len(body["failures"].(map[string]any)) != 0 {
		t.Fatalf("the self-signing key's signature: %v", body)
	}
	if changed := keys_sync(t, "@root:example.com", rd, position)["changed"]; fmt.Sprint(changed) != "[@alice:example.com]" {
		t.Errorf("THE POINT: the room told alice's device was signed: %v", changed)
	}
	_, body := keys_request(t, "m2", "@root:example.com", rd, "POST", "keys/query", map[string]any{"device_keys": map[string]any{"@alice:example.com": []any{}}})
	read := body["device_keys"].(map[string]any)["@alice:example.com"].(map[string]any)[ad].(map[string]any)
	if read["signatures"].(map[string]any)["@alice:example.com"].(map[string]any)["ed25519:"+self.public] == nil {
		t.Errorf("THE POINT: the device read with the self-signing key's signature: %v", read)
	}
	if body["master_keys"].(map[string]any)["@alice:example.com"] == nil || body["self_signing_keys"].(map[string]any)["@alice:example.com"] == nil {
		t.Errorf("alice's cross-signing keys read: %v", body)
	}
	if len(body["user_signing_keys"].(map[string]any)) != 0 {
		t.Errorf("THE POINT: alice's user-signing key shown to root: %v", body["user_signing_keys"])
	}

	// Alice's user-signing key signs root's master key: alice sees it, root
	// does not.
	rootmaster := create_olm_key(t)
	keys_request(t, "m2", "@root:example.com", rd, "POST", "keys/device_signing/upload", map[string]any{"master_key": rootmaster.signing_key("@root:example.com", "master")})
	theirs := users.sign(t, rootmaster.signing_key("@root:example.com", "master"), "@alice:example.com", "ed25519:"+users.public)
	if _, body := keys_request(t, "m1", "@alice:example.com", ad, "POST", "keys/signatures/upload", map[string]any{"@root:example.com": map[string]any{rootmaster.public: theirs}}); len(body["failures"].(map[string]any)) != 0 {
		t.Fatalf("the user-signing signature: %v", body)
	}
	_, seen := keys_request(t, "m1", "@alice:example.com", ad, "POST", "keys/query", map[string]any{"device_keys": map[string]any{"@root:example.com": []any{}}})
	if !strings.Contains(string(mustjson(seen["master_keys"])), users.public) {
		t.Errorf("THE POINT: the signer sees their signature: %v", seen["master_keys"])
	}
	_, unseen := keys_request(t, "m2", "@root:example.com", rd, "POST", "keys/query", map[string]any{"device_keys": map[string]any{"@root:example.com": []any{}}})
	if strings.Contains(string(mustjson(unseen["master_keys"])), users.public) {
		t.Errorf("THE POINT: someone else's signature shown to root: %v", unseen["master_keys"])
	}
}

// TestKeyringBackup: a backup version is made and read; room keys go into
// the current version only, a better copy replacing a worse; they are read
// back whole, by room and by session; the account's data is kept and read,
// another user's refused; and a path outside the device's requests is
// refused.
func TestKeyringBackup(t *testing.T) {
	_, _, ad, rd, room := keys_hosted_pair(t)
	auth := map[string]any{"public_key": "pk1"}
	status, body := keys_request(t, "m1", "@alice:example.com", ad, "POST", "room_keys/version", map[string]any{"algorithm": "m.megolm_backup.v1.curve25519-aes-sha2", "auth_data": auth})
	if status != 200 || body["version"] != "1" {
		t.Fatalf("THE POINT: the backup made: %d %v", status, body)
	}
	if _, body := keys_request(t, "m1", "@alice:example.com", ad, "GET", "room_keys/version", nil); body["version"] != "1" || body["algorithm"] != "m.megolm_backup.v1.curve25519-aes-sha2" || fmt.Sprint(body["auth_data"]) != "map[public_key:pk1]" {
		t.Errorf("the backup read: %v", body)
	}
	key := func(first int, data string) map[string]any {
		return map[string]any{"first_message_index": first, "forwarded_count": 0, "is_verified": false, "session_data": map[string]any{"ciphertext": data}}
	}
	put := map[string]any{"rooms": map[string]any{room: map[string]any{"sessions": map[string]any{"s1": key(3, "late")}}}}
	if status, body := keys_request(t, "m1", "@alice:example.com", ad, "PUT", "room_keys/keys?version=2", put); status != 403 || body["errcode"] != "M_WRONG_ROOM_KEYS_VERSION" || body["current_version"] != "1" {
		t.Errorf("THE POINT: keys for a version not current taken: %d %v", status, body)
	}
	if status, body := keys_request(t, "m1", "@alice:example.com", ad, "PUT", "room_keys/keys?version=1", put); status != 200 || fmt.Sprint(body["count"]) != "1" {
		t.Fatalf("THE POINT: the room key backed up: %d %v", status, body)
	}
	keys_request(t, "m1", "@alice:example.com", ad, "PUT", "room_keys/keys?version=1", map[string]any{"rooms": map[string]any{room: map[string]any{"sessions": map[string]any{"s1": key(0, "early")}}}})
	keys_request(t, "m1", "@alice:example.com", ad, "PUT", "room_keys/keys?version=1", map[string]any{"rooms": map[string]any{room: map[string]any{"sessions": map[string]any{"s1": key(5, "later")}}}})
	_, whole := keys_request(t, "m1", "@alice:example.com", ad, "GET", "room_keys/keys?version=1", nil)
	if !strings.Contains(string(mustjson(whole)), "early") || strings.Contains(string(mustjson(whole)), "later") {
		t.Errorf("THE POINT: the better copy kept: %v", whole)
	}
	// Of two copies from the same index, the one forwarded fewer times.
	forwarded := func(count int, data string) map[string]any {
		return map[string]any{"first_message_index": 0, "forwarded_count": count, "is_verified": false, "session_data": map[string]any{"ciphertext": data}}
	}
	for _, copy := range []map[string]any{forwarded(2, "relayed"), forwarded(0, "direct"), forwarded(3, "relayed again")} {
		keys_request(t, "m1", "@alice:example.com", ad, "PUT", "room_keys/keys?version=1", map[string]any{"rooms": map[string]any{room: map[string]any{"sessions": map[string]any{"s2": copy}}}})
	}
	if _, two := keys_request(t, "m1", "@alice:example.com", ad, "GET", "room_keys/keys/"+room+"/s2?version=1", nil); !strings.Contains(string(mustjson(two)), "direct") {
		t.Errorf("THE POINT: the copy forwarded fewest times kept: %v", two)
	}
	if _, one := keys_request(t, "m1", "@alice:example.com", ad, "GET", "room_keys/keys/"+room+"/s1?version=1", nil); !strings.Contains(string(mustjson(one)), "early") {
		t.Errorf("a session read alone: %v", one)
	}

	data := map[string]any{"key": "default"}
	if status, _ := keys_request(t, "m1", "@alice:example.com", ad, "PUT", "user/@alice:example.com/account_data/m.secret_storage.default_key", data); status != 200 {
		t.Errorf("the account's data kept: %d", status)
	}
	if _, read := keys_request(t, "m1", "@alice:example.com", ad, "GET", "user/@alice:example.com/account_data/m.secret_storage.default_key", nil); read["key"] != "default" {
		t.Errorf("THE POINT: the account's data read: %v", read)
	}
	if _, read := keys_request(t, "m1", "@alice:example.com", ad, "GET", "user/%40alice%3Aexample.com/account_data/m.secret_storage.default_key", nil); read["key"] != "default" {
		t.Errorf("THE POINT: read by the address escaped, as a device writes the path: %v", read)
	}
	// A secret storage key's id may hold a slash, escaped in the path.
	keys_request(t, "m1", "@alice:example.com", ad, "PUT", "user/@alice:example.com/account_data/m.secret_storage.key.ab%2Fcd", map[string]any{"key": "slashed"})
	if _, read := keys_request(t, "m1", "@alice:example.com", ad, "GET", "user/@alice:example.com/account_data/m.secret_storage.key.ab%2Fcd", nil); read["key"] != "slashed" {
		t.Errorf("THE POINT: a type with an escaped slash kept and read: %v", read)
	}
	refused := func(account string, user string, device string, path string, want int) {
		t.Helper()
		if status, _ := keys_request(t, account, user, device, "GET", path, nil); status != want {
			t.Errorf("THE POINT: %s answered %d, not %d", path, status, want)
		}
	}
	refused("m2", "@root:example.com", rd, "user/@alice:example.com/account_data/m.secret_storage.default_key", 403)
	refused("m2", "@root:example.com", rd, "user/%40alice%3Aexample.com/account_data/m.secret_storage.default_key", 403)
	refused("m1", "@alice:example.com", ad, "rooms/"+room+"/messages", 404)
	refused("m1", "@alice:example.com", ad, "account/whoami", 404)
	refused("m1", "@alice:example.com", "NOBODY", "keys/query", 401)
	refused("m1", "@alice:example.com", rd, "keys/query", 401)
}

// TestKeyringFederation: another server reads the keys of a device here and
// claims its one-time key, both as kept and still signed, and reads the
// account's device list; it is told when the device's keys come; what its
// users send a device here waits for it, not what it says another server's
// users sent; a change it tells of reaches the device; and keys of its users
// are read from it, of its own users only.
func TestKeyringFederation(t *testing.T) {
	alice, _ := create_matrix_test_env(t)
	r := create_matrix_remote(t, gomatrixserverlib.RoomVersionV10)
	room := r.room("talk", "invite")
	matrix_join(t, r, alice, room)
	ad := keyring_device("@alice:example.com", "Mochi")
	position := fmt.Sprint(keys_sync(t, "@alice:example.com", ad, "")["next"])
	device := create_olm_key(t)
	keys_request(t, "m1", "@alice:example.com", ad, "POST", "keys/upload", map[string]any{
		"device_keys":   device.device_keys(t, "@alice:example.com", ad),
		"one_time_keys": map[string]any{"signed_curve25519:AAAAAQ": device.sign(t, map[string]any{"key": "otk1"}, "@alice:example.com", "ed25519:"+ad)},
	})
	wait(t, "THE POINT: the remote told of alice's device", func() bool { return len(r.edus_of("m.device_list_update")) > 0 })
	update := r.edus_of("m.device_list_update")[0]
	if !strings.Contains(string(update.Content), ad) || !strings.Contains(string(update.Content), "@alice:example.com") {
		t.Errorf("the update: %s", update.Content)
	}

	status, body := r.call(t, "POST", "/_matrix/federation/v1/user/keys/query", map[string]any{"device_keys": map[string]any{"@alice:example.com": []any{}, "@carol:elsewhere.test": []any{}}})
	keys, _ := body["device_keys"].(map[string]any)["@alice:example.com"].(map[string]any)[ad].(map[string]any)
	if status != 200 || keys == nil {
		t.Fatalf("THE POINT: the remote read alice's keys: %d %v", status, body)
	}
	delete(keys, "unsigned")
	if !keyring_verify(mustjson(keys), "@alice:example.com", "ed25519:"+ad, device.public) {
		t.Error("THE POINT: the keys the remote read are not signed as uploaded")
	}
	if _, others := body["device_keys"].(map[string]any)["@carol:elsewhere.test"]; others {
		t.Error("another server's user answered for")
	}
	status, body = r.call(t, "POST", "/_matrix/federation/v1/user/keys/claim", map[string]any{"one_time_keys": map[string]any{"@alice:example.com": map[string]any{ad: "signed_curve25519"}}})
	if status != 200 || !strings.Contains(string(mustjson(body)), "otk1") {
		t.Errorf("THE POINT: the remote claimed the one-time key: %d %v", status, body)
	}
	status, body = r.call(t, "GET", "/_matrix/federation/v1/user/devices/@alice:example.com", nil)
	if devices, _ := body["devices"].([]any); status != 200 || len(devices) != 1 || devices[0].(map[string]any)["device_id"] != ad || as_int64(body["stream_id"]) == 0 {
		t.Errorf("THE POINT: the device list read: %d %v", status, body)
	}

	r.signal(t, "e1", matrix_edu("m.direct_to_device", map[string]any{"sender": r.user("bob"), "type": "m.room.encrypted", "message_id": "m1", "messages": map[string]any{"@alice:example.com": map[string]any{ad: map[string]any{"ciphertext": "from bob"}}}}))
	r.signal(t, "e2", matrix_edu("m.direct_to_device", map[string]any{"sender": "@mallory:elsewhere.test", "type": "m.room.encrypted", "message_id": "m2", "messages": map[string]any{"@alice:example.com": map[string]any{ad: map[string]any{"ciphertext": "forged"}}}}))
	r.signal(t, "e3", matrix_edu("m.device_list_update", map[string]any{"user_id": r.user("bob"), "device_id": "BOBDEV", "stream_id": 1, "prev_id": []any{}}))
	got := keys_sync(t, "@alice:example.com", ad, position)
	events, _ := got["events"].([]any)
	if len(events) != 1 || events[0].(map[string]any)["sender"] != r.user("bob") {
		t.Errorf("THE POINT: what bob sent alice's device, and only that: %v", events)
	}
	if !strings.Contains(fmt.Sprint(got["changed"]), r.user("bob")) {
		t.Errorf("THE POINT: bob's device change reached alice's device: %v", got["changed"])
	}

	r.lock.Lock()
	r.keys = map[string]any{"device_keys": map[string]any{r.user("bob"): map[string]any{"BOBDEV": map[string]any{"user_id": r.user("bob"), "device_id": "BOBDEV", "keys": map[string]any{"ed25519:BOBDEV": "bobkey"}}}, "@carol:elsewhere.test": map[string]any{"C": map[string]any{}}}}
	r.lock.Unlock()
	_, body = keys_request(t, "m1", "@alice:example.com", ad, "POST", "keys/query", map[string]any{"device_keys": map[string]any{r.user("bob"): []any{}}})
	if !strings.Contains(string(mustjson(body["device_keys"])), "bobkey") || strings.Contains(string(mustjson(body["device_keys"])), "carol") {
		t.Errorf("THE POINT: bob's keys read from his server, his only: %v", body)
	}

	message := map[string]any{"messages": map[string]any{r.user("bob"): map[string]any{"BOBDEV": map[string]any{"ciphertext": "to bob"}}}}
	keys_request(t, "m1", "@alice:example.com", ad, "PUT", "sendToDevice/m.room.encrypted/t9", message)
	wait(t, "THE POINT: what alice's device sent bob reached his server", func() bool {
		for _, edu := range r.edus_of("m.direct_to_device") {
			if strings.Contains(string(edu.Content), "to bob") {
				return true
			}
		}
		return false
	})
}
