// Mochi server: the hosted Matrix accounts' end-to-end encryption keys (the
// federation plan, phase 27): each account's devices with their keys, the
// one-time and fallback keys others claim to start a session with them, the
// account's cross-signing keys and the signatures on them, the messages sent
// to a device, which devices changed, the key backup and the account's own
// data, where secret storage is kept. The keys are public or encrypted to
// someone: the server reads none of what they protect.
//
// keyring_request answers a device's requests by the paths of the Matrix
// client interface they would take, so that interface (phase 28) serves other
// clients from the same store; keyring_sync is what a sync gives a device;
// and the federation half answers other servers' queries and claims and takes
// what they send.
//
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the Mochi
// Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/matrix-org/gomatrixserverlib"
	"github.com/matrix-org/gomatrixserverlib/fclient"
	"github.com/matrix-org/gomatrixserverlib/spec"
)

// keyring_tables makes the key store in matrix.db.
func keyring_tables(db *DB) {
	// A device of an account here: its name and its uploaded keys.
	db.exec("create table if not exists clients (user text not null, device text not null, name text not null default '', keys text not null default '', created integer not null, seen integer not null default 0, primary key (user, device))")
	db.exec("create table if not exists onetime (user text not null, device text not null, id text not null, key text not null, primary key (user, device, id))")
	db.exec("create table if not exists fallbacks (user text not null, device text not null, algorithm text not null, id text not null, key text not null, used integer not null default 0, primary key (user, device, algorithm))")
	// An account's cross-signing keys, by purpose: master, self_signing,
	// user_signing.
	db.exec("create table if not exists signing (user text not null, purpose text not null, key text not null, primary key (user, purpose))")
	// A signature on a key of a user's: one of their devices, by its id, or a
	// cross-signing key, by its public key.
	db.exec("create table if not exists signatures (user text not null, target text not null, signer text not null, id text not null, value text not null, primary key (user, target, signer, id))")
	// What another device sent one of an account's devices.
	db.exec("create table if not exists deliveries (id integer primary key autoincrement, user text not null, device text not null, sender text not null, type text not null, content text not null, created integer not null)")
	db.exec("create index if not exists deliveries_device on deliveries(user, device, id)")
	// Whose devices or cross-signing keys changed, in order; a change seen
	// only by viewer where one is named (a signature of theirs on someone
	// else's key).
	db.exec("create table if not exists changes (id integer primary key autoincrement, user text not null, viewer text not null default '', created integer not null)")
	// The account's key backup: its versions, and the room keys backed up.
	db.exec("create table if not exists backups (user text not null, version integer not null, algorithm text not null, auth text not null, etag integer not null default 0, deleted integer not null default 0, primary key (user, version))")
	db.exec("create table if not exists sessions (user text not null, version integer not null, room text not null, session text not null, data text not null, primary key (user, version, room, session))")
	// What the account's clients keep on the server: secret storage, above
	// all, encrypted under the user's recovery key.
	db.exec("create table if not exists data (user text not null, type text not null, content text not null, primary key (user, type))")
}

// keyring_body_maximum bounds a request's body: a key backup of many rooms
// is the largest.
const keyring_body_maximum = 4 << 20

// keyring_delivered is how long what was sent a device waits for it.
const keyring_delivered = 30 * 86400

// A keyring_answer is a request's answer: its status and its JSON.
type keyring_answer struct {
	status int
	body   any
}

// keyring_error is a Matrix error answer.
func keyring_error(status int, code string, message string) keyring_answer {
	return keyring_answer{status, map[string]any{"errcode": code, "error": message}} // i18n-ok: Matrix error codes and their text are protocol, read by clients
}

// keyring_ok is a successful answer.
func keyring_ok(body any) keyring_answer {
	return keyring_answer{http.StatusOK, body}
}

// === Devices ===

// keyring_device makes a device of a user's, named, and answers its id.
func keyring_device(user string, name string) string {
	db := federation_database()
	for {
		id := strings.ToUpper(random_alphanumeric(10))
		if exists, _ := db.exists("select 1 from clients where user=? and device=?", user, id); exists {
			continue
		}
		db.exec("insert into clients (user, device, name, created, seen) values (?, ?, ?, ?, ?)", user, id, name, now(), now())
		return id
	}
}

// keyring_known reports whether a device of a user's is one this server
// keeps.
func keyring_known(user string, device string) bool {
	exists, _ := federation_database().exists("select 1 from clients where user=? and device=?", user, device)
	return exists
}

// keyring_delete forgets a device of a user's with its keys and what waited
// for it, and tells those who share a room with the user.
func keyring_delete(user string, device string) {
	db := federation_database()
	if !keyring_known(user, device) {
		return
	}
	for _, table := range []string{"clients", "onetime", "fallbacks", "deliveries"} {
		db.exec("delete from "+table+" where user=? and device=?", user, device)
	}
	db.exec("delete from signatures where user=? and target=?", user, device)
	keyring_changed(user, device, nil, true)
}

// keyring_changed records that a user's devices changed and tells the other
// servers they share a room with: one device's keys, or its deletion.
func keyring_changed(user string, device string, keys json.RawMessage, deleted bool) {
	db := federation_database()
	previous, _ := db.row("select max(id) as id from changes where user=? and viewer=''", user)
	db.exec("insert into changes (user, created) values (?, ?)", user, now())
	row, _ := db.row("select max(id) as id from changes where user=?", user)
	content := map[string]any{"user_id": user, "device_id": device, "stream_id": row_int(row, "id"), "deleted": deleted}
	if id := row_int(previous, "id"); id > 0 {
		content["prev_id"] = []int64{id}
	} else {
		content["prev_id"] = []int64{}
	}
	if keys != nil {
		content["keys"] = keys
	}
	if name := keyring_name(user, device); name != "" && !deleted {
		content["device_display_name"] = name
	}
	keyring_tell(user, "m.device_list_update", content)
}

// keyring_signing_changed records that a user's cross-signing keys changed
// and tells the other servers they share a room with.
func keyring_signing_changed(user string) {
	db := federation_database()
	db.exec("insert into changes (user, created) values (?, ?)", user, now())
	content := map[string]any{"user_id": user}
	if master := keyring_signing_key(user, "master"); master != nil {
		content["master_key"] = master
	}
	if self := keyring_signing_key(user, "self_signing"); self != nil {
		content["self_signing_key"] = self
	}
	keyring_tell(user, "m.signing_key_update", content)
}

// keyring_tell queues an ephemeral event of a user's that must not be lost
// for each other server they share a room with.
func keyring_tell(user string, kind string, content any) {
	id, err := spec.NewUserID(user, true)
	if err != nil || !federation_local(id.Domain()) {
		return
	}
	var servers []string
	for _, room := range signals_rooms(user) {
		state, _ := rooms_current(room)
		for _, server := range rooms_servers(state) {
			if !federation_local(spec.ServerName(server)) && !slices.Contains(servers, server) {
				servers = append(servers, server)
			}
		}
	}
	if len(servers) > 0 {
		federation_message(servers, string(id.Domain()), kind, content)
	}
}

// keyring_name is a device's name.
func keyring_name(user string, device string) string {
	row, _ := federation_database().row("select name from clients where user=? and device=?", user, device)
	return row_string(row, "name")
}

// === Requests ===

// keyring_request answers one request of a device of an account here, by the
// path under /_matrix/client/v3/ it would take on the client interface:
// uploading, querying and claiming keys, signing them, sending to devices,
// the key backup, and the account's data. The body is the request's JSON.
func keyring_request(ctx context.Context, account string, user string, device string, method string, path string, body json.RawMessage) keyring_answer {
	if !keyring_known(user, device) {
		return keyring_error(http.StatusUnauthorized, "M_UNKNOWN_TOKEN", "This device is not signed in")
	}
	address, err := url.Parse(path)
	if err != nil {
		return keyring_error(http.StatusBadRequest, "M_UNRECOGNIZED", "Invalid path")
	}
	// Split before unescaping, so an escaped slash in a part - a secret
	// storage key's id, say - stays in it.
	parts := strings.Split(strings.Trim(address.EscapedPath(), "/"), "/")
	for i, part := range parts {
		if unescaped, err := url.PathUnescape(part); err == nil {
			parts[i] = unescaped
		}
	}
	query := address.Query()
	if len(body) == 0 {
		body = json.RawMessage("{}")
	}
	if len(body) > keyring_body_maximum {
		return keyring_error(http.StatusRequestEntityTooLarge, "M_TOO_LARGE", "The request is too large")
	}
	federation_database().exec("update clients set seen=? where user=? and device=?", now(), user, device)
	route := method + " " + strings.Join(parts, "/")
	switch {
	case route == "POST keys/upload":
		return keyring_upload(user, device, body)
	case route == "POST keys/query":
		return keyring_query(ctx, user, body)
	case route == "POST keys/claim":
		return keyring_claim(ctx, user, body)
	case route == "POST keys/signatures/upload":
		return keyring_sign(user, body)
	case route == "POST keys/device_signing/upload":
		return keyring_signing_upload(user, body)
	case method == "PUT" && len(parts) == 3 && parts[0] == "sendToDevice":
		return keyring_send(user, device, parts[1], parts[2], body)
	case parts[0] == "room_keys":
		return keyring_backup(user, method, parts[1:], query, body)
	case len(parts) == 4 && parts[0] == "user" && parts[2] == "account_data":
		if parts[1] != user {
			return keyring_error(http.StatusForbidden, "M_FORBIDDEN", "Cannot read another user's data")
		}
		return keyring_data(user, method, parts[3], body)
	case method == "PUT" && len(parts) == 5 && parts[0] == "rooms" && parts[2] == "send":
		var content map[string]any
		if json.Unmarshal(body, &content) != nil {
			return keyring_error(http.StatusBadRequest, "M_NOT_JSON", "Invalid content")
		}
		id, err := rooms_message(ctx, account, user, parts[1], parts[3], content, "keys|"+device+"|"+parts[4])
		if err != nil {
			return keyring_error(http.StatusForbidden, "M_FORBIDDEN", err.Error())
		}
		return keyring_ok(map[string]any{"event_id": id})
	}
	return keyring_error(http.StatusNotFound, "M_UNRECOGNIZED", "Unrecognised request")
}

// keyring_upload keeps a device's own keys, and its one-time and fallback
// keys, answering how many one-time keys it has of each algorithm.
func keyring_upload(user string, device string, body json.RawMessage) keyring_answer {
	var request struct {
		Device   json.RawMessage            `json:"device_keys"`
		Onetime  map[string]json.RawMessage `json:"one_time_keys"`
		Fallback map[string]json.RawMessage `json:"fallback_keys"`
	}
	if json.Unmarshal(body, &request) != nil {
		return keyring_error(http.StatusBadRequest, "M_NOT_JSON", "Invalid keys")
	}
	db := federation_database()
	if len(request.Device) > 0 && string(request.Device) != "null" {
		var keys struct {
			User   string            `json:"user_id"`
			Device string            `json:"device_id"`
			Keys   map[string]string `json:"keys"`
		}
		if json.Unmarshal(request.Device, &keys) != nil || keys.User != user || keys.Device != device {
			return keyring_error(http.StatusBadRequest, "M_INVALID_PARAM", "The keys are not this device's")
		}
		signing := keys.Keys["ed25519:"+device]
		if signing == "" || !keyring_verify(request.Device, user, "ed25519:"+device, signing) {
			return keyring_error(http.StatusBadRequest, "M_INVALID_SIGNATURE", "The keys are not signed by the device")
		}
		row, _ := db.row("select keys from clients where user=? and device=?", user, device)
		stored := row_string(row, "keys")
		if stored != "" && keyring_stripped(stored) != keyring_stripped(string(request.Device)) {
			// A device's keys are its identity: others would see a device
			// they verified change under them.
			return keyring_error(http.StatusBadRequest, "M_INVALID_PARAM", "The device's keys cannot change")
		}
		if stored == "" {
			db.exec("update clients set keys=? where user=? and device=?", string(keyring_strip(request.Device)), user, device)
			keyring_changed(user, device, keyring_strip(request.Device), false)
		}
	}
	for id, key := range request.Onetime {
		if strings.Count(id, ":") != 1 || len(key) > 4096 {
			continue
		}
		db.exec("insert or ignore into onetime (user, device, id, key) values (?, ?, ?, ?)", user, device, id, string(key))
	}
	for id, key := range request.Fallback {
		algorithm, _, found := strings.Cut(id, ":")
		if !found || len(key) > 4096 {
			continue
		}
		db.exec("insert into fallbacks (user, device, algorithm, id, key, used) values (?, ?, ?, ?, ?, 0) on conflict(user, device, algorithm) do update set id=excluded.id, key=excluded.key, used=0", user, device, algorithm, id, string(key))
	}
	return keyring_ok(map[string]any{"one_time_key_counts": keyring_counts(user, device)})
}

// keyring_counts is how many one-time keys a device has of each algorithm.
func keyring_counts(user string, device string) map[string]any {
	counts := map[string]int64{"signed_curve25519": 0}
	rows, _ := federation_database().rows("select id from onetime where user=? and device=?", user, device)
	for _, row := range rows {
		algorithm, _, _ := strings.Cut(row_string(row, "id"), ":")
		counts[algorithm]++
	}
	out := map[string]any{}
	for algorithm, count := range counts {
		out[algorithm] = count
	}
	return out
}

// keyring_unused is the algorithms of a device's fallback keys no one has
// claimed.
func keyring_unused(user string, device string) []string {
	rows, _ := federation_database().rows("select algorithm from fallbacks where user=? and device=? and used=0", user, device)
	out := []string{}
	for _, row := range rows {
		out = append(out, row_string(row, "algorithm"))
	}
	return out
}

// keyring_strip takes the signatures and unsigned part off a key object, as
// kept: signatures are kept beside it, and merged back when it is read.
func keyring_strip(raw json.RawMessage) json.RawMessage {
	var object map[string]json.RawMessage
	if json.Unmarshal(raw, &object) != nil {
		return raw
	}
	signatures := object["signatures"]
	delete(object, "unsigned")
	// The key's own signature stays with it: without it no one could check
	// the key.
	var signed map[string]map[string]string
	json.Unmarshal(signatures, &signed)
	var user string
	json.Unmarshal(object["user_id"], &user)
	if own, found := signed[user]; found {
		object["signatures"] = json.RawMessage(json_encode(map[string]any{user: own}))
	} else {
		delete(object, "signatures")
	}
	return json.RawMessage(json_encode(object))
}

// keyring_stripped is a key object without any signatures, to compare two
// uploads of the same key.
func keyring_stripped(raw string) string {
	var object map[string]json.RawMessage
	if json.Unmarshal([]byte(raw), &object) != nil {
		return raw
	}
	delete(object, "signatures")
	delete(object, "unsigned")
	canonical, _ := gomatrixserverlib.CanonicalJSON([]byte(json_encode(object)))
	return string(canonical)
}

// keyring_verify checks a signature of signer's, by key id, over a key object
// with the ed25519 public key given unpadded in base64.
func keyring_verify(raw json.RawMessage, signer string, id string, public string) bool {
	key, err := base64.RawStdEncoding.DecodeString(strings.TrimRight(public, "="))
	if err != nil || len(key) != ed25519.PublicKeySize {
		return false
	}
	return gomatrixserverlib.VerifyJSON(signer, gomatrixserverlib.KeyID(id), ed25519.PublicKey(key), raw) == nil
}

// === Reading keys ===

// keyring_local reports whether a user is an account of this server's.
func keyring_local(user string) bool {
	id, err := spec.NewUserID(user, true)
	return err == nil && federation_local(id.Domain())
}

// keyring_devices is the keys of a user's devices here, those named or all,
// each with the signatures on it and its name; for whom asked.
func keyring_devices(user string, devices []string) map[string]json.RawMessage {
	out := map[string]json.RawMessage{}
	rows, _ := federation_database().rows("select device, name, keys from clients where user=? and keys!=''", user)
	for _, row := range rows {
		device := row_string(row, "device")
		if len(devices) > 0 && !slices.Contains(devices, device) {
			continue
		}
		object := keyring_signed(user, device, row_string(row, "keys"), "")
		var decoded map[string]any
		json.Unmarshal(object, &decoded)
		if name := row_string(row, "name"); name != "" {
			decoded["unsigned"] = map[string]any{"device_display_name": name}
		}
		out[device] = json.RawMessage(json_encode(decoded))
	}
	return out
}

// keyring_signing_key is one of a user's cross-signing keys with the
// signatures on it the asker may see, or nil.
func keyring_signing_key(user string, purpose string) json.RawMessage {
	return keyring_signing_for(user, purpose, "")
}

// keyring_signing_for is keyring_signing_key as seen by viewer: a signature
// of theirs on the user's master key, which only they are shown.
func keyring_signing_for(user string, purpose string, viewer string) json.RawMessage {
	row, _ := federation_database().row("select key from signing where user=? and purpose=?", user, purpose)
	if row == nil {
		return nil
	}
	return keyring_signed(user, keyring_public(row_string(row, "key")), row_string(row, "key"), viewer)
}

// keyring_public is a cross-signing key's public key, as its signatures name
// it.
func keyring_public(raw string) string {
	var key struct {
		Keys map[string]string `json:"keys"`
	}
	json.Unmarshal([]byte(raw), &key)
	for _, public := range key.Keys {
		return public
	}
	return ""
}

// keyring_signed merges the signatures kept on a key into it: those by its
// own user, and by viewer where one is named.
func keyring_signed(user string, target string, raw string, viewer string) json.RawMessage {
	var object map[string]any
	if json.Unmarshal([]byte(raw), &object) != nil {
		return json.RawMessage(raw)
	}
	signatures, _ := object["signatures"].(map[string]any)
	if signatures == nil {
		signatures = map[string]any{}
	}
	rows, _ := federation_database().rows("select signer, id, value from signatures where user=? and target=?", user, target)
	for _, row := range rows {
		signer := row_string(row, "signer")
		if signer != user && signer != viewer {
			continue
		}
		by, _ := signatures[signer].(map[string]any)
		if by == nil {
			by = map[string]any{}
			signatures[signer] = by
		}
		by[row_string(row, "id")] = row_string(row, "value")
	}
	if len(signatures) > 0 {
		object["signatures"] = signatures
	}
	return json.RawMessage(json_encode(object))
}

// keyring_query answers whose keys a device asked for: those of accounts here
// from the store, those of others from their servers, a server that does not
// answer named in the failures.
func keyring_query(ctx context.Context, user string, body json.RawMessage) keyring_answer {
	var request struct {
		Keys map[string][]string `json:"device_keys"`
	}
	if json.Unmarshal(body, &request) != nil {
		return keyring_error(http.StatusBadRequest, "M_NOT_JSON", "Invalid query")
	}
	devices := map[string]any{}
	masters := map[string]any{}
	selves := map[string]any{}
	users := map[string]any{}
	failures := map[string]any{}
	remote := map[string]map[string][]string{}
	for person, wanted := range request.Keys {
		id, err := spec.NewUserID(person, true)
		if err != nil {
			continue
		}
		if !federation_local(id.Domain()) {
			server := string(id.Domain())
			if remote[server] == nil {
				remote[server] = map[string][]string{}
			}
			remote[server][person] = wanted
			continue
		}
		devices[person] = keyring_devices(person, wanted)
		if master := keyring_signing_for(person, "master", user); master != nil {
			masters[person] = master
		}
		if self := keyring_signing_key(person, "self_signing"); self != nil {
			selves[person] = self
		}
		if person == user {
			if own := keyring_signing_key(person, "user_signing"); own != nil {
				users[person] = own
			}
		}
	}
	for server, wanted := range remote {
		answer, err := keyring_remote_query(ctx, user, server, wanted)
		if err != nil {
			debug("Matrix key query to %s failed: %v", server, err)
			failures[server] = map[string]any{"status": 503, "message": "The server did not answer"} // i18n-ok: protocol text read by clients
			continue
		}
		for person, keys := range answer.Devices {
			devices[person] = keys
		}
		for person, key := range answer.Masters {
			masters[person] = key
		}
		for person, key := range answer.Selves {
			selves[person] = key
		}
	}
	return keyring_ok(map[string]any{"device_keys": devices, "master_keys": masters, "self_signing_keys": selves, "user_signing_keys": users, "failures": failures})
}

// A keyring_remote is another server's answer to a key query, kept as it came
// so its signatures still check.
type keyring_remote struct {
	Devices map[string]map[string]json.RawMessage `json:"device_keys"`
	Masters map[string]json.RawMessage            `json:"master_keys"`
	Selves  map[string]json.RawMessage            `json:"self_signing_keys"`
}

// keyring_remote_query asks another server for its users' keys, from the
// asker's domain.
func keyring_remote_query(ctx context.Context, user string, server string, wanted map[string][]string) (*keyring_remote, error) {
	for person, devices := range wanted {
		if devices == nil {
			wanted[person] = []string{}
		}
	}
	var answer keyring_remote
	if err := keyring_federation(ctx, user, server, "/_matrix/federation/v1/user/keys/query", map[string]any{"device_keys": wanted}, &answer); err != nil {
		return nil, err
	}
	// Only the server's own users' keys are taken from it.
	for person := range answer.Devices {
		if id, err := spec.NewUserID(person, true); err != nil || !strings.EqualFold(string(id.Domain()), server) {
			delete(answer.Devices, person)
		}
	}
	for _, keys := range []map[string]json.RawMessage{answer.Masters, answer.Selves} {
		for person := range keys {
			if id, err := spec.NewUserID(person, true); err != nil || !strings.EqualFold(string(id.Domain()), server) {
				delete(keys, person)
			}
		}
	}
	return &answer, nil
}

// keyring_federation sends a signed POST to another server from the domain
// of the user it is for, decoding the answer as it came.
func keyring_federation(ctx context.Context, user string, server string, path string, content any, into any) error {
	id, err := spec.NewUserID(user, true)
	if err != nil {
		return err
	}
	identity := federation_identity_of(id.Domain())
	if identity == nil {
		return fmt.Errorf("this server does not speak for %s", id.Domain())
	}
	request := fclient.NewFederationRequest("POST", identity.domain, spec.ServerName(server), path)
	if err := request.SetContent(content); err != nil {
		return err
	}
	if err := request.Sign(identity.domain, identity.key, identity.private); err != nil {
		return err
	}
	prepared, err := request.HTTPRequest()
	if err != nil {
		return err
	}
	bounded, cancel := context.WithTimeout(ctx, federation_timeout)
	defer cancel()
	var raw json.RawMessage
	if err := federation_client().DoRequestAndParseResponse(bounded, prepared.WithContext(bounded), &raw); err != nil {
		return err
	}
	return json.Unmarshal(raw, into)
}

// keyring_claim claims a one-time key of each device named, else its
// fallback key, for a device to start a session with: from the store for
// accounts here, from their servers for others.
func keyring_claim(ctx context.Context, user string, body json.RawMessage) keyring_answer {
	var request struct {
		Keys map[string]map[string]string `json:"one_time_keys"`
	}
	if json.Unmarshal(body, &request) != nil {
		return keyring_error(http.StatusBadRequest, "M_NOT_JSON", "Invalid claim")
	}
	claimed := map[string]any{}
	failures := map[string]any{}
	remote := map[string]map[string]map[string]string{}
	for person, wanted := range request.Keys {
		id, err := spec.NewUserID(person, true)
		if err != nil {
			continue
		}
		if federation_local(id.Domain()) {
			claimed[person] = keyring_take(person, wanted)
			continue
		}
		server := string(id.Domain())
		if remote[server] == nil {
			remote[server] = map[string]map[string]string{}
		}
		remote[server][person] = wanted
	}
	for server, wanted := range remote {
		var answer struct {
			Keys map[string]map[string]map[string]json.RawMessage `json:"one_time_keys"`
		}
		if err := keyring_federation(ctx, user, server, "/_matrix/federation/v1/user/keys/claim", map[string]any{"one_time_keys": wanted}, &answer); err != nil {
			debug("Matrix key claim to %s failed: %v", server, err)
			failures[server] = map[string]any{"status": 503, "message": "The server did not answer"} // i18n-ok: protocol text read by clients
			continue
		}
		for person, keys := range answer.Keys {
			if id, err := spec.NewUserID(person, true); err == nil && strings.EqualFold(string(id.Domain()), server) {
				claimed[person] = keys
			}
		}
	}
	return keyring_ok(map[string]any{"one_time_keys": claimed, "failures": failures})
}

// keyring_take claims a key of each of a user's devices named, by algorithm:
// a one-time key, used once, else the device's fallback key.
func keyring_take(user string, wanted map[string]string) map[string]map[string]json.RawMessage {
	db := federation_database()
	out := map[string]map[string]json.RawMessage{}
	for device, algorithm := range wanted {
		row, _ := db.row("select id, key from onetime where user=? and device=? and id like ? order by id limit 1", user, device, algorithm+":%")
		if row != nil {
			db.exec("delete from onetime where user=? and device=? and id=?", user, device, row_string(row, "id"))
		} else if row, _ = db.row("select id, key from fallbacks where user=? and device=? and algorithm=?", user, device, algorithm); row != nil {
			db.exec("update fallbacks set used=1 where user=? and device=? and algorithm=?", user, device, algorithm)
		}
		if row != nil {
			out[device] = map[string]json.RawMessage{row_string(row, "id"): json.RawMessage(row_string(row, "key"))}
		}
	}
	return out
}

// === Signing ===

// keyring_signing_upload keeps the account's cross-signing keys: its master
// key, the self-signing key it signs, which signs the account's devices, and
// the user-signing key, which signs other people's master keys. A new master
// key drops the signatures on the old one.
func keyring_signing_upload(user string, body json.RawMessage) keyring_answer {
	var request map[string]json.RawMessage
	if json.Unmarshal(body, &request) != nil {
		return keyring_error(http.StatusBadRequest, "M_NOT_JSON", "Invalid keys")
	}
	db := federation_database()
	type uploaded struct {
		User  string            `json:"user_id"`
		Usage []string          `json:"usage"`
		Keys  map[string]string `json:"keys"`
	}
	master := request["master_key"]
	masterPublic := ""
	if len(master) > 0 {
		var key uploaded
		if json.Unmarshal(master, &key) != nil || key.User != user || !slices.Contains(key.Usage, "master") || len(key.Keys) != 1 {
			return keyring_error(http.StatusBadRequest, "M_INVALID_PARAM", "Invalid master key")
		}
		masterPublic = keyring_public(string(master))
	} else if row, _ := db.row("select key from signing where user=? and purpose='master'", user); row != nil {
		masterPublic = keyring_public(row_string(row, "key"))
	}
	for _, purpose := range []string{"self_signing", "user_signing"} {
		raw := request[purpose+"_key"]
		if len(raw) == 0 {
			continue
		}
		var key uploaded
		if json.Unmarshal(raw, &key) != nil || key.User != user || !slices.Contains(key.Usage, purpose) || len(key.Keys) != 1 {
			return keyring_error(http.StatusBadRequest, "M_INVALID_PARAM", "Invalid "+purpose+" key")
		}
		if masterPublic == "" || !keyring_verify(raw, user, "ed25519:"+masterPublic, masterPublic) {
			return keyring_error(http.StatusBadRequest, "M_INVALID_SIGNATURE", "The "+purpose+" key is not signed by the master key")
		}
	}
	if len(master) > 0 {
		if row, _ := db.row("select key from signing where user=? and purpose='master'", user); row != nil && keyring_public(row_string(row, "key")) != masterPublic {
			db.exec("delete from signatures where user=? and target=?", user, keyring_public(row_string(row, "key")))
		}
		db.exec("insert into signing (user, purpose, key) values (?, 'master', ?) on conflict(user, purpose) do update set key=excluded.key", user, string(keyring_strip(master)))
	}
	for _, purpose := range []string{"self_signing", "user_signing"} {
		if raw := request[purpose+"_key"]; len(raw) > 0 {
			db.exec("insert into signing (user, purpose, key) values (?, ?, ?) on conflict(user, purpose) do update set key=excluded.key", user, purpose, string(keyring_strip(raw)))
		}
	}
	keyring_signing_changed(user)
	return keyring_ok(map[string]any{})
}

// keyring_sign keeps the signatures a device uploads, each checked: the
// self-signing key's on the account's own devices, a device's on the
// account's master key, and the user-signing key's on someone else's master
// key, which only the signer is shown.
func keyring_sign(user string, body json.RawMessage) keyring_answer {
	var request map[string]map[string]json.RawMessage
	if json.Unmarshal(body, &request) != nil {
		return keyring_error(http.StatusBadRequest, "M_NOT_JSON", "Invalid signatures")
	}
	db := federation_database()
	failures := map[string]map[string]any{}
	fail := func(person string, target string, message string) {
		if failures[person] == nil {
			failures[person] = map[string]any{}
		}
		failures[person][target] = map[string]any{"errcode": "M_INVALID_SIGNATURE", "error": message} // i18n-ok: protocol text read by clients
	}
	changed := map[string]bool{}
	signing := false
	for person, objects := range request {
		for target, raw := range objects {
			var signed struct {
				Signatures map[string]map[string]string `json:"signatures"`
			}
			if json.Unmarshal(raw, &signed) != nil {
				fail(person, target, "Invalid object")
				continue
			}
			signatures := signed.Signatures[user]
			if len(signatures) == 0 {
				fail(person, target, "No signature of the uploader's")
				continue
			}
			// The object must be the key it names, as kept here.
			kept := ""
			if row, _ := db.row("select keys from clients where user=? and device=?", person, target); row != nil && person == user {
				kept = row_string(row, "keys")
			} else if master := keyring_signing_key(person, "master"); master != nil && keyring_public(string(master)) == target {
				kept = string(master)
			} else if self := keyring_signing_key(person, "self_signing"); self != nil && person == user && keyring_public(string(self)) == target {
				kept = string(self)
			}
			if kept == "" && !keyring_local(person) {
				// Someone on another server: their master key as their server
				// gives it, which a user-signing key signs.
				kept = string(raw)
			}
			if kept == "" || keyring_stripped(kept) != keyring_stripped(string(raw)) {
				fail(person, target, "Unknown key")
				continue
			}
			// The signatures already on the key are not uploaded again.
			var existing struct {
				Signatures map[string]map[string]string `json:"signatures"`
			}
			json.Unmarshal([]byte(keyring_signed(person, target, kept, user)), &existing)
			for id, value := range signatures {
				if existing.Signatures[user][id] == value {
					continue
				}
				public := strings.TrimPrefix(id, "ed25519:")
				allowed := false
				switch {
				case person == user && keyring_known(user, target):
					// A device of the account's, signed by its self-signing key.
					self := keyring_signing_key(user, "self_signing")
					allowed = self != nil && keyring_public(string(self)) == public
				case person == user:
					// The account's master key, signed by one of its devices.
					row, _ := db.row("select keys from clients where user=? and device=?", user, public)
					var keys struct {
						Keys map[string]string `json:"keys"`
					}
					json.Unmarshal([]byte(row_string(row, "keys")), &keys)
					if device := keys.Keys["ed25519:"+public]; device != "" {
						public = device
						allowed = true
					}
				default:
					// Someone else's master key, signed by the user-signing key.
					own := keyring_signing_key(user, "user_signing")
					allowed = own != nil && keyring_public(string(own)) == public
				}
				if !allowed || !keyring_verify(raw, user, id, public) {
					fail(person, target, "The signature does not check")
					continue
				}
				db.exec("insert or replace into signatures (user, target, signer, id, value) values (?, ?, ?, ?, ?)", person, target, user, id, value)
				if person == user {
					changed[target] = true
				} else {
					db.exec("insert into changes (user, viewer, created) values (?, ?, ?)", person, user, now())
				}
			}
		}
	}
	for target := range changed {
		if keyring_known(user, target) {
			row, _ := db.row("select keys from clients where user=? and device=?", user, target)
			keyring_changed(user, target, keyring_signed(user, target, row_string(row, "keys"), ""), false)
		} else {
			signing = true
		}
	}
	if signing {
		keyring_signing_changed(user)
	}
	return keyring_ok(map[string]any{"failures": failures})
}

// === Sending to devices ===

// keyring_send sends an event to devices, each named or every device of a
// user by "*": into the store for accounts here, to their servers for others.
// A transaction sent again is taken once.
func keyring_send(user string, device string, kind string, transaction string, body json.RawMessage) keyring_answer {
	var request struct {
		Messages map[string]map[string]json.RawMessage `json:"messages"`
	}
	if json.Unmarshal(body, &request) != nil {
		return keyring_error(http.StatusBadRequest, "M_NOT_JSON", "Invalid messages")
	}
	db := federation_database()
	key := "todevice|" + device + "|" + transaction
	if taken, _ := db.exists("select 1 from sent where account=? and key=?", user, key); taken {
		return keyring_ok(map[string]any{})
	}
	db.exec("insert into sent (account, key, event, created) values (?, ?, '', ?)", user, key, now())
	remote := map[string]map[string]map[string]json.RawMessage{}
	for person, devices := range request.Messages {
		id, err := spec.NewUserID(person, true)
		if err != nil {
			continue
		}
		if federation_local(id.Domain()) {
			keyring_deliver(user, person, kind, devices)
			continue
		}
		server := string(id.Domain())
		if remote[server] == nil {
			remote[server] = map[string]map[string]json.RawMessage{}
		}
		remote[server][person] = devices
	}
	if sender, err := spec.NewUserID(user, true); err == nil {
		for server, messages := range remote {
			federation_message([]string{server}, string(sender.Domain()), "m.direct_to_device", map[string]any{"sender": user, "type": kind, "message_id": uid(), "messages": messages})
		}
	}
	return keyring_ok(map[string]any{})
}

// keyring_deliver keeps what a sender sent to devices of a user's here.
func keyring_deliver(sender string, user string, kind string, devices map[string]json.RawMessage) {
	db := federation_database()
	for device, content := range devices {
		var targets []string
		if device == "*" {
			rows, _ := db.rows("select device from clients where user=?", user)
			for _, row := range rows {
				targets = append(targets, row_string(row, "device"))
			}
		} else if keyring_known(user, device) {
			targets = []string{device}
		}
		for _, target := range targets {
			db.exec("insert into deliveries (user, device, sender, type, content, created) values (?, ?, ?, ?, ?, ?)", user, target, sender, kind, string(content), now())
			// The account's own device here takes it at once (encryption.go).
			encryption_delivered(user, target)
		}
	}
	db.exec("delete from deliveries where created<?", now()-keyring_delivered)
}

// === The key backup ===

// keyring_backup answers the requests of the account's key backup: making,
// reading, changing and deleting a version, and putting, reading and
// deleting the room keys of the current one.
func keyring_backup(user string, method string, parts []string, query url.Values, body json.RawMessage) keyring_answer {
	db := federation_database()
	current := func() map[string]any {
		row, _ := db.row("select version, algorithm, auth, etag from backups where user=? and deleted=0 order by version desc limit 1", user)
		return row
	}
	describe := func(row map[string]any) map[string]any {
		var auth any
		json.Unmarshal([]byte(row_string(row, "auth")), &auth)
		count, _ := db.row("select count(*) as count from sessions where user=? and version=?", user, row_int(row, "version"))
		return map[string]any{"version": strconv.FormatInt(row_int(row, "version"), 10), "algorithm": row_string(row, "algorithm"), "auth_data": auth, "etag": strconv.FormatInt(row_int(row, "etag"), 10), "count": row_int(count, "count")}
	}
	switch {
	case len(parts) == 1 && parts[0] == "version" && method == "POST":
		var request struct {
			Algorithm string          `json:"algorithm"`
			Auth      json.RawMessage `json:"auth_data"`
		}
		if json.Unmarshal(body, &request) != nil || request.Algorithm == "" || len(request.Auth) == 0 {
			return keyring_error(http.StatusBadRequest, "M_BAD_JSON", "Invalid backup")
		}
		row, _ := db.row("select max(version) as version from backups where user=?", user)
		version := row_int(row, "version") + 1
		db.exec("insert into backups (user, version, algorithm, auth) values (?, ?, ?, ?)", user, version, request.Algorithm, string(request.Auth))
		return keyring_ok(map[string]any{"version": strconv.FormatInt(version, 10)})
	case len(parts) <= 2 && len(parts) >= 1 && parts[0] == "version" && method == "GET":
		var row map[string]any
		if len(parts) == 1 {
			row = current()
		} else {
			row, _ = db.row("select version, algorithm, auth, etag from backups where user=? and version=? and deleted=0", user, parts[1])
		}
		if row == nil {
			return keyring_error(http.StatusNotFound, "M_NOT_FOUND", "No backup")
		}
		return keyring_ok(describe(row))
	case len(parts) == 2 && parts[0] == "version" && method == "PUT":
		var request struct {
			Algorithm string          `json:"algorithm"`
			Auth      json.RawMessage `json:"auth_data"`
		}
		row := current()
		if json.Unmarshal(body, &request) != nil || row == nil || strconv.FormatInt(row_int(row, "version"), 10) != parts[1] || request.Algorithm != row_string(row, "algorithm") {
			return keyring_error(http.StatusBadRequest, "M_INVALID_PARAM", "Not the current backup")
		}
		db.exec("update backups set auth=? where user=? and version=?", string(request.Auth), user, parts[1])
		return keyring_ok(map[string]any{})
	case len(parts) == 2 && parts[0] == "version" && method == "DELETE":
		db.exec("update backups set deleted=1 where user=? and version=?", user, parts[1])
		db.exec("delete from sessions where user=? and version=?", user, parts[1])
		return keyring_ok(map[string]any{})
	case len(parts) >= 1 && parts[0] == "keys":
		version := query.Get("version")
		row := current()
		if method == "PUT" && (row == nil || strconv.FormatInt(row_int(row, "version"), 10) != version) {
			answer := keyring_error(http.StatusForbidden, "M_WRONG_ROOM_KEYS_VERSION", "Not the current backup")
			if row != nil {
				answer.body.(map[string]any)["current_version"] = strconv.FormatInt(row_int(row, "version"), 10)
			}
			return answer
		}
		switch method {
		case "PUT":
			var request struct {
				Rooms map[string]struct {
					Sessions map[string]json.RawMessage `json:"sessions"`
				} `json:"rooms"`
			}
			if json.Unmarshal(body, &request) != nil {
				return keyring_error(http.StatusBadRequest, "M_NOT_JSON", "Invalid keys")
			}
			if len(parts) == 3 {
				// One session: rooms/{room}/{session} as the path.
				request.Rooms = map[string]struct {
					Sessions map[string]json.RawMessage `json:"sessions"`
				}{parts[1]: {Sessions: map[string]json.RawMessage{parts[2]: body}}}
			}
			for room, sessions := range request.Rooms {
				for session, data := range sessions.Sessions {
					if keyring_better(user, version, room, session, data) {
						db.exec("insert or replace into sessions (user, version, room, session, data) values (?, ?, ?, ?, ?)", user, version, room, session, string(data))
					}
				}
			}
			db.exec("update backups set etag=etag+1 where user=? and version=?", user, version)
			return keyring_ok(map[string]any{"etag": strconv.FormatInt(row_int(current(), "etag"), 10), "count": describe(current())["count"]})
		case "GET":
			args := []any{user, version}
			filter := ""
			if len(parts) >= 2 {
				filter += " and room=?"
				args = append(args, parts[1])
			}
			if len(parts) == 3 {
				filter += " and session=?"
				args = append(args, parts[2])
			}
			rows, _ := db.rows("select room, session, data from sessions where user=? and version=?"+filter, args...)
			if len(parts) == 3 {
				if len(rows) == 0 {
					return keyring_error(http.StatusNotFound, "M_NOT_FOUND", "No such key")
				}
				return keyring_ok(json.RawMessage(row_string(rows[0], "data")))
			}
			rooms := map[string]map[string]map[string]json.RawMessage{}
			for _, r := range rows {
				room := row_string(r, "room")
				if rooms[room] == nil {
					rooms[room] = map[string]map[string]json.RawMessage{"sessions": {}}
				}
				rooms[room]["sessions"][row_string(r, "session")] = json.RawMessage(row_string(r, "data"))
			}
			if len(parts) == 2 {
				if sessions, found := rooms[parts[1]]; found {
					return keyring_ok(sessions)
				}
				return keyring_ok(map[string]any{"sessions": map[string]any{}})
			}
			return keyring_ok(map[string]any{"rooms": rooms})
		case "DELETE":
			db.exec("delete from sessions where user=? and version=?", user, version)
			db.exec("update backups set etag=etag+1 where user=? and version=?", user, version)
			return keyring_ok(map[string]any{"etag": strconv.FormatInt(row_int(current(), "etag"), 10), "count": 0})
		}
	}
	return keyring_error(http.StatusNotFound, "M_UNRECOGNIZED", "Unrecognised request")
}

// keyring_better reports whether a room key uploaded to the backup replaces
// the one kept: one checked against a verified device over one not, then
// the one from earlier in the session, then the one forwarded fewer times.
func keyring_better(user string, version string, room string, session string, data json.RawMessage) bool {
	type backed struct {
		First    int64 `json:"first_message_index"`
		Forwards int64 `json:"forwarded_count"`
		Verified bool  `json:"is_verified"`
	}
	row, _ := federation_database().row("select data from sessions where user=? and version=? and room=? and session=?", user, version, room, session)
	if row == nil {
		return true
	}
	var old, fresh backed
	if json.Unmarshal([]byte(row_string(row, "data")), &old) != nil || json.Unmarshal(data, &fresh) != nil {
		return true
	}
	if old.Verified != fresh.Verified {
		return fresh.Verified
	}
	if old.First != fresh.First {
		return fresh.First < old.First
	}
	return fresh.Forwards < old.Forwards
}

// keyring_data reads or keeps an item of the account's data by its type.
func keyring_data(user string, method string, kind string, body json.RawMessage) keyring_answer {
	switch method {
	case "GET":
		content := clients_setting(user, "", kind)
		if content == nil {
			return keyring_error(http.StatusNotFound, "M_NOT_FOUND", "No such data")
		}
		return keyring_ok(content)
	case "PUT":
		var object map[string]any
		if json.Unmarshal(body, &object) != nil {
			return keyring_error(http.StatusBadRequest, "M_NOT_JSON", "Invalid data")
		}
		clients_setting_set(user, "", kind, body)
		return keyring_ok(map[string]any{})
	}
	return keyring_error(http.StatusNotFound, "M_UNRECOGNIZED", "Unrecognised request")
}

// === Syncing ===

// keyring_sync is what a device has not yet been given since the position it
// gives back: what was sent it, who changed their devices of those it shares
// a room with, its count of one-time keys and its unclaimed fallback keys,
// and the position to give next. What it was given before that position is
// let go.
func keyring_sync(user string, device string, since string) map[string]any {
	delivered, changes := keyring_position(since)
	events, delivered := keyring_deliveries(user, device, delivered)
	changed, changes := keyring_changes(user, changes, since == "")
	return map[string]any{"events": events, "changed": changed, "left": []string{}, "counts": keyring_counts(user, device), "fallback": keyring_unused(user, device), "next": fmt.Sprintf("%d_%d", delivered, changes)}
}

// keyring_deliveries is what was sent a device since a delivery it was
// given, at most a hundred, and the last given; those before are let go.
func keyring_deliveries(user string, device string, delivered int64) ([]map[string]any, int64) {
	db := federation_database()
	if delivered > 0 {
		db.exec("delete from deliveries where user=? and device=? and id<=?", user, device, delivered)
	}
	events := []map[string]any{}
	rows, _ := db.rows("select id, sender, type, content from deliveries where user=? and device=? and id>? order by id limit 100", user, device, delivered)
	for _, row := range rows {
		var content any
		json.Unmarshal([]byte(row_string(row, "content")), &content)
		events = append(events, map[string]any{"sender": row_string(row, "sender"), "type": row_string(row, "type"), "content": content})
		delivered = row_int(row, "id")
	}
	return events, delivered
}

// keyring_changes is who changed their devices since a change, of those a
// user shares a room with, and the last change; a first sync starts from
// now, since the device asks for what it needs.
func keyring_changes(user string, changes int64, first bool) ([]string, int64) {
	db := federation_database()
	changed := []string{}
	if first {
		row, _ := db.row("select max(id) as id from changes")
		return changed, row_int(row, "id")
	}
	rows, _ := db.rows("select id, user, viewer from changes where id>? order by id", changes)
	shared := keyring_sharing(user)
	for _, row := range rows {
		changes = row_int(row, "id")
		person := row_string(row, "user")
		viewer := row_string(row, "viewer")
		if (viewer == "" || viewer == user) && (person == user || shared[person]) && !slices.Contains(changed, person) {
			changed = append(changed, person)
		}
	}
	return changed, changes
}

// keyring_position reads a sync position: the last delivery and the last
// change given.
func keyring_position(since string) (int64, int64) {
	first, second, _ := strings.Cut(since, "_")
	delivered, _ := strconv.ParseInt(first, 10, 64)
	changes, _ := strconv.ParseInt(second, 10, 64)
	return delivered, changes
}

// keyring_sharing is everyone who shares a room with a user here.
func keyring_sharing(user string) map[string]bool {
	out := map[string]bool{}
	for _, room := range signals_rooms(user) {
		state, _ := rooms_current(room)
		for member, membership := range rooms_members(state) {
			if membership == spec.Join || membership == spec.Invite {
				out[member] = true
			}
		}
	}
	return out
}

// === Other servers ===

// keyring_receive takes what another server sends about its users' keys: a
// device that changed, cross-signing keys that changed, and events sent to
// devices here.
func keyring_receive(origin spec.ServerName, kind string, content json.RawMessage) {
	theirs := func(user string) bool {
		id, err := spec.NewUserID(user, true)
		return err == nil && id.Domain() == origin
	}
	db := federation_database()
	switch kind {
	case "m.device_list_update", "m.signing_key_update":
		var update struct {
			User string `json:"user_id"`
		}
		if json.Unmarshal(content, &update) != nil || !theirs(update.User) {
			return
		}
		db.exec("insert into changes (user, created) values (?, ?)", update.User, now())
	case "m.direct_to_device":
		var message struct {
			Sender   string                                `json:"sender"`
			Type     string                                `json:"type"`
			ID       string                                `json:"message_id"`
			Messages map[string]map[string]json.RawMessage `json:"messages"`
		}
		if json.Unmarshal(content, &message) != nil || !theirs(message.Sender) || message.Type == "" {
			return
		}
		key := "todevice|" + string(origin) + "|" + message.ID
		if message.ID != "" {
			if taken, _ := db.exists("select 1 from sent where account=? and key=?", message.Sender, key); taken {
				return
			}
			db.exec("insert into sent (account, key, event, created) values (?, ?, '', ?)", message.Sender, key, now())
		}
		for user, devices := range message.Messages {
			if keyring_local(user) {
				keyring_deliver(message.Sender, user, message.Type, devices)
			}
		}
	}
}

// keyring_serve_devices answers another server asking for a user's devices.
func keyring_serve_devices(c *gin.Context, user string) {
	if !keyring_local(user) {
		federation_error(c, http.StatusNotFound, "M_NOT_FOUND", "Not a user of this server")
		return
	}
	db := federation_database()
	row, _ := db.row("select max(id) as id from changes where user=? and viewer=''", user)
	devices := []map[string]any{}
	for device, keys := range keyring_devices(user, nil) {
		entry := map[string]any{"device_id": device, "keys": keys}
		if name := keyring_name(user, device); name != "" {
			entry["device_display_name"] = name
		}
		devices = append(devices, entry)
	}
	answer := map[string]any{"user_id": user, "stream_id": row_int(row, "id"), "devices": devices}
	if master := keyring_signing_key(user, "master"); master != nil {
		answer["master_key"] = master
	}
	if self := keyring_signing_key(user, "self_signing"); self != nil {
		answer["self_signing_key"] = self
	}
	c.JSON(http.StatusOK, answer)
}

// keyring_serve_query answers another server's key query, for users here.
func keyring_serve_query(c *gin.Context, r *fclient.FederationRequest) {
	var request struct {
		Keys map[string][]string `json:"device_keys"`
	}
	if json.Unmarshal(r.Content(), &request) != nil {
		federation_error(c, http.StatusBadRequest, "M_NOT_JSON", "Invalid query")
		return
	}
	devices := map[string]any{}
	masters := map[string]any{}
	selves := map[string]any{}
	for user, wanted := range request.Keys {
		if !keyring_local(user) {
			continue
		}
		devices[user] = keyring_devices(user, wanted)
		if master := keyring_signing_key(user, "master"); master != nil {
			masters[user] = master
		}
		if self := keyring_signing_key(user, "self_signing"); self != nil {
			selves[user] = self
		}
	}
	c.JSON(http.StatusOK, map[string]any{"device_keys": devices, "master_keys": masters, "self_signing_keys": selves})
}

// keyring_serve_claim answers another server claiming keys of devices here.
func keyring_serve_claim(c *gin.Context, r *fclient.FederationRequest) {
	var request struct {
		Keys map[string]map[string]string `json:"one_time_keys"`
	}
	if json.Unmarshal(r.Content(), &request) != nil {
		federation_error(c, http.StatusBadRequest, "M_NOT_JSON", "Invalid claim")
		return
	}
	claimed := map[string]any{}
	for user, wanted := range request.Keys {
		if keyring_local(user) {
			claimed[user] = keyring_take(user, wanted)
		}
	}
	c.JSON(http.StatusOK, map[string]any{"one_time_keys": claimed})
}

// keyring_prune drops what waited past its time for a device and the
// changes every device has been given long since.
func keyring_prune() {
	db := federation_database()
	db.exec("delete from deliveries where created<?", now()-keyring_delivered)
	db.exec("delete from changes where created<? and id<(select max(id) from changes)", now()-keyring_delivered)
	db.exec("delete from sent where key like 'todevice|%' and created<?", now()-int64(7*24*time.Hour/time.Second))
}
