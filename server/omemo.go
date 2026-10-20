// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

// Mochi server: OMEMO devices
//
// Each of a user's XMPP accounts, hosted or linked, has one OMEMO device here
// (the federation plan, phase 27), in both versions: one identity and one set
// of one-time pre-keys serve both, each version with its own device list,
// bundle and sessions (ratchet.go). The device is published in the account's
// device lists on its own server, open to anyone, and a hosted account's
// device lists are its own publishing service's (bookmarks.go).
//
// A message to the account is decrypted as it comes, before the apps hear it;
// one that cannot be is handed on as encrypted and unread. A message of a
// conversation goes encrypted while its person has devices: to every usable
// device of theirs and of the account's own, in the version each speaks, its
// file shared encrypted (an aesgcm address). A room's message goes encrypted
// where the room shows who its people are and each of them has devices. A
// person's devices are trusted blindly until the user verifies one of them,
// after which a new one waits for the user; a device the user distrusts is
// neither encrypted to nor read.

package main

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"encoding/xml"
	"fmt"
	mathrand "math/rand/v2"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"mellium.im/xmpp/jid"
)

// The device's nodes in each version, and the namespaces of what goes with
// its messages.
const (
	omemo_legacy_list   = omemo_legacy + ".devicelist"
	omemo_legacy_bundle = omemo_legacy + ".bundles:"
	omemo_current_list  = omemo_current + ":devices"
	omemo_current_nodes = omemo_current + ":bundles"
	omemo_ns_envelope   = "urn:xmpp:sce:1"
	omemo_ns_hints      = "urn:xmpp:hints"
	omemo_ns_eme        = "urn:xmpp:eme:0"
)

// How many one-time pre-keys the device publishes, how long a signed pre-key
// is used and then kept, and how long a person's device lists are read again
// after.
var (
	omemo_prekeys  = 100
	omemo_rotation = int64(30 * 86400)
	omemo_fresh    = int64(300)
)

// omemo_fallback is the text clients that cannot read OMEMO show for an
// encrypted message, the protocol's own.
const omemo_fallback = "I sent you an OMEMO encrypted message but your client doesn't seem to support that." // i18n-ok: protocol text other clients show, not Mochi's

// An omemo_device is the OMEMO device of one of a user's XMPP accounts.
type omemo_device struct {
	user     *User
	account  string
	address  jid.JID
	id       uint32
	identity omemo_pair
	hosted   *xmpp_account
	linked   *linked

	// lock holds the device while a message is sealed or opened, its
	// sessions changing as it is. It is never held across a request: a
	// linked account's answers are read by the session's handler, which
	// takes it to open what arrives.
	lock sync.Mutex
	// announcing holds the device while it is published, one publication
	// at a time.
	announcing sync.Mutex
	// published records that the device's bundles and lists went to the
	// account's server since it was opened.
	published atomic.Bool
}

var (
	omemo_lock    sync.Mutex
	omemo_devices = map[string]*omemo_device{}
	// omemo_opening makes devices one at a time: a session's first message
	// and its publication open the same new device together.
	omemo_opening sync.Mutex
)

// An omemo_item is an item of a node as read.
type omemo_item struct {
	ID    string `xml:"id,attr"`
	Inner string `xml:",innerxml"`
}

// omemo_tables makes the device's tables in the user's encryption database.
func omemo_tables(db *DB) {
	db.exec("create table if not exists omemo (account text not null primary key, device integer not null, identity text not null, next integer not null, created integer not null)")
	db.exec("create table if not exists signed (account text not null, id integer not null, key text not null, legacy text not null, current text not null, created integer not null, primary key (account, id))")
	db.exec("create table if not exists prekeys (account text not null, id integer not null, key text not null, primary key (account, id))")
	db.exec("create table if not exists sessions (account text not null, version text not null, person text not null, device integer not null, state text not null, updated integer not null, primary key (account, version, person, device))")
	db.exec("create table if not exists identities (account text not null, person text not null, device integer not null, key text not null, trust text not null default '', created integer not null, primary key (account, person, device))")
	db.exec("create table if not exists lists (account text not null, person text not null, version text not null, devices text not null, fetched integer not null, primary key (account, person, version))")
	db.exec("create table if not exists opened (account text not null, digest text not null, body text not null, created integer not null, primary key (account, digest))")
}

// omemo_database is the user's encryption database, where the devices keep
// their keys and sessions.
func omemo_database(user *User) *DB {
	return encryption_database(user)
}

// omemo_device_of is the device of a user's XMPP account, made the first
// time: its id, not one the account's lists hold, its identity, its signed
// pre-key and its one-time pre-keys.
func omemo_device_of(user *User, account string) (*omemo_device, error) {
	key := user.UID + "|" + account
	omemo_lock.Lock()
	d := omemo_devices[key]
	omemo_lock.Unlock()
	if d != nil {
		return d, nil
	}
	omemo_opening.Lock()
	defer omemo_opening.Unlock()
	omemo_lock.Lock()
	d = omemo_devices[key]
	omemo_lock.Unlock()
	if d != nil {
		return d, nil
	}
	row, _ := db_user(user, "user").row("select * from accounts where id=?", account)
	d = &omemo_device{user: user, account: account}
	if a := xmpp_hosted_of(user, row); a != nil {
		d.hosted, d.address = a, a.address
	} else if l := linked_from(user, row); l != nil && l.kind == "xmpp" {
		address, err := xmpp_address(l.get("address"))
		if err != nil {
			return nil, err
		}
		d.linked, d.address = l, address.Bare()
	} else {
		return nil, fmt.Errorf("not an XMPP account")
	}
	db := omemo_database(user)
	if db == nil {
		return nil, fmt.Errorf("the encryption database cannot be opened")
	}
	if found, _ := db.row("select device, identity from omemo where account=?", account); found != nil {
		d.id = uint32(row_int(found, "device"))
		json.Unmarshal([]byte(row_string(found, "identity")), &d.identity)
	} else {
		var buffer [4]byte
		for d.id == 0 {
			rand.Read(buffer[:])
			d.id = binary.BigEndian.Uint32(buffer[:]) & 0x7fffffff
		}
		d.identity = omemo_generate()
		db.exec("insert into omemo (account, device, identity, next, created) values (?, ?, ?, ?, ?)", account, d.id, json_encode(d.identity), omemo_prekeys+1, now())
		d.rotate(db)
		for i := 1; i <= omemo_prekeys; i++ {
			db.exec("insert into prekeys (account, id, key) values (?, ?, ?)", account, i, json_encode(omemo_generate()))
		}
	}
	omemo_lock.Lock()
	if existing := omemo_devices[key]; existing != nil {
		d = existing
	} else {
		omemo_devices[key] = d
	}
	omemo_lock.Unlock()
	return d, nil
}

// omemo_device_close forgets a device, as its account goes.
func omemo_device_close(user *User, account string) {
	omemo_lock.Lock()
	delete(omemo_devices, user.UID+"|"+account)
	omemo_lock.Unlock()
	if db := omemo_database(user); db != nil {
		for _, table := range []string{"omemo", "signed", "prekeys", "sessions", "identities", "lists", "opened"} {
			db.exec("delete from "+table+" where account=?", account)
		}
	}
}

// rotate makes a new signed pre-key, signed for each version, keeping the one
// before it for key exchanges made with it.
func (d *omemo_device) rotate(db *DB) {
	row, _ := db.row("select max(id) as id from signed where account=?", d.account)
	id := row_int(row, "id") + 1
	key := omemo_generate()
	legacy := omemo_sign(d.identity.Private, omemo_typed(key.Public))
	current := omemo_sign(d.identity.Private, key.Public)
	db.exec("insert into signed (account, id, key, legacy, current, created) values (?, ?, ?, ?, ?, ?)", d.account, id, json_encode(key), base64.StdEncoding.EncodeToString(legacy), base64.StdEncoding.EncodeToString(current), now())
	db.exec("delete from signed where account=? and id<?", d.account, id-1)
}

// === The network ===

// request sends a request of the device's to an address and reads the
// answer into into: from Mochi's own sessions on a hosted account, through
// the session of a linked one.
func (d *omemo_device) request(ctx context.Context, to jid.JID, kind string, inner string, into any) error {
	if d.hosted != nil {
		answer, err := xmpp_ask(d.hosted, to, kind, inner)
		if err != nil {
			return err
		}
		if into != nil {
			return answer.decode(into)
		}
		return nil
	}
	c, err := linked_client(d.linked)
	if err != nil {
		return err
	}
	address := ""
	if !to.Equal(d.address) {
		address = ` to="` + xmpp_escape(to.String()) + `"`
	}
	var head xmpp_head
	if into == nil {
		into = &head
	}
	headed, ok := into.(xmpp_headed)
	if !ok {
		return fmt.Errorf("an answer must read its head")
	}
	return c.query(ctx, `<iq xmlns="jabber:client" type="`+kind+`"`+address+`>`+inner+`</iq>`, headed)
}

// send sends a message of the device's: a key exchange's answer.
func (d *omemo_device) send(ctx context.Context, to jid.JID, inner string) error {
	if d.hosted != nil {
		xmpp_route(xmpp_make("message", inner, "type", "chat", "id", uid()), d.hosted.mochi(), to)
		return nil
	}
	c, err := linked_client(d.linked)
	if err != nil {
		return err
	}
	return c.send(ctx, `<message xmlns="jabber:client" to="`+xmpp_escape(to.String())+`" type="chat" id="`+uid()+`">`+inner+`</message>`)
}

// publish publishes an item of a node of the account's, open to anyone. A
// server whose node is set up otherwise is asked to open it first.
func (d *omemo_device) publish(ctx context.Context, node string, item string, payload string, many bool) error {
	maximum := "1"
	if many {
		maximum = "max"
	}
	if d.hosted != nil {
		xmpp_pep_publish(d.hosted, node, item, payload, map[string]string{"pubsub#access_model": "open", "pubsub#max_items": maximum})
		return nil
	}
	options := `<x xmlns="jabber:x:data" type="submit"><field var="FORM_TYPE" type="hidden"><value>http://jabber.org/protocol/pubsub#publish-options</value></field><field var="pubsub#access_model"><value>open</value></field><field var="pubsub#max_items"><value>` + maximum + `</value></field></x>`
	text := `<pubsub xmlns="` + xmpp_ns_pubsub + `"><publish node="` + xmpp_escape(node) + `"><item id="` + xmpp_escape(item) + `">` + payload + `</item></publish><publish-options>` + options + `</publish-options></pubsub>`
	err := d.request(ctx, d.address, "set", text, nil)
	if err != nil && strings.Contains(err.Error(), "precondition-not-met") {
		configure := strings.Replace(options, "pubsub#publish-options", "pubsub#node_config", 1)
		d.request(ctx, d.address, "set", `<pubsub xmlns="`+xmpp_ns_pubsub_owner+`"><configure node="`+xmpp_escape(node)+`">`+configure+`</configure></pubsub>`, nil)
		err = d.request(ctx, d.address, "set", text, nil)
	}
	return err
}

// fetch reads a node of someone's: its items, or those named.
func (d *omemo_device) fetch(ctx context.Context, owner jid.JID, node string, item string) ([]omemo_item, error) {
	named := ""
	if item != "" {
		named = `<item id="` + xmpp_escape(item) + `"/>`
	}
	var answer struct {
		xmpp_head
		Pubsub struct {
			Items struct {
				Item []omemo_item `xml:"item"`
			} `xml:"items"`
		} `xml:"http://jabber.org/protocol/pubsub pubsub"`
	}
	if err := d.request(ctx, owner, "get", `<pubsub xmlns="`+xmpp_ns_pubsub+`"><items node="`+xmpp_escape(node)+`">`+named+`</items></pubsub>`, &answer); err != nil {
		return nil, err
	}
	return answer.Pubsub.Items.Item, nil
}

// === Publishing ===

// announce publishes the device: its bundle in each version, and its id in
// the account's device lists where they do not hold it. It runs once each
// time the device is opened, and again when its keys change.
func (d *omemo_device) announce(ctx context.Context, force bool) error {
	d.announcing.Lock()
	defer d.announcing.Unlock()
	if d.published.Load() && !force {
		return nil
	}
	d.lock.Lock()
	db := omemo_database(d.user)
	signed, _ := db.row("select * from signed where account=? order by id desc limit 1", d.account)
	if signed == nil || row_int(signed, "created") < now()-omemo_rotation {
		d.rotate(db)
		signed, _ = db.row("select * from signed where account=? order by id desc limit 1", d.account)
	}
	var key omemo_pair
	json.Unmarshal([]byte(row_string(signed, "key")), &key)
	id := strconv.FormatInt(row_int(signed, "id"), 10)
	prekeys, _ := db.rows("select id, key from prekeys where account=? order by id", d.account)
	var legacy, current strings.Builder
	for _, row := range prekeys {
		var pair omemo_pair
		json.Unmarshal([]byte(row_string(row, "key")), &pair)
		n := strconv.FormatInt(row_int(row, "id"), 10)
		legacy.WriteString(`<preKeyPublic preKeyId="` + n + `">` + base64.StdEncoding.EncodeToString(omemo_typed(pair.Public)) + `</preKeyPublic>`)
		current.WriteString(`<pk id="` + n + `">` + base64.StdEncoding.EncodeToString(pair.Public) + `</pk>`)
	}
	original := `<bundle xmlns="` + omemo_legacy + `"><signedPreKeyPublic signedPreKeyId="` + id + `">` + base64.StdEncoding.EncodeToString(omemo_typed(key.Public)) + `</signedPreKeyPublic><signedPreKeySignature>` + row_string(signed, "legacy") + `</signedPreKeySignature><identityKey>` + base64.StdEncoding.EncodeToString(omemo_typed(d.identity.Public)) + `</identityKey><prekeys>` + legacy.String() + `</prekeys></bundle>`
	latest := `<bundle xmlns="` + omemo_current + `"><spk id="` + id + `">` + base64.StdEncoding.EncodeToString(key.Public) + `</spk><spks>` + row_string(signed, "current") + `</spks><ik>` + base64.StdEncoding.EncodeToString(omemo_identity(d.identity.Public)) + `</ik><prekeys>` + current.String() + `</prekeys></bundle>`
	device := strconv.FormatUint(uint64(d.id), 10)
	d.lock.Unlock()
	if err := d.publish(ctx, omemo_legacy_bundle+device, "current", original, false); err != nil {
		return err
	}
	if err := d.publish(ctx, omemo_current_nodes, device, latest, true); err != nil {
		return err
	}
	for _, version := range []string{omemo_legacy, omemo_current} {
		listed, err := d.listed(ctx, d.address, version, true)
		if err != nil {
			return err
		}
		if slices.Contains(listed, d.id) {
			continue
		}
		listed = append(listed, d.id)
		d.list_publish(ctx, version, listed)
	}
	d.published.Store(true)
	return nil
}

// list_publish publishes the account's device list in a version.
func (d *omemo_device) list_publish(ctx context.Context, version string, devices []uint32) error {
	var b strings.Builder
	for _, id := range devices {
		if version == omemo_current && id == d.id {
			b.WriteString(`<device id="` + strconv.FormatUint(uint64(id), 10) + `" label="Mochi"/>`)
		} else {
			b.WriteString(`<device id="` + strconv.FormatUint(uint64(id), 10) + `"/>`)
		}
	}
	node, payload := omemo_legacy_list, `<list xmlns="`+omemo_legacy+`">`+b.String()+`</list>`
	if version == omemo_current {
		node, payload = omemo_current_list, `<devices xmlns="`+omemo_current+`">`+b.String()+`</devices>`
	}
	err := d.publish(ctx, node, "current", payload, false)
	if err == nil {
		omemo_database(d.user).exec("insert or replace into lists (account, person, version, devices, fetched) values (?, ?, ?, ?, ?)", d.account, d.address.String(), version, json_encode(devices), now())
	}
	return err
}

// omemo_unlisted reports whether a failure to read someone's device list is
// an answer that there is nothing to encrypt to: no such list, no such
// account, no publishing service, or a list not shown to the account. A
// server not reached, or not answering, says nothing either way.
func omemo_unlisted(err error) bool {
	for _, condition := range []string{"item-not-found", "forbidden", "service-unavailable", "feature-not-implemented", "not-allowed"} {
		if strings.Contains(err.Error(), condition) {
			return true
		}
	}
	return false
}

// omemo_list_parse is the device ids a device list item holds.
func omemo_list_parse(payload string) []uint32 {
	var list struct {
		Devices []struct {
			ID string `xml:"id,attr"`
		} `xml:"device"`
	}
	if xml.Unmarshal([]byte(payload), &list) != nil {
		return nil
	}
	var out []uint32
	for _, device := range list.Devices {
		if n, err := strconv.ParseUint(device.ID, 10, 32); err == nil && n > 0 && n < 1<<31 && !slices.Contains(out, uint32(n)) {
			out = append(out, uint32(n))
		}
	}
	return out
}

func (d *omemo_device) listed(ctx context.Context, person jid.JID, version string, fresh bool) ([]uint32, error) {
	if person.String() == "" {
		return nil, nil
	}
	db := omemo_database(d.user)
	row, _ := db.row("select devices, fetched from lists where account=? and person=? and version=?", d.account, person.String(), version)
	kept := func() []uint32 {
		var out []uint32
		json.Unmarshal([]byte(row_string(row, "devices")), &out)
		return out
	}
	if row != nil && !fresh && row_int(row, "fetched") > now()-omemo_fresh {
		return kept(), nil
	}
	node := omemo_legacy_list
	if version == omemo_current {
		node = omemo_current_list
	}
	// Any failure but an answer that lists nothing says nothing of the
	// list: the one last read stands in, unless one read now is asked for.
	items, err := d.fetch(ctx, person, node, "")
	if err != nil && !omemo_unlisted(err) {
		if row != nil && !fresh {
			return kept(), nil
		}
		return nil, err
	}
	var out []uint32
	if len(items) > 0 {
		out = omemo_list_parse(items[0].Inner)
	}
	db.exec("insert or replace into lists (account, person, version, devices, fetched) values (?, ?, ?, ?, ?)", d.account, person.String(), version, json_encode(out), now())
	return out, nil
}

// heard keeps a device list someone's server told of as it changed: the
// account's own, published again where it no longer holds this device.
func (d *omemo_device) heard(ctx context.Context, person jid.JID, node string, payload string) {
	version := omemo_legacy
	switch node {
	case omemo_legacy_list:
	case omemo_current_list:
		version = omemo_current
	default:
		return
	}
	devices := omemo_list_parse(payload)
	omemo_database(d.user).exec("insert or replace into lists (account, person, version, devices, fetched) values (?, ?, ?, ?, ?)", d.account, person.Bare().String(), version, json_encode(devices), now())
	if person.Bare().Equal(d.address) && !slices.Contains(devices, d.id) {
		encryption_running.Go(func() { d.list_publish(context.Background(), version, append(devices, d.id)) })
	}
}

// === Devices and trust ===

// An omemo_target is a device a message may go to.
type omemo_target struct {
	person  jid.JID
	id      uint32
	version string
}

// key names a target in a map.
func (t omemo_target) key() string {
	return t.version + " " + t.person.String() + " " + strconv.FormatUint(uint64(t.id), 10)
}

// targets are someone's devices, each in the version it speaks: OMEMO 2
// where it is listed there.
func (d *omemo_device) targets(ctx context.Context, person jid.JID, fresh bool) ([]omemo_target, error) {
	current, err := d.listed(ctx, person, omemo_current, fresh)
	if err != nil {
		return nil, err
	}
	legacy, err := d.listed(ctx, person, omemo_legacy, fresh)
	if err != nil {
		return nil, err
	}
	var out []omemo_target
	for _, id := range current {
		out = append(out, omemo_target{person: person, id: id, version: omemo_current})
	}
	for _, id := range legacy {
		if !slices.Contains(current, id) {
			out = append(out, omemo_target{person: person, id: id, version: omemo_legacy})
		}
	}
	return out, nil
}

// trusted is the user's word on a device of someone's: verified, distrusted,
// or nothing yet.
func (d *omemo_device) trusted(person jid.JID, id uint32) string {
	row, _ := omemo_database(d.user).row("select trust from identities where account=? and person=? and device=?", d.account, person.String(), id)
	return row_string(row, "trust")
}

// usable reports whether a message goes to a device: not one the user
// distrusts, and once one of the person's devices is verified, only those
// verified.
func (d *omemo_device) usable(person jid.JID, id uint32) bool {
	trust := d.trusted(person, id)
	if trust == "distrusted" {
		return false
	}
	verified, _ := omemo_database(d.user).exists("select 1 from identities where account=? and person=? and trust='verified'", d.account, person.String())
	return !verified || trust == "verified"
}

// identity keeps a device's identity as first seen. One that changes is a
// new device to the user, whose word on the old one is dropped.
func (d *omemo_device) remember(person jid.JID, id uint32, key []byte) {
	db := omemo_database(d.user)
	encoded := base64.StdEncoding.EncodeToString(key)
	row, _ := db.row("select key from identities where account=? and person=? and device=?", d.account, person.String(), id)
	if row != nil && row_string(row, "key") == encoded {
		return
	}
	db.exec("insert or replace into identities (account, person, device, key, trust, created) values (?, ?, ?, ?, '', ?)", d.account, person.String(), id, encoded, now())
}

// trust records the user's word on a device of someone's.
func (d *omemo_device) trust(person jid.JID, id uint32, trust string) error {
	db := omemo_database(d.user)
	if found, _ := db.exists("select 1 from identities where account=? and person=? and device=?", d.account, person.String(), id); !found {
		return fmt.Errorf("unknown device")
	}
	if trust == "undecided" {
		trust = ""
	}
	db.exec("update identities set trust=? where account=? and person=? and device=?", trust, d.account, person.String(), id)
	return nil
}

// omemo_fingerprint is an identity as people compare it: its Curve25519 key
// in the original version, its Ed25519 form in OMEMO 2, in hex.
func omemo_fingerprint(key []byte, version string) string {
	if version == omemo_current {
		return hex.EncodeToString(omemo_identity(key))
	}
	return hex.EncodeToString(key)
}

// devices are someone's devices: each with its version, its fingerprint
// where its identity is known, and the user's word on it.
func (d *omemo_device) devices(ctx context.Context, person jid.JID) ([]map[string]any, error) {
	targets, err := d.targets(ctx, person, true)
	if err != nil {
		return nil, err
	}
	var out []map[string]any
	for _, t := range targets {
		own := person.Equal(d.address) && t.id == d.id
		key := d.identity_of(person, t.id)
		if key == nil && !own {
			if b, err := d.bundle(ctx, t); err == nil {
				d.remember(person, t.id, b.identity)
				key = b.identity
			}
		}
		if own {
			key = d.identity.Public
		}
		fingerprint := ""
		if key != nil {
			fingerprint = omemo_fingerprint(key, t.version)
		}
		trust := d.trusted(person, t.id)
		if trust == "" {
			trust = "undecided"
		}
		out = append(out, map[string]any{"id": strconv.FormatUint(uint64(t.id), 10), "version": t.version, "fingerprint": fingerprint, "trust": trust, "own": own})
	}
	return out, nil
}

// identity_of is a device's identity as kept, or nil.
func (d *omemo_device) identity_of(person jid.JID, id uint32) []byte {
	row, _ := omemo_database(d.user).row("select key from identities where account=? and person=? and device=?", d.account, person.String(), id)
	if row == nil {
		return nil
	}
	key, _ := base64.StdEncoding.DecodeString(row_string(row, "key"))
	return key
}

// status is the device as the apps show it: its id and its fingerprint in
// each version.
func (d *omemo_device) status() map[string]any {
	return map[string]any{
		"network": "xmpp",
		"address": d.address.String(),
		"device":  strconv.FormatUint(uint64(d.id), 10),
		"fingerprints": []any{
			map[string]any{"version": omemo_legacy, "fingerprint": omemo_fingerprint(d.identity.Public, omemo_legacy)},
			map[string]any{"version": omemo_current, "fingerprint": omemo_fingerprint(d.identity.Public, omemo_current)},
		},
	}
}

// === Sessions ===

// bundle reads a device's bundle and picks one of its one-time pre-keys.
func (d *omemo_device) bundle(ctx context.Context, t omemo_target) (*omemo_bundle, error) {
	device := strconv.FormatUint(uint64(t.id), 10)
	node, item := omemo_legacy_bundle+device, ""
	if t.version == omemo_current {
		node, item = omemo_current_nodes, device
	}
	items, err := d.fetch(ctx, t.person, node, item)
	if err != nil {
		return nil, err
	}
	payload := ""
	for _, i := range items {
		if item == "" || i.ID == item {
			payload = i.Inner
			break
		}
	}
	if payload == "" {
		return nil, fmt.Errorf("the device has no bundle")
	}
	return omemo_bundle_parse(t.version, payload)
}

// omemo_bundle_parse reads a bundle, with one of its one-time pre-keys
// chosen at random.
func omemo_bundle_parse(version string, payload string) (*omemo_bundle, error) {
	type key struct {
		ID   string `xml:",any,attr"`
		Text string `xml:",chardata"`
	}
	decode := func(text string) []byte {
		out, _ := base64.StdEncoding.DecodeString(strings.TrimSpace(text))
		return out
	}
	number := func(text string) uint32 {
		n, _ := strconv.ParseUint(text, 10, 32)
		return uint32(n)
	}
	b := &omemo_bundle{}
	var prekeys []key
	if version == omemo_legacy {
		var x struct {
			Signed struct {
				ID   string `xml:"signedPreKeyId,attr"`
				Text string `xml:",chardata"`
			} `xml:"signedPreKeyPublic"`
			Signature string `xml:"signedPreKeySignature"`
			Identity  string `xml:"identityKey"`
			Prekeys   []struct {
				ID   string `xml:"preKeyId,attr"`
				Text string `xml:",chardata"`
			} `xml:"prekeys>preKeyPublic"`
		}
		if xml.Unmarshal([]byte(payload), &x) != nil {
			return nil, fmt.Errorf("invalid bundle")
		}
		b.identity, b.signed, b.key, b.signature = omemo_untyped(decode(x.Identity)), number(x.Signed.ID), omemo_untyped(decode(x.Signed.Text)), decode(x.Signature)
		for _, p := range x.Prekeys {
			prekeys = append(prekeys, key{p.ID, p.Text})
		}
	} else {
		var x struct {
			Signed struct {
				ID   string `xml:"id,attr"`
				Text string `xml:",chardata"`
			} `xml:"spk"`
			Signature string `xml:"spks"`
			Identity  string `xml:"ik"`
			Prekeys   []struct {
				ID   string `xml:"id,attr"`
				Text string `xml:",chardata"`
			} `xml:"prekeys>pk"`
		}
		if xml.Unmarshal([]byte(payload), &x) != nil {
			return nil, fmt.Errorf("invalid bundle")
		}
		b.edwards = decode(x.Identity)
		b.identity, b.signed, b.key, b.signature = omemo_montgomery(b.edwards), number(x.Signed.ID), omemo_untyped(decode(x.Signed.Text)), decode(x.Signature)
		for _, p := range x.Prekeys {
			prekeys = append(prekeys, key{p.ID, p.Text})
		}
	}
	if b.identity == nil || b.key == nil || len(b.signature) != 64 {
		return nil, fmt.Errorf("invalid bundle")
	}
	mathrand.Shuffle(len(prekeys), func(i, j int) { prekeys[i], prekeys[j] = prekeys[j], prekeys[i] })
	for _, p := range prekeys {
		if once := omemo_untyped(decode(p.Text)); once != nil && number(p.ID) != 0 {
			b.prekey, b.once = number(p.ID), once
			break
		}
	}
	return b, nil
}

// session is the device's session with another in a version, or nil.
func (d *omemo_device) session(version string, person jid.JID, id uint32) *omemo_session {
	row, _ := omemo_database(d.user).row("select state from sessions where account=? and version=? and person=? and device=?", d.account, version, person.String(), id)
	if row == nil {
		return nil
	}
	var s omemo_session
	if json.Unmarshal([]byte(row_string(row, "state")), &s) != nil {
		return nil
	}
	return &s
}

// keep keeps a session as it now stands.
func (d *omemo_device) keep(person jid.JID, id uint32, s *omemo_session) {
	omemo_database(d.user).exec("insert or replace into sessions (account, version, person, device, state, updated) values (?, ?, ?, ?, ?, ?)", d.account, s.Version, person.String(), id, json_encode(s), now())
}

// === Messages ===

// An omemo_header is a version's encrypted element as it came.
type omemo_header struct {
	SID  string `xml:"sid,attr"`
	Keys []struct {
		RID    string `xml:"rid,attr"`
		Prekey string `xml:"prekey,attr"`
		Kex    string `xml:"kex,attr"`
		Text   string `xml:",chardata"`
	} `xml:"key"`
	Groups []struct {
		JID  string `xml:"jid,attr"`
		Keys []struct {
			RID  string `xml:"rid,attr"`
			Kex  string `xml:"kex,attr"`
			Text string `xml:",chardata"`
		} `xml:"key"`
	} `xml:"keys"`
	IV string `xml:"iv"`
}

// An omemo_element is an encrypted element of a message in either version.
type omemo_element struct {
	Header  omemo_header `xml:"header"`
	Payload *string      `xml:"payload"`
}

// wanted reports whether a message to people goes encrypted: each of them
// has a device. It fails where someone's devices cannot be read and never
// were, since whether they would be cannot then be told.
func (d *omemo_device) wanted(ctx context.Context, people []jid.JID) (bool, error) {
	if len(people) == 0 {
		return false, nil
	}
	for _, person := range people {
		targets, err := d.targets(ctx, person, false)
		if err != nil {
			debug("OMEMO %s: %s's devices not read: %v", d.address, person, err)
			return false, err
		}
		if len(targets) == 0 {
			return false, nil
		}
	}
	return true, nil
}

// seal encrypts a message's body for every usable device of the people given
// and of the account's own: the encrypted elements it is sent with. In a
// room OMEMO 2's envelope names the room.
func (d *omemo_device) seal(ctx context.Context, people []jid.JID, room string, body string) (string, error) {
	if err := d.announce(ctx, false); err != nil {
		return "", err
	}
	// Each person once, the account itself among them: a room's people
	// include its sender.
	var everyone []jid.JID
	for _, person := range append(slices.Clone(people), d.address) {
		if !slices.ContainsFunc(everyone, func(p jid.JID) bool { return p.Equal(person) }) {
			everyone = append(everyone, person)
		}
	}
	// Their devices, and the bundles of those with no session yet, asked
	// of the network before the device is held.
	var found []omemo_target
	bundles := map[string]*omemo_bundle{}
	for _, person := range everyone {
		listed, err := d.targets(ctx, person, false)
		if err != nil {
			continue
		}
		for _, t := range listed {
			if person.Equal(d.address) && t.id == d.id {
				continue
			}
			found = append(found, t)
			if d.session(t.version, person, t.id) != nil {
				continue
			}
			b, err := d.bundle(ctx, t)
			if err != nil {
				debug("OMEMO %s: no bundle of %s's device %d: %v", d.address, person, t.id, err)
				continue
			}
			bundles[t.key()] = b
		}
	}
	d.lock.Lock()
	defer d.lock.Unlock()
	var targets []omemo_target
	reached := false
	for _, t := range found {
		person := t.person
		if !d.usable(person, t.id) {
			continue
		}
		if d.session(t.version, person, t.id) == nil {
			b := bundles[t.key()]
			if b == nil {
				continue
			}
			if known := d.identity_of(person, t.id); known != nil && !slices.Equal(known, b.identity) {
				d.remember(person, t.id, b.identity)
				if !d.usable(person, t.id) {
					continue
				}
			}
			d.remember(person, t.id, b.identity)
			s, err := omemo_begin(t.version, d.identity, b)
			if err != nil {
				debug("OMEMO %s: no session with %s's device %d: %v", d.address, person, t.id, err)
				continue
			}
			d.keep(person, t.id, s)
		}
		targets = append(targets, t)
		if !person.Equal(d.address) {
			reached = true
		}
	}
	if !reached {
		return "", fmt.Errorf("no device of theirs can be encrypted to")
	}
	out := ""
	legacy := slices.DeleteFunc(slices.Clone(targets), func(t omemo_target) bool { return t.version != omemo_legacy })
	if len(legacy) > 0 {
		key, iv := make([]byte, 16), make([]byte, 12)
		rand.Read(key)
		rand.Read(iv)
		block, _ := aes.NewCipher(key)
		gcm, _ := cipher.NewGCM(block)
		sealed := gcm.Seal(nil, iv, []byte(body), nil)
		material := append(slices.Clone(key), sealed[len(sealed)-16:]...)
		var keys strings.Builder
		for _, t := range legacy {
			text, kex, err := d.encrypt(t, material)
			if err != nil {
				continue
			}
			prekey := ""
			if kex {
				prekey = ` prekey="true"`
			}
			keys.WriteString(`<key rid="` + strconv.FormatUint(uint64(t.id), 10) + `"` + prekey + `>` + text + `</key>`)
		}
		out += `<encrypted xmlns="` + omemo_legacy + `"><header sid="` + strconv.FormatUint(uint64(d.id), 10) + `">` + keys.String() + `<iv>` + base64.StdEncoding.EncodeToString(iv) + `</iv></header><payload>` + base64.StdEncoding.EncodeToString(sealed[:len(sealed)-16]) + `</payload></encrypted>`
	}
	current := slices.DeleteFunc(slices.Clone(targets), func(t omemo_target) bool { return t.version != omemo_current })
	if len(current) > 0 {
		to := room
		if to == "" && len(people) > 0 {
			to = people[0].String()
		}
		material, payload := omemo_envelope(`<body xmlns="jabber:client">`+xmpp_escape(body)+`</body>`, d.address.String(), to)
		groups := map[string]*strings.Builder{}
		var order []string
		for _, t := range current {
			text, kex, err := d.encrypt(t, material)
			if err != nil {
				continue
			}
			group := groups[t.person.String()]
			if group == nil {
				group = &strings.Builder{}
				groups[t.person.String()] = group
				order = append(order, t.person.String())
			}
			flag := ""
			if kex {
				flag = ` kex="true"`
			}
			group.WriteString(`<key rid="` + strconv.FormatUint(uint64(t.id), 10) + `"` + flag + `>` + text + `</key>`)
		}
		var keys strings.Builder
		for _, person := range order {
			keys.WriteString(`<keys jid="` + xmpp_escape(person) + `">` + groups[person].String() + `</keys>`)
		}
		out += `<encrypted xmlns="` + omemo_current + `"><header sid="` + strconv.FormatUint(uint64(d.id), 10) + `">` + keys.String() + `</header><payload>` + payload + `</payload></encrypted>`
	}
	return out, nil
}

// encrypt encrypts a message's key material for a device on its session.
func (d *omemo_device) encrypt(t omemo_target, material []byte) (string, bool, error) {
	s := d.session(t.version, t.person, t.id)
	if s == nil {
		return "", false, fmt.Errorf("no session")
	}
	sealed, kex, err := s.encrypt(d.identity.Public, material, d.id)
	if err != nil {
		return "", false, err
	}
	d.keep(t.person, t.id, s)
	return base64.StdEncoding.EncodeToString(sealed), kex, nil
}

// An omemo_opened is a message as its device read it: its content, or that
// it carried only keys, which completes a session.
type omemo_opened struct {
	version   string
	content   string
	transport bool
}

// open decrypts a message sent to the device by someone, from the encrypted
// elements it came with, in the version they hold a key for the device in.
// In a room the sender is the person the room shows them to be. It asks
// nothing of the network, running as the message is read from it; what it
// sends afterwards goes once it returns.
func (d *omemo_device) open(sender jid.JID, room string, latest *omemo_element, original *omemo_element) (*omemo_opened, error) {
	d.lock.Lock()
	var opened *omemo_opened
	var answer *omemo_target
	var err error = omemo_refused
	if latest != nil {
		opened, answer, err = d.open_version(omemo_current, sender, room, latest)
	}
	if opened == nil && original != nil {
		opened, answer, err = d.open_version(omemo_legacy, sender, room, original)
	}
	d.lock.Unlock()
	if opened == nil {
		return nil, err
	}
	// A session another device began is answered at once, so it stops
	// sending its key exchange; the one-time pre-key it used is replaced.
	if answer != nil {
		encryption_running.Go(func() {
			ctx := context.Background()
			d.replenish(ctx)
			d.answer(ctx, *answer)
		})
	}
	return opened, nil
}

// open_version decrypts a message's element of a version where it holds a
// key for the device, answering a device to send a key transport to where
// the message began a session.
func (d *omemo_device) open_version(version string, sender jid.JID, room string, e *omemo_element) (*omemo_opened, *omemo_target, error) {
	sid64, err := strconv.ParseUint(e.Header.SID, 10, 32)
	if err != nil {
		return nil, nil, omemo_refused
	}
	sid := uint32(sid64)
	mine := strconv.FormatUint(uint64(d.id), 10)
	text, kex := "", false
	if version == omemo_legacy {
		for _, k := range e.Header.Keys {
			if k.RID == mine {
				text, kex = k.Text, k.Prekey == "true" || k.Prekey == "1"
			}
		}
	} else {
		for _, g := range e.Header.Groups {
			if bare, err := jid.Parse(g.JID); err != nil || !bare.Bare().Equal(d.address) {
				continue
			}
			for _, k := range g.Keys {
				if k.RID == mine {
					text, kex = k.Text, k.Kex == "true" || k.Kex == "1"
				}
			}
		}
	}
	if text == "" {
		return nil, nil, fmt.Errorf("the message is not encrypted for this device")
	}
	if d.trusted(sender, sid) == "distrusted" {
		return nil, nil, fmt.Errorf("the user distrusts the sending device")
	}
	raw, err := base64.StdEncoding.DecodeString(strings.TrimSpace(text))
	if err != nil {
		return nil, nil, omemo_refused
	}
	digest := sha256.Sum256(append([]byte(version+"|"+sender.String()+"|"), raw...))
	db := omemo_database(d.user)
	// A message read before, as an archive gives it again.
	if row, _ := db.row("select body from opened where account=? and digest=?", d.account, hex.EncodeToString(digest[:])); row != nil {
		return &omemo_opened{version: version, content: row_string(row, "body")}, nil, nil
	}
	var material []byte
	var answer *omemo_target
	s := d.session(version, sender, sid)
	if kex || (version == omemo_current && len(raw) > 0 && raw[0] == 0x08) {
		k, err := omemo_kex_parse(version, raw)
		if err != nil {
			if !kex {
				goto plain
			}
			return nil, nil, err
		}
		if s == nil || !slices.Equal(s.Base, k.base) {
			signed, _ := db.row("select key from signed where account=? and id=?", d.account, k.signed)
			if signed == nil {
				return nil, nil, fmt.Errorf("the key exchange names an unknown signed pre-key")
			}
			var signed_pair omemo_pair
			json.Unmarshal([]byte(row_string(signed, "key")), &signed_pair)
			var once *omemo_pair
			if k.prekey != 0 {
				row, _ := db.row("select key from prekeys where account=? and id=?", d.account, k.prekey)
				if row == nil {
					return nil, nil, fmt.Errorf("the key exchange names a used one-time pre-key")
				}
				once = &omemo_pair{}
				json.Unmarshal([]byte(row_string(row, "key")), once)
			}
			s, err = omemo_accept(version, d.identity, signed_pair, once, k)
			if err != nil {
				return nil, nil, err
			}
			material, err = s.decrypt(d.identity.Public, k.message)
			if err != nil {
				return nil, nil, err
			}
			if k.prekey != 0 {
				db.exec("delete from prekeys where account=? and id=?", d.account, k.prekey)
			}
			if known := d.identity_of(sender, sid); known == nil || !slices.Equal(known, k.identity) {
				d.remember(sender, sid, k.identity)
			}
			d.keep(sender, sid, s)
			answer = &omemo_target{person: sender, id: sid, version: version}
			goto payload
		}
		raw = k.message
	}
plain:
	if s == nil {
		return nil, nil, fmt.Errorf("no session with the sending device")
	}
	material, err = s.decrypt(d.identity.Public, raw)
	if err != nil {
		return nil, nil, err
	}
	d.keep(sender, sid, s)
payload:
	if e.Payload == nil {
		return &omemo_opened{version: version, transport: true}, answer, nil
	}
	sealed, err := base64.StdEncoding.DecodeString(strings.TrimSpace(*e.Payload))
	if err != nil {
		return nil, nil, omemo_refused
	}
	var content string
	if version == omemo_legacy {
		iv, _ := base64.StdEncoding.DecodeString(strings.TrimSpace(e.Header.IV))
		content, err = omemo_gcm_open(material, iv, sealed)
	} else {
		content, err = omemo_unenvelope(material, sealed, sender.String(), room, d.address.String())
	}
	if err != nil {
		return nil, nil, err
	}
	db.exec("insert or replace into opened (account, digest, body, created) values (?, ?, ?, ?)", d.account, hex.EncodeToString(digest[:]), content, now())
	return &omemo_opened{version: version, content: content}, answer, nil
}

// answer sends an empty message to a device that began a session, which
// completes it.
func (d *omemo_device) answer(ctx context.Context, t omemo_target) {
	d.lock.Lock()
	material := make([]byte, 32)
	if t.version == omemo_current {
		material = make([]byte, 48)
	}
	rand.Read(material)
	text, _, err := d.encrypt(t, material)
	d.lock.Unlock()
	if err != nil {
		return
	}
	device := strconv.FormatUint(uint64(d.id), 10)
	key := `<key rid="` + strconv.FormatUint(uint64(t.id), 10) + `">` + text + `</key>`
	inner := ""
	if t.version == omemo_legacy {
		iv := make([]byte, 12)
		rand.Read(iv)
		inner = `<encrypted xmlns="` + omemo_legacy + `"><header sid="` + device + `">` + key + `<iv>` + base64.StdEncoding.EncodeToString(iv) + `</iv></header></encrypted>`
	} else {
		inner = `<encrypted xmlns="` + omemo_current + `"><header sid="` + device + `"><keys jid="` + xmpp_escape(t.person.String()) + `">` + key + `</keys></header></encrypted>`
	}
	d.send(ctx, t.person, inner+`<store xmlns="`+omemo_ns_hints+`"/>`)
}

// replenish makes one-time pre-keys for those used, and publishes them.
func (d *omemo_device) replenish(ctx context.Context) {
	d.lock.Lock()
	db := omemo_database(d.user)
	row, _ := db.row("select count(*) as n from prekeys where account=?", d.account)
	have := int(row_int(row, "n"))
	if have >= omemo_prekeys {
		d.lock.Unlock()
		return
	}
	state, _ := db.row("select next from omemo where account=?", d.account)
	next := row_int(state, "next")
	for ; have < omemo_prekeys; have++ {
		db.exec("insert into prekeys (account, id, key) values (?, ?, ?)", d.account, next, json_encode(omemo_generate()))
		next++
	}
	db.exec("update omemo set next=? where account=?", next, d.account)
	d.lock.Unlock()
	d.announce(ctx, true)
}

// === Content ===

// omemo_gcm_open decrypts the original version's payload: AES-GCM under the
// key the message's key material carries, its tag after the key or, from
// older clients, after the payload.
func omemo_gcm_open(material []byte, iv []byte, sealed []byte) (string, error) {
	var key []byte
	switch len(material) {
	case 16:
		key = material
	case 32:
		key, sealed = material[:16], append(slices.Clone(sealed), material[16:]...)
	case 48:
		key, sealed = material[:32], append(slices.Clone(sealed), material[32:]...)
	default:
		return "", omemo_refused
	}
	block, err := aes.NewCipher(key)
	if err != nil || len(iv) == 0 {
		return "", omemo_refused
	}
	gcm, err := cipher.NewGCMWithNonceSize(block, len(iv))
	if err != nil {
		return "", omemo_refused
	}
	plain, err := gcm.Open(nil, iv, sealed, nil)
	if err != nil {
		return "", omemo_refused
	}
	return string(plain), nil
}

// omemo_payload_keys are OMEMO 2's payload keys from a message's key.
func omemo_payload_keys(key []byte) ([]byte, []byte, []byte) {
	derived := omemo_derive(key, nil, "OMEMO Payload", 80)
	return derived[:32], derived[32:64], derived[64:80]
}

// omemo_envelope encrypts content in OMEMO 2's envelope (XEP-0420): the key
// material sent to each device, and the payload.
func omemo_envelope(content string, from string, to string) ([]byte, string) {
	key := make([]byte, 32)
	rand.Read(key)
	encryption, authentication, iv := omemo_payload_keys(key)
	padding := make([]byte, 1+mathrand.IntN(100))
	rand.Read(padding)
	envelope := `<envelope xmlns="` + omemo_ns_envelope + `"><content>` + content + `</content><rpad>` + base64.StdEncoding.EncodeToString(padding) + `</rpad><time stamp="` + time.Now().UTC().Format(time.RFC3339) + `"/><to jid="` + xmpp_escape(to) + `"/><from jid="` + xmpp_escape(from) + `"/></envelope>`
	sealed := omemo_cbc_seal(encryption, iv, []byte(envelope))
	mac := omemo_mac(authentication, sealed)[:16]
	return append(key, mac...), base64.StdEncoding.EncodeToString(sealed)
}

// omemo_unenvelope decrypts OMEMO 2's payload and answers what its envelope
// holds, checking it came from the sender and, in a room, was for it.
func omemo_unenvelope(material []byte, sealed []byte, sender string, room string, own string) (string, error) {
	if len(material) != 48 {
		return "", omemo_refused
	}
	encryption, authentication, iv := omemo_payload_keys(material[:32])
	if subtle.ConstantTimeCompare(omemo_mac(authentication, sealed)[:16], material[32:]) != 1 {
		return "", omemo_refused
	}
	plain, err := omemo_cbc_open(encryption, iv, sealed)
	if err != nil {
		return "", err
	}
	var envelope struct {
		XMLName xml.Name `xml:"urn:xmpp:sce:1 envelope"`
		Content struct {
			Inner string `xml:",innerxml"`
		} `xml:"content"`
		From struct {
			JID string `xml:"jid,attr"`
		} `xml:"from"`
		To struct {
			JID string `xml:"jid,attr"`
		} `xml:"to"`
	}
	if xml.Unmarshal(plain, &envelope) != nil {
		return "", omemo_refused
	}
	if from, err := jid.Parse(envelope.From.JID); envelope.From.JID != "" && (err != nil || from.Bare().String() != sender) {
		return "", fmt.Errorf("the envelope names another sender")
	}
	if to, err := jid.Parse(envelope.To.JID); room != "" && envelope.To.JID != "" && (err != nil || to.Bare().String() != room) {
		return "", fmt.Errorf("the envelope names another room")
	}
	return envelope.Content.Inner, nil
}

// === Messages of conversations ===

// omemo_apply gives a message what its device read: its body, and in OMEMO
// 2 the elements its envelope protected, of those a message carries in the
// clear only where it does not carry them.
func omemo_apply(m *xmpp_message, version string, content string) {
	if version == omemo_legacy {
		m.opened = &content
		return
	}
	var inner xmpp_message
	if xml.Unmarshal([]byte(`<message xmlns="jabber:client">`+content+`</message>`), &inner) != nil {
		empty := ""
		m.opened = &empty
		return
	}
	body := inner.Body
	m.opened = &body
	if m.Replace == nil {
		m.Replace = inner.Replace
	}
	if m.Reply == nil {
		m.Reply = inner.Reply
	}
	if m.Reactions == nil {
		m.Reactions = inner.Reactions
	}
	if m.Retract == nil {
		m.Retract = inner.Retract
	}
	if len(m.Links) == 0 {
		m.Links = inner.Links
	}
	m.Fallbacks = append(m.Fallbacks, inner.Fallbacks...)
}

// omemo_read decrypts an encrypted message of an account's as it comes, from
// the person who sent it - in a room, the person the room shows them to be -
// and gives the message what it carried. A message that cannot be read
// stays encrypted; one that carried only keys is marked so. It asks nothing
// of the network.
func omemo_read(user *User, account string, m *xmpp_message, sender jid.JID, room string) {
	if m.OMEMO == nil && m.OMEMO2 == nil {
		return
	}
	d, err := omemo_device_of(user, account)
	if err != nil {
		return
	}
	opened, err := d.open(sender.Bare(), room, m.OMEMO2, m.OMEMO)
	if err != nil {
		debug("OMEMO %s: message from %s not decrypted: %v", d.address, sender, err)
		// A device not in their lists is news of the lists changed.
		encryption_running.Go(func() {
			ctx, cancel := context.WithTimeout(context.Background(), xmpp_query_timeout)
			defer cancel()
			d.listed(ctx, sender.Bare(), omemo_current, true)
			d.listed(ctx, sender.Bare(), omemo_legacy, true)
		})
		return
	}
	if opened.transport {
		m.transport = true
		return
	}
	omemo_apply(m, opened.version, opened.content)
	// A file shared encrypted names its key in its address.
	if address, key := omemo_shared(*m.opened); address != "" {
		encryption_database(user).exec("insert or replace into files (account, url, file, created) values (?, ?, ?, ?)", account, address, json_encode(map[string]string{"aesgcm": key}), now())
		m.Links = append(m.Links[:0:0], struct {
			URL string `xml:"url"`
		}{URL: address})
		*m.opened = address
	}
}

// omemo_withdraw takes an account's device out of the account's device lists
// and its bundles off the account's server, as a linked account goes, so
// its contacts stop encrypting to it. An account that never had a device is
// left alone.
func omemo_withdraw(ctx context.Context, user *User, account string) {
	db := omemo_database(user)
	if db == nil {
		return
	}
	if made, _ := db.exists("select 1 from omemo where account=?", account); !made {
		return
	}
	d, err := omemo_device_of(user, account)
	if err != nil {
		return
	}
	for _, version := range []string{omemo_legacy, omemo_current} {
		listed, err := d.listed(ctx, d.address, version, true)
		if err != nil || !slices.Contains(listed, d.id) {
			continue
		}
		d.list_publish(ctx, version, slices.DeleteFunc(listed, func(id uint32) bool { return id == d.id }))
	}
	device := strconv.FormatUint(uint64(d.id), 10)
	d.request(ctx, d.address, "set", `<pubsub xmlns="`+xmpp_ns_pubsub_owner+`"><delete node="`+xmpp_escape(omemo_legacy_bundle+device)+`"/></pubsub>`, nil)
	d.request(ctx, d.address, "set", `<pubsub xmlns="`+xmpp_ns_pubsub+`"><retract node="`+omemo_current_nodes+`" notify="true"><item id="`+device+`"/></retract></pubsub>`, nil)
}

// omemo_start opens the device of an account and publishes it, away from
// whatever asked.
func omemo_start(user *User, account string) {
	d, err := omemo_device_of(user, account)
	if err != nil {
		return
	}
	encryption_running.Go(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*xmpp_query_timeout)
		defer cancel()
		if err := d.announce(ctx, false); err != nil {
			debug("OMEMO %s: device not published: %v", d.address, err)
		}
	})
}

// omemo_shared is the address and key of a file shared encrypted, where a
// body is one: aesgcm://host/path#key, its key the IV and the AES key in hex.
func omemo_shared(body string) (string, string) {
	body = strings.TrimSpace(body)
	if !strings.HasPrefix(body, "aesgcm://") || strings.ContainsAny(body, " \n") {
		return "", ""
	}
	u, err := url.Parse(body)
	if err != nil || u.Host == "" || (len(u.Fragment) != 88 && len(u.Fragment) != 96) {
		return "", ""
	}
	if _, err := hex.DecodeString(u.Fragment); err != nil {
		return "", ""
	}
	key := u.Fragment
	u.Scheme, u.Fragment = "https", ""
	return u.String(), key
}

// omemo_file_open decrypts a file shared encrypted.
func omemo_file_open(key string, data []byte) ([]byte, error) {
	raw, err := hex.DecodeString(key)
	if err != nil || (len(raw) != 44 && len(raw) != 48) {
		return nil, omemo_refused
	}
	iv, secret := raw[:len(raw)-32], raw[len(raw)-32:]
	block, err := aes.NewCipher(secret)
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCMWithNonceSize(block, len(iv))
	if err != nil {
		return nil, err
	}
	return gcm.Open(nil, iv, data, nil)
}

// omemo_file_seal encrypts a file to share encrypted, answering the key its
// address carries.
func omemo_file_seal(data []byte) ([]byte, string) {
	iv, secret := make([]byte, 12), make([]byte, 32)
	rand.Read(iv)
	rand.Read(secret)
	block, _ := aes.NewCipher(secret)
	gcm, _ := cipher.NewGCM(block)
	return gcm.Seal(nil, iv, data, nil), hex.EncodeToString(append(iv, secret...))
}

// omemo_people are those a conversation's messages are encrypted to: the
// person, or a room's people as it shows them, nil where a room shows anyone
// only by their nick.
func omemo_people(account string, to jid.JID, room bool) []jid.JID {
	if !room {
		return []jid.JID{to.Bare()}
	}
	omemo_occupants_lock.Lock()
	defer omemo_occupants_lock.Unlock()
	occupants := omemo_occupants[account+"|"+to.Bare().String()]
	var out []jid.JID
	for _, address := range occupants {
		if address == "" {
			return nil
		}
		if person, err := jid.Parse(address); err == nil && !slices.ContainsFunc(out, func(p jid.JID) bool { return p.Equal(person) }) {
			out = append(out, person)
		}
	}
	return out
}

// The people in each room an account is in, by nick: the address the room
// shows for each, "" where it shows none.
var (
	omemo_occupants      = map[string]map[string]string{}
	omemo_occupants_lock sync.Mutex
)

// omemo_occupant keeps who is in a room, as its presence says.
func omemo_occupant(account string, room string, nick string, address string, left bool) {
	omemo_occupants_lock.Lock()
	defer omemo_occupants_lock.Unlock()
	key := account + "|" + room
	if omemo_occupants[key] == nil {
		omemo_occupants[key] = map[string]string{}
	}
	if left {
		delete(omemo_occupants[key], nick)
		return
	}
	if parsed, err := jid.Parse(address); err == nil && address != "" {
		address = parsed.Bare().String()
	}
	omemo_occupants[key][nick] = address
}

// omemo_sender is who sent a room's message: the address the room shows for
// its nick.
func omemo_sender(account string, room string, nick string) (jid.JID, bool) {
	omemo_occupants_lock.Lock()
	address := omemo_occupants[account+"|"+room][nick]
	omemo_occupants_lock.Unlock()
	if address == "" {
		return jid.JID{}, false
	}
	parsed, err := jid.Parse(address)
	return parsed, err == nil
}

// omemo_seal_message encrypts a message of a conversation where it goes
// encrypted, answering its encrypted elements, or "" where it goes in the
// clear: its text, or a file shared encrypted by its address.
func omemo_seal_message(ctx context.Context, user *User, account string, to jid.JID, room bool, body string, file string) (string, error) {
	people := omemo_people(account, to, room)
	d, err := omemo_device_of(user, account)
	if err != nil {
		return "", nil
	}
	// Nothing goes in the clear to someone who may read only encrypted.
	wanted, err := d.wanted(ctx, people)
	if err != nil {
		return "", fmt.Errorf("their encryption devices could not be read: %w", err)
	}
	if !wanted {
		return "", nil
	}
	if file != "" {
		key := encryption_shared(user, account, file)
		if key == "" {
			return "", fmt.Errorf("a file for an encrypted conversation must be uploaded for it")
		}
		u, err := url.Parse(file)
		if err != nil {
			return "", err
		}
		u.Scheme, u.Fragment = "aesgcm", key
		body = u.String()
	}
	destination := ""
	if room {
		destination = to.Bare().String()
	}
	return d.seal(ctx, people, destination, body)
}

// omemo_encrypted reports whether a conversation's messages go encrypted:
// where that cannot be told, a file for it goes encrypted, since its message
// will not go in the clear.
func omemo_encrypted(ctx context.Context, user *User, account string, to jid.JID, room bool) bool {
	d, err := omemo_device_of(user, account)
	if err != nil {
		return false
	}
	wanted, err := d.wanted(ctx, omemo_people(account, to, room))
	return wanted || err != nil
}

// omemo_room reports whether an address is a room the account is in.
func omemo_room(user *User, account string, to jid.JID) bool {
	if to.Resourcepart() != "" {
		return false
	}
	row, _ := db_user(user, "user").row("select * from accounts where id=?", account)
	if a := xmpp_hosted_of(user, row); a != nil {
		return xmpp_joined_of(a, to.Bare().String()) != nil
	}
	if l := linked_from(user, row); l != nil {
		if c, err := linked_client(l); err == nil {
			c.lock.Lock()
			_, in := c.rooms[to.Bare().String()]
			c.lock.Unlock()
			return in
		}
	}
	return false
}

// person is someone as an app names them: by their address, or in a room as
// the room shows them, the person it shows them to be.
func (d *omemo_device) person(name string) (jid.JID, bool) {
	address, err := jid.Parse(strings.TrimPrefix(name, "xmpp:"))
	if err != nil || address.Localpart() == "" {
		return jid.JID{}, false
	}
	if address.Resourcepart() != "" {
		return omemo_sender(d.account, address.Bare().String(), address.Resourcepart())
	}
	return address, true
}
