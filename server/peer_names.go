// Mochi server: Peer display names — the hostname a peer announces in
// peers/publish, stored per peer and shown wherever a peer is presented
// to a human.
//
// A name is a self-asserted label, not a credential. A peer announces its
// own hostname (the `hostname` setting, or the OS hostname); receivers
// display it as-is and the reader decides what to trust. Nothing keys
// logic off a name, and a peer's authoritative identity is its peer ID /
// fingerprint, which a name can never override or impersonate.
//
// Copyright © 2026 Mochi OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"os"
	"strings"
	"sync"

	sl "go.starlark.net/starlark"
)

// PeerName is the name a peer announced, with the last time it appeared
// in a publish (for ageing out).
type PeerName struct {
	Name    string
	Updated int64 // last time the name appeared in a publish
}

// peer_name_row is the scan target for the peers.db names table.
type peer_name_row struct {
	ID      string
	Name    string
	Updated int64
}

// peer_names_maximum caps how many names are stored per peer. A real
// announcement carries a single hostname; the cap is defensive.
const peer_names_maximum = 1

var (
	peer_names      = map[string][]PeerName{}
	peer_names_lock = &sync.Mutex{}
)

// peer_name_valid reports whether an announced name is acceptable: ASCII
// RFC-1123 hostname grammar only, lowercase, 253 chars or fewer. The
// strictness is anti-spoofing — Unicode homoglyphs, RTL overrides and
// control characters never reach a screen.
func peer_name_valid(name string) bool {
	if name == "" || len(name) > 253 {
		return false
	}
	for _, label := range strings.Split(name, ".") {
		if label == "" || len(label) > 63 {
			return false
		}
		if label[0] == '-' || label[len(label)-1] == '-' {
			return false
		}
		for i := 0; i < len(label); i++ {
			c := label[i]
			if (c < 'a' || c > 'z') && (c < '0' || c > '9') && c != '-' {
				return false
			}
		}
	}
	return true
}

// peer_names_announce returns this server's own hostname for
// peers/publish: the `hostname` setting, defaulting to the OS hostname.
// Empty when the administrator has turned `hostname_publish` off or the
// resolved name is invalid.
func peer_names_announce() string {
	if setting_get("hostname_publish", "true") != "true" {
		return ""
	}

	name := strings.ToLower(strings.TrimSpace(setting_get("hostname", "")))
	if name == "" {
		if h, err := os.Hostname(); err == nil {
			name = strings.ToLower(strings.TrimSpace(h))
		}
	}
	if !peer_name_valid(name) {
		return ""
	}

	return name
}

// peer_names_apply stores the name a peer announced, replacing whatever it
// claimed before. An announcement with no name from a peer that
// previously claimed one clears it (the operator turned announcing off —
// honor it).
func peer_names_apply(id string, names []string) {
	if id == "" || id == net_id {
		return
	}
	if len(names) > peer_names_maximum {
		names = names[:peer_names_maximum]
	}

	t := now()
	peer_names_lock.Lock()
	existing := peer_names[id]
	var next []PeerName
	for _, n := range names {
		duplicate := false
		for _, c := range next {
			if c.Name == n {
				duplicate = true
				break
			}
		}
		if !duplicate {
			next = append(next, PeerName{Name: n, Updated: t})
		}
	}
	changed := len(next) != len(existing)
	for i := 0; i < len(next) && !changed; i++ {
		if next[i].Name != existing[i].Name {
			changed = true
		}
	}
	if len(next) == 0 {
		delete(peer_names, id)
	} else {
		peer_names[id] = next
	}
	peer_names_lock.Unlock()

	if changed {
		debug("Peer %q announced name %v", id, names)
		peer_names_save(id)
	}
}

// peer_name returns the name a peer announced, or "" if none.
func peer_name(id string) string {
	peer_names_lock.Lock()
	defer peer_names_lock.Unlock()
	for _, c := range peer_names[id] {
		return c.Name
	}
	return ""
}

// peer_names_load fills the in-memory registry from peers.db at startup.
func peer_names_load() {
	var rows []peer_name_row
	db := db_open("db/peers.db")
	if err := db.scans(&rows, "select id, name, updated from names order by id, name"); err != nil {
		warn("Database error loading peer names: %v", err)
		return
	}
	peer_names_lock.Lock()
	for _, r := range rows {
		peer_names[r.ID] = append(peer_names[r.ID], PeerName{Name: r.Name, Updated: r.Updated})
	}
	peer_names_lock.Unlock()
}

// peer_names_save persists a peer's current name, replacing whatever
// peers.db held for it.
func peer_names_save(id string) {
	peer_names_lock.Lock()
	claims := append([]PeerName{}, peer_names[id]...)
	peer_names_lock.Unlock()

	db := db_open("db/peers.db")
	db.exec("delete from names where id=?", id)
	for _, c := range claims {
		db.exec("replace into names ( id, name, updated ) values ( ?, ?, ? )", id, c.Name, c.Updated)
	}
}

// peer_name_dict adds the display fields for a peer to a starlark dict:
// the self-asserted `name` and the authoritative `fingerprint`.
func peer_name_dict(entry *sl.Dict, id string) {
	_ = entry.SetKey(sl.String("name"), sl.String(peer_name(id)))
	_ = entry.SetKey(sl.String("fingerprint"), sl.String(fingerprint(id)))
}

// peer_name_fields is peer_name_dict for JSON-bound maps (gin handlers).
func peer_name_fields(m map[string]any, id string) {
	m["name"] = peer_name(id)
	m["fingerprint"] = fingerprint(id)
}

// peer_names_sweep drops a peer's name once it has gone quiet, with the
// same expiry as the peer prune.
func peer_names_sweep(expiry int64) {
	db := db_open("db/peers.db")
	db.exec("delete from names where updated<?", expiry)

	peer_names_lock.Lock()
	for id, claims := range peer_names {
		kept := claims[:0]
		for _, c := range claims {
			if c.Updated >= expiry {
				kept = append(kept, c)
			}
		}
		if len(kept) == 0 {
			delete(peer_names, id)
		} else {
			peer_names[id] = kept
		}
	}
	peer_names_lock.Unlock()
}
