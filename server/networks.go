// Mochi server: Other networks
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

// The networks Mochi can host accounts on - the fediverse (ActivityPub, which
// Mastodon and Lemmy speak), Bluesky, XMPP and Matrix - each behind a system
// setting the owner may switch off, and all of them behind the development
// switch in mochi.conf that keeps the code dormant until every part is ready.
// A protocol's engine, the Networks page and the apps all ask here whether a
// network is on; with the switch off, or a setting off, nothing of that network
// is offered or answered and Mochi behaves as it did before any of it existed.

package main

import (
	"encoding/json"
	"sort"

	sl "go.starlark.net/starlark"
)

// network_names are the networks, in the order the settings and the Networks
// page list them. Each is also the name of its system setting.
var network_names = []string{"fediverse", "bluesky", "xmpp", "matrix"}

// networks_development is the [development] networks switch in mochi.conf. Off
// by default: every engine, page and provider for the networks stays dormant
// until the whole of the federation work is ready, when the switch is removed.
var networks_development bool

// network_known reports whether a name is one of the networks.
func network_known(name string) bool {
	for _, n := range network_names {
		if n == name {
			return true
		}
	}
	return false
}

// network_enabled reports whether a network is on: the development switch is
// on and the owner has not switched the network's setting off.
func network_enabled(name string) bool {
	return networks_development && network_known(name) && setting_effective(name) == "true"
}

// networks_enabled lists the networks that are on, in their standard order.
func networks_enabled() []string {
	var on []string
	for _, n := range network_names {
		if network_enabled(n) {
			on = append(on, n)
		}
	}
	return on
}

// network_address is the address a hosted account has on its network: the
// handle (with a suffix name's separator written as the network wants it) on
// the domain. The fediverse and XMPP write user@domain, Matrix @user:domain,
// and Bluesky makes the handle a host name under the domain.
func network_address(network string, name string, domain string) string {
	switch network {
	case "fediverse", "xmpp":
		return name + "@" + domain
	case "matrix":
		return "@" + name + ":" + domain
	case "bluesky":
		return handle_bluesky(name) + "." + domain
	}
	return ""
}

// mochi.server.networks() -> dict: Each network's state for the owner's Status
// page: "working", "unavailable" with a reason, or "disabled" (the setting or
// the development switch off), and whether the development switch is on.
func api_server_networks(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if err := require_permission(t, fn, "server/read"); err != nil {
		return sl_error(fn, "%v", err)
	}
	networks := map[string]any{}
	for _, n := range network_names {
		state := map[string]any{"state": "disabled", "reason": ""}
		if network_enabled(n) {
			// No engine answers for a network yet; each reports its own
			// unavailability here when it is built.
			state["state"] = "working"
		}
		networks[n] = state
	}
	return sl_encode(map[string]any{
		"development": networks_development,
		"networks":    networks,
	}), nil
}

// mochi.account.networks() -> list: The networks a user may host accounts on,
// in their standard order: those the owner has on, when the development switch
// is on. Empty when networks are not offered on this server.
func api_account_networks(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if principal_caller(t) == nil {
		return sl_error(fn, "no user")
	}
	on := networks_enabled()
	sort.SliceStable(on, func(i, j int) bool { return network_index(on[i]) < network_index(on[j]) })
	return sl_encode(on), nil
}

// network_index is a network's place in the standard order.
func network_index(name string) int {
	for i, n := range network_names {
		if n == name {
			return i
		}
	}
	return len(network_names)
}

// === Hosted accounts ===
//
// A hosted account is a row of the user's accounts table (accounts.go) whose
// type is a network and whose data carries hosted, handle and domain. Its id
// is its permanent identifier; its identifier column is its address.

// account_hosted returns a user's hosted account on a network for a handle, or
// nil.
func account_hosted(user *User, network string, domain string, name string) map[string]any {
	for _, row := range accounts_hosted(user, network) {
		data, _ := row["data"].(map[string]any)
		if data["domain"] == domain && data["handle"] == name {
			return row
		}
	}
	return nil
}

// accounts_hosted lists a user's hosted accounts, on one network or on every
// network when network is empty, each with its data decoded.
func accounts_hosted(user *User, network string) []map[string]any {
	db := db_user(user, "user")
	rows, _ := db.rows("select id, type, label, identifier, data, created, verified, enabled from accounts order by created")
	var hosted []map[string]any
	for _, row := range rows {
		ptype, _ := row["type"].(string)
		if !network_known(ptype) || (network != "" && ptype != network) {
			continue
		}
		var data map[string]any
		if text, _ := row["data"].(string); text != "" {
			json.Unmarshal([]byte(text), &data)
		}
		if is_hosted, _ := data["hosted"].(bool); !is_hosted {
			continue
		}
		row["data"] = data
		hosted = append(hosted, row)
	}
	return hosted
}

// accounts_hosted_remove deletes a user's hosted accounts on a handle, on
// every network: the handle is being released or freed.
func accounts_hosted_remove(user *User, domain string, name string) {
	db := db_user(user, "user")
	for _, row := range accounts_hosted(user, "") {
		data, _ := row["data"].(map[string]any)
		if data["domain"] == domain && data["handle"] == name {
			db.row_remove(reg_accounts, map[string]any{"id": row["id"]})
		}
	}
}
