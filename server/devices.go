// Mochi server: devices, the durable record of a client the user runs Mochi on.
//
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the Mochi
// Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"regexp"

	sl "go.starlark.net/starlark"
	sls "go.starlark.net/starlarkstruct"
)

// A device is a phone or tablet the user runs Mochi on, identified by an id the
// client generated once and keeps. It outlives what used to stand in for it:
// push accounts churn when a transport switches or an endpoint is reissued, and
// sessions are wiped by design. Push accounts hang off it (accounts.device), so
// one phone stays one push target, and an app can key a per-device preference
// on it.
//
// The id is client-asserted. That is fine for what it gates - what the device
// shows itself, which push accounts group under it - because anyone able to
// forge it already holds the user's session. It must never become an
// authorization boundary: nothing cross-user may read it.

// The client mints a UUID; anything outside that alphabet is a caller bug.
var device_pattern = regexp.MustCompile(`^[A-Za-z0-9-]{8,64}$`)

func device_valid(id string) bool {
	return device_pattern.MatchString(id)
}

// device_get returns the user's device row, or nil when the id is unknown.
func device_get(db *DB, id string) map[string]any {
	if !device_valid(id) {
		return nil
	}
	row, _ := db.row("select id, label, created, seen from devices where id=?", id)
	return row
}

// account_device_bind stamps account id with its device and retires any other
// per-device account on that device, returning the retired ids. A phone
// re-registering on a new transport, or with a reissued endpoint, is the same
// push target; left in place, the old row would go on receiving every
// notification until its TTL sweep.
func account_device_bind(db *DB, id, device string) []any {
	superseded := []any{}
	if device == "" {
		return superseded
	}
	rows, _ := db.rows("select id, type from accounts where device=? and id!=?", device, id)
	for _, row := range rows {
		if !account_device(row_string(row, "type")) {
			continue
		}
		superseded = append(superseded, row_string(row, "id"))
		db.exec("delete from accounts where id=?", row["id"])
	}
	db.exec("update accounts set device=? where id=?", device, id)
	return superseded
}

// account_superseded marks an in-place re-registration's result: the account
// already existed, and these are the ids it retired, so the caller can move
// whatever it keyed on them across rather than starting the account afresh.
func account_superseded(row map[string]any, superseded []any) map[string]any {
	row["superseded"] = superseded
	row["existing"] = true
	return row
}

// mochi.device.register(id, label) -> dict: Register the calling client's device, or refresh its label and last-seen time
func api_device_register(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var id, label string
	if err := sl.UnpackArgs(fn.Name(), args, kwargs, "id", &id, "label", &label); err != nil {
		return nil, err
	}
	if err := require_permission(t, fn, "accounts/write"); err != nil {
		return sl_error(fn, "%v", err)
	}
	user := principal_caller(t)
	if user == nil {
		return sl_error(fn, "no user")
	}
	if !device_valid(id) {
		return sl_error(fn, "invalid device id")
	}
	if len(label) > 256 {
		return sl_error(fn, "label too long")
	}
	db := db_user(user, "user")
	at := now()
	if device_get(db, id) == nil {
		db.exec("insert into devices (id, label, created, seen) values (?, ?, ?, ?)", id, label, at, at)
	} else {
		db.exec("update devices set label=?, seen=? where id=?", label, at, id)
	}
	return sl_encode(device_get(db, id)), nil
}

// mochi.device.get(id) -> dict|None: Get one of the user's devices, or None when the id is not registered
func api_device_get(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var id string
	if err := sl.UnpackArgs(fn.Name(), args, kwargs, "id", &id); err != nil {
		return nil, err
	}
	if err := require_permission(t, fn, "accounts/read"); err != nil {
		return sl_error(fn, "%v", err)
	}
	user := principal_caller(t)
	if user == nil {
		return sl_error(fn, "no user")
	}
	row := device_get(db_user(user, "user"), id)
	if row == nil {
		return sl.None, nil
	}
	return sl_encode(row), nil
}

// mochi.device.list() -> list: List the user's devices, oldest first
func api_device_list(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if err := require_permission(t, fn, "accounts/read"); err != nil {
		return sl_error(fn, "%v", err)
	}
	user := principal_caller(t)
	if user == nil {
		return sl_error(fn, "no user")
	}
	rows, err := db_user(user, "user").rows("select id, label, created, seen from devices order by created")
	if err != nil {
		return sl_error(fn, "database error: %v", err)
	}
	result := []map[string]any{}
	result = append(result, rows...)
	return sl_encode(result), nil
}

// mochi.device.remove(id) -> bool: Forget a device and the push accounts registered from it
func api_device_remove(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var id string
	if err := sl.UnpackArgs(fn.Name(), args, kwargs, "id", &id); err != nil {
		return nil, err
	}
	if err := require_permission(t, fn, "accounts/write"); err != nil {
		return sl_error(fn, "%v", err)
	}
	user := principal_caller(t)
	if user == nil {
		return sl_error(fn, "no user")
	}
	db := db_user(user, "user")
	if device_get(db, id) == nil {
		return sl.False, nil
	}
	db.exec("delete from accounts where device=?", id)
	db.exec("delete from devices where id=?", id)
	return sl.True, nil
}

// Starlark API module
var api_device = sls.FromStringDict(sl.String("mochi.device"), sl.StringDict{
	"get":      sl.NewBuiltin("mochi.device.get", api_device_get),
	"list":     sl.NewBuiltin("mochi.device.list", api_device_list),
	"register": sl.NewBuiltin("mochi.device.register", api_device_register),
	"remove":   sl.NewBuiltin("mochi.device.remove", api_device_remove),
})
