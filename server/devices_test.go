// Mochi server: devices - the durable record push accounts hang off.
//
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the Mochi
// Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"testing"

	sl "go.starlark.net/starlark"
)

type api_builtin = func(*sl.Thread, *sl.Builtin, sl.Tuple, []sl.Tuple) (sl.Value, error)

// device_test_thread is a user with a user.db and an app holding the account
// permissions the device API is gated on.
func device_test_thread(t *testing.T) (*sl.Thread, *User) {
	t.Helper()
	user := create_test_user(t)
	db_user(user, "user")
	permission_grant(user, "app", "accounts/read")
	permission_grant(user, "app", "accounts/write")
	thread := &sl.Thread{Name: "test"}
	thread.SetLocal("user", user)
	thread.SetLocal("app", &App{id: "app"})
	return thread, user
}

func device_call(t *testing.T, thread *sl.Thread, name string, fn api_builtin, args sl.Tuple, kwargs []sl.Tuple) sl.Value {
	t.Helper()
	result, err := fn(thread, sl.NewBuiltin(name, fn), args, kwargs)
	if err != nil {
		t.Fatalf("%s: %v", name, err)
	}
	return result
}

func device_kwargs(pairs ...string) []sl.Tuple {
	kwargs := []sl.Tuple{}
	for i := 0; i+1 < len(pairs); i += 2 {
		kwargs = append(kwargs, sl.Tuple{sl.String(pairs[i]), sl.String(pairs[i+1])})
	}
	return kwargs
}

func dict_string(t *testing.T, value sl.Value, key string) string {
	t.Helper()
	dict, ok := value.(*sl.Dict)
	if !ok {
		t.Fatalf("result is %T, want a dict", value)
	}
	item, found, _ := dict.Get(sl.String(key))
	if !found {
		t.Fatalf("result has no %q", key)
	}
	s, _ := sl.AsString(item)
	return s
}

func dict_strings(t *testing.T, value sl.Value, key string) []string {
	t.Helper()
	dict := value.(*sl.Dict)
	item, found, _ := dict.Get(sl.String(key))
	if !found {
		t.Fatalf("result has no %q", key)
	}
	// sl_encode renders a slice as a tuple; accept either sequence.
	sequence, ok := item.(sl.Indexable)
	if !ok {
		t.Fatalf("%q is %T, want a sequence", key, item)
	}
	out := []string{}
	for i := 0; i < sequence.Len(); i++ {
		s, _ := sl.AsString(sequence.Index(i))
		out = append(out, s)
	}
	return out
}

func TestDeviceRegisterIsAnUpsert(t *testing.T) {
	thread, user := device_test_thread(t)
	db := db_user(user, "user")

	first := device_call(t, thread, "mochi.device.register", api_device_register,
		sl.Tuple{sl.String("phone-0001"), sl.String("S24U")}, nil)
	if got := dict_string(t, first, "label"); got != "S24U" {
		t.Errorf("label = %q, want S24U", got)
	}

	// A renamed phone re-registers under the same id: one row, new label.
	second := device_call(t, thread, "mochi.device.register", api_device_register,
		sl.Tuple{sl.String("phone-0001"), sl.String("Tablet")}, nil)
	if got := dict_string(t, second, "label"); got != "Tablet" {
		t.Errorf("label after re-registration = %q, want Tablet", got)
	}
	if n := db.integer("select count(*) from devices"); n != 1 {
		t.Errorf("devices after re-registration = %d, want 1", n)
	}

	list := device_call(t, thread, "mochi.device.list", api_device_list, nil, nil)
	if n := sl.Len(list); n != 1 {
		t.Errorf("list returned %d devices, want 1", n)
	}

	unknown := device_call(t, thread, "mochi.device.get", api_device_get, sl.Tuple{sl.String("phone-9999")}, nil)
	if unknown != sl.None {
		t.Errorf("get of an unregistered id = %v, want None", unknown)
	}

	// The id is client-minted, so its shape is the only thing the server can
	// hold it to.
	if _, err := api_device_register(thread, sl.NewBuiltin("mochi.device.register", api_device_register),
		sl.Tuple{sl.String("bad id"), sl.String("x")}, nil); err == nil {
		t.Error("an id outside the UUID alphabet was accepted")
	}
}

// One phone is one push target: a second per-device account on the same
// device retires the first, whichever transport either used.
func TestDeviceSupersedesPushAccounts(t *testing.T) {
	thread, user := device_test_thread(t)
	db := db_user(user, "user")
	device_call(t, thread, "mochi.device.register", api_device_register,
		sl.Tuple{sl.String("phone-0001"), sl.String("S24U")}, nil)

	fcm := device_call(t, thread, "mochi.account.add", api_account_add, sl.Tuple{sl.String("fcm")},
		device_kwargs("token", "t1", "install_id", "i1", "label", "S24U", "device", "phone-0001"))
	if got := dict_string(t, fcm, "device"); got != "phone-0001" {
		t.Errorf("fcm account device = %q, want phone-0001", got)
	}
	if got := dict_strings(t, fcm, "superseded"); len(got) != 0 {
		t.Errorf("first account on the device superseded %v, want nothing", got)
	}

	up := device_call(t, thread, "mochi.account.add", api_account_add, sl.Tuple{sl.String("unifiedpush")},
		device_kwargs("auth", "a", "p256dh", "p", "endpoint", "/menu/-/push/inbound/x", "label", "S24U", "device", "phone-0001"))
	if got := dict_strings(t, up, "superseded"); len(got) != 1 || got[0] != dict_string(t, fcm, "id") {
		t.Errorf("unifiedpush registration superseded %v, want the fcm account %q", got, dict_string(t, fcm, "id"))
	}
	if n := db.integer("select count(*) from accounts where device='phone-0001'"); n != 1 {
		t.Errorf("accounts on the device = %d, want 1: the superseded row is still a push target", n)
	}

	// Re-registering the same endpoint is the in-place update path; it must
	// bind the device too, or a registration that raced the device's own would
	// never heal.
	db.exec("update accounts set device='' where type='unifiedpush'")
	again := device_call(t, thread, "mochi.account.add", api_account_add, sl.Tuple{sl.String("unifiedpush")},
		device_kwargs("auth", "a", "p256dh", "p", "endpoint", "/menu/-/push/inbound/x", "label", "S24U", "device", "phone-0001"))
	if got := dict_string(t, again, "device"); got != "phone-0001" {
		t.Errorf("in-place re-registration left device = %q, want phone-0001", got)
	}
	if got := dict_string(t, again, "id"); got != dict_string(t, up, "id") {
		t.Errorf("re-registering the same endpoint minted a new account %q, want %q", got, dict_string(t, up, "id"))
	}

	// A device the user never registered is a caller bug, not a device to
	// invent; a device on an account that is not per-device is the same.
	builtin := sl.NewBuiltin("mochi.account.add", api_account_add)
	if _, err := api_account_add(thread, builtin, sl.Tuple{sl.String("fcm")},
		device_kwargs("token", "t2", "install_id", "i2", "device", "phone-9999")); err == nil {
		t.Error("an unregistered device id was accepted")
	}
	if _, err := api_account_add(thread, builtin, sl.Tuple{sl.String("url")},
		device_kwargs("url", "https://example.com/hook", "device", "phone-0001")); err == nil {
		t.Error("a device on a url account was accepted")
	}
}

func TestDeviceRemoveTakesItsAccounts(t *testing.T) {
	thread, user := device_test_thread(t)
	db := db_user(user, "user")
	device_call(t, thread, "mochi.device.register", api_device_register,
		sl.Tuple{sl.String("phone-0001"), sl.String("S24U")}, nil)
	device_call(t, thread, "mochi.account.add", api_account_add, sl.Tuple{sl.String("fcm")},
		device_kwargs("token", "t1", "install_id", "i1", "device", "phone-0001"))
	// An account with no device is not the phone's and must survive.
	device_call(t, thread, "mochi.account.add", api_account_add, sl.Tuple{sl.String("url")},
		device_kwargs("url", "https://example.com/hook"))

	removed := device_call(t, thread, "mochi.device.remove", api_device_remove, sl.Tuple{sl.String("phone-0001")}, nil)
	if removed != sl.True {
		t.Fatalf("remove = %v, want True", removed)
	}
	if n := db.integer("select count(*) from accounts where type='fcm'"); n != 0 {
		t.Errorf("fcm accounts after forgetting the device = %d, want 0", n)
	}
	if n := db.integer("select count(*) from accounts"); n != 1 {
		t.Errorf("accounts after forgetting the device = %d, want the 1 that was not its", n)
	}
	if again := device_call(t, thread, "mochi.device.remove", api_device_remove, sl.Tuple{sl.String("phone-0001")}, nil); again != sl.False {
		t.Errorf("removing a forgotten device = %v, want False", again)
	}
}
