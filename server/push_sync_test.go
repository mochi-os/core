// Mochi server: tests for sync signals.
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the Mochi
// Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"sync"
	"testing"
	"time"

	sl "go.starlark.net/starlark"
)

// account_sync_record swaps in a recorder for the sender and a short window,
// and puts both back when the test ends.
func account_sync_record(t *testing.T, window time.Duration) func() []string {
	t.Helper()
	var lock sync.Mutex
	var sent []string
	deliver, previous := account_sync_deliver, account_sync_window
	account_sync_deliver = func(user *User, kind string) {
		lock.Lock()
		sent = append(sent, user.UID+":"+kind)
		lock.Unlock()
	}
	account_sync_window = window
	account_sync_lock.Lock()
	account_sync_states = map[string]*account_sync_state{}
	account_sync_lock.Unlock()
	t.Cleanup(func() {
		account_sync_deliver, account_sync_window = deliver, previous
	})
	return func() []string {
		lock.Lock()
		defer lock.Unlock()
		return append([]string(nil), sent...)
	}
}

func TestAccountSyncMergesABurstIntoOneSignalAtEachEnd(t *testing.T) {
	sent := account_sync_record(t, 150*time.Millisecond)
	user := &User{UID: "u1"}
	for range 5 {
		account_sync_signal(user, "calendars")
	}
	time.Sleep(50 * time.Millisecond)
	if got := sent(); len(got) != 1 {
		t.Fatalf("at the start of a burst: %v, want one signal", got)
	}
	time.Sleep(250 * time.Millisecond)
	if got := sent(); len(got) != 2 {
		t.Fatalf("after the window: %v, want the burst's closing signal too", got)
	}
	time.Sleep(200 * time.Millisecond)
	account_sync_signal(user, "calendars")
	time.Sleep(50 * time.Millisecond)
	if got := sent(); len(got) != 3 {
		t.Fatalf("a change after a quiet window: %v, want it sent at once", got)
	}
}

func TestAccountSyncKeepsKindsAndUsersApart(t *testing.T) {
	sent := account_sync_record(t, time.Second)
	account_sync_signal(&User{UID: "u1"}, "calendars")
	account_sync_signal(&User{UID: "u1"}, "contacts")
	account_sync_signal(&User{UID: "u2"}, "calendars")
	time.Sleep(50 * time.Millisecond)
	if got := sent(); len(got) != 3 {
		t.Fatalf("sent %v, want one signal each", got)
	}
}

func TestAccountSyncRefusesAnUnknownKind(t *testing.T) {
	account_sync_record(t, time.Second)
	fn := sl.NewBuiltin("mochi.account.sync", api_account_sync)
	if _, err := api_account_sync(&sl.Thread{Name: "test"}, fn, sl.Tuple{sl.String("photos")}, nil); err == nil {
		t.Fatal("an unknown kind was accepted")
	}
	if _, err := api_account_sync(&sl.Thread{Name: "test"}, fn, sl.Tuple{sl.String("calendars")}, nil); err != nil {
		t.Fatalf("a known kind with no caller: %v", err)
	}
}
