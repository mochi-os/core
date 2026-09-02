// Mochi server: who may connect to the admin socket.
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the Mochi
// Application Interface Exception - see license.txt and license-exception.md.
//
// The peer gate decides who reaches stop, restart, backup export and pprof,
// and nothing pinned it.

//go:build linux

package main

import (
	"os"
	"os/user"
	"strconv"
	"testing"
)

// TestAdminCredentialBasicAuthorized: root, the mochi user and a member of
// the mochi primary group pass; gid 0 does not pass while the group is
// unresolved; anyone else is refused.
func TestAdminCredentialBasicAuthorized(t *testing.T) {
	uid, gid := admin_mochi_uid, admin_mochi_gid
	t.Cleanup(func() { admin_mochi_uid, admin_mochi_gid = uid, gid })

	admin_mochi_uid, admin_mochi_gid = 1234, 5678
	cases := []struct {
		name     string
		uid, gid uint32
		want     bool
	}{
		{"root", 0, 0, true},
		{"the mochi user", 1234, 100, true},
		{"the mochi primary group", 4321, 5678, true},
		{"a stranger", 4321, 100, false},
		{"a stranger in gid 0", 4321, 0, false},
	}
	for _, c := range cases {
		if got := admin_credential_basic_authorized(c.uid, c.gid); got != c.want {
			t.Errorf("%s (uid %d gid %d): got %v, want %v", c.name, c.uid, c.gid, got, c.want)
		}
	}

	// With no mochi group resolved, gid 0 must not be treated as a match.
	admin_mochi_gid = 0
	if admin_credential_basic_authorized(4321, 0) {
		t.Errorf("gid 0 passed while the mochi group is unresolved")
	}
}

// TestAdminGroupsParsesTheStatusLine pins the /proc/<pid>/status shape the
// supplementary-group check depends on.
func TestAdminGroupsParsesTheStatusLine(t *testing.T) {
	status := "Name:\tmochictl\nUid:\t1000\t1000\t1000\t1000\nGid:\t1000\t1000\t1000\t1000\nGroups:\t4 24 1000 \nNSpgid:\t7\n"
	got := admin_groups(status)
	want := []uint32{4, 24, 1000}
	if len(got) != len(want) {
		t.Fatalf("groups = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("groups = %v, want %v", got, want)
		}
	}
	if len(admin_groups("Name:\tx\n")) != 0 {
		t.Errorf("a status with no Groups line yielded groups")
	}
}

// TestAdminPidInGroupReadsTheLiveProcess: the running test process is in
// exactly the groups the kernel reports for it.
func TestAdminPidInGroupReadsTheLiveProcess(t *testing.T) {
	groups, err := os.Getgroups()
	if err != nil {
		t.Fatal(err)
	}
	present := map[uint32]bool{}
	for _, g := range groups {
		present[uint32(g)] = true
		if !admin_pid_in_group(os.Getpid(), uint32(g)) {
			t.Errorf("gid %d is one of this process's groups but was not found", g)
		}
	}
	absent := uint32(65533)
	for present[absent] {
		absent--
	}
	if admin_pid_in_group(os.Getpid(), absent) {
		t.Errorf("gid %d is not one of this process's groups but was found", absent)
	}
	if admin_pid_in_group(1<<30, 0) {
		t.Errorf("a process that does not exist was found in a group")
	}
}

// TestAdminResolveCredsFallsBackToTheEffectiveUser: with no mochi account on
// the host (development), the server's own effective uid is the admin uid.
func TestAdminResolveCredsFallsBackToTheEffectiveUser(t *testing.T) {
	admin_resolve_creds()
	want := uint32(os.Geteuid())
	if account, err := user.Lookup(admin_account); err == nil {
		if uid, err := strconv.ParseUint(account.Uid, 10, 32); err == nil {
			want = uint32(uid)
		}
	}
	if admin_mochi_uid != want {
		t.Errorf("admin uid resolved to %d, want %d", admin_mochi_uid, want)
	}
}
