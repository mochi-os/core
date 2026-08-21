// Mochi server: Linux peer-credential check for the world-status UDS.
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

//go:build linux

package main

import (
	"net"

	"golang.org/x/sys/unix"
)

// world_account_group is the group whose members may push world-server
// status. The mochi-world package's postinst puts its service user here.
const world_account_group = "mochi-world"

// world_peer_authorized reads SO_PEERCRED off a connected UnixConn and reports
// whether the peer may push world status: the mochi-world group (primary or
// supplementary), or anyone the admin socket accepts - root, mochi user, mochi
// group.
func world_peer_authorized(c *net.UnixConn) (bool, *admin_credential) {
	raw, err := c.SyscallConn()
	if err != nil {
		return false, nil
	}
	var ucred *unix.Ucred
	var credential_error error
	err = raw.Control(func(fd uintptr) {
		ucred, credential_error = unix.GetsockoptUcred(int(fd), unix.SOL_SOCKET, unix.SO_PEERCRED)
	})
	if err != nil || credential_error != nil || ucred == nil {
		return false, nil
	}
	credential := &admin_credential{uid: ucred.Uid, gid: ucred.Gid, pid: ucred.Pid}
	if world_gid != 0 && (credential.gid == world_gid || admin_pid_in_group(int(credential.pid), world_gid)) {
		return true, credential
	}
	if admin_credential_basic_authorized(credential.uid, credential.gid) {
		return true, credential
	}
	if admin_mochi_gid != 0 && admin_pid_in_group(int(credential.pid), admin_mochi_gid) {
		return true, credential
	}
	return false, credential
}
