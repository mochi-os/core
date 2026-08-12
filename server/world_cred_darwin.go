// Mochi server: macOS peer-credential check for the world-status UDS.
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

//go:build darwin

package main

import (
	"net"

	"golang.org/x/sys/unix"
)

// world_account_group is the group whose members may push world-server
// status; underscore-prefixed per the macOS service-account convention,
// matching _mochi.
const world_account_group = "_mochi-world"

// world_peer_authorized reads LOCAL_PEERCRED off a connected UnixConn and
// reports whether the peer may push world status: the mochi-world group
// (from the xucred group list), or anyone the ADMIN socket would accept.
func world_peer_authorized(c *net.UnixConn) (bool, *admin_cred) {
	raw, err := c.SyscallConn()
	if err != nil {
		return false, nil
	}
	var xucred *unix.Xucred
	var credErr error
	err = raw.Control(func(fd uintptr) {
		xucred, credErr = unix.GetsockoptXucred(int(fd), unix.SOL_LOCAL, unix.LOCAL_PEERCRED)
	})
	if err != nil || credErr != nil || xucred == nil {
		return false, nil
	}
	var gid uint32
	if xucred.Ngroups > 0 {
		gid = xucred.Groups[0]
	}
	cred := &admin_cred{uid: xucred.Uid, gid: gid, pid: 0}
	n := int(xucred.Ngroups)
	if n > len(xucred.Groups) {
		n = len(xucred.Groups)
	}
	if world_gid != 0 {
		for i := 0; i < n; i++ {
			if xucred.Groups[i] == world_gid {
				return true, cred
			}
		}
	}
	if admin_cred_basic_authorized(cred.uid, cred.gid) {
		return true, cred
	}
	if admin_mochi_gid != 0 {
		for i := 0; i < n; i++ {
			if xucred.Groups[i] == admin_mochi_gid {
				return true, cred
			}
		}
	}
	return false, cred
}
