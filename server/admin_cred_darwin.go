// Mochi server: macOS peer-credential check for the admin UDS.
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.
//
// macOS has no SO_PEERCRED. The equivalent is LOCAL_PEERCRED, which returns an
// xucred carrying the peer's uid and its group list (primary group first, up
// to 16 entries). There is no pid, so admin_cred.pid stays 0. The xucred group
// list gives the supplementary-group membership directly, so no /proc-style
// lookup is needed.

//go:build darwin

package main

import (
	"net"

	"golang.org/x/sys/unix"
)

// admin_account is the unprivileged user/group the server runs as; macOS
// daemon accounts are conventionally underscore-prefixed (see the _mochi
// account created by the .pkg preinstall). Note that under the static
// (CGO_ENABLED=0) darwin build os/user cannot query OpenDirectory, so this
// lookup is best-effort; the root/owner authorization path does not depend on
// it.
const admin_account = "_mochi"

// admin_peer_authorized reads LOCAL_PEERCRED off a connected UnixConn and
// reports whether the peer clears the admin gate: the mochi user, root, or a
// member of the mochi group (primary or supplementary, from the xucred group
// list).
func admin_peer_authorized(c *net.UnixConn) (bool, *admin_cred) {
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
	if admin_cred_basic_authorized(cred.uid, cred.gid) {
		return true, cred
	}
	if admin_mochi_gid != 0 {
		n := int(xucred.Ngroups)
		if n > len(xucred.Groups) {
			n = len(xucred.Groups)
		}
		for i := 0; i < n; i++ {
			if xucred.Groups[i] == admin_mochi_gid {
				return true, cred
			}
		}
	}
	return false, cred
}
