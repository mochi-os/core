// Mochi server: Linux peer-credential check for the admin UDS.
// Copyright © 2026 Mochi OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.
//
// On Linux the connected peer's identity is read with SO_PEERCRED, which
// reports uid, gid, and pid. SO_PEERCRED gives only the primary gid, so the
// supplementary-group fallback reads /proc/<pid>/status to see the full
// Groups: list.

//go:build linux

package main

import (
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"

	"golang.org/x/sys/unix"
)

// admin_account is the unprivileged user/group the server runs as; the admin
// socket is owned by it and peers running as it are authorized.
const admin_account = "mochi"

// admin_peer_authorized reads SO_PEERCRED off a connected UnixConn and reports
// whether the peer clears the admin gate: the mochi user, root, or a member of
// the mochi group (primary or supplementary).
func admin_peer_authorized(c *net.UnixConn) (bool, *admin_cred) {
	raw, err := c.SyscallConn()
	if err != nil {
		return false, nil
	}
	var ucred *unix.Ucred
	var credErr error
	err = raw.Control(func(fd uintptr) {
		ucred, credErr = unix.GetsockoptUcred(int(fd), unix.SOL_SOCKET, unix.SO_PEERCRED)
	})
	if err != nil || credErr != nil || ucred == nil {
		return false, nil
	}
	cred := &admin_cred{uid: ucred.Uid, gid: ucred.Gid, pid: ucred.Pid}
	if admin_cred_basic_authorized(cred.uid, cred.gid) {
		return true, cred
	}
	if admin_mochi_gid != 0 && admin_pid_in_group(int(cred.pid), admin_mochi_gid) {
		return true, cred
	}
	return false, cred
}

// admin_pid_in_group reports whether the given process is a member of gid,
// including supplementary groups, by reading /proc/<pid>/status.
func admin_pid_in_group(pid int, gid uint32) bool {
	data, err := os.ReadFile(fmt.Sprintf("/proc/%d/status", pid))
	if err != nil {
		return false
	}
	for _, line := range strings.Split(string(data), "\n") {
		if !strings.HasPrefix(line, "Groups:") {
			continue
		}
		for _, s := range strings.Fields(strings.TrimPrefix(line, "Groups:")) {
			if g, err := strconv.ParseUint(s, 10, 32); err == nil && uint32(g) == gid {
				return true
			}
		}
	}
	return false
}
