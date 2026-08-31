// Mochi server: the Windows data directory inherits %ProgramData%'s permissive
// ACL, so its private keys would otherwise be readable by every local account.
//
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the Mochi
// Application Interface Exception - see license.txt and license-exception.md.

//go:build windows

package main

import (
	"fmt"

	"golang.org/x/sys/windows"
)

// Protected DACL, full control for SYSTEM and the Administrators group only,
// inherited by every file and directory beneath. PAI stops the permissive
// %ProgramData% ACE (BUILTIN\Users: read + create) from flowing back in.
const data_directory_descriptor = "D:PAI(A;OICI;FA;;;SY)(A;OICI;FA;;;BA)"

// directories_secure locks the data directory down to SYSTEM and
// Administrators.
//
// The installer cannot do this: the MSI is built with wixl, which rejects both
// <Permission> and the WiX-only <PermissionEx>. Doing it here also repairs
// installations that predate the fix, which an installer change never would.
// This is the Windows counterpart of the UMask=0077 / chmod go-rwx the Linux
// packaging applies to the same tree.
func directories_secure(dir string) error {
	if dir == "" {
		return nil
	}
	descriptor, err := windows.SecurityDescriptorFromString(data_directory_descriptor)
	if err != nil {
		return fmt.Errorf("parse data directory security descriptor: %w", err)
	}
	dacl, _, err := descriptor.DACL()
	if err != nil {
		return fmt.Errorf("read data directory DACL: %w", err)
	}
	err = windows.SetNamedSecurityInfo(
		dir,
		windows.SE_FILE_OBJECT,
		windows.DACL_SECURITY_INFORMATION|windows.PROTECTED_DACL_SECURITY_INFORMATION,
		nil, nil, dacl, nil,
	)
	if err != nil {
		return fmt.Errorf("set data directory DACL on %s: %w", dir, err)
	}
	return nil
}
