// Mochi server: Windows-specific helpers for the self-install path.
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the Mochi
// Application Interface Exception - see license.txt and license-exception.md.
//
// Spawns a detached cmd.exe that settles, then runs msiexec on the downloaded
// MSI. CREATE_BREAKAWAY_FROM_JOB is required: without it the service's own exit
// tears down the job object and kills msiexec with it. SysProcAttr.CmdLine is
// set explicitly because Go's arg escaper emits \" pairs that cmd.exe passes
// through, leaving msiexec a path with literal quotes it cannot open.

//go:build windows

package main

import (
	"fmt"
	"os/exec"
	"strconv"
	"syscall"
)

// Process-creation flags not exported by the standard syscall package;
// define from the documented Win32 values.
// https://learn.microsoft.com/en-us/windows/win32/procthread/process-creation-flags
const (
	win_detached_process          = 0x00000008
	win_create_breakaway_from_job = 0x01000000
)

func update_install_spawn(msi_path, msi_log string) error {
	cmd_line := `cmd /c ping -n ` + strconv.Itoa(update_install_pre_wait+1) +
		` 127.0.0.1 > NUL & msiexec /i "` + msi_path +
		`" /quiet /norestart /l*v "` + msi_log + `"`

	cmd := exec.Command("cmd")
	cmd.Stdin = nil
	cmd.Stdout = nil
	cmd.Stderr = nil
	cmd.SysProcAttr = &syscall.SysProcAttr{
		CmdLine: cmd_line,
		CreationFlags: win_detached_process |
			syscall.CREATE_NEW_PROCESS_GROUP |
			win_create_breakaway_from_job,
	}
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("start cmd.exe: %v", err)
	}
	// Release the Go-side os.Process so the service exit doesn't wait
	// on a Wait() call we're never going to make.
	if err := cmd.Process.Release(); err != nil {
		warn("Server update: cmd.Process.Release: %v", err)
	}
	return nil
}
