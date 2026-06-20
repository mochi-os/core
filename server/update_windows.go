// Mochi server: Windows-specific helpers for the self-install path.
// Copyright © 2026 Mochi OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.
//
// Spawns the detached cmd.exe wrapper that runs ping (for a settling
// delay) and then msiexec to install the downloaded MSI. Two Windows
// gotchas are handled here together because they're inseparable:
//
// 1. CREATE_BREAKAWAY_FROM_JOB lets the spawned cmd.exe escape the
//    mochi-server service's Windows job object. Without it, the
//    service's own exit (a few milliseconds after we return) tears
//    down the job and kills cmd.exe, ping, and msiexec before
//    msiexec has a chance to run. Service jobs created by the SCM
//    have JOB_OBJECT_LIMIT_BREAKAWAY_OK set by default, so the
//    breakaway request succeeds without reconfiguration.
//    DETACHED_PROCESS suppresses the inherited console;
//    CREATE_NEW_PROCESS_GROUP isolates the upgrader from any Ctrl-C
//    tree the service might still own at exit time.
//
// 2. SysProcAttr.CmdLine is set explicitly rather than letting Go
//    build it from Args. Go's standard arg-escaper wraps the whole
//    "ping ... & msiexec /i "<path>" ..." string in outer quotes and
//    rewrites every internal " as \" — the convention required by
//    MSVCRT-style argv parsing. cmd.exe doesn't follow MSVCRT: it
//    strips the outer quotes (because the command line contains the
//    & special character, hitting cmd's "old behavior" rule) but
//    leaves the inner \" pairs untouched. When the resulting command
//    line reaches msiexec, MSVCRT parses each \" as a literal " (not
//    a delimiter), so argv[2] arrives as `"C:\...\file.msi"` with
//    literal quote characters inside the path — msiexec then can't
//    find the file and the upgrade silently fails. The fix is to do
//    the quoting ourselves: cmd.exe sees "<path>" as proper quoted
//    text, the quotes act as delimiters when msiexec reads its argv,
//    and the path arrives clean. The Go stdlib documents this
//    exact failure mode for cmd.exe and msiexec.exe at exec.go:394.
//
// Duc reported the symptom on 0.4.73 → 0.4.76: API call returned
// success, the service stopped (we asked it to), but the MSI never
// installed and on manual restart the version was still 0.4.73.

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
