// Mochi server: Windows-specific helpers for the self-install path.
// Copyright Alistair Cunningham 2026
//
// Configures the spawned msiexec wrapper so it survives the service's
// own exit. Without these flags the child cmd.exe is attached to the
// mochi-server service's Windows job object — when the service exits
// to make way for the upgrade, the OS tears down the job and kills
// every descendant, including the ping/msiexec pipeline that hasn't
// fired yet. CREATE_BREAKAWAY_FROM_JOB lets the new process leave the
// job; DETACHED_PROCESS suppresses the inherited console;
// CREATE_NEW_PROCESS_GROUP isolates the upgrader from any Ctrl-C tree
// the service might still own at exit time.
//
// Service jobs created by the SCM have JOB_OBJECT_LIMIT_BREAKAWAY_OK
// set by default, so the breakaway request succeeds without
// reconfiguration.

//go:build windows

package main

import (
	"os/exec"
	"syscall"
)

// Process-creation flags not exported by the standard syscall package;
// define from the documented Win32 values.
// https://learn.microsoft.com/en-us/windows/win32/procthread/process-creation-flags
const (
	win_detached_process          = 0x00000008
	win_create_breakaway_from_job = 0x01000000
)

func update_install_detach(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{
		CreationFlags: win_detached_process |
			syscall.CREATE_NEW_PROCESS_GROUP |
			win_create_breakaway_from_job,
	}
}
