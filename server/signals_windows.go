// Mochi server: signals on Windows (interrupt only — no SIGHUP).
// Copyright © 2026 Mochi OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

//go:build windows

package main

import "os"

func extra_signals() []os.Signal { return nil }

func is_ignorable_signal(s os.Signal) bool { return false }
