// Mochi server: Windows service log tests
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the Mochi
// Application Interface Exception - see license.txt and license-exception.md.
//
// The service file only compiles on Windows, so its log redirect is pinned by
// reading the source.

package main

import (
	"os"
	"strings"
	"testing"
)

// TestWindowsServiceLogKeepsTheTimestampWriter. A log.SetOutput(f) in the
// redirect replaced log_writer, whose Write stamps each line with the time, so
// every line of the service log was undated. Assigning os.Stdout is the whole
// redirect: log_writer writes through fmt.Print, which reads it at call time.
func TestWindowsServiceLogKeepsTheTimestampWriter(t *testing.T) {
	data, err := os.ReadFile("service_windows.go")
	if err != nil {
		t.Fatal(err)
	}
	var code []string
	for _, line := range strings.Split(string(data), "\n") {
		if !strings.HasPrefix(strings.TrimSpace(line), "//") {
			code = append(code, line)
		}
	}
	source := strings.Join(code, "\n")
	if strings.Contains(source, "log.SetOutput(") {
		t.Errorf("the service redirect replaces the timestamping log writer")
	}
	if !strings.Contains(source, "os.Stdout = f") {
		t.Errorf("the service redirect no longer reassigns os.Stdout")
	}
}
