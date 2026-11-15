// Mochi server: resuming an interrupted release download
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// download_cut is a body that ends in an error after its bytes, as a stream
// that breaks part way does.
type download_cut struct {
	data io.Reader
}

func (c download_cut) Read(p []byte) (int, error) {
	n, err := c.data.Read(p)
	if err == io.EOF {
		return n, errors.New("stream broken")
	}
	return n, err
}

func download_read(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading %s: %v", path, err)
	}
	return string(data)
}

// A download cut part way keeps what arrived; the next attempt asks for the
// rest, and with the offset echoed appends only the tail.
func TestDownloadResumesFromWhatArrived(t *testing.T) {
	zip := filepath.Join(t.TempDir(), "tmp", "app_x_1.0.zip")
	release := "0123456789abcdef"

	if app_download_receive(zip, 0, map[string]any{"status": "200"}, download_cut{strings.NewReader(release[:6])}) {
		t.Fatal("a broken body was reported complete")
	}
	offset := app_download_offset(zip + ".partial")
	if offset != 6 {
		t.Fatalf("THE POINT: the interrupted download kept %d bytes to resume from, want 6", offset)
	}
	if !app_download_receive(zip, offset, map[string]any{"status": "200", "offset": uint64(offset)}, strings.NewReader(release[6:])) {
		t.Fatal("the resumed download failed")
	}
	if got := download_read(t, zip); got != release {
		t.Errorf("THE POINT: the resumed release is %q, want %q", got, release)
	}
	if _, err := os.Stat(zip + ".partial"); !os.IsNotExist(err) {
		t.Error("the partial is still there after it became the release")
	}
}

// An answer that echoes no offset is the whole file - an older publisher, or
// one that could not resume - and replaces what the partial held.
func TestDownloadStartsAgainWithoutTheEcho(t *testing.T) {
	zip := filepath.Join(t.TempDir(), "app_x_1.0.zip")
	if err := os.WriteFile(zip+".partial", []byte("stale"), 0o644); err != nil {
		t.Fatal(err)
	}
	if !app_download_receive(zip, 5, map[string]any{"status": "200"}, strings.NewReader("whole release")) {
		t.Fatal("the download failed")
	}
	if got := download_read(t, zip); got != "whole release" {
		t.Errorf("THE POINT: with no echo the release is %q, want the whole file alone", got)
	}
}

// An echo of an offset other than the one asked for is a body this partial
// cannot take: nothing is installed and the partial goes, so the next attempt
// starts over.
func TestDownloadRefusesAnotherOffset(t *testing.T) {
	zip := filepath.Join(t.TempDir(), "app_x_1.0.zip")
	if err := os.WriteFile(zip+".partial", []byte("01234"), 0o644); err != nil {
		t.Fatal(err)
	}
	if app_download_receive(zip, 5, map[string]any{"status": "200", "offset": int64(3)}, strings.NewReader("3456789")) {
		t.Error("THE POINT: a body sent from another offset was accepted")
	}
	if _, err := os.Stat(zip); !os.IsNotExist(err) {
		t.Error("a release was written from a mismatched body")
	}
	if app_download_offset(zip+".partial") != 0 {
		t.Error("the partial survived a mismatched answer, so the next attempt would resume a broken file")
	}
}

// A body shorter than the size the publisher named is kept to resume from,
// not installed: a publisher whose send fails ends the stream cleanly. One
// longer than it is thrown away.
func TestDownloadKeepsAShortBodyToResume(t *testing.T) {
	zip := filepath.Join(t.TempDir(), "app_x_1.0.zip")
	if app_download_receive(zip, 0, map[string]any{"status": "200", "size": uint64(16)}, strings.NewReader("0123456")) {
		t.Fatal("THE POINT: a body of 7 of 16 bytes ending cleanly was taken for the whole release")
	}
	if app_download_offset(zip+".partial") != 7 {
		t.Errorf("the short body was not kept to resume from: %d bytes", app_download_offset(zip+".partial"))
	}
	if !app_download_receive(zip, 7, map[string]any{"status": "200", "offset": uint64(7), "size": uint64(16)}, strings.NewReader("789abcdef")) {
		t.Fatal("the rest of the release was not taken")
	}
	if got := download_read(t, zip); got != "0123456789abcdef" {
		t.Errorf("the release is %q", got)
	}
	long := filepath.Join(t.TempDir(), "app_y_1.0.zip")
	if app_download_receive(long, 0, map[string]any{"status": "200", "size": uint64(4)}, strings.NewReader("too long")) {
		t.Error("a body longer than the named size was taken")
	}
	if app_download_offset(long+".partial") != 0 {
		t.Error("a body longer than the named size was kept")
	}
}
