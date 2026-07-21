// Mochi server: git request body limit tests
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"bytes"
	"compress/gzip"
	"io"
	"testing"
)

// TestGitLimitedReaderStopsDecompressionBomb pins the bound on a gzipped git
// request body.
//
// git pack bodies are exempt from web_body_limit, so before this the
// compressed body and its decompressed expansion were both unbounded — a small
// gzip bomb from any client allowed to fetch (public repositories permit
// anonymous reads) expanded without limit. Same class as the zstd frame bomb
// on the peer protocol.
func TestGitLimitedReaderStopsDecompressionBomb(t *testing.T) {
	// 64 MB of zeros compresses to a few tens of KB.
	var compressed bytes.Buffer
	writer := gzip.NewWriter(&compressed)
	if _, err := io.Copy(writer, io.LimitReader(zero_reader{}, 64<<20)); err != nil {
		t.Fatalf("build payload: %v", err)
	}
	writer.Close()
	t.Logf("payload: %d compressed bytes expanding to %d", compressed.Len(), 64<<20)

	reader, err := gzip.NewReader(bytes.NewReader(compressed.Bytes()))
	if err != nil {
		t.Fatalf("open payload: %v", err)
	}
	const maximum = 1 << 20
	limited := &git_limited_reader{reader: reader, remaining: maximum}

	read, err := io.Copy(io.Discard, limited)
	if err == nil {
		t.Fatalf("decompression was not bounded: read %d bytes without error", read)
	}
	if read > maximum {
		t.Errorf("read %d bytes, more than the %d limit", read, maximum)
	}

	// A body inside the limit must still pass through untouched — the bound
	// must not truncate legitimate pushes.
	small := bytes.Repeat([]byte("x"), 1024)
	ok := &git_limited_reader{reader: bytes.NewReader(small), remaining: maximum}
	got, err := io.ReadAll(ok)
	if err != nil {
		t.Fatalf("a body within the limit was rejected: %v", err)
	}
	if !bytes.Equal(got, small) {
		t.Errorf("body within the limit was altered: got %d bytes, want %d", len(got), len(small))
	}
}

// TestGitRequestMaximumByService pins that negotiation and content bodies get
// different ceilings: an upload-pack body is never stored, a receive-pack body
// becomes repository content.
func TestGitRequestMaximumByService(t *testing.T) {
	if got := git_request_maximum("git-upload-pack", nil); got != git_negotiation_maximum {
		t.Errorf("upload-pack maximum = %d, want %d", got, git_negotiation_maximum)
	}
	// With no measurable quota the receive-pack ceiling must still be finite
	// and positive rather than falling open.
	got := git_request_maximum("git-receive-pack", nil)
	if got <= 0 {
		t.Errorf("receive-pack maximum = %d, want a positive finite ceiling", got)
	}
	if got > file_max_storage {
		t.Errorf("receive-pack maximum = %d, above the per-user storage ceiling %d", got, file_max_storage)
	}
}

type zero_reader struct{}

func (zero_reader) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = 0
	}
	return len(p), nil
}
