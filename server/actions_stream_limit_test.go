// Mochi server: bounding the bytes a.write.stream will relay from a peer.
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"bytes"
	"io"
	"strings"
	"testing"
)

// stream_limit_copy relays through the limiter and reports what got through.
func stream_limit_copy(t *testing.T, body string, maximum int64) (string, bool) {
	t.Helper()
	limited := &stream_limit_reader{reader: strings.NewReader(body), remaining: maximum}
	var out bytes.Buffer
	if _, err := io.Copy(&out, limited); err != nil {
		t.Fatalf("copy returned %v", err)
	}
	return out.String(), limited.exceeded
}

// TestStreamLimitUnderCap is the ordinary case: an asset smaller than the cap is
// relayed whole and is not reported as curtailed.
func TestStreamLimitUnderCap(t *testing.T) {
	got, exceeded := stream_limit_copy(t, "a small avatar", 1024)

	if got != "a small avatar" {
		t.Errorf("relayed %q, want the whole body", got)
	}
	if exceeded {
		t.Errorf("a body well under the cap was reported as curtailed")
	}
}

// TestStreamLimitExactlyAtCap is the boundary that decides whether a legitimate
// asset of exactly the permitted size is served or rejected. It must be served,
// and must NOT be reported as curtailed - people's banner cap is 10MB, so a
// 10MB banner is allowed, not one byte too big.
func TestStreamLimitExactlyAtCap(t *testing.T) {
	body := strings.Repeat("x", 64)
	got, exceeded := stream_limit_copy(t, body, 64)

	if got != body {
		t.Errorf("relayed %d bytes, want all %d - a body exactly at the cap is legitimate", len(got), len(body))
	}
	if exceeded {
		t.Errorf("a body ending exactly at the cap was reported as curtailed, which would fail every maximum-size asset")
	}
}

// TestStreamLimitOverCap is the defect being fixed. Before this, io.Copy ran to
// whatever the peer chose to send.
func TestStreamLimitOverCap(t *testing.T) {
	body := strings.Repeat("x", 5000)
	got, exceeded := stream_limit_copy(t, body, 100)

	if len(got) > 100 {
		t.Errorf("relayed %d bytes, want no more than 100 - the cap did not stop the read", len(got))
	}
	if !exceeded {
		t.Errorf("an oversized body was not reported as curtailed, so a truncated asset would pass for a complete one")
	}
}

// TestStreamLimitStopsReadingFromThePeer is the point of the cap. Serving a
// truncated image is cosmetic; continuing to PULL an unbounded body is the cost.
// This counts what the far end was actually asked for.
func TestStreamLimitStopsReadingFromThePeer(t *testing.T) {
	// A reader that would happily supply bytes forever, as a hostile peer would.
	endless := &stream_counting_reader{}
	limited := &stream_limit_reader{reader: endless, remaining: 4096}

	written, err := io.Copy(io.Discard, limited)
	if err != nil {
		t.Fatalf("copy returned %v", err)
	}

	if written > 4096 {
		t.Errorf("relayed %d bytes from an endless peer, want at most 4096", written)
	}
	// The one extra byte is the probe that distinguishes "ended at the cap" from
	// "had more"; anything beyond that means we kept paying for the peer's data.
	if endless.supplied > 4096+1 {
		t.Errorf("read %d bytes from the peer, want at most 4097 - the cap must stop the transfer, not just the response", endless.supplied)
	}
	if !limited.exceeded {
		t.Errorf("an endless peer was not reported as curtailed")
	}
}

// stream_counting_reader supplies bytes without end and records how many were
// taken, standing in for a peer that ignores its own size limits.
type stream_counting_reader struct {
	supplied int64
}

func (r *stream_counting_reader) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = 'x'
	}
	r.supplied += int64(len(p))
	return len(p), nil
}

func TestStreamLimitDefaultClearsTheLargestLegitimateRelay(t *testing.T) {
	if stream_maximum_default != object_maximum {
		t.Errorf("default cap is %d, want object_maximum (%d): a repository archive or market download can be as large as the largest stored object",
			int64(stream_maximum_default), int64(object_maximum))
	}
}
