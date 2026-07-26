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

	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/storer"
	"github.com/go-git/go-git/v5/plumbing/transport"
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
	// A large remaining quota must not raise the negotiation ceiling.
	if got := git_request_maximum("git-upload-pack", file_max_storage); got != git_negotiation_maximum {
		t.Errorf("upload-pack maximum = %d, want %d", got, git_negotiation_maximum)
	}
	// With no measurable quota the receive-pack ceiling must still be finite
	// and positive rather than falling open.
	got := git_request_maximum("git-receive-pack", 0)
	if got <= 0 {
		t.Errorf("receive-pack maximum = %d, want a positive finite ceiling", got)
	}
	if got > file_max_storage {
		t.Errorf("receive-pack maximum = %d, above the per-user storage ceiling %d", got, file_max_storage)
	}
	// A measured quota becomes the ceiling.
	if got := git_request_maximum("git-receive-pack", 4096); got != 4096 {
		t.Errorf("receive-pack maximum = %d, want the measured budget 4096", got)
	}
}

// git_storage_budget must not dereference a missing owner, and must report
// "no room" rather than falling open.
func TestGitStorageBudgetWithoutOwner(t *testing.T) {
	if got := git_storage_budget(nil); got != 0 {
		t.Errorf("git_storage_budget(nil) = %d, want 0", got)
	}
}

type zero_reader struct{}

func (zero_reader) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = 0
	}
	return len(p), nil
}

// The service decides whether a git request is authorised as a read or a
// write. Taking it from the caller-controlled ?service= query let
// POST .../git-receive-pack?service=git-upload-pack be authorised as a read
// and then dispatched as a push, so info/refs - the one endpoint that may read
// it - whitelists the value first.
func TestGitServiceNameWhitelist(t *testing.T) {
	for _, service := range []string{"git-upload-pack", "git-receive-pack"} {
		if got := git_service_name(service); got != service {
			t.Errorf("git_service_name(%q) = %q, want %q", service, got, service)
		}
	}
	for _, service := range []string{"", "git-receive-pack ", "GIT-RECEIVE-PACK", "../git-receive-pack", "git-upload-archive", "receive-pack"} {
		if got := git_service_name(service); got != "" {
			t.Errorf("git_service_name(%q) = %q, want it rejected", service, got)
		}
	}
}

// A pack stores its objects deltified against each other, so bounding the
// request body does not bound what the body decodes to. The storer meters the
// decoded objects and fails the push before any ref is updated.
func TestGitStorageMetersDecodedObjects(t *testing.T) {
	user, _, cleanup := create_git_test_env(t)
	defer cleanup()

	if err := git_init(user, test_app, "repo1"); err != nil {
		t.Fatalf("git_init: %v", err)
	}
	endpoint := &transport.Endpoint{Path: git_repo_path(user, test_app, "repo1")}

	store := func(s storer.Storer, size int) error {
		obj := s.NewEncodedObject()
		obj.SetType(plumbing.BlobObject)
		w, err := obj.Writer()
		if err != nil {
			return err
		}
		w.Write(make([]byte, size))
		w.Close()
		_, err = s.SetEncodedObject(obj)
		return err
	}

	metered, err := (&git_loader{budget: 1000}).Load(endpoint)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if err := store(metered, 400); err != nil {
		t.Errorf("storing 400 bytes against a 1000 byte budget = %v, want success", err)
	}
	if err := store(metered, 400); err != nil {
		t.Errorf("storing a second 400 bytes against a 1000 byte budget = %v, want success", err)
	}
	// Cumulative, not per object: 400+400+400 exceeds 1000.
	if err := store(metered, 400); err == nil {
		t.Error("storing past the budget succeeded, want refusal")
	}

	// A single object larger than the whole budget is refused outright.
	tight, err := (&git_loader{budget: 100}).Load(endpoint)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if err := store(tight, 4096); err == nil {
		t.Error("storing an object larger than the budget succeeded, want refusal")
	}

	// Fetches and ref advertisements store nothing, so their loader is
	// unmetered and must not start refusing objects.
	unmetered, err := (&git_loader{}).Load(endpoint)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if err := store(unmetered, 1<<20); err != nil {
		t.Errorf("unmetered store = %v, want success", err)
	}
}
