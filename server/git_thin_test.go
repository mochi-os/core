// Mochi server: git smart-HTTP thin packs.
//
// A pack may only deltify an object against a base it also carries, so before
// this every changed file travelled in full however small the edit. thin-pack
// lifts that: the base can be an object the client already holds, named by hash
// in a REF_DELTA, and the client resolves it from its own store.
//
// go-git's encoder cannot produce one - writeBaseIfDelta puts every base in the
// pack, and the entry point that takes pre-built deltas is unexported - so the
// pack written here is ours, and these assert both halves of that: that it
// saves what it is supposed to save, and that real git can read it.
//
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"bytes"
	"errors"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/format/packfile"
	"github.com/go-git/go-git/v5/plumbing/protocol/packp/capability"
	"github.com/go-git/go-git/v5/plumbing/transport"
)

// git_thin_payload is a large, deliberately incompressible blob. Random bytes
// mean the saving measured below comes from the delta and not from zlib.
func git_thin_payload(seed int64, size int) []byte {
	payload := make([]byte, size)
	rand.New(rand.NewSource(seed)).Read(payload)
	return payload
}

// git_thin_repo builds a repository whose second commit makes a small edit to a
// large file - the case a thin pack exists for.
func git_thin_repo(t *testing.T, user *User, repo_id string) (string, plumbing.Hash, plumbing.Hash) {
	t.Helper()
	if err := git_init(user, test_app, repo_id); err != nil {
		t.Fatalf("git_init: %v", err)
	}
	repo_path := git_repo_path(user, test_app, repo_id)

	work := git_work_open(t, "git_thin_work")
	payload := git_thin_payload(7, 4<<20)
	if err := os.WriteFile(filepath.Join(work.dir, "large.bin"), payload, 0644); err != nil {
		t.Fatalf("write: %v", err)
	}
	first := work.commit("first")

	// One short run of bytes changes in the middle; everything else is
	// identical, so a delta against the held copy is tiny and a full copy is
	// four megabytes.
	copy(payload[2<<20:], git_thin_payload(9, 512))
	if err := os.WriteFile(filepath.Join(work.dir, "large.bin"), payload, 0644); err != nil {
		t.Fatalf("write: %v", err)
	}
	second := work.commit("second")

	work.run("push", repo_path, "main")
	return repo_path, first, second
}

// TestGitFetchThinPackShrinksAModifiedFile — the measurement. A client that
// already holds the previous version of a large file must be sent the
// difference, not the file.
func TestGitFetchThinPackShrinksAModifiedFile(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git binary not available")
	}

	user, _, cleanup := create_git_test_env(t)
	defer cleanup()

	repo_path, first, second := git_thin_repo(t, user, "thin")

	whole := git_negotiation_response(t, repo_path, second, []plumbing.Hash{first})
	thin := git_negotiation_response(t, repo_path, second, []plumbing.Hash{first}, "thin-pack")

	t.Logf("a 512-byte edit to a 4 MiB file: %d bytes without thin-pack, %d with", len(whole), len(thin))

	if len(thin) >= len(whole) {
		t.Fatalf("the thin pack is %d bytes against %d: it is not deltifying against what the client holds",
			len(thin), len(whole))
	}
	// The whole file is four megabytes of random data, so it cannot compress;
	// the delta is a few hundred bytes of change plus copy instructions.
	if len(thin) > len(whole)/100 {
		t.Errorf("the thin pack is %d bytes for a 512-byte edit to a 4 MiB file (whole pack %d): "+
			"the delta base chosen is a poor one", len(thin), len(whole))
	}
}

// TestGitFetchThinPackNeverLargerThanWhole — thin deltifies against the
// client's objects but not between the pack's own, and the ordinary encoder
// does the opposite, so neither wins every time. A fetch of entirely new files
// has nothing on the client's side to delta against, and must not come out
// worse for having asked.
func TestGitFetchThinPackNeverLargerThanWhole(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git binary not available")
	}

	user, _, cleanup := create_git_test_env(t)
	defer cleanup()

	if err := git_init(user, test_app, "thinnew"); err != nil {
		t.Fatalf("git_init: %v", err)
	}
	repo_path := git_repo_path(user, test_app, "thinnew")

	work := git_work_open(t, "git_thin_new")
	first := work.commit("first")
	// Several new files, each a small variation on the last, so the ordinary
	// encoder's window has plenty to work with and the client has nothing.
	payload := git_thin_payload(11, 512<<10)
	for i := 0; i < 6; i++ {
		payload[i*1024] ^= 0xff
		name := filepath.Join(work.dir, "new"+strconv.Itoa(i)+".bin")
		if err := os.WriteFile(name, payload, 0644); err != nil {
			t.Fatalf("write: %v", err)
		}
	}
	second := work.commit("second")
	work.run("push", repo_path, "main")

	whole := git_negotiation_response(t, repo_path, second, []plumbing.Hash{first})
	thin := git_negotiation_response(t, repo_path, second, []plumbing.Hash{first}, "thin-pack")

	t.Logf("six new near-identical files: %d bytes without thin-pack, %d with", len(whole), len(thin))
	if len(thin) > len(whole) {
		t.Errorf("asking for a thin pack made the response %d bytes against %d: "+
			"the smaller of the two candidates is not being chosen", len(thin), len(whole))
	}
}

// TestGitPackMemoryIsBounded — comparing a thin pack against a whole one means
// holding both in memory, and the object-count ceiling bounds neither: one
// large blob is one object. A client fetching after a long absence is sent most
// of the repository, so the comparison has to give up rather than allocate it
// twice.
func TestGitPackMemoryIsBounded(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git binary not available")
	}

	user, _, cleanup := create_git_test_env(t)
	defer cleanup()

	repo_path, first, second := git_thin_repo(t, user, "thinmemory")
	storage, err := (&git_loader{}).Load(&transport.Endpoint{Path: repo_path})
	if err != nil {
		t.Fatalf("load: %v", err)
	}

	capabilities := capability.NewList()
	if err := capabilities.Add(capability.ThinPack); err != nil {
		t.Fatalf("capabilities: %v", err)
	}
	selection, err := git_upload_pack_select(storage, &git_request{
		capabilities: capabilities,
		wants:        []plumbing.Hash{second},
		done:         true,
	}, []plumbing.Hash{first}, true)
	if err != nil {
		t.Fatalf("select: %v", err)
	}

	// A limit between the two candidates: the whole pack carries a 4 MiB
	// incompressible blob, the thin one is about a kilobyte.
	original := git_pack_memory_maximum
	git_pack_memory_maximum = 1 << 20
	defer func() { git_pack_memory_maximum = original }()

	if _, err := git_pack_plain(storage, selection.objects); !errors.Is(err, git_pack_oversize) {
		t.Errorf("a 4 MiB pack against a 1 MiB limit gave %v, want it refused", err)
	}

	// The chooser must still answer, and answer with the thin pack: a whole
	// pack that did not fit is by definition larger than a thin one that did.
	// This is exactly the case thin-pack exists for, so falling back to
	// streaming the whole pack here would give up the entire saving.
	chosen := git_upload_pack_candidate(storage, selection)
	if chosen == nil {
		t.Fatal("no candidate was chosen, so the fetch would stream the 4 MiB pack instead of the 1 KiB one")
	}
	if len(chosen) > 4096 {
		t.Errorf("the chosen pack is %d bytes; the thin one was a little over a kilobyte", len(chosen))
	}

	// And with the limit below both, the fetch gives up on comparing and
	// streams rather than allocating either.
	git_pack_memory_maximum = 512
	if chosen := git_upload_pack_candidate(storage, selection); chosen != nil {
		t.Errorf("a %d byte candidate was built against a 512 byte limit", len(chosen))
	}
}

// TestGitPackIsDeterministic — the same fetch twice has to produce the same
// bytes. revlist returns its objects by iterating a Go map, so the pack was
// built in a different order on every request; two identical fetches differed
// by a few dozen bytes, which is enough to make any measurement of pack size -
// including the one the thin/whole choice above is decided by - noise.
func TestGitPackIsDeterministic(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git binary not available")
	}

	user, _, cleanup := create_git_test_env(t)
	defer cleanup()

	repo_path, commits := git_negotiation_repo(t, user, "deterministic", 12)
	head := commits[len(commits)-1]

	first := git_negotiation_response(t, repo_path, head, commits[:4])
	second := git_negotiation_response(t, repo_path, head, commits[:4])
	if !bytes.Equal(first, second) {
		t.Errorf("two identical fetches produced %d and %d bytes: the object order is not stable",
			len(first), len(second))
	}
}

// TestGitFetchThinPackAppliedByGit — the pack writer is ours, so the test that
// matters is whether real git accepts it. index-pack checks the trailing
// checksum, every entry header, and resolves each REF_DELTA against the
// client's own store; fsck then walks the result.
func TestGitFetchThinPackAppliedByGit(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git binary not available")
	}

	user, _, cleanup := create_git_test_env(t)
	defer cleanup()

	repo_path, _, _ := git_thin_repo(t, user, "thinapply")
	server := git_negotiation_server(t, repo_path)

	for _, version := range git_versions {
		t.Run("protocol v"+version, func(t *testing.T) {
			// Clone at the first commit only, so the second arrives by fetch -
			// which is where git asks for a thin pack.
			dir := git_temporary(t, "git_thin_clone")
			git_run(t, "", git_protocol(version, "clone", "--quiet", server.URL, dir)...)
			git_run(t, dir, "-C", dir, "reset", "--hard", "-q", "HEAD~1")
			git_run(t, dir, "-C", dir, "update-ref", "refs/heads/main", "HEAD")
			git_run(t, dir, "-C", dir, "reflog", "expire", "--expire=now", "--all")
			git_run(t, dir, "-C", dir, "gc", "--prune=now", "--quiet")

			git_run(t, dir, git_protocol(version, "-C", dir, "fetch", "--quiet", "origin", "main")...)
			git_run(t, dir, "-C", dir, "merge", "--quiet", "--ff-only", "FETCH_HEAD")
			git_run(t, dir, "-C", dir, "fsck", "--strict")

			// The file has to come back byte for byte: a delta applied against
			// the wrong base still produces a hash-checked object, but a base
			// chosen from the wrong path would fail here first.
			expected := git_run(t, "", "-C", repo_path, "rev-parse", "main:large.bin")
			actual := git_run(t, dir, "-C", dir, "rev-parse", "HEAD:large.bin")
			if strings.TrimSpace(expected) != strings.TrimSpace(actual) {
				t.Errorf("large.bin is %s after the fetch, want %s", strings.TrimSpace(actual), strings.TrimSpace(expected))
			}
		})
	}
}

// TestGitPackThinEntries — the pack format itself, read back by go-git's
// scanner, which is an independent implementation of the entry encoding. The
// variable-width type-and-size header is hand-written here, and an off-by-one
// in it produces a pack that only fails at the far end.
func TestGitPackThinEntries(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git binary not available")
	}

	user, _, cleanup := create_git_test_env(t)
	defer cleanup()

	repo_path, first, second := git_thin_repo(t, user, "thinformat")
	storage, err := (&git_loader{}).Load(&transport.Endpoint{Path: repo_path})
	if err != nil {
		t.Fatalf("load: %v", err)
	}

	capabilities := capability.NewList()
	if err := capabilities.Add(capability.ThinPack); err != nil {
		t.Fatalf("capabilities: %v", err)
	}
	objects, err := git_upload_pack_select(storage, &git_request{
		capabilities: capabilities,
		wants:        []plumbing.Hash{second},
		done:         true,
	}, []plumbing.Hash{first}, true)
	if err != nil {
		t.Fatalf("select: %v", err)
	}

	bases, err := git_thin_bases(storage, objects.objects, []plumbing.Hash{first})
	if err != nil {
		t.Fatalf("bases: %v", err)
	}
	if len(bases) == 0 {
		t.Fatal("no delta base was found for a file the client holds an earlier version of")
	}

	pack, deltas, err := git_pack_thin(storage, objects.objects, bases)
	if err != nil {
		t.Fatalf("pack: %v", err)
	}
	if deltas == 0 {
		t.Fatal("the pack was built with no deltas at all")
	}

	scanner := packfile.NewScanner(bytes.NewReader(pack))
	version, count, err := scanner.Header()
	if err != nil {
		t.Fatalf("go-git cannot read the pack header: %v", err)
	}
	if version != 2 {
		t.Errorf("pack version %d, want 2", version)
	}
	if int(count) != len(objects.objects) {
		t.Errorf("pack header claims %d objects, %d were written", count, len(objects.objects))
	}

	references := 0
	for i := uint32(0); i < count; i++ {
		header, err := scanner.NextObjectHeader()
		if err != nil {
			t.Fatalf("entry %d does not decode: %v", i, err)
		}
		if header.Type == plumbing.REFDeltaObject {
			references++
			if header.Reference.IsZero() {
				t.Errorf("entry %d is a REF_DELTA naming no base", i)
			}
			// A thin pack's whole point: the base is NOT in the pack.
			if storage.HasEncodedObject(header.Reference) != nil {
				t.Errorf("entry %d deltas against %s, which this repository does not hold", i, header.Reference)
			}
		}
	}
	if references != deltas {
		t.Errorf("go-git read %d REF_DELTA entries, %d were written", references, deltas)
	}

	if _, err := scanner.Checksum(); err != nil {
		t.Errorf("the pack checksum does not verify: %v", err)
	}
}
