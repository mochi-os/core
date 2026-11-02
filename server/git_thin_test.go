// Mochi server: git smart-HTTP thin packs.
//
// A thin pack may deltify against a base the client already holds, named by
// hash in a REF_DELTA. go-git's encoder cannot produce one, so the pack written
// here is ours; these assert that it saves bytes and that real git can read it.
//
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the Mochi
// Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"bytes"
	"context"
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
	"github.com/go-git/go-git/v5/plumbing/object"
	"github.com/go-git/go-git/v5/plumbing/protocol/packp/capability"
	"github.com/go-git/go-git/v5/plumbing/storer"
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

	user, _ := create_git_test_env(t)

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

// TestGitFetchThinPackNeverLargerThanWhole - a fetch of entirely new files has
// nothing on the client's side to delta against, and must not come out worse
// for having asked for a thin pack.
func TestGitFetchThinPackNeverLargerThanWhole(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git binary not available")
	}

	user, _ := create_git_test_env(t)

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

// TestGitPackMemoryIsBounded - comparing thin against whole holds both packs in
// memory, and the object-count ceiling bounds no bytes at all, so the
// comparison has to give up rather than allocate twice.
func TestGitPackMemoryIsBounded(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git binary not available")
	}

	user, _ := create_git_test_env(t)

	repo_path, first, second := git_thin_repo(t, user, "thinmemory")
	storage, err := (&git_loader{}).Load(&transport.Endpoint{Path: repo_path})
	if err != nil {
		t.Fatalf("load: %v", err)
	}

	capabilities := capability.NewList()
	if err := capabilities.Add(capability.ThinPack); err != nil {
		t.Fatalf("capabilities: %v", err)
	}
	selection, err := git_upload_pack_select(context.Background(), storage, &git_request{
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

// TestGitPackIsDeterministic - revlist returns its objects by iterating a Go
// map, so without a stable sort two identical fetches differ by a few dozen
// bytes and any pack-size measurement is noise.
func TestGitPackIsDeterministic(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git binary not available")
	}

	user, _ := create_git_test_env(t)

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

	user, _ := create_git_test_env(t)

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

	user, _ := create_git_test_env(t)

	repo_path, first, second := git_thin_repo(t, user, "thinformat")
	storage, err := (&git_loader{}).Load(&transport.Endpoint{Path: repo_path})
	if err != nil {
		t.Fatalf("load: %v", err)
	}

	capabilities := capability.NewList()
	if err := capabilities.Add(capability.ThinPack); err != nil {
		t.Fatalf("capabilities: %v", err)
	}
	objects, err := git_upload_pack_select(context.Background(), storage, &git_request{
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

	pack, deltas, _, err := git_pack_thin(storage, objects.objects, bases)
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

// thin_counter counts what a fetch reads from a store: every object by hash,
// and how many objects were asked for as stored (DeltaObject), which only the
// whole pack's encoder asks for.
type thin_counter struct {
	storer.Storer
	reads  map[plumbing.Hash]int
	stored int
}

func (s *thin_counter) EncodedObject(kind plumbing.ObjectType, hash plumbing.Hash) (plumbing.EncodedObject, error) {
	s.reads[hash]++
	return s.Storer.EncodedObject(kind, hash)
}

func (s *thin_counter) DeltaObject(kind plumbing.ObjectType, hash plumbing.Hash) (plumbing.EncodedObject, error) {
	s.stored++
	if stored, ok := s.Storer.(storer.DeltaObjectStorer); ok {
		return stored.DeltaObject(kind, hash)
	}
	return s.Storer.EncodedObject(kind, hash)
}

// thin_select chooses what a thin-capable fetch of want sends a client
// offering haves.
func thin_select(t *testing.T, storage storer.Storer, want plumbing.Hash, haves []plumbing.Hash) *git_selection {
	t.Helper()
	capabilities := capability.NewList()
	if err := capabilities.Add(capability.ThinPack); err != nil {
		t.Fatalf("capabilities: %v", err)
	}
	selection, err := git_upload_pack_select(context.Background(), storage, &git_request{
		capabilities: capabilities,
		wants:        []plumbing.Hash{want},
		done:         true,
	}, haves, true)
	if err != nil {
		t.Fatalf("select: %v", err)
	}
	return selection
}

// TestGitThinBasesComeFromWhereHistoriesMeet. The thin pack's base search
// walked the tree of every commit the client offered - git offers sixteen at
// once - to pair what is sent with what the client holds at the same path. It
// walks the trees of the commits where the client's history meets the pack's.
func TestGitThinBasesComeFromWhereHistoriesMeet(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git binary not available")
	}
	user, _ := create_git_test_env(t)
	repo_path, commits := git_negotiation_repo(t, user, "thinedges", 20)
	storage, err := (&git_loader{}).Load(&transport.Endpoint{Path: repo_path})
	if err != nil {
		t.Fatalf("load: %v", err)
	}

	// The sixteen newest commits the client holds, as git offers them.
	haves := commits[3:19]
	selection := thin_select(t, storage, commits[19], haves)
	if len(selection.edges) != 1 || selection.edges[0] != commits[18] {
		t.Fatalf("THE POINT: the edges are %v, want only %s, where the histories meet", selection.edges, commits[18])
	}

	counter := &thin_counter{Storer: storage, reads: map[plumbing.Hash]int{}}
	if pack, _ := git_thin_candidate(counter, selection); pack == nil {
		t.Fatal("no thin pack was built for a commit adding a file next to those the client holds")
	}
	for _, hash := range haves[:len(haves)-1] {
		commit, err := object.GetCommit(storage, hash)
		if err != nil {
			t.Fatalf("commit: %v", err)
		}
		if counter.reads[commit.TreeHash] > 0 {
			t.Errorf("THE POINT: the base search read the tree of %s, which is not where the histories meet", hash)
		}
	}
}

// TestGitThinPackSkipsTheWholePack. Every thin-capable fetch also built the
// whole pack, delta search and all, only to compare sizes. When every file and
// folder in the thin pack is already a delta against the client's copy, the
// whole pack is not built; when one had to go whole, the comparison still runs,
// since the whole pack can then be the smaller.
func TestGitThinPackSkipsTheWholePack(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git binary not available")
	}
	user, _ := create_git_test_env(t)

	// A small edit to a large file, and nothing else: the file and the
	// folder are deltas.
	if err := git_init(user, test_app, "thinskip"); err != nil {
		t.Fatalf("git_init: %v", err)
	}
	repo_path := git_repo_path(user, test_app, "thinskip")
	edits := git_work_open(t, "git_thin_skip")
	payload := git_thin_payload(7, 1<<20)
	if err := os.WriteFile(filepath.Join(edits.dir, "large.bin"), payload, 0644); err != nil {
		t.Fatalf("write: %v", err)
	}
	first := edits.commit("first")
	copy(payload[512<<10:], git_thin_payload(9, 512))
	if err := os.WriteFile(filepath.Join(edits.dir, "large.bin"), payload, 0644); err != nil {
		t.Fatalf("write: %v", err)
	}
	edits.run("commit", "-q", "-a", "-m", "edit")
	second := plumbing.NewHash(edits.run("rev-parse", "HEAD"))
	edits.run("push", repo_path, "main")
	storage, err := (&git_loader{}).Load(&transport.Endpoint{Path: repo_path})
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	counter := &thin_counter{Storer: storage, reads: map[plumbing.Hash]int{}}
	chosen := git_upload_pack_candidate(counter, thin_select(t, storage, second, []plumbing.Hash{first}))
	if chosen == nil {
		t.Fatal("no candidate was chosen")
	}
	if _, deltas := store_count(t, chosen); deltas == 0 {
		t.Error("the chosen pack holds no deltas: it is not the thin one")
	}
	if counter.stored > 0 {
		t.Errorf("THE POINT: the whole pack was built (%d objects asked for as stored) though every file and folder was a delta", counter.stored)
	}

	// New files beside the old: they go whole in the thin pack, so the
	// whole pack is built to compare.
	if err := git_init(user, test_app, "thincompare"); err != nil {
		t.Fatalf("git_init: %v", err)
	}
	compare_path := git_repo_path(user, test_app, "thincompare")
	work := git_work_open(t, "git_thin_compare")
	before := work.commit("first")
	payload = git_thin_payload(11, 64<<10)
	for i := 0; i < 3; i++ {
		payload[i*1024] ^= 0xff
		if err := os.WriteFile(filepath.Join(work.dir, "new"+strconv.Itoa(i)+".bin"), payload, 0644); err != nil {
			t.Fatalf("write: %v", err)
		}
	}
	after := work.commit("second")
	work.run("push", compare_path, "main")
	storage, err = (&git_loader{}).Load(&transport.Endpoint{Path: compare_path})
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	counter = &thin_counter{Storer: storage, reads: map[plumbing.Hash]int{}}
	if chosen := git_upload_pack_candidate(counter, thin_select(t, storage, after, []plumbing.Hash{before})); chosen == nil {
		t.Fatal("no candidate was chosen")
	}
	if counter.stored == 0 {
		t.Error("new files went whole in the thin pack, but the whole pack was never built to compare")
	}
}

// TestGitThinPackComparesVersionsOfOnePath. Several new versions of one file
// each go in the thin pack as a delta against the one copy the client holds,
// so each carries every change made since; the whole pack keeps the chain of
// small deltas between them. Every file is a delta then, and still the whole
// pack can be the smaller, so it is built to compare, and the smaller sent.
func TestGitThinPackComparesVersionsOfOnePath(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git binary not available")
	}
	user, _ := create_git_test_env(t)
	if err := git_init(user, test_app, "thinversions"); err != nil {
		t.Fatalf("git_init: %v", err)
	}
	repo_path := git_repo_path(user, test_app, "thinversions")
	work := git_work_open(t, "git_thin_versions")
	log := []byte(store_lines(5, 200))
	if err := os.WriteFile(filepath.Join(work.dir, "changes.log"), log, 0644); err != nil {
		t.Fatalf("write: %v", err)
	}
	first := work.commit("first")
	for i := 0; i < 120; i++ {
		log = append(log, []byte(store_lines(int64(1000+i), 1))...)
		if err := os.WriteFile(filepath.Join(work.dir, "changes.log"), log, 0644); err != nil {
			t.Fatalf("write: %v", err)
		}
		work.run("commit", "-q", "-a", "-m", "append "+strconv.Itoa(i))
	}
	last := plumbing.NewHash(work.run("rev-parse", "HEAD"))
	work.run("push", repo_path, "main")

	storage, err := (&git_loader{}).Load(&transport.Endpoint{Path: repo_path})
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	selection := thin_select(t, storage, last, []plumbing.Hash{first})
	plain, err := git_pack_plain(storage, selection.objects)
	if err != nil {
		t.Fatalf("whole pack: %v", err)
	}
	thin, _ := git_thin_candidate(storage, selection)
	if len(thin) <= len(plain) {
		t.Fatalf("setup: the thin pack is %d bytes and the whole %d; the whole must be the smaller", len(thin), len(plain))
	}
	counter := &thin_counter{Storer: storage, reads: map[plumbing.Hash]int{}}
	chosen := git_upload_pack_candidate(counter, selection)
	if chosen == nil {
		t.Fatal("no candidate was chosen")
	}
	t.Logf("120 appends to one file: %d bytes sent, the thin pack %d, the whole %d", len(chosen), len(thin), len(plain))
	if counter.stored == 0 {
		t.Error("THE POINT: 120 versions of one file went as deltas of one base, and the whole pack was never built to compare")
	}
	if len(chosen) > len(plain) {
		t.Errorf("THE POINT: %d bytes sent where the whole pack is %d", len(chosen), len(plain))
	}
}
