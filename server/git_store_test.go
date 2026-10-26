// Mochi server: how repositories are stored and sent - a push kept as the pack
// it came in, made whole when it is thin; a clone sending what is stored as it
// is stored; and repacks that search for delta bases by path, once over packs
// made before they did.
//
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the Mochi
// Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"bytes"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/go-git/go-billy/v5/osfs"
	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/cache"
	"github.com/go-git/go-git/v5/plumbing/format/packfile"
	"github.com/go-git/go-git/v5/plumbing/object"
	"github.com/go-git/go-git/v5/plumbing/storer"
	"github.com/go-git/go-git/v5/storage/filesystem"
)

// store_packs lists a repository's packs.
func store_packs(t *testing.T, path string) []string {
	t.Helper()
	found, err := filepath.Glob(filepath.Join(path, "objects", "pack", "pack-*.pack"))
	if err != nil {
		t.Fatalf("listing packs: %v", err)
	}
	sort.Strings(found)
	return found
}

// store_count reads a pack and counts its objects and how many are deltas.
func store_count(t *testing.T, data []byte) (int, int) {
	t.Helper()
	scanner := packfile.NewScanner(bytes.NewReader(data))
	_, count, err := scanner.Header()
	if err != nil {
		t.Fatalf("pack header: %v", err)
	}
	deltas := 0
	for range count {
		header, err := scanner.NextObjectHeader()
		if err != nil {
			t.Fatalf("pack entry: %v", err)
		}
		if header.Type == plumbing.OFSDeltaObject || header.Type == plumbing.REFDeltaObject {
			deltas++
		}
		if _, _, err := scanner.NextObject(&bytes.Buffer{}); err != nil {
			t.Fatalf("pack entry: %v", err)
		}
	}
	return int(count), deltas
}

// store_file counts the objects and deltas of a pack on disk.
func store_file(t *testing.T, path string) (int, int) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading %s: %v", path, err)
	}
	return store_count(t, data)
}

// store_lines is a text of count lines that a seed makes its own.
func store_lines(seed int64, count int) string {
	random := rand.New(rand.NewSource(seed))
	var out strings.Builder
	for range count {
		fmt.Fprintf(&out, "line %08x %08x %08x %08x\n", random.Uint32(), random.Uint32(), random.Uint32(), random.Uint32())
	}
	return out.String()
}

// store_edit commits count edits of file.txt in the client repository at work,
// each changing one of its 300 lines, from edit start on.
func store_edit(t *testing.T, work string, start, count int) {
	t.Helper()
	lines := strings.SplitAfter(store_lines(1, 300), "\n")
	for edit := start; edit < start+count; edit++ {
		for i := 0; i <= edit; i++ {
			lines[(i*37)%300] = fmt.Sprintf("edit %d of line %d\n", i, (i*37)%300)
		}
		if err := os.WriteFile(filepath.Join(work, "file.txt"), []byte(strings.Join(lines, "")), 0644); err != nil {
			t.Fatalf("write: %v", err)
		}
		git_run(t, work, "add", "file.txt")
		git_run(t, work, "commit", "-m", fmt.Sprintf("edit %d", edit))
	}
}

// TestPushKeepsThePack. A push was expanded into loose objects, one by one, and
// its deltas thrown away; the repack that later packed them found a third of
// the deltas git does, so repositories were stored five times larger than git
// stores them. The pack is now kept as it came. A second push is thin - its
// deltas are based on objects the first one stored - and is kept with those
// bases appended, so each pack stands on its own.
func TestPushKeepsThePack(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git binary not available")
	}
	user, _ := create_git_test_env(t)
	if err := git_init(user, test_app, "keep"); err != nil {
		t.Fatalf("git_init: %v", err)
	}
	path := git_repo_path(user, test_app, "keep")
	server := git_negotiation_server(t, path)

	work := t.TempDir()
	git_run(t, work, "init", "-b", "main")
	store_edit(t, work, 0, 20)
	git_run(t, work, "push", server.URL, "main")

	found := store_packs(t, path)
	if len(found) != 1 {
		t.Fatalf("THE POINT: the push left %d packs, want the one it came in", len(found))
	}
	if loose := git_loose_count(path); loose != 0 {
		t.Errorf("THE POINT: the push left %d loose objects, want none", loose)
	}
	if objects, deltas := store_file(t, found[0]); objects != 60 || deltas < 15 {
		t.Errorf("the kept pack holds %d objects, %d of them deltas; want 60 with git's deltas between the file's versions", objects, deltas)
	}

	old := strings.TrimSpace(git_run(t, work, "rev-parse", "HEAD"))
	store_edit(t, work, 20, 5)
	git_run(t, work, "push", server.URL, "main")
	sent := len(strings.Split(strings.TrimSpace(git_run(t, work, "rev-list", "--objects", old+"..HEAD")), "\n"))

	found = store_packs(t, path)
	if len(found) != 2 {
		t.Fatalf("THE POINT: the second push left %d packs, want 2", len(found))
	}
	if loose := git_loose_count(path); loose != 0 {
		t.Errorf("the second push left %d loose objects, want none", loose)
	}
	// verify-pack refuses a pack with a delta whose base it does not hold.
	second := 0
	for _, pack := range found {
		git_run(t, path, "verify-pack", strings.TrimSuffix(pack, ".pack")+".idx")
		if objects, _ := store_file(t, pack); objects != 60 {
			second = objects
		}
	}
	if second <= sent {
		t.Errorf("THE POINT: the second push's pack holds %d objects for the %d sent: it was not thin, or the bases its deltas need were not appended", second, sent)
	}
	git_run(t, path, "fsck")

	// The clone is sent the deltas as they are stored, not every object whole.
	clone := t.TempDir()
	git_run(t, clone, "clone", "-q", server.URL, ".")
	git_run(t, clone, "fsck")
	received := store_packs(t, filepath.Join(clone, ".git"))
	if len(received) != 1 {
		t.Fatalf("the clone holds %d packs, want 1", len(received))
	}
	if objects, deltas := store_file(t, received[0]); objects != 75 || deltas < 15 {
		t.Errorf("THE POINT: a clone of 75 objects stored mostly as deltas was sent %d objects, %d of them deltas", objects, deltas)
	}
	if head := strings.TrimSpace(git_run(t, clone, "rev-parse", "HEAD")); head != strings.TrimSpace(git_run(t, work, "rev-parse", "HEAD")) {
		t.Errorf("the clone's head is %s, not the pushed one", head)
	}
}

// TestPushNotKeptStoresItsObjects. A pack that cannot be kept - here, one whose
// copy is missing - is stored one object at a time, as before.
func TestPushNotKeptStoresItsObjects(t *testing.T) {
	path, repo := repack_test_empty(t)
	staging, err := git_quarantine(path)
	if err != nil {
		t.Fatalf("git_quarantine: %v", err)
	}
	defer os.RemoveAll(staging)
	staged := filesystem.NewStorage(osfs.New(staging), cache.NewObjectLRUDefault())
	blob := staged.NewEncodedObject()
	blob.SetType(plumbing.BlobObject)
	writer, err := blob.Writer()
	if err != nil {
		t.Fatalf("blob writer: %v", err)
	}
	writer.Write([]byte("staged\n"))
	writer.Close()
	hash, err := staged.SetEncodedObject(blob)
	if err != nil {
		t.Fatalf("staging a blob: %v", err)
	}

	if err := git_store(staging, path); err != nil {
		t.Fatalf("git_store: %v", err)
	}
	if _, err := repo.Storer.EncodedObject(plumbing.AnyObject, hash); err != nil {
		t.Errorf("THE POINT: the staged object did not reach the repository: %v", err)
	}
}

// store_versions stores, under five commits on main, versions of files:
// file i's version k is its own text with k more lines. Sizes are alike across
// files and grow with the version, so by size alone a version sorts among
// other files' same version, and only the path brings one file's together.
func store_versions(t *testing.T, repo *git.Repository, files int) {
	t.Helper()
	var parent []plumbing.Hash
	for version := range 5 {
		var entries []object.TreeEntry
		for file := range files {
			text := store_lines(int64(file)+100, 40) + store_lines(int64(file)+1000, version*4)
			entries = append(entries, object.TreeEntry{Name: fmt.Sprintf("file-%02d.txt", file), Mode: 0100644, Hash: repack_test_blob(t, repo, text)})
		}
		tree := repack_test_store(t, repo, &object.Tree{Entries: entries})
		signature := object.Signature{Name: "Test", Email: "test@example.com", When: time.Unix(1700000000+int64(version), 0)}
		commit := repack_test_store(t, repo, &object.Commit{Author: signature, Committer: signature, Message: fmt.Sprintf("version %d\n", version), TreeHash: tree, ParentHashes: parent})
		parent = []plumbing.Hash{commit}
	}
	if err := repo.Storer.SetReference(plumbing.NewHashReference("refs/heads/main", parent[0])); err != nil {
		t.Fatalf("SetReference: %v", err)
	}
}

// store_whole packs every object of a repository whole, with no deltas, and
// drops the loose copies: the shape of a pack whose objects were searched
// before and stored whole.
func store_whole(t *testing.T, path string) {
	t.Helper()
	repo, err := git.PlainOpen(path)
	if err != nil {
		t.Fatalf("PlainOpen: %v", err)
	}
	reachable, _, err := git_reachable(repo.Storer)
	if err != nil {
		t.Fatalf("git_reachable: %v", err)
	}
	var hashes []plumbing.Hash
	for hash := range reachable {
		hashes = append(hashes, hash)
	}
	writer, err := repo.Storer.(storer.PackfileWriter).PackfileWriter()
	if err != nil {
		t.Fatalf("PackfileWriter: %v", err)
	}
	if _, err := packfile.NewEncoder(writer, repo.Storer, false).Encode(hashes, 0); err != nil {
		t.Fatalf("encode: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	loose := repo.Storer.(storer.LooseObjectStorer)
	loose.ForEachObjectHash(func(hash plumbing.Hash) error { return loose.DeleteLooseObject(hash) })
}

// TestRepackSearchesByPath. Delta bases were looked for among objects sorted by
// type and size alone, which seldom put versions of one file in the same
// window: with thirty files, a version sat among the other files' same
// version. Sorted by path as git sorts them, each file's versions meet.
func TestRepackSearchesByPath(t *testing.T) {
	path, repo := repack_test_empty(t)
	store_versions(t, repo, 30)

	if err := git_repack(path); err != nil {
		t.Fatalf("git_repack: %v", err)
	}
	found := store_packs(t, path)
	if len(found) != 1 {
		t.Fatalf("%d packs after a repack, want 1", len(found))
	}
	// 30 files of 5 versions: 4 deltas a file when its versions meet.
	if _, deltas := store_file(t, found[0]); deltas < 100 {
		t.Errorf("THE POINT: the repack found %d deltas among 30 files of 5 versions, want about 120", deltas)
	}
}

// TestCloneSendsWhatIsStored. Every clone searched again for a delta base for
// each object stored whole - most of the time a clone took - though a stored
// object has been searched already: by the git client that pushed it, or by a
// repack. Only a loose object, which nothing has searched, is searched now.
func TestCloneSendsWhatIsStored(t *testing.T) {
	path, repo := repack_test_empty(t)
	store_versions(t, repo, 10)
	reachable, _, err := git_reachable(repo.Storer)
	if err != nil {
		t.Fatalf("git_reachable: %v", err)
	}
	var hashes []plumbing.Hash
	for hash := range reachable {
		hashes = append(hashes, hash)
	}

	sent, err := git_pack_plain(repo.Storer, hashes)
	if err != nil {
		t.Fatalf("git_pack_plain: %v", err)
	}
	if _, deltas := store_count(t, sent); deltas == 0 {
		t.Error("loose objects, which nothing has searched, were sent with no deltas")
	}

	store_whole(t, path)
	repo, err = git.PlainOpen(path)
	if err != nil {
		t.Fatalf("PlainOpen: %v", err)
	}
	sent, err = git_pack_plain(repo.Storer, hashes)
	if err != nil {
		t.Fatalf("git_pack_plain: %v", err)
	}
	if objects, deltas := store_count(t, sent); objects != len(hashes) || deltas != 0 {
		t.Errorf("THE POINT: %d objects stored whole were sent with %d deltas, searched for again", objects, deltas)
	}

	// A clone is sent the same, through the encoder that streams it. A commit
	// written loose keeps the pack from being sent as it is.
	if _, err := exec.LookPath("git"); err != nil {
		return
	}
	head, err := repo.Reference(plumbing.NewBranchReferenceName("main"), true)
	if err != nil {
		t.Fatalf("main: %v", err)
	}
	blob := repack_test_blob(t, repo, "written by Mochi\n")
	tree := repack_test_store(t, repo, &object.Tree{Entries: []object.TreeEntry{{Name: "new.txt", Mode: 0100644, Hash: blob}}})
	signature := object.Signature{Name: "Test", Email: "test@example.com", When: time.Now()}
	written := repack_test_store(t, repo, &object.Commit{Author: signature, Committer: signature, Message: "written\n", TreeHash: tree, ParentHashes: []plumbing.Hash{head.Hash()}})
	if err := repo.Storer.SetReference(plumbing.NewHashReference(plumbing.NewBranchReferenceName("main"), written)); err != nil {
		t.Fatalf("SetReference: %v", err)
	}
	server := git_negotiation_server(t, path)
	clone := t.TempDir()
	git_run(t, clone, "-c", "transfer.unpackLimit=1", "clone", "-q", "--bare", server.URL, ".")
	received := store_packs(t, clone)
	if len(received) != 1 {
		t.Fatalf("the clone holds %d packs, want 1", len(received))
	}
	// Only the three loose objects are searched, and may find a base.
	if objects, deltas := store_file(t, received[0]); objects != len(hashes)+3 || deltas > 3 {
		t.Errorf("THE POINT: a clone of %d objects stored whole was sent %d deltas, searched for again", objects, deltas)
	}
}

// TestFetchSearchesWhereTheBaseStaysBehind. An object stored as a delta whose
// base is not being sent goes whole, and is searched for another base among
// what is sent: it was only ever searched against the base left behind.
func TestFetchSearchesWhereTheBaseStaysBehind(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git binary not available")
	}
	work := t.TempDir()
	git_run(t, work, "init", "-q", "-b", "main")
	// The first version is the largest, so with deltas one deep git bases
	// both later versions on it.
	text := store_lines(7, 300)
	for version, lines := range []int{300, 290, 280} {
		content := strings.Join(strings.SplitAfter(text, "\n")[:lines], "")
		if err := os.WriteFile(filepath.Join(work, "file.txt"), []byte(content), 0644); err != nil {
			t.Fatalf("write: %v", err)
		}
		git_run(t, work, "add", "file.txt")
		git_run(t, work, "commit", "-q", "-m", fmt.Sprintf("version %d", version))
	}
	git_run(t, work, "repack", "-adfq", "--depth=1")
	var blobs []plumbing.Hash
	for _, revision := range []string{"HEAD~1:file.txt", "HEAD:file.txt"} {
		blobs = append(blobs, plumbing.NewHash(strings.TrimSpace(git_run(t, work, "rev-parse", revision))))
	}
	base := strings.TrimSpace(git_run(t, work, "rev-parse", "HEAD~2:file.txt"))
	listing := git_run(t, work, "verify-pack", "-v", store_packs(t, filepath.Join(work, ".git"))[0])
	for _, blob := range blobs {
		if !strings.Contains(listing, blob.String()) || !strings.Contains(listing, base) {
			t.Fatal("setup: the pack does not list the versions")
		}
		for _, line := range strings.Split(listing, "\n") {
			if strings.HasPrefix(line, blob.String()) && !strings.HasSuffix(strings.TrimSpace(line), base) {
				t.Fatalf("setup: %s is not stored as a delta of the first version: %s", blob, line)
			}
		}
	}

	repo, err := git.PlainOpen(work)
	if err != nil {
		t.Fatalf("PlainOpen: %v", err)
	}
	sent, err := git_pack_plain(repo.Storer, blobs)
	if err != nil {
		t.Fatalf("git_pack_plain: %v", err)
	}
	if objects, deltas := store_count(t, sent); objects != 2 || deltas != 1 {
		t.Errorf("THE POINT: two versions whose base stays behind were sent as %d objects with %d deltas, want one a delta of the other", objects, deltas)
	}
}

// TestRepackSearchesOldPacksOnce. A repository packed before packs were
// searched by path is repacked on its next push however few loose objects it
// holds, searching every object stored whole, and marked so that later
// repacks search only loose objects. One made now starts marked.
func TestRepackSearchesOldPacksOnce(t *testing.T) {
	path, repo := repack_test_empty(t)
	store_versions(t, repo, 10)
	if git_unsearched(path) {
		t.Error("a repository with nothing packed counts as packed unsearched")
	}
	store_whole(t, path)
	if !git_unsearched(path) {
		t.Fatal("THE POINT: a pack made before path search does not count as unsearched")
	}

	minimum := git_repack_minimum
	dispatch := git_repack_dispatch
	defer func() {
		git_repack_minimum = minimum
		git_repack_dispatch = dispatch
	}()
	git_repack_minimum = 1 << 30
	done := make(chan struct{}, 4)
	git_repack_dispatch = func(string) error {
		done <- struct{}{}
		return nil
	}
	git_repack_attempted.Delete(path)
	git_repack_consider(path)
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("THE POINT: an unsearched pack did not repack on the next push")
	}
	for i := 0; i < 500; i++ {
		if _, running := git_repack_running.Load(path); !running {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	git_repack_attempted.Delete(path)
	git_repack_remaining.Delete(path)

	if err := git_repack(path); err != nil {
		t.Fatalf("git_repack: %v", err)
	}
	found := store_packs(t, path)
	if _, deltas := store_file(t, found[0]); deltas < 30 {
		t.Errorf("THE POINT: the first repack found %d deltas among objects stored whole, want the files' versions deltified", deltas)
	}
	if git_unsearched(path) {
		t.Error("THE POINT: the repack did not mark the repository as searched")
	}

	// Searched: a repack keeps what is stored and searches only loose
	// objects, so objects packed whole stay whole.
	searched, repo := repack_test_empty(t)
	store_versions(t, repo, 10)
	store_whole(t, searched)
	if err := git_searched_mark(repo.Storer); err != nil {
		t.Fatalf("mark: %v", err)
	}
	if err := git_repack(searched); err != nil {
		t.Fatalf("git_repack: %v", err)
	}
	found = store_packs(t, searched)
	if _, deltas := store_file(t, found[0]); deltas != 0 {
		t.Errorf("THE POINT: a repack of a searched repository searched %d objects stored whole again", deltas)
	}

	user, _ := create_git_test_env(t)
	if err := git_init(user, test_app, "fresh"); err != nil {
		t.Fatalf("git_init: %v", err)
	}
	fresh, err := git.PlainOpen(git_repo_path(user, test_app, "fresh"))
	if err != nil {
		t.Fatalf("PlainOpen: %v", err)
	}
	if !git_searched(fresh.Storer) {
		t.Error("THE POINT: a new repository does not start searched, so its first repack would search everything")
	}
}

// TestRepackOnTooManyPacks. Each push is kept as a pack of its own, and every
// object read looks through them all, so a push repacks a repository once it
// holds more than git_packs_maximum, however few loose objects it has.
func TestRepackOnTooManyPacks(t *testing.T) {
	path, repo := repack_test_empty(t)
	if err := git_searched_mark(repo.Storer); err != nil {
		t.Fatalf("mark: %v", err)
	}
	minimum, maximum, dispatch := git_repack_minimum, git_packs_maximum, git_repack_dispatch
	defer func() {
		git_repack_minimum, git_packs_maximum, git_repack_dispatch = minimum, maximum, dispatch
	}()
	git_repack_minimum = 1 << 30
	git_packs_maximum = 1
	done := make(chan struct{}, 4)
	git_repack_dispatch = func(string) error {
		done <- struct{}{}
		return nil
	}
	quiet := func(reason string) {
		t.Helper()
		select {
		case <-done:
			t.Errorf("a repack ran: %s", reason)
		case <-time.After(300 * time.Millisecond):
		}
	}

	repack_test_commit(t, repo, nil)
	store_whole(t, path)
	git_repack_attempted.Delete(path)
	git_repack_consider(path)
	quiet("one pack is within the limit")

	repack_test_blob(t, repo, "another push\n")
	writer, err := repo.Storer.(storer.PackfileWriter).PackfileWriter()
	if err != nil {
		t.Fatalf("PackfileWriter: %v", err)
	}
	var hashes []plumbing.Hash
	repo.Storer.(storer.LooseObjectStorer).ForEachObjectHash(func(hash plumbing.Hash) error {
		hashes = append(hashes, hash)
		return nil
	})
	if _, err := packfile.NewEncoder(writer, repo.Storer, false).Encode(hashes, 0); err != nil {
		t.Fatalf("encode: %v", err)
	}
	writer.Close()
	if count := git_pack_count(path); count != 2 {
		t.Fatalf("setup: %d packs, want 2", count)
	}
	git_repack_attempted.Delete(path)
	git_repack_consider(path)
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("THE POINT: a repository over the pack limit did not repack")
	}
}

// TestGitName. git's pack_name_hash: the last sixteen characters other than
// spaces, the last counting most.
func TestGitName(t *testing.T) {
	if got := git_name("ab"); got != 0x7a400000 {
		t.Errorf("git_name(ab) = %#x, want 0x7a400000", got)
	}
	if git_name("a b\t") != git_name("ab") {
		t.Error("spaces are counted")
	}
	tail := "abcdefghijklmnop"
	if git_name("one/"+tail) != git_name("another/long/folder/"+tail) {
		t.Error("more than the last sixteen characters are counted")
	}
	if git_name("src/main.go") == git_name("src/main.c") {
		t.Error("different names hash alike")
	}
	if git_name("") != 0 {
		t.Error("the root's name is not 0")
	}
}

// TestCloneSendsTheStoredPack. A clone of a repository whose one pack holds
// exactly what the clone asks for is sent that pack as it is stored, with
// nothing encoded. Once the pack holds an object no reference reaches - a
// deleted branch - it is never sent whole, so the deleted work stays behind.
func TestCloneSendsTheStoredPack(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git binary not available")
	}
	user, _ := create_git_test_env(t)
	if err := git_init(user, test_app, "whole"); err != nil {
		t.Fatalf("git_init: %v", err)
	}
	path := git_repo_path(user, test_app, "whole")
	server := git_negotiation_server(t, path)

	work := t.TempDir()
	git_run(t, work, "init", "-q", "-b", "main")
	store_edit(t, work, 0, 10)
	git_run(t, work, "checkout", "-q", "-b", "extra")
	store_edit(t, work, 10, 1)
	extra := strings.TrimSpace(git_run(t, work, "rev-parse", "HEAD"))
	git_run(t, work, "checkout", "-q", "main")
	git_run(t, work, "push", "-q", server.URL, "main", "extra")
	stored := store_packs(t, path)
	if len(stored) != 1 {
		t.Fatalf("setup: %d packs after one push, want 1", len(stored))
	}

	// A clone of so few objects is unpacked loose unless told to keep the pack.
	for _, version := range git_versions {
		clone := t.TempDir()
		git_run(t, clone, append([]string{"-c", "transfer.unpackLimit=1"}, git_protocol(version, "clone", "-q", "--bare", "--no-single-branch", server.URL, ".")...)...)
		received := store_packs(t, clone)
		if len(received) != 1 || filepath.Base(received[0]) != filepath.Base(stored[0]) {
			t.Errorf("THE POINT: protocol %s: the clone was not sent the stored pack as it is: got %v, stored %s", version, received, filepath.Base(stored[0]))
		}
	}

	// Deleted, the branch's commit stays in the pack but no reference reaches it.
	git_run(t, work, "push", "-q", server.URL, ":extra")
	for _, version := range git_versions {
		clone := t.TempDir()
		git_run(t, clone, append([]string{"-c", "transfer.unpackLimit=1"}, git_protocol(version, "clone", "-q", "--bare", server.URL, ".")...)...)
		received := store_packs(t, clone)
		if len(received) == 1 && filepath.Base(received[0]) == filepath.Base(stored[0]) {
			t.Errorf("THE POINT: protocol %s: a pack holding a deleted branch's commit was sent whole", version)
		}
		cmd := exec.Command("git", "-C", clone, "cat-file", "-e", extra)
		if cmd.Run() == nil {
			t.Errorf("THE POINT: protocol %s: the deleted branch's commit reached the clone", version)
		}
	}

	// A commit Mochi writes itself is loose. With one, a clone asks for as
	// many objects as the pack holds, but not the same ones.
	repo, err := git.PlainOpen(path)
	if err != nil {
		t.Fatalf("PlainOpen: %v", err)
	}
	head, err := repo.Reference(plumbing.NewBranchReferenceName("main"), true)
	if err != nil {
		t.Fatalf("main: %v", err)
	}
	blob := repack_test_blob(t, repo, "written by Mochi\n")
	tree := repack_test_store(t, repo, &object.Tree{Entries: []object.TreeEntry{{Name: "new.txt", Mode: 0100644, Hash: blob}}})
	signature := object.Signature{Name: "Test", Email: "test@example.com", When: time.Now()}
	written := repack_test_store(t, repo, &object.Commit{Author: signature, Committer: signature, Message: "written\n", TreeHash: tree, ParentHashes: []plumbing.Hash{head.Hash()}})
	if err := repo.Storer.SetReference(plumbing.NewHashReference(plumbing.NewBranchReferenceName("main"), written)); err != nil {
		t.Fatalf("SetReference: %v", err)
	}
	if objects, _ := store_file(t, stored[0]); objects != 33 {
		t.Fatalf("setup: the pack holds %d objects, want 33 - as many as the clone now asks for", objects)
	}
	clone := t.TempDir()
	git_run(t, clone, "-c", "transfer.unpackLimit=1", "clone", "-q", "--bare", server.URL, ".")
	if exec.Command("git", "-C", clone, "cat-file", "-e", extra).Run() == nil {
		t.Error("THE POINT: a pack as large as what was asked for, but not the same objects, was sent whole")
	}
	git_run(t, clone, "cat-file", "-e", written.String())
	git_run(t, clone, "fsck")
}

// store_cut stages a push - its objects as receive-pack writes them, and its
// pack as it came - with the pack's bytes made by shape, and answers the
// staging directory and the objects. The pack is whole: blob, tree, commit,
// the blob's text naming the push so that two pushes hold different objects.
func store_cut(t *testing.T, path string, number int, shape func([]byte) []byte) (string, []plumbing.Hash) {
	t.Helper()
	staging, err := git_quarantine(path)
	if err != nil {
		t.Fatalf("git_quarantine: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(staging) })
	staged := filesystem.NewStorage(osfs.New(staging), cache.NewObjectLRUDefault())
	store := func(value interface {
		Encode(plumbing.EncodedObject) error
	}) plumbing.Hash {
		encoded := staged.NewEncodedObject()
		if err := value.Encode(encoded); err != nil {
			t.Fatal(err)
		}
		hash, err := staged.SetEncodedObject(encoded)
		if err != nil {
			t.Fatal(err)
		}
		return hash
	}
	blob := staged.NewEncodedObject()
	blob.SetType(plumbing.BlobObject)
	writer, _ := blob.Writer()
	fmt.Fprintf(writer, "push %d, whole, then cut\n", number)
	writer.Close()
	hash, err := staged.SetEncodedObject(blob)
	if err != nil {
		t.Fatal(err)
	}
	tree := store(&object.Tree{Entries: []object.TreeEntry{{Name: "a.txt", Mode: 0100644, Hash: hash}}})
	signature := object.Signature{Name: "Test", Email: "test@example.com", When: time.Now()}
	commit := store(&object.Commit{Author: signature, Committer: signature, Message: "commit\n", TreeHash: tree})
	objects := []plumbing.Hash{commit, tree, hash}

	data, _, err := git_pack_thin(staged, objects, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(staging, git_push_pack), shape(data), 0644); err != nil {
		t.Fatal(err)
	}
	return staging, objects
}

// A pack ends with a checksum of everything before it. go-git's parser takes
// a pack cut off exactly there for a whole one, and nothing checked past it:
// the pack was kept under an all-zero name and sent to cloners as it was,
// failing them, and a second pack cut the same way found the name taken and
// was discarded with its objects. Such a pack is not kept; its objects are
// stored one by one, and a clone has them all.
func TestPushCutAtItsChecksumIsNotKept(t *testing.T) {
	cases := []struct {
		name  string
		shape func([]byte) []byte
	}{
		{"cut at its checksum", func(data []byte) []byte { return data[:len(data)-20] }},
		{"with bytes after its checksum", func(data []byte) []byte { return append(append([]byte{}, data...), "trailing"...) }},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			path, repo := repack_test_empty(t)
			// Two such pushes: the second used to find the first's all-zero
			// name taken, and was discarded.
			var all []plumbing.Hash
			for i := 0; i < 2; i++ {
				staging, objects := store_cut(t, path, i, tt.shape)
				if err := git_store(staging, path); err != nil {
					t.Fatalf("git_store: %v", err)
				}
				all = append(all, objects...)
			}

			if packs := store_packs(t, path); len(packs) != 0 {
				t.Errorf("the pushes left packs %v; a pack not whole must not be kept", packs)
			}
			for _, hash := range all {
				if _, err := repo.Storer.EncodedObject(plumbing.AnyObject, hash); err != nil {
					t.Errorf("object %s was not stored: %v", hash, err)
				}
			}
			data, err := git_pack_plain(repo.Storer, all)
			if err != nil {
				t.Fatalf("clone: %v", err)
			}
			read := repack_test_unpack(t, data)
			for _, hash := range all {
				if _, err := read.EncodedObject(plumbing.AnyObject, hash); err != nil {
					t.Errorf("the clone lacks %s: %v", hash, err)
				}
			}
		})
	}
}

// Control: a whole pack is kept as it came.
func TestPushWholeIsKept(t *testing.T) {
	path, _ := repack_test_empty(t)
	staging, _ := store_cut(t, path, 0, func(data []byte) []byte { return data })
	if err := git_store(staging, path); err != nil {
		t.Fatalf("git_store: %v", err)
	}
	if packs := store_packs(t, path); len(packs) != 1 {
		t.Errorf("a whole push left packs %v, want the one it came in", packs)
	}
}

// store_flushes records what git_flush is asked to write through, and what the
// repository held at that moment, for the test's lifetime.
type store_flush struct {
	path  string
	packs int
	loose int
}

func store_flushes(t *testing.T, repository string) *[]store_flush {
	t.Helper()
	flushed := &[]store_flush{}
	original := git_flush
	git_flush = func(path string) error {
		*flushed = append(*flushed, store_flush{path: path, packs: len(store_packs(t, repository)), loose: git_loose_count(repository)})
		return original(path)
	}
	t.Cleanup(func() { git_flush = original })
	return flushed
}

// After a crash, a reference must never name a pack whose bytes were lost: a
// push's index and pack reach disk before the pack can be found, and the
// folder after, all before git_store returns and the references move.
func TestPushIsOnDiskBeforeItIsFound(t *testing.T) {
	path, _ := repack_test_empty(t)
	staging, _ := store_cut(t, path, 0, func(data []byte) []byte { return data })
	flushed := store_flushes(t, path)
	if err := git_store(staging, path); err != nil {
		t.Fatalf("git_store: %v", err)
	}
	packs := store_packs(t, path)
	if len(packs) != 1 {
		t.Fatalf("the push left packs %v, want one", packs)
	}
	name := strings.TrimSuffix(packs[0], ".pack")

	var index, pack, folder bool
	for _, flush := range *flushed {
		switch {
		case flush.path == name+".idx":
			index = flush.packs == 0
		case flush.path == filepath.Join(staging, git_push_pack):
			pack = flush.packs == 0
		case flush.path == filepath.Dir(name):
			folder = flush.packs == 1
		}
	}
	if !index {
		t.Error("the index did not reach disk before the pack could be found")
	}
	if !pack {
		t.Error("the pack did not reach disk before it was renamed into place")
	}
	if !folder {
		t.Error("the folder the pack was renamed into was not written through after the rename")
	}
}

// A repack deletes the loose objects and the packs its new pack replaces. The
// new pack, its index and the folder reach disk before anything is deleted:
// otherwise a crash could keep the deletions and lose the repository.
func TestRepackIsOnDiskBeforeItDeletes(t *testing.T) {
	path, _ := repack_test_repository(t, 3)
	if err := git_repack(path); err != nil {
		t.Fatalf("first git_repack: %v", err)
	}
	repo, err := git.PlainOpen(path)
	if err != nil {
		t.Fatal(err)
	}
	blob := repack_test_blob(t, repo, "packed by the second repack\n")
	if err := repo.Storer.SetReference(plumbing.NewHashReference("refs/tags/new", blob)); err != nil {
		t.Fatal(err)
	}
	loose := git_loose_count(path)
	if loose == 0 {
		t.Fatal("setup: nothing loose to pack")
	}

	flushed := store_flushes(t, path)
	if err := git_repack(path); err != nil {
		t.Fatalf("second git_repack: %v", err)
	}
	after := store_packs(t, path)
	if len(after) != 1 || git_loose_count(path) != 0 {
		t.Fatalf("setup: the repack left packs %v and %d loose objects", after, git_loose_count(path))
	}
	name := strings.TrimSuffix(after[0], ".pack")

	var pack, index, folder bool
	for _, flush := range *flushed {
		// Before anything is deleted: the loose objects all there, and the
		// old pack beside the new one once it lands.
		whole := flush.loose == loose
		switch {
		case strings.Contains(filepath.Base(flush.path), "tmp_pack_"):
			pack = whole && flush.packs == 1
		case flush.path == name+".idx":
			index = whole
		case flush.path == filepath.Dir(name):
			folder = folder || (whole && flush.packs == 2)
		}
	}
	if !pack {
		t.Error("the new pack did not reach disk before it was renamed into place")
	}
	if !index {
		t.Error("the new index did not reach disk before the repack deleted what it replaces")
	}
	if !folder {
		t.Error("the folder did not reach disk with the new pack in it before the repack deleted what it replaces")
	}
}
