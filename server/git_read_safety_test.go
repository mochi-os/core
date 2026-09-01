// Mochi server: what a read of a repository may cost.
//
// The mochi.git builtins were written for a trusted caller, but the
// repositories app hands them to public actions, other apps' service calls
// and remote peers. Each test here pins one ceiling: a blob read, a file
// compared, the number of files and the text a diff renders, a listing's
// page, the two walks of commit.between, and what a merge writes into a
// commit's identity lines.
//
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"archive/tar"
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/filemode"
	"github.com/go-git/go-git/v5/plumbing/object"
	sl "go.starlark.net/starlark"
)

// read_identity is the owner's identity in every test here; the access
// grants are made against it.
const read_identity = "12ReadOwnerAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"

// read_id pads a label to the shape the entity validator accepts, so the
// builtins see an id they would from a real caller.
func read_id(label string) string {
	return label + strings.Repeat("0", 50-len(label))
}

// read_env makes an empty repository the owner holds every right on, and a
// thread that calls the git builtins as that owner. The repository's entity
// id is the label padded by read_id.
func read_env(t *testing.T, label string) (string, *User, *git.Repository, *sl.Thread) {
	t.Helper()
	repo := read_id(label)
	owner, _ := create_git_test_env(t)
	owner.Identity = &Entity{ID: read_identity}
	os.MkdirAll(filepath.Join(data_dir, "users", owner.UID, test_app.id), 0755)
	os.MkdirAll(filepath.Join(data_dir, "db"), 0755)

	db := db_app_system(owner, test_app)
	if db == nil {
		t.Fatal("db_app_system returned nil")
	}
	defer db.close()
	db.access_setup()
	db.exec("insert into access ( subject, resource, operation, grant, granter, created ) values ( ?, ?, ?, ?, ?, ? )",
		read_identity, "repository/"+repo, "*", 1, read_identity, now())

	if err := git_init(owner, test_app, repo); err != nil {
		t.Fatalf("git_init: %v", err)
	}
	repository, err := git_open(owner, test_app, repo)
	if err != nil {
		t.Fatalf("git_open: %v", err)
	}

	thread := &sl.Thread{}
	thread.SetLocal("user", owner)
	thread.SetLocal("owner", owner)
	thread.SetLocal("app", test_app)
	return repo, owner, repository, thread
}

// read_env_repo is read_env for tests that never need the owner.
func read_env_repo(t *testing.T, label string) (string, *git.Repository, *sl.Thread) {
	t.Helper()
	id, _, repo, thread := read_env(t, label)
	return id, repo, thread
}

// read_file is one entry of a snapshot: its mode and content.
type read_file struct {
	mode filemode.FileMode
	data []byte
}

// read_text is a regular file holding s.
func read_text(s string) read_file {
	return read_file{mode: filemode.Regular, data: []byte(s)}
}

// read_blob stores content as a loose blob.
func read_blob(t *testing.T, repo *git.Repository, data []byte) plumbing.Hash {
	t.Helper()
	obj := repo.Storer.NewEncodedObject()
	obj.SetType(plumbing.BlobObject)
	writer, err := obj.Writer()
	if err != nil {
		t.Fatalf("blob writer: %v", err)
	}
	writer.Write(data)
	writer.Close()
	hash, err := repo.Storer.SetEncodedObject(obj)
	if err != nil {
		t.Fatalf("store blob: %v", err)
	}
	return hash
}

// read_snapshot stores a tree holding files, keyed by path; a "/" in a path
// makes a subtree. Entries are ordered the way git orders them, a directory
// sorting as its name with "/" appended.
func read_snapshot(t *testing.T, repo *git.Repository, files map[string]read_file) plumbing.Hash {
	t.Helper()
	direct := map[string]read_file{}
	nested := map[string]map[string]read_file{}
	for path, file := range files {
		if before, after, found := strings.Cut(path, "/"); found {
			if nested[before] == nil {
				nested[before] = map[string]read_file{}
			}
			nested[before][after] = file
		} else {
			direct[path] = file
		}
	}
	var entries []object.TreeEntry
	for name, file := range direct {
		entries = append(entries, object.TreeEntry{Name: name, Mode: file.mode, Hash: read_blob(t, repo, file.data)})
	}
	for name, sub := range nested {
		entries = append(entries, object.TreeEntry{Name: name, Mode: filemode.Dir, Hash: read_snapshot(t, repo, sub)})
	}
	key := func(e object.TreeEntry) string {
		if e.Mode == filemode.Dir {
			return e.Name + "/"
		}
		return e.Name
	}
	sort.Slice(entries, func(i, j int) bool { return key(entries[i]) < key(entries[j]) })
	obj := repo.Storer.NewEncodedObject()
	if err := (&object.Tree{Entries: entries}).Encode(obj); err != nil {
		t.Fatalf("encode tree: %v", err)
	}
	hash, err := repo.Storer.SetEncodedObject(obj)
	if err != nil {
		t.Fatalf("store tree: %v", err)
	}
	return hash
}

// read_clock hands every test commit a later time than the last, so the
// commit-time iterators walk a chain newest first.
var read_clock int64

// read_commit stores a commit on tree with the given parents.
func read_commit(t *testing.T, repo *git.Repository, parents []plumbing.Hash, tree plumbing.Hash, message string) plumbing.Hash {
	t.Helper()
	read_clock++
	when := time.Unix(1700000000+read_clock, 0)
	signature := object.Signature{Name: "Reader", Email: "reader@example.com", When: when}
	commit := &object.Commit{
		Author:       signature,
		Committer:    signature,
		Message:      message,
		TreeHash:     tree,
		ParentHashes: parents,
	}
	obj := repo.Storer.NewEncodedObject()
	if err := commit.Encode(obj); err != nil {
		t.Fatalf("encode commit: %v", err)
	}
	hash, err := repo.Storer.SetEncodedObject(obj)
	if err != nil {
		t.Fatalf("store commit: %v", err)
	}
	return hash
}

// read_branch points a branch at a commit.
func read_branch(t *testing.T, repo *git.Repository, name string, hash plumbing.Hash) {
	t.Helper()
	if err := repo.Storer.SetReference(plumbing.NewHashReference(plumbing.NewBranchReferenceName(name), hash)); err != nil {
		t.Fatalf("set branch %s: %v", name, err)
	}
}

// read_call invokes a builtin the way the interpreter would.
func read_call(t *testing.T, thread *sl.Thread, fn func(*sl.Thread, *sl.Builtin, sl.Tuple, []sl.Tuple) (sl.Value, error), args ...sl.Value) (sl.Value, error) {
	t.Helper()
	return fn(thread, nil, sl.Tuple(args), nil)
}

// TestBlobContentRefusesAnOversizedFile. The blob became one Starlark string
// with no ceiling, so a public repository holding a large file was a
// per-request allocation of that size for anyone allowed to read it. The size
// is the tree entry's, so the refusal must not read the blob.
func TestBlobContentRefusesAnOversizedFile(t *testing.T) {
	id, repo, thread := read_env_repo(t, "blob")
	tree := read_snapshot(t, repo, map[string]read_file{
		"small.txt": read_text("hello"),
		"big.bin":   {mode: filemode.Regular, data: bytes.Repeat([]byte("0"), int(git_read_maximum)+1)},
	})
	read_branch(t, repo, "main", read_commit(t, repo, nil, tree, "files"))

	value, err := read_call(t, thread, api_git_blob_content, sl.String(id), sl.String("main"), sl.String("small.txt"))
	if err != nil {
		t.Fatalf("a small file was refused: %v", err)
	}
	if got, _ := sl.AsString(value); got != "hello" {
		t.Errorf("small file content = %q, want %q", got, "hello")
	}

	_, err = read_call(t, thread, api_git_blob_content, sl.String(id), sl.String("main"), sl.String("big.bin"))
	if err == nil {
		t.Fatal("a file over git_read_maximum was returned whole")
	}
	if !strings.Contains(err.Error(), "over the") {
		t.Errorf("refusal does not name the limit: %v", err)
	}
}

// TestArchiveSkipsAnImplausibleSymlink. The archive writer reads a symlink's
// target with Contents(), and a pusher can store any blob under symlink mode.
// A target longer than any path the OS accepts is left out of the archive.
func TestArchiveSkipsAnImplausibleSymlink(t *testing.T) {
	_, repo, _ := read_env_repo(t, "archive")
	tree := read_snapshot(t, repo, map[string]read_file{
		"file":     read_text("content"),
		"link":     {mode: filemode.Symlink, data: []byte("file")},
		"enormous": {mode: filemode.Symlink, data: bytes.Repeat([]byte("x"), int(git_link_maximum)+1)},
	})
	commit, err := repo.CommitObject(read_commit(t, repo, nil, tree, "links"))
	if err != nil {
		t.Fatalf("commit object: %v", err)
	}
	root, err := commit.Tree()
	if err != nil {
		t.Fatalf("tree: %v", err)
	}

	var buffer bytes.Buffer
	if err := git_archive_write_tar(&buffer, root, "p/", time.Unix(0, 0)); err != nil {
		t.Fatalf("archive: %v", err)
	}
	names := map[string]string{}
	reader := tar.NewReader(&buffer)
	for {
		header, err := reader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("read archive: %v", err)
		}
		names[header.Name] = header.Linkname
	}
	if names["p/link"] != "file" {
		t.Errorf("a plausible symlink was not archived with its target: %q", names["p/link"])
	}
	if _, ok := names["p/file"]; !ok {
		t.Error("a regular file was not archived")
	}
	if _, ok := names["p/enormous"]; ok {
		t.Error("a symlink with a target over git_link_maximum was archived, so its blob was read whole")
	}
}

// read_diff_repository is a base and head that change one small file and
// one file too large to compare.
func read_diff_repository(t *testing.T) (string, *git.Repository, *sl.Thread) {
	t.Helper()
	id, repo, thread := read_env_repo(t, "diff")
	base := read_commit(t, repo, nil, read_snapshot(t, repo, map[string]read_file{
		"small.txt": read_text("a\n"),
		"large.txt": read_text("old\n"),
	}), "base")
	head := read_commit(t, repo, []plumbing.Hash{base}, read_snapshot(t, repo, map[string]read_file{
		"small.txt": read_text("b\n"),
		"large.txt": {mode: filemode.Regular, data: bytes.Repeat([]byte("x\n"), int(git_compare_maximum)/2+1)},
	}), "head")
	read_branch(t, repo, "base", base)
	read_branch(t, repo, "head", head)
	return id, repo, thread
}

// TestDiffLeavesOversizedFilesUncompared. changes.Patch() compared every file
// whatever its size, and go-git's line diff runs for minutes on a few
// megabytes of differing lines - under a one-hour internal deadline the
// Starlark call timeout cannot interrupt. A file over git_compare_maximum on
// either side is listed with a note instead.
func TestDiffLeavesOversizedFilesUncompared(t *testing.T) {
	id, _, thread := read_diff_repository(t)
	value, err := read_call(t, thread, api_git_diff, sl.String(id), sl.String("base"), sl.String("head"))
	if err != nil {
		t.Fatalf("diff: %v", err)
	}
	patch, _ := sl.AsString(value)
	if !strings.Contains(patch, "-a\n") || !strings.Contains(patch, "+b\n") {
		t.Errorf("the small file was not diffed:\n%s", patch)
	}
	if !strings.Contains(patch, "diff --git a/large.txt b/large.txt\n# not compared:") {
		t.Errorf("the oversized file carries no note:\n%.400s", patch)
	}
	if strings.Contains(patch, "+x\n") || len(patch) > 4096 {
		t.Errorf("the oversized file was compared and rendered (%d bytes of patch)", len(patch))
	}
}

// TestDiffStatsSkipsOversizedFiles. diff.stats paid the whole comparison to
// return per-file counts; a file too large to compare is reported skipped.
func TestDiffStatsSkipsOversizedFiles(t *testing.T) {
	id, _, thread := read_diff_repository(t)
	value, err := read_call(t, thread, api_git_diff_stats, sl.String(id), sl.String("base"), sl.String("head"))
	if err != nil {
		t.Fatalf("diff.stats: %v", err)
	}
	result := read_dict(t, value)
	files := read_files(t, result["files"])
	if files["small.txt"]["additions"] != 1 || files["small.txt"]["deletions"] != 1 || files["small.txt"]["skipped"] != false {
		t.Errorf("small.txt counts = %v, want one addition and one deletion", files["small.txt"])
	}
	if files["large.txt"]["skipped"] != true || files["large.txt"]["additions"] != 0 {
		t.Errorf("large.txt = %v, want skipped with no counts", files["large.txt"])
	}
	if result["truncated"] != false {
		t.Error("a two-file diff was reported truncated")
	}
}

// read_dict decodes a Starlark dict of scalars, nested lists included.
func read_dict(t *testing.T, value sl.Value) map[string]any {
	t.Helper()
	dict, ok := value.(*sl.Dict)
	if !ok {
		t.Fatalf("result is %T, want dict", value)
	}
	out := map[string]any{}
	for _, item := range dict.Items() {
		key, _ := sl.AsString(item[0])
		out[key] = read_value(t, item[1])
	}
	return out
}

func read_value(t *testing.T, value sl.Value) any {
	t.Helper()
	switch v := value.(type) {
	case sl.String:
		return string(v)
	case sl.Bool:
		return bool(v)
	case sl.Int:
		n, _ := v.Int64()
		return int(n)
	case *sl.List:
		var out []any
		for i := 0; i < v.Len(); i++ {
			out = append(out, read_value(t, v.Index(i)))
		}
		return out
	case sl.Tuple:
		// sl_encode renders a Go slice as a tuple.
		out := []any{}
		for _, item := range v {
			out = append(out, read_value(t, item))
		}
		return out
	case *sl.Dict:
		return read_dict(t, v)
	case sl.NoneType:
		return nil
	}
	t.Fatalf("unexpected Starlark value %T", value)
	return nil
}

// read_files indexes a diff.stats file list by name.
func read_files(t *testing.T, value any) map[string]map[string]any {
	t.Helper()
	out := map[string]map[string]any{}
	list, _ := value.([]any)
	for _, item := range list {
		file, _ := item.(map[string]any)
		name, _ := file["name"].(string)
		out[name] = file
	}
	return out
}

// TestDiffStopsAtTheChangesLimit. A diff of two commits can touch any number
// of files; both the rendered patch and the stats stop at git_changes_maximum
// and say so.
func TestDiffStopsAtTheChangesLimit(t *testing.T) {
	id, repo, thread := read_env_repo(t, "many")
	before := map[string]read_file{"keep": read_text("k")}
	after := map[string]read_file{"keep": read_text("k")}
	for i := 0; i <= git_changes_maximum; i++ {
		after[fmt.Sprintf("f%04d", i)] = read_text("new\n")
	}
	base := read_commit(t, repo, nil, read_snapshot(t, repo, before), "base")
	head := read_commit(t, repo, []plumbing.Hash{base}, read_snapshot(t, repo, after), "head")
	read_branch(t, repo, "base", base)
	read_branch(t, repo, "head", head)

	value, err := read_call(t, thread, api_git_diff, sl.String(id), sl.String("base"), sl.String("head"))
	if err != nil {
		t.Fatalf("diff: %v", err)
	}
	patch, _ := sl.AsString(value)
	if !strings.Contains(patch, "# diff truncated: 1 more files\n") {
		t.Errorf("a diff of %d files carries no truncation note", git_changes_maximum+1)
	}
	if strings.Count(patch, "diff --git ") != git_changes_maximum {
		t.Errorf("rendered %d files, want %d", strings.Count(patch, "diff --git "), git_changes_maximum)
	}

	value, err = read_call(t, thread, api_git_diff_stats, sl.String(id), sl.String("base"), sl.String("head"))
	if err != nil {
		t.Fatalf("diff.stats: %v", err)
	}
	result := read_dict(t, value)
	if result["truncated"] != true {
		t.Error("stats over the changes limit were not reported truncated")
	}
	if files, _ := result["files"].([]any); len(files) != git_changes_maximum {
		t.Errorf("stats listed %d files, want %d", len(files), git_changes_maximum)
	}
}

// TestDiffStopsWhenTheCallIsCancelled. thread.Cancel is only observed between
// interpreter steps, so the 90 s call timeout could not stop a diff already
// running inside Go. Checking the call's context between files means a
// cancelled call stops at the next file instead of running to the end.
func TestDiffStopsWhenTheCallIsCancelled(t *testing.T) {
	id, repo, thread := read_diff_repository(t)
	base, _ := repo.CommitObject(*read_resolve(t, repo, "base"))
	head, _ := repo.CommitObject(*read_resolve(t, repo, "head"))
	base_tree, _ := base.Tree()
	head_tree, _ := head.Tree()
	changes, err := base_tree.Diff(head_tree)
	if err != nil {
		t.Fatalf("diff: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	patch, err := git_patch(ctx, changes)
	if err != nil {
		t.Fatalf("git_patch: %v", err)
	}
	if patch != fmt.Sprintf("# diff truncated: %d more files\n", len(changes)) {
		t.Errorf("a cancelled diff still rendered:\n%.300s", patch)
	}

	thread.SetLocal("context", ctx)
	value, err := read_call(t, thread, api_git_diff_stats, sl.String(id), sl.String("base"), sl.String("head"))
	if err != nil {
		t.Fatalf("diff.stats: %v", err)
	}
	result := read_dict(t, value)
	if result["truncated"] != true {
		t.Error("cancelled stats were not reported truncated")
	}
	if files, _ := result["files"].([]any); len(files) != 0 {
		t.Errorf("cancelled stats still compared %d files", len(files))
	}
}

func read_resolve(t *testing.T, repo *git.Repository, ref string) *plumbing.Hash {
	t.Helper()
	hash, err := git_resolve_ref(repo, ref)
	if err != nil {
		t.Fatalf("resolve %s: %v", ref, err)
	}
	return hash
}

// TestMergeAuthorIsSanitised. go-git writes "name <email>" into the commit
// with no escaping, so a newline in the author name injected a header line -
// an injected committer line replaced the real one on decode - and the
// repositories app forwards both fields from a remote peer's merge event.
func TestMergeAuthorIsSanitised(t *testing.T) {
	id, repo, thread := read_env_repo(t, "merge")
	main := read_commit(t, repo, nil, read_snapshot(t, repo, map[string]read_file{"a": read_text("a")}), "main")
	feature := read_commit(t, repo, []plumbing.Hash{main}, read_snapshot(t, repo, map[string]read_file{
		"a": read_text("a"), "b": read_text("b"),
	}), "feature")
	read_branch(t, repo, "main", main)
	read_branch(t, repo, "feature", feature)

	name := "Eve\ncommitter Mallory <mallory@evil> 1700000000 +0000\ngpgsig fake"
	_, err := read_call(t, thread, api_git_merge_perform,
		sl.String(id), sl.String("feature"), sl.String("main"), sl.String("squash it"),
		sl.String(name), sl.String("<eve@x>\n"), sl.String("squash"))
	if err != nil {
		t.Fatalf("merge: %v", err)
	}

	tip, err := repo.CommitObject(*read_resolve(t, repo, "main"))
	if err != nil {
		t.Fatalf("read the merge commit: %v", err)
	}
	if tip.Committer.Name == "Mallory" {
		t.Error("the injected committer line replaced the real committer")
	}
	if strings.ContainsAny(tip.Author.Name, "<>\r\n") || strings.ContainsAny(tip.Author.Email, "<>\r\n") {
		t.Errorf("the identity reached the commit unsanitised: name %q email %q", tip.Author.Name, tip.Author.Email)
	}
	if tip.Author.Email != "eve@x" {
		t.Errorf("author email = %q, want %q", tip.Author.Email, "eve@x")
	}
}

// TestIdentityStripsWhatGitStrips pins git_identity itself.
func TestIdentityStripsWhatGitStrips(t *testing.T) {
	cases := map[string]string{
		"Plain Name":                  "Plain Name",
		" padded ":                    "padded",
		"a<b>c":                       "abc",
		"line\nbreak\r":               "linebreak",
		"nul\x00byte":                 "nulbyte",
		"\n<committer x@y 1 +0000>\n": "committer x@y 1 +0000",
	}
	for in, want := range cases {
		if got := git_identity(in); got != want {
			t.Errorf("git_identity(%q) = %q, want %q", in, got, want)
		}
	}
}

// TestCommitLogIgnoresSiblingPaths. The path filter was a bare prefix test,
// so the log for "src" included every commit touching "src2/" or "src.txt".
func TestCommitLogIgnoresSiblingPaths(t *testing.T) {
	id, repo, thread := read_env_repo(t, "log")
	first := read_commit(t, repo, nil, read_snapshot(t, repo, map[string]read_file{
		"src/a.txt": read_text("a"),
	}), "touch src")
	second := read_commit(t, repo, []plumbing.Hash{first}, read_snapshot(t, repo, map[string]read_file{
		"src/a.txt": read_text("a"), "src2/b.txt": read_text("b"),
	}), "touch src2")
	third := read_commit(t, repo, []plumbing.Hash{second}, read_snapshot(t, repo, map[string]read_file{
		"src/a.txt": read_text("a"), "src2/b.txt": read_text("b"), "src.txt": read_text("c"),
	}), "touch src.txt")
	read_branch(t, repo, "main", third)

	messages := func(path string) []string {
		value, err := read_call(t, thread, api_git_commit_log, sl.String(id), sl.String("main"), sl.String(path), sl.None)
		if err != nil {
			t.Fatalf("log %q: %v", path, err)
		}
		var out []string
		for _, item := range read_value(t, value).([]any) {
			out = append(out, item.(map[string]any)["message"].(string))
		}
		return out
	}
	if got := messages("src"); len(got) != 1 || got[0] != "touch src" {
		t.Errorf("log for src = %v, want only the commit that touched src/", got)
	}
	if got := messages("src/"); len(got) != 1 {
		t.Errorf("log for src/ = %v, want the same one commit", got)
	}
	if got := messages(""); len(got) != 3 {
		t.Errorf("log for the whole tree = %v, want all three commits", got)
	}
}

// TestCommitListRefusesBadPaging. limit and offset came from sl.AsInt32 with
// the error ignored: a negative limit returned nothing, an absurd limit or
// offset walked the whole history into memory.
func TestCommitListRefusesBadPaging(t *testing.T) {
	id, repo, thread := read_env_repo(t, "page")
	tree := read_snapshot(t, repo, map[string]read_file{"a": read_text("a")})
	read_branch(t, repo, "main", read_commit(t, repo, nil, tree, "only"))

	if _, err := read_call(t, thread, api_git_commit_list, sl.String(id), sl.String("main"), sl.MakeInt(-1), sl.MakeInt(0)); err == nil {
		t.Error("a negative limit was accepted")
	}
	if _, err := read_call(t, thread, api_git_commit_list, sl.String(id), sl.String("main"), sl.MakeInt(5), sl.MakeInt(-1)); err == nil {
		t.Error("a negative offset was accepted")
	}
	if _, err := read_call(t, thread, api_git_commit_list, sl.String(id), sl.String("main"), sl.MakeInt(5), sl.MakeInt(git_offset_maximum+1)); err == nil {
		t.Error("an offset past git_offset_maximum was accepted")
	}
	if _, err := read_call(t, thread, api_git_commit_log, sl.String(id), sl.String("main"), sl.String(""), sl.MakeInt(-1)); err == nil {
		t.Error("commit.log accepted a negative limit")
	}
	if _, err := read_call(t, thread, api_git_commit_list, sl.String(id), sl.String("main"), sl.MakeInt(5), sl.MakeInt(0)); err != nil {
		t.Errorf("a well-formed page was refused: %v", err)
	}

	limit, offset := 5000, 0
	if err := git_page(&limit, &offset); err != nil || limit != git_list_maximum {
		t.Errorf("git_page(5000, 0) = limit %d err %v, want the limit clamped to %d", limit, err, git_list_maximum)
	}
}

// TestCommitBetweenIsBounded. between walked every ancestor of base into a map
// and every ancestor of head into the result. Both walks stop at
// git_offset_maximum commits and the result at git_list_maximum, and the
// visit cap keeps the base cap honest: without it, base ancestors older than
// the map's cut-off would be reported as new.
func TestCommitBetweenIsBounded(t *testing.T) {
	id, repo, thread := read_env_repo(t, "between")
	tree := read_snapshot(t, repo, map[string]read_file{"a": read_text("a")})
	root := read_commit(t, repo, nil, tree, "root")
	tip := root
	for i := 1; i < git_offset_maximum+5; i++ {
		tip = read_commit(t, repo, []plumbing.Hash{tip}, tree, fmt.Sprintf("c%d", i))
	}
	fresh := read_commit(t, repo, []plumbing.Hash{tip}, tree, "fresh")
	read_branch(t, repo, "root", root)
	read_branch(t, repo, "tip", tip)
	read_branch(t, repo, "fresh", fresh)

	count := func(base, head string) int {
		value, err := read_call(t, thread, api_git_commit_between, sl.String(id), sl.String(base), sl.String(head))
		if err != nil {
			t.Fatalf("between %s..%s: %v", base, head, err)
		}
		return len(read_value(t, value).([]any))
	}
	if got := count("tip", "fresh"); got != 1 {
		t.Errorf("tip..fresh = %d commits, want 1 - base ancestors beyond the map cap were reported as new", got)
	}
	if got := count("root", "fresh"); got != git_list_maximum {
		t.Errorf("root..fresh = %d commits, want the result capped at %d", got, git_list_maximum)
	}
}

// TestGitHandlerServesTheEntityItIsGiven. The path-routed handler used to
// re-resolve the repository by fingerprint from users.db although web_action
// had already resolved the entity; there is one handler now, and it needs no
// entities row at all. A request with no entity is refused.
func TestGitHandlerServesTheEntityItIsGiven(t *testing.T) {
	id, owner, _, _ := read_env(t, "served")
	db := db_app_system(owner, test_app)
	db.exec("insert into access ( subject, resource, operation, grant, granter, created ) values ( ?, ?, ?, ?, ?, ? )",
		"*", "repository/"+id, "read", 1, read_identity, now())
	db.close()
	gin.SetMode(gin.TestMode)

	serve := func(e *Entity) *httptest.ResponseRecorder {
		recorder := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(recorder)
		c.Request = httptest.NewRequest("GET", "/x/git/info/refs?service=git-upload-pack", nil)
		if !git_http_handler(c, test_app, owner, nil, e, "info/refs") {
			t.Fatal("handler did not handle the request")
		}
		return recorder
	}
	if recorder := serve(&Entity{ID: id, Class: "repository"}); recorder.Code != http.StatusOK {
		t.Errorf("info/refs for the given entity answered %d: %s", recorder.Code, recorder.Body.String())
	}
	if recorder := serve(nil); recorder.Code != http.StatusNotFound {
		t.Errorf("info/refs with no entity answered %d, want 404", recorder.Code)
	}
}
