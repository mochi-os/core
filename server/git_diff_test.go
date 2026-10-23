// Mochi server: what mochi.git.diff and mochi.git.diff.stats report - renames
// paired, a first commit diffed against the empty tree, a diff limited to a
// file or folder, and each file's status.
//
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/filemode"
	sl "go.starlark.net/starlark"
)

// diff_lines is a text file of count numbered lines, the one at edit changed
// when edit is not negative.
func diff_lines(label string, count, edit int) read_file {
	var out strings.Builder
	for i := 0; i < count; i++ {
		if i == edit {
			fmt.Fprintf(&out, "%s line %d edited\n", label, i)
		} else {
			fmt.Fprintf(&out, "%s line %d\n", label, i)
		}
	}
	return read_text(out.String())
}

// diff_repository commits before and after on branches "base" and "head".
func diff_repository(t *testing.T, label string, before, after map[string]read_file) (string, *sl.Thread) {
	t.Helper()
	id, repo, thread := read_env_repo(t, label)
	base := read_commit(t, repo, nil, read_snapshot(t, repo, before), "base")
	head := read_commit(t, repo, []plumbing.Hash{base}, read_snapshot(t, repo, after), "head")
	read_branch(t, repo, "base", base)
	read_branch(t, repo, "head", head)
	return id, thread
}

// diff_patch calls mochi.git.diff and answers its text.
func diff_patch(t *testing.T, thread *sl.Thread, args ...sl.Value) string {
	t.Helper()
	value, err := read_call(t, thread, api_git_diff, args...)
	if err != nil {
		t.Fatalf("diff: %v", err)
	}
	patch, _ := sl.AsString(value)
	return patch
}

// diff_stats calls mochi.git.diff.stats and answers its result and its files
// by name.
func diff_stats(t *testing.T, thread *sl.Thread, args ...sl.Value) (map[string]any, map[string]map[string]any) {
	t.Helper()
	value, err := read_call(t, thread, api_git_diff_stats, args...)
	if err != nil {
		t.Fatalf("diff.stats: %v", err)
	}
	result := read_dict(t, value)
	return result, read_files(t, result["files"])
}

// diff_headers lists the "diff --git" lines of a patch, sorted.
func diff_headers(patch string) []string {
	var out []string
	for _, line := range strings.Split(patch, "\n") {
		if strings.HasPrefix(line, "diff --git ") {
			out = append(out, line)
		}
	}
	sort.Strings(out)
	return out
}

// diff_names lists the names of a stats file list, sorted.
func diff_names(files map[string]map[string]any) []string {
	var out []string
	for name := range files {
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}

// TestDiffReportsRenames. The tree diff had no rename detection, so a file
// moved showed as one deleted and another added, and the history of a moved
// file stopped at the move. A move with an edit pairs by content, a move
// without one by hash, and an unedited move of a file too large to compare
// still pairs and has nothing to compare.
func TestDiffReportsRenames(t *testing.T) {
	large := read_file{mode: filemode.Regular, data: bytes.Repeat([]byte("y\n"), int(git_compare_maximum)/2+1)}
	id, thread := diff_repository(t, "rename",
		map[string]read_file{
			"old/edited.txt": diff_lines("edited", 20, -1),
			"same.txt":       diff_lines("same", 5, -1),
			"big/data.txt":   large,
		},
		map[string]read_file{
			"new/edited.txt":  diff_lines("edited", 20, 7),
			"moved.txt":       diff_lines("same", 5, -1),
			"assets/data.txt": large,
		})

	patch := diff_patch(t, thread, sl.String(id), sl.String("base"), sl.String("head"))
	for _, want := range []string{
		"diff --git a/old/edited.txt b/new/edited.txt\nrename from old/edited.txt\nrename to new/edited.txt\n",
		"-edited line 7\n+edited line 7 edited\n",
		"diff --git a/same.txt b/moved.txt\nrename from same.txt\nrename to moved.txt\n",
		"diff --git a/big/data.txt b/assets/data.txt\nrename from big/data.txt\nrename to assets/data.txt\n",
	} {
		if !strings.Contains(patch, want) {
			t.Errorf("patch lacks %q:\n%.2000s", want, patch)
		}
	}
	if strings.Contains(patch, "new file mode") || strings.Contains(patch, "deleted file mode") || strings.Contains(patch, "# not compared") {
		t.Errorf("a moved file was reported added, deleted or uncompared:\n%.2000s", patch)
	}

	result, files := diff_stats(t, thread, sl.String(id), sl.String("base"), sl.String("head"))
	if got := diff_names(files); strings.Join(got, ",") != "assets/data.txt,moved.txt,new/edited.txt" {
		t.Fatalf("stats list %v, want the three new paths", got)
	}
	for name, want := range map[string]map[string]any{
		"new/edited.txt":  {"status": "renamed", "previous": "old/edited.txt", "additions": 1, "deletions": 1, "skipped": false},
		"moved.txt":       {"status": "renamed", "previous": "same.txt", "additions": 0, "deletions": 0, "skipped": false},
		"assets/data.txt": {"status": "renamed", "previous": "big/data.txt", "additions": 0, "deletions": 0, "skipped": false},
	} {
		for key, value := range want {
			if files[name][key] != value {
				t.Errorf("%s %s = %v, want %v", name, key, files[name][key], value)
			}
		}
	}
	if result["additions"] != 1 || result["deletions"] != 1 {
		t.Errorf("totals = %v/%v, want 1/1", result["additions"], result["deletions"])
	}
}

// TestDiffStatsReportsStatus. Each file says what happened to it, and only a
// rename names a previous path.
func TestDiffStatsReportsStatus(t *testing.T) {
	id, thread := diff_repository(t, "status",
		map[string]read_file{
			"kept.txt":    read_text("a\n"),
			"removed.txt": read_text("gone\n"),
			"image.bin":   {mode: filemode.Regular, data: []byte("\x00one")},
		},
		map[string]read_file{
			"kept.txt":  read_text("b\n"),
			"added.txt": read_text("new\nfile\n"),
			"image.bin": {mode: filemode.Regular, data: []byte("\x00two")},
		})

	_, files := diff_stats(t, thread, sl.String(id), sl.String("base"), sl.String("head"))
	for name, want := range map[string]map[string]any{
		"kept.txt":    {"status": "modified", "previous": "", "additions": 1, "deletions": 1},
		"added.txt":   {"status": "added", "previous": "", "additions": 2, "deletions": 0},
		"removed.txt": {"status": "deleted", "previous": "", "additions": 0, "deletions": 1},
		"image.bin":   {"status": "modified", "previous": "", "additions": 0, "deletions": 0},
	} {
		if files[name] == nil {
			t.Errorf("%s is not listed: %v", name, files)
			continue
		}
		for key, value := range want {
			if files[name][key] != value {
				t.Errorf("%s %s = %v, want %v", name, key, files[name][key], value)
			}
		}
	}
}

// TestDiffOfAFirstCommit. A first commit has no parent, and a diff refused a
// missing base, so its changes could not be shown. None or "" is the empty
// tree: every file in head is added.
func TestDiffOfAFirstCommit(t *testing.T) {
	id, repo, thread := read_env_repo(t, "first")
	read_branch(t, repo, "main", read_commit(t, repo, nil, read_snapshot(t, repo, map[string]read_file{
		"readme.md":   read_text("hello\n"),
		"src/main.go": read_text("package main\n"),
	}), "first"))

	for _, base := range []sl.Value{sl.None, sl.String("")} {
		patch := diff_patch(t, thread, sl.String(id), base, sl.String("main"))
		if got := diff_headers(patch); strings.Join(got, "\n") != "diff --git a/readme.md b/readme.md\ndiff --git a/src/main.go b/src/main.go" {
			t.Errorf("base %v: patch files = %v", base, got)
		}
		if strings.Count(patch, "new file mode") != 2 || !strings.Contains(patch, "+package main\n") {
			t.Errorf("base %v: files were not shown as added:\n%s", base, patch)
		}

		_, files := diff_stats(t, thread, sl.String(id), base, sl.String("main"))
		if len(files) != 2 {
			t.Errorf("base %v: stats list %v, want two files", base, diff_names(files))
		}
		for name, file := range files {
			if file["status"] != "added" || file["previous"] != "" || file["additions"] != 1 || file["deletions"] != 0 {
				t.Errorf("base %v: %s = %v, want added with one line", base, name, file)
			}
		}
	}

	if _, err := read_call(t, thread, api_git_diff, sl.String(id), sl.String("main"), sl.None); err == nil {
		t.Error("a diff with no head was accepted")
	}
}

// diff_tree is a base and head with changes inside src, beside it in srcfoo,
// elsewhere in docs, and a file moved from lib into src.
func diff_tree(t *testing.T) (string, *sl.Thread) {
	t.Helper()
	return diff_repository(t, "filter",
		map[string]read_file{
			"src/a.go":     read_text("a\n"),
			"src/b.go":     read_text("b\n"),
			"srcfoo/c.go":  read_text("c\n"),
			"docs/d.md":    read_text("d\n"),
			"lib/moved.go": diff_lines("moved", 20, -1),
		},
		map[string]read_file{
			"src/a.go":     read_text("a2\n"),
			"src/new.go":   read_text("new\n"),
			"srcfoo/c.go":  read_text("c2\n"),
			"docs/d.md":    read_text("d2\n"),
			"src/moved.go": diff_lines("moved", 20, 3),
		})
}

// TestDiffLimitedToAPath. A file's history and a folder's view need the
// changes to that file or folder only. Paths match on whole segments, so
// "src" is not "srcfoo", and a rename matches by either of its paths.
func TestDiffLimitedToAPath(t *testing.T) {
	id, thread := diff_tree(t)
	cases := []struct {
		path    string
		headers []string
		names   []string
	}{
		{"src/a.go", []string{"diff --git a/src/a.go b/src/a.go"}, []string{"src/a.go"}},
		{"src", []string{
			"diff --git a/lib/moved.go b/src/moved.go",
			"diff --git a/src/a.go b/src/a.go",
			"diff --git a/src/b.go b/src/b.go",
			"diff --git a/src/new.go b/src/new.go",
		}, []string{"src/a.go", "src/b.go", "src/moved.go", "src/new.go"}},
		{"src/", []string{
			"diff --git a/lib/moved.go b/src/moved.go",
			"diff --git a/src/a.go b/src/a.go",
			"diff --git a/src/b.go b/src/b.go",
			"diff --git a/src/new.go b/src/new.go",
		}, []string{"src/a.go", "src/b.go", "src/moved.go", "src/new.go"}},
		{"srcfoo", []string{"diff --git a/srcfoo/c.go b/srcfoo/c.go"}, []string{"srcfoo/c.go"}},
		{"sr", nil, nil},
		{"lib", []string{"diff --git a/lib/moved.go b/src/moved.go"}, []string{"src/moved.go"}},
		{"src/moved.go", []string{"diff --git a/lib/moved.go b/src/moved.go"}, []string{"src/moved.go"}},
	}
	for _, c := range cases {
		patch := diff_patch(t, thread, sl.String(id), sl.String("base"), sl.String("head"), sl.String(c.path))
		if got := diff_headers(patch); strings.Join(got, "\n") != strings.Join(c.headers, "\n") {
			t.Errorf("path %q: patch files = %v, want %v", c.path, got, c.headers)
		}
		result, files := diff_stats(t, thread, sl.String(id), sl.String("base"), sl.String("head"), sl.String(c.path))
		if got := diff_names(files); strings.Join(got, ",") != strings.Join(c.names, ",") {
			t.Errorf("path %q: stats list %v, want %v", c.path, got, c.names)
		}
		if result["truncated"] != false {
			t.Errorf("path %q: reported truncated", c.path)
		}
	}

	_, files := diff_stats(t, thread, sl.String(id), sl.String("base"), sl.String("head"), sl.String("src"))
	if files["src/moved.go"]["status"] != "renamed" || files["src/moved.go"]["previous"] != "lib/moved.go" {
		t.Errorf("a file moved into the folder = %v, want renamed from lib/moved.go", files["src/moved.go"])
	}

	// None and "" are no filter.
	for _, path := range []sl.Value{sl.None, sl.String("")} {
		_, files := diff_stats(t, thread, sl.String(id), sl.String("base"), sl.String("head"), path)
		if len(files) != 6 {
			t.Errorf("path %v: stats list %v, want all six files", path, diff_names(files))
		}
	}
}

// TestDiffPathFilterPrecedesTheCap. The cap on files is for what is shown;
// a folder's changes are not lost behind a thousand changes elsewhere.
func TestDiffPathFilterPrecedesTheCap(t *testing.T) {
	after := map[string]read_file{"keep": read_text("k"), "wanted/file.txt": read_text("here\n")}
	for i := 0; i <= git_changes_maximum; i++ {
		after[fmt.Sprintf("many/f%04d", i)] = read_text("new\n")
	}
	id, thread := diff_repository(t, "precede", map[string]read_file{"keep": read_text("k")}, after)

	patch := diff_patch(t, thread, sl.String(id), sl.String("base"), sl.String("head"), sl.String("wanted"))
	if got := diff_headers(patch); len(got) != 1 || strings.Contains(patch, "# diff truncated") {
		t.Errorf("filtered patch = %v, want wanted/file.txt alone and untruncated", got)
	}
	result, files := diff_stats(t, thread, sl.String(id), sl.String("base"), sl.String("head"), sl.String("wanted"))
	if len(files) != 1 || files["wanted/file.txt"] == nil || result["truncated"] != false {
		t.Errorf("filtered stats = %v truncated %v, want wanted/file.txt alone", diff_names(files), result["truncated"])
	}
}

// TestDiffNotesNameTheFullPath. The note for a file too large to compare
// named it by the last segment of its path, so a nested file's header pointed
// at a file that does not exist.
func TestDiffNotesNameTheFullPath(t *testing.T) {
	id, thread := diff_repository(t, "nested",
		map[string]read_file{"deep/folder/large.txt": read_text("old\n")},
		map[string]read_file{"deep/folder/large.txt": {mode: filemode.Regular, data: bytes.Repeat([]byte("x\n"), int(git_compare_maximum)/2+1)}})

	patch := diff_patch(t, thread, sl.String(id), sl.String("base"), sl.String("head"))
	if !strings.HasPrefix(patch, "diff --git a/deep/folder/large.txt b/deep/folder/large.txt\n# not compared:") {
		t.Errorf("note = %.200q", patch)
	}
	_, files := diff_stats(t, thread, sl.String(id), sl.String("base"), sl.String("head"))
	if files["deep/folder/large.txt"]["skipped"] != true {
		t.Errorf("stats = %v, want deep/folder/large.txt skipped", files)
	}
}

// diff_statuses counts the statuses git_changes reports between two
// snapshots.
func diff_statuses(t *testing.T, label string, before, after map[string]read_file) map[string]int {
	t.Helper()
	_, repo, _ := read_env_repo(t, label)
	trees := []map[string]read_file{before, after}
	var roots []plumbing.Hash
	for _, files := range trees {
		roots = append(roots, read_snapshot(t, repo, files))
	}
	base, err := repo.TreeObject(roots[0])
	if err != nil {
		t.Fatalf("base tree: %v", err)
	}
	head, err := repo.TreeObject(roots[1])
	if err != nil {
		t.Fatalf("head tree: %v", err)
	}
	changes, err := git_changes(context.Background(), repo, base, head, "")
	if err != nil {
		t.Fatalf("git_changes: %v", err)
	}
	out := map[string]int{}
	for _, change := range changes {
		_, _, status := git_status(change)
		out[status]++
	}
	return out
}

// TestDiffRenameLimits. go-git's rename matching cannot be interrupted, and
// scores every deleted file against every added one, so a commit moving
// thousands of files is not matched at all, and moves with edits are matched
// by content only while the pairs stay under git_pairs_maximum.
func TestDiffRenameLimits(t *testing.T) {
	before := map[string]read_file{}
	after := map[string]read_file{}
	for i := 0; i <= git_rename_maximum; i++ {
		before[fmt.Sprintf("old/f%04d", i)] = read_text(fmt.Sprintf("file %d\n", i))
		after[fmt.Sprintf("new/f%04d", i)] = read_text(fmt.Sprintf("file %d\n", i))
	}
	if got := diff_statuses(t, "manymoves", before, after); got["renamed"] != 0 || got["added"] != git_rename_maximum+1 {
		t.Errorf("%d files moved: %v, want none paired", git_rename_maximum+1, got)
	}
	delete(before, fmt.Sprintf("old/f%04d", git_rename_maximum))
	delete(after, fmt.Sprintf("new/f%04d", git_rename_maximum))
	if got := diff_statuses(t, "moves", before, after); got["renamed"] != git_rename_maximum {
		t.Errorf("%d files moved: %v, want all paired", git_rename_maximum, got)
	}

	// 101 deleted against 100 added is past git_pairs_maximum; 100 against
	// 100 is not.
	before = map[string]read_file{}
	after = map[string]read_file{}
	for i := 0; i < 100; i++ {
		before[fmt.Sprintf("old/f%03d", i)] = diff_lines(fmt.Sprint(i), 20, -1)
		after[fmt.Sprintf("new/f%03d", i)] = diff_lines(fmt.Sprint(i), 20, 5)
	}
	if got := diff_statuses(t, "edits", before, after); got["renamed"] != 100 {
		t.Errorf("100 files moved and edited: %v, want all paired", got)
	}
	before["old/extra"] = read_text("unrelated\n")
	if got := diff_statuses(t, "manyedits", before, after); got["renamed"] != 0 {
		t.Errorf("101 deleted against 100 added: %v, want none paired by content", got)
	}
}
