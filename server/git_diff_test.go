// Mochi server: what mochi.git.diff and mochi.git.diff.stats report - renames
// paired, a first commit diffed against the empty tree, a diff limited to a
// file or folder, each file's status, and patch text as go-git writes it at a
// cost that does not grow with each line of a file.
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
	"math/rand"
	"sort"
	"strings"
	"testing"

	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/filemode"
	"github.com/go-git/go-git/v5/plumbing/object"
	"github.com/go-git/go-git/v5/storage/memory"
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

// diff_changes answers the changes git_changes reports between two snapshots.
func diff_changes(t testing.TB, repo *git.Repository, before, after map[string]read_file) object.Changes {
	t.Helper()
	base, err := repo.TreeObject(read_snapshot(t, repo, before))
	if err != nil {
		t.Fatalf("base tree: %v", err)
	}
	head, err := repo.TreeObject(read_snapshot(t, repo, after))
	if err != nil {
		t.Fatalf("head tree: %v", err)
	}
	changes, err := git_changes(context.Background(), repo, base, head, "")
	if err != nil {
		t.Fatalf("git_changes: %v", err)
	}
	return changes
}

// diff_statuses counts the statuses git_changes reports between two
// snapshots.
func diff_statuses(t *testing.T, label string, before, after map[string]read_file) map[string]int {
	t.Helper()
	_, repo, _ := read_env_repo(t, label)
	changes := diff_changes(t, repo, before, after)
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

// diff_words are the lines random texts are drawn from, few enough that equal
// lines recur as they do in source.
var diff_words = []string{"alpha", "beta", "gamma", "", "}", "\treturn nil", "delta", "epsilon"}

// diff_random is count lines drawn from diff_words.
func diff_random(random *rand.Rand, count int) []string {
	lines := make([]string, count)
	for i := range lines {
		lines[i] = diff_words[random.Intn(len(diff_words))]
	}
	return lines
}

// diff_edit changes up to four runs of up to six lines: inserted, removed or
// replaced.
func diff_edit(random *rand.Rand, lines []string) []string {
	out := append([]string(nil), lines...)
	for range random.Intn(5) {
		at := random.Intn(len(out) + 1)
		count := 1 + random.Intn(6)
		end := min(at+count, len(out))
		switch random.Intn(3) {
		case 0:
			out = append(out[:at:at], append(diff_random(random, count), out[at:]...)...)
		case 1:
			out = append(out[:at:at], out[end:]...)
		default:
			copy(out[at:end], diff_random(random, end-at))
		}
	}
	return out
}

// diff_join is lines as a file, with a final newline or without one.
func diff_join(lines []string, newline bool) read_file {
	text := strings.Join(lines, "\n")
	if newline && len(lines) > 0 {
		text += "\n"
	}
	return read_text(text)
}

// TestDiffTextMatchesGoGit. The patch text comes from a copy of go-git's
// encoder that splits lines without a regular expression. Every kind of
// change reads as go-git's own patch.String() writes it.
func TestDiffTextMatchesGoGit(t *testing.T) {
	_, repo, _ := read_env_repo(t, "text")
	executable := func(s string) read_file { return read_file{mode: filemode.Executable, data: []byte(s)} }
	long := diff_lines("long", 200, -1)
	before := map[string]read_file{
		"modify/middle":   long,
		"modify/close":    long,
		"modify/far":      long,
		"modify/first":    long,
		"modify/last":     long,
		"newline/remove":  read_text("a\nb\n"),
		"newline/add":     read_text("a\nb"),
		"newline/neither": read_text("a\nb"),
		"blank":           read_text("\n\nx\n\n\ny\n\n"),
		"crlf":            read_text("a\r\nb\r\nc\r\n"),
		"invalid":         read_text("\xff\xfe one\nmid\n\xc3 two\n"),
		"empty/emptied":   read_text("x\ny\n"),
		"empty/filled":    read_text(""),
		"empty/deleted":   read_text(""),
		"deleted":         diff_lines("deleted", 30, -1),
		"binary/changed":  read_text("\x00\x01binary\n"),
		"binary/deleted":  read_text("\x00gone"),
		"mode/only":       read_text("same\n"),
		"mode/edited":     read_text("one\ntwo\n"),
		"old/moved":       diff_lines("moved", 50, -1),
	}
	after := map[string]read_file{
		"modify/middle":   diff_lines("long", 200, 100),
		"modify/close":    read_text(strings.Replace(strings.Replace(string(long.data), "long line 50\n", "fifty\n", 1), "long line 55\n", "", 1)),
		"modify/far":      read_text(strings.Replace(strings.Replace(string(long.data), "long line 20\n", "twenty\n", 1), "long line 120\n", "a\nb\nc\n", 1)),
		"modify/first":    diff_lines("long", 200, 0),
		"modify/last":     diff_lines("long", 200, 199),
		"newline/remove":  read_text("a\nb"),
		"newline/add":     read_text("a\nb\n"),
		"newline/neither": read_text("a\nc"),
		"blank":           read_text("\n\nx\n\nz\ny\n"),
		"crlf":            read_text("a\r\nB\r\nc\r\n"),
		"invalid":         read_text("\xff\xfe one\nmiddle\n\xc3 two\n"),
		"empty/emptied":   read_text(""),
		"empty/filled":    read_text("x\n"),
		"empty/added":     read_text(""),
		"added":           diff_lines("added", 30, -1),
		"unended":         read_text("one\ntwo"),
		"binary/changed":  read_text("\x00\x02binary\n"),
		"binary/added":    read_text("\x00new"),
		"mode/only":       executable("same\n"),
		"mode/edited":     executable("one\n2\n"),
		"new/moved":       diff_lines("moved", 50, 25),
	}
	random := rand.New(rand.NewSource(1))
	for i := range 300 {
		lines := diff_random(random, random.Intn(61))
		name := fmt.Sprintf("random/%03d", i)
		before[name] = diff_join(lines, random.Intn(4) > 0)
		after[name] = diff_join(diff_edit(random, lines), random.Intn(4) > 0)
	}

	changes := diff_changes(t, repo, before, after)
	var all strings.Builder
	for _, change := range changes {
		name, _, _ := git_status(change)
		patch, err := object.Changes{change}.Patch()
		if err != nil {
			t.Fatalf("%s: patch: %v", name, err)
		}
		want := patch.String()
		all.WriteString(want)
		got, err := git_patch(context.Background(), object.Changes{change})
		if err != nil {
			t.Fatalf("%s: git_patch: %v", name, err)
		}
		if got != want {
			t.Errorf("%s differs from go-git's text:\n--- go-git\n%s--- git_patch\n%s", name, want, got)
		}
	}

	// The fixtures reach every part of the format.
	text := all.String()
	for _, part := range []string{"new file mode ", "deleted file mode ", "old mode 100644\nnew mode 100755\n",
		"rename from old/moved\nrename to new/moved\n", "Binary files ", "\\ No newline at end of file\n", "\r\n", "\xff"} {
		if !strings.Contains(text, part) {
			t.Errorf("no fixture's text holds %q", part)
		}
	}
	if count := strings.Count(diff_file(text, "modify/far"), "\n@@ "); count != 2 {
		t.Errorf("modify/far has %d hunks, want 2", count)
	}
	if count := strings.Count(diff_file(text, "modify/close"), "\n@@ "); count != 1 {
		t.Errorf("modify/close has %d hunks, want 1", count)
	}
	if count := strings.Count(text, "diff --git a/random/"); count < 200 {
		t.Errorf("%d random files compared, want at least 200", count)
	}
}

// diff_file is one file's part of a patch's text.
func diff_file(text, name string) string {
	start := strings.Index(text, "diff --git a/"+name+" ")
	if start < 0 {
		return ""
	}
	end := strings.Index(text[start+1:], "diff --git ")
	if end < 0 {
		return text[start:]
	}
	return text[start : start+1+end]
}

// diff_edited is one line edited in the middle of a file of count lines.
func diff_edited(t testing.TB, repo *git.Repository, count int) object.Changes {
	t.Helper()
	return diff_changes(t, repo,
		map[string]read_file{"file": diff_lines("text", count, -1)},
		map[string]read_file{"file": diff_lines("text", count, count/2)})
}

// TestDiffTextCostDoesNotGrowPerLine. go-git's encoder split every chunk into
// lines with a regular expression, one allocation for each line of the file
// however small the edit, and 40% of the time a diff of large files took.
func TestDiffTextCostDoesNotGrowPerLine(t *testing.T) {
	_, repo, _ := read_env_repo(t, "perline")
	allocations := func(count int) float64 {
		changes := diff_edited(t, repo, count)
		return testing.AllocsPerRun(5, func() {
			if _, err := git_patch(context.Background(), changes); err != nil {
				t.Fatalf("git_patch: %v", err)
			}
		})
	}
	small, large := allocations(2000), allocations(8000)
	if grown := large - small; grown > 600 {
		t.Errorf("a one-line edit to 8,000 lines took %.0f allocations more than to 2,000 lines, want at most 600", grown)
	}
}

// BenchmarkDiffText times the text of a one-line edit to a file of 5,000
// lines, against go-git's own encoder of the same change.
func BenchmarkDiffText(b *testing.B) {
	repo, err := git.Init(memory.NewStorage(), nil)
	if err != nil {
		b.Fatalf("init: %v", err)
	}
	changes := diff_edited(b, repo, 5000)
	b.Run("git_patch", func(b *testing.B) {
		for b.Loop() {
			git_patch(context.Background(), changes)
		}
	})
	b.Run("go-git", func(b *testing.B) {
		for b.Loop() {
			patch, _ := changes.Patch()
			_ = patch.String()
		}
	})
}
