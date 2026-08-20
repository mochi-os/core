// Mochi server: app assets are read through an os.Root, not a symlink check.
//
// The five app-asset readers used to join the caller's path onto the app
// directory and then ask file_is_symlink about the result. os.Lstat declines
// to follow the FINAL component and follows every intermediate one, so a
// symlinked parent - "assets/secret" where assets points out of the app
// directory - resolved to the far side, reported the target's mode, and passed
// a check that was only ever looking at the leaf.
//
// os.Root refuses symlink traversal at any depth as a property of the API, so
// there is no ordering to get wrong and no shape to miss. Seventeen other
// sites in this package already read that way; these five were the holdouts.
//
// Installed packages cannot carry symlinks, so this only ever mattered for a
// dev app, where whoever authors the app can write its directory.
//
// Copyright © 2026 Mochisoft OU
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	sl "go.starlark.net/starlark"
)

// asset_symlink_fixture builds an app directory containing a legitimate file,
// a symlink whose TARGET is outside (the shape the old leaf check caught), and
// a symlinked DIRECTORY through which an outside file is reachable (the shape
// it missed). Returns the app directory and the outside directory.
func asset_symlink_fixture(t *testing.T) (string, string) {
	t.Helper()
	base := t.TempDir()
	outside := t.TempDir()

	if err := os.WriteFile(filepath.Join(outside, "secret"), []byte("private"), 0o600); err != nil {
		t.Fatalf("writing the outside file: %v", err)
	}
	if err := os.WriteFile(filepath.Join(base, "real.txt"), []byte("mine"), 0o644); err != nil {
		t.Fatalf("writing the app file: %v", err)
	}
	if err := os.Mkdir(filepath.Join(base, "sub"), 0o755); err != nil {
		t.Fatalf("mkdir sub: %v", err)
	}
	if err := os.WriteFile(filepath.Join(base, "sub", "nested.txt"), []byte("nested"), 0o644); err != nil {
		t.Fatalf("writing the nested file: %v", err)
	}
	// Leaf symlink: the old check caught this one.
	if err := os.Symlink(filepath.Join(outside, "secret"), filepath.Join(base, "leaf")); err != nil {
		t.Skipf("symlinks unavailable here: %v", err)
	}
	// Symlinked directory: the old check followed it and never noticed.
	if err := os.Symlink(outside, filepath.Join(base, "parent")); err != nil {
		t.Skipf("symlinks unavailable here: %v", err)
	}
	return base, outside
}

// TestAssetRootRefusesSymlinksAtEveryDepth is the regression proper.
func TestAssetRootRefusesSymlinksAtEveryDepth(t *testing.T) {
	base, _ := asset_symlink_fixture(t)

	root, err := os.OpenRoot(base)
	if err != nil {
		t.Fatalf("OpenRoot: %v", err)
	}
	defer root.Close()

	for _, escape := range []string{
		"parent/secret", // symlinked intermediate directory - the missed shape
		"leaf",          // symlinked leaf - the caught shape
	} {
		if _, err := root.Open(escape); err == nil {
			t.Errorf("root.Open(%q) succeeded; the app directory does not contain that file", escape)
		}
		if _, err := root.Stat(escape); err == nil {
			t.Errorf("root.Stat(%q) succeeded; the app directory does not contain that file", escape)
		}
	}

	// And the honest paths still work, so the fix is not simply "refuse
	// everything".
	for _, allowed := range []string{"real.txt", "sub/nested.txt"} {
		f, err := root.Open(allowed)
		if err != nil {
			t.Errorf("root.Open(%q) failed on a genuine app file: %v", allowed, err)
			continue
		}
		f.Close()
	}
}

// TestLeafOnlyCheckMissedTheSymlinkedParent pins WHY the old guard was
// insufficient, so the reasoning survives even if os.Root is ever swapped out.
// os.Lstat on a path whose parent is a symlink reports the TARGET, not a link.
func TestLeafOnlyCheckMissedTheSymlinkedParent(t *testing.T) {
	base, _ := asset_symlink_fixture(t)

	through := filepath.Join(base, "parent", "secret")
	information, err := os.Lstat(through)
	if err != nil {
		t.Fatalf("Lstat through the symlinked parent: %v", err)
	}
	if information.Mode()&os.ModeSymlink != 0 {
		t.Fatal("Lstat reported a symlink; this platform does not follow intermediate links, and the premise of this test does not hold here")
	}
	if data, err := os.ReadFile(through); err != nil || string(data) != "private" {
		t.Fatalf("reading through the symlinked parent gave (%q, %v); the fixture is not set up as intended", data, err)
	}
	// So: a leaf-only symlink test sees a regular file and lets this through.
}

// TestAppAssetPathContains is the helper the two path-consuming callers use -
// a.write.asset, which hands the path to gin, and the stream writer, which
// hands it to os.Open. They cannot take a handle without changing what they
// call, so the helper establishes containment through the same os.Root and
// returns the path only when that succeeds.
func TestAppAssetPathContains(t *testing.T) {
	base, _ := asset_symlink_fixture(t)
	// internal is the branch App.active resolves first, with no database
	// behind it - the shortest way to give this app one active version.
	app := &App{id: "test", internal: &AppVersion{Version: "1.0", base: base}}

	if got := app_asset_path(app, nil, "real.txt"); got != filepath.Join(base, "real.txt") {
		t.Errorf("app_asset_path(real.txt) = %q, want the file inside the app directory", got)
	}
	if got := app_asset_path(app, nil, "sub/nested.txt"); got == "" {
		t.Error("app_asset_path refused a genuine nested app file")
	}
	for _, escape := range []string{"parent/secret", "leaf"} {
		if got := app_asset_path(app, nil, escape); got != "" {
			t.Errorf("app_asset_path(%q) = %q; it resolves outside the app directory and must be refused", escape, got)
		}
	}
	if got := app_asset_path(app, nil, "absent.txt"); got != "" {
		t.Errorf("app_asset_path(absent.txt) = %q, want \"\" for a file that does not exist", got)
	}
}

// TestNoAssetReaderUsesALeafOnlySymlinkCheck is the gate. file_is_symlink is
// gone; if it comes back it will come back next to a joined path, which is
// exactly the pattern that was wrong.
func TestNoAssetReaderUsesALeafOnlySymlinkCheck(t *testing.T) {
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("reading the package directory: %v", err)
	}
	for _, entry := range entries {
		name := entry.Name()
		if !strings.HasSuffix(name, ".go") || name == "app_asset_root_test.go" {
			continue
		}
		data, err := os.ReadFile(name)
		if err != nil {
			t.Fatalf("reading %s: %v", name, err)
		}
		source := string(data)
		if strings.Contains(source, "func file_is_symlink(") || strings.Contains(source, "file_is_symlink(") {
			t.Errorf("%s uses file_is_symlink; an os.Lstat on a joined path follows every intermediate symlink, so it only ever tested the last component. Read app assets through app_asset_root instead", name)
		}
		if strings.Contains(source, "app_local_path(") {
			t.Errorf("%s uses app_local_path, which returned an unvalidated join; use app_asset_root or app_asset_path", name)
		}
	}
}

// TestAssetApisRefuseASymlinkedParent drives the three exported APIs, not the
// mechanism underneath them. The tests above would pass against the old
// leaf-only check too - os.Root has always behaved this way, and the helper
// they exercise is new - so this is the one that fails if the APIs are ever
// wired back to a joined path.
func TestAssetApisRefuseASymlinkedParent(t *testing.T) {
	base, _ := asset_symlink_fixture(t)
	app := &App{id: "test", internal: &AppVersion{Version: "1.0", base: base}}

	thread := &sl.Thread{Name: "test"}
	thread.SetLocal("app", app)
	thread.SetLocal("user", (*User)(nil))

	read := sl.NewBuiltin("mochi.app.asset.read", api_app_asset_read)
	exists := sl.NewBuiltin("mochi.app.asset.exists", api_app_asset_exists)
	list := sl.NewBuiltin("mochi.app.asset.list", api_app_asset_list)

	// The genuine file first, so a blanket refusal cannot pass this test.
	if value, err := api_app_asset_read(thread, read, sl.Tuple{sl.String("real.txt")}, nil); err != nil {
		t.Fatalf("mochi.app.asset.read could not read a genuine app file: %v", err)
	} else if got := string(value.(sl.Bytes)); got != "mine" {
		t.Fatalf("read a genuine app file as %q, want %q", got, "mine")
	}

	for _, escape := range []string{"parent/secret", "leaf"} {
		if value, err := api_app_asset_read(thread, read, sl.Tuple{sl.String(escape)}, nil); err == nil {
			t.Errorf("mochi.app.asset.read(%q) returned %q; that file is outside the app directory", escape, value)
		}
		if value, err := api_app_asset_exists(thread, exists, sl.Tuple{sl.String(escape)}, nil); err == nil && value == sl.True {
			t.Errorf("mochi.app.asset.exists(%q) said true; that file is outside the app directory", escape)
		}
	}

	// list through a symlinked directory must not enumerate it either.
	if value, err := api_app_asset_list(thread, list, sl.Tuple{sl.String("parent")}, nil); err == nil {
		// sl_encode returns a Tuple here, so read it through Indexable
		// rather than asserting a concrete list type.
		if encoded, ok := value.(sl.Indexable); ok && encoded.Len() > 0 {
			t.Errorf("mochi.app.asset.list(%q) enumerated %d entries outside the app directory", "parent", encoded.Len())
		}
	}
	// And it still lists a real subdirectory.
	value, err := api_app_asset_list(thread, list, sl.Tuple{sl.String("sub")}, nil)
	if err != nil {
		t.Fatalf("mochi.app.asset.list could not list a genuine app directory: %v", err)
	}
	if encoded, ok := value.(sl.Indexable); !ok || encoded.Len() != 1 {
		t.Errorf("listing a genuine app directory gave %v, want one entry", value)
	}
}
