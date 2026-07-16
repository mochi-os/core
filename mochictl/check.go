// mochictl: pre-deploy validation subcommands.
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.
//
// `mochictl check starlark <path>` - parse every .star file under
// the given path (or the file itself) using the same go.starlark.net
// parser the server uses at load time. Returns non-zero on the first
// parse error with file:line:col + reason. Intended for the deploy.sh
// pre-flight check so a Python-ism like implicit string concatenation
// (a valid Python construct that Starlark rejects) fails the deploy
// locally rather than silently breaking every action in the app once
// the server tries to load the deployed bundle. See
// claude/memory/feedback_starlark_no_implicit_concat.md and task #89.

package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"go.starlark.net/syntax"
)

// cmd_check_starlark handles `mochictl check starlark <path>`.
//
// Single-file mode: parse exactly the named file.
// Directory mode: walk recursively, parse every *.star file.
//
// Errors stop at the first failure so the caller (deploy.sh, /commit
// hook, ad-hoc dev check) gets immediate feedback on the bad file
// rather than a wall of redundant noise from downstream files that
// were going to fail too.
func cmd_check_starlark(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: mochictl check starlark <file-or-directory>")
	}
	root := args[0]
	info, err := os.Stat(root)
	if err != nil {
		return fmt.Errorf("stat %q: %v", root, err)
	}

	var paths []string
	if info.IsDir() {
		err := filepath.WalkDir(root, func(p string, d os.DirEntry, walk_err error) error {
			if walk_err != nil {
				return walk_err
			}
			if d.IsDir() {
				// Skip the directories that never contain runtime
				// Starlark - they bulk-up the walk and any .star
				// files there are by definition not app code.
				name := d.Name()
				if name == ".git" || name == "node_modules" || name == "web" {
					return filepath.SkipDir
				}
				return nil
			}
			if strings.HasSuffix(p, ".star") {
				paths = append(paths, p)
			}
			return nil
		})
		if err != nil {
			return fmt.Errorf("walk %q: %v", root, err)
		}
	} else {
		paths = []string{root}
	}

	if len(paths) == 0 {
		if flag_verbose {
			fmt.Fprintf(os.Stderr, "mochictl check starlark: no .star files under %q\n", root)
		}
		return nil
	}

	for _, p := range paths {
		content, err := os.ReadFile(p)
		if err != nil {
			return fmt.Errorf("read %q: %v", p, err)
		}
		// syntax.Parse returns *syntax.File and an error. The error's
		// String() already includes file:line:col when the file name
		// is passed in (which we do here). Mode 0 = parser defaults,
		// same as the server's runtime path which loads apps via
		// starlark.ExecFile with no custom parse mode.
		_, err = syntax.Parse(p, content, 0)
		if err != nil {
			return fmt.Errorf("%v", err)
		}
		if flag_verbose {
			fmt.Printf("ok: %s\n", p)
		}
	}
	return nil
}
