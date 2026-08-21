// mochictl: pre-deploy validation subcommands.
// Copyright (c) 2026 Mochisoft OU
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the Mochi
// Application Interface Exception - see license.txt and license-exception.md.
//
// `mochictl check starlark <path>` - parse every .star file under the path with
// the same go.starlark.net parser the server uses at load time. Non-zero on the
// first parse error, with file:line:col. deploy.sh's pre-flight gate.

package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"go.starlark.net/syntax"
)

// cmd_check_starlark handles `mochictl check starlark <path>`, a file or a
// directory walked recursively. Stops at the first parse error rather than
// reporting every downstream file that was going to fail too.
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
		err := filepath.WalkDir(root, func(p string, d os.DirEntry, walk_error error) error {
			if walk_error != nil {
				return walk_error
			}
			if d.IsDir() {
				// Skip only the trees that are not the app's own source. `web` is
				// deliberately not skipped: this walk is deploy.sh's blocking gate, and a
				// .star it skips ships unparsed.
				name := d.Name()
				if name == ".git" || name == "node_modules" {
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
		// Mode 0 matches the server's starlark.ExecFile. The error's String() carries
		// file:line:col because the file name is passed in.
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
