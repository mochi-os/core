// Mochi server: the naming rules, enforced.
//
// CLAUDE.md's rule is that every identifier leaving a function is one single,
// full English word - snake_case, no camelCase, no abbreviations. Four sweeps
// brought core/server to that state; this keeps it there, because a naming
// rule that is only ever applied by hand drifts back within weeks. The gofmt
// gate exists for the same reason.
//
// Scope is deliberately narrow: DECLARATIONS in this repository. A camelCase
// name arriving from a dependency (svc.ChangeRequest, cbor.DecMode) is not
// ours to rename, and matching identifier USES would flag every one of them.
//
// Copyright © 2026 Mochisoft OU
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// naming_directories are the trees this gate covers. Platform-gated files are
// excluded per-file below, not here.
var naming_directories = []string{".", "../mochictl", "../common/adminclient", "../common/ini", "../common/paths"}

// naming_exempt lists files this gate does not read, with the reason.
var naming_exempt = map[string]string{
	// Built only for their own GOOS, so a rename here cannot be compiled or
	// tested on the development platform. Renaming code you cannot build is
	// how a cosmetic sweep becomes an outage.
	"service_windows.go":    "windows-only build tag",
	"admin_cred_darwin.go":  "darwin-only build tag",
	"service_darwin.go":     "darwin-only build tag",
	"admin_cred_windows.go": "windows-only build tag",
	"admin_cred_linux.go":   "linux-only, kept beside its siblings",
	"world_cred_darwin.go":  "darwin-only build tag",
	"world_cred_windows.go": "windows-only build tag",
	"naming_test.go":        "this gate's own tables name the patterns it hunts",
	"dead_code_test.go":     "its table names removed symbols verbatim",
	"thread_locals_test.go": "its message text quotes the pattern it forbids",
}

// naming_declarations walks every declaration this repository makes and hands
// each name to f, with the position for the error message.
func naming_declarations(t *testing.T, f func(name, position string)) {
	t.Helper()
	set := token.NewFileSet()
	for _, directory := range naming_directories {
		entries, err := os.ReadDir(directory)
		if err != nil {
			t.Fatalf("reading %s: %v", directory, err)
		}
		for _, entry := range entries {
			name := entry.Name()
			if !strings.HasSuffix(name, ".go") {
				continue
			}
			if _, skip := naming_exempt[name]; skip {
				continue
			}
			path := filepath.Join(directory, name)
			file, err := parser.ParseFile(set, path, nil, 0)
			if err != nil {
				// A build-tagged file for another platform may not parse
				// against this one's imports; it is not this gate's business.
				continue
			}
			ast.Inspect(file, func(n ast.Node) bool {
				var names []*ast.Ident
				switch d := n.(type) {
				case *ast.ValueSpec:
					names = d.Names
				case *ast.TypeSpec:
					names = []*ast.Ident{d.Name}
				case *ast.FuncDecl:
					names = []*ast.Ident{d.Name}
				case *ast.Field:
					names = d.Names
				case *ast.AssignStmt:
					if d.Tok != token.DEFINE {
						return true
					}
					for _, expression := range d.Lhs {
						if identifier, ok := expression.(*ast.Ident); ok {
							names = append(names, identifier)
						}
					}
				}
				for _, identifier := range names {
					if identifier.Name == "_" {
						continue
					}
					f(identifier.Name, set.Position(identifier.Pos()).String())
				}
				return true
			})
		}
	}
}

// TestNoCamelCaseDeclarations is #102 and #146. Go's own convention is
// camelCase for unexported names; this project's is not, and CLAUDE.md says so
// explicitly ("Default to project conventions, not ambient Go conventions").
func TestNoCamelCaseDeclarations(t *testing.T) {
	naming_declarations(t, func(name, position string) {
		// Exported Go names are legitimately capitalised; the rule they must
		// meet is the abbreviation one below, not this one.
		if name == "" || name[0] < 'a' || name[0] > 'z' {
			return
		}
		for i := 1; i < len(name); i++ {
			if name[i] >= 'A' && name[i] <= 'Z' {
				t.Errorf("%s: %s is camelCase; this project uses snake_case for its own identifiers", position, name)
				return
			}
		}
	})
}

// TestNoAbbreviatedDeclarations is #103, #104 and #147. Each of these was
// found in the tree and fixed; the entry is what stops it coming back.
func TestNoAbbreviatedDeclarations(t *testing.T) {
	// Whole-word matches only, on the underscore-separated parts of a name.
	// "max" catches max_age but not maximum; "msg" catches msg_id but not
	// message.
	abbreviations := map[string]string{
		"max":  "maximum",
		"min":  "minimum",
		"msg":  "message",
		"recv": "received",
		"prev": "previous",
		"stmt": "statement",
		"src":  "source",
		"dst":  "destination",
		"err":  "error",
		"attr": "attribute",
		"neg":  "negative",
		"pos":  "positive",
		"sem":  "semaphore",
		"svc":  "service",
		"cred": "credential",
	}
	// Names whose abbreviation is not ours to fix, with the reason.
	allowed := map[string]bool{
		"Attr":      true, // starlark.HasAttrs; the interface names the method
		"AttrNames": true,
		"err":       true, // the Go error idiom, universal and function-local
		"src":       true, // io.Copy(dst, src) parameter order, mirrors stdlib
		"dst":       true,
		"min_free":  true, // matches the SQLite pragma of the same name
	}

	naming_declarations(t, func(name, position string) {
		if allowed[name] {
			return
		}
		for _, part := range strings.Split(strings.ToLower(name), "_") {
			if full, bad := abbreviations[part]; bad {
				t.Errorf("%s: %s abbreviates %q; write %q", position, name, part, full)
				return
			}
		}
	})
}

// TestNoLocalShadowsTheLogger is #105. info() is the package logger, so a
// local named info silently takes its name for the rest of the scope.
func TestNoLocalShadowsTheLogger(t *testing.T) {
	loggers := map[string]bool{"info": true, "warn": true, "debug": true}
	naming_declarations(t, func(name, position string) {
		// log.go declares them; mochictl and common have no logger of these
		// names, so a local there shadows nothing.
		if strings.HasPrefix(position, "log.go:") || strings.HasPrefix(position, "..") {
			return
		}
		if loggers[name] {
			t.Errorf("%s: a local named %q shadows the package logger of the same name; use `information` and friends", position, name)
		}
	})
}

// TestNoSessionTaskNumbersInComments: a comment saying "task #83" points at a
// numbering that lived in one session's todo list and means nothing to a later
// reader - and the numbers get reused, so it points at the WRONG thing rather
// than at nothing. Say what the code does instead.
func TestNoSessionTaskNumbersInComments(t *testing.T) {
	set := token.NewFileSet()
	for _, directory := range naming_directories {
		entries, err := os.ReadDir(directory)
		if err != nil {
			t.Fatalf("reading %s: %v", directory, err)
		}
		for _, entry := range entries {
			name := entry.Name()
			if !strings.HasSuffix(name, ".go") {
				continue
			}
			if _, skip := naming_exempt[name]; skip {
				continue
			}
			path := filepath.Join(directory, name)
			file, err := parser.ParseFile(set, path, nil, parser.ParseComments)
			if err != nil {
				continue
			}
			for _, group := range file.Comments {
				for _, comment := range group.List {
					lower := strings.ToLower(comment.Text)
					if !strings.Contains(lower, "task #") {
						continue
					}
					t.Errorf("%s: %s\n    a session task number means nothing to a later reader, and the numbers are reused; describe the behaviour instead",
						set.Position(comment.Pos()), strings.TrimSpace(comment.Text))
				}
			}
		}
	}
}
