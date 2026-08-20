// Mochi server: a deferred close inside a loop is not a close.
//
// load_version opened each of an app's label files and wrote `defer f.Close()`.
// A defer runs when the FUNCTION returns, not when the loop iteration ends, so
// every label file stayed open for the rest of the call. Apps carry 99 locale
// files, so one load_version held 99 descriptors where it needed one.
//
// The twin three hundred lines down in reload() does it correctly - open,
// scan, f.Close() at the end of the iteration - which is what this now matches.
//
// This was never going to exhaust anything: the box allows 524,288 open files.
// It matters because it is the shape that becomes a leak silently, the moment
// the loop grows or the function is restructured, and because the file
// contradicted itself about how to do the same job twice.
//
// Three shapes are NOT this defect, and the detector below knows all three:
//
//   - A defer inside a `go func() { ... }()` spawned in a loop belongs to the
//     goroutine, which is exactly right (peer_connect, push, queue, schedule).
//   - A loop that returns on the iteration reaching the defer runs it at most
//     once - the "find the entry, use it, return" shape in files.go and
//     utilities.go.
//   - A defer in a loop inside a nested function literal is that function's.
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
	"strings"
	"testing"
)

// deferred_in_loop is one defer that a loop can reach more than once.
type deferred_in_loop struct {
	file string
	line int
	text string
}

// loop_body returns the body of a for/range statement, or nil.
func loop_body(node ast.Node) *ast.BlockStmt {
	switch loop := node.(type) {
	case *ast.ForStmt:
		return loop.Body
	case *ast.RangeStmt:
		return loop.Body
	}
	return nil
}

// block_returns reports whether a statement list ends in a return. A loop body
// whose reachable tail is a return cannot come back round, so a defer in it
// runs at most once.
func block_returns(statements []ast.Stmt) bool {
	if len(statements) == 0 {
		return false
	}
	switch last := statements[len(statements)-1].(type) {
	case *ast.ReturnStmt:
		return true
	case *ast.BlockStmt:
		return block_returns(last.List)
	}
	return false
}

// defers_reachable_twice walks one loop body, collecting defers the loop can
// execute on more than one iteration. It does NOT descend into function
// literals: a defer inside one belongs to that literal, not to this loop.
func defers_reachable_twice(fset *token.FileSet, body *ast.BlockStmt, file string) []deferred_in_loop {
	if block_returns(body.List) {
		return nil
	}
	var found []deferred_in_loop
	var walk func(node ast.Node) bool
	walk = func(node ast.Node) bool {
		switch typed := node.(type) {
		case *ast.FuncLit:
			return false // its own function; its defers are its own
		case *ast.DeferStmt:
			position := fset.Position(typed.Pos())
			found = append(found, deferred_in_loop{file, position.Line, "defer"})
		}
		return true
	}
	for _, statement := range body.List {
		ast.Inspect(statement, walk)
	}
	return found
}

// scan_defers_in_loops parses one file and reports every defer a loop can reach
// on more than one iteration.
func scan_defers_in_loops(t *testing.T, file string) []deferred_in_loop {
	t.Helper()
	fset := token.NewFileSet()
	parsed, err := parser.ParseFile(fset, file, nil, 0)
	if err != nil {
		t.Fatalf("parsing %s: %v", file, err)
	}

	var found []deferred_in_loop
	ast.Inspect(parsed, func(node ast.Node) bool {
		body := loop_body(node)
		if body == nil {
			return true
		}
		found = append(found, defers_reachable_twice(fset, body, file)...)
		return true
	})
	return found
}

// TestNoDeferReachableTwiceInALoop is the gate.
func TestNoDeferReachableTwiceInALoop(t *testing.T) {
	for _, file := range package_source_files(t) {
		for _, found := range scan_defers_in_loops(t, file) {
			t.Errorf("%s:%d defers inside a loop; the defer runs when the function returns, not when the iteration ends, so each pass leaks whatever it holds. Close at the end of the iteration",
				found.file, found.line)
		}
	}
}

// TestLabelFilesAreClosedPerIteration pins the specific fix, so a later edit
// that reintroduces the defer fails here with the reason rather than only in
// the general gate.
func TestLabelFilesAreClosedPerIteration(t *testing.T) {
	source, err := os.ReadFile("apps.go")
	if err != nil {
		t.Fatalf("reading apps.go: %v", err)
	}
	text := string(source)
	at := strings.Index(text, "func (a *App) load_version(")
	if at < 0 {
		t.Fatal("apps.go no longer defines load_version")
	}
	body := text[at:]
	if end := strings.Index(body, "\n}\n"); end > 0 {
		body = body[:end]
	}

	if strings.Contains(body, "defer f.Close()") {
		t.Error("load_version defers the label file close again; it opens one file per locale and apps carry 99, so all 99 stay open until the function returns")
	}
	if !strings.Contains(body, "f.Close()") {
		t.Error("load_version no longer closes the label files at all")
	}
}

// TestDetectorFindsADeferredCloseInALoop proves the gate can fail, since a
// scanner that matched nothing would pass the suite for ever.
func TestDetectorFindsADeferredCloseInALoop(t *testing.T) {
	planted := t.TempDir() + "/planted.go"
	source := `package main

import "os"

func leaky(paths []string) {
	for _, path := range paths {
		f, err := os.Open(path)
		if err != nil {
			continue
		}
		defer f.Close()
		_ = f
	}
}
`
	if err := os.WriteFile(planted, []byte(source), 0o644); err != nil {
		t.Fatalf("writing the planted file: %v", err)
	}

	found := scan_defers_in_loops(t, planted)
	if len(found) != 1 {
		t.Fatalf("the detector found %d defers in a file with exactly one leaky defer", len(found))
	}
}

// TestDetectorAcceptsTheThreeLegitimateShapes is the other half. Each of these
// is in the package today and must not be reported.
func TestDetectorAcceptsTheThreeLegitimateShapes(t *testing.T) {
	clean := t.TempDir() + "/clean.go"
	source := `package main

import "os"

// A defer in a goroutine spawned by a loop belongs to the goroutine.
func spawner(items []string, slots chan struct{}) {
	for range items {
		slots <- struct{}{}
		go func() {
			defer func() { <-slots }()
		}()
	}
}

// A loop that returns on the iteration reaching the defer runs it once.
func finder(paths []string, want string) []byte {
	for _, path := range paths {
		if path != want {
			continue
		}
		f, err := os.Open(path)
		if err != nil {
			return nil
		}
		defer f.Close()
		return []byte(path)
	}
	return nil
}
`
	if err := os.WriteFile(clean, []byte(source), 0o644); err != nil {
		t.Fatalf("writing the clean file: %v", err)
	}

	if found := scan_defers_in_loops(t, clean); len(found) != 0 {
		t.Errorf("the detector reported %d defers in a clean file: %+v", len(found), found)
	}
}
