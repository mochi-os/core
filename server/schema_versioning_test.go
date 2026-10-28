// Mochi server: every schema change goes through the versioning system.
//
// A create, alter or drop of a table, index, trigger or view may sit only in
// db_create, a numbered upgrade step (db_upgrade_N for the server-wide
// databases, or a per-file step such as user_upgrade_N), or a function only
// those reach. Anything else lays schema when a database opens, at every start
// or on first use, and the schema drifts between installs with no record of
// what ran. claude/scripts/check-schema-versioning.py holds every commit to
// the same rule.
//
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the Mochi
// Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"
)

// schema_statement is a change to a table, index, trigger or view: a string
// that is one, at its start or after a ";", not a query mentioning one.
var schema_statement = regexp.MustCompile(`(?i)(^|;)\s*(create\s+(unique\s+)?(table|index|virtual\s+table|trigger|view)|alter\s+table|drop\s+(table|index|trigger|view))\b`)

// schema_versioned_root names the functions a schema change may sit in:
// db_create, a server-wide db_upgrade_N, or a per-file step such as
// user_upgrade_N.
var schema_versioned_root = regexp.MustCompile(`^(db_create|[a-z][a-z0-9]*_upgrade_[0-9]+)$`)

// schema_step names a numbered step, which only the versioning calls: db_upgrade
// for the server-wide steps, a list of steps for a file's own.
var schema_step = regexp.MustCompile(`^[a-z][a-z0-9]*_upgrade_[0-9]+$`)

// schema_unit is a function or package-level name: where it changes the schema,
// and the names it mentions, which are what it reaches.
type schema_unit struct {
	at       []string
	names    map[string]bool
	function bool
	position string
}

// schema_unversioned returns, for Go sources by file name, each schema
// statement outside the versioned paths, as "file:line: function". A statement
// whose line carries a comment holding "schema-ok:" is let through.
func schema_unversioned(sources map[string]string) ([]string, error) {
	files := token.NewFileSet()
	units := map[string]*schema_unit{}
	unit := func(name string) *schema_unit {
		if units[name] == nil {
			units[name] = &schema_unit{names: map[string]bool{}}
		}
		return units[name]
	}
	for name, source := range sources {
		file, err := parser.ParseFile(files, name, source, parser.ParseComments)
		if err != nil {
			return nil, err
		}
		cleared := map[int]bool{}
		for _, group := range file.Comments {
			for _, comment := range group.List {
				if strings.Contains(comment.Text, "schema-ok:") {
					cleared[files.Position(comment.Pos()).Line] = true
				}
			}
		}
		visit := func(u *schema_unit, node ast.Node) {
			ast.Inspect(node, func(n ast.Node) bool {
				switch x := n.(type) {
				case *ast.Ident:
					u.names[x.Name] = true
				case *ast.BasicLit:
					if x.Kind != token.STRING {
						break
					}
					text, err := strconv.Unquote(x.Value)
					position := files.Position(x.Pos())
					if err == nil && schema_statement.MatchString(text) && !cleared[position.Line] {
						u.at = append(u.at, fmt.Sprintf("%s:%d", position.Filename, position.Line))
					}
				}
				return true
			})
		}
		for _, declaration := range file.Decls {
			switch d := declaration.(type) {
			case *ast.FuncDecl:
				if d.Body != nil {
					u := unit(d.Name.Name)
					u.function = true
					u.position = fmt.Sprintf("%s:%d", files.Position(d.Pos()).Filename, files.Position(d.Pos()).Line)
					visit(u, d.Body)
				}
			case *ast.GenDecl:
				for _, spec := range d.Specs {
					value, ok := spec.(*ast.ValueSpec)
					if !ok {
						continue
					}
					for _, name := range value.Names {
						for _, expression := range value.Values {
							visit(unit(name.Name), expression)
						}
					}
				}
			}
		}
	}

	callers := map[string]map[string]bool{}
	for name, u := range units {
		for other := range u.names {
			if other != name && units[other] != nil {
				if callers[other] == nil {
					callers[other] = map[string]bool{}
				}
				callers[other][name] = true
			}
		}
	}
	versioned := map[string]bool{}
	for name := range units {
		if schema_versioned_root.MatchString(name) {
			versioned[name] = true
		}
	}
	for changed := true; changed; {
		changed = false
		for name := range units {
			if versioned[name] || len(callers[name]) == 0 {
				continue
			}
			reached := true
			for caller := range callers[name] {
				if !versioned[caller] {
					reached = false
					break
				}
			}
			if reached {
				versioned[name] = true
				changed = true
			}
		}
	}

	var found []string
	for name, u := range units {
		if u.function && schema_step.MatchString(name) {
			for caller := range callers[name] {
				if units[caller].function && caller != "db_upgrade" {
					found = append(found, u.position+": "+name+" called by "+caller)
				}
			}
		}
		if versioned[name] {
			continue
		}
		for _, at := range u.at {
			found = append(found, at+": "+name)
		}
	}
	slices.Sort(found)
	return found, nil
}

// TestSchemaChangesAreVersioned holds the server's own sources to the rule.
func TestSchemaChangesAreVersioned(t *testing.T) {
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	sources := map[string]string{}
	for _, entry := range entries {
		name := entry.Name()
		if !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		data, err := os.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		sources[name] = string(data)
	}
	if len(sources) < 100 {
		t.Fatalf("read %d sources; the check is looking in the wrong place", len(sources))
	}
	found, err := schema_unversioned(sources)
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range found {
		t.Errorf("THE POINT: %s changes the schema outside db_create and the numbered upgrade steps", f)
	}
}

// TestSchemaCheckSeesWhatIsNotVersioned: the check finds an opener laying its
// tables, a helper also reached from outside the versioned paths and a
// constant used outside them, and passes what only those paths reach, a
// per-file step and a line marked as not a schema change.
func TestSchemaCheckSeesWhatIsNotVersioned(t *testing.T) {
	source := `package main

const kept = "create table kept (a)"
const loose = "create table loose (a)"

func thing_database() *DB {
	db := db_open("thing.db")
	db.exec("create table if not exists t (a)")
	return db
}

func db_create() {
	tables(db_open("thing.db"))
	_ = kept
}

func db_upgrade_3() {
	tables(db_open("thing.db"))
	shared(nil)
}

func tables(db *DB) {
	db.exec("create index if not exists t_a on t(a)")
}

func shared(db *DB) {
	db.exec(` + "`" + `alter table t
		add column b text` + "`" + `)
}

func other() {
	shared(nil)
	_ = loose
}

func user_upgrade_2(db *DB) error {
	db.exec("drop table old")
	return nil
}

func scratch(db *DB) {
	db.exec("create table scratch (a)") // schema-ok: an in-memory working table
	db.exec("select 1 from sqlite_master where type='table' and sql like '%create table%'")
}

func joined(db *DB) {
	db.exec("pragma foreign_keys=on; create table joined (a)")
}

func networks_upgrade_1(db *DB) error {
	return nil
}

func networks_database() *DB {
	db := db_open("networks.db")
	networks_upgrade_1(db)
	return db
}
`
	found, err := schema_unversioned(map[string]string{"example.go": source})
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"example.go:27: shared", "example.go:4: loose", "example.go:47: joined", "example.go:50: networks_upgrade_1 called by networks_database", "example.go:8: thing_database"}
	slices.Sort(want)
	if !slices.Equal(found, want) {
		t.Errorf("THE POINT: the check found %q, want %q", found, want)
	}
}
