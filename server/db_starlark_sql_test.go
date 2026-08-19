// Mochi server: what an app's own SQL may and may not do.
//
// db_starlark_sql_blocked reads the first token of the first statement, so
// "/*x*/ANALYZE", "-- c\nANALYZE", "select 1; analyze" and
// "BEGIN; ANALYZE; COMMIT" all walked past it - and nothing behind it stopped
// them, because the authoriser had no ANALYZE case. VACUUM was never a hole:
// it attaches a temporary database, so AUTH_ATTACH denies it in every form.
//
// The string check is deliberately left as it is. Splitting on ";" to reach
// later statements would refuse "select 'a; analyze'", turning working SQL
// into an error; the authoriser sees parsed input and cannot be evaded by
// spelling, so that is where the decision belongs.
//
// Copyright © 2026 Mochisoft OU
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"database/sql"
	"path/filepath"
	"strings"
	"testing"

	sqlitedrv "github.com/ncruces/go-sqlite3/driver"
	_ "github.com/ncruces/go-sqlite3/embed"
)

// starlark_sql_conn opens a database with the production Starlark authoriser
// installed - the same hook db.go:1163 uses for the Starlark pool.
func starlark_sql_conn(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sqlitedrv.Open(filepath.Join(t.TempDir(), "app.db"), db_setup_conn_starlark)
	if err != nil {
		t.Fatalf("opening a Starlark-authorised database: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	if _, err := db.Exec("create table t (a integer)"); err != nil {
		t.Fatalf("seeding: %v", err)
	}
	return db
}

// TestAnalyzeIsRefusedHoweverItIsSpelled is the defect. Each of these reached
// the planner before; the string gate saw none of them.
func TestAnalyzeIsRefusedHoweverItIsSpelled(t *testing.T) {
	db := starlark_sql_conn(t)

	for _, query := range []string{
		"ANALYZE",
		"analyze",
		"/*x*/ANALYZE",
		"-- comment\nANALYZE",
		"select 1; analyze",
		"BEGIN; ANALYZE; COMMIT",
		"ANALYZE t",
	} {
		if _, err := db.Exec(query); err == nil {
			t.Errorf("%q ran; ANALYZE is a full scan of every index, so an app can spend the host's CPU on demand", query)
		}
	}
}

// TestVacuumWasNeverAHole records the measurement that narrowed this task.
// The backlog said VACUUM rode the same gap; it does not, because it attaches
// a temporary database and AUTH_ATTACH denies that.
func TestVacuumWasNeverAHole(t *testing.T) {
	db := starlark_sql_conn(t)

	for _, query := range []string{"VACUUM", "/*x*/VACUUM", "select 1; vacuum"} {
		if _, err := db.Exec(query); err == nil {
			t.Errorf("%q ran; the authoriser is expected to deny it through the attach it performs", query)
		}
	}
}

// TestTheAuthoriserStillDeniesTheRest: adding a case must not have disturbed
// the denials that were already load-bearing.
func TestTheAuthoriserStillDeniesTheRest(t *testing.T) {
	db := starlark_sql_conn(t)

	for _, query := range []string{
		"ATTACH DATABASE ':memory:' AS other",
		"PRAGMA journal_mode = DELETE",
		"PRAGMA max_page_count = 1000000",
		"create trigger tr after insert on t begin select 1; end",
		"create virtual table v using fts5(a)",
		// Carries its target as the PRAGMA argument, so the PRAGMA rule
		// denies it. Apps read a table's columns with mochi.db.table(),
		// which asks over the ordinary pool. The authoriser's comment used
		// to claim these reads were unaffected; they are not.
		"select * from pragma_table_info('t')",
	} {
		if _, err := db.Exec(query); err == nil {
			t.Errorf("%q ran; it must be denied", query)
		}
	}
}

// TestOrdinaryAppSqlStillWorks is the guard against over-denial - including
// the string literal that a hardened ";"-splitting gate would have refused.
func TestOrdinaryAppSqlStillWorks(t *testing.T) {
	db := starlark_sql_conn(t)

	for _, query := range []string{
		"insert into t (a) values (1)",
		"select a from t",
		"select 'a; analyze'",
		"create index t_a on t(a)",
		"BEGIN; insert into t (a) values (2); COMMIT",
	} {
		if _, err := db.Exec(query); err != nil {
			t.Errorf("%q was refused: %v", query, err)
		}
	}
}

// TestTheStringGateIsStillFriendly: it is courtesy, not enforcement, and the
// plain spellings must keep producing a readable message rather than an opaque
// authoriser denial.
func TestTheStringGateIsStillFriendly(t *testing.T) {
	for query, want := range map[string]string{
		"ANALYZE":                      "ANALYZE",
		"VACUUM":                       "VACUUM",
		"PRAGMA journal_mode = WAL":    "PRAGMA",
		"select a from t":              "",
		"insert into t (a) values (1)": "",
	} {
		got := db_starlark_sql_blocked(query)
		if want == "" && got != "" {
			t.Errorf("%q was refused by the string gate: %q", query, got)
			continue
		}
		if want != "" && !strings.Contains(got, want) {
			t.Errorf("%q gave the message %q; want it to name %s so an app author can read it", query, got, want)
		}
	}
}

// TestTheDataDatabaseCarriesNoCommitLog: the live log is on the system DB, and
// a second copy on the data DB is app-writable and shadows the real one in any
// later diagnosis - which is what the broadcast tables did in the 2026-07 News
// wedge.
func TestTheDataDatabaseCarriesNoCommitLog(t *testing.T) {
	body := function_body(t, "db.go", "func db_app(")
	if strings.Contains(body, "commits_table_create(") {
		t.Error("db_app creates a commits table on the app's data database; nothing reads it, and the app can write it")
	}
}
