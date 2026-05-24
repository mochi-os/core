// Mochi server: Per-app SQL command replication
// Copyright Alistair Cunningham 2026

package main

import (
	"strings"

	cbor "github.com/fxamacker/cbor/v2"
)

// SQLCommand is the wire payload for an opt-out app-DB SQL replication
// op. The receiver re-executes Statement against the same per-(user,
// app) DB using Args as bound parameters. Convergence is by re-execution:
// INSERTs dedup on PK conflict, UPDATE/DELETE follow last-write-wins by
// arrival order. Apps that need stronger semantics use the log-plus-
// derived-view pattern (see CLAUDE.md replication-safety bullets).
//
// Args are encoded as `any` so the same Go-side parameter types that
// went into the local exec round-trip through cbor and reach the
// replica's exec untouched (int64, string, []byte, nil).
type SQLCommand struct {
	Statement string `cbor:"sql"`
	Args      []any  `cbor:"args,omitempty"`
}

// sql_default_excluded names tables that never replicate, no matter
// what the app declares. SQLite's internal namespace plus the
// session-local commit log used by the future mochi.db.commit.hook
// drainer.
var sql_default_excluded = []string{
	"sqlite_",
	"_commit_log",
}

// sql_target_table extracts the target table from a mutating SQL
// statement. Returns "" for read-only statements (SELECT, PRAGMA …)
// and for schema statements (CREATE/DROP/ALTER) — neither replicates.
// The parser is intentionally simple: skip leading comments + whitespace,
// match the verb, then take the next identifier as the table name. CTE
// (WITH …) prefixes are not recognised and stay local; apps that need
// CTE writes to replicate should reshape to a plain INSERT/UPDATE/DELETE.
func sql_target_table(sql string) string {
	s := sql_strip_lead(sql)
	verb, rest := sql_take_word(s)
	switch strings.ToUpper(verb) {
	case "INSERT", "REPLACE":
		// INSERT [OR IGNORE|REPLACE|...] INTO <table>
		rest = sql_strip_lead(rest)
		if w, after := sql_take_word(rest); strings.ToUpper(w) == "OR" {
			_, after = sql_take_word(sql_strip_lead(after))
			rest = sql_strip_lead(after)
			w, after = sql_take_word(rest)
			if strings.ToUpper(w) != "INTO" {
				return ""
			}
			rest = sql_strip_lead(after)
		} else if strings.ToUpper(w) == "INTO" {
			rest = sql_strip_lead(after)
		} else {
			return ""
		}
		name, _ := sql_take_ident(rest)
		return name
	case "UPDATE":
		// UPDATE [OR ...] <table>
		rest = sql_strip_lead(rest)
		if w, after := sql_take_word(rest); strings.ToUpper(w) == "OR" {
			_, after = sql_take_word(sql_strip_lead(after))
			rest = sql_strip_lead(after)
		}
		name, _ := sql_take_ident(rest)
		return name
	case "DELETE":
		// DELETE FROM <table>
		rest = sql_strip_lead(rest)
		w, after := sql_take_word(rest)
		if strings.ToUpper(w) != "FROM" {
			return ""
		}
		name, _ := sql_take_ident(sql_strip_lead(after))
		return name
	}
	return ""
}

// sql_target_uid extracts the row identifier value bound to a
// mutating SQL statement, used as the row uid passed to
// commit_hook_fire on replication apply. App-side commit hooks then
// know which specific row a replicated write created or changed.
//
// Recognised shapes (cover the bulk of app SQL written under the
// Mochi single-word PK convention where the row id column is named
// "id" and bound as a string):
//
//   INSERT|REPLACE INTO <table> (id, ...) VALUES (?, ...)
//   INSERT|REPLACE INTO <table> VALUES (?, ...)
//   UPDATE <table> SET ... WHERE id = ?
//   DELETE FROM <table> WHERE id = ?
//
// Returns "" for any other shape. Apps whose row PK isn't "id", or
// whose WHERE clause carries more than a single id-equality, fall
// back to the empty-uid behaviour (commit hooks still fire on
// replication apply, just without a specific row identifier).
func sql_target_uid(sql string, args []any) string {
	s := sql_strip_lead(sql)
	verb, rest := sql_take_word(s)
	switch strings.ToUpper(verb) {
	case "INSERT", "REPLACE":
		rest = sql_strip_lead(rest)
		// Skip OR <conflict-action>.
		if word, after := sql_take_word(rest); strings.ToUpper(word) == "OR" {
			_, after = sql_take_word(sql_strip_lead(after))
			rest = sql_strip_lead(after)
		}
		word, after := sql_take_word(rest)
		if strings.ToUpper(word) != "INTO" {
			return ""
		}
		_, after = sql_take_ident(sql_strip_lead(after))
		after = sql_strip_lead(after)
		// Either an explicit "(id, ...) values (?, ...)" or the
		// implicit positional "values (?, ...)" - both have args[0]
		// bound to the row id.
		if strings.HasPrefix(after, "(") {
			column, _ := sql_take_ident(sql_strip_lead(after[1:]))
			if !strings.EqualFold(column, "id") {
				return ""
			}
		} else {
			keyword, _ := sql_take_word(after)
			if strings.ToUpper(keyword) != "VALUES" {
				return ""
			}
		}
		if len(args) == 0 {
			return ""
		}
		if uid, ok := args[0].(string); ok {
			return uid
		}
		return ""

	case "UPDATE", "DELETE":
		// Recognised only when the WHERE clause is exactly "id = ?".
		// The bound value is then the last entry in args.
		lower := strings.ToLower(sql)
		where := strings.LastIndex(lower, " where ")
		if where < 0 {
			return ""
		}
		clause := strings.TrimSpace(lower[where+len(" where "):])
		clause = strings.TrimRight(clause, " \t;")
		column, rest := sql_take_ident(clause)
		if !strings.EqualFold(column, "id") {
			return ""
		}
		rest = strings.TrimLeft(rest, " \t")
		if !strings.HasPrefix(rest, "=") {
			return ""
		}
		rest = strings.TrimLeft(rest[1:], " \t")
		if rest != "?" {
			return ""
		}
		if len(args) == 0 {
			return ""
		}
		if uid, ok := args[len(args)-1].(string); ok {
			return uid
		}
		return ""
	}
	return ""
}

// sql_strip_lead skips over leading whitespace and line / block comments.
func sql_strip_lead(s string) string {
	for {
		s = strings.TrimLeft(s, " \t\r\n")
		if strings.HasPrefix(s, "--") {
			if i := strings.IndexByte(s, '\n'); i >= 0 {
				s = s[i+1:]
				continue
			}
			return ""
		}
		if strings.HasPrefix(s, "/*") {
			if i := strings.Index(s, "*/"); i >= 0 {
				s = s[i+2:]
				continue
			}
			return ""
		}
		return s
	}
}

// sql_take_word reads the next contiguous run of letters as a single
// keyword. Stops at the first non-letter, returning the word and the
// remainder.
func sql_take_word(s string) (string, string) {
	i := 0
	for i < len(s) {
		c := s[i]
		if (c < 'A' || c > 'Z') && (c < 'a' || c > 'z') {
			break
		}
		i++
	}
	return s[:i], s[i:]
}

// sql_take_ident reads a SQL identifier: bare word, `"quoted"`, or
// `[bracketed]`. Returns the unquoted name plus the tail.
func sql_take_ident(s string) (string, string) {
	if s == "" {
		return "", s
	}
	if s[0] == '"' {
		if i := strings.IndexByte(s[1:], '"'); i >= 0 {
			return s[1 : 1+i], s[2+i:]
		}
		return "", ""
	}
	if s[0] == '[' {
		if i := strings.IndexByte(s[1:], ']'); i >= 0 {
			return s[1 : 1+i], s[2+i:]
		}
		return "", ""
	}
	if s[0] == '`' {
		if i := strings.IndexByte(s[1:], '`'); i >= 0 {
			return s[1 : 1+i], s[2+i:]
		}
		return "", ""
	}
	i := 0
	for i < len(s) {
		c := s[i]
		if (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '_' {
			i++
			continue
		}
		break
	}
	return s[:i], s[i:]
}

// sql_table_excluded returns true when the given table is in either
// the core default-excluded set or the app's app.json exclude list.
func sql_table_excluded(av *AppVersion, table string) bool {
	if table == "" {
		return true
	}
	for _, prefix := range sql_default_excluded {
		if strings.HasPrefix(table, prefix) {
			return true
		}
	}
	if av == nil {
		return false
	}
	for _, t := range av.Database.Replicate.Exclude.Tables {
		if t == table {
			return true
		}
	}
	return false
}

// replication_emit_sql_command fans out a successful local app-DB write
// to the user's host set. Called from api_db_query (mochi.db.execute)
// after the local exec returns nil error, and from the deferred-emit
// flush at transaction commit. Skipped when the user has no UID
// (anonymous or pre-v51 row) or the table is excluded.
func replication_emit_sql_command(user *User, app *App, av *AppVersion, sql string, args []any) {
	if user == nil || user.UID == "" || app == nil || av == nil {
		return
	}
	table := sql_target_table(sql)
	if sql_table_excluded(av, table) {
		return
	}
	payload := cbor_encode(&SQLCommand{Statement: sql, Args: args})
	replication_emit(user.UID, &ReplicationOp{
		Scope:     repl_scope_app,
		User:      user.UID,
		Database:  app.id,
		Table:     table,
		UID:       sql_target_uid(sql, args),
		Operation: repl_op_exec,
		Payload:   payload,
		Schema:    av.Database.Schema,
	})
}

// replication_apply_sql_command re-executes a replicated SQL statement
// on the local replica's per-(user, app) DB. Defers when the user or
// app isn't yet present, or when the receiver's schema is older than
// the sender's (the op will retry on database_upgrade landing).
func replication_apply_sql_command(op *ReplicationOp) ApplyResult {
	var cmd SQLCommand
	if err := cbor.Unmarshal(op.Payload, &cmd); err != nil {
		info("Replication exec: decode failed: %v", err)
		return ApplyInvalid
	}
	if cmd.Statement == "" {
		return ApplyInvalid
	}

	if !user_exists(op.User) {
		return ApplyDeferred
	}
	u := &User{UID: op.User}
	a := app_by_id(op.Database)
	if a == nil {
		return ApplyDeferred
	}
	av := a.active(u)
	if av == nil {
		return ApplyDeferred
	}
	if op.Schema > av.Database.Schema {
		// Receiver schema older than sender's. Defer; the
		// database_upgrade triggered by the next app activity
		// drives a pending-drain.
		return ApplyDeferred
	}

	db := db_app(u, a)
	if db == nil {
		return ApplyDeferred
	}

	if _, err := db.starlark.Exec(cmd.Statement, cmd.Args...); err != nil {
		// FK violations under out-of-order arrival (parallel-queue
		// send sends N ops to one peer concurrently; receiver applies
		// in arrival order). The parent row may arrive a fraction of
		// a second after the child — defer so the next pending-drain
		// tick retries once the parent has landed. Other errors
		// (schema drift, real bugs) log + advance dedup as before.
		if strings.Contains(err.Error(), "FOREIGN KEY constraint failed") {
			debug("Replication exec deferred (FK): user=%q app=%q table=%q sql=%q", op.User, op.Database, op.Table, cmd.Statement)
			return ApplyDeferred
		}
		warn("Replication exec failed on user=%q app=%q sql=%q: %v", op.User, op.Database, cmd.Statement, err)
		return ApplyApplied
	}
	debug("Replication exec apply: user=%q app=%q table=%q", op.User, op.Database, op.Table)
	return ApplyApplied
}
