// Mochi server: Commit hook pattern library helper
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	sl "go.starlark.net/starlark"
	sls "go.starlark.net/starlarkstruct"
)

// api_commit exposes mochi.db.commit.{hook,fire}.
//
//   - mochi.db.commit.hook(function_name) — register a Starlark function
//     to invoke after every committed write the framework knows about for
//     this app. Typically called once at module load.
//   - mochi.db.commit.fire(table, kind, row_uid) — manually fire the hook
//     for a local write the app just made. Replication-applied writes
//     fire the hook automatically.
//
// Handlers MUST be idempotent: the hook can fire more than once for the
// same (table, kind, row_uid) if a replication op replays after a local
// fire of the same row. The receive-side dedup prevents the underlying
// SQL apply from running twice, but the hook fires on each invocation.
//
// V1 limitations (documented in claude/plans/replication.md pattern 1.6):
//   - The commits log + drainer is best-effort: a handler crash between
//     the commit and the log insert still means a missed fire (the
//     underlying SQL write is durable, the hook fire is not).
//   - Local-write hooking is opt-in via mochi.db.commit.fire; the
//     framework only auto-fires from replication apply paths.
//   - Only one handler per app version. Apps that need fan-out can
//     dispatch from their single registered handler.
var api_commit = sls.FromStringDict(sl.String("mochi.db.commit"), sl.StringDict{
	"hook": sl.NewBuiltin("mochi.db.commit.hook", api_commit_hook),
	"fire": sl.NewBuiltin("mochi.db.commit.fire", api_commit_fire),
})

// api_commit_hook stores the named function on the app version. Subsequent
// commit_hook_fire calls for this app invoke this function.
func api_commit_hook(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var function string
	if err := sl.UnpackArgs(fn.Name(), args, kwargs, "function", &function); err != nil {
		return nil, err
	}
	if function == "" {
		return sl_error(fn, "function name must be non-empty")
	}

	app, _ := t.Local("app").(*App)
	user, _ := t.Local("user").(*User)
	if app == nil || user == nil {
		return sl_error(fn, "no app/user context")
	}
	av := app.active(user)
	if av == nil {
		return sl_error(fn, "no active version for app %q", app.id)
	}
	apps_lock.Lock()
	av.Commit.Function = function
	apps_lock.Unlock()
	return sl.None, nil
}

// api_commit_fire is the local-side trigger: apps call this after a
// committed write so the registered hook fires for both local and
// replicated writes.
func api_commit_fire(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var table, kind, row_uid string
	if err := sl.UnpackArgs(fn.Name(), args, kwargs, "table", &table, "kind", &kind, "row_uid?", &row_uid); err != nil {
		return nil, err
	}

	app, _ := t.Local("app").(*App)
	user, _ := t.Local("user").(*User)
	if app == nil || user == nil {
		return sl_error(fn, "no app/user context")
	}
	commit_hook_fire(user.UID, app.id, table, kind, row_uid)
	return sl.None, nil
}

// commit_hook_fire invokes the app's registered commit-hook function.
// No-op when the app isn't installed locally, the user isn't on this
// host, or the app version declares no commit hook.
//
// The call writes a pending row to the per-app `commits` table in the
// app system DB (app.db) before invoking the handler, then marks it
// fired on success. A failed handler leaves the row pending so
// commit_hook_drain retries on the next fire. Crash between the commit
// and the log insert is the documented V1 gap — the underlying SQL write
// is already durable but the hook fire would be lost. The log lives in
// app.db (server-only, like access/attachments), so an app cannot tamper
// with its own pending-fire bookkeeping via mochi.db.execute.
func commit_hook_fire(userUID, appID, table, kind, row_uid string) {
	u, a, av := commit_hook_resolve(userUID, appID)
	if u == nil || a == nil || av == nil {
		return
	}
	function := commit_hook_function(av)
	if function == "" {
		return
	}

	sys := commits_setup(u, a)
	if sys == nil {
		return
	}
	// Opportunistic retry of any previously-failed hooks before logging
	// the new one — keeps the log from growing unboundedly while the
	// app is active.
	commit_hook_drain(sys, av, a, u, function)

	seq := commits_append(sys, table, kind, row_uid)
	if commit_hook_invoke(av, a, u, function, table, kind, row_uid) {
		commits_mark_fired(sys, seq)
	}
}

// commit_hook_drain walks pending `commits` entries and retries each
// against the registered handler. Successful invocations mark the row
// fired; failed ones stay pending for the next drain.
func commit_hook_drain(db *DB, av *AppVersion, a *App, u *User, function string) {
	commits_table_create(db)
	commits_trim(db)
	rows, err := db.rows("select seq, name, kind, row_uid from commits where fired=0 order by seq limit 100")
	if err != nil {
		return
	}
	for _, r := range rows {
		seq, _ := r["seq"].(int64)
		table, _ := r["name"].(string)
		kind, _ := r["kind"].(string)
		row_uid, _ := r["row_uid"].(string)
		if commit_hook_invoke(av, a, u, function, table, kind, row_uid) {
			commits_mark_fired(db, seq)
		}
	}
}

// commit_hook_invoke calls the Starlark handler and returns whether it
// completed cleanly. Errors are logged but don't propagate; the caller
// uses the return to decide whether to mark the log row fired or leave
// it for the drainer to retry.
func commit_hook_invoke(av *AppVersion, a *App, u *User, function, table, kind, row_uid string) bool {
	if av.Architecture.Engine != "starlark" {
		return true
	}
	s := av.starlark()
	s.set("app", a)
	s.set("user", u)
	s.set("owner", u)

	if _, err := s.call(function, sl.Tuple{sl.String(table), sl.String(kind), sl.String(row_uid)}); err != nil {
		warn("Commit hook %q in app %q failed: %v", function, a.id, err)
		return false
	}
	return true
}

// commit_hook_resolve translates (userUID, appID) into the User, App,
// and AppVersion the hook needs. Returns nil for any field that can't
// be resolved — caller treats nil as "skip silently".
func commit_hook_resolve(userUID, appID string) (*User, *App, *AppVersion) {
	if userUID == "" || appID == "" {
		return nil, nil, nil
	}

	u := user_by_uid(userUID)
	if u == nil {
		return nil, nil, nil
	}
	a := app_by_id(appID)
	if a == nil {
		return nil, nil, nil
	}
	av := a.active(u)
	if av == nil {
		return nil, nil, nil
	}
	return u, a, av
}

// commit_hook_function reads the commit-hook function name off an
// AppVersion under the apps_lock.
func commit_hook_function(av *AppVersion) string {
	apps_lock.Lock()
	defer apps_lock.Unlock()
	return av.Commit.Function
}

// commits_table_create lazily creates the `commits` pending-fire log and
// its index on the handle it's given (the app system DB, app.db).
func commits_table_create(db *DB) {
	db.exec("create table if not exists commits (seq integer primary key autoincrement, name text not null, kind text not null, row_uid text not null default '', ts integer not null, fired integer not null default 0)")
	db.exec("create index if not exists commits_fired on commits(fired, ts)")
}

// commits_setup opens the app system DB (app.db), ensures the `commits`
// table, and returns the handle. On first creation it drops the
// pre-relocation `_commit_log` orphan from the app's data DB — the log
// used to live there, where an app could tamper with it via
// mochi.db.execute; it now lives in app.db alongside access/attachments,
// which apps cannot write via SQL. The drop is gated on `commits` not
// having existed, so it touches the data DB at most once per (user, app).
func commits_setup(u *User, a *App) *DB {
	sys := db_app_system(u, a)
	if sys == nil {
		return nil
	}
	existed, _ := sys.exists("select name from sqlite_master where type='table' and name='commits'")
	commits_table_create(sys)
	if !existed {
		// FUTURE CLEANUP (post-relocation migration): drops the pre-relocation
		// `_commit_log` orphan from the app's data DB. Removable once no system
		// in the fleet can still carry the old table — manually confirmed gone
		// on yuzu/wasabi/mochi1/mochi2 (2026-06-20). When removing, delete this
		// `if !existed` branch and the now-unused `existed` lookup above.
		if data := db_app(u, a); data != nil {
			data.exec("drop table if exists _commit_log")
		}
	}
	return sys
}

// commits_append writes a pending row and returns its seq. The "name"
// column holds the table that committed (renamed from the parameter so
// it doesn't collide with SQL reserved-word handling).
func commits_append(db *DB, table, kind, row_uid string) int64 {
	commits_table_create(db)
	db.exec("insert into commits (name, kind, row_uid, ts, fired) values (?, ?, ?, ?, 0)", table, kind, row_uid, now())
	var seq int64
	if row, _ := db.row("select seq from commits where name=? and kind=? and row_uid=? and fired=0 order by seq desc limit 1", table, kind, row_uid); row != nil {
		if v, ok := row["seq"].(int64); ok {
			seq = v
		}
	}
	return seq
}

// commits_mark_fired marks a row as fired. Idempotent.
func commits_mark_fired(db *DB, seq int64) {
	if seq <= 0 {
		return
	}
	db.exec("update commits set fired=1 where seq=?", seq)
}

// commits_log_age bounds how long a fired commit-hook row is retained. The fired
// flag is set once and never cleared, so without a trim the log grows without
// bound (31k rows observed on a busy app); fired rows have no further use, so this
// is just a short debugging window.
const commits_log_age = 86400 // 1 day

// commits_trim deletes fired rows older than commits_log_age. Called from
// commit_hook_drain — which runs on every commit_hook_fire — using the
// commits_fired (fired, ts) index, so the table tracks ~a day of recent activity
// rather than growing forever. Unfired (pending) rows are never trimmed, so a
// stuck handler's retries are preserved regardless of age.
func commits_trim(db *DB) {
	db.exec("delete from commits where fired=1 and ts < ?", now()-commits_log_age)
}
