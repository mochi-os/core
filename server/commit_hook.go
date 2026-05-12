// Mochi server: Commit hook pattern library helper
// Copyright Alistair Cunningham 2026

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
//   - No durable _commit_log + drainer yet, so handler crash between
//     commit and hook fire means a missed fire.
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
// The call writes a pending row to the per-app `_commit_log` table
// before invoking the handler, then marks it fired on success. A failed
// handler leaves the row pending so commit_hook_drain (run periodically
// via replication_manager) retries later. Crash between the commit and
// the log insert is the documented V1 gap — the underlying SQL write is
// already durable but the hook fire would be lost.
func commit_hook_fire(userUID, appID, table, kind, rowUID string) {
	u, a, av := commit_hook_resolve(userUID, appID)
	if u == nil || a == nil || av == nil {
		return
	}
	function := commit_hook_function(av)
	if function == "" {
		return
	}

	appDB := db_app(u, a)
	if appDB == nil {
		return
	}
	// Opportunistic retry of any previously-failed hooks before logging
	// the new one — keeps the log from growing unboundedly while the
	// app is active.
	commit_hook_drain(appDB, av, a, u, function)

	seq := commit_log_append(appDB, table, kind, rowUID)
	if commit_hook_invoke(av, a, u, function, table, kind, rowUID) {
		commit_log_mark_fired(appDB, seq)
	}
}

// commit_hook_drain walks pending _commit_log entries and retries each
// against the registered handler. Successful invocations mark the row
// fired; failed ones stay pending for the next drain.
func commit_hook_drain(db *DB, av *AppVersion, a *App, u *User, function string) {
	commit_log_table_create(db)
	rows, err := db.rows("select seq, name, kind, row_uid from _commit_log where fired=0 order by seq limit 100")
	if err != nil {
		return
	}
	for _, r := range rows {
		seq, _ := r["seq"].(int64)
		table, _ := r["name"].(string)
		kind, _ := r["kind"].(string)
		rowUID, _ := r["row_uid"].(string)
		if commit_hook_invoke(av, a, u, function, table, kind, rowUID) {
			commit_log_mark_fired(db, seq)
		}
	}
}

// commit_hook_invoke calls the Starlark handler and returns whether it
// completed cleanly. Errors are logged but don't propagate; the caller
// uses the return to decide whether to mark the log row fired or leave
// it for the drainer to retry.
func commit_hook_invoke(av *AppVersion, a *App, u *User, function, table, kind, rowUID string) bool {
	if av.Architecture.Engine != "starlark" {
		return true
	}
	s := av.starlark()
	s.set("app", a)
	s.set("user", u)
	s.set("owner", u)

	if _, err := s.call(function, sl.Tuple{sl.String(table), sl.String(kind), sl.String(rowUID)}); err != nil {
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

// commit_log_table_create lazily creates _commit_log for an app DB.
func commit_log_table_create(db *DB) {
	db.exec("create table if not exists _commit_log (seq integer primary key autoincrement, name text not null, kind text not null, row_uid text not null default '', ts integer not null, fired integer not null default 0)")
	db.exec("create index if not exists commit_log_fired on _commit_log(fired, ts)")
}

// commit_log_append writes a pending row and returns its seq. The
// "name" column holds the table that committed (renamed from the
// parameter so it doesn't collide with SQL reserved-word handling).
func commit_log_append(db *DB, table, kind, rowUID string) int64 {
	commit_log_table_create(db)
	db.exec("insert into _commit_log (name, kind, row_uid, ts, fired) values (?, ?, ?, ?, 0)", table, kind, rowUID, now())
	var seq int64
	if row, _ := db.row("select seq from _commit_log where name=? and kind=? and row_uid=? and fired=0 order by seq desc limit 1", table, kind, rowUID); row != nil {
		if v, ok := row["seq"].(int64); ok {
			seq = v
		}
	}
	return seq
}

// commit_log_mark_fired marks a row as fired. Idempotent.
func commit_log_mark_fired(db *DB, seq int64) {
	if seq <= 0 {
		return
	}
	db.exec("update _commit_log set fired=1 where seq=?", seq)
}
