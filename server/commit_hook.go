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
// host, or the app version declares no commit hook. Errors from the
// handler are logged but don't propagate — V1 has no dead-letter table
// so a failed hook on a remote-driven apply is simply missed (the
// underlying SQL write is already durable).
func commit_hook_fire(userUID, appID, table, kind, rowUID string) {
	if userUID == "" || appID == "" {
		return
	}

	udb := db_open("db/users.db")
	row, _ := udb.row("select id from users where uid=?", userUID)
	if row == nil {
		return
	}
	var localID int
	if v, ok := row["id"].(int64); ok {
		localID = int(v)
	}
	if localID == 0 {
		return
	}

	u := user_by_id(localID)
	if u == nil {
		return
	}
	a := app_by_id(appID)
	if a == nil {
		return
	}
	av := a.active(u)
	if av == nil {
		return
	}

	apps_lock.Lock()
	function := av.Commit.Function
	engine := av.Architecture.Engine
	apps_lock.Unlock()

	if function == "" {
		return
	}
	if engine != "starlark" {
		// Internal apps would wire a Go-side hook differently; not
		// supported in V1.
		return
	}

	s := av.starlark()
	s.set("app", a)
	s.set("user", u)
	s.set("owner", u)

	if _, err := s.call(function, sl.Tuple{sl.String(table), sl.String(kind), sl.String(rowUID)}); err != nil {
		warn("Commit hook %q in app %q failed: %v", function, appID, err)
	}
}
