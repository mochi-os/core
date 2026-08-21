// Mochi server: the calling app must be read through principal_app.
//
// An unset thread local is a nil interface, which does not assert to a concrete
// type - it panics. Locals are unset for the whole of module load, so a
// module-level mochi.* call reaches a builtin with no app.
//
// Copyright © 2026 Mochisoft OU
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the Mochi
// Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	sl "go.starlark.net/starlark"
)

// module_level runs one Starlark statement at a file's top level, exactly as
// starlark() does when it loads an app, and reports whether it panicked.
func module_level(t *testing.T, statement string) (panicked any) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "module.star")
	if err := os.WriteFile(path, []byte(statement+"\n"), 0o644); err != nil {
		t.Fatalf("writing %s: %v", path, err)
	}
	defer func() { panicked = recover() }()
	starlark([]string{path})
	return nil
}

func TestModuleLevelApiCallsDoNotPanic(t *testing.T) {
	entity := strings.Repeat("e", 50)
	for _, statement := range []string{
		// The natural way to write a module constant, and the shortest path
		// to the fault.
		`BASE = mochi.app.url()`,
		`mochi.app.services()`,
		`mochi.access.revoke("s", "r", "o")`,
		`mochi.access.clear.resource("r")`,
		`mochi.access.clear.subject("s")`,
		`mochi.access.list.resource("r")`,
		`mochi.access.list.subject("s")`,
		`mochi.access.check("", "r", "read")`,
		`mochi.access.allow("s", "r", "o")`,
		`mochi.access.deny("s", "r", "o")`,
		`mochi.git.init("` + entity + `")`,
		`mochi.git.delete("` + entity + `")`,
		`mochi.git.branches("` + entity + `")`,
		`mochi.schedule.list()`,
		`mochi.token.create("a", "b")`,
	} {
		t.Run(statement, func(t *testing.T) {
			if r := module_level(t, statement); r != nil {
				t.Errorf("%s at module level panicked: %v", statement, r)
			}
		})
	}
}

// A panic loading one file unwinds out of starlark(), and starlark_once.Do
// counts a panicking f as done - the app version keeps nil globals for the life
// of the process. The fault is in the first file, the handler in the second.
func TestModuleLoadPanicDoesNotPoisonTheAppVersion(t *testing.T) {
	dir := t.TempDir()
	first := filepath.Join(dir, "constants.star")
	second := filepath.Join(dir, "handlers.star")
	os.WriteFile(first, []byte("BASE = mochi.app.url()\n"), 0o644)
	os.WriteFile(second, []byte("def handler():\n    return 1\n"), 0o644)

	av := &AppVersion{Execute: []string{first, second}}
	saved := dev_reload
	dev_reload = false
	defer func() { dev_reload = saved }()

	func() {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("loading the app panicked: %v", r)
			}
		}()
		av.starlark()
	}()

	// The Once has now run. If the first file had panicked, it ran to
	// completion having stored nothing, and this second call - every real
	// call after the first - gets an empty interpreter.
	s := av.starlark()
	if !s.has("handler") {
		t.Error("the app version has no handler after module load: a fault in an earlier file took the later ones with it, and starlark_once will not retry")
	}
}

// TestPrincipalAppReturnsNilForAnUnsetLocal pins the accessor's whole reason
// for existing.
func TestPrincipalAppReturnsNilForAnUnsetLocal(t *testing.T) {
	thread := &sl.Thread{Name: "test"}

	func() {
		defer func() {
			if r := recover(); r != nil {
				t.Errorf("principal_app panicked on a thread with no locals: %v", r)
			}
		}()
		if app := principal_app(thread); app != nil {
			t.Errorf("principal_app returned %v for an unset local, want nil", app)
		}
	}()

	// A bare assertion is what it replaces; this is the panic being avoided.
	func() {
		defer func() {
			if recover() == nil {
				t.Error("a bare t.Local(\"app\").(*App) no longer panics on an unset local; if the language changed, the accessor's doc comment needs revisiting")
			}
		}()
		var value any = thread.Local("app")
		_ = value.(*App)
	}()
}

// TestPrincipalAppPassesThroughASetApp: the accessor must not change what the
// 47 call sites saw when the local WAS set, including a typed nil - which
// asserts successfully and is the case each `if app == nil` guard was
// actually catching.
func TestPrincipalAppPassesThroughASetApp(t *testing.T) {
	thread := &sl.Thread{Name: "test"}
	app := &App{id: "test"}
	thread.SetLocal("app", app)
	if got := principal_app(thread); got != app {
		t.Errorf("principal_app returned %v, want the app that was set", got)
	}

	thread.SetLocal("app", (*App)(nil))
	if got := principal_app(thread); got != nil {
		t.Errorf("principal_app returned %v for a typed nil, want nil", got)
	}

	// Something that is not an *App must read as absent, not panic.
	thread.SetLocal("app", "not an app")
	func() {
		defer func() {
			if r := recover(); r != nil {
				t.Errorf("principal_app panicked on a local of the wrong type: %v", r)
			}
		}()
		if got := principal_app(thread); got != nil {
			t.Errorf("principal_app returned %v for a non-*App local, want nil", got)
		}
	}()
}

// TestNoBareAppLocalAssertionsRemain is the sweep's gate. Only the BARE form
// is a defect - a comma-ok read of the same local already yields nil rather
// than panicking, which is exactly what the accessor does.
func TestNoBareAppLocalAssertionsRemain(t *testing.T) {
	bare := regexp.MustCompile(`(^|[^,])\s*:?=\s*\w*\.?Local\("app"\)\.\(\*App\)`)
	checked := regexp.MustCompile(`,\s*(ok|_)\s*:?=`)

	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("reading the package directory: %v", err)
	}
	for _, entry := range entries {
		name := entry.Name()
		if !strings.HasSuffix(name, ".go") || name == "principal.go" || name == "thread_locals_test.go" {
			continue
		}
		data, err := os.ReadFile(name)
		if err != nil {
			t.Fatalf("reading %s: %v", name, err)
		}
		for number, line := range strings.Split(string(data), "\n") {
			if bare.MatchString(line) && !checked.MatchString(line) {
				t.Errorf("%s:%d asserts the app local directly: %s\n    use principal_app(t) - a bare assertion panics when the local is unset, which is every module-level call", name, number+1, strings.TrimSpace(line))
			}
		}
	}
}

// TestGitApiGuardsTheApp: the 24 mochi.git.* builtins guarded owner and not
// app, so comma-ok alone would have moved the panic to git_repo_path's app.id
// rather than removing it.
func TestGitApiGuardsTheApp(t *testing.T) {
	data, err := os.ReadFile("git.go")
	if err != nil {
		t.Fatalf("reading git.go: %v", err)
	}
	source := string(data)

	owner_guards := strings.Count(source, `return sl_error(fn, "no owner")`)
	app_guards := strings.Count(source, `return sl_error(fn, "no app")`)
	if owner_guards == 0 {
		t.Fatal("git.go no longer guards the owner; this test's premise is gone")
	}
	if app_guards < owner_guards {
		t.Errorf("git.go guards the owner %d times but the app only %d; git_repo_path dereferences app.id, so an unguarded nil app is a deref rather than the error the builtin means to return", owner_guards, app_guards)
	}
}
