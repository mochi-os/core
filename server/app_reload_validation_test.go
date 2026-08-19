// Mochi server: a hot-reloaded manifest is validated like a loaded one.
//
// reload() re-applies fourteen manifest fields onto a live AppVersion and
// re-ran only themes_validate - its own comment said so. Everything else
// app_read checks was skipped, and an action's File is concatenated onto
// av.base and handed to c.File with nothing else looking at it, so a reloaded
// "../../../etc/shadow" was served.
//
// Dev-only: reload runs under dev_reload, which is off by default and unset on
// yuzu. The exposure is a machine where the manifest's author already has the
// filesystem - so this is a validation layer that silently was not there, not
// a privilege boundary.
//
// Copyright © 2026 Mochisoft OU
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// reload_app writes an app.json and returns a loaded version pointing at it,
// as though the server had read it at startup.
func reload_app(t *testing.T, manifest string) *AppVersion {
	t.Helper()
	base := t.TempDir()
	if err := os.WriteFile(filepath.Join(base, "app.json"), []byte(manifest), 0o600); err != nil {
		t.Fatalf("writing app.json: %v", err)
	}
	av := &AppVersion{base: base, Version: "1.0", Label: "loaded"}
	av.Actions = map[string]AppAction{"index": {File: "index.html"}}
	av.Architecture.Engine = "starlark"
	av.Architecture.Version = app_version_maximum
	return av
}

// reload_manifest is a minimal valid app.json with the action file swapped in.
func reload_manifest(file string) string {
	return `{"version":"1.1","label":"reloaded","architecture":{"engine":"starlark","version":` +
		itoa(app_version_maximum) + `},"actions":{"index":{"file":"` + file + `"}}}`
}

// reload_now runs the reload, defeating the mtime gate so the test does not
// depend on filesystem timestamp resolution.
func reload_now(av *AppVersion) {
	apps_lock.Lock()
	av.app_json_mtime = time.Time{}
	apps_lock.Unlock()
	av.reload()
}

// TestReloadRefusesAnEscapingActionFile is the defect. The path is joined to
// av.base and served, so nothing downstream catches it.
func TestReloadRefusesAnEscapingActionFile(t *testing.T) {
	av := reload_app(t, reload_manifest("../../../../etc/shadow"))

	reload_now(av)

	if got := av.Actions["index"].File; got != "index.html" {
		t.Errorf("reload applied the action file %q; app_read refuses it with \"App bad file path\", and web_action joins it to the app's base directory and serves it", got)
	}
	if av.Label != "loaded" {
		t.Error("reload applied a manifest it should have rejected wholesale; a failed validation must keep the running version")
	}
}

// TestReloadRefusesEveryFieldAppReadChecks: the action file is the reachable
// one, but the gap was the whole validation, so the fix has to be the whole
// validation. Each of these is refused by app_read at load.
func TestReloadRefusesEveryFieldAppReadChecks(t *testing.T) {
	for name, manifest := range map[string]string{
		"bad version":        `{"version":"not a version","label":"reloaded","architecture":{"engine":"starlark","version":` + itoa(app_version_maximum) + `}}`,
		"bad engine":         `{"version":"1.1","label":"reloaded","architecture":{"engine":"wasm","version":` + itoa(app_version_maximum) + `}}`,
		"bad execute":        `{"version":"1.1","label":"reloaded","architecture":{"engine":"starlark","version":` + itoa(app_version_maximum) + `},"execute":["../escape.star"]}`,
		"bad database file":  `{"version":"1.1","label":"reloaded","architecture":{"engine":"starlark","version":` + itoa(app_version_maximum) + `},"database":{"file":"../../other.db"}}`,
		"bad event function": `{"version":"1.1","label":"reloaded","architecture":{"engine":"starlark","version":` + itoa(app_version_maximum) + `},"events":{"ping":{"function":"not a function"}}}`,
		"bad service":        `{"version":"1.1","label":"reloaded","architecture":{"engine":"starlark","version":` + itoa(app_version_maximum) + `},"services":["not a service"]}`,
	} {
		av := reload_app(t, manifest)
		reload_now(av)
		if av.Label != "loaded" {
			t.Errorf("%s: reload applied the manifest; app_read refuses it at load", name)
		}
	}
}

// TestReloadStillAppliesAValidManifest is the guard against the validation
// refusing everything - reload exists so a developer's edit takes effect.
func TestReloadStillAppliesAValidManifest(t *testing.T) {
	av := reload_app(t, reload_manifest("page.html"))

	reload_now(av)

	if av.Label != "reloaded" {
		t.Fatalf("reload did not apply a valid manifest (label %q)", av.Label)
	}
	if got := av.Actions["index"].File; got != "page.html" {
		t.Errorf("the reloaded action file is %q, want page.html", got)
	}
}

// TestReloadValidatesBeforeMakingExecuteAbsolute: manifest_validate checks
// Execute entries as relative filepaths, so running it after the conversion
// would reject every app - av.base is rooted.
func TestReloadValidatesBeforeMakingExecuteAbsolute(t *testing.T) {
	av := reload_app(t, `{"version":"1.1","label":"reloaded","architecture":{"engine":"starlark","version":`+
		itoa(app_version_maximum)+`},"execute":["app.star"]}`)

	reload_now(av)

	if av.Label != "reloaded" {
		t.Fatal("reload refused a manifest whose execute path is an ordinary relative file")
	}
	if len(av.Execute) != 1 || !strings.HasPrefix(av.Execute[0], av.base+"/") {
		t.Errorf("execute = %v, want the app's base directory prefixed", av.Execute)
	}
}

// TestReloadAndLoadShareOneValidation pins the shape. Two lists of checks is
// how this drifted: themes_validate was added to reload when themes landed and
// nothing generalised it.
func TestReloadAndLoadShareOneValidation(t *testing.T) {
	for _, function := range []string{"func app_read(", "func (av *AppVersion) reload()"} {
		body := function_body(t, "apps.go", function)
		if !strings.Contains(body, "manifest_validate(") {
			t.Errorf("%s does not call manifest_validate; the two paths validate differently again", function)
		}
		if strings.Contains(body, "themes_validate(") {
			t.Errorf("%s calls themes_validate directly rather than through manifest_validate, which is the drift this replaced", function)
		}
	}
}
