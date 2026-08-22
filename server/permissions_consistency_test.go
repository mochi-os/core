// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	sl "go.starlark.net/starlark"
)

// verbs that are deliberately not read or write: they name capabilities rather
// than mutations, so calling any of them "write" would describe the wrong
// thing.
var permission_verbs_exempt = map[string]bool{
	"accounts/ai": true, "accounts/notify": true,
	"apps/install": true, "camera": true, "entity/sign": true, "microphone": true,
	"notifications/send": true, "server/update": true, "tokens/create": true,
	"user/authentication/sign": true, "user/close": true, "user/export": true,
	"webpush/send": true,
}

// TestPermissionsAreReadWriteOrDeliberateVerbs. The registry had twelve
// different verbs, with `manage` and `write` both meaning write. Read/write is
// now the shape; anything else has to be in the exempt list above, so a new
// bespoke verb fails here rather than quietly joining the sprawl.
func TestPermissionsAreReadWriteOrDeliberateVerbs(t *testing.T) {
	for _, p := range permissions {
		if permission_verbs_exempt[p.Name] {
			continue
		}
		leaf := p.Name
		if i := strings.LastIndex(leaf, "/"); i >= 0 {
			leaf = leaf[i+1:]
		}
		if leaf != "read" && leaf != "write" {
			t.Errorf("permission %q ends in %q; use read/write, or add it to permission_verbs_exempt with a reason", p.Name, leaf)
		}
	}
	for name := range permission_verbs_exempt {
		found := false
		for _, p := range permissions {
			if p.Name == name {
				found = true
			}
		}
		if !found {
			t.Errorf("permission_verbs_exempt lists %q, which is no longer in the registry", name)
		}
	}
}

// TestNoManagePermissionsRemain. `manage` and `write` were both in use for the
// same thing across four namespaces.
func TestNoManagePermissionsRemain(t *testing.T) {
	for _, p := range permissions {
		if strings.HasSuffix(p.Name, "/manage") {
			t.Errorf("permission %q uses manage; the write half of a namespace is spelled write", p.Name)
		}
	}
	for _, f := range []string{"permissions.go", "accounts.go", "groups.go", "apps.go"} {
		body, err := os.ReadFile(f)
		if err != nil {
			t.Fatalf("read %s: %v", f, err)
		}
		if strings.Contains(string(body), "/manage\"") {
			t.Errorf("%s still names a */manage permission", f)
		}
	}
}

// TestEveryPermissionHasALabelEverywhere. The catalogue is shown to the user by
// name, so a permission with no label renders as English (or blank) in every
// other locale. en-us is the deliberate 19-key sparse overlay.
func TestEveryPermissionHasALabelEverywhere(t *testing.T) {
	files, err := os.ReadDir("labels")
	if err != nil {
		t.Fatalf("read labels: %v", err)
	}
	for _, f := range files {
		locale := strings.TrimSuffix(f.Name(), ".conf")
		if !strings.HasSuffix(f.Name(), ".conf") || locale == "en-us" {
			continue
		}
		body, err := os.ReadFile("labels/" + f.Name())
		if err != nil {
			t.Fatalf("read %s: %v", f.Name(), err)
		}
		present := map[string]bool{}
		for _, line := range strings.Split(string(body), "\n") {
			if key, value, ok := strings.Cut(line, " = "); ok && strings.TrimSpace(value) != "" {
				present[key] = true
			}
		}
		for _, p := range permissions {
			key := "permissions." + strings.ReplaceAll(p.Name, "/", ".")
			if !present[key] {
				t.Errorf("%s has no %s label", locale, key)
			}
		}
	}
}

// TestReadAndWriteHalvesAreSplitHonestly. Two reads were filed under a
// write-shaped permission, so an app that only wanted to display something had
// to be granted the power to change it - and being restricted, it could not ask
// for the lesser one instead.
func TestReadAndWriteHalvesAreSplitHonestly(t *testing.T) {
	body, err := os.ReadFile("permissions.go")
	if err != nil {
		t.Fatalf("read permissions.go: %v", err)
	}
	text := string(body)

	list := text[strings.Index(text, "func api_permission_list("):]
	list = list[:strings.Index(list, "\n}")]
	if !strings.Contains(list, `"permissions/read"`) {
		t.Error("mochi.permission.list does not require permissions/read; reading which grants an app holds is a display concern")
	}
	for _, name := range []string{"api_permission_grant", "api_permission_revoke"} {
		body := text[strings.Index(text, "func "+name+"("):]
		body = body[:strings.Index(body, "\n}")]
		if !strings.Contains(body, `"permissions/write"`) {
			t.Errorf("%s does not require permissions/write", name)
		}
	}
}

// TestSettingReadsAreGatedByTheSettingAlone: a permission in front of a setting
// read refuses non-administrators before the UserReadable tier is consulted,
// making that tier unreachable. Reads are per-setting; writes keep
// settings/write.
func TestSettingReadsAreGatedByTheSettingAlone(t *testing.T) {
	body, err := os.ReadFile("settings.go")
	if err != nil {
		t.Fatalf("read settings.go: %v", err)
	}
	text := string(body)
	for _, name := range []string{"api_setting_get", "api_setting_list"} {
		fn := text[strings.Index(text, "func "+name+"("):]
		fn = fn[:strings.Index(fn, "\n}")]
		if strings.Contains(fn, `require_permission(t, fn, "settings/read")`) {
			t.Errorf("%s gates on settings/read again; an administrator-only permission there makes the UserReadable tier unreachable", name)
		}
	}
	set := text[strings.Index(text, "func api_setting_set("):]
	set = set[:strings.Index(set, "\n}")]
	if !strings.Contains(set, `require_permission(t, fn, "settings/write")`) {
		t.Error("api_setting_set no longer requires settings/write; writes have no per-setting statement of who may write, so the permission is the only gate")
	}
	for _, p := range permissions {
		if p.Name == "settings/read" {
			t.Error("settings/read is back in the catalogue")
		}
	}
}

// TestDocumentGetStaysUngated. help and login render the operator's terms and
// privacy pages, to visitors who have not signed in. Enumerating documents and
// rewriting them are gated; fetching one by name is the public path.
func TestDocumentGetStaysUngated(t *testing.T) {
	body, err := os.ReadFile("documents.go")
	if err != nil {
		t.Fatalf("read documents.go: %v", err)
	}
	text := string(body)
	get := text[strings.Index(text, "func api_document_get("):]
	get = get[:strings.Index(get, "\n}")]
	if strings.Contains(get, "require_permission") {
		t.Error("api_document_get is gated; help and login show terms pages to anonymous visitors through it")
	}
	for _, name := range []string{"api_document_list", "api_document_set"} {
		fn := text[strings.Index(text, "func "+name+"("):]
		fn = fn[:strings.Index(fn, "\n}")]
		if !strings.Contains(fn, "require_permission") {
			t.Errorf("%s has no permission", name)
		}
	}
}

// TestEveryAppRegistryAPIIsGated. Thirteen were administrator-only with no app
// check and seven had no check at all, so any app an administrator opened could
// rewrite the URL prefix another app is served under - including the login app,
// which core exempts from its own auth gates.
func TestEveryAppRegistryAPIIsGated(t *testing.T) {
	body, err := os.ReadFile("apps.go")
	if err != nil {
		t.Fatalf("read apps.go: %v", err)
	}
	// Need no grant: the first five read only the calling app's own namespace,
	// and app.presets is pure computation over three static density bundles.
	confined := map[string]bool{
		"api_app_package_get": true, "api_app_asset_exists": true,
		"api_app_asset_list": true, "api_app_asset_read": true,
		"api_app_label": true, "api_app_url": true, "api_app_services": true,
		"api_app_presets": true,
	}
	functions := regexp.MustCompile(`(?sm)^func (api_app[a-z_0-9]*)\(t \*sl\.Thread.*?\n\}$`)
	found := 0
	for _, m := range functions.FindAllStringSubmatch(string(body), -1) {
		name, fn := m[1], m[0]
		if confined[name] {
			continue
		}
		found++
		if !strings.Contains(fn, "require_permission") && !strings.Contains(fn, "user_allowed") {
			t.Errorf("%s has neither a permission nor a user_allowed filter", name)
		}
	}
	if found < 20 {
		t.Errorf("only %d apps.go builtins scanned; the pattern is not matching", found)
	}
}

// TestDefaultAppsHoldWhatTheyCall. Every permission added here is restricted,
// so a consent dialog cannot supply it: an app that calls the API and lacks the
// apps_default grant is simply broken, with no way for the user to fix it.
func TestDefaultAppsHoldWhatTheyCall(t *testing.T) {
	required := map[string][]string{
		"Apps":      {"apps/read", "apps/write", "permissions/read", "permissions/write"},
		"Publisher": {"apps/write"},
		"Menu":      {"apps/read", "permissions/read", "permissions/write"},
		"Settings": {"apps/read", "settings/write", "server/read",
			"documents/read", "documents/write", "domains/read", "domains/write",
			"user/verification/write",
			// notifications/category/list draws the notification categories page.
			"notifications/read"},
	}
	for name, permissions := range required {
		held := map[string]bool{}
		for _, app := range apps_default {
			if app.Name != name {
				continue
			}
			for _, g := range app.Permissions {
				held[g.Permission] = true
			}
		}
		for _, p := range permissions {
			if !held[p] {
				t.Errorf("default app %q calls an API needing %q but has no such grant", name, p)
			}
		}
	}
}

// TestVerificationCodesAreNotExport. mochi.user.code.send and .verify are an
// email confirmation ceremony; they were gated on user/export, which describes
// something else entirely and is what the user sees in the dialog.
func TestVerificationCodesAreNotExport(t *testing.T) {
	body, err := os.ReadFile("users.go")
	if err != nil {
		t.Fatalf("read users.go: %v", err)
	}
	text := string(body)
	for _, name := range []string{"api_user_code_send", "api_user_code_verify"} {
		fn := text[strings.Index(text, "func "+name+"("):]
		fn = fn[:strings.Index(fn, "\n}")]
		if !strings.Contains(fn, `"user/verification/write"`) {
			t.Errorf("%s does not require user/verification/write", name)
		}
		if strings.Contains(fn, `"user/export"`) {
			t.Errorf("%s still requires user/export", name)
		}
	}
	// The export API itself lives in its own file and keeps user/export.
	exported, err := os.ReadFile("api_user_export.go")
	if err != nil {
		t.Fatalf("read api_user_export.go: %v", err)
	}
	if !strings.Contains(string(exported), `require_permission(t, fn, "user/export")`) {
		t.Error("api_user_export no longer requires user/export")
	}
}

var _ = sl.None

// TestDefaultGrantsReachExistingInstalls pins app_user_setup's counter to
// len(defaults)+1: re-running when the count changes is what carries a newly
// added apps_default grant to accounts that already exist.
func TestDefaultGrantsReachExistingInstalls(t *testing.T) {
	body, err := os.ReadFile("permissions.go")
	if err != nil {
		t.Fatalf("read permissions.go: %v", err)
	}
	text := string(body)
	start := strings.Index(text, "func app_user_setup(")
	if start < 0 {
		t.Fatal("app_user_setup not found in permissions.go")
	}
	end := strings.Index(text[start:], "\n}\n")
	if end < 0 {
		t.Fatal("could not find the end of app_user_setup")
	}
	function := text[start : start+end]

	if !strings.Contains(function, "expected := len(defaults) + 1") {
		t.Error("app_user_setup no longer derives its setup counter from the size of the default set, so adding a permission to apps_default would never reach an account that already exists")
	}
	if !strings.Contains(function, "setup == expected") {
		t.Error("app_user_setup no longer compares the recorded counter against the current default set, so a changed default set would not re-run setup")
	}
}

// TestPasskeyVerifyIsGatedByTheSigningPermission. verify.begin/finish run an
// assertion and create no session, but demanded user/authentication/write -
// which its own registry comment defines as rewriting how the account
// authenticates, since recovery.generate invalidates the codes the user holds
// and totp.setup drops their authenticator. An app that only wants a step-up
// prompt had to be granted that. user/authentication/sign exists for exactly
// this and gated nothing.
func TestPasskeyVerifyIsGatedByTheSigningPermission(t *testing.T) {
	body, err := os.ReadFile("passkeys.go")
	if err != nil {
		t.Fatalf("read passkeys.go: %v", err)
	}
	text := string(body)

	for _, name := range []string{"api_user_passkey_verify_begin", "api_user_passkey_verify_finish"} {
		fn := text[strings.Index(text, "func "+name+"("):]
		fn = fn[:strings.Index(fn, "\n}")]
		if !strings.Contains(fn, `require_permission(t, fn, "user/authentication/sign")`) {
			t.Errorf("%s does not require user/authentication/sign", name)
		}
		if strings.Contains(fn, `require_permission(t, fn, "user/authentication/write")`) {
			t.Errorf("%s still requires user/authentication/write; an assertion must not cost the power to rewrite the account's factors", name)
		}
	}

	// The other direction. These do change the credential set, so demoting them
	// to the assertion permission would be the same mistake inverted.
	for _, name := range []string{
		"api_user_passkey_register_begin", "api_user_passkey_register_finish",
		"api_user_passkey_rename", "api_user_passkey_delete",
	} {
		fn := text[strings.Index(text, "func "+name+"("):]
		fn = fn[:strings.Index(fn, "\n}")]
		if !strings.Contains(fn, `require_permission(t, fn, "user/authentication/write")`) {
			t.Errorf("%s does not require user/authentication/write; it mutates the credential set", name)
		}
	}
}

// TestEveryDeclaredPermissionIsEnforcedSomewhere. user/authentication/sign was
// declared, granted to Settings by default, shown in the grant UI and revocable
// - and passed to require_permission nowhere, so revoking it changed nothing. A
// permission the user can act on has to gate something.
//
// The exempt names are enforced outside require_permission: app.json service
// declarations are checked in api.go, and the capture pair is surfaced by the
// browser's own prompt rather than by core.
func TestEveryDeclaredPermissionIsEnforcedSomewhere(t *testing.T) {
	elsewhere := map[string]bool{
		"camera":              true,
		"microphone":          true,
		"friends/read":        true,
		"notifications/read":  true,
		"notifications/send":  true,
		"notifications/write": true,
		"repositories/read":   true,
		"repositories/write":  true,
	}

	sources, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatalf("glob: %v", err)
	}
	enforced := ""
	for _, file := range sources {
		if strings.HasSuffix(file, "_test.go") {
			continue
		}
		body, err := os.ReadFile(file)
		if err != nil {
			t.Fatalf("read %s: %v", file, err)
		}
		enforced = enforced + string(body)
	}

	checked := 0
	for _, permission := range permissions {
		if elsewhere[permission.Name] {
			continue
		}
		checked++
		if !strings.Contains(enforced, `require_permission(t, fn, "`+permission.Name+`")`) &&
			!strings.Contains(enforced, `require_permission_acting(t, fn, "`+permission.Name+`")`) {
			t.Errorf("permission %q is declared and grantable but reaches no require_permission call; revoking it changes nothing", permission.Name)
		}
	}
	if checked < 20 {
		t.Errorf("only %d permissions scanned; the registry is not being read", checked)
	}
}
