// Mochi server: reading a system setting is authorized by that setting's own
// classification, and by nothing else.
//
// An administrator-only permission sits in FRONT of the tiers -
// require_permission tests the administrator flag before the function body - so
// the UserReadable tier would never be consulted. Each tier is exercised
// through the real builtin.
//
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the Mochi
// Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"testing"

	sl "go.starlark.net/starlark"
)

// tier_test_read calls mochi.setting.get exactly as Starlark does, for the user
// given (nil means an anonymous caller). The app holds no grants at all, which
// is the point: classification is the whole gate.
func tier_test_read(t *testing.T, user *User, name string) (string, error) {
	t.Helper()
	thread := &sl.Thread{Name: "test"}
	if user != nil {
		thread.SetLocal("user", user)
	}
	thread.SetLocal("app", create_external_app("reader"))
	value, err := api_setting_get(thread, sl.NewBuiltin("setting.get", nil), sl.Tuple{sl.String(name)}, nil)
	if err != nil {
		return "", err
	}
	text, ok := sl.AsString(value)
	if !ok {
		t.Fatalf("setting.get(%q) returned %T, not a string", name, value)
	}
	return text, nil
}

func tier_test_user(role string) *User {
	return &User{UID: "u_" + role, Username: role + "@example.com", Role: role}
}

// tier_test_tier names the tier a setting sits in. The order matches the checks
// in api_setting_get: Secret is refused to everyone before anything else, and
// the public branch returns before UserReadable is consulted, so a public
// setting's UserReadable flag says nothing about how it is read.
func tier_test_tier(def SystemSetting) string {
	switch {
	case def.Secret:
		return "secret"
	case def.Public:
		return "public"
	case def.UserReadable:
		return "user"
	default:
		return "administrator"
	}
}

// tier_test_classified asserts the fixture still sits in the tier the test
// depends on. Reclassifying a setting is legitimate; silently invalidating the
// test that guards its tier is not.
func tier_test_classified(t *testing.T, name, tier string) {
	t.Helper()
	def, exists := system_settings[name]
	if !exists {
		t.Fatalf("%s is no longer a system setting; this test needs a fixture in the %s tier", name, tier)
	}
	if got := tier_test_tier(def); got != tier {
		t.Fatalf("%s is now in the %s tier, not %s, so this test no longer covers the tier it names", name, got, tier)
	}
}

// TestUserReadableSettingReachesANonAdministrator is the finding. default_theme
// is UserReadable, and the settings app reads it to render every user's own
// preferences page. An administrator-only permission in front of the tiers made
// that read fail for everyone who is not an administrator.
func TestUserReadableSettingReachesANonAdministrator(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()
	db_create()

	tier_test_classified(t, "default_theme", "user")

	value, err := tier_test_read(t, tier_test_user("user"), "default_theme")
	if err != nil {
		t.Fatalf("an ordinary user cannot read default_theme: %v\nthe UserReadable tier is unreachable, so every non-administrator's preferences page fails", err)
	}
	if value == "" {
		t.Error("default_theme read back empty; the tier is reachable but the value is not being returned")
	}
}

// TestServerVersionReachesANonAdministrator. The other inhabitant of the tier,
// so a fixture-specific fix to default_theme does not pass this file.
func TestServerVersionReachesANonAdministrator(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()
	db_create()

	tier_test_classified(t, "server_version", "user")

	if _, err := tier_test_read(t, tier_test_user("user"), "server_version"); err != nil {
		t.Errorf("an ordinary user cannot read server_version: %v", err)
	}
}

// TestAdministratorTierStaysRefused is the control. Removing the permission must
// not widen anything: a setting in neither public tier is still administrators
// only, decided by the setting, not by a grant.
func TestAdministratorTierStaysRefused(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()
	db_create()

	for _, name := range []string{"email_from", "hostname", "relay", "login_app"} {
		tier_test_classified(t, name, "administrator")
		if _, err := tier_test_read(t, tier_test_user("user"), name); err == nil {
			t.Errorf("an ordinary user read %s; operator configuration is administrators only", name)
		}
	}
}

// TestAdministratorReadsTheAdministratorTier. Without this the test above would
// pass on a build where nobody can read anything.
func TestAdministratorReadsTheAdministratorTier(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()
	db_create()

	setting_set("email_from", "noreply@example.com")
	value, err := tier_test_read(t, tier_test_user("administrator"), "email_from")
	if err != nil {
		t.Fatalf("an administrator cannot read email_from: %v", err)
	}
	if value != "noreply@example.com" {
		t.Errorf("email_from read back %q, want the stored value", value)
	}
}

// TestPublicSettingStaysAnonymous. The login page renders the operator's
// branding and its enabled sign-in methods before anyone has signed in, so the
// public tier has to answer with no user on the thread at all.
func TestPublicSettingStaysAnonymous(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()
	db_create()

	for _, name := range []string{"operator_name", "signup_enabled", "auth_email"} {
		tier_test_classified(t, name, "public")
		if _, err := tier_test_read(t, nil, name); err != nil {
			t.Errorf("an anonymous caller cannot read the public setting %s: %v\nthe login page renders this before sign-in", name, err)
		}
	}
}

// TestSecretSettingIsNeverReturned. Secret is the tier no permission could ever
// express, and it is the reason the removal is safe: a credential is not handed
// back even to an administrator who is allowed to set it.
func TestSecretSettingIsNeverReturned(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()
	db_create()

	tier_test_classified(t, "oauth_google_client_secret", "secret")
	setting_set("oauth_google_client_secret", "the-real-secret")

	value, err := tier_test_read(t, tier_test_user("administrator"), "oauth_google_client_secret")
	if err != nil {
		t.Fatalf("administrator read of a secret errored: %v", err)
	}
	if value != "" {
		t.Errorf("the stored credential came back as %q; secrets are write-only", value)
	}
	if _, err := tier_test_read(t, tier_test_user("user"), "oauth_google_client_secret"); err == nil {
		t.Error("an ordinary user reached a secret setting")
	}
}

// TestSettingListStaysAdministratorOnly. list carries its own administrator
// check in its body, so dropping the permission does not widen it - it hands
// back every setting's value, description and which secrets are set.
func TestSettingListStaysAdministratorOnly(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()
	db_create()

	call := func(user *User) error {
		thread := &sl.Thread{Name: "test"}
		thread.SetLocal("user", user)
		thread.SetLocal("app", create_external_app("reader"))
		_, err := api_setting_list(thread, sl.NewBuiltin("setting.list", nil), sl.Tuple{}, nil)
		return err
	}

	if err := call(tier_test_user("user")); err == nil {
		t.Error("an ordinary user enumerated every system setting")
	}
	if err := call(tier_test_user("administrator")); err != nil {
		t.Errorf("an administrator cannot list settings: %v", err)
	}
}

// TestEverySettingSitsInATier. A setting added with no classification lands in
// the administrator tier by default, which is the safe direction; this pins that
// the fixtures above still cover all four, so the file cannot quietly stop
// testing a tier that has emptied out.
func TestEverySettingSitsInATier(t *testing.T) {
	counts := map[string]int{}
	for _, def := range system_settings {
		counts[tier_test_tier(def)]++
	}
	for _, tier := range []string{"public", "user", "administrator", "secret"} {
		if counts[tier] == 0 {
			t.Errorf("no setting is classified %q, so the test for that tier proves nothing", tier)
		}
	}
}
