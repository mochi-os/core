// Mochi internal/ini unit tests.
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package ini

import (
	"reflect"
	"testing"

	goini "gopkg.in/ini.v1"
)

func loadIniBytes(t *testing.T, body string) {
	t.Helper()
	f, err := goini.Load([]byte(body))
	if err != nil {
		t.Fatalf("ini.Load: %v", err)
	}
	file = f
}

func TestStringEnvOverride(t *testing.T) {
	loadIniBytes(t, "[web]\ndomain = file.example\n")

	if got := String("web", "domain", "fallback"); got != "file.example" {
		t.Errorf("without env: got %q, want %q", got, "file.example")
	}

	t.Setenv("MOCHI_WEB_DOMAIN", "env.example")
	if got := String("web", "domain", "fallback"); got != "env.example" {
		t.Errorf("with env: got %q, want %q", got, "env.example")
	}
}

func TestStringEnvEmptyOverridesFile(t *testing.T) {
	loadIniBytes(t, "[web]\ndomain = file.example\n")

	t.Setenv("MOCHI_WEB_DOMAIN", "")
	if got := String("web", "domain", "fallback"); got != "" {
		t.Errorf("explicit empty env: got %q, want empty", got)
	}
}

func TestStringFallsBackToDefault(t *testing.T) {
	loadIniBytes(t, "")
	if got := String("nope", "missing", "default"); got != "default" {
		t.Errorf("no file value, no env: got %q, want %q", got, "default")
	}
}

func TestIntEnvOverride(t *testing.T) {
	loadIniBytes(t, "[p2p]\nport = 1443\n")

	if got := Int("p2p", "port", 9999); got != 1443 {
		t.Errorf("without env: got %d, want 1443", got)
	}

	t.Setenv("MOCHI_P2P_PORT", "5555")
	if got := Int("p2p", "port", 9999); got != 5555 {
		t.Errorf("with env: got %d, want 5555", got)
	}
}

func TestIntEnvUnparseableFallsBack(t *testing.T) {
	loadIniBytes(t, "[p2p]\nport = 1443\n")
	t.Setenv("MOCHI_P2P_PORT", "not-a-number")
	if got := Int("p2p", "port", 9999); got != 1443 {
		t.Errorf("unparseable env should fall back to file: got %d, want 1443", got)
	}
}

func TestBoolEnvOverride(t *testing.T) {
	loadIniBytes(t, "[p2p]\nrelay = false\n")

	if got := Bool("p2p", "relay", false); got {
		t.Errorf("without env: got true, want false")
	}

	t.Setenv("MOCHI_P2P_RELAY", "true")
	if got := Bool("p2p", "relay", false); !got {
		t.Errorf("with env=true: got false, want true")
	}
}

func TestBoolEnvUnparseableFallsBack(t *testing.T) {
	loadIniBytes(t, "[p2p]\nrelay = true\n")
	t.Setenv("MOCHI_P2P_RELAY", "maybe")
	if got := Bool("p2p", "relay", false); !got {
		t.Errorf("unparseable env should fall back to file: got false, want true")
	}
}

func TestStringsEnvOverride(t *testing.T) {
	loadIniBytes(t, "[web]\nports = 80, 443\n")

	if got := Strings("web", "ports"); !reflect.DeepEqual(got, []string{"80", "443"}) {
		t.Errorf("without env: got %v", got)
	}

	t.Setenv("MOCHI_WEB_PORTS", "8080,8443,9000")
	want := []string{"8080", "8443", "9000"}
	if got := Strings("web", "ports"); !reflect.DeepEqual(got, want) {
		t.Errorf("with env: got %v, want %v", got, want)
	}
}

func TestIntsEnvOverride(t *testing.T) {
	loadIniBytes(t, "[web]\nports = 80, 443\n")

	t.Setenv("MOCHI_WEB_PORTS", "8080,8443")
	want := []int{8080, 8443}
	if got := Ints("web", "ports"); !reflect.DeepEqual(got, want) {
		t.Errorf("with env: got %v, want %v", got, want)
	}
}

func TestEnvNameUppercases(t *testing.T) {
	loadIniBytes(t, "")
	t.Setenv("MOCHI_DIRECTORIES_DATA", "/var/lib/test")
	if got := String("directories", "data", "/wrong"); got != "/var/lib/test" {
		t.Errorf("lowercase section/key should map to MOCHI_DIRECTORIES_DATA: got %q", got)
	}
}

func TestUnsetEnvFallsToFile(t *testing.T) {
	loadIniBytes(t, "[web]\ndomain = file.example\n")
	// MOCHI_WEB_DOMAIN deliberately not set
	if got := String("web", "domain", "default"); got != "file.example" {
		t.Errorf("unset env should yield file value: got %q", got)
	}
}

// TestAccessorsHandleNilFile covers the case where Load was never called
// successfully (e.g. mochictl tolerating a missing mochi.conf). Accessors
// must return their default rather than panic on the nil package-level file.
func TestAccessorsHandleNilFile(t *testing.T) {
	prev := file
	file = nil
	defer func() { file = prev }()

	if got := String("web", "domain", "default-string"); got != "default-string" {
		t.Errorf("String with nil file: got %q, want default-string", got)
	}
	if got := Int("p2p", "port", 1443); got != 1443 {
		t.Errorf("Int with nil file: got %d, want 1443", got)
	}
	if got := Bool("development", "reload", true); got != true {
		t.Errorf("Bool with nil file: got %v, want true", got)
	}
	if got := Strings("web", "ports"); got != nil {
		t.Errorf("Strings with nil file: got %v, want nil", got)
	}
	if got := Ints("web", "ports"); got != nil {
		t.Errorf("Ints with nil file: got %v, want nil", got)
	}

	// Env override should still take effect even with nil file.
	t.Setenv("MOCHI_DIRECTORIES_DATA", "/from-env")
	if got := String("directories", "data", "/wrong"); got != "/from-env" {
		t.Errorf("env override with nil file: got %q, want /from-env", got)
	}
}

func TestEffectiveMergesFileAndEnv(t *testing.T) {
	loadIniBytes(t, "[web]\ndomain = file.example\nports = 80,443\n[email]\nadmin = ops@example.com\n")
	t.Setenv("MOCHI_WEB_PORTS", "8080,8443")
	t.Setenv("MOCHI_NEWSECT_VALUE", "from-env")

	got := Effective()

	if got["web"]["domain"] != "file.example" {
		t.Errorf("web.domain: got %q, want file.example", got["web"]["domain"])
	}
	if got["web"]["ports"] != "8080,8443" {
		t.Errorf("env override should win: got %q, want 8080,8443", got["web"]["ports"])
	}
	if got["newsect"]["value"] != "from-env" {
		t.Errorf("env-only key should appear: got %q (full map: %v)", got["newsect"]["value"], got)
	}
}

func TestEffectiveRedactsSensitiveKeys(t *testing.T) {
	loadIniBytes(t, "[email]\npassword = supersecret\nadmin = ops@example.com\n[oauth]\nclient_secret = abc\napi_token = xyz\n")

	got := Effective()

	if got["email"]["password"] != "***redacted***" {
		t.Errorf("password not redacted: got %q", got["email"]["password"])
	}
	if got["oauth"]["client_secret"] != "***redacted***" {
		t.Errorf("client_secret not redacted: got %q", got["oauth"]["client_secret"])
	}
	if got["oauth"]["api_token"] != "***redacted***" {
		t.Errorf("api_token not redacted: got %q", got["oauth"]["api_token"])
	}
	if got["email"]["admin"] != "ops@example.com" {
		t.Errorf("non-sensitive key should not be redacted: got %q", got["email"]["admin"])
	}
}
