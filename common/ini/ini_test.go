// Mochi internal/ini unit tests.
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package ini

import (
	"os"
	"reflect"
	"strings"
	"testing"

	goini "gopkg.in/ini.v1"
)

func load_ini_bytes(t *testing.T, body string) {
	t.Helper()
	f, err := goini.Load([]byte(body))
	if err != nil {
		t.Fatalf("ini.Load: %v", err)
	}
	file = f
}

func TestStringEnvOverride(t *testing.T) {
	load_ini_bytes(t, "[web]\ndomain = file.example\n")

	if got := String("web", "domain", "fallback"); got != "file.example" {
		t.Errorf("without env: got %q, want %q", got, "file.example")
	}

	t.Setenv("MOCHI_WEB_DOMAIN", "env.example")
	if got := String("web", "domain", "fallback"); got != "env.example" {
		t.Errorf("with env: got %q, want %q", got, "env.example")
	}
}

func TestStringEnvEmptyOverridesFile(t *testing.T) {
	load_ini_bytes(t, "[web]\ndomain = file.example\n")

	t.Setenv("MOCHI_WEB_DOMAIN", "")
	if got := String("web", "domain", "fallback"); got != "" {
		t.Errorf("explicit empty env: got %q, want empty", got)
	}
}

func TestStringFallsBackToDefault(t *testing.T) {
	load_ini_bytes(t, "")
	if got := String("nope", "missing", "default"); got != "default" {
		t.Errorf("no file value, no env: got %q, want %q", got, "default")
	}
}

func TestIntEnvOverride(t *testing.T) {
	load_ini_bytes(t, "[p2p]\nport = 1443\n")

	if got := Int("p2p", "port", 9999); got != 1443 {
		t.Errorf("without env: got %d, want 1443", got)
	}

	t.Setenv("MOCHI_P2P_PORT", "5555")
	if got := Int("p2p", "port", 9999); got != 5555 {
		t.Errorf("with env: got %d, want 5555", got)
	}
}

func TestIntEnvUnparseableFallsBack(t *testing.T) {
	load_ini_bytes(t, "[p2p]\nport = 1443\n")
	t.Setenv("MOCHI_P2P_PORT", "not-a-number")
	if got := Int("p2p", "port", 9999); got != 1443 {
		t.Errorf("unparseable env should fall back to file: got %d, want 1443", got)
	}
}

func TestBoolEnvOverride(t *testing.T) {
	load_ini_bytes(t, "[p2p]\nrelay = false\n")

	if got := Bool("p2p", "relay", false); got {
		t.Errorf("without env: got true, want false")
	}

	t.Setenv("MOCHI_P2P_RELAY", "true")
	if got := Bool("p2p", "relay", false); !got {
		t.Errorf("with env=true: got false, want true")
	}
}

func TestBoolEnvUnparseableFallsBack(t *testing.T) {
	load_ini_bytes(t, "[p2p]\nrelay = true\n")
	t.Setenv("MOCHI_P2P_RELAY", "maybe")
	if got := Bool("p2p", "relay", false); !got {
		t.Errorf("unparseable env should fall back to file: got false, want true")
	}
}

func TestStringsEnvOverride(t *testing.T) {
	load_ini_bytes(t, "[web]\nports = 80, 443\n")

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
	load_ini_bytes(t, "[web]\nports = 80, 443\n")

	t.Setenv("MOCHI_WEB_PORTS", "8080,8443")
	want := []int{8080, 8443}
	if got := Ints("web", "ports"); !reflect.DeepEqual(got, want) {
		t.Errorf("with env: got %v, want %v", got, want)
	}
}

func TestEnvNameUppercases(t *testing.T) {
	load_ini_bytes(t, "")
	t.Setenv("MOCHI_DIRECTORIES_DATA", "/var/lib/test")
	if got := String("directories", "data", "/wrong"); got != "/var/lib/test" {
		t.Errorf("lowercase section/key should map to MOCHI_DIRECTORIES_DATA: got %q", got)
	}
}

func TestUnsetEnvFallsToFile(t *testing.T) {
	load_ini_bytes(t, "[web]\ndomain = file.example\n")
	// MOCHI_WEB_DOMAIN deliberately not set
	if got := String("web", "domain", "default"); got != "file.example" {
		t.Errorf("unset env should yield file value: got %q", got)
	}
}

// TestAccessorsHandleNilFile covers the case where Load was never called
// successfully (e.g. mochictl tolerating a missing mochi.conf). Accessors
// must return their default rather than panic on the nil package-level file.
func TestAccessorsHandleNilFile(t *testing.T) {
	previous := file
	file = nil
	defer func() { file = previous }()

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
	load_ini_bytes(t, "[web]\ndomain = file.example\nports = 80,443\n[email]\nadmin = ops@example.com\n")
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
	load_ini_bytes(t, "[email]\npassword = supersecret\nadmin = ops@example.com\n[oauth]\nclient_secret = abc\napi_token = xyz\n")

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

// TestRedactMatchesOnContainment: keys that contain a marker without ending in
// one, which a suffix rule would leave unmasked at /_/admin/config.
func TestRedactMatchesOnContainment(t *testing.T) {
	for _, key := range []string{
		"key_file",
		"secret_path",
		"token_url",
		"api_key_id",
		"password_file",
		"PASSWORD_FILE",
		"SecretPath",
	} {
		if got := redact(key, "sensitive"); got != "***redacted***" {
			t.Errorf("redact(%q, ...) = %q; the key names a credential and the value reaches /_/admin/config unmasked", key, got)
		}
	}
}

// TestRedactStillMatchesOnSuffix: containment is a superset, so nothing the
// suffix rule caught may be lost.
func TestRedactStillMatchesOnSuffix(t *testing.T) {
	for _, key := range []string{
		"password", "client_secret", "api_token", "signing_key",
		"PASSWORD", "Client_Secret",
	} {
		if got := redact(key, "sensitive"); got != "***redacted***" {
			t.Errorf("redact(%q, ...) = %q, want ***redacted***", key, got)
		}
	}
}

// TestRedactLeavesOrdinaryKeysAlone. /_/admin/config exists to be read, so
// over-redaction is cheap but not free: a dump where everything is masked
// tells an operator nothing.
func TestRedactLeavesOrdinaryKeysAlone(t *testing.T) {
	for _, key := range []string{
		"admin", "host", "port", "ports", "data", "signup", "reload",
		"cache", "cache_prepare", "apps", "domain", "timeout", "concurrency",
	} {
		if got := redact(key, "plain"); got != "plain" {
			t.Errorf("redact(%q, \"plain\") = %q; that key names no credential", key, got)
		}
	}
}

// TestRedactKeepsAnUnsetKeyEmpty. An unset credential returns "" rather than
// the mask, so a dump distinguishes "configured, hidden" from "not set at
// all" - which is the question an operator reading this endpoint usually has.
func TestRedactKeepsAnUnsetKeyEmpty(t *testing.T) {
	for _, key := range []string{"password", "key_file", "client_secret"} {
		if got := redact(key, ""); got != "" {
			t.Errorf("redact(%q, \"\") = %q; an unset key must not read as though it holds something", key, got)
		}
	}
}

// TestEffectiveRedactsAContainedMarker drives the exported path rather than
// the helper, since Effective is what /_/admin/config serves.
func TestEffectiveRedactsAContainedMarker(t *testing.T) {
	load_ini_bytes(t, "[tls]\nkey_file = /etc/mochi/tls.key\ncert_file = /etc/mochi/tls.crt\n")

	got := Effective()

	if got["tls"]["key_file"] != "***redacted***" {
		t.Errorf("tls.key_file: got %q, want ***redacted***", got["tls"]["key_file"])
	}
	if got["tls"]["cert_file"] != "/etc/mochi/tls.crt" {
		t.Errorf("tls.cert_file was redacted; a certificate path names no secret: got %q", got["tls"]["cert_file"])
	}
}

// TestRedactRuleIsWrittenOnce is the gate, on both halves of the defect: the
// subsumed operand, and the documentation that described it.
func TestRedactRuleIsWrittenOnce(t *testing.T) {
	source, err := os.ReadFile("ini.go")
	if err != nil {
		t.Fatalf("reading ini.go: %v", err)
	}
	text := string(source)

	at := strings.Index(text, "func redact(")
	if at < 0 {
		t.Fatal("ini.go no longer defines redact")
	}
	body := text[at:]
	if end := strings.Index(body, "\n}\n"); end > 0 {
		body = body[:end]
	}
	if strings.Contains(body, "strings.HasSuffix") {
		t.Error("redact tests HasSuffix again; Contains already implies it, so the pair reduces to Contains alone and the suffix operand only misleads the reader about which rule is in force")
	}

	// And the doc must not go back to describing a suffix rule.
	doc := text[:strings.Index(text, "func Effective(")]
	for _, glob := range []string{"*password", "*secret", "*key", "*token"} {
		if strings.Contains(doc, glob) {
			t.Errorf("Effective's documentation describes %q; that is glob suffix notation, and redact matches on containment", glob)
		}
	}
}
