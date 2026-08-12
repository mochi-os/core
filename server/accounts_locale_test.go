package main

import (
	"strings"
	"testing"
)

// Every account tester must answer in the caller's language. The no-network
// branches are the ones that can be asserted without an external service.
func Test_account_test_localised(t *testing.T) {
	load_core_labels()

	cases := []struct {
		language string
		url      string // expected accounts.test.no_url
		topic    string // expected accounts.test.no_topic
		key      string // expected accounts.test.no_key
		unknown  string // expected accounts.test.unknown_type
	}{
		{"en", "No URL configured", "No topic configured", "No API key", "Unknown account type"},
		{"de", "Keine URL eingerichtet", "Kein Thema eingerichtet", "Kein API-Schlüssel", "Unbekannter Kontotyp"},
		{"ja", "URL が設定されていません", "トピックが設定されていません", "API キーがありません", "不明なアカウント種別"},
		{"ar", "لم يتم إعداد عنوان URL", "لم يتم إعداد موضوع", "لا يوجد مفتاح API", "نوع حساب غير معروف"},
	}

	for _, c := range cases {
		if got := account_test_url("", "", c.language).Message; got != c.url {
			t.Errorf("account_test_url(%s) = %q, want %q", c.language, got, c.url)
		}
		if got := account_test_ntfy("", "", "", c.language, "x").Message; got != c.topic {
			t.Errorf("account_test_ntfy(%s) = %q, want %q", c.language, got, c.topic)
		}
		if got := account_test_claude("", c.language).Message; got != c.key {
			t.Errorf("account_test_claude(%s) = %q, want %q", c.language, got, c.key)
		}
		if got := resolve_core_label(c.language, "accounts.test.unknown_type", nil); got != c.unknown {
			t.Errorf("unknown_type(%s) = %q, want %q", c.language, got, c.unknown)
		}
	}
}

// A message carrying a placeholder must substitute it, not print the ICU
// source or drop the detail.
//
// Resolved directly rather than through a provider. This used to call
// account_test_mcp against a closed port and read the error off the result,
// but the providers that take a caller-supplied address no longer report the
// underlying error - the difference between "refused", "timed out" and "no
// such host" is an oracle on an address the caller chose. The _detail form is
// still used by the fixed-vendor providers, and substitution is what this test
// is actually about.
func Test_account_test_localised_arguments(t *testing.T) {
	load_core_labels()

	for _, c := range []struct{ language, prefix string }{
		{"de", "Verbindung fehlgeschlagen: "},
		{"ja", "接続に失敗しました: "},
	} {
		got := resolve_core_label(c.language, "accounts.test.connection_failed_detail", map[string]any{"detail": "boom"})
		if !strings.HasPrefix(got, c.prefix) {
			t.Errorf("%s = %q, want prefix %q", c.language, got, c.prefix)
		}
		if !strings.Contains(got, "boom") || strings.Contains(got, "{detail}") || strings.HasSuffix(got, ": ") {
			t.Errorf("%s = %q, placeholder not substituted", c.language, got)
		}
	}
}

// Every locale catalogue carries every accounts.test.* key, so no user falls
// back to English on a path this change was meant to translate.
func Test_account_test_labels_complete(t *testing.T) {
	load_core_labels()

	english, ok := core_labels["en"]
	if !ok {
		t.Fatal("no en catalogue")
	}
	var keys []string
	for k := range english {
		if strings.HasPrefix(k, "accounts.test.") {
			keys = append(keys, k)
		}
	}
	if len(keys) != 27 {
		t.Fatalf("expected 27 accounts.test.* keys in en, got %d", len(keys))
	}

	for language, catalogue := range core_labels {
		if language == "en" || language == "en-us" {
			continue
		}
		for _, k := range keys {
			value, ok := catalogue[k]
			if !ok {
				t.Errorf("%s: missing %s", language, k)
				continue
			}
			if value == english[k] {
				t.Errorf("%s: %s still English (%q)", language, k, value)
			}
			if strings.Count(value, "{") != strings.Count(english[k], "{") {
				t.Errorf("%s: %s placeholder count differs (%q)", language, k, value)
			}
		}
	}
}
