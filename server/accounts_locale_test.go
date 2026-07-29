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
func Test_account_test_localised_arguments(t *testing.T) {
	load_core_labels()

	got := account_test_mcp("http://127.0.0.1:9/mcp", "", "de").Message
	if !strings.HasPrefix(got, "Verbindung fehlgeschlagen: ") {
		t.Errorf("mcp de = %q, want German prefix", got)
	}
	if strings.Contains(got, "{detail}") || strings.HasSuffix(got, ": ") {
		t.Errorf("mcp de = %q, placeholder not substituted", got)
	}

	got = account_test_mcp("http://127.0.0.1:9/mcp", "", "ja").Message
	if !strings.HasPrefix(got, "接続に失敗しました: ") {
		t.Errorf("mcp ja = %q, want Japanese prefix", got)
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
