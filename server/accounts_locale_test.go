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

// A message carrying a placeholder must substitute it, not print the ICU source
// or drop the detail. Resolved directly: the providers taking a caller-supplied
// address deliberately do not report the underlying error.
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

// The provider form's field labels reach two consumers: lib/web's account-add
// dialog renders them as the field labels, and settings / notifications
// interpolate them into the translated errors.field_required sentence. They
// were English literals, so a Japanese user read "API keyは必須です" and saw an
// English form. They are keys now, resolved against the caller's language.
func Test_account_provider_field_labels_are_keys(t *testing.T) {
	load_core_labels()

	for _, p := range providers {
		for _, f := range p.Fields {
			if !strings.HasPrefix(f.Label, "accounts.field.") {
				t.Errorf("provider %q field %q label %q is not a label key",
					p.Type, f.Name, f.Label)
				continue
			}
			// resolve_core_label answers with the key itself when nothing
			// matches, so a key with no English entry would ship the key to the
			// user. Catch that here rather than in the form.
			if got := resolve_core_label("en", f.Label, nil); got == f.Label {
				t.Errorf("provider %q field %q key %q has no English label",
					p.Type, f.Name, f.Label)
			}
		}
	}
}

// The resolution itself, per language. account-add renders whatever this
// returns, so an unresolved key or an English fallback is what the user sees.
func Test_account_provider_field_labels_resolve(t *testing.T) {
	load_core_labels()

	cases := []struct {
		language string
		key      string // accounts.field.key
		address  string // accounts.field.address
		token    string // accounts.field.token
	}{
		{"en", "API key", "Email address", "Access token"},
		{"de", "API-Schlüssel", "E-Mail-Adresse", "Zugriffstoken"},
		{"ja", "API キー", "メールアドレス", "アクセストークン"},
		{"fr", "Clé API", "Adresse e-mail", "Jeton d'accès"},
	}

	for _, c := range cases {
		if got := resolve_core_label(c.language, "accounts.field.key", nil); got != c.key {
			t.Errorf("accounts.field.key(%s) = %q, want %q", c.language, got, c.key)
		}
		if got := resolve_core_label(c.language, "accounts.field.address", nil); got != c.address {
			t.Errorf("accounts.field.address(%s) = %q, want %q", c.language, got, c.address)
		}
		if got := resolve_core_label(c.language, "accounts.field.token", nil); got != c.token {
			t.Errorf("accounts.field.token(%s) = %q, want %q", c.language, got, c.token)
		}
	}
}
