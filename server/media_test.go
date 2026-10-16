// Mochi server: tests of the files a hosted fediverse account carries
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	sl "go.starlark.net/starlark"
)

// TestMediaStore: an upload through a hosted fediverse account is kept and served
// under the account as the Document the answer names; a type that is not one
// is refused; the file is nobody else's and goes with the account.
func TestMediaStore(t *testing.T) {
	alice, _ := create_activitypub_test_env(t)
	thread := create_acting_thread(alice)
	fn := sl.NewBuiltin("test", nil)

	upload := decoded(t)(api_account_upload(thread, fn, sl.Tuple{sl.String("f1"), sl.Bytes("\x89PNG\r\n\x1a\nimage"), sl.String("image/png")}, []sl.Tuple{{sl.String("name"), sl.String("cat.png")}, {sl.String("description"), sl.String("A cat")}}))
	id, _ := upload["id"].(string)
	url, _ := upload["url"].(string)
	document, _ := upload["data"].(map[string]any)
	if id == "" || url != "https://example.com/~f1/media/"+id || document["type"] != "Image" || document["mediaType"] != "image/png" || document["url"] != url || document["name"] != "A cat" {
		t.Fatalf("upload = %v", upload)
	}
	if _, err := os.Stat(media_path(alice, id)); err != nil {
		t.Fatalf("file not kept: %v", err)
	}

	r := httptest.NewRequest("GET", url, nil)
	w := activitypub_request(t, r)
	if w.Code != http.StatusOK || w.Header().Get("Content-Type") != "image/png" || w.Body.String() != "\x89PNG\r\n\x1a\nimage" || w.Header().Get("X-Content-Type-Options") != "nosniff" {
		t.Errorf("served = %d %q %q", w.Code, w.Header().Get("Content-Type"), w.Body.String())
	}
	// Another fediverse account's path and a made-up id answer not found.
	db_user(alice, "user").exec("insert into accounts (id, type, label, identifier, data, created, verified) values ('f2', 'fediverse', '', '@second@example.com', '{\"hosted\":true,\"handle\":\"second\",\"domain\":\"example.com\"}', 1, 1)")
	actor_register(alice, "f2", "fediverse", "example.com", "second")
	if w := activitypub_request(t, httptest.NewRequest("GET", "https://example.com/~f2/media/"+id, nil)); w.Code != http.StatusNotFound {
		t.Errorf("another account's media = %d", w.Code)
	}
	if w := activitypub_request(t, httptest.NewRequest("GET", "https://example.com/~f1/media/nothing", nil)); w.Code != http.StatusNotFound {
		t.Errorf("unknown media = %d", w.Code)
	}

	if _, err := api_account_upload(thread, fn, sl.Tuple{sl.String("f1"), sl.Bytes("x"), sl.String("png")}, nil); err == nil || !strings.Contains(err.Error(), "invalid type") {
		t.Errorf("a bare type: %v", err)
	}

	activitypub_account_remove(alice, "f1")
	if _, err := os.Stat(media_path(alice, id)); err == nil {
		t.Error("the file outlived the account")
	}
	if row, _ := db_user(alice, "user").row("select id from media where id=?", id); row != nil {
		t.Error("the media row outlived the account")
	}
}
