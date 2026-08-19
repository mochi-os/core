// Mochi server: a plain-text error body is user-facing text too.
//
// check-i18n-server.py greps c.JSON, so every route answering with c.String
// was invisible to it - which is how "Shell unavailable", "Missing repository"
// and "File not found" stayed English long after the rest of core was
// translated. respond_error's JSON body is right for an API caller and wrong to
// render in a browser window, so these keep their shape through respond_text
// and gain only a language.
//
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"fmt"
	"net/http/httptest"
	"os"
	"regexp"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
)

// TestRespondTextResolvesTheLabel. The whole point: the body is a resolved
// label, not the key and not English-by-construction.
func TestRespondTextResolvesTheLabel(t *testing.T) {
	load_core_labels()
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest("GET", "/", nil)

	respond_text(c, 500, "errors.shell_unavailable", nil)

	body := w.Body.String()
	if body == "" {
		t.Fatal("respond_text wrote no body")
	}
	if strings.Contains(body, "errors.") {
		t.Errorf("body is %q: the label key reached the user instead of its text", body)
	}
	if got := w.Code; got != 500 {
		t.Errorf("status = %d, want 500", got)
	}
	// Plain text, not JSON: these routes are reached by navigation, and a JSON
	// body renders as raw JSON in the window.
	if strings.HasPrefix(strings.TrimSpace(body), "{") {
		t.Errorf("body is %q, which is JSON; respond_error already covers API callers", body)
	}
}

// TestRespondTextFollowsTheRequestLanguage. A label resolved against the
// server default in every language would look translated and not be.
func TestRespondTextFollowsTheRequestLanguage(t *testing.T) {
	load_core_labels()
	english := ""
	french := ""
	for _, language := range []struct{ header, into string }{{"en", ""}, {"fr", ""}} {
		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		c.Request = httptest.NewRequest("GET", "/", nil)
		c.Request.Header.Set("Accept-Language", language.header)
		respond_text(c, 404, "errors.file_not_found", nil)
		if language.header == "en" {
			english = w.Body.String()
		} else {
			french = w.Body.String()
		}
	}
	if english == "" || french == "" {
		t.Fatal("respond_text produced an empty body")
	}
	if english == french {
		t.Errorf("en and fr both produced %q: the body does not follow the request language", english)
	}
}

// TestTranslatedKeysExistEverywhere. A key added to en.conf alone renders as
// English for every other language, which is the failure this task is about.
func TestTranslatedKeysExistEverywhere(t *testing.T) {
	locales, err := os.ReadDir("labels")
	if err != nil {
		t.Fatalf("reading labels: %v", err)
	}
	assignment := regexp.MustCompile(`(?m)^errors\.(repository_required|shell_unavailable)\s*=\s*(\S.*)$`)
	checked := 0
	for _, entry := range locales {
		name := entry.Name()
		if !strings.HasSuffix(name, ".conf") {
			continue
		}
		locale := strings.TrimSuffix(name, ".conf")
		// en-us is a regional overlay: sparse by design, inheriting from en.
		if locale == "en-us" {
			continue
		}
		body, err := os.ReadFile("labels/" + name)
		if err != nil {
			t.Errorf("reading %s: %v", name, err)
			continue
		}
		found := map[string]string{}
		for _, m := range assignment.FindAllStringSubmatch(string(body), -1) {
			found[m[1]] = strings.TrimSpace(m[2])
		}
		for _, key := range []string{"repository_required", "shell_unavailable"} {
			value, ok := found[key]
			if !ok || value == "" {
				t.Errorf("%s has no errors.%s", name, key)
				continue
			}
			if locale != "en" && (value == "Repository required" || value == "Shell unavailable") {
				t.Errorf("%s still carries the English for errors.%s", name, key)
			}
		}
		checked++
	}
	if checked < 90 {
		t.Errorf("only %d locales checked; the label set is around 100, so this test is not seeing them all", checked)
	}
}

// TestGateSeesPlainTextBodies. The gate greps c.JSON; without the plain-text
// patterns it reports nothing on a route that answers c.String, which is how
// these three literals survived.
func TestGateSeesPlainTextBodies(t *testing.T) {
	body, err := os.ReadFile("../../claude/scripts/check-i18n-server.py")
	if err != nil {
		t.Skipf("gate not present: %v", err)
	}
	gate := string(body)
	for _, want := range []string{"GO_TEXT_LITERAL", "GO_HTTP_ERROR_LITERAL", "GO_TEXT_PATTERNS"} {
		if !strings.Contains(gate, want) {
			t.Errorf("the gate has no %s, so a plain-text error body is invisible to it", want)
		}
	}
	if !strings.Contains(gate, "text_hits") {
		t.Error("the gate computes no plain-text hits")
	}
	if !strings.Contains(gate, "star_hits or go_hits or text_hits") {
		t.Error("plain-text hits do not affect the exit code, so CI would stay green with them present")
	}
	// The list has to hold the patterns, not merely exist: an empty
	// GO_TEXT_PATTERNS keeps every name above present while checking nothing.
	patterns := regexp.MustCompile(`GO_TEXT_PATTERNS\s*=\s*\[([^\]]*)\]`).FindStringSubmatch(gate)
	if patterns == nil {
		t.Fatal("GO_TEXT_PATTERNS is not a list")
	}
	for _, want := range []string{"GO_TEXT_LITERAL", "GO_HTTP_ERROR_LITERAL"} {
		if !strings.Contains(patterns[1], want) {
			t.Errorf("GO_TEXT_PATTERNS does not include %s, so that rule is compiled but never applied", want)
		}
	}
	// StatusOK bodies are content, not messages, and must not be flagged.
	if !strings.Contains(gate, "http\\.Status(?!OK\\b)") {
		t.Error("the gate does not exclude StatusOK; serving an HTML file or robots.txt would report as untranslated text")
	}
}

// TestGitProtocolIsMarkedMachineFacing. Git prints its own message for most
// failures and discards the body, and a git client carries no Mochi session to
// resolve a language against - so these stay English by decision, recorded
// where a reader will find it rather than left looking like an oversight.
func TestGitProtocolIsMarkedMachineFacing(t *testing.T) {
	body, err := os.ReadFile("git.go")
	if err != nil {
		t.Fatalf("reading git.go: %v", err)
	}
	// RE2 has no negative lookahead, so the status is captured and judged
	// rather than excluded in the pattern - the Python gate can write (?!OK)
	// and this cannot.
	responder := regexp.MustCompile(`\.(?:String|Data)\s*\(\s*http\.Status(\w+)\s*,[^,)]*?"[A-Z]`)
	marked, bare := 0, []string{}
	for n, line := range strings.Split(string(body), "\n") {
		match := responder.FindStringSubmatch(line)
		if match == nil || match[1] == "OK" {
			continue
		}
		if strings.Contains(line, "i18n-ok") {
			marked++
			continue
		}
		bare = append(bare, fmt.Sprintf("%s (git.go:%d)", strings.TrimSpace(line), n+1))
	}
	if len(bare) > 0 {
		t.Errorf("%d git protocol responses carry no i18n-ok marker, so the gate will fail on them: %v", len(bare), bare)
	}
	if marked == 0 {
		t.Error("no git protocol response is marked; this test is looking in the wrong place")
	}
}
