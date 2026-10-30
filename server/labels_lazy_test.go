// Mochi server: an app version's label catalogues are read when first asked
// for, not at load, and the installed language set is kept between requests.
// An install keeps every version it has run, and reading every catalogue of
// every one held 525 MB of mochi1's 569 MB heap.
//
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the Mochi
// Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"sort"
	"testing"

	"github.com/gin-gonic/gin"
)

// labels_apps gives the test an empty app table and restores the real one,
// moving the generation each way so no cached language set crosses over.
func labels_apps(t *testing.T) {
	t.Helper()
	apps_record_setup(t)
	apps_lock.Lock()
	saved := apps
	apps = map[string]*App{}
	apps_lock.Unlock()
	resolution_invalidate()
	t.Cleanup(func() {
		apps_lock.Lock()
		apps = saved
		apps_lock.Unlock()
		resolution_invalidate()
	})
}

// labels_version writes a version's labels directory and loads the version
// as the server does at startup.
func labels_version(t *testing.T, a *App, version string, catalogues map[string]string) *AppVersion {
	t.Helper()
	base := t.TempDir()
	if err := os.MkdirAll(base+"/labels", 0o755); err != nil {
		t.Fatal(err)
	}
	for name, body := range catalogues {
		if err := os.WriteFile(base+"/labels/"+name, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	av := &AppVersion{base: base, Version: version, Label: "app.name"}
	a.load_version(av)
	return av
}

// loaded_languages lists the catalogues a version has read so far.
func loaded_languages(labels *Labels) []string {
	labels.lock.Lock()
	defer labels.lock.Unlock()
	var out []string
	for tag := range labels.loaded {
		out = append(out, tag)
	}
	sort.Strings(out)
	return out
}

var labels_catalogues = map[string]string{
	"en.conf": "# header = not a label\napp.name = Labelled\ngreeting = Hello\n" +
		"only.english = English only\nequation = a = b\n  padded   =   spaced value  \n",
	"de.conf":    "greeting = Hallo\n",
	"de-ch.conf": "greeting = Grüezi\n",
	"notes.txt":  "greeting = not a catalogue\n",
	// Listed before de.conf; as at load before, the later file is the one read
	// and the language is listed once.
	"de": "greeting = stray\n",
}

// TestLabelsLoadNoCatalogueUntilAsked is the memory finding: loading a
// version lists its languages and reads nothing, and a label reads only the
// catalogues its fallback chain reaches.
func TestLabelsLoadNoCatalogueUntilAsked(t *testing.T) {
	labels_apps(t)
	av := labels_version(t, app_external("labelled"), "1.0", labels_catalogues)
	labels := av.labels.Load()

	if want := []string{"de", "de-ch", "en"}; !reflect.DeepEqual(labels.languages, want) {
		t.Errorf("languages = %v, want %v", labels.languages, want)
	}
	if got := loaded_languages(labels); len(got) != 0 {
		t.Errorf("loading the version read %v; every installed version is loaded at startup, so nothing may be read until a label is asked for", got)
	}

	if got := resolve_label(av, "de", "greeting", nil); got != "Hallo" {
		t.Fatalf("greeting in de = %q, want Hallo", got)
	}
	if got, want := loaded_languages(labels), []string{"de"}; !reflect.DeepEqual(got, want) {
		t.Errorf("after one German label the version holds %v, want %v", got, want)
	}
}

// TestLabelsResolveAsTheFilesSay: the same values as reading every catalogue
// at load gave, through the same fallback chain and the same line parsing.
func TestLabelsResolveAsTheFilesSay(t *testing.T) {
	labels_apps(t)
	av := labels_version(t, app_external("labelled"), "1.0", labels_catalogues)

	for _, test := range []struct{ language, key, want string }{
		{"de-ch", "greeting", "Grüezi"},
		{"de-ch", "only.english", "English only"},
		{"de", "greeting", "Hallo"},
		{"de", "equation", "a = b"},
		{"en", "padded", "spaced value"},
		{"fr", "greeting", "Hello"},
		{"en", "app.name", "Labelled"},
		{"en", "missing", "missing"},
	} {
		if got := resolve_label(av, test.language, test.key, nil); got != test.want {
			t.Errorf("%s in %s = %q, want %q", test.key, test.language, got, test.want)
		}
	}

	serve := func(suffix string) (int, string) {
		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		c.Request = httptest.NewRequest(http.MethodGet, "/labelled/-/labels"+suffix, nil)
		web_serve_labels(c, av, suffix)
		return w.Code, w.Body.String()
	}
	if code, body := serve(""); code != http.StatusOK || body != `{"languages":["de","de-ch","en"]}` {
		t.Errorf("-/labels = %d %s, want the three listed languages", code, body)
	}
	code, body := serve("/de")
	var catalogue map[string]string
	if code != http.StatusOK || json.Unmarshal([]byte(body), &catalogue) != nil || !reflect.DeepEqual(catalogue, map[string]string{"greeting": "Hallo"}) {
		t.Errorf("-/labels/de = %d %s, want the German catalogue", code, body)
	}
	if code, _ := serve("/fr"); code != http.StatusNotFound {
		t.Errorf("-/labels/fr = %d, want 404 for a language the version does not carry", code)
	}
}

// TestLabelsNeverOpenAPathFromARequest: the tag in a label lookup comes from
// the request, and only a tag the directory listed reaches the disk.
func TestLabelsNeverOpenAPathFromARequest(t *testing.T) {
	labels_apps(t)
	av := labels_version(t, app_external("labelled"), "1.0", labels_catalogues)
	if err := os.WriteFile(av.base+"/secret.conf", []byte("greeting = leaked\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	if got := av.labels.Load().get("../secret"); got != nil {
		t.Errorf("the tag ../secret read %v from outside the labels directory", got)
	}
	if got := resolve_label(av, "../secret", "greeting", nil); got != "Hello" {
		t.Errorf("greeting in ../secret = %q, want the English fallback", got)
	}
}

// TestLabelsSurviveARemovedVersion: a version removed while a request holds
// it has no catalogues left to read, and its labels fall through to what it
// already holds, or to the key.
func TestLabelsSurviveARemovedVersion(t *testing.T) {
	labels_apps(t)
	av := labels_version(t, app_external("labelled"), "1.0", labels_catalogues)
	if got := resolve_label(av, "en", "greeting", nil); got != "Hello" {
		t.Fatalf("greeting in en = %q, want Hello", got)
	}
	if err := os.RemoveAll(av.base); err != nil {
		t.Fatal(err)
	}

	if got := resolve_label(av, "de", "greeting", nil); got != "Hello" {
		t.Errorf("greeting in de after removal = %q, want the English already read", got)
	}
}

// TestInstalledLanguagesFollowTheInstalledVersions is the integrity half of
// keeping the set: a version that adds a language must make it negotiable.
func TestInstalledLanguagesFollowTheInstalledVersions(t *testing.T) {
	labels_apps(t)
	if _, ok := core_labels["tlh"]; ok {
		t.Skip("core now carries tlh; pick another tag no catalogue uses")
	}
	a := app_external("labelled")
	labels_version(t, a, "1.0", map[string]string{"en.conf": "greeting = Hello\n"})

	if got := negotiate_language([]string{"tlh"}); got == "tlh" {
		t.Fatal("negotiated tlh before any version carried it")
	}

	labels_version(t, a, "1.1", map[string]string{"en.conf": "greeting = Hello\n", "tlh.conf": "greeting = nuqneH\n"})

	if got := negotiate_language([]string{"tlh"}); got != "tlh" {
		t.Errorf("negotiated %q after installing a version with tlh, want tlh: the kept set was not rebuilt", got)
	}
	found := false
	for _, tag := range installed_languages().list {
		found = found || tag == "tlh"
	}
	if !found {
		t.Error("/_/languages does not list tlh after the install")
	}
}

// TestInstalledLanguagesAreKeptBetweenRequests is the speed finding: every
// action from a browser asked for the set, and each rebuilt it under apps_lock.
func TestInstalledLanguagesAreKeptBetweenRequests(t *testing.T) {
	labels_apps(t)
	labels_version(t, app_external("labelled"), "1.0", labels_catalogues)

	first := installed_languages()
	if second := installed_languages(); second != first {
		t.Error("the language set was rebuilt with nothing installed or removed in between")
	}
	resolution_invalidate()
	third := installed_languages()
	if third == first {
		t.Error("the language set was not rebuilt after the installed versions changed")
	}
	if !reflect.DeepEqual(third.list, first.list) {
		t.Errorf("rebuilt set %v differs from %v with nothing changed on disk", third.list, first.list)
	}
}

// TestDevReloadListsTheCataloguesAgain: an edited or added catalogue is picked
// up by the reload that follows an app.json change, as before.
func TestDevReloadListsTheCataloguesAgain(t *testing.T) {
	labels_apps(t)
	av := reload_app(t, reload_manifest("index.html"))
	if err := os.MkdirAll(av.base+"/labels", 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(av.base+"/labels/en.conf", []byte("greeting = Hello\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	reload_now(av)
	if got := resolve_label(av, "en", "greeting", nil); got != "Hello" {
		t.Fatalf("greeting after the first reload = %q, want Hello", got)
	}

	if err := os.WriteFile(av.base+"/labels/en.conf", []byte("greeting = Hi\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(av.base+"/labels/de.conf", []byte("greeting = Hallo\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	reload_now(av)

	if got := resolve_label(av, "en", "greeting", nil); got != "Hi" {
		t.Errorf("greeting in en after editing en.conf = %q, want Hi", got)
	}
	if got := resolve_label(av, "de", "greeting", nil); got != "Hallo" {
		t.Errorf("greeting in de after adding de.conf = %q, want Hallo", got)
	}
}
