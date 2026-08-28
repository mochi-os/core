// Mochi server: regional label overlays carry only what differs
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"os"
	"strings"
	"testing"
)

// The regional catalogues allowed to be sparse: language_fallbacks resolves
// each to its parent key by key. Mirrors OVERLAY plus conf_inherits in
// .github/scripts/check-labels.py and claude/scripts/conf-refresh.py.
//
// es-419 is deliberately absent. It falls through to es here exactly as these
// do, but Lingui makes it the base es-ar leans on for the WEB catalogues
// (`'es-ar': 'es-419'` in every app's lingui.config.js), so it is kept complete
// on both layers rather than sparse on one and complete on the other.
var overlay_catalogues = []string{"en-us", "en-ca", "fr-ca", "es-ar", "zh-hk", "de-ch", "nl-be", "pt-br"}

// A cell copied verbatim from the parent overrides nothing today and pins the
// child to a stale string the moment the parent is reworded - a present key
// beats an inherited one. en-us held 28 such copies and nothing else, so the
// whole file was inert.
//
// nl-be and pt-br joined this list once conf_inherits taught the gate the .conf
// fallback rule; 166 and 107 copies came out of them. es-419 still holds 175,
// by the decision recorded above.
func TestOverlayCatalogueHoldsOnlyDifferences(t *testing.T) {
	load_core_labels()

	for _, tag := range overlay_catalogues {
		child, ok := core_labels[tag]
		if !ok {
			continue // not shipped; installed_languages decides that, not this test
		}
		parent_tag := tag[:strings.LastIndex(tag, "-")]
		parent, ok := core_labels[parent_tag]
		if !ok {
			t.Errorf("%s has no parent catalogue %s to fall back to", tag, parent_tag)
			continue
		}
		for key, value := range child {
			if parent[key] == value {
				t.Errorf("%s.conf repeats %s.conf verbatim for %q: it overrides nothing, and pins %s to this wording once %s changes",
					tag, parent_tag, key, tag, parent_tag)
			}
		}
	}
}

// The other half: thinning a file must not lose the strings. Every key the
// parent carries has to still resolve for the child, through the fallback
// chain, with the parent's own wording.
func TestOverlayResolvesThroughToItsParent(t *testing.T) {
	load_core_labels()

	if len(core_labels["en"]) == 0 {
		t.Fatal("en.conf resolved to nothing; the rest of this test would be vacuous")
	}
	for _, tag := range overlay_catalogues {
		child, ok := core_labels[tag]
		if !ok {
			continue
		}
		parent_tag := tag[:strings.LastIndex(tag, "-")]
		parent, ok := core_labels[parent_tag]
		if !ok {
			continue
		}
		inherited := 0
		for key, want := range parent {
			if _, own := child[key]; own {
				continue // the child overrides this one, by design
			}
			inherited++
			if got := resolve_core_label(tag, key, nil); got != want {
				t.Errorf("resolve_core_label(%s, %q) = %q, want %s's %q", tag, key, got, parent_tag, want)
			}
		}
		if inherited == 0 {
			t.Errorf("%s inherits nothing from %s, so this test proved nothing for it", tag, parent_tag)
		}
	}
}

// label_inherits reports whether a catalogue may omit a key because a parent
// catalogue supplies it: language_fallbacks strips subtags, so nl-be resolves
// through nl before en, key by key. Mirrors conf_inherits in
// claude/scripts/i18n_glossary.py and the same rule in every
// .github/scripts/check-labels.py.
//
// The completeness tests in this package used to hardcode a single "en-us"
// skip, which is the same rule written for one locale.
func label_inherits(locale string) bool {
	cut := strings.LastIndex(locale, "-")
	if cut < 0 {
		return false
	}
	_, err := os.Stat("labels/" + locale[:cut] + ".conf")
	return err == nil
}
