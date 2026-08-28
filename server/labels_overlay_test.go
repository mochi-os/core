// Mochi server: regional label overlays carry only what differs
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"strings"
	"testing"
)

// The regional catalogues the label gate treats as overlays: they are allowed
// to be sparse because language_fallbacks resolves them to their parent.
// Mirrors OVERLAY in .github/scripts/check-labels.py and conf-refresh.py.
var overlay_catalogues = []string{"en-us", "en-ca", "fr-ca", "es-ar", "zh-hk", "de-ch"}

// A cell copied verbatim from the parent overrides nothing today and pins the
// child to a stale string the moment the parent is reworded - a present key
// beats an inherited one. en-us held 28 such copies and nothing else, so the
// whole file was inert.
//
// Scoped to the overlay set on purpose. es-419, nl-be and pt-br carry the same
// duplication (175, 166 and 107 cells) but are NOT overlays to the gate, which
// requires a value for every key in them; thinning those needs the OVERLAY set
// widened first, in the Python gate and its 27 mirrors.
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

// The other half: emptying the file must not lose the strings. Every key en
// carries has to still resolve for en-us, through the fallback chain, with the
// parent's own wording.
func TestOverlayResolvesThroughToItsParent(t *testing.T) {
	load_core_labels()

	english := core_labels["en"]
	if len(english) == 0 {
		t.Fatal("en.conf resolved to nothing; the rest of this test would be vacuous")
	}
	for key, want := range english {
		if got := resolve_core_label("en-us", key, nil); got != want {
			t.Errorf("resolve_core_label(en-us, %q) = %q, want %q", key, got, want)
		}
	}
}
