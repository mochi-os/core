// Tests for the Wikidata QID rate-limiter (#35).
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"testing"
	"time"

	sl "go.starlark.net/starlark"
)

// TestQidRateWaitSkipsDuringBackoff: while a 429 backoff window is active,
// qid_rate_wait returns false immediately so callers skip the request rather
// than block a Starlark handler past the 90s watchdog (#35); once the window
// has elapsed it returns true.
func TestQidRateWaitSkipsDuringBackoff(t *testing.T) {
	orig_until := qid_backoff_until
	orig_last := qid_rate_last
	defer func() { qid_backoff_until = orig_until; qid_rate_last = orig_last }()

	// Active backoff window: must return false without blocking.
	qid_backoff_until = time.Now().Add(time.Hour)
	start := time.Now()
	if qid_rate_wait() {
		t.Error("qid_rate_wait returned true during an active backoff window")
	}
	if elapsed := time.Since(start); elapsed > 100*time.Millisecond {
		t.Errorf("qid_rate_wait blocked %v during backoff; it must return immediately", elapsed)
	}

	// No active backoff: proceeds (true). Pre-age qid_rate_last so the
	// 1-request/second spacing doesn't add a real sleep.
	qid_backoff_until = time.Now().Add(-time.Hour)
	qid_rate_last = time.Now().Add(-time.Hour)
	if !qid_rate_wait() {
		t.Error("qid_rate_wait returned false with no active backoff window")
	}
}

// qid_cache_setup gives a test the external.db cache tables and a clean slate.
func qid_cache_setup(t *testing.T) {
	t.Helper()
	cleanup := setup_replication_test(t)
	t.Cleanup(cleanup)
	db_create()
	db := qid_db()
	db.exec("delete from qids")
	db.exec("delete from qid_searches")
}

// qid_cache_store writes one label row with an explicit age, so a test can
// describe a row on either side of the TTL without waiting for it.
func qid_cache_store(t *testing.T, qid, lang, label string, age time.Duration) {
	t.Helper()
	qid_db().exec("replace into qids (qid, lang, label, fetched) values (?, ?, ?, ?)",
		qid, lang, label, time.Now().Add(-age).Unix())
}

// qid_offline puts the 429 backoff window in the future for the duration of a
// test, so qid_rate_wait refuses and qid_fetch_labels returns without touching
// the network. A cache miss then yields an empty label rather than a live
// request, which is what lets these tests drive the real lookup path.
func qid_offline(t *testing.T) {
	t.Helper()
	previous := qid_backoff_until
	qid_backoff_until = time.Now().Add(time.Hour)
	t.Cleanup(func() { qid_backoff_until = previous })
}

// qid_lookup_one calls the production API the way an app does and returns the
// label. Deliberately not a re-implementation of the cache query: an earlier
// version of this test read the row itself with the TTL applied, so it passed
// against a lookup path that had no TTL at all.
func qid_lookup_one(t *testing.T, qid, lang string) string {
	t.Helper()
	value, err := api_qid_lookup(&sl.Thread{}, sl.NewBuiltin("mochi.qid.lookup", api_qid_lookup),
		sl.Tuple{sl.String(qid), sl.String(lang)}, nil)
	if err != nil {
		t.Fatalf("mochi.qid.lookup(%q): %v", qid, err)
	}
	label, _ := sl.AsString(value)
	return label
}

// TestQidLookupCacheHasATtl is the finding. Both lookup reads selected on
// (qid, lang) alone, so `fetched` was written on every store and read nowhere:
// a cached label was served for as long as the row existed, and the only thing
// that ever expired one was the daily prune. The expiry was therefore whatever
// the prune's retention happened to be, at a site that never mentions it -
// shorten the prune for disk and lookups silently start refetching, lengthen it
// and labels silently go stale.
func TestQidLookupCacheHasATtl(t *testing.T) {
	qid_cache_setup(t)
	qid_offline(t)

	qid_cache_store(t, "Q42", "en", "Douglas Adams", qid_lookup_ttl/2)
	if label := qid_lookup_one(t, "Q42", "en"); label != "Douglas Adams" {
		t.Errorf("a row half a TTL old returned %q, want it served from cache: labels are stable and every miss costs a paced round trip", label)
	}

	qid_cache_store(t, "Q43", "en", "Stale", qid_lookup_ttl+time.Hour)
	if label := qid_lookup_one(t, "Q43", "en"); label != "" {
		t.Errorf("a row past the TTL returned %q, so it was still served; without the fetched predicate the cache never expires by policy, only by prune", label)
	}
}

// TestQidLookupBatchCacheHasATtl covers the other read. The batch path is a
// separate query built by hand, so it needs the predicate separately - and a
// single-QID test would never notice its absence.
func TestQidLookupBatchCacheHasATtl(t *testing.T) {
	qid_cache_setup(t)
	qid_offline(t)

	qid_cache_store(t, "Q1", "en", "Fresh", qid_lookup_ttl/2)
	qid_cache_store(t, "Q2", "en", "Stale", qid_lookup_ttl+time.Hour)

	value, err := api_qid_lookup(&sl.Thread{}, sl.NewBuiltin("mochi.qid.lookup", api_qid_lookup),
		sl.Tuple{sl.NewList([]sl.Value{sl.String("Q1"), sl.String("Q2")}), sl.String("en")}, nil)
	if err != nil {
		t.Fatalf("mochi.qid.lookup(batch): %v", err)
	}
	labels, ok := value.(*sl.Dict)
	if !ok {
		t.Fatalf("batch lookup returned %T, want a dict", value)
	}
	get := func(qid string) string {
		v, found, err := labels.Get(sl.String(qid))
		if err != nil || !found {
			return ""
		}
		s, _ := sl.AsString(v)
		return s
	}
	if get("Q1") != "Fresh" {
		t.Errorf("the batch path returned %q for a row inside its TTL, want Fresh", get("Q1"))
	}
	if got := get("Q2"); got != "" {
		t.Errorf("the batch path returned %q for a row past its TTL; the predicate is missing from the IN query", got)
	}
}

// TestQidPrunesEachTableAtItsOwnTtl. One shared retention meant search rows
// stopped being served after seven days and then sat in a table every app
// shares for twenty-three more, while label rows had no TTL at all and were
// expired by that same retention. Pruning each table at the value that governs
// it makes a pruned row exactly one that would no longer have been served.
func TestQidPrunesEachTableAtItsOwnTtl(t *testing.T) {
	qid_cache_setup(t)
	db := qid_db()

	qid_cache_store(t, "Q1", "en", "Fresh label", qid_lookup_ttl/2)
	qid_cache_store(t, "Q2", "en", "Expired label", qid_lookup_ttl+time.Hour)

	fresh := time.Now().Add(-qid_search_ttl / 2).Unix()
	stale := time.Now().Add(-qid_search_ttl - time.Hour).Unix()
	db.exec("replace into qid_searches (query, lang, results, fetched) values ('fresh', 'en', '[]', ?)", fresh)
	db.exec("replace into qid_searches (query, lang, results, fetched) values ('stale', 'en', '[]', ?)", stale)

	qid_prune()

	if have, _ := db.exists("select 1 from qids where qid='Q1'"); !have {
		t.Error("the prune deleted a label row inside its TTL, so a lookup that would have been served now costs a round trip")
	}
	if have, _ := db.exists("select 1 from qids where qid='Q2'"); have {
		t.Error("the prune kept a label row past its TTL; nothing will read it again, so it is pure growth in a database every app shares")
	}
	if have, _ := db.exists("select 1 from qid_searches where query='fresh'"); !have {
		t.Error("the prune deleted a search row inside its TTL")
	}
	if have, _ := db.exists("select 1 from qid_searches where query='stale'"); have {
		t.Error("a search row past its 7-day TTL survived the prune; it stopped being served long before it stopped taking up space")
	}
}

// TestQidLabelsOutliveSearches states the relationship the two constants are
// chosen for, rather than leaving it to whoever next edits one of them. A
// Wikidata label belongs to one entity and rarely changes; a search is a
// ranking over the whole corpus and moves as entities are added.
func TestQidLabelsOutliveSearches(t *testing.T) {
	if qid_lookup_ttl <= qid_search_ttl {
		t.Errorf("qid_lookup_ttl (%v) is not longer than qid_search_ttl (%v)", qid_lookup_ttl, qid_search_ttl)
	}
	if qid_search_empty_ttl >= qid_search_ttl {
		t.Errorf("qid_search_empty_ttl (%v) is not shorter than qid_search_ttl (%v); an empty result is retried sooner so a transient glitch does not lock out a term that really has a match", qid_search_empty_ttl, qid_search_ttl)
	}
}
