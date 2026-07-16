// Mochi server: Wikidata QID lookup and search API
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.
//
// Provides mochi.qid.lookup() and mochi.qid.search() for Starlark apps to resolve
// Wikidata QIDs to labels and search for entities. Results are cached in external.db
// (shared across all apps) to avoid repeated Wikidata API calls.

package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	sl "go.starlark.net/starlark"
	sls "go.starlark.net/starlarkstruct"
)

const (
	qid_search_ttl       = 7 * 24 * time.Hour
	qid_search_empty_ttl = time.Hour
	qid_backoff_base     = 60 * time.Second
	qid_backoff_max      = 30 * time.Minute
	// Wikimedia's User-Agent policy throttles/blocks generic or missing UAs
	// hard (the cause of the 429 storm in #35); a descriptive UA with a
	// contact URL gets far higher limits.
	qid_user_agent = "Mochi/1.0 (+https://mochi-os.org)"
)

var api_qid = sls.FromStringDict(sl.String("mochi.qid"), sl.StringDict{
	"lookup": sl.NewBuiltin("mochi.qid.lookup", api_qid_lookup),
	"search": sl.NewBuiltin("mochi.qid.search", api_qid_search),
})

var (
	qid_regex         = regexp.MustCompile(`^Q[0-9]+$`)
	qid_rate_lock     sync.Mutex
	qid_rate_last     time.Time
	qid_backoff_lock  sync.Mutex
	qid_backoff_until time.Time
	qid_backoff_cur   = qid_backoff_base
)

// qid_db opens external.db and creates the qids and qid_searches tables on first use
func qid_db() *DB {
	return db_open("db/external.db")
}

// qid_rate_wait paces Wikidata requests at 1/second and reports whether the
// caller may proceed. It returns false when a 429 backoff window is active so
// the caller skips the request and returns empty/cached immediately, instead of
// blocking: a Starlark handler must not sleep out the 40-50s backoff, which
// stacked on the AI call blew past the 90s watchdog (#35). The 1-request/second
// spacing still applies to requests that do proceed.
func qid_rate_wait() bool {
	qid_backoff_lock.Lock()
	active := time.Now().Before(qid_backoff_until)
	qid_backoff_lock.Unlock()
	if active {
		return false
	}
	qid_rate_lock.Lock()
	defer qid_rate_lock.Unlock()
	elapsed := time.Since(qid_rate_last)
	if elapsed < time.Second {
		time.Sleep(time.Second - elapsed)
	}
	qid_rate_last = time.Now()
	return true
}

// qid_handle_429 records a 429 response and sets an exponential backoff window.
// Respects the Retry-After header if present.
func qid_handle_429(resp *http.Response, context string) {
	qid_backoff_lock.Lock()
	defer qid_backoff_lock.Unlock()
	wait := qid_backoff_cur
	if ra := resp.Header.Get("Retry-After"); ra != "" {
		if secs, err := strconv.Atoi(strings.TrimSpace(ra)); err == nil && secs > 0 {
			wait = time.Duration(secs) * time.Second
		}
	}
	qid_backoff_until = time.Now().Add(wait)
	qid_backoff_cur *= 2
	if qid_backoff_cur > qid_backoff_max {
		qid_backoff_cur = qid_backoff_max
	}
	info("mochi.qid: Wikidata 429 on %s, backing off for %v", context, wait)
}

// qid_request_ok is called after a successful Wikidata response to decay backoff
func qid_request_ok() {
	qid_backoff_lock.Lock()
	qid_backoff_cur = qid_backoff_base
	qid_backoff_lock.Unlock()
}

// mochi.qid.lookup(qid, lang) -> string|dict: Look up Wikidata QID labels with caching
func api_qid_lookup(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 2 {
		return sl_error(fn, "syntax: <qid: string|list>, <lang: string>")
	}

	lang, ok := sl.AsString(args[1])
	if !ok || lang == "" {
		return sl_error(fn, "invalid language")
	}

	// Detect single vs batch
	single := false
	var qids []string

	switch v := args[0].(type) {
	case sl.String:
		single = true
		qids = []string{string(v)}
	case *sl.List:
		for i := 0; i < v.Len(); i++ {
			s, ok := sl.AsString(v.Index(i))
			if !ok {
				return sl_error(fn, "invalid QID at index %d", i)
			}
			qids = append(qids, s)
		}
	case sl.Tuple:
		for i, item := range v {
			s, ok := sl.AsString(item)
			if !ok {
				return sl_error(fn, "invalid QID at index %d", i)
			}
			qids = append(qids, s)
		}
	default:
		return sl_error(fn, "qid must be a string or list")
	}

	// Validate QID format
	for _, qid := range qids {
		if !qid_regex.MatchString(qid) {
			return sl_error(fn, "invalid QID format: %q", qid)
		}
	}

	if len(qids) == 0 {
		if single {
			return sl.String(""), nil
		}
		return sl_encode(map[string]any{}), nil
	}

	db := qid_db()
	labels := make(map[string]string)
	var misses []string

	// Batch cache lookup
	if len(qids) == 1 {
		row, err := db.row("select label from qids where qid=? and lang=?", qids[0], lang)
		if err == nil && row != nil {
			label, _ := row["label"].(string)
			labels[qids[0]] = label
		} else {
			misses = append(misses, qids[0])
		}
	} else {
		placeholders := make([]string, len(qids))
		args := make([]any, 0, len(qids)+1)
		for i, qid := range qids {
			placeholders[i] = "?"
			args = append(args, qid)
		}
		args = append(args, lang)
		rows, err := db.rows("select qid, label from qids where qid in ("+strings.Join(placeholders, ",")+") and lang=?", args...)
		cached := make(map[string]string)
		if err == nil {
			for _, row := range rows {
				qid, _ := row["qid"].(string)
				label, _ := row["label"].(string)
				cached[qid] = label
			}
		}
		for _, qid := range qids {
			if label, ok := cached[qid]; ok {
				labels[qid] = label
			} else {
				misses = append(misses, qid)
			}
		}
	}

	// Fetch missing labels from Wikidata
	if len(misses) > 0 {
		fetched := qid_fetch_labels(misses, lang)
		now := time.Now().Unix()
		for qid, label := range fetched {
			labels[qid] = label
			db.exec("replace into qids (qid, lang, label, fetched) values (?, ?, ?, ?)", qid, lang, label, now)
		}
	}

	if single {
		label := labels[qids[0]]
		return sl.String(label), nil
	}

	result := make(map[string]any)
	for qid, label := range labels {
		result[qid] = label
	}
	return sl_encode(result), nil
}

// qid_fetch_labels fetches labels from Wikidata for a list of QIDs
func qid_fetch_labels(qids []string, lang string) map[string]string {
	result := make(map[string]string)

	// Batch into groups of 50
	for i := 0; i < len(qids); i += 50 {
		end := i + 50
		if end > len(qids) {
			end = len(qids)
		}
		batch := qids[i:end]

		if !qid_rate_wait() {
			// Active 429 backoff; stop fetching rather than block the caller.
			// Return the labels gathered so far (best-effort, #35).
			break
		}

		// Build Wikidata API URL
		ids := strings.Join(batch, "|")
		languages := lang
		if lang != "en" {
			languages = lang + ",en"
		}

		u := fmt.Sprintf("https://www.wikidata.org/w/api.php?action=wbgetentities&ids=%s&languages=%s&props=labels&format=json",
			url.QueryEscape(ids), url.QueryEscape(languages))

		req, err := http.NewRequest("GET", u, nil)
		if err != nil {
			continue
		}
		req.Header.Set("User-Agent", qid_user_agent)

		client := &http.Client{Timeout: 10 * time.Second}
		resp, err := client.Do(req)
		if err != nil {
			continue
		}

		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()

		if resp.StatusCode == 429 {
			qid_handle_429(resp, "wbgetentities")
			continue
		}
		if resp.StatusCode != 200 {
			info("mochi.qid: wbgetentities returned %d", resp.StatusCode)
			continue
		}
		qid_request_ok()

		var data map[string]any
		if json.Unmarshal(body, &data) != nil {
			continue
		}

		entities, ok := data["entities"].(map[string]any)
		if !ok {
			continue
		}

		for _, qid := range batch {
			entity, ok := entities[qid].(map[string]any)
			if !ok {
				result[qid] = qid
				continue
			}

			labels, ok := entity["labels"].(map[string]any)
			if !ok {
				result[qid] = qid
				continue
			}

			// Try requested language first, then English, then QID itself
			if lang_object, ok := labels[lang].(map[string]any); ok {
				if val, ok := lang_object["value"].(string); ok {
					result[qid] = val
					continue
				}
			}
			if lang != "en" {
				if en_object, ok := labels["en"].(map[string]any); ok {
					if val, ok := en_object["value"].(string); ok {
						result[qid] = val
						continue
					}
				}
			}
			result[qid] = qid
		}
	}

	return result
}

// mochi.qid.search(query, lang) -> list: Search Wikidata for entities
func api_qid_search(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 2 {
		return sl_error(fn, "syntax: <query: string>, <lang: string>")
	}

	query, ok := sl.AsString(args[0])
	if !ok || query == "" {
		return sl_error(fn, "invalid query")
	}

	lang, ok := sl.AsString(args[1])
	if !ok || lang == "" {
		return sl_error(fn, "invalid language")
	}

	db := qid_db()
	now_time := time.Now()
	cutoff_full := now_time.Add(-qid_search_ttl).Unix()
	cutoff_empty := now_time.Add(-qid_search_empty_ttl).Unix()

	// Serve from cache if fresh. Empty results use a shorter TTL so a transient
	// Wikidata glitch doesn't lock out a term that really does have a match.
	if row, err := db.row("select results, fetched from qid_searches where query=? and lang=?", query, lang); err == nil && row != nil {
		fetched, _ := row["fetched"].(int64)
		results_json, _ := row["results"].(string)
		var cached []map[string]any
		if results_json != "" && json.Unmarshal([]byte(results_json), &cached) == nil {
			if cached == nil {
				cached = []map[string]any{}
			}
			cutoff := cutoff_full
			if len(cached) == 0 {
				cutoff = cutoff_empty
			}
			if fetched >= cutoff {
				return sl_encode(cached), nil
			}
		}
	}

	if !qid_rate_wait() {
		// Wikidata is in a 429 backoff window; skip rather than block the
		// caller. Empty + uncached so a later call retries once it clears (#35).
		return sl_encode([]map[string]any{}), nil
	}

	u := fmt.Sprintf("https://www.wikidata.org/w/api.php?action=wbsearchentities&search=%s&language=%s&limit=10&format=json",
		url.QueryEscape(query), url.QueryEscape(lang))

	req, err := http.NewRequest("GET", u, nil)
	if err != nil {
		return sl_encode([]map[string]any{}), nil
	}
	req.Header.Set("User-Agent", qid_user_agent)

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		info("mochi.qid: wbsearchentities request failed: %v", err)
		return sl_encode([]map[string]any{}), nil
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)

	if resp.StatusCode == 429 {
		qid_handle_429(resp, "wbsearchentities")
		// Do not cache: return empty but allow retry next call
		return sl_encode([]map[string]any{}), nil
	}
	if resp.StatusCode != 200 {
		info("mochi.qid: wbsearchentities returned %d for %q", resp.StatusCode, query)
		return sl_encode([]map[string]any{}), nil
	}
	qid_request_ok()

	var data map[string]any
	if json.Unmarshal(body, &data) != nil {
		return sl_encode([]map[string]any{}), nil
	}

	search, ok := data["search"].([]any)
	if !ok {
		return sl_encode([]map[string]any{}), nil
	}

	now := time.Now().Unix()
	results := []map[string]any{}

	for _, item := range search {
		entry, ok := item.(map[string]any)
		if !ok {
			continue
		}

		qid, _ := entry["id"].(string)
		label, _ := entry["label"].(string)
		description, _ := entry["description"].(string)

		if qid == "" {
			continue
		}

		results = append(results, map[string]any{
			"qid":         qid,
			"label":       label,
			"description": description,
		})

		// Cache the QID label for future lookups
		if label != "" {
			db.exec("replace into qids (qid, lang, label, fetched) values (?, ?, ?, ?)", qid, lang, label, now)
		}
	}

	// Cache the result set. Empty results use a short TTL on read so transient
	// Wikidata issues clear quickly, while genuine "no match" terms don't re-query constantly.
	if results_json, err := json.Marshal(results); err == nil {
		db.exec("replace into qid_searches (query, lang, results, fetched) values (?, ?, ?, ?)", query, lang, string(results_json), now)
	}

	return sl_encode(results), nil
}
