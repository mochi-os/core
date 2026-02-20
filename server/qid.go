// Mochi server: Wikidata QID lookup and search API
// Copyright Alistair Cunningham 2025
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
	"strings"
	"sync"
	"time"

	sl "go.starlark.net/starlark"
	sls "go.starlark.net/starlarkstruct"
)

var api_qid = sls.FromStringDict(sl.String("mochi.qid"), sl.StringDict{
	"lookup": sl.NewBuiltin("mochi.qid.lookup", api_qid_lookup),
	"search": sl.NewBuiltin("mochi.qid.search", api_qid_search),
})

var (
	qid_db_once   sync.Once
	qid_regex     = regexp.MustCompile(`^Q[0-9]+$`)
	qid_rate_lock sync.Mutex
	qid_rate_last time.Time
)

// qid_db opens external.db and creates the qids table on first use
func qid_db() *DB {
	qid_db_once.Do(func() {
		db := db_open("db/external.db")
		db.exec("create table if not exists qids (qid text not null, lang text not null, label text not null, fetched integer not null, primary key (qid, lang))")
	})
	return db_open("db/external.db")
}

// qid_rate_wait enforces 1 request per second to Wikidata
func qid_rate_wait() {
	qid_rate_lock.Lock()
	defer qid_rate_lock.Unlock()
	elapsed := time.Since(qid_rate_last)
	if elapsed < time.Second {
		time.Sleep(time.Second - elapsed)
	}
	qid_rate_last = time.Now()
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

	// Check cache
	for _, qid := range qids {
		row, err := db.row("select label from qids where qid=? and lang=?", qid, lang)
		if err == nil && row != nil {
			label, _ := row["label"].(string)
			labels[qid] = label
		} else {
			misses = append(misses, qid)
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

		qid_rate_wait()

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
		req.Header.Set("User-Agent", "Mochi/1.0")

		client := &http.Client{Timeout: 10 * time.Second}
		resp, err := client.Do(req)
		if err != nil {
			continue
		}

		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()

		if resp.StatusCode != 200 {
			continue
		}

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
			if langObj, ok := labels[lang].(map[string]any); ok {
				if val, ok := langObj["value"].(string); ok {
					result[qid] = val
					continue
				}
			}
			if lang != "en" {
				if enObj, ok := labels["en"].(map[string]any); ok {
					if val, ok := enObj["value"].(string); ok {
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

	qid_rate_wait()

	u := fmt.Sprintf("https://www.wikidata.org/w/api.php?action=wbsearchentities&search=%s&language=%s&limit=10&format=json",
		url.QueryEscape(query), url.QueryEscape(lang))

	req, err := http.NewRequest("GET", u, nil)
	if err != nil {
		return sl_encode([]map[string]any{}), nil
	}
	req.Header.Set("User-Agent", "Mochi/1.0")

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return sl_encode([]map[string]any{}), nil
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)

	if resp.StatusCode != 200 {
		return sl_encode([]map[string]any{}), nil
	}

	var data map[string]any
	if json.Unmarshal(body, &data) != nil {
		return sl_encode([]map[string]any{}), nil
	}

	search, ok := data["search"].([]any)
	if !ok {
		return sl_encode([]map[string]any{}), nil
	}

	db := qid_db()
	now := time.Now().Unix()
	var results []map[string]any

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

	if results == nil {
		results = []map[string]any{}
	}

	return sl_encode(results), nil
}
