// Mochi server: User interest profiles API
// Copyright Alistair Cunningham 2025-2026
//
// Provides mochi.interests.* builtins for managing user interest profiles.
// Interests are stored as Wikidata QIDs with weights (-100 to 100) in user.db.
// Used by feeds and forums for personalised "relevant" sort ranking.

package main

import (
	"encoding/json"
	"fmt"
	"strings"

	sl "go.starlark.net/starlark"
	sls "go.starlark.net/starlarkstruct"
)

var api_interests = sls.FromStringDict(sl.String("mochi.interests"), sl.StringDict{
	"list":    sl.NewBuiltin("mochi.interests.list", api_interests_list),
	"set":     sl.NewBuiltin("mochi.interests.set", api_interests_set),
	"remove":  sl.NewBuiltin("mochi.interests.remove", api_interests_remove),
	"adjust":  sl.NewBuiltin("mochi.interests.adjust", api_interests_adjust),
	"top":     sl.NewBuiltin("mochi.interests.top", api_interests_top),
	"bottom":  sl.NewBuiltin("mochi.interests.bottom", api_interests_bottom),
	"summary": sl.NewBuiltin("mochi.interests.summary", api_interests_summary),
})

// mochi.interests.list() -> list: List all user interests sorted by weight descending
func api_interests_list(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if err := require_permission(t, fn, "interests/read"); err != nil {
		return sl_error(fn, "%v", err)
	}

	user, _ := t.Local("user").(*User)
	if user == nil {
		return sl_error(fn, "no user")
	}

	db := db_user(user, "user")
	rows, err := db.rows("select qid, weight, updated from interests order by weight desc")
	if err != nil {
		return sl_error(fn, "database error: %v", err)
	}

	if rows == nil {
		rows = []map[string]any{}
	}
	return sl_encode(rows), nil
}

// mochi.interests.set(qid, weight) -> None: Set an interest weight (-100 to 100)
func api_interests_set(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 2 {
		return sl_error(fn, "syntax: <qid: string>, <weight: int>")
	}

	if err := require_permission(t, fn, "interests/write"); err != nil {
		return sl_error(fn, "%v", err)
	}

	qid, ok := sl.AsString(args[0])
	if !ok || qid == "" || !qid_regex.MatchString(qid) {
		return sl_error(fn, "invalid QID")
	}

	weight, err := sl.AsInt32(args[1])
	if err != nil {
		return sl_error(fn, "invalid weight")
	}

	// Clamp weight to -100 to 100
	if weight < -100 {
		weight = -100
	}
	if weight > 100 {
		weight = 100
	}

	user, _ := t.Local("user").(*User)
	if user == nil {
		return sl_error(fn, "no user")
	}

	db := db_user(user, "user")
	db.exec("insert or replace into interests (qid, weight, updated) values (?, ?, ?)", qid, weight, now())

	return sl.None, nil
}

// mochi.interests.remove(qid) -> None: Remove an interest
func api_interests_remove(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 1 {
		return sl_error(fn, "syntax: <qid: string>")
	}

	if err := require_permission(t, fn, "interests/write"); err != nil {
		return sl_error(fn, "%v", err)
	}

	qid, ok := sl.AsString(args[0])
	if !ok || qid == "" {
		return sl_error(fn, "invalid QID")
	}

	user, _ := t.Local("user").(*User)
	if user == nil {
		return sl_error(fn, "no user")
	}

	db := db_user(user, "user")
	db.exec("delete from interests where qid=?", qid)

	return sl.None, nil
}

// mochi.interests.adjust(qid_or_list, delta) -> None: Adjust interest weights by delta
func api_interests_adjust(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 2 {
		return sl_error(fn, "syntax: <qid: string|list>, <delta: int>")
	}

	if err := require_permission(t, fn, "interests/write"); err != nil {
		return sl_error(fn, "%v", err)
	}

	delta, err := sl.AsInt32(args[1])
	if err != nil {
		return sl_error(fn, "invalid delta")
	}

	// Parse QID(s)
	var qids []string
	switch v := args[0].(type) {
	case sl.String:
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

	if len(qids) == 0 {
		return sl.None, nil
	}

	// Validate QID format
	for _, qid := range qids {
		if !qid_regex.MatchString(qid) {
			return sl_error(fn, "invalid QID format: %q", qid)
		}
	}

	user, _ := t.Local("user").(*User)
	if user == nil {
		return sl_error(fn, "no user")
	}

	db := db_user(user, "user")
	ts := now()

	for _, qid := range qids {
		row, err := db.row("select weight from interests where qid=?", qid)
		if err == nil && row != nil {
			// Existing interest: adjust and clamp
			current, _ := row["weight"].(int64)
			w := int(current) + delta
			if w < -100 {
				w = -100
			}
			if w > 100 {
				w = 100
			}
			db.exec("update interests set weight=?, updated=? where qid=?", w, ts, qid)
		} else if delta != 0 {
			// New interest: insert with delta as initial weight (clamped)
			w := delta
			if w < -100 {
				w = -100
			}
			if w > 100 {
				w = 100
			}
			db.exec("insert or ignore into interests (qid, weight, updated) values (?, ?, ?)", qid, w, ts)
		}
	}

	return sl.None, nil
}

// mochi.interests.top(n) -> list: Get top N interests by weight
func api_interests_top(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 1 {
		return sl_error(fn, "syntax: <n: int>")
	}

	if err := require_permission(t, fn, "interests/read"); err != nil {
		return sl_error(fn, "%v", err)
	}

	n, err := sl.AsInt32(args[0])
	if err != nil || n < 1 {
		return sl_error(fn, "invalid count")
	}
	if n > 100 {
		n = 100
	}

	user, _ := t.Local("user").(*User)
	if user == nil {
		return sl_error(fn, "no user")
	}

	db := db_user(user, "user")
	rows, err := db.rows("select qid, weight from interests where weight > 0 order by weight desc limit ?", n)
	if err != nil {
		return sl_error(fn, "database error: %v", err)
	}

	if rows == nil {
		rows = []map[string]any{}
	}
	return sl_encode(rows), nil
}

// mochi.interests.bottom(n) -> list: Get bottom N interests by weight (negative only)
func api_interests_bottom(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 1 {
		return sl_error(fn, "syntax: <n: int>")
	}

	if err := require_permission(t, fn, "interests/read"); err != nil {
		return sl_error(fn, "%v", err)
	}

	n, err := sl.AsInt32(args[0])
	if err != nil || n < 1 {
		return sl_error(fn, "invalid count")
	}
	if n > 100 {
		n = 100
	}

	user, _ := t.Local("user").(*User)
	if user == nil {
		return sl_error(fn, "no user")
	}

	db := db_user(user, "user")
	rows, err := db.rows("select qid, weight from interests where weight < 0 order by weight asc limit ?", n)
	if err != nil {
		return sl_error(fn, "database error: %v", err)
	}

	if rows == nil {
		rows = []map[string]any{}
	}
	return sl_encode(rows), nil
}

// mochi.interests.summary() -> string: Get or regenerate a natural language summary of user interests
func api_interests_summary(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if err := require_permission(t, fn, "interests/read"); err != nil {
		return sl_error(fn, "%v", err)
	}

	var force sl.Bool
	if err := sl.UnpackArgs(fn.Name(), args, kwargs, "force?", &force); err != nil {
		return sl_error(fn, "%v", err)
	}

	user, _ := t.Local("user").(*User)
	if user == nil {
		return sl_error(fn, "no user")
	}

	db := db_user(user, "user")

	// Check cached summary (skip if force=True)
	if !bool(force) {
		row, err := db.row("select text, number from settings where key='interest_summary'")
		if err == nil && row != nil {
			text, _ := row["text"].(string)
			cached_at, _ := row["number"].(int64)

			// Check staleness: regenerate if older than 24 hours
			if text != "" && now()-cached_at < 86400 {
				return sl.String(text), nil
			}
		}
	}

	// Regenerate summary
	summary := interests_generate_summary(user, db)
	db.exec("replace into settings (key, text, number) values ('interest_summary', ?, ?)", summary, now())

	return sl.String(summary), nil
}

// interests_generate_summary builds a summary of the user's interests
func interests_generate_summary(user *User, db *DB) string {
	rows, err := db.rows("select qid from interests where weight > 0 order by weight desc limit 30")
	if err != nil || len(rows) == 0 {
		rows = []map[string]any{}
	}

	var posQids []string
	for _, row := range rows {
		qid, _ := row["qid"].(string)
		if qid != "" {
			posQids = append(posQids, qid)
		}
	}

	negRows, err := db.rows("select qid from interests where weight < 0 order by weight asc limit 15")
	if err != nil {
		negRows = []map[string]any{}
	}

	var negQids []string
	for _, row := range negRows {
		qid, _ := row["qid"].(string)
		if qid != "" {
			negQids = append(negQids, qid)
		}
	}

	if len(posQids) == 0 && len(negQids) == 0 {
		return ""
	}

	// Resolve QID labels for both positive and negative
	allQids := append(posQids, negQids...)
	labels := qid_fetch_labels(allQids, "en")

	// Try AI summary first
	summary := interests_ai_summary(user, db, posQids, negQids, labels)
	if summary != "" {
		return summary
	}

	// Fallback: simple label list
	var posParts []string
	for _, qid := range posQids {
		label := labels[qid]
		if label != "" && label != qid {
			posParts = append(posParts, label)
		}
	}
	var negParts []string
	for _, qid := range negQids {
		label := labels[qid]
		if label != "" && label != qid {
			negParts = append(negParts, label)
		}
	}
	if len(posParts) == 0 && len(negParts) == 0 {
		return ""
	}
	var result string
	if len(posParts) > 0 {
		result = "Interested in: " + strings.Join(posParts, ", ")
	}
	if len(negParts) > 0 {
		if result != "" {
			result += ". "
		}
		result += "Dislikes: " + strings.Join(negParts, ", ")
	}
	return result
}

// interests_ai_summary attempts to generate an AI-powered summary
func interests_ai_summary(user *User, db *DB, posQids []string, negQids []string, labels map[string]string) string {
	// Find first enabled AI account
	rows, err := db.rows("select id, type, data, enabled from accounts order by id")
	if err != nil {
		return ""
	}

	var api_key, model, ptype string
	for _, row := range rows {
		t, _ := row["type"].(string)
		enabled, _ := row["enabled"].(int64)
		if enabled == 1 && provider_has_capability(t, "ai") {
			ptype = t
			raw, _ := row["data"].(string)
			var data map[string]any
			if raw != "" {
				json.Unmarshal([]byte(raw), &data)
			}
			api_key, _ = data["api_key"].(string)
			model, _ = data["model"].(string)
			break
		}
	}

	if api_key == "" {
		return ""
	}

	if model == "" || model == "default" {
		model = aiProviderDefaults[ptype]
	}

	// Build interest list for prompt
	var posLines []string
	for _, qid := range posQids {
		label := labels[qid]
		if label != "" && label != qid {
			posLines = append(posLines, fmt.Sprintf("- %s (%s)", label, qid))
		}
	}
	var negLines []string
	for _, qid := range negQids {
		label := labels[qid]
		if label != "" && label != qid {
			negLines = append(negLines, fmt.Sprintf("- %s (%s)", label, qid))
		}
	}
	if len(posLines) == 0 && len(negLines) == 0 {
		return ""
	}

	var sections []string
	if len(posLines) > 0 {
		sections = append(sections, "Liked topics:\n"+strings.Join(posLines, "\n"))
	}
	if len(negLines) > 0 {
		sections = append(sections, "Disliked topics:\n"+strings.Join(negLines, "\n"))
	}

	prompt := fmt.Sprintf("Summarise the following user interests in 2-3 sentences. Be concise and natural. Do not list them — describe what kind of topics and themes the person is interested in, and what they dislike if applicable.\n\n%s", strings.Join(sections, "\n\n"))

	var result aiResult
	switch ptype {
	case "claude":
		result = ai_call_claude(api_key, model, prompt)
	case "openai":
		result = ai_call_openai(api_key, model, prompt)
	default:
		return ""
	}

	if result.status == 200 && result.text != "" {
		return result.text
	}
	return ""
}
