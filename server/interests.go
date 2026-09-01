// Mochi server: User interest profiles API
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.
//
// Provides mochi.interests.* builtins for managing user interest profiles.
// Interests are stored as Wikidata QIDs with weights (-100 to 100) in user.db.
// Used by feeds and forums for personalised "relevant" sort ranking.

package main

import (
	"fmt"
	"strings"
	"sync"

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

// reg_interests is the upsert definition for the user.db interests table
// (qid → weight); `updated` is informational.
var reg_interests = upsert_def{"interests", []string{"qid"}, []string{"weight", "updated"}}

// interests_writer resolves the account an interests write belongs to. It is
// the caller and never the storage account: the gate above each writer is
// require_permission, checked against the caller's own grant, while
// principal_storage answers with the route owner for every request to a domain
// route carrying a context. A signed-in visitor to a hosted site would
// otherwise spend their own grant adjusting the site owner's profile, and an
// anonymous one would do it with no account at all.
func interests_writer(t *sl.Thread) *User {
	return principal_caller(t)
}

// interests_reader resolves the account an interests read answers for. An
// anonymous request keeps the storage account, which is what renders a hosted
// site; a request by someone other than that account is refused rather than
// answered with the owner's profile. Same split as mochi.account.list.
func interests_reader(t *sl.Thread) *User {
	storage, _ := principal_storage(t)
	if storage == nil {
		return nil
	}
	if caller := principal_caller(t); caller != nil && caller.UID != storage.UID {
		return nil
	}
	return storage
}

// interests_regenerating names the accounts whose summary a background rebuild
// is already running for. The stale row is only rewritten once a rebuild
// finishes, so without this every request arriving in the meantime reads the
// same stale row, decides it is stale, and starts a rebuild of its own - one
// Wikidata resolution and one paid provider call each. Feeds and forums call
// mochi.interests.summary() from their ranking paths, so the arrivals are page
// loads. The forced path carries a 300-second interval for the same reason.
var interests_regenerating sync.Map

// interests_regenerate rebuilds the account's summary in the background unless
// a rebuild is already running, and reports whether it started one.
func interests_regenerate(user *User, db *DB) bool {
	if _, running := interests_regenerating.LoadOrStore(user.UID, true); running {
		return false
	}
	go func() {
		defer interests_regenerating.Delete(user.UID)
		summary := interests_generate_summary(user, db)
		db.exec("replace into settings (key, text, number) values ('interest_summary', ?, ?)", summary, now())
	}()
	return true
}

// mochi.interests.list() -> list: List all user interests sorted by weight descending
func api_interests_list(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if err := require_permission_acting(t, fn, "interests/read"); err != nil {
		return sl_error(fn, "%v", err)
	}

	user := interests_reader(t)
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

	user := interests_writer(t)
	if user == nil {
		return sl_error(fn, "no user")
	}

	db := db_user(user, "user")
	db.row_write(reg_interests, map[string]any{"qid": qid, "weight": weight, "updated": now()})

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

	user := interests_writer(t)
	if user == nil {
		return sl_error(fn, "no user")
	}

	db := db_user(user, "user")
	db.row_remove(reg_interests, map[string]any{"qid": qid})

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

	user := interests_writer(t)
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
			db.row_write(reg_interests, map[string]any{"qid": qid, "weight": w, "updated": ts})
		} else if delta != 0 {
			// New (or previously removed) interest: delta as initial weight (clamped)
			w := delta
			if w < -100 {
				w = -100
			}
			if w > 100 {
				w = 100
			}
			db.row_write(reg_interests, map[string]any{"qid": qid, "weight": w, "updated": ts})
		}
	}

	return sl.None, nil
}

// mochi.interests.top(n) -> list: Get top N interests by weight
func api_interests_top(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 1 {
		return sl_error(fn, "syntax: <n: int>")
	}

	if err := require_permission_acting(t, fn, "interests/read"); err != nil {
		return sl_error(fn, "%v", err)
	}

	n, err := sl.AsInt32(args[0])
	if err != nil || n < 1 {
		return sl_error(fn, "invalid count")
	}

	user := interests_reader(t)
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

	if err := require_permission_acting(t, fn, "interests/read"); err != nil {
		return sl_error(fn, "%v", err)
	}

	n, err := sl.AsInt32(args[0])
	if err != nil || n < 1 {
		return sl_error(fn, "invalid count")
	}

	user := interests_reader(t)
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
	if err := require_permission_acting(t, fn, "interests/read"); err != nil {
		return sl_error(fn, "%v", err)
	}

	var force sl.Bool
	if err := sl.UnpackArgs(fn.Name(), args, kwargs, "force?", &force); err != nil {
		return sl_error(fn, "%v", err)
	}

	user := interests_reader(t)
	if user == nil {
		return sl_error(fn, "no user")
	}

	db := db_user(user, "user")

	// A forced regeneration runs the AI synchronously, so it carries a minimum
	// interval of its own: without one, any caller wired to a button spends
	// unbounded AI budget at the user's request rate. Inside the interval the
	// cached text is returned, which is what force was asking to refresh.
	if bool(force) {
		row, err := db.row("select text, number from settings where key='interest_summary'")
		if err == nil && row != nil {
			text, _ := row["text"].(string)
			generated, _ := row["number"].(int64)
			if text != "" && now()-generated < 300 {
				return sl.String(text), nil
			}
		}
	}

	// Check cached summary (skip if force=True)
	if !bool(force) {
		row, err := db.row("select text, number from settings where key='interest_summary'")
		if err == nil && row != nil {
			text, _ := row["text"].(string)
			cached_at, _ := row["number"].(int64)

			if text != "" {
				// Fresh cache: return immediately
				if now()-cached_at < 86400 {
					return sl.String(text), nil
				}
				// Stale cache: return stale value, regenerate in background
				interests_regenerate(user, db)
				return sl.String(text), nil
			}
		}
	}

	// No cached summary or force=True: regenerate synchronously
	summary := interests_generate_summary(user, db)
	db.exec("replace into settings (key, text, number) values ('interest_summary', ?, ?)", summary, now())

	return sl.String(summary), nil
}

// interests_generate_summary builds a summary of the user's interests
// in the user's preferred language. The QID labels and the AI summary are
// both fetched in that language; the non-AI fallback uses translatable
// label keys (`interests.summary.liked`, `interests.summary.disliked`).
func interests_generate_summary(user *User, db *DB) string {
	rows, err := db.rows("select qid from interests where weight > 0 order by weight desc limit 30")
	if err != nil || len(rows) == 0 {
		rows = []map[string]any{}
	}

	var positive_qids []string
	for _, row := range rows {
		qid, _ := row["qid"].(string)
		if qid != "" {
			positive_qids = append(positive_qids, qid)
		}
	}

	negative_rows, err := db.rows("select qid from interests where weight < 0 order by weight asc limit 15")
	if err != nil {
		negative_rows = []map[string]any{}
	}

	var negative_qids []string
	for _, row := range negative_rows {
		qid, _ := row["qid"].(string)
		if qid != "" {
			negative_qids = append(negative_qids, qid)
		}
	}

	if len(positive_qids) == 0 && len(negative_qids) == 0 {
		return ""
	}

	// Resolve QID labels and the AI summary in the user's preferred language.
	language := user_language(user)
	all_qids := append(positive_qids, negative_qids...)
	labels := qid_fetch_labels(all_qids, qid_lang_for_fetch(language))

	// Try AI summary first
	summary := interests_ai_summary(user, db, positive_qids, negative_qids, labels, language)
	if summary != "" {
		return summary
	}

	// Fallback: simple label list, with translatable lead-in.
	var positive_parts []string
	for _, qid := range positive_qids {
		label := labels[qid]
		if label != "" && label != qid {
			positive_parts = append(positive_parts, label)
		}
	}
	var negative_parts []string
	for _, qid := range negative_qids {
		label := labels[qid]
		if label != "" && label != qid {
			negative_parts = append(negative_parts, label)
		}
	}
	if len(positive_parts) == 0 && len(negative_parts) == 0 {
		return ""
	}
	var result string
	if len(positive_parts) > 0 {
		result = resolve_core_label(language, "interests.summary.liked",
			map[string]any{"list": strings.Join(positive_parts, ", ")})
	}
	if len(negative_parts) > 0 {
		if result != "" {
			result += ". "
		}
		result += resolve_core_label(language, "interests.summary.disliked",
			map[string]any{"list": strings.Join(negative_parts, ", ")})
	}
	return result
}

// qid_lang_for_fetch normalises a BCP 47 tag for Wikidata's label fetch,
// which keys on language code only (not region). Variants like en-us, pt-br,
// zh-hant fall back to the bare language code.
func qid_lang_for_fetch(lang string) string {
	if lang == "" {
		return "en"
	}
	if i := strings.Index(lang, "-"); i > 0 {
		return lang[:i]
	}
	return lang
}

// interests_summary_tokens bounds the summary request. The prompt asks for two
// or three sentences, so the 16k default every call used to carry was paid for
// and never used.
const interests_summary_tokens = 1024

// interests_ai_summary attempts to generate an AI-powered summary in the
// user's preferred language.
func interests_ai_summary(user *User, db *DB, positive_qids []string, negative_qids []string, labels map[string]string, language string) string {
	// The account the user designated as their default for "ai", resolved the
	// same way mochi.ai.prompt resolves it. Taking the first enabled AI-capable
	// account instead billed the summary to whichever account was connected
	// first rather than the one the user chose.
	provider, api_key, model := ai_account(db, "")
	if api_key == "" {
		return ""
	}

	// Build interest list for prompt
	var positive_lines []string
	for _, qid := range positive_qids {
		label := labels[qid]
		if label != "" && label != qid {
			positive_lines = append(positive_lines, fmt.Sprintf("- %s (%s)", label, qid))
		}
	}
	var negative_lines []string
	for _, qid := range negative_qids {
		label := labels[qid]
		if label != "" && label != qid {
			negative_lines = append(negative_lines, fmt.Sprintf("- %s (%s)", label, qid))
		}
	}
	if len(positive_lines) == 0 && len(negative_lines) == 0 {
		return ""
	}

	var sections []string
	if len(positive_lines) > 0 {
		sections = append(sections, "Liked topics:\n"+strings.Join(positive_lines, "\n"))
	}
	if len(negative_lines) > 0 {
		sections = append(sections, "Disliked topics:\n"+strings.Join(negative_lines, "\n"))
	}

	// Localise the response. The QIDs are protocol identifiers in any language;
	// the LLM should produce the prose in the user's preferred language. Using
	// the BCP 47 tag in the prompt is enough — modern LLMs understand standard
	// language tags directly without needing a native-language name.
	lang := language
	if lang == "" {
		lang = "en"
	}
	prompt := fmt.Sprintf("Summarise the following user interests in 2-3 sentences in BCP 47 language %q. Be concise and natural. Do not list them — describe what kind of topics and themes the person is interested in, and what they dislike if applicable. Respond only with the summary text, in language %q.\n\n%s", lang, lang, strings.Join(sections, "\n\n"))

	var result ai_result
	switch provider {
	case "claude":
		result = ai_call_claude(api_key, model, prompt, interests_summary_tokens)
	case "openai":
		result = ai_call_openai(api_key, model, prompt, interests_summary_tokens)
	default:
		return ""
	}

	if result.status == 200 && result.text != "" {
		return result.text
	}
	return ""
}
