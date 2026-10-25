// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

// Mochi server: Mastodon clients' notifications and streaming
//
// The notifications app's notifications of the feeds app's - replies,
// favourites, boosts, follows, mentions - as Mastodon's; the read positions,
// filters and lists clients keep; push subscriptions, each sent a Web Push
// for each new notification; and the streaming interface, which tells a
// client of new statuses in its timelines and new notifications as the apps
// announce them on the websocket bus.

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	webpush "github.com/SherClockHolmes/webpush-go"
	"github.com/gin-gonic/gin"
	"nhooyr.io/websocket"
)

// === Notifications ===

// The notifications app keeps one row for what it rolls up - each of the
// account's events on the fediverse lands on the account's one row - so the
// interface keeps its own record of each notification as it arrives: the
// feeds app's account of it, beside the row it came by.

// mastodon_resolve_rows asks the feeds app what notifications rows are, in
// Mastodon's terms: each a reply, a favourite, a boost, a follow or a
// mention, or nil for one Mastodon has no form for. Other apps' rows are
// nil.
func mastodon_resolve_rows(s *mastodon_session, rows []map[string]any) []map[string]any {
	out := make([]map[string]any, len(rows))
	// A message to one of the account's conversations is a mention, as
	// Mastodon shows a direct message.
	if chat := mastodon_app(s.user, "direct/message"); chat != nil {
		for i, row := range rows {
			event := strings.TrimPrefix(row_string(row, "event"), chat.id+":")
			if row_string(row, "app") != chat.id || row_string(row, "topic") != "message" || !strings.HasPrefix(event, "message:") {
				continue
			}
			result, err := mastodon_direct(s, "message", Map{"message": strings.TrimPrefix(event, "message:")})
			if message := mastodon_map(result); err == nil && message != nil && message["mine"] != true {
				out[i] = map[string]any{"type": "mention", "message": message}
			}
		}
	}
	feeds := mastodon_app(s.user, "mastodon/notifications")
	if feeds == nil {
		return out
	}
	var asked []any
	var places []int
	for i, row := range rows {
		if row_string(row, "app") == feeds.id {
			asked = append(asked, map[string]any{"id": row_string(row, "id"), "topic": row_string(row, "topic"), "object": row_string(row, "object"), "event": strings.TrimPrefix(row_string(row, "event"), feeds.id+":"), "created": row_int(row, "created")})
			places = append(places, i)
		}
	}
	if len(asked) == 0 {
		return out
	}
	result, err := mastodon_call(s, "mastodon/notifications", Map{"rows": asked})
	if err != nil {
		return out
	}
	answers, _ := result.([]any)
	for i, raw := range answers {
		if i < len(places) {
			out[places[i]], _ = raw.(map[string]any)
		}
	}
	return out
}

// mastodon_alert_record keeps a notification the feeds app accounted for,
// once for each row and event, and says whether it is new.
func mastodon_alert_record(s *mastodon_session, row map[string]any, answer map[string]any) bool {
	if answer == nil || mastodon_string(answer, "type") == "" {
		return false
	}
	result, err := db_user(s.user, "user").internal.Exec("insert or ignore into alerts (account, row, event, answer, created) values (?, ?, ?, ?, ?)", s.account, row_string(row, "id"), row_string(row, "event"), json_encode(answer), row_int(row, "created"))
	if err != nil {
		return false
	}
	added, _ := result.RowsAffected()
	return added > 0
}

// mastodon_alerts_backfill records the notifications already there when
// the account's first client asks, so its list does not start empty.
func mastodon_alerts_backfill(s *mastodon_session) {
	db := db_user(s.user, "user")
	if found, _ := db.exists("select 1 from alerts where account=?", s.account); found {
		return
	}
	if found, _ := db.exists("select 1 from markers where account=? and timeline='alerts'", s.account); found {
		return
	}
	db.exec("insert or ignore into markers (account, timeline, position, version, updated) values (?, 'alerts', '', 0, ?)", s.account, now())
	rows := mastodon_notification_rows(s)
	answers := mastodon_resolve_rows(s, rows)
	for i, row := range rows {
		mastodon_alert_record(s, row, answers[i])
	}
}

// mastodon_alert is a kept notification as clients see it, or nil where
// what it names is gone.
func mastodon_alert(c *gin.Context, s *mastodon_session, row map[string]any) map[string]any {
	var answer map[string]any
	json.Unmarshal([]byte(row_string(row, "answer")), &answer)
	created := row_int(row, "created")
	notification := map[string]any{
		"id":         mastodon_number(s.user, "notification", "alert:"+strconv.FormatInt(row_int(row, "id"), 10), created),
		"type":       mastodon_string(answer, "type"),
		"created_at": mastodon_time(created),
		"group_key":  "ungrouped-" + strconv.FormatInt(row_int(row, "id"), 10),
	}
	var status map[string]any
	if message, ok := answer["message"].(map[string]any); ok {
		status = mastodon_direct_status(c, s, message)
	} else if comment, ok := answer["comment"].(map[string]any); ok {
		post, _ := comment["post"].(map[string]any)
		status = mastodon_comment(s, comment, post)
	} else if post, ok := answer["post"].(map[string]any); ok {
		status = mastodon_post(s, post)
	} else if remote := mastodon_string(answer, "remote"); remote != "" {
		if held := mastodon_snapshot_get(s.user, "status", "note:"+remote); held != nil {
			status = held
		} else if c != nil {
			if note := mastodon_fetch(c, s, remote); note != nil {
				status = mastodon_note(c, s, note)
			}
		}
	}
	if status != nil {
		notification["status"] = status
	}
	if person, ok := answer["person"].(map[string]any); ok {
		username, host, _ := strings.Cut(strings.TrimPrefix(mastodon_string(person, "address"), "@"), "@")
		p := mastodon_figure{key: mastodon_string(person, "id"), username: username, host: host, name: mastodon_string(person, "name"), avatar: mastodon_string(person, "avatar"), url: mastodon_string(person, "url")}
		if p.key == "" {
			p.key = "identity:" + mastodon_string(person, "identity")
		}
		if p.username == "" {
			p.username = mastodon_slug(p.name)
			p.host = s.domain
		}
		notification["account"] = mastodon_account(s, p)
	} else if status != nil {
		notification["account"] = status["account"]
	}
	if notification["account"] == nil || (status == nil && notification["type"] != "follow") {
		return nil
	}
	return notification
}

// mastodon_notifications_of are the account's kept notifications, newest
// first.
func mastodon_notifications_of(c *gin.Context, s *mastodon_session) []map[string]any {
	mastodon_alerts_backfill(s)
	rows, _ := db_user(s.user, "user").rows("select * from alerts where account=? order by created desc, id desc limit 400", s.account)
	var out []map[string]any
	for _, row := range rows {
		if n := mastodon_alert(c, s, row); n != nil {
			out = append(out, n)
		}
	}
	return out
}

// mastodon_notification_rows are the notifications app's rows; a test
// stands its own in.
var mastodon_notification_rows = func(s *mastodon_session) []map[string]any {
	a := app_for_service(s.user, "notifications")
	if a == nil {
		return nil
	}
	result, err := app_call_as_server(s.user, a, "list", Map{})
	if err != nil {
		return nil
	}
	return mastodon_list(result)
}

// mastodon_notifications_page answers a page of notifications, of the kinds
// asked, between the positions given.
func mastodon_notifications_page(c *gin.Context, s *mastodon_session, grouped bool) {
	in := mastodon_read(c)
	limit := mastodon_limit(in, 20)
	types := in.list("types")
	excluded := in.list("exclude_types")
	maximum, _ := strconv.ParseInt(in.text("max_id"), 10, 64)
	since, _ := strconv.ParseInt(in.text("since_id"), 10, 64)
	least, _ := strconv.ParseInt(in.text("min_id"), 10, 64)
	account := in.text("account_id")
	all := mastodon_notifications_of(c, s)
	var page []map[string]any
	for _, n := range all {
		id, _ := strconv.ParseInt(mastodon_string(n, "id"), 10, 64)
		kind := mastodon_string(n, "type")
		if (maximum > 0 && id >= maximum) || (since > 0 && id <= since) || (least > 0 && id <= least) {
			continue
		}
		if (len(types) > 0 && !slices.Contains(types, kind)) || slices.Contains(excluded, kind) {
			continue
		}
		if who, _ := n["account"].(map[string]any); account != "" && mastodon_string(who, "id") != account {
			continue
		}
		page = append(page, n)
	}
	if least > 0 && len(page) > limit {
		page = page[len(page)-limit:]
	} else if len(page) > limit {
		page = page[:limit]
	}
	if len(page) > 0 {
		mastodon_link(c, mastodon_string(page[0], "id"), mastodon_string(page[len(page)-1], "id"))
	}
	if !grouped {
		out := make([]any, 0, len(page))
		for _, n := range page {
			out = append(out, n)
		}
		c.JSON(http.StatusOK, out)
		return
	}
	// Each notification is its own group: the accounts and statuses they
	// name beside them, as grouped notifications are answered.
	accounts, statuses, groups := []any{}, []any{}, []any{}
	seen := map[string]bool{}
	for _, n := range page {
		who, _ := n["account"].(map[string]any)
		if id := mastodon_string(who, "id"); !seen["a"+id] {
			seen["a"+id] = true
			accounts = append(accounts, who)
		}
		group := map[string]any{
			"group_key":                   n["group_key"],
			"notifications_count":         1,
			"type":                        n["type"],
			"most_recent_notification_id": n["id"],
			"page_min_id":                 n["id"],
			"page_max_id":                 n["id"],
			"latest_page_notification_at": n["created_at"],
			"sample_account_ids":          []any{mastodon_string(who, "id")},
		}
		if status, ok := n["status"].(map[string]any); ok {
			group["status_id"] = status["id"]
			if id := mastodon_string(status, "id"); !seen["s"+id] {
				seen["s"+id] = true
				statuses = append(statuses, status)
			}
		}
		groups = append(groups, group)
	}
	c.JSON(http.StatusOK, map[string]any{"accounts": accounts, "statuses": statuses, "notification_groups": groups})
}

// === Read positions ===

// mastodon_markers answers how far the account has read its timelines, or
// records it.
func mastodon_markers(c *gin.Context, s *mastodon_session) {
	db := db_user(s.user, "user")
	in := mastodon_read(c)
	if c.Request.Method == "POST" {
		for _, timeline := range []string{"home", "notifications"} {
			position := mastodon_string(in.object(timeline), "last_read_id")
			if position == "" {
				continue
			}
			db.exec("insert into markers (account, timeline, position, version, updated) values (?, ?, ?, 1, ?) on conflict(account, timeline) do update set position=excluded.position, version=markers.version+1, updated=excluded.updated", s.account, timeline, position, now())
		}
	}
	timelines := in.list("timeline")
	if len(timelines) == 0 || c.Request.Method == "POST" {
		timelines = []string{"home", "notifications"}
	}
	out := map[string]any{}
	for _, timeline := range timelines {
		row, _ := db.row("select position, version, updated from markers where account=? and timeline=?", s.account, timeline)
		if row != nil {
			out[timeline] = map[string]any{"last_read_id": row_string(row, "position"), "version": row_int(row, "version"), "updated_at": mastodon_time(row_int(row, "updated"))}
		}
	}
	c.JSON(http.StatusOK, out)
}

// === Filters ===

// mastodon_filter is a filter as clients see it.
func mastodon_filter(row map[string]any) map[string]any {
	var context []string
	json.Unmarshal([]byte(row_string(row, "context")), &context)
	var keywords []map[string]any
	json.Unmarshal([]byte(row_string(row, "keywords")), &keywords)
	if keywords == nil {
		keywords = []map[string]any{}
	}
	id := strconv.FormatInt(row_int(row, "id"), 10)
	for i := range keywords {
		keywords[i]["id"] = id + "." + strconv.Itoa(i)
	}
	out := map[string]any{"id": id, "title": row_string(row, "title"), "context": context, "filter_action": row_string(row, "action"), "keywords": keywords, "statuses": []any{}, "expires_at": nil}
	if expires := row_int(row, "expires"); expires > 0 {
		out["expires_at"] = mastodon_time(expires)
	}
	return out
}

// mastodon_filters are the account's filters in force.
func mastodon_filters(s *mastodon_session) []map[string]any {
	rows, _ := db_user(s.user, "user").rows("select * from filters where account=? and (expires=0 or expires>?) order by id", s.account, now())
	out := []map[string]any{}
	for _, row := range rows {
		out = append(out, mastodon_filter(row))
	}
	return out
}

// mastodon_filtered is what of the account's filters a status's text
// matches, as a status carries it.
func mastodon_filtered(s *mastodon_session, text string) []any {
	lower := strings.ToLower(text)
	out := []any{}
	for _, filter := range mastodon_filters(s) {
		var matched []any
		for _, keyword := range filter["keywords"].([]map[string]any) {
			word := strings.ToLower(mastodon_string(keyword, "keyword"))
			if word == "" {
				continue
			}
			if keyword["whole_word"] == true {
				for _, w := range strings.FieldsFunc(lower, func(r rune) bool {
					return !(r == '_' || r == '#' || r == '@' || ('a' <= r && r <= 'z') || ('0' <= r && r <= '9') || r > 127)
				}) {
					if w == word {
						matched = append(matched, keyword["keyword"])
						break
					}
				}
			} else if strings.Contains(lower, word) {
				matched = append(matched, keyword["keyword"])
			}
		}
		if len(matched) > 0 {
			out = append(out, map[string]any{"filter": filter, "keyword_matches": matched, "status_matches": nil})
		}
	}
	return out
}

// mastodon_hidden says whether a filter hides a status outright.
func mastodon_hidden(status map[string]any) bool {
	results, _ := status["filtered"].([]any)
	for _, raw := range results {
		result, _ := raw.(map[string]any)
		filter, _ := result["filter"].(map[string]any)
		if filter["filter_action"] == "hide" {
			return true
		}
	}
	return false
}

// mastodon_filter_write makes or changes a filter from a request.
func mastodon_filter_write(c *gin.Context, s *mastodon_session, id int64) {
	in := mastodon_read(c)
	db := db_user(s.user, "user")
	var held map[string]any
	if id > 0 {
		held, _ = db.row("select * from filters where id=? and account=?", id, s.account)
		if held == nil {
			mastodon_error(c, http.StatusNotFound, "Record not found")
			return
		}
	}
	title := in.text("title")
	if title == "" && held != nil {
		title = row_string(held, "title")
	}
	if strings.TrimSpace(title) == "" || len(title) > 200 {
		mastodon_error(c, http.StatusUnprocessableEntity, "Validation failed: Title can't be blank")
		return
	}
	context := in.list("context")
	if len(context) == 0 && held != nil {
		json.Unmarshal([]byte(row_string(held, "context")), &context)
	}
	if len(context) == 0 {
		mastodon_error(c, http.StatusUnprocessableEntity, "Validation failed: Context can't be blank")
		return
	}
	action := in.text("filter_action")
	if action == "" {
		action = "warn"
		if held != nil {
			action = row_string(held, "action")
		}
	}
	if action != "warn" && action != "hide" && action != "blur" {
		mastodon_error(c, http.StatusUnprocessableEntity, "Validation failed: Filter action is invalid")
		return
	}
	var keywords []map[string]any
	if held != nil {
		json.Unmarshal([]byte(row_string(held, "keywords")), &keywords)
	}
	attributes := in.json["keywords_attributes"]
	if list, ok := attributes.([]any); ok {
		for _, raw := range list {
			k, _ := raw.(map[string]any)
			keywords = mastodon_keyword_apply(keywords, k)
		}
	} else {
		for key := range in.values {
			if strings.HasPrefix(key, "keywords_attributes[") && strings.HasSuffix(key, "][keyword]") {
				index := strings.TrimSuffix(strings.TrimPrefix(key, "keywords_attributes["), "][keyword]")
				prefix := "keywords_attributes[" + index + "]"
				keywords = mastodon_keyword_apply(keywords, map[string]any{"keyword": in.text(prefix + "[keyword]"), "whole_word": in.text(prefix + "[whole_word]"), "id": in.text(prefix + "[id]"), "_destroy": in.text(prefix + "[_destroy]")})
			}
		}
	}
	if len(keywords) > 50 {
		mastodon_error(c, http.StatusUnprocessableEntity, "Validation failed: Too many keywords")
		return
	}
	expires := int64(0)
	if in.has("expires_in") {
		if seconds, err := strconv.ParseInt(in.text("expires_in"), 10, 64); err == nil && seconds > 0 {
			expires = now() + seconds
		}
	} else if held != nil {
		expires = row_int(held, "expires")
	}
	if keywords == nil {
		keywords = []map[string]any{}
	}
	if id > 0 {
		db.exec("update filters set title=?, context=?, action=?, keywords=?, expires=? where id=? and account=?", title, json_encode(context), action, json_encode(keywords), expires, id, s.account)
	} else {
		result, err := db.internal.Exec("insert into filters (account, title, context, action, keywords, expires, created) values (?, ?, ?, ?, ?, ?, ?)", s.account, title, json_encode(context), action, json_encode(keywords), expires, now())
		if err != nil {
			mastodon_error(c, http.StatusInternalServerError, "The filter could not be kept")
			return
		}
		id, _ = result.LastInsertId()
	}
	row, _ := db.row("select * from filters where id=?", id)
	c.JSON(http.StatusOK, mastodon_filter(row))
}

// mastodon_keyword_apply adds, changes or removes a filter's keyword.
func mastodon_keyword_apply(keywords []map[string]any, k map[string]any) []map[string]any {
	position := -1
	if id := mastodon_string(k, "id"); id != "" {
		if _, index, found := strings.Cut(id, "."); found {
			position, _ = strconv.Atoi(index)
		}
	}
	destroy := fmt.Sprint(k["_destroy"]) == "true" || fmt.Sprint(k["_destroy"]) == "1"
	whole := fmt.Sprint(k["whole_word"]) == "true" || fmt.Sprint(k["whole_word"]) == "1"
	word := strings.TrimSpace(mastodon_string(k, "keyword"))
	if position >= 0 && position < len(keywords) {
		if destroy {
			return append(keywords[:position], keywords[position+1:]...)
		}
		if word != "" {
			keywords[position]["keyword"] = word
		}
		keywords[position]["whole_word"] = whole
		return keywords
	}
	if word == "" || destroy || len(word) > 200 {
		return keywords
	}
	return append(keywords, map[string]any{"keyword": word, "whole_word": whole})
}

// === The rest ===

// mastodon_other_dispatch answers notifications, read positions, filters,
// lists, push, conversations and reports, reporting whether the request was
// one.
func mastodon_other_dispatch(c *gin.Context, s *mastodon_session, version string, parts []string) bool {
	method := c.Request.Method
	route := method + " " + version + "/" + strings.Join(parts, "/")
	db := db_user(s.user, "user")
	switch route {
	case "GET v1/notifications":
		mastodon_notifications_page(c, s, false)
		return true
	case "GET v2/notifications":
		mastodon_notifications_page(c, s, true)
		return true
	case "GET v1/notifications/unread_count", "GET v2/notifications/unread_count":
		marker, _ := db.row("select position from markers where account=? and timeline='notifications'", s.account)
		after, _ := strconv.ParseInt(row_string(marker, "position"), 10, 64)
		count := 0
		for _, n := range mastodon_notifications_of(c, s) {
			if id, _ := strconv.ParseInt(mastodon_string(n, "id"), 10, 64); id > after {
				count++
			}
		}
		c.JSON(http.StatusOK, map[string]any{"count": count})
		return true
	case "POST v1/notifications/clear":
		db.exec("delete from alerts where account=?", s.account)
		c.JSON(http.StatusOK, map[string]any{})
		return true
	case "GET v2/notifications/policy", "GET v1/notifications/policy":
		c.JSON(http.StatusOK, map[string]any{"for_not_following": "accept", "for_not_followers": "accept", "for_new_accounts": "accept", "for_private_mentions": "accept", "for_limited_accounts": "accept", "filter_not_following": false, "filter_not_followers": false, "filter_new_accounts": false, "filter_private_mentions": false, "summary": map[string]any{"pending_requests_count": 0, "pending_notifications_count": 0}})
		return true
	case "GET v1/notifications/requests":
		c.JSON(http.StatusOK, []any{})
		return true
	case "GET v1/markers", "POST v1/markers":
		mastodon_markers(c, s)
		return true
	case "GET v2/filters":
		out := []any{}
		for _, f := range mastodon_filters(s) {
			out = append(out, f)
		}
		c.JSON(http.StatusOK, out)
		return true
	case "POST v2/filters":
		mastodon_filter_write(c, s, 0)
		return true
	case "GET v1/filters":
		c.JSON(http.StatusOK, []any{})
		return true
	case "GET v1/lists":
		result, _ := mastodon_call(s, "mastodon/lists", Map{})
		out := []any{}
		for _, list := range mastodon_list(result) {
			out = append(out, mastodon_list_json(s, list))
		}
		c.JSON(http.StatusOK, out)
		return true
	case "POST v1/lists":
		in := mastodon_read(c)
		result, err := mastodon_call(s, "mastodon/list", Map{"action": "create", "name": in.text("title")})
		mastodon_list_answer(c, s, result, err)
		return true
	case "GET v1/push/subscription", "POST v1/push/subscription", "PUT v1/push/subscription", "DELETE v1/push/subscription":
		mastodon_subscription(c, s)
		return true
	case "GET v1/conversations":
		mastodon_conversations_page(c, s)
		return true
	case "POST v1/reports":
		mastodon_report(c, s)
		return true
	}
	if taken, ok := mastodon_match(parts, "notifications", "*"); ok && method == "GET" {
		id := strings.TrimPrefix(mastodon_key(s.user, "notification", taken[0]), "alert:")
		row, _ := db.row("select * from alerts where id=? and account=?", id, s.account)
		if n := mastodon_alert(c, s, row); row != nil && n != nil {
			c.JSON(http.StatusOK, n)
			return true
		}
		mastodon_error(c, http.StatusNotFound, "Record not found")
		return true
	}
	if taken, ok := mastodon_match(parts, "notifications", "*", "dismiss"); ok && method == "POST" {
		id := strings.TrimPrefix(mastodon_key(s.user, "notification", taken[0]), "alert:")
		db.exec("delete from alerts where id=? and account=?", id, s.account)
		c.JSON(http.StatusOK, map[string]any{})
		return true
	}
	if taken, ok := mastodon_match(parts, "filters", "*"); ok && version == "v2" {
		id, _ := strconv.ParseInt(taken[0], 10, 64)
		switch method {
		case "GET":
			row, _ := db.row("select * from filters where id=? and account=?", id, s.account)
			if row == nil {
				mastodon_error(c, http.StatusNotFound, "Record not found")
				return true
			}
			c.JSON(http.StatusOK, mastodon_filter(row))
		case "PUT":
			mastodon_filter_write(c, s, id)
		case "DELETE":
			db.exec("delete from filters where id=? and account=?", id, s.account)
			c.JSON(http.StatusOK, map[string]any{})
		default:
			return false
		}
		return true
	}
	if taken, ok := mastodon_match(parts, "filters", "*", "keywords"); ok && version == "v2" {
		id, _ := strconv.ParseInt(taken[0], 10, 64)
		row, _ := db.row("select * from filters where id=? and account=?", id, s.account)
		if row == nil {
			mastodon_error(c, http.StatusNotFound, "Record not found")
			return true
		}
		filter := mastodon_filter(row)
		if method == "POST" {
			in := mastodon_read(c)
			var keywords []map[string]any
			json.Unmarshal([]byte(row_string(row, "keywords")), &keywords)
			keywords = mastodon_keyword_apply(keywords, map[string]any{"keyword": in.text("keyword"), "whole_word": in.text("whole_word")})
			db.exec("update filters set keywords=? where id=?", json_encode(keywords), id)
			row, _ = db.row("select * from filters where id=?", id)
			filter = mastodon_filter(row)
			all := filter["keywords"].([]map[string]any)
			if len(all) > 0 {
				c.JSON(http.StatusOK, all[len(all)-1])
				return true
			}
		}
		c.JSON(http.StatusOK, filter["keywords"])
		return true
	}
	if taken, ok := mastodon_match(parts, "filters", "keywords", "*"); ok && version == "v2" {
		filter, index, _ := strings.Cut(taken[0], ".")
		id, _ := strconv.ParseInt(filter, 10, 64)
		row, _ := db.row("select * from filters where id=? and account=?", id, s.account)
		position, err := strconv.Atoi(index)
		if row == nil || err != nil {
			mastodon_error(c, http.StatusNotFound, "Record not found")
			return true
		}
		var keywords []map[string]any
		json.Unmarshal([]byte(row_string(row, "keywords")), &keywords)
		if position < 0 || position >= len(keywords) {
			mastodon_error(c, http.StatusNotFound, "Record not found")
			return true
		}
		switch method {
		case "DELETE":
			keywords = append(keywords[:position], keywords[position+1:]...)
		case "PUT":
			in := mastodon_read(c)
			keywords = mastodon_keyword_apply(keywords, map[string]any{"id": taken[0], "keyword": in.text("keyword"), "whole_word": in.text("whole_word")})
		}
		db.exec("update filters set keywords=? where id=?", json_encode(keywords), id)
		if method == "DELETE" {
			c.JSON(http.StatusOK, map[string]any{})
		} else {
			keywords[position]["id"] = taken[0]
			c.JSON(http.StatusOK, keywords[position])
		}
		return true
	}
	if taken, ok := mastodon_match(parts, "lists", "*"); ok {
		list := mastodon_key(s.user, "list", taken[0])
		if list == "" {
			mastodon_error(c, http.StatusNotFound, "Record not found")
			return true
		}
		switch method {
		case "GET":
			result, _ := mastodon_call(s, "mastodon/lists", Map{})
			for _, l := range mastodon_list(result) {
				if mastodon_string(l, "id") == list {
					c.JSON(http.StatusOK, mastodon_list_json(s, l))
					return true
				}
			}
			mastodon_error(c, http.StatusNotFound, "Record not found")
		case "PUT":
			in := mastodon_read(c)
			result, err := mastodon_call(s, "mastodon/list", Map{"action": "rename", "id": list, "name": in.text("title")})
			mastodon_list_answer(c, s, result, err)
		case "DELETE":
			result, err := mastodon_call(s, "mastodon/list", Map{"action": "delete", "id": list})
			if answer, _ := result.(map[string]any); err != nil || mastodon_string(answer, "error") != "" {
				mastodon_list_answer(c, s, result, err)
				return true
			}
			c.JSON(http.StatusOK, map[string]any{})
		default:
			return false
		}
		return true
	}
	if taken, ok := mastodon_match(parts, "lists", "*", "accounts"); ok {
		list := mastodon_key(s.user, "list", taken[0])
		if list == "" {
			mastodon_error(c, http.StatusNotFound, "Record not found")
			return true
		}
		if method == "GET" {
			result, _ := mastodon_call(s, "mastodon/list", Map{"action": "accounts", "id": list})
			out := []any{}
			for _, address := range mastodon_list_strings(result) {
				if held := mastodon_snapshot_get(s.user, "account", address); held != nil {
					out = append(out, held)
				} else if r := mastodon_resolve(c, s, address); r != nil {
					out = append(out, mastodon_account(s, mastodon_resolved(r)))
				}
			}
			c.JSON(http.StatusOK, out)
			return true
		}
		action := "add"
		if method == "DELETE" {
			action = "remove"
		}
		for _, number := range mastodon_read(c).list("account_ids") {
			key := mastodon_key(s.user, "account", number)
			if key == "" {
				mastodon_error(c, http.StatusNotFound, "Record not found")
				return true
			}
			result, err := mastodon_call(s, "mastodon/list", Map{"action": action, "id": list, "address": key})
			if answer, _ := result.(map[string]any); err != nil || mastodon_string(answer, "error") != "" {
				mastodon_list_answer(c, s, result, err)
				return true
			}
		}
		c.JSON(http.StatusOK, map[string]any{})
		return true
	}
	if taken, ok := mastodon_match(parts, "conversations", "*"); ok && method == "DELETE" {
		// Taken off the user's list until it has a new message.
		chat := mastodon_key(s.user, "conversation", taken[0])
		if chat == "" {
			mastodon_error(c, http.StatusNotFound, "Record not found")
			return true
		}
		mastodon_snapshot(s.user, "hidden", chat, map[string]any{"at": now()})
		c.JSON(http.StatusOK, map[string]any{})
		return true
	}
	if taken, ok := mastodon_match(parts, "conversations", "*", "read"); ok && method == "POST" {
		chat := mastodon_key(s.user, "conversation", taken[0])
		if chat == "" {
			mastodon_error(c, http.StatusNotFound, "Record not found")
			return true
		}
		if result, err := mastodon_direct(s, "read", Map{"chat": chat}); err != nil || mastodon_string(mastodon_map(result), "error") != "" {
			mastodon_error(c, http.StatusNotFound, "Record not found")
			return true
		}
		for _, conversation := range mastodon_conversations_of(c, s, 0, 100) {
			if conversation["id"] == taken[0] {
				c.JSON(http.StatusOK, conversation)
				return true
			}
		}
		mastodon_error(c, http.StatusNotFound, "Record not found")
		return true
	}
	return false
}

// mastodon_map is an answer as a map, or nil.
func mastodon_map(result any) map[string]any {
	m, _ := result.(map[string]any)
	return m
}

// mastodon_conversations_of are the account's direct conversations as
// Mastodon's, the latest first, those the user took off the list and that
// have had nothing since left out; before is the time a page ends at.
func mastodon_conversations_of(c *gin.Context, s *mastodon_session, before int64, limit int) []map[string]any {
	result, err := mastodon_direct(s, "conversations", Map{"limit": limit, "before": before})
	out := []map[string]any{}
	if err != nil {
		return out
	}
	for _, chat := range mastodon_list(result) {
		id := mastodon_string(chat, "id")
		updated := mastodon_integer(chat, "updated")
		if hidden := mastodon_snapshot_get(s.user, "hidden", id); hidden != nil && mastodon_integer(hidden, "at") >= updated {
			continue
		}
		accounts := []any{}
		for _, person := range mastodon_list(chat["people"]) {
			accounts = append(accounts, mastodon_member(c, s, mastodon_string(person, "id"), mastodon_string(person, "name")))
		}
		conversation := map[string]any{"id": mastodon_number(s.user, "conversation", id, updated), "unread": chat["unread"] == true, "accounts": accounts, "last_status": nil}
		if last, ok := chat["last"].(map[string]any); ok && last != nil {
			last["people"] = chat["people"]
			conversation["last_status"] = mastodon_direct_status(c, s, last)
		}
		mastodon_snapshot(s.user, "conversation", id, map[string]any{"updated": updated})
		out = append(out, conversation)
	}
	return out
}

// mastodon_conversations_page answers a page of the account's conversations.
func mastodon_conversations_page(c *gin.Context, s *mastodon_session) {
	in := mastodon_read(c)
	limit := mastodon_limit(in, 20)
	before := int64(0)
	if maximum := in.text("max_id"); maximum != "" {
		if held := mastodon_snapshot_get(s.user, "conversation", mastodon_key(s.user, "conversation", maximum)); held != nil {
			before = mastodon_integer(held, "updated")
		}
	}
	out := mastodon_conversations_of(c, s, before, limit)
	if len(out) > 0 {
		mastodon_link(c, mastodon_string(out[0], "id"), mastodon_string(out[len(out)-1], "id"))
	}
	c.JSON(http.StatusOK, out)
}

// mastodon_list_strings is a function's answer as a list of text.
func mastodon_list_strings(result any) []string {
	items, _ := result.([]any)
	var out []string
	for _, item := range items {
		if text, ok := item.(string); ok {
			out = append(out, text)
		}
	}
	return out
}

// mastodon_list_json is one of the user's feeds as a list.
func mastodon_list_json(s *mastodon_session, list map[string]any) map[string]any {
	return map[string]any{"id": mastodon_number(s.user, "list", mastodon_string(list, "id"), mastodon_integer(list, "created")), "title": mastodon_string(list, "name"), "replies_policy": "list", "exclusive": false}
}

// mastodon_list_answer answers what the feeds app said of a list.
func mastodon_list_answer(c *gin.Context, s *mastodon_session, result any, err error) {
	answer, _ := result.(map[string]any)
	if err != nil || answer == nil {
		mastodon_error(c, http.StatusUnprocessableEntity, "The list could not be changed")
		return
	}
	if failure := mastodon_string(answer, "error"); failure != "" {
		mastodon_error(c, http.StatusUnprocessableEntity, failure)
		return
	}
	if list, ok := answer["list"].(map[string]any); ok {
		c.JSON(http.StatusOK, mastodon_list_json(s, list))
		return
	}
	c.JSON(http.StatusOK, map[string]any{})
}

// mastodon_report keeps a report the user makes of someone for this
// server's administrators, with the statuses named and their text; and, the
// person being on another server, sends it on to their server's moderators
// as a Flag naming the statuses by their ids there, unless the client asks it
// not be forwarded.
func mastodon_report(c *gin.Context, s *mastodon_session) {
	in := mastodon_read(c)
	key, account := mastodon_account_by_number(c, s, in.text("account_id"))
	local := person_local(key)
	if key == s.actor || (local != nil && local.UID == s.user.UID) {
		mastodon_error(c, http.StatusUnprocessableEntity, "Validation failed: You cannot report yourself")
		return
	}
	if key == "" || account == nil {
		mastodon_error(c, http.StatusNotFound, "Record not found")
		return
	}
	comment := in.text("comment")
	if utf8.RuneCountInString(comment) > 1000 {
		mastodon_error(c, http.StatusUnprocessableEntity, "Validation failed: Comment is too long (maximum is 1000 characters)")
		return
	}
	category := in.text("category")
	if !slices.Contains([]string{"spam", "legal", "violation", "other"}, category) {
		category = "other"
	}
	var objects, texts []string
	numbers := []string{}
	for _, number := range in.list("status_ids") {
		item := mastodon_find(c, s, number)
		if item == nil {
			continue
		}
		numbers = append(numbers, number)
		status := mastodon_render(c, s, item)
		if uri := strings.TrimPrefix(item.key, "note:"); strings.HasPrefix(item.key, "note:") {
			objects = append(objects, uri)
		} else if remote := mastodon_string(item.post, "remote"); item.comment == nil && strings.HasPrefix(remote, "https://") {
			objects = append(objects, remote)
		} else if remote := mastodon_string(item.comment, "remote"); strings.HasPrefix(remote, "https://") {
			objects = append(objects, remote)
		} else if uri, _ := status["uri"].(string); uri != "" {
			objects = append(objects, uri)
		}
		if content, _ := status["content"].(string); html_text(content) != "" {
			texts = append(texts, html_text(content))
		}
	}
	user := ""
	if local != nil {
		user = local.UID
	}
	name := key
	if acct, _ := account["acct"].(string); acct != "" {
		name = "@" + acct
		if !strings.Contains(acct, "@") {
			name += "@" + s.domain
		}
	}
	id := report_add(report{
		Network:  "fediverse",
		Reporter: s.actor,
		Person:   key,
		User:     user,
		Objects:  objects,
		Category: category,
		Comment:  comment,
		Content:  strings.Join(texts, "\n\n"),
		Names:    map[string]string{s.actor: "@" + s.handle + "@" + s.domain, key: name},
	})
	if id == "" {
		mastodon_error(c, http.StatusTooManyRequests, "Too many reports are waiting")
		return
	}
	forwarded := false
	if local == nil && strings.HasPrefix(key, "https://") && (!in.has("forward") || in.flag("forward")) {
		var theirs []string
		for _, object := range objects {
			if activitypub_host_of(object) == activitypub_host_of(key) {
				theirs = append(theirs, object)
			}
		}
		if err := account_flag(c.Request.Context(), s.user, s.account, s.actor, key, theirs, comment); err != nil {
			debug("Mastodon report of %s not forwarded: %v", key, err)
		} else {
			forwarded = true
		}
	}
	c.JSON(http.StatusOK, map[string]any{
		"id":              mastodon_number(s.user, "report", id, now()),
		"action_taken":    false,
		"action_taken_at": nil,
		"category":        category,
		"comment":         comment,
		"forwarded":       forwarded,
		"created_at":      mastodon_time(now()),
		"status_ids":      numbers,
		"rule_ids":        []any{},
		"target_account":  account,
	})
}

// === Push ===

// mastodon_alerts are the kinds of notification a subscription may ask for.
var mastodon_alerts = []string{"mention", "status", "reblog", "follow", "follow_request", "favourite", "poll", "update", "admin.sign_up", "admin.report"}

// mastodon_subscription answers the token's push subscription, or makes,
// changes or ends it.
func mastodon_subscription(c *gin.Context, s *mastodon_session) {
	db := db_user(s.user, "user")
	hash := s.token.Hash
	in := mastodon_read(c)
	switch c.Request.Method {
	case "DELETE":
		db.exec("delete from subscriptions where token=?", hash)
		c.JSON(http.StatusOK, map[string]any{})
		return
	case "POST":
		subscription := in.object("subscription")
		keys, _ := subscription["keys"].(map[string]any)
		if keys == nil {
			keys = map[string]any{}
			for name, value := range in.values {
				if inner, ok := strings.CutPrefix(name, "subscription[keys]["); ok && len(value) > 0 {
					keys[strings.TrimSuffix(inner, "]")] = value[0]
				}
			}
		}
		endpoint := mastodon_string(subscription, "endpoint")
		if endpoint == "" {
			endpoint = in.text("subscription[endpoint]")
		}
		auth, key := mastodon_string(keys, "auth"), mastodon_string(keys, "p256dh")
		u, err := url.Parse(endpoint)
		if err != nil || u.Scheme != "https" || u.Host == "" || auth == "" || key == "" || len(endpoint) > 2048 {
			mastodon_error(c, http.StatusUnprocessableEntity, "Validation failed: Endpoint is invalid")
			return
		}
		standard := strings.EqualFold(mastodon_string(subscription, "standard"), "true") || in.text("subscription[standard]") == "true"
		db.exec("insert into subscriptions (token, account, endpoint, auth, key, alerts, policy, standard, created) values (?, ?, ?, ?, ?, '{}', 'all', ?, ?) on conflict(token) do update set endpoint=excluded.endpoint, auth=excluded.auth, key=excluded.key, standard=excluded.standard", hash, s.account, endpoint, auth, key, standard, now())
		fallthrough
	case "PUT":
		row, _ := db.row("select * from subscriptions where token=?", hash)
		if row == nil {
			mastodon_error(c, http.StatusNotFound, "Record not found")
			return
		}
		alerts := map[string]bool{}
		json.Unmarshal([]byte(row_string(row, "alerts")), &alerts)
		data := in.object("data")
		given, _ := data["alerts"].(map[string]any)
		for _, kind := range mastodon_alerts {
			if given != nil {
				if v, found := given[kind]; found {
					alerts[kind] = fmt.Sprint(v) == "true"
				}
			}
			if value := in.text("data[alerts][" + kind + "]"); value != "" {
				alerts[kind] = value == "true"
			}
		}
		policy := mastodon_string(data, "policy")
		if policy == "" {
			policy = in.text("data[policy]")
		}
		if policy == "" {
			policy = row_string(row, "policy")
		}
		if !slices.Contains([]string{"all", "followed", "follower", "none"}, policy) {
			policy = "all"
		}
		db.exec("update subscriptions set alerts=?, policy=? where token=?", json_encode(alerts), policy, hash)
	}
	row, _ := db.row("select * from subscriptions where token=?", hash)
	if row == nil {
		mastodon_error(c, http.StatusNotFound, "Record not found")
		return
	}
	alerts := map[string]bool{}
	json.Unmarshal([]byte(row_string(row, "alerts")), &alerts)
	shown := map[string]any{}
	for _, kind := range mastodon_alerts {
		shown[kind] = alerts[kind]
	}
	webpush_ensure()
	c.JSON(http.StatusOK, map[string]any{"id": row_int(row, "created"), "endpoint": row_string(row, "endpoint"), "standard": row_int(row, "standard") != 0, "alerts": shown, "policy": row_string(row, "policy"), "server_key": webpush_public})
}

// mastodon_guarded sends a push through the server's guard on requests to
// other hosts, as any request a user's data causes is sent.
type mastodon_guarded struct {
	user *User
}

func (g mastodon_guarded) Do(request *http.Request) (*http.Response, error) {
	return network_request(request.Context(), g.user, "fediverse", request, 0)
}

// mastodon_push sends a notification to each push subscription of the
// account's that asks for its kind: a Web Push, encrypted to the
// subscription's keys and signed with the server's key, telling the app the
// notification to fetch.
func mastodon_push(s *mastodon_session, notification map[string]any) {
	rows, _ := db_user(s.user, "user").rows("select * from subscriptions where account=?", s.account)
	if len(rows) == 0 {
		return
	}
	webpush_ensure()
	if webpush_public == "" || webpush_private == "" {
		return
	}
	kind := mastodon_string(notification, "type")
	account, _ := notification["account"].(map[string]any)
	title := mastodon_string(account, "display_name")
	body := ""
	if status, ok := notification["status"].(map[string]any); ok {
		body = strip_tags(mastodon_string(status, "content"))
		if len([]rune(body)) > 140 {
			body = string([]rune(body)[:140])
		}
	}
	for _, row := range rows {
		alerts := map[string]bool{}
		json.Unmarshal([]byte(row_string(row, "alerts")), &alerts)
		if !alerts[kind] || row_string(row, "policy") == "none" {
			continue
		}
		hash := row_string(row, "token")
		var t Token
		if !db_open("db/users.db").scan(&t, "select hash, kind from tokens where hash=?", hash) {
			db_user(s.user, "user").exec("delete from subscriptions where token=?", hash)
			continue
		}
		payload := map[string]any{
			"notification_id":   notification["id"],
			"notification_type": kind,
			"preferred_locale":  user_language(s.user),
			"icon":              mastodon_string(account, "avatar"),
			"title":             title,
			"body":              body,
		}
		subscription := &webpush.Subscription{Endpoint: row_string(row, "endpoint"), Keys: webpush.Keys{Auth: row_string(row, "auth"), P256dh: row_string(row, "key")}}
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		response, err := webpush.SendNotificationWithContext(ctx, []byte(json_encode(payload)), subscription, &webpush.Options{
			HTTPClient:      mastodon_guarded{s.user},
			Subscriber:      "mailto:webpush@" + s.domain,
			VAPIDPublicKey:  webpush_public,
			VAPIDPrivateKey: webpush_private,
			TTL:             86400,
			Urgency:         webpush.UrgencyNormal,
		})
		cancel()
		if err != nil {
			debug("Mastodon push to %s failed: %v", webpush_endpoint_redact(row_string(row, "endpoint")), err)
			continue
		}
		response.Body.Close()
		if response.StatusCode == http.StatusNotFound || response.StatusCode == http.StatusGone {
			db_user(s.user, "user").exec("delete from subscriptions where token=?", hash)
		}
	}
}

// === Streaming ===

// A mastodon_stream is one client's streaming connection: the account, the
// streams it follows, and its queue of events.
type mastodon_stream struct {
	session *mastodon_session
	lock    sync.Mutex
	streams map[string]bool
	events  chan []byte
}

var mastodon_streams = map[string]map[*mastodon_stream]bool{}
var mastodon_streams_lock sync.Mutex

// mastodon_stream_add and mastodon_stream_remove keep the registry of
// connections by user.
func mastodon_stream_add(st *mastodon_stream) {
	mastodon_streams_lock.Lock()
	defer mastodon_streams_lock.Unlock()
	uid := st.session.user.UID
	if mastodon_streams[uid] == nil {
		mastodon_streams[uid] = map[*mastodon_stream]bool{}
	}
	mastodon_streams[uid][st] = true
}

func mastodon_stream_remove(st *mastodon_stream) {
	mastodon_streams_lock.Lock()
	defer mastodon_streams_lock.Unlock()
	uid := st.session.user.UID
	delete(mastodon_streams[uid], st)
	if len(mastodon_streams[uid]) == 0 {
		delete(mastodon_streams, uid)
	}
}

// mastodon_streams_of is a user's streaming connections.
func mastodon_streams_of(uid string) []*mastodon_stream {
	mastodon_streams_lock.Lock()
	defer mastodon_streams_lock.Unlock()
	var out []*mastodon_stream
	for st := range mastodon_streams[uid] {
		out = append(out, st)
	}
	return out
}

// mastodon_stream_name is a stream as a connection names it, with its tag or
// list.
func mastodon_stream_name(stream string, tag string, list string) string {
	switch stream {
	case "hashtag", "hashtag:local":
		return "hashtag:" + strings.ToLower(tag)
	case "list":
		return "list:" + list
	case "public:local", "public:remote", "public:media", "public:local:media", "public:remote:media":
		return "public"
	}
	return stream
}

// send queues an event for streams of the connection's that want it.
func (st *mastodon_stream) send(streams []string, event string, payload string) {
	st.lock.Lock()
	var wanted []string
	for _, name := range streams {
		if st.streams[name] {
			wanted = append(wanted, name)
		}
	}
	st.lock.Unlock()
	if len(wanted) == 0 {
		return
	}
	message, _ := json.Marshal(map[string]any{"stream": wanted, "event": event, "payload": payload})
	select {
	case st.events <- message:
	default:
	}
}

// mastodon_streaming answers a streaming connection: a websocket, whose
// client subscribes and unsubscribes to streams by message, or a stream of
// server-sent events for one stream named by the path.
func mastodon_streaming(c *gin.Context, parts []string) {
	s := mastodon_authenticate(c, "read:statuses")
	if s == nil {
		return
	}
	st := &mastodon_stream{session: s, streams: map[string]bool{}, events: make(chan []byte, 64)}
	if name := strings.Join(parts, ":"); name != "" {
		st.streams[mastodon_stream_name(name, c.Query("tag"), c.Query("list"))] = true
	} else if stream := c.Query("stream"); stream != "" {
		st.streams[mastodon_stream_name(stream, c.Query("tag"), mastodon_key(s.user, "list", c.Query("list")))] = true
	}
	if st.streams["user"] {
		st.streams["user:notification"] = true
	}
	mastodon_stream_add(st)
	defer mastodon_stream_remove(st)
	if strings.EqualFold(c.GetHeader("Upgrade"), "websocket") {
		accept := &websocket.AcceptOptions{InsecureSkipVerify: true}
		if protocol := c.GetHeader("Sec-WebSocket-Protocol"); protocol != "" {
			accept.Subprotocols = []string{strings.TrimSpace(strings.Split(protocol, ",")[0])}
		}
		ws, err := websocket.Accept(c.Writer, c.Request, accept)
		if err != nil {
			return
		}
		defer ws.CloseNow()
		ctx, cancel := context.WithCancel(c.Request.Context())
		defer cancel()
		go func() {
			defer cancel()
			for {
				kind, data, err := ws.Read(ctx)
				if err != nil {
					return
				}
				if kind != websocket.MessageText {
					continue
				}
				var message struct {
					Type   string `json:"type"`
					Stream string `json:"stream"`
					Tag    string `json:"tag"`
					List   string `json:"list"`
				}
				if json.Unmarshal(data, &message) != nil {
					continue
				}
				name := mastodon_stream_name(message.Stream, message.Tag, mastodon_key(s.user, "list", message.List))
				st.lock.Lock()
				switch message.Type {
				case "subscribe":
					st.streams[name] = true
					if name == "user" {
						st.streams["user:notification"] = true
					}
				case "unsubscribe":
					delete(st.streams, name)
				}
				st.lock.Unlock()
			}
		}()
		heartbeat := time.NewTicker(30 * time.Second)
		defer heartbeat.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case message := <-st.events:
				write, done := context.WithTimeout(ctx, websocket_write_timeout)
				err := ws.Write(write, websocket.MessageText, message)
				done()
				if err != nil {
					return
				}
			case <-heartbeat.C:
				ping, done := context.WithTimeout(ctx, websocket_write_timeout)
				err := ws.Ping(ping)
				done()
				if err != nil {
					return
				}
			}
		}
	}
	// Server-sent events.
	c.Header("Content-Type", "text/event-stream")
	c.Header("Cache-Control", "no-cache")
	c.Status(http.StatusOK)
	c.Writer.Flush()
	heartbeat := time.NewTicker(30 * time.Second)
	defer heartbeat.Stop()
	ctx := c.Request.Context()
	for {
		select {
		case <-ctx.Done():
			return
		case message := <-st.events:
			var event struct {
				Event   string `json:"event"`
				Payload string `json:"payload"`
			}
			json.Unmarshal(message, &event)
			fmt.Fprintf(c.Writer, "event: %s\ndata: %s\n\n", event.Event, event.Payload)
			c.Writer.Flush()
		case <-heartbeat.C:
			fmt.Fprint(c.Writer, ":thump\n\n")
			c.Writer.Flush()
		}
	}
}

// === What the apps announce ===

// mastodon_heard is told what an app announces on the websocket bus for a
// user: the feeds app's new, changed and deleted posts become timeline
// events for the user's streaming connections, and the notifications app's
// new notifications become notification events and pushes. Run apart from
// the announcing app, which is not kept waiting.
func mastodon_heard(user *User, a *App, key string, content any) {
	message, _ := content.(map[string]any)
	if !networks_development || message == nil || user == nil || a == nil {
		return
	}
	kind := mastodon_string(message, "type")
	switch {
	case kind == "new" && key == "notifications" && message["read"] != true && mastodon_integer(message, "read") == 0:
		// Kept only for a user some Mastodon app is signed in for.
		if signed, _ := db_open("db/users.db").exists("select 1 from tokens where user=? and network='fediverse' and kind=?", user.UID, token_signin); !signed {
			return
		}
		go mastodon_heard_notification(user, mastodon_string(message, "id"))
	case kind == "post/create" || kind == "post/edit" || kind == "post/delete":
		streams := mastodon_streams_of(user.UID)
		if len(streams) == 0 {
			return
		}
		go mastodon_heard_post(streams, kind, mastodon_string(message, "post"))
	case kind == "" && message["event"] == nil && mastodon_string(message, "id") != "" && message["body"] != nil:
		// A new message of a conversation, as chat writes it.
		av := a.active(user)
		if av == nil {
			return
		}
		if _, declared := av.Functions["direct/message"]; !declared {
			return
		}
		streams := mastodon_streams_of(user.UID)
		if len(streams) == 0 {
			return
		}
		go mastodon_heard_direct(streams, mastodon_string(message, "id"))
	}
}

// mastodon_heard_direct tells the direct streams of a new message: the
// conversation it is in, as it stands now.
func mastodon_heard_direct(streams []*mastodon_stream, id string) {
	for _, st := range streams {
		s := st.session
		result, err := mastodon_direct(s, "message", Map{"message": id})
		message := mastodon_map(result)
		if err != nil || message == nil {
			continue
		}
		number := mastodon_number(s.user, "conversation", mastodon_string(message, "chat"), mastodon_integer(message, "created"))
		for _, conversation := range mastodon_conversations_of(nil, s, 0, 40) {
			if conversation["id"] == number {
				payload, _ := json.Marshal(conversation)
				st.send([]string{"direct"}, "conversation", string(payload))
			}
		}
	}
}

// mastodon_heard_notification tells each of the user's accounts' clients of
// a new notification: by streaming and by push.
func mastodon_heard_notification(user *User, id string) {
	for _, account := range accounts_hosted(user, "fediverse") {
		s := mastodon_session_for(user, account)
		if s == nil {
			continue
		}
		var row map[string]any
		for _, r := range mastodon_notification_rows(s) {
			if row_string(r, "id") == id {
				row = r
			}
		}
		if row == nil {
			return
		}
		answer := mastodon_resolve_rows(s, []map[string]any{row})[0]
		if answer == nil {
			continue
		}
		// A row refreshed with an event already told of is not told again.
		if !mastodon_alert_record(s, row, answer) {
			continue
		}
		kept, _ := db_user(user, "user").row("select * from alerts where account=? and row=? and event=?", s.account, row_string(row, "id"), row_string(row, "event"))
		n := mastodon_alert(nil, s, kept)
		if kept == nil || n == nil {
			continue
		}
		payload, _ := json.Marshal(n)
		for _, st := range mastodon_streams_of(user.UID) {
			if st.session.account == s.account {
				st.send([]string{"user", "user:notification"}, "notification", string(payload))
			}
		}
		mastodon_push(s, n)
	}
}

// mastodon_heard_post tells streaming connections of a post made, changed or
// deleted in a feed the user follows.
func mastodon_heard_post(streams []*mastodon_stream, kind string, id string) {
	for _, st := range streams {
		s := st.session
		if kind == "post/delete" {
			if number := mastodon_known(s.user, "status", "post:"+id); number != "" {
				st.lock.Lock()
				var names []string
				for name := range st.streams {
					names = append(names, name)
				}
				st.lock.Unlock()
				st.send(names, "delete", number)
			}
			continue
		}
		result, err := mastodon_call(s, "mastodon/posts", Map{"kind": "ids", "ids": []any{id}})
		posts := mastodon_list(result)
		if err != nil || len(posts) != 1 {
			continue
		}
		statuses := mastodon_statuses(s, posts)
		if len(statuses) == 0 {
			continue
		}
		status := statuses[0].(map[string]any)
		payload, _ := json.Marshal(status)
		names := []string{"user", "public", "list:" + mastodon_string(posts[0], "feed")}
		for _, tag := range status["tags"].([]any) {
			if t, ok := tag.(map[string]any); ok {
				names = append(names, "hashtag:"+mastodon_string(t, "name"))
			}
		}
		event := "update"
		if kind == "post/edit" {
			event = "status.update"
		}
		st.send(names, event, string(payload))
	}
}

// mastodon_known is an item's number where it has one, without giving it
// one.
func mastodon_known(user *User, kind string, key string) string {
	row, _ := db_user(user, "user").row("select number from snowflakes where kind=? and key=?", kind, key)
	if row == nil {
		return ""
	}
	return strconv.FormatInt(row_int(row, "number"), 10)
}

// mastodon_session_for is a session for an account of a user's, as the
// interface acts outside a request.
func mastodon_session_for(user *User, row map[string]any) *mastodon_session {
	data := account_data(row)
	handle, _ := data["handle"].(string)
	domain, _ := data["domain"].(string)
	id, _ := row["id"].(string)
	if handle == "" || domain == "" || id == "" {
		return nil
	}
	return &mastodon_session{token: &Token{}, user: user, row: row, account: id, handle: handle, domain: strings.ToLower(domain), actor: account_url(domain, id)}
}
