// Mochi server: retry for pushes whose destination was momentarily unreachable.
//
// Same shape as queue.go's P2P retry - durable row, exponential ladder with
// jitter, bounded attempts - plus an expiry: a row that has not landed within
// push_expiry is dropped rather than buzzing a phone about stale news.
//
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the Mochi
// Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"encoding/json"
	"sync"
	"time"
)

const (
	// push_attempts_maximum counts total tries across the queue, not retries.
	// The ladder below reaches ~30 minutes by the fifth, which is already past
	// the point a notification is worth delivering.
	push_attempts_maximum = 5

	// push_expiry drops a row that has not landed in this long, however many
	// attempts remain. A push is about something that just happened.
	push_expiry = 6 * 3600
)

// Push is one notification aimed at one account: everything the delivery
// switch needs, and therefore everything a queued row has to carry.
type Push struct {
	User       *User
	Account    string
	Type       string
	Identifier string
	Data       map[string]any
	App        string
	Category   string
	Object     string
	Title      string
	Body       string
	Link       string
	Event      string
}

// tag is the collapse key the client groups notifications by.
func (p *Push) tag() string { return p.App + "-" + p.Category + "-" + p.Object }

// push_deliverers maps provider type to its delivery function. Both the live
// path (api_account_notify) and the retry path below go through it, so a
// provider is wired once; push_test asserts every notify provider has an entry.
var push_deliverers = map[string]func(*Push) (success, retire bool){
	"browser": func(p *Push) (bool, bool) {
		return account_deliver_browser(p.Data, p.Title, p.Body, p.Link, p.tag()), false
	},
	"email": func(p *Push) (bool, bool) {
		return account_deliver_email(p.Identifier, p.Title, p.Body, p.Link), false
	},
	"pushbullet": func(p *Push) (bool, bool) {
		token, _ := p.Data["token"].(string)
		return account_deliver_pushbullet(token, p.Title, p.Body, p.Link), false
	},
	"ntfy": func(p *Push) (bool, bool) {
		server, _ := p.Data["server"].(string)
		topic, _ := p.Data["topic"].(string)
		token, _ := p.Data["token"].(string)
		return account_deliver_ntfy(server, topic, token, p.Title, p.Body, p.Link), false
	},
	"unifiedpush": func(p *Push) (bool, bool) {
		return account_deliver_unifiedpush(p.User, p.Account, p.Data, p.Title, p.Body, p.Link, p.tag(), p.App, p.Event), false
	},
	"fcm": func(p *Push) (bool, bool) {
		success, retire, _ := account_deliver_fcm(p.Data, p.Title, p.Body, p.Link, p.tag(), p.App, p.Event)
		return success, retire
	},
	"url": func(p *Push) (bool, bool) {
		secret, _ := p.Data["secret"].(string)
		return account_deliver_url(p.Identifier, secret, p.App, p.Category, p.Object, p.Title, p.Body, p.Link), false
	},
}

// account_deliver sends one push to one account. handled is false for a
// provider the table does not know, which the caller treats as no attempt
// rather than a failure to retry; retire is true only when the destination is
// permanently dead.
func account_deliver(p *Push) (success, retire, handled bool) {
	deliver := push_deliverers[p.Type]
	if deliver == nil {
		return false, false, false
	}
	success, retire = deliver(p)
	return success, retire, true
}

// push_queue_add records a failed push for a later attempt. Called only for a
// transient failure: a retired destination is deleted by the caller instead,
// and an unknown provider never reaches here.
func push_queue_add(p *Push) {
	if p.User == nil || p.Account == "" {
		return
	}
	data, err := json.Marshal(p.Data)
	if err != nil {
		debug("Push not queuing %q: payload will not encode: %v", p.Account, err)
		return
	}
	db := db_open("db/queue.db")
	db.exec(`insert into pushes (id, user, account, type, identifier, data, app, category,
			object, title, body, link, event, attempts, next_retry, created)
		values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?)`,
		uid(), p.User.UID, p.Account, p.Type, p.Identifier, string(data),
		p.App, p.Category, p.Object, p.Title, p.Body, p.Link, p.Event,
		queue_next_retry(0), now())
}

// push_queue_batch is how many due rows one pass takes, push_queue_workers how
// many are in flight at once. Serial sending would let one unreachable
// destination hold the whole queue for its timeout.
const (
	push_queue_batch   = 100
	push_queue_workers = 8
)

// push_queue_process makes one pass over the due rows and reports how many it
// acted on, so the manager can loop straight back in. Read, send concurrently,
// then apply outcomes: every database write stays on this goroutine.
func push_queue_process() int {
	db := db_open("db/queue.db")

	// Expired rows go first: a notification nobody has received in six hours
	// is not worth a phone buzzing now, and dropping them before the due query
	// keeps a dead destination from occupying the batch every tick.
	db.exec("delete from pushes where created < ?", now()-push_expiry)

	rows, err := db.rows("select * from pushes where next_retry <= ? order by next_retry limit ?", now(), push_queue_batch)
	if err != nil || len(rows) == 0 {
		return 0
	}

	type attempt struct {
		id       string
		attempts int
		user     *User
		push     *Push
		success  bool
		retire   bool
		handled  bool
	}

	acted := 0
	var sending []*attempt

	for _, row := range rows {
		id := row_string(row, "id")

		user := user_by_uid(row_string(row, "user"))
		if user == nil {
			// The account went away with its user. Nothing to deliver to.
			db.exec("delete from pushes where id=?", id)
			acted++
			continue
		}

		account := row_string(row, "account")

		// The account must still exist and still be verified: a user who
		// removed a destination should not have it retried at them.
		if have, _ := db_user(user, "user").exists("select 1 from accounts where id=? and verified > 0", account); !have {
			db.exec("delete from pushes where id=?", id)
			acted++
			continue
		}

		var data map[string]any
		if raw := row_string(row, "data"); raw != "" {
			json.Unmarshal([]byte(raw), &data)
		}
		sending = append(sending, &attempt{
			id:       id,
			attempts: int(row_int(row, "attempts")),
			user:     user,
			push: &Push{
				User: user, Account: account, Type: row_string(row, "type"),
				Identifier: row_string(row, "identifier"), Data: data,
				App: row_string(row, "app"), Category: row_string(row, "category"),
				Object: row_string(row, "object"), Title: row_string(row, "title"),
				Body: row_string(row, "body"), Link: row_string(row, "link"),
				Event: row_string(row, "event"),
			},
		})
	}

	var group sync.WaitGroup
	slots := make(chan struct{}, push_queue_workers)
	for _, a := range sending {
		group.Add(1)
		go func(a *attempt) {
			defer group.Done()
			slots <- struct{}{}
			defer func() { <-slots }()
			a.success, a.retire, a.handled = account_deliver(a.push)
		}(a)
	}
	group.Wait()

	for _, a := range sending {
		acted++
		switch {
		case a.success:
			db.exec("delete from pushes where id=?", a.id)
			db_user(a.user, "user").exec("update accounts set last_delivered=? where id=?", now(), a.push.Account)
		case a.retire || !a.handled:
			// Permanently dead, or a provider this build no longer knows.
			// Retrying either forever is how a queue becomes a leak.
			db.exec("delete from pushes where id=?", a.id)
			if a.retire {
				db_user(a.user, "user").exec("delete from accounts where id=?", a.push.Account)
			}
		case a.attempts+1 >= push_attempts_maximum:
			debug("Push giving up on %q after %d attempts", a.push.Account, a.attempts+1)
			db.exec("delete from pushes where id=?", a.id)
		default:
			db.exec("update pushes set attempts=?, next_retry=? where id=?",
				a.attempts+1, queue_next_retry(a.attempts), a.id)
		}
	}
	return acted
}

// push_manager retries queued pushes. Separate from queue_manager because the
// two have unrelated cadences: a P2P message is worth chasing for days, a push
// for minutes.
func push_manager() {
	for range time.Tick(30 * time.Second) {
		for push_queue_process() > 0 {
		}
	}
}
