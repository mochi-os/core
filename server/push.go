// Mochi server: the push queue. A notify with queue=True sends each push from
// another goroutine, or leaves it as a row when every place is taken; a push
// that fails, inline or queued, is retried from the same rows.
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

	sl "go.starlark.net/starlark"
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
		return account_deliver_browser(p.Data, p.Title, p.Body, p.Link, p.tag())
	},
	"email": func(p *Push) (bool, bool) {
		return account_deliver_email(p.Identifier, p.Title, p.Body, p.Link), false
	},
	"pushbullet": func(p *Push) (bool, bool) {
		token, _ := p.Data["token"].(string)
		return account_deliver_pushbullet(token, p.Title, p.Body, p.Link), false
	},
	"matrix": func(p *Push) (bool, bool) {
		return notices_send(p.User, p.Account, p.Identifier, p.Title, p.Body, p.Link), false
	},
	"xmpp": func(p *Push) (bool, bool) {
		return notices_xmpp(p.User, p.Identifier, p.Title, p.Body, p.Link), false
	},
	"ntfy": func(p *Push) (bool, bool) {
		server, _ := p.Data["server"].(string)
		topic, _ := p.Data["topic"].(string)
		token, _ := p.Data["token"].(string)
		return account_deliver_ntfy(server, topic, token, p.Title, p.Body, p.Link), false
	},
	"unifiedpush": func(p *Push) (bool, bool) {
		return account_deliver_unifiedpush(p.User, p.Account, p.Data, p.Title, p.Body, p.Link, p.tag(), p.App, p.Event)
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

// push_configured reports whether this server can deliver to a provider at
// all. FCM needs a service account only the operator can supply, so a push to
// an FCM destination on a server without one is not a transient failure:
// retrying it five times over a quarter of an hour and then mailing the
// administrator reports nothing the operator did not already decide. The
// account row stays, and delivers as soon as the setting is filled.
func push_configured(kind string) bool {
	if kind == "fcm" {
		return setting_effective("fcm.service_account") != ""
	}
	return true
}

// account_deliver sends one push to one account. handled is false for a
// provider the table does not know, or one this server has not configured;
// the caller treats either as no attempt rather than a failure to retry.
// retire is true only when the destination is permanently dead.
func account_deliver(p *Push) (success, retire, handled bool) {
	deliver := push_deliverers[p.Type]
	if deliver == nil {
		return false, false, false
	}
	if !push_configured(p.Type) {
		debug("Push to %s destination %q skipped: provider not configured on this server", p.Type, p.Account)
		return false, false, false
	}
	success, retire = deliver(p)
	return success, retire, true
}

// push_queue_insert writes p as a row tried attempts times and due at due, and
// answers its id, or "" when the row could not be written.
func push_queue_insert(p *Push, attempts int, due int64) string {
	if p.User == nil || p.Account == "" {
		return ""
	}
	data, err := json.Marshal(p.Data)
	if err != nil {
		debug("Push not queuing %q: payload will not encode: %v", p.Account, err)
		return ""
	}
	db := db_open("db/queue.db")
	if db == nil {
		info("Push not queuing %q: %v", p.Account, error_queue_unavailable)
		return ""
	}
	id := uid()
	if err := push_write(db, `insert into pushes (id, user, account, type, identifier, data, app, category,
			object, title, body, link, event, attempts, next_retry, created)
		values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		id, p.User.UID, p.Account, p.Type, p.Identifier, string(data),
		p.App, p.Category, p.Object, p.Title, p.Body, p.Link, p.Event,
		attempts, due, now()); err != nil {
		info("Push not queuing %q: %v", p.Account, err)
		return ""
	}
	return id
}

// Places: every push sent from the queue holds one while its destination
// answers. Serial sending would let one unreachable destination hold every
// other push for its timeout, and an unbounded number would let a burst of
// notifications open a connection each. push_senders new pushes and
// push_retry_senders retries are in flight at once across the server, so
// destinations that keep failing never hold the places a new notification is
// sent from; a user has at most push_user_senders in flight, so one user's slow
// destinations never hold everyone's; and an account has one, so its pushes
// arrive in the order they were raised and a provider's first send (a Matrix
// notices room, made then) is never made twice at once. push_queue_batch is
// how many due rows of each kind one pass reads.
const (
	push_queue_batch   = 100
	push_senders       = 32
	push_retry_senders = 8
	push_user_senders  = 4
)

type push_places struct {
	lock     sync.Mutex
	fresh    int
	retry    int
	users    map[string]int
	accounts map[string]bool
	// backlog records that a push was left as a due row for want of a place,
	// so the next place to come free wakes the manager to send it.
	backlog bool
}

var push_flight = &push_places{users: map[string]int{}, accounts: map[string]bool{}}

// free reports whether a push of user's to account could be sent now. The lock
// is held.
func (places *push_places) free(user, account string, retry bool) bool {
	if places.accounts[user+"/"+account] || places.users[user] >= push_user_senders {
		return false
	}
	if retry {
		return places.retry < push_retry_senders
	}
	return places.fresh < push_senders
}

// room reports whether a place of the kind could be free for anyone, and if
// not records that a push is waiting, so a pass with every place taken reads
// no rows.
func (places *push_places) room(retry bool) bool {
	places.lock.Lock()
	defer places.lock.Unlock()
	full := places.fresh >= push_senders
	if retry {
		full = places.retry >= push_retry_senders
	}
	if full {
		places.backlog = true
	}
	return !full
}

// take holds a place for a push of user's to account if one is free, without
// waiting, and otherwise records that a push is waiting.
func (places *push_places) take(user, account string, retry bool) bool {
	places.lock.Lock()
	defer places.lock.Unlock()
	if !places.free(user, account, retry) {
		places.backlog = true
		return false
	}
	places.accounts[user+"/"+account] = true
	places.users[user]++
	if retry {
		places.retry++
	} else {
		places.fresh++
	}
	return true
}

// release gives a place back. With wake, it also wakes the manager if a push is
// waiting for one; a pass giving back the places of rows it will not send does
// not, since that would only start the same pass again.
func (places *push_places) release(user, account string, retry, wake bool) {
	places.lock.Lock()
	delete(places.accounts, user+"/"+account)
	places.users[user]--
	if places.users[user] <= 0 {
		delete(places.users, user)
	}
	if retry {
		places.retry--
	} else {
		places.fresh--
	}
	waiting := wake && places.backlog
	if waiting {
		places.backlog = false
	}
	places.lock.Unlock()
	if waiting {
		push_wake_manager()
	}
}

// wait records that a push has been left as a due row: the manager is woken
// now if a place is already free for it, and otherwise by the next place to
// come free.
func (places *push_places) wait(user, account string, retry bool) {
	places.lock.Lock()
	free := places.free(user, account, retry)
	if !free {
		places.backlog = true
	}
	places.lock.Unlock()
	if free {
		push_wake_manager()
	}
}

// push_wake tells the manager that a row is due now.
var push_wake = make(chan struct{}, 1)

func push_wake_manager() {
	select {
	case push_wake <- struct{}{}:
	default:
	}
}

// push_sending counts the pushes in flight, so a test can wait for them.
var push_sending sync.WaitGroup

// push_dispatch sends a queued notify's push without holding its caller. With a
// place free it goes from a goroutine at once, and is written to the queue only
// if it fails, as an inline push is. With none it is written due now, and the
// manager sends it when a place comes free. A row queue.db will not take is
// sent by the caller, as an unqueued notify is.
func push_dispatch(p *Push) {
	if push_flight.take(p.User.UID, p.Account, false) {
		push_start("", 0, p)
		return
	}
	if push_queue_insert(p, 0, now()) == "" {
		success, retire, handled := push_deliver(p)
		push_settle("", 0, p, success, retire, handled)
		return
	}
	push_flight.wait(p.User.UID, p.Account, false)
}

// push_start sends p, the push of row id (or of no row) tried attempts times
// before, from a goroutine holding its place. The place goes back as soon as
// the destination has answered, before the outcome is written.
func push_start(id string, attempts int, p *Push) {
	push_sending.Add(1)
	go func() {
		defer push_sending.Done()
		success, retire, handled := push_deliver(p)
		push_flight.release(p.User.UID, p.Account, attempts > 0, true)
		guard("push settle", nil, func() {
			push_settle(id, attempts, p, success, retire, handled)
		})
	}()
}

// push_deliver is account_deliver for a push sent by the queue. A provider that
// panics costs the push an attempt, retried like any transient failure;
// unguarded, it would end the process.
func push_deliver(p *Push) (success, retire, handled bool) {
	guard("push delivery", func() { handled = true }, func() {
		success, retire, handled = account_deliver(p)
	})
	return success, retire, handled
}

// push_settle records an attempt's outcome. Row id goes once the push lands,
// its destination is dead or unknown, or it has run out of attempts, and is
// otherwise due again further up the ladder; a push sent with no row is given
// one at its first failure. A delivery updates the account's last_delivered,
// and a dead destination's account is deleted.
func push_settle(id string, attempts int, p *Push, success, retire, handled bool) {
	write := func(query string, values ...any) {
		if id == "" {
			return
		}
		db := db_open("db/queue.db")
		if db == nil {
			return
		}
		// A refused write leaves the row claimed: it is sent again only once
		// the claim runs out.
		if err := push_write(db, query, values...); err != nil {
			info("Push queue write failed: %v", err)
		}
	}
	switch {
	case success:
		write("delete from pushes where id=?", id)
		db_user_brief(p.User, "user").exec_bg("push delivered", "update accounts set last_delivered=? where id=?", now(), p.Account)
	case retire || !handled:
		// Permanently dead, or a provider this build does not know or this
		// server has not configured. Retrying any of them forever is how a
		// queue becomes a leak.
		write("delete from pushes where id=?", id)
		if retire {
			db_user_brief(p.User, "user").exec_bg("push retire", "delete from accounts where id=?", p.Account)
		}
	case attempts+1 >= push_attempts_maximum:
		// The notification is lost, and this is the one moment in the path
		// worth an operator's attention: every failure that reaches here was
		// transient by construction, and this is where that stops being true.
		// The email throttle keys on the format, so a provider outage costs one
		// mail rather than one per push.
		warn("Push giving up on %s destination %q after %d attempts",
			p.Type, p.Account, attempts+1)
		write("delete from pushes where id=?", id)
	case id == "":
		push_queue_insert(p, attempts+1, queue_next_retry(attempts))
	default:
		write("update pushes set attempts=?, next_retry=? where id=?",
			attempts+1, queue_next_retry(attempts), id)
	}
}

// push_claim is how far a pass moves the retry time of the rows it sends on, in
// seconds: past any send's time, so a row whose outcome cannot be recorded is
// sent again only once this has passed, not on the next pass.
const push_claim = 15 * 60

// push_queue_expire drops the rows that have not landed within push_expiry: a
// notification nobody has received in six hours is not worth a phone buzzing
// now. The manager runs it on its tick, not on every pass, as it reads the
// whole table.
func push_queue_expire() {
	db := db_open("db/queue.db")
	if db == nil {
		return
	}
	if err := push_write(db, "delete from pushes where created<?", now()-push_expiry); err != nil {
		info("Push queue write failed: %v", err)
	}
}

// push_lookup finds a row's user. A var so a test can make it fail.
var push_lookup = user_by_uid

// push_queue_process makes one pass over the due rows, starts a send for each
// that finds a place, and reports how many rows it acted on, so the manager can
// loop straight back in. It never waits for a send: each records its own
// outcome, and a row left for want of a place is sent when one comes free.
func push_queue_process() int {
	db := db_open("db/queue.db")
	if db == nil {
		return 0
	}
	// The first write queue.db refuses ends the pass, and with 0, so the
	// manager waits for its next tick rather than looping straight back onto
	// rows it could not settle.
	write := func(query string, values ...any) bool {
		if err := push_write(db, query, values...); err != nil {
			info("Push queue write failed: %v", err)
			return false
		}
		return true
	}

	// New pushes left for want of a place, and retries, are read apart, each
	// in the order it came due, so neither keeps the other out of the pass. A
	// row past its expiry is left for push_queue_expire.
	var rows []map[string]any
	for _, retry := range []bool{false, true} {
		if !push_flight.room(retry) {
			continue
		}
		kind := "attempts=0"
		if retry {
			kind = "attempts>0"
		}
		part, err := db.rows("select * from pushes where "+kind+" and next_retry<=? and created>=? order by next_retry limit ?", now(), now()-push_expiry, push_queue_batch)
		if err != nil {
			return 0
		}
		rows = append(rows, part...)
	}

	type attempt struct {
		id       string
		attempts int
		user     string
		account  string
		push     *Push
	}

	acted := 0
	var taken []*attempt
	// A place held for a row whose send never starts goes back however the
	// pass ends, a panic in a lookup included. Each row is added the moment
	// its place is taken.
	started := false
	defer func() {
		if !started {
			for _, a := range taken {
				push_flight.release(a.user, a.account, a.attempts > 0, false)
			}
		}
	}()

	for _, row := range rows {
		a := &attempt{
			id:       row_string(row, "id"),
			attempts: int(row_int(row, "attempts")),
			user:     row_string(row, "user"),
			account:  row_string(row, "account"),
		}
		// Taken before the row is looked at further, so a row without a place
		// costs no user's database a read.
		if !push_flight.take(a.user, a.account, a.attempts > 0) {
			continue
		}
		taken = append(taken, a)

		user := push_lookup(a.user)
		// The account must still exist and still be verified: a user who
		// removed a destination should not have it retried at them, and one
		// that went away with its user has nothing to deliver to.
		have := false
		if user != nil {
			have, _ = db_user_brief(user, "user").exists("select 1 from accounts where id=? and verified > 0", a.account)
		}
		if !have {
			taken = taken[:len(taken)-1]
			push_flight.release(a.user, a.account, a.attempts > 0, false)
			if !write("delete from pushes where id=?", a.id) {
				return 0
			}
			acted++
			continue
		}

		var data map[string]any
		if raw := row_string(row, "data"); raw != "" {
			json.Unmarshal([]byte(raw), &data)
		}
		a.push = &Push{
			User: user, Account: a.account, Type: row_string(row, "type"),
			Identifier: row_string(row, "identifier"), Data: data,
			App: row_string(row, "app"), Category: row_string(row, "category"),
			Object: row_string(row, "object"), Title: row_string(row, "title"),
			Body: row_string(row, "body"), Link: row_string(row, "link"),
			Event: row_string(row, "event"),
		}
	}
	if len(taken) == 0 {
		return acted
	}

	// Claimed before anything is sent: a send whose outcome cannot be recorded
	// leaves its row not due again until the claim runs out, rather than on
	// the next pass. One statement's text for any batch size, as the cache
	// keeps a statement per text.
	ids := make([]string, len(taken))
	for i, a := range taken {
		ids[i] = a.id
	}
	claimed, _ := json.Marshal(ids)
	if !write("update pushes set next_retry=? where id in (select value from json_each(?))", now()+push_claim, string(claimed)) {
		return 0
	}
	started = true
	for _, a := range taken {
		push_start(a.id, a.attempts, a.push)
	}
	return acted + len(taken)
}

// push_write is how the queue writes to queue.db. A var so tests can refuse it.
var push_write = func(db *DB, query string, values ...any) error {
	return db.exec_e(query, values...)
}

// push_manager sends queued pushes as they come due: every 30 seconds, when it
// also drops expired rows, and at once when a push left for want of a place
// can have one. Separate from queue_manager because the two have unrelated
// cadences: a P2P message is worth chasing for days, a push for minutes.
func push_manager() {
	tick := time.NewTicker(30 * time.Second)
	defer tick.Stop()
	for {
		select {
		case <-tick.C:
			guard("push expiry", nil, push_queue_expire)
		case <-push_wake:
		}
		guard("push queue", nil, func() {
			for push_queue_process() > 0 {
			}
		})
	}
}

// Sync signals: a push with no content, telling the user's own devices that
// an app's data changed so the Mochi app runs that sync now rather than at its
// next scheduled one. A burst - a calendar poll writing fifty events - is one
// signal at once and at most one more when the window closes, so the last
// change always reaches the device.

// account_sync_kinds are the syncs the Mochi app runs on a signal.
var account_sync_kinds = map[string]bool{"calendars": true, "contacts": true}

// account_sync_window is the shortest gap between two signals of one kind to
// one user.
var account_sync_window = 10 * time.Second

// account_sync_deliver sends one signal; tests replace it.
var account_sync_deliver = account_sync_send

type account_sync_state struct {
	last  time.Time
	timer *time.Timer
}

var (
	account_sync_lock   sync.Mutex
	account_sync_states = map[string]*account_sync_state{}
)

// mochi.account.sync(kind) -> None: Tell the user's devices that their data
// of this kind changed, so the Mochi app syncs it now: "calendars" or
// "contacts". The push carries nothing but the kind, reaches only the Mochi
// app's own push accounts, and a burst of calls is merged into at most one
// push when it starts and one when it ends.
func api_account_sync(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var kind string
	if err := sl.UnpackArgs(fn.Name(), args, kwargs, "kind", &kind); err != nil {
		return nil, err
	}
	if !account_sync_kinds[kind] {
		return sl_error(fn, "unknown kind %q", kind)
	}
	if user := principal_caller(t); user != nil {
		account_sync_signal(user, kind)
		if kind == "contacts" {
			go xmpp_friends_changed(user)
		}
	}
	return sl.None, nil
}

// account_sync_signal sends a signal now, or, inside the window of the last
// one, once when the window closes.
func account_sync_signal(user *User, kind string) {
	key := user.UID + ":" + kind
	account_sync_lock.Lock()
	defer account_sync_lock.Unlock()
	state := account_sync_states[key]
	if state == nil {
		state = &account_sync_state{}
		account_sync_states[key] = state
	}
	if state.timer != nil {
		return
	}
	wait := account_sync_window - time.Since(state.last)
	if wait <= 0 {
		state.last = time.Now()
		go account_sync_deliver(user, kind)
		return
	}
	state.timer = time.AfterFunc(wait, func() {
		account_sync_lock.Lock()
		state.timer = nil
		state.last = time.Now()
		account_sync_lock.Unlock()
		account_sync_deliver(user, kind)
	})
}

// account_sync_send pushes {"sync": kind} to each of the user's verified FCM
// and UnifiedPush accounts: the Mochi app's own. A signal that fails is not
// retried; the device's scheduled sync catches up.
func account_sync_send(user *User, kind string) {
	rows, err := db_user_brief(user, "user").rows("select id, type, data from accounts where verified>0 and type in ('fcm', 'unifiedpush')")
	if err != nil {
		return
	}
	fields := map[string]string{"sync": kind}
	debug("sync: %s signal to %d device account(s) of user %s", kind, len(rows), user.UID)
	for _, row := range rows {
		account, _ := row["id"].(string)
		provider, _ := row["type"].(string)
		raw, _ := row["data"].(string)
		var data map[string]any
		if raw != "" {
			json.Unmarshal([]byte(raw), &data)
		}
		switch provider {
		case "fcm":
			if token, _ := data["token"].(string); token != "" {
				fcm_send(token, fields)
			}
		case "unifiedpush":
			unifiedpush_send(user, account, data, fields)
		}
	}
}
