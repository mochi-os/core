// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

// Mochi server: Delivery to other servers
//
// The queue of requests to other servers' inboxes, server-wide in networks.db so
// it survives a restart. A row names the user and account it is sent as, the
// inbox, the body and the network. The manager runs a worker for each host with
// rows due, which claims them and sends each in turn under the host policy and
// the limiter per host, signing every attempt afresh with the account's key -
// Lemmy refuses a signature over an hour old, Mastodon over twelve - and backs
// off on failure. A host that keeps failing is dead: its rows wait for one probe
// at a time, at a growing interval, so a server that stops answering costs
// nothing but queue rows, and holds up no other host's.

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// A delivery is one request to one inbox.
type delivery struct {
	ID       string `db:"id"`
	User     string `db:"user"`
	Account  string `db:"account"`
	Network  string `db:"network"`
	Inbox    string `db:"inbox"`
	Host     string `db:"host"`
	Body     string `db:"body"`
	Attempts int    `db:"attempts"`
	Next     int64  `db:"next"`
	Created  int64  `db:"created"`
	Address  string `db:"address"`
}

// delivery_batch is how many of a host's rows one claim takes;
// delivery_claim_timeout frees a claim a crashed worker left;
// delivery_attempts_maximum and delivery_age_maximum are where a row is given
// up; delivery_dead_failures is the failures in a row after which a host is
// dead, delivery_probe_maximum the longest wait between probes of one;
// delivery_poll is how often the manager looks unprompted.
// Variables so tests can lower them.
var delivery_batch = 100
var delivery_claim_timeout int64 = 600
var delivery_attempts_maximum = 30
var delivery_age_maximum int64 = 7 * 86400
var delivery_dead_failures = 5
var delivery_probe_maximum int64 = 86400
var delivery_poll = 10 * time.Second

// delivery_sent and delivery_failed count attempts since start, for the Status
// page.
var delivery_sent, delivery_failed atomic.Int64

var delivery_wake_ch = make(chan struct{}, 1)

// delivery_wake prods the manager after an enqueue.
func delivery_wake() {
	select {
	case delivery_wake_ch <- struct{}{}:
	default:
	}
}

// deliver queues one body for each inbox, as an account of a user on a network,
// leaving out the inboxes the host policy refuses, and returns how many it
// queued.
func deliver(user *User, account string, network string, inboxes []string, body []byte) int {
	db := db_open("db/networks.db")
	queued := 0
	for _, inbox := range inboxes {
		u, err := url.Parse(inbox)
		if err != nil || u.Hostname() == "" {
			continue
		}
		host := strings.ToLower(u.Hostname())
		if err := host_allowed(user, network, host); err != nil {
			debug("Delivery to %s not queued: %v", inbox, err)
			continue
		}
		db.exec("insert into deliveries (id, user, account, network, inbox, host, body, next, created) values (?, ?, ?, ?, ?, ?, ?, ?, ?)", uid(), user.UID, account, network, inbox, host, string(body), delivery_held(db, host), now())
		queued++
	}
	if queued > 0 {
		delivery_wake()
	}
	return queued
}

// deliver_unresolved queues a body for each actor whose inbox could not be
// found when it was sent - their server was over the limiter's budget, slow or
// down - to be resolved and sent by the delivery manager, under the same
// retries as any other row, rather than lost. Returns how many it queued.
func deliver_unresolved(user *User, account string, network string, addresses []string, body []byte) int {
	db := db_open("db/networks.db")
	queued := 0
	for _, address := range addresses {
		host := identifier_host(address)
		if host == "" {
			continue
		}
		if err := host_allowed(user, network, host); err != nil {
			debug("Delivery to %s not queued: %v", address, err)
			continue
		}
		db.exec("insert into deliveries (id, user, account, network, inbox, host, body, next, created, address) values (?, ?, ?, ?, '', ?, ?, ?, ?, ?)", uid(), user.UID, account, network, host, string(body), delivery_held(db, host), now(), address)
		queued++
	}
	if queued > 0 {
		delivery_wake()
	}
	return queued
}

// delivery_former is the address an account had on another server before it
// was restored here, when the delivery's activity is sent as that address.
func delivery_former(user *User, d *delivery) string {
	former, _ := account_data(account_hosted_get(user, d.Account))["former"].(string)
	if former == "" {
		return ""
	}
	var activity map[string]any
	if json.Unmarshal([]byte(d.Body), &activity) != nil || activitypub_actor_of(activity["actor"]) != former {
		return ""
	}
	return former
}

// delivery_resolve finds the inbox of a row queued by its actor's address,
// and answers it, having recorded it on the row; or answers "" with what
// became of the row: deferred while the host's budget is spent, backed off on
// any other failure, dropped when the actor has no inbox.
func delivery_resolve(db *DB, d *delivery, user *User) (string, delivery_outcome) {
	ctx := context.Background()
	if user != nil {
		ctx = activitypub_signing(ctx, user, d.Account)
	}
	r, err := author_resolve(ctx, d.Address)
	var limited *RateLimitError
	switch {
	case errors.As(err, &limited):
		delivery_defer(db, d.ID, now()+int64(limited.Retry)+1)
		return "", delivery_deferred
	case err != nil:
		debug("Delivery to %s not resolved (attempt %d): %v", d.Address, d.Attempts+1, err)
		return "", delivery_failed_attempt(db, d, err.Error())
	case r.Inbox == "":
		debug("Delivery to %s dropped: no inbox", d.Address)
		db.exec("delete from deliveries where id=?", d.ID)
		return "", delivery_dropped
	}
	db.exec("update deliveries set inbox=? where id=?", r.Inbox, d.ID)
	return r.Inbox, delivery_success
}

// delivery_manager starts a worker for each host with rows due, woken by an
// enqueue or every delivery_poll, and drops what is too old once an hour.
func delivery_manager() {
	delivery_park_all()
	go func() {
		for {
			delivery_start()
			select {
			case <-time.After(delivery_poll):
			case <-delivery_wake_ch:
			}
		}
	}()
	for range time.Tick(time.Hour) {
		delivery_cleanup()
	}
}

// delivery_workers is how many hosts are sent to at once. Each host has one
// worker, which sends its rows in turn, so a host that stops answering holds
// up only its own rows.
var delivery_workers = 64

// delivery_running is each host's worker, by host, with the channel that tells
// it more was queued; delivery_waiting says a host was refused a place since,
// so the next worker to end wakes the manager to give it one. Both guarded by
// delivery_running_lock. Only the manager looks for due hosts.
var delivery_running = map[string]chan struct{}{}
var delivery_waiting bool
var delivery_running_lock sync.Mutex

// delivery_live counts the running workers, so a test can wait for them.
var delivery_live atomic.Int64

// delivery_start runs a worker for each host with rows due, earliest first,
// while fewer than delivery_workers run. With every place taken it does not
// look: the next worker to end wakes the manager, and a running host's worker
// claims what was queued for it before it ends.
func delivery_start() {
	delivery_running_lock.Lock()
	running := len(delivery_running)
	if running >= delivery_workers {
		delivery_waiting = true
		delivery_running_lock.Unlock()
		return
	}
	delivery_running_lock.Unlock()
	for _, host := range delivery_due(delivery_workers + running) {
		if !delivery_worker_start(host) {
			return
		}
	}
}

// delivery_due_query lists hosts with rows due and not waiting for a probe,
// the earliest due first. The due rows are read by their time alone, so a dead
// host's rows, parked until its probe, are never read.
const delivery_due_query = `with due as materialized (select host, next from deliveries where next<=? and claimed<?)
	select due.host from due left join hosts h on h.host=due.host
	where h.host is null or h.next<=?
	group by due.host order by min(due.next) limit ?`

// delivery_due lists up to limit hosts with rows due.
func delivery_due(limit int) []string {
	rows, err := db_open("db/networks.db").rows(delivery_due_query, now(), now()-delivery_claim_timeout, now(), limit)
	if err != nil {
		info("delivery_due: %v", err)
		return nil
	}
	hosts := make([]string, 0, len(rows))
	for _, row := range rows {
		hosts = append(hosts, row_string(row, "host"))
	}
	return hosts
}

// delivery_worker_start runs a host's worker, or tells the running one there is
// more, and answers false when the host has none and no more may start.
func delivery_worker_start(host string) bool {
	delivery_running_lock.Lock()
	wake, running := delivery_running[host]
	if !running {
		if len(delivery_running) >= delivery_workers {
			delivery_waiting = true
			delivery_running_lock.Unlock()
			return false
		}
		wake = make(chan struct{}, 1)
		delivery_running[host] = wake
		delivery_live.Add(1)
	}
	delivery_running_lock.Unlock()
	if running {
		select {
		case wake <- struct{}{}:
		default:
		}
		return true
	}
	go delivery_worker(host, wake)
	return true
}

// delivery_worker sends a host's due rows until none is left. When the queue
// cannot be claimed from it ends at once, and the manager's poll tries again.
func delivery_worker(host string, wake chan struct{}) {
	defer delivery_live.Add(-1)
	for {
		tried := delivery_host(host)
		if tried > 0 {
			continue
		}
		if tried < 0 {
			delivery_running_lock.Lock()
			delete(delivery_running, host)
			delivery_running_lock.Unlock()
			return
		}
		if delivery_worker_done(host, wake) {
			return
		}
	}
}

// delivery_worker_done ends a host's worker where nothing was queued since it
// last looked, and answers whether it ended. A host refused a place meanwhile
// has the manager woken to give it this one.
func delivery_worker_done(host string, wake chan struct{}) bool {
	delivery_running_lock.Lock()
	select {
	case <-wake:
		delivery_running_lock.Unlock()
		return false
	default:
	}
	delete(delivery_running, host)
	waiting := delivery_waiting
	delivery_waiting = false
	delivery_running_lock.Unlock()
	if waiting {
		delivery_wake()
	}
	return true
}

// delivery_claim takes up to delivery_batch of a host's rows that are due,
// unless the host is waiting for a probe, and answers false when it cannot.
func delivery_claim(host string) ([]delivery, bool) {
	db := db_open("db/networks.db")
	var rows []delivery
	err := db.scans(&rows, `update deliveries set claimed=? where id in (
		select d.id from deliveries d left join hosts h on h.host=d.host
		where d.host=? and d.next<=? and d.claimed<? and (h.host is null or h.next<=?)
		order by d.next limit ?)
		returning id, user, account, network, inbox, host, body, attempts, next, created, address`,
		now(), host, now(), now()-delivery_claim_timeout, now(), delivery_batch)
	if err != nil {
		info("delivery_claim: %v", err)
		return nil, false
	}
	return rows, true
}

// delivery_host sends one claim of a host's rows in turn and returns how many
// it tried, or -1 when it could claim none. A dead host gets one row as a
// probe; the rest wait for the outcome, as do the rest of a host's rows once a
// failure makes it dead.
func delivery_host(host string) int {
	rows, claimed := delivery_claim(host)
	if !claimed {
		return -1
	}
	dead := delivery_host_dead(host)
	for i := range rows {
		switch delivery_attempt(&rows[i]) {
		case delivery_success:
			dead = false
		case delivery_failure, delivery_abandoned:
			if !dead {
				dead = delivery_host_dead(host)
			}
			if dead {
				for _, rest := range rows[i+1:] {
					delivery_release(rest.ID)
				}
				delivery_park(host)
				return i + 1
			}
		}
	}
	return len(rows)
}

type delivery_outcome int

const (
	delivery_success delivery_outcome = iota
	delivery_failure
	// delivery_abandoned is a failure after which the row was given up.
	delivery_abandoned
	delivery_dropped
	delivery_deferred
)

// delivery_attempt sends one row and records the outcome. The row is deleted on
// success and when it can never succeed: the account or its key is gone, the
// host is blocked, or the inbox refused it with a client error. It is deferred
// when the network is off or the host's budget for the minute is spent, and
// backed off on any other failure until its attempts or its age run out.
func delivery_attempt(d *delivery) delivery_outcome {
	db := db_open("db/networks.db")
	user := user_by_uid(d.User)
	actor, private := "", ""
	if domain, found := strings.CutPrefix(d.Account, activitypub_server_account); found {
		// The server's own account, which a forwarded report goes out as
		// (activitypub.go).
		actor, private = activitypub_server_actor(domain), activitypub_server_private()
	} else {
		if user != nil {
			actor = account_actor(user, d.Account)
		}
		if actor != "" {
			if key := keypair_get(user, d.Account, key_signing); key != nil {
				private = key.Private
			}
		}
		if private == "" {
			// A removed account's tombstone keeps its key for a while, so the
			// notice of its own deletion still goes out signed (activitypub.go).
			actor, private = tombstone_key(d.Account)
		} else if former := delivery_former(user, d); former != "" {
			// An account restored here from another server sends the Move of
			// its followers as the address it had there, whose key it kept
			// (auth_restore.go).
			actor = former
		}
	}
	if private == "" {
		debug("Delivery to %s dropped: account %s gone", d.Inbox, d.Account)
		db.exec("delete from deliveries where id=?", d.ID)
		return delivery_dropped
	}

	if d.Inbox == "" {
		inbox, outcome := delivery_resolve(db, d, user)
		if inbox == "" {
			return outcome
		}
		d.Inbox = inbox
	}

	body := []byte(d.Body)
	request, err := http.NewRequestWithContext(context.Background(), "POST", d.Inbox, bytes.NewReader(body))
	if err != nil {
		debug("Delivery to %s dropped: %v", d.Inbox, err)
		db.exec("delete from deliveries where id=?", d.ID)
		return delivery_dropped
	}
	request.Header.Set("Content-Type", "application/activity+json")
	request.Header.Set("Accept", "application/activity+json, application/ld+json")
	if err := signature_sign(request, actor+"#main-key", private, body); err != nil {
		return delivery_failed_attempt(db, d, err.Error())
	}

	response, err := network_request(context.Background(), user, d.Network, request, 0)
	var limited *RateLimitError
	var refusal *host_refusal
	switch {
	case errors.As(err, &limited):
		delivery_defer(db, d.ID, now()+int64(limited.Retry)+1)
		return delivery_deferred
	case errors.As(err, &refusal) && refusal.Off:
		delivery_defer(db, d.ID, now()+3600)
		return delivery_deferred
	case errors.As(err, &refusal):
		debug("Delivery to %s dropped: %v", d.Inbox, err)
		db.exec("delete from deliveries where id=?", d.ID)
		return delivery_dropped
	case err != nil:
		debug("Delivery to %s failed (attempt %d): %v", d.Inbox, d.Attempts+1, err)
		return delivery_failed_attempt(db, d, err.Error())
	}
	defer response.Body.Close()
	io.Copy(io.Discard, io.LimitReader(response.Body, 64<<10))
	status := response.StatusCode
	debug("Delivery to %s: %d (attempt %d)", d.Inbox, status, d.Attempts+1)

	switch {
	case status >= 200 && status < 300:
		db.exec("delete from deliveries where id=?", d.ID)
		delivery_host_alive(d.Host)
		delivery_sent.Add(1)
		return delivery_success
	case status == 408 || status == 429 || status >= 500:
		return delivery_failed_attempt(db, d, response.Status)
	case status >= 400:
		// The inbox refused this body; it will not take it later either. The
		// host answered, so it is alive.
		db.exec("delete from deliveries where id=?", d.ID)
		delivery_host_alive(d.Host)
		delivery_failed.Add(1)
		return delivery_dropped
	}
	return delivery_failed_attempt(db, d, response.Status)
}

// delivery_failed_attempt backs a row off, or gives it up when its attempts or
// its age run out, and counts the failure against its host.
func delivery_failed_attempt(db *DB, d *delivery, reason string) delivery_outcome {
	delivery_failed.Add(1)
	delivery_host_failure(d.Host, reason)
	attempts := d.Attempts + 1
	if attempts >= delivery_attempts_maximum || now()-d.Created > delivery_age_maximum {
		info("Delivery to %s given up after %d attempts: %s", d.Inbox, attempts, reason)
		db.exec("delete from deliveries where id=?", d.ID)
		return delivery_abandoned
	}
	db.exec("update deliveries set attempts=?, next=?, claimed=0 where id=?", attempts, queue_next_retry(attempts), d.ID)
	return delivery_failure
}

// delivery_defer puts a row back for a later time without counting an attempt.
func delivery_defer(db *DB, id string, next int64) {
	db.exec("update deliveries set next=?, claimed=0 where id=?", next, id)
}

// delivery_release puts a claimed row back untouched.
func delivery_release(id string) {
	db_open("db/networks.db").exec("update deliveries set claimed=0 where id=?", id)
}

// delivery_held is when a row queued now for a host is due: at once, or at
// the probe time of a dead host.
func delivery_held(db *DB, host string) int64 {
	if row, _ := db.row("select next from hosts where host=?", host); row != nil && row_int(row, "next") > now() {
		return row_int(row, "next")
	}
	return now()
}

// delivery_park holds a dead host's due rows until its probe time, as its
// claims would anyway, so the manager's look for due hosts never reads them.
func delivery_park(host string) {
	db := db_open("db/networks.db")
	if until := delivery_held(db, host); until > now() {
		db.exec("update deliveries set next=? where host=? and next<?", until, host, until)
	}
}

// delivery_park_all holds every dead host's rows until its probe time: at
// start, for rows queued before rows were parked, and hourly, for a row queued
// while its host was being found dead.
func delivery_park_all() {
	db_open("db/networks.db").exec("update deliveries set next=h.next from hosts h where h.host=deliveries.host and h.next>? and deliveries.next<h.next", now())
}

// delivery_cleanup drops rows past delivery_age_maximum, however they got
// there, and forgets hosts not heard from in a month.
func delivery_cleanup() {
	db := db_open("db/networks.db")
	db.exec("delete from deliveries where created<?", now()-delivery_age_maximum)
	db.exec("delete from hosts where success<? and failure<?", now()-30*86400, now()-30*86400)
	delivery_park_all()
	activitypub_cleanup()
}

// delivery_host_alive records that a host answered, ending any dead spell.
func delivery_host_alive(host string) {
	db_open("db/networks.db").exec("insert into hosts (host, success) values (?, ?) on conflict(host) do update set failures=0, success=excluded.success, next=0, error=''", host, now())
}

// delivery_host_failure counts a failure against a host. From
// delivery_dead_failures in a row the host is dead and its rows wait for a
// probe, at an interval that doubles from an hour to delivery_probe_maximum.
func delivery_host_failure(host string, reason string) {
	db := db_open("db/networks.db")
	failures := 1
	if row, _ := db.row("select failures from hosts where host=?", host); row != nil {
		failures = int(row["failures"].(int64)) + 1
	}
	next := int64(0)
	if failures >= delivery_dead_failures {
		wait := int64(3600) << uint(min(failures-delivery_dead_failures, 10))
		if wait > delivery_probe_maximum {
			wait = delivery_probe_maximum
		}
		next = now() + wait
	}
	db.exec("insert into hosts (host, failures, failure, next, error) values (?, ?, ?, ?, ?) on conflict(host) do update set failures=excluded.failures, failure=excluded.failure, next=excluded.next, error=excluded.error", host, failures, now(), next, reason)
}

// delivery_host_dead says whether a host is waiting for probes.
func delivery_host_dead(host string) bool {
	row, _ := db_open("db/networks.db").row("select failures from hosts where host=?", host)
	return row != nil && int(row["failures"].(int64)) >= delivery_dead_failures
}
