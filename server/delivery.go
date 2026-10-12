// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

// Mochi server: Delivery to other servers
//
// The queue of requests to other servers' inboxes, server-wide in networks.db so
// it survives a restart. A row names the user and account it is sent as, the
// inbox, the body and the network. The manager claims the rows that are due and
// sends each under the host policy and the limiter per host, signing every
// attempt afresh with the account's key - Lemmy refuses a signature over an
// hour old, Mastodon over twelve - and backs off on failure. A host that keeps
// failing is dead: its rows wait for one probe at a time, at a growing interval,
// so a server that stops answering costs nothing but queue rows.

package main

import (
	"bytes"
	"context"
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
}

// delivery_batch is how many rows one pass claims; delivery_claim_timeout frees
// a claim a crashed pass left; delivery_attempts_maximum and delivery_age_maximum
// are where a row is given up; delivery_dead_failures is the failures in a row
// after which a host is dead, delivery_probe_maximum the longest wait between
// probes of one; delivery_poll is how often the manager looks unprompted.
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
		db.exec("insert into deliveries (id, user, account, network, inbox, host, body, next, created) values (?, ?, ?, ?, ?, ?, ?, ?, ?)", uid(), user.UID, account, network, inbox, host, string(body), now(), now())
		queued++
	}
	if queued > 0 {
		delivery_wake()
	}
	return queued
}

// delivery_manager sends what is due, woken by an enqueue or every
// delivery_poll, and drops what is too old once an hour.
func delivery_manager() {
	go func() {
		for {
			if delivery_process() > 0 {
				continue
			}
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

// delivery_claim takes up to delivery_batch rows that are due, on hosts not
// waiting for a probe, and returns them grouped by host.
func delivery_claim() map[string][]delivery {
	db := db_open("db/networks.db")
	var rows []delivery
	err := db.scans(&rows, `update deliveries set claimed=? where id in (
		select d.id from deliveries d left join hosts h on h.host=d.host
		where d.next<=? and d.claimed<? and (h.host is null or h.next<=?)
		order by d.next limit ?)
		returning id, user, account, network, inbox, host, body, attempts, next, created`,
		now(), now(), now()-delivery_claim_timeout, now(), delivery_batch)
	if err != nil {
		info("delivery_claim: %v", err)
		return nil
	}
	hosts := map[string][]delivery{}
	for _, row := range rows {
		hosts[row.Host] = append(hosts[row.Host], row)
	}
	return hosts
}

// delivery_process sends one claimed batch, hosts in parallel and each host's
// rows in turn, and returns how many rows it acted on. A dead host gets one row
// as a probe; the rest wait for the outcome.
func delivery_process() int {
	hosts := delivery_claim()
	var group sync.WaitGroup
	var acted atomic.Int64
	for host, rows := range hosts {
		group.Add(1)
		go func(host string, rows []delivery) {
			defer group.Done()
			dead := delivery_host_dead(host)
			for i := range rows {
				outcome := delivery_attempt(&rows[i])
				acted.Add(1)
				if outcome == delivery_failure && dead {
					for _, rest := range rows[i+1:] {
						delivery_release(rest.ID)
					}
					return
				}
				if outcome == delivery_success {
					dead = false
				}
			}
		}(host, rows)
	}
	group.Wait()
	return int(acted.Load())
}

type delivery_outcome int

const (
	delivery_success delivery_outcome = iota
	delivery_failure
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
	actor := ""
	if user != nil {
		actor = account_actor(user, d.Account)
	}
	var key *keypair
	if actor != "" {
		key = keypair_get(user, d.Account, key_signing)
	}
	if key == nil {
		debug("Delivery to %s dropped: account %s gone", d.Inbox, d.Account)
		db.exec("delete from deliveries where id=?", d.ID)
		return delivery_dropped
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
	if err := signature_sign(request, actor+"#main-key", key.Private, body); err != nil {
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
		return delivery_dropped
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

// delivery_cleanup drops rows past delivery_age_maximum, however they got
// there, and forgets hosts not heard from in a month.
func delivery_cleanup() {
	db := db_open("db/networks.db")
	db.exec("delete from deliveries where created<?", now()-delivery_age_maximum)
	db.exec("delete from hosts where success<? and failure<?", now()-30*86400, now()-30*86400)
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
