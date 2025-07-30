// Comms server: Events
// Copyright Alistair Cunningham 2024-2025

package main

import (
	"crypto/ed25519"
	"time"
)

type Event struct {
	ID        string `json:"id"`
	From      string `json:"from"`
	To        string `json:"to"`
	Service   string `json:"service"`
	Action    string `json:"action"`
	Content   string `json:"content"`
	Signature string `json:"signature"`
	source    string `json:"-"`
	user      *User  `json:"-"`
	db        *DB    `json:"-"`
}

type BroadcastQueue struct {
	ID      string
	Topic   string
	Content string
	Updated int
}

type EventsQueue struct {
	ID       string
	Method   string
	Location string
	Event    string
	Updated  int
}

func events_check_queue(method string, location string) {
	var queue []EventsQueue
	db := db_open("db/queue.db")
	db.scans(&queue, "select * from events where method=? and location=?", method, location)
	for _, q := range queue {
		log_debug("Trying to send queued event '%s' to %s '%s'", q.Event, q.Method, q.Location)
		success := false

		if q.Method == "peer" {
			if peer_send(q.Location, q.Event) {
				success = true
			}

		} else if q.Method == "entity" {
			method, location, _, _ := entity_location(location)
			if method == "libp2p" && libp2p_send(location, q.Event) {
				success = true
			}
		}

		if success {
			log_debug("Queue event sent")
			db := db_open("db/queue.db")
			db.exec("delete from events where id=?", q.ID)
		} else {
			log_debug("Still unable to send queued event; keeping in queue")
		}
	}
}

func events_manager() {
	db := db_open("db/queue.db")

	for {
		time.Sleep(time.Minute)
		if len(peers_connected) >= peers_minimum {
			var eq EventsQueue
			if db.scan(&eq, "select * from events limit 1 offset abs(random()) % max((select count(*) from events), 1)") {
				log_debug("Events queue helper nudging events to %s '%s'", eq.Method, eq.Location)
				events_check_queue(eq.Method, eq.Location)
			}

			var broadcasts []BroadcastQueue
			db.scans(&broadcasts, "select * from broadcast")
			for _, b := range broadcasts {
				log_debug("Broadcast queue helper sending event '%s'", b.ID)
				libp2p_topics[b.Topic].Publish(libp2p_context, []byte(b.Content))
				db.exec("delete from broadcast where id=?", b.ID)
			}
		}
	}
}

func event_receive_json(event string, source string) {
	var e Event
	if !json_decode(&e, event) {
		log_info("Dropping event with malformed JSON: '%s'", event)
		return
	}
	e.source = source
	e.receive()
}

func (e *Event) receive() {
	if !valid(e.ID, "id") {
		log_info("Dropping received event due to invalid 'id' field '%s'", e.ID)
		return
	}

	if e.From != "" {
		if !valid(e.From, "entity") {
			log_info("Dropping received event due to invalid 'from' field '%s'", e.From)
			return
		}

		if e.source != "" {
			public := base58_decode(e.From, "")
			if len(public) != ed25519.PublicKeySize {
				log_info("Dropping received event due to invalid from length %d!=%d", len(public), ed25519.PublicKeySize)
				return
			}
			if !ed25519.Verify(public, []byte(e.ID+e.From+e.To+e.Service+e.Action+e.Content), base58_decode(e.Signature, "")) {
				log_info("Dropping received event due to invalid sender signature")
				return
			}
		}
	}

	e.user = user_owning_entity(e.To)

	a := services[e.Service]
	if a == nil {
		log_info("Dropping received event due to unknown service '%s'", e.Service)
		return
	}

	//TODO
	if a.db_file != "" {
		e.db = db_user(e.user, a.db_file, a.db_create)
		defer e.db.close()
	}

	var f func(*Event)
	var found bool
	// Look for app event matching action
	if e.To == "" {
		f, found = a.events_broadcast[e.Action]
	} else {
		f, found = a.events[e.Action]
	}
	if !found {
		// Look for app default event
		if e.To == "" {
			f, found = a.events_broadcast[""]
		} else {
			f, found = a.events[""]
		}
	}
	if !found {
		log_info("Dropping received event due to unknown event '%s' in app '%s' for service '%s'", e.Action, a.name, e.Service)
		return
	}

	f(e)
}

func (e *Event) send() {
	if e.ID == "" {
		log_warn("Event did not specify ID; adding one")
		e.ID = uid()
	}

	method, location, queue_method, queue_location := entity_location(e.To)
	log_debug("Sending event '%#v' to %s '%s'", e, method, location)

	if method == "local" {
		go e.receive()
		return
	}

	e.sign()
	j := json_encode(e)

	if method == "libp2p" && libp2p_send(location, j) {
		return
	}

	log_debug("Unable to send event to '%s', adding to queue", e.To)
	db := db_open("db/queue.db")
	db.exec("replace into events ( id, method, location, event, updated ) values ( ?, ?, ?, ?, ? )", e.ID, queue_method, queue_location, j, now())
}

func (e *Event) sign() {
	if e.From == "" {
		return
	}

	if e.ID == "" {
		panic("Event did not specify ID")
	}

	db := db_open("db/users.db")
	var from Entity
	if !db.scan(&from, "select private from entities where id=?", e.From) {
		log_warn("Not signing event due unknown sending entity")
		return
	}
	private := base58_decode(from.Private, "")
	if string(private) == "" {
		log_warn("Not signing event due to invalid private key")
		return
	}
	e.Signature = base58_encode(ed25519.Sign(private, []byte(e.ID+e.From+e.To+e.Service+e.Action+e.Content)))
}

func (a *App) event(event string, f func(*Event)) {
	a.events[event] = f
}

func (a *App) event_broadcast(event string, f func(*Event)) {
	a.events_broadcast[event] = f
}
