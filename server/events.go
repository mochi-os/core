// Comms server: Events
// Copyright Alistair Cunningham 2024

package main

import (
	"crypto/ed25519"
	"time"
)

type Event struct {
	ID        string `json:"id"`
	From      string `json:"from"`
	To        string `json:"to"`
	App       string `json:"app"`
	Action    string `json:"action"`
	Content   string `json:"content"`
	Signature string `json:"signature"`
	Source    string `json:"-"`
}

type Queue struct {
	ID       string
	Method   string
	Location string
	Event    string
	Updated  string
}

func events_check_queue(method string, location string) {
	var queue []Queue
	db := db_open("db/queue.db")
	db.scans(&queue, "select * from queue where method=? and location=?", method, location)
	for _, q := range queue {
		log_debug("Trying to send queued event '%s' to %s '%s'", q.Event, q.Method, q.Location)
		success := false

		if q.Method == "peer" {
			if peer_send(q.Location, q.Event) {
				success = true
			}

		} else if q.Method == "identity" {
			method, location, _, _ := identity_location(location)
			if method == "libp2p" && libp2p_send(location, q.Event) {
				success = true
			}
		}

		if success {
			log_debug("Queue event sent")
			db := db_open("db/queue.db")
			db.exec("delete from queue where id=?", q.ID)
		} else {
			log_debug("Still unable to send queued event; keeping in queue")
		}
	}
}

func events_manager() {
	db := db_open("db/queue.db")

	for {
		time.Sleep(time.Minute)
		var q Queue
		if db.scan(&q, "select * from queue limit 1 offset abs(random()) % max((select count(*) from queue), 1)") {
			log_debug("Queue helper nudging events to %s '%s'", q.Method, q.Location)
			events_check_queue(q.Method, q.Location)
		}
	}
}

func event_receive_json(event []byte, source string) {
	var e Event
	if !json_decode(event, &e) {
		log_info("Dropping event with malformed JSON: '%s'", event)
		return
	}
	e.Source = source
	e.receive()
}

func (e *Event) receive() {
	if e.From != "" {
		if !valid(e.From, "id") {
			log_info("Dropping received event due to invalid 'from' field '%s'", e.From)
			return
		}

		if e.Source != "" {
			public := base64_decode(e.From, "")
			if len(public) != ed25519.PublicKeySize {
				log_info("Dropping received event due to invalid from length %d!=%d", len(public), ed25519.PublicKeySize)
				return
			}
			if !ed25519.Verify(public, []byte(e.ID+e.From+e.To+e.App+e.Action+e.Content), base64_decode(e.Signature, "")) {
				log_info("Dropping received event due to invalid sender signature")
				return
			}
		}
	}

	u := user_owning_identity(e.To)

	//TODO Route on destination identity class, rather than app? If so, remove app field from event?
	a := apps[e.App]
	if a == nil {
		log_info("Dropping received event due to unknown app '%s'", e.App)
		return
	}

	switch a.Type {
	case "internal":
		ae, found := a.Internal.Events[e.Action]
		if u == nil && ae.Addressed {
			log_info("Dropping received event due to no matching identity")
			return
		}
		if found {
			ae.Function(u, e)
			return
		} else {
			ae, found = a.Internal.Events[""]
			if found {
				ae.Function(u, e)
				return
			}
		}

	case "wasm":
		for _, try := range []string{e.Action, ""} {
			function, found := a.WASM.Events[try]
			if found {
				_, err := wasm_run(u, a, function, 0, e)
				if err != nil {
					log_info("Event handler returned error: %s", err)
					return
				}
			}
		}
	}

	log_info("Dropping received event due to unknown action '%s' for app '%s'", e.Action, e.App)
}

func (a *App) register_event(event string, f func(*User, *Event), addressed bool) {
	a.Internal.Events[event] = &AppEvent{Function: f, Addressed: addressed}
}

func (e *Event) send() {
	if e.ID == "" {
		e.ID = uid()
	}

	method, location, queue_method, queue_location := identity_location(e.To)

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
	db.exec("replace into queue ( id, method, location, event, updated ) values ( ?, ?, ?, ?, ? )", e.ID, queue_method, queue_location, j, time_unix())
}

func (e *Event) sign() {
	if e.From == "" {
		return
	}

	if e.ID == "" {
		e.ID = uid()
	}

	db := db_open("db/users.db")
	var i Identity
	if !db.scan(&i, "select private from identities where id=?", e.From) {
		log_warn("Not signing event due unknown sending identity")
		return
	}
	private := base64_decode(i.Private, "")
	if string(private) == "" {
		log_warn("Not signing event due to invalid private key")
		return
	}
	e.Signature = base64_encode(ed25519.Sign(private, []byte(e.ID+e.From+e.To+e.App+e.Action+e.Content)))
}
