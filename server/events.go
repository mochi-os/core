// Comms server: Events
// Copyright Alistair Cunningham 2024

package main

import (
	"crypto/ed25519"
	"time"
)

type Event struct {
	ID        string `json:"id"`
	Source    string
	From      string `json:"from"`
	To        string `json:"to"`
	App       string `json:"app"`
	Entity    string `json:"entity"`
	Action    string `json:"action"`
	Content   string `json:"content"`
	Signature string `json:"signature"`
}

type Queue struct {
	ID       string
	Method   string
	Location string
	Event    string
	Updated  string
}

func event(u *User, to string, app string, entity string, action string, content string) *Event {
	from := ""
	if u != nil {
		from = u.Public
	}
	e := Event{ID: uid(), Source: "", From: from, To: to, App: app, Entity: entity, Action: action, Content: content}
	method, location, queue_method, queue_location := user_location(e.To)
	if method != "none" {
		log_debug("Sending event '%#v' to %s '%s'", e, method, location)
	}

	if method == "local" {
		go event_receive(&e)
		return &e
	}

	if u != nil {
		private := base64_decode(u.Private, "")
		if string(private) == "" {
			log_warn("Dropping event due to invalid private key")
			return nil
		}
		e.Signature = base64_encode(ed25519.Sign(private, []byte(e.From+e.To+e.App+e.Entity+e.Action+e.Content)))
	}

	j := json_encode(e)

	if method == "libp2p" && libp2p_send(location, j) {
		return &e
	}

	if method == "none" {
		return &e
	}

	log_debug("Unable to send event to '%s', adding to queue", e.To)
	db_exec("db/queue.db", "replace into queue ( id, method, location, event, updated ) values ( ?, ?, ?, ?, ? )", e.ID, queue_method, queue_location, j, time_unix())
	return &e
}

func events_check_queue(method string, location string) {
	log_debug("Checking queue for queued events to %s '%s'", method, location)
	var queue []Queue
	db_structs(&queue, "db/queue.db", "select * from queue where method=? and location=?", method, location)
	for _, q := range queue {
		log_debug("Trying to send queued event '%s' to %s '%s'", q.Event, q.Method, q.Location)
		success := false

		if q.Method == "peer" {
			if peer_send(q.Location, q.Event) {
				success = true
			}

		} else if q.Method == "user" {
			method, location, _, _ := user_location(location)
			if method == "libp2p" && libp2p_send(location, q.Event) {
				success = true
			}
		}

		if success {
			log_debug("Queue event sent")
			db_exec("db/queue.db", "delete from queue where id=?", q.ID)
		} else {
			log_debug("Still unable to send queued event; keeping in queue")
		}
	}
}

func event_receive(e *Event) {
	//log_debug("Event received: id='%s', from='%s', to='%s', app='%s', entity='%s', action='%s', content='%s', signature='%s'", e.ID, e.From, e.To, e.App, e.Entity, e.Action, e.Content, e.Signature)

	if e.Source != "" && e.From != "" {
		public := base64_decode(e.From, "")
		if len(public) != ed25519.PublicKeySize {
			log_info("Dropping received event due to invalid from length %d!=%d", len(public), ed25519.PublicKeySize)
			return
		}
		if !ed25519.Verify(public, []byte(e.From+e.To+e.App+e.Entity+e.Action+e.Content), base64_decode(e.Signature, "")) {
			log_info("Dropping received event due to invalid sender signature")
			return
		}
	}

	if e.Entity != "" && !valid(e.Entity, "id") {
		log_info("Dropping received event due to invalid entity '%s'", e.Entity)
		return
	}

	a := apps_by_name[e.App]
	if a == nil {
		log_info("Dropping received event due to unknown app '%s'", e.App)
		return
	}

	var u *User = nil
	if e.To != "" {
		u = &User{}
		db_struct(u, "db/users.db", "select id from users where public=?", e.To)
	}

	switch a.Type {
	case "internal":
		f, found := a.Internal.Events[e.Action]
		if found {
			f(u, e)
			return
		} else {
			f, found = a.Internal.Events[""]
			if found {
				f(u, e)
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

func event_receive_json(event []byte, source string) {
	var e Event
	if !json_decode(event, &e) {
		log_info("Dropping event with malformed JSON: '%s'", event)
		return
	}
	e.Source = source
	event_receive(&e)
}

func queue_manager() {
	for {
		time.Sleep(time.Minute)
		var q Queue
		if db_struct(&q, "db/queue.db", "select * from queue limit 1 offset abs(random()) % max((select count(*) from queue), 1)") {
			log_debug("Queue helper nudging events to %s '%s'", q.Method, q.Location)
			events_check_queue(q.Method, q.Location)
		}
	}
}
