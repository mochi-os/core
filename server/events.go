// Comms server: Events
// Copyright Alistair Cunningham 2024

package main

import (
	"crypto/ed25519"
	"encoding/json"
)

type Event struct {
	ID        string `json:"id"`
	From      string `json:"from"`
	To        string `json:"to"`
	App       string `json:"app"`
	Entity    string `json:"entity"`
	Action    string `json:"action"`
	Content   string `json:"content"`
	Signature string `json:"signature"`
}

func event(u *User, to string, app string, entity string, action string, content string) *Event {
	log_debug("Sending event: from='%s', to='%s', app='%s', entity='%s', action='%s', content='%s'", u.Public, to, app, entity, action, content)
	e := Event{ID: uid(), From: u.Public, To: to, App: app, Entity: entity, Action: action, Content: content}

	method, location := user_location(e.To)
	log_debug("Routing event to %s '%s'", method, location)

	if method == "local" {
		go event_receive(&e, false)
		return &e
	}

	// Add signature
	private := base64_decode(u.Private, "")
	if string(private) == "" {
		log_warn("Dropping event due to invalid private key")
		return nil
	}
	e.Signature = base64_encode(ed25519.Sign(private, []byte(e.From+e.To+e.App+e.Entity+e.Action+e.Content)))
	j, err := json.Marshal(e)
	fatal(err)

	if method == "libp2p" && libp2p_send(location, j) {
		return &e
	}

	if method == "none" {
		return &e
	}

	log_debug("Unable to send event to '%s', adding to queue", e.To)
	db_exec("queue", "replace into queue ( id, event, method, location, updated ) values ( ?, ?, ?, ?, ? )", e.ID, j, method, location, time_unix())
	return &e
}

func event_receive(e *Event, external bool) {
	log_debug("Event received: id='%s', from='%s', to='%s', app='%s', entity='%s', action='%s', content='%s', signature='%s'", e.ID, e.From, e.To, e.App, e.Entity, e.Action, e.Content, e.Signature)

	if external && e.From != "" {
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
		db_struct(u, "users", "select id from users where public=?", e.To)
	}

	_, found := a.Events[e.Action]
	if found {
		a.Events[e.Action](u, e)
		return
	} else {
		_, found := a.Events[""]
		if found {
			a.Events[""](u, e)
			return
		}
	}
	log_info("Dropping received event due to unknown action '%s' for app '%s'", e.Action, e.App)
}

func event_receive_json(event []byte, external bool) {
	var e Event
	err := json.Unmarshal(event, &e)
	if err != nil {
		log_info("Dropping event with malformed JSON: '%s'", event)
		return
	}
	event_receive(&e, external)
}
