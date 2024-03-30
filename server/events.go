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
	Service   string `json:"service"`
	Instance  string `json:"instance"`
	Action    string `json:"action"`
	Content   string `json:"content"`
	Signature string `json:"signature"`
}

func event(u *User, to string, service string, instance string, action string, content string) (*Event, error) {
	log_debug("Sending event: from='%s', to='%s', service='%s', instance='%s', action='%s', content='%s'", u.Public, to, service, instance, action, content)
	e := Event{ID: uid(), From: u.Public, To: to, Service: service, Instance: instance, Action: action, Content: content}

	method, location := user_location(e.To)

	if method == "local" {
		go event_receive(&e, false)
		return &e, nil

	} else if method == "libp2p" {
		private := base64_decode(u.Private, "")
		if string(private) == "" {
			log_warn("Dropping event due to invalid private key")
			return &e, error_message("Invalid private key")
		}
		e.Signature = base64_encode(ed25519.Sign(private, []byte(e.From+e.To+e.Service+e.Instance+e.Action+e.Content)))
		j, err := json.Marshal(e)
		fatal(err)
		libp2p_send(location, j)
		return &e, nil
	}

	log_debug("No destination found for event to '%s'", e.To)
	return &e, error_message("No destination")
}

func event_receive(e *Event, external bool) {
	log_debug("Event received: id='%s', from='%s', to='%s', service='%s', instance='%s', action='%s', content='%s', signature='%s'", e.ID, e.From, e.To, e.Service, e.Instance, e.Action, e.Content, e.Signature)

	if external && e.From != "" {
		public := base64_decode(e.From, "")
		if len(public) != ed25519.PublicKeySize {
			log_info("Dropping received event due to invalid from length %d!=%d", len(public), ed25519.PublicKeySize)
			return
		}
		if !ed25519.Verify(public, []byte(e.From+e.To+e.Service+e.Instance+e.Action+e.Content), base64_decode(e.Signature, "")) {
			log_info("Dropping received event due to invalid sender signature")
			return
		}
	}

	if !valid(e.Instance, "id") {
		log_info("Dropping received event due to invalid instance ID '%s'", e.Instance)
		return
	}

	a := app_by_service(e.Service)
	if a == nil {
		log_info("Dropping received event due to unknown service '%s'", e.Service)
		return
	}

	var u User
	if e.To != "" {
		db_struct(&u, "users", "select id from users where public=?", e.To)
	}

	_, found := a.Events[e.Action]
	if found {
		a.Events[e.Action](&u, e)
		return
	} else {
		_, found := a.Events[""]
		if found {
			a.Events[""](&u, e)
			return
		}
	}
	log_info("Dropping received event due to unknown action '%s' for service '%s'", e.Action, e.Service)
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
