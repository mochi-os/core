// Comms server: Events
// Copyright Alistair Cunningham 2024

package main

import (
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

func event(from string, to string, service string, instance string, action string, content string) (*Event, error) {
	log_debug("Sending event: from='%s', to='%s', service='%s', instance='%s', action='%s', content='%s'", from, to, service, instance, action, content)
	e := Event{ID: uid(), From: from, To: to, Service: service, Instance: instance, Action: action, Content: content}

	// Check if local recipient
	if db_exists("users", "select id from users where public=?", e.To) {
		go event_receive(&e)
		return &e, nil
	}

	//TODO Set signature
	//TODO Send via libp2p

	log_debug("No destination found for event")
	return &e, error_message("No destination")
}

func event_receive(e *Event) {
	log_debug("Event received: id='%s', from='%s', to='%s', service='%s', instance='%s', action='%s', content='%s', signature='%s'", e.ID, e.From, e.To, e.Service, e.Instance, e.Action, e.Content, e.Signature)

	if !valid(e.Instance, "id") {
		log_info("Dropping received event due to invalid instance ID '%s'", e.Instance)
		return
	}

	//TODO Check signature if from external

	var u *User = nil
	if e.To != "" {
		db_struct(u, "users", "select id from users where public=?", e.To)
	}

	a := app_by_service(e.Service)
	if a == nil {
		log_info("Dropping received event due to unknown service '%s'", e.Service)
		return
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
	log_info("Dropping received event due to unknown action '%s' for service '%s'", e.Action, e.Service)
}

func event_receive_json(event []byte) {
	var e Event
	err := json.Unmarshal(event, &e)
	if err != nil {
		log_info("Dropping event with malformed JSON: '%s'", event)
		return
	}
	event_receive(&e)
}
