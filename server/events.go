// Comms server: Events
// Copyright Alistair Cunningham 2024

package main

import (
	"encoding/json"
)

type Event struct {
	ID       string `json:"id"`
	From     string `json:"from"`
	To       string `json:"to"`
	Service  string `json:"service"`
	Instance string `json:"instance"`
	Action   string `json:"action"`
	Content  string `json:"content"`
}

func event(from string, to string, service string, instance string, action string, content string) (*Event, error) {
	log_debug("Sending event: from='%s', to='%s', service='%s', instance='%s', action='%s', content='%s'", from, to, service, instance, action, content)
	e := Event{ID: uid(), From: from, To: to, Service: service, Instance: instance, Action: action, Content: content}

	// Check if local recipient
	if db_exists("users", "select id from users where public=?", e.To) {
		go event_receive(&e)
		return &e, nil
	}

	log_debug("No destination found for event")
	return &e, error_message("No destination")
}

func event_receive(e *Event) {
	log_debug("Event received: id='%s', from='%s', to='%s', service='%s', instance='%s', action='%s', content='%s'", e.ID, e.From, e.To, e.Service, e.Instance, e.Action, e.Content)

	var u User
	if !db_struct(&u, "users", "select id from users where public=?", e.To) {
		log_info("Dropping received event due to unknown local identity '%s'", e.To)
		return
	}

	if !valid(e.Instance, "uid") {
		log_info("Dropping received event due to invalid instance ID '%s'", e.Instance)
		return
	}

	a := app_by_service(e.Service)
	if a == nil {
		log_info("Dropping received event due to unknown service '%s'", e.Service)
		return
	}
	a.Event(&u, e)
}

func event_receive_json(event string) {
	log_debug("Event received: '%s'", event)
	var e Event
	err := json.Unmarshal([]byte(event), &e)
	if err != nil {
		log_info("Dropping event with malformed JSON: '%s'", event)
		return
	}
	event_receive(&e)
}
