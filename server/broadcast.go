// Comms server: Broadcast
// Copyright Alistair Cunningham 2024

package main

type broadcast_map map[string]func(*User, string, string, string, string)

var broadcasts_by_sender = map[string]broadcast_map{}

func broadcast(u *User, sender string, action string, entity string, content string) {
	log_debug("Broadcast: user='%d', sender='%s', action='%s', entity='%s', content='%s'", u.ID, sender, action, entity, content)

	s := broadcasts_by_sender[sender]
	f := s[action]
	if f != nil {
		go f(u, sender, action, entity, content)
	}
	f = s[""]
	if f != nil {
		go f(u, sender, action, entity, content)
	}
}
