// Mochi server: Broadcasts
// Copyright Alistair Cunningham 2024

package main

type (
	broadcast_action_functions []func(*User, string, string, string, any)
	broadcast_actions          map[string]broadcast_action_functions
)

var (
	broadcasts_by_sender = map[string]broadcast_actions{}
)

func broadcast(u *User, sender string, action string, entity string, content any) {
	debug("Broadcast: user='%d', sender='%s', action='%s', entity='%s', content='%s'", u.ID, sender, action, entity, content)

	s, sender_found := broadcasts_by_sender[sender]
	if sender_found {
		action_functions, action_found := s[action]
		if action_found {
			for _, f := range action_functions {
				go f(u, sender, action, entity, content)
			}
		}

		all_functions, all_found := s[""]
		if all_found {
			for _, f := range all_functions {
				go f(u, sender, action, entity, content)
			}
		}
	}
}
