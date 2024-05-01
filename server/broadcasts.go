// Comms server: Broadcasts
// Copyright Alistair Cunningham 2024

package main

type broadcast_action_functions []func(int, string, string, string, any)
type broadcast_actions map[string]broadcast_action_functions

var broadcasts_by_sender = map[string]broadcast_actions{}

func broadcast(user int, sender string, action string, entity string, content any) {
	log_debug("Broadcast: user='%d', sender='%s', action='%s', entity='%s', content='%s'", user, sender, action, entity, content)

	s, sender_found := broadcasts_by_sender[sender]
	if sender_found {
		action_functions, action_found := s[action]
		if action_found {
			for _, f := range action_functions {
				go f(user, sender, action, entity, content)
			}
		}

		all_functions, all_found := s[""]
		if all_found {
			for _, f := range all_functions {
				go f(user, sender, action, entity, content)
			}
		}
	}
}

func (a *App) register_broadcast(sender string, action string, f func(int, string, string, string, any)) {
	s, sender_found := broadcasts_by_sender[sender]
	if sender_found {
		_, action_found := s[action]
		if action_found {
			s[action] = append(s[action], f)
		} else {
			s[action] = broadcast_action_functions{f}
		}
	} else {
		broadcasts_by_sender[sender] = broadcast_actions{action: broadcast_action_functions{f}}
	}
}
