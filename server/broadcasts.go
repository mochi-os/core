// Mochi server: Broadcasts
// Copyright Alistair Cunningham 2024-2025

package main

func broadcast(u *User, sender string, action string, entity string, content any) {
	debug("Broadcast user %d, sender %q, action %q, entity %q, content %v", u.ID, sender, action, entity, content)
	apps_lock.Lock()
	defer apps_lock.Unlock()

	for _, a := range apps {
		f, found := a.active.broadcasts[sender+"/"+action]
		if found {
			go f(u, sender, action, entity, content)

		} else {
			f, found := a.active.broadcasts[sender+"/"]
			if found {
				go f(u, sender, action, entity, content)
			}
		}
	}
}
