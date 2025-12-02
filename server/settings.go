// Mochi server: Settings
// Copyright Alistair Cunningham 2024-2025

package main

// Preference stores a user preference key-value pair
type Preference struct {
	Name  string
	Value string
}

// Setting stores a global setting key-value pair
type Setting struct {
	Name  string
	Value string
}

// user_preferences_load loads all preferences for a user
func user_preferences_load(u *User) map[string]string {
	prefs := map[string]string{}
	db := db_user(u, "settings")
	rows, err := db.rows("select * from preferences")
	if err != nil {
		return prefs
	}
	for _, row := range rows {
		if name, ok := row["name"].(string); ok {
			if value, ok := row["value"].(string); ok {
				prefs[name] = value
			}
		}
	}
	return prefs
}

// user_preference_get returns a user preference or default
func user_preference_get(u *User, name, def string) string {
	if v, ok := u.Preferences[name]; ok {
		return v
	}
	return def
}

// user_preference_set sets a user preference
func user_preference_set(u *User, name, value string) {
	db := db_user(u, "settings")
	db.exec("replace into preferences (name, value) values (?, ?)", name, value)
	u.Preferences[name] = value
}

// user_preference_delete deletes a user preference, returns true if it existed
func user_preference_delete(u *User, name string) bool {
	if _, ok := u.Preferences[name]; !ok {
		return false
	}
	db := db_user(u, "settings")
	db.exec("delete from preferences where name = ?", name)
	delete(u.Preferences, name)
	return true
}

func setting_get(name string, def string) string {
	var s Setting
	db := db_open("db/settings.db")
	if db.scan(&s, "select * from settings where name=?", name) {
		return s.Value
	}
	return def
}

func setting_set(name string, value string) {
	db := db_open("db/settings.db")
	db.exec("replace into settings ( name, value ) values ( ?, ? )", name, value)
}
