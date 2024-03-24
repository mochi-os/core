// Comms server: Settings
// Copyright Alistair Cunningham 2024

package main

type Setting struct {
	Name  string
	Value string
}

func setting_get(name string, def string) string {
	var s Setting
	if db_struct(&s, "settings", "select * from settings where name=?", name) {
		return s.Value
	}
	return def
}

func setting_set(name string, value string) {
	db_exec("settings", "replace into settings ( name, value ) values ( ?, ? )", name, value)
}
