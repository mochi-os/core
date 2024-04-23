// Comms server: Settings
// Copyright Alistair Cunningham 2024

package main

type Setting struct {
	Name  string
	Value string
}

func setting_get(name string, def string) string {
	var s Setting
	if db_struct(&s, "db/settings.db", "select * from settings where name=?", name) {
		return s.Value
	}
	return def
}

func setting_set(name string, value string) {
	db_exec("db/settings.db", "replace into settings ( name, value ) values ( ?, ? )", name, value)
}
