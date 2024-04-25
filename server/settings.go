// Comms server: Settings
// Copyright Alistair Cunningham 2024

package main

type Setting struct {
	Name  string
	Value string
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
