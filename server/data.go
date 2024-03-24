// Comms server: Data
// Copyright Alistair Cunningham 2024

package main

type Data struct {
	User     int
	App      string
	Instance string
	Name     string
	Value    string
}

func data_append(user int, app string, instance string, name string, value string) error {
	if !db_exists("data", "select * from instances where user=? and id=?", user, instance) {
		return error_message("Instance not found")
	}
	if !valid(name, "constant") {
		return error_message("Invalid name")
	}
	if !valid(value, "text") {
		return error_message("Invalid value")
	}

	if db_exists("data", "select value from data where user=? and app=? and instance=? and name=?", user, app, instance, name) {
		db_exec("data", "update data set value=value||? where user=? and app=? and instance=? and name=?", value, user, app, instance, name)
		return nil
	}

	return data_set(user, app, instance, name, value)
}

func data_get(user int, app string, instance string, name string, def string) string {
	var d Data
	if db_struct(&d, "data", "select value from data where user=? and app=? and instance=? and name=?", user, app, instance, name) {
		return d.Value
	}
	return def
}

func data_set(user int, app string, instance string, name string, value string) error {
	if !db_exists("data", "select * from instances where user=? and id=?", user, instance) {
		return error_message("Instance not found")
	}
	if !valid(name, "constant") {
		return error_message("Invalid name")
	}
	if !valid(value, "text") {
		return error_message("Invalid value")
	}

	db_exec("data", "replace into data ( user, app, instance, name, value ) values ( ?, ?, ?, ?, ? )", user, app, instance, name, value)
	return nil
}
