// Comms server: Objects
// Copyright Alistair Cunningham 2024

package main

type Object struct {
	ID      string
	User    int
	App     string
	Parent  string
	Path    string
	Name    string
	Updated int64
}

type ObjectValue struct {
	Object string
	Name   string
	Value  string
}

func object_by_id(u *User, id string) *Object {
	var o Object
	db_struct(&o, "data", "select * from objects where user=? and id=?", u.ID, id)
	return &o
}

func object_by_path(u *User, app string, path string) *Object {
	var o Object
	db_struct(&o, "data", "select * from objects where user=? and app=? and path=?", u.ID, app, path)
	return &o
}

func objects_by_parent(u *User, app string, parent string, sort string) *[]Object {
	var o []Object
	db_structs(&o, "data", "select * from objects where user=? and app=? and parent=? order by ?", u.ID, app, parent, sort)
	return &o
}

func object_create(u *User, app string, path string, parent string, name string) *Object {
	_, found := apps_by_name[app]
	if !found || !valid(path, "name") || !valid(name, "name") {
		return nil
	}
	if parent != "" && !db_exists("data", "select id from objects where user=? and id=?", u.ID, parent) {
		return nil
	}

	id := uid()
	db_exec("data", "replace into objects ( id, user, app, path, parent, name, updated ) values ( ?, ?, ?, ?, ?, ?, ? )", id, u.ID, app, path, parent, name, time_unix())
	return object_by_id(u, id)
}

func object_delete_by_id(u *User, id string) {
	var o Object
	if !db_struct(&o, "data", "select id from objects where user=? and id=?", u.ID, id) {
		return
	}
	objects_delete_by_parent(u, o.App, id)

	db_exec("data", "delete from object_values where object=?", id)
	db_exec("data", "delete from objects where id=?", id)
}

func objects_delete_by_parent(u *User, app string, parent string) {
	for _, o := range *objects_by_parent(u, app, parent, "id") {
		object_delete_by_id(u, o.ID)
	}
}

func object_delete_by_path(u *User, app string, path string) {
	o := object_by_path(u, app, path)
	if o != nil {
		object_delete_by_id(u, o.ID)
	}
}

func object_touch(u *User, id string) {
	db_exec("data", "update objects set updated=? where user=? and id=?", time_unix(), u.ID, id)
}

func object_value_append(u *User, object string, name string, value string) error {
	if !db_exists("data", "select id from objects where user=? and id=?", u.ID, object) {
		return error_message("Object not found")
	}
	if !valid(name, "constant") {
		return error_message("Invalid name")
	}
	if !valid(value, "text") {
		return error_message("Invalid value")
	}

	if db_exists("data", "select value from object_values where object=? and name=?", object, name) {
		db_exec("data", "update data set value=value||? where object=? and name=?", value, object, name)
		return nil
	}

	return object_value_set(u, object, name, value)
}

func object_value_get(u *User, object string, name string, def string) string {
	if !db_exists("data", "select id from objects where user=? and id=?", u.ID, object) {
		return def
	}
	var v ObjectValue
	if db_struct(&v, "data", "select value from object_values where object=? and name=?", object, name) {
		return v.Value
	}
	return def
}

func object_value_set(u *User, object string, name string, value string) error {
	if !db_exists("data", "select id from objects where user=? and id=?", u.ID, object) {
		return error_message("Object not found")
	}
	if !valid(name, "constant") {
		return error_message("Invalid name")
	}
	if !valid(value, "text") {
		return error_message("Invalid value")
	}

	db_exec("data", "replace into object_values ( user, object, name, value ) values ( ?, ?, ?, ? )", u.ID, object, name, value)
	return nil
}
