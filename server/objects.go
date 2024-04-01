// Comms server: Objects
// Copyright Alistair Cunningham 2024

package main

type Object struct {
	ID       string
	User     int
	App      string
	Category string
	Tag      string
	Name     string
	Updated  int64
}

type ObjectValue struct {
	Object string
	Name   string
	Value  string
}

func object_by_id(u *User, id string) *Object {
	var o Object
	if db_struct(&o, "data", "select * from objects where user=? and id=?", u.ID, id) {
		return &o
	}
	return nil
}

func object_by_tag(u *User, app string, category string, tag string) *Object {
	log_debug("Getting object app '%s', category '%s', tag '%s'", app, category, tag)
	var o Object
	if db_struct(&o, "data", "select * from objects where user=? and app=? and category=? and tag=?", u.ID, app, category, tag) {
		log_debug("Object found")
		return &o
	}
	log_debug("Object not found")
	return nil
}

func objects_by_category(u *User, app string, category string, sort string) *[]Object {
	var o []Object
	db_structs(&o, "data", "select * from objects where user=? and app=? and category=? order by ?", u.ID, app, category, sort)
	return &o
}

func object_create(u *User, app string, category string, tag string, name string) *Object {
	_, found := apps_by_name[app]
	if !found {
		log_warn("App '%s' not found when creating object", app)
		return nil
	}
	if category != "" && !valid(category, "name") {
		log_warn("Category '%s' not valid when creating object", category)
		return nil
	}
	if !valid(tag, "name") {
		log_warn("Tag '%s' not valid when creating object", tag)
		return nil
	}
	if name != "" && !valid(name, "name") {
		log_warn("Name '%s' not valid when creating object", name)
		return nil
	}

	id := uid()
	db_exec("data", "replace into objects ( id, user, app, category, tag, name, updated ) values ( ?, ?, ?, ?, ?, ?, ? )", id, u.ID, app, category, tag, name, time_unix())
	return object_by_id(u, id)
}

func objects_delete_by_category(u *User, app string, category string) {
	for _, o := range *objects_by_category(u, app, category, "id") {
		object_delete_by_id(u, o.ID)
	}
}

func object_delete_by_id(u *User, id string) {
	log_debug("Deleting object '%s'", id)
	db_exec("data", "delete from object_values where object=?", id)
	db_exec("data", "delete from objects where id=?", id)
}

func object_delete_by_tag(u *User, app string, category string, tag string) {
	log_debug("Deleting object app '%s', category '%s', tag '%s'", app, category, tag)
	o := object_by_tag(u, app, category, tag)
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
		db_exec("data", "update object_values set value=value||? where object=? and name=?", value, object, name)
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

	db_exec("data", "replace into object_values ( object, name, value ) values ( ?, ?, ? )", object, name, value)
	return nil
}
