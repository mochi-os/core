// Comms server: Instances
// Copyright Alistair Cunningham 2024

package main

type Instance struct {
	User    int
	ID      string
	Name    string
	Service string
	Updated int64
}

func instance_by_id(user int, id string) *Instance {
	var i Instance
	if db_struct(&i, "data", "select * from instances where user=? and id=?", user, id) {
		return &i
	}
	return nil
}

func instances_by_service(user int, service string, sort string) *[]Instance {
	var i []Instance
	db_structs(&i, "data", "select * from instances where user=? and service=? order by ?", user, service, sort)
	return &i
}

func instance_create(user int, id string, name string, service string) error {
	log_debug("Creating instance: user='%d', id='%s', name='%s', service='%s'", user, id, name, service)

	if !valid(id, "uid") {
		return error_message("Invalid instance ID")
	}
	if !valid(name, "name") {
		return error_message("Invalid service")
	}
	if !valid(service, "constant") {
		return error_message("Invalid name")
	}

	db_exec("data", "insert into instances ( user, id, name, service, updated ) values ( ?, ?, ?, ?, ? )", user, id, name, service, time_unix())
	return nil
}

func instance_delete(user int, id string) {
	if !db_exists("data", "select id from instances where user=? and id=?", user, id) {
		return
	}

	db_exec("data", "delete from data where user=? and instance=?", user, id)
	db_exec("data", "delete from instances where user=? and id=?", user, id)
}

func instance_touch(user int, id string) {
	db_exec("data", "update instances set updated=? where user=? and id=?", time_unix(), user, id)
}
