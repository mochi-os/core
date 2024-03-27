// Comms server: Notifications
// Copyright Alistair Cunningham 2024

package main

type Notification struct {
	ID       string
	User     int
	Instance string
	Content  string
	Link     string
	Updated  int64
}

func init() {
	app_register("comms/notifications", "Notifications")
	app_register_display("comms/notifications", notifications_display)
	app_register_function("comms/notifications", "clear", notifications_clear)
	app_register_function("comms/notifications", "clear/instance", notifications_clear_instance)
	app_register_function("comms/notifications", "create", notification_create)
	app_register_function("comms/notifications", "create/instance", notification_create)
	app_register_service("comms/notifications", "notifications")
}

func notifications_clear(u *User, service string, function string, values ...any) any {
	log_debug("Clearing notifications for user '%d'", u.ID)
	db_exec("notifications", "delete from notifications where user=?", u.ID)
	return nil
}

func notifications_clear_instance(u *User, service string, function string, values ...any) any {
	if len(values) > 0 {
		log_debug("Clearing notifications for user '%d', instance '%s'", u.ID, values[0].(string))
		db_exec("notifications", "delete from notifications where user=? and instance=?", u.ID, values[0].(string))
	}
	return nil
}

func notification_create(u *User, service string, function string, values ...any) any {
	instance := values[0].(string)
	content := values[1].(string)
	link := values[2].(string)
	log_debug("Creating notification: user='%d', instance='%s', content='%s', link='%s'", u.ID, instance, content, link)

	if !db_exists("data", "select id from instances where user=? and id=?", u.ID, instance) || !valid(content, "text") || !valid(link, "url") {
		return ""
	}

	var id string
	if function == "create/instance" {
		var n Notification
		if db_struct(&n, "notifications", "select * from notifications where user=? and instance=? limit 1", u.ID, instance) {
			id = n.ID
		}
	} else {
		id = uid()
	}

	db_exec("notifications", "replace into notifications ( id, user, instance, content, link, updated ) values ( ?, ?, ?, ?, ?, ? )", id, u.ID, instance, content, link, time_unix())
	return id
}

func notifications_display(u *User, p app_parameters, format string) string {
	var n []Notification
	db_structs(&n, "notifications", "select * from notifications where user=? order by updated", u.ID)
	return app_template("notifications/"+format+"/list", n)
}
