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
	app_register_internal("comms/notifications", "Notifications", []string{"notifications"})
	app_register_function_call("comms/notifications", notifications_call)
	app_register_function_display("comms/notifications", notifications_display)
}

func notifications_call(u *User, service string, function string, values ...any) any {
	if function == "clear" {
		log_debug("Clearing notifications for user '%d'", u.ID)
		db_exec("notifications", "delete from notifications where user=?", u.ID)

	} else if function == "clear/instance" {
		if len(values) > 0 {
			log_debug("Clearing notifications for user '%d', instance '%s'", u.ID, values[0].(string))
			db_exec("notifications", "delete from notifications where user=? and instance=?", u.ID, values[0].(string))
		}

	} else if function == "create" && len(values) == 3 {
		instance := values[0].(string)
		content := values[1].(string)
		link := values[2].(string)
		log_debug("Creating notification: user='%d', instance='%s', content='%s', link='%s'", u.ID, instance, content, link)

		if !db_exists("data", "select id from instances where user=? and id=?", u.ID, instance) || !valid(content, "text") || !valid(link, "url") {
			return ""
		}

		id := uid()
		db_exec("notifications", "replace into notifications ( id, user, instance, content, link, updated ) values ( ?, ?, ?, ?, ?, ? )", id, u.ID, instance, content, link, time_unix())
		return id
	}

	return nil
}

func notifications_display(u *User, p app_parameters, format string) string {
	var n []Notification
	db_structs(&n, "notifications", "select * from notifications where user=? order by updated", u.ID)
	return app_template("notifications/"+format+"/list", n)
}
