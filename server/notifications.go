// Comms server: Notifications
// Copyright Alistair Cunningham 2024

package main

func init() {
	app_register("notifications", map[string]string{"en": "Notifications"})
	app_register_display("notifications", notifications_display)
	app_register_service("notifications", "clear", notifications_clear)
	app_register_service("notifications", "clear/object", notifications_clear_object)
	app_register_service("notifications", "create", notification_create)
}

func notifications_clear(u *User, service string, values ...any) any {
	log_debug("Clearing notifications for user '%d'", u.ID)
	objects_delete_by_parent(u, "notifications", "")
	return nil
}

func notifications_clear_object(u *User, service string, values ...any) any {
	if len(values) > 0 {
		object := values[0].(string)
		log_debug("Clearing notifications for user '%d', object '%s'", u.ID, object)
		object_delete_by_path(u, "notifications", object)
	}
	return nil
}

func notification_create(u *User, service string, values ...any) any {
	object := values[0].(string)
	content := values[1].(string)
	link := values[2].(string)
	log_debug("Creating notification: user='%d', object='%s', content='%s', link='%s'", u.ID, object, content, link)

	if object_by_id(u, object) == nil || !valid(content, "text") || !valid(link, "url") {
		return ""
	}

	n := object_by_path(u, "notifications", object)
	if n == nil {
		n := object_create(u, "notifications", object, "notifications", "")
		if n == nil {
			log_warn("Unable to create notification")
			return nil
		}
	}
	object_value_set(u, n.ID, "content", content)
	object_value_set(u, n.ID, "link", link)
	return n
}

func notifications_display(u *User, p app_parameters, format string) string {
	var notifications []map[string]string
	for _, n := range *objects_by_parent(u, "notifications", "", "updated") {
		notifications = append(notifications, map[string]string{"ID": n.ID, "Content": object_value_get(u, n.ID, "content", ""), "Link": object_value_get(u, n.ID, "link", "")})
	}
	return app_template("notifications/"+format+"/list", notifications)
}
