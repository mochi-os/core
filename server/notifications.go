// Comms server: Notifications
// Copyright Alistair Cunningham 2024

package main

type Notification struct {
	ID       string
	App      string
	Category string
	Entity   string
	Content  string
	Link     string
}

func init() {
	app_register("notifications", map[string]string{"en": "Notifications"})
	app_register_action("notifications", "", notifications_display)
	app_register_service("notifications", "clear", notifications_clear)
	app_register_service("notifications", "clear/entity", notifications_clear_entity)
	app_register_service("notifications", "create", notification_create)
}

// Create app database
func notifications_db_create(db string) {
	db_exec(db, "create table notifications ( id text not null primary key, app text not null, category text not null, entity text not null, content text not null, link text not null default '' )")
	db_exec(db, "create index notifications_app_entity on notifications( app, entity )")
}

func notifications_clear(u *User, service string, values ...any) any {
	db := db_app(u, "notifications", "data.db", notifications_db_create)
	log_debug("Clearing notifications for user '%d'", u.ID)
	db_exec(db, "delete from notifications")
	return nil
}

func notifications_clear_entity(u *User, service string, values ...any) any {
	db := db_app(u, "notifications", "data.db", notifications_db_create)
	if len(values) >= 2 {
		app := values[1].(string)
		entity := values[0].(string)
		log_debug("Clearing notifications for user '%d', app '%s', entity '%s'", u.ID, app, entity)
		db_exec(db, "delete from notifications where app=? and entity=?", app, entity)
	}
	return nil
}

func notification_create(u *User, service string, values ...any) any {
	db := db_app(u, "notifications", "data.db", notifications_db_create)
	app := values[0].(string)
	category := values[1].(string)
	entity := values[2].(string)
	content := values[3].(string)
	link := values[4].(string)
	log_debug("Creating notification: user='%d', app='%s', category='%s', entity='%s', content='%s', link='%s'", u.ID, app, category, entity, content, link)

	if !valid(app, "constant") || !valid(category, "constant") || !valid(content, "text") || !valid(link, "url") {
		log_info("Notification data not valid")
		return ""
	}

	id := uid()
	db_exec(db, "replace into notifications ( id, app, category, entity, content, link ) values ( ?, ?, ?, ?, ?, ? )", id, app, category, entity, content, link)
	return id
}

func notifications_display(u *User, action string, format string, p app_parameters) string {
	db := db_app(u, "notifications", "data.db", notifications_db_create)
	var n []Notification
	db_structs(&n, db, "select * from notifications order by id")
	return app_template("notifications/list", n)
}
