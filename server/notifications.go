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

// Create app database
func notifications_db_create(db *DB) {
	db.exec("create table settings ( name text not null primary key, value text not null )")
	db.exec("replace into settings ( name, value ) values ( 'schema', 1 )")

	db.exec("create table notifications ( id text not null primary key, app text not null, category text not null, entity text not null, content text not null, link text not null default '' )")
	db.exec("create index notifications_app_entity on notifications( app, entity )")
}

func notifications_clear(user int) {
	db := db_user(user, "db/notifications.db", notifications_db_create)
	defer db.close()

	db.exec("delete from notifications")
}

func notifications_clear_entity(user int, app string, entity string) {
	db := db_user(user, "db/notifications.db", notifications_db_create)
	defer db.close()

	db.exec("delete from notifications where app=? and entity=?", app, entity)
}

func notification_create(user int, app string, category string, entity string, content string, link string) string {
	db := db_user(user, "db/notifications.db", notifications_db_create)
	defer db.close()

	log_debug("Creating notification: user='%d', app='%s', category='%s', entity='%s', content='%s', link='%s'", user, app, category, entity, content, link)

	if !valid(app, "constant") || !valid(category, "constant") || !valid(content, "text") || !valid(link, "url") {
		log_info("Notification data not valid")
		return ""
	}

	id := uid()
	db.exec("replace into notifications ( id, app, category, entity, content, link ) values ( ?, ?, ?, ?, ?, ? )", id, app, category, entity, content, link)
	return id
}

func notifications_list(user int) *[]Notification {
	db := db_user(user, "db/notifications.db", notifications_db_create)
	defer db.close()

	var n []Notification
	db.scans(&n, "select * from notifications order by id")
	return &n
}
