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
func notifications_db_create(db string) {
	db_exec(db, "create table notifications ( id text not null primary key, app text not null, category text not null, entity text not null, content text not null, link text not null default '' )")
	db_exec(db, "create index notifications_app_entity on notifications( app, entity )")
}

func notifications_clear(u *User) {
	db := db_app(u, "notifications", "data.db", notifications_db_create)
	db_exec(db, "delete from notifications")
}

func notifications_clear_entity(u *User, app string, entity string) {
	db := db_app(u, "notifications", "data.db", notifications_db_create)
	db_exec(db, "delete from notifications where app=? and entity=?", app, entity)
}

func notification_create(u *User, app string, category string, entity string, content string, link string) string {
	db := db_app(u, "notifications", "data.db", notifications_db_create)
	log_debug("Creating notification: user='%d', app='%s', category='%s', entity='%s', content='%s', link='%s'", u.ID, app, category, entity, content, link)

	if !valid(app, "constant") || !valid(category, "constant") || !valid(content, "text") || !valid(link, "url") {
		log_info("Notification data not valid")
		return ""
	}

	id := uid()
	db_exec(db, "replace into notifications ( id, app, category, entity, content, link ) values ( ?, ?, ?, ?, ?, ? )", id, app, category, entity, content, link)
	return id
}

func notifications_list(u *User) *[]Notification {
	db := db_app(u, "notifications", "data.db", notifications_db_create)
	var n []Notification
	db_structs(&n, db, "select * from notifications order by id")
	return &n
}
