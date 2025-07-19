// Comms server: Notifications
// Copyright Alistair Cunningham 2024

package main

type Notification struct {
	App      string
	Object   string
	Category string
	Content  string
	Link     string
	Created  int64
}

// Create app database
func notifications_db_create(db *DB) {
	db.exec("create table settings ( name text not null primary key, value text not null )")
	db.exec("replace into settings ( name, value ) values ( 'schema', 1 )")

	db.exec("create table notifications ( app text not null, object text not null, category text not null, content text not null, link text not null default '', created integer not null, primary key ( app, object, category ) )")
}

func notification(u *User, app string, category string, object string, content string, link string) {
	db := db_user(u, "db/notifications.db", notifications_db_create)
	defer db.close()

	log_debug("Creating notification: user='%d', app='%s', object='%s', category='%s', content='%s', link='%s'", u.ID, app, object, category, content, link)

	if !valid(app, "constant") || !valid(category, "constant") || !valid(content, "text") || !valid(link, "url") {
		log_info("Notification data not valid")
		return
	}

	db.exec("replace into notifications ( app, object, category, content, link, created ) values ( ?, ?, ?, ?, ?, ? )", app, object, category, content, link, now())
}

func notifications_clear(u *User) {
	db := db_user(u, "db/notifications.db", notifications_db_create)
	defer db.close()

	db.exec("delete from notifications")
}

func notifications_clear_object(u *User, app string, object string) {
	db := db_user(u, "db/notifications.db", notifications_db_create)
	defer db.close()

	db.exec("delete from notifications where app=? and object=?", app, object)
}

func notifications_list(u *User) *[]Notification {
	db := db_user(u, "db/notifications.db", notifications_db_create)
	defer db.close()

	var n []Notification
	db.scans(&n, "select * from notifications order by created, content")
	return &n
}
