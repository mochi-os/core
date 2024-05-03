// Comms: Forums app
// Copyright Alistair Cunningham 2024

package main

type Forum struct {
	ID      string
	Master  string
	Name    string
	Joining string
	Viewing string
	Posting string
	Updated int64
}

type ForumMember struct {
	Forum  string
	Member string
	Role   string
}

type ForumPost struct {
	ID     string
	Forum  string
	Time   int64
	Status string
	Author string
	Name   string
	Title  string
	Body   string
	Up     int
	Down   int
}

type ForumComment struct {
	ID     string
	Forum  string
	Post   string
	Parent string
	Time   int64
	Author string
	Name   string
	Body   string
	Up     int
	Down   int
}

type ForumVote struct {
	Voter string
	ID    string
	Vote  int
}

func init() {
	a := register_app("forums")
	a.register_db_app("data.db", forums_db_create)
	a.register_home("forums", map[string]string{"en": "Forums"})
	a.register_action("forums", forums_list, true)
	a.register_action("forums/create", forums_create, true)
	a.register_action("forums/new", forums_new, true)
	a.register_action("forums/comment/create", forums_comment_create, true)
	a.register_action("forums/comment/new", forums_comment_new, true)
	a.register_action("forums/post/create", forums_post_create, true)
	a.register_action("forums/post/new", forums_post_new, true)
	a.register_action("forums/post/view", forums_post_view, true)
	a.register_action("forums/subscribe", forums_subscribe, true)
	a.register_action("forums/subscribed", forums_subscribed, true)
	a.register_action("forums/view", forums_view, true)
	a.register_event("post", forums_event_post, true)
}

// Create app database
func forums_db_create(db *DB) {
	db.exec("create table settings ( name text not null primary key, value text not null )")
	db.exec("replace into settings ( name, value ) values ( 'schema', 1 )")

	db.exec("create table forums ( id text not null primary key, master text not null, name text not null, joining text not null, viewing text not null, posting text not null, updated integer not null )")
	db.exec("create index forums_name on forums( name )")
	db.exec("create index forums_updated on forums( updated )")

	db.exec("create table members ( forum references forums( id ), member text not null, role text not null default 'poster', primary key ( forum, member ) )")
	db.exec("create index members_member on members( member )")

	db.exec("create table posts ( id text not null primary key, forum references forum( id ), time integer not null, status text not null, author text not null, name text not null, title text not null, body text not null, up integer not null default 1, down integer not null default 0 )")
	db.exec("create index posts_forum on posts( forum )")
	db.exec("create index posts_time on posts( time )")
	db.exec("create index posts_status on posts( status )")

	db.exec("create table comments ( id text not null primary key, forum references forum( id ), post text not null, parent text not null, time integer not null, author text not null, name text not null, body text not null, up integer not null default 1, down integer not null default 0 )")
	db.exec("create index comments_forum on comments( forum )")
	db.exec("create index comments_post on comments( post )")
	db.exec("create index comments_parent on comments( parent )")
	db.exec("create index comments_time on comments( time )")

	db.exec("create table votes ( voter text not null, id text not null, vote integer not null, primary key ( voter, id ) )")
}

func forum_by_id(u *User, id string) *Forum {
	db := db_app(u, "forums", "data.db", forums_db_create)
	var f Forum
	if db.scan(&f, "select * from forums where id=?", id) {
		return &f
	}
	return nil
}

// New comment
func forums_comment_create(u *User, a *Action) {
	db := a.db("data.db")

	f := forum_by_id(u, a.input("forum"))
	if f == nil {
		a.error(404, "Forum not found")
		return
	}

	post := a.input("post")
	if !db.exists("select id from posts where id=? and forum=?", post, f.ID) {
		a.error(404, "Post not found")
		return
	}
	parent := a.input("parent")
	if parent != "" && !db.exists("select id from comments where id=? and post=?", parent, post) {
		a.error(404, "Parent not found")
		return
	}
	body := a.input("body")
	if !valid(body, "text") {
		a.error(400, "Invalid body")
		return
	}

	if f.Master == "" {
		id := uid()
		db.exec("replace into comments ( id, forum, post, parent, time, author, name, body ) values ( ?, ?, ?, ?, ?, ?, ?, ? )", id, f.ID, post, parent, time_unix(), u.Identity.ID, u.Identity.Name, body)
	} else {
		e := Event{ID: uid(), From: u.Identity.ID, To: f.Master, App: "forums", Action: "comment/create", Content: json_encode(map[string]string{"body": body})}
		e.send()
	}

	a.template("forums/comment/create", map[string]any{"Forum": f, "Post": post})
}

// Enter details for new comment
func forums_comment_new(u *User, a *Action) {
	a.template("forums/comment/new", map[string]any{"Forum": forum_by_id(u, a.input("forum")), "Post": a.input("post"), "Parent": a.input("parent")})
}

// Create new forum
func forums_create(u *User, a *Action) {
	name := a.input("name")
	if !valid(name, "name") {
		a.error(400, "Invalid name")
		return
	}
	if !valid(a.input("privacy"), "^(public|private)$") || !valid(a.input("joining"), "^(anyone|moderated)$") || !valid(a.input("viewing"), "^(anyone|members)$") || !valid(a.input("posting"), "^(members|moderated)$") {
		a.error(400, "Invalid input")
		return
	}

	i, err := identity_create(u, "forum", name, a.input("privacy"))
	if err != nil {
		a.error(500, "Unable to create identity: %s", err)
		return
	}
	a.db("data.db").exec("replace into forums ( id, master, name, joining, viewing, posting, updated ) values ( ?, '', ?, ?, ?, ?, ? )", i.ID, name, a.input("joining"), a.input("viewing"), a.input("posting"), time_unix())

	a.template("forums/create", i.ID)
}

// List existing forums
func forums_list(u *User, a *Action) {
	var f []Forum
	a.db("data.db").scans(&f, "select * from forums order by name")
	a.write(a.input("format"), "forums/list", f)
}

// Enter details for new forum to be created
func forums_new(u *User, a *Action) {
	a.template("forums/new")
}

// New post
func forums_post_create(u *User, a *Action) {
	db := a.db("data.db")

	f := forum_by_id(u, a.input("forum"))
	if f == nil {
		a.error(404, "Forum not found")
		return
	}

	title := a.input("title")
	if !valid(a.input("title"), "line") {
		a.error(400, "Invalid title")
		return
	}
	body := a.input("body")
	if !valid(body, "text") {
		a.error(400, "Invalid body")
		return
	}

	if f.Master == "" {
		id := uid()
		db.exec("replace into posts ( id, forum, time, status, author, name, title, body ) values ( ?, ?, ?, 'posted', ?, ?, ?, ? )", id, f.ID, time_unix(), u.Identity.ID, u.Identity.Name, title, body)
	} else {
		e := Event{ID: uid(), From: u.Identity.ID, To: f.Master, App: "forums", Action: "post/create", Content: json_encode(map[string]string{"title": title, "body": body})}
		e.send()
	}

	a.template("forums/post/create", f)
}

// Enter details for new post
func forums_post_new(u *User, a *Action) {
	a.template("forums/post/new", forum_by_id(u, a.input("forum")))
}

// View a post
func forums_post_view(u *User, a *Action) {
	db := a.db("data.db")

	f := forum_by_id(u, a.input("forum"))
	if f == nil {
		a.error(404, "Forum not found")
		return
	}

	var p ForumPost
	if !db.scan(&p, "select * from posts where forum=? and id=?", f.ID, a.input("post")) {
		a.error(404, "Post not found")
		return
	}

	//TODO Nested comments
	var c []ForumComment
	db.scans(&c, "select * from comments where forum=? and post=? order by time desc", f.ID, a.input("post"))

	a.template("forums/post/view", map[string]any{"Forum": forum_by_id(u, a.input("forum")), "Post": p, "Comments": c})
}

// Enter details of forums to be subscribed to
func forums_subscribe(u *User, a *Action) {
	//TODO
	a.template("forums/subscribe")
}

// Subscribe to a forum
func forums_subscribed(u *User, a *Action) {
	f := forum_by_id(u, a.input("forum"))
	if f == nil {
		a.error(404, "Forum not found")
		return
	}

	if f.Master == "" {
	} else {
	}

	//TODO
	a.template("forums/subscribed")
}

// View a forum
func forums_view(u *User, a *Action) {
	db := a.db("data.db")
	var p []ForumPost
	db.scans(&p, "select * from posts where forum=? order by time desc", a.input("id"))
	a.template("forums/view", map[string]any{"Forum": forum_by_id(u, a.input("id")), "Posts": &p})
}

// Received a forum post from another user
func forums_event_post(u *User, e *Event) {
}
