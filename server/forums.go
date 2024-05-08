// Comms: Forums app
// Copyright Alistair Cunningham 2024

package main

type Forum struct {
	ID        string
	Name      string
	View      string
	Subscribe string
	Post      string
	Updated   int64
	Identity  *Identity
}

type ForumMember struct {
	Forum string
	ID    string
	Name  string
	Role  string
}

type ForumPost struct {
	ID            string
	Forum         string `json:"-"`
	Created       int64
	CreatedString string `json:"-"`
	Updated       int64
	Status        string
	Author        string
	Name          string
	Title         string
	Body          string
	Up            int
	Down          int
}

type ForumComment struct {
	ID            string
	Forum         string `json:"-"`
	Post          string
	Parent        string
	Created       int64
	CreatedString string `json:"-"`
	Author        string
	Name          string
	Body          string
	Up            int
	Down          int
	Children      *[]ForumComment `json:"-"`
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
	a.register_action("forums/comment/vote", forums_comment_vote, true)
	a.register_action("forums/find", forums_find, false)
	a.register_action("forums/post/create", forums_post_create, true)
	a.register_action("forums/post/new", forums_post_new, true)
	a.register_action("forums/post/view", forums_post_view, true)
	a.register_action("forums/post/vote", forums_post_vote, true)
	a.register_action("forums/search", forums_search, false)
	a.register_action("forums/subscribe", forums_subscribe, true)
	a.register_action("forums/unsubscribe", forums_unsubscribe, true)
	a.register_action("forums/view", forums_view, true)
	a.register_event("comment/create", forums_comment_create_event, true)
	a.register_event("comment/submit", forums_comment_submit_event, true)
	a.register_event("comment/vote", forums_comment_vote_event, true)
	a.register_event("post/create", forums_post_create_event, true)
	a.register_event("post/submit", forums_post_submit_event, true)
	a.register_event("post/vote", forums_post_vote_event, true)
	a.register_event("subscribe", forums_subscribe_event, true)
	a.register_event("unsubscribe", forums_unsubscribe_event, true)
}

// Create app database
func forums_db_create(db *DB) {
	db.exec("create table settings ( name text not null primary key, value text not null )")
	db.exec("replace into settings ( name, value ) values ( 'schema', 1 )")

	db.exec("create table forums ( id text not null primary key, name text not null, view text not null default '', subscribe text not null default '', post text not null default '', updated integer not null )")
	db.exec("create index forums_name on forums( name )")
	db.exec("create index forums_updated on forums( updated )")

	db.exec("create table members ( forum references forums( id ), id text not null, name text not null, role text not null default 'poster', primary key ( forum, id ) )")
	db.exec("create index members_id on members( id )")

	db.exec("create table posts ( id text not null primary key, forum references forum( id ), created integer not null, updated integer not null, status text not null, author text not null, name text not null, title text not null, body text not null, up integer not null default 1, down integer not null default 0 )")
	db.exec("create index posts_forum on posts( forum )")
	db.exec("create index posts_created on posts( created )")
	db.exec("create index posts_updated on posts( updated )")
	db.exec("create index posts_status on posts( status )")

	db.exec("create table comments ( id text not null primary key, forum references forum( id ), post text not null, parent text not null, created integer not null, author text not null, name text not null, body text not null, up integer not null default 1, down integer not null default 0 )")
	db.exec("create index comments_forum on comments( forum )")
	db.exec("create index comments_post on comments( post )")
	db.exec("create index comments_parent on comments( parent )")
	db.exec("create index comments_created on comments( created )")

	db.exec("create table votes ( voter text not null, id text not null, vote integer not null, primary key ( voter, id ) )")
}

func forum_by_id(u *User, id string, owner bool) *Forum {
	db := db_app(u, "forums", "data.db", forums_db_create)

	var f Forum
	if !db.scan(&f, "select * from forums where id=?", id) {
		log_debug("Forum not found")
		return nil
	}

	f.Identity = identity_by_id(u, f.ID)
	if f.Identity == nil && owner {
		log_debug("Forum identity not found and owner required")
		return nil
	}

	return &f
}

// New comment
func forums_comment_create(u *User, a *Action) {
	db := db_app(u, "forums", "data.db", forums_db_create)
	defer db.close()

	f := forum_by_id(u, a.input("forum"), false)
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

	id := uid()
	now := now()
	db.exec("replace into comments ( id, forum, post, parent, created, author, name, body ) values ( ?, ?, ?, ?, ?, ?, ?, ? )", id, f.ID, post, parent, now, u.Identity.ID, u.Identity.Name, body)
	db.exec("update posts set updated=? where id=?", now, post)
	db.exec("update forums set updated=? where id=?", now, f.ID)

	if f.Identity == nil {
		// We are not forum owner, so send to the owner
		e := Event{ID: id, From: u.Identity.ID, To: f.ID, App: "forums", Action: "comment/submit", Content: json_encode(ForumComment{ID: id, Post: post, Parent: parent, Body: body})}
		e.send()

	} else {
		// We are the forum owner, to send to all members except us
		j := json_encode(ForumComment{ID: id, Post: post, Parent: parent, Created: now, Author: u.Identity.ID, Name: u.Identity.Name, Body: body})
		var ms []ForumMember
		db.scans(&ms, "select * from members where forum=? and role!='pending'", f.ID)
		for _, m := range ms {
			if m.ID != u.Identity.ID {
				e := Event{ID: id, From: f.ID, To: m.ID, App: "forums", Action: "comment/create", Content: j}
				e.send(f.Identity.Private)
			}
		}
	}

	a.template("forums/comment/create", map[string]any{"Forum": f, "Post": post})
}

// Received a forum comment from owner
func forums_comment_create_event(u *User, e *Event) {
	log_debug("Forum receieved comment create event '%#v'", e)
	db := db_app(u, "forums", "data.db", forums_db_create)
	defer db.close()

	f := forum_by_id(u, e.From, false)
	if f == nil {
		log_info("Forum dropping comment to unknown forum")
		return
	}
	if e.From != f.ID {
		log_info("Forum dropping comment claiming to be from owner but isn't '%s'!='%s'", e.From, f.ID)
		return
	}

	var c ForumComment
	if !json_decode(e.Content, &c) {
		log_info("Forum dropping comment with invalid JSON content '%s'", e.Content)
		return
	}
	if !valid(c.Author, "public") {
		log_info("Forum dropping comment with invalid author '%s'", c.Author)
		return
	}
	if !valid(c.Name, "name") {
		log_info("Forum dropping comment with invalid name '%s'", c.Name)
		return
	}
	if !valid(c.Body, "text") {
		log_info("Forum dropping comment with invalid body '%s'", c.Body)
		return
	}

	db.exec("replace into comments ( id, forum, post, parent, created, author, name, body ) values ( ?, ?, ?, ?, ?, ?, ?, ? )", e.ID, f.ID, c.Post, c.Parent, c.Created, c.Author, c.Name, c.Body)
	db.exec("update posts set updated=? where id=?", c.Created, c.Post)
	db.exec("update forums set updated=? where id=?", c.Created, f.ID)
}

// Received a forum comment from member
func forums_comment_submit_event(u *User, e *Event) {
	log_debug("Forum receieved comment submit event '%#v'", e)
	db := db_app(u, "forums", "data.db", forums_db_create)
	defer db.close()

	if db.exists("select id from comments where id=?", e.ID) {
		log_info("Forum dropping comment with duplicate ID '%s'", e.ID)
		return
	}

	f := forum_by_id(u, e.To, true)
	if f == nil {
		log_info("Forum dropping comment to unknown forum, or forum not owned by us")
		return
	}

	var c ForumComment
	if !json_decode(e.Content, &c) {
		log_info("Forum dropping comment with invalid JSON content '%s'", e.Content)
		return
	}
	if !db.exists("select id from posts where forum=? and id=?", f.ID, c.Post) {
		log_info("Forum dropping comment for unknown post '%s'", c.Post)
		return
	}
	if c.Parent != "" && !db.exists("select id from comments where forum=? and post=? and id=?", f.ID, c.Post, c.Parent) {
		log_info("Forum dropping comment with unknown parent '%s'", c.Parent)
		return
	}
	var m ForumMember
	if !db.scan(&m, "select * from members where forum=? and id=? and role!='pending'", f.ID, e.From) {
		log_info("Forum dropping comment from unknown or pending member '%s'", e.From)
		return
	}
	c.Created = now()
	c.Author = e.From
	c.Name = m.Name
	if !valid(c.Body, "text") {
		log_info("Forum dropping comment with invalid body '%s'", c.Body)
		return
	}

	db.exec("replace into comments ( id, forum, post, parent, created, author, name, body ) values ( ?, ?, ?, ?, ?, ?, ?, ? )", e.ID, f.ID, c.Post, c.Parent, c.Created, c.Author, c.Name, c.Body)
	db.exec("update posts set updated=? where id=?", c.Created, c.Post)
	db.exec("update forums set updated=? where id=?", c.Created, f.ID)

	j := json_encode(c)
	var ms []ForumMember
	db.scans(&ms, "select * from members where forum=? and role!='pending'", f.ID)
	for _, m := range ms {
		if m.ID != e.From && m.ID != u.Identity.ID {
			e := Event{ID: e.ID, From: f.ID, To: m.ID, App: "forums", Action: "comment/create", Content: j}
			e.send(f.Identity.Private)
		}
	}
}

// Enter details for new comment
func forums_comment_new(u *User, a *Action) {
	a.template("forums/comment/new", map[string]any{"Forum": forum_by_id(u, a.input("forum"), false), "Post": a.input("post"), "Parent": a.input("parent")})
}

// Vote on a comment
func forums_comment_vote(u *User, a *Action) {
	// TODO Vote on a comment
	a.template("forums/comment/vote")
}

// Get comments recursively
func forum_comments(u *User, db *DB, f *Forum, p *ForumPost, parent *ForumComment, depth int) *[]ForumComment {
	if depth > 1000 {
		return nil
	}

	id := ""
	if parent != nil {
		id = parent.ID
	}
	var cs []ForumComment
	db.scans(&cs, "select * from comments where forum=? and post=? and parent=? order by created desc", f.ID, p.ID, id)
	for j, c := range cs {
		cs[j].CreatedString = u.time_local(c.Created)
		cs[j].Children = forum_comments(u, db, f, p, &c, depth+1)
	}
	return &cs
}

// Received a forum comment vote from another user
func forums_comment_vote_event(u *User, e *Event) {
	log_debug("Forum receieved comment vote event '%#v'", e)
	// TODO Receive forum comment vote
}

// Create new forum
func forums_create(u *User, a *Action) {
	db := db_app(u, "forums", "data.db", forums_db_create)
	defer db.close()

	name := a.input("name")
	if !valid(name, "name") {
		a.error(400, "Invalid name")
		return
	}
	if !valid(a.input("privacy"), "^(public|private)$") || !valid(a.input("view"), "^(anyone|members)$") || !valid(a.input("subscribe"), "^(anyone|moderated)$") || !valid(a.input("post"), "^(members|moderated)$") {
		a.error(400, "Invalid input")
		return
	}

	i, err := identity_create(u, "forum", name, a.input("privacy"))
	if err != nil {
		a.error(500, "Unable to create identity: %s", err)
		return
	}
	db.exec("replace into forums ( id, name, view, subscribe, post, updated ) values ( ?, ?, ?, ?, ?, ? )", i.ID, name, a.input("view"), a.input("subscribe"), a.input("post"), now())
	db.exec("replace into members ( forum, id, name, role ) values ( ?, ?, ?, 'administrator' )", i.ID, u.Identity.ID, u.Identity.Name)

	a.template("forums/create", i.ID)
}

// Enter details of forums to be subscribed to
func forums_find(u *User, a *Action) {
	//TODO Find private forums
	a.template("forums/find")
}

// List existing forums
func forums_list(u *User, a *Action) {
	db := db_app(u, "forums", "data.db", forums_db_create)
	defer db.close()

	var f []Forum
	db.scans(&f, "select * from forums order by updated desc")
	a.write(a.input("format"), "forums/list", f)
}

// Enter details for new forum to be created
func forums_new(u *User, a *Action) {
	a.template("forums/new")
}

// New post
func forums_post_create(u *User, a *Action) {
	db := db_app(u, "forums", "data.db", forums_db_create)
	defer db.close()

	f := forum_by_id(u, a.input("forum"), false)
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

	id := uid()
	now := now()
	db.exec("replace into posts ( id, forum, created, updated, status, author, name, title, body ) values ( ?, ?, ?, ?, 'posted', ?, ?, ?, ? )", id, f.ID, now, now, u.Identity.ID, u.Identity.Name, title, body)
	db.exec("update forums set updated=? where id=?", now, f.ID)

	if f.Identity == nil {
		// We are not forum owner, so send to the owner
		log_debug("Sending post to forum owner")
		e := Event{ID: id, From: u.Identity.ID, To: f.ID, App: "forums", Action: "post/submit", Content: json_encode(ForumPost{ID: id, Title: title, Body: body})}
		e.send()

	} else {
		// We are the forum owner, to send to all members except us
		log_debug("Sending post to forum members. u='%#v', f='%#v'", u, f)
		j := json_encode(ForumPost{ID: id, Created: now, Status: "posted", Author: u.Identity.ID, Name: u.Identity.Name, Title: title, Body: body})
		var ms []ForumMember
		db.scans(&ms, "select * from members where forum=? and role!='pending'", f.ID)
		for _, m := range ms {
			if m.ID != u.Identity.ID {
				e := Event{ID: id, From: f.ID, To: m.ID, App: "forums", Action: "post/create", Content: j}
				e.send(f.Identity.Private)
			}
		}
	}

	a.template("forums/post/create", ForumPost{ID: id, Forum: f.ID})
}

// Received a forum post from the owner
func forums_post_create_event(u *User, e *Event) {
	log_debug("Forum receieved post create event '%#v'", e)
	db := db_app(u, "forums", "data.db", forums_db_create)
	defer db.close()

	f := forum_by_id(u, e.From, false)
	if f == nil {
		log_info("Forum dropping post to unknown forum")
		return
	}
	if e.From != f.ID {
		log_info("Forum dropping post claiming to be from owner but isn't '%s'!='%s'", e.From, f.ID)
		return
	}

	var p ForumPost
	if !json_decode(e.Content, &p) {
		log_info("Forum dropping post with invalid JSON content '%s'", e.Content)
		return
	}
	if !valid(p.Author, "public") {
		log_info("Forum dropping post with invalid author '%s'", p.Author)
		return
	}
	if !valid(p.Name, "name") {
		log_info("Forum dropping post with invalid name '%s'", p.Name)
		return
	}
	if !valid(p.Title, "line") {
		log_info("Forum dropping post with invalid title '%s'", p.Title)
		return
	}
	if !valid(p.Body, "text") {
		log_info("Forum dropping post with invalid body '%s'", p.Body)
		return
	}

	db.exec("replace into posts ( id, forum, created, updated, status, author, name, title, body ) values ( ?, ?, ?, ?, 'posted', ?, ?, ?, ? )", e.ID, f.ID, p.Created, p.Created, p.Author, p.Name, p.Title, p.Body)
	db.exec("update forums set updated=? where id=?", now(), f.ID)
}

// Enter details for new post
func forums_post_new(u *User, a *Action) {
	a.template("forums/post/new", forum_by_id(u, a.input("forum"), false))
}

// Received a forum post from a member
func forums_post_submit_event(u *User, e *Event) {
	log_debug("Forum receieved post submit event '%#v'", e)
	db := db_app(u, "forums", "data.db", forums_db_create)
	defer db.close()

	if db.exists("select id from posts where id=?", e.ID) {
		log_info("Forum dropping post with duplicate ID '%s'", e.ID)
		return
	}

	f := forum_by_id(u, e.To, true)
	if f == nil {
		log_info("Forum dropping post to unknown forum, or forum not owned by us")
		return
	}

	var p ForumPost
	if !json_decode(e.Content, &p) {
		log_info("Forum dropping post with invalid JSON content '%s'", e.Content)
		return
	}
	var m ForumMember
	if !db.scan(&m, "select * from members where forum=? and id=? and role!='pending'", f.ID, e.From) {
		log_info("Forum dropping post from unknown or pending member '%s'", e.From)
		return
	}
	p.Created = now()
	p.Status = "pending"
	if f.Post == "members" {
		p.Status = "posted"
	}
	p.Author = e.From
	p.Name = m.Name
	if !valid(p.Title, "line") {
		log_info("Forum dropping post with invalid title '%s'", p.Title)
		return
	}
	if !valid(p.Body, "text") {
		log_info("Forum dropping post with invalid body '%s'", p.Body)
		return
	}

	db.exec("replace into posts ( id, forum, created, updated, status, author, name, title, body ) values ( ?, ?, ?, ?, ?, ?, ?, ?, ? )", e.ID, f.ID, p.Created, p.Created, p.Status, p.Author, p.Name, p.Title, p.Body)
	db.exec("update forums set updated=? where id=?", now(), f.ID)

	j := json_encode(p)
	var ms []ForumMember
	db.scans(&ms, "select * from members where forum=? and role!='pending'", f.ID)
	for _, m := range ms {
		if m.ID != e.From && m.ID != u.Identity.ID {
			e := Event{ID: e.ID, From: f.ID, To: m.ID, App: "forums", Action: "post/create", Content: j}
			e.send(f.Identity.Private)
		}
	}
}

// Vote on a post
func forums_post_vote(u *User, a *Action) {
	// TODO Vote on a post
	a.template("forums/post/vote")
}

// Received a forum post vote from another user
func forums_post_vote_event(u *User, e *Event) {
	log_debug("Forum receieved post vote event '%#v'", e)
	// TODO Receive forum post vote
}

// View a post
func forums_post_view(u *User, a *Action) {
	db := db_app(u, "forums", "data.db", forums_db_create)
	defer db.close()

	f := forum_by_id(u, a.input("forum"), false)
	if f == nil {
		a.error(404, "Forum not found")
		return
	}

	var p ForumPost
	if !db.scan(&p, "select * from posts where forum=? and id=?", f.ID, a.input("post")) {
		a.error(404, "Post not found")
		return
	}
	p.CreatedString = u.time_local(p.Created)

	a.template("forums/post/view", map[string]any{"Forum": f, "Post": p, "Comments": forum_comments(u, db, f, &p, nil, 0)})
}

// Search for a forum
func forums_search(u *User, a *Action) {
	search := a.input("search")
	if search == "" {
		a.error(400, "No search entered")
		return
	}
	a.template("forums/search", directory_search(u, "forum", search, false))
}

// Subscribe to a forum
func forums_subscribe(u *User, a *Action) {
	db := db_app(u, "forums", "data.db", forums_db_create)
	defer db.close()

	id := a.input("id")
	if !valid(id, "public") {
		a.error(400, "Invalid ID")
		return
	}
	f := forum_by_id(u, a.input("id"), false)
	if f != nil {
		a.error(400, "You are already subscribed to this forum")
		return
	}
	name := a.input("name")
	if !valid(name, "name") {
		a.error(400, "Invalid name")
		return
	}

	db.exec("replace into forums ( id, name, updated ) values ( ?, ?, ? )", id, name, now())
	e := Event{ID: uid(), From: u.Identity.ID, To: id, App: "forums", Action: "subscribe", Content: json_encode(map[string]string{"name": u.Identity.Name})}
	e.send()

	a.template("forums/subscribe", id)
}

// Received a subscribe from a member
func forums_subscribe_event(u *User, e *Event) {
	log_debug("Forum receieved subscribe event '%#v'", e)
	db := db_app(u, "forums", "data.db", forums_db_create)
	defer db.close()

	f := forum_by_id(u, e.To, true)
	if f == nil {
		return
	}

	name := "Unknown"
	var m ForumMember
	if json_decode(e.Content, &m) {
		name = m.Name
	}

	role := "member"
	if f.Subscribe != "anyone" {
		role = "pending"
	}

	db.exec("insert or ignore into members ( forum, id, name, role ) values ( ?, ?, ?, ? )", f.ID, e.From, name, role)
	db.exec("update forums set updated=? where id=?", now(), f.ID)

	var ps []ForumPost
	db.scans(&ps, "select * from posts where forum=? order by updated desc limit 100", f.ID)
	for _, p := range ps {
		e := Event{ID: p.ID, From: f.ID, To: e.From, App: "forums", Action: "post/create", Content: json_encode(p)}
		e.send(f.Identity.Private)
	}
	for _, p := range ps {
		var cs []ForumComment
		db.scans(&cs, "select * from comments where post=?", p.ID)
		for _, c := range cs {
			e := Event{ID: c.ID, From: f.ID, To: e.From, App: "forums", Action: "comment/create", Content: json_encode(c)}
			e.send(f.Identity.Private)
		}
	}
}

// Unsubscribe from forum
func forums_unsubscribe(u *User, a *Action) {
	//TODO Unsubscribe from forum
}

// Received an unsubscribe from another user
func forums_unsubscribe_event(u *User, e *Event) {
	log_debug("Forum receieved unsubscribe event '%#v'", e)
	// TODO Receive forum unsubscribe
}

// View a forum
func forums_view(u *User, a *Action) {
	db := db_app(u, "forums", "data.db", forums_db_create)
	defer db.close()

	f := forum_by_id(u, a.input("id"), false)
	if f == nil {
		a.error(404, "Forum not found")
		return
	}

	var p []ForumPost
	db.scans(&p, "select * from posts where forum=? order by updated desc", f.ID)

	a.template("forums/view", map[string]any{"Forum": f, "Posts": &p})
}
