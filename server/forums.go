// Comms: Forums app
// Copyright Alistair Cunningham 2024-2025

package main

type Forum struct {
	ID          string
	Fingerprint string
	Name        string
	Role        string
	Members     int
	Updated     int64
	identity    *Identity
}

type ForumMember struct {
	Forum     string
	ID        string
	Name      string
	Role      string
	ForumName string `json:"-"`
}

type ForumPost struct {
	ID            string
	Forum         string `json:"-"`
	Created       int64
	CreatedString string `json:"-"`
	Updated       int64
	Author        string
	Name          string
	Type          string
	Title         string
	Body          string
	Link          string
	Comments      int
	Up            int
	Down          int
	ForumName     string `json:"-"`
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
	RoleVoter     bool            `json:"-"`
	RoleCommenter bool            `json:"-"`
}

type ForumVote struct {
	Voter string
	Class string
	ID    string
	Vote  string
}

var (
	forum_roles = map[string]int{"disabled": 0, "viewer": 1, "voter": 2, "commenter": 3, "poster": 4, "administrator": 5}
)

func init() {
	a := app("forums")
	a.home("forums", map[string]string{"en": "Forums"})
	a.db("forums.db", forums_db_create)

	a.path("forums", forums_list)
	a.path("forums/create", forums_create)
	a.path("forums/find", forums_find)
	a.path("forums/new", forums_new)
	a.path("forums/search", forums_search)
	a.path("forums/:entity", forums_view)
	a.path("forums/:entity/create", forums_post_create)
	a.path("forums/:entity/members", forums_members_edit)
	a.path("forums/:entity/members/save", forums_members_save)
	a.path("forums/:entity/post", forums_post_new)
	a.path("forums/:entity/subscribe", forums_subscribe)
	a.path("forums/:entity/unsubscribe", forums_unsubscribe)
	a.path("forums/:entity/:post", forums_post_view)
	a.path("forums/:entity/:post/comment", forums_comment_new)
	a.path("forums/:entity/:post/create", forums_comment_create)
	a.path("forums/:entity/:post/vote/:vote", forums_post_vote)
	a.path("forums/:entity/:post/:comment/vote/:vote", forums_comment_vote)

	a.service("forums")
	a.event("comment/create", forums_comment_create_event)
	a.event("comment/submit", forums_comment_submit_event)
	a.event("comment/update", forums_comment_update_event)
	a.event("comment/vote", forums_comment_vote_event)
	a.event("member/update", forums_member_update_event)
	a.event("post/create", forums_post_create_event)
	a.event("post/submit", forums_post_submit_event)
	a.event("post/update", forums_post_update_event)
	a.event("post/vote", forums_post_vote_event)
	a.event("subscribe", forums_subscribe_event)
	a.event("unsubscribe", forums_unsubscribe_event)
	a.event("update", forums_update_event)
}

// Create app database
func forums_db_create(db *DB) {
	db.exec("create table settings ( name text not null primary key, value text not null )")
	db.exec("replace into settings ( name, value ) values ( 'schema', 1 )")

	db.exec("create table forums ( id text not null primary key, fingerprint text not null, name text not null, role text not null default 'disabled', members integer not null default 0, updated integer not null )")
	db.exec("create index forums_fingerprint on forums( fingerprint )")
	db.exec("create index forums_name on forums( name )")
	db.exec("create index forums_updated on forums( updated )")

	db.exec("create table members ( forum references forums( id ), id text not null, name text not null default '', role text not null, primary key ( forum, id ) )")
	db.exec("create index members_id on members( id )")

	db.exec("create table posts ( id text not null primary key, forum references forum( id ), created integer not null, updated integer not null, author text not null, name text not null, type text not null default 'text', title text not null, body text not null, link text not null default '', comments integer not null default 0, up integer not null default 0, down integer not null default 0 )")
	db.exec("create index posts_forum on posts( forum )")
	db.exec("create index posts_created on posts( created )")
	db.exec("create index posts_updated on posts( updated )")

	db.exec("create table comments ( id text not null primary key, forum references forum( id ), post text not null, parent text not null, created integer not null, author text not null, name text not null, body text not null, up integer not null default 0, down integer not null default 0 )")
	db.exec("create index comments_forum on comments( forum )")
	db.exec("create index comments_post on comments( post )")
	db.exec("create index comments_parent on comments( parent )")
	db.exec("create index comments_created on comments( created )")

	db.exec("create table votes ( voter text not null, forum references forum( id ), class text not null, id text not null, vote text not null, primary key ( voter, class, id ) )")
	db.exec("create index votes_forum on votes( forum )")
}

func forum_by_id(u *User, db *DB, id string) *Forum {
	var f Forum
	if !db.scan(&f, "select * from forums where id=?", id) {
		if !db.scan(&f, "select * from forums where fingerprint=?", id) {
			return nil
		}
	}
	if u != nil {
		f.identity = identity_by_user_id(u, f.ID)
	}
	return &f
}

// Get comments recursively
func forum_comments(u *User, db *DB, f *Forum, m *ForumMember, p *ForumPost, parent *ForumComment, depth int) *[]ForumComment {
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
		cs[j].CreatedString = time_local(u, c.Created)
		cs[j].Children = forum_comments(u, db, f, m, p, &c, depth+1)
		cs[j].RoleVoter = forum_role(m, "voter")
		cs[j].RoleCommenter = forum_role(m, "commenter")
	}
	return &cs
}

// New comment
func forums_comment_create(a *Action) {
	now := now()

	if a.user == nil {
		a.error(401, "Not logged in")
		return
	}

	f := forum_by_id(a.user, a.db, a.id())
	if f == nil {
		a.error(404, "Forum not found")
		return
	}

	post := a.input("post")
	if !a.db.exists("select id from posts where id=? and forum=?", post, f.ID) {
		a.error(404, "Post not found")
		return
	}

	parent := a.input("parent")
	if parent != "" && !a.db.exists("select id from comments where id=? and post=?", parent, post) {
		a.error(404, "Parent not found")
		return
	}

	body := a.input("body")
	if !valid(body, "text") {
		a.error(400, "Invalid body")
		return
	}

	id := uid()
	if a.db.exists("select id from comments where id=?", id) {
		a.error(500, "Duplicate ID")
		return
	}

	a.db.exec("replace into comments ( id, forum, post, parent, created, author, name, body ) values ( ?, ?, ?, ?, ?, ?, ?, ? )", id, f.ID, post, parent, now, a.user.Identity.ID, a.user.Identity.Name, body)
	a.db.exec("update posts set updated=?, comments=comments+1 where id=?", now, post)
	a.db.exec("update forums set updated=? where id=?", now, f.ID)

	if f.identity == nil {
		// We are not forum owner, so send to the owner
		e := Event{ID: id, From: a.user.Identity.ID, To: f.ID, Service: "forums", Action: "comment/submit", Content: json_encode(ForumComment{ID: id, Post: post, Parent: parent, Body: body})}
		e.send()

	} else {
		// We are the forum owner, to send to all members except us
		j := json_encode(ForumComment{ID: id, Post: post, Parent: parent, Created: now, Author: a.user.Identity.ID, Name: a.user.Identity.Name, Body: body})
		var ms []ForumMember
		a.db.scans(&ms, "select * from members where forum=? and role!='disabled'", f.ID)
		for _, m := range ms {
			if m.ID != a.user.Identity.ID {
				e := Event{ID: id, From: f.ID, To: m.ID, Service: "forums", Action: "comment/create", Content: j}
				e.send()
			}
		}
	}

	a.template("forums/comment/create", Map{"Forum": f, "Post": post})
}

// Received a forum comment from owner
func forums_comment_create_event(e *Event) {
	log_debug("Forum receieved comment create event '%#v'", e)

	f := forum_by_id(e.user, e.db, e.From)
	if f == nil {
		log_info("Forum dropping comment to unknown forum")
		return
	}

	var c ForumComment
	if !json_decode(&c, e.Content) {
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

	if e.db.exists("select id from comments where id=?", e.ID) {
		log_info("Forum dropping comment with duplicate ID '%s'", e.ID)
		return
	}

	e.db.exec("replace into comments ( id, forum, post, parent, created, author, name, body ) values ( ?, ?, ?, ?, ?, ?, ?, ? )", e.ID, f.ID, c.Post, c.Parent, c.Created, c.Author, c.Name, c.Body)
	e.db.exec("update posts set updated=?, comments=comments+1 where id=?", c.Created, c.Post)
	e.db.exec("update forums set updated=? where id=?", c.Created, f.ID)
}

// Received a forum comment from member
func forums_comment_submit_event(e *Event) {
	log_debug("Forum receieved comment submit event '%#v'", e)

	if e.db.exists("select id from comments where id=?", e.ID) {
		log_info("Forum dropping comment with duplicate ID '%s'", e.ID)
		return
	}

	f := forum_by_id(e.user, e.db, e.To)
	if f == nil {
		log_info("Forum dropping comment to unknown forum")
		return
	}

	var c ForumComment
	if !json_decode(&c, e.Content) {
		log_info("Forum dropping comment with invalid JSON content '%s'", e.Content)
		return
	}
	if !e.db.exists("select id from posts where forum=? and id=?", f.ID, c.Post) {
		log_info("Forum dropping comment for unknown post '%s'", c.Post)
		return
	}
	if c.Parent != "" && !e.db.exists("select id from comments where forum=? and post=? and id=?", f.ID, c.Post, c.Parent) {
		log_info("Forum dropping comment with unknown parent '%s'", c.Parent)
		return
	}
	m := forum_member(e.db, f, e.From, "commenter")
	if m == nil {
		log_info("Forum dropping comment from unknown member '%s'", e.From)
		return
	}
	c.Created = now()
	c.Author = e.From
	c.Name = m.Name
	if !valid(c.Body, "text") {
		log_info("Forum dropping comment with invalid body '%s'", c.Body)
		return
	}

	e.db.exec("replace into comments ( id, forum, post, parent, created, author, name, body ) values ( ?, ?, ?, ?, ?, ?, ?, ? )", e.ID, f.ID, c.Post, c.Parent, c.Created, c.Author, c.Name, c.Body)
	e.db.exec("update posts set updated=?, comments=comments+1 where id=?", c.Created, c.Post)
	e.db.exec("update forums set updated=? where id=?", c.Created, f.ID)

	j := json_encode(c)
	var ms []ForumMember
	e.db.scans(&ms, "select * from members where forum=? and role!='disabled'", f.ID)
	for _, m := range ms {
		if m.ID != e.From && m.ID != e.user.Identity.ID {
			e := Event{ID: e.ID, From: f.ID, To: m.ID, Service: "forums", Action: "comment/create", Content: j}
			e.send()
		}
	}
}

// Enter details for new comment
func forums_comment_new(a *Action) {
	if a.user == nil {
		a.error(401, "Not logged in")
		return
	}

	a.template("forums/comment/new", Map{"Forum": forum_by_id(a.user, a.db, a.id()), "Post": a.input("post"), "Parent": a.input("parent")})
}

// Received a forum comment update event
func forums_comment_update_event(e *Event) {
	log_debug("Forum receieved comment update event '%#v'", e)

	var c ForumComment
	if !json_decode(&c, e.Content) {
		log_info("Forum dropping comment update with invalid JSON content '%s'", e.Content)
		return
	}
	var o ForumComment
	if !e.db.scan(&o, "select * from comments where forum=? and id=?", e.From, c.ID) {
		log_info("Forum dropping comment update for unknown comment")
		return
	}

	now := now()
	e.db.exec("update comments set up=?, down=? where id=?", c.Up, c.Down, o.ID)
	e.db.exec("update posts set updated=? where id=?", now, o.Post)
	e.db.exec("update forums set updated=? where id=?", now, o.Forum)
}

// Vote on a comment
func forums_comment_vote(a *Action) {
	if a.user == nil {
		a.error(401, "Not logged in")
		return
	}

	var c ForumComment
	if !a.db.scan(&c, "select * from comments where id=?", a.input("comment")) {
		a.error(404, "Comment not found")
		return
	}
	f := forum_by_id(a.user, a.db, c.Forum)
	if f == nil {
		a.error(404, "Forum not found")
		return
	}

	vote := a.input("vote")
	forums_comment_vote_set(a.db, &c, a.user.Identity.ID, vote)

	if f.identity == nil {
		// We are not forum owner, so send to the owner
		e := Event{ID: uid(), From: a.user.Identity.ID, To: f.ID, Service: "forums", Action: "comment/vote", Content: json_encode(ForumVote{ID: c.ID, Vote: vote})}
		e.send()

	} else {
		// We are the forum owner, to send to all members except us
		id := uid()
		j := json_encode(c)
		var ms []ForumMember
		a.db.scans(&ms, "select * from members where forum=? and role!='disabled'", f.ID)
		for _, m := range ms {
			if m.ID != a.user.Identity.ID {
				e := Event{ID: id, From: f.ID, To: m.ID, Service: "forums", Action: "comment/update", Content: j}
				e.send()
			}
		}
	}

	a.template("forums/comment/vote", Map{"Forum": f, "Post": c.Post})
}

func forums_comment_vote_set(db *DB, c *ForumComment, voter string, vote string) {
	now := now()

	var o ForumVote
	if db.scan(&o, "select vote from votes where voter=? and class='comment' and id=?", voter, c.ID) {
		switch o.Vote {
		case "up":
			c.Up = c.Up - 1
			db.exec("update comments set up=up-1 where id=?", c.ID)
		case "down":
			c.Down = c.Down - 1
			db.exec("update comments set down=down-1 where id=?", c.ID)
		}
	}

	db.exec("replace into votes ( voter, forum, class, id, vote ) values ( ?, ?, 'comment', ?, ? )", voter, c.Forum, c.ID, vote)
	switch vote {
	case "up":
		c.Up = c.Up + 1
		db.exec("update comments set up=up+1 where id=?", c.ID)
	case "down":
		c.Down = c.Down + 1
		db.exec("update comments set down=down+1 where id=?", c.ID)
	}

	db.exec("update posts set updated=? where id=?", now, c.Post)
	db.exec("update forums set updated=? where id=?", now, c.Forum)
}

// Received a forum comment vote from a member
func forums_comment_vote_event(e *Event) {
	var v ForumVote
	if !json_decode(&v, e.Content) {
		log_info("Forum dropping comment vote with invalid JSON content '%s'", e.Content)
		return
	}

	var c ForumComment
	if !e.db.scan(&c, "select * from comments where id=?", v.ID) {
		log_info("Forum dropping comment vote for unknown comment")
		return
	}
	f := forum_by_id(e.user, e.db, c.Forum)
	if f == nil {
		log_info("Forum dropping comment vote for unknown forum")
		return
	}
	m := forum_member(e.db, f, e.From, "voter")
	if m == nil {
		log_info("Forum dropping comment vote from unknown member '%s'", e.From)
		return
	}

	forums_comment_vote_set(e.db, &c, e.From, v.Vote)

	j := json_encode(c)
	var ms []ForumMember
	e.db.scans(&ms, "select * from members where forum=? and role!='disabled'", f.ID)
	for _, m := range ms {
		if m.ID != e.From && m.ID != e.user.Identity.ID {
			e := Event{ID: e.ID, From: f.ID, To: m.ID, Service: "forums", Action: "comment/update", Content: j}
			e.send()
		}
	}
}

// Create new forum
func forums_create(a *Action) {
	if a.user == nil {
		a.error(401, "Not logged in")
		return
	}

	name := a.input("name")
	if !valid(name, "name") {
		a.error(400, "Invalid name")
		return
	}
	role := a.input("role")
	_, found := forum_roles[role]
	if !found {
		a.error(400, "Invalid role")
		return
	}
	privacy := a.input("privacy")
	if !valid(privacy, "^(public|private)$") {
		a.error(400, "Invalid privacy")
		return
	}

	i, err := identity_create(a.user, "forum", name, privacy, json_encode(Map{"role": role}))
	if err != nil {
		a.error(500, "Unable to create identity: %s", err)
		return
	}
	a.db.exec("replace into forums ( id, fingerprint, name, role, members, updated ) values ( ?, ?, ?, ?, 1, ? )", i.ID, i.Fingerprint, name, role, now())
	a.db.exec("replace into members ( forum, id, name, role ) values ( ?, ?, ?, 'administrator' )", i.ID, a.user.Identity.ID, a.user.Identity.Name)

	a.template("forums/create", i.Fingerprint)
}

// Enter details of forums to be subscribed to
func forums_find(a *Action) {
	a.template("forums/find")
}

// List existing forums
func forums_list(a *Action) {
	var f []Forum
	a.db.scans(&f, "select * from forums order by updated desc")
	a.write(a.input("format"), "forums/list", f)
}

// Get details of a forum member
func forum_member(db *DB, f *Forum, member string, role string) *ForumMember {
	var m ForumMember
	if !db.scan(&m, "select * from members where forum=? and id=?", f.ID, member) {
		return nil
	}
	if !forum_role(&m, role) {
		return nil
	}
	return &m
}

// Edit members
func forums_members_edit(a *Action) {
	if a.user == nil {
		a.error(401, "Not logged in")
		return
	}

	f := forum_by_id(a.user, a.db, a.id())
	if f == nil || f.identity == nil {
		a.error(404, "Forum not found")
		return
	}

	var ms []ForumMember
	a.db.scans(&ms, "select * from members where forum=? order by name", f.ID)

	a.template("forums/members/edit", Map{"User": a.user, "Forum": f, "Members": ms})
}

// Save members
func forums_members_save(a *Action) {
	if a.user == nil {
		a.error(401, "Not logged in")
		return
	}

	f := forum_by_id(a.user, a.db, a.id())
	if f == nil || f.identity == nil {
		a.error(404, "Forum not found")
		return
	}

	var ms []ForumMember
	a.db.scans(&ms, "select * from members where forum=? and id!=?", f.ID, a.user.Identity.ID)
	for _, m := range ms {
		role := a.input("role_" + m.ID)
		if role != m.Role {
			_, found := forum_roles[role]
			if !found {
				a.error(400, "Invalid role")
				return
			}
			a.db.exec("update members set role=? where forum=? and id=?", role, f.ID, m.ID)
			e := Event{ID: uid(), From: f.ID, To: m.ID, Service: "forums", Action: "member/update", Content: json_encode(Map{"role": role})}
			e.send()

			if m.Role == "disabled" {
				forum_send_recent_posts(a.db, f, m.ID)
			}
		}
	}

	forum_update(a.user, a.db, f)
	a.template("forums/members/save", Map{"Forum": f})
}

// Member update from owner
func forums_member_update_event(e *Event) {
	f := forum_by_id(e.user, e.db, e.From)
	if f == nil {
		return
	}

	var m ForumMember
	if !json_decode(&m, e.Content) {
		log_info("Forum dropping member update with invalid JSON content '%s'", e.Content)
		return
	}
	_, found := forum_roles[m.Role]
	if !found {
		log_info("Forum dropping member update with invalid role '%s'", e.Content)
		return
	}

	var o ForumMember
	if e.db.scan(&o, "select * from members where forum=? and id=?", e.From, e.user.Identity.ID) {
		if m.Role != o.Role {
			message := "An error occurred"
			switch m.Role {
			case "disabled":
				message = "You may not access this forum"
			case "voter":
				message = "You may vote, but not post or comment"
			case "commenter":
				message = "You may comment and vote, but not post"
			case "poster":
				message = "You may post, comment, and vote"
			case "administrator":
				message = "You are an administrator"
			}
			notification(e.user, "forums", "member/update", f.ID, "Forum "+f.Name+": "+message, "/forums/"+f.ID)
		}
	}
	e.db.exec("replace into members ( forum, id, role ) values ( ?, ?, ? )", e.From, e.user.Identity.ID, m.Role)
}

// Enter details for new forum to be created
func forums_new(a *Action) {
	a.template("forums/new")
}

// New post
func forums_post_create(a *Action) {
	now := now()

	if a.user == nil {
		a.error(401, "Not logged in")
		return
	}

	f := forum_by_id(a.user, a.db, a.id())
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
	if a.db.exists("select id from comments where id=?", id) {
		a.error(500, "Duplicate ID")
		return
	}

	a.db.exec("replace into posts ( id, forum, created, updated, author, name, title, body ) values ( ?, ?, ?, ?, ?, ?, ?, ? )", id, f.ID, now, now, a.user.Identity.ID, a.user.Identity.Name, title, body)
	a.db.exec("update forums set updated=? where id=?", now, f.ID)

	if f.identity == nil {
		// We are not forum owner, so send to the owner
		log_debug("Sending post to forum owner")
		e := Event{ID: id, From: a.user.Identity.ID, To: f.ID, Service: "forums", Action: "post/submit", Content: json_encode(ForumPost{ID: id, Title: title, Body: body})}
		e.send()

	} else {
		// We are the forum owner, to send to all members except us
		j := json_encode(ForumPost{ID: id, Created: now, Author: a.user.Identity.ID, Name: a.user.Identity.Name, Title: title, Body: body})
		var ms []ForumMember
		a.db.scans(&ms, "select * from members where forum=? and role!='disabled'", f.ID)
		for _, m := range ms {
			if m.ID != a.user.Identity.ID {
				e := Event{ID: id, From: f.ID, To: m.ID, Service: "forums", Action: "post/create", Content: j}
				e.send()
			}
		}
	}

	a.template("forums/post/create", Map{"Forum": f, "ID": id})
}

// Received a forum post from owner
func forums_post_create_event(e *Event) {
	f := forum_by_id(e.user, e.db, e.From)
	if f == nil {
		log_info("Forum dropping post to unknown forum")
		return
	}

	var p ForumPost
	if !json_decode(&p, e.Content) {
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

	if e.db.exists("select id from comments where id=?", e.ID) {
		log_info("Forum dropping post with duplicate ID '%s'", e.ID)
		return
	}

	e.db.exec("replace into posts ( id, forum, created, updated, author, name, title, body, up, down ) values ( ?, ?, ?, ?, ?, ?, ?, ?, ?, ? )", e.ID, f.ID, p.Created, p.Created, p.Author, p.Name, p.Title, p.Body, p.Up, p.Down)
	e.db.exec("update forums set updated=? where id=?", now(), f.ID)
}

// Enter details for new post
func forums_post_new(a *Action) {
	if a.user == nil {
		a.error(401, "Not logged in")
		return
	}

	a.template("forums/post/new", forum_by_id(a.user, a.db, a.id()))
}

// Received a forum post from a member
func forums_post_submit_event(e *Event) {
	if e.db.exists("select id from posts where id=?", e.ID) {
		log_info("Forum dropping post with duplicate ID '%s'", e.ID)
		return
	}

	f := forum_by_id(e.user, e.db, e.To)
	if f == nil {
		log_info("Forum dropping post to unknown forum")
		return
	}

	var p ForumPost
	if !json_decode(&p, e.Content) {
		log_info("Forum dropping post with invalid JSON content '%s'", e.Content)
		return
	}
	m := forum_member(e.db, f, e.From, "poster")
	if m == nil {
		log_info("Forum dropping post from unknown member '%s'", e.From)
		return
	}
	p.Created = now()
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

	e.db.exec("replace into posts ( id, forum, created, updated, author, name, title, body ) values ( ?, ?, ?, ?, ?, ?, ?, ? )", e.ID, f.ID, p.Created, p.Created, p.Author, p.Name, p.Title, p.Body)
	e.db.exec("update forums set updated=? where id=?", now(), f.ID)

	j := json_encode(p)
	var ms []ForumMember
	e.db.scans(&ms, "select * from members where forum=? and role!='disabled'", f.ID)
	for _, m := range ms {
		if m.ID != e.From && m.ID != e.user.Identity.ID {
			e := Event{ID: e.ID, From: f.ID, To: m.ID, Service: "forums", Action: "post/create", Content: j}
			e.send()
		}
	}
}

// Received a forum post update event
func forums_post_update_event(e *Event) {
	var p ForumPost
	if !json_decode(&p, e.Content) {
		log_info("Forum dropping post update with invalid JSON content '%s'", e.Content)
		return
	}
	if !e.db.exists("select id from posts where forum=? and id=?", e.From, p.ID) {
		log_info("Forum dropping post update for unknown post")
		return
	}

	now := now()
	e.db.exec("update posts set updated=?, up=?, down=? where id=?", now, p.Up, p.Down, p.ID)
	e.db.exec("update forums set updated=? where id=?", now, e.From)
}

// View a post
func forums_post_view(a *Action) {
	var p ForumPost
	if !a.db.scan(&p, "select * from posts where id=?", a.input("post")) {
		a.error(404, "Post not found")
		return
	}
	p.CreatedString = time_local(a.user, p.Created)

	f := forum_by_id(a.user, a.db, p.Forum)
	if f == nil {
		a.error(404, "Forum not found")
		return
	}
	var m *ForumMember = nil
	if a.user != nil {
		m = &ForumMember{}
		if !a.db.scan(m, "select * from members where forum=? and id=?", f.ID, a.user.Identity.ID) {
			m = nil
		}
	}
	if m == nil && f.Role == "disabled" {
		a.error(404, "Forum not found")
		return
	}

	a.template("forums/post/view", Map{"Forum": f, "Post": &p, "Comments": forum_comments(a.user, a.db, f, m, &p, nil, 0), "RoleVoter": forum_role(m, "voter"), "RoleCommenter": forum_role(m, "commenter")})
}

// Vote on a post
func forums_post_vote(a *Action) {
	if a.user == nil {
		a.error(401, "Not logged in")
		return
	}

	var p ForumPost
	if !a.db.scan(&p, "select * from posts where id=?", a.input("post")) {
		a.error(404, "Post not found")
		return
	}
	f := forum_by_id(a.user, a.db, p.Forum)
	if f == nil {
		a.error(404, "Forum not found")
		return
	}

	vote := a.input("vote")
	forums_post_vote_set(a.db, &p, a.user.Identity.ID, vote)

	if f.identity == nil {
		// We are not forum owner, so send to the owner
		e := Event{ID: uid(), From: a.user.Identity.ID, To: f.ID, Service: "forums", Action: "post/vote", Content: json_encode(ForumVote{ID: p.ID, Vote: vote})}
		e.send()

	} else {
		// We are the forum owner, to send to all members except us
		id := uid()
		j := json_encode(p)
		var ms []ForumMember
		a.db.scans(&ms, "select * from members where forum=? and role!='disabled'", f.ID)
		for _, m := range ms {
			if m.ID != a.user.Identity.ID {
				e := Event{ID: id, From: f.ID, To: m.ID, Service: "forums", Action: "post/update", Content: j}
				e.send()
			}
		}
	}

	a.template("forums/post/vote", Map{"Forum": f, "ID": p.ID})
}

func forums_post_vote_set(db *DB, p *ForumPost, voter string, vote string) {
	now := now()

	var o ForumVote
	if db.scan(&o, "select vote from votes where voter=? and class='post' and id=?", voter, p.ID) {
		switch o.Vote {
		case "up":
			p.Up = p.Up - 1
			db.exec("update posts set up=up-1, updated=? where id=?", now, p.ID)
		case "down":
			p.Down = p.Down - 1
			db.exec("update posts set down=down-1, updated=? where id=?", now, p.ID)
		}
	}

	db.exec("replace into votes ( voter, forum, class, id, vote ) values ( ?, ?, 'post', ?, ? )", voter, p.Forum, p.ID, vote)
	switch vote {
	case "up":
		p.Up = p.Up + 1
		db.exec("update posts set up=up+1, updated=? where id=?", now, p.ID)
	case "down":
		p.Down = p.Down + 1
		db.exec("update posts set down=down+1, updated=? where id=?", now, p.ID)
	}

	db.exec("update forums set updated=? where id=?", now, p.Forum)
}

// Received a forum post vote from a member
func forums_post_vote_event(e *Event) {
	var v ForumVote
	if !json_decode(&v, e.Content) {
		log_info("Forum dropping post vote with invalid JSON content '%s'", e.Content)
		return
	}

	var p ForumPost
	if !e.db.scan(&p, "select * from posts where id=?", v.ID) {
		log_info("Forum dropping post vote for unknown post")
		return
	}
	f := forum_by_id(e.user, e.db, p.Forum)
	if f == nil {
		log_info("Forum dropping post vote for unknown forum")
		return
	}
	m := forum_member(e.db, f, e.From, "voter")
	if m == nil {
		log_info("Forum dropping post vote from unknown member")
		return
	}

	forums_post_vote_set(e.db, &p, e.From, v.Vote)

	j := json_encode(p)
	var ms []ForumMember
	e.db.scans(&ms, "select * from members where forum=? and role!='disabled'", f.ID)
	for _, m := range ms {
		if m.ID != e.From && m.ID != e.user.Identity.ID {
			e := Event{ID: e.ID, From: f.ID, To: m.ID, Service: "forums", Action: "post/update", Content: j}
			e.send()
		}
	}
}

// Return whether a forum member has a required role or not
func forum_role(m *ForumMember, need string) bool {
	have := "viewer"
	if m != nil {
		have = m.Role
	}

	h, found := forum_roles[have]
	if !found {
		return false
	}
	n, found := forum_roles[need]
	if !found {
		return false
	}

	if h < n {
		return false
	}
	return true
}

// Search for a forum
func forums_search(a *Action) {
	if a.user == nil {
		a.error(401, "Not logged in")
		return
	}

	search := a.input("search")
	if search == "" {
		a.error(400, "No search entered")
		return
	}
	a.template("forums/search", directory_search(a.user, "forum", search, false))
}

// Send recent posts to a member
func forum_send_recent_posts(db *DB, f *Forum, member string) {
	var ps []ForumPost
	db.scans(&ps, "select * from posts where forum=? order by updated desc limit 1000", f.ID)
	for _, p := range ps {
		e := Event{ID: p.ID, From: f.ID, To: member, Service: "forums", Action: "post/create", Content: json_encode(p)}
		e.send()
	}
	for _, p := range ps {
		var cs []ForumComment
		db.scans(&cs, "select * from comments where post=?", p.ID)
		for _, c := range cs {
			e := Event{ID: c.ID, From: f.ID, To: member, Service: "forums", Action: "comment/create", Content: json_encode(c)}
			e.send()
		}
	}
}

// Subscribe to a forum
func forums_subscribe(a *Action) {
	if a.user == nil {
		a.error(401, "Not logged in")
		return
	}

	id := a.id()
	if !valid(id, "public") {
		a.error(400, "Invalid ID")
		return
	}
	if forum_by_id(a.user, a.db, id) != nil {
		a.error(400, "You are already subscribed to this forum")
		return
	}
	d := directory_by_id(id)
	if d == nil {
		a.error(404, "Unable to find forum in directory")
		return
	}
	var m ForumMember
	if !json_decode(&m, d.Data) {
		a.error(400, "Forum directory entry does not contain data")
		return
	}

	a.db.exec("replace into forums ( id, fingerprint, name, members, updated ) values ( ?, ?, ?, 1, ? )", id, fingerprint(id), d.Name, now())
	a.db.exec("replace into members ( forum, id, name, role ) values ( ?, ?, ?, ? )", id, a.user.Identity.ID, a.user.Identity.Name, m.Role)

	e := Event{ID: uid(), From: a.user.Identity.ID, To: id, Service: "forums", Action: "subscribe", Content: json_encode(map[string]string{"name": a.user.Identity.Name})}
	e.send()

	a.template("forums/subscribe", Map{"Forum": id, "Role": m.Role})
}

// Received a subscribe from a member
func forums_subscribe_event(e *Event) {
	f := forum_by_id(e.user, e.db, e.To)
	if f == nil {
		return
	}

	var m ForumMember
	if !json_decode(&m, e.Content) {
		log_info("Forum dropping subscribe event with invalid JSON")
	}

	e.db.exec("insert or ignore into members ( forum, id, name, role ) values ( ?, ?, ?, ? )", f.ID, e.From, m.Name, f.Role)
	e.db.exec("update forums set members=(select count(*) from members where forum=? and role!='disabled'), updated=? where id=?", f.ID, now(), f.ID)

	if f.Role != "disabled" {
		forum_send_recent_posts(e.db, f, e.From)
	}

	forum_update(e.user, e.db, f)
}

// Unsubscribe from forum
func forums_unsubscribe(a *Action) {
	if a.user == nil {
		a.error(401, "Not logged in")
		return
	}

	f := forum_by_id(a.user, a.db, a.id())
	if f == nil {
		a.error(404, "Forum not found")
		return
	}

	a.db.exec("delete from votes where forum=?", f.ID)
	a.db.exec("delete from comments where forum=?", f.ID)
	a.db.exec("delete from posts where forum=?", f.ID)
	a.db.exec("delete from members where forum=?", f.ID)
	a.db.exec("delete from forums where id=?", f.ID)

	e := Event{ID: uid(), From: a.user.Identity.ID, To: f.ID, Service: "forums", Action: "unsubscribe"}
	e.send()

	a.template("forums/unsubscribe")
}

// Received an unsubscribe from member
func forums_unsubscribe_event(e *Event) {
	f := forum_by_id(e.user, e.db, e.To)
	if f == nil {
		return
	}

	e.db.exec("delete from members where forum=? and id=?", e.To, e.From)
	forum_update(e.user, e.db, f)
}

// Send updated forum details to members
func forum_update(u *User, db *DB, f *Forum) {
	var ms []ForumMember
	db.scans(&ms, "select * from members where forum=? and role!='disabled'", f.ID)
	db.exec("update forums set members=?, updated=? where id=?", len(ms), now(), f.ID)

	j := json_encode(Map{"members": len(ms)})
	id := uid()
	for _, m := range ms {
		if m.ID != u.Identity.ID {
			e := Event{ID: id, From: f.ID, To: m.ID, Service: "forums", Action: "update", Content: j}
			e.send()
		}
	}
}

// Received a forum update event from owner
func forums_update_event(e *Event) {
	log_debug("Forum receieved update event '%#v'", e)

	f := forum_by_id(e.user, e.db, e.From)
	if f == nil {
		return
	}
	var n Forum
	if !json_decode(&n, e.Content) {
		log_info("Forum dropping update with invalid JSON content '%s'", e.Content)
		return
	}
	e.db.exec("update forums set members=?, updated=? where id=?", n.Members, now(), f.ID)
}

// View a forum
func forums_view(a *Action) {
	f := forum_by_id(a.user, a.db, a.id())
	if f == nil {
		a.error(404, "Forum not found")
		return
	}
	var m *ForumMember = nil
	if a.user != nil {
		m = &ForumMember{}
		if !a.db.scan(m, "select * from members where forum=? and id=?", f.ID, a.user.Identity.ID) {
			m = nil
		}
	}
	if m == nil && f.Role == "disabled" {
		a.error(404, "Forum not found")
		return
	}

	var ps []ForumPost
	a.db.scans(&ps, "select * from posts where forum=? order by updated desc", f.ID)

	a.template("forums/view", Map{"Forum": f, "Member": m, "Posts": &ps, "RoleVoter": forum_role(m, "voter"), "RolePoster": forum_role(m, "poster"), "RoleAdministrator": forum_role(m, "administrator")})
}
