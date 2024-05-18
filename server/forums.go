// Comms: Forums app
// Copyright Alistair Cunningham 2024

package main

type Forum struct {
	ID       string
	Name     string
	Role     string
	Members  int
	Updated  int64
	Identity *Identity
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
	Title         string
	Body          string
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
	ID    string
	Vote  string
}

var forum_roles = map[string]int{"disabled": 0, "viewer": 1, "voter": 2, "commenter": 3, "poster": 4, "administrator": 5}

func init() {
	a := register_app("forums")
	a.register_home("forums", map[string]string{"en": "Forums"})
	a.register_action("forums", forums_list, true)
	a.register_action("forums/create", forums_create, true)
	a.register_action("forums/new", forums_new, true)
	a.register_action("forums/comment/create", forums_comment_create, true)
	a.register_action("forums/comment/new", forums_comment_new, true)
	a.register_action("forums/comment/vote", forums_comment_vote, true)
	a.register_action("forums/find", forums_find, false)
	a.register_action("forums/members", forums_members_edit, true)
	a.register_action("forums/members/save", forums_members_save, true)
	a.register_action("forums/post/create", forums_post_create, true)
	a.register_action("forums/post/new", forums_post_new, true)
	a.register_action("forums/post/view", forums_post_view, true)
	a.register_action("forums/post/vote", forums_post_vote, true)
	a.register_action("forums/search", forums_search, false)
	a.register_action("forums/subscribe", forums_subscribe, true)
	a.register_action("forums/unsubscribe", forums_unsubscribe, true)
	a.register_action("forums/view", forums_view, true)
	a.register_event("comment/create", forums_comment_create_event)
	a.register_event("comment/submit", forums_comment_submit_event)
	a.register_event("comment/update", forums_comment_update_event)
	a.register_event("comment/vote", forums_comment_vote_event)
	a.register_event("member/update", forums_member_update_event)
	a.register_event("post/create", forums_post_create_event)
	a.register_event("post/submit", forums_post_submit_event)
	a.register_event("post/update", forums_post_update_event)
	a.register_event("post/vote", forums_post_vote_event)
	a.register_event("subscribe", forums_subscribe_event)
	a.register_event("unsubscribe", forums_unsubscribe_event)
	a.register_event("update", forums_update_event)
}

// Create app database
func forums_db_create(db *DB) {
	db.exec("create table settings ( name text not null primary key, value text not null )")
	db.exec("replace into settings ( name, value ) values ( 'schema', 1 )")

	db.exec("create table forums ( id text not null primary key, name text not null, role text not null default 'disabled', members integer not null default 0, updated integer not null )")
	db.exec("create index forums_name on forums( name )")
	db.exec("create index forums_updated on forums( updated )")

	db.exec("create table members ( forum references forums( id ), id text not null, name text not null default '', role text not null, primary key ( forum, id ) )")
	db.exec("create index members_id on members( id )")

	db.exec("create table posts ( id text not null primary key, forum references forum( id ), created integer not null, updated integer not null, author text not null, name text not null, title text not null, body text not null, comments integer not null default 0, up integer not null default 0, down integer not null default 0 )")
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
		cs[j].CreatedString = u.time_local(c.Created)
		cs[j].Children = forum_comments(u, db, f, m, p, &c, depth+1)
		cs[j].RoleVoter = forum_role(m, "voter")
		cs[j].RoleCommenter = forum_role(m, "commenter")
	}
	return &cs
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
	db.exec("update posts set updated=?, comments=comments+1 where id=?", now, post)
	db.exec("update forums set updated=? where id=?", now, f.ID)

	if f.Identity == nil {
		// We are not forum owner, so send to the owner
		e := Event{ID: id, From: u.Identity.ID, To: f.ID, App: "forums", Action: "comment/submit", Content: json_encode(ForumComment{ID: id, Post: post, Parent: parent, Body: body})}
		e.send()

	} else {
		// We are the forum owner, to send to all members except us
		j := json_encode(ForumComment{ID: id, Post: post, Parent: parent, Created: now, Author: u.Identity.ID, Name: u.Identity.Name, Body: body})
		var ms []ForumMember
		db.scans(&ms, "select * from members where forum=? and role!='disabled'", f.ID)
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

	db.exec("replace into comments ( id, forum, post, parent, created, author, name, body ) values ( ?, ?, ?, ?, ?, ?, ?, ? )", e.ID, f.ID, c.Post, c.Parent, c.Created, c.Author, c.Name, c.Body)
	db.exec("update posts set updated=?, comments=comments+1 where id=?", c.Created, c.Post)
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
	if !json_decode(&c, e.Content) {
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
	m := forum_member(u, f, e.From, "commenter")
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

	db.exec("replace into comments ( id, forum, post, parent, created, author, name, body ) values ( ?, ?, ?, ?, ?, ?, ?, ? )", e.ID, f.ID, c.Post, c.Parent, c.Created, c.Author, c.Name, c.Body)
	db.exec("update posts set updated=?, comments=comments+1 where id=?", c.Created, c.Post)
	db.exec("update forums set updated=? where id=?", c.Created, f.ID)

	j := json_encode(c)
	var ms []ForumMember
	db.scans(&ms, "select * from members where forum=? and role!='disabled'", f.ID)
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

// Received a forum comment update event
func forums_comment_update_event(u *User, e *Event) {
	log_debug("Forum receieved comment update event '%#v'", e)
	db := db_app(u, "forums", "data.db", forums_db_create)
	defer db.close()

	var c ForumComment
	if !json_decode(&c, e.Content) {
		log_info("Forum dropping comment update with invalid JSON content '%s'", e.Content)
		return
	}
	var o ForumComment
	if !db.scan(&o, "select * from comments where forum=? and id=?", e.From, c.ID) {
		log_info("Forum dropping comment update for unknown comment")
		return
	}

	now := now()
	db.exec("update comments set up=?, down=? where id=?", c.Up, c.Down, o.ID)
	db.exec("update posts set updated=? where id=?", now, o.Post)
	db.exec("update forums set updated=? where id=?", now, o.Forum)
}

// Vote on a comment
func forums_comment_vote(u *User, a *Action) {
	db := db_app(u, "forums", "data.db", forums_db_create)
	defer db.close()

	var c ForumComment
	if !db.scan(&c, "select * from comments where id=?", a.input("id")) {
		a.error(404, "Comment not found")
		return
	}
	f := forum_by_id(u, c.Forum, false)
	if f == nil {
		a.error(404, "Forum not found")
		return
	}

	vote := a.input("vote")
	forums_comment_vote_set(u, &c, u.Identity.ID, vote)

	if f.Identity == nil {
		// We are not forum owner, so send to the owner
		e := Event{ID: uid(), From: u.Identity.ID, To: f.ID, App: "forums", Action: "comment/vote", Content: json_encode(ForumVote{ID: c.ID, Vote: vote})}
		e.send()

	} else {
		// We are the forum owner, to send to all members except us
		id := uid()
		j := json_encode(c)
		var ms []ForumMember
		db.scans(&ms, "select * from members where forum=? and role!='disabled'", f.ID)
		for _, m := range ms {
			if m.ID != u.Identity.ID {
				e := Event{ID: id, From: f.ID, To: m.ID, App: "forums", Action: "comment/update", Content: j}
				e.send(f.Identity.Private)
			}
		}
	}

	a.template("forums/comment/vote", map[string]any{"Forum": f, "Post": c.Post})
}

func forums_comment_vote_set(u *User, c *ForumComment, voter string, vote string) {
	db := db_app(u, "forums", "data.db", forums_db_create)
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
func forums_comment_vote_event(u *User, e *Event) {
	db := db_app(u, "forums", "data.db", forums_db_create)
	defer db.close()

	var v ForumVote
	if !json_decode(&v, e.Content) {
		log_info("Forum dropping comment vote with invalid JSON content '%s'", e.Content)
		return
	}

	var c ForumComment
	if !db.scan(&c, "select * from comments where id=?", v.ID) {
		log_info("Forum dropping comment vote for unknown comment")
		return
	}
	f := forum_by_id(u, c.Forum, true)
	if f == nil {
		log_info("Forum dropping comment vote for unknown forum")
		return
	}
	m := forum_member(u, f, e.From, "voter")
	if m == nil {
		log_info("Forum dropping comment vote from unknown member '%s'", e.From)
		return
	}

	forums_comment_vote_set(u, &c, e.From, v.Vote)

	j := json_encode(c)
	var ms []ForumMember
	db.scans(&ms, "select * from members where forum=? and role!='disabled'", f.ID)
	for _, m := range ms {
		if m.ID != e.From && m.ID != u.Identity.ID {
			e := Event{ID: e.ID, From: f.ID, To: m.ID, App: "forums", Action: "comment/update", Content: j}
			e.send(f.Identity.Private)
		}
	}
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

	i, err := identity_create(u, "forum", name, privacy, json_encode(map[string]any{"role": role}))
	if err != nil {
		a.error(500, "Unable to create identity: %s", err)
		return
	}
	db.exec("replace into forums ( id, name, role, members, updated ) values ( ?, ?, ?, 1, ? )", i.ID, name, role, now())
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

// Gte details of a forum member
func forum_member(u *User, f *Forum, member string, role string) *ForumMember {
	db := db_app(u, "forums", "data.db", forums_db_create)
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
func forums_members_edit(u *User, a *Action) {
	db := db_app(u, "forums", "data.db", forums_db_create)
	defer db.close()

	f := forum_by_id(u, a.input("id"), true)
	if f == nil {
		a.error(404, "Forum not found")
		return
	}

	var ms []ForumMember
	db.scans(&ms, "select * from members where forum=? order by name", f.ID)

	a.template("forums/members/edit", map[string]any{"User": u, "Forum": f, "Members": ms})
}

// Save members
func forums_members_save(u *User, a *Action) {
	db := db_app(u, "forums", "data.db", forums_db_create)
	defer db.close()

	f := forum_by_id(u, a.input("id"), true)
	if f == nil {
		a.error(404, "Forum not found")
		return
	}

	var ms []ForumMember
	db.scans(&ms, "select * from members where forum=? and id!=?", f.ID, u.Identity.ID)
	for _, m := range ms {
		role := a.input("role_" + m.ID)
		if role != m.Role {
			_, found := forum_roles[role]
			if !found {
				a.error(400, "Invalid role")
				return
			}
			db.exec("update members set role=? where forum=? and id=?", role, f.ID, m.ID)
			e := Event{ID: uid(), From: f.ID, To: m.ID, App: "forums", Action: "member/update", Content: json_encode(map[string]any{"role": role})}
			e.send()

			if m.Role == "disabled" {
				forum_send_recent_posts(u, f, m.ID)
			}
		}
	}

	forum_update(u, f)
	a.template("forums/members/save", map[string]any{"Forum": f})
}

// Member update from owner
func forums_member_update_event(u *User, e *Event) {
	log_debug("Forum receieved member update event '%#v'", e)
	db := db_app(u, "forums", "data.db", forums_db_create)
	defer db.close()

	f := forum_by_id(u, e.From, false)
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
	if db.scan(&o, "select * from members where forum=? and id=?", e.From, u.Identity.ID) {
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
			notification_create(u, "forums", "member/update", f.ID, "Forum "+f.Name+": "+message, "/forums/view/?id="+f.ID)
		}
	}
	db.exec("replace into members ( forum, id, role ) values ( ?, ?, ? )", e.From, u.Identity.ID, m.Role)
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

	db.exec("replace into posts ( id, forum, created, updated, author, name, title, body ) values ( ?, ?, ?, ?, ?, ?, ?, ? )", id, f.ID, now, now, u.Identity.ID, u.Identity.Name, title, body)
	db.exec("update forums set updated=? where id=?", now, f.ID)

	if f.Identity == nil {
		// We are not forum owner, so send to the owner
		log_debug("Sending post to forum owner")
		e := Event{ID: id, From: u.Identity.ID, To: f.ID, App: "forums", Action: "post/submit", Content: json_encode(ForumPost{ID: id, Title: title, Body: body})}
		e.send()

	} else {
		// We are the forum owner, to send to all members except us
		log_debug("Sending post to forum members. u='%#v', f='%#v'", u, f)
		j := json_encode(ForumPost{ID: id, Created: now, Author: u.Identity.ID, Name: u.Identity.Name, Title: title, Body: body})
		var ms []ForumMember
		db.scans(&ms, "select * from members where forum=? and role!='disabled'", f.ID)
		for _, m := range ms {
			if m.ID != u.Identity.ID {
				e := Event{ID: id, From: f.ID, To: m.ID, App: "forums", Action: "post/create", Content: j}
				e.send(f.Identity.Private)
			}
		}
	}

	a.template("forums/post/create", map[string]any{"Forum": f, "ID": id})
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

	db.exec("replace into posts ( id, forum, created, updated, author, name, title, body, up, down ) values ( ?, ?, ?, ?, ?, ?, ?, ?, ?, ? )", e.ID, f.ID, p.Created, p.Created, p.Author, p.Name, p.Title, p.Body, p.Up, p.Down)
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
	if !json_decode(&p, e.Content) {
		log_info("Forum dropping post with invalid JSON content '%s'", e.Content)
		return
	}
	m := forum_member(u, f, e.From, "poster")
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

	db.exec("replace into posts ( id, forum, created, updated, author, name, title, body ) values ( ?, ?, ?, ?, ?, ?, ?, ? )", e.ID, f.ID, p.Created, p.Created, p.Author, p.Name, p.Title, p.Body)
	db.exec("update forums set updated=? where id=?", now(), f.ID)

	j := json_encode(p)
	var ms []ForumMember
	db.scans(&ms, "select * from members where forum=? and role!='disabled'", f.ID)
	for _, m := range ms {
		if m.ID != e.From && m.ID != u.Identity.ID {
			e := Event{ID: e.ID, From: f.ID, To: m.ID, App: "forums", Action: "post/create", Content: j}
			e.send(f.Identity.Private)
		}
	}
}

// Received a forum post update event
func forums_post_update_event(u *User, e *Event) {
	log_debug("Forum receieved post update event '%#v'", e)
	db := db_app(u, "forums", "data.db", forums_db_create)
	defer db.close()

	var p ForumPost
	if !json_decode(&p, e.Content) {
		log_info("Forum dropping post update with invalid JSON content '%s'", e.Content)
		return
	}
	if !db.exists("select id from posts where forum=? and id=?", e.From, p.ID) {
		log_info("Forum dropping post update for unknown post")
		return
	}

	now := now()
	db.exec("update posts set updated=?, up=?, down=? where id=?", now, p.Up, p.Down, p.ID)
	db.exec("update forums set updated=? where id=?", now, e.From)
}

// View a post
func forums_post_view(u *User, a *Action) {
	db := db_app(u, "forums", "data.db", forums_db_create)
	defer db.close()

	var p ForumPost
	if !db.scan(&p, "select * from posts where id=?", a.input("id")) {
		a.error(404, "Post not found")
		return
	}
	p.CreatedString = u.time_local(p.Created)

	f := forum_by_id(u, p.Forum, false)
	if f == nil {
		a.error(404, "Forum not found")
		return
	}

	var m ForumMember
	if !db.scan(&m, "select * from members where forum=? and id=?", f.ID, u.Identity.ID) {
		a.error(404, "Forum member not found")
		return
	}

	a.template("forums/post/view", map[string]any{"Forum": f, "Post": &p, "Comments": forum_comments(u, db, f, &m, &p, nil, 0), "RoleVoter": forum_role(&m, "voter"), "RoleCommenter": forum_role(&m, "commenter")})
}

// Vote on a post
func forums_post_vote(u *User, a *Action) {
	db := db_app(u, "forums", "data.db", forums_db_create)
	defer db.close()

	var p ForumPost
	if !db.scan(&p, "select * from posts where id=?", a.input("id")) {
		a.error(404, "Post not found")
		return
	}
	f := forum_by_id(u, p.Forum, false)
	if f == nil {
		a.error(404, "Forum not found")
		return
	}

	vote := a.input("vote")
	forums_post_vote_set(u, &p, u.Identity.ID, vote)

	if f.Identity == nil {
		// We are not forum owner, so send to the owner
		e := Event{ID: uid(), From: u.Identity.ID, To: f.ID, App: "forums", Action: "post/vote", Content: json_encode(ForumVote{ID: p.ID, Vote: vote})}
		e.send()

	} else {
		// We are the forum owner, to send to all members except us
		id := uid()
		j := json_encode(p)
		var ms []ForumMember
		db.scans(&ms, "select * from members where forum=? and role!='disabled'", f.ID)
		for _, m := range ms {
			if m.ID != u.Identity.ID {
				e := Event{ID: id, From: f.ID, To: m.ID, App: "forums", Action: "post/update", Content: j}
				e.send(f.Identity.Private)
			}
		}
	}

	a.template("forums/post/vote", map[string]any{"Forum": f, "ID": p.ID})
}

func forums_post_vote_set(u *User, p *ForumPost, voter string, vote string) {
	db := db_app(u, "forums", "data.db", forums_db_create)
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
func forums_post_vote_event(u *User, e *Event) {
	db := db_app(u, "forums", "data.db", forums_db_create)
	defer db.close()

	var v ForumVote
	if !json_decode(&v, e.Content) {
		log_info("Forum dropping post vote with invalid JSON content '%s'", e.Content)
		return
	}

	var p ForumPost
	if !db.scan(&p, "select * from posts where id=?", v.ID) {
		log_info("Forum dropping post vote for unknown post")
		return
	}
	f := forum_by_id(u, p.Forum, true)
	if f == nil {
		log_info("Forum dropping post vote for unknown forum")
		return
	}
	m := forum_member(u, f, e.From, "voter")
	if m == nil {
		log_info("Forum dropping post vote from unknown member")
		return
	}

	forums_post_vote_set(u, &p, e.From, v.Vote)

	j := json_encode(p)
	var ms []ForumMember
	db.scans(&ms, "select * from members where forum=? and role!='disabled'", f.ID)
	for _, m := range ms {
		if m.ID != e.From && m.ID != u.Identity.ID {
			e := Event{ID: e.ID, From: f.ID, To: m.ID, App: "forums", Action: "post/update", Content: j}
			e.send(f.Identity.Private)
		}
	}
}

// Return whether a forum member has a required role or not
func forum_role(m *ForumMember, role string) bool {
	have, found := forum_roles[m.Role]
	if !found {
		return false
	}
	need, found := forum_roles[role]
	if !found {
		return false
	}
	if have < need {
		return false
	}
	return true
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

// Send recent posts to a member
func forum_send_recent_posts(u *User, f *Forum, member string) {
	db := db_app(u, "forums", "data.db", forums_db_create)
	var ps []ForumPost
	db.scans(&ps, "select * from posts where forum=? order by updated desc limit 100", f.ID)
	for _, p := range ps {
		e := Event{ID: p.ID, From: f.ID, To: member, App: "forums", Action: "post/create", Content: json_encode(p)}
		e.send(f.Identity.Private)
	}
	for _, p := range ps {
		var cs []ForumComment
		db.scans(&cs, "select * from comments where post=?", p.ID)
		for _, c := range cs {
			e := Event{ID: c.ID, From: f.ID, To: member, App: "forums", Action: "comment/create", Content: json_encode(c)}
			e.send(f.Identity.Private)
		}
	}
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
	if forum_by_id(u, id, false) != nil {
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

	db.exec("replace into forums ( id, name, members, updated ) values ( ?, ?, 1, ? )", id, d.Name, now())
	db.exec("replace into members ( forum, id, name, role ) values ( ?, ?, ?, ? )", id, u.Identity.ID, u.Identity.Name, m.Role)

	e := Event{ID: uid(), From: u.Identity.ID, To: id, App: "forums", Action: "subscribe", Content: json_encode(map[string]string{"name": u.Identity.Name})}
	e.send()

	a.template("forums/subscribe", map[string]any{"Forum": id, "Role": m.Role})
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

	var m ForumMember
	if !json_decode(&m, e.Content) {
		log_info("Forum dropping subscribe event with invalid JSON")
	}

	db.exec("insert or ignore into members ( forum, id, name, role ) values ( ?, ?, ?, ? )", f.ID, e.From, m.Name, f.Role)
	db.exec("update forums set members=(select count(*) from members where forum=? and role!='disabled'), updated=? where id=?", f.ID, now(), f.ID)

	if f.Role != "disabled" {
		forum_send_recent_posts(u, f, e.From)
	}

	forum_update(u, f)
}

// Unsubscribe from forum
func forums_unsubscribe(u *User, a *Action) {
	db := db_app(u, "forums", "data.db", forums_db_create)
	defer db.close()

	f := forum_by_id(u, a.input("id"), false)
	if f == nil {
		a.error(404, "Forum not found")
		return
	}

	db.exec("delete from votes where forum=?", f.ID)
	db.exec("delete from comments where forum=?", f.ID)
	db.exec("delete from posts where forum=?", f.ID)
	db.exec("delete from members where forum=?", f.ID)
	db.exec("delete from forums where id=?", f.ID)

	e := Event{ID: uid(), From: u.Identity.ID, To: f.ID, App: "forums", Action: "unsubscribe"}
	e.send()

	a.template("forums/unsubscribe")
}

// Received an unsubscribe from member
func forums_unsubscribe_event(u *User, e *Event) {
	log_debug("Forum receieved unsubscribe event '%#v'", e)
	db := db_app(u, "forums", "data.db", forums_db_create)
	db.close()

	f := forum_by_id(u, e.To, true)
	if f == nil {
		return
	}

	db.exec("delete from members where forum=? and id=?", e.To, e.From)
	forum_update(u, f)
}

// Send updated forum details to members
func forum_update(u *User, f *Forum) {
	db := db_app(u, "forums", "data.db", forums_db_create)

	var ms []ForumMember
	db.scans(&ms, "select * from members where forum=? and role!='disabled'", f.ID)
	db.exec("update forums set members=?, updated=? where id=?", len(ms), now(), f.ID)

	j := json_encode(map[string]any{"members": len(ms)})
	id := uid()
	for _, m := range ms {
		if m.ID != u.Identity.ID {
			e := Event{ID: id, From: f.ID, To: m.ID, App: "forums", Action: "update", Content: j}
			e.send(f.Identity.Private)
		}
	}
}

// Received a forum update event from owner
func forums_update_event(u *User, e *Event) {
	db := db_app(u, "forums", "data.db", forums_db_create)
	log_debug("Forum receieved update event '%#v'", e)

	f := forum_by_id(u, e.From, false)
	if f == nil {
		return
	}
	var n Forum
	if !json_decode(&n, e.Content) {
		log_info("Forum dropping update with invalid JSON content '%s'", e.Content)
		return
	}
	db.exec("update forums set members=?, updated=? where id=?", n.Members, now(), f.ID)
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

	var m ForumMember
	if !db.scan(&m, "select * from members where forum=? and id=?", f.ID, u.Identity.ID) {
		a.error(404, "Forum member not found")
		return
	}

	var ps []ForumPost
	db.scans(&ps, "select * from posts where forum=? order by updated desc", f.ID)

	log_debug("Forum='%#v', member='%#v'", f, m)
	a.template("forums/view", map[string]any{"Forum": f, "Member": &m, "Posts": &ps, "RoleVoter": forum_role(&m, "voter"), "RolePoster": forum_role(&m, "poster"), "RoleAdministrator": forum_role(&m, "administrator")})
}
