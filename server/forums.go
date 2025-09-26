// Mochi: Forums app
// Copyright Alistair Cunningham 2024-2025

package main

import (
	"fmt"
	"html/template"
)

type Forum struct {
	ID          string  `cbor:"id" json:"id"`
	Fingerprint string  `cbor:"-" json:"fingerprint"`
	Name        string  `cbor:"name" json:"name"`
	Role        string  `cbor:"-" json:"role"`
	Members     int     `cbor:"members" json:"members"`
	Updated     int64   `cbor:"updated" json:"updated"`
	Entity      *Entity `cbor:"-" json:"-"`
}

type ForumMember struct {
	Forum     string `cbor:"forum" json:"forum"`
	ID        string `cbor:"id" json:"id"`
	Name      string `cbor:"name" json:"name"`
	Role      string `cbor:"role" json:"role"`
	ForumName string `cbor:"-" json:"-"`
}

type ForumPost struct {
	ID               string        `cbor:"id" json:"id"`
	Forum            string        `cbor:"-" json:"forum"`
	ForumFingerprint string        `cbor:"-" json:"-"`
	ForumName        string        `cbor:"-" json:"-"`
	Member           string        `cbor:"member" json:"member"`
	Name             string        `cbor:"name" json:"name"`
	Title            string        `cbor:"title" json:"title"`
	Body             string        `cbor:"body" json:"body"`
	BodyMarkdown     template.HTML `cbor:"-" json:"-"`
	Comments         int           `cbor:"comments" json:"comments"`
	Up               int           `cbor:"up" json:"up"`
	Down             int           `cbor:"down" json:"down"`
	Created          int64         `cbor:"created" json:"created"`
	CreatedString    string        `cbor:"-" json:"-"`
	Updated          int64         `cbor:"updated" json:"updated"`
	Attachments      *[]Attachment `cbor:"attachments,omitempty" json:"attachments,omitempty"`
}

type ForumComment struct {
	ID               string          `cbor:"id" json:"id"`
	Forum            string          `cbor:"-" json:"forum"`
	ForumFingerprint string          `cbor:"-" json:"-"`
	Post             string          `cbor:"post" json:"post"`
	Parent           string          `cbor:"parent" json:"parent"`
	Member           string          `cbor:"member" json:"member"`
	Name             string          `cbor:"name" json:"name"`
	Body             string          `cbor:"body" json:"body"`
	BodyMarkdown     template.HTML   `cbor:"-" json:"-"`
	Up               int             `cbor:"up" json:"up"`
	Down             int             `cbor:"down" json:"down"`
	Created          int64           `cbor:"created" json:"created"`
	CreatedString    string          `cbor:"-" json:"-"`
	Children         *[]ForumComment `cbor:"-" json:"-"`
	RoleVoter        bool            `cbor:"-" json:"-"`
	RoleCommenter    bool            `cbor:"-" json:"-"`
}

type ForumVote struct {
	Post    string `cbor:"post" json:"post"`
	Comment string `cbor:"comment" json:"comment"`
	Voter   string `cbor:"voter" json:"voter"`
	Vote    string `cbor:"vote" json:"vote"`
}

var (
	forum_roles = map[string]int{"disabled": 0, "viewer": 1, "voter": 2, "commenter": 3, "poster": 4, "administrator": 5}
)

func init() {
	a := app("forums")
	a.icon("forums", "forums", "Forums", "forums.png")
	a.entity("forum")
	a.db("forums/forums.db", forums_db_create)

	a.path("forums", forums_view)
	a.path("forums/create", forums_create)
	a.path("forums/find", forums_find)
	a.path("forums/new", forums_new)
	a.path("forums/post/create", forums_post_create)
	a.path("forums/post/new", forums_post_new)
	a.path("forums/search", forums_search)
	a.path("forums/:forum", forums_view)
	a.path("forums/:forum/members", forums_members_edit)
	a.path("forums/:forum/members/save", forums_members_save)
	a.path("forums/:forum/subscribe", forums_subscribe)
	a.path("forums/:forum/unsubscribe", forums_unsubscribe)
	a.path("forums/:forum/:post", forums_post_view)
	a.path("forums/:forum/:post/comment", forums_comment_new)
	a.path("forums/:forum/:post/create", forums_comment_create)
	a.path("forums/:forum/:post/vote/:vote", forums_post_vote)
	a.path("forums/:forum/:post/:comment/vote/:vote", forums_comment_vote)

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

	db.exec("create table forums ( id text not null primary key, fingerprint text not null, name text not null, role text not null default '', members integer not null default 0, updated integer not null )")
	db.exec("create index forums_fingerprint on forums( fingerprint )")
	db.exec("create index forums_name on forums( name )")
	db.exec("create index forums_updated on forums( updated )")

	db.exec("create table members ( forum references forums( id ), id text not null, name text not null default '', role text not null, primary key ( forum, id ) )")
	db.exec("create index members_id on members( id )")

	db.exec("create table posts ( id text not null primary key, forum references forum( id ), member text not null, name text not null, title text not null, body text not null, comments integer not null default 0, up integer not null default 0, down integer not null default 0, created integer not null, updated integer not null )")
	db.exec("create index posts_forum on posts( forum )")
	db.exec("create index posts_created on posts( created )")
	db.exec("create index posts_updated on posts( updated )")

	db.exec("create table comments ( id text not null primary key, forum references forum( id ), post text not null, parent text not null, member text not null, name text not null, body text not null, up integer not null default 0, down integer not null default 0, created integer not null )")
	db.exec("create index comments_forum on comments( forum )")
	db.exec("create index comments_post on comments( post )")
	db.exec("create index comments_parent on comments( parent )")
	db.exec("create index comments_created on comments( created )")

	db.exec("create table votes ( forum references forum( id ), post text not null, comment text not null default '', voter text not null, vote text not null, primary key ( forum, post, comment, voter ) )")
	db.exec("create index votes_post on votes( post )")
	db.exec("create index votes_comment on votes( comment )")
	db.exec("create index votes_voter on votes( voter )")
}

func forum_by_id(u *User, db *DB, id string) *Forum {
	var f Forum
	if !db.scan(&f, "select * from forums where id=?", id) {
		if !db.scan(&f, "select * from forums where fingerprint=?", id) {
			return nil
		}
	}
	if u != nil {
		f.Entity = entity_by_user_id(u, f.ID)
	}
	return &f
}

// Get comments recursively
func forum_comments(u *User, f *Forum, m *ForumMember, p *ForumPost, parent *ForumComment, depth int) *[]ForumComment {
	if depth > 1000 {
		return nil
	}

	id := ""
	if parent != nil {
		id = parent.ID
	}

	var cs []ForumComment
	u.db.scans(&cs, "select * from comments where forum=? and post=? and parent=? order by created desc", f.ID, p.ID, id)
	for j, c := range cs {
		cs[j].ForumFingerprint = fingerprint(c.Forum)
		cs[j].BodyMarkdown = web_markdown(c.Body)
		cs[j].CreatedString = time_local(u, c.Created)
		cs[j].Children = forum_comments(u, f, m, p, &c, depth+1)
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

	f := forum_by_id(a.user, a.user.db, a.input("forum"))
	if f == nil {
		a.error(404, "Forum not found")
		return
	}

	post := a.input("post")
	if !a.user.db.exists("select id from posts where id=? and forum=?", post, f.ID) {
		a.error(404, "Post not found")
		return
	}

	parent := a.input("parent")
	if parent != "" && !a.user.db.exists("select id from comments where id=? and post=?", parent, post) {
		a.error(404, "Parent not found")
		return
	}

	body := a.input("body")
	if !valid(body, "text") {
		a.error(400, "Invalid body")
		return
	}

	id := uid()
	if a.user.db.exists("select id from comments where id=?", id) {
		a.error(500, "Duplicate ID")
		return
	}

	a.user.db.exec("replace into comments ( id, forum, post, parent, member, name, body, created ) values ( ?, ?, ?, ?, ?, ?, ?, ? )", id, f.ID, post, parent, a.user.Identity.ID, a.user.Identity.Name, body, now)
	a.user.db.exec("update posts set updated=?, comments=comments+1 where id=?", now, post)
	a.user.db.exec("update forums set updated=? where id=?", now, f.ID)

	if f.Entity != nil {
		// We are the forum owner, so send to all members except us
		var ms []ForumMember
		a.user.db.scans(&ms, "select * from members where forum=? and role!='disabled'", f.ID)
		for _, m := range ms {
			if m.ID != a.user.Identity.ID {
				message(f.ID, m.ID, "forums", "comment/create").add(ForumComment{ID: id, Post: post, Parent: parent, Created: now, Member: a.user.Identity.ID, Name: a.user.Identity.Name, Body: body}).send()
			}
		}

	} else {
		// We are not forum owner, so send to the owner
		message(a.user.Identity.ID, f.ID, "forums", "comment/submit").add(ForumComment{ID: id, Post: post, Parent: parent, Body: body}).send()
	}

	a.template("forums/comment/create", a.input("format"), Map{"Forum": f, "Post": post})
}

// Received a forum comment from owner
func forums_comment_create_event(e *Event) {
	debug("Forum receieved comment create event '%+v'", e)

	f := forum_by_id(e.user, e.db, e.from)
	if f == nil {
		info("Forum dropping comment to unknown forum")
		return
	}

	var c ForumComment
	if !e.segment(&c) {
		info("Forum dropping comment with invalid data")
		return
	}

	if !valid(c.ID, "id") {
		info("Forum dropping comment with invalid ID '%s'", c.ID)
		return
	}
	if e.db.exists("select id from comments where id=?", c.ID) {
		info("Forum dropping comment with duplicate ID '%s'", c.ID)
		return
	}

	if !valid(c.Member, "entity") {
		info("Forum dropping comment with invalid member '%s'", c.Member)
		return
	}

	if !valid(c.Name, "name") {
		info("Forum dropping comment with invalid name '%s'", c.Name)
		return
	}

	if !valid(c.Body, "text") {
		info("Forum dropping comment with invalid body '%s'", c.Body)
		return
	}

	e.db.exec("replace into comments ( id, forum, post, parent, member, name, body, created ) values ( ?, ?, ?, ?, ?, ?, ?, ? )", c.ID, f.ID, c.Post, c.Parent, c.Member, c.Name, c.Body, c.Created)
	e.db.exec("update posts set updated=?, comments=comments+1 where id=?", c.Created, c.Post)
	e.db.exec("update forums set updated=? where id=?", c.Created, f.ID)
}

// Received a forum comment from member
func forums_comment_submit_event(e *Event) {
	f := forum_by_id(e.user, e.db, e.to)
	if f == nil {
		info("Forum dropping comment to unknown forum")
		return
	}

	var c ForumComment
	if !e.segment(&c) {
		info("Forum dropping comment with invalid data")
		return
	}

	if !valid(c.ID, "id") {
		info("Forum dropping comment with invalid ID '%s'", c.ID)
		return
	}
	if e.db.exists("select id from comments where id=?", c.ID) {
		info("Forum dropping comment with duplicate ID '%s'", c.ID)
		return
	}

	if !e.db.exists("select id from posts where forum=? and id=?", f.ID, c.Post) {
		info("Forum dropping comment for unknown post '%s'", c.Post)
		return
	}

	if c.Parent != "" && !e.db.exists("select id from comments where forum=? and post=? and id=?", f.ID, c.Post, c.Parent) {
		info("Forum dropping comment with unknown parent '%s'", c.Parent)
		return
	}

	m := forum_member(e.db, f, e.from, "commenter")
	if m == nil {
		info("Forum dropping comment from unknown member '%s'", e.from)
		return
	}

	c.Created = now()
	c.Member = e.from
	c.Name = m.Name

	if !valid(c.Body, "text") {
		info("Forum dropping comment with invalid body '%s'", c.Body)
		return
	}

	e.db.exec("replace into comments ( id, forum, post, parent, member, name, body, created ) values ( ?, ?, ?, ?, ?, ?, ?, ? )", e.id, f.ID, c.Post, c.Parent, c.Member, c.Name, c.Body, c.Created)
	e.db.exec("update posts set updated=?, comments=comments+1 where id=?", c.Created, c.Post)
	e.db.exec("update forums set updated=? where id=?", c.Created, f.ID)

	var ms []ForumMember
	e.db.scans(&ms, "select * from members where forum=? and role!='disabled'", f.ID)
	for _, m := range ms {
		if m.ID != e.from && m.ID != e.user.Identity.ID {
			message(f.ID, m.ID, "forums", "comment/create").add(c).send()
		}
	}
}

// Enter details for new comment
func forums_comment_new(a *Action) {
	if a.user == nil {
		a.error(401, "Not logged in")
		return
	}

	a.template("forums/comment/new", a.input("format"), Map{"Forum": forum_by_id(a.user, a.user.db, a.input("forum")), "Post": a.input("post"), "Parent": a.input("parent")})
}

// Received a forum comment update event
func forums_comment_update_event(e *Event) {
	debug("Forum receieved comment update event '%+v'", e)

	var c ForumComment
	if !e.segment(&c) {
		info("Forum dropping comment update with invalid data")
		return
	}

	var o ForumComment
	if !e.db.scan(&o, "select * from comments where forum=? and id=?", e.from, c.ID) {
		info("Forum dropping comment update for unknown comment")
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
	if !a.user.db.scan(&c, "select * from comments where id=?", a.input("comment")) {
		a.error(404, "Comment not found")
		return
	}
	f := forum_by_id(a.user, a.user.db, c.Forum)
	if f == nil {
		a.error(404, "Forum not found")
		return
	}

	vote := a.input("vote")
	forums_comment_vote_set(a.user.db, &c, a.user.Identity.ID, vote)

	if f.Entity != nil {
		// We are the forum owner, to send to all members except us
		var ms []ForumMember
		a.user.db.scans(&ms, "select * from members where forum=? and role!='disabled'", f.ID)
		for _, m := range ms {
			if m.ID != a.user.Identity.ID {
				message(f.ID, m.ID, "forums", "comment/update").add(c).send()
			}
		}

	} else {
		// We are not forum owner, so send to the owner
		message(a.user.Identity.ID, f.ID, "forums", "comment/vote").add(ForumVote{Comment: c.ID, Vote: vote}).send()
	}

	a.template("forums/comment/vote", a.input("format"), Map{"Forum": f, "Post": c.Post})
}

func forums_comment_vote_set(db *DB, c *ForumComment, voter string, vote string) {
	now := now()

	var o ForumVote
	if db.scan(&o, "select vote from votes where comment=? and voter=?", c.ID, voter) {
		switch o.Vote {
		case "up":
			c.Up = c.Up - 1
			db.exec("update comments set up=up-1 where id=?", c.ID)
		case "down":
			c.Down = c.Down - 1
			db.exec("update comments set down=down-1 where id=?", c.ID)
		}
	}

	db.exec("replace into votes ( forum, post, comment, voter, vote ) values ( ?, ?, ?, ?, ? )", c.Forum, c.Post, voter, c.ID, vote)
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
	if !e.segment(&v) {
		info("Forum dropping comment vote with invalid data")
		return
	}

	var c ForumComment
	if !e.db.scan(&c, "select * from comments where id=?", v.Comment) {
		info("Forum dropping comment vote for unknown comment")
		return
	}

	f := forum_by_id(e.user, e.db, c.Forum)
	if f == nil {
		info("Forum dropping comment vote for unknown forum")
		return
	}

	m := forum_member(e.db, f, e.from, "voter")
	if m == nil {
		info("Forum dropping comment vote from unknown member '%s'", e.from)
		return
	}

	forums_comment_vote_set(e.db, &c, e.from, v.Vote)

	var ms []ForumMember
	e.db.scans(&ms, "select * from members where forum=? and role!='disabled'", f.ID)
	for _, m := range ms {
		if m.ID != e.from && m.ID != e.user.Identity.ID {
			message(f.ID, m.ID, "forums", "comment/update").add(c).send()
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

	e, err := entity_create(a.user, "forum", name, privacy, json_encode(Map{"role": role}))
	if err != nil {
		a.error(500, "Unable to create entity: %s", err)
		return
	}

	a.user.db.exec("replace into forums ( id, fingerprint, name, role, members, updated ) values ( ?, ?, ?, ?, 1, ? )", e.ID, e.Fingerprint, name, role, now())
	a.user.db.exec("replace into members ( forum, id, name, role ) values ( ?, ?, ?, 'administrator' )", e.ID, a.user.Identity.ID, a.user.Identity.Name)

	a.template("forums/create", a.input("format"), e.Fingerprint)
}

// Enter details of forums to be subscribed to
func forums_find(a *Action) {
	a.template("forums/find", a.input("format"))
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

	f := forum_by_id(a.user, a.user.db, a.input("forum"))
	if f == nil || f.Entity == nil {
		a.error(404, "Forum not found")
		return
	}

	var ms []ForumMember
	a.user.db.scans(&ms, "select * from members where forum=? order by name", f.ID)

	a.template("forums/members/edit", a.input("format"), Map{"User": a.user, "Forum": f, "Members": ms})
}

// Save members
func forums_members_save(a *Action) {
	if a.user == nil {
		a.error(401, "Not logged in")
		return
	}

	f := forum_by_id(a.user, a.user.db, a.input("forum"))
	if f == nil || f.Entity == nil {
		a.error(404, "Forum not found")
		return
	}

	var ms []ForumMember
	a.user.db.scans(&ms, "select * from members where forum=? and id!=?", f.ID, a.user.Identity.ID)
	for _, m := range ms {
		role := a.input("role_" + m.ID)
		if role != m.Role {
			_, found := forum_roles[role]
			if !found {
				a.error(400, "Invalid role")
				return
			}
			a.user.db.exec("update members set role=? where forum=? and id=?", role, f.ID, m.ID)
			message(f.ID, m.ID, "forums", "member/update").set("role", role).send()

			if m.Role == "disabled" {
				forum_send_recent_posts(a.user, a.user.db, f, m.ID)
			}
		}
	}

	forum_update(a.user, a.user.db, f)
	a.template("forums/members/save", a.input("format"), Map{"Forum": f})
}

// Member update from owner
func forums_member_update_event(e *Event) {
	f := forum_by_id(e.user, e.db, e.from)
	if f == nil {
		return
	}

	role := e.get("role", "")
	_, found := forum_roles[role]
	if !found {
		info("Forum dropping member update with invalid role '%s'", role)
		return
	}

	var o ForumMember
	if e.db.scan(&o, "select * from members where forum=? and id=?", e.from, e.user.Identity.ID) {
		if role != o.Role {
			/* notification() has been removed. When porting to Starlark, replace with service call
			message := "An error occurred"
			switch role {
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
			*/
		}
	}
	e.db.exec("replace into members ( forum, id, role ) values ( ?, ?, ? )", e.from, e.user.Identity.ID, role)
}

// Enter details for new forum to be created
func forums_new(a *Action) {
	a.template("forums/new", a.input("format"))
}

// New post
func forums_post_create(a *Action) {
	now := now()

	if a.user == nil {
		a.error(401, "Not logged in")
		return
	}

	f := forum_by_id(a.user, a.user.db, a.input("forum"))
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

	post := uid()
	if a.user.db.exists("select id from posts where id=?", post) {
		a.error(500, "Duplicate ID")
		return
	}

	a.user.db.exec("replace into posts ( id, forum, member, name, title, body, created, updated ) values ( ?, ?, ?, ?, ?, ?, ?, ? )", post, f.ID, a.user.Identity.ID, a.user.Identity.Name, title, body, now, now)
	a.user.db.exec("update forums set updated=? where id=?", now, f.ID)

	if f.Entity != nil {
		// We are the forum owner, so send to all members except us
		attachments := a.upload_attachments("attachments", f.ID, fmt.Sprintf("forums/%s/%s", f.ID, post), true)
		var ms []ForumMember
		a.user.db.scans(&ms, "select * from members where forum=? and role!='disabled'", f.ID)
		for _, m := range ms {
			if m.ID != a.user.Identity.ID {
				message(f.ID, m.ID, "forums", "post/create").add(ForumPost{ID: post, Created: now, Member: a.user.Identity.ID, Name: a.user.Identity.Name, Title: title, Body: body, Attachments: attachments}).send()
			}
		}

	} else {
		// We are not forum owner, so send to the owner
		attachments := a.upload_attachments("attachments", f.ID, fmt.Sprintf("forums/%s/%s", f.ID, post), false)
		message(a.user.Identity.ID, f.ID, "forums", "post/submit").add(ForumPost{ID: post, Title: title, Body: body, Attachments: attachments}).send()
	}

	a.template("forums/post/create", a.input("format"), Map{"Forum": f, "Post": post})
}

// Received a forum post from owner
func forums_post_create_event(e *Event) {
	f := forum_by_id(e.user, e.db, e.from)
	if f == nil {
		info("Forum dropping post to unknown forum")
		return
	}

	var p ForumPost
	if !e.segment(&p) {
		info("Forum dropping post with invalid data")
		return
	}

	if !valid(p.ID, "id") {
		info("Forum dropping post with invalid ID '%s'", p.ID)
		return
	}

	if e.db.exists("select id from comments where id=?", p.ID) {
		info("Forum dropping post with duplicate ID '%s'", p.ID)
		return
	}

	if !valid(p.Member, "entity") {
		info("Forum dropping post with invalid member '%s'", p.Member)
		return
	}

	if !valid(p.Name, "name") {
		info("Forum dropping post with invalid name '%s'", p.Name)
		return
	}

	if !valid(p.Title, "line") {
		info("Forum dropping post with invalid title '%s'", p.Title)
		return
	}

	if !valid(p.Body, "text") {
		info("Forum dropping post with invalid body '%s'", p.Body)
		return
	}

	e.db.exec("replace into posts ( id, forum, member, name, title, body, up, down, created, updated ) values ( ?, ?, ?, ?, ?, ?, ?, ?, ?, ? )", p.ID, f.ID, p.Member, p.Name, p.Title, p.Body, p.Up, p.Down, p.Created, p.Created)
	attachments_save(p.Attachments, e.user, f.ID, "forums/%s/%s", f.ID, p.ID)

	e.db.exec("update forums set updated=? where id=?", now(), f.ID)
}

// Enter details for new post
func forums_post_new(a *Action) {
	if a.user == nil {
		a.error(401, "Not logged in")
		return
	}

	var fs []Forum
	a.user.db.scans(&fs, "select * from forums order by name")
	if len(fs) == 0 {
		a.error(500, "You are not a member of any forums")
		return
	}

	a.template("forums/post/new", a.input("format"), Map{"Forums": fs, "Current": a.input("current")})
}

// Received a forum post from a member
func forums_post_submit_event(e *Event) {
	f := forum_by_id(e.user, e.db, e.to)
	if f == nil {
		info("Forum dropping post to unknown forum")
		return
	}

	var p ForumPost
	if !e.segment(&p) {
		info("Forum dropping post with invalid data")
		return
	}

	if !valid(p.ID, "id") {
		info("Forum dropping post with invalid ID '%s'", p.ID)
		return
	}
	if e.db.exists("select id from posts where id=?", p.ID) {
		info("Forum dropping post with duplicate ID '%s'", p.ID)
		return
	}

	m := forum_member(e.db, f, e.from, "poster")
	if m == nil {
		info("Forum dropping post from unknown member '%s'", e.from)
		return
	}

	p.Created = now()
	p.Member = e.from
	p.Name = m.Name

	if !valid(p.Title, "line") {
		info("Forum dropping post with invalid title '%s'", p.Title)
		return
	}

	if !valid(p.Body, "text") {
		info("Forum dropping post with invalid body '%s'", p.Body)
		return
	}

	e.db.exec("replace into posts ( id, forum, member, name, title, body, created, updated ) values ( ?, ?, ?, ?, ?, ?, ?, ? )", p.ID, f.ID, p.Member, p.Name, p.Title, p.Body, p.Created, p.Created)
	attachments_save(p.Attachments, e.user, f.ID, "forums/%s/%s", f.ID, p.ID)

	e.db.exec("update forums set updated=? where id=?", now(), f.ID)

	p.Attachments = nil
	var ms []ForumMember
	e.db.scans(&ms, "select * from members where forum=? and role!='disabled'", f.ID)
	for _, m := range ms {
		if m.ID != e.from && m.ID != e.user.Identity.ID {
			message(f.ID, m.ID, "forums", "post/create").add(p).send()
		}
	}
}

// Received a forum post update event
func forums_post_update_event(e *Event) {
	var p ForumPost
	if !e.segment(&p) {
		info("Forum dropping post update with invalid data")
		return
	}

	if !e.db.exists("select id from posts where forum=? and id=?", e.from, p.ID) {
		info("Forum dropping post update for unknown post")
		return
	}

	now := now()
	e.db.exec("update posts set updated=?, up=?, down=? where id=?", now, p.Up, p.Down, p.ID)
	e.db.exec("update forums set updated=? where id=?", now, e.from)
}

// View a post
func forums_post_view(a *Action) {
	var p ForumPost
	if !a.owner.db.scan(&p, "select * from posts where id=?", a.input("post")) {
		a.error(404, "Post not found")
		return
	}
	p.BodyMarkdown = web_markdown(p.Body)
	p.CreatedString = time_local(a.user, p.Created)

	f := forum_by_id(a.owner, a.owner.db, p.Forum)
	if f == nil {
		a.error(404, "Forum not found")
		return
	}

	var m *ForumMember = nil
	if a.user != nil {
		m = &ForumMember{}
		if !a.owner.db.scan(m, "select * from members where forum=? and id=?", f.ID, a.user.Identity.ID) {
			m = nil
		}
	}
	if m == nil && f.Role == "disabled" {
		a.error(404, "Forum not found")
		return
	}

	a.template("forums/post/view", a.input("format"), Map{"Forum": f, "Post": &p, "Attachments": attachments(a.owner, fmt.Sprintf("forums/%s/%s", f.ID, p.ID)), "Comments": forum_comments(a.owner, f, m, &p, nil, 0), "RoleVoter": forum_role(m, "voter"), "RoleCommenter": forum_role(m, "commenter")})
}

// Vote on a post
func forums_post_vote(a *Action) {
	if a.user == nil {
		a.error(401, "Not logged in")
		return
	}

	var p ForumPost
	if !a.user.db.scan(&p, "select * from posts where id=?", a.input("post")) {
		a.error(404, "Post not found")
		return
	}
	f := forum_by_id(a.user, a.user.db, p.Forum)
	if f == nil {
		a.error(404, "Forum not found")
		return
	}

	vote := a.input("vote")
	forums_post_vote_set(a.user.db, &p, a.user.Identity.ID, vote)

	if f.Entity != nil {
		// We are the forum owner, to send to all members except us
		var ms []ForumMember
		a.user.db.scans(&ms, "select * from members where forum=? and role!='disabled'", f.ID)
		for _, m := range ms {
			if m.ID != a.user.Identity.ID {
				message(f.ID, m.ID, "forums", "post/update").add(p).send()
			}
		}

	} else {
		// We are not forum owner, so send to the owner
		m := message(a.user.Identity.ID, f.ID, "forums", "post/vote")
		m.add(ForumVote{Post: p.ID, Vote: vote})
		m.send()
	}

	a.template("forums/post/vote", a.input("format"), Map{"Forum": f, "ID": p.ID})
}

func forums_post_vote_set(db *DB, p *ForumPost, voter string, vote string) {
	now := now()

	var o ForumVote
	if db.scan(&o, "select vote from votes where post=? and voter=?", p.ID, voter) {
		switch o.Vote {
		case "up":
			p.Up = p.Up - 1
			db.exec("update posts set up=up-1, updated=? where id=?", now, p.ID)
		case "down":
			p.Down = p.Down - 1
			db.exec("update posts set down=down-1, updated=? where id=?", now, p.ID)
		}
	}

	db.exec("replace into votes ( forum, post, voter, vote ) values ( ?, ?, ?, ? )", p.Forum, p.ID, voter, vote)
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
	if !e.segment(&v) {
		info("Forum dropping post vote with invalid data")
		return
	}

	var p ForumPost
	if !e.db.scan(&p, "select * from posts where id=?", v.Post) {
		info("Forum dropping post vote for unknown post")
		return
	}

	f := forum_by_id(e.user, e.db, p.Forum)
	if f == nil {
		info("Forum dropping post vote for unknown forum")
		return
	}

	m := forum_member(e.db, f, e.from, "voter")
	if m == nil {
		info("Forum dropping post vote from unknown member")
		return
	}

	forums_post_vote_set(e.db, &p, e.from, v.Vote)

	var ms []ForumMember
	e.db.scans(&ms, "select * from members where forum=? and role!='disabled'", f.ID)
	for _, m := range ms {
		if m.ID != e.from && m.ID != e.user.Identity.ID {
			message(f.ID, m.ID, "forums", "post/update").add(p).send()
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

	a.template("forums/search", a.input("format"), directory_search(a.user, "forum", search, false))
}

// Send recent posts to a new member
func forum_send_recent_posts(u *User, db *DB, f *Forum, member string) {
	var ps []ForumPost
	db.scans(&ps, "select * from posts where forum=? order by updated desc limit 1000", f.ID)
	for _, p := range ps {
		p.Attachments = attachments(u, fmt.Sprintf("forums/%s/%s", f.ID, p.ID))
		message(f.ID, member, "forums", "post/create").add(p).send()
	}

	for _, p := range ps {
		var cs []ForumComment
		db.scans(&cs, "select * from comments where post=?", p.ID)
		for _, c := range cs {
			message(f.ID, member, "forums", "comment/create").add(c).send()
		}
	}
}

// Subscribe to a forum
func forums_subscribe(a *Action) {
	if a.user == nil {
		a.error(401, "Not logged in")
		return
	}

	forum := a.input("forum")
	if !valid(forum, "entity") {
		a.error(400, "Invalid ID")
		return
	}
	if forum_by_id(a.user, a.user.db, forum) != nil {
		a.error(400, "You are already subscribed to this forum")
		return
	}

	d := directory_by_id(forum)
	if d == nil {
		a.error(404, "Unable to find forum in directory")
		return
	}

	var m ForumMember
	if !json_decode(&m, d.Data) {
		a.error(400, "Forum directory entry does not contain data")
		return
	}

	a.user.db.exec("replace into forums ( id, fingerprint, name, members, updated ) values ( ?, ?, ?, 1, ? )", forum, fingerprint(forum), d.Name, now())
	a.user.db.exec("replace into members ( forum, id, name, role ) values ( ?, ?, ?, ? )", forum, a.user.Identity.ID, a.user.Identity.Name, m.Role)

	message(a.user.Identity.ID, forum, "forums", "subscribe").set("name", a.user.Identity.Name).send()

	a.template("forums/subscribe", a.input("format"), Map{"Forum": forum, "Fingerprint": fingerprint(forum), "Role": m.Role})
}

// Received a subscribe from a member
func forums_subscribe_event(e *Event) {
	f := forum_by_id(e.user, e.db, e.to)
	if f == nil {
		return
	}

	name := e.get("name", "")
	if !valid(name, "line") {
		info("Forums dropping subscribe with invalid name '%s'", name)
		return
	}

	e.db.exec("insert or ignore into members ( forum, id, name, role ) values ( ?, ?, ?, ? )", f.ID, e.from, name, f.Role)
	e.db.exec("update forums set members=(select count(*) from members where forum=? and role!='disabled'), updated=? where id=?", f.ID, now(), f.ID)

	if f.Role != "disabled" {
		forum_send_recent_posts(e.user, e.db, f, e.from)
	}

	forum_update(e.user, e.db, f)
}

// Unsubscribe from forum
func forums_unsubscribe(a *Action) {
	if a.user == nil {
		a.error(401, "Not logged in")
		return
	}

	f := forum_by_id(a.user, a.user.db, a.input("forum"))
	if f == nil {
		a.error(404, "Forum not found")
		return
	}

	a.user.db.exec("delete from votes where forum=?", f.ID)
	a.user.db.exec("delete from comments where forum=?", f.ID)
	a.user.db.exec("delete from posts where forum=?", f.ID)
	a.user.db.exec("delete from members where forum=?", f.ID)
	a.user.db.exec("delete from forums where id=?", f.ID)

	message(a.user.Identity.ID, f.ID, "forums", "unsubscribe").send()

	a.template("forums/unsubscribe", a.input("format"))
}

// Received an unsubscribe from member
func forums_unsubscribe_event(e *Event) {
	f := forum_by_id(e.user, e.db, e.to)
	if f == nil {
		return
	}

	e.db.exec("delete from members where forum=? and id=?", e.to, e.from)
	forum_update(e.user, e.db, f)
}

// Send updated forum details to members
func forum_update(u *User, db *DB, f *Forum) {
	var ms []ForumMember
	db.scans(&ms, "select * from members where forum=? and role!='disabled'", f.ID)
	db.exec("update forums set members=?, updated=? where id=?", len(ms), now(), f.ID)

	for _, m := range ms {
		if m.ID != u.Identity.ID {
			message(f.ID, m.ID, "forums", "update").set("members", itoa(len(ms))).send()
		}
	}
}

// Received a forum update event from owner
func forums_update_event(e *Event) {
	debug("Forum receieved update event '%+v'", e)

	f := forum_by_id(e.user, e.db, e.from)
	if f == nil {
		return
	}

	members := e.get("members", "0")
	if !valid(members, "natural") {
		info("Forum dropping update with invalid number of members '%s'", members)
		return
	}
	e.db.exec("update forums set members=?, updated=? where id=?", members, now(), f.ID)
}

// View a forum, or all forums
func forums_view(a *Action) {
	forum := a.input("forum")

	var f *Forum = nil
	db := a.owner.db
	if forum != "" {
		f = forum_by_id(a.owner, db, forum)
	} else if a.user != nil {
		db = a.user.db
	}

	identity := ""
	if a.user != nil {
		identity = a.user.Identity.ID
	}

	if identity == "" && f == nil {
		a.error(404, "No forum specified")
		return
	}

	var m *ForumMember = nil
	if a.user != nil && f != nil {
		m = &ForumMember{}
		if !db.scan(m, "select * from members where forum=? and id=?", f.ID, a.user.Identity.ID) {
			m = nil
		}
	}
	if m == nil && f != nil && f.Role == "disabled" {
		a.error(404, "Forum not found")
		return
	}

	var ps []ForumPost
	if f != nil {
		db.scans(&ps, "select * from posts where forum=? order by updated desc", f.ID)
	} else {
		db.scans(&ps, "select * from posts order by updated desc")
	}

	for i, p := range ps {
		var f Forum
		if db.scan(&f, "select name from forums where id=?", p.Forum) {
			ps[i].ForumFingerprint = fingerprint(f.ID)
			ps[i].ForumName = f.Name
		}
		ps[i].BodyMarkdown = web_markdown(p.Body)
		ps[i].CreatedString = time_local(a.user, p.Created)
		ps[i].Attachments = attachments(a.owner, fmt.Sprintf("forums/%s/%s", p.Forum, p.ID))
	}

	var fs []Forum
	db.scans(&fs, "select * from forums order by updated desc")

	a.template("forums/view", a.input("format"), Map{"Forum": f, "Posts": &ps, "Forums": fs, "User": a.user, "Member": m, "RoleAdministrator": forum_role(m, "administrator")})
}
