// Mochi: Feeds app
// Copyright Alistair Cunningham 2025

package main

import (
	"fmt"
	"html/template"
)

type Feed struct {
	ID          string  `cbor:"id" json:"id"`
	Fingerprint string  `cbor:"-" json:"fingerprint"`
	Name        string  `cbor:"name" json:"name"`
	Privacy     string  `cbor:"privacy" json:"privacy"`
	Owner       int     `cbor:"-" json:"-"`
	Subscribers int     `cbor:"subscribers" json:"subscribers"`
	Updated     int64   `cbor:"updated" json:"updated"`
	entity      *Entity `cbor:"-" json:"-"`
}

type FeedSubscriber struct {
	Feed     string `cbor:"feed" json:"feed"`
	ID       string `cbor:"id" json:"id"`
	Name     string `cbor:"name" json:"name"`
	FeedName string `cbor:"-" json:"-"`
}

type FeedPost struct {
	ID              string          `cbor:"id" json:"id"`
	Feed            string          `cbor:"-" json:"feed"`
	FeedFingerprint string          `cbor:"-" json:"-"`
	FeedName        string          `cbor:"-" json:"-"`
	Created         int64           `cbor:"created" json:"created"`
	CreatedString   string          `cbor:"-" json:"-"`
	Updated         int64           `cbor:"updated" json:"updated"`
	Body            string          `cbor:"body" json:"body"`
	BodyMarkdown    template.HTML   `cbor:"-" json:"-"`
	MyReaction      string          `cbor:"-" json:"-"`
	Attachments     *[]Attachment   `cbor:"attachments,omitempty" json:"attachments,omitempty"`
	Reactions       *[]FeedReaction `cbor:"-" json:"-"`
	Comments        *[]FeedComment  `cbor:"-" json:"-"`
}

type FeedComment struct {
	ID              string          `cbor:"id" json:"id"`
	Feed            string          `cbor:"-" json:"-"`
	FeedFingerprint string          `cbor:"-" json:"-"`
	Post            string          `cbor:"post" json:"post"`
	Parent          string          `cbor:"parent" json:"parent"`
	Created         int64           `cbor:"created" json:"created"`
	CreatedString   string          `cbor:"-" json:"-"`
	Subscriber      string          `cbor:"subscriber" json:"subscriber"`
	Name            string          `cbor:"name" json:"name"`
	Body            string          `cbor:"body" json:"body`
	BodyMarkdown    template.HTML   `cbor:"-" json:"-"`
	MyReaction      string          `cbor:"-" json:"-"`
	Reactions       *[]FeedReaction `cbor:"-" json:"-"`
	Children        *[]FeedComment  `cbor:"-" json:"-"`
	User            int             `cbor:"-" json:"-"`
}

type FeedReaction struct {
	Feed       string `cbor:"feed" json:"feed"`
	Post       string `cbor:"post" json:"post"`
	Comment    string `cbor:"comment,omitempty" json:"comment,omitempty"`
	Subscriber string `cbor:"subscriber" json:"subscriber"`
	Name       string `cbor:"name" json:"name"`
	Reaction   string `cbor:"reaction" json:"reaction"`
}

func init() {
	a := app("feeds")
	a.icon("feeds", "feeds", "Feeds", "feeds.png")
	a.entity("feed")
	a.db("feeds/feeds.db", feeds_db_create)

	a.path("feeds", feeds_view)
	a.path("feeds/create", feeds_create)
	a.path("feeds/find", feeds_find)
	a.path("feeds/new", feeds_new)
	a.path("feeds/post/create", feeds_post_create)
	a.path("feeds/post/new", feeds_post_new)
	a.path("feeds/search", feeds_search)
	a.path("feeds/:feed", feeds_view)
	a.path("feeds/:feed/create", feeds_post_create)
	a.path("feeds/:feed/post", feeds_post_new)
	a.path("feeds/:feed/subscribe", feeds_subscribe)
	a.path("feeds/:feed/unsubscribe", feeds_unsubscribe)
	a.path("feeds/:feed/:post", feeds_view)
	a.path("feeds/:feed/:post/comment", feeds_comment_new)
	a.path("feeds/:feed/:post/create", feeds_comment_create)
	a.path("feeds/:feed/:post/react/:reaction", feeds_post_react)
	a.path("feeds/:feed/:post/:comment/react/:reaction", feeds_comment_react)

	a.service("feeds")
	a.event("comment/create", feeds_comment_create_event)
	a.event("comment/submit", feeds_comment_submit_event)
	a.event("comment/react", feeds_comment_reaction_event)
	a.event("post/create", feeds_post_create_event)
	a.event("post/react", feeds_post_reaction_event)
	a.event("subscribe", feeds_subscribe_event)
	a.event("unsubscribe", feeds_unsubscribe_event)
	a.event("update", feeds_update_event)
}

// Create app database
func feeds_db_create(db *DB) {
	db.exec("create table settings ( name text not null primary key, value text not null )")
	db.exec("replace into settings ( name, value ) values ( 'schema', 1 )")

	db.exec("create table feeds ( id text not null primary key, fingerprint text not null, name text not null, privacy text not null default 'public', owner integer not null default 0, subscribers integer not null default 0, updated integer not null )")
	db.exec("create index feeds_fingerprint on feeds( fingerprint )")
	db.exec("create index feeds_name on feeds( name )")
	db.exec("create index feeds_updated on feeds( updated )")

	db.exec("create table subscribers ( feed references feeds( id ), id text not null, name text not null default '', primary key ( feed, id ) )")
	db.exec("create index subscriber_id on subscribers( id )")

	db.exec("create table posts ( id text not null primary key, feed references feed( id ), body text not null, created integer not null, updated integer not null )")
	db.exec("create index posts_feed on posts( feed )")
	db.exec("create index posts_created on posts( created )")
	db.exec("create index posts_updated on posts( updated )")

	db.exec("create table comments ( id text not null primary key, feed references feed( id ), post references posts( id ), parent text not null, subscriber text not null, name text not null, body text not null, created integer not null )")
	db.exec("create index comments_feed on comments( feed )")
	db.exec("create index comments_post on comments( post )")
	db.exec("create index comments_parent on comments( parent )")
	db.exec("create index comments_created on comments( created )")

	db.exec("create table reactions ( feed references feed( id ), post references posts( id ), comment text not null default '', subscriber text not null, name text not null, reaction text not null default '', primary key ( feed, post, comment, subscriber ) )")
	db.exec("create index reactions_post on reactions( post )")
	db.exec("create index reactions_comment on reactions( comment )")
}

func feed_by_id(u *User, db *DB, id string) *Feed {
	var f Feed
	if !db.scan(&f, "select * from feeds where id=?", id) {
		if !db.scan(&f, "select * from feeds where fingerprint=?", id) {
			return nil
		}
	}

	if u != nil {
		f.entity = entity_by_user_id(u, f.ID)
	}

	return &f
}

// Get comments recursively
func feed_comments(u *User, db *DB, p *FeedPost, parent *FeedComment, depth int) *[]FeedComment {
	if depth > 1000 {
		return nil
	}

	id := ""
	if parent != nil {
		id = parent.ID
	}

	entity := ""
	if u != nil {
		entity = u.Identity.ID
	}

	var cs []FeedComment
	db.scans(&cs, "select * from comments where post=? and parent=? order by created desc", p.ID, id)
	for j, c := range cs {
		cs[j].FeedFingerprint = fingerprint(c.Feed)
		cs[j].BodyMarkdown = web_markdown(c.Body)
		cs[j].CreatedString = time_local(u, c.Created)
		cs[j].User = 0
		if u != nil {
			cs[j].User = u.ID
		}

		var r FeedReaction
		if db.scan(&r, "select reaction from reactions where comment=? and subscriber=?", c.ID, entity) {
			cs[j].MyReaction = r.Reaction
		}

		var rs []FeedReaction
		db.scans(&rs, "select * from reactions where comment=? and subscriber!=? and reaction!='' order by name", c.ID, entity)
		cs[j].Reactions = &rs

		cs[j].Children = feed_comments(u, db, p, &c, depth+1)
	}

	return &cs
}

// New comment
func feeds_comment_create(a *Action) {
	now := now()

	if a.user == nil {
		a.error(401, "Not logged in")
		return
	}

	f := feed_by_id(a.user, a.user.db, a.input("feed"))
	if f == nil {
		a.error(404, "Feed not found")
		return
	}

	post := a.input("post")
	if !a.user.db.exists("select id from posts where id=? and feed=?", post, f.ID) {
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

	a.user.db.exec("replace into comments ( id, feed, post, parent, subscriber, name, body, created ) values ( ?, ?, ?, ?, ?, ?, ?, ? )", id, f.ID, post, parent, a.user.Identity.ID, a.user.Identity.Name, body, now)
	a.user.db.exec("update posts set updated=? where id=?", now, post)
	a.user.db.exec("update feeds set updated=? where id=?", now, f.ID)

	if f.entity != nil {
		// We are the feed owner, to send to all subscribers except us
		var ss []FeedSubscriber
		a.user.db.scans(&ss, "select * from subscribers where feed=?", f.ID)
		for _, s := range ss {
			if s.ID != a.user.Identity.ID {
				message(f.ID, s.ID, "feeds", "comment/create").add(FeedComment{ID: id, Post: post, Parent: parent, Created: now, Subscriber: a.user.Identity.ID, Name: a.user.Identity.Name, Body: body}).send()
			}
		}

	} else {
		// We are not feed owner, so send to the owner
		message(a.user.Identity.ID, f.ID, "feeds", "comment/submit").add(FeedComment{ID: id, Post: post, Parent: parent, Body: body}).send()
	}

	a.template("feeds/comment/create", a.input("format"), Map{"Feed": f, "Post": post})
}

// Received a feed comment from owner
func feeds_comment_create_event(e *Event) {
	f := feed_by_id(e.user, e.db, e.from)
	if f == nil {
		info("Feed dropping comment to unknown feed")
		return
	}

	var c FeedComment
	if !e.segment(&c) {
		info("Feed dropping comment with invalid data")
		return
	}

	if !valid(c.ID, "id") {
		info("Feed dropping comment with invalid ID '%s'", c.ID)
		return
	}
	if e.db.exists("select id from comments where id=?", c.ID) {
		info("Feed dropping comment with duplicate ID '%s'", c.ID)
		return
	}

	if !valid(c.Subscriber, "entity") {
		info("Feed dropping comment with invalid subscriber '%s'", c.Subscriber)
		return
	}

	if !valid(c.Name, "name") {
		info("Feed dropping comment with invalid name '%s'", c.Name)
		return
	}

	if !valid(c.Body, "text") {
		info("Feed dropping comment with invalid body '%s'", c.Body)
		return
	}

	e.db.exec("replace into comments ( id, feed, post, parent, subscriber, name, body, created ) values ( ?, ?, ?, ?, ?, ?, ?, ? )", c.ID, f.ID, c.Post, c.Parent, c.Subscriber, c.Name, c.Body, c.Created)
	e.db.exec("update posts set updated=? where id=?", c.Created, c.Post)
	e.db.exec("update feeds set updated=? where id=?", c.Created, f.ID)
}

// Received a feed comment from subscriber
func feeds_comment_submit_event(e *Event) {
	f := feed_by_id(e.user, e.db, e.to)
	if f == nil {
		info("Feed dropping comment to unknown feed")
		return
	}

	var c FeedComment
	if !e.segment(&c) {
		info("Feed dropping comment with invalid data")
		return
	}

	if !valid(c.ID, "id") {
		info("Feed dropping comment with invalid ID '%s'", c.ID)
		return
	}
	if e.db.exists("select id from comments where id=?", c.ID) {
		info("Feed dropping comment with duplicate ID '%s'", c.ID)
		return
	}

	if !e.db.exists("select id from posts where feed=? and id=?", f.ID, c.Post) {
		info("Feed dropping comment for unknown post '%s'", c.Post)
		return
	}

	if c.Parent != "" && !e.db.exists("select id from comments where feed=? and post=? and id=?", f.ID, c.Post, c.Parent) {
		info("Feed dropping comment with unknown parent '%s'", c.Parent)
		return
	}

	s := feed_subscriber(e.db, f, e.from)
	if s == nil {
		info("Feed dropping comment from unknown subscriber '%s'", e.from)
		return
	}

	c.Created = now()
	c.Subscriber = e.from
	c.Name = s.Name

	if !valid(c.Body, "text") {
		info("Feed dropping comment with invalid body '%s'", c.Body)
		return
	}

	e.db.exec("replace into comments ( id, feed, post, parent, subscriber, name, body, created ) values ( ?, ?, ?, ?, ?, ?, ?, ? )", e.id, f.ID, c.Post, c.Parent, c.Subscriber, c.Name, c.Body, c.Created)
	e.db.exec("update posts set updated=? where id=?", c.Created, c.Post)
	e.db.exec("update feeds set updated=? where id=?", c.Created, f.ID)

	var ss []FeedSubscriber
	e.db.scans(&ss, "select * from subscribers where feed=?", f.ID)
	for _, s := range ss {
		if s.ID != e.from && s.ID != e.user.Identity.ID {
			message(f.ID, s.ID, "feeds", "comment/create").add(c).send()
		}
	}
}

// Enter details for new comment
func feeds_comment_new(a *Action) {
	if a.user == nil {
		a.error(401, "Not logged in")
		return
	}

	a.template("feeds/comment/new", a.input("format"), Map{"Feed": feed_by_id(a.user, a.user.db, a.input("feed")), "Post": a.input("post"), "Parent": a.input("parent")})
}

// Reaction to a comment
func feeds_comment_react(a *Action) {
	if a.user == nil {
		a.error(401, "Not logged in")
		return
	}

	var c FeedComment
	if !a.user.db.scan(&c, "select * from comments where id=?", a.input("comment")) {
		a.error(404, "Comment not found")
		return
	}

	f := feed_by_id(a.user, a.user.db, c.Feed)
	if f == nil {
		a.error(404, "Feed not found")
		return
	}

	reaction := feeds_reaction_valid(a.input("reaction"))
	feeds_comment_reaction_set(a.user.db, &c, a.user.Identity.ID, a.user.Identity.Name, reaction)

	if f.entity != nil {
		// We are the feed owner, to send to all subscribers except us
		var ss []FeedSubscriber
		a.user.db.scans(&ss, "select id from subscribers where feed=?", f.ID)
		for _, s := range ss {
			if s.ID != a.user.Identity.ID {
				message(f.ID, s.ID, "feeds", "comment/react").add(FeedReaction{Feed: f.ID, Post: c.Post, Comment: c.ID, Subscriber: a.user.Identity.ID, Name: a.user.Identity.Name, Reaction: reaction}).send()
			}
		}

	} else {
		// We are not feed owner, so send to the owner
		message(a.user.Identity.ID, f.ID, "feeds", "comment/react").add(FeedReaction{Comment: c.ID, Name: a.user.Identity.Name, Reaction: reaction}).send()
	}

	a.template("feeds/comment/react", a.input("format"), Map{"Feed": f, "Post": c.Post})
}

func feeds_comment_reaction_set(db *DB, c *FeedComment, subscriber string, name string, reaction string) {
	now := now()

	db.exec("replace into reactions ( feed, post, comment, subscriber, name, reaction ) values ( ?, ?, ?, ?, ?, ? )", c.Feed, c.Post, c.ID, subscriber, name, reaction)

	db.exec("update posts set updated=? where id=?", now, c.Post)
	db.exec("update feeds set updated=? where id=?", now, c.Feed)
}

// Received a feed comment reaction
func feeds_comment_reaction_event(e *Event) {
	var fr FeedReaction
	if !e.segment(&fr) {
		info("Feed dropping comment reaction with invalid data")
		return
	}

	if !valid(fr.Name, "line") {
		info("Feed dropping post reaction with invalid name '%s'", fr.Name)
		return
	}

	var c FeedComment
	if !e.db.scan(&c, "select * from comments where id=?", fr.Comment) {
		info("Feed dropping comment reaction for unknown comment")
		return
	}

	f := feed_by_id(e.user, e.db, c.Feed)
	if f == nil {
		info("Feed dropping comment reaction for unknown feed")
		return
	}

	reaction := feeds_reaction_valid(fr.Reaction)

	if f.entity != nil {
		// We are the feed owner
		s := feed_subscriber(e.db, f, e.from)
		if s == nil {
			info("Feed dropping comment reaction from unknown subscriber '%s'", e.from)
			return
		}

		feeds_comment_reaction_set(e.db, &c, e.from, fr.Name, reaction)

		var ss []FeedSubscriber
		e.db.scans(&ss, "select * from subscribers where feed=?", f.ID)
		for _, s := range ss {
			if s.ID != e.from && s.ID != e.user.Identity.ID {
				message(f.ID, s.ID, "feeds", "comment/react").add(FeedReaction{Feed: f.ID, Post: c.Post, Comment: c.ID, Subscriber: e.from, Name: fr.Name, Reaction: reaction}).send()
			}
		}

	} else {
		// We are not feed owner
		if e.from != c.Feed {
			info("Feed dropping comment reaction from unknown owner")
			return
		}
		feeds_comment_reaction_set(e.db, &c, fr.Subscriber, fr.Name, reaction)
	}
}

// Create new feed
func feeds_create(a *Action) {
	if a.user == nil {
		a.error(401, "Not logged in")
		return
	}

	name := a.input("name")
	if !valid(name, "name") {
		a.error(400, "Invalid name")
		return
	}

	privacy := a.input("privacy")
	if !valid(privacy, "^(public|private)$") {
		a.error(400, "Invalid privacy")
		return
	}

	e, err := entity_create(a.user, "feed", name, privacy, "")
	if err != nil {
		a.error(500, "Unable to create entity: %s", err)
		return
	}
	a.user.db.exec("replace into feeds ( id, fingerprint, name, owner, subscribers, updated ) values ( ?, ?, ?, 1, 1, ? )", e.ID, e.Fingerprint, name, now())
	a.user.db.exec("replace into subscribers ( feed, id, name ) values ( ?, ?, ? )", e.ID, a.user.Identity.ID, a.user.Identity.Name)

	a.template("feeds/create", a.input("format"), e.Fingerprint)
}

// Enter details of feeds to be subscribed to
func feeds_find(a *Action) {
	a.template("feeds/find", a.input("format"))
}

// Get details of a feed subscriber
func feed_subscriber(db *DB, f *Feed, subscriber string) *FeedSubscriber {
	var s FeedSubscriber
	if !db.scan(&s, "select * from subscribers where feed=? and id=?", f.ID, subscriber) {
		return nil
	}
	return &s
}

// Enter details for new feed to be created
func feeds_new(a *Action) {
	name := ""

	if !a.user.db.exists("select * from feeds where owner=1 limit 1") {
		// This is our first feed, so suggest our name as the feed name
		name = a.user.Identity.Name
	}

	a.template("feeds/new", a.input("format"), Map{"Name": name})
}

// New post. Only posts by the owner are supported for now.
func feeds_post_create(a *Action) {
	now := now()

	if a.user == nil {
		a.error(401, "Not logged in")
		return
	}

	f := feed_by_id(a.user, a.user.db, a.input("feed"))
	if f == nil {
		a.error(404, "Feed not found")
		return
	}
	if f.entity == nil {
		a.error(403, "Not feed owner")
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

	a.user.db.exec("replace into posts ( id, feed, body, created, updated ) values ( ?, ?, ?, ?, ? )", post, f.ID, body, now, now)
	a.user.db.exec("update feeds set updated=? where id=?", now, f.ID)
	attachments := a.upload_attachments("attachments", f.ID, fmt.Sprintf("feeds/%s/%s", f.ID, post), true)

	var ss []FeedSubscriber
	a.user.db.scans(&ss, "select * from subscribers where feed=? and id!=?", f.ID, a.user.Identity.ID)
	for _, s := range ss {
		message(f.ID, s.ID, "feeds", "post/create").add(FeedPost{ID: post, Created: now, Body: body, Attachments: attachments}).send()
	}

	a.template("feeds/post/create", a.input("format"), Map{"Feed": f, "Post": post})
}

// Received a feed post from the owner
func feeds_post_create_event(e *Event) {
	f := feed_by_id(e.user, e.db, e.from)
	if f == nil {
		info("Feed dropping post to unknown feed")
		return
	}

	var p FeedPost
	if !e.segment(&p) {
		info("Feed dropping post with invalid data")
		return
	}

	if !valid(p.ID, "id") {
		info("Feed dropping post with invalid ID '%s'", p.ID)
		return
	}
	if e.db.exists("select id from posts where id=?", p.ID) {
		info("Feed dropping post with duplicate ID '%s'", p.ID)
		return
	}

	if !valid(p.Body, "text") {
		info("Feed dropping post with invalid body '%s'", p.Body)
		return
	}

	e.db.exec("replace into posts ( id, feed, body, created, updated ) values ( ?, ?, ?, ?, ? )", p.ID, f.ID, p.Body, p.Created, p.Created)
	attachments_save(p.Attachments, e.user, f.ID, "feeds/%s/%s", f.ID, p.ID)

	e.db.exec("update feeds set updated=? where id=?", now(), f.ID)
}

// Enter details for new post
func feeds_post_new(a *Action) {
	if a.user == nil {
		a.error(401, "Not logged in")
		return
	}

	var fs []Feed
	a.user.db.scans(&fs, "select * from feeds where owner=1 order by name")
	if len(fs) == 0 {
		a.error(500, "You do not own any feeds")
		return
	}

	a.template("feeds/post/new", a.input("format"), Map{"Feeds": fs, "Current": a.input("current")})
}

// Reaction to a post
func feeds_post_react(a *Action) {
	if a.user == nil {
		a.error(401, "Not logged in")
		return
	}

	var p FeedPost
	if !a.user.db.scan(&p, "select * from posts where id=?", a.input("post")) {
		a.error(404, "Post not found")
		return
	}
	f := feed_by_id(a.user, a.user.db, p.Feed)
	if f == nil {
		a.error(404, "Feed not found")
		return
	}

	reaction := feeds_reaction_valid(a.input("reaction"))
	feeds_post_reaction_set(a.user.db, &p, a.user.Identity.ID, a.user.Identity.Name, reaction)

	if f.entity != nil {
		// We are the feed owner, to send to all subscribers except us
		var ss []FeedSubscriber
		a.user.db.scans(&ss, "select * from subscribers where feed=?", f.ID)
		for _, s := range ss {
			if s.ID != a.user.Identity.ID {
				message(f.ID, s.ID, "feeds", "post/react").add(FeedReaction{Feed: f.ID, Post: p.ID, Subscriber: a.user.Identity.ID, Name: a.user.Identity.Name, Reaction: reaction}).send()
			}
		}

	} else {
		// We are not feed owner, so send to the owner
		message(a.user.Identity.ID, f.ID, "feeds", "post/react").add(FeedReaction{Post: p.ID, Name: a.user.Identity.Name, Reaction: reaction}).send()
	}

	a.template("feeds/post/react", a.input("format"), Map{"Feed": f, "ID": p.ID})
}

func feeds_post_reaction_set(db *DB, p *FeedPost, subscriber string, name string, reaction string) {
	now := now()

	db.exec("replace into reactions ( feed, post, subscriber, name, reaction ) values ( ?, ?, ?, ?, ? )", p.Feed, p.ID, subscriber, name, reaction)
	db.exec("update posts set updated=? where id=?", now, p.ID)
	db.exec("update feeds set updated=? where id=?", now, p.Feed)
}

// Received a feed post reaction
func feeds_post_reaction_event(e *Event) {
	var fr FeedReaction
	if !e.segment(&fr) {
		info("Feed dropping post reaction with invalid data")
		return
	}

	if !valid(fr.Name, "line") {
		info("Feed dropping post reaction with invalid name '%s'", fr.Name)
		return
	}

	var p FeedPost
	if !e.db.scan(&p, "select * from posts where id=?", fr.Post) {
		info("Feed dropping post reaction for unknown post")
		return
	}

	f := feed_by_id(e.user, e.db, p.Feed)
	if f == nil {
		info("Feed dropping post reaction for unknown feed")
		return
	}

	reaction := feeds_reaction_valid(fr.Reaction)

	if f.entity != nil {
		// We are the feed owner
		s := feed_subscriber(e.db, f, e.from)
		if s == nil {
			info("Feed dropping post reaction from unknown subscriber")
			return
		}

		feeds_post_reaction_set(e.db, &p, e.from, fr.Name, reaction)

		var ss []FeedSubscriber
		e.db.scans(&ss, "select * from subscribers where feed=?", f.ID)
		for _, s := range ss {
			if s.ID != e.from && s.ID != e.user.Identity.ID {
				message(f.ID, s.ID, "feeds", "post/react").add(FeedReaction{Feed: f.ID, Post: p.ID, Subscriber: e.from, Name: fr.Name, Reaction: reaction}).send()
			}
		}

	} else {
		// We are not feed owner
		if e.from != p.Feed {
			info("Feed dropping post reaction from unknown owner")
			return
		}
		feeds_post_reaction_set(e.db, &p, fr.Subscriber, fr.Name, reaction)
	}
}

// Validate a reaction
func feeds_reaction_valid(reaction string) string {
	if valid(reaction, "^(|like|dislike|laugh|amazed|love|sad|angry|agree|disagree)$") {
		return reaction
	}
	return ""
}

// Search for a feed
func feeds_search(a *Action) {
	if a.user == nil {
		a.error(401, "Not logged in")
		return
	}

	search := a.input("search")
	if search == "" {
		a.error(400, "No search entered")
		return
	}
	a.template("feeds/search", a.input("format"), directory_search(a.user, "feed", search, false))
}

// Send recent posts to a new subscriber
func feed_send_recent_posts(u *User, db *DB, f *Feed, subscriber string) {
	var ps []FeedPost
	db.scans(&ps, "select * from posts where feed=? order by created desc limit 1000", f.ID)
	for _, p := range ps {
		p.Attachments = attachments(u, fmt.Sprintf("feeds/%s/%s", f.ID, p.ID))
		message(f.ID, subscriber, "feeds", "post/create").add(p).send()

		var cs []FeedComment
		db.scans(&cs, "select * from comments where post=? order by created", p.ID)
		for _, c := range cs {
			message(f.ID, subscriber, "feeds", "comment/create").add(c).send()

			var frs []FeedReaction
			db.scans(&frs, "select * from reactions where comment=?", c.ID)
			for _, fr := range frs {
				message(f.ID, subscriber, "feeds", "comment/react").add(FeedReaction{Feed: f.ID, Post: p.ID, Comment: c.ID, Subscriber: fr.Subscriber, Name: fr.Name, Reaction: fr.Reaction}).send()
			}
		}

		var frs []FeedReaction
		db.scans(&frs, "select * from reactions where post=?", p.ID)
		for _, fr := range frs {
			message(f.ID, subscriber, "feeds", "post/react").add(FeedReaction{Feed: f.ID, Post: p.ID, Subscriber: fr.Subscriber, Name: fr.Name, Reaction: fr.Reaction}).send()
		}
	}
}

// Subscribe to a feed
func feeds_subscribe(a *Action) {
	if a.user == nil {
		a.error(401, "Not logged in")
		return
	}

	feed := a.input("feed")
	if !valid(feed, "entity") {
		a.error(400, "Invalid ID")
		return
	}
	if feed_by_id(a.user, a.user.db, feed) != nil {
		a.error(400, "You are already subscribed to this feed")
		return
	}
	d := directory_by_id(feed)
	if d == nil {
		a.error(404, "Unable to find feed in directory")
		return
	}

	a.user.db.exec("replace into feeds ( id, fingerprint, name, owner, subscribers, updated ) values ( ?, ?, ?, 0, 1, ? )", feed, fingerprint(feed), d.Name, now())

	message(a.user.Identity.ID, feed, "feeds", "subscribe").set("name", a.user.Identity.Name).send()

	a.template("feeds/subscribe", a.input("format"), Map{"Feed": feed, "Fingerprint": fingerprint(feed)})
}

// Received a subscribe from a subscriber
func feeds_subscribe_event(e *Event) {
	f := feed_by_id(e.user, e.db, e.to)
	if f == nil {
		return
	}

	name := e.get("name", "")
	if !valid(name, "line") {
		debug("Feeds dropping subscribe with invalid name '%s'", name)
		return
	}

	e.db.exec("insert or ignore into subscribers ( feed, id, name ) values ( ?, ?, ? )", f.ID, e.from, name)
	e.db.exec("update feeds set subscribers=(select count(*) from subscribers where feed=?), updated=? where id=?", f.ID, now(), f.ID)

	feed_update(e.user, e.db, f)
	feed_send_recent_posts(e.user, e.db, f, e.from)
}

// Unsubscribe from feed
func feeds_unsubscribe(a *Action) {
	if a.user == nil {
		a.error(401, "Not logged in")
		return
	}

	f := feed_by_id(a.user, a.user.db, a.input("feed"))
	if f == nil {
		a.error(404, "Feed not found")
		return
	}
	if f.Owner == 1 {
		a.error(404, "You own this feed")
		return
	}

	a.user.db.exec("delete from reactions where feed=?", f.ID)
	a.user.db.exec("delete from comments where feed=?", f.ID)
	a.user.db.exec("delete from posts where feed=?", f.ID)
	a.user.db.exec("delete from subscribers where feed=?", f.ID)
	a.user.db.exec("delete from feeds where id=?", f.ID)

	if f.entity == nil {
		message(a.user.Identity.ID, f.ID, "feeds", "unsubscribe").send()
	}

	a.template("feeds/unsubscribe", a.input("format"))
}

// Received an unsubscribe from subscriber
func feeds_unsubscribe_event(e *Event) {
	f := feed_by_id(e.user, e.db, e.to)
	if f == nil {
		return
	}

	e.db.exec("delete from subscribers where feed=? and id=?", e.to, e.from)
	feed_update(e.user, e.db, f)
}

// Send updated feed details to subscribers
func feed_update(u *User, db *DB, f *Feed) {
	var ss []FeedSubscriber
	db.scans(&ss, "select * from subscribers where feed=?", f.ID)
	db.exec("update feeds set subscribers=?, updated=? where id=?", len(ss), now(), f.ID)

	for _, s := range ss {
		if s.ID != u.Identity.ID {
			message(f.ID, s.ID, "feeds", "update").set("subscribers", string(len(ss))).send()
		}
	}
}

// Received a feed update event from owner
func feeds_update_event(e *Event) {
	f := feed_by_id(e.user, e.db, e.from)
	if f == nil {
		return
	}

	subscribers := e.get("subscribers", "0")
	if !valid(subscribers, "natural") {
		info("Feed dropping update with invalid number of subscribers '%s'", subscribers)
		return
	}

	e.db.exec("update feeds set subscribers=?, updated=? where id=?", subscribers, now(), f.ID)
}

// View a feed, or all feeds
func feeds_view(a *Action) {
	feed := a.input("feed")

	var f *Feed = nil
	db := a.owner.db
	if feed != "" {
		f = feed_by_id(a.owner, db, feed)
	} else if a.user != nil {
		db = a.user.db
	}

	entity := ""
	if a.user != nil {
		entity = a.user.Identity.ID
	}

	if entity == "" && f == nil {
		a.error(404, "No feed specified")
		return
	}

	post := a.input("post")
	var ps []FeedPost
	if post != "" {
		db.scans(&ps, "select * from posts where id=?", post)
	} else if f != nil {
		db.scans(&ps, "select * from posts where feed=? order by created desc", f.ID)
	} else {
		db.scans(&ps, "select * from posts order by created desc")
	}

	for i, p := range ps {
		var f Feed
		if db.scan(&f, "select name from feeds where id=?", p.Feed) {
			ps[i].FeedFingerprint = fingerprint(p.Feed)
			ps[i].FeedName = f.Name
		}

		ps[i].BodyMarkdown = web_markdown(p.Body)
		ps[i].CreatedString = time_local(a.user, p.Created)
		ps[i].Attachments = attachments(a.owner, fmt.Sprintf("feeds/%s/%s", p.Feed, p.ID))

		var r FeedReaction
		if db.scan(&r, "select reaction from reactions where post=? and subscriber=?", p.ID, entity) {
			ps[i].MyReaction = r.Reaction
		}

		var rs []FeedReaction
		db.scans(&rs, "select * from reactions where post=? and subscriber!=? and reaction!='' order by name", p.ID, entity)
		ps[i].Reactions = &rs

		ps[i].Comments = feed_comments(a.owner, db, &p, nil, 0)
	}

	owner := false
	if db.exists("select id from feeds where owner=1 limit 1") {
		owner = true
	}

	var fs []Feed
	db.scans(&fs, "select * from feeds order by updated desc")

	a.template("feeds/view", a.input("format"), Map{"Feed": f, "Posts": &ps, "Feeds": &fs, "Owner": owner, "User": a.user})
}
