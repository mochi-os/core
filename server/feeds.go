// Comms: Feeds app
// Copyright Alistair Cunningham 2025

package main

type Feed struct {
	ID          string
	Fingerprint string
	Name        string
	Privacy     string
	Subscribers int
	Updated     int64
	identity    *Identity
}

type FeedSubscriber struct {
	Feed     string
	ID       string
	Name     string
	FeedName string `json:"-"`
}

type FeedPost struct {
	ID            string
	Feed          string `json:"-"`
	Created       int64
	CreatedString string `json:"-"`
	Updated       int64
	Author        string
	Name          string
	Type          string
	Body          string
	Link          string
	Comments      int
	FeedName      string           `json:"-"`
	Attachments   []FeedAttachment `json:",omitempty"`
}

type FeedAttachment struct {
	Feed  string
	Class string
	ID    string
	File  string
	Name  string
	Size  int64
	Rank  int
}

type FeedComment struct {
	ID            string
	Feed          string `json:"-"`
	Post          string
	Parent        string
	Created       int64
	CreatedString string `json:"-"`
	Author        string
	Name          string
	Body          string
	MyReaction    string          `json:"-"`
	Reactions     *[]FeedReaction `json:"-"`
	Children      *[]FeedComment  `json:"-"`
}

type FeedReaction struct {
	Feed       string
	Class      string
	ID         string
	Subscriber string
	Name       string
	Reaction   string
}

func init() {
	a := app("feeds")
	a.home("feeds", map[string]string{"en": "Feeds"})
	a.db("feeds.db", feeds_db_create)

	a.path("feeds", feeds_list)
	a.path("feeds/create", feeds_create)
	a.path("feeds/find", feeds_find)
	a.path("feeds/new", feeds_new)
	a.path("feeds/search", feeds_search)
	a.path("feeds/:entity", feeds_view)
	a.path("feeds/:entity/create", feeds_post_create)
	a.path("feeds/:entity/post", feeds_post_new)
	a.path("feeds/:entity/subscribe", feeds_subscribe)
	a.path("feeds/:entity/unsubscribe", feeds_unsubscribe)
	a.path("feeds/:entity/:post", feeds_post_view)
	a.path("feeds/:entity/:post/comment", feeds_comment_new)
	a.path("feeds/:entity/:post/create", feeds_comment_create)
	a.path("feeds/:entity/:post/react/:reaction", feeds_post_react)
	a.path("feeds/:entity/:post/:comment/react/:reaction", feeds_comment_react)

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

	db.exec("create table feeds ( id text not null primary key, fingerprint text not null, name text not null, privacy text not null default 'public', subscribers integer not null default 0, updated integer not null )")
	db.exec("create index feeds_fingerprint on feeds( fingerprint )")
	db.exec("create index feeds_name on feeds( name )")
	db.exec("create index feeds_updated on feeds( updated )")

	db.exec("create table subscribers ( feed references feeds( id ), id text not null, name text not null default '', primary key ( feed, id ) )")
	db.exec("create index subscriber_id on subscribers( id )")

	db.exec("create table posts ( id text not null primary key, feed references feed( id ), created integer not null, updated integer not null, author text not null, name text not null, type text not null default 'text', body text not null, link text not null default '', comments integer not null default 0 )")
	db.exec("create index posts_feed on posts( feed )")
	db.exec("create index posts_created on posts( created )")
	db.exec("create index posts_updated on posts( updated )")

	db.exec("create table attachments ( feed references feed( id ), class text not null, id text not null, file string not null, name text not null, size integer default 0, rank integer not null default 1, primary key ( class, id, file ) )")
	db.exec("create index attachments_feed on attachments( feed )")
	db.exec("create index attachments_name on attachments( name )")

	db.exec("create table comments ( id text not null primary key, feed references feed( id ), post text not null, parent text not null, created integer not null, author text not null, name text not null, body text not null )")
	db.exec("create index comments_feed on comments( feed )")
	db.exec("create index comments_post on comments( post )")
	db.exec("create index comments_parent on comments( parent )")
	db.exec("create index comments_created on comments( created )")

	db.exec("create table reactions ( feed references feed( id ), class text not null, id text not null, subscriber text not null, name text not null, reaction text not null default '', primary key ( class, id, subscriber ) )")
	db.exec("create index reactions_feed on reactions( feed )")
}

func feed_by_id(u *User, db *DB, id string) *Feed {
	var f Feed
	if !db.scan(&f, "select * from feeds where id=?", id) {
		if !db.scan(&f, "select * from feeds where fingerprint=?", id) {
			return nil
		}
	}

	if u != nil {
		f.identity = identity_by_user_id(u, f.ID)
	}

	return &f
}

// Get comments recursively
func feed_comments(u *User, db *DB, f *Feed, s *FeedSubscriber, p *FeedPost, parent *FeedComment, depth int) *[]FeedComment {
	if depth > 1000 {
		return nil
	}

	id := ""
	if parent != nil {
		id = parent.ID
	}
	var cs []FeedComment
	db.scans(&cs, "select * from comments where feed=? and post=? and parent=? order by created desc", f.ID, p.ID, id)
	for j, c := range cs {
		cs[j].CreatedString = time_local(u, c.Created)

		var r FeedReaction
		if db.scan(&r, "select reaction from reactions where class='comment' and id=? and subscriber=?", cs[j].ID, u.Identity.ID) {
			cs[j].MyReaction = r.Reaction
		}

		var rs []FeedReaction
		db.scans(&rs, "select * from reactions where class='comment' and id=? and subscriber!=? and reaction!='' order by name", cs[j].ID, u.Identity.ID)
		cs[j].Reactions = &rs

		cs[j].Children = feed_comments(u, db, f, s, p, &c, depth+1)
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

	f := feed_by_id(a.user, a.db, a.id())
	if f == nil {
		a.error(404, "Feed not found")
		return
	}

	post := a.input("post")
	if !a.db.exists("select id from posts where id=? and feed=?", post, f.ID) {
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

	a.db.exec("replace into comments ( id, feed, post, parent, created, author, name, body ) values ( ?, ?, ?, ?, ?, ?, ?, ? )", id, f.ID, post, parent, now, a.user.Identity.ID, a.user.Identity.Name, body)
	a.db.exec("update posts set updated=?, comments=comments+1 where id=?", now, post)
	a.db.exec("update feeds set updated=? where id=?", now, f.ID)

	if f.identity == nil {
		// We are not feed owner, so send to the owner
		e := Event{ID: id, From: a.user.Identity.ID, To: f.ID, Service: "feeds", Action: "comment/submit", Content: json_encode(FeedComment{ID: id, Post: post, Parent: parent, Body: body})}
		e.send()

	} else {
		// We are the feed owner, to send to all subscribers except us
		j := json_encode(FeedComment{ID: id, Post: post, Parent: parent, Created: now, Author: a.user.Identity.ID, Name: a.user.Identity.Name, Body: body})
		var ss []FeedSubscriber
		a.db.scans(&ss, "select * from subscribers where feed=?", f.ID)
		for _, s := range ss {
			if s.ID != a.user.Identity.ID {
				e := Event{ID: id, From: f.ID, To: s.ID, Service: "feeds", Action: "comment/create", Content: j}
				e.send()
			}
		}
	}

	a.template("feeds/comment/create", Map{"Feed": f, "Post": post})
}

// Received a feed comment from owner
func feeds_comment_create_event(e *Event) {
	f := feed_by_id(e.user, e.db, e.From)
	if f == nil {
		log_info("Feed dropping comment to unknown feed")
		return
	}

	var c FeedComment
	if !json_decode(&c, e.Content) {
		log_info("Feed dropping comment with invalid JSON content '%s'", e.Content)
		return
	}

	if !valid(c.Author, "public") {
		log_info("Feed dropping comment with invalid author '%s'", c.Author)
		return
	}

	if !valid(c.Name, "name") {
		log_info("Feed dropping comment with invalid name '%s'", c.Name)
		return
	}

	if !valid(c.Body, "text") {
		log_info("Feed dropping comment with invalid body '%s'", c.Body)
		return
	}

	if e.db.exists("select id from comments where id=?", e.ID) {
		log_info("Feed dropping comment with duplicate ID '%s'", e.ID)
		return
	}

	e.db.exec("replace into comments ( id, feed, post, parent, created, author, name, body ) values ( ?, ?, ?, ?, ?, ?, ?, ? )", e.ID, f.ID, c.Post, c.Parent, c.Created, c.Author, c.Name, c.Body)
	e.db.exec("update posts set updated=?, comments=comments+1 where id=?", c.Created, c.Post)
	e.db.exec("update feeds set updated=? where id=?", c.Created, f.ID)
}

// Received a feed comment from subscriber
func feeds_comment_submit_event(e *Event) {
	if e.db.exists("select id from comments where id=?", e.ID) {
		log_info("Feed dropping comment with duplicate ID '%s'", e.ID)
		return
	}

	f := feed_by_id(e.user, e.db, e.To)
	if f == nil {
		log_info("Feed dropping comment to unknown feed")
		return
	}

	var c FeedComment
	if !json_decode(&c, e.Content) {
		log_info("Feed dropping comment with invalid JSON content '%s'", e.Content)
		return
	}
	if !e.db.exists("select id from posts where feed=? and id=?", f.ID, c.Post) {
		log_info("Feed dropping comment for unknown post '%s'", c.Post)
		return
	}
	if c.Parent != "" && !e.db.exists("select id from comments where feed=? and post=? and id=?", f.ID, c.Post, c.Parent) {
		log_info("Feed dropping comment with unknown parent '%s'", c.Parent)
		return
	}
	s := feed_subscriber(e.db, f, e.From)
	if s == nil {
		log_info("Feed dropping comment from unknown subscriber '%s'", e.From)
		return
	}
	c.Created = now()
	c.Author = e.From
	c.Name = s.Name
	if !valid(c.Body, "text") {
		log_info("Feed dropping comment with invalid body '%s'", c.Body)
		return
	}

	e.db.exec("replace into comments ( id, feed, post, parent, created, author, name, body ) values ( ?, ?, ?, ?, ?, ?, ?, ? )", e.ID, f.ID, c.Post, c.Parent, c.Created, c.Author, c.Name, c.Body)
	e.db.exec("update posts set updated=?, comments=comments+1 where id=?", c.Created, c.Post)
	e.db.exec("update feeds set updated=? where id=?", c.Created, f.ID)

	j := json_encode(c)
	var ss []FeedSubscriber
	e.db.scans(&ss, "select * from subscribers where feed=?", f.ID)
	for _, s := range ss {
		if s.ID != e.From && s.ID != e.user.Identity.ID {
			e := Event{ID: e.ID, From: f.ID, To: s.ID, Service: "feeds", Action: "comment/create", Content: j}
			e.send()
		}
	}
}

// Enter details for new comment
func feeds_comment_new(a *Action) {
	if a.user == nil {
		a.error(401, "Not logged in")
		return
	}

	a.template("feeds/comment/new", Map{"Feed": feed_by_id(a.user, a.db, a.id()), "Post": a.input("post"), "Parent": a.input("parent")})
}

// Reaction to a comment
func feeds_comment_react(a *Action) {
	if a.user == nil {
		a.error(401, "Not logged in")
		return
	}

	var c FeedComment
	if !a.db.scan(&c, "select * from comments where id=?", a.input("comment")) {
		a.error(404, "Comment not found")
		return
	}

	f := feed_by_id(a.user, a.db, c.Feed)
	if f == nil {
		a.error(404, "Feed not found")
		return
	}

	reaction := feeds_reaction_valid(a.input("reaction"))
	feeds_comment_reaction_set(a.db, &c, a.user.Identity.ID, a.user.Identity.Name, reaction)

	if f.identity == nil {
		// We are not feed owner, so send to the owner
		e := Event{ID: uid(), From: a.user.Identity.ID, To: f.ID, Service: "feeds", Action: "comment/react", Content: json_encode(FeedReaction{ID: c.ID, Name: a.user.Identity.Name, Reaction: reaction})}
		e.send()

	} else {
		// We are the feed owner, to send to all subscribers except us
		id := uid()
		j := json_encode(FeedReaction{Feed: f.ID, Class: "comment", ID: c.ID, Subscriber: a.user.Identity.ID, Name: a.user.Identity.Name, Reaction: reaction})
		var ss []FeedSubscriber
		a.db.scans(&ss, "select id from subscribers where feed=?", f.ID)
		for _, s := range ss {
			if s.ID != a.user.Identity.ID {
				e := Event{ID: id, From: f.ID, To: s.ID, Service: "feeds", Action: "comment/react", Content: j}
				e.send()
			}
		}
	}

	a.template("feeds/comment/react", Map{"Feed": f, "Post": c.Post})
}

func feeds_comment_reaction_set(db *DB, c *FeedComment, subscriber string, name string, reaction string) {
	now := now()

	db.exec("replace into reactions ( feed, class, id, subscriber, name, reaction ) values ( ?, 'comment', ?, ?, ?, ? )", c.Feed, c.ID, subscriber, name, reaction)

	db.exec("update posts set updated=? where id=?", now, c.Post)
	db.exec("update feeds set updated=? where id=?", now, c.Feed)
}

// Received a feed comment reaction
func feeds_comment_reaction_event(e *Event) {
	var r FeedReaction
	if !json_decode(&r, e.Content) {
		log_info("Feed dropping comment reaction with invalid JSON content '%s'", e.Content)
		return
	}

	var c FeedComment
	if !e.db.scan(&c, "select * from comments where id=?", r.ID) {
		log_info("Feed dropping comment reaction for unknown comment")
		return
	}

	f := feed_by_id(e.user, e.db, c.Feed)
	if f == nil {
		log_info("Feed dropping comment reaction for unknown feed")
		return
	}

	reaction := feeds_reaction_valid(r.Reaction)

	if f.identity == nil {
		// We are not feed owner
		if e.From != c.Feed {
			log_info("Feed dropping comment reaction from unknown owner")
			return
		}
		feeds_comment_reaction_set(e.db, &c, r.Subscriber, r.Name, reaction)

	} else {
		// We are the feed owner
		s := feed_subscriber(e.db, f, e.From)
		if s == nil {
			log_info("Feed dropping comment reaction from unknown subscriber '%s'", e.From)
			return
		}

		feeds_comment_reaction_set(e.db, &c, e.From, r.Name, reaction)

		j := json_encode(FeedReaction{Feed: f.ID, Class: "comment", ID: c.ID, Subscriber: e.From, Name: r.Name, Reaction: reaction})
		var ss []FeedSubscriber
		e.db.scans(&ss, "select * from subscribers where feed=?", f.ID)
		for _, s := range ss {
			if s.ID != e.From && s.ID != e.user.Identity.ID {
				e := Event{ID: e.ID, From: f.ID, To: s.ID, Service: "feeds", Action: "comment/react", Content: j}
				e.send()
			}
		}
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

	i, err := identity_create(a.user, "feed", name, privacy, "")
	if err != nil {
		a.error(500, "Unable to create identity: %s", err)
		return
	}
	a.db.exec("replace into feeds ( id, fingerprint, name, subscribers, updated ) values ( ?, ?, ?, 1, ? )", i.ID, i.Fingerprint, name, now())
	a.db.exec("replace into subscribers ( feed, id, name ) values ( ?, ?, ? )", i.ID, a.user.Identity.ID, a.user.Identity.Name)

	a.template("feeds/create", i.Fingerprint)
}

// Enter details of feeds to be subscribed to
func feeds_find(a *Action) {
	a.template("feeds/find")
}

// List existing feeds and show posts from all of them
func feeds_list(a *Action) {
	var fs []Feed
	a.db.scans(&fs, "select * from feeds order by updated desc")

	var ps []FeedPost
	a.db.scans(&ps, "select * from posts order by updated desc")

	a.template("feeds/list", Map{"Feeds": fs, "Posts": &ps})
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

	if !a.db.exists("select * from feeds") {
		// This is our first feed, so suggest our name as the feed name
		name = a.user.Identity.Name
	}

	a.template("feeds/new", Map{"Name": name})
}

// New post by owner
func feeds_post_create(a *Action) {
	now := now()

	if a.user == nil {
		a.error(401, "Not logged in")
		return
	}

	f := feed_by_id(a.user, a.db, a.id())
	if f == nil {
		a.error(404, "Feed not found")
		return
	}
	if f.identity == nil {
		a.error(403, "Not feed owner")
	}

	body := a.input("body")
	if !valid(body, "text") {
		a.error(400, "Invalid body")
		return
	}

	id := uid()
	if a.db.exists("select id from posts where id=?", id) {
		a.error(500, "Duplicate ID")
		return
	}

	a.db.exec("replace into posts ( id, feed, created, updated, author, name, body ) values ( ?, ?, ?, ?, ?, ?, ? )", id, f.ID, now, now, a.user.Identity.ID, a.user.Identity.Name, body)
	a.db.exec("update feeds set updated=? where id=?", now, f.ID)

	var as []FeedAttachment
	for _, at := range a.upload("attachments") {
		a.db.exec("replace into attachments ( feed, class, id, file, name, size, rank ) values ( ?, 'post', ?, ?, ?, ?, ? )", f.ID, id, at.ID, at.Name, at.Size, at.Rank)
		as = append(as, FeedAttachment{Feed: f.ID, Class: "post", ID: id, File: at.ID, Name: at.Name, Size: at.Size, Rank: at.Rank})
	}

	j := json_encode(FeedPost{ID: id, Created: now, Author: a.user.Identity.ID, Name: a.user.Identity.Name, Body: body, Attachments: as})
	var ss []FeedSubscriber
	a.db.scans(&ss, "select * from subscribers where feed=? and id!=?", f.ID, a.user.Identity.ID)
	for _, s := range ss {
		e := Event{ID: id, From: f.ID, To: s.ID, Service: "feeds", Action: "post/create", Content: j}
		e.send()
	}

	a.template("feeds/post/create", Map{"Feed": f, "ID": id})
}

// Received a feed post from the owner
func feeds_post_create_event(e *Event) {
	f := feed_by_id(e.user, e.db, e.From)
	if f == nil {
		log_info("Feed dropping post to unknown feed")
		return
	}

	var p FeedPost
	if !json_decode(&p, e.Content) {
		log_info("Feed dropping post with invalid JSON content '%s'", e.Content)
		return
	}

	if !valid(p.Author, "public") {
		log_info("Feed dropping post with invalid author '%s'", p.Author)
		return
	}

	if !valid(p.Name, "name") {
		log_info("Feed dropping post with invalid name '%s'", p.Name)
		return
	}

	if !valid(p.Body, "text") {
		log_info("Feed dropping post with invalid body '%s'", p.Body)
		return
	}

	if e.db.exists("select id from posts where id=?", e.ID) {
		log_info("Feed dropping post with duplicate ID '%s'", e.ID)
		return
	}

	e.db.exec("replace into posts ( id, feed, created, updated, author, name, body ) values ( ?, ?, ?, ?, ?, ?, ? )", e.ID, f.ID, p.Created, p.Created, p.Author, p.Name, p.Body)

	for _, at := range p.Attachments {
		e.db.exec("replace into attachments ( feed, class, id, file, name, size, rank ) values ( ?, 'post', ?, ?, ?, ?, ? )", f.ID, e.ID, at.File, at.Name, at.Size, at.Rank)
	}

	e.db.exec("update feeds set updated=? where id=?", now(), f.ID)
}

// Enter details for new post
func feeds_post_new(a *Action) {
	if a.user == nil {
		a.error(401, "Not logged in")
		return
	}

	a.template("feeds/post/new", feed_by_id(a.user, a.db, a.id()))
}
// Reaction to a post
func feeds_post_react(a *Action) {
	if a.user == nil {
		a.error(401, "Not logged in")
		return
	}

	var p FeedPost
	if !a.db.scan(&p, "select * from posts where id=?", a.input("post")) {
		a.error(404, "Post not found")
		return
	}
	f := feed_by_id(a.user, a.db, p.Feed)
	if f == nil {
		a.error(404, "Feed not found")
		return
	}

	reaction := feeds_reaction_valid(a.input("reaction"))
	feeds_post_reaction_set(a.db, &p, a.user.Identity.ID, a.user.Identity.Name, reaction)

	if f.identity == nil {
		// We are not feed owner, so send to the owner
		e := Event{ID: uid(), From: a.user.Identity.ID, To: f.ID, Service: "feeds", Action: "post/react", Content: json_encode(FeedReaction{ID: p.ID, Name: a.user.Identity.Name, Reaction: reaction})}
		e.send()

	} else {
		// We are the feed owner, to send to all subscribers except us
		id := uid()
		j := json_encode(FeedReaction{Feed: f.ID, Class: "post", ID: p.ID, Subscriber: a.user.Identity.ID, Name: a.user.Identity.Name, Reaction: reaction})
		var ss []FeedSubscriber
		a.db.scans(&ss, "select * from subscribers where feed=?", f.ID)
		for _, s := range ss {
			if s.ID != a.user.Identity.ID {
				e := Event{ID: id, From: f.ID, To: s.ID, Service: "feeds", Action: "post/react", Content: j}
				e.send()
			}
		}
	}

	a.template("feeds/post/react", Map{"Feed": f, "ID": p.ID})
}

func feeds_post_reaction_set(db *DB, p *FeedPost, subscriber string, name string, reaction string) {
	now := now()

	db.exec("replace into reactions ( feed, class, id, subscriber, name, reaction ) values ( ?, 'post', ?, ?, ?, ? )", p.Feed, p.ID, subscriber, name, reaction)
	db.exec("update posts set updated=? where id=?", now, p.ID)
	db.exec("update feeds set updated=? where id=?", now, p.Feed)
}

// Received a feed post reaction
func feeds_post_reaction_event(e *Event) {
	var r FeedReaction
	if !json_decode(&r, e.Content) {
		log_info("Feed dropping post reaction with invalid JSON content '%s'", e.Content)
		return
	}

	var p FeedPost
	if !e.db.scan(&p, "select * from posts where id=?", r.ID) {
		log_info("Feed dropping post reaction for unknown post")
		return
	}

	f := feed_by_id(e.user, e.db, p.Feed)
	if f == nil {
		log_info("Feed dropping post reaction for unknown feed")
		return
	}

	reaction := feeds_reaction_valid(r.Reaction)

	if f.identity == nil {
		// We are not feed owner
		if e.From != p.Feed {
			log_info("Feed dropping post reaction from unknown owner")
			return
		}
		feeds_post_reaction_set(e.db, &p, r.Subscriber, r.Name, reaction)

	} else {
		// We are the feed owner
		s := feed_subscriber(e.db, f, e.From)
		if s == nil {
			log_info("Feed dropping post reaction from unknown subscriber")
			return
		}

		feeds_post_reaction_set(e.db, &p, e.From, r.Name, reaction)

		j := json_encode(FeedReaction{Feed: f.ID, Class: "post", ID: p.ID, Subscriber: e.From, Name: r.Name, Reaction: reaction})
		var ss []FeedSubscriber
		e.db.scans(&ss, "select * from subscribers where feed=?", f.ID)
		for _, s := range ss {
			if s.ID != e.From && s.ID != e.user.Identity.ID {
				e := Event{ID: e.ID, From: f.ID, To: s.ID, Service: "feeds", Action: "post/react", Content: j}
				e.send()
			}
		}
	}
}

// View a post
func feeds_post_view(a *Action) {
	var p FeedPost
	if !a.db.scan(&p, "select * from posts where id=?", a.input("post")) {
		a.error(404, "Post not found")
		return
	}
	p.CreatedString = time_local(a.user, p.Created)

	f := feed_by_id(a.user, a.db, p.Feed)
	if f == nil {
		a.error(404, "Feed not found")
		return
	}
	var s *FeedSubscriber = nil
	if a.user != nil {
		s = &FeedSubscriber{}
		if !a.db.scan(s, "select * from subscribers where feed=? and id=?", f.ID, a.user.Identity.ID) {
			s = nil
		}
	}

	var as []FeedAttachment
	a.db.scans(&as, "select * from attachments where class='post' and id=? order by rank, name", p.ID)

	var r FeedReaction
	a.db.scan(&r, "select reaction from reactions where class='post' and id=? and subscriber=?", p.ID, a.user.Identity.ID)

	var rs []FeedReaction
	a.db.scans(&rs, "select * from reactions where class='post' and id=? and subscriber!=? and reaction!='' order by name", p.ID, a.user.Identity.ID)

	a.template("feeds/post/view", Map{"Feed": f, "Post": &p, "Attachments": &as, "Comments": feed_comments(a.user, a.db, f, s, &p, nil, 0), "MyReaction": r.Reaction, "Reactions": rs})
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
	a.template("feeds/search", directory_search(a.user, "feed", search, false))
}

// Send recent posts to a subscriber
func feed_send_recent_posts(db *DB, f *Feed, subscriber string) {
	var ps []FeedPost
	db.scans(&ps, "select * from posts where feed=? order by updated desc limit 1000", f.ID)
	for _, p := range ps {
		var as []FeedAttachment
		db.scans(&as, "select * from attachments where class='post' and id=? order by rank, name", p.ID)
		p.Attachments = as

		e := Event{ID: p.ID, From: f.ID, To: subscriber, Service: "feeds", Action: "post/create", Content: json_encode(p)}
		e.send()

		var cs []FeedComment
		db.scans(&cs, "select * from comments where post=?", p.ID)
		for _, c := range cs {
			e := Event{ID: c.ID, From: f.ID, To: subscriber, Service: "feeds", Action: "comment/create", Content: json_encode(c)}
			e.send()

			var rs []FeedReaction
			db.scans(&rs, "select * from reactions where class='comment' and id=?", c.ID)
			for _, r := range rs {
				e := Event{ID: uid(), From: f.ID, To: subscriber, Service: "feeds", Action: "comment/react", Content: json_encode(FeedReaction{Class: "comment", ID: c.ID, Subscriber: r.Subscriber, Name: r.Name, Reaction: r.Reaction})}
				e.send()
			}
		}

		var rs []FeedReaction
		db.scans(&rs, "select * from reactions where class='post' and id=?", p.ID)
		for _, r := range rs {
			e := Event{ID: uid(), From: f.ID, To: subscriber, Service: "feeds", Action: "post/react", Content: json_encode(FeedReaction{Class: "post", ID: p.ID, Subscriber: r.Subscriber, Name: r.Name, Reaction: r.Reaction})}
			e.send()
		}
	}
}

// Subscribe to a feed
func feeds_subscribe(a *Action) {
	if a.user == nil {
		a.error(401, "Not logged in")
		return
	}

	id := a.id()
	if !valid(id, "public") {
		a.error(400, "Invalid ID")
		return
	}
	if feed_by_id(a.user, a.db, id) != nil {
		a.error(400, "You are already subscribed to this feed")
		return
	}
	d := directory_by_id(id)
	if d == nil {
		a.error(404, "Unable to find feed in directory")
		return
	}

	a.db.exec("replace into feeds ( id, fingerprint, name, subscribers, updated ) values ( ?, ?, ?, 1, ? )", id, fingerprint(id), d.Name, now())
	a.db.exec("replace into subscribers ( feed, id, name ) values ( ?, ?, ? )", id, a.user.Identity.ID, a.user.Identity.Name)

	e := Event{ID: uid(), From: a.user.Identity.ID, To: id, Service: "feeds", Action: "subscribe", Content: json_encode(map[string]string{"name": a.user.Identity.Name})}
	e.send()

	a.template("feeds/subscribe", Map{"Feed": id})
}

// Received a subscribe from a subscriber
func feeds_subscribe_event(e *Event) {
	f := feed_by_id(e.user, e.db, e.To)
	if f == nil {
		return
	}

	var s FeedSubscriber
	if !json_decode(&s, e.Content) {
		log_info("Feed dropping subscribe event with invalid JSON")
	}

	e.db.exec("insert or ignore into subscribers ( feed, id, name ) values ( ?, ?, ? )", f.ID, e.From, s.Name)
	e.db.exec("update feeds set subscribers=(select count(*) from subscribers where feed=?), updated=? where id=?", f.ID, now(), f.ID)

	feed_update(e.user, e.db, f)
	feed_send_recent_posts(e.db, f, e.From)
}

// Unsubscribe from feed
func feeds_unsubscribe(a *Action) {
	if a.user == nil {
		a.error(401, "Not logged in")
		return
	}

	f := feed_by_id(a.user, a.db, a.id())
	if f == nil {
		a.error(404, "Feed not found")
		return
	}

	a.db.exec("delete from reactions where feed=?", f.ID)
	a.db.exec("delete from comments where feed=?", f.ID)
	a.db.exec("delete from posts where feed=?", f.ID)
	a.db.exec("delete from subscribers where feed=?", f.ID)
	a.db.exec("delete from feeds where id=?", f.ID)

	if f.identity == nil {
		e := Event{ID: uid(), From: a.user.Identity.ID, To: f.ID, Service: "feeds", Action: "unsubscribe"}
		e.send()
	}

	a.template("feeds/unsubscribe")
}

// Received an unsubscribe from subscriber
func feeds_unsubscribe_event(e *Event) {
	f := feed_by_id(e.user, e.db, e.To)
	if f == nil {
		return
	}

	e.db.exec("delete from subscribers where feed=? and id=?", e.To, e.From)
	feed_update(e.user, e.db, f)
}

// Send updated feed details to subscribers
func feed_update(u *User, db *DB, f *Feed) {
	var ss []FeedSubscriber
	db.scans(&ss, "select * from subscribers where feed=?", f.ID)
	db.exec("update feeds set subscribers=?, updated=? where id=?", len(ss), now(), f.ID)

	j := json_encode(Map{"subscribers": len(ss)})
	id := uid()
	for _, s := range ss {
		if s.ID != u.Identity.ID {
			e := Event{ID: id, From: f.ID, To: s.ID, Service: "feeds", Action: "update", Content: j}
			e.send()
		}
	}
}

// Received a feed update event from owner
func feeds_update_event(e *Event) {
	f := feed_by_id(e.user, e.db, e.From)
	if f == nil {
		return
	}

	var n Feed
	if !json_decode(&n, e.Content) {
		log_info("Feed dropping update with invalid JSON content '%s'", e.Content)
		return
	}

	e.db.exec("update feeds set subscribers=?, updated=? where id=?", n.Subscribers, now(), f.ID)
}

// View a feed
func feeds_view(a *Action) {
	f := feed_by_id(a.user, a.db, a.id())
	if f == nil {
		a.error(404, "Feed not found")
		return
	}

	owner := false
	if f.identity != nil {
		owner = true
	}

	var s *FeedSubscriber = nil
	if a.user != nil {
		s = &FeedSubscriber{}
		if !a.db.scan(s, "select * from subscribers where feed=? and id=?", f.ID, a.user.Identity.ID) {
			s = nil
		}
	}

	var ps []FeedPost
	a.db.scans(&ps, "select * from posts where feed=? order by updated desc", f.ID)

	a.template("feeds/view", Map{"Feed": f, "Owner": owner, "Subscriber": s, "Posts": &ps})
}
