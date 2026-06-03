// Mochi server: irreparable replication detection + dual-side notification.
// Copyright Alistair Cunningham 2026

package main

// A replication stream broken (stalled on an unfillable gap, or a member
// gone) for longer than T_forget can no longer be recovered without data
// loss: the predecessor ops are past retention on the sender, and a
// snapshot-over would silently discard any divergent writes on this side.
// Past that line the stream is IRREPARABLE - a terminal state only an
// operator can resolve (remove the relationship, or re-bootstrap accepting
// the loss). We deliberately do NOT auto-repair, because after T_forget
// there is no lossless repair to perform.
//
// When a stream crosses the line we record it in replication.db.irreparable
// (so we neither re-notify nor keep warning every tick) and notify the
// administrators on BOTH sides: the local admins / owning user via a Mochi
// notification, and the peer via a replica/irreparable event so its admins
// learn of it too - as long as that machine is still up. The local side is
// leader-gated so a multi-host set notifies once; the optimistic claim lets
// a sole survivor self-elect when its only co-member is the dead peer.

// irreparable_threshold is the broken-duration past which a stalled stream
// is declared irreparable. Tied to replication_op_retention so it can never
// be shorter than the window in which a returning peer's ops are still
// replayable: only once the ops are genuinely gone is the stream truly
// unrecoverable.
const irreparable_threshold = replication_op_retention

// replication_irreparable_scan reconciles the irreparable marker table
// against the currently-stalled streams, then fires the dual-side
// notification for any newly-marked stream. Called from the replication
// manager on the same slow cadence as the pending GC.
func replication_irreparable_scan() {
	db := db_open("db/replication.db")

	// Mark any stream stalled past T_forget. Run before the pending GC
	// (the caller orders it so) because the GC purges the aged pending
	// rows that prove the gap - once they are gone the stream drops out
	// of replication_pending_stalled and there is nothing left to detect.
	// A marker, once set, is NOT auto-cleared from the stalled set: past
	// T_forget the only recovery is an operator re-bootstrap, which clears
	// it via bootstrap_set_state(done); a natural drain can't happen
	// because the predecessor ops are gone from the sender too.
	cutoff := now() - irreparable_threshold
	for _, s := range replication_pending_stalled() {
		if s.Oldest > cutoff {
			continue // broken, but not yet past T_forget
		}
		exists, _ := db.exists("select 1 from irreparable where peer=? and scope=? and user=? and db=?",
			s.Peer, s.Scope, s.User, s.Database)
		if !exists {
			db.exec("insert into irreparable (peer, scope, user, db, reason, since, notified) values (?, ?, ?, ?, 'stalled', ?, 0)",
				s.Peer, s.Scope, s.User, s.Database, now())
			warn("Replication stream irreparable: peer=%q scope=%q user=%q db=%q age=%ds (broken past T_forget; operator action required)",
				s.Peer, s.Scope, s.User, s.Database, now()-s.Oldest)
		}
	}

	// Offline members: a peer whose Sender has been unreachable past
	// T_forget, for every replication relationship it participates in
	// (whole-server pair and/or per-user host). Unlike a stalled stream
	// this has no buffered pending - the member simply went silent - so
	// it's detected from the persisted unreachable mark instead. db is ''
	// (the whole relationship, not one stream), so these never collide
	// with the per-stream stalled rows above.
	gone, _ := db.rows("select peer from peer_unreachable where since < ?", now()-irreparable_threshold)
	for _, g := range gone {
		peer, _ := g["peer"].(string)
		if peer == "" {
			continue
		}
		if paired, _ := db.exists("select 1 from pair where peer=?", peer); paired {
			replication_irreparable_offline_mark(db, peer, repl_scope_core, "")
		}
		users, _ := db.rows("select distinct user from hosts where peer=?", peer)
		for _, u := range users {
			user, _ := u["user"].(string)
			replication_irreparable_offline_mark(db, peer, repl_scope_app, user)
		}
	}

	// Notify both sides once per newly-marked stream.
	pending, _ := db.rows("select peer, scope, user, db, reason from irreparable where notified=0")
	for _, r := range pending {
		peer, _ := r["peer"].(string)
		scope, _ := r["scope"].(string)
		user, _ := r["user"].(string)
		database, _ := r["db"].(string)
		reason, _ := r["reason"].(string)
		if !replication_irreparable_leader(scope, user) {
			continue // a co-host holds the lease and will notify
		}
		replication_irreparable_notify_local(scope, user)
		replication_irreparable_notify_remote(peer, scope, user, database, reason)
		db.exec("update irreparable set notified=1 where peer=? and scope=? and user=? and db=?",
			peer, scope, user, database)
	}
}

// replication_irreparable_offline_mark records a relationship as irreparable
// because its peer has been unreachable past T_forget (reason "offline",
// db='' for the whole relationship). Idempotent; the scan fires the
// notification for notified=0 rows.
func replication_irreparable_offline_mark(db *DB, peer, scope, user string) {
	exists, _ := db.exists("select 1 from irreparable where peer=? and scope=? and user=? and db=''", peer, scope, user)
	if exists {
		return
	}
	db.exec("insert into irreparable (peer, scope, user, db, reason, since, notified) values (?, ?, ?, '', 'offline', ?, 0)",
		peer, scope, user, now())
	warn("Replication member irreparable (offline): peer=%q scope=%q user=%q (unreachable past T_forget; operator action required)",
		peer, scope, user)
}

// peer_is_replication_member reports whether this peer participates in any
// replication relationship (whole-server pair or per-user host) - the only
// peers whose unreachability is worth persisting for the offline-irreparable
// detector. Gating the peer_unreachable stamp on this keeps the table from
// accumulating rows for transient message recipients we happen never to ack
// again.
func peer_is_replication_member(id string) bool {
	if id == "" || id == net_id {
		return false
	}
	db := db_open("db/replication.db")
	if ok, _ := db.exists("select 1 from pair where peer=?", id); ok {
		return true
	}
	ok, _ := db.exists("select 1 from hosts where peer=?", id)
	return ok
}

// replication_irreparable_clear removes any irreparable markers for a peer
// (optionally narrowed to one scope). Called when an operator re-bootstraps
// the relationship (the only recovery past T_forget) or removes it
// entirely, so the terminal state and its UI badge clear cleanly.
func replication_irreparable_clear(peer, scope string) {
	db := db_open("db/replication.db")
	if scope == "" {
		db.exec("delete from irreparable where peer=?", peer)
		return
	}
	db.exec("delete from irreparable where peer=? and scope=?", peer, scope)
}

// replication_irreparable_leader gates the local notification so a
// multi-host set notifies once. Core (whole-server) streams elect among the
// pair members; app (per-user) streams elect among that user's hosts. The
// claim is optimistic, so a sole survivor whose only co-member is the dead
// peer still wins and fires.
func replication_irreparable_leader(scope, user string) bool {
	leader_scope := "platform"
	if scope == repl_scope_app && user != "" {
		leader_scope = "user:" + user
	}
	return replication_leader_claim(leader_scope, "irreparable", false)
}

// replication_irreparable_notify_local raises a Mochi notification on this
// host: to every administrator for a whole-server stream, or to the owning
// user for a per-user stream. Modelled on update_notify_admins - object=""
// so the sender shows as "Mochi server", and the title/body are resolved
// per-recipient against core labels.
func replication_irreparable_notify_local(scope, user string) {
	if scope == repl_scope_app && user != "" {
		// Per-user stream: the owning user manages it on their own hosts
		// page.
		replication_irreparable_notify_user(user, "/settings/user/replication")
		return
	}
	// Whole-server stream: every administrator manages it on the pair page.
	db := db_open("db/users.db")
	rows, err := db.rows("select uid from users where role = ?", "administrator")
	if err != nil {
		return
	}
	for _, row := range rows {
		id, _ := row["uid"].(string)
		if id != "" {
			replication_irreparable_notify_user(id, "/settings/system/replication")
		}
	}
}

// replication_irreparable_notify_user sends the irreparable notification to
// one user uid, resolving the text in that user's language and deep-linking
// to the page (`url`) where they can remove the relationship or re-sync.
func replication_irreparable_notify_user(uid, url string) {
	user := user_by_uid(uid)
	if user == nil {
		return
	}
	lang := user_language(user)
	args := Map{
		"topic":  "replica/irreparable",
		"object": "",
		"title":  resolve_core_label(lang, "replica.irreparable.title", nil),
		"body":   resolve_core_label(lang, "replica.irreparable.body", nil),
		"url":    url,
		"label":  resolve_core_label(lang, "replica.irreparable.topic", nil),
		"count":  int64(1),
	}
	if err := service_call_as_server(uid, "notifications", "send", args); err != nil {
		info("Replication irreparable: notify user %q: %v", uid, err)
	}
}

// replication_irreparable_notify_remote tells the other end of the stream
// that the relationship is irreparable, so its admins / owning user are
// notified too. Best-effort fire-and-forget: it only lands if that machine
// is up. The receiver notifies its local side but does NOT echo back, so no
// loop forms.
func replication_irreparable_notify_remote(peer, scope, user, database, reason string) {
	m := message("", "", "replication", "replica/irreparable")
	m.content = map[string]any{
		"scope":  scope,
		"user":   user,
		"db":     database,
		"reason": reason,
	}
	m.send_peer(peer)
}

// replication_irreparable_event is the inbound handler: a peer is telling us
// our replication relationship with it is irreparable from its side. We
// NOTIFY our local admins / owning user (this side may not be able to detect
// the problem locally - the peer can't reach us, but we can reach it) and
// do NOT echo the event back.
//
// Deliberately notify-only, no persistent marker: a mirrored marker here has
// no local recovery signal to clear it (our ack-based clear keys off our own
// peer_unreachable, which a remote-reported marker never sets), so it would
// orphan once the relationship recovers. This side's own Pair / My-hosts
// badge is driven by its own detection (peer_unreachable / stalled), which
// has a proper lifecycle. The notifications app dedups repeats on topic.
func replication_irreparable_event(e *Event) {
	scope, _ := e.content["scope"].(string)
	user, _ := e.content["user"].(string)
	if scope == "" {
		return
	}
	replication_irreparable_notify_local(scope, user)
	info("Replication irreparable reported by peer: peer=%q scope=%q user=%q", e.peer, scope, user)
}
