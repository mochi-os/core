// Mochi server: /_/admin/replication/* handlers for mochictl
// Copyright Alistair Cunningham 2026
//
// Sibling to admin_replica.go. The /_/admin/replica/* endpoints are
// for the *joining replica's* perspective (join / leave / status of
// THIS server's pair attempt). The /_/admin/replication/* endpoints
// below are for operator inspection / management of the pair set as
// it exists right now — listing current members, removing a specific
// member, summarising replication health.
//
//   GET  /_/admin/replication/status         — pair + per-user-host + bootstrap-pending summary
//   GET  /_/admin/replication/pair           — current pair members
//   GET  /_/admin/replication/pairs          — per-pair health rollup (bootstrap, cursors, pending, leases)
//   GET  /_/admin/replication/progress       — per-(peer, scope) bootstrap progress
//   GET  /_/admin/replication/ops[?user=]    — per-(user, scope) op-replication snapshot
//   GET  /_/admin/replication/stalled        — stuck inbound streams (pending buffer cannot drain)
//   POST /_/admin/replication/pair/remove    — kick a specific pair member
//   POST /_/admin/replication/pending/gc     — on-demand purge of aged unfillable pending rows
//   POST /_/admin/replication/resync         — re-run bulk bootstrap against a peer
//
// Per-(user, scope) lag is a future addition; the current status
// surfaces an aggregate `bootstrap_pending` count + a drill-down via
// `/progress` that's enough for operators to spot stuck transfers.

//go:build linux || darwin

package main

import (
	"net/http"
	"sort"

	"github.com/gin-gonic/gin"
)

// admin_replication_status is GET /_/admin/replication/status.
// Returns a minimal snapshot of the replication state visible from
// this server: own peer-id, current pair members, count of per-user
// opt-in hosts, count of pending link-requests, count of pending
// join-requests.
//
// Bootstrap progress is surfaced as the aggregate `bootstrap_pending`
// count here; the per-(peer, scope) drill-down lives at
// `/_/admin/replication/progress`. Per-(user, scope) op-replication
// lag is a future addition.
func admin_replication_status(c *gin.Context) {
	rdb := db_open("db/replication.db")

	var pair []string
	if rows, err := rdb.rows("select peer from pair"); err == nil {
		for _, r := range rows {
			if p, ok := r["peer"].(string); ok && p != "" {
				pair = append(pair, p)
			}
		}
	}

	hosts_count := int64(0)
	if row, _ := rdb.row("select count(*) as c from hosts"); row != nil {
		if v, ok := row["c"].(int64); ok {
			hosts_count = v
		}
	}

	links_pending := int64(0)
	if row, _ := rdb.row("select count(*) as c from links"); row != nil {
		if v, ok := row["c"].(int64); ok {
			links_pending = v
		}
	}

	joins_pending := int64(0)
	if row, _ := rdb.row("select count(*) as c from joins"); row != nil {
		if v, ok := row["c"].(int64); ok {
			joins_pending = v
		}
	}

	bootstrap_pending := int64(0)
	if row, _ := rdb.row("select count(*) as c from bootstrap where state != 'done'"); row != nil {
		if v, ok := row["c"].(int64); ok {
			bootstrap_pending = v
		}
	}

	c.JSON(http.StatusOK, gin.H{
		"peer":              net_id,
		"pair":              pair,
		"hosts_count":       hosts_count,
		"links_pending":     links_pending,
		"joins_pending":     joins_pending,
		"bootstrap_pending": bootstrap_pending,
	})
}

// admin_replication_pair is GET /_/admin/replication/pair.
// Returns the current pair member list.
func admin_replication_pair(c *gin.Context) {
	rdb := db_open("db/replication.db")
	var members []map[string]any
	if rows, err := rdb.rows("select peer, added, role from pair order by added"); err == nil {
		for _, r := range rows {
			peer, _ := r["peer"].(string)
			added, _ := r["added"].(int64)
			role, _ := r["role"].(string)
			members = append(members, map[string]any{
				"peer":  peer,
				"added": added,
				"role":  role,
			})
		}
	}
	c.JSON(http.StatusOK, gin.H{"members": members})
}

// admin_replication_resync is POST /_/admin/replication/resync.
// Body: {"peer": "<peer-id>"}
//
// Re-seeds the bootstrap state machine against `peer` by clearing
// any 'done' rows for that peer and re-running bootstrap_start.
// Idempotent on the wire: the receiver's manifest-diff skips files
// whose local copy already matches by size + sha256, so a resync of
// a fully-synced replica is cheap (just hash comparisons).
//
// Use cases: a transient transport failure dropped some chunks; the
// operator manually copied files between data dirs and wants to
// re-verify; alpha-rollout verification re-runs.
func admin_replication_resync(c *gin.Context) {
	var input struct {
		Peer string `json:"peer"`
	}
	if err := c.ShouldBindJSON(&input); err != nil || input.Peer == "" {
		respond_error(c, http.StatusBadRequest, "missing_peer", "errors.missing_peer", nil)
		return
	}

	// Safety: refuse if this server has users. Bulk bootstrap on a
	// populated, running server overwrites SQLite files mid-flight
	// and crashes the daemon (caught live by a 'database is locked'
	// panic in queue_add_direct after a clobber). Bootstrap is meant
	// for fresh replicas; this matches the existing /_/admin/replica/join
	// guard.
	udb := db_open("db/users.db")
	if has, _ := udb.exists("select 1 from users limit 1"); has {
		respond_error(c, http.StatusForbidden, "populated_server", "errors.populated_server", nil)
		return
	}

	// Wipe any previous bootstrap rows for this peer so the state
	// machine starts fresh. bootstrap_start re-seeds the four scopes
	// at 'queued' and emits the manifest-requests.
	rdb := db_open("db/replication.db")
	rdb.exec("delete from bootstrap where peer=?", input.Peer)
	bootstrap_start(input.Peer)

	c.JSON(http.StatusOK, gin.H{
		"peer":  input.Peer,
		"state": "queued",
	})
}

// admin_replication_backfill is POST /_/admin/replication/backfill.
// Re-runs replication_pair_backfill against `peer`. Unlike the
// bulk-bootstrap resync, this is safe on a populated host: it emits
// rows through the live op channel (REPLACE INTO on the receiver),
// never rename-replaces an open DB file. Used after adding a new
// system table to backfill coverage, and as an ops escape hatch when
// per-event ops missed a window of state between pair members.
func admin_replication_backfill(c *gin.Context) {
	var input struct {
		Peer string `json:"peer"`
	}
	if err := c.ShouldBindJSON(&input); err != nil || input.Peer == "" {
		respond_error(c, http.StatusBadRequest, "missing_peer", "errors.missing_peer", nil)
		return
	}
	go replication_pair_backfill(input.Peer)
	c.JSON(http.StatusOK, gin.H{
		"peer":  input.Peer,
		"state": "dispatched",
	})
}

// admin_replication_progress is GET /_/admin/replication/progress.
// Returns the per-(peer, scope) bulk-bootstrap progress as
// {"rows": [{"peer", "scope", "state", "position"}, ...]}. Same data
// as the mochi.replication.bootstrap.progress() Starlark API, exposed
// over the admin socket for mochictl.
//
// Optional `peer` query parameter filters to a single peer.
func admin_replication_progress(c *gin.Context) {
	rdb := db_open("db/replication.db")
	var rows []map[string]any
	var err error
	if peer_filter := c.Query("peer"); peer_filter != "" {
		rows, err = rdb.rows("select peer, scope, state, position from bootstrap where peer=? order by peer, scope", peer_filter)
	} else {
		rows, err = rdb.rows("select peer, scope, state, position from bootstrap order by peer, scope")
	}
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"rows": rows})
}

// admin_replication_ops is GET /_/admin/replication/ops[?user=<uid>].
// Returns a snapshot of per-(user, scope) op replication: this server's
// emit high-water marks, the highest sequence applied from every peer,
// and the pending-buffer state. Aimed at the cross-host manual test
// flow — answers "did host A's writes reach host B?" in one call.
//
// Without ?user: returns aggregates across every user.
// With ?user=<uid>: returns the per-user breakdown.
func admin_replication_ops(c *gin.Context) {
	rdb := db_open("db/replication.db")
	user_filter := c.Query("user")

	type seqRow struct {
		User  string `db:"user"`
		Scope string `db:"scope"`
		Next  int64  `db:"next"`
	}
	type seenRow struct {
		Peer  string `db:"peer"`
		User  string `db:"user"`
		Scope string `db:"scope"`
		Max   int64  `db:"max"`
	}
	type pendRow struct {
		Peer    string `db:"peer"`
		User    string `db:"user"`
		Scope   string `db:"scope"`
		Count   int64  `db:"count"`
		Oldest  int64  `db:"oldest"`
	}

	emitted_query := "select user, scope, next from sequence"
	seen_query := "select peer, user, scope, max(sequence) as max from seen group by peer, user, scope"
	pending_query := "select peer, user, scope, count(*) as count, min(received) as oldest from pending group by peer, user, scope"
	args := []any{}
	if user_filter != "" {
		emitted_query += " where user=?"
		seen_query = "select peer, user, scope, max(sequence) as max from seen where user=? group by peer, user, scope"
		pending_query = "select peer, user, scope, count(*) as count, min(received) as oldest from pending where user=? group by peer, user, scope"
		args = []any{user_filter}
	}

	emitted_rows, _ := rdb.rows(emitted_query, args...)
	seen_rows, _ := rdb.rows(seen_query, args...)
	pending_rows, _ := rdb.rows(pending_query, args...)

	// emitted_by_user[user][scope] = highest local sequence
	emitted_by_user := map[string]map[string]int64{}
	for _, r := range emitted_rows {
		u, _ := r["user"].(string)
		s, _ := r["scope"].(string)
		n, _ := r["next"].(int64)
		if emitted_by_user[u] == nil {
			emitted_by_user[u] = map[string]int64{}
		}
		emitted_by_user[u][s] = n
	}

	// applied_by_user[user][peer][scope] = highest sequence we've applied
	applied_by_user := map[string]map[string]map[string]int64{}
	for _, r := range seen_rows {
		u, _ := r["user"].(string)
		p, _ := r["peer"].(string)
		s, _ := r["scope"].(string)
		m, _ := r["max"].(int64)
		if applied_by_user[u] == nil {
			applied_by_user[u] = map[string]map[string]int64{}
		}
		if applied_by_user[u][p] == nil {
			applied_by_user[u][p] = map[string]int64{}
		}
		applied_by_user[u][p][s] = m
	}

	// pending_by_user[user][peer][scope] = {count, age_seconds}
	now_ts := now()
	pending_by_user := map[string]map[string]map[string]map[string]int64{}
	pending_total := int64(0)
	oldest_age := int64(0)
	for _, r := range pending_rows {
		u, _ := r["user"].(string)
		p, _ := r["peer"].(string)
		s, _ := r["scope"].(string)
		cnt, _ := r["count"].(int64)
		oldest, _ := r["oldest"].(int64)
		age := now_ts - oldest
		if age > oldest_age {
			oldest_age = age
		}
		pending_total += cnt
		if pending_by_user[u] == nil {
			pending_by_user[u] = map[string]map[string]map[string]int64{}
		}
		if pending_by_user[u][p] == nil {
			pending_by_user[u][p] = map[string]map[string]int64{}
		}
		pending_by_user[u][p][s] = map[string]int64{
			"count":       cnt,
			"age_seconds": age,
		}
	}

	if user_filter != "" {
		c.JSON(http.StatusOK, gin.H{
			"user":    user_filter,
			"emitted": emitted_by_user[user_filter],
			"applied": applied_by_user[user_filter],
			"pending": pending_by_user[user_filter],
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"emitted":              emitted_by_user,
		"applied":              applied_by_user,
		"pending":              pending_by_user,
		"pending_total":        pending_total,
		"pending_oldest_age_s": oldest_age,
	})
}

// admin_replication_pair_remove is POST /_/admin/replication/pair/remove.
// Body: {"peer": "<peer-id>"}
//
// Removes the specified peer from this server's pair set and emits a
// pair-membership-change op to the remaining members so they update
// their views too. The removed peer's local copies are NOT wiped per
// the plan — leave-vs-admin-delete distinction is preserved. The
// removed peer learns it's been kicked the next time it processes a
// membership-change from one of the remaining members.
func admin_replication_pair_remove(c *gin.Context) {
	var input struct {
		Peer string `json:"peer"`
	}
	if err := c.ShouldBindJSON(&input); err != nil || input.Peer == "" {
		respond_error(c, http.StatusBadRequest, "missing_peer", "errors.missing_peer", nil)
		return
	}

	removed, remaining, ok := replication_pair_remove(input.Peer)
	if !ok {
		respond_error(c, http.StatusNotFound, "not_a_pair_member", "errors.not_a_pair_member", nil)
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"removed": removed,
		"members": remaining,
	})
}

// admin_replication_pairs is GET /_/admin/replication/pairs.
// Per-pair-member rollup of the operator-facing health signals: where
// each peer is in bootstrap, how much we've sent outbound to it
// (tail), how much we've applied from it (cursor), how many ops are
// buffered waiting on a gap (pending count + oldest age), how many
// distinct ops we've recorded from it (seen count), when it last
// delivered something we accepted (last_op_at), and what leases
// currently exist between us and this peer. Aimed at the operator
// who's investigating why two paired hosts have diverged in
// row counts and wants a single screen of "is the wire alive, is
// bootstrap done, am I behind, is there a stuck stream?".
func admin_replication_pairs(c *gin.Context) {
	rdb := db_open("db/replication.db")
	n := now()

	type peerData struct {
		Peer           string                       `json:"peer"`
		Added          int64                        `json:"added"`
		Role           string                       `json:"role"`
		Bootstrap      map[string]string            `json:"bootstrap"`
		OutboundTail   map[string]int64             `json:"outbound_tail"`
		InboundCursor  map[string]int64             `json:"inbound_cursor"`
		Pending        map[string]any               `json:"pending"`
		SeenCount      int64                        `json:"seen_count"`
		LastOpAt       int64                        `json:"last_op_at"`
		LeasesHeldBy   []map[string]any             `json:"leases_held_by_self"`
		LeasesHeldPeer []map[string]any             `json:"leases_held_by_peer"`
	}

	pairs := map[string]*peerData{}

	// Seed pairs from replication.db.pair so an idle / not-yet-talked-
	// to peer still shows up.
	if rows, err := rdb.rows("select peer, added, role from pair"); err == nil {
		for _, r := range rows {
			peer, _ := r["peer"].(string)
			if peer == "" {
				continue
			}
			added, _ := r["added"].(int64)
			role, _ := r["role"].(string)
			pairs[peer] = &peerData{
				Peer: peer, Added: added, Role: role,
				Bootstrap:      map[string]string{},
				OutboundTail:   map[string]int64{},
				InboundCursor:  map[string]int64{},
				LeasesHeldBy:   []map[string]any{},
				LeasesHeldPeer: []map[string]any{},
			}
		}
	}

	get := func(peer string) *peerData {
		if pd, ok := pairs[peer]; ok {
			return pd
		}
		pd := &peerData{
			Peer:           peer,
			Bootstrap:      map[string]string{},
			OutboundTail:   map[string]int64{},
			InboundCursor:  map[string]int64{},
			LeasesHeldBy:   []map[string]any{},
			LeasesHeldPeer: []map[string]any{},
		}
		pairs[peer] = pd
		return pd
	}

	if rows, err := rdb.rows("select scope, peer, state from bootstrap"); err == nil {
		for _, r := range rows {
			peer, _ := r["peer"].(string)
			scope, _ := r["scope"].(string)
			state, _ := r["state"].(string)
			if peer == "" || peer == net_id {
				continue
			}
			get(peer).Bootstrap[scope] = state
		}
	}

	// outbound tail is per-(user, scope, db) and not naturally per-peer
	// (the same op stream is sent to every recipient). Roll it up here
	// keyed by "scope/user/db" so the operator sees per-stream
	// outbound progress; it's the same across pairs, so duplicate
	// under each pair entry to keep the JSON self-contained.
	tail_rows := map[string]int64{}
	if rows, err := rdb.rows("select user, scope, db, last from tail"); err == nil {
		for _, r := range rows {
			user, _ := r["user"].(string)
			scope, _ := r["scope"].(string)
			database, _ := r["db"].(string)
			last, _ := r["last"].(int64)
			key := scope + "/" + user + "/" + database
			tail_rows[key] = last
		}
	}
	for _, pd := range pairs {
		for k, v := range tail_rows {
			pd.OutboundTail[k] = v
		}
	}

	if rows, err := rdb.rows("select peer, scope, user, db, sequence from cursor"); err == nil {
		for _, r := range rows {
			peer, _ := r["peer"].(string)
			scope, _ := r["scope"].(string)
			user, _ := r["user"].(string)
			database, _ := r["db"].(string)
			seq, _ := r["sequence"].(int64)
			if peer == "" || peer == net_id {
				continue
			}
			get(peer).InboundCursor[scope+"/"+user+"/"+database] = seq
		}
	}

	pending_rows := map[string]struct {
		count  int64
		oldest int64
	}{}
	if rows, err := rdb.rows("select peer, count(*) as c, min(received) as oldest from pending group by peer"); err == nil {
		for _, r := range rows {
			peer, _ := r["peer"].(string)
			count, _ := r["c"].(int64)
			oldest, _ := r["oldest"].(int64)
			if peer == "" {
				continue
			}
			pending_rows[peer] = struct {
				count  int64
				oldest int64
			}{count, oldest}
		}
	}
	for peer, pd := range pairs {
		if pr, ok := pending_rows[peer]; ok {
			pd.Pending = map[string]any{
				"count":      pr.count,
				"oldest_age": n - pr.oldest,
			}
		} else {
			pd.Pending = map[string]any{"count": int64(0), "oldest_age": int64(0)}
		}
	}

	if rows, err := rdb.rows("select peer, count(*) as c, max(applied) as last from seen group by peer"); err == nil {
		for _, r := range rows {
			peer, _ := r["peer"].(string)
			count, _ := r["c"].(int64)
			last, _ := r["last"].(int64)
			if peer == "" || peer == net_id {
				continue
			}
			pd := get(peer)
			pd.SeenCount = count
			pd.LastOpAt = last
		}
	}

	if rows, err := rdb.rows("select scope, key, peer, expires, fence from leadership where expires > ?", n); err == nil {
		for _, r := range rows {
			scope, _ := r["scope"].(string)
			key, _ := r["key"].(string)
			peer, _ := r["peer"].(string)
			expires, _ := r["expires"].(int64)
			fence, _ := r["fence"].(int64)
			entry := map[string]any{
				"scope":   scope,
				"key":     key,
				"expires": expires,
				"fence":   fence,
			}
			if peer == net_id {
				// We hold this lease; surface it under every peer
				// so the operator sees what THIS host owns relative
				// to each partner.
				for _, pd := range pairs {
					pd.LeasesHeldBy = append(pd.LeasesHeldBy, entry)
				}
			} else if pd, ok := pairs[peer]; ok {
				pd.LeasesHeldPeer = append(pd.LeasesHeldPeer, entry)
			}
		}
	}

	out := make([]*peerData, 0, len(pairs))
	for _, pd := range pairs {
		out = append(out, pd)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Peer < out[j].Peer })

	c.JSON(http.StatusOK, gin.H{
		"host":  net_id,
		"now":   n,
		"pairs": out,
	})
}

// admin_replication_stalled is GET /_/admin/replication/stalled.
// Returns the (peer, scope, user, db) streams whose pending buffer
// cannot drain - either anchored with a gap (smallest predecessor >
// cursor) or unanchored with no zero-predecessor stream-start op
// present. Each entry carries the cursor, the predecessor range
// (minimum / maximum) across pending rows, the row count, and the
// age of the oldest pending row in seconds. Aimed at operator triage
// when users report missing data on a paired host.
func admin_replication_stalled(c *gin.Context) {
	stalled := replication_pending_stalled()
	out := make([]map[string]any, 0, len(stalled))
	n := now()
	for _, s := range stalled {
		out = append(out, map[string]any{
			"peer":        s.Peer,
			"scope":       s.Scope,
			"user":        s.User,
			"database":    s.Database,
			"cursor":      s.Cursor,
			"anchored":    s.Anchored,
			"predecessor": map[string]int64{"minimum": s.Predecessor.Minimum, "maximum": s.Predecessor.Maximum},
			"count":       s.Count,
			"oldest":      s.Oldest,
			"age":         n - s.Oldest,
		})
	}
	c.JSON(http.StatusOK, gin.H{"stalled": out})
}

// admin_replication_irreparable is GET /_/admin/replication/irreparable.
// Runs the irreparable scan on demand (the same detect-and-notify pass the
// manager runs hourly) then returns every relationship marked irreparable -
// broken past T_forget with no lossless recovery left. Each entry carries
// the peer, scope, user, db, reason ('stalled' | 'offline'), the time it
// became irreparable, and whether the dual-side notification has fired.
// Exposed so an operator can check + alert immediately instead of waiting
// for the hourly pass.
func admin_replication_irreparable(c *gin.Context) {
	replication_irreparable_scan()
	rdb := db_open("db/replication.db")
	rows, _ := rdb.rows("select peer, scope, user, db, reason, since, notified from irreparable order by since")
	out := make([]map[string]any, 0, len(rows))
	n := now()
	for _, r := range rows {
		since := row_int(r, "since")
		out = append(out, map[string]any{
			"peer":     row_string(r, "peer"),
			"scope":    row_string(r, "scope"),
			"user":     row_string(r, "user"),
			"database": row_string(r, "db"),
			"reason":   row_string(r, "reason"),
			"since":    since,
			"age":      n - since,
			"notified": row_int(r, "notified"),
		})
	}
	c.JSON(http.StatusOK, gin.H{"irreparable": out})
}

// admin_replication_pending_gc is POST /_/admin/replication/pending/gc.
// Runs the unfillable-pending GC pass on demand and returns the count
// of rows dropped. Same logic the manager loop runs hourly, exposed so
// an operator who's just finished cleaning up a peer can immediately
// flush the now-orphaned pending rows instead of waiting for the next
// scheduled pass.
func admin_replication_pending_gc(c *gin.Context) {
	dropped := replication_pending_gc()
	c.JSON(http.StatusOK, gin.H{"dropped": dropped})
}
