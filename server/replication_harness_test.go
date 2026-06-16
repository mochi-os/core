// Mochi server: multi-master replication wire-model test harness
// Copyright Alistair Cunningham 2026
//
// integration_setup (replication_test.go) is great for sequential
// two-host scenarios - switch to h1, do thing, switch to h2, observe -
// but real paired hosts emit ops concurrently and the wire can
// reorder, drop, or partition between them. This harness captures
// every replication emit on either side into a deterministic in-
// memory queue and lets tests:
//
//   - flush()       drain everything, switching context per delivery
//                   so receivers apply in their own data_dir / net_id
//   - partition()   stop new ops from being delivered; emits land in
//                   a held bucket
//   - heal()        move the held bucket back into the live queue
//   - reorder()     shuffle a receiver's pending queue
//
// Intercepts the four emit paths used in production:
//   - replication_emit_to     per-user-scope ops (users-row, sessions-
//                             row, schedule-row, session insert/delete)
//   - replication_emit_system_set   pair-scope field writes (settings)
//   - replication_emit_system_row   pair-scope row writes (domains,
//                                   apps, users.users, settings.documents)
//   - attachment_notify_move        federation-scope events emitted on
//                                   mochi.attachment.move (task #79).
//                                   Routes to federation_hosts[entity]
//                                   minus the sender. Other
//                                   _attachment/* notifiers follow the
//                                   same shape and can be added on demand
//                                   when a test needs them.
//
// For a 2-host pair the recipient set is always "the other host", so
// the harness skips the production recipient-resolver (which would
// query replication.db.pair / .hosts that we never populate) and just
// fans out to the non-sender.

package main

import (
	"fmt"
	"math/rand"
	"os"
	"sync"
	"testing"
)

type harness_host struct {
	name string
	p2p  string
	dir  string
}

type harness_delivery struct {
	sender   string
	receiver string
	op       *ReplicationOp
	sys_set  *SystemSet
	sys_row  *SystemRow
	fed_move *harness_federation_move
}

// harness_federation_move carries one captured attachment_notify_move
// emit through the harness queue. Mirrors the wire payload the
// federation event would carry, including the legacy old_rank header
// (so the legacy fallback path can be exercised too if a test wants
// to set fed_move.legacy=true and clear the ranks list).
type harness_federation_move struct {
	entity     string
	attachment map[string]any
	old_rank   int
	ranks      []map[string]any
}

type harness struct {
	t       *testing.T
	hosts   map[string]*harness_host
	current string

	mu          sync.Mutex
	queues      map[string][]harness_delivery // receiver -> queue
	held        []harness_delivery            // partitioned-but-not-yet-healed
	partitioned bool

	// Topology metadata controls capture-time routing. pair_members is
	// the set of hosts that share pair-scope state (system-set /
	// system-row); user_hosts[user] is the set of hosts that share
	// per-user-scope state (users-row / sessions-row / schedule-row /
	// session insert/delete) for a specific user. Per-user emits route
	// to (pair_members union user_hosts[user]) minus the sender,
	// matching the production recipients(user) resolver.
	//
	// Defaults match the "everyone with everyone" symmetric setup -
	// what 2-host pair tests expect. set_pair_members and
	// set_user_hosts override per topology:
	//   - server-server-server: pair_members = {all}, user_hosts left
	//     unset (default to all)
	//   - user-user-user: pair_members = {} (no operator pair),
	//     set_user_hosts(user, all)
	pair_members map[string]bool
	user_hosts   map[string]map[string]bool

	// host_recipients[host][user] overrides recipients_per_user for an
	// asymmetric chain that isn't a clique - e.g. D <-user-user-> W
	// <-server-pair-> Y, where each host's recipient set differs and D
	// and Y never connect directly. Hosts with no entry fall back to the
	// global pair_members/user_hosts resolution.
	host_recipients map[string]map[string]map[string]bool

	// gated routes op deliveries through the production inbound path
	// replication_op_receive (the in-order gate, op_land, and the
	// transit relay) instead of the raw replication_apply_op, so a test
	// exercises the real receive path and any relay it triggers.
	gated bool

	// federation_hosts[entity] is the set of host names that own a
	// replica of the named entity. Federation emits (attachment_notify_*)
	// route to (federation_hosts[entity] minus sender) - matching how
	// federation messages in production go to every host of the
	// recipient entity. Unset entity means "no federation routing for
	// this entity"; the capture is dropped silently rather than fanned
	// out to all hosts (federation defaults to "no one" because the
	// entity may genuinely have zero subscribers).
	federation_hosts map[string]map[string]bool

	original_data                   string
	original_p2p                    string
	original_emit_to                func(user string, op *ReplicationOp, peers []string)
	original_emit_system_set        func(database, table, row, field, value string)
	original_emit_system_row        func(database, table string, key, cols map[string]string, del bool)
	original_drain_async            func(user, app_id string)
	original_attachment_notify_move func(app *App, owner *User, attachment map[string]any, old_rank int, ranks []map[string]any, notify []string)
}

// new_harness mints N host contexts, swaps the three emit vars for
// queue-capturing stubs, and returns the harness. Always defer
// h.cleanup() immediately after the call. With no names supplied
// defaults to {"h1", "h2"} for the historical 2-host shape.
func new_harness(t *testing.T, names ...string) *harness {
	t.Helper()
	if len(names) == 0 {
		names = []string{"h1", "h2"}
	}
	h := &harness{
		t:                               t,
		hosts:                           map[string]*harness_host{},
		queues:                          map[string][]harness_delivery{},
		pair_members:                    map[string]bool{},
		user_hosts:                      map[string]map[string]bool{},
		federation_hosts:                map[string]map[string]bool{},
		original_data:                   data_dir,
		original_p2p:                    net_id,
		original_emit_to:                replication_emit_to,
		original_emit_system_set:        replication_emit_system_set,
		original_emit_system_row:        replication_emit_system_row,
		original_drain_async:            post_migration_drain_async,
		original_attachment_notify_move: attachment_notify_move,
	}
	for _, name := range names {
		dir, err := os.MkdirTemp("", "mochi_harness_"+name)
		if err != nil {
			t.Fatalf("temp dir %s: %v", name, err)
		}
		h.hosts[name] = &harness_host{name: name, p2p: "peer-" + name, dir: dir}
		h.queues[name] = nil
		h.pair_members[name] = true // default: every host is pair-paired with every other
	}
	replication_emit_to = h.capture_emit_to
	replication_emit_system_set = h.capture_system_set
	replication_emit_system_row = h.capture_system_row
	attachment_notify_move = h.capture_attachment_notify_move
	// Suppress the post-migration drain goroutine. It reads data_dir
	// asynchronously, which races with switch_to. The drain is a
	// performance prefetch the harness doesn't need - flush() drains
	// every queue deterministically.
	post_migration_drain_async = func(user, app_id string) {}
	return h
}

// set_pair_members declares which hosts are operator-paired. Pair-scope
// emits (system_set / system_row) route only among these hosts.
// Replaces any previous setting. For a user-user-user topology call
// with no names (or just the local host) to disable pair-scope
// fan-out entirely.
func (h *harness) set_pair_members(names ...string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.pair_members = map[string]bool{}
	for _, name := range names {
		h.pair_members[name] = true
	}
}

// set_user_hosts declares the per-user link membership for the given
// user. Per-user-scope emits with op.User == user route to these
// hosts (minus the sender). For users without an explicit entry the
// harness falls back to "every host" - matching the 2-host default.
func (h *harness) set_user_hosts(user string, names ...string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	set := map[string]bool{}
	for _, name := range names {
		set[name] = true
	}
	h.user_hosts[user] = set
}

// set_host_recipients declares the recipient host set visible FROM
// `host` for `user` (asymmetric, per-host), modelling a chain where each
// host has a different recipient set.
func (h *harness) set_host_recipients(host, user string, names ...string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.host_recipients == nil {
		h.host_recipients = map[string]map[string]map[string]bool{}
	}
	if h.host_recipients[host] == nil {
		h.host_recipients[host] = map[string]map[string]bool{}
	}
	set := map[string]bool{}
	for _, name := range names {
		set[name] = true
	}
	h.host_recipients[host][user] = set
}

// cleanup restores all originals and removes both host data_dirs.
// Safe to call multiple times.
func (h *harness) cleanup() {
	data_dir = h.original_data
	net_id = h.original_p2p
	replication_emit_to = h.original_emit_to
	replication_emit_system_set = h.original_emit_system_set
	replication_emit_system_row = h.original_emit_system_row
	post_migration_drain_async = h.original_drain_async
	attachment_notify_move = h.original_attachment_notify_move
	for _, ctx := range h.hosts {
		os.RemoveAll(ctx.dir)
	}
}

// switch_to flips data_dir + net_id to the named host. Records the
// current host so subsequent emit captures know who to route from.
func (h *harness) switch_to(name string) {
	h.t.Helper()
	ctx, ok := h.hosts[name]
	if !ok {
		h.t.Fatalf("unknown harness host %q", name)
	}
	data_dir = ctx.dir
	net_id = ctx.p2p
	h.current = name
}

func (h *harness) enqueue(d harness_delivery) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.partitioned {
		h.held = append(h.held, d)
		return
	}
	h.queues[d.receiver] = append(h.queues[d.receiver], d)
}

// recipients_per_user computes the per-user-scope recipient set for
// the given user. Mirrors production recipients(user): pair members
// UNION the user's host set, minus the sender. When user_hosts has
// no explicit entry for the user, falls back to "every host" so
// historical 2-host tests keep working without calling set_user_hosts.
func (h *harness) recipients_per_user(user, sender string) []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	set := map[string]bool{}
	if recipients, ok := h.host_recipients[sender]; ok {
		// Asymmetric chain: this sender has an explicit recipient set,
		// so D->{W}, W->{D,Y}, Y->{W} model a user-user/pair chain.
		for name := range recipients[user] {
			set[name] = true
		}
		delete(set, sender)
		out := make([]string, 0, len(set))
		for name := range set {
			out = append(out, name)
		}
		return out
	}
	for name := range h.pair_members {
		set[name] = true
	}
	if hosts, ok := h.user_hosts[user]; ok {
		for name := range hosts {
			set[name] = true
		}
	} else {
		// Default: every host is in the user's host set. Preserves
		// the 2-host default where tests don't call set_user_hosts.
		for name := range h.hosts {
			set[name] = true
		}
	}
	delete(set, sender)
	out := make([]string, 0, len(set))
	for name := range set {
		out = append(out, name)
	}
	return out
}

// set_federation_hosts declares which host names own a replica of the
// named entity for federation routing. Repeated calls overwrite the
// previous set for that entity. Without an entry, federation emits
// for that entity are dropped (matches production behaviour where an
// entity unknown to the local directory gets no fan-out).
func (h *harness) set_federation_hosts(entity string, names ...string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	set := map[string]bool{}
	for _, name := range names {
		set[name] = true
	}
	h.federation_hosts[entity] = set
}

// recipients_federation computes the federation-scope recipient set for
// the given entity. Mirrors entity_peers() in production but reads from
// the harness's declared federation_hosts table rather than the libp2p
// directory.
func (h *harness) recipients_federation(entity, sender string) []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	hosts, ok := h.federation_hosts[entity]
	if !ok {
		return nil
	}
	out := make([]string, 0, len(hosts))
	for name := range hosts {
		if name == sender {
			continue
		}
		out = append(out, name)
	}
	return out
}

// recipients_pair computes the pair-scope recipient set. Mirrors the
// "select peer from pair" loop in replication_emit_system_set/_row.
func (h *harness) recipients_pair(sender string) []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make([]string, 0, len(h.pair_members))
	for name := range h.pair_members {
		if name == sender {
			continue
		}
		out = append(out, name)
	}
	return out
}

func (h *harness) capture_emit_to(user string, op *ReplicationOp, peers []string) {
	sender := h.current
	// Production stamps the origin in replication_emit_to_real, which the
	// harness bypasses; mirror it so transit-relay dedup works in tests.
	replication_origin_ensure(op)
	for _, receiver := range h.recipients_per_user(user, sender) {
		h.enqueue(harness_delivery{sender: sender, receiver: receiver, op: op})
	}
}

func (h *harness) capture_system_set(database, table, row, field, value string) {
	payload := &SystemSet{Database: database, Table: table, Row: row, Field: field, Value: value}
	sender := h.current
	for _, receiver := range h.recipients_pair(sender) {
		h.enqueue(harness_delivery{sender: sender, receiver: receiver, sys_set: payload})
	}
}

func (h *harness) capture_system_row(database, table string, key, cols map[string]string, del bool) {
	payload := &SystemRow{Database: database, Table: table, Key: key, Cols: cols, Delete: del}
	sender := h.current
	for _, receiver := range h.recipients_pair(sender) {
		h.enqueue(harness_delivery{sender: sender, receiver: receiver, sys_row: payload})
	}
}

// capture_attachment_notify_move intercepts the federation emit for
// _attachment/move and routes one delivery to every peer host listed
// in federation_hosts[entity]. The app + owner args are not threaded
// through to the receiver - the harness opens the receiver's
// attachment DB by path in apply_one rather than going through
// db_app_system, because the receiver doesn't have a real User /
// App. Tests that need richer dispatch can set up Users/Apps and
// extend apply_one accordingly.
func (h *harness) capture_attachment_notify_move(app *App, owner *User, attachment map[string]any, old_rank int, ranks []map[string]any, notify []string) {
	sender := h.current
	for _, entity := range notify {
		payload := &harness_federation_move{
			entity:     entity,
			attachment: attachment,
			old_rank:   old_rank,
			ranks:      ranks,
		}
		for _, receiver := range h.recipients_federation(entity, sender) {
			h.enqueue(harness_delivery{sender: sender, receiver: receiver, fed_move: payload})
		}
	}
}

// flush drains every receiver's queue until idle. Operations emitted
// during an apply (a commit hook firing a replicated write) re-enter
// the queue and get drained on the next iteration. Aborts after
// flushIterationLimit cycles to surface a runaway loop instead of
// hanging the test.
const flushIterationLimit = 100

func (h *harness) flush() {
	for i := 0; i < flushIterationLimit; i++ {
		h.mu.Lock()
		anything := false
		for _, q := range h.queues {
			if len(q) > 0 {
				anything = true
				break
			}
		}
		if !anything {
			h.mu.Unlock()
			return
		}
		snapshot := make(map[string][]harness_delivery, len(h.queues))
		for receiver, q := range h.queues {
			snapshot[receiver] = q
			h.queues[receiver] = nil
		}
		h.mu.Unlock()

		// Process each receiver's batch under its host context. Restore
		// the prior host context afterwards so the test's outer
		// switch_to state survives the flush.
		prior := h.current
		for receiver, deliveries := range snapshot {
			h.switch_to(receiver)
			for _, d := range deliveries {
				h.apply_one(d)
			}
		}
		h.switch_to(prior)
	}
	h.t.Fatalf("harness flush did not converge after %d iterations", flushIterationLimit)
}

func (h *harness) apply_one(d harness_delivery) {
	switch {
	case d.op != nil:
		if h.gated {
			replication_op_receive(h.hosts[d.sender].p2p, d.op)
		} else {
			replication_apply_op(d.op)
		}
	case d.sys_set != nil:
		replication_system_set_apply(h.hosts[d.sender].p2p, d.sys_set)
	case d.sys_row != nil:
		replication_system_row_apply(h.hosts[d.sender].p2p, d.sys_row)
	case d.fed_move != nil:
		h.apply_federation_move(d)
	}
}

// apply_federation_move synthesises the receiver's Event the way the
// p2p stack would, then dispatches to attachment_event_move. The
// receiver is the host the harness has just switched to via the flush
// loop, so db_open here opens the receiver's attachment DB. Tests are
// responsible for ensuring the attachments table exists on every host
// (the harness doesn't seed it; see setup_attachment_move_test in
// attachments_test.go).
func (h *harness) apply_federation_move(d harness_delivery) {
	db := db_open("db/attachments.db")
	if db == nil {
		h.t.Fatalf("federation move at %s: cannot open attachments DB", d.receiver)
	}
	e := &Event{
		from: d.fed_move.entity,
		db:   db,
		content: map[string]any{
			"old_rank": fmt.Sprintf("%d", d.fed_move.old_rank),
		},
	}
	if len(d.fed_move.ranks) > 0 {
		entries := make([]any, 0, len(d.fed_move.ranks))
		for _, r := range d.fed_move.ranks {
			entries = append(entries, map[string]any{"id": r["id"], "rank": r["rank"]})
		}
		e.content["ranks"] = entries
	}
	e.attachment_event_move()
}

// partition stops new emits from being queued. Captured emits land in
// h.held; heal() promotes them back to the live queue.
func (h *harness) partition() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.partitioned = true
}

func (h *harness) heal() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.partitioned = false
	for _, d := range h.held {
		h.queues[d.receiver] = append(h.queues[d.receiver], d)
	}
	h.held = nil
}

// reorder shuffles the named receiver's pending queue with a seeded
// random. Use to simulate out-of-order wire delivery between
// partition and flush.
func (h *harness) reorder(receiver string, seed int64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	q := h.queues[receiver]
	r := rand.New(rand.NewSource(seed))
	r.Shuffle(len(q), func(i, j int) { q[i], q[j] = q[j], q[i] })
	h.queues[receiver] = q
}

// pending reports the current queue depth at a receiver. Useful for
// asserting that partition() actually held things back.
func (h *harness) pending(receiver string) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.queues[receiver]) + len(h.held)
}

// setup_harness_user seeds users.db on the currently-switched-to host
// with one user (uid, username) so per-user-scope apply paths pass
// user_exists. The user's identity entity is also seeded so the
// replication signing path (which looks up an entity for the user)
// has something to find.
func (h *harness) setup_harness_user(uid, username, entity_id string) {
	setup_users_test_schema()
	udb := db_open("db/users.db")
	udb.exec("insert or ignore into users (uid, username) values (?, ?)", uid, username)
	udb.exec(
		"insert or ignore into entities (id, private, fingerprint, user, class, name, privacy) values (?, 'priv', 'fp', ?, 'person', ?, 'private')",
		entity_id, uid, username)
}
