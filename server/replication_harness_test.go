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
//                   so receivers apply in their own data_dir / p2p_id
//   - partition()   stop new ops from being delivered; emits land in
//                   a held bucket
//   - heal()        move the held bucket back into the live queue
//   - reorder()     shuffle a receiver's pending queue
//
// Intercepts the three emit paths used in production:
//   - replication_emit_to     per-user-scope ops (users-row, sessions-
//                             row, schedule-row, session insert/delete)
//   - replication_emit_system_set   pair-scope field writes (settings)
//   - replication_emit_system_row   pair-scope row writes (domains,
//                                   apps, users.users, settings.documents)
//
// For a 2-host pair the recipient set is always "the other host", so
// the harness skips the production recipient-resolver (which would
// query replication.db.pair / .hosts that we never populate) and just
// fans out to the non-sender.

package main

import (
	"math/rand"
	"os"
	"sync"
	"testing"
)

type harnessHost struct {
	name string
	p2p  string
	dir  string
}

type harnessDelivery struct {
	sender   string
	receiver string
	op       *ReplicationOp
	sysSet   *SystemSet
	sysRow   *SystemRow
}

type harness struct {
	t       *testing.T
	hosts   map[string]*harnessHost
	current string

	mu          sync.Mutex
	queues      map[string][]harnessDelivery // receiver -> queue
	held        []harnessDelivery            // partitioned-but-not-yet-healed
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

	origData          string
	origP2P           string
	origEmitTo        func(user string, op *ReplicationOp, peers []string)
	origEmitSystemSet func(database, table, row, field, value string)
	origEmitSystemRow func(database, table string, key, cols map[string]string, del bool)
}

// newHarness mints N host contexts, swaps the three emit vars for
// queue-capturing stubs, and returns the harness. Always defer
// h.cleanup() immediately after the call. With no names supplied
// defaults to {"h1", "h2"} for the historical 2-host shape.
func newHarness(t *testing.T, names ...string) *harness {
	t.Helper()
	if len(names) == 0 {
		names = []string{"h1", "h2"}
	}
	h := &harness{
		t:                 t,
		hosts:             map[string]*harnessHost{},
		queues:            map[string][]harnessDelivery{},
		pair_members:      map[string]bool{},
		user_hosts:        map[string]map[string]bool{},
		origData:          data_dir,
		origP2P:           p2p_id,
		origEmitTo:        replication_emit_to,
		origEmitSystemSet: replication_emit_system_set,
		origEmitSystemRow: replication_emit_system_row,
	}
	for _, name := range names {
		dir, err := os.MkdirTemp("", "mochi_harness_"+name)
		if err != nil {
			t.Fatalf("temp dir %s: %v", name, err)
		}
		h.hosts[name] = &harnessHost{name: name, p2p: "peer-" + name, dir: dir}
		h.queues[name] = nil
		h.pair_members[name] = true // default: every host is pair-paired with every other
	}
	replication_emit_to = h.captureEmitTo
	replication_emit_system_set = h.captureSystemSet
	replication_emit_system_row = h.captureSystemRow
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

// cleanup restores all originals and removes both host data_dirs.
// Safe to call multiple times.
func (h *harness) cleanup() {
	data_dir = h.origData
	p2p_id = h.origP2P
	replication_emit_to = h.origEmitTo
	replication_emit_system_set = h.origEmitSystemSet
	replication_emit_system_row = h.origEmitSystemRow
	for _, ctx := range h.hosts {
		os.RemoveAll(ctx.dir)
	}
}

// switchTo flips data_dir + p2p_id to the named host. Records the
// current host so subsequent emit captures know who to route from.
func (h *harness) switchTo(name string) {
	h.t.Helper()
	ctx, ok := h.hosts[name]
	if !ok {
		h.t.Fatalf("unknown harness host %q", name)
	}
	data_dir = ctx.dir
	p2p_id = ctx.p2p
	h.current = name
}

func (h *harness) enqueue(d harnessDelivery) {
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

func (h *harness) captureEmitTo(user string, op *ReplicationOp, peers []string) {
	sender := h.current
	for _, receiver := range h.recipients_per_user(user, sender) {
		h.enqueue(harnessDelivery{sender: sender, receiver: receiver, op: op})
	}
}

func (h *harness) captureSystemSet(database, table, row, field, value string) {
	payload := &SystemSet{Database: database, Table: table, Row: row, Field: field, Value: value}
	sender := h.current
	for _, receiver := range h.recipients_pair(sender) {
		h.enqueue(harnessDelivery{sender: sender, receiver: receiver, sysSet: payload})
	}
}

func (h *harness) captureSystemRow(database, table string, key, cols map[string]string, del bool) {
	payload := &SystemRow{Database: database, Table: table, Key: key, Cols: cols, Delete: del}
	sender := h.current
	for _, receiver := range h.recipients_pair(sender) {
		h.enqueue(harnessDelivery{sender: sender, receiver: receiver, sysRow: payload})
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
		snapshot := make(map[string][]harnessDelivery, len(h.queues))
		for receiver, q := range h.queues {
			snapshot[receiver] = q
			h.queues[receiver] = nil
		}
		h.mu.Unlock()

		// Process each receiver's batch under its host context. Restore
		// the prior host context afterwards so the test's outer
		// switchTo state survives the flush.
		prior := h.current
		for receiver, deliveries := range snapshot {
			h.switchTo(receiver)
			for _, d := range deliveries {
				h.applyOne(d)
			}
		}
		h.switchTo(prior)
	}
	h.t.Fatalf("harness flush did not converge after %d iterations", flushIterationLimit)
}

func (h *harness) applyOne(d harnessDelivery) {
	switch {
	case d.op != nil:
		replication_apply_op(d.op)
	case d.sysSet != nil:
		replication_system_set_apply(h.hosts[d.sender].p2p, d.sysSet)
	case d.sysRow != nil:
		replication_system_row_apply(h.hosts[d.sender].p2p, d.sysRow)
	}
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
func (h *harness) setup_harness_user(uid, username, entityID string) {
	setup_users_test_schema()
	udb := db_open("db/users.db")
	udb.exec("insert or ignore into users (uid, username) values (?, ?)", uid, username)
	udb.exec(
		"insert or ignore into entities (id, private, fingerprint, user, class, name, privacy) values (?, 'priv', 'fp', ?, 'person', ?, 'private')",
		entityID, uid, username)
}
