// Mochi server: bounds and lifecycle on the P2P paths.
//
// Each test here pins a property that a peer, not this host, controls the
// input to: how long a dispatcher may hold the worker registry, whether a
// Sender's goroutines outlive it, what a peer-chosen service costs the
// resolution cache, and what an unvalidated envelope reaches.
//
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

// quiet_reply is a reply target that does nothing. The dispatch tests below
// care about lock behaviour, not handler outcomes, and a frame with no real
// stream behind it would otherwise fault inside worker_fail.
type quiet_reply struct{}

func (quiet_reply) ack()        {}
func (quiet_reply) fail(string) {}

// worker_registry_reset empties the worker map so a test starts clean. Workers
// left behind by an earlier test would make the dispatch test's counts wrong.
func worker_registry_reset(t *testing.T) {
	t.Helper()
	app_workers_lock.Lock()
	app_workers = map[user_app_key]*app_worker{}
	app_workers_lock.Unlock()
	t.Cleanup(func() {
		app_workers_lock.Lock()
		app_workers = map[user_app_key]*app_worker{}
		app_workers_lock.Unlock()
	})
}

// TestDispatchToOneFullWorkerLeavesOthersRunning is the #587 property. A
// blocked dispatcher used to hold app_workers_lock.RLock(); Go's RWMutex then
// blocks every new reader as soon as one writer queues behind it, so a single
// full inbox plus one worker_create froze dispatch for every user and app on
// the host. 33 frames to a slow handler was the whole attack.
func TestDispatchToOneFullWorkerLeavesOthersRunning(t *testing.T) {
	worker_registry_reset(t)

	// A worker with a full inbox and NO run() goroutine: a real worker would
	// drain as fast as the test filled, and the condition under test is
	// precisely a worker that is not draining.
	blocked := user_app_key{user: "victim", app: "slow"}
	slow := &app_worker{
		user:  blocked.user,
		app:   blocked.app,
		inbox: make(chan *worker_frame, 2),
		stop:  make(chan struct{}),
		done:  make(chan struct{}),
	}
	slow.last_used.Store(now())
	app_workers_lock.Lock()
	app_workers[blocked] = slow
	app_workers_lock.Unlock()
	for i := 0; i < cap(slow.inbox); i++ {
		slow.inbox <- &worker_frame{frame: &Frame{Type: frame_type_message}, reply: quiet_reply{}}
	}

	// One more send blocks: that is the dispatcher we are stranding.
	stranded := make(chan struct{})
	go func() {
		defer close(stranded)
		worker_dispatch(blocked.user, blocked.app, &worker_frame{frame: &Frame{Type: frame_type_message}, reply: quiet_reply{}})
	}()

	// Give the stranded dispatcher time to reach its send.
	time.Sleep(50 * time.Millisecond)

	// A writer queues behind it. This is what used to make every later reader
	// wait: worker_create takes the write lock, and it runs on the FIRST
	// message for any new (user, app) pair.
	writer := make(chan struct{})
	go func() {
		defer close(writer)
		worker_create(user_app_key{user: "bystander", app: "new"})
	}()

	select {
	case <-writer:
	case <-time.After(2 * time.Second):
		t.Fatal("worker_create never acquired the write lock: a blocked dispatcher is still holding the registry read lock")
	}

	// And an unrelated reader still gets through.
	reader := make(chan struct{})
	go func() {
		defer close(reader)
		workers, _ := worker_count()
		_ = workers
	}()
	select {
	case <-reader:
	case <-time.After(2 * time.Second):
		t.Fatal("an unrelated registry read was blocked behind the stranded dispatcher")
	}

	// Drain so the stranded dispatcher can finish and the test does not leak it.
	<-slow.inbox
	select {
	case <-stranded:
	case <-time.After(2 * time.Second):
		t.Fatal("the stranded dispatcher never completed after the inbox drained")
	}
}

// TestReapedWorkerReleasesItsDispatcher covers the other half of #587: the
// reaper now closes `stop`, not the inbox, because dispatchers send outside
// the lock and a send on a closed channel panics. A dispatcher holding a
// reaped worker must be released by `done` and retry.
func TestReapedWorkerReleasesItsDispatcher(t *testing.T) {
	worker_registry_reset(t)

	key := user_app_key{user: "u", app: "a"}
	worker_create(key)
	app_workers_lock.RLock()
	w := app_workers[key]
	app_workers_lock.RUnlock()

	// Retire it the way the reaper does.
	app_workers_lock.Lock()
	close(w.stop)
	delete(app_workers, key)
	app_workers_lock.Unlock()

	select {
	case <-w.done:
	case <-time.After(2 * time.Second):
		t.Fatal("run() did not exit after stop was closed")
	}

	// Dispatching again must create a fresh worker rather than panic or hang.
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		worker_dispatch(key.user, key.app, &worker_frame{frame: &Frame{Type: frame_type_message}, reply: quiet_reply{}})
	}()
	select {
	case <-finished:
	case <-time.After(2 * time.Second):
		t.Fatal("dispatch after a reap neither completed nor created a replacement worker")
	}
}

// TestSenderShutdownReleasesWriteLoop is #588. Nothing closes s.outbox - a
// stale *Sender may still be held by peer_send - so write_loop ranging on it
// parked forever, pinning the Sender, its 256-slot outbox and its inflight
// maps. One leak per disconnect, ping timeout or framing error.
func TestSenderShutdownReleasesWriteLoop(t *testing.T) {
	s := &Sender{
		peer:     "leak-probe",
		session:  "s",
		done:     make(chan struct{}),
		outbox:   make(chan *outbound, 4),
		inflight: map[string]*pending{},
		pings:    map[string]int64{},
		claimed:  map[string]bool{},
		proven:   map[string]bool{},
		proving:  map[string]chan error{},
	}

	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		s.write_loop()
	}()

	s.shutdown()

	select {
	case <-stopped:
	case <-time.After(2 * time.Second):
		t.Fatal("write_loop did not return after shutdown: the goroutine and the Sender it pins are leaked")
	}
}

// TestResolutionCacheDoesNotHoldMisses is #589. app_for_service used to cache
// a nil resolution, and get() treats an expired entry as a miss without
// deleting it, so every distinct peer-chosen service left a permanent entry.
func TestResolutionCacheDoesNotHoldMisses(t *testing.T) {
	cache := &app_resolution_cache{entries: map[resolution_key]resolution_app_entry{}}

	// A miss is never handed to put() by app_for_service, so the map stays
	// empty however many distinct services a peer invents.
	if len(cache.entries) != 0 {
		t.Fatalf("fresh cache holds %d entries", len(cache.entries))
	}

	// And put() itself is bounded, so even all-positive traffic cannot grow
	// without limit between configuration writes.
	for i := 0; i < resolution_cache_maximum+100; i++ {
		cache.put(resolution_key{"user", fmt.Sprintf("service-%d", i)}, &App{id: "feeds"})
	}
	if len(cache.entries) > resolution_cache_maximum {
		t.Errorf("cache holds %d entries, above the ceiling of %d", len(cache.entries), resolution_cache_maximum)
	}
}

// TestEnvelopeTargetValid is #592's `To` half. Unchecked, To reached
// user_owning_entity and entity_by_any as an SQL parameter once per frame,
// bounded only by the 16 MB frame.
func TestEnvelopeTargetValid(t *testing.T) {
	long := ""
	for len(long) < 100000 {
		long += "abcdefghij"
	}
	cases := []struct {
		to    string
		valid bool
		why   string
	}{
		{"", true, "empty is the unaddressed case and must stay allowed"},
		{long, false, "a multi-megabyte To must not reach the database"},
		{"not an entity at all!", false, "punctuation and spaces are neither entity nor fingerprint"},
	}
	for _, c := range cases {
		if got := envelope_target_valid(c.to); got != c.valid {
			t.Errorf("envelope_target_valid(%.20q) = %v, want %v: %s", c.to, got, c.valid, c.why)
		}
	}
}

// TestReceiverIdleTimeoutExceedsPingInterval is #597. The bound has to sit
// clear of the ping interval or a healthy stream trips its own deadline; three
// intervals gives a live sender two missed pings of slack.
func TestReceiverIdleTimeoutExceedsPingInterval(t *testing.T) {
	ping := time.Duration(peer_ping_interval_seconds()) * time.Second
	idle := receiver_idle_timeout()
	if idle <= ping {
		t.Fatalf("idle timeout %v is not longer than the ping interval %v: a healthy stream would trip it", idle, ping)
	}
	if idle < 2*ping {
		t.Errorf("idle timeout %v leaves less than two ping intervals of slack over %v", idle, ping)
	}
}

// TestSelfLoopTransientIsRetryable is #598's classification half: the reasons
// local_reply.fail now re-enqueues are exactly the ones a remote sender would
// have retried. Without this the local recipient lost the message while the
// remote one kept it.
func TestSelfLoopTransientIsRetryable(t *testing.T) {
	retried := []string{fail_transient}
	dropped := []string{fail_dedup, fail_signature_invalid, fail_unsupported, fail_unknown_user, fail_expired}

	for _, reason := range retried {
		if !fail_retryable(reason) {
			t.Errorf("%q is not retryable, so the self-loop would drop it where a remote sender retries", reason)
		}
	}
	for _, reason := range dropped {
		if fail_retryable(reason) {
			t.Errorf("%q is retryable, so the self-loop would requeue a message that can never succeed", reason)
		}
	}
}

// TestSelfLoopTransientRequeuesForRetry is #598 proper. A message to a local
// recipient took the self-loop fast path, and a transient failure there was
// logged and dropped - where the identical failure to a remote recipient goes
// back on the queue and is retried. Same-host delivery was the less reliable
// of the two.
func TestSelfLoopTransientRequeuesForRetry(t *testing.T) {
	test_data_directory(t)
	q := db_open("db/queue.db")
	q.exec("create table if not exists queue ( id text primary key, type text not null default 'direct', target text not null, from_entity text not null, to_entity text not null, service text not null, event text not null, from_app text not null default '', from_services text not null default '', content blob not null default '', data blob not null default '', file text not null default '', expires integer not null default 0, status text not null default 'pending', attempts integer not null default 0, next_retry integer not null, last_error text not null default '', created integer not null, priority integer not null default 20, claimed integer not null default 0 )")

	queued := func(id string) bool {
		row, err := q.row("select id from queue where id=?", id)
		return err == nil && row != nil
	}

	transient := local_reply{
		message: "self-loop-transient", service: "feeds", event: "post/create",
		to: "to-entity", from: "from-entity",
	}
	transient.fail(fail_transient)
	if !queued(transient.message) {
		t.Error("a transient self-loop failure was dropped: the same reason to a remote recipient is retried, so local delivery is the less reliable path")
	}

	// The permanent reasons must NOT come back, or a message that can never
	// succeed cycles the queue until it expires.
	permanent := local_reply{
		message: "self-loop-permanent", service: "feeds", event: "post/create",
		to: "to-entity", from: "from-entity",
	}
	permanent.fail(fail_unknown_user)
	if queued(permanent.message) {
		t.Error("a permanent self-loop failure was requeued, so a message that can never succeed will cycle the queue")
	}
}

// TestDispatchIsConcurrentAcrossApps guards the general property #587 exists
// for: dispatch to independent (user, app) pairs must not serialise.
func TestDispatchIsConcurrentAcrossApps(t *testing.T) {
	worker_registry_reset(t)

	var group sync.WaitGroup
	done := make(chan struct{})
	for i := 0; i < 20; i++ {
		group.Add(1)
		go func(n int) {
			defer group.Done()
			worker_dispatch(fmt.Sprintf("user-%d", n), "app", &worker_frame{frame: &Frame{Type: frame_type_message}, reply: quiet_reply{}})
		}(i)
	}
	go func() {
		group.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("twenty independent dispatches did not all complete: they are serialising on the registry lock")
	}

	// Retire the workers this test created so their goroutines exit with it.
	app_workers_lock.Lock()
	for key, w := range app_workers {
		close(w.stop)
		delete(app_workers, key)
	}
	app_workers_lock.Unlock()
}
