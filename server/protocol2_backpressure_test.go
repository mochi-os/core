// Inbound flow-control regression guard (threat-model #92,
// claude/plans/replication-threat-model.md). worker_dispatch propagates
// back-pressure into libp2p flow control by BLOCKING on a full, bounded inbox
// rather than buffering unbounded or dropping — so a fast or flooding sender
// (including a same-machine/same-LAN peer) is paced by TCP, never dropped.
// This is the mechanism that bounds an inbound op-flood; the test fails if a
// future change makes the inbox send non-blocking (select/default drop) or the
// inbox unbounded.
//
// Copyright © 2026 Mochi OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"testing"
	"time"
)

func TestWorkerDispatchBackpressure(t *testing.T) {
	key := user_app_key{user: "u-backpressure", app: "test"}
	// A worker with a tiny inbox and NO run() goroutine — nothing drains it, so it
	// fills and stays full, modelling a worker that cannot keep up with the sender.
	w := &app_worker{user: key.user, app: key.app, inbox: make(chan *worker_frame, 2)}
	app_workers_lock.Lock()
	app_workers[key] = w
	app_workers_lock.Unlock()
	defer func() {
		app_workers_lock.Lock()
		delete(app_workers, key)
		app_workers_lock.Unlock()
	}()

	// Fill to capacity (cache hit on the pre-inserted worker, so no worker_create
	// / no run() goroutine is started).
	worker_dispatch(key.user, key.app, &worker_frame{})
	worker_dispatch(key.user, key.app, &worker_frame{})

	// A third dispatch must BLOCK — back-pressure, not unbounded buffering or a drop.
	done := make(chan struct{})
	go func() { worker_dispatch(key.user, key.app, &worker_frame{}); close(done) }()
	select {
	case <-done:
		t.Fatal("worker_dispatch returned on a full inbox — no back-pressure (unbounded buffer or silent drop)")
	case <-time.After(150 * time.Millisecond):
		// still blocked = back-pressure holds, the sender is paced not dropped
	}

	// Freeing one slot lets the blocked dispatch proceed — paced, never dropped.
	<-w.inbox
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("worker_dispatch stayed blocked after a slot freed — should have proceeded")
	}
}
