// Mochi server: WebSocket registry termination and the connection's writer
//
// A broken connection drives two goroutines at once: the reader's ws.Read
// fails and terminates it, and the connection's writer fails its ws.Write and
// terminates it too. Both then touch the same registry entry, so termination
// has to be safe to reach twice and from either side.
//
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"regexp"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"nhooyr.io/websocket"
)

// websocket_pair returns a live server-side connection and a function that
// breaks it, so a send against it fails the way a vanished client does.
func websocket_pair(t *testing.T) (*websocket.Conn, func()) {
	t.Helper()
	ws, client, server := websocket_dial(t)
	break_it := func() {
		client.CloseNow()
		server.CloseClientConnections()
	}
	return ws, break_it
}

// websocket_dial returns a live server-side connection, its client end and the
// test server. The client end reads nothing unless the test reads it, so a
// payload past the socket buffers blocks the server's write for real.
func websocket_dial(t *testing.T) (*websocket.Conn, *websocket.Conn, *httptest.Server) {
	t.Helper()
	accepted := make(chan *websocket.Conn, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ws, err := websocket.Accept(w, r, &websocket.AcceptOptions{InsecureSkipVerify: true})
		if err != nil {
			t.Errorf("accept: %v", err)
			return
		}
		accepted <- ws
		<-r.Context().Done()
	}))

	client, _, err := websocket.Dial(context.Background(), "ws"+server.URL[len("http"):], nil)
	if err != nil {
		server.Close()
		t.Fatalf("dial: %v", err)
	}
	ws := <-accepted
	t.Cleanup(func() {
		client.CloseNow()
		server.Close()
	})
	return ws, client, server
}

// websocket_register puts a connection into the registry and starts its
// writer the way websocket_connection does, and stops the user's writers and
// clears the user's entry afterwards.
func websocket_register(t *testing.T, u *User, key, id string, ws *websocket.Conn) {
	t.Helper()
	websocket_add(u, key, id, ws, "")
	t.Cleanup(func() {
		websockets_lock.Lock()
		for _, clients := range websockets[u.UID] {
			for _, client := range clients {
				client.cancel()
			}
		}
		delete(websockets, u.UID)
		websockets_lock.Unlock()
		websocket_settle(t)
	})
}

// websocket_settle waits, for up to five seconds, for every connection writer
// to end, and fails the test if any outlives that. A writer ends a moment
// after its connection is cancelled, and one still running when its test ends
// touches the next test's registry and presence.
func websocket_settle(t testing.TB) {
	t.Helper()
	if !websocket_wait(func() bool { return websocket_writers() == 0 }, 5*time.Second) {
		t.Errorf("%d connection writers still running after their connections ended", websocket_writers())
	}
}

// websocket_wait reports whether condition holds within the time given. A
// send only queues, so what a connection's writer does is seen a moment later.
func websocket_wait(condition func() bool, within time.Duration) bool {
	limit := time.Now().Add(within)
	for time.Now().Before(limit) {
		if condition() {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return condition()
}

// websocket_writer_frame matches a writer's own frame in a goroutine dump,
// whatever the package is called in the binary.
var websocket_writer_frame = regexp.MustCompile(`(?m)^\S+\.websocket_writer\(`)

// websocket_writers counts the connection writer goroutines alive.
func websocket_writers() int {
	buffer := make([]byte, 1<<22)
	for {
		n := runtime.Stack(buffer, true)
		if n < len(buffer) {
			return len(websocket_writer_frame.FindAllIndex(buffer[:n], -1))
		}
		buffer = make([]byte, 2*len(buffer))
	}
}

// websocket_numbers reads a client end's frames and passes on each one's "n",
// in the order they arrive.
func websocket_numbers(client *websocket.Conn) <-chan int {
	client.SetReadLimit(-1)
	numbers := make(chan int, 1024)
	go func() {
		for {
			_, data, err := client.Read(context.Background())
			if err != nil {
				return
			}
			var frame struct {
				N int `json:"n"`
			}
			if json.Unmarshal(data, &frame) == nil {
				numbers <- frame.N
			}
		}
	}()
	return numbers
}

// websocket_frames expects first..last on numbers, in that order.
func websocket_frames(t *testing.T, numbers <-chan int, first, last int) {
	t.Helper()
	for want := first; want <= last; want++ {
		select {
		case n := <-numbers:
			if n != want {
				t.Fatalf("the healthy tab received frame %d where %d was due: a tab's frames must arrive in the order they were sent", n, want)
			}
		case <-time.After(10 * time.Second):
			t.Fatalf("the healthy tab never received frame %d", want)
		}
	}
}

// websocket_stall is a frame past the socket buffers, so a write of it to a
// client that reads nothing blocks for real.
func websocket_stall() map[string]any {
	return map[string]any{"n": -1, "a": strings.Repeat("x", 24<<20)}
}

func websocket_registered(u *User, key, id string) bool {
	websockets_lock.RLock()
	defer websockets_lock.RUnlock()
	_, present := websockets[u.UID][key][id]
	return present
}

// The termination pass can hold a connection the reader goroutine already
// removed; CloseNow dereferences its receiver, so a nil must not panic.
func TestTerminateToleratesNilConnection(t *testing.T) {
	u := &User{UID: "user-nil"}
	websocket_register(t, u, "key", "other", nil)

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("terminating an already-removed connection panicked: %v", r)
		}
	}()
	websocket_terminate(nil, u, "key", "gone")
}

// TestSendTerminatesBrokenConnection — the behaviour the termination pass
// exists for: a connection that cannot be written to is closed and removed.
func TestSendTerminatesBrokenConnection(t *testing.T) {
	u := &User{UID: "user-broken"}
	ws, break_it := websocket_pair(t)
	websocket_register(t, u, "key", "id", ws)
	break_it()

	// The first write may land in the socket buffer even though the peer has
	// gone; the second reliably fails. Sending twice matches what a live
	// server does anyway.
	websockets_send(u, "", "key", map[string]any{"a": 1})
	websockets_send(u, "", "key", map[string]any{"a": 2})

	if !websocket_wait(func() bool { return !websocket_registered(u, "key", "id") }, 5*time.Second) {
		t.Error("a connection that could not be written to is still registered")
	}
}

// TestSendRacesReaderTermination — the race itself. The reader goroutine
// terminates the connection while a send queues to it and its writer fails the
// write and terminates it too, which is what one broken connection actually
// produces. Run under -race this also covers the registry access, not just the
// panic.
func TestSendRacesReaderTermination(t *testing.T) {
	u := &User{UID: "user-race"}

	for round := 0; round < 25; round++ {
		ws, break_it := websocket_pair(t)
		websocket_register(t, u, "key", "id", ws)
		break_it()

		var wg sync.WaitGroup
		wg.Add(2)
		// The reader goroutine's path: Read fails, terminate.
		go func() {
			defer wg.Done()
			websocket_terminate(ws, u, "key", "id")
		}()
		// A concurrent send: its writer's write fails, and it terminates too.
		go func() {
			defer wg.Done()
			websockets_send(u, "", "key", map[string]any{"round": round})
		}()
		wg.Wait()

		if websocket_registered(u, "key", "id") {
			t.Fatalf("round %d: the connection survived termination from both sides", round)
		}
	}
}

// TestTerminateIsIdempotent — the read loop's deferred terminate and any
// explicit one must not compound. Closing twice and deleting twice has to be
// harmless, since which of the two paths gets there first is a race.
func TestTerminateIsIdempotent(t *testing.T) {
	u := &User{UID: "user-twice"}
	ws, _ := websocket_pair(t)
	websocket_register(t, u, "key", "id", ws)

	websocket_terminate(ws, u, "key", "id")
	websocket_terminate(ws, u, "key", "id")

	if websocket_registered(u, "key", "id") {
		t.Error("still registered after termination")
	}
	websockets_lock.RLock()
	_, user_present := websockets[u.UID]
	websockets_lock.RUnlock()
	if user_present {
		t.Error("the user's empty entry was left behind")
	}
}

// TestSendLeavesLiveConnectionsRegistered — the guard against over-correcting:
// a working connection must survive a send that terminates a broken sibling.
func TestSendLeavesLiveConnectionsRegistered(t *testing.T) {
	u := &User{UID: "user-mixed"}

	live, _ := websocket_pair(t)
	broken, break_broken := websocket_pair(t)
	websocket_register(t, u, "key", "live", live)
	websocket_register(t, u, "key", "broken", broken)
	break_broken()

	websockets_send(u, "", "key", map[string]any{"a": 1})
	websockets_send(u, "", "key", map[string]any{"a": 2})

	if !websocket_wait(func() bool { return !websocket_registered(u, "key", "broken") }, 5*time.Second) {
		t.Error("the broken connection is still registered")
	}
	if !websocket_registered(u, "key", "live") {
		t.Error("a working connection was terminated alongside the broken one")
	}
}

// --- the write deadline -------------------------------------------------
//
// A client that stops reading puts its socket in TCP zero-window, where a write
// never returns. The send used to write while holding websockets_lock.RLock()
// with context.Background(), so the next terminate would queue for the write
// lock and every later send would queue behind it. Writes now happen on each
// connection's own writer, bounded by websocket_write_timeout.

// A client that stops reading blocks its writer for real, and only the
// deadline can end the write. Without it the writer is parked for the life of
// the process and the connection is never removed.
func TestSendAppliesTheWriteDeadline(t *testing.T) {
	previous := websocket_write_timeout
	websocket_write_timeout = 500 * time.Millisecond
	t.Cleanup(func() { websocket_write_timeout = previous })

	u := &User{UID: "user-deadline"}
	stalled, _ := websocket_pair(t) // its client never reads
	websocket_register(t, u, "key", "stalled", stalled)

	websockets_send(u, "", "key", websocket_stall())
	if !websocket_wait(func() bool { return !websocket_registered(u, "key", "stalled") }, 10*time.Second) {
		t.Error("the connection whose write passed its deadline is still registered: an unresponsive client parks its writer for the life of the process")
	}
}

// The control for the test above: with the ordinary timeout, a live connection
// is written to and stays registered, so the deadline is not simply killing
// every send.
func TestSendWithinTheDeadlineKeepsTheConnection(t *testing.T) {
	u := &User{UID: "user-deadline-ok"}
	live, client, _ := websocket_dial(t)
	websocket_register(t, u, "key", "live", live)
	numbers := websocket_numbers(client)

	websockets_send(u, "", "key", map[string]any{"n": 1})

	websocket_frames(t, numbers, 1, 1)
	if !websocket_registered(u, "key", "live") {
		t.Error("a live connection was terminated by a send well inside the deadline")
	}
}

// The writer must not hold the registry lock while it writes. Holding it
// across a write is what turns one stalled client into a stalled subsystem, so
// the write lock has to stay obtainable while a write is actually in flight.
func TestWriterDoesNotHoldTheLockWhileWriting(t *testing.T) {
	previous := websocket_write_timeout
	websocket_write_timeout = 2 * time.Second
	t.Cleanup(func() { websocket_write_timeout = previous })

	u := &User{UID: "user-lockscope"}
	stalled, _ := websocket_pair(t)
	websocket_register(t, u, "key", "stalled", stalled)

	websockets_send(u, "", "key", websocket_stall())
	// Give the writer time to reach the write and block in it.
	time.Sleep(500 * time.Millisecond)
	if !websocket_registered(u, "key", "stalled") {
		t.Fatal("the write did not block; the stalled connection is already gone, so lock scope is not observable here")
	}

	taken := make(chan struct{})
	go func() {
		websockets_lock.Lock()
		websockets_lock.Unlock()
		close(taken)
	}()
	// Granted in microseconds; holding the lock across the write would make it
	// wait out the whole write deadline.
	select {
	case <-taken:
	case <-time.After(1 * time.Second):
		t.Error("the write lock was unobtainable while a writer was blocked in a write: one client that stops reading stalls every other send")
	}
}

// --- the caller waits for no connection ---------------------------------
//
// A send used to write to the user's connections one after another, each with
// a 10 s deadline, on the caller's goroutine: one tab that stopped reading held
// the action, event handler or commit hook that raised the event, and the
// user's other tabs, for 10 s per stalled tab.

// The send returns at once with a stalled tab among the targets, and the
// user's healthy tab gets every frame, in order, while the stalled one is still
// blocked.
func TestSendDoesNotWaitForAStalledTab(t *testing.T) {
	previous := websocket_write_timeout
	websocket_write_timeout = 30 * time.Second
	t.Cleanup(func() { websocket_write_timeout = previous })

	u := &User{UID: "user-stalled-tab"}
	stalled, _, _ := websocket_dial(t) // its client never reads
	healthy, client, _ := websocket_dial(t)
	websocket_register(t, u, "key", "stalled", stalled)
	websocket_register(t, u, "key", "healthy", healthy)
	numbers := websocket_numbers(client)

	const sends = 40
	took := make(chan time.Duration, 1)
	go func() {
		start := time.Now()
		websockets_send(u, "", "key", websocket_stall())
		for n := 0; n < sends; n++ {
			websockets_send(u, "", "key", map[string]any{"n": n})
		}
		took <- time.Since(start)
	}()
	select {
	case elapsed := <-took:
		// Encoding the 24 MB frame is most of this.
		if elapsed > 2*time.Second {
			t.Errorf("%d sends took %v with one tab not reading: the caller is waiting on the tab", sends+1, elapsed)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the sends were held by the tab that stopped reading")
	}

	websocket_frames(t, numbers, -1, sends-1)
	if !websocket_registered(u, "key", "stalled") {
		t.Error("the stalled tab was closed though it is within its queue and its write deadline")
	}
}

// A tab whose queue fills and stays full is closed once the send has waited
// websocket_queue_timeout for room, so its client reconnects and fetches what
// it missed, rather than having frames dropped or held for the write deadline.
// The user's healthy tab is untouched.
func TestSendClosesATabThatFallsBehind(t *testing.T) {
	previous, waited := websocket_write_timeout, websocket_queue_timeout
	websocket_write_timeout = 30 * time.Second
	websocket_queue_timeout = 300 * time.Millisecond
	t.Cleanup(func() { websocket_write_timeout, websocket_queue_timeout = previous, waited })

	u := &User{UID: "user-behind"}
	stalled, _, _ := websocket_dial(t) // its client never reads
	healthy, client, _ := websocket_dial(t)
	websocket_register(t, u, "key", "stalled", stalled)
	websocket_register(t, u, "key", "healthy", healthy)
	numbers := websocket_numbers(client)

	// The stall blocks the stalled tab's writer for the rest of the test. The
	// healthy tab takes each frame before the next is sent, so only the
	// stalled tab falls behind: one more frame than its queue holds.
	sends := websocket_queue + 1
	var elapsed time.Duration
	start := time.Now()
	websockets_send(u, "", "key", websocket_stall())
	elapsed += time.Since(start)
	websocket_frames(t, numbers, -1, -1)
	for n := 0; n < sends; n++ {
		start = time.Now()
		websockets_send(u, "", "key", map[string]any{"n": n})
		elapsed += time.Since(start)
		websocket_frames(t, numbers, n, n)
	}
	// The one send that found the queue full waits websocket_queue_timeout
	// for room; nothing else waits.
	if elapsed > websocket_queue_timeout+2*time.Second {
		t.Errorf("%d sends took %v: the caller waited on the tab that fell behind", sends+1, elapsed)
	}

	// Well inside the 30 s write deadline, so it is the full queue that ends it.
	if !websocket_wait(func() bool { return !websocket_registered(u, "key", "stalled") }, 5*time.Second) {
		t.Fatal("a tab whose queue is full is still open: its frames are being dropped or held, and its client is never told to reconnect")
	}
	if !websocket_registered(u, "key", "healthy") {
		t.Error("the healthy tab was closed alongside the one that fell behind")
	}
}

// Every connection's writer ends with it. A writer that outlived its
// connection would leave a goroutine behind for every socket a user ever
// opened.
func TestWriterEndsWithItsConnection(t *testing.T) {
	u := &User{UID: "user-writer"}
	before := websocket_writers()
	ws, _ := websocket_pair(t)
	websocket_register(t, u, "key", "id", ws)
	if !websocket_wait(func() bool { return websocket_writers() == before+1 }, 2*time.Second) {
		t.Fatalf("%d writers after registering one connection, want %d", websocket_writers(), before+1)
	}

	websocket_terminate(ws, u, "key", "id")
	if !websocket_wait(func() bool { return websocket_writers() == before }, 5*time.Second) {
		t.Error("the connection's writer outlived it: every socket a user ever opened would leave a goroutine behind")
	}
}

// A tab that is only behind, not stalled, is waited for and kept: each frame
// its writer finishes frees a place, and a send that finds the queue full goes
// on as soon as one does, within websocket_queue_timeout. A Starlark loop of
// mochi.websocket.write outruns any writer, so closing a tab whenever its
// queue is full would close healthy tabs in every burst.
func TestSendWaitsForATabThatIsOnlyBehind(t *testing.T) {
	previous, waited := websocket_write_timeout, websocket_queue_timeout
	websocket_write_timeout = 30 * time.Second
	websocket_queue_timeout = 10 * time.Second
	t.Cleanup(func() { websocket_write_timeout, websocket_queue_timeout = previous, waited })

	u := &User{UID: "user-behind-only"}
	healthy, client, _ := websocket_dial(t)
	websocket_register(t, u, "key", "healthy", healthy)
	// The client starts reading a moment late, so the writer is held in the
	// stall's write and the queue fills behind it.
	reading := make(chan (<-chan int), 1)
	go func() {
		time.Sleep(300 * time.Millisecond)
		reading <- websocket_numbers(client)
	}()

	sends := 3 * websocket_queue
	start := time.Now()
	websockets_send(u, "", "key", websocket_stall())
	for n := 0; n < sends; n++ {
		websockets_send(u, "", "key", map[string]any{"n": n})
	}
	elapsed := time.Since(start)

	websocket_frames(t, <-reading, -1, sends-1)
	if !websocket_registered(u, "key", "healthy") {
		t.Error("a tab that was only behind was closed: every burst of writes would close the user's healthy tabs")
	}
	if elapsed < 250*time.Millisecond {
		t.Errorf("the sends took %v, so the queue never filled and this test exercised nothing", elapsed)
	}
}

// A cancelled connection is skipped at once, full queue or not: its writer is
// ending it, and a send that waited websocket_queue_timeout for it would pay
// the stall again on every send until the registry entry goes.
func TestEnqueueSkipsACancelledConnection(t *testing.T) {
	previous := websocket_queue_timeout
	websocket_queue_timeout = 5 * time.Second
	t.Cleanup(func() { websocket_queue_timeout = previous })

	client := &websocket_client{queue: make(chan []byte, 1)}
	client.context, client.cancel = context.WithCancel(context.Background())
	client.queue <- []byte("{}")
	client.cancel()

	start := time.Now()
	websocket_enqueue([]*websocket_client{client}, []byte("{}"))
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("a send to a cancelled connection with a full queue waited %v", elapsed)
	}
}

// Several stalled tabs with full queues cost a send websocket_queue_timeout
// once, not once each, and the user's healthy tab has the frame at once
// rather than after the waits.
func TestSendWaitsOnceForSeveralStalledTabs(t *testing.T) {
	previous, waited := websocket_write_timeout, websocket_queue_timeout
	websocket_write_timeout = 30 * time.Second
	websocket_queue_timeout = 500 * time.Millisecond
	t.Cleanup(func() { websocket_write_timeout, websocket_queue_timeout = previous, waited })

	u := &User{UID: "user-several-stalled"}
	const stalled = 4
	for i := 0; i < stalled; i++ {
		ws, _, _ := websocket_dial(t) // its client never reads
		websocket_register(t, u, "key", "stalled"+string(rune('a'+i)), ws)
	}
	healthy, client, _ := websocket_dial(t)
	websocket_register(t, u, "key", "healthy", healthy)
	numbers := websocket_numbers(client)

	// Each stalled writer is held in the stall's write, and the healthy tab
	// takes each frame before the next is sent, until every stalled queue is
	// full.
	websockets_send(u, "", "key", websocket_stall())
	websocket_frames(t, numbers, -1, -1)
	for n := 0; n < websocket_queue; n++ {
		websockets_send(u, "", "key", map[string]any{"n": n})
		websocket_frames(t, numbers, n, n)
	}

	start := time.Now()
	go websockets_send(u, "", "key", map[string]any{"n": websocket_queue})
	websocket_frames(t, numbers, websocket_queue, websocket_queue)
	if arrived := time.Since(start); arrived > websocket_queue_timeout/2 {
		t.Errorf("the healthy tab had the frame after %v: it waited behind the stalled tabs", arrived)
	}
	gone := func() bool {
		for i := 0; i < stalled; i++ {
			if websocket_registered(u, "key", "stalled"+string(rune('a'+i))) {
				return false
			}
		}
		return true
	}
	if !websocket_wait(gone, 5*time.Second) {
		t.Fatal("the stalled tabs are still open")
	}
	// One shared wait closes all four; a wait for each would take four times as
	// long.
	if elapsed := time.Since(start); elapsed > 2*websocket_queue_timeout {
		t.Errorf("closing %d stalled tabs took %v: each was waited for in turn", stalled, elapsed)
	}
}

// A connection's late terminate - its reader's or its writer's, both run on
// the way out - removes only its own entry, never a newer connection
// registered under the same id.
func TestTerminateLeavesANewerConnectionUnderTheSameId(t *testing.T) {
	u := &User{UID: "user-same-id"}
	for round := 0; round < 20; round++ {
		older, _ := websocket_pair(t)
		newer, _ := websocket_pair(t)
		websocket_register(t, u, "key", "id", older)
		websocket_terminate(older, u, "key", "id")
		// The older connection's writer is ending as this registers, and
		// its deferred terminate names the same id.
		websocket_register(t, u, "key", "id", newer)
		if !websocket_wait(func() bool { return websocket_writers() == 1 }, 5*time.Second) {
			t.Fatalf("round %d: %d writers, want the newer connection's alone", round, websocket_writers())
		}
		if !websocket_registered(u, "key", "id") {
			t.Fatalf("round %d: the older connection's terminate removed the newer connection registered under its id", round)
		}
		websocket_terminate(newer, u, "key", "id")
		websocket_settle(t)
	}
}
