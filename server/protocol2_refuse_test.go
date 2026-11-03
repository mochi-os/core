// Mochi server: message frames a receiver cannot decode
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	cbor "github.com/fxamacker/cbor/v2"
)

// refuse_capture records queue_error_dispatch calls; the drop paths run on
// sender goroutines, hence the lock. A message with a gate holds its call, as
// a slow app handler would, until the gate closes.
type refuse_capture struct {
	lock  sync.Mutex
	calls []error_call
	gates map[string]chan struct{}
}

func (c *refuse_capture) install(t *testing.T) {
	t.Helper()
	previous := queue_error_dispatch
	queue_error_dispatch = func(q *QueueEntry, code, reason string) {
		c.lock.Lock()
		c.calls = append(c.calls, error_call{q.ID, q.ToEntity, code, reason})
		gate := c.gates[q.ID]
		c.lock.Unlock()
		if gate != nil {
			select {
			case <-gate:
			case <-time.After(10 * time.Second):
			}
		}
	}
	t.Cleanup(func() { queue_error_dispatch = previous })
}

// hold makes the message's error handler wait until the returned release.
func (c *refuse_capture) hold(id string) func() {
	gate := make(chan struct{})
	c.lock.Lock()
	if c.gates == nil {
		c.gates = map[string]chan struct{}{}
	}
	c.gates[id] = gate
	c.lock.Unlock()
	var once sync.Once
	return func() { once.Do(func() { close(gate) }) }
}

// of counts the dispatches for one message, and how many of them were the
// rejection a fail_unsupported maps to.
func (c *refuse_capture) of(id string) (int, int) {
	c.lock.Lock()
	defer c.lock.Unlock()
	all, rejected := 0, 0
	for _, call := range c.calls {
		if call.id != id {
			continue
		}
		all++
		if call.code == error_code_message_rejected && call.reason == fail_unsupported {
			rejected++
		}
	}
	return all, rejected
}

// refuse_wait polls until done reports true or the time runs out.
func refuse_wait(within time.Duration, done func() bool) bool {
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if done() {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return done()
}

// refuse_array is content holding one array of n elements.
func refuse_array(n int) map[string]any {
	list := make([]any, n)
	for i := range list {
		list[i] = i
	}
	return map[string]any{"comments": list}
}

// refuse_pairs is content holding one map of n pairs.
func refuse_pairs(n int) map[string]any {
	pairs := make(map[string]any, n)
	for i := 0; i < n; i++ {
		pairs["k"+strconv.Itoa(i)] = i
	}
	return map[string]any{"pairs": pairs}
}

// refuse_nested is content with n maps nested inside it.
func refuse_nested(n int) map[string]any {
	content := map[string]any{}
	inner := content
	for i := 0; i < n; i++ {
		next := map[string]any{}
		inner["n"] = next
		inner = next
	}
	return content
}

// refuse_received reports whether a receiver decodes f off the wire.
func refuse_received(t *testing.T, f *Frame) bool {
	t.Helper()
	var wire bytes.Buffer
	if err := frame_write(&wire, f); err != nil {
		t.Fatalf("frame_write: %v", err)
	}
	_, err := frame_read(&wire)
	return err == nil
}

// The sender's check is the receiver's decode: a message frame_check passes
// is one frame_read takes, and one it refuses is one frame_read refuses, at
// the edge of each cap.
func TestFrameCheckMatchesReceiverCaps(t *testing.T) {
	cases := map[string]map[string]any{
		"elements at the cap":   refuse_array(cbor_maximum_elements),
		"elements past the cap": refuse_array(cbor_maximum_elements + 1),
		"pairs at the cap":      refuse_pairs(cbor_maximum_pairs),
		"pairs past the cap":    refuse_pairs(cbor_maximum_pairs + 1),
	}
	for depth := cbor_maximum_depth - 4; depth <= cbor_maximum_depth+2; depth++ {
		cases["nested "+strconv.Itoa(depth)] = refuse_nested(depth)
	}
	refused, taken := 0, 0
	for name, content := range cases {
		f := &Frame{Type: frame_type_message, ID: "check", From: test_entity_id('c'), Service: "s", Event: "e", Content: content}
		received := refuse_received(t, f)
		err := frame_check(f)
		if received && err != nil {
			t.Errorf("%s: frame_check refused a frame the receiver decodes: %v", name, err)
		}
		if !received && err == nil {
			t.Errorf("%s: frame_check passed a frame the receiver cannot decode", name)
		}
		if err != nil {
			refused++
		} else {
			taken++
		}
	}
	// Control: both sides of every cap were exercised.
	if refused < 3 || taken < 3 {
		t.Errorf("cases straddle no cap: %d refused, %d taken", refused, taken)
	}
}

// refuse_link is a Sender and a peer on one stream, with the stream ends kept
// so a test can tell whether either was reset.
type refuse_link struct {
	peer      string
	sender    *Sender
	send      *pipe_stream
	receive   *pipe_stream
	stop      func()
	entity    string
	challenge []byte
}

// refuse_open sets up the server state and a Sender for peer, its far end
// unstarted: the caller runs a receiver on receive.
func refuse_open(t *testing.T, peer string, receiver func(link *refuse_link)) *refuse_link {
	t.Helper()
	setup_replication_test(t)
	setup_users_test_schema()
	reset_workers(t)
	t.Cleanup(func() { reset_workers(t) })
	entity, _ := new_entity_keys(t)

	link := &refuse_link{peer: peer, entity: entity, challenge: must_challenge(t)}
	link.send, link.receive = new_stream_pair()
	receiver(link)
	hello, err := hello_read(link.send, 2)
	if err != nil {
		t.Fatalf("hello_read: %v", err)
	}
	sender, stop := install_sender_for(t, peer, link.send, hello)
	var once sync.Once
	link.sender = sender
	link.stop = func() { once.Do(stop) }
	t.Cleanup(link.stop)
	return link
}

// refuse_row queues a direct message to the link's peer, as an app's send does.
func refuse_row(t *testing.T, link *refuse_link, id, from, service, event string, content map[string]any, data []byte) {
	t.Helper()
	if err := queue_add_direct(id, link.peer, from, "", service, event, "", nil, cbor_encode(content), data, "", 0); err != nil {
		t.Fatalf("queue_add_direct %q: %v", id, err)
	}
}

func refuse_held(id string) bool {
	found, _ := db_open("db/queue.db").exists("select 1 from queue where id=?", id)
	return found
}

// A message no receiver would decode is dropped by its sender as a rejection,
// before it reaches the wire. The stream carries on, so the message queued
// behind it is delivered, while the app's handler for the rejection is still
// running. The same holds for a frame over the size cap, which is only known
// once compression has been chosen for the stream.
func TestSenderDropsMessageNoReceiverDecodes(t *testing.T) {
	var capture refuse_capture
	capture.install(t)

	var received <-chan string
	link := refuse_open(t, "refuse-sender-peer", func(link *refuse_link) {
		received = run_test_receiver(t, link.receive, link.challenge, "refuse-sender-peer")
	})

	over := uid()
	large := uid()
	good := uid()
	refuse_row(t, link, over, link.entity, "s", "e", refuse_array(cbor_maximum_elements+500), nil)
	noise := make([]byte, frame_maximum+1024*1024)
	if _, err := rand.Read(noise); err != nil {
		t.Fatalf("rand: %v", err)
	}
	refuse_row(t, link, large, link.entity, "s", "e", map[string]any{}, noise)
	refuse_row(t, link, good, link.entity, "s", "e", map[string]any{"body": "fine"}, nil)
	// The undecodable rows come up first, and the first one's handler does not
	// return until the good message has arrived.
	db_open("db/queue.db").exec("update queue set next_retry = next_retry - 10 where id in (?, ?)", over, large)
	release := capture.hold(over)
	t.Cleanup(release)

	pulled := make(chan struct{})
	go func() {
		defer close(pulled)
		link.sender.pull_loop()
	}()
	t.Cleanup(func() {
		link.sender.shutdown()
		<-pulled
	})

	select {
	case id := <-received:
		if id != good {
			t.Errorf("receiver got %q, want only the good message %q", id, good)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the good message was not delivered while the rejection's handler ran")
	}
	release()

	for _, id := range []string{over, large} {
		if !refuse_wait(5*time.Second, func() bool { _, rejected := capture.of(id); return rejected > 0 }) {
			t.Errorf("message %q was not rejected as unsupported", id)
		}
		if refuse_held(id) {
			t.Errorf("message %q is still queued after its rejection", id)
		}
	}
	time.Sleep(200 * time.Millisecond)
	for _, id := range []string{over, large} {
		if all, rejected := capture.of(id); all != 1 || rejected != 1 {
			t.Errorf("message %q: %d error dispatches (%d unsupported rejections), want exactly one rejection", id, all, rejected)
		}
	}
	if all, _ := capture.of(good); all != 0 {
		t.Errorf("the good message drew %d error dispatches", all)
	}
	select {
	case id, open := <-received:
		if open {
			t.Errorf("receiver got %q as well; a refused message went over the wire", id)
		}
	default:
	}
	if n := link.send.reset_count.Load() + link.receive.reset_count.Load(); n != 0 {
		t.Errorf("stream reset %d times", n)
	}
}

// refuse_service registers an internal app whose anonymous event reports each
// message it is delivered.
func refuse_service(t *testing.T, id string) chan string {
	t.Helper()
	delivered := make(chan string, 16)
	a := app(id)
	a.service(id)
	a.event_anonymous("arrive", func(e *Event) { delivered <- e.message })
	t.Cleanup(func() {
		apps_lock.Lock()
		delete(apps, id)
		delete(internal_services, id)
		apps_lock.Unlock()
		resolution_invalidate()
	})
	resolution_invalidate()
	return delivered
}

// A receiver that cannot decode a message names it in a fail{unsupported},
// which every sender drops at once, and reads on: the frame's length bounded
// it. The sender here writes the frame unchecked, as one without frame_check
// does.
func TestReceiverRefusesUndecodableMessage(t *testing.T) {
	var capture refuse_capture
	capture.install(t)

	const service = "refusetest"
	var receiver *Receiver
	finished := make(chan struct{})
	link := refuse_open(t, "refuse-receiver-peer", func(link *refuse_link) {
		receiver = &Receiver{
			peer:      test_opener,
			stream:    link.receive,
			session:   "refuse",
			challenge: link.challenge,
			replies:   make(chan *Frame, receiver_replies_buffer),
			done:      make(chan struct{}),
			claimed:   map[string]bool{},
		}
		go func() {
			defer close(finished)
			if err := hello_write(link.receive, 2, "refuse", link.challenge, receiver_codecs(), receiver_features()); err != nil {
				t.Errorf("hello_write: %v", err)
				return
			}
			go receiver.write_replies()
			receiver.read_loop()
			receiver.closed.Store(true)
			close(receiver.done)
		}()
	})
	t.Cleanup(func() {
		link.stop()
		<-finished
	})
	delivered := refuse_service(t, service)

	bad := uid()
	good := uid()
	for _, id := range []string{bad, good} {
		refuse_row(t, link, id, "", service, "arrive", map[string]any{}, nil)
	}
	frames := []*Frame{
		{Type: frame_type_message, ID: bad, Service: service, Event: "arrive", Content: refuse_array(cbor_maximum_elements + 500)},
		{Type: frame_type_message, ID: good, Service: service, Event: "arrive", Content: map[string]any{"body": "fine"}},
	}
	for _, f := range frames {
		if err := peer_send(link.peer, f.ID, f); err != nil {
			t.Fatalf("peer_send %q: %v", f.ID, err)
		}
	}

	select {
	case id := <-delivered:
		if id != good {
			t.Errorf("delivered %q, want the good message %q", id, good)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the message after the undecodable one was not delivered")
	}
	if !refuse_wait(5*time.Second, func() bool { _, rejected := capture.of(bad); return rejected > 0 }) {
		t.Fatal("the undecodable message was not rejected as unsupported")
	}
	if !refuse_wait(5*time.Second, func() bool { queue_ack_drain(); return !refuse_held(bad) && !refuse_held(good) }) {
		t.Errorf("queued after the exchange: undecodable %v, delivered %v", refuse_held(bad), refuse_held(good))
	}
	time.Sleep(200 * time.Millisecond)
	if all, rejected := capture.of(bad); all != 1 || rejected != 1 {
		t.Errorf("undecodable message: %d error dispatches (%d unsupported rejections), want exactly one rejection", all, rejected)
	}
	if all, _ := capture.of(good); all != 0 {
		t.Errorf("the delivered message drew %d error dispatches", all)
	}
	if n := link.send.reset_count.Load() + link.receive.reset_count.Load(); n != 0 {
		t.Errorf("stream reset %d times", n)
	}
}

// refuse_input is a stream carrying the frames, ready for a Receiver to read.
type refuse_input struct {
	buffer    bytes.Buffer
	t         *testing.T
	challenge []byte // the Receiver's own, which claims must sign
}

func (r *refuse_input) caps() *refuse_input {
	if err := caps_write(&r.buffer, receiver_codecs(), nil, must_challenge(r.t)); err != nil {
		r.t.Fatalf("caps_write: %v", err)
	}
	return r
}

func (r *refuse_input) frame(f *Frame) *refuse_input {
	if err := frame_write(&r.buffer, f); err != nil {
		r.t.Fatalf("frame_write: %v", err)
	}
	return r
}

// raw writes body as a frame, with its length prefix.
func (r *refuse_input) raw(body []byte) *refuse_input {
	var length [frame_length_size]byte
	binary.BigEndian.PutUint32(length[:], uint32(len(body)))
	r.buffer.Write(length[:])
	r.buffer.Write(body)
	return r
}

// cut writes f with its length prefix but only half its body.
func (r *refuse_input) cut(f *Frame) *refuse_input {
	var whole bytes.Buffer
	if err := frame_write(&whole, f); err != nil {
		r.t.Fatalf("frame_write: %v", err)
	}
	body := whole.Bytes()
	length := binary.BigEndian.Uint32(body[:frame_length_size])
	r.buffer.Write(body[:frame_length_size+int(length)/2])
	return r
}

// read runs a Receiver over the input to its end, returning the replies it
// queued and how often it reset the stream.
func (r *refuse_input) read() ([]*Frame, int32) {
	reset := new(atomic.Int32)
	receiver := &Receiver{
		stream:    &fake_stream{buf: &r.buffer, reset_count: reset},
		replies:   make(chan *Frame, 4*messages_undecodable_maximum),
		claimed:   map[string]bool{},
		challenge: r.challenge,
	}
	receiver.read_loop()
	var replies []*Frame
	for {
		select {
		case f := <-receiver.replies:
			replies = append(replies, f)
		default:
			return replies, reset.Load()
		}
	}
}

func refuse_undecodable(id string) *Frame {
	return &Frame{Type: frame_type_message, ID: id, Service: "s", Event: "e", Content: refuse_array(cbor_maximum_elements + 1)}
}

func refuse_is(f *Frame, kind, id, reason string) bool {
	if f.Type != kind || f.Reason != reason {
		return false
	}
	if kind == frame_type_pong {
		return f.ID == id
	}
	return len(f.Replies) == 1 && f.Replies[0] == id
}

// The receiver answers an undecodable message and reads on; everything else
// that does not read as a frame still resets the stream.
func TestReceiverRefuseBounds(t *testing.T) {
	t.Run("message answered and stream read on", func(t *testing.T) {
		in := &refuse_input{t: t}
		replies, reset := in.caps().frame(refuse_undecodable("bad-1")).frame(&Frame{Type: frame_type_ping, ID: "after"}).read()
		if reset != 0 {
			t.Errorf("stream reset %d times for an undecodable message", reset)
		}
		if len(replies) != 2 || !refuse_is(replies[0], frame_type_fail, "bad-1", fail_unsupported) || !refuse_is(replies[1], frame_type_pong, "after", "") {
			t.Errorf("replies %+v, want fail{unsupported} naming bad-1, then the pong for the ping behind it", replies)
		}
	})
	t.Run("cut frame resets", func(t *testing.T) {
		in := &refuse_input{t: t}
		replies, reset := in.caps().cut(&Frame{Type: frame_type_message, ID: "cut", Service: "s", Event: "e", Content: map[string]any{"body": strings.Repeat("x", 4096)}}).read()
		if reset != 1 || len(replies) != 0 {
			t.Errorf("a truncated frame: %d resets, replies %+v; want the stream reset and nothing answered", reset, replies)
		}
	})
	t.Run("undecodable frame that is not a message resets", func(t *testing.T) {
		in := &refuse_input{t: t}
		replies, reset := in.caps().frame(&Frame{Type: frame_type_ping, ID: "ping", Content: refuse_array(cbor_maximum_elements + 1)}).read()
		if reset != 1 || len(replies) != 0 {
			t.Errorf("an undecodable ping: %d resets, replies %+v; want the stream reset and nothing answered", reset, replies)
		}
	})
	t.Run("before caps resets", func(t *testing.T) {
		in := &refuse_input{t: t}
		replies, reset := in.frame(refuse_undecodable("early")).read()
		if reset != 1 || len(replies) != 0 {
			t.Errorf("an undecodable message before caps: %d resets, replies %+v", reset, replies)
		}
	})
	t.Run("id past the envelope limit resets", func(t *testing.T) {
		in := &refuse_input{t: t}
		replies, reset := in.caps().frame(refuse_undecodable(strings.Repeat("i", id_length_maximum+1))).read()
		if reset != 1 || len(replies) != 0 {
			t.Errorf("an undecodable message with an overlong id: %d resets, replies %+v", reset, replies)
		}
	})
	t.Run("type mismatch resets for a retry", func(t *testing.T) {
		// Well formed and within every cap, but content is not a map: what a
		// field changing type between versions looks like, which a later build
		// of this side decodes.
		body, err := cbor.Marshal(map[string]any{"type": frame_type_message, "id": "skew", "service": "s", "event": "e", "content": "not a map"})
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		var typed *cbor.UnmarshalTypeError
		if _, err := frame_read((&refuse_input{t: t}).raw(body).buffer_reader()); !errors.As(err, &typed) {
			t.Fatalf("control: the body fails to decode with %v, not a type mismatch", err)
		}
		replies, reset := (&refuse_input{t: t}).caps().raw(body).read()
		if reset != 1 || len(replies) != 0 {
			t.Errorf("a type mismatch: %d resets, replies %+v; want the stream reset and nothing answered, so the sender retries", reset, replies)
		}
	})
	t.Run("budget", func(t *testing.T) {
		in := (&refuse_input{t: t}).caps()
		for i := 0; i <= messages_undecodable_maximum; i++ {
			in.frame(refuse_undecodable("bad-" + uid()))
		}
		replies, reset := in.read()
		if reset != 1 {
			t.Errorf("%d undecodable messages: %d resets, want one past the budget of %d", messages_undecodable_maximum+1, reset, messages_undecodable_maximum)
		}
		if len(replies) != messages_undecodable_maximum {
			t.Errorf("%d answered, want the budget of %d", len(replies), messages_undecodable_maximum)
		}
	})
}

func (r *refuse_input) buffer_reader() *bytes.Reader {
	return bytes.NewReader(r.buffer.Bytes())
}

// A refused message counts as a message for the stream's budgets, as handle
// counts one: it backs a prove, and returns the claim its sender used. A
// sender at either cap whose message is refused may then send one more.
func TestReceiverRefusedMessageCountsAsAMessage(t *testing.T) {
	messages_net_id(t)
	in := &refuse_input{t: t, challenge: []byte("0123456789abcdef0123456789abcdef")}
	in.caps()
	for i := 0; i < messages_proves_maximum; i++ {
		in.frame(&Frame{Type: frame_type_prove})
	}
	var claim *Frame
	for i := 0; i < messages_claims_maximum; i++ {
		claim = messages_claim(t, in.challenge)
		in.frame(claim)
	}
	backed := refuse_undecodable("backed")
	backed.From = claim.From
	in.frame(backed)
	in.frame(&Frame{Type: frame_type_prove})
	in.frame(messages_claim(t, in.challenge))

	replies, reset := in.read()
	refused := false
	for _, f := range replies {
		if refuse_is(f, frame_type_fail, "backed", fail_unsupported) {
			refused = true
		}
	}
	if !refused {
		t.Fatal("control: the undecodable message was not refused, so it backed nothing")
	}
	if reset != 0 {
		t.Errorf("stream reset %d times: the refused message was not counted, so the sender's next prove or claim went over a budget it had back", reset)
	}
}

// frame_check leaves Data out of its encoding: a byte string counts against
// none of the caps, so the verdict is the same and the copy of up to
// frame_maximum is saved.
func TestFrameCheckLeavesDataOut(t *testing.T) {
	data := make([]byte, 8<<20)
	at := &Frame{Type: frame_type_message, ID: "data", Service: "s", Event: "e", Content: refuse_array(cbor_maximum_elements), Data: data}
	past := &Frame{Type: frame_type_message, ID: "data", Service: "s", Event: "e", Content: refuse_array(cbor_maximum_elements + 1), Data: data}
	if err := frame_check(at); err != nil {
		t.Errorf("a frame at the caps carrying Data was refused: %v", err)
	}
	if err := frame_check(past); err == nil {
		t.Error("a frame past a cap carrying Data was passed")
	}
	if len(at.Data) != len(data) {
		t.Fatal("frame_check changed the frame it was given")
	}

	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	_ = frame_check(at)
	runtime.ReadMemStats(&after)
	if grown := after.TotalAlloc - before.TotalAlloc; grown > 4<<20 {
		t.Errorf("frame_check allocated %d bytes for a frame carrying %d of Data", grown, len(data))
	}
}
