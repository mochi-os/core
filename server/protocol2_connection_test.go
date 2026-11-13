// Mochi server: /mochi/2 connection reuse. A receiver's challenge is one value
// per libp2p connection, and an opener sends its request with its open on a
// connection it knows, but never before the far side has proved the entity it
// addresses there.
//
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the Mochi
// Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	p2p "github.com/libp2p/go-libp2p"
	p2p_host "github.com/libp2p/go-libp2p/core/host"
	p2p_network "github.com/libp2p/go-libp2p/core/network"
	p2p_peer "github.com/libp2p/go-libp2p/core/peer"
)

// challenge_conn is a connection known only by its two peers and its ID.
type challenge_conn struct {
	p2p_network.Conn
	local, remote p2p_peer.ID
	id            string
}

func (c *challenge_conn) LocalPeer() p2p_peer.ID  { return c.local }
func (c *challenge_conn) RemotePeer() p2p_peer.ID { return c.remote }
func (c *challenge_conn) ID() string              { return c.id }

// TestHelloConnectionChallengeOnePerConnection: one connection always gets the
// same challenge, and a different connection, remote peer or local peer never
// does.
func TestHelloConnectionChallengeOnePerConnection(t *testing.T) {
	challenge := func(c *challenge_conn) []byte {
		t.Helper()
		value, err := hello_connection_challenge(c)
		if err != nil {
			t.Fatalf("hello_connection_challenge: %v", err)
		}
		return value
	}
	first := challenge(&challenge_conn{local: "local", remote: "remote", id: "remote-1"})
	if len(first) != challenge_size_v2 || frame_reject_challenge(first) != nil {
		t.Fatalf("a connection challenge an opener would reject: %x", first)
	}
	if again := challenge(&challenge_conn{local: "local", remote: "remote", id: "remote-1"}); !bytes.Equal(first, again) {
		t.Error("THE POINT: one connection was given two challenges")
	}
	for name, other := range map[string]*challenge_conn{
		"another connection":  {local: "local", remote: "remote", id: "remote-2"},
		"another remote peer": {local: "local", remote: "other", id: "remote-1"},
		"another local peer":  {local: "other", remote: "remote", id: "remote-1"},
	} {
		if bytes.Equal(first, challenge(other)) {
			t.Errorf("THE POINT: %s was given the same challenge", name)
		}
	}
}

// claims_hello_on is the hello the stream receiver gives a stream on the
// connection with this ID.
func claims_hello_on(t *testing.T, connection string) *Frame {
	t.Helper()
	s := new_claims_stream_on(connection)
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		receive_stream_guarded(s)
	}()
	hello, err := frame_read(&opener_reader{f: s.outbound})
	if err != nil {
		t.Fatalf("reading the hello: %v", err)
	}
	close(s.inbound.expire)
	select {
	case <-finished:
	case <-time.After(10 * time.Second):
		t.Fatal("the receiver did not end when its read deadline fired")
	}
	return hello
}

// TestStreamReceiverChallengeIsTheConnections: every stream on one connection
// is given the same challenge, another connection's streams another, and the
// hello says so.
func TestStreamReceiverChallengeIsTheConnections(t *testing.T) {
	first := claims_hello_on(t, "12D3KooWCl-7")
	second := claims_hello_on(t, "12D3KooWCl-7")
	other := claims_hello_on(t, "12D3KooWCl-8")
	if !bytes.Equal(first.Challenge, second.Challenge) {
		t.Error("THE POINT: two streams on one connection were given two challenges")
	}
	if bytes.Equal(first.Challenge, other.Challenge) {
		t.Error("THE POINT: two connections were given one challenge")
	}
	if !contains_string(first.Features, feature_connection) {
		t.Errorf("THE POINT: the hello did not announce %q: %v", feature_connection, first.Features)
	}
}

// TestStreamRefusesAClaimOverAnotherConnectionsChallenge: a claim verifies only
// over the challenge of the connection it arrives on.
func TestStreamRefusesAClaimOverAnotherConnectionsChallenge(t *testing.T) {
	setup_peer_discovery_test(t)
	setup_users_test_schema()
	elsewhere := claims_hello_on(t, "12D3KooWCl-9").Challenge
	for _, c := range []struct {
		name     string
		accepted bool
	}{
		{"its own connection's challenge", true},
		{"another connection's challenge", false},
	} {
		t.Run(c.name, func(t *testing.T) {
			s := new_claims_stream_on("12D3KooWCl-10")
			finished := make(chan struct{})
			go func() {
				defer close(finished)
				receive_stream_guarded(s)
			}()
			challenge := claims_hello(t, s)
			if !c.accepted {
				challenge = elsewhere
			}
			claims_caps(t, s)
			claim := claims_signed(t, challenge)
			claims_send(t, s, claim)
			claims_send(t, s, &Frame{Type: frame_type_open, ID: "open-1", From: claim.From, Service: "test", Event: "test"})

			replies := make(chan *Frame, 1)
			go func() {
				if f, err := frame_read(&opener_reader{f: s.outbound}); err == nil {
					replies <- f
				}
			}()
			accepted := false
			select {
			case f := <-replies:
				accepted = f.Type == frame_type_ack
				close(s.inbound.expire)
				<-finished
			case <-finished:
			case <-time.After(10 * time.Second):
				t.Fatal("the receiver neither answered the open nor ended")
			}
			close(s.outbound.chunks)
			if accepted != c.accepted {
				t.Errorf("THE POINT: a claim over %s accepted=%v, want %v", c.name, accepted, c.accepted)
			}
		})
	}
}

// connection_pair is two real libp2p hosts on loopback: a, which the server
// takes as its own host for the test, and b, answering /mochi/2/stream with
// receive_stream unless a test gives it a script.
type connection_pair struct {
	a, b   p2p_host.Host
	peer   string
	lock   sync.Mutex
	script func(p2p_network.Stream)
}

func connection_pair_start(t *testing.T) *connection_pair {
	t.Helper()
	setup_peer_discovery_test(t)
	setup_users_test_schema()
	host := func() p2p_host.Host {
		h, err := p2p.New(p2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
		if err != nil {
			t.Fatalf("test host: %v", err)
		}
		t.Cleanup(func() { h.Close() })
		return h
	}
	p := &connection_pair{a: host(), b: host()}
	p.peer = p.b.ID().String()

	saved_me, saved_id := net_me, net_id
	net_me, net_id = p.a, p.a.ID().String()
	t.Cleanup(func() { net_me, net_id = saved_me, saved_id })
	net_watch_connections(p.a.Network())

	p.b.SetStreamHandler(protocol_stream, func(s p2p_network.Stream) {
		p.lock.Lock()
		script := p.script
		p.lock.Unlock()
		if script == nil {
			receive_stream(s)
			return
		}
		script(s)
	})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := p.a.Connect(ctx, p2p_peer.AddrInfo{ID: p.b.ID(), Addrs: p.b.Addrs()}); err != nil {
		t.Fatalf("connecting the hosts: %v", err)
	}
	peers_lock.Lock()
	peers[p.peer] = Peer{ID: p.peer, state: peer_state_connected}
	peers_lock.Unlock()
	return p
}

// play has b answer its next streams with script, reporting what each found.
func (p *connection_pair) play(script func(p2p_network.Stream) error) chan error {
	report := make(chan error, 8)
	p.lock.Lock()
	p.script = func(s p2p_network.Stream) {
		report <- script(s)
		s.Close()
	}
	p.lock.Unlock()
	return report
}

// connection is a's one connection to b.
func (p *connection_pair) connection(t *testing.T) p2p_network.Conn {
	t.Helper()
	conns := p.a.Network().ConnsToPeer(p.b.ID())
	if len(conns) != 1 {
		t.Fatalf("%d connections between the hosts, want 1", len(conns))
	}
	return conns[0]
}

// ask opens a stream to b from `from` for `to`, sending request, and reads the
// first segment of its answer.
func (p *connection_pair) ask(ctx context.Context, from, to string, request any) (map[string]any, error) {
	s, _, err := stream_open(ctx, p.peer, from, to, "test", "test", "", nil, request)
	if err != nil {
		return nil, err
	}
	defer s.close()
	answer := map[string]any{}
	if err := s.read_within(&answer, 5*time.Second); err != nil {
		return nil, err
	}
	return answer, nil
}

// learn has b's own receiver answer one stream for `to`, which records the
// connection and, as b proves `to`, the target.
func (p *connection_pair) learn(t *testing.T, to string) p2p_network.Conn {
	t.Helper()
	if _, err := p.ask(context.Background(), "", to, map[string]any{"n": 1}); err != nil {
		t.Fatalf("the first stream: %v", err)
	}
	conn := p.connection(t)
	if _, proven, known := connection_recall(conn, to); !known || !proven {
		t.Fatalf("after the first stream the connection is known=%v and the target proven=%v", known, proven)
	}
	return conn
}

// report_wait is the script's report of the stream it answered.
func report_wait(t *testing.T, report chan error) {
	t.Helper()
	select {
	case err := <-report:
		if err != nil {
			t.Errorf("THE POINT: %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("b never answered a stream")
	}
}

// script_ack answers an open with an ack proving open.To over the opener's
// caps challenge, as receive_stream does.
func script_ack(s p2p_network.Stream, caps, open *Frame) error {
	ack := &Frame{Type: frame_type_ack, Replies: []string{open.ID}}
	if open.To != "" {
		ack.Signature = responder_sign(open.To, caps.Challenge, s.Conn().RemotePeer().String(), protocol_stream)
		ack.To = open.To
	}
	return frame_write(s, ack)
}

// script_answer reads the request and answers it with its own number.
func script_answer(s p2p_network.Stream) error {
	st := stream_rw(s, s)
	request := map[string]any{}
	if err := st.read_within(&request, 3*time.Second); err != nil {
		return fmt.Errorf("reading the request: %w", err)
	}
	return st.write(map[string]any{"answer": request["n"]})
}

// script_quiet reports whether nothing arrives on s within wait.
func script_quiet(s p2p_network.Stream, wait time.Duration) bool {
	_ = s.SetReadDeadline(time.Now().Add(wait))
	defer s.SetReadDeadline(time.Time{})
	n, err := s.Read(make([]byte, 1))
	return n == 0 && err != nil
}

// TestStreamOpenSendsTheRequestWithTheOpen: on a connection whose receiver has
// already proved the target, caps, claim, open and request all arrive before
// the receiver has written anything, the claim signed over the connection's
// challenge - one round trip for the whole request.
func TestStreamOpenSendsTheRequestWithTheOpen(t *testing.T) {
	p := connection_pair_start(t)
	target, _ := new_entity_keys(t)
	sender, _ := new_entity_keys(t)
	p.learn(t, target)

	report := p.play(func(s p2p_network.Stream) error {
		_ = s.SetReadDeadline(time.Now().Add(3 * time.Second))
		caps, err := caps_read(s)
		if err != nil {
			return fmt.Errorf("no caps before the hello: %w", err)
		}
		claim, err := frame_read(s)
		if err != nil || claim.Type != frame_type_claim {
			return fmt.Errorf("no claim before the hello: %v", err)
		}
		challenge, err := hello_connection_challenge(s.Conn())
		if err != nil {
			return err
		}
		if err := claim_verify(claim.From, challenge, claim.Signature, s.Conn().LocalPeer().String(), protocol_stream); err != nil {
			return fmt.Errorf("the claim is not over the connection's challenge: %w", err)
		}
		open, err := frame_read(s)
		if err != nil || open.Type != frame_type_open {
			return fmt.Errorf("no open before the hello: %v", err)
		}
		st := stream_rw(s, s)
		request := map[string]any{}
		if err := st.read_within(&request, 3*time.Second); err != nil {
			return fmt.Errorf("no request before the hello: %w", err)
		}
		if err := hello_write(s, 2, "s", challenge, receiver_codecs(), receiver_features()); err != nil {
			return err
		}
		if err := script_ack(s, caps, open); err != nil {
			return err
		}
		return st.write(map[string]any{"answer": request["n"]})
	})
	answer, err := p.ask(context.Background(), sender, target, map[string]any{"n": 2})
	report_wait(t, report)
	if err != nil || fmt.Sprint(answer["answer"]) != "2" {
		t.Errorf("the request was answered %v, %v", answer, err)
	}
}

// TestStreamOpenHoldsTheRequestForAnUnprovenTarget: on a known connection the
// opener does not wait for the hello, but a target not yet proven there gets
// no request until this stream's ack has proved it; it is proven afterwards.
func TestStreamOpenHoldsTheRequestForAnUnprovenTarget(t *testing.T) {
	p := connection_pair_start(t)
	first, _ := new_entity_keys(t)
	target, _ := new_entity_keys(t)
	conn := p.learn(t, first)

	report := p.play(func(s p2p_network.Stream) error {
		_ = s.SetReadDeadline(time.Now().Add(3 * time.Second))
		caps, err := caps_read(s)
		if err != nil {
			return fmt.Errorf("no caps before the hello: %w", err)
		}
		open, err := frame_read(s)
		if err != nil || open.Type != frame_type_open {
			return fmt.Errorf("no open before the hello: %v", err)
		}
		if !script_quiet(s, 500*time.Millisecond) {
			return errors.New("the request reached the far side before it proved its target")
		}
		challenge, err := hello_connection_challenge(s.Conn())
		if err != nil {
			return err
		}
		if err := hello_write(s, 2, "s", challenge, receiver_codecs(), receiver_features()); err != nil {
			return err
		}
		if err := script_ack(s, caps, open); err != nil {
			return err
		}
		return script_answer(s)
	})
	answer, err := p.ask(context.Background(), "", target, map[string]any{"n": 3})
	report_wait(t, report)
	if err != nil || fmt.Sprint(answer["answer"]) != "3" {
		t.Errorf("the request was answered %v, %v", answer, err)
	}
	if _, proven, _ := connection_recall(conn, target); !proven {
		t.Error("THE POINT: a target the ack proved was not recorded as proven")
	}
}

// TestStreamOpenKeepsTheHandshakeWithAnOldReceiver: a receiver whose hello has
// no connection feature gets today's handshake on every stream: nothing until
// its hello, and each claim over that stream's own challenge.
func TestStreamOpenKeepsTheHandshakeWithAnOldReceiver(t *testing.T) {
	p := connection_pair_start(t)
	target, _ := new_entity_keys(t)
	sender, _ := new_entity_keys(t)

	report := p.play(func(s p2p_network.Stream) error {
		if !script_quiet(s, 500*time.Millisecond) {
			return errors.New("the opener wrote before an old receiver's hello")
		}
		challenge := make([]byte, challenge_size_v2)
		if _, err := rand.Read(challenge); err != nil {
			return err
		}
		if err := hello_write(s, 2, "s", challenge, receiver_codecs(), nil); err != nil {
			return err
		}
		_ = s.SetReadDeadline(time.Now().Add(3 * time.Second))
		caps, err := caps_read(s)
		if err != nil {
			return err
		}
		claim, err := frame_read(s)
		if err != nil || claim.Type != frame_type_claim {
			return fmt.Errorf("no claim: %v", err)
		}
		if err := claim_verify(claim.From, challenge, claim.Signature, s.Conn().LocalPeer().String(), protocol_stream); err != nil {
			return fmt.Errorf("the claim is not over this stream's challenge: %w", err)
		}
		open, err := frame_read(s)
		if err != nil || open.Type != frame_type_open {
			return fmt.Errorf("no open: %v", err)
		}
		if err := script_ack(s, caps, open); err != nil {
			return err
		}
		return script_answer(s)
	})
	for n := range 2 {
		answer, err := p.ask(context.Background(), sender, target, map[string]any{"n": n})
		report_wait(t, report)
		if err != nil || fmt.Sprint(answer["answer"]) != fmt.Sprint(n) {
			t.Errorf("request %d was answered %v, %v", n, answer, err)
		}
	}
	if _, _, known := connection_recall(p.connection(t), ""); known {
		t.Error("THE POINT: a connection to an old receiver was recorded")
	}
}

// TestStreamOpenForgetsATargetThatFails: a target the far side no longer holds
// fails its stream and leaves the proven set, so the next request waits for
// its proof again.
func TestStreamOpenForgetsATargetThatFails(t *testing.T) {
	p := connection_pair_start(t)
	target, _ := new_entity_keys(t)
	conn := p.learn(t, target)

	db_open("db/users.db").exec("delete from entities where id=?", target)
	if _, err := p.ask(context.Background(), "", target, map[string]any{"n": 2}); err == nil {
		t.Fatal("a request for an entity the far side no longer holds succeeded")
	}
	if _, proven, known := connection_recall(conn, target); !known || proven {
		t.Errorf("THE POINT: after its failure the connection is known=%v and the target proven=%v", known, proven)
	}
}

// TestStreamOpenDropsAConnectionWhoseChallengeChanged: a hello whose challenge
// is not the one recorded for its connection fails the call and drops the
// record, and the next call starts afresh.
func TestStreamOpenDropsAConnectionWhoseChallengeChanged(t *testing.T) {
	p := connection_pair_start(t)
	target, _ := new_entity_keys(t)
	conn := p.learn(t, target)

	connection_records_lock.Lock()
	connection_records[conn].challenge = must_challenge(t)
	connection_records_lock.Unlock()
	_, err := p.ask(context.Background(), "", target, map[string]any{"n": 2})
	if err == nil || !strings.Contains(err.Error(), "changed its connection challenge") {
		t.Errorf("THE POINT: a call over the wrong challenge answered %v", err)
	}
	if _, _, known := connection_recall(conn, target); known {
		t.Error("THE POINT: the record survived a hello that broke it")
	}
	if answer, err := p.ask(context.Background(), "", target, map[string]any{"n": 3}); err != nil || answer == nil {
		t.Errorf("the next call answered %v, %v", answer, err)
	}
	if _, _, known := connection_recall(conn, target); !known {
		t.Error("the next call did not record the connection again")
	}
}

// TestStreamOpenSendsNothingOnceItsCallerGivesUp: a request held for its
// target's proof is never sent if the caller gave up while it waited.
func TestStreamOpenSendsNothingOnceItsCallerGivesUp(t *testing.T) {
	p := connection_pair_start(t)
	first, _ := new_entity_keys(t)
	target, _ := new_entity_keys(t)
	p.learn(t, first)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	report := p.play(func(s p2p_network.Stream) error {
		_ = s.SetReadDeadline(time.Now().Add(3 * time.Second))
		caps, err := caps_read(s)
		if err != nil {
			return err
		}
		open, err := frame_read(s)
		if err != nil {
			return err
		}
		cancel()
		challenge, err := hello_connection_challenge(s.Conn())
		if err != nil {
			return err
		}
		if err := hello_write(s, 2, "s", challenge, receiver_codecs(), receiver_features()); err != nil {
			return err
		}
		if err := script_ack(s, caps, open); err != nil {
			return err
		}
		if !script_quiet(s, 2*time.Second) {
			return errors.New("the request went after its caller gave up")
		}
		return nil
	})
	_, err := p.ask(ctx, "", target, map[string]any{"n": 2})
	report_wait(t, report)
	if !errors.Is(err, context.Canceled) {
		t.Errorf("a call given up on answered %v", err)
	}
}

// TestConnectionRecordGoesWithItsConnection: libp2p's report of a closed
// connection drops its record.
func TestConnectionRecordGoesWithItsConnection(t *testing.T) {
	p := connection_pair_start(t)
	target, _ := new_entity_keys(t)
	conn := p.learn(t, target)

	if err := p.a.Network().ClosePeer(p.b.ID()); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, _, known := connection_recall(conn, target); !known {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("THE POINT: a closed connection's record was kept")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestStreamOpenUsesTheChallengeAMessagesHelloGave: a connection that carries
// a /mochi/2/messages stream is recorded from that stream's hello, so its
// first /mochi/2/stream open waits for no hello either.
func TestStreamOpenUsesTheChallengeAMessagesHelloGave(t *testing.T) {
	p := connection_pair_start(t)
	p.b.SetStreamHandler(protocol_messages, receive_messages)
	sender, err := sender_open(p.peer)
	if err != nil {
		t.Fatalf("sender_open: %v", err)
	}
	t.Cleanup(sender.shutdown)
	if _, _, known := connection_recall(p.connection(t), ""); !known {
		t.Fatal("THE POINT: the messages hello did not record its connection")
	}

	report := p.play(func(s p2p_network.Stream) error {
		_ = s.SetReadDeadline(time.Now().Add(3 * time.Second))
		caps, err := caps_read(s)
		if err != nil {
			return fmt.Errorf("no caps before the hello: %w", err)
		}
		open, err := frame_read(s)
		if err != nil || open.Type != frame_type_open {
			return fmt.Errorf("no open before the hello: %v", err)
		}
		st := stream_rw(s, s)
		request := map[string]any{}
		if err := st.read_within(&request, 3*time.Second); err != nil {
			return fmt.Errorf("no request before the hello: %w", err)
		}
		challenge, err := hello_connection_challenge(s.Conn())
		if err != nil {
			return err
		}
		if err := hello_write(s, 2, "s", challenge, receiver_codecs(), receiver_features()); err != nil {
			return err
		}
		if err := script_ack(s, caps, open); err != nil {
			return err
		}
		return st.write(map[string]any{"answer": request["n"]})
	})
	answer, err := p.ask(context.Background(), "", "", map[string]any{"n": 4})
	report_wait(t, report)
	if err != nil || fmt.Sprint(answer["answer"]) != "4" {
		t.Errorf("the request was answered %v, %v", answer, err)
	}
}

// TestStreamOpenForgetsATargetWhoseProofFails: an ack whose proof does not
// verify fails the call, and its target leaves the proven set.
func TestStreamOpenForgetsATargetWhoseProofFails(t *testing.T) {
	p := connection_pair_start(t)
	target, _ := new_entity_keys(t)
	conn := p.learn(t, target)

	report := p.play(func(s p2p_network.Stream) error {
		_ = s.SetReadDeadline(time.Now().Add(3 * time.Second))
		if _, err := caps_read(s); err != nil {
			return err
		}
		open, err := frame_read(s)
		if err != nil {
			return err
		}
		challenge, err := hello_connection_challenge(s.Conn())
		if err != nil {
			return err
		}
		if err := hello_write(s, 2, "s", challenge, receiver_codecs(), receiver_features()); err != nil {
			return err
		}
		forged := make([]byte, 64)
		return frame_write(s, &Frame{Type: frame_type_ack, Replies: []string{open.ID}, Signature: forged, To: open.To})
	})
	_, err := p.ask(context.Background(), "", target, map[string]any{"n": 2})
	report_wait(t, report)
	if err == nil || !strings.Contains(err.Error(), "failed to prove") {
		t.Errorf("an ack with a forged proof answered %v", err)
	}
	if _, proven, _ := connection_recall(conn, target); proven {
		t.Error("THE POINT: a target whose proof failed stayed proven")
	}
}

// TestStreamOpenSendsNothingForACallGivenUpBeforeItsOpen: a call whose caller
// has already given up opens no stream at all.
func TestStreamOpenSendsNothingForACallGivenUpBeforeItsOpen(t *testing.T) {
	p := connection_pair_start(t)
	target, _ := new_entity_keys(t)

	report := p.play(func(s p2p_network.Stream) error { return nil })
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := p.ask(ctx, "", target, map[string]any{"n": 2}); !errors.Is(err, context.Canceled) {
		t.Errorf("a call given up before its open answered %v", err)
	}
	select {
	case <-report:
		t.Error("THE POINT: a call given up before its open reached the far side")
	case <-time.After(500 * time.Millisecond):
	}
}

// TestStreamOpenSendsNothingWhenGivenUpDuringTheHello: a call given up while it
// waits for the hello of a connection it does not know sends nothing after it.
func TestStreamOpenSendsNothingWhenGivenUpDuringTheHello(t *testing.T) {
	p := connection_pair_start(t)
	target, _ := new_entity_keys(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	report := p.play(func(s p2p_network.Stream) error {
		cancel()
		challenge, err := hello_connection_challenge(s.Conn())
		if err != nil {
			return err
		}
		if err := hello_write(s, 2, "s", challenge, receiver_codecs(), receiver_features()); err != nil {
			return err
		}
		if !script_quiet(s, 2*time.Second) {
			return errors.New("the opener wrote after its caller gave up")
		}
		return nil
	})
	_, err := p.ask(ctx, "", target, map[string]any{"n": 2})
	report_wait(t, report)
	if !errors.Is(err, context.Canceled) {
		t.Errorf("a call given up during the hello answered %v", err)
	}
}

// TestStreamContentGivesNoneAnEmptyRequest: a script's None is sent as an empty
// request, as its null was, rather than as no request at all, which would
// leave the far side waiting for one.
func TestStreamContentGivesNoneAnEmptyRequest(t *testing.T) {
	if content, ok := stream_content(nil).(map[string]any); !ok || content == nil || len(content) != 0 {
		t.Errorf("THE POINT: None became %#v", stream_content(nil))
	}
	request := map[string]any{"n": 1}
	if content, _ := stream_content(request).(map[string]any); content["n"] != 1 {
		t.Errorf("a request became %#v", content)
	}
}

// TestStreamFailoverStopsWhereTheRequestMayHaveRun: a failed open whose request
// had already gone is not tried at another location, which would run it twice;
// any other failure is.
func TestStreamFailoverStopsWhereTheRequestMayHaveRun(t *testing.T) {
	for _, c := range []struct {
		name  string
		err   error
		tried int
	}{
		{"a request that may have run", fmt.Errorf("%w: reply read failed", error_stream_sent), 1},
		{"a request that cannot have run", errors.New("hello read failed"), 2},
	} {
		tried := 0
		_, err := stream_failover([]string{"first", "second"}, func(string) (*Stream, error) {
			tried++
			return nil, c.err
		})
		if tried != c.tried || err == nil {
			t.Errorf("THE POINT: after %s, %d locations tried and %v answered, want %d", c.name, tried, err, c.tried)
		}
	}
}

// TestStreamOpenGivesNoStreamOnceItsCallerGivesUp: a request sent with its
// open may run, but a caller that gave up while it waited for the ack gets no
// stream, and the error says the request had gone.
func TestStreamOpenGivesNoStreamOnceItsCallerGivesUp(t *testing.T) {
	p := connection_pair_start(t)
	target, _ := new_entity_keys(t)
	p.learn(t, target)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	report := p.play(func(s p2p_network.Stream) error {
		_ = s.SetReadDeadline(time.Now().Add(3 * time.Second))
		caps, err := caps_read(s)
		if err != nil {
			return err
		}
		open, err := frame_read(s)
		if err != nil {
			return err
		}
		request := map[string]any{}
		if err := stream_rw(s, s).read_within(&request, 3*time.Second); err != nil {
			return fmt.Errorf("no request with the open: %w", err)
		}
		cancel()
		challenge, err := hello_connection_challenge(s.Conn())
		if err != nil {
			return err
		}
		if err := hello_write(s, 2, "s", challenge, receiver_codecs(), receiver_features()); err != nil {
			return err
		}
		return script_ack(s, caps, open)
	})
	s, _, err := stream_open(ctx, p.peer, "", target, "test", "test", "", nil, map[string]any{"n": 2})
	report_wait(t, report)
	if s != nil || !errors.Is(err, context.Canceled) || !errors.Is(err, error_stream_sent) {
		t.Errorf("THE POINT: a call given up after its request went answered %v, %v", s, err)
	}
}

// TestStreamOpenEndsAtOnceWhenALostTargetLeavesItsRequestUnread: a request
// larger than the stream's window, sent with its open to a target the far side
// no longer holds, fails as soon as the far side says so, with its reason, and
// the target leaves the proven set.
func TestStreamOpenEndsAtOnceWhenALostTargetLeavesItsRequestUnread(t *testing.T) {
	p := connection_pair_start(t)
	target, _ := new_entity_keys(t)
	conn := p.learn(t, target)

	db_open("db/users.db").exec("delete from entities where id=?", target)
	start := time.Now()
	_, _, err := stream_open(context.Background(), p.peer, "", target, "test", "test", "", nil, map[string]any{"data": make([]byte, 4<<20)})
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Errorf("THE POINT: the refused request held its call %s", elapsed)
	}
	if err == nil || !strings.Contains(err.Error(), fail_unknown_user) {
		t.Errorf("THE POINT: the refusal answered %v, not its reason", err)
	}
	if errors.Is(err, error_stream_sent) {
		t.Errorf("a refused open said its request may have run: %v", err)
	}
	if _, proven, _ := connection_recall(conn, target); proven {
		t.Error("the lost target stayed proven")
	}
}

// TestStreamOpenSaysARequestThatWentMayHaveRun: when a stream fails after its
// request went with the open, the error says the request may have run, so no
// caller sends it to another location.
func TestStreamOpenSaysARequestThatWentMayHaveRun(t *testing.T) {
	p := connection_pair_start(t)
	target, _ := new_entity_keys(t)
	p.learn(t, target)

	report := p.play(func(s p2p_network.Stream) error {
		_ = s.SetReadDeadline(time.Now().Add(3 * time.Second))
		if _, err := caps_read(s); err != nil {
			return err
		}
		if _, err := frame_read(s); err != nil {
			return err
		}
		request := map[string]any{}
		if err := stream_rw(s, s).read_within(&request, 3*time.Second); err != nil {
			return err
		}
		// The connection gives out before any answer.
		return s.Reset()
	})
	_, _, err := stream_open(context.Background(), p.peer, "", target, "test", "test", "", nil, map[string]any{"n": 2})
	report_wait(t, report)
	if !errors.Is(err, error_stream_sent) {
		t.Errorf("THE POINT: a stream that failed after its request went answered %v", err)
	}
}

// TestStreamOpenProvesAFingerprintAsAddressed: a target addressed by its
// fingerprint is proven by its full id, and recorded as the fingerprint the
// next call addresses.
func TestStreamOpenProvesAFingerprintAsAddressed(t *testing.T) {
	p := connection_pair_start(t)
	target, _ := new_entity_keys(t)
	short := fingerprint(target)
	conn := p.learn(t, short)
	if _, proven, _ := connection_recall(conn, target); proven {
		t.Error("the full id was recorded for a call that addressed the fingerprint")
	}
	if answer, err := p.ask(context.Background(), "", short, map[string]any{"n": 2}); err != nil || answer == nil {
		t.Errorf("the next call by fingerprint answered %v, %v", answer, err)
	}
}

// TestStreamReceiverTakesAClaimOverTheConnectionsChallenge: the real receiver
// accepts a claim the opener signed over the challenge it recorded for the
// connection, without waiting for that stream's hello.
func TestStreamReceiverTakesAClaimOverTheConnectionsChallenge(t *testing.T) {
	p := connection_pair_start(t)
	target, _ := new_entity_keys(t)
	sender, _ := new_entity_keys(t)
	for n := range 3 {
		if answer, err := p.ask(context.Background(), sender, target, map[string]any{"n": n}); err != nil || answer == nil {
			t.Errorf("THE POINT: claimed request %d answered %v, %v", n, answer, err)
		}
	}
	if _, proven, known := connection_recall(p.connection(t), target); !known || !proven {
		t.Errorf("the connection is known=%v and the target proven=%v", known, proven)
	}
}

// TestStreamOpensTogetherOnOneConnection: many opens at once on one connection,
// some to targets it has proven and some not, all answer.
func TestStreamOpensTogetherOnOneConnection(t *testing.T) {
	p := connection_pair_start(t)
	sender, _ := new_entity_keys(t)
	targets := make([]string, 4)
	for i := range targets {
		targets[i], _ = new_entity_keys(t)
	}
	p.learn(t, targets[0])
	var wait sync.WaitGroup
	failures := make(chan error, 32)
	for i := range 32 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			if _, err := p.ask(context.Background(), sender, targets[i%len(targets)], map[string]any{"n": i}); err != nil {
				failures <- err
			}
		}()
	}
	wait.Wait()
	close(failures)
	for err := range failures {
		t.Errorf("THE POINT: an open beside others failed: %v", err)
	}
}

// bounded_conn is a connection known only by its identity and whether it has
// closed.
type bounded_conn struct {
	p2p_network.Conn
	closed bool
}

func (c *bounded_conn) IsClosed() bool { return c.closed }

// TestConnectionRecordsAreBounded: a record keeps at most
// connection_proven_maximum proven targets, and past
// connection_records_maximum records a new connection is recorded only once a
// closed one's record is swept.
func TestConnectionRecordsAreBounded(t *testing.T) {
	connection_records_lock.Lock()
	saved := connection_records
	connection_records = map[p2p_network.Conn]*connection_record{}
	connection_records_lock.Unlock()
	t.Cleanup(func() {
		connection_records_lock.Lock()
		connection_records = saved
		connection_records_lock.Unlock()
	})
	hello := &Frame{Type: frame_type_hello, Challenge: must_challenge(t), Features: []string{feature_connection}}

	first := &bounded_conn{}
	connection_learn(first, hello)
	for i := range connection_proven_maximum + 10 {
		connection_prove(first, fmt.Sprintf("entity-%d", i))
	}
	connection_records_lock.Lock()
	proven := len(connection_records[first].proven)
	connection_records_lock.Unlock()
	if proven != connection_proven_maximum {
		t.Errorf("THE POINT: a record holds %d proven targets, want %d", proven, connection_proven_maximum)
	}

	for len(connection_records) < connection_records_maximum {
		connection_learn(&bounded_conn{}, hello)
	}
	extra := &bounded_conn{}
	connection_learn(extra, hello)
	if _, _, known := connection_recall(extra, ""); known {
		t.Error("THE POINT: a record was kept past the maximum")
	}
	first.closed = true
	connection_learn(extra, hello)
	if _, _, known := connection_recall(extra, ""); !known {
		t.Error("a closed connection's record was not swept to make room")
	}
	if _, _, known := connection_recall(first, ""); known {
		t.Error("the closed connection's record survived the sweep")
	}
}

// TestRemoteOpenHeldSendsNothingOnceItsCallerGivesUp: a held payload, as
// mochi.remote.streams sends one, waits for the ack even to a target the
// connection has proven, and is never sent once its call has given up.
func TestRemoteOpenHeldSendsNothingOnceItsCallerGivesUp(t *testing.T) {
	p := connection_pair_start(t)
	target, _ := new_entity_keys(t)
	p.learn(t, target)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	report := p.play(func(s p2p_network.Stream) error {
		_ = s.SetReadDeadline(time.Now().Add(3 * time.Second))
		caps, err := caps_read(s)
		if err != nil {
			return err
		}
		open, err := frame_read(s)
		if err != nil {
			return err
		}
		if !script_quiet(s, 300*time.Millisecond) {
			return errors.New("a held payload went with its open")
		}
		cancel()
		challenge, err := hello_connection_challenge(s.Conn())
		if err != nil {
			return err
		}
		if err := hello_write(s, 2, "s", challenge, receiver_codecs(), receiver_features()); err != nil {
			return err
		}
		if err := script_ack(s, caps, open); err != nil {
			return err
		}
		if !script_quiet(s, 2*time.Second) {
			return errors.New("a held payload went after its call gave up")
		}
		return nil
	})
	s, err := remote_open(ctx, "", target, p.peer, "test", "test", "", nil, map[string]any{"n": 2}, true)
	report_wait(t, report)
	if s != nil || !errors.Is(err, context.Canceled) {
		t.Errorf("a held call given up on answered %v, %v", s, err)
	}
}
