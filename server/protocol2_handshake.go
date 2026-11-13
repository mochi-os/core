// Mochi server: Protocol 2 — shared handshake.
//
// /mochi/2/messages and /mochi/2/stream both begin with the same
// sequence:
//
//   1. Receiver writes a `hello` frame with its challenge for the
//      connection, the protocol version, a session ID for log
//      correlation, and its supported codecs + features.
//   2. Sender writes a `caps` frame with its own codecs + features.
//   3. Sender writes one or more `claim` frames — one per local entity
//      that will send on the stream. Receiver verifies each signature
//      over its challenge and caches `claimed[From]=true`.
//
// After step 3 the two protocols diverge: /mochi/2/messages enters the
// multiplexed dispatch loop; /mochi/2/stream waits for an `open` frame
// then transitions to raw bytes.
//
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"bytes"
	"crypto/hmac"
	crand "crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"sync"

	p2p_network "github.com/libp2p/go-libp2p/core/network"
)

// session_id returns a fresh 8-byte hex log-correlation token, logged on both
// sides at stream open and close.
func session_id() string {
	var b [8]byte
	if _, err := crand.Read(b[:]); err != nil {
		// crypto/rand failing is "the OS is broken" — return a marker
		// that's visibly wrong so logs make the cause obvious.
		return "00000000-rand-fail"
	}
	return hex.EncodeToString(b[:])
}

// hello_challenge fills a fresh challenge_size_v2 buffer from the OS entropy
// pool: the challenge an opener's caps gives the far side to sign, one per
// stream.
func hello_challenge() ([]byte, error) {
	b := make([]byte, challenge_size_v2)
	if _, err := crand.Read(b); err != nil {
		return nil, fmt.Errorf("hello: challenge entropy failed: %w", err)
	}
	return b, nil
}

// feature_connection is the hello feature of a receiver whose challenge is one
// value per libp2p connection, so an opener may sign over a challenge it read
// from an earlier hello on the same connection.
const feature_connection = "connection"

// hello_connection_domain separates the connection challenge's input from any
// other use of the secret; a change to that input MUST change it.
const hello_connection_domain = "mochi/2/hello/connection/1"

// hello_secret keys every connection challenge this process makes. Drawn on
// first use; a restart draws another, and no connection outlives its process.
var hello_secret struct {
	once sync.Once
	key  []byte
	err  error
}

// hello_connection_challenge is a receiver's challenge for every stream of conn,
// on either protocol: HMAC-SHA256 under this process's secret over the two
// peers and the connection's ID, which the swarm never gives two connections.
// Nobody without the secret can compute it, and a claim over it verifies on no
// other connection; only the peer at the far end can send on this one. See
// claude/plans/protocol2.md Hello handshake → Challenge generation.
func hello_connection_challenge(conn p2p_network.Conn) ([]byte, error) {
	hello_secret.once.Do(func() {
		key := make([]byte, 32)
		if _, err := crand.Read(key); err != nil {
			hello_secret.err = err
			return
		}
		hello_secret.key = key
	})
	if hello_secret.err != nil {
		return nil, fmt.Errorf("hello: secret entropy failed: %w", hello_secret.err)
	}
	if conn == nil {
		return nil, fmt.Errorf("hello: no connection")
	}
	mac := hmac.New(sha256.New, hello_secret.key)
	// Each part length-prefixed, so no two sequences of parts hash alike.
	for _, part := range []string{hello_connection_domain, conn.LocalPeer().String(), conn.RemotePeer().String(), conn.ID()} {
		var size [4]byte
		binary.BigEndian.PutUint32(size[:], uint32(len(part)))
		mac.Write(size[:])
		mac.Write([]byte(part))
	}
	return mac.Sum(nil), nil
}

// connection_record is what an opener knows of one libp2p connection to a
// receiver whose hello listed feature_connection: that hello's challenge, the
// codecs and features both sides share, and the entities the receiver has
// proved on the connection over /mochi/2/stream.
type connection_record struct {
	challenge []byte
	codecs    []string
	features  []string
	proven    map[string]bool
}

// connection_records is keyed by the connection itself, so a record can never
// match a later connection. net_watch_connections drops each record as libp2p
// reports its connection closed.
var (
	connection_records_lock = &sync.Mutex{}
	connection_records      = map[p2p_network.Conn]*connection_record{}
)

const (
	// connection_records_maximum bounds the records kept; past it, records of
	// closed connections are swept, and a new one is not kept if none was.
	connection_records_maximum = 4096
	// connection_proven_maximum bounds the entities one record holds as proven;
	// past it a new target takes the two-round-trip path.
	connection_proven_maximum = 256
)

// connection_learn records the hello read on a stream of conn, when its
// receiver announced feature_connection. A hello whose challenge differs from
// the one recorded for its connection breaks that promise, and the record goes.
func connection_learn(conn p2p_network.Conn, hello *Frame) {
	if conn == nil || hello == nil || !contains_string(hello.Features, feature_connection) {
		return
	}
	connection_records_lock.Lock()
	defer connection_records_lock.Unlock()
	if existing := connection_records[conn]; existing != nil {
		if !bytes.Equal(existing.challenge, hello.Challenge) {
			delete(connection_records, conn)
		}
		return
	}
	if conn.IsClosed() {
		return
	}
	if len(connection_records) >= connection_records_maximum {
		for c := range connection_records {
			if c.IsClosed() {
				delete(connection_records, c)
			}
		}
		if len(connection_records) >= connection_records_maximum {
			return
		}
	}
	connection_records[conn] = &connection_record{
		challenge: bytes.Clone(hello.Challenge),
		codecs:    codec_intersect(receiver_codecs(), hello.Codecs),
		features:  features_intersect(receiver_features(), hello.Features),
		proven:    map[string]bool{},
	}
}

// connection_recall returns a copy of conn's record, without its proven set,
// and whether the receiver has proved `to` on it.
func connection_recall(conn p2p_network.Conn, to string) (connection_record, bool, bool) {
	connection_records_lock.Lock()
	defer connection_records_lock.Unlock()
	record := connection_records[conn]
	if record == nil {
		return connection_record{}, false, false
	}
	return connection_record{challenge: record.challenge, codecs: record.codecs, features: record.features}, record.proven[to], true
}

// connection_prove notes that the receiver at the far end of conn proved it
// holds `to` there.
func connection_prove(conn p2p_network.Conn, to string) {
	connection_records_lock.Lock()
	defer connection_records_lock.Unlock()
	record := connection_records[conn]
	if record == nil || to == "" || len(record.proven) >= connection_proven_maximum {
		return
	}
	record.proven[to] = true
}

// connection_doubt takes `to` out of conn's proven set: the receiver failed to
// prove it, or answered for it with a fail.
func connection_doubt(conn p2p_network.Conn, to string) {
	connection_records_lock.Lock()
	defer connection_records_lock.Unlock()
	if record := connection_records[conn]; record != nil {
		delete(record.proven, to)
	}
}

// connection_forget drops conn's record.
func connection_forget(conn p2p_network.Conn) {
	connection_records_lock.Lock()
	defer connection_records_lock.Unlock()
	delete(connection_records, conn)
}

// net_watch_connections drops each connection's record as libp2p reports the
// connection closed. Called once per host, from net_start.
func net_watch_connections(network p2p_network.Network) {
	network.Notify(&p2p_network.NotifyBundle{
		DisconnectedF: func(_ p2p_network.Network, conn p2p_network.Conn) {
			connection_forget(conn)
		},
	})
}

func hello_write(w io.Writer, version int, session string, challenge []byte,
	codecs, features []string) error {
	return frame_write(w, &Frame{
		Type:      frame_type_hello,
		Version:   version,
		Session:   session,
		Challenge: challenge,
		Codecs:    codecs,
		Features:  features,
	})
}

// hello_read decodes the receiver's hello frame and runs the
// rejection tests demanded by Challenge generation. The sender treats
// a rejected hello (bad challenge, wrong version) as a protocol
// negotiation failure.
func hello_read(r io.Reader, version int) (*Frame, error) {
	f, err := frame_read(r)
	if err != nil {
		return nil, fmt.Errorf("hello: %w", err)
	}
	if f.Type != frame_type_hello {
		return nil, fmt.Errorf("hello: unexpected type %q", f.Type)
	}
	if f.Version != version {
		return nil, fmt.Errorf("hello: version mismatch want %d got %d", version, f.Version)
	}
	if err := frame_reject_challenge(f.Challenge); err != nil {
		return nil, err
	}
	return f, nil
}

// caps_write writes the sender's caps frame. MUST be the sender's first frame;
// the receiver closes the stream on anything else. The challenge is mandatory:
// the answering side signs it to prove it holds the entity being addressed.
func caps_write(w io.Writer, codecs, features []string, challenge []byte) error {
	return frame_write(w, &Frame{
		Type:      frame_type_caps,
		Codecs:    codecs,
		Features:  features,
		Challenge: challenge,
	})
}

// caps_read decodes the sender's caps frame. Receiver MUST call this
// for the first frame after writing hello; receiving anything else
// before caps is a protocol violation.
func caps_read(r io.Reader) (*Frame, error) {
	f, err := frame_read(r)
	if err != nil {
		return nil, fmt.Errorf("caps: %w", err)
	}
	if f.Type != frame_type_caps {
		return nil, fmt.Errorf("caps: first frame was %q not caps", f.Type)
	}
	return f, nil
}

// claim_write writes one `claim` frame for `entity`. receiver must be the
// authenticated peer this stream runs to.
func claim_write(w io.Writer, entity string, challenge []byte, receiver string, protocol string) error {
	sig := claim_sign(entity, challenge, receiver, protocol)
	if sig == nil {
		return fmt.Errorf("claim: signing failed for entity %q", entity)
	}
	return frame_write(w, &Frame{
		Type:      frame_type_claim,
		From:      entity,
		Signature: sig,
	})
}
