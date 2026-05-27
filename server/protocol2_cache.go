// Mochi server: Protocol 2 — per-(peer, protocol) selection cache.
//
// During the mixed-version rollout, the first stream-open attempt to
// each peer probes whether it supports the requested /mochi/2/* protocol.
// We cache the result for the lifetime of the libp2p connection, so
// subsequent sends skip the probe and either open /mochi/2/* directly
// or fall back to /mochi/1 without round-tripping.
//
// Cleared on peer disconnect; the peer may have upgraded (or
// downgraded) before reconnecting, so the next attempt re-probes.
//
// Per the operational context (< 10 production peers, mixed-version
// window measured in days), this is intentionally minimal — see
// claude/plans/protocol2.md → Protocol selection.
//
// Copyright Alistair Cunningham 2026

package main

import (
	"errors"
	"strings"
	"sync"

	p2p_protocol "github.com/libp2p/go-libp2p/core/protocol"
	multistream "github.com/multiformats/go-multistream"
)

type protocol_state int

const (
	protocol_state_unknown protocol_state = iota
	protocol_state_supported
	protocol_state_unsupported
)

var (
	protocol_known      = map[string]map[string]protocol_state{}
	protocol_known_lock sync.RWMutex
)

// protocol_known_get returns the cached state for (peer, proto), or
// protocol_state_unknown if we haven't probed yet.
func protocol_known_get(peer, proto string) protocol_state {
	protocol_known_lock.RLock()
	defer protocol_known_lock.RUnlock()
	if m := protocol_known[peer]; m != nil {
		return m[proto]
	}
	return protocol_state_unknown
}

// protocol_known_set records the outcome of a probe.
func protocol_known_set(peer, proto string, state protocol_state) {
	protocol_known_lock.Lock()
	defer protocol_known_lock.Unlock()
	m := protocol_known[peer]
	if m == nil {
		m = map[string]protocol_state{}
		protocol_known[peer] = m
	}
	m[proto] = state
}

// protocol_known_clear drops every protocol entry for peer. Called
// from the libp2p disconnect handler — the peer may have upgraded or
// downgraded before next connect, so the cache must re-probe.
func protocol_known_clear(peer string) {
	protocol_known_lock.Lock()
	delete(protocol_known, peer)
	protocol_known_lock.Unlock()
}

// is_protocol_not_supported tests whether err came from libp2p's
// multistream-select rejecting the requested protocol.
//
// multistream.ErrNotSupported is parameterised on the protocol-id
// type. libp2p's host returns the protocol.ID specialisation; tests
// sometimes construct the plain-string one. We try both via errors.As.
//
// As a belt-and-braces fallback we also string-match the wrapped
// message — libp2p wraps the error in basic_host with "failed to
// negotiate protocol:" and earlier go-libp2p releases sometimes
// returned the wrapped form without the original error chain.
func is_protocol_not_supported(err error) bool {
	if err == nil {
		return false
	}
	var es multistream.ErrNotSupported[string]
	if errors.As(err, &es) {
		return true
	}
	var ep multistream.ErrNotSupported[p2p_protocol.ID]
	if errors.As(err, &ep) {
		return true
	}
	// String fallback for any future libp2p wrapping that strips the
	// typed error. The phrase is stable across multistream versions.
	if msg := err.Error(); strings.Contains(msg, "protocols not supported") {
		return true
	}
	return false
}
