// Mochi server: logical bootstrap wire protocol (#15).
//
// Carries the dump engine's messages over a stream as one tagged envelope type.
// serve (sender) dumps a DB and writes envelopes; fetch (receiver) reads them
// into the loader and verifies. Both take plain write/read closures so they sit
// on the libp2p stream RPC unchanged and are testable in-process.
//
// Copyright © 2026 Mochi OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import "fmt"

// BootstrapDBMessage is the single tagged envelope sent on the wire. Exactly one
// of the payload fields is set per Kind.
type BootstrapDBMessage struct {
	Kind     string              `cbor:"kind"` // "schema" | "batch" | "done" | "complete"
	Schema   *BootstrapSchema    `cbor:"schema,omitempty"`
	Batch    *BootstrapRowBatch  `cbor:"batch,omitempty"`
	Done     *BootstrapTableDone `cbor:"done,omitempty"`
	Sequence int64               `cbor:"sequence,omitempty"` // op-stream cursor, on "complete"
}

func bootstrap_db_envelope(msg any) (*BootstrapDBMessage, error) {
	switch m := msg.(type) {
	case *BootstrapSchema:
		return &BootstrapDBMessage{Kind: "schema", Schema: m}, nil
	case *BootstrapRowBatch:
		return &BootstrapDBMessage{Kind: "batch", Batch: m}, nil
	case *BootstrapTableDone:
		return &BootstrapDBMessage{Kind: "done", Done: m}, nil
	default:
		return nil, fmt.Errorf("bootstrap-serve: unknown dump message %T", msg)
	}
}

// bootstrap_logical_serve dumps db and writes each message as an envelope,
// finishing with a "complete" carrying the op-stream sequence the snapshot was
// taken at (so the receiver resumes the op-stream exactly at the boundary).
func bootstrap_logical_serve(db *DB, skip map[string]bool, version int, sequence int64, write func(*BootstrapDBMessage) error) error {
	emit := func(msg any) error {
		env, err := bootstrap_db_envelope(msg)
		if err != nil {
			return err
		}
		return write(env)
	}
	if err := bootstrap_logical_dump(db, skip, 0, version, emit); err != nil {
		return err
	}
	return write(&BootstrapDBMessage{Kind: "complete", Sequence: sequence})
}

// bootstrap_logical_fetch reads envelopes via read, rebuilds and verifies a
// scratch file at scratchPath, and returns the snapshot sequence from the
// "complete" message. On any error the scratch file is discarded by the caller;
// the live DB is never touched here. The returned sequence is only meaningful
// when err == nil.
func bootstrap_logical_fetch(scratchPath string, read func(*BootstrapDBMessage) error) (int64, error) {
	loader, err := bootstrap_logical_loader(scratchPath)
	if err != nil {
		return 0, err
	}
	var sequence int64
	complete := false
	for !complete {
		var env BootstrapDBMessage
		if err := read(&env); err != nil {
			_ = loader.finish()
			return 0, fmt.Errorf("bootstrap-fetch: read: %w", err)
		}
		switch env.Kind {
		case "schema":
			err = loader.apply(env.Schema)
		case "batch":
			err = loader.apply(env.Batch)
		case "done":
			err = loader.apply(env.Done)
		case "complete":
			sequence = env.Sequence
			complete = true
		default:
			err = fmt.Errorf("bootstrap-fetch: unknown message kind %q", env.Kind)
		}
		if err != nil {
			_ = loader.finish()
			return 0, err
		}
	}
	if err := loader.finish(); err != nil {
		return 0, err
	}
	return sequence, nil
}
