// Mochi server: broadcast stream stall watchdog regression.
//
// The News feed self-loop wedge (2026-07-06 to 2026-07-15) kept gapping on
// the same received watermark for 9 days while resync never healed it, with
// no signal. broadcast_stall_note tracks gapping streams and warns once the
// watermark has been stuck for broadcast_stall_age, resetting whenever the
// watermark moves (a healing stream must never warn).
//
// Copyright © 2026 Mochi OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import "testing"

func TestBroadcastStallNote(t *testing.T) {
	age, repeat := broadcast_stall_age, broadcast_stall_repeat
	broadcast_stall_age = 0 // trip on the second gap at the same watermark
	defer func() { broadcast_stall_age, broadcast_stall_repeat = age, repeat }()

	stall := func() *broadcast_stall {
		v, ok := broadcast_stalls.Load("u1|feeds|peer-x|key-1")
		if !ok {
			return nil
		}
		return v.(*broadcast_stall)
	}

	// First gap at a watermark: tracked, not warned (no stall duration yet).
	broadcast_stall_note("u1", "feeds", "peer-x", "key-1", 100, 500)
	s := stall()
	if s == nil || s.warned != 0 {
		t.Fatal("first gap must track without warning")
	}

	// Watermark advanced between gaps: the stream is healing, tracking resets
	// and no warn fires even though gaps continue.
	broadcast_stall_note("u1", "feeds", "peer-x", "key-1", 150, 510)
	s = stall()
	if s.watermark != 150 || s.warned != 0 {
		t.Fatal("advancing watermark must reset tracking without warning")
	}

	// Same watermark past broadcast_stall_age: warns.
	broadcast_stall_note("u1", "feeds", "peer-x", "key-1", 150, 520)
	s = stall()
	if s.warned == 0 {
		t.Fatal("stuck watermark past broadcast_stall_age must warn")
	}
	first := s.warned

	// Still stuck inside the repeat window: no second warn stamp.
	broadcast_stall_note("u1", "feeds", "peer-x", "key-1", 150, 530)
	if s.warned != first {
		t.Error("stalled stream re-warned within broadcast_stall_repeat")
	}

	// Watermark finally moves (healed or manually re-anchored): tracking
	// resets so a later, unrelated stall warns fresh.
	broadcast_stall_note("u1", "feeds", "peer-x", "key-1", 900, 910)
	s = stall()
	if s.watermark != 900 || s.warned != 0 {
		t.Error("recovered stream must reset stall tracking")
	}
}
