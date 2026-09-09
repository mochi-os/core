// Mochi server: the cycle check that guards nested group membership.
//
// Copyright © 2026 Mochisoft OU
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the Mochi
// Application Interface Exception - see license.txt and license-exception.md.

package main

import "testing"

// group_graph returns a user database holding the given parent -> member
// edges between groups.
func group_graph(t *testing.T, edges [][2]string) *DB {
	t.Helper()
	test_data_directory(t)
	db := db_user(&User{UID: "u-groups"}, "user")
	for _, edge := range edges {
		db.row_write(reg_members, map[string]any{"parent": edge[0], "member": edge[1], "type": "group", "created": now()})
	}
	return db
}

// The check answers whether making member a member of group would let a group
// contain itself. Only an edge that closes a loop is refused: a container may
// reach a group both directly and through a subgroup.
func TestGroupWouldCycle(t *testing.T) {
	cases := []struct {
		name   string
		edges  [][2]string
		group  string
		member string
		want   bool
	}{
		{"closing a three-group ring", [][2]string{{"a", "b"}, {"b", "c"}}, "c", "a", true},
		{"reversing a direct edge", [][2]string{{"a", "b"}}, "b", "a", true},
		{"a group inside itself", nil, "a", "a", true},
		{"a shortcut into an existing chain", [][2]string{{"x", "y"}, {"y", "z"}}, "x", "z", false},
		{"a group nowhere near the chain", [][2]string{{"a", "b"}, {"b", "c"}}, "d", "a", false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			db := group_graph(t, c.edges)
			if got := db.group_would_cycle(c.group, c.member); got != c.want {
				t.Errorf("group_would_cycle(%q, %q) = %v with edges %v, want %v", c.group, c.member, got, c.edges, c.want)
			}
		})
	}
}
