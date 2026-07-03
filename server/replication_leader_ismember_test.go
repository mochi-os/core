package main

import "testing"

// #133 gap: replica_leader_granted_event authorises the sender via
// replication_leader_is_member. For a user: scope it must accept the operator
// pair partner (union with the pair), exactly like replication_leader_membership
// — otherwise paired hosts reject each other's leader grants and never coordinate.
func TestLeaderIsMemberUnionsPairForUserScope(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()

	db := db_open("db/replication.db")
	db.exec("insert into pair (peer, added) values ('pair-peer', ?)", now())
	db.exec("insert into hosts (user, peer, added) values ('user-x', 'host-peer', ?)", now())
	pair_membership_refresh() // peer_is_pair reads the in-memory map, not the table

	// A peer in the user's host set is a member.
	if !replication_leader_is_member("user:user-x", "host-peer") {
		t.Error("host-set peer must be a member of the user scope")
	}
	// The pair partner is a member too (the #133-gap fix), even though it is not
	// in this user's hosts table.
	if !replication_leader_is_member("user:user-x", "pair-peer") {
		t.Error("pair partner must be a member of a user scope (union with pair)")
	}
	// A stranger is not.
	if replication_leader_is_member("user:user-x", "stranger-peer") {
		t.Error("unrelated peer must not be a member")
	}
	// Server (non-user) scopes remain pair-only.
	if !replication_leader_is_member("platform", "pair-peer") {
		t.Error("pair partner must be a member of a server scope")
	}
	if replication_leader_is_member("platform", "host-peer") {
		t.Error("a mere host-set peer must not be a member of a server scope")
	}
}
