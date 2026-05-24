// mochictl/replica.go: safety-check tests for cmd_replica_reset
// Copyright Alistair Cunningham 2026

//go:build linux

package main

import (
	"strings"
	"testing"
)

// TestReplicaResetRefusesWithoutFrom: bare reset (no --from, no --confirm)
// refuses with the usage banner.
func TestReplicaResetRefusesWithoutFrom(t *testing.T) {
	err := cmd_replica_reset(nil)
	if err == nil {
		t.Fatal("must refuse without --from")
	}
	if !strings.Contains(err.Error(), "--from=") {
		t.Errorf("error %q must mention --from=", err)
	}
}

// TestReplicaResetRefusesWithoutConfirm: --from on its own is not enough;
// --confirm is required because reset is destructive.
func TestReplicaResetRefusesWithoutConfirm(t *testing.T) {
	err := cmd_replica_reset([]string{"--from=12D3KooWPeer"})
	if err == nil {
		t.Fatal("must refuse without --confirm")
	}
	if !strings.Contains(err.Error(), "--confirm") {
		t.Errorf("error %q must mention --confirm", err)
	}
	if !strings.Contains(err.Error(), "destructive") {
		t.Errorf("error %q must convey destructiveness", err)
	}
}

// TestReplicaResetRejectsUnknownArg: defends against typos like
// --form= or --comfirm so the operator doesn't silently get a no-op
// run when they thought they invoked reset.
func TestReplicaResetRejectsUnknownArg(t *testing.T) {
	cases := []string{
		"--form=12D3KooWPeer",
		"--comfirm",
		"--peer=12D3KooWPeer",
	}
	for _, arg := range cases {
		err := cmd_replica_reset([]string{arg, "--confirm"})
		if err == nil {
			t.Errorf("must reject unknown arg %q", arg)
			continue
		}
		if !strings.Contains(err.Error(), "unknown") {
			t.Errorf("arg %q: error %q must mention 'unknown'", arg, err)
		}
	}
}
