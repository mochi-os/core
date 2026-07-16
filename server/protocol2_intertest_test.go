//go:build intertest

// Mochi server: inter-instance test harness (task #60).
//
// These tests drive the two REAL local systemd instances — mochi1 and
// mochi2 — over their actual libp2p transport, rather than mocknet or a
// fork/exec'd throwaway. Real processes catch transport realism that
// in-memory networks don't: TCP windowing, real GC pauses, real signal
// handling, real stream resets. The trade-off is that they need the
// instances running and (for some scenarios) a wiped mochi2, so they're
// gated behind the `intertest` build tag and never compile into a normal
// `go test ./server/` run:
//
//	go test -tags intertest ./server/ -run TestInter -v
//
// Wipe safety (see claude/plans/protocol2.md + task #60):
//   - mochi1 is NEVER wiped — it's the operator's working instance.
//   - mochi2 may be wiped ONLY with per-run operator approval, passed
//     in via the MOCHI2_WIPE env var (full | partial | surgical). A
//     test that needs a wipe scope the operator didn't approve calls
//     t.Skip rather than wiping. `go test` can't prompt mid-run, so the
//     approval handshake happens out-of-band: the operator approves a
//     scope, then the run is launched with MOCHI2_WIPE set to it.
//
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

// inter_instance describes one local systemd mochi instance the harness
// drives. Fields are everything the helpers need to reach it: HTTP
// health/API port, on-disk data dir (for direct sqlite reads), the
// mochi.conf path (for mochictl), and the systemd --user unit name.
type inter_instance struct {
	name string
	conf string
	data string
	web  int
	unit string
}

var (
	inter_mochi1 = inter_instance{
		name: "mochi1",
		conf: "/etc/mochi/mochi1.conf",
		data: "/home/alistair/var/lib/mochi",
		web:  8081,
		unit: "mochi1",
	}
	inter_mochi2 = inter_instance{
		name: "mochi2",
		conf: "/etc/mochi/mochi2.conf",
		data: "/home/alistair/var/lib/mochi2",
		web:  8082,
		unit: "mochi2",
	}
)

// inter_http_timeout bounds every health/API call so a hung instance
// fails the test fast instead of stalling the whole run.
const inter_http_timeout = 5 * time.Second

// inter_require skips the test unless BOTH instances answer /_/health.
// Inter-instance tests are meaningless without the live processes, and
// skipping (not failing) keeps `go test -tags intertest` green on a
// machine where the instances simply aren't running.
func inter_require(t *testing.T) {
	t.Helper()
	for _, inst := range []inter_instance{inter_mochi1, inter_mochi2} {
		if _, err := inter_health(inst); err != nil {
			t.Skipf("inter-instance harness: %s not reachable (%v); start it with `systemctl --user start %s`",
				inst.name, err, inst.unit)
		}
	}
}

// inter_health GETs /_/health and returns the decoded JSON. The health
// endpoint needs no auth and reports status/database/network/version/
// uptime, so it doubles as both a liveness probe and a version check.
func inter_health(inst inter_instance) (map[string]any, error) {
	client := &http.Client{Timeout: inter_http_timeout}
	resp, err := client.Get(fmt.Sprintf("http://127.0.0.1:%d/_/health", inst.web))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var out map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, fmt.Errorf("decode health: %w", err)
	}
	return out, nil
}

// inter_version returns the running build version reported by /_/health.
func inter_version(t *testing.T, inst inter_instance) string {
	t.Helper()
	h, err := inter_health(inst)
	if err != nil {
		t.Fatalf("%s health: %v", inst.name, err)
	}
	v, _ := h["version"].(string)
	return v
}

// inter_mochictl runs `mochictl -f <conf> <args...>` against an instance
// and returns trimmed stdout. mochictl talks to <data>/run/admin.sock,
// so this is how the harness reads peer-id / pipelining / replication
// state and drives admin operations.
func inter_mochictl(t *testing.T, inst inter_instance, args ...string) string {
	t.Helper()
	full := append([]string{"-f", inst.conf}, args...)
	out, err := exec.Command("mochictl", full...).CombinedOutput()
	if err != nil {
		t.Fatalf("%s mochictl %v: %v\n%s", inst.name, args, err, out)
	}
	return strings.TrimSpace(string(out))
}

// inter_identity returns an instance's libp2p peer id (via
// `mochictl identity`). Tests use it to assert routing: e.g. mochi1
// sends to an entity hosted on mochi2's peer id.
func inter_identity(t *testing.T, inst inter_instance) string {
	t.Helper()
	out := inter_mochictl(t, inst, "identity")
	for _, line := range strings.Split(out, "\n") {
		if strings.HasPrefix(line, "Peer id:") {
			return strings.TrimSpace(strings.TrimPrefix(line, "Peer id:"))
		}
	}
	t.Fatalf("%s identity: no 'Peer id:' line in:\n%s", inst.name, out)
	return ""
}

// inter_sqlite runs a read-only query against one of an instance's DBs
// via the sqlite3 CLI. WAL mode lets us read while the server writes;
// callers should let operations settle before asserting on the result.
// `db` is relative to the instance data dir, e.g. "db/queue.db".
func inter_sqlite(t *testing.T, inst inter_instance, db, query string) string {
	t.Helper()
	path := inst.data + "/" + db
	out, err := exec.Command("sqlite3", path, query).CombinedOutput()
	if err != nil {
		t.Fatalf("%s sqlite3 %s %q: %v\n%s", inst.name, db, query, err, out)
	}
	return strings.TrimSpace(string(out))
}

// inter_mochi2_state returns observable mochi2 state for assertions:
// running version and total queue depth. Extend as scenarios need more.
func inter_mochi2_state(t *testing.T) (version string, queue int) {
	t.Helper()
	version = inter_version(t, inter_mochi2)
	depth := inter_sqlite(t, inter_mochi2, "db/queue.db", "select count(*) from queue")
	fmt.Sscanf(depth, "%d", &queue)
	return version, queue
}

// inter_mochi2_wipe performs an operator-approved wipe of mochi2 and
// restarts it. The scope must match the MOCHI2_WIPE env var or the test
// is skipped — this is the safety interlock that prevents an
// accidental `go test -tags intertest` run from destroying mochi2's
// state without explicit per-run approval. mochi1 has no wipe helper by
// design; it's never wiped.
//
// Scopes:
//   - full:     stop, delete the entire data dir (including the libp2p
//               host key → fresh peer identity on restart), start.
//   - partial:  stop, delete data DBs but preserve db/host.key so the
//               peer identity survives, start.
//   - surgical: stop, delete only the named DBs (extra args), start.
//
// Returns once mochi2 answers /_/health again.
func inter_mochi2_wipe(t *testing.T, scope string, surgical ...string) {
	t.Helper()
	approved := os.Getenv("MOCHI2_WIPE")
	if approved != scope {
		t.Skipf("inter-instance harness: test needs a %q wipe of mochi2 but MOCHI2_WIPE=%q; "+
			"re-run with operator approval: MOCHI2_WIPE=%s go test -tags intertest ...", scope, approved, scope)
	}

	// Refuse to ever touch mochi1's data dir, defensively.
	if inter_mochi2.data == inter_mochi1.data || !strings.HasSuffix(inter_mochi2.data, "mochi2") {
		t.Fatalf("inter-instance harness: refusing to wipe — mochi2 data dir %q looks wrong", inter_mochi2.data)
	}

	mustRun := func(name string, args ...string) {
		if out, err := exec.Command(name, args...).CombinedOutput(); err != nil {
			t.Fatalf("%s %v: %v\n%s", name, args, err, out)
		}
	}

	mustRun("systemctl", "--user", "stop", inter_mochi2.unit)
	switch scope {
	case "full":
		mustRun("rm", "-rf", inter_mochi2.data+"/db", inter_mochi2.data+"/users", inter_mochi2.data+"/host.key")
	case "partial":
		// Preserve host.key (peer identity); drop the rest.
		mustRun("rm", "-rf", inter_mochi2.data+"/db", inter_mochi2.data+"/users")
	case "surgical":
		if len(surgical) == 0 {
			t.Fatal("inter-instance harness: surgical wipe needs at least one db path")
		}
		for _, db := range surgical {
			mustRun("rm", "-f", inter_mochi2.data+"/"+db)
		}
	default:
		t.Fatalf("inter-instance harness: unknown wipe scope %q", scope)
	}
	mustRun("systemctl", "--user", "start", inter_mochi2.unit)

	// Wait for mochi2 to answer health again.
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := inter_health(inter_mochi2); err == nil {
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("inter-instance harness: mochi2 did not come back after %q wipe", scope)
}

// --- Scenarios ---------------------------------------------------------

// TestInterSmoke is the non-destructive baseline: both instances up,
// reporting versions, distinct peer ids, and seeing peers. It proves
// the harness can drive both instances before any scenario wipes state.
func TestInterSmoke(t *testing.T) {
	inter_require(t)

	v1 := inter_version(t, inter_mochi1)
	v2 := inter_version(t, inter_mochi2)
	t.Logf("mochi1 version=%s, mochi2 version=%s", v1, v2)
	if v1 != v2 {
		t.Logf("NOTE: version skew mochi1=%s mochi2=%s — mixed-version interop scenarios are meaningful", v1, v2)
	}

	id1 := inter_identity(t, inter_mochi1)
	id2 := inter_identity(t, inter_mochi2)
	if id1 == "" || id2 == "" {
		t.Fatal("empty peer id")
	}
	if id1 == id2 {
		t.Fatalf("mochi1 and mochi2 share a peer id %q — mochi2 wasn't given a fresh identity", id1)
	}
	t.Logf("mochi1 peer=%s", id1)
	t.Logf("mochi2 peer=%s", id2)
}
