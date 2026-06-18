// Mochi server: active convergence audit (#29)
// Copyright Alistair Cunningham 2026
//
// Two complementary checks, both driven off replication_manager's hourly
// tick:
//
//   (a) Local stale-installed-code self-audit. Each host compares every
//       installed app's apps.db-claimed version against the highest
//       version directory actually on disk. apps.db's versions table is
//       REPLICATED metadata, so on a replica it can read the cluster's
//       current version (e.g. 3.100) while the host's own on-disk code is
//       frozen at an older release (3.95) — typically a restricted app
//       the publisher won't serve to a non-primary. A DB-only convergence
//       check sails right past this; this catches it with no peer round
//       trip, since the host already knows both numbers.
//
//   (b) Cross-host content audit. The pair leader asks each member for a
//       per-stream replicated-row-count manifest and compares. Lag is
//       filtered by quiescence rather than by trusting cursors: a stream's
//       count is compared ONLY when it is STABLE (unchanged since the
//       previous round) on BOTH hosts — an actively-replicating stream is
//       still moving and is skipped. A stable-but-unequal count is real
//       divergence, not lag. Host-local tables (journal, broadcast
//       bookkeeping) are excluded via audit_table_replicates so they
//       never register as a false divergence.

package main

import (
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
)

// ---------------------------------------------------------------------------
// (a) Local stale-installed-code self-audit
// ---------------------------------------------------------------------------

// stale_app_grace_seconds is how long an app may sit with its on-disk code
// behind the apps.db-claimed version before the self-audit alerts. Set past
// the apps_manager auto-update cycle (24h) so a freshly-deployed version the
// host simply hasn't pulled yet does NOT fire — only a genuinely stuck install
// (restricted app frozen on a replica, or broken auto-update) stays stale long
// enough to alert.
const stale_app_grace_seconds = 48 * 60 * 60

// StaleApp is an installed app whose apps.db-claimed version has no code
// directory on disk: this host cannot actually run what it claims to.
type StaleApp struct {
	App     string `json:"app"`
	Claimed string `json:"claimed"`
	OnDisk  string `json:"ondisk"` // highest version dir present, "" if none
	Since   int64  `json:"since"`  // first observed stale (0 until tracked)
}

// replication_stale_apps returns the installed apps whose apps.db-claimed
// version directory is absent on disk — the on-disk reality the replicated
// versions table can hide on a replica. Apps with no install directory under
// data_dir/apps (dev apps, or simply not installed here) are skipped: they are
// not a stale-install case.
func replication_stale_apps() []StaleApp {
	rows, err := db_apps().rows("select app, version from versions")
	if err != nil {
		return nil
	}
	var stale []StaleApp
	for _, r := range rows {
		app, _ := r["app"].(string)
		claimed, _ := r["version"].(string)
		if app == "" || claimed == "" {
			continue
		}
		dir := filepath.Join(data_dir, "apps", app)
		if info, err := os.Stat(dir); err != nil || !info.IsDir() {
			continue
		}
		if info, err := os.Stat(filepath.Join(dir, claimed)); err == nil && info.IsDir() {
			continue // claimed version present on disk — fine
		}
		stale = append(stale, StaleApp{App: app, Claimed: claimed, OnDisk: app_highest_version_dir(dir)})
	}
	sort.Slice(stale, func(i, j int) bool { return stale[i].App < stale[j].App })
	return stale
}

// app_highest_version_dir returns the highest version-numbered subdirectory of
// dir (numeric-aware, so 3.100 beats 3.95), or "" if there are none.
func app_highest_version_dir(dir string) string {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return ""
	}
	best := ""
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		n := e.Name()
		if n == "" || n[0] < '0' || n[0] > '9' {
			continue
		}
		if best == "" || version_greater(n, best) {
			best = n
		}
	}
	return best
}

var (
	stale_app_mutex   sync.Mutex
	stale_app_since   = map[string]int64{} // app -> first observed stale
	stale_app_alerted = map[string]bool{}  // app -> already alerted this episode
)

// replication_stale_apps_scan runs on the manager's hourly tick. It tracks how
// long each app has been stale and raises ONE de-duplicated admin alert per
// episode once an app has stayed stale past stale_app_grace_seconds — long
// enough to rule out a fresh deploy the host simply hasn't auto-updated to yet.
// An app dropping out of the stale set (it finally updated) clears its state so
// a later re-stall re-arms.
func replication_stale_apps_scan() {
	stale := replication_stale_apps()

	stale_app_mutex.Lock()
	defer stale_app_mutex.Unlock()

	present := make(map[string]bool, len(stale))
	for i := range stale {
		s := stale[i]
		present[s.App] = true
		if stale_app_since[s.App] == 0 {
			stale_app_since[s.App] = now()
		}
		if now()-stale_app_since[s.App] >= stale_app_grace_seconds && !stale_app_alerted[s.App] {
			stale_app_alerted[s.App] = true
			warn("Replication audit: app %q is running stale code — apps.db claims version %s but the highest version on disk is %q (stale %dh). The host cannot run the claimed version; a restricted app on a replica will not auto-update because the publisher refuses to serve its zip to non-primary hosts.",
				s.App, s.Claimed, s.OnDisk, (now()-stale_app_since[s.App])/3600)
		}
	}
	for app := range stale_app_since {
		if !present[app] {
			delete(stale_app_since, app)
			delete(stale_app_alerted, app)
		}
	}
}

// ---------------------------------------------------------------------------
// (b) Cross-host content audit
// ---------------------------------------------------------------------------

// audit_period_seconds throttles the cross-host audit well below the manager's
// hourly tick: it's a slow-moving safety net (the journal already prevents the
// main divergence cause), and a divergence is only confirmed across two
// consecutive rounds, so the effective confirm latency is ~2 periods.
const audit_period_seconds = 6 * 60 * 60

// AuditManifestRequest asks a pair member for its content manifest. Empty for
// now — the member returns every replicated per-user stream.
type AuditManifestRequest struct {
	Scope string `cbor:"scope,omitempty"`
}

// AuditStream is one stream's fingerprint: the (user, stream) key (same keying
// as the apply cursor and the tail, so two hosts' manifests line up), the total
// rows across the DB's REPLICATED tables (journal + broadcast bookkeeping
// excluded via audit_table_replicates, so equal applied ops ⇒ equal count), and
// Tail — this host's highest emitted sequence for the stream, 0 if it doesn't
// originate it. The receiver compares its cursor against the originator's Tail
// to detect a stream that has stopped advancing (see replication_audit_liveness).
type AuditStream struct {
	User   string `cbor:"user"`
	Stream string `cbor:"stream"`
	Count  int64  `cbor:"count"`
	Tail   int64  `cbor:"tail,omitempty"`
}

type AuditManifestResult struct {
	Streams []AuditStream `cbor:"streams"`
}

// replication_audit_local_manifest computes this host's per-stream content
// fingerprint across every per-user app DB.
func replication_audit_local_manifest() []AuditStream {
	entries, err := bootstrap_walk_db_manifest(bootstrap_scope_userdbs, "")
	if err != nil {
		return nil
	}
	out := make([]AuditStream, 0, len(entries))
	for _, e := range entries {
		rel := e.Path
		if rel == "" && e.User != "" && e.App != "" && e.DB != "" {
			rel = "users/" + e.User + "/" + e.App + "/db/" + e.DB
		}
		if rel == "" {
			continue
		}
		count := db_replicated_row_count(rel)
		if count < 0 {
			continue
		}
		stream := bootstrap_stream_key(rel)
		out = append(out, AuditStream{
			User:   e.User,
			Stream: stream,
			Count:  count,
			Tail:   replication_tail(e.User, repl_scope_app, stream),
		})
	}
	return out
}

// audit_local_tables are the core-created host-local infrastructure tables that
// may live inside an app DB but do NOT replicate: the replication journal and
// the broadcast bookkeeping (sender sequence/log, receiver received/acknowledged,
// the pending buffer, the commit log). Their contents legitimately differ per
// host, so the content audit must exclude them or every broadcast-using app
// would look permanently diverged. journal_table_replicates is NOT reused here:
// it gates the journal's write-replication decision (sql_default_excluded only
// lists sqlite_/_commit_log), not which existing tables hold replicated data.
var audit_local_tables = map[string]bool{
	"journal":      true,
	"_commit_log":  true,
	"sequence":     true,
	"received":     true,
	"log":          true,
	"acknowledged": true,
	"pending":      true,
}

// audit_table_replicates reports whether a table's rows are replicated content
// (so they belong in the cross-host count) rather than host-local bookkeeping.
func audit_table_replicates(name string) bool {
	return name != "" && !strings.HasPrefix(name, "sqlite_") && !audit_local_tables[name]
}

// db_replicated_row_count sums count(*) over the replicated tables of the DB at
// rel (host-local journal + broadcast bookkeeping excluded via
// audit_table_replicates). Returns -1 if the file is absent or unreadable so
// callers skip it rather than treat it as an empty (diverged) stream.
func db_replicated_row_count(rel string) int64 {
	full := filepath.Join(data_dir, filepath.FromSlash(rel))
	if info, err := os.Stat(full); err != nil || info.IsDir() {
		return -1
	}
	db := db_open(rel)
	if db == nil {
		return -1
	}
	tables, err := db.rows("select name from sqlite_master where type = 'table' and name not like 'sqlite_%'")
	if err != nil {
		return -1
	}
	var total int64
	for _, t := range tables {
		name, _ := t["name"].(string)
		if !audit_table_replicates(name) {
			continue
		}
		total += int64(db.integer("select count(*) from \"" + name + "\""))
	}
	return total
}

// replication_audit_request_manifest fetches a pair member's content manifest
// over a synchronous Net stream (same request/response shape as
// lookup/freshness — the response IS the ack, no queue involvement).
func replication_audit_request_manifest(peer string) ([]AuditStream, error) {
	s, err := stream_to_peer(peer, "", "", "replication", "audit/manifest", "", nil)
	if err != nil {
		return nil, err
	}
	defer s.close()
	if err := s.write(&AuditManifestRequest{}); err != nil {
		return nil, err
	}
	var res AuditManifestResult
	if err := s.read(&res); err != nil {
		return nil, err
	}
	return res.Streams, nil
}

// replication_audit_manifest_event answers a pair member's manifest request with
// this host's local manifest. Live stream only; a queued retry has no caller.
func replication_audit_manifest_event(e *Event) {
	if e.stream == nil {
		info("Replication audit-manifest: no stream (queued retry?) — dropping")
		return
	}
	res := &AuditManifestResult{Streams: replication_audit_local_manifest()}
	if err := e.stream.write(res); err != nil {
		warn("Replication audit-manifest: failed to write response: %v", err)
	}
}

func audit_manifest_map(streams []AuditStream) map[string]int64 {
	m := make(map[string]int64, len(streams))
	for _, s := range streams {
		m[s.User+"|"+s.Stream] = s.Count
	}
	return m
}

var (
	audit_convergence_mutex sync.Mutex
	audit_last              int64
	audit_previous          = map[string]int64{}            // this host, previous round
	audit_peer_previous     = map[string]map[string]int64{} // peer -> previous round's counts
	audit_alerted           = map[string]bool{}             // "peer|user|stream" -> divergence-alerted
	audit_cursor_previous   = map[string]int64{}            // "peer|user|stream" -> apply cursor at previous round
	audit_liveness_alerted  = map[string]bool{}             // "peer|user|stream" -> not-advancing-alerted
)

// replication_convergence_audit runs on the manager tick but throttles itself to
// audit_period_seconds. For each pair member it fetches a manifest and runs two
// checks. (1) Liveness (both sides, every round): is this host's apply cursor
// keeping up with the peer's emitted tail? — see replication_audit_liveness.
// (2) Content divergence (leader-gated so it emails once): compares row counts,
// but ONLY for streams whose count is STABLE on BOTH hosts since the previous
// round — an actively-replicating stream is still moving and is skipped, while a
// stable-but-unequal count is real divergence, not lag.
func replication_convergence_audit() {
	if now()-audit_last < audit_period_seconds {
		return
	}
	audit_last = now()

	rdb := db_open("db/replication.db")
	members, err := rdb.rows("select peer from pair")
	if err != nil || len(members) == 0 {
		return
	}

	local := audit_manifest_map(replication_audit_local_manifest())
	audit_convergence_mutex.Lock()
	prevLocal := audit_previous
	audit_previous = local
	audit_convergence_mutex.Unlock()

	for _, m := range members {
		peer, _ := m["peer"].(string)
		if peer == "" {
			continue
		}
		remote, err := replication_audit_request_manifest(peer)
		if err != nil {
			info("Replication audit: manifest fetch from %q failed: %v", peer, err)
			continue
		}

		// Liveness runs on BOTH sides (no leader gate): each host audits its OWN
		// receive direction against the peer's emitted tail, so a replica that
		// has silently stopped catching up to the primary — the case the stall
		// alert can't see — is actually covered.
		replication_audit_liveness(peer, remote)

		// The content-divergence compare is leader-gated so one divergence emails
		// the admin once, not from both members.
		if !replication_leader_claim("audit", peer, false) {
			continue
		}
		remoteMap := audit_manifest_map(remote)

		audit_convergence_mutex.Lock()
		prevRemote := audit_peer_previous[peer]
		audit_peer_previous[peer] = remoteMap
		audit_convergence_mutex.Unlock()

		replication_audit_compare(peer, local, prevLocal, remoteMap, prevRemote)
	}
}

// replication_audit_liveness flags a stream this host RECEIVES from `peer` whose
// apply cursor is below the peer's emitted tail AND has not advanced since the
// previous round — the host should be catching up but isn't. This closes the gap
// neither other check sees: a stream that silently stops with no pending buffer
// (so the stall alert #3 is blind) while the peer keeps emitting (so the content
// audit's stability gate skips it). A stream that is behind but still advancing
// is just lag and does not alert; confirmation needs two rounds (the first
// sighting only records the cursor), and the alert re-arms once the stream
// catches up or resumes progress.
func replication_audit_liveness(peer string, remote []AuditStream) {
	rdb := db_open("db/replication.db")
	audit_convergence_mutex.Lock()
	defer audit_convergence_mutex.Unlock()
	stuck := map[string]bool{}
	for _, s := range remote {
		if s.Tail <= 0 {
			continue // peer doesn't originate this stream — nothing to keep up with
		}
		key := peer + "|" + s.User + "|" + s.Stream
		cursor, _ := replication_cursor(rdb, peer, repl_scope_app, s.User, s.Stream)
		prev, seen := audit_cursor_previous[key]
		audit_cursor_previous[key] = cursor
		if cursor >= s.Tail {
			continue // caught up to the peer's tail
		}
		if !seen || cursor != prev {
			continue // first sighting, or still advancing — lag, not stuck
		}
		stuck[key] = true
		if !audit_liveness_alerted[key] {
			audit_liveness_alerted[key] = true
			warn("Replication stream not advancing: user=%q stream=%q from peer=%q — apply cursor=%d is stuck below the peer's emitted tail=%d (behind %d ops, no progress since the last audit round, and no pending buffer to trip the stall alert). Investigate; a targeted re-seed (mochictl replication reseed) may recover it.",
				s.User, s.Stream, peer, cursor, s.Tail, s.Tail-cursor)
		}
	}
	for key := range audit_liveness_alerted {
		if strings.HasPrefix(key, peer+"|") && !stuck[key] {
			delete(audit_liveness_alerted, key)
		}
	}
}

// replication_audit_compare flags streams that both hosts hold at a STABLE but
// UNEQUAL count (real divergence), de-duplicated to one alert per episode and
// re-armed once a stream converges or starts moving again. Comparison needs the
// previous round, so the first round after start (prev maps empty) never alerts.
func replication_audit_compare(peer string, local, prevLocal, remote, prevRemote map[string]int64) {
	audit_convergence_mutex.Lock()
	defer audit_convergence_mutex.Unlock()
	diverged := map[string]bool{}
	for key, lc := range local {
		rc, ok := remote[key]
		if !ok {
			continue // stream absent on peer — could be bootstrap lag, not compared
		}
		pl, okl := prevLocal[key]
		pr, okr := prevRemote[key]
		if !okl || !okr || pl != lc || pr != rc {
			continue // not stable on both sides since last round — still settling
		}
		if lc == rc {
			continue // converged
		}
		akey := peer + "|" + key
		diverged[akey] = true
		if !audit_alerted[akey] {
			audit_alerted[akey] = true
			warn("Replication audit: stream %q diverged from peer %q — local rows=%d, peer rows=%d, both stable since last round (not lag). Investigate; a targeted re-seed (mochictl replication reseed) can recover the lagging side.",
				key, peer, lc, rc)
		}
	}
	for akey := range audit_alerted {
		if strings.HasPrefix(akey, peer+"|") && !diverged[akey] {
			delete(audit_alerted, akey)
		}
	}
}

// replication_audit_divergences returns the currently-alerted divergence keys
// ("peer|user|stream") for the status endpoint.
func replication_audit_divergences() []string {
	audit_convergence_mutex.Lock()
	defer audit_convergence_mutex.Unlock()
	out := make([]string, 0, len(audit_alerted))
	for k := range audit_alerted {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// replication_audit_stuck_streams returns the streams currently alerted as not
// advancing — apply cursor stuck below the peer's emitted tail — for the status
// endpoint.
func replication_audit_stuck_streams() []string {
	audit_convergence_mutex.Lock()
	defer audit_convergence_mutex.Unlock()
	out := make([]string, 0, len(audit_liveness_alerted))
	for k := range audit_liveness_alerted {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
