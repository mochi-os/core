// Mochi server: active convergence audit (#29)
// Copyright © 2026 Mochi OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.
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
//       per-stream manifest (replicated-row COUNT + an order-independent
//       content HASH) and compares. Lag is filtered by quiescence rather
//       than by trusting cursors: a stream is compared ONLY when it is
//       STABLE (unchanged since the previous round) on BOTH hosts — an
//       actively-replicating stream is still moving and is skipped. A
//       stable-but-unequal count is real divergence, not lag. The content
//       hash catches the case the count cannot: counts MATCH but the row
//       CONTENT differs — the dropped-UPDATE class (e.g. UPDATEs lost after
//       a replica reset, where row counts stay identical). Host-local tables
//       (journal, broadcast bookkeeping) are excluded via
//       audit_table_replicates so they never register as a false divergence.

package main

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
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

// audit_critical_period_seconds is the fast cadence for the auth-critical
// liveness pass (#41). core:user (login methods / identities) and system:users
// (roles, account status) must reach a failover host promptly; waiting on the
// 6h content audit means up to ~12h to detect+heal a stuck stream, far too long
// for credential / authorization state. This dedicated pass runs liveness +
// reanchor for only those streams every 5 min (~2 rounds → ~10 min confirm).
const audit_critical_period_seconds = 5 * 60

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

// AuditKey identifies one stream for the on-demand content-hash exchange.
type AuditKey struct {
	User   string `cbor:"user"`
	Stream string `cbor:"stream"`
}

// AuditHashRequest asks a peer for the content hashes of specific streams — only
// the ones whose row COUNTS already match, where a dropped-UPDATE could hide.
// Sending that subset (not every stream) is what keeps the response cheap: the
// expensive full-content hash never blocks the manifest exchange.
type AuditHashRequest struct {
	Keys []AuditKey `cbor:"keys"`
}

// AuditHashEntry / AuditHashResult carry the answer: one content hash per stream.
type AuditHashEntry struct {
	User   string `cbor:"user"`
	Stream string `cbor:"stream"`
	Hash   string `cbor:"hash"`
}

type AuditHashResult struct {
	Hashes []AuditHashEntry `cbor:"hashes"`
}

// audit_hash_cache memoises db_replicated_content_hash per DB, keyed by a cheap
// change fingerprint, so an UNCHANGED DB is never re-hashed. Between audit rounds
// almost no DB changes, so almost every lookup is a cache hit — this is what makes
// hashing cheap enough to compute on demand instead of eagerly in the manifest (#48).
var (
	audit_hash_mutex sync.Mutex
	audit_hash_cache = map[string]auditHashCacheEntry{}
)

type auditHashCacheEntry struct {
	fingerprint string
	hash        string
}

// audit_db_fingerprint is a cheap change-indicator for a DB: the mtime+size of the
// main file AND of its -wal sidecar. SQLite runs in WAL mode (db.go), so writes
// land in <db>-wal and DON'T bump the main file's mtime until a checkpoint —
// keying on the main file alone would serve a stale hash and miss real changes.
// "" if the DB is absent.
func audit_db_fingerprint(rel string) string {
	full := filepath.Join(data_dir, filepath.FromSlash(rel))
	st, err := os.Stat(full)
	if err != nil || st.IsDir() {
		return ""
	}
	fp := fmt.Sprintf("%d:%d", st.ModTime().UnixNano(), st.Size())
	if w, err := os.Stat(full + "-wal"); err == nil {
		fp += fmt.Sprintf(":%d:%d", w.ModTime().UnixNano(), w.Size())
	}
	return fp
}

// replication_audit_content_hash returns rel's content hash, recomputing only when
// the DB (main file or -wal) has changed since the last hash. "" if unreadable.
func replication_audit_content_hash(rel string) string {
	fp := audit_db_fingerprint(rel)
	if fp == "" {
		return ""
	}
	audit_hash_mutex.Lock()
	e, ok := audit_hash_cache[rel]
	audit_hash_mutex.Unlock()
	if ok && e.fingerprint == fp {
		return e.hash
	}
	h := db_replicated_content_hash(rel)
	audit_hash_mutex.Lock()
	audit_hash_cache[rel] = auditHashCacheEntry{fingerprint: fp, hash: h}
	audit_hash_mutex.Unlock()
	return h
}

// audit_entry_rel resolves a DB-manifest entry to its relative path under data_dir.
func audit_entry_rel(e BootstrapDBEntry) string {
	rel := e.Path
	if rel == "" && e.User != "" && e.App != "" && e.DB != "" {
		rel = "users/" + e.User + "/" + e.App + "/db/" + e.DB
	}
	return rel
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
		rel := audit_entry_rel(e)
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

// replication_audit_hashes computes this host's content hash for each requested
// (user, stream) — used both to answer a peer's audit/hash request and to hash the
// local side of the same comparison. Walks the cheap DB manifest and hashes only
// the requested streams via the mtime cache, so unchanged DBs aren't re-hashed.
func replication_audit_hashes(keys []AuditKey) []AuditHashEntry {
	if len(keys) == 0 {
		return nil
	}
	want := make(map[string]bool, len(keys))
	for _, k := range keys {
		want[k.User+"|"+k.Stream] = true
	}
	entries, err := bootstrap_walk_db_manifest(bootstrap_scope_userdbs, "")
	if err != nil {
		return nil
	}
	out := make([]AuditHashEntry, 0, len(keys))
	for _, e := range entries {
		rel := audit_entry_rel(e)
		if rel == "" {
			continue
		}
		stream := bootstrap_stream_key(rel)
		if !want[e.User+"|"+stream] {
			continue
		}
		out = append(out, AuditHashEntry{User: e.User, Stream: stream, Hash: replication_audit_content_hash(rel)})
	}
	return out
}

// audit_content_candidates returns the streams present on both hosts with EQUAL
// row counts — the only ones a content (dropped-UPDATE) divergence can hide in, so
// the only ones worth fetching content hashes for.
func audit_content_candidates(local, remote map[string]int64) []AuditKey {
	var out []AuditKey
	for key, lc := range local {
		if rc, ok := remote[key]; ok && rc == lc {
			if parts := strings.SplitN(key, "|", 2); len(parts) == 2 {
				out = append(out, AuditKey{User: parts[0], Stream: parts[1]})
			}
		}
	}
	return out
}

// audit_hash_map keys content-hash entries by "user|stream" for comparison.
func audit_hash_map(entries []AuditHashEntry) map[string]string {
	m := make(map[string]string, len(entries))
	for _, e := range entries {
		m[e.User+"|"+e.Stream] = e.Hash
	}
	return m
}

// audit_local_tables are the core-created host-local infrastructure tables that
// may live inside an app DB but do NOT replicate: the replication journal and
// the broadcast bookkeeping (sender sequence/log, receiver received/acknowledged,
// the pending buffer, the commit log). Their contents legitimately differ per
// host, so the content audit must exclude them or every broadcast-using app
// would look permanently diverged. journal_table_replicates is NOT reused here:
// it gates the journal's write-replication decision (sql_default_excluded lists
// only sqlite_), not which existing tables hold replicated data.
// audit_local_tables are the tables that live inside a replicated DB but are
// maintained per host, so they don't belong in the cross-host count: the journal
// and broadcast bookkeeping (sender sequence/log, receiver received/acknowledged,
// pending buffer, commit log), plus email_delivered — which IS replicated but is
// pruned per host on a TTL, so its count wobbles transiently between hosts and
// isn't worth a divergence alert. Host-local infra tables that live inside a
// replicated app DB must be listed by name here: `commits` (the commit-hook
// pending-fire log) and `idempotency` (the per-app idempotent-call cache),
// both in app.db, are host-local, so their counts would otherwise read as
// cross-host divergence.
// audit_local_tables_core are the CORE-created host-local infrastructure tables
// that may live inside any app DB but never hold replicated content: the
// replication journal, the broadcast bookkeeping (sender sequence/log, receiver
// received/acknowledged, the pending buffer), the commit-hook fire log
// (`commits`), the idempotent-call cache (`idempotency`), and email_delivered
// (replicated but TTL-pruned per host, so its count wobbles). These are present
// in many DBs and aren't app-declared, so they stay built-in. App-specific
// host-local tables (caches like feeds' post_scores/score_cache/poll_locks) are
// NOT listed here — they come from each app's database.replicate.exclude.tables,
// merged per-DB by audit_excludes_for_path (see
// claude/plans/audit-host-local-columns.md).
var audit_local_tables_core = map[string]bool{
	"journal":         true,
	"commits":         true,
	"sequence":        true,
	"received":        true,
	"log":             true,
	"acknowledged":    true,
	"pending":         true,
	"email_delivered": true,
	"idempotency":     true,
}

// audit_local_columns_core lists, per table, host-LOCAL columns in CORE DBs
// (user.db and the like) that legitimately differ per host so the content hash
// must skip them. App data-DB columns are NOT listed here — they come from each
// app's database.replicate.exclude.columns, resolved per-DB by
// audit_excludes_for_path, which keys by the owning app so two apps' same-named
// tables can't collide.
var audit_local_columns_core = map[string]map[string]bool{
	"accounts":    {"last_delivered": true}, // user.db: per-host notification timestamp
	"attachments": {"entity": true},         // app.db: per-host owned("")-vs-foreign(<source>) pointer
}

// audit_app_id_from_path returns the app id owning the data DB at a
// replication-relative path of the form users/<user>/<app>/db/<file>, or "" for
// core DBs (users/<user>/user.db) and app system DBs (users/<user>/<app>/app.db).
// App declarations describe only the app's own data DB; app.db is core-managed.
func audit_app_id_from_path(rel string) string {
	parts := strings.Split(rel, "/")
	if len(parts) == 5 && parts[0] == "users" && parts[3] == "db" {
		return parts[2]
	}
	return ""
}

// audit_app_excludes returns the host-local tables and per-table columns an app
// declares in its app.json (database.replicate.exclude). Reads the app's active
// version; the declaration is schema-stable so the exact version doesn't matter.
// Returns nil maps for an unknown/unloaded app.
func audit_app_excludes(app_id string) (tables map[string]bool, columns map[string]map[string]bool) {
	a := app_by_id(app_id)
	if a == nil {
		return nil, nil
	}
	av := a.active(nil)
	if av == nil {
		return nil, nil
	}
	if len(av.Database.Replicate.Exclude.Tables) > 0 {
		tables = map[string]bool{}
		for _, t := range av.Database.Replicate.Exclude.Tables {
			tables[t] = true
		}
	}
	if len(av.Database.Replicate.Exclude.Columns) > 0 {
		columns = map[string]map[string]bool{}
		for table, cols := range av.Database.Replicate.Exclude.Columns {
			m := map[string]bool{}
			for _, c := range cols {
				m[c] = true
			}
			columns[table] = m
		}
	}
	return tables, columns
}

// audit_excludes_for_path returns the host-local exclude-set for the DB at a
// replication-relative path: the always-excluded core infra tables, plus — for
// an app's data DB — that app's declared exclude tables and columns; for a core
// or app-system DB, the core host-local columns. Keying app declarations by the
// owning app (not a global table name) is what lets two apps' same-named tables
// carry different exclusions without colliding.
func audit_excludes_for_path(rel string) (tables map[string]bool, columns map[string]map[string]bool) {
	tables = map[string]bool{}
	for t := range audit_local_tables_core {
		tables[t] = true
	}
	if app_id := audit_app_id_from_path(rel); app_id != "" {
		appTables, appColumns := audit_app_excludes(app_id)
		for t := range appTables {
			tables[t] = true
		}
		columns = appColumns
		return tables, columns
	}
	// Core / app-system DB: the core host-local columns (e.g. user.db accounts).
	columns = audit_local_columns_core
	return tables, columns
}

// audit_table_replicates reports whether a table's rows are replicated content
// (so they belong in the cross-host count) rather than host-local bookkeeping,
// given the DB's resolved local-table set from audit_excludes_for_path.
func audit_table_replicates(name string, local map[string]bool) bool {
	return name != "" && !strings.HasPrefix(name, "sqlite_") && !local[name]
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
	localTables, _ := audit_excludes_for_path(rel)
	var total int64
	for _, t := range tables {
		name, _ := t["name"].(string)
		if !audit_table_replicates(name, localTables) {
			continue
		}
		total += int64(db.integer("select count(*) from \"" + name + "\""))
	}
	return total
}

// db_replicated_content_hash is the content analogue of db_replicated_row_count:
// an order-independent fingerprint of the actual ROW CONTENT across the DB's
// replicated tables, so two hosts with equal row counts but diverged row values
// (the dropped-UPDATE class — invisible to count(*)) produce different hashes.
// Per table, each row is hashed (audit_row_hash) and the row hashes are XORed
// together, so a differing PHYSICAL (rowid) order between hosts doesn't matter —
// only the set of row contents does. Table hashes are folded in name order.
// Returns "" if the file is absent/unreadable so callers skip it rather than
// treat it as diverged. Cost: a full scan of every replicated table — acceptable
// because the convergence audit runs on a multi-hour period (audit_period_seconds).
func db_replicated_content_hash(rel string) string {
	full := filepath.Join(data_dir, filepath.FromSlash(rel))
	if info, err := os.Stat(full); err != nil || info.IsDir() {
		return ""
	}
	db := db_open(rel)
	if db == nil {
		return ""
	}
	tables, err := db.rows("select name from sqlite_master where type = 'table' and name not like 'sqlite_%' order by name")
	if err != nil {
		return ""
	}
	localTables, localColumns := audit_excludes_for_path(rel)
	outer := sha256.New()
	for _, t := range tables {
		name, _ := t["name"].(string)
		if !audit_table_replicates(name, localTables) {
			continue
		}
		rows, err := db.rows("select * from \"" + name + "\"")
		if err != nil {
			return ""
		}
		exclude := localColumns[name]
		var acc [sha256.Size]byte
		for _, r := range rows {
			rh := audit_row_hash(r, exclude)
			for i := range acc {
				acc[i] ^= rh[i]
			}
		}
		outer.Write([]byte(name))
		outer.Write(acc[:])
	}
	return hex.EncodeToString(outer.Sum(nil))
}

// audit_row_hash hashes one row deterministically: columns in sorted-name order,
// each value written with a type tag so an int 0, "0", and NULL stay distinct.
// Both hosts produce the same hash for the same logical row regardless of column
// map iteration order. Columns in `exclude` are skipped — host-local columns
// (computed scores, per-host timestamps) that legitimately differ per host and
// must not register as content divergence.
func audit_row_hash(r map[string]any, exclude map[string]bool) [sha256.Size]byte {
	keys := make([]string, 0, len(r))
	for k := range r {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	h := sha256.New()
	for _, k := range keys {
		if exclude[k] {
			continue
		}
		h.Write([]byte(k))
		h.Write([]byte{0})
		switch v := r[k].(type) {
		case nil:
			h.Write([]byte{'N'})
		case int64:
			h.Write([]byte{'I'})
			h.Write([]byte(strconv.FormatInt(v, 10)))
		case float64:
			h.Write([]byte{'F'})
			h.Write([]byte(strconv.FormatFloat(v, 'g', -1, 64)))
		case string:
			h.Write([]byte{'S'})
			h.Write([]byte(v))
		case []byte:
			h.Write([]byte{'B'})
			h.Write(v)
		default:
			h.Write([]byte{'?'})
			h.Write([]byte(fmt.Sprint(v)))
		}
		h.Write([]byte{0})
	}
	var out [sha256.Size]byte
	copy(out[:], h.Sum(nil))
	return out
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

// replication_audit_request_hashes fetches a pair member's content hashes for the
// given (count-matching) streams over a synchronous Net stream — same shape as the
// manifest request, but the response carries only the requested subset.
func replication_audit_request_hashes(peer string, keys []AuditKey) (map[string]string, error) {
	s, err := stream_to_peer(peer, "", "", "replication", "audit/hash", "", nil)
	if err != nil {
		return nil, err
	}
	defer s.close()
	if err := s.write(&AuditHashRequest{Keys: keys}); err != nil {
		return nil, err
	}
	var res AuditHashResult
	if err := s.read(&res); err != nil {
		return nil, err
	}
	return audit_hash_map(res.Hashes), nil
}

// replication_audit_hash_event answers a pair member's request for the content
// hashes of specific streams. Live stream only; a queued retry has no caller. The
// write failure is info, not warn — a missed hash exchange is a transient transport
// hiccup the audit re-fetches next round, so it must not email; the divergence
// itself (once a hash is obtained) is what alerts (#47).
func replication_audit_hash_event(e *Event) {
	if e.stream == nil {
		return
	}
	var req AuditHashRequest
	if err := e.stream.read(&req); err != nil {
		return
	}
	res := &AuditHashResult{Hashes: replication_audit_hashes(req.Keys)}
	if err := e.stream.write(res); err != nil {
		info("Replication audit-hash: failed to write response: %v", err)
	}
}

var (
	audit_convergence_mutex  sync.Mutex
	audit_last               int64
	audit_previous           = map[string]int64{}             // this host, previous round
	audit_peer_previous      = map[string]map[string]int64{}  // peer -> previous round's counts
	audit_alerted            = map[string]bool{}              // "peer|user|stream" -> count-divergence-alerted
	audit_cursor_previous    = map[string]int64{}             // "peer|user|stream" -> apply cursor at previous round
	audit_liveness_alerted   = map[string]bool{}              // "peer|user|stream" -> not-advancing-alerted
	audit_previous_hash      = map[string]string{}            // this host, previous round's content hashes
	audit_peer_previous_hash = map[string]map[string]string{} // peer -> previous round's content hashes
	audit_content_alerted    = map[string]bool{}              // "peer|user|stream" -> content-divergence-alerted
)

// #41: separate state for the fast auth-critical liveness pass, kept apart from
// the slow audit's previous-cursor map so the two cadences don't corrupt each
// other's two-round freeze detection.
var (
	audit_critical_last            int64
	audit_critical_cursor_previous = map[string]int64{}
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

	localStreams := replication_audit_local_manifest()
	local := audit_manifest_map(localStreams)
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
		replication_audit_liveness(peer, local, remote, false)

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

		// Content compare: only streams whose row COUNTS already match can hide a
		// dropped-UPDATE, so fetch content hashes for just that subset (cheap and
		// mtime-cached on both sides) rather than hashing every stream every round.
		candidates := audit_content_candidates(local, remoteMap)
		if len(candidates) == 0 {
			continue
		}
		localHash := audit_hash_map(replication_audit_hashes(candidates))
		remoteHash, err := replication_audit_request_hashes(peer, candidates)
		if err != nil {
			info("Replication audit: hash fetch from %q failed: %v", peer, err)
			continue
		}
		audit_convergence_mutex.Lock()
		prevLocalHash := audit_previous_hash
		audit_previous_hash = localHash
		prevRemoteHash := audit_peer_previous_hash[peer]
		audit_peer_previous_hash[peer] = remoteHash
		audit_convergence_mutex.Unlock()

		replication_audit_content_compare(peer, localHash, prevLocalHash, remoteHash, prevRemoteHash, local, remoteMap)
	}
}

// replication_convergence_audit_critical runs ONLY the liveness check, and only
// for the auth-critical streams, on a fast 5-min cadence (#41). It fetches the
// cheap manifest (no content hashing) from each pair member and lets the shared
// liveness logic detect + reanchor a stuck core:user / system:users stream, so
// credential and authorization changes on the primary reach a failover host in
// ~10 min instead of the content audit's ~12h. The slow audit (false) skips
// these streams, so each owns a disjoint set.
func replication_convergence_audit_critical() {
	if now()-audit_critical_last < audit_critical_period_seconds {
		return
	}
	audit_critical_last = now()

	rdb := db_open("db/replication.db")
	members, err := rdb.rows("select peer from pair")
	if err != nil || len(members) == 0 {
		return
	}

	local := audit_manifest_map(replication_audit_local_manifest())
	for _, m := range members {
		peer, _ := m["peer"].(string)
		if peer == "" {
			continue
		}
		remote, err := replication_audit_request_manifest(peer)
		if err != nil {
			info("Replication critical-stream audit: manifest fetch from %q failed: %v", peer, err)
			continue
		}
		replication_audit_liveness(peer, local, remote, true)
	}
}

// audit_stream_is_critical reports whether a stream carries auth-critical state
// that must reach a failover host fast (#41): core:user (per-user login methods
// and identities) and system:users (server user table — roles, account status).
// A lag there means the failover serves stale credentials or authorization.
// Sessions and schedule are self-healing (a failover just re-authenticates), so
// they stay on the slow audit.
func audit_stream_is_critical(stream string) bool {
	return stream == repl_stream_key(repl_stream_class_core, "user") ||
		stream == repl_stream_key(repl_stream_class_system, "users")
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
//
// A frozen cursor is NOT alerted when the stream's CONTENT is already converged
// (audit_stream_converged): in a multi-host set the same rows can arrive via a
// different peer or via local writes, leaving this per-peer cursor permanently
// behind the emitted tail with no data missing — a false positive. Only a genuine
// gap (row-count mismatch) or a dropped UPDATE (count match, content-hash
// mismatch) still alerts.
func replication_audit_liveness(peer string, local map[string]int64, remote []AuditStream, critical bool) {
	rdb := db_open("db/replication.db")

	// #41: the fast auth-critical pass (critical=true) and the slow 6h pass
	// (critical=false) each own a disjoint set of streams and keep separate
	// previous-cursor state, so the auth streams get a 5-min cadence without the
	// two passes corrupting each other's two-round freeze detection.
	prevMap := audit_cursor_previous
	if critical {
		prevMap = audit_critical_cursor_previous
	}

	// Phase 1 (locked): snapshot this round's cursors and collect the streams
	// frozen below the peer's tail. The convergence check in phase 2 does a P2P
	// hash fetch, so it must run OUTSIDE audit_convergence_mutex — never hold the
	// lock across network I/O.
	type frozenStream struct {
		key    string
		s      AuditStream
		cursor int64
	}
	var frozen []frozenStream
	audit_convergence_mutex.Lock()
	for _, s := range remote {
		if s.Tail <= 0 {
			continue // peer doesn't originate this stream — nothing to keep up with
		}
		if audit_stream_is_critical(s.Stream) != critical {
			continue // each pass owns only its stream set (#41)
		}
		// This host ORIGINATES the stream (it has its own emitted tail), so the
		// peer's emitted tail is largely our own ops relayed back as echo — our
		// per-peer apply cursor sits permanently behind it with nothing missing.
		// cursor-vs-tail is not a meaningful liveness signal for an originator
		// (convergence is covered by the content audit, replication_audit_compare),
		// so don't let such a stream become a permanent frozen candidate that
		// re-alerts whenever the phase-2 hash fetch flaps. This is the home-host
		// cursor=0 false positive seen on the chat stream in task #60. (#62)
		if rdb.integer("select coalesce(last, 0) from tail where user=? and scope=? and db=?", s.User, repl_scope_app, s.Stream) > 0 {
			continue
		}
		key := peer + "|" + s.User + "|" + s.Stream
		cursor, _ := replication_cursor(rdb, peer, repl_scope_app, s.User, s.Stream)
		prev, seen := prevMap[key]
		prevMap[key] = cursor
		if cursor >= s.Tail {
			continue // caught up to the peer's tail
		}
		if !seen || cursor != prev {
			continue // first sighting, or still advancing — lag, not stuck
		}
		frozen = append(frozen, frozenStream{key, s, cursor})
	}
	audit_convergence_mutex.Unlock()

	// Phase 2 (unlocked): drop frozen streams whose content already matches the
	// peer (the multi-host false positive). What remains is genuinely stuck.
	stuck := map[string]bool{}
	var alert []frozenStream
	for _, f := range frozen {
		if audit_stream_converged(peer, f.s, local) {
			continue // content matches — not a real gap; leave out of `stuck` so any prior alert clears
		}
		stuck[f.key] = true
		alert = append(alert, f)
	}

	// Phase 3 (locked): one alert per episode for the genuinely stuck streams, and
	// re-arm any whose alert no longer applies (converged, caught up, or resumed).
	audit_convergence_mutex.Lock()
	for _, f := range alert {
		if !audit_liveness_alerted[f.key] {
			audit_liveness_alerted[f.key] = true
			warn("Replication stream not advancing: user=%q stream=%q from peer=%q — apply cursor=%d is stuck below the peer's emitted tail=%d (behind %d ops, no progress since the last audit round, no pending buffer: the missing-cursor / frozen-cursor class, #33). Auto-recovery will attempt a targeted re-seed; if it persists, run the /replication-audit plumbing pass for the remedy.",
				f.s.User, f.s.Stream, peer, f.cursor, f.s.Tail, f.s.Tail-f.cursor)
		}
	}
	for key := range audit_liveness_alerted {
		if !strings.HasPrefix(key, peer+"|") || stuck[key] {
			continue
		}
		// Re-arm only streams THIS pass owns: the fast and slow passes share the
		// alerted map, so without this filter one pass would clear the other's
		// alerts for streams it never examined this round (#41).
		if audit_stream_is_critical(key[strings.LastIndex(key, "|")+1:]) != critical {
			continue
		}
		delete(audit_liveness_alerted, key)
	}
	audit_convergence_mutex.Unlock()

	// #33: the dormant cursor-misalignment class — a pure-receiver stream
	// (Phase-1 guard above restricts this to our emitted tail==0) frozen below
	// the peer's tail with NO pending buffer, so replication_wiped_rebootstrap
	// never sees it (it only scans buffered-pending streams). Auto-recover with
	// the SAME safe gated reseed used for the pending class: reseed_source_
	// missing_ops re-confirms the source is authoritative, and the reanchor
	// backoff/attempt-cap paces it and escalates to irreparable if it won't
	// anchor. Run unlocked — the reseed opens DBs and must not hold the audit
	// convergence lock.
	for _, f := range alert {
		replication_reanchor_misaligned(StalledStream{
			Peer: peer, Scope: repl_scope_app, User: f.s.User, Database: f.s.Stream,
		})
	}
}

// audit_stream_converged reports whether a frozen-cursor stream's content already
// matches the peer's, despite the apply cursor lagging the emitted tail — the
// multi-host case where the same rows arrived via another peer or via local
// writes, so this per-peer cursor sits permanently behind with NO data missing.
// Only then is the "not advancing" alert a false positive and safe to suppress. A
// row-count mismatch is a genuine gap; a count match with a differing content hash
// is a dropped UPDATE — both return false so both still alert. A var so tests can
// stub the verdict without a live peer.
var audit_stream_converged = func(peer string, s AuditStream, local map[string]int64) bool {
	lc, ok := local[s.User+"|"+s.Stream]
	if !ok || lc != s.Count {
		return false // missing locally or row counts differ — a real gap
	}
	key := AuditKey{User: s.User, Stream: s.Stream}
	localHashes := replication_audit_hashes([]AuditKey{key})
	if len(localHashes) == 0 || localHashes[0].Hash == "" {
		return false // can't hash locally — don't suppress
	}
	remoteHashes, err := replication_audit_request_hashes(peer, []AuditKey{key})
	if err != nil {
		return false // couldn't fetch the peer's hash — don't suppress
	}
	return remoteHashes[s.User+"|"+s.Stream] == localHashes[0].Hash
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

// replication_audit_content_compare is the content analogue of
// replication_audit_compare. It alerts when a stream's row COUNTS match (so the
// count compare stays silent) but its content HASHES differ AND both hashes have
// been stable since the previous round on both hosts — the dropped-UPDATE
// divergence class that equal counts hide (e.g. a replica reset that silently
// dropped UPDATEs, where row counts stayed identical but values diverged).
// Count-diverging streams are left to replication_audit_compare; a hash still
// changing on either side (mid content-replication) is treated as lag. The
// re-arm sweep clears a content alert once the hashes converge.
func replication_audit_content_compare(peer string, localHash, prevLocalHash, remoteHash, prevRemoteHash map[string]string, localCount, remoteCount map[string]int64) {
	audit_convergence_mutex.Lock()
	defer audit_convergence_mutex.Unlock()
	diverged := map[string]bool{}
	for key, lh := range localHash {
		if lh == "" {
			continue // we couldn't hash it (absent/unreadable) — skip
		}
		rh, ok := remoteHash[key]
		if !ok || rh == "" {
			continue // peer didn't hash it — can't compare
		}
		if localCount[key] != remoteCount[key] {
			continue // count diverges — replication_audit_compare owns this one
		}
		pl, okl := prevLocalHash[key]
		pr, okr := prevRemoteHash[key]
		if !okl || !okr || pl != lh || pr != rh {
			continue // content still settling on one side — lag, not divergence
		}
		if lh == rh {
			continue // content converged
		}
		akey := peer + "|" + key
		diverged[akey] = true
		if !audit_content_alerted[akey] {
			audit_content_alerted[akey] = true
			// Alerting (warn): emails and degrades /_/health. The content audit is now
			// column-aware (#45) and every host-LOCAL table/column is excluded via app
			// declarations — database.replicate.exclude.{tables,columns}, resolved per
			// DB by audit_excludes_for_path (#47) — plus the core host-local set. So a
			// stable content mismatch on EQUAL row counts is a real dropped-UPDATE
			// divergence (e.g. UPDATEs lost after a replica reset), not a per-host
			// computed value. If a genuinely new false positive appears, an app has an
			// UNDECLARED host-local column: declare it in
			// database.replicate.exclude.columns rather than re-muting this alert.
			warn("Replication audit: stream %q content-diverged from peer %q — row counts MATCH (%d rows) but the replicated row CONTENT differs, stable on both sides since the last round (not lag). Host-local columns/tables are excluded via app declarations (#45/#47), so this is the dropped-UPDATE class a count audit can't see. Investigate; a targeted re-seed (mochictl replication reseed) converges it.",
				key, peer, localCount[key])
		}
	}
	for akey := range audit_content_alerted {
		if strings.HasPrefix(akey, peer+"|") && !diverged[akey] {
			delete(audit_content_alerted, akey)
		}
	}
}

// replication_audit_divergences returns the currently-alerted divergence keys
// ("peer|user|stream") for the status endpoint.
func replication_audit_divergences() []string {
	audit_convergence_mutex.Lock()
	defer audit_convergence_mutex.Unlock()
	out := make([]string, 0, len(audit_alerted)+len(audit_content_alerted))
	for k := range audit_alerted {
		out = append(out, k)
	}
	for k := range audit_content_alerted {
		out = append(out, k+" (content)")
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

// replication_active_alerts counts the replication problems currently held in
// the in-memory alert state: stalled streams (#3), not-advancing streams and
// content divergences (the convergence audit), and stale installed apps. These
// are exactly the conditions the manager emails about, so surfacing the count in
// /_/health lets an EXTERNAL monitor see a replication problem without depending
// on the server's own (possibly broken) email path. Content divergence is counted
// again (#47): host-local tables/columns are now excluded via app declarations, so
// a stable content mismatch is a real signal. Cheap — map lengths under their
// mutexes, no scans — safe on the frequently-hit health route.
func replication_active_alerts() int {
	n := 0
	stall_alerted_mutex.Lock()
	n += len(stall_alerted)
	stall_alerted_mutex.Unlock()
	audit_convergence_mutex.Lock()
	// audit_content_alerted is counted again (#47): host-local tables/columns are
	// excluded via app declarations, so a content divergence is a real signal.
	n += len(audit_alerted) + len(audit_content_alerted) + len(audit_liveness_alerted)
	audit_convergence_mutex.Unlock()
	stale_app_mutex.Lock()
	n += len(stale_app_alerted)
	stale_app_mutex.Unlock()
	return n
}
