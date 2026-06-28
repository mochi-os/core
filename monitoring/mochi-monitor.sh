#!/bin/bash
# Copyright © 2026 Mochi OÜ
# SPDX-License-Identifier: AGPL-3.0-only
# This file is part of Mochi, licensed under the GNU AGPL v3 with the
# Mochi Application Interface Exception - see license.txt and license-exception.md.

# Mochi external replication monitor — the dead-man's-switch for the servers'
# own, in-process, email-based alerting.
#
# The Mochi servers email the admin on replication problems (stalls, divergence,
# stale code, dead peers). But two failure modes silence that channel and the
# server CANNOT report them itself:
#   - the replication manager goroutine hangs   -> no scans run, no alerts fire
#   - the server's local mail path breaks        -> alerts generated, never sent
#
# Run THIS on a host SEPARATE from the monitored servers, with its own mail path
# (we use sansho). It polls each server's public /_/health on a short cron and
# alerts via an INDEPENDENT path when a server is unreachable, its replication is
# degraded, or its replication manager has hung (heartbeat stale). Repeated
# alerts for an unchanged, persistent problem are deduped (STATE/REALERT) so a
# long-lived condition (e.g. a dead peer kept until cleaned manually) doesn't
# mail every run. Fields consumed are set in core/server/health.go. Requires:
# curl, jq, and a working `mail` (or ALERT_CMD).

set -u

# --- config (override via environment) -------------------------------------
SERVERS="${MOCHI_MONITOR_SERVERS:-https://yuzu.mochi-os.org https://wasabi.mochi-os.org}"
ALERT_TO="${MOCHI_MONITOR_TO:-alistair@acunningham.org}"
MANAGER_STALL="${MOCHI_MONITOR_MANAGER_STALL:-180}" # seconds; the manager ticks every 30s
TIMEOUT="${MOCHI_MONITOR_TIMEOUT:-15}"
STATE="${MOCHI_MONITOR_STATE:-/var/lib/mochi-monitor/state}" # dedup state file
REALERT="${MOCHI_MONITOR_REALERT:-21600}"                    # re-alert an UNCHANGED problem at most every 6h
# ---------------------------------------------------------------------------

problems=""
note() { problems="${problems}${1}\n"; }

for url in $SERVERS; do
	host=$(printf '%s' "$url" | sed -e 's#^https\?://##' -e 's#/.*##')
	body=$(curl -fsS --max-time "$TIMEOUT" "$url/_/health" 2>/dev/null)
	rc=$?
	if [ "$rc" -ne 0 ] || [ -z "$body" ]; then
		note "$host: UNREACHABLE — /_/health did not respond (server down, or network/TLS failure)."
		continue
	fi

	status=$(printf '%s' "$body" | jq -r '.status // "?"')
	repl=$(printf '%s' "$body" | jq -r '.replication // "?"')
	mage=$(printf '%s' "$body" | jq -r '.manager_age // -1')
	irr=$(printf '%s' "$body" | jq -r '.irreparable // 0')

	[ "$status" != "ok" ] && note "$host: liveness status=$status (database/network degraded)."
	[ "$repl" != "ok" ] && note "$host: replication=DEGRADED (irreparable=$irr) — a stall / not-advancing / divergence / stale-app alert is active, or a peer is dead. Check: mochictl replication audit / stalled."
	# manager_age: seconds since the last manager tick; -1 = never started.
	if [ "$mage" -lt 0 ] || [ "$mage" -gt "$MANAGER_STALL" ]; then
		note "$host: replication MANAGER HUNG — last tick ${mage}s ago (threshold ${MANAGER_STALL}s). The server's own alerting is DEAD; restart mochi-server."
	fi
done

# send: subject as $1, body on stdin -> ALERT_CMD (if set), else mail.
send() {
	if [ -n "${ALERT_CMD:-}" ]; then
		sh -c "$ALERT_CMD"
	else
		mail -s "$1" "$ALERT_TO"
	fi
}

# Dedup: a stateless cron would mail on EVERY run while a problem persists — and
# replication peers are kept until cleaned manually, so a problem CAN persist a
# long time. Remember the last alerted problem-set + timestamp and re-send only
# when the set CHANGES or REALERT seconds have elapsed. When problems clear, send
# one "recovered" note and reset.
if [ -n "$problems" ]; then
	sig=$(printf '%s' "$problems" | md5sum 2>/dev/null | awk '{print $1}')
	nowts=$(date +%s)
	last_sig=""; last_ts=0
	if [ -f "$STATE" ]; then read -r last_sig last_ts < "$STATE" 2>/dev/null || true; fi
	case "${last_ts:-}" in ""|*[!0-9]*) last_ts=0 ;; esac
	if [ "$sig" != "$last_sig" ] || [ "$((nowts - last_ts))" -ge "$REALERT" ]; then
		printf 'Mochi external monitor detected problems:\n\n%bPolled: %s\n' "$problems" "$SERVERS" | send "Mochi MONITOR alert"
		mkdir -p "$(dirname "$STATE")" 2>/dev/null
		# Persist the problem text (after the "sig ts" header line) so the
		# eventual "cleared" note can name exactly what recovered.
		printf '%s %s\n%b' "$sig" "$nowts" "$problems" > "$STATE"
	fi
	exit 1
fi

# All clear. If we were previously alerting, send one recovery note naming the
# problem(s) that cleared (read back from the state file) and reset.
if [ -f "$STATE" ]; then
	cleared=$(tail -n +2 "$STATE")
	if [ -n "$cleared" ]; then
		printf 'Mochi external monitor: the following previously-detected problem(s) have CLEARED:\n\n%s\n\nPolled: %s\n' "$cleared" "$SERVERS" | send "Mochi MONITOR recovered"
	else
		# Legacy state file written before the problem text was recorded.
		printf 'Mochi external monitor: previously-detected problem(s) have CLEARED.\nPolled: %s\n' "$SERVERS" | send "Mochi MONITOR recovered"
	fi
	rm -f "$STATE"
fi
exit 0
