#!/bin/bash
# Mochi external replication monitor — the dead-man's-switch for the servers'
# own, in-process, email-based alerting.
#
# The Mochi servers email the admin on replication problems (stalls, divergence,
# stale code, dead peers). But two failure modes silence that channel and the
# server CANNOT report them itself:
#   - the replication manager goroutine hangs   -> no scans run, no alerts fire
#   - the server's local mail path breaks        -> alerts generated, never sent
#
# Run THIS on a host OUTSIDE the mochi mesh (we use falcon, the mail relay) on a
# short cron. It polls each server's public /_/health and alerts via an
# INDEPENDENT path when a server is unreachable, its replication is degraded, or
# its replication manager has hung (heartbeat stale). Fields consumed are set in
# core/server/health.go. Requires: curl, jq, and a working `mail` (or ALERT_CMD).

set -u

# --- config (override via environment) -------------------------------------
SERVERS="${MOCHI_MONITOR_SERVERS:-https://yuzu.mochi-os.org https://wasabi.mochi-os.org}"
ALERT_TO="${MOCHI_MONITOR_TO:-alistair@acunningham.org}"
MANAGER_STALL="${MOCHI_MONITOR_MANAGER_STALL:-180}" # seconds; the manager ticks every 30s
TIMEOUT="${MOCHI_MONITOR_TIMEOUT:-15}"
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

if [ -n "$problems" ]; then
	msg=$(printf 'Mochi external monitor detected problems:\n\n%bPolled: %s\n' "$problems" "$SERVERS")
	if [ -n "${ALERT_CMD:-}" ]; then
		printf '%s\n' "$msg" | sh -c "$ALERT_CMD"
	else
		printf '%s\n' "$msg" | mail -s "Mochi MONITOR alert" "$ALERT_TO"
	fi
	exit 1
fi
exit 0
