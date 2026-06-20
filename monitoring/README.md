# Mochi external replication monitor

`mochi-monitor.sh` is the **dead-man's-switch** for the Mochi servers' own,
in-process, email-based alerting.

The servers email the admin on replication problems — stalls (#3), not-advancing
streams and content divergence (the convergence audit), stale installed code,
and dead peers. But two failure modes silence that channel, and the server
cannot report them itself:

- the **replication manager goroutine hangs** — no health scans run, so no
  alerts fire (the web server keeps answering, so a plain liveness probe can't
  tell);
- the server's **local mail path breaks** — alerts are generated but never
  leave the box.

This script runs on a host **outside the mochi mesh** (we use **falcon**, the
mail relay) and polls each server's public `/_/health`, alerting via an
**independent** path. It catches:

- **server unreachable** — `/_/health` did not respond;
- **`replication: degraded`** — any active alert (stall / not-advancing /
  divergence / stale app) or a dead peer;
- **manager hung** — `manager_age` stale (the heartbeat the manager bumps every
  30s has stopped advancing).

## Deploy (on falcon)

```sh
install -m 0755 mochi-monitor.sh /usr/local/bin/mochi-monitor.sh
```

Cron, every 3 minutes:

```cron
*/3 * * * * /usr/local/bin/mochi-monitor.sh
```

(or a systemd `mochi-monitor.service` + `.timer` with `OnUnitActiveSec=3min`).

## Config (environment)

| variable | default |
|----------|---------|
| `MOCHI_MONITOR_SERVERS` | `https://yuzu.mochi-os.org https://wasabi.mochi-os.org` |
| `MOCHI_MONITOR_TO` | `alistair@acunningham.org` |
| `MOCHI_MONITOR_MANAGER_STALL` | `180` (seconds; the manager ticks every 30s) |
| `MOCHI_MONITOR_TIMEOUT` | `15` (seconds per HTTP poll) |
| `ALERT_CMD` | unset → uses `mail`; set to e.g. a `curl` webhook for a channel fully independent of email |

## Independence caveat

falcon is also the servers' mail **relay**, so the monitor's `mail` and the
servers' own alerts share falcon's outbound path. The monitor still catches the
hung-manager and server-down cases (which don't touch mail at all) and a broken
**server-local** mail path. For full independence from a falcon-relay outage,
point `ALERT_CMD` at a non-email channel (SMS / webhook / push).

## Fields consumed

From `/_/health` (public, unauthenticated), defined in `core/server/health.go`:

- `status` — `ok` / `degraded` (database + network liveness)
- `replication` — `ok` / `degraded` (any active replication alert or dead peer)
- `irreparable` — count of streams/peers given up on
- `manager_age` — seconds since the last replication-manager tick (`-1` = never
  started)

## Licence

Part of the Mochi server - licensed under the GNU AGPL version 3 with the Mochi
Application Interface Exception. See the [license.txt](../license.txt) and
[license-exception.md](../license-exception.md) files at the repository root.
