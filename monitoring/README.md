# Mochi external monitor

A dead-man's-switch for the production servers: the servers email the admin on
their own problems, but a down server cannot report its own death. This script
polls each server's public `/_/health` from a separate host and emails when a
server is unreachable or reports a degraded liveness status.

## Where it runs

A host that is not a Mochi production server (we use falcon). Install:

    install -m 0755 mochi-monitor.sh /usr/local/bin/mochi-monitor

Run every 3 minutes via cron:

    */3 * * * * /usr/local/bin/mochi-monitor

or a systemd timer of the same shape.

## Configuration (environment)

| Variable | Default |
|---|---|
| `MOCHI_MONITOR_SERVERS` | `https://yuzu.mochi-os.org` |
| `MOCHI_MONITOR_TO` | `alistair@acunningham.org` |
| `MOCHI_MONITOR_TIMEOUT` | `15` (seconds per poll) |
| `MOCHI_MONITOR_STATE` | `/var/tmp/mochi-monitor.state` |
| `MOCHI_MONITOR_ALERT_CMD` | unset (use `mail`) |

## What it checks

- server reachable: `/_/health` responds within the timeout
- `status` — `ok` / `degraded` (database or network liveness)

Alerts dedup via the state file: one email when a problem appears, one when it
clears ("recovered").
