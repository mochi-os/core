% MOCHICTL(1) Mochi | Mochi Server Admin/Ops CLI
% Alistair Cunningham
% 2026

# NAME

mochictl - Mochi server admin/ops CLI

# SYNOPSIS

**mochictl** [*global flags*] *subcommand* [*subcommand args*]

**mochictl** **status** | **version** | **health** | **identity** | **config show** | **rsync-filter**

**mochictl** **snapshot** | **backup** [*path*] | **restore** *dir*

**mochictl** **stop** | **start** | **restart**

# DESCRIPTION

**mochictl** is the operator's control tool for a running **mochi-server**(8).
It connects to the server's admin listener — a Unix domain socket at
*<data_dir>*/run/admin.sock — and is authenticated by Unix peer credentials.
There is no token, network port, or password; the kernel's `SO_PEERCRED`
mechanism authorises the calling process by its UID/GID.

The connecting user must be **root**, the **mochi** user the server runs as,
or a member of the **mochi** group. Operators who need to invoke **mochictl**
from a non-privileged shell can join the mochi group with
`usermod -aG mochi <user>` (and start a new login session for it to take
effect).

Subcommands fall into three groups:

**Inspection** — *status*, *version*, *health*, *identity*, *config show*,
*rsync-filter*. Read-only. Output goes to stdout in one of three formats
(see **OPTIONS**).

**Maintenance** — *snapshot*, *backup*, *restore*. Drive backup workflows.
*snapshot* writes transactionally-consistent SQLite copies (`*.db.snap`)
alongside each live database; *backup* streams a tar.gz of the entire data
directory; *restore* renames `*.db.snap` to `*.db` after a directory has
been rsync'd into place.

**Lifecycle** — *stop*, *restart*, *start*. *stop* exits the server with
exit code 0 (supervisor will not restart). *restart* exits with code 75
(EX_TEMPFAIL) so a supervisor configured with `Restart=on-failure` brings
the server back. *start* shells out to `systemctl start mochi-server` on
native installs; not applicable inside Docker containers.

Every state-changing subcommand (*snapshot*, *stop*, *restart*) writes one
audit row to syslog (LOG_DAEMON). Read-only subcommands do not.

# OPTIONS

Global flags can appear before or after the subcommand name; both
`mochictl -t status` and `mochictl status -t` work.

**-f** *path*
:   Path to *mochi.conf*. Defaults to */etc/mochi/mochi.conf*. **mochictl**
    reads only the *[directories] data* setting from this file (to find the
    admin socket); all other server config is irrelevant to **mochictl**.

**-s** *path*
:   Override the admin UDS socket path. Defaults to
    *<data_dir>*/run/admin.sock derived from the config file. Useful
    for multi-instance setups or when probing a non-standard data dir.

**-t**
:   Tab-separated `key<TAB>value` output, with nested objects flattened to
    `section.key`. Designed for `awk`/`cut`/shell scripting.

**-j**
:   Pretty-printed JSON output. Designed for `jq` and structured tooling.

**-v**
:   Verbose. Show output for subcommands that are silent by default
    (*snapshot*, *stop*, *restart*, and *backup* with an explicit path).
    Without **-v** these commands signal success only via exit code 0.

**-h**, **--help**
:   Print a list of subcommands and global flags.

# SUBCOMMANDS

**status**
:   Server liveness summary: status, version, uptime, peer counts, app count.

**version**
:   Server build version, schema version, and **mochictl** build version.

**health**
:   Liveness probe: status, uptime, database, network. Exit code 0 if all
    subsystems are healthy, 1 otherwise. Used by Docker `HEALTHCHECK` and
    Kubernetes liveness probes.

**identity**
:   Server libp2p peer ID and the resolved data directory.

**config show**
:   Effective configuration after merging the file and `MOCHI_*` env-var
    overrides. Sensitive values (anything matching */password*, */secret*,
    */key*, */token*) are replaced with `***redacted***`.

**rsync-filter**
:   Print the canonical rsync exclude rules for backing up *<data_dir>*.
    Excludes live `*.db`, WAL/SHM siblings, in-flight `*.snap.tmp` files,
    and the runtime state directory. Pipe into a temp file with
    `mochictl rsync-filter > rules` and pass to rsync as
    `--filter='. rules'`.

**snapshot**
:   Write `*.db.snap` siblings of every live database in the data
    directory, using SQLite's online-backup API for transactional
    consistency. Silent on success — pass **-v** for bytes-written,
    database count, and elapsed time.

**backup** [*path*]
:   Stream a tar.gz of the data directory (live DBs replaced with their
    snapshots) to *path*, or to a timestamped file
    `mochi-backup_YYYYMMDD_HHMMSS.tar.gz` in the current directory if no
    path is given, or to stdout if *path* is `-`. Refreshes snapshots
    automatically; no separate **snapshot** call needed.

**restore** *dir*
:   Walk *dir* and rename every `*.db.snap` to its corresponding `*.db`,
    overwriting any live database file. Refuses to run while the admin
    socket is live (the server must be stopped first).

**stop**
:   Graceful shutdown. Server exits 0; the supervisor decides whether to
    restart based on its policy. Silent on success unless **-v**.

**restart**
:   Graceful shutdown with exit code 75 so a supervisor configured with
    `Restart=on-failure` (systemd) or `--restart=on-failure` (Docker)
    brings the server back. Silent on success unless **-v**.

**start**
:   Start a stopped server via `systemctl start mochi-server`. Native
    installs only; inside Docker, use `docker start` on the container.

# EXAMPLES

Watch readiness:

    mochictl status

Cron-friendly backup pipeline:

    #!/bin/sh
    set -e
    mochictl snapshot
    rsync -a --delete --delete-excluded \\
        --filter=". <(mochictl rsync-filter)" \\
        /var/lib/mochi/ backup@host:/var/lib/backup/mochi/

Disaster-recovery restore on a fresh server:

    systemctl stop mochi-server
    rsync -a backup@host:/var/lib/backup/mochi/ /var/lib/mochi/
    mochictl restore /var/lib/mochi
    systemctl start mochi-server

Scripted JSON consumer:

    if ! mochictl -j health | jq -e '.status == "ok"' >/dev/null; then
        page-oncall "mochi degraded"
    fi

# FILES

*/etc/mochi/mochi.conf*
:   Server configuration; **mochictl** reads only *[directories] data*.

*<data_dir>/run/admin.sock*
:   Unix domain socket used for all admin requests. Created by
    **mochi-server**(8) at startup, removed at shutdown.

*<data_dir>/db/*.db.snap*, *<data_dir>/users/<id>/<app>/db/*.db.snap*
:   Snapshot files written by **mochictl snapshot** and consumed by
    **mochictl restore**.

# DIAGNOSTICS

**mochictl** prints errors to stderr and exits non-zero. Exit codes:

**0**
:   Success.

**1**
:   Server unreachable, permission denied, or non-2xx response from the
    admin endpoint.

# SEE ALSO

**mochi-server**(8), **systemctl**(1), **rsync**(1)
