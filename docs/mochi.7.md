% MOCHI(7) Mochi | Mochi project overview
% Alistair Cunningham
% 2026

# NAME

mochi - decentralised peer-to-peer social platform

# DESCRIPTION

Mochi is a peer-to-peer social platform where every server is a first-class
participant in a global mesh, rather than a tenant of someone else's
service. There is no central server, no required account at a hosting
provider, and no DNS-name dependency for users to reach each other; nodes
find each other via libp2p's Kademlia DHT and direct connect over TCP or
QUIC.

A single binary, **mochi-server**(8), runs the whole stack: peer discovery,
peer-to-peer transport, web server, app loader, database, and admin
listener. The companion CLI is **mochictl**(1).

# ARCHITECTURE

## Peers

Each server has a permanent libp2p **peer ID** — an ed25519-derived
identifier of the form `12D3KooW...`. The private key lives at
*<data_dir>*/p2p/private.key. Losing the key means losing the peer's
identity in the mesh; treat the data directory as essential for backups.

Peers find each other through:

- **Bootstrap peers** — a small list of well-known servers (currently
  Mochi-org-operated nodes) that new peers connect to first.
- **Kademlia DHT** — once connected to even one peer, a node joins the
  global DHT and can resolve any other peer's reachable addresses.
- **Multicast DNS** — for peers on the same LAN, mDNS short-circuits the
  DHT lookup. Best-effort; failure is non-fatal.

## Entities

Inside each server, the unit of identity exposed to users is the
**entity**. An entity might be a person, a group, a wiki, a forum, a
chess game in progress, or any other addressable object. Each entity has
its own libp2p-derived ID and a 9-character **fingerprint** used in
human-readable URLs (`/forums/yuGtwdxVh/-/welcome`).

Entities are owned by exactly one server. Other peers can subscribe to
or replicate an entity's content, but write authority stays with the
owner. The forums, feeds, and wikis apps build their replication models
on this foundation.

## Apps

User-facing functionality is built as **apps** that run on top of the
server: feeds, forums, wikis, chat, projects, files, etc. Each app is a
bundle of:

- a manifest (*app.json*) declaring routes, services, schema, etc.
- one or more *.star* files (Starlark) implementing actions and event
  handlers
- a *web/* tree with a React/TypeScript frontend
- a *labels/* tree with translations

Apps are distributed through the **publisher** app: an app developer
publishes a new version, the publisher serves it, and remote servers
pull and install on their own schedule. Each server has its own copy of
each installed app.

Apps run in a sandboxed Starlark interpreter. They access the host
through a small `mochi.*` API surface (database, attachments, peers,
HTTP, etc.) rather than bare Go syscalls; the server enforces
permissions declared in *app.json* against each call.

## Data layout

Inside *<data_dir>* (default */var/lib/mochi*):

*db/users.db*
:   Core auth and user state — one shared file across the server.

*p2p/private.key*
:   Permanent libp2p identity. Backup-critical.

*apps/<entity-id>/<version>/*
:   Installed app code, one tree per app version.

*users/<id>/<app>/db/\*.db*
:   Per-user-per-app SQLite databases.

*users/<id>/<app>/files/*
:   Per-user-per-app blob storage (attachments etc.).

*run/admin.sock*
:   UDS for **mochictl**(1). Recreated each startup.

# OPERATIONS

## Lifecycle

`systemctl start | stop | restart mochi-server` on a native install;
**mochictl** **stop** | **restart** on Docker (or anywhere else, native
installs included). **mochictl** **stop** exits the server cleanly with
status 0; **mochictl** **restart** exits with status 75 (EX_TEMPFAIL) so
a supervisor configured with **Restart=on-failure** brings it back.

## Backups

The supported flow is **mochictl snapshot** (writes transactionally
consistent SQLite copies as `*.db.snap`) followed by rsync of the data
directory using the exclude rules from **mochictl rsync-filter**.
Restore is the reverse plus **mochictl restore** to rename `*.db.snap`
back to `*.db`. See **mochictl**(1) and the
[Backup and restore](https://docs.mochi-os.org/wikis/yuGtwdxVh/backup-restore)
docs.

## Monitoring

The HTTP endpoint */_/health* returns a JSON liveness summary:

    {"status":"ok","version":"0.4.50","uptime":1234,
     "database":"ok","network":"ok"}

200 if all subsystems are healthy, 503 with detail otherwise. No auth.
Suitable for Docker `HEALTHCHECK`, Kubernetes liveness probes, and
external uptime monitors.

# DISTRIBUTION

Native packages for Debian/Ubuntu (.deb), Fedora/RHEL (.rpm), Windows
(.msi), macOS (.pkg), and Docker (multi-arch image at
*ghcr.io/mochi-os/mochi-server*).

Source: <https://github.com/mochi-os/core>.

Docs: <https://docs.mochi-os.org/>.

# SEE ALSO

**mochi-server**(8), **mochictl**(1), **mochi.conf**(5)
