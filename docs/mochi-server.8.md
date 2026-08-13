% MOCHI-SERVER(8) Mochi | Mochi Server
% Mochisoft OÜ
% 2026

# NAME

mochi-server - Mochi distributed app platform server

# SYNOPSIS

**mochi-server** [**-f** *config_file*]

# DESCRIPTION

**mochi-server** is the Mochi server daemon: a libp2p-based distributed
app platform. Each instance is one node in a global mesh of independent
servers, hosting one or more user accounts and the apps they install (feeds,
forums, wiki, chat, etc.).

The server maintains:

- An ed25519 libp2p identity (one per data directory) that is the node's
  permanent address in the mesh.
- One or more user accounts, each with their own per-app SQLite databases
  and on-disk files (attachments, app state, etc.).
- A web frontend on configurable HTTP/HTTPS ports.
- A transport between servers on a libp2p port (default 1443, TCP and
  QUIC/UDP).
- An admin Unix domain socket at *<data_dir>*/run/admin.sock for
  **mochictl**(1).
- A world-status Unix domain socket at *<data_dir>*/run/world.sock (mode
  0660, group **mochi-world**) where a co-located **mochi-world**(8) server
  pushes its public listing - name, address, per-service player counts. The
  server announces listings to the network over pubsub; they age out 45
  minutes after the world stops refreshing. Members of the **mochi-world**
  group, the **mochi** user, and root may connect; on Windows the equivalent
  named pipe \\.\pipe\mochi-world admits LocalSystem and Administrators.
  Inspect known listings with **mochictl worlds**.

  The **mochi-world** package creates that group and adds the **mochi**
  account to it, because the socket is created by this server and a process
  may only set a file's group to one it belongs to. Group membership is read
  at process start, so a freshly installed pair takes the group on this
  server's next restart; until then the world server reaches the socket as
  its owner. The shipped **mochi-world** unit runs as the **mochi** user, so
  it connects either way - the group is what lets an operator run the world
  server under a separate account without granting it this server's data
  directory. *<data_dir>*/run is mode 0751 so such an account can traverse
  to the socket; each socket in it is gated by its own mode and group.

By default, **mochi-server** runs as the dedicated **mochi** user (created
by the deb/rpm postinst). Inside Docker, it starts as root, creates and
chowns its data directories, then drops to uid/gid 1000 before serving
any request — see *directories.ensure* in **CONFIGURATION**.

# OPTIONS

**-f** *config_file*
:   Path to *mochi.conf*. Defaults to */etc/mochi/mochi.conf*.

# CONFIGURATION

The configuration file is INI-shaped, with one section per subsystem.
A minimal native-install config is shipped at */etc/mochi/mochi.conf*;
a Docker-tuned version is baked into the official image.

Common sections:

**[directories]** — *data*, *cache*, *ensure*, *uid*, *gid*. *ensure = true*
makes the server mkdir + chown the data and cache directories at startup
and drop privileges to *uid:gid* before serving traffic. Used by Docker;
deb/rpm installs leave it false and rely on systemd to start as the mochi
user directly.

**[web]** — *ports* (comma-separated), *compress*, *cache*, *gzip*, *brotli*.
Each entry in *ports* binds an HTTP listener; if a port is 443 or 8443,
the server runs HTTPS using auto-provisioned ACME certificates managed
through the Domains UI.

**[p2p]** — *port* for the libp2p TCP+QUIC listener.

**[email]** — *host*, *port*, *tls*, *from*, *admin*. SMTP relay for
outbound mail (login codes, admin alerts). Set *tls = false* when relaying
to a localhost or LAN-private postfix whose certificate isn't in any
public CA chain.

**[development]** — *apps*, *reload*. Dev-only knobs for serving apps from
a working directory without going through the publisher pipeline.

Every key may be overridden by an environment variable of the form
**MOCHI\_<SECTION>\_<KEY>**, uppercased. Example: *[email] host = albatross*
becomes *MOCHI_EMAIL_HOST=albatross*. Env vars take precedence over the
file. Sensitive keys (anything whose name contains *password*, *secret*,
*key*, or *token*) are redacted from **mochictl config show** output.

# SIGNALS

**SIGTERM**, **SIGINT**
:   Graceful shutdown. Drains in-flight HTTP requests and notifies
    connected peers, then exits 0.

**SIGHUP**
:   Logged and ignored. Mochi has no on-the-fly reload; restart the
    service to pick up config changes.

# EXIT STATUS

**0**
:   Clean shutdown (SIGTERM/SIGINT or **mochictl stop**).

**75** (EX_TEMPFAIL)
:   Restart hint, emitted by **mochictl restart**. A supervisor configured
    with **Restart=on-failure** (systemd) or **--restart=on-failure**
    (Docker) treats this as a failure and brings the service back; **stop**
    (exit 0) leaves the service stopped.

**non-zero (other)**
:   Startup failure: bad config, port collision, data-dir permission error,
    failed schema migration. Inspect **journalctl -u mochi-server** or
    container logs.

# FILES

*/etc/mochi/mochi.conf*
:   Server configuration.

*/var/lib/mochi/*
:   Default data directory. Per-user app databases under
    *users/<id>/<app>/db/*, libp2p identity at *p2p/private.key*,
    installed app code under *apps/<entity-id>/*, runtime UDS at
    *run/admin.sock*. Treat as essential for backups.

*/var/cache/mochi/*
:   Cache directory. Safe to delete; will be recreated.

*/etc/systemd/system/mochi-server.service*
:   systemd unit shipped by the deb/rpm packages.

*/usr/sbin/mochi-server*
:   Server binary (deb/rpm). Inside the Docker image: same path.

*/usr/local/sbin/mochi-server*
:   Server binary (macOS .pkg).

# SEE ALSO

**mochictl**(1), **systemctl**(1)

Online docs: <https://docs.mochi-os.org/>
