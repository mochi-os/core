% MOCHI.CONF(5) Mochi | Mochi server configuration
% Mochisoft OÜ
% 2026

# NAME

mochi.conf - configuration file for **mochi-server**(8)

# SYNOPSIS

*/etc/mochi/mochi.conf*

# DESCRIPTION

The configuration file is INI-shaped: square-bracketed sections, `key = value`
pairs within each section. Comments begin with `#` or `;` and run to end of
line. Whitespace around the `=` is ignored.

Every key may also be set through an environment variable named
**MOCHI\_<SECTION>\_<KEY>**, uppercased. Environment values take precedence
over file values; both override the built-in defaults. This is the primary
way to configure Mochi inside Docker.

Sensitive values (any key whose name contains *password*, *secret*, *key*,
or *token*) are masked as `***redacted***` in **mochictl config show**
output, so the effective configuration is safe to copy into bug reports.

# SECTIONS

## [directories]

**cache** = *path*
:   Cache directory. Defaults to */var/cache/mochi*. Safe to delete; will
    be recreated.

**data** = *path*
:   Data directory. Defaults to */var/lib/mochi*. Holds peer identity,
    the ACME account key and issued TLS certificates, SQLite databases,
    attachments, and installed app code. Treat as essential for backups.

**ensure** = **true** | **false**
:   When **true**, the server starts as root, creates and chowns the
    cache and data directories, then drops privileges to *uid*/*gid*
    before serving any request. Used by the Docker image; deb/rpm
    installs leave it **false** and rely on systemd to start as the
    *mochi* user. Defaults to **false**.

**uid** = *integer*
:   Numeric user ID to drop to when *ensure* is true. Defaults to **1000**.

**gid** = *integer*
:   Numeric group ID to drop to when *ensure* is true. Defaults to **1000**.

## [web]

**ports** = *port*[,*port*...]
:   Comma-separated list of ports to bind. If a port is **443** or **8443**
    the listener serves HTTPS using auto-provisioned certificates managed
    through the Domains UI; otherwise plain HTTP. Required (no default).

**listen** = *address*
:   Bind address, e.g. *127.0.0.1* to limit to loopback. Defaults to all
    interfaces.

**connections** = *integer*
:   Maximum connections accepted at once, across every listener. Past it
    further connections wait in the kernel's backlog rather than being
    accepted, so they consume no file descriptor here. Not a throughput
    limit: HTTP/2 carries a browser's whole session over one connection,
    so the default of **32768** is far above normal use and exists to
    bound what a flood can occupy. **0** disables the limit. Raising it
    much beyond the default needs *LimitNOFILE* raised to match, and
    enough memory for the buffers each connection holds.

**compress** = **auto** | **gzip** | **br** | **none**
:   Response compression. **auto** picks per-request based on the
    client's `Accept-Encoding` header. Defaults to **auto**.

**gzip** = *integer*
:   Gzip compression level (1-9). Defaults to **6**.

**brotli** = *integer*
:   Brotli compression level (0-11). Defaults to **4**.

**cache** = **true** | **false**
:   Whether to enable HTTP response caching for static assets.
    Defaults to **true**.

**debug** = **true** | **false**
:   Enable Gin's debug mode (verbose route registration logs).
    Defaults to **false**.

## [p2p]

**port** = *integer*
:   libp2p listen port — TCP and QUIC over UDP. Defaults to **1443**.
    Without inbound reachability on this port the server can still
    initiate connections but cannot serve as a peer for others.

**relay** = **true** | **false**
:   When **true**, the server advertises as a libp2p relay, helping
    NAT-restricted peers reach each other. Defaults to **false**.

## [email]

**host** = *hostname*
:   SMTP relay host. Defaults to **127.0.0.1**.

**port** = *integer*
:   SMTP relay port. Defaults to **25**.

**tls** = **true** | **false**
:   When **true** (default), the server uses opportunistic STARTTLS and
    verifies the relay's certificate against the system trust store.
    Set to **false** when relaying through a localhost or LAN-private
    postfix whose certificate isn't in any public CA chain — the network
    path is private by position, so plain SMTP is acceptable there.

**from** = *address*
:   Default `From` address for outbound mail (login codes, admin
    alerts). Defaults to **mochi-server@localhost** unless overridden
    by the *email_from* setting in the database (set through the UI).

**admin** = *address*
:   Email address that receives **warn()**-level alerts. If empty (the
    default), no admin alerts are emailed.

## [files]

**domains** = *path*
:   Path to the domain-routing config used by the file-server entity for
    custom domains (*packages.mochi-os.org*, *mochi-os.org* etc).
    Defaults to empty.

## [starlark]

**concurrency** = *integer*
:   Maximum number of Starlark interpreters that may run concurrently.
    Defaults to **32**.

**timeout** = *integer*
:   Maximum wall-clock seconds any single Starlark invocation may run
    before being aborted. Defaults to **90**.

**file_timeout** = *integer*
:   Maximum wall-clock seconds an invocation that is streaming a file to
    the client may continue after **timeout** has passed. Such an
    invocation has finished its Starlark work and is only sending bytes,
    so aborting it at **timeout** would truncate large downloads on slow
    connections. Values below **timeout** are raised to it. Defaults to
    **900**.

## [development]

**apps** = *path*
:   Path to a directory of unpublished apps loaded directly from the
    filesystem (typically *~/mochi/apps* during development). Empty by
    default; production installs leave it unset and load apps only
    from the publisher pipeline.

**reload** = **true** | **false**
:   When **true**, dev apps reload on file change without a server
    restart. Defaults to **false**.

## [update]

**check** = **true** | **false**
:   When **true** (default), the server polls
    *https://packages.mochi-os.org/<platform>/versions.json* once every
    24 hours and notifies all administrator users when a newer release
    is published on the production track. Set to **false** to disable
    the daily poll entirely.

    Released builds (`make release` from the Mochi source tree) include
    a build-time `build_platform` tag for *linux*, *windows*, *macos*
    or *docker*; the daily check only runs when both that tag and a
    `build_version` are present, so source builds stay quiet.

# ENVIRONMENT OVERRIDES

Every key has an environment-variable counterpart of the form
**MOCHI\_<SECTION>\_<KEY>**, uppercased. Examples:

| File entry              | Environment variable |
|-------------------------|----------------------|
| *[directories] data*    | **MOCHI_DIRECTORIES_DATA** |
| *[web] ports*           | **MOCHI_WEB_PORTS** |
| *[email] tls*           | **MOCHI_EMAIL_TLS** |
| *[p2p] port*            | **MOCHI_P2P_PORT** |

Comma-separated lists (such as *web.ports*) carry the same comma-separated
form in the env var: *MOCHI_WEB_PORTS=80,443*.

Empty environment values (*MOCHI_FOO_BAR=*) are treated as explicit
overrides to empty string, not as fall-through to the file value. This
matches standard environment-variable conventions.

Unparseable integer or boolean overrides log a warning and fall back to
the file value, so a typo doesn't silently change behaviour.

# EXAMPLES

Minimal native install, public HTTPS:

    [web]
    ports = 80, 443

    [email]
    admin = ops@example.com

Docker-style config baked into the image:

    [directories]
    data   = /var/lib/mochi
    cache  = /var/cache/mochi
    ensure = true
    uid    = 1000
    gid    = 1000

    [web]
    ports = 8080

    [p2p]
    port = 1443

Local-relay config (host postfix on loopback with snake-oil cert):

    [email]
    host = 127.0.0.1
    port = 25
    tls  = false
    admin = ops@example.com

# FILES

*/etc/mochi/mochi.conf*
:   Default location read by **mochi-server**(8) and **mochictl**(1)
    when neither has been pointed elsewhere with **-f**.

# SEE ALSO

**mochi-server**(8), **mochictl**(1), **mochi**(7)
