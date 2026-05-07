% MOCHICTL(1) Mochi 0.4.45 | Mochi Server Admin/Ops CLI
% Alistair Cunningham
% 2026

# NAME

mochictl - Mochi server admin/ops CLI

# SYNOPSIS

**mochictl** [*global flags*] *subcommand* [*subcommand flags*]

# DESCRIPTION

**mochictl** is the operator's control tool for a running **mochi-server**(8).
It connects to the server's UDS admin listener at *<data_dir>*/run/admin.sock
and is authenticated by Unix peer credentials — no tokens, no network.

This is a stub man page. Phase 8 of the Docker / mochictl plan
(`claude/plans/mochictl.md`) fleshes out OPTIONS, EXAMPLES, FILES, and
SEE ALSO sections to publication quality.

Run **mochictl** with no arguments for the current list of subcommands.

# OPTIONS

**-f** *path*
:   Path to *mochi.conf*. Defaults to */etc/mochi/mochi.conf*.

**-s** *path*
:   Override the admin UDS socket path. Defaults to *<data_dir>*/run/admin.sock.

**-t**
:   Tab-separated `key<TAB>value` output (TSV-style), suitable for scripting.

**-j**
:   JSON output, pretty-printed.

**-v**
:   Verbose: show output for normally silent commands. By default
    **mochictl snapshot** prints nothing on success — pass **-v** to see
    bytes written, database count, and elapsed time.

# SEE ALSO

**mochi-server**(8)
