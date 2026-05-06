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

# SEE ALSO

**mochi-server**(8)
