#!/usr/bin/env python3
# Copyright © 2026 Mochi OÜ
# SPDX-License-Identifier: AGPL-3.0-only
# This file is part of Mochi, licensed under the GNU AGPL v3 with the
# Mochi Application Interface Exception - see license.txt and license-exception.md.

"""check-core-replicated-writes.py — flag a plain db.exec write to a REPLICATED core table.

Core Go APIs that mutate a table replicated across a user's host set must use a
journaling helper: db.exec_replicated (app-system tables: access, attachments,
permissions) or db.exec_app_user (broadcast sender-side: log, acknowledged). Both
record the replication op in the SAME transaction as the write, so it fans out to
the host set and converges — the guarantee mochi.db.execute gives app Starlark.

Plain db.exec (db.internal.Exec) is the LOCAL-ONLY primitive: it writes but never
journals, so a mutation it makes to one of those tables lands on THIS host only —
silent, no error, discovered later by the convergence audit or a user noticing
missing data on failover (#166).

db.exec is legitimately required on these same tables for genuinely host-local
state — e.g. `update attachments set entity=''`, the cache-promotion write that is
only true on the host that fetched the bytes — and for schema DDL (CREATE/ALTER,
which is not a flagged verb anyway). Allowlist an intentional local write with a
trailing `// exec-ok: <reason>`.

Flags a mutating (insert / replace / update / delete) `.exec(...)` — the lowercase DB
method, NOT exec_replicated / exec_app_user / .Exec — whose literal target table is a
known replicated core table. SQL whose verb+table are built in a variable, or sit on
a different line from `.exec(`, are the blind spot (as with check-determinism /
check-mutating-reads); the literal case is the early warning, the audit catches the
rest.

Also self-checks the table set: an exec_replicated / exec_app_user call with a literal
target table not in KNOWN_REPLICATED is reported so the gate is kept current when a
new replicated core table is added.

Usage:
  check-core-replicated-writes.py [paths...]       # report (exit 0)
  check-core-replicated-writes.py --check [paths]  # exit 1 if any (CI / preflight)

Default path: core/server/*.go from the monorepo root, or server/*.go from the core
repo root (so it runs both as a /release preflight and in core CI). Lives in the core
repo at core/.github/scripts/ so core CI can reach it on a core-only checkout.
"""
import re
import sys
from pathlib import Path

# Core-owned tables written through the journaling helpers (db.exec_replicated /
# db.exec_app_user), hence replicated. Keep in sync with those call-sites — the
# self-check below flags a new LITERAL helper target that is missing here.
KNOWN_REPLICATED = {"access", "attachments", "permissions", "log", "acknowledged"}

ALLOW = "exec-ok:"

# The lowercase `.exec(` DB method. `\.exec\(` cannot match `.exec_replicated(` /
# `.exec_app_user(` / `.exec_e(` (a `_` follows `exec`, not `(`) nor `.Exec(` (capital).
EXEC_RE = re.compile(r"\.exec\(")

# The journaling helpers, so we can self-check their literal target set.
HELPER_RE = re.compile(r"\.exec_(?:replicated|app_user)\(")

# Verb + target table at the head of the SQL right after the call's `(` (skipping an
# opening quote / backtick and any leading whitespace). Table identifier only.
TARGET_RE = re.compile(
    r"""^\s*["'`]?\s*(?:
        (?:insert|replace)\s+(?:or\s+\w+\s+)?into\s+["'`\[]?(\w+) |
        update\s+(?:or\s+\w+\s+)?["'`\[]?(\w+) |
        delete\s+from\s+["'`\[]?(\w+)
    )""",
    re.IGNORECASE | re.VERBOSE,
)


def _target(rest):
    m = TARGET_RE.match(rest)
    if not m:
        return None
    return (m.group(1) or m.group(2) or m.group(3) or "").lower()


def check_file(path):
    """Returns (violations, notes). violations = plain-exec writes to a replicated
    table; notes = helper writes to a table not yet in KNOWN_REPLICATED."""
    violations, notes = [], []
    try:
        text = path.read_text(errors="replace")
    except OSError:
        return violations, notes
    for i, line in enumerate(text.splitlines(), 1):
        for m in EXEC_RE.finditer(line):
            table = _target(line[m.end():])
            if table in KNOWN_REPLICATED and ALLOW not in line:
                violations.append((i, table, line.strip()))
        for m in HELPER_RE.finditer(line):
            table = _target(line[m.end():])
            if table and table not in KNOWN_REPLICATED:
                notes.append((i, table, line.strip()))
    return violations, notes


def main():
    args = sys.argv[1:]
    strict = "--check" in args
    paths = [a for a in args if a != "--check"]
    if paths:
        files = []
        for p in paths:
            pp = Path(p)
            files += [pp] if pp.is_file() else sorted(pp.rglob("*.go"))
    else:
        root = Path("core/server") if Path("core/server").is_dir() else Path("server")
        files = sorted(root.glob("*.go"))
    files = [f for f in files if not f.name.endswith("_test.go")]

    total_v, total_n = 0, 0
    for f in files:
        violations, notes = check_file(f)
        for ln, table, src in violations:
            total_v += 1
            print(f"{f}:{ln}: plain db.exec writes replicated table '{table}' "
                  f"(use db.exec_replicated/exec_app_user, or annotate // exec-ok: <reason>)")
            print(f"    {src}")
        for ln, table, src in notes:
            total_n += 1
            print(f"{f}:{ln}: NOTE: helper writes '{table}' not in KNOWN_REPLICATED "
                  f"— add it to check-core-replicated-writes.py")

    if total_v or total_n:
        print(f"\n{total_v} unguarded write(s), {total_n} set-drift note(s).")
    else:
        print("No unguarded replicated-table writes found.")
    if strict and (total_v or total_n):
        sys.exit(1)


if __name__ == "__main__":
    main()
