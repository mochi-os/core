#!/usr/bin/env python3
# Copyright © 2026 Mochisoft OÜ
# SPDX-License-Identifier: AGPL-3.0-only
# This file is part of Mochi, licensed under the GNU AGPL v3 with the
# Mochi Application Interface Exception - see license.txt and license-exception.md.

"""Static check for hardcoded user-facing English in Starlark and Go server code.

Flags Starlark `a.error(...)` that is not `a.error.label(...)`, and Go answers
whose body is a literal (c.JSON "error"/"message", c.String/Data on a non-2xx,
http.Error). Log and internal error strings are not checked.

Usage: check-i18n-server.py [--check] [path ...]   (defaults to apps/ and core/server/)

A regex audit, not a parser: allowlist with a trailing `# i18n-ok: <reason>` on
the line that holds the flagged value.
"""
import argparse
import os
import re
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
GATE = 'check-i18n-server'

# Starlark: any `a.error(...)` that is not `a.error.label(...)`. Matching the
# CALL FORM rather than a literal argument is deliberate - a.error never
# resolves labels, so a variable argument renders a raw label key to the user.
# Outside apps/test/ the monorepo has no legitimate a.error() call.
STAR_ERROR_LITERAL = re.compile(
    r'\ba\.error\s*\(\s*([^)]*)'
)

# Go: c.JSON(..., gin.H{"error": "literal"}) — flag the literal value.
GO_JSON_ERROR_LITERAL = re.compile(
    r'c\.JSON\s*\([^)]*"error"\s*:\s*("[^"]+")'
)
GO_JSON_MESSAGE_LITERAL = re.compile(
    r'c\.JSON\s*\([^)]*"message"\s*:\s*("[^"]+")'
)

# Go: a plain-text answer whose body is a literal, which the .JSON rules miss.
# Only non-2xx statuses count - c.String at StatusOK is serving content.
GO_TEXT_LITERAL = re.compile(
    r'\b\w+(?:\.\w+)*\.(?:String|Data)\s*\(\s*(?:http\.Status(?!OK\b)\w+|[45]\d\d)\s*,[^,)]*?("[A-Z][^"]{3,}")'
)
# http.Error takes the message as its second argument and never reaches a label.
GO_HTTP_ERROR_LITERAL = re.compile(
    r'\bhttp\.Error\s*\([^,]+,\s*("[A-Z][^"]{3,}")'
)

# Both comment styles: Starlark writes `# i18n-ok: <reason>`, Go writes
# `// i18n-ok: <reason>`. The reason is required: a bare token anywhere on the
# line - inside a string, a URL or a variable name - must not suppress the check.
ALLOW_TRAILING = re.compile(r'(?://|#).*i18n-ok:\s*\S')

# The regexes above only see a literal on the .JSON call's own line, so
# scan_go_json joins the call across lines and judges expression values and
# receivers not named `c` as well.
GO_JSON_CALL = re.compile(r'\b\w+(?:\.\w+)*\.JSON\s*\(')
GO_JSON_KEY = re.compile(r'"(error|message)"\s*:\s*([^,}]+)')
# A value that renders a Go error is the leak this exists to catch: err.Error()
# carries internal function names and, from the database, driver text.
GO_ERROR_VALUE = re.compile(r'\.Error\s*\(\s*\)')
GO_STRING_VALUE = re.compile(r'^"[^"]+"$')


def scan_go_json(path):
    """Report .JSON calls whose "error"/"message" value is a hardcoded string or
    renders a Go error. Joins each call across lines, so a key on its own line is
    seen; the `// i18n-ok: <reason>` marker exempts only the line that holds
    the flagged key, so one marker cannot excuse every key in the call."""
    hits = []
    try:
        text = path.read_text(errors='replace')
    except (OSError, UnicodeDecodeError):
        return hits
    lines = text.splitlines()
    for index, line in enumerate(lines):
        stripped = line.lstrip()
        if stripped.startswith('//'):
            continue
        m = GO_JSON_CALL.search(line)
        if not m:
            continue
        # Join from the call to its closing parenthesis (bounded, so an
        # unbalanced line cannot run away with the rest of the file).
        depth, span, last = 0, [], index
        for offset in range(index, min(index + 40, len(lines))):
            fragment = lines[offset] if offset > index else lines[offset][m.start():]
            span.append(fragment)
            depth += fragment.count('(') - fragment.count(')')
            last = offset
            if depth <= 0:
                break
        call = '\n'.join(span)
        for key, raw in GO_JSON_KEY.findall(call):
            value = raw.strip()
            if not (GO_STRING_VALUE.match(value) or GO_ERROR_VALUE.search(value)):
                continue
            # The marker binds to the line holding the flagged key.
            holder = next((lines[offset] for offset in range(index, last + 1)
                           if f'"{key}"' in lines[offset]), lines[index])
            if ALLOW_TRAILING.search(holder):
                continue
            hits.append((index + 1, lines[index].rstrip(), f'"{key}": {value}'))
            break
    return hits


GO_TEXT_PATTERNS = [GO_TEXT_LITERAL, GO_HTTP_ERROR_LITERAL]


def exempt(path):
    """True for apps/test/* - its a.error() calls are internal assertions.
    Judged on the path relative to the repo root, so an app's own test/
    directory (apps/<app>/test/x.star) is still checked."""
    path = Path(path)
    if not path.is_absolute():
        path = REPO / path
    if not path.is_relative_to(REPO):
        return False
    return path.relative_to(REPO).parts[:2] == ('apps', 'test')


def starlark_files(roots):
    """Yield Starlark source files under the supplied roots, minus apps/test."""
    for root in roots:
        for path in Path(root).rglob('*.star'):
            parts = set(path.parts)
            if 'node_modules' in parts or 'release' in parts:
                continue
            if exempt(path):
                continue
            yield path


def go_files(roots):
    """Yield Go server source files under any of the supplied roots."""
    for root in roots:
        for path in Path(root).rglob('*.go'):
            if '_test.go' in path.name:
                continue
            parts = set(path.parts)
            if 'node_modules' in parts or 'vendor' in parts:
                continue
            # Only scan core/server (request handlers); skip core/common.
            if '/core/' in str(path) and '/server/' not in str(path):
                continue
            yield path


def scan_file(path, patterns):
    """Return a list of (line_no, line, snippet) for each match in `patterns`."""
    hits = []
    try:
        text = path.read_text(errors='replace')
    except (OSError, UnicodeDecodeError):
        return hits
    for n, line in enumerate(text.splitlines(), 1):
        if ALLOW_TRAILING.search(line):
            continue
        # Comment lines are prose, not calls. Without this, a comment that
        # merely NAMES the offending API (explaining why it must not be used)
        # reports itself as a violation.
        stripped = line.lstrip()
        if stripped.startswith('#') or stripped.startswith('//'):
            continue
        for pat in patterns:
            m = pat.search(line)
            if m:
                hits.append((n, line.rstrip(), m.group(1)))
                break  # one report per line
    return hits


SELFTEST_GO = [
    ('literal message', True, '''
func h(c *gin.Context) {
	c.JSON(400, gin.H{"message": "Something went wrong"})
}
'''),
    ('wrapped Go error', True, '''
func h(c *gin.Context) {
	c.JSON(500, gin.H{"error": path_scrub(err.Error())})
}
'''),
    ('literal on a later line', True, '''
func h(c *gin.Context) {
	c.JSON(http.StatusForbidden, gin.H{
		"error": "permission_required",
		"app":   app,
	})
}
'''),
    ('receiver not named c', True, '''
func h(a *Action) {
	a.web.JSON(500, gin.H{"error": err.Error()})
}
'''),
    ('respond_error itself', False, '''
func respond_error(c *gin.Context, status int, code, key string) {
	msg := resolve_core_label(lang, key, args)
	c.JSON(status, gin.H{"error": code, "message": msg})
}
'''),
    ('annotated exception', False, '''
func h(c *gin.Context) {
	c.JSON(500, gin.H{"error": err.Error()}) // i18n-ok: admin socket
}
'''),
]

SELFTEST_STAR = [
    ('literal a.error', True, 'def f(a):\n    a.error(400, "Not found")\n'),
    ('label form', False, 'def f(a):\n    a.error.label(400, "errors.not_found")\n'),
]


def selftest():
    """Check every shape the rules are meant to judge, including the ones that
    must NOT be reported — a gate that flags respond_error, or that cannot
    express an exception, gets switched off rather than fixed."""
    import tempfile
    failures = 0
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        for name, expected, body in SELFTEST_GO:
            path = root / (name.replace(' ', '_') + '.go')
            path.write_text(body)
            got = bool(scan_go_json(path))
            if got != expected:
                print(f'  FAIL go/{name}: reported={got} expected={expected}')
                failures += 1
        for name, expected, body in SELFTEST_STAR:
            path = root / (name.replace(' ', '_') + '.star')
            path.write_text(body)
            got = bool(scan_file(path, [STAR_ERROR_LITERAL]))
            if got != expected:
                print(f'  FAIL starlark/{name}: reported={got} expected={expected}')
                failures += 1
    total = len(SELFTEST_GO) + len(SELFTEST_STAR)
    print(f'selftest: {total - failures}/{total} cases behave as intended')
    return 1 if failures else 0




def resolve(paths, defaults):
    """Roots to scan: the given paths, a relative one anchored at the repo root
    so a run from an app directory scans the same tree as one from the root;
    else the defaults. Every named path must exist - a flag or a typo that
    became a "path" used to scan nothing and pass."""
    if not paths:
        return list(defaults)
    roots = [Path(p) if Path(p).is_absolute() else REPO / p for p in paths]
    missing = [str(p) for p in roots if not p.exists()]
    if missing:
        raise FileNotFoundError(', '.join(missing))
    return roots


def main(argv):
    parser = argparse.ArgumentParser(prog=GATE)
    parser.add_argument('--check', action='store_true',
                        help='accepted for parity with the other gates; a finding fails either way')
    parser.add_argument('--selftest', action='store_true',
                        help='check every shape the rules judge, then exit')
    parser.add_argument('paths', nargs='*')
    args = parser.parse_args(argv[1:])
    if args.selftest:
        return selftest()
    try:
        roots = resolve(args.paths, [REPO / 'apps', REPO / 'core/server'])
    except FileNotFoundError as e:
        print(f'{GATE}: no such path: {e}', file=sys.stderr)
        return 2

    scanned = 0
    star_hits = []
    for path in starlark_files(roots):
        scanned += 1
        for n, line, snippet in scan_file(path, [STAR_ERROR_LITERAL]):
            star_hits.append((path, n, line, snippet))

    go_hits = []
    text_hits = []
    for path in go_files(roots):
        scanned += 1
        for n, line, snippet in scan_go_json(path):
            go_hits.append((path, n, line, snippet))
        for n, line, snippet in scan_file(path, GO_TEXT_PATTERNS):
            text_hits.append((path, n, line, snippet))

    if scanned == 0:
        print(f'{GATE}: scanned 0 files - nothing was checked', file=sys.stderr)
        return 2
    print(f'{GATE}: scanned {scanned} files', file=sys.stderr)

    if star_hits:
        print(f'Starlark hardcoded a.error literals ({len(star_hits)}):')
        for path, n, line, snippet in star_hits:
            rel = path.relative_to(REPO) if path.is_relative_to(REPO) else path
            print(f'  {rel}:{n}: {line}')
        print('  -> migrate to a.error.label(status, "errors.<key>", ...)')
        print()

    if go_hits:
        print(f'Go hardcoded c.JSON error/message literals ({len(go_hits)}):')
        for path, n, line, snippet in go_hits:
            rel = path.relative_to(REPO) if path.is_relative_to(REPO) else path
            print(f'  {rel}:{n}: {line}')
        print('  -> migrate to respond_error(c, status, "code", "errors.<key>", nil)')
        print()

    if text_hits:
        print(f'Go hardcoded plain-text error bodies ({len(text_hits)}):')
        for path, n, line, snippet in text_hits:
            rel = path.relative_to(REPO) if path.is_relative_to(REPO) else path
            print(f'  {rel}:{n}: {line}')
        print('  -> migrate to respond_text(c, status, "errors.<key>", nil), or mark')
        print('     machine-facing output with a trailing // i18n-ok: <reason>')
        print()

    return 1 if (star_hits or go_hits or text_hits) else 0


if __name__ == '__main__':
    sys.exit(main(sys.argv))
