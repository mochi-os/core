#!/usr/bin/env python3
# Copyright © 2026 Mochi OÜ
# SPDX-License-Identifier: AGPL-3.0-only
# This file is part of Mochi, licensed under the GNU AGPL v3 with the
# Mochi Application Interface Exception - see license.txt and license-exception.md.

"""CI guard: every key in server/labels/en.conf must be translated in all
sibling <lang>.conf catalogs.

"Translated" means present and non-empty. A value identical to the English
source is allowed only when the source is acceptable-English: every alphabetic
word in it (placeholders stripped) is a keep-word — a brand/protocol token,
loanword, or colour name. A real error or notification message always has an
ordinary word, so it is never exempted; only standalone brand/loanword labels
are.

Exits non-zero with a per-locale breakdown when any catalog is incomplete, so
the i18n-check workflow fails the PR.

KEEP_WORDS is an inline copy of the canonical list in the monorepo's
claude/scripts/i18n_glossary.py (that module can't be imported here — separate
repo). Keep the two in sync.
"""
import re
import sys
from pathlib import Path

LABELS = Path(__file__).resolve().parents[2] / "server" / "labels"

# Locales that are intentionally English (the source and its regional English
# variants) or are maintained as overlays on another base — never gap-checked.
# Mirrors conf-refresh.py's OVERLAY set in the monorepo.
OVERLAY = {"en", "en-us", "en-ca", "fr-ca", "es-ar", "zh-hk", "yue", "nn", "de-ch"}

# Lowercase alphabetic keep-words: brand/protocol nouns, loanwords, colours.
KEEP_WORDS = {
    "mochi", "stripe", "github", "pushbullet", "ntfy", "oauth", "oidc",
    "pkce", "jwt", "saml", "libp2p", "crm", "comptroller", "git", "google",
    "paypal", "api", "url", "id", "rss", "qr", "sgf", "pgn", "sha",
    "apps", "chat", "chess", "disputes", "email", "feeds", "forums", "go",
    "help", "home", "market", "mentions", "menu", "messages", "moderation",
    "notifications", "offline", "replica", "rose", "teal", "violet",
    "wiki", "wikis", "server",
}

_WORD = re.compile(r"[A-Za-z]+")
_PLACEHOLDER = re.compile(r"\{[^}]*\}")


def keep_english(source):
    """True when leaving `source` in English is acceptable (not a gap)."""
    words = _WORD.findall(_PLACEHOLDER.sub("", source))
    return bool(words) and all(w.lower() in KEEP_WORDS for w in words)


def parse(path):
    out = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        out[key.strip()] = value.strip()
    return out


def main():
    en = parse(LABELS / "en.conf")
    failures = {}
    for conf in sorted(LABELS.glob("*.conf")):
        if conf.stem in OVERLAY:
            continue
        translated = parse(conf)
        missing = []
        for key, en_value in en.items():
            value = translated.get(key, "")
            if not value:
                missing.append(key)
            elif value == en_value and not keep_english(en_value):
                missing.append(key)
        if missing:
            failures[conf.stem] = missing
    if failures:
        total = sum(len(v) for v in failures.values())
        print(f"Untranslated server labels: {total} across {len(failures)} locales")
        for lang in sorted(failures):
            keys = failures[lang]
            print(f"  {lang}: {len(keys)} missing (e.g. {', '.join(keys[:5])})")
        return 1
    print("All server labels translated in every locale.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
