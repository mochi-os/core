#!/usr/bin/env python3
# Copyright © 2026 Mochisoft OÜ
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
    "air", "api", "apps", "chat", "chess", "comptroller", "crm",
    "disputes", "email", "feeds", "forums", "git", "github", "go",
    "google", "help", "home", "id", "invitations", "jwt", "libp2p",
    "market", "matcha", "mentions", "menu", "messages", "mochi",
    "moderation", "normal", "notifications", "ntfy", "oauth", "offline",
    "oidc", "paypal", "pgn", "pkce", "pushbullet", "qr", "replica",
    "rose", "rss", "saml", "server", "sgf", "sha", "steel", "stripe",
    "teal", "terracotta", "url", "version", "violet", "wiki", "wikis",
}

# Exact-string allowlist, checked before word matching. A digit-bearing
# token only matches here: _WORD finds alphabetic runs, so "libp2p" splits
# into "libp" and "p" and is in no word list. Mirrors KEEP_ENGLISH in the
# monorepo's claude/scripts/i18n_glossary.py — keep the two in sync.
KEEP_ENGLISH = frozenset({
    "API", "Air", "Apps", "CRM", "Chat", "Chess", "Comptroller",
    "Disputes", "Email", "Feeds", "Forums", "Git", "GitHub", "Go",
    "Google", "Help", "Home", "ID", "Invitations", "JWT", "Market",
    "Matcha", "Mentions", "Menu", "Messages", "Mochi", "Moderation",
    "Normal", "Notifications", "OAuth", "OIDC", "Offline", "P2P", "PGN",
    "PKCE", "PayPal", "Pushbullet", "QR", "RSS", "Replica", "Rose",
    "SAML", "SGF", "SHA", "Server", "Steel", "Stripe", "Teal",
    "Terracotta", "URL", "Version", "Violet", "Wiki", "Wikis", "libp2p",
    "ntfy",
})

def _strip_placeholders(value):
    """Remove every {...} group, including nested ICU constructs.

    A regex cannot do this: a {...} pattern stops at the first closing brace,
    so `{count, plural, one {#m} other {#m}}` loses `{count, plural, one {#m}`
    and leaves ` other {#m}}` behind - which then reads as the English word
    "other" and makes a translated plural look like untranslated prose.
    Mirrors claude/scripts/i18n_glossary.py strip_placeholders; keep in sync.
    """
    out = []
    depth = 0
    for c in value:
        if c == "{":
            depth += 1
        elif c == "}":
            if depth:
                depth -= 1
            else:
                out.append(c)
        elif not depth:
            out.append(c)
    return "".join(out)


_WORD = re.compile(r"[A-Za-z]+")


def translatable(value):
    """False when stripping {placeholders} leaves no letters — "{listing}" and
    "{author}: {excerpt}" are pure substitution tokens, so the correct
    translation is the English string byte for byte. Mirrors real() in the
    monorepo's conf-refresh.py, which is why that checker accepts them."""
    return bool(re.search(r"[A-Za-z]", _strip_placeholders(value)))


def keep_english(source):
    """True when leaving `source` in English is acceptable (not a gap)."""
    source = source.strip()
    if source in KEEP_ENGLISH:
        return True
    words = _WORD.findall(_strip_placeholders(source))
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
            # Nothing to translate and nothing to miss: resolve_label falls back
            # through language_fallbacks to "en", so an absent placeholder-only
            # key renders the same string in every language. conf-refresh.py
            # skips these the same way.
            if not translatable(en_value):
                continue
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
