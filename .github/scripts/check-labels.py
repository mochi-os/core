#!/usr/bin/env python3
# Copyright © 2026 Mochisoft OÜ
# SPDX-License-Identifier: AGPL-3.0-only
# This file is part of Mochi, licensed under the GNU AGPL v3 with the
# Mochi Application Interface Exception - see license.txt and license-exception.md.

"""CI guard: every key in server/labels/en.conf must be translated in all
sibling <lang>.conf catalogs. Exits non-zero with a per-locale breakdown.

"Translated" means present and non-empty. A value identical to the English is
allowed only when every alphabetic word in the source is a keep-word - a brand
or protocol token, loanword, or colour name.

KEEP_WORDS is an inline copy of claude/scripts/i18n_glossary.py in the monorepo
(separate repo, so it cannot be imported). Keep the two in sync.
"""
import re
import sys
from pathlib import Path

LABELS = Path(__file__).resolve().parents[2] / "server" / "labels"

# Locales that are intentionally English (the source and its regional English
# variants) or are maintained as overlays on another base — never gap-checked.
# Mirrors conf-refresh.py's OVERLAY set in the monorepo.
OVERLAY = {"en", "en-us", "en-ca", "fr-ca", "es-ar", "zh-hk", "yue", "de-ch"}

# Lowercase alphabetic keep-words: brand/protocol nouns, loanwords, colours.
KEEP_WORDS = {
    "air", "api", "apps", "chat", "chess", "comptroller", "crm", "data",
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
    "Data", "Disputes", "Email", "Feeds", "Forums", "Git", "GitHub", "Go",
    "Google", "Help", "Home", "ID", "Invitations", "JWT", "Market",
    "Matcha", "Mentions", "Menu", "Messages", "Mochi", "Moderation",
    "Normal", "Notifications", "OAuth", "OIDC", "Offline", "P2P", "PGN",
    "PKCE", "PayPal", "Pushbullet", "QR", "RSS", "Replica", "Rose",
    "SAML", "SGF", "SHA", "Server", "Steel", "Stripe", "Teal",
    "Terracotta", "URL", "Version", "Violet", "Wiki", "Wikis", "libp2p",
    "ntfy",
})

def _strip_placeholders(value):
    """Remove every {...} group, nested ICU constructs included - a regex stops at the first closing
    brace and leaves ` other {#m}}` behind. Mirrors i18n_glossary.py strip_placeholders; keep in sync."""
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


# Cells where the English spelling IS the target language's word, so a value
# equal to the source is finished rather than missing. Scoped to one (locale,
# source) pair each, never to a word: "Interval" is Danish and Czech, but
# Polish wants "Interwał", Turkish "Aralık" and Finnish "Aikaväli". Mirrors
# KEEP_LOCALE in claude/scripts/i18n_glossary.py — keep the two in sync.
KEEP_LOCALE = frozenset({
    ("ca", "Interval"), ("cs", "Interval"), ("da", "Interval"),
    ("id", "Interval"), ("jv", "Interval"), ("nl", "Interval"),
    ("nl-be", "Interval"), ("ro", "Interval"), ("sk", "Interval"),
    ("sl", "Interval"), ("su", "Interval"),
    ("da", "Region"), ("de", "Region"), ("nb", "Region"),
    ("nn", "Region"), ("sv", "Region"),
    ("da", "Type"), ("fr", "Type"), ("nb", "Type"), ("nn", "Type"),
    ("fr", "Description"), ("fr", "Notes"), ("sv", "Information"),
    # "Error" is the Catalan and Spanish word, and the loanword Filipino UI
    # uses; Javanese has "Kesalahan".
    ("ca", "Error"), ("es", "Error"), ("es-419", "Error"), ("tl", "Error"),
    # The account-provider field labels. Every cell here came from
    # clients/android's own settings catalogue, where the same English source is
    # already translated - so these are the translators' own decisions that the
    # word is identical, not an unfilled cell. "Model" is the same word across
    # the Germanic, Slavic, Romance and Malay families listed; "Name" is
    # identical in German.
    ('af', 'Model'),
    ('az', 'Model'),
    ('bs', 'Model'),
    ('ca', 'Model'),
    ('cs', 'Model'),
    ('cy', 'Model'),
    ('da', 'Model'),
    ('ha', 'Model'),
    ('hr', 'Model'),
    ('id', 'Model'),
    ('jv', 'Model'),
    ('ku', 'Model'),
    ('ms', 'Model'),
    ('nl', 'Model'),
    ('nl-be', 'Model'),
    ('pl', 'Model'),
    ('ro', 'Model'),
    ('si', 'Model'),
    ('sk', 'Model'),
    ('sl', 'Model'),
    ('tk', 'Model'),
    ('tl', 'Model'),
    ('tr', 'Model'),
    ('uz', 'Model'),
    ('lo', 'API key'),
    ('my', 'API key'),
    ('tl', 'API key'),
    ('de', 'Name'),
    # "default" as an account form's model placeholder. Indonesian, Maltese,
    # Quechua and Tagalog all keep the loanword: their own web and Android
    # catalogues already render it "default" in place ("Kont default", "Default
    # na account", "Akun default"). Javanese does translate it, as "standar".
    ('id', 'default'), ('mt', 'default'), ('qu', 'default'), ('tl', 'default'),
})


def keep_english(source, locale=None):
    """True when leaving `source` in English is acceptable (not a gap)."""
    source = source.strip()
    if source in KEEP_ENGLISH:
        return True
    if locale and (locale, source) in KEEP_LOCALE:
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



# The smallest legitimate catalogue set, for a repo with no web/src/locales to
# measure against. Deliberately loose: it only has to catch a set that has
# COLLAPSED, not police a locale or two.
FLOOR = 90


def expected():
    """The locales this repo is supposed to carry, or None if nothing to
    measure against.

    The reference is this repo's OWN web/src/locales — the same locale set one
    layer up, and present in this checkout, which matters because CI checks out
    only this app. Overlay variants are dropped: they are Lingui fallbacks that
    legitimately have no .conf.
    """
    web = LABELS.parent / "web" / "src" / "locales"
    if not web.is_dir():
        return None
    return {d.name for d in web.iterdir() if d.is_dir() and d.name not in OVERLAY} or None


def missing_catalogues():
    """Locales that ought to exist here and do not.

    The key check below only inspects the .conf files it FINDS, so a repo that
    has lost its catalogues passes trivially and one carrying only en.conf
    passes vacuously — then prints "All server labels translated in every
    locale", which is not silence but a false statement. Air shipped with 2 of
    99 that way and neither gate objected.
    """
    have = {c.stem for c in LABELS.glob("*.conf")}
    want = expected()
    if want is None:
        return [f"only {len(have)} catalogues, below the {FLOOR} floor"] if len(have) < FLOOR else []
    return sorted(want - have)


def main():
    en = parse(LABELS / "en.conf")
    # Presence BEFORE keys: an absent catalogue is the larger failure, and the
    # key check cannot see it.
    gone = missing_catalogues()
    if gone:
        shown = ", ".join(gone[:8]) + (" ..." if len(gone) > 8 else "")
        print(f"Missing label catalogues: {len(gone)} — {shown}")
        return 1
    # A regional catalogue whose parent is present falls through to it key by
    # key (core's language_fallbacks strips subtags), so it is allowed to carry
    # only what differs - a verbatim copy of the parent overrides nothing and
    # pins this locale to a wording the parent may later change. Deliberately
    # narrow: zh-hans and zh-hant have no zh.conf, so they stay fully gated.
    # Mirrors conf_inherits in claude/scripts/i18n_glossary.py.
    present = {c.stem for c in LABELS.glob("*.conf")}
    inherits = {loc for loc in present if "-" in loc and loc.rsplit("-", 1)[0] in present}
    failures = {}
    for conf in sorted(LABELS.glob("*.conf")):
        if conf.stem in OVERLAY or conf.stem in inherits:
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
            elif value == en_value and not keep_english(en_value, conf.stem):
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
