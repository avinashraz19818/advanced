#!/usr/bin/env python3
"""webapp/emoji-data.js generate karta hai (search-able emoji picker ke liye).

Source: PyPI package `emojis` (emoji + aliases + tags + category ka database).
Isi tarah picker me "pizza" likhne par 🍕 mil jata hai, aur emojis category-wise
(smileys, food, symbols...) dikhte hain — bilkul Telegram/GroupHelp jaisa.

Ek baar chalana hai (ya jab emoji list update karni ho):
    pip download emojis -d /tmp/emopkg --no-deps   # ya: pip install emojis
    python3 webapp/gen_emoji_data.py
"""
import json
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(HERE, "emoji-data.js")

# Category order + UI icon (picker me isi order me tabs/dikhenge)
CATEGORY_ICONS = {
    "Smileys & Emotion": "😀",
    "People & Body": "🙋",
    "Animals & Nature": "🐻",
    "Food & Drink": "🍕",
    "Travel & Places": "✈️",
    "Activities": "🎯",
    "Objects": "💡",
    "Symbols": "❤️",
    "Flags": "🏁",
}
MAX_WORDS = 7          # ek emoji ke keywords (file size kam rakhne ke liye)


def load_db():
    # pehle normal import try karo, warna /tmp/ext se (pip download kiya hua)
    for extra in (None, "/tmp/ext", "/tmp/emopkg"):
        if extra and os.path.isdir(extra) and extra not in sys.path:
            sys.path.insert(0, extra)
        try:
            from emojis.db import get_emojis_by_category, get_categories  # noqa
            break
        except Exception:
            continue
    else:
        raise SystemExit("`emojis` package nahi mila. Chalao: pip install emojis")
    emoji_names = {}
    try:  # `emoji` package ke official naam bhi keywords me (search behtar hota hai)
        import emoji as emoji_pkg
        for ch, meta in emoji_pkg.EMOJI_DATA.items():
            name = (meta.get("en") or "").strip(":")
            if name:
                emoji_names[ch] = name.replace("_", " ")
    except Exception as ex:
        print(f"(note: `emoji` package nahi mila - sirf aliases se keywords: {ex})")
    return get_categories(), get_emojis_by_category, emoji_names


def keywords(entry, emoji_names):
    words = []
    for alias in (entry.aliases or []):
        words.extend(str(alias).replace("_", " ").split())
    for tag in (entry.tags or []):
        words.extend(str(tag).replace("_", " ").split())
    official = emoji_names.get(entry.emoji, "")
    if official:
        words.extend(official.split())
    out, seen = [], set()
    for w in words:
        w = w.strip().lower()
        if len(w) < 2 or w in seen:
            continue
        seen.add(w)
        out.append(w)
        if len(out) >= MAX_WORDS:
            break
    return " ".join(out)


def main():
    categories, by_cat, emoji_names = load_db()
    groups = []
    for cat in list(CATEGORY_ICONS.keys()) + [c for c in sorted(categories) if c not in CATEGORY_ICONS]:
        try:
            items = list(by_cat(cat) or [])
        except Exception:
            items = []
        if not items:
            continue
        seen, pairs = set(), []
        for e in items:
            ch = e.emoji
            if ch in seen:
                continue
            seen.add(ch)
            pairs.append([ch, keywords(e, emoji_names)])
        groups.append({"name": cat, "icon": CATEGORY_ICONS.get(cat, "•"), "items": pairs})

    payload = {"groups": groups}
    js = ("/* Emoji picker data — webapp/gen_emoji_data.py se generate hota hai.\n"
          "   Source: PyPI `emojis` package (aliases + tags + category). Hataye mat. */\n"
          "window.EMOJI_GROUPS = " + json.dumps(payload["groups"], ensure_ascii=False,
                                                separators=(",", ":")) + ";\n")
    open(OUT, "w", encoding="utf-8").write(js)
    total = sum(len(g["items"]) for g in groups)
    size_kb = os.path.getsize(OUT) / 1024
    print(f"✅ {total} emojis, {len(groups)} categories -> {OUT} ({size_kb:.0f} KB)")
    for g in groups:
        print(f"   {g['icon']} {g['name']}: {len(g['items'])}")


if __name__ == "__main__":
    main()
