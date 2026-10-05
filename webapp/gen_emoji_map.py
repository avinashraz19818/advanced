#!/usr/bin/env python3
"""index.html ke andar premium emoji ka map advanced.py se regenerate karta hai.

advanced.py ke `EMOJI_IDS` (char -> premium emoji id) hi bot ke buttons me lagte hain.
Mini App ka picker wahi emojis dikhata hai, isliye dono jagah same map hona chahiye.

Chalane ka tarika:   python3 webapp/gen_emoji_map.py
"""
import os
import re
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
HTML = os.path.join(HERE, "index.html")
ADVANCED = os.path.join(REPO, "advanced.py")

START = "/* EMOJI_MAP_START"
END = "/* EMOJI_MAP_END */"


def read_emoji_ids():
    src = open(ADVANCED, encoding="utf-8").read()
    m = re.search(r"EMOJI_IDS = \{(.*?)\n\}", src, re.S)
    if not m:
        raise SystemExit("advanced.py me EMOJI_IDS nahi mila")
    pairs, seen = [], set()
    for ch, eid in re.findall(r'"([^"]+)":\s*"(\d+)"', m.group(1)):
        if ch in seen:
            continue
        seen.add(ch)
        pairs.append((ch, eid))
    if not pairs:
        raise SystemExit("EMOJI_IDS khaali hai")
    return pairs


def read_html_map():
    html = open(HTML, encoding="utf-8").read()
    m = re.search(re.escape(START) + r".*?const EMOJI_MAP = \{(.*?)\};", html, re.S)
    if not m:
        raise SystemExit("index.html me EMOJI_MAP nahi mila")
    out = {}
    for ch, eid in re.findall(r'"([^"]+)":\s*"(\d+)"', m.group(1)):
        out.setdefault(ch, eid)
    return out


def check():
    """Verify karo ki picker me bilkul wahi premium emojis hain jo bot (advanced.py) me hain."""
    want = dict(read_emoji_ids())
    have = read_html_map()
    problems = []
    for ch, eid in want.items():
        if have.get(ch) != eid:
            problems.append(f"  {ch}: bot={eid} picker={have.get(ch)}")
    for ch in have:
        if ch not in want:
            problems.append(f"  {ch}: picker me extra hai (bot me nahi)")
    if problems:
        print("❌ Mini App picker bot ke EMOJI_IDS se match NAHI karta:")
        print("\n".join(problems))
        print("\nTheek karne ke liye: python3 webapp/gen_emoji_map.py")
        return 1
    print(f"✅ {len(want)} premium emojis bot (advanced.py) aur Mini App picker me bilkul same hain")
    return 0


def main():
    if "--check" in sys.argv:
        raise SystemExit(check())
    pairs = read_emoji_ids()
    lines = [START + " — premium emoji (advanced.py ke EMOJI_IDS se, gen_emoji_map.py se banta hai) */",
             "const EMOJI_MAP = {"]
    for ch, eid in pairs:
        lines.append(f'  "{ch}": "{eid}",')
    lines.append("};")
    lines.append(END)
    block = "\n".join(lines)

    html = open(HTML, encoding="utf-8").read()
    start = html.index(START)
    end = html.index(END) + len(END)
    html = html[:start] + block + html[end:]
    open(HTML, "w", encoding="utf-8").write(html)
    print(f"✅ {len(pairs)} premium emojis index.html me likh diye")


if __name__ == "__main__":
    main()
