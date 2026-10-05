# Tests

`verify_advanced.py` — `advanced.py` ka offline verification harness.

* PostgreSQL ki zaroorat nahi (psycopg2 stub hota hai), koi network call nahi
* Fake DB / Bot / Context / Query + jahan zaroori ho **asli** python-telegram-bot
  objects (`JobQueue`, `CallbackContext.from_job`) — isi tarah "job context me
  user_data None" wala production bug pakda gaya tha
* Coverage (sirf bot-token mode): premium emoji buttons, Add Button wizard
  (+ bulk), album + caption + buttons placement (default + toggle), album flush
  (JobQueue + fallback), admin multi-select + auto 1-day Basic, broadcast
  delivery robustness (cross-bot media, blocked/dead users, log noise), token
  masking + startup guards, network noise filter + retry + FORCE_IPV4,
  leave-recovery DMs, panel routing, app wiring, **admin ADD ACCOUNT wizard**
  (direct user_id -> token -> subscription, cancel, legacy one-line format),
  **HTML safety net** (Telegram HTML sanitize, welcome ERROR spam band),
  **initiate-blocked flow** (soft mark, pending leave-recovery DM, /start par
  auto-delivery, broadcast me hard-drop nahi), **leave-recovery per-channel
  panel** (saare channels - koi 20-cap nahi, pagination, default 🔴 OFF,
  Sab ON/OFF, one-time all-off migration), **Mini App integration** (web_app
  button, base64 rows prefill, web_app_data se buttons+text save, expired
  session handling), **network hiccup throttle**
  (ek window me ek hi WARNING) aur **diagnostics** (build tag + bot status +
  /diag report + unreachable reset)

## Chalane ka tarika
```bash
python3 -m venv /tmp/v
/tmp/v/bin/pip install "python-telegram-bot[job-queue]" pyflakes
/tmp/v/bin/python tests/verify_advanced.py     # 253 checks
```

PTB 22.x (naya) aur 21.x (purana) dono par green hona chahiye:
```bash
python3 -m venv /tmp/v21 && /tmp/v21/bin/pip install "python-telegram-bot[job-queue]==21.11.1"
/tmp/v21/bin/python tests/verify_advanced.py
```

## Direct Mini App transport (r23)

`/tmp/v/bin/python tests/test_miniapp_direct.py` exercises an actual local HTTP
server, signed synthetic Telegram initData, the bot asyncio loop, and the fake DB.
Covers direct Add/Edit buttons, existing-row prefill, premium IDs, save/readback,
clear-all, cross-user/wrong-bot/tampered/expired auth, malformed rows, deleted
messages, DB failures, and a static-file allowlist (.env/source cannot be served).
`node webapp/selftest.mjs` includes direct API ACK/failure/load-failure tests.
Live Telegram and VPS deployment still require user-side verification.

## Shared emoji packs (r24)

`/tmp/v/bin/python tests/test_emoji_packs.py`: multi-link parsing/deduplication,
custom-pack type checks, static/video/TGS cache import, refresh/remove, pagination,
partial failure, external-animation-resource rejection, and admin-only controls.
`tests/test_miniapp_direct.py` also tests authenticated `/api/packs` listing.
Frontend selftest covers explicit pack IDs (same Unicode glyph, different IDs),
serialization, authenticated catalog requests, and embedded layout wiring.
Actual Telegram downloads and animation playback on devices need a live smoke test.
