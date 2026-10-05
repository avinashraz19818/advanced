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
  Sab ON/OFF, one-time all-off migration), **network hiccup throttle**
  (ek window me ek hi WARNING) aur **diagnostics** (build tag + bot status +
  /diag report + unreachable reset)

## Chalane ka tarika
```bash
python3 -m venv /tmp/v
/tmp/v/bin/pip install "python-telegram-bot[job-queue]" pyflakes
/tmp/v/bin/python tests/verify_advanced.py     # 222 checks
```

PTB 22.x (naya) aur 21.x (purana) dono par green hona chahiye:
```bash
python3 -m venv /tmp/v21 && /tmp/v21/bin/pip install "python-telegram-bot[job-queue]==21.11.1"
/tmp/v21/bin/python tests/verify_advanced.py
```
