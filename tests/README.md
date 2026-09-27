# Tests

`verify_advanced.py` — `advanced.py` ka offline verification harness.

* PostgreSQL ki zaroorat nahi (psycopg2 stub hota hai), koi network call nahi
* Fake DB / Bot / Context / Query + jahan zaroori ho **asli** python-telegram-bot
  objects (`JobQueue`, `CallbackContext.from_job`) — isi tarah "job context me
  user_data None" wala production bug pakda gaya tha
* Coverage: premium emoji buttons, ➕ Add Button wizard (+ bulk), album + caption +
  buttons placement (default + toggle), album flush (JobQueue + fallback), admin
  multi-select + auto 1-day Basic, broadcast delivery robustness (cross-bot media,
  blocked/dead users, log noise), token masking + startup guards, network noise
  filter + userbot retry + FORCE_IPV4, leave-recovery DMs, panel routing, app wiring,
  **user account (MTProto) mode** (phone/OTP/2FA login wizard, session save, premium
  emoji + buttons->links adapter, media/album translate, throttle, flood/blocked error
  translation, start/stop/self-heal, join request + pending list, panel routing),
  **admin ADD ACCOUNT wizard** (choose bot/user -> user_id -> token/phone -> OTP,
  quick subscription, cancel, legacy one-line format) aur **HTML safety net**
  (Telegram HTML sanitize, hint text safe, welcome ERROR spam band), **user account owner flows** (main-bot message -> userbot handler adapter, channel add / welcome set, account ka apna id owner hone par /start panel, aur account ke DM me /start par panel wapas)

## Chalane ka tarika
```bash
python3 -m venv /tmp/v
/tmp/v/bin/pip install "python-telegram-bot[job-queue]" pyflakes telethon
/tmp/v/bin/python tests/verify_advanced.py     # 291 checks
```

PTB 22.x (naya) aur 21.x (purana) dono par green hona chahiye:
```bash
python3 -m venv /tmp/v21 && /tmp/v21/bin/pip install "python-telegram-bot[job-queue]==21.11.1" telethon
/tmp/v21/bin/python tests/verify_advanced.py
```
