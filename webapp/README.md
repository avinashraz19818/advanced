# Button Builder — direct Add/Edit Mini App (r23)

## Supported direct flow

With `WEBAPP_API_URL` configured, every `button_builder_row` is a **single inline
web_app button**: **Add Button** when empty, **Edit Buttons** when buttons exist.
There is no separate Mini App tab, Paste Many tab, or manual-choice screen.

The frontend and API are served together by `miniapp_bridge.py` from the bot's
VPS through HTTPS. Telegram initData is HMAC-verified against the originating
bot token, user ID, timestamp and a random one-hour session. Only whitelisted
frontend files are served; neither `.env` nor Python source is publicly served.
Existing text/buttons load through authenticated POST, not URL query contents.
Message text is read-only in this button editor.

SAVE writes through the existing message/album/draft/leave-message store,
reads back the stored buttons, then returns a saved acknowledgement. The app
closes only after this acknowledgement. A best-effort Telegram preview and
navigation message are also sent. On failure, the editor stays open.
Deleting all buttons and saving is supported. Saved message changes affect
future sends; previous messages delivered to recipients are not retroactively
edited. Broadcast SAVE updates the draft; it does NOT mass-send a broadcast.

## VPS setup without a domain / Cloudflare credentials

```bash
cd ~/advanced &&
git fetch origin arena/01a1067e-advanced &&
git merge --ff-only FETCH_HEAD &&
bash scripts/enable-miniapp
```

The script downloads the official Linux cloudflared binary if unavailable,
starts/reuses a Cloudflare Quick Tunnel to loopback port 8110, updates only
`WEBAPP_URL`, `WEBAPP_API_URL`, `WEBAPP_PORT` in `.env`, and runs `./start`.
The existing bot credentials remain in `.env`; no credentials are requested.
The tunnel PID/log/binary live outside Git in `~/.advanced-miniapp/`.
Open a **fresh bot panel** after updating: Telegram does not rewrite old keyboards.
No ZIP upload to the old static Cloudflare Worker is needed for this mode.

**Limitations:** Quick Tunnel is for testing and has no uptime guarantee. Its URL
changes if restarted and the tunnel is NOT a reboot-persistent service. After VPS
reboot/tunnel exit run `bash scripts/enable-miniapp` again, then reopen bot panels.
Do not treat this as production high-availability deployment. For a permanent URL,
configure a named Cloudflare Tunnel with a domain and systemd; route its HTTPS host
to `http://127.0.0.1:8110`, set `WEBAPP_API_URL=https://your-host`, then restart bot.
Keep port 8110 private. The frontend calls relative `/api/load` and `/api/save`,
never browser localhost. In-memory sessions expire on restart (reopen the panel).

Changing hosting URLs does not make the GitHub repo private. Change repository
visibility separately if the backend source must not be publicly downloadable.

## Legacy static ZIP mode

`downloads/button-studio.zip` contains only HTML and emoji data, not this backend.
It CANNOT provide direct inline-button saves alone. With only `WEBAPP_URL` and no
`WEBAPP_API_URL`, the old callback/reply-keyboard launch remains a fallback.
Keyboard-mode `sendData` allows 4096 bytes and may have empty initData. Browser
previews and unsupported inline sendData launches cannot save to the bot.

## Button types and emojis

- **Link khule**: opens a website/channel/register URL.
- **Bot kuch kare**: a bot callback, e.g. `live_chat_support` or `start_now` in userbots.
  Custom actions need corresponding bot handlers.
- 1849 searchable emojis; premium picker mapping is generated from the bot's
  `EMOJI_IDS` (65 exact entries). Telegram renders premium icons; browser preview
  uses ordinary emoji glyphs. Add IDs to advanced.py, then regenerate the mapping.

## Tests

```bash
node webapp/selftest.mjs                         # 44 frontend checks
python3 webapp/gen_emoji_map.py --check           # premium mapping sync
/tmp/v/bin/python tests/verify_advanced.py        # 253 bot checks
/tmp/v/bin/python tests/test_miniapp_direct.py    # HTTP + HMAC + DB integration
```

Direct integration tests use signed synthetic Telegram data and a fake database;
they do not constitute a live Telegram/VPS deployment test.
