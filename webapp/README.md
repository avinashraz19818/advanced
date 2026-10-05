# Button Builder — direct Add/Edit Mini App (r25)

## r25 fixes and keyboard-style picker

- Album JobQueue contexts now retain the initiating user's ID as well as their
  user_data. Sessions bind to this actor, never a managed bot owner or group ID.
  Missing identity fails closed (legacy callback fallback), not an int(None) crash.
- Rounded emoji search field, compact lazy-loaded pack-cover tabs (names shown in
  active-pack heading/tooltips), denser transparent tiles and touch-scroll grid.
- Search across installed admin packs by Unicode emoji, English emoji keywords
  (fire, heart, diamond, etc.), or pack title/name. Searches are paginated/debounced;
  this matches metadata, not visual recognition of arbitrary sticker artwork.
- **Premium icon** keeps the exact animated custom emoji ID in the Telegram button's
  fixed leading icon slot (one per button). **Text me insert** inserts the associated
  NORMAL Unicode glyph at the label's caret/replaces selected text, before/in/after
  the name. It does NOT create arbitrary animated custom emoji entities in button
  labels: Telegram's inline-button text field does not support those entities.

## Admin-managed premium emoji packs (r24)

Main bot: **Admin Panel → Emoji Packs → Add Pack Links**. Send one or more
`t.me/addemoji/PackName` links (up to 20 per message; send more in the next message).
There is no fixed total pack count cap; VPS disk/database/network capacity applies.
Only admins can import, refresh or remove packs. Packs are a SHARED library visible
to all Mini App users, not an automatic import of each user's Telegram account.

Imports use Telegram getStickerSet/getFile and store actual custom_emoji_id values.
Links to normal sticker sets are rejected. Each pack is published only after all
its assets are downloaded; failure leaves an existing version intact. Resending a
link refreshes the pack. The admin list has pagination and removal controls.

The **direct Mini App** embeds this library inside the button editor below Emoji,
not in a separate overlay. It does not display the fixed/default picker there.
Search pack names, switch packs, and page through 48 emojis at a time. TGS assets
are decompressed into Lottie JSON and rendered by vendored lottie-web 5.13.0 light
(SVG renderer, MIT license included). WEBM uses muted looping video; static WEBP
uses images. Offscreen animations pause and leaving the editor destroys players.
Unsupported assets/codecs fall back to the associated Unicode glyph. Selected
custom IDs remain distinct even when several emojis share the same Unicode glyph.

Metadata persists in PostgreSQL `system_settings.miniapp_emoji_packs_v1`.
Downloaded assets live in `.emoji-assets/` (gitignored), surviving ordinary bot
restarts and Git pulls. Back up this directory with the DB on VPS migrations, or
resend pack links to download missing assets. Removing a pack removes it from the
picker, not from already-saved buttons; cached files are retained. The bridge serves
only hash-named asset files, never Telegram token-bearing download URLs. Use the
HTTPS backend mode; a static ZIP alone cannot fetch this authenticated catalog.

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
node webapp/selftest.mjs                         # 57 frontend checks
python3 webapp/gen_emoji_map.py --check           # premium mapping sync
/tmp/v/bin/python tests/verify_advanced.py        # 253 bot checks
/tmp/v/bin/python tests/test_miniapp_direct.py    # HTTP + HMAC + DB integration
```

Direct integration tests use signed synthetic Telegram data and a fake database;
they do not constitute a live Telegram/VPS deployment test.

Pack importer/admin tests: `/tmp/v/bin/python tests/test_emoji_packs.py` (offline fakes).
