# Chat button builder — r27

The Mini App/Cloudflare integration and Emoji Packs admin panel have been removed.
All button editing happens in the existing Telegram chat wizard again.

1. Open **Add Button** and send the label (Telegram premium emoji entities supported).
2. Send the URL (or existing `cb:action` format).
3. Tap a sample of your button: **Blue / Green / Red / Default**.
4. Add Same Row / Add New Row, Preview, Undo Last, or **Save & Done**.

The selected color and premium icon are preserved when saved. Default uses Telegram's
native uncolored appearance; actual styling availability depends on Telegram client
and API support. Existing **Paste Many** flow is unchanged.

## Upgrade from a Mini App build

```bash
cd ~/advanced &&
git fetch origin arena/01a1067e-advanced &&
git merge --ff-only FETCH_HEAD &&
bash scripts/disable-miniapp &&
./start
```

The cleanup script removes only WEBAPP_URL/WEBAPP_API_URL/WEBAPP_PORT from `.env`
and stops the recorded Quick Tunnel only after verifying its process command.
Other credentials, messages, saved button configurations and imported pack caches
are NOT deleted. A separately created static Cloudflare Worker is not deleted by
this script; it is no longer used by the bot and can be removed in Cloudflare.
Old Telegram messages cannot be rewritten automatically: reopen the bot panel.

The bot neither launches an HTTP preview backend nor consumes Mini App save data.
No domain, browser, ZIP upload or Cloudflare setup is needed for this wizard.
