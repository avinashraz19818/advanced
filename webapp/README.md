# 🌐 Button Builder — Telegram Mini App (Premium)

`index.html` = **premium visual button builder** (GroupHelp jaisa), jo bilkul usi format me
output deta hai jo `advanced.py` use karta hai:

```json
[[{"text":"Register","url":"https://t.me/x","style":"success","icon_char":"🔗"}]]
```

* `style` = `primary` (blue) / `success` (green) / `danger` (red) — khaali = transparent
* `cb` = callback (bot action), `url` = link
* `icon_id` + `icon_char` = premium emoji (bot ke `EMOJI_IDS` se)

## Button ke 2 type (client ko yahi samajhna hai)

| Type | Kab use kare | Example |
|---|---|---|
| **🔗 Link khule** | Website / channel / register link kholna ho (99% buttons yahi hain) | `https://veergame41.com/#/register?invitationCode=…`, `@yourchannel` |
| **⚙️ Bot kuch kare** | Koi link nahi — bot khud kuch kare | 💬 **Live Chat Support** bheje, 🏠 **Welcome / Menu dobara** bheje |

* **Link** wala button sab jagah chalta hai (kisi ke bhi chat me).
* **Bot action** ke liye action aapke bot me pehle se hona chahiye — Mini App me
  sirf wahi ready-made actions dikhti hain jo bot me already kaam karti hain
  (`live_chat_support`, `start_now`), aur ek "Custom (advanced)" option bhi hai
  (wo tab kaam karega jab bot me us naam ka handler ho).

## Features (client-facing — koi JSON/emoji-id nahi dikhta)

| Feature | Kya karta hai |
|---|---|
| **Live preview** | Asli Telegram post card jaisa — message + buttons (colors/emojis ke saath) |
| **Message box** | Jo message ke neeche buttons lagenge |
| **Rows editor** | `＋ Naya row`, per-row `＋` (row me max 8), row delete |
| **Tap = edit sheet** | Naam, Link/Bot-action, color, emoji |
| **Emoji picker (Telegram jaisa)** | 🔍 **Search** (`pizza` → 🍕), ⭐️ **Premium tab** (bot ke 65 premium emojis, ★ badge), 🕘 **Recent**, + 9 categories (1849 emojis) |
| **Premium emojis** | Jo emojis bot me premium id wale hain unpar ★ badge; select karne par `icon_id` + `icon_char` dono save |
| **⬆️⬇️⬅️➡️** | Button ko row/dabba badlo |
| **SAVE** | Telegram me: seedha bot ko (`sendData`). Browser me: JSON copy |
| **Auto-save draft** | localStorage me kaam save (galti se band ho to bhi kaam nahi jata) |

### Emoji data (search ke liye)
`emoji-data.js` — 1849 emojis, 9 categories, keywords ke saath (jaise Telegram ka search).
Regenerate: `pip install emojis emoji && python3 webapp/gen_emoji_data.py`

## Abhi kaise chalta hai (do modes)

1. **Browser mode (abhi)** — local/preview me khulta hai. SAVE karne par JSON milta hai
   jo aap bot ke "Paste Many" me paste kar sakte ho.
2. **Telegram Mini App mode** — jab ye page HTTPS URL par host ho aur bot `web_app`
   button se khole, to SAVE karne par buttons **seedha bot me** save ho jate hain
   (`Telegram.WebApp.sendData` → bot ko `web_app_data` message milta hai).

## Mini App banane ke liye kya chahiye

| Cheez | Kyun | Free tareeka |
|---|---|---|
| **HTTPS URL** | Telegram sirf `https://` mini app URL allow karta hai | Cloudflare Tunnel (free, domain ke bina bhi) ya apna domain + `certbot` |
| **Bot-side handler** | `web_app_data` se aaya JSON lena + save karna | `advanced.py` me ~40 lines (niche plan) |

### Deployment — 3 options

**A. Cloudflare Tunnel (domain ke bina, sabse fast)**
```bash
cd ~/advanced/webapp && python3 -m http.server 8110 --bind 127.0.0.1
# doosri terminal me:
cloudflared tunnel --url http://127.0.0.1:8110     # free, turant https URL deta hai
```

**B. Apna domain + nginx + free SSL (permanent)**
```bash
sudo apt install -y nginx certbot python3-certbot-nginx
# nginx me /var/www/buttonbuilder -> ~/advanced/webapp serve karo
sudo certbot --nginx -d yourdomain.com              # HTTPS free
```

**C. Static hosting** (Vercel/Netlify/GitHub Pages) — HTML waisa hi chalega, bas
`sendData` ke liye wahi page Telegram ke andar khulna chahiye.

## Bot me integration (ho chuka hai ✅)

1. `.env` me sirf ek line:
   ```bash
   WEBAPP_URL=https://<aapka-https-url>/
   ```
2. Bot me ab automatic:
   * Button builder panel me **🌐 Mini App** tab aata hai (`button_builder_row`) — purane
     buttons URL me prefill ho kar aate hain (`tid`, `kind`, `rows`, `text`).
   * Mini App ka **SAVE** → bot ko `web_app_data` message → buttons **aur** message text
     wahi save hote hain jahan wizard save karta tha (`target_save_rows` +
     `apply_mini_app_text`) — register link, welcome, broadcast draft, leave message sab.
   * Save hone par chat me confirm + buttons ka preview aata hai.
   * Agar session purana ho gaya (`tid` expire) → saaf message: "dobara kholo".

## Hosting (HTTPS) — 3 tareeke

**A. GitHub Pages (free, permanent, repo already public hai)**
1. GitHub → repo → **Settings → Pages**
2. Source: **Deploy from a branch** → Branch: `main` (ya `arena/01a1067e-advanced`) →
   Folder: **/webapp** → Save
3. URL banega: `https://avinashraz19818.github.io/advanced/`
4. `.env` me: `WEBAPP_URL=https://avinashraz19818.github.io/advanced`

**B. Cloudflare Tunnel (turant, domain ke bina)**
```bash
cd ~/advanced/webapp && python3 -m http.server 8110 --bind 127.0.0.1
cloudflared tunnel --url http://127.0.0.1:8110     # free https URL deta hai
```

**C. Apna domain + nginx + certbot** (`sudo certbot --nginx -d buttons.yourdomain.com`)

## Test

```bash
node webapp/selftest.mjs                    # 31 checks (JS <-> Python format match)
python3 webapp/gen_emoji_map.py --check     # picker <-> bot ke premium emojis sync
```

Self-test verify karta hai: output **Python ke `rows_to_buttons_json()` se byte-to-byte
match**, premium emoji map bot ke `EMOJI_IDS` se aata hai, search (`pizza`/`fire`/`cake`)
chalta hai, aur purana data (rows) theek load hota hai.
