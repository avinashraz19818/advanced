import asyncio
import logging
import json
import time
import os
import re
from datetime import datetime, timedelta, timezone
from typing import Optional, List, Dict, Any, Tuple
from functools import wraps

from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Bot,
    InputMediaPhoto,
    InputMediaVideo,
    InputMediaDocument,
    InputMediaAudio,
    MessageEntity,
)
from telegram.ext import (
    ApplicationBuilder,
    Application,
    CommandHandler,
    CallbackQueryHandler,
    MessageHandler,
    ChatJoinRequestHandler,
    ChatMemberHandler,
    ContextTypes,
    filters,
)
from telegram.constants import ParseMode
from telegram.error import BadRequest, Forbidden, InvalidToken, NetworkError, TimedOut

# ================= RETRY DECORATOR =================
def retry_async(max_retries=3, delay=1, backoff=2):
    """
    IMPORTANT: In python-telegram-bot, `BadRequest` is a SUBCLASS of `NetworkError`.
    That means a plain `except (NetworkError, TimedOut, ConnectionError)` clause will
    also silently swallow-and-retry things like "Document_invalid",
    "Voice_messages_forbidden", "Message is not modified", etc. Those are permanent
    errors caused by the request itself (bad file, forbidden content, bad markup) -
    retrying them wastes time (delay + backoff seconds) and will NEVER succeed, and
    then finally raises anyway, which is what caused the "Retry 1/3 ... Document_invalid"
    spam in the logs and the generic "Callback error: Document_invalid" crashes.
    Fix: catch BadRequest FIRST and re-raise it immediately, without retrying.
    """
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            retries = 0
            current_delay = delay
            while retries < max_retries:
                try:
                    return await func(*args, **kwargs)
                except BadRequest:
                    # Permanent error - retrying is pointless, fail fast.
                    raise
                except (NetworkError, TimedOut, ConnectionError) as e:
                    retries += 1
                    if retries >= max_retries:
                        logging.error(f"Failed after {max_retries} retries: {e}")
                        raise
                    logging.warning(f"Retry {retries}/{max_retries} after {current_delay}s: {e}")
                    await asyncio.sleep(current_delay)
                    current_delay *= backoff
                except Exception:
                    raise
            return None
        return wrapper
    return decorator

# ================= TIMEZONE HELPER =================
def now_aware():
    return datetime.now(timezone.utc)

def make_aware(dt):
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt

# ================= CONFIG =================
def _load_dotenv(path: str = None) -> None:
    """Read KEY=VALUE lines from a local .env file (real env vars always win).

    Bot token ko code me edit karne ki zaroorat nahi - repo folder me .env banao:
        MAIN_BOT_TOKEN=123456:ABC-xyz...
    (.env .gitignore me hai, isliye token git me kabhi nahi jayega.)
    """
    path = path or os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
    try:
        with open(path, "r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, _, val = line.partition("=")
                key = key.strip()
                val = val.strip().strip('"').strip("'")
                if key and key not in os.environ:
                    os.environ[key] = val
    except FileNotFoundError:
        pass
    except Exception as ex:
        logging.warning(f"Could not read .env: {ex}")


_load_dotenv()

MAIN_BOT_TOKEN = os.getenv("MAIN_BOT_TOKEN", "").strip() or "7687421668:AAFzEsDO2L2EVkCm4MxhzSo8oGD0-8t5GKE"
ADMIN_USER_ID = 8015937475
ADMIN_USERNAME = "@zayro_o"
_ADMIN_IDS_RAW = os.getenv("ADMIN_USER_IDS", "").strip()
ADMIN_USER_IDS = {ADMIN_USER_ID}
if _ADMIN_IDS_RAW:
    for _x in _ADMIN_IDS_RAW.split(","):
        _x = _x.strip()
        if _x.isdigit():
            ADMIN_USER_IDS.add(int(_x))

SUPPORT_REPLY_MAP: Dict[int, Dict] = {}
USERBOT_SUPPORT_REPLY_MAP: Dict[str, Dict] = {}
SUPPORT_MAP_TTL = 86400

# ================= PREMIUM EMOJI IDS =================
EMOJI_IDS = {
    "💎": "5042050649248760772", "⭐️": "5042176294222037888", "⚡️": "5042334757040423886",
    "👑": "5039727497143387500", "✅": "5039844895779455925", "❌": "5040042498634810056",
    "‼️": "5042003580702164014", "🔔": "5042111805288089118", "📊": "5042290883949495533",
    "💬": "5040036030414062506", "🔄": "5041837837914211014", "🎉": "5039778134807806727",
    "🔍": "5039649904264217620", "👀": "5039623284056917259", "🛡": "5042328396193864923",
    "🔴": "5042042652019655612", "🟢": "5039928501612839813", "📣": "5041888071851705019",
    "🗑": "5039614900280754969", "💰": "5039789890133296083", "⚠️": "5039665997506675838",
    "🔗": "5042101437237036298", "📞": "5407025283456835913", "🔥": "5389038097860144794",
    "🚀": "5389057356493511934", "🎯": "5041888071851705019", "📅": "5413879192267805083",
    "🔐": "5305609152704297298", "⚙️": "5042101437237036298", "✈️": "5041888071851705019",
    "📝": "5039844895779455925", "📋": "5042290883949495533", "🔙": "5041837837914211014",
    "➡️": "5041837837914211014", "⬅️": "5041837837914211014", "➕": "5039844895779455925",
    "🖼️": "5040016479722931047", "🔘": "5042101437237036298", "🤖": "5042290883949495533",
    "🛑": "5042042652019655612", "⏰": "6285240160120477644", "📇": "5042290883949495533",
    "📌": "5039600026809009149", "🔝": "5042102141611672423", "💙": "5039560388555834382",
    "👍": "5039544445637231745", "👎": "5042067236412458007", "🚫": "5039671744172917707",
    "🧠": "5040030395416969985", "💡": "5039660273953853888", "💫": "5042200814190330758",
    "✨": "5040016479722931047", "🎨": "5040016479722931047",
    "📁": "5042290883949495533", "🎵": "5042101437237036298", "🎬": "5039778134807806727",
    "📄": "5039844895779455925", "🗂️": "5042290883949495533", "📦": "5042101437237036298",
    "🎙️": "5042101437237036298", "🎤": "5042101437237036298",
    "📩": "5267930814523847773", "👤": "5426536590798641686",
    "🔖": "5445273572664679078", "🟥": "5271091265042120271",
}

def pe(emoji_char: str) -> str:
    """Return plain emoji character (no HTML tags) for safe use in user-facing messages."""
    return emoji_char

def pp(emoji_char: str) -> str:
    """Return premium emoji HTML tag for bot UI/admin messages. Returns plain emoji if no ID found."""
    eid = EMOJI_IDS.get(emoji_char)
    if eid:
        return f'<tg-emoji emoji-id="{eid}">{emoji_char}</tg-emoji>'
    return emoji_char

def format_support_msg(user_name: str, username: str, user_id: int, message_text: str = None, clickable: bool = True) -> str:
    """Format support message with PLAIN emojis (user-facing, no premium)."""
    if clickable:
        name_line = f'<a href="tg://user?id={user_id}">{pe("👤")} {user_name}</a>'
    else:
        name_line = f'{pe("👤")} {user_name}'
    username_line = f'{pe("🔖")} @{username}' if username and username != "N/A" else f'{pe("🔖")} —'
    header = f'\n{pe("📩")} <b>New Message</b>\n'
    body = f'{name_line}\n{username_line}'
    if message_text:
        body += f'\n\n{pe("💬")} <b>Message:</b>\n▸ {message_text}'
    return f'{header}\n\n{body}\n\n'

def _degrade_markup(markup):
    """
    Fallback helper: strip icon_custom_emoji_id/style from every button, keeping
    only text/url/callback_data. Used when Telegram rejects a "styled" message
    (custom emoji icon referencing an invalid/inaccessible document) so we can
    retry with a plain version instead of crashing.
    """
    if not markup:
        return markup
    try:
        new_rows = []
        for row in markup.inline_keyboard:
            new_row = []
            for b in row:
                if b.url:
                    new_row.append(InlineKeyboardButton(b.text, url=b.url))
                elif b.callback_data:
                    new_row.append(InlineKeyboardButton(b.text, callback_data=b.callback_data))
                else:
                    new_row.append(b)
            new_rows.append(new_row)
        return InlineKeyboardMarkup(new_rows)
    except Exception:
        return markup


def btn(text: str, callback_data: str, style: str = "primary", emoji: str = None) -> InlineKeyboardButton:
    api_kwargs = {}
    style_map = {"primary": "primary", "success": "success", "danger": "danger"}
    if style and style in style_map:
        api_kwargs["style"] = style_map[style]
    if emoji and emoji in EMOJI_IDS:
        api_kwargs["icon_custom_emoji_id"] = EMOJI_IDS[emoji]
        clean_text = text
        for e in EMOJI_IDS.keys():
            clean_text = clean_text.replace(e, "").strip()
        text = clean_text if clean_text else "Button"
    else:
        clean_text = text
        for e in EMOJI_IDS.keys():
            clean_text = clean_text.replace(e, "").strip()
        text = clean_text if clean_text else text
    if api_kwargs:
        return InlineKeyboardButton(text, callback_data=callback_data, **api_kwargs)
    return InlineKeyboardButton(text, callback_data=callback_data)

def btn_url(text: str, url: str, style: str = "primary", emoji: str = None) -> InlineKeyboardButton:
    api_kwargs = {}
    style_map = {"primary": "primary", "success": "success", "danger": "danger"}
    if style and style in style_map:
        api_kwargs["style"] = style_map[style]
    if emoji and emoji in EMOJI_IDS:
        api_kwargs["icon_custom_emoji_id"] = EMOJI_IDS[emoji]
        clean_text = text
        for e in EMOJI_IDS.keys():
            clean_text = clean_text.replace(e, "").strip()
        text = clean_text if clean_text else "Button"
    else:
        clean_text = text
        for e in EMOJI_IDS.keys():
            clean_text = clean_text.replace(e, "").strip()
        text = clean_text if clean_text else text
    if api_kwargs:
        return InlineKeyboardButton(text, url=url, **api_kwargs)
    return InlineKeyboardButton(text, url=url)

def premiumize_ui_emojis(text: Optional[str]) -> str:
    """Convert plain emojis to premium <tg-emoji> tags in bot UI text."""
    if not text:
        return ""
    # First remove any existing <tg-emoji> tags to avoid nesting
    text = re.sub(r'<tg-emoji[^>]*>', '', text)
    text = re.sub(r'</tg-emoji>', '', text)
    # Now convert plain emojis to premium
    for emoji_char, eid in EMOJI_IDS.items():
        text = text.replace(emoji_char, f'<tg-emoji emoji-id="{eid}">{emoji_char}</tg-emoji>')
    return text

def strip_premium_emojis(text: Optional[str]) -> str:
    """Strip <tg-emoji> tags and return plain text with plain emojis. For user-facing messages."""
    if not text:
        return ""
    text = re.sub(r'<tg-emoji[^>]*>', '', text)
    text = re.sub(r'</tg-emoji>', '', text)
    return text

def escape_preserving_premium_emojis(text: Optional[str]) -> str:
    """Escape unsafe HTML while keeping Telegram premium emoji tags usable."""
    if not text:
        return ""
    placeholders = {}

    def keep_tag(match):
        key = f"__PREMIUM_EMOJI_{len(placeholders)}__"
        placeholders[key] = match.group(0)
        return key

    protected = re.sub(r'</?tg-emoji(?:\s+emoji-id="\d+")?>', keep_tag, text)
    escaped = EmojiManager._html_escape(protected)
    for key, tag in placeholders.items():
        escaped = escaped.replace(key, tag)
    return escaped

# ================= STYLED KEYBOARD BUILDERS =================
def main_menu_kb(uid: int) -> InlineKeyboardMarkup:
    lines = []
    user_bots = db.get_user_bots_by_owner(uid)
    if user_bots:
        for bot in user_bots:
            bot_id = bot["bot_id"]
            bot_username = bot["bot_username"]
            sub = db.get_subscription_for_bot(bot_id)
            is_active = False
            if sub and sub.get("expiry_date"):
                expiry = make_aware(sub["expiry_date"]) if isinstance(sub["expiry_date"], datetime) else sub["expiry_date"]
                if isinstance(expiry, str):
                    try:
                        expiry = datetime.fromisoformat(expiry.replace('+00:00', ''))
                        expiry = make_aware(expiry)
                    except:
                        expiry = now_aware()
                is_active = expiry > now_aware()
            status = "🟢" if is_active else "🔴"
            lines.append([btn(f"{status} @{bot_username}", f"manage_bot_{bot_id}", "primary", "🤖")])
        if is_admin(uid):
            lines.append([btn("Add New Bot", "add_new_bot", "success", "➕")])
    else:
        if is_admin(uid):
            lines.append([btn("Create New Bot", "add_new_bot", "success", "➕")])
    # NOTE: bot add karne ka (token) option sirf ADMIN panel me hai. Clients apna bot
    # admin se add karwate hain.
    lines.append([btn_url("Contact Admin", f"https://t.me/{ADMIN_USERNAME.lstrip('@')}", "primary", "📞")])
    if is_admin(uid):
        lines.append([btn("Admin Panel", "admin_panel", "danger", "👑")])
    return InlineKeyboardMarkup(lines)

def bot_management_kb(bot_id: str, user_id: int) -> InlineKeyboardMarkup:
    sub = db.get_subscription_for_bot(bot_id)
    lines = []
    is_active = False
    days_left = 0
    if sub and sub.get("expiry_date"):
        expiry = make_aware(sub["expiry_date"]) if isinstance(sub["expiry_date"], datetime) else sub["expiry_date"]
        if isinstance(expiry, str):
            try:
                expiry = datetime.fromisoformat(expiry.replace('+00:00', ''))
                expiry = make_aware(expiry)
            except:
                expiry = now_aware()
        is_active = expiry > now_aware()
        if is_active:
            days_left = (expiry - now_aware()).days
    if is_active:
        lines.append([
            btn("Add Channel", f"ub_add_channel_{bot_id}", "success", "✈️"),
            btn("Set Message(s)", f"ub_set_message_{bot_id}", "primary", "📝")
        ])
        lines.append([
            btn("Preview/Edit", f"ub_manage_messages_{bot_id}", "primary", "👀"),
            btn("Delete Msgs", f"ub_delete_messages_{bot_id}", "danger", "🗑")
        ])
        lines.append([
            btn("Remove Channel", f"ub_remove_channel_{bot_id}", "danger", "❌"),
            btn("Auto-Approve", f"ub_toggle_auto_{bot_id}", "success", "⚙️")
        ])
        lines.append([
            btn("My Channels", f"ub_list_channels_{bot_id}", "primary", "📋"),
            btn("Bot Stats", f"ub_stats_{bot_id}", "primary", "📊")
        ])
        lines.append([
            btn("Pending", f"ub_pending_requests_{bot_id}", "primary", "📊"),
            btn("Accept All", f"ub_accept_all_{bot_id}", "success", "✅")
        ])
        lines.append([btn("Broadcast to Users", f"ub_broadcast_{bot_id}", "success", "✈️")])
        lines.append([btn(f"Subscription — {days_left}d left", f"ub_subscription_{bot_id}", "primary", "📅")])
    else:
        lines.append([btn("Get Subscription", f"sub_for_bot_{bot_id}", "danger", "⚠️")])
    lines.append([btn_url("Contact Admin", f"https://t.me/{ADMIN_USERNAME.lstrip('@')}", "primary", "📞")])
    lines.append([btn("Back to Main", "main_menu", "primary", "🔙")])
    return InlineKeyboardMarkup(lines)

def subscription_plans_kb(bot_id: str = None) -> InlineKeyboardMarkup:
    back_cb = f"manage_bot_{bot_id}" if bot_id else "main_menu"
    return InlineKeyboardMarkup([
        [btn("Basic — Rs2599/mo (1 channel)", f"sub_basic_{bot_id}" if bot_id else "sub_basic", "primary", "💰")],
        [btn("Pro — Rs3999/mo (5 channels)", f"sub_pro_{bot_id}" if bot_id else "sub_pro", "success", "⚡️")],
        [btn("Back", back_cb, "primary", "🔙")],
    ])

def admin_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [btn("All Users", "admin_all_users", "primary", "📇"),
         btn("Manage UserBots", "admin_userbots", "success", "🤖")],
        [btn("Add UserBot", "admin_add_userbot", "success", "➕"),
         btn("Add Subscription", "admin_add_sub", "success", "⭐️")],
        [btn("Subscription List", "admin_sub_list", "primary", "📋")],
        [btn("Check Expiry", "admin_check_expiry", "primary", "⏰"),
         btn("Stats", "admin_stats", "primary", "📊")],
        [btn("Start All Bots", "admin_start_all", "success", "🚀"),
         btn("Stop All Bots", "admin_stop_all", "danger", "🛑")],
        [btn("Leave Recovery", "admin_leave_recovery", "primary", "🔔")],
        [btn("Default First Message", "admin_default_first_msg", "primary", "💬")],
        [btn("Broadcast", "admin_broadcast", "success", "✈️"),
         btn("Send Reminders", "admin_send_reminders", "primary", "🔔")],
        [btn("Main Menu", "main_menu", "primary", "🔙")],
    ])

def verification_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[btn("Verify Now", "human_verify", "success", "✅")]])

def confirm_kb(confirm_cb: str, cancel_cb: str, confirm_text: str = "Confirm", cancel_text: str = "Cancel") -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [btn(confirm_text, confirm_cb, "success", "✅"),
         btn(cancel_text, cancel_cb, "danger", "❌")]
    ])

def pagination_kb(current_page: int, total_pages: int, prefix: str) -> list:
    nav = []
    if current_page > 1:
        nav.append(btn("Prev", f"{prefix}_page_{current_page-1}", "primary", "⬅️"))
    if current_page < total_pages:
        nav.append(btn("Next", f"{prefix}_page_{current_page+1}", "primary", "➡️"))
    return [nav] if nav else []

# ================= SUPPORT MAP HELPERS =================
def _store_support_map(store: dict, key, user_id: int):
    store[key] = {"uid": user_id, "ts": time.time()}

def _get_support_uid(store: dict, key):
    entry = store.get(key)
    if not entry:
        return None
    if time.time() - entry["ts"] > SUPPORT_MAP_TTL:
        store.pop(key, None)
        return None
    return entry["uid"]

def _cleanup_support_maps():
    now = time.time()
    for store in [SUPPORT_REPLY_MAP, USERBOT_SUPPORT_REPLY_MAP]:
        stale = [k for k, v in store.items() if now - v.get("ts", 0) > SUPPORT_MAP_TTL]
        for k in stale:
            store.pop(k, None)

def _parse_id(s: str):
    try:
        return int(s)
    except (ValueError, TypeError):
        return None

def _extract_last_id(parts: list) -> int:
    v = _parse_id(parts[-1])
    if v is not None:
        return v
    if len(parts) >= 2:
        v = _parse_id(parts[-2])
        if v is not None:
            return v
    return 0


# Callback prefixes that belong to a userbot's manage panel. The main bot shows the
# very same panel (manage_bot_...), so it has to recognise these too - otherwise
# every button of that panel is dead on the main bot and the owner/admin only gets
# the menu back again and again.
MANAGED_CB_PREFIXES = ("ub_", "ubm_", "ubmm_", "setmsg_", "setbtn_", "setbtng",
                       "bcast_", "delmsg_", "removechan_", "toggleauto_")


def managed_bot_id_from_data(data: str, uid: int) -> Optional[str]:
    """Return the bot_id a manage-panel callback belongs to, or None.

    bot_id is "<user_id>_<n>", so it is matched as a sequence of "_"-separated
    tokens inside the callback data (works for ubmm_<bot>_<chan>_<msg> too).
    """
    if not data or not data.startswith(MANAGED_CB_PREFIXES):
        return None
    candidates = [b["bot_id"] for b in (db.get_user_bots_by_owner(uid) or [])]
    if is_admin(uid):
        candidates += [b["bot_id"] for b in (db.get_all_user_bots() or [])]
    tokens = data.split("_")
    best = None
    for bid in candidates:
        bid_tokens = str(bid).split("_")
        n = len(bid_tokens)
        for i in range(len(tokens) - n + 1):
            if tokens[i:i + n] == bid_tokens:
                if best is None or len(str(bid)) > len(str(best)):
                    best = bid
                break
    return best

# ================= DATABASE =================
DATABASE_URL = "postgresql://postgres:postgres@localhost:5432/advanced_bot"

try:
    import psycopg2
    from psycopg2.extras import Json, RealDictCursor
except Exception:
    psycopg2 = None


class Database:
    def __init__(self):
        if psycopg2 is None:
            raise RuntimeError("PostgreSQL driver missing. Install: pip install psycopg2-binary")
        self.conn = psycopg2.connect(DATABASE_URL)
        self.conn.autocommit = True
        self.init_db()
        logging.info("PostgreSQL connected")

    def _execute(self, sql: str, params: tuple = ()):
        with self.conn.cursor() as cur:
            cur.execute(sql, params)

    def _fetchone(self, sql: str, params: tuple = ()):
        with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, params)
            return cur.fetchone()

    def _fetchall(self, sql: str, params: tuple = ()):
        with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, params)
            return cur.fetchall()

    def init_db(self):
        statements = [
            """CREATE TABLE IF NOT EXISTS users (\n                user_id BIGINT PRIMARY KEY, username TEXT, first_name TEXT, last_name TEXT,\n                verified BOOLEAN DEFAULT FALSE, created_at TIMESTAMPTZ DEFAULT now()\n            )""",
            """CREATE TABLE IF NOT EXISTS user_bots (\n                bot_id TEXT PRIMARY KEY, user_id BIGINT REFERENCES users(user_id) ON DELETE CASCADE,\n                bot_token TEXT UNIQUE, bot_username TEXT, is_active INT DEFAULT 0,\n                created_at TIMESTAMPTZ DEFAULT now()\n            )""",
            """CREATE TABLE IF NOT EXISTS bot_subscriptions (\n                id BIGSERIAL PRIMARY KEY, bot_id TEXT REFERENCES user_bots(bot_id) ON DELETE CASCADE,\n                subscription_type TEXT, expiry_date TIMESTAMPTZ, max_channels INT DEFAULT 1,\n                reminder_3d_sent BOOLEAN DEFAULT FALSE, reminder_1d_sent BOOLEAN DEFAULT FALSE,\n                created_at TIMESTAMPTZ DEFAULT now()\n            )""",
            """CREATE TABLE IF NOT EXISTS user_bot_channels (\n                bot_id TEXT REFERENCES user_bots(bot_id) ON DELETE CASCADE,\n                channel_id BIGINT, channel_username TEXT, channel_title TEXT,\n                welcome_message TEXT, welcome_media_id TEXT, welcome_media_type TEXT,\n                auto_approve INT DEFAULT 0, created_at TIMESTAMPTZ DEFAULT now(),\n                PRIMARY KEY (bot_id, channel_id)\n            )""",
            """CREATE TABLE IF NOT EXISTS user_bot_messages (\n                id BIGSERIAL PRIMARY KEY, bot_id TEXT REFERENCES user_bots(bot_id) ON DELETE CASCADE,\n                channel_id BIGINT, content_text TEXT, media_id TEXT, media_type TEXT,\n                file_name TEXT, mime_type TEXT, telegram_message_id BIGINT,\n                media_group_id TEXT, buttons_json TEXT, entities_json TEXT,\n                created_at TIMESTAMPTZ DEFAULT now()\n            )""",
            """CREATE TABLE IF NOT EXISTS join_requests (\n                id BIGSERIAL PRIMARY KEY, bot_id TEXT REFERENCES user_bots(bot_id) ON DELETE CASCADE,\n                requester_id BIGINT, channel_id BIGINT, status TEXT,\n                request_date TIMESTAMPTZ DEFAULT now(), approved_date TIMESTAMPTZ,\n                UNIQUE(bot_id, requester_id, channel_id)\n            )""",
            """CREATE TABLE IF NOT EXISTS reachable_users (\n                bot_id TEXT REFERENCES user_bots(bot_id) ON DELETE CASCADE,\n                requester_id BIGINT, last_ok_at TIMESTAMPTZ DEFAULT now(),\n                PRIMARY KEY (bot_id, requester_id)\n            )""",
            """CREATE TABLE IF NOT EXISTS user_emoji_maps (\n                bot_id TEXT REFERENCES user_bots(bot_id) ON DELETE CASCADE,\n                msg_id BIGINT, emoji_map JSONB DEFAULT '{}',\n                updated_at TIMESTAMPTZ DEFAULT now(), PRIMARY KEY (bot_id, msg_id)\n            )""",
            """CREATE TABLE IF NOT EXISTS system_settings (\n                key TEXT PRIMARY KEY, value_json JSONB DEFAULT '{}', updated_at TIMESTAMPTZ DEFAULT now()\n            )""",
            """CREATE TABLE IF NOT EXISTS leave_recovery_messages (\n                id BIGSERIAL PRIMARY KEY, bot_id TEXT, user_id BIGINT,\n                source_channel_id BIGINT, target_channel_id BIGINT, message_id BIGINT,\n                sent_at TIMESTAMPTZ DEFAULT now(), deleted_at TIMESTAMPTZ\n            )""",
            "CREATE INDEX IF NOT EXISTS idx_bot_subscriptions ON bot_subscriptions(bot_id, expiry_date)",
            "CREATE INDEX IF NOT EXISTS idx_join_requests ON join_requests(bot_id, status)",
            "CREATE INDEX IF NOT EXISTS idx_user_bots_user ON user_bots(user_id)",
            "CREATE INDEX IF NOT EXISTS idx_messages_bot ON user_bot_messages(bot_id, channel_id)",
            "CREATE INDEX IF NOT EXISTS idx_reachable_bot ON reachable_users(bot_id, last_ok_at DESC)",
        ]
        with self.conn.cursor() as cur:
            for stmt in statements:
                try:
                    cur.execute(stmt)
                except Exception as e:
                    logging.warning(f"Table creation warning: {e}")

    def add_user(self, user_id, username, first_name, last_name):
        self._execute("""INSERT INTO users (user_id, username, first_name, last_name)\n               VALUES (%s,%s,%s,%s) ON CONFLICT (user_id) DO UPDATE\n               SET username=COALESCE(EXCLUDED.username, users.username),\n                   first_name=COALESCE(EXCLUDED.first_name, users.first_name),\n                   last_name=COALESCE(EXCLUDED.last_name, users.last_name)""",
            (user_id, username, first_name, last_name))

    def get_user(self, user_id):
        return self._fetchone("SELECT * FROM users WHERE user_id=%s", (user_id,)) or {}

    def mark_user_verified(self, user_id: int):
        self._execute("UPDATE users SET verified=TRUE WHERE user_id=%s", (user_id,))

    def is_user_verified(self, user_id: int) -> bool:
        row = self._fetchone("SELECT verified FROM users WHERE user_id=%s", (user_id,))
        return bool(row and row["verified"])

    def get_all_users(self):
        rows = self._fetchall("SELECT DISTINCT user_id, username, first_name, last_name, created_at FROM users ORDER BY user_id")
        return [dict(r) for r in rows]

    def get_next_bot_id(self, user_id: int) -> str:
        rows = self._fetchall("SELECT bot_id FROM user_bots WHERE user_id=%s", (user_id,))
        numbers = []
        for row in rows:
            parts = row["bot_id"].split("_")
            if len(parts) == 2 and parts[1].isdigit():
                numbers.append(int(parts[1]))
        next_num = max(numbers) + 1 if numbers else 1
        return f"{user_id}_{next_num}"

    def add_user_bot(self, user_id, token, username):
        bot_id = self.get_next_bot_id(user_id)
        self.add_user(user_id, None, f"User{user_id}", None)
        self._execute("""INSERT INTO user_bots (bot_id, user_id, bot_token, bot_username, is_active)\n               VALUES (%s,%s,%s,%s,1) ON CONFLICT (bot_token) DO UPDATE\n               SET bot_username=EXCLUDED.bot_username, is_active=1""",
            (bot_id, user_id, token, username))
        return bot_id

    def get_user_bot(self, bot_id: str):
        b = self._fetchone("SELECT * FROM user_bots WHERE bot_id=%s", (bot_id,))
        return dict(b) if b else None

    def get_user_bots_by_owner(self, user_id: int):
        rows = self._fetchall("SELECT * FROM user_bots WHERE user_id=%s ORDER BY created_at DESC", (user_id,))
        return [dict(r) for r in rows]

    def get_all_user_bots(self):
        rows = self._fetchall("SELECT * FROM user_bots ORDER BY user_id, created_at")
        return [dict(r) for r in rows]

    def get_bot_by_username(self, username: str):
        return self._fetchone("SELECT * FROM user_bots WHERE bot_username=%s", (username,))

    def set_user_bot_active(self, bot_id: str, active):
        self._execute("UPDATE user_bots SET is_active=%s WHERE bot_id=%s", (1 if active else 0, bot_id))

    def remove_user_bot(self, bot_id: str):
        self._execute("DELETE FROM user_bots WHERE bot_id=%s", (bot_id,))

    def get_subscription_for_bot(self, bot_id: str):
        s = self._fetchone("SELECT * FROM bot_subscriptions WHERE bot_id=%s ORDER BY expiry_date DESC LIMIT 1", (bot_id,))
        return dict(s) if s else None

    def add_subscription_for_bot(self, bot_id: str, sub_type, days):
        max_channels = 1 if sub_type.lower() == "basic" else 5
        expiry_date = now_aware() + timedelta(days=days)
        self._execute("""INSERT INTO bot_subscriptions (bot_id, subscription_type, expiry_date, max_channels)\n               VALUES (%s,%s,%s,%s)""", (bot_id, sub_type, expiry_date, max_channels))

    def update_subscription_expiry(self, bot_id: str, new_expiry):
        row = self._fetchone("SELECT id FROM bot_subscriptions WHERE bot_id=%s ORDER BY expiry_date DESC LIMIT 1", (bot_id,))
        if row:
            self._execute("UPDATE bot_subscriptions SET expiry_date=%s, reminder_3d_sent=FALSE, reminder_1d_sent=FALSE WHERE id=%s", (new_expiry, row["id"]))

    def get_expiring_subscriptions(self, days_threshold: int):
        reminder = "reminder_3d_sent" if days_threshold == 3 else "reminder_1d_sent"
        rows = self._fetchall(f"""SELECT bot_id, subscription_type, expiry_date FROM bot_subscriptions\n                WHERE expiry_date BETWEEN now() AND now() + interval '%s days'\n                AND {reminder}=FALSE ORDER BY expiry_date""", (days_threshold,))
        return [dict(r) for r in rows]

    def get_expired_subscriptions(self):
        rows = self._fetchall("""SELECT DISTINCT bot_id FROM bot_subscriptions s\n               WHERE NOT EXISTS (SELECT 1 FROM bot_subscriptions live\n               WHERE live.bot_id=s.bot_id AND live.expiry_date >= now())""")
        return [r["bot_id"] for r in rows]

    def mark_reminder_sent(self, bot_id: str, days: int):
        field = "reminder_3d_sent" if days == 3 else "reminder_1d_sent"
        self._execute(f"UPDATE bot_subscriptions SET {field}=TRUE WHERE bot_id=%s AND expiry_date >= now()", (bot_id,))

    def get_all_subscriptions(self):
        rows = self._fetchall("""SELECT s.bot_id, s.subscription_type, s.expiry_date, s.max_channels,\n               b.user_id, b.bot_username, COALESCE(b.is_active, 0) AS bot_active,\n               u.username as owner_username, u.first_name as owner_name\n               FROM bot_subscriptions s LEFT JOIN user_bots b ON b.bot_id=s.bot_id\n               LEFT JOIN users u ON u.user_id=b.user_id ORDER BY s.expiry_date DESC""")
        return [dict(r) for r in rows]

    def add_channel(self, bot_id: str, channel_id, username, title):
        self._execute("""INSERT INTO user_bot_channels (bot_id, channel_id, channel_username, channel_title)\n               VALUES (%s,%s,%s,%s) ON CONFLICT (bot_id, channel_id) DO UPDATE\n               SET channel_username=EXCLUDED.channel_username, channel_title=EXCLUDED.channel_title""",
            (bot_id, channel_id, username, title))

    def get_bot_channels(self, bot_id: str):
        rows = self._fetchall("SELECT * FROM user_bot_channels WHERE bot_id=%s ORDER BY channel_id", (bot_id,))
        return [dict(r) for r in rows]

    def set_auto_approve(self, bot_id: str, channel_id, val):
        self._execute("UPDATE user_bot_channels SET auto_approve=%s WHERE bot_id=%s AND channel_id=%s", (1 if val else 0, bot_id, channel_id))

    def get_channel_owner_data(self, channel_id, bot_id=None):
        if bot_id:
            r = self._fetchone("SELECT * FROM user_bot_channels WHERE bot_id=%s AND channel_id=%s", (bot_id, channel_id))
        else:
            r = self._fetchone("SELECT * FROM user_bot_channels WHERE channel_id=%s ORDER BY created_at DESC LIMIT 1", (channel_id,))
        return dict(r) if r else None

    def clear_messages(self, bot_id: str, channel_id):
        msgs = self._fetchall("SELECT id FROM user_bot_messages WHERE bot_id=%s AND channel_id=%s", (bot_id, channel_id))
        for msg in msgs:
            self.delete_user_emoji_map(bot_id, msg["id"])
        self._execute("DELETE FROM user_bot_messages WHERE bot_id=%s AND channel_id=%s", (bot_id, channel_id))
        self._execute("UPDATE user_bot_channels SET welcome_message=NULL, welcome_media_id=NULL, welcome_media_type=NULL WHERE bot_id=%s AND channel_id=%s", (bot_id, channel_id))

    def remove_channel(self, bot_id: str, channel_id):
        self.clear_messages(bot_id, channel_id)
        self._execute("DELETE FROM user_bot_channels WHERE bot_id=%s AND channel_id=%s", (bot_id, channel_id))
        self._execute("DELETE FROM join_requests WHERE bot_id=%s AND channel_id=%s", (bot_id, channel_id))
        return True

    def _refresh_channel_welcome(self, bot_id: str, channel_id):
        first = self._fetchone("SELECT * FROM user_bot_messages WHERE bot_id=%s AND channel_id=%s ORDER BY id LIMIT 1", (bot_id, channel_id))
        if first:
            self._execute("UPDATE user_bot_channels SET welcome_message=%s, welcome_media_id=%s, welcome_media_type=%s WHERE bot_id=%s AND channel_id=%s",
                (first["content_text"], first["media_id"], first["media_type"], bot_id, channel_id))
        else:
            self._execute("UPDATE user_bot_channels SET welcome_message=NULL, welcome_media_id=NULL, welcome_media_type=NULL WHERE bot_id=%s AND channel_id=%s",
                (bot_id, channel_id))

    def add_message(self, bot_id: str, channel_id, text, media_id, media_type, media_group_id=None, entities_json=None, file_name=None, mime_type=None, telegram_message_id=None):
        with self.conn.cursor() as cur:
            cur.execute("""INSERT INTO user_bot_messages\n                   (bot_id, channel_id, content_text, media_id, media_type, file_name, mime_type, media_group_id, entities_json, telegram_message_id)\n                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id""",
                (bot_id, channel_id, text, media_id, media_type, file_name, mime_type, media_group_id, entities_json, telegram_message_id))
            msg_id = cur.fetchone()[0]
        self._refresh_channel_welcome(bot_id, channel_id)
        return msg_id

    def update_message_text(self, msg_id, text, entities_json=None):
        row = self.get_message_by_id(msg_id)
        if row:
            self._execute("UPDATE user_bot_messages SET content_text=%s, entities_json=COALESCE(%s, entities_json) WHERE id=%s", (text, entities_json, int(msg_id)))
            self._refresh_channel_welcome(row["bot_id"], row["channel_id"])

    def update_message_media(self, msg_id, media_id, media_type, text=None, entities_json=None, file_name=None, mime_type=None, telegram_message_id=None):
        row = self.get_message_by_id(msg_id)
        if row:
            self._execute("""UPDATE user_bot_messages SET media_id=%s, media_type=%s,\n                   content_text=COALESCE(%s, content_text), entities_json=COALESCE(%s, entities_json),\n                   file_name=COALESCE(%s, file_name), mime_type=COALESCE(%s, mime_type),\n                   telegram_message_id=COALESCE(%s, telegram_message_id) WHERE id=%s""",
                (media_id, media_type, text, entities_json, file_name, mime_type, telegram_message_id, int(msg_id)))
            self._refresh_channel_welcome(row["bot_id"], row["channel_id"])

    def delete_message(self, msg_id):
        row = self.get_message_by_id(msg_id)
        if row:
            self._execute("DELETE FROM user_bot_messages WHERE id=%s", (int(msg_id),))
            self.delete_user_emoji_map(row["bot_id"], int(msg_id))
            self._refresh_channel_welcome(row["bot_id"], row["channel_id"])
            return True
        return False

    def delete_message_by_telegram_id(self, bot_id: str, telegram_message_id: int):
        row = self._fetchone("SELECT id, channel_id FROM user_bot_messages WHERE bot_id=%s AND telegram_message_id=%s", (bot_id, telegram_message_id))
        if row:
            self._execute("DELETE FROM user_bot_messages WHERE id=%s", (row["id"],))
            self.delete_user_emoji_map(bot_id, row["id"])
            self._refresh_channel_welcome(bot_id, row["channel_id"])
            return True
        return False

    def delete_media_group_messages(self, bot_id: str, media_group_id):
        first = self._fetchone("SELECT channel_id FROM user_bot_messages WHERE bot_id=%s AND media_group_id=%s LIMIT 1", (bot_id, media_group_id))
        if first:
            ids = [r["id"] for r in self._fetchall("SELECT id FROM user_bot_messages WHERE bot_id=%s AND media_group_id=%s", (bot_id, media_group_id))]
            self._execute("DELETE FROM user_bot_messages WHERE bot_id=%s AND media_group_id=%s", (bot_id, media_group_id))
            for msg_id in ids:
                self.delete_user_emoji_map(bot_id, msg_id)
            self._refresh_channel_welcome(bot_id, first["channel_id"])

    def get_message_count(self, channel_id, bot_id=None):
        if bot_id:
            row = self._fetchone("SELECT COUNT(*) as count FROM user_bot_messages WHERE bot_id=%s AND channel_id=%s", (bot_id, channel_id))
        else:
            row = self._fetchone("SELECT COUNT(*) as count FROM user_bot_messages WHERE channel_id=%s", (channel_id,))
        return int(row["count"]) if row else 0

    def get_messages(self, channel_id, bot_id=None):
        if bot_id:
            rows = self._fetchall("SELECT * FROM user_bot_messages WHERE bot_id=%s AND channel_id=%s ORDER BY id", (bot_id, channel_id))
        else:
            rows = self._fetchall("SELECT * FROM user_bot_messages WHERE channel_id=%s ORDER BY id", (channel_id,))
        return [dict(r) for r in rows]

    def get_message_by_id(self, msg_id):
        r = self._fetchone("SELECT * FROM user_bot_messages WHERE id=%s", (int(msg_id),))
        return dict(r) if r else None

    def get_message_by_telegram_id(self, bot_id: str, telegram_message_id: int):
        r = self._fetchone("SELECT * FROM user_bot_messages WHERE bot_id=%s AND telegram_message_id=%s", (bot_id, telegram_message_id))
        return dict(r) if r else None

    def update_message_buttons(self, msg_id, buttons_json):
        self._execute("UPDATE user_bot_messages SET buttons_json=%s WHERE id=%s", (buttons_json, int(msg_id)))

    def append_message_buttons(self, msg_id, new_buttons_json):
        row = self._fetchone("SELECT buttons_json FROM user_bot_messages WHERE id=%s", (int(msg_id),))
        existing = []
        if row and row["buttons_json"]:
            try:
                existing = json.loads(row["buttons_json"]) or []
            except Exception:
                existing = []
        new_btns = []
        if new_buttons_json:
            try:
                new_btns = json.loads(new_buttons_json) or []
            except Exception:
                new_btns = []
        combined = existing + new_btns
        self._execute("UPDATE user_bot_messages SET buttons_json=%s WHERE id=%s", (json.dumps(combined), int(msg_id)))

    def save_user_emoji_map(self, bot_id: str, msg_id: int, emoji_map: dict):
        if emoji_map:
            self._execute("""INSERT INTO user_emoji_maps (bot_id, msg_id, emoji_map, updated_at)\n                   VALUES (%s,%s,%s,now()) ON CONFLICT (bot_id, msg_id) DO UPDATE\n                   SET emoji_map=EXCLUDED.emoji_map, updated_at=now()""",
                (bot_id, msg_id, Json(emoji_map)))

    def get_user_emoji_map(self, bot_id: str, msg_id: int) -> dict:
        row = self._fetchone("SELECT emoji_map FROM user_emoji_maps WHERE bot_id=%s AND msg_id=%s", (bot_id, msg_id))
        return dict(row["emoji_map"]) if row and row["emoji_map"] else {}

    def delete_user_emoji_map(self, bot_id: str, msg_id: int):
        self._execute("DELETE FROM user_emoji_maps WHERE bot_id=%s AND msg_id=%s", (bot_id, int(msg_id)))

    def add_join_request(self, bot_id: str, requester_id, channel_id, status):
        self._execute("""INSERT INTO join_requests (bot_id, requester_id, channel_id, status, approved_date)\n               VALUES (%s,%s,%s,%s,%s) ON CONFLICT (bot_id, requester_id, channel_id) DO UPDATE\n               SET status=CASE WHEN join_requests.status='approved' AND EXCLUDED.status='pending'\n               THEN join_requests.status ELSE EXCLUDED.status END,\n               approved_date=CASE WHEN EXCLUDED.status='approved' THEN now() ELSE join_requests.approved_date END""",
            (bot_id, requester_id, channel_id, status, now_aware() if status == "approved" else None))

    def get_pending_requests(self, bot_id: str):
        rows = self._fetchall("SELECT id, requester_id, channel_id FROM join_requests WHERE bot_id=%s AND status='pending' ORDER BY request_date", (bot_id,))
        return [dict(r) for r in rows]

    def mark_request_status(self, request_id, status):
        self._execute("UPDATE join_requests SET status=%s, approved_date=CASE WHEN %s='approved' THEN now() ELSE approved_date END WHERE id=%s", (status, status, int(request_id)))

    def get_pending_count(self, bot_id: str):
        row = self._fetchone("SELECT COUNT(*) as count FROM join_requests WHERE bot_id=%s AND status='pending'", (bot_id,))
        return int(row["count"]) if row else 0

    def mark_reachable(self, bot_id: str, requester_id):
        self._execute("INSERT INTO reachable_users (bot_id, requester_id, last_ok_at) VALUES (%s,%s,now()) ON CONFLICT (bot_id, requester_id) DO UPDATE SET last_ok_at=now()", (bot_id, requester_id))

    def mark_unreachable(self, bot_id: str, requester_id):
        self._execute("DELETE FROM reachable_users WHERE bot_id=%s AND requester_id=%s", (bot_id, requester_id))

    def get_requesters_for_bot(self, bot_id: str):
        rows = self._fetchall("SELECT requester_id FROM reachable_users WHERE bot_id=%s ORDER BY last_ok_at DESC", (bot_id,))
        if rows:
            return [r["requester_id"] for r in rows]
        rows = self._fetchall("SELECT DISTINCT requester_id FROM join_requests WHERE bot_id=%s AND status='approved'", (bot_id,))
        return [r["requester_id"] for r in rows]

    def get_total_requesters_count(self, bot_id: str):
        row = self._fetchone("SELECT COUNT(DISTINCT requester_id) as count FROM join_requests WHERE bot_id=%s", (bot_id,))
        return int(row["count"]) if row else 0

    def get_reachable_requesters_count(self, bot_id: str):
        row = self._fetchone("SELECT COUNT(DISTINCT requester_id) as count FROM reachable_users WHERE bot_id=%s", (bot_id,))
        return int(row["count"]) if row else 0

    def get_userbot_user_counts(self):
        rows = self._fetchall("""SELECT b.bot_id, b.bot_username, b.user_id, COUNT(DISTINCT j.requester_id) as users,\n               COALESCE(s.subscription_type, 'None') as plan,\n               COALESCE(s.expiry_date < now(), true) as expired\n               FROM user_bots b LEFT JOIN join_requests j ON j.bot_id=b.bot_id\n               LEFT JOIN bot_subscriptions s ON s.bot_id=b.bot_id\n               GROUP BY b.bot_id, b.bot_username, b.user_id, s.subscription_type, s.expiry_date\n               ORDER BY b.user_id""")
        return [dict(r) for r in rows]

    def get_setting(self, key: str, default=None):
        row = self._fetchone("SELECT value_json FROM system_settings WHERE key=%s", (key,))
        return row["value_json"] if row else default

    def set_setting(self, key: str, value: dict):
        self._execute("INSERT INTO system_settings (key, value_json, updated_at) VALUES (%s,%s,now()) ON CONFLICT (key) DO UPDATE SET value_json=EXCLUDED.value_json, updated_at=now()", (key, Json(value)))

    def get_leave_recovery_config(self) -> dict:
        cfg = self.get_setting("leave_recovery", {}) or {}
        cfg.setdefault("enabled", False)
        cfg.setdefault("target_channel_id", None)
        cfg.setdefault("target_channel_link", "")
        cfg.setdefault("messages", [])
        if not cfg["messages"] and cfg.get("message"):
            cfg["messages"] = [{"text": cfg["message"], "buttons_json": cfg.get("buttons_json", "")}]
        cfg.setdefault("channel_configs", {})
        return cfg

    def set_leave_recovery_config(self, cfg: dict):
        self.set_setting("leave_recovery", cfg)

    def get_default_first_message(self) -> str:
        return self.get_setting("default_first_message", None) or "Hello {first_name},\n\nAapki request mil gayi hai, jaldi hi accept ho jayegi.\n\nTab tak aap niche diye hue video dekh lo ⚠️ Miss mat karna — properly follow karna!"

    def set_default_first_message(self, text: str):
        self.set_setting("default_first_message", text)

    def add_leave_recovery_message(self, bot_id: str, user_id, source_channel_id, target_channel_id, message_id):
        self._execute("INSERT INTO leave_recovery_messages (bot_id, user_id, source_channel_id, target_channel_id, message_id) VALUES (%s,%s,%s,%s,%s)", (bot_id, user_id, source_channel_id, target_channel_id, message_id))

    def get_pending_leave_recovery_messages(self, bot_id: str, user_id, target_channel_id):
        rows = self._fetchall("SELECT id, message_id FROM leave_recovery_messages WHERE bot_id=%s AND user_id=%s AND target_channel_id=%s AND deleted_at IS NULL ORDER BY sent_at DESC", (bot_id, user_id, target_channel_id))
        return [(r["id"], r["message_id"]) for r in rows]

    def mark_leave_recovery_deleted(self, row_id):
        self._execute("UPDATE leave_recovery_messages SET deleted_at=now() WHERE id=%s", (row_id,))


# ================= GLOBALS =================
db = Database()
user_bot_applications: Dict[str, Application] = {}


# ================= LEAVE RECOVERY HELPERS =================
def leave_recovery_channel_enabled(cfg: dict, channel_id) -> bool:
    """Per-channel leave-recovery switch.

    FIX: default is OFF for everybody. Earlier an unconfigured channel was treated
    as ON, so switching the global flag on silently enabled leave recovery for every
    channel of every userbot. Now a channel is active only when it was explicitly
    switched ON in the per-channel settings (already-configured channels keep
    working exactly as before).
    """
    if not cfg:
        return False
    channel_configs = cfg.get("channel_configs") or {}
    try:
        return bool(channel_configs.get(str(int(channel_id)), False))
    except (TypeError, ValueError):
        return bool(channel_configs.get(str(channel_id), False))


# ================= EMOJI MANAGER =================
class EmojiManager:
    @staticmethod
    def extract_from_entities(text: str, entities: Optional[List]) -> dict:
        if not entities or not text:
            return {}
        emoji_map = {}
        char_to_utf16 = []
        utf16_pos = 0
        for ch in text:
            char_to_utf16.append(utf16_pos)
            utf16_pos += len(ch.encode('utf-16-le')) // 2
        for entity in entities:
            if entity.type == "custom_emoji" and entity.custom_emoji_id:
                try:
                    start_utf16 = entity.offset
                    end_utf16 = entity.offset + entity.length
                    start_char = None
                    end_char = None
                    for ci, u16 in enumerate(char_to_utf16):
                        if u16 == start_utf16 and start_char is None:
                            start_char = ci
                        if u16 == end_utf16 and end_char is None:
                            end_char = ci
                            break
                    if end_char is None:
                        end_char = len(text)
                    if start_char is None:
                        continue
                    emoji_char = text[start_char:end_char]
                    emoji_map[emoji_char] = entity.custom_emoji_id
                except Exception as ex:
                    logging.warning(f"Emoji extraction failed: {ex}")
        return emoji_map

    @staticmethod
    def entities_to_json(entities: Optional[List]) -> Optional[str]:
        if not entities:
            return None
        try:
            serialized = []
            for e in entities:
                d = {"type": e.type, "offset": e.offset, "length": e.length}
                if hasattr(e, "custom_emoji_id") and e.custom_emoji_id:
                    d["custom_emoji_id"] = e.custom_emoji_id
                if hasattr(e, "url") and e.url:
                    d["url"] = e.url
                if hasattr(e, "user") and e.user:
                    d["user_id"] = e.user.id
                if hasattr(e, "language") and e.language:
                    d["language"] = e.language
                serialized.append(d)
            return json.dumps(serialized, ensure_ascii=False)
        except Exception as ex:
            logging.error(f"Error serializing entities: {ex}")
            return None

    @staticmethod
    def render_entities_html(text: str, entities_json: Optional[str]) -> str:
        if not text:
            return ""
        if not entities_json:
            return EmojiManager._html_escape(text)
        try:
            entities = json.loads(entities_json)
        except Exception:
            return EmojiManager._html_escape(text)
        if not entities:
            return EmojiManager._html_escape(text)
        char_to_utf16: List[int] = []
        utf16_pos = 0
        for ch in text:
            char_to_utf16.append(utf16_pos)
            utf16_pos += len(ch.encode('utf-16-le')) // 2
        total_utf16 = utf16_pos
        opens: Dict[int, List[str]] = {}
        closes: Dict[int, List[str]] = {}
        for e in sorted(entities, key=lambda x: (x.get("offset", 0), -(x.get("length", 0)))):
            etype = e.get("type", "")
            offset = e.get("offset", 0)
            length = e.get("length", 0)
            end = offset + length
            open_tag = close_tag = None
            if etype == "bold":
                open_tag, close_tag = "<b>", "</b>"
            elif etype == "italic":
                open_tag, close_tag = "<i>", "</i>"
            elif etype == "code":
                open_tag, close_tag = "<code>", "</code>"
            elif etype == "pre":
                lang = e.get("language", "")
                open_tag = f'<pre><code class="language-{lang}">' if lang else "<pre>"
                close_tag = "</code></pre>" if lang else "</pre>"
            elif etype == "strikethrough":
                open_tag, close_tag = "<s>", "</s>"
            elif etype == "underline":
                open_tag, close_tag = "<u>", "</u>"
            elif etype == "spoiler":
                open_tag, close_tag = '<span class="tg-spoiler">', "</span>"
            elif etype == "blockquote":
                open_tag, close_tag = "<blockquote>", "</blockquote>"
            elif etype == "text_link":
                url = e.get("url", "")
                open_tag, close_tag = f'<a href="{url}">', "</a>"
            elif etype == "custom_emoji":
                emoji_id = e.get("custom_emoji_id", "")
                open_tag = f'<tg-emoji emoji-id="{emoji_id}">'
                close_tag = "</tg-emoji>"
            if open_tag and close_tag:
                opens.setdefault(offset, []).append(open_tag)
                closes.setdefault(end, []).append(close_tag)
        result = []
        for i, ch in enumerate(text):
            u16 = char_to_utf16[i]
            if u16 in closes:
                for ct in reversed(closes[u16]):
                    result.append(ct)
            if u16 in opens:
                for ot in opens[u16]:
                    result.append(ot)
            if ch == '<':
                result.append('&lt;')
            elif ch == '>':
                result.append('&gt;')
            elif ch == '&':
                result.append('&amp;')
            else:
                result.append(ch)
        if total_utf16 in closes:
            for ct in reversed(closes[total_utf16]):
                result.append(ct)
        return "".join(result)

    @staticmethod
    def _html_escape(text: str) -> str:
        return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


class MessageManager:
    @staticmethod
    def extract_from_message(msg) -> dict:
        text = msg.text or msg.caption or ""
        entities = list(msg.entities or msg.caption_entities or [])
        media_id = None
        media_type = None
        file_name = None
        mime_type = None
        telegram_message_id = msg.message_id
        if msg.photo:
            media_id = msg.photo[-1].file_id
            media_type = "photo"
        elif msg.video:
            media_id = msg.video.file_id
            media_type = "video"
            file_name = getattr(msg.video, "file_name", None)
            mime_type = getattr(msg.video, "mime_type", None)
        elif msg.document:
            media_id = msg.document.file_id
            media_type = "document"
            file_name = getattr(msg.document, "file_name", None)
            mime_type = getattr(msg.document, "mime_type", None)
        elif msg.animation:
            media_id = msg.animation.file_id
            media_type = "animation"
            file_name = getattr(msg.animation, "file_name", None)
            mime_type = getattr(msg.animation, "mime_type", None)
        elif msg.audio:
            media_id = msg.audio.file_id
            media_type = "audio"
            file_name = getattr(msg.audio, "file_name", None)
            mime_type = getattr(msg.audio, "mime_type", None)
        elif msg.voice:
            media_id = msg.voice.file_id
            media_type = "voice"
            mime_type = getattr(msg.voice, "mime_type", None)
        elif msg.video_note:
            media_id = msg.video_note.file_id
            media_type = "video_note"
        elif msg.sticker:
            media_id = msg.sticker.file_id
            media_type = "sticker"
        elif msg.contact:
            media_type = "contact"
        elif msg.location:
            media_type = "location"
        elif msg.venue:
            media_type = "venue"
        elif msg.poll:
            media_type = "poll"
        elif msg.dice:
            media_type = "dice"
        emoji_map = EmojiManager.extract_from_entities(text, entities)
        entities_json = EmojiManager.entities_to_json(entities)
        return {
            "text": text, "entities": entities, "entities_json": entities_json,
            "emoji_map": emoji_map, "media_id": media_id, "media_type": media_type,
            "file_name": file_name, "mime_type": mime_type,
            "media_group_id": getattr(msg, "media_group_id", None),
            "telegram_message_id": telegram_message_id,
        }

    @staticmethod
    def prepare_for_sending(text: str, entities_json: Optional[str] = None,
                            emoji_map: Optional[dict] = None) -> str:
        """Prepare text for sending to USERS - uses plain emojis, no premium tags."""
        if not text:
            return ""
        if entities_json:
            # Use entities but any custom_emoji will be rendered as plain text by render_entities_html
            return EmojiManager.render_entities_html(text, entities_json)
        return EmojiManager._html_escape(text)


class UIFormatter:
    """Bot UI messages - use pp() for PREMIUM emojis in admin/bot interface."""
    @staticmethod
    def main_menu(user_name: str = "") -> str:
        name_part = f" <b>{user_name}</b>" if user_name else ""
        return (f"<blockquote>{pp('💎')} <b>WELCOME{name_part}</b></blockquote>\n\n"
                f"<b>Your Premium Bot Panel</b>\n\n"
                f"{pp('💎')} Manage your bots\n"
                f"{pp('⚡️')} Set welcome messages\n"
                f"{pp('📊')} Track your users\n"
                f"{pp('📣')} Broadcast to audience\n\n"
                f"<i>Select a bot below to get started:</i>")

    @staticmethod
    def verification_prompt() -> str:
        return (f"<blockquote>{pp('🔐')} <b>HUMAN VERIFICATION REQUIRED</b></blockquote>\n\n"
                f"To ensure you are a real person, please complete verification.\n\n"
                f"<i>Click the button to verify.</i>")

    @staticmethod
    def verification_success(first_name: str) -> str:
        return (f"<blockquote>{pp('✅')} <b>VERIFICATION COMPLETE</b></blockquote>\n\n"
                f"Welcome, <b>{first_name}</b>! {pp('🎉')}\n\n"
                f"You now have access to the bot panel.")

    @staticmethod
    def subscription_required() -> str:
        return (f"<blockquote>{pp('💎')} <b>SUBSCRIPTION REQUIRED</b></blockquote>\n\n"
                f"You need an active subscription.\n\n"
                f"<b>Plans:</b>\n"
                f"• {pp('💰')} Basic — Rs2599/mo — 1 channel\n"
                f"• {pp('⚡️')} Pro — Rs3999/mo — 5 channels\n\n"
                f"Contact {ADMIN_USERNAME}")

    @staticmethod
    def subscription_details(sub_type: str, expiry, days: int, max_ch: int) -> str:
        if isinstance(expiry, str):
            try:
                expiry = datetime.fromisoformat(expiry.replace('+00:00', ''))
            except:
                expiry = now_aware()
        expiry = make_aware(expiry) if expiry.tzinfo is None else expiry
        status = f"{pp('✅')} Active" if days > 0 else f"{pp('❌')} Expired"
        return (f"<blockquote>{pp('👑')} <b>YOUR SUBSCRIPTION</b></blockquote>\n\n"
                f"{pp('⭐️')} <b>Plan:</b> {sub_type}\n"
                f"{pp('💎')} <b>Max Channels:</b> {max_ch}\n"
                f"{pp('📅')} <b>Expiry:</b> {expiry.strftime('%d %b %Y')}\n"
                f"{pp('⏰')} <b>Days Left:</b> {days}\n"
                f"{pp('🔘')} <b>Status:</b> {status}")

    @staticmethod
    def bot_stats(channels: int, total_users: int, reachable: int, pending: int) -> str:
        return (f"<blockquote>{pp('📊')} <b>BOT STATISTICS</b></blockquote>\n\n"
                f"{pp('📣')} <b>Channels:</b> {channels}\n"
                f"{pp('👀')} <b>Total Users:</b> {total_users}\n"
                f"{pp('✅')} <b>Reachable:</b> {reachable}\n"
                f"{pp('🔔')} <b>Pending:</b> {pending}")

    @staticmethod
    def live_chat_header() -> str:
        return (f"<blockquote>{pp('💬')} <b>LIVE CHAT SUPPORT</b></blockquote>\n\n"
                f"You are now connected to support.\n"
                f"Please type your message.")

    @staticmethod
    def broadcast_confirm(sent: int, failed: int) -> str:
        return (f"<blockquote>{pp('✅')} <b>BROADCAST COMPLETE</b></blockquote>\n\n"
                f"{pp('📤')} Sent: {sent}\n"
                f"{pp('❌')} Failed: {failed}")

    @staticmethod
    def expiry_reminder_3d(sub_type: str, expiry: datetime, days: int) -> str:
        return (f"<blockquote>{pp('🔔')} <b>SUBSCRIPTION EXPIRY REMINDER</b></blockquote>\n\n"
                f"{pp('⭐️')} <b>Plan:</b> {sub_type}\n"
                f"{pp('📅')} <b>Expires:</b> {expiry.strftime('%d %b %Y')}\n"
                f"{pp('⏰')} <b>Days left:</b> {days}\n\n"
                f"Renew now! Contact {ADMIN_USERNAME}")

    @staticmethod
    def expiry_reminder_1d(sub_type: str, expiry: datetime) -> str:
        return (f"<blockquote>{pp('‼️')} <b>LAST DAY REMINDER</b></blockquote>\n\n"
                f"{pp('⭐️')} <b>Plan:</b> {sub_type}\n"
                f"{pp('📅')} <b>Expires TOMORROW:</b> {expiry.strftime('%d %b %Y')}\n\n"
                f"{pp('🚨')} Renew immediately! Contact {ADMIN_USERNAME}")

    @staticmethod
    def subscription_expired() -> str:
        return (f"<blockquote>{pp('❌')} <b>SUBSCRIPTION EXPIRED</b></blockquote>\n\n"
                f"Your bot has been paused. Contact {ADMIN_USERNAME} to renew.")


class UserFlowManager:
    @staticmethod
    def needs_verification(user_id: int) -> bool:
        return not db.is_user_verified(user_id)

    @staticmethod
    def verification_button() -> InlineKeyboardMarkup:
        return verification_kb()


# ================= HELPER FUNCTIONS =================
def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_USER_IDS


def is_bot_owner(bot_id: str, user_id: int) -> bool:
    """Check if user is owner of the bot OR is admin"""
    if is_admin(user_id):
        return True
    bot_data = db.get_user_bot(bot_id)
    return bool(bot_data and bot_data.get("user_id") == user_id)


@retry_async(max_retries=3, delay=1, backoff=2)
async def safe_edit_message_text(q, *args, **kwargs):
    """
    Tries the "styled" version first (premium <tg-emoji> text + custom-emoji button
    icons). Telegram's editMessageText only works on messages originally sent as
    plain TEXT, and custom-emoji references (in text or button icons) can be
    rejected by Telegram as invalid/inaccessible "documents" - both cases can
    surface as BadRequest (e.g. "Document_invalid").

    Fallback chain so navigation never breaks:
      1. Styled edit (as requested)
      2. Plain edit (strip premium emoji tags from text + strip button icons)
      3. Delete old message + send a brand new plain text message
    """
    raw_text = None
    if len(args) > 0:
        args = list(args)
        raw_text = args[0]
        args[0] = premiumize_ui_emojis(raw_text)
    elif "text" in kwargs:
        raw_text = kwargs["text"]
        kwargs["text"] = premiumize_ui_emojis(raw_text)

    try:
        return await q.edit_message_text(*args, **kwargs)
    except BadRequest as ex:
        if "Message is not modified" in str(ex):
            return None

        logging.warning(f"styled edit_message_text failed ({ex}); retrying plain")
        plain_text = strip_premium_emojis(raw_text) if raw_text else raw_text
        plain_kwargs = dict(kwargs)
        if "reply_markup" in plain_kwargs:
            plain_kwargs["reply_markup"] = _degrade_markup(plain_kwargs["reply_markup"])
        try:
            if len(args) > 0:
                plain_args = list(args)
                plain_args[0] = plain_text
                return await q.edit_message_text(*plain_args, **plain_kwargs)
            else:
                plain_kwargs["text"] = plain_text
                return await q.edit_message_text(**plain_kwargs)
        except Exception as ex2:
            logging.warning(f"plain edit also failed ({ex2}); falling back to delete+resend")

        try:
            await q.message.delete()
        except Exception:
            pass
        try:
            send_kwargs = {k: v for k, v in plain_kwargs.items() if k != "text"}
            return await q.message.chat.send_message(plain_text or "", **send_kwargs)
        except Exception as send_ex:
            logging.error(f"safe_edit_message_text fallback send also failed: {send_ex}")
            return None


@retry_async(max_retries=2, delay=0.5, backoff=1.5)
async def send_premium_message(bot, chat_id, text, *args, **kwargs):
    """Send message with PREMIUM emojis (for bot UI/admin messages). Falls back to
    a plain (non-premium) version if Telegram rejects the styled one."""
    try:
        premium_text = premiumize_ui_emojis(text)
        return await bot.send_message(chat_id, premium_text, *args, **kwargs)
    except Forbidden:
        logging.warning(f"Cannot send message to {chat_id}: bot blocked or can't initiate")
        return None
    except BadRequest as ex:
        logging.warning(f"styled send_premium_message failed for {chat_id} ({ex}); retrying plain")
        try:
            plain_kwargs = dict(kwargs)
            if "reply_markup" in plain_kwargs:
                plain_kwargs["reply_markup"] = _degrade_markup(plain_kwargs["reply_markup"])
            return await bot.send_message(chat_id, strip_premium_emojis(text), *args, **plain_kwargs)
        except Exception as ex2:
            logging.error(f"plain retry of send_premium_message also failed for {chat_id}: {ex2}")
            return None
    except (NetworkError, TimedOut) as ex:
        logging.warning(f"Network error sending to {chat_id}: {ex}")
        raise
    except Exception as ex:
        logging.error(f"send_premium_message failed: {ex}")
        return None


@retry_async(max_retries=2, delay=0.5, backoff=1.5)
async def reply_premium_message(message, text, *args, **kwargs):
    """Reply with PREMIUM emojis (for bot UI/admin messages). Falls back to a
    plain (non-premium) version if Telegram rejects the styled one."""
    try:
        premium_text = premiumize_ui_emojis(text)
        return await message.reply_text(premium_text, *args, **kwargs)
    except Forbidden:
        logging.warning(f"Cannot reply to {message.chat_id}: bot blocked")
        return None
    except BadRequest as ex:
        logging.warning(f"styled reply_premium_message failed for {message.chat_id} ({ex}); retrying plain")
        try:
            plain_kwargs = dict(kwargs)
            if "reply_markup" in plain_kwargs:
                plain_kwargs["reply_markup"] = _degrade_markup(plain_kwargs["reply_markup"])
            return await message.reply_text(strip_premium_emojis(text), *args, **plain_kwargs)
        except Exception as ex2:
            logging.error(f"plain retry of reply_premium_message also failed for {message.chat_id}: {ex2}")
            return None
    except (NetworkError, TimedOut) as ex:
        logging.warning(f"Network error replying to {message.chat_id}: {ex}")
        raise
    except Exception as ex:
        logging.error(f"reply_premium_message failed: {ex}")
        return None


@retry_async(max_retries=2, delay=0.5, backoff=1.5)
async def send_user_message(bot, chat_id, text, *args, **kwargs):
    """Send user-facing messages without stripping premium custom emoji tags."""
    try:
        return await bot.send_message(chat_id, text, *args, **kwargs)
    except Forbidden:
        logging.warning(f"Cannot send message to {chat_id}: bot blocked or can't initiate")
        return None
    except BadRequest as ex:
        if kwargs.get("parse_mode") == ParseMode.HTML and "can't parse entities" in str(ex).lower():
            logging.warning(f"HTML parse failed for {chat_id}; retrying with escaped text: {ex}")
            safe_kwargs = dict(kwargs)
            safe_kwargs["parse_mode"] = ParseMode.HTML
            safe_text = escape_preserving_premium_emojis(text)
            try:
                return await bot.send_message(chat_id, safe_text, *args, **safe_kwargs)
            except Exception as retry_ex:
                logging.error(f"send_user_message escaped retry failed: {retry_ex}")
                return None
        logging.error(f"send_user_message failed: {ex}")
        return None
    except (NetworkError, TimedOut) as ex:
        logging.warning(f"Network error sending to {chat_id}: {ex}")
        raise
    except Exception as ex:
        logging.error(f"send_user_message failed: {ex}")
        return None


DEFAULT_WELCOME_MESSAGE = "Hello {first_name}, Aapki request mil gayi hai, jaldi hi accept ho jayegi."


def render_dynamic_text(text: Optional[str], user=None, extra: Optional[dict] = None) -> str:
    if not text:
        return ""
    first_name = getattr(user, "first_name", None) or "User"
    last_name = getattr(user, "last_name", None) or ""
    username = getattr(user, "username", None) or ""
    user_id = getattr(user, "id", None) or ""
    values = {"first_name": first_name, "last_name": last_name,
              "full_name": f"{first_name} {last_name}".strip(),
              "username": f"@{username}" if username else "", "user_id": str(user_id)}
    if extra:
        values.update({str(k): "" if v is None else str(v) for k, v in extra.items()})
    rendered = text
    for key, value in values.items():
        rendered = rendered.replace("{" + key + "}", str(value))
    return rendered


# Emoji detection regex - used to decide whether a button label already carries the
# owner's own emoji (in that case we must NOT attach a default icon on top of it).
_EMOJI_RE = re.compile(
    "[\U0001F000-\U0001FAFF\U00002600-\U000027BF\U0001F1E6-\U0001F1FF"
    "\U00002190-\U000021FF\U00002B00-\U00002BFF\U0000FE00-\U0000FE0F"
    "\U0001F900-\U0001F9FF\u2764\u2765\u203C\u2049\u3030\u303D\u3297\u3299]"
)


def has_emoji(text: Optional[str]) -> bool:
    """True when the text contains at least one emoji/pictograph character."""
    if not text:
        return False
    return bool(_EMOJI_RE.search(text))


def parse_buttons_text(text: Optional[str]):
    if not text:
        return []
    buttons = []
    for line in text.strip().split('\n'):
        line = line.strip()
        if not line:
            continue
        parts = [p.strip() for p in line.split(' || ')]
        row_buttons = []
        for part in parts:
            if '|' not in part:
                continue
            idx = part.index('|')
            label = part[:idx].strip()
            url = part[idx + 1:].strip()
            if label and url:
                # FIX: the owner's own emoji stays exactly as typed inside the
                # button text. We no longer strip it and no longer force the
                # default "link" icon on top of it.
                row_buttons.append(InlineKeyboardButton(
                    label, url=url, api_kwargs={"style": "primary"}
                ))
        if row_buttons:
            buttons.append(row_buttons)
    return buttons if buttons else []


def buttons_to_markup(buttons_json: Optional[str]):
    if not buttons_json:
        return None
    try:
        data = json.loads(buttons_json)
        if not data:
            return None
        rows = []
        for row in data:
            row_btns = []
            for btn_data in row:
                text = btn_data.get('text', '')
                # Legacy rows (saved before the emoji fix) stored a stripped label
                # plus an "icon_id". Keep rendering those exactly as before so old
                # bots look unchanged. New rows carry the emoji inside the text and
                # have no icon_id at all.
                icon_id = btn_data.get('icon_id') if not has_emoji(text) else None
                if btn_data.get('url'):
                    api_kwargs = {"style": "primary"}
                    if icon_id:
                        api_kwargs["icon_custom_emoji_id"] = icon_id
                    row_btns.append(InlineKeyboardButton(
                        text, url=btn_data['url'], api_kwargs=api_kwargs
                    ))
                elif btn_data.get('cb'):
                    row_btns.append(InlineKeyboardButton(text, callback_data=btn_data['cb']))
                elif btn_data.get('callback_data'):
                    row_btns.append(InlineKeyboardButton(text, callback_data=btn_data['callback_data']))
            if row_btns:
                rows.append(row_btns)
        return InlineKeyboardMarkup(rows) if rows else None
    except Exception:
        return None


def buttons_json_from_text(text: str):
    if not text:
        return None
    json_rows = []
    for line in text.strip().split('\n'):
        line = line.strip()
        if not line:
            continue
        parts = [p.strip() for p in line.split(' || ')]
        json_row = []
        for part in parts:
            if '|' not in part:
                continue
            idx = part.index('|')
            label = part[:idx].strip()
            url = part[idx + 1:].strip()
            if not label or not url:
                continue
            # FIX (owner's own emoji): the label is stored verbatim - emoji included.
            # No icon_id is written for new buttons, so Telegram shows the emoji the
            # owner actually typed instead of the default link icon.
            json_row.append({
                "text": label,
                "url": url
            })
        if json_row:
            json_rows.append(json_row)
    return json.dumps(json_rows) if json_rows else None


def add_callback_button_to_json(buttons_json: Optional[str], text: str, cb: str, url: Optional[str] = None) -> str:
    data = []
    if buttons_json:
        try:
            data = json.loads(buttons_json) or []
        except Exception:
            data = []
    for row in data:
        for btn_data in row:
            if (btn_data.get('cb') == cb or btn_data.get('callback_data') == cb or (url and btn_data.get('url') == url)):
                return json.dumps(data)
    if url:
        data.append([{"text": text, "url": url}])
    else:
        data.append([{"text": text, "cb": cb}])
    return json.dumps(data)


async def send_ephemeral_reply(msg, text: str, seconds: int = 2):
    try:
        sent = await reply_premium_message(msg, text, parse_mode=ParseMode.HTML)
        if sent:
            await asyncio.sleep(seconds)
            try:
                await sent.delete()
            except Exception:
                pass
    except Exception:
        pass


@retry_async(max_retries=2, delay=0.5, backoff=1.5)
async def send_media(bot_or_context, chat_id: int, media_id, media_type: str,
                     text: str = "", markup=None, emoji_map: dict = None,
                     entities_json: Optional[str] = None, file_name: Optional[str] = None,
                     mime_type: Optional[str] = None):
    bot = bot_or_context if isinstance(bot_or_context, Bot) else bot_or_context.bot
    kwargs = {}
    if markup:
        kwargs["reply_markup"] = markup
    if text:
        display_text = MessageManager.prepare_for_sending(text, entities_json, emoji_map)
    else:
        display_text = None
    async def _do_send(send_kwargs):
        if media_type == "photo":
            await bot.send_photo(chat_id, media_id, caption=display_text or None, parse_mode=ParseMode.HTML if display_text else None, **send_kwargs)
        elif media_type == "video":
            await bot.send_video(chat_id, media_id, caption=display_text or None, parse_mode=ParseMode.HTML if display_text else None, **send_kwargs)
        elif media_type == "document":
            doc_kwargs = dict(send_kwargs)
            if file_name:
                doc_kwargs["filename"] = file_name
            await bot.send_document(chat_id, media_id, caption=display_text or None, parse_mode=ParseMode.HTML if display_text else None, **doc_kwargs)
        elif media_type == "animation":
            await bot.send_animation(chat_id, media_id, caption=display_text or None, parse_mode=ParseMode.HTML if display_text else None, **send_kwargs)
        elif media_type == "audio":
            await bot.send_audio(chat_id, media_id, caption=display_text or None, parse_mode=ParseMode.HTML if display_text else None, **send_kwargs)
        elif media_type == "voice":
            await bot.send_voice(chat_id, media_id, caption=display_text or None, parse_mode=ParseMode.HTML if display_text else None, **send_kwargs)
        elif media_type == "video_note":
            await bot.send_video_note(chat_id, media_id, **send_kwargs)
        elif media_type == "sticker":
            await bot.send_sticker(chat_id, media_id, **send_kwargs)
        else:
            if display_text:
                await send_user_message(bot, chat_id, display_text, parse_mode=ParseMode.HTML, **send_kwargs)

    try:
        await _do_send(kwargs)
    except Forbidden:
        logging.warning(f"Cannot send media to {chat_id}: bot blocked")
    except BadRequest as ex:
        # Try again with button icons stripped first - a single bad custom-emoji
        # icon on a button shouldn't cost the whole media delivery.
        logging.warning(f"send_media BadRequest for {chat_id} ({media_type}): {ex}; retrying with plain buttons")
        degraded_kwargs = dict(kwargs)
        if "reply_markup" in degraded_kwargs:
            degraded_kwargs["reply_markup"] = _degrade_markup(degraded_kwargs["reply_markup"])
        try:
            await _do_send(degraded_kwargs)
        except Exception as ex2:
            logging.error(f"send_media plain-button retry also failed for {chat_id} ({media_type}): {ex2}")
            try:
                plain_text = (text or "").strip()
                if plain_text:
                    await send_user_message(bot, chat_id, plain_text, **degraded_kwargs)
            except Exception:
                pass
    except (NetworkError, TimedOut) as ex:
        logging.warning(f"Network error sending to {chat_id}: {ex}")
        raise
    except Exception as ex:
        logging.error(f"send_media failed: {ex}")
        try:
            plain_text = (text or "").strip()
            if plain_text:
                await send_user_message(bot, chat_id, plain_text, **kwargs)
        except Exception:
            pass


# ================= SAFE COPY MESSAGE =================
@retry_async(max_retries=2, delay=0.5, backoff=1.5)
async def safe_copy_message(bot, chat_id: int, from_chat_id: int, message_id: int,
                             fallback_text: str = None) -> Optional[Any]:
    try:
        return await bot.copy_message(chat_id=chat_id, from_chat_id=from_chat_id, message_id=message_id)
    except BadRequest as ex:
        err = str(ex)
        if "Document_invalid" in err or "document_invalid" in err.lower() or "DOCUMENT_INVALID" in err:
            try:
                return await bot.forward_message(chat_id=chat_id, from_chat_id=from_chat_id, message_id=message_id)
            except Exception as fwd_ex:
                logging.warning(f"forward_message also failed: {fwd_ex}")
                if fallback_text:
                    try:
                        return await bot.send_message(chat_id=chat_id, text=fallback_text,
                                                       parse_mode=ParseMode.HTML, disable_web_page_preview=True)
                    except Exception:
                        pass
                return None
        # Other permanent BadRequest errors - don't retry, just log and give up gracefully.
        logging.warning(f"safe_copy_message BadRequest (not retrying): {ex}")
        if fallback_text:
            try:
                return await bot.send_message(chat_id=chat_id, text=fallback_text,
                                               parse_mode=ParseMode.HTML, disable_web_page_preview=True)
            except Exception:
                pass
        return None


# ================= SUBSCRIPTION JOBS =================
async def check_expired_subscriptions_job(context: ContextTypes.DEFAULT_TYPE):
    expired_bots = db.get_expired_subscriptions()
    for bot_id in expired_bots:
        bot_data = db.get_user_bot(bot_id)
        if bot_data and bot_data["is_active"] == 1:
            if bot_id in user_bot_applications:
                try:
                    app = user_bot_applications[bot_id]
                    await app.updater.stop()
                    await app.stop()
                    await app.shutdown()
                except Exception:
                    pass
                user_bot_applications.pop(bot_id, None)
            db.set_user_bot_active(bot_id, False)
            await send_premium_message(context.bot, bot_data["user_id"], UIFormatter.subscription_expired(),
                parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup([[
                    btn("Renew Now", f"https://t.me/{ADMIN_USERNAME.lstrip('@')}", "danger", "💰")
                ]]))


async def subscription_reminder_job(context: ContextTypes.DEFAULT_TYPE):
    _cleanup_support_maps()
    for days_threshold in [3, 1]:
        expiring = db.get_expiring_subscriptions(days_threshold)
        for sub in expiring:
            bot_id = sub["bot_id"]
            sub_type = sub["subscription_type"]
            bot_data = db.get_user_bot(bot_id)
            if not bot_data:
                continue
            try:
                expiry_dt = sub["expiry_date"]
                if isinstance(expiry_dt, str):
                    expiry_dt = datetime.fromisoformat(expiry_dt.replace('+00:00', ''))
                expiry_dt = make_aware(expiry_dt)
                days_left = (expiry_dt - now_aware()).days
                if days_threshold == 3:
                    msg_text = UIFormatter.expiry_reminder_3d(sub_type, expiry_dt, days_left)
                else:
                    msg_text = UIFormatter.expiry_reminder_1d(sub_type, expiry_dt)
                await send_premium_message(context.bot, bot_data["user_id"], msg_text, parse_mode=ParseMode.HTML,
                    reply_markup=InlineKeyboardMarkup([[
                        btn("Renew", f"https://t.me/{ADMIN_USERNAME.lstrip('@')}", "danger", "💰")
                    ]]))
                db.mark_reminder_sent(bot_id, days_threshold)
            except Exception as ex:
                logging.error(f"Reminder error: {ex}")


# ================= MESSAGE SENDING =================
async def _send_messages_with_media_groups(chat_id: int, msgs: List[dict], context: ContextTypes.DEFAULT_TYPE,
                                            bot_id: str = None, attach_start_button: bool = True, placeholder_user=None):
    live_chat_markup = None
    if bot_id and attach_start_button:
        try:
            bot_data = db.get_user_bot(bot_id)
            if bot_data and bot_data.get("bot_username"):
                live_chat_url = f"https://t.me/{bot_data['bot_username']}?start=live_chat"
                live_chat_markup = InlineKeyboardMarkup([[btn_url("Live Chat Support", live_chat_url, "success", "💬")]])
        except Exception:
            pass

    i = 0
    while i < len(msgs):
        row = msgs[i]
        text = render_dynamic_text(row.get("content_text", ""), placeholder_user)
        media_id = row.get("media_id")
        media_type = row.get("media_type")
        media_group_id = row.get("media_group_id")
        buttons_json = row.get("buttons_json")
        entities_json = row.get("entities_json")
        file_name = row.get("file_name")
        mime_type = row.get("mime_type")

        if media_group_id:
            group_items = []
            group_buttons_json = None
            group_caption_text = None
            j = i
            while j < len(msgs) and msgs[j].get("media_group_id") == media_group_id:
                g = msgs[j]
                g_text = render_dynamic_text(g.get("content_text", ""), placeholder_user)
                g_media_id = g.get("media_id")
                g_media_type = g.get("media_type")
                if not group_buttons_json and g.get("buttons_json"):
                    group_buttons_json = g.get("buttons_json")
                if not group_caption_text and g_text:
                    group_caption_text = g_text
                if g_media_id and g_media_type in ("photo", "video", "document", "audio"):
                    display_text = MessageManager.prepare_for_sending(g_text, g.get("entities_json")) if g_text else None
                    pm = ParseMode.HTML if display_text else None
                    if g_media_type == "photo":
                        group_items.append(InputMediaPhoto(media=g_media_id, caption=display_text or None, parse_mode=pm))
                    elif g_media_type == "video":
                        group_items.append(InputMediaVideo(media=g_media_id, caption=display_text or None, parse_mode=pm))
                    elif g_media_type == "document":
                        group_items.append(InputMediaDocument(media=g_media_id, caption=display_text or None, parse_mode=pm))
                j += 1
            if group_items:
                try:
                    await context.bot.send_media_group(chat_id=chat_id, media=group_items)
                except BadRequest as ex:
                    logging.error(f"send_media_group failed for {chat_id}: {ex}")
                group_markup = buttons_to_markup(group_buttons_json)
                if group_markup:
                    await send_user_message(context.bot, chat_id, group_caption_text or "Open links:", parse_mode=ParseMode.HTML, reply_markup=group_markup)
            i = j
            continue

        markup = buttons_to_markup(buttons_json)
        if i == len(msgs) - 1 and live_chat_markup:
            if markup:
                combined_rows = markup.inline_keyboard + live_chat_markup.inline_keyboard
                markup = InlineKeyboardMarkup(combined_rows)
            else:
                markup = live_chat_markup
        await send_media(context, chat_id, media_id, media_type or "text", text or "", markup,
                         entities_json=entities_json, file_name=file_name, mime_type=mime_type)
        i += 1


async def send_saved_welcome(bot_id: str, chat_id: int, context: ContextTypes.DEFAULT_TYPE, user=None):
    try:
        channels = db.get_bot_channels(bot_id) or []
        if not channels:
            await send_user_message(context.bot, chat_id, render_dynamic_text(DEFAULT_WELCOME_MESSAGE, user), parse_mode=ParseMode.HTML)
            return
        channel_id = channels[0]["channel_id"]
        msgs = db.get_messages(channel_id, bot_id) or []
        if not msgs:
            await send_user_message(context.bot, chat_id, render_dynamic_text(DEFAULT_WELCOME_MESSAGE, user), parse_mode=ParseMode.HTML)
            return
        await _send_messages_with_media_groups(chat_id, msgs, context, bot_id=bot_id, placeholder_user=user)
    except Exception as ex:
        logging.error(f"send_saved_welcome error: {ex}")


def _runtime_store(context: ContextTypes.DEFAULT_TYPE, key: str) -> dict:
    if key not in context.user_data:
        context.user_data[key] = {}
    return context.user_data[key]


# Transient "the bot is waiting for your next message" flags. When one of these is
# left behind (user pressed a back/cancel button that did not clean up, or the flow
# was interrupted) the next message the owner sends gets swallowed by the wrong
# branch - which is exactly why setting a second message kept failing and the menu
# kept popping up again.
FLOW_STATE_KEYS = (
    "setting_message", "messages", "pending_buttons", "pending_buttons_group",
    "pending_buttons_group_addmore", "waiting_buttons",
    "editing_text_msg_id", "editing_media_msg_id", "editing_buttons_msg_id",
    "adding_channel",
)


def clear_flow_state(context: ContextTypes.DEFAULT_TYPE, uid: int, bot_id: str) -> None:
    """Drop every pending-input flag of this owner for this bot."""
    ud = context.user_data.get(f"{uid}_{bot_id}")
    if isinstance(ud, dict):
        for key in FLOW_STATE_KEYS:
            ud.pop(key, None)
    for key in FLOW_STATE_KEYS:
        context.user_data.pop(f"{key}_{bot_id}", None)


def has_active_flow(context: ContextTypes.DEFAULT_TYPE, uid: int, bot_id: str) -> bool:
    """True while the bot is waiting for a message/media from this owner for this bot."""
    ud = context.user_data.get(f"{uid}_{bot_id}") or {}
    if not isinstance(ud, dict):
        return False
    for key in ("setting_message", "waiting_buttons", "adding_channel",
                "editing_text_msg_id", "editing_media_msg_id", "editing_buttons_msg_id"):
        if ud.get(key):
            return True
    for key in ("setting_message", "waiting_buttons", "adding_channel"):
        if context.user_data.get(f"{key}_{bot_id}"):
            return True
    if context.user_data.get(f"broadcast_stage_{bot_id}"):
        return True
    return False


def bot_for(bot_id: str, context: ContextTypes.DEFAULT_TYPE):
    """Return the Bot object that must talk to this userbot's users.

    When the owner/admin drives the panel from the MAIN bot, context.bot is the main
    bot - using it would send DMs/join-request approvals from the wrong account.
    """
    app = user_bot_applications.get(bot_id)
    if app is not None:
        try:
            return app.bot
        except Exception:
            pass
    return context.bot


async def sync_pending_join_requests_for_channel(bot_id: str, channel_id: int, bot):
    try:
        if not hasattr(bot, 'get_chat_join_requests'):
            logging.info(f"get_chat_join_requests not available in this PTB version. Skipping sync.")
            return
        count = 0
        async for jr in bot.get_chat_join_requests(channel_id):
            db.add_join_request(bot_id, jr.from_user.id, channel_id, "pending")
            count += 1
        if count > 0:
            logging.info(f"Synced {count} pending join requests for channel {channel_id}")
    except Exception as ex:
        logging.warning(f"sync_pending_join_requests_for_channel: {ex}")


# ================= USER BOT START =================
async def user_bot_start(update: Update, context: ContextTypes.DEFAULT_TYPE, bot_id: str, owner_id: int):
    user = update.effective_user
    if not user:
        return
    db.add_user(user.id, user.username, user.first_name, user.last_name)
    # FIX: previously this only checked is_admin(user.id), so the actual bot OWNER
    # (the client who bought/created this userbot) fell through to the regular
    # subscriber welcome flow instead of getting their bot management panel.
    # is_bot_owner() correctly covers "is admin OR is the owner of this bot_id".
    if is_bot_owner(bot_id, user.id):
        bot_data = db.get_user_bot(bot_id)
        bot_username = bot_data.get("bot_username") if bot_data else None
        title = f"@{bot_username}" if bot_username else bot_id
        await send_premium_message(context.bot, user.id,
            f"<blockquote>{pp('🤖')} <b>MANAGE BOT</b></blockquote>\n\nBot: {title}\nBot ID: {bot_id}",
            parse_mode=ParseMode.HTML, reply_markup=bot_management_kb(bot_id, user.id))
        return
    start_param = context.args[0] if context.args else ""
    if start_param == "live_chat":
        await send_premium_message(context.bot, user.id, UIFormatter.live_chat_header(), parse_mode=ParseMode.HTML)
        return
    await send_saved_welcome(bot_id, user.id, context, user=user)


async def handle_public_userbot_callback(update: Update, context: ContextTypes.DEFAULT_TYPE, bot_id: str):
    q = update.callback_query
    if not q:
        return
    try:
        await q.answer()
    except Exception:
        pass
    data = q.data
    user = q.from_user
    if data == "live_chat_support":
        await send_premium_message(context.bot, user.id, UIFormatter.live_chat_header(), parse_mode=ParseMode.HTML)


async def user_bot_callback(update: Update, context: ContextTypes.DEFAULT_TYPE, bot_id: str, owner_id: int):
    q = update.callback_query
    if not q or not q.from_user:
        return
    try:
        await q.answer()
    except Exception:
        pass
    uid = q.from_user.id
    data = q.data

    # Check if user is owner OR admin
    if not is_bot_owner(bot_id, uid):
        await safe_edit_message_text(q, f"{pe('❌')} You don't have permission to manage this bot.", parse_mode=ParseMode.HTML)
        return

    # FIX: "Set Inline Button" used to be a dead button. The dedicated
    # handle_set_buttons_callback handler is registered AFTER the generic one, and
    # in python-telegram-bot only the FIRST matching handler of a group runs - so
    # these two callback shapes landed here, matched nothing and silently did
    # nothing. Route them explicitly (and they are also registered first now).
    if data == f"setbtng_{bot_id}" or data.startswith(f"setbtn_{bot_id}_"):
        await handle_set_buttons_callback(update, context, bot_id, owner_id)
        return

    if data == "main_menu":
        clear_flow_state(context, uid, bot_id)
        user = q.from_user
        await safe_edit_message_text(q, UIFormatter.main_menu(user.first_name), parse_mode=ParseMode.HTML,
            reply_markup=bot_management_kb(bot_id, uid))
        return

    if data == f"back_to_manage_{bot_id}" or data == f"manage_bot_{bot_id}":
        # Going back to the manage menu ends any half-finished input flow, otherwise
        # a stale state (waiting_buttons / editing_*) keeps eating the next messages.
        clear_flow_state(context, uid, bot_id)
        await safe_edit_message_text(q, f"<blockquote>{pp('🤖')} <b>MANAGE BOT</b></blockquote>", parse_mode=ParseMode.HTML,
            reply_markup=bot_management_kb(bot_id, uid))
        return

    if data == f"sub_for_bot_{bot_id}":
        await safe_edit_message_text(q, UIFormatter.subscription_required(), parse_mode=ParseMode.HTML,
            reply_markup=subscription_plans_kb(bot_id))
        return

    if data == f"sub_basic_{bot_id}" or data == f"sub_pro_{bot_id}":
        plan = "Basic" if data.startswith("sub_basic_") else "Pro"
        await safe_edit_message_text(q,
            f"<blockquote>{pp('💰') if plan == 'Basic' else pp('⚡️')} <b>{plan} PLAN SELECTED</b></blockquote>\n\n"
            f"{'Rs2599/month — 1 channel' if plan == 'Basic' else 'Rs3999/month — 5 channels'}\n\n"
            f"{pp('📞')} Contact {ADMIN_USERNAME} to complete payment.",
            parse_mode=ParseMode.HTML, reply_markup=subscription_plans_kb(bot_id))
        return

    if data == f"ub_subscription_{bot_id}":
        sub = db.get_subscription_for_bot(bot_id)
        if sub:
            try:
                expiry = make_aware(sub["expiry_date"]) if isinstance(sub["expiry_date"], datetime) else sub["expiry_date"]
                days = (expiry - now_aware()).days
                await safe_edit_message_text(q, UIFormatter.subscription_details(sub["subscription_type"], expiry, days, sub["max_channels"]),
                    parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup([[btn("Back", f"manage_bot_{bot_id}", "primary", "🔙")]]))
            except Exception as ex:
                await safe_edit_message_text(q, f"Error: {ex}", parse_mode=ParseMode.HTML, reply_markup=bot_management_kb(bot_id, uid))
        return

    if data == f"ub_stats_{bot_id}":
        channels = db.get_bot_channels(bot_id) or []
        total = db.get_total_requesters_count(bot_id)
        reachable = db.get_reachable_requesters_count(bot_id)
        pending = db.get_pending_count(bot_id)
        await safe_edit_message_text(q, UIFormatter.bot_stats(len(channels), total, reachable, pending),
            parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup([[btn("Back", f"manage_bot_{bot_id}", "primary", "🔙")]]))
        return

    if data == f"ub_add_channel_{bot_id}":
        ud = _runtime_store(context, f"{uid}_{bot_id}")
        ud["adding_channel"] = True
        context.user_data[f"adding_channel_{bot_id}"] = True
        await safe_edit_message_text(q,
            f"<blockquote>{pp('✈️')} <b>ADD CHANNEL</b></blockquote>\n\n"
            "1. Add this bot as admin in your channel\n"
            "2. Forward any message from that channel here\n\n"
            "<i>The bot will auto-detect the channel.</i>",
            parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup([[btn("Cancel", f"manage_bot_{bot_id}", "danger", "❌")]]))
        return

    if data == f"ub_set_message_{bot_id}":
        channels = db.get_bot_channels(bot_id)
        if not channels:
            await safe_edit_message_text(q, f"{pe('❌')} Add a channel first.", parse_mode=ParseMode.HTML, reply_markup=bot_management_kb(bot_id, uid))
            return
        # Always start from a clean slate: a leftover waiting_buttons/editing_* flag
        # from a previous (aborted) attempt would otherwise swallow the new message.
        clear_flow_state(context, uid, bot_id)
        ud = _runtime_store(context, f"{uid}_{bot_id}")
        ud["setting_message"] = True
        context.user_data[f"setting_message_{bot_id}"] = True
        await safe_edit_message_text(q,
            f"<blockquote>{pp('📝')} <b>SET MESSAGES</b></blockquote>\n\n"
            "Send messages one by one.\n\n"
            "Supported: text, photo, video, document, audio, sticker, media albums\n\n"
            "Placeholders: <code>{{first_name}}</code> <code>{{username}}</code> <code>{{user_id}}</code>\n\n"
            "Type <b>done</b> when finished.",
            parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup([[btn("Cancel", f"setmsg_cancel_{bot_id}", "danger", "❌")]]))
        return

    if data == f"ub_delete_messages_{bot_id}":
        channels = db.get_bot_channels(bot_id)
        if not channels:
            await safe_edit_message_text(q, f"{pe('❌')} No channels found.", parse_mode=ParseMode.HTML, reply_markup=bot_management_kb(bot_id, uid))
            return
        channel_id = channels[0]["channel_id"]
        db.clear_messages(bot_id, channel_id)
        await safe_edit_message_text(q, f"{pe('✅')} All messages deleted.", parse_mode=ParseMode.HTML, reply_markup=bot_management_kb(bot_id, uid))
        return

    if data == f"ub_list_channels_{bot_id}":
        channels = db.get_bot_channels(bot_id)
        if not channels:
            await safe_edit_message_text(q, f"{pe('📋')} No channels added yet.", parse_mode=ParseMode.HTML, reply_markup=bot_management_kb(bot_id, uid))
            return
        lines = [f"<blockquote>{pp('📋')} <b>MY CHANNELS</b></blockquote>\n"]
        for ch in channels:
            lines.append(f"• {ch['channel_title']} (<code>{ch['channel_id']}</code>)")
        await safe_edit_message_text(q, "\n".join(lines), parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([[btn("Back", f"manage_bot_{bot_id}", "primary", "🔙")]]))
        return

    if data == f"ub_remove_channel_{bot_id}":
        await prompt_remove_channel(q, bot_id, uid)
        return

    if data.startswith(f"removechan_{bot_id}_"):
        parts = data.split("_")
        channel_id = _extract_last_id(parts)
        if channel_id:
            db.remove_channel(bot_id, channel_id)
            await safe_edit_message_text(q, f"{pe('✅')} Channel removed.", parse_mode=ParseMode.HTML, reply_markup=bot_management_kb(bot_id, uid))
        return

    if data == f"ub_toggle_auto_{bot_id}":
        channels = db.get_bot_channels(bot_id)
        if not channels:
            await safe_edit_message_text(q, f"{pe('❌')} No channels.", parse_mode=ParseMode.HTML, reply_markup=bot_management_kb(bot_id, uid))
            return
        kb = []
        for ch in channels:
            auto = int(ch.get("auto_approve", 0)) == 1
            status = f"{pe('🟢')} ON" if auto else f"{pe('🔴')} OFF"
            kb.append([btn(f"{ch['channel_title'][:20]} — Auto: {status}", f"toggleauto_{bot_id}_{ch['channel_id']}", "primary", "⚙️")])
        kb.append([btn("Back", f"manage_bot_{bot_id}", "primary", "🔙")])
        await safe_edit_message_text(q, f"<blockquote>{pp('⚙️')} <b>AUTO-APPROVE SETTINGS</b></blockquote>",
            parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup(kb))
        return

    if data.startswith(f"toggleauto_{bot_id}_"):
        parts = data.split("_")
        channel_id = _extract_last_id(parts)
        ch_data = db.get_channel_owner_data(channel_id, bot_id)
        if ch_data:
            current = int(ch_data.get("auto_approve", 0)) == 1
            db.set_auto_approve(bot_id, channel_id, not current)
            new_status = f"{pe('🟢')} ON" if not current else f"{pe('🔴')} OFF"
            await safe_edit_message_text(q, f"{pe('✅')} Auto-approve set to <b>{new_status}</b>",
                parse_mode=ParseMode.HTML, reply_markup=bot_management_kb(bot_id, uid))
        return

    if data == f"ub_pending_requests_{bot_id}":
        pending = db.get_pending_count(bot_id)
        await safe_edit_message_text(q, f"<blockquote>{pp('📊')} <b>PENDING REQUESTS</b></blockquote>\n\n{pe('🔔')} Total pending: <b>{pending}</b>",
            parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup([[btn("Back", f"manage_bot_{bot_id}", "primary", "🔙")]]))
        return

    if data == f"ub_accept_all_{bot_id}":
        await accept_all(q, bot_id, uid, context)
        return

    if data == f"ub_broadcast_{bot_id}":
        context.user_data[f"broadcast_stage_{bot_id}"] = "await_message"
        await safe_edit_message_text(q,
            f"<blockquote>{pp('✈️')} <b>BROADCAST</b></blockquote>\n\nSend message to broadcast to all your users:",
            parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup([[btn("Cancel", f"manage_bot_{bot_id}", "danger", "❌")]]))
        return

    if data == f"bcast_add_btns_{bot_id}":
        context.user_data[f"broadcast_stage_{bot_id}"] = "await_buttons"
        await safe_edit_message_text(q,
            f"{pe('🔘')} Send inline buttons (Text|https://link per line):",
            parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup([[btn("Back", f"manage_bot_{bot_id}", "primary", "🔙")]]))
        return

    if data == f"bcast_send_{bot_id}":
        await preview_user_broadcast(q, context, bot_id, uid)
        return

    if data == f"bcast_confirm_{bot_id}":
        await send_user_broadcast(q, context, bot_id, uid)
        return

    if data.startswith(f"ub_manage_messages_{bot_id}"):
        channels = db.get_bot_channels(bot_id)
        if not channels:
            await safe_edit_message_text(q, f"{pe('❌')} No channels.", parse_mode=ParseMode.HTML, reply_markup=bot_management_kb(bot_id, uid))
            return
        channel_id = channels[0]["channel_id"]
        msgs = db.get_messages(channel_id, bot_id)
        if not msgs:
            await safe_edit_message_text(q, f"{pe('📭')} No messages saved.", parse_mode=ParseMode.HTML, reply_markup=bot_management_kb(bot_id, uid))
            return
        kb = []
        for i, m in enumerate(msgs):
            preview = (m.get("content_text") or m.get("media_type") or "Message")[:30]
            kb.append([btn(f"#{i+1} {preview}", f"ubmm_{bot_id}_{channel_id}_{m['id']}", "primary", "📝")])
        kb.append([btn("Back", f"manage_bot_{bot_id}", "primary", "🔙")])
        await safe_edit_message_text(q, f"<blockquote>{pp('👀')} <b>YOUR MESSAGES</b></blockquote>\n\nSelect a message to edit:",
            parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup(kb))
        return

    if data.startswith(f"ubmm_{bot_id}_"):
        parts = data.split("_")
        msg_id = _extract_last_id(parts)
        row = db.get_message_by_id(msg_id)
        if not row:
            await safe_edit_message_text(q, f"{pe('❌')} Message not found.", parse_mode=ParseMode.HTML, reply_markup=bot_management_kb(bot_id, uid))
            return
        channel_id = row["channel_id"]
        preview = (row.get("content_text") or "")[:200]
        media_info = f"\nMedia: {row.get('media_type')}" if row.get('media_type') else ""
        btns_info = "\nHas buttons: ✅" if row.get("buttons_json") else ""
        await safe_edit_message_text(q,
            f"<blockquote>{pp('📝')} <b>EDIT MESSAGE #{msg_id}</b></blockquote>\n\n"
            f"<b>Preview:</b>\n{EmojiManager._html_escape(preview)}{media_info}{btns_info}",
            parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup([
                [btn("Edit Text", f"ubm_edtext_{bot_id}_{msg_id}", "primary", "📝"),
                 btn("Edit Media", f"ubm_edmed_{bot_id}_{msg_id}", "primary", "🖼️")],
                [btn("Edit Buttons", f"ubm_edbtn_{bot_id}_{msg_id}", "primary", "🔘"),
                 btn("Delete", f"delmsg_{bot_id}_{msg_id}", "danger", "🗑")],
                [btn("Back", f"ub_manage_messages_{bot_id}", "primary", "🔙")],
            ]))
        return

    if data.startswith(f"ubm_edtext_{bot_id}_"):
        parts = data.split("_")
        msg_id = _extract_last_id(parts)
        ud = _runtime_store(context, f"{uid}_{bot_id}")
        ud["editing_text_msg_id"] = msg_id
        await safe_edit_message_text(q, f"{pe('📝')} Send the new text:", parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([[btn("Cancel", f"ub_manage_messages_{bot_id}", "danger", "❌")]]))
        return

    if data.startswith(f"ubm_edmed_{bot_id}_"):
        parts = data.split("_")
        msg_id = _extract_last_id(parts)
        ud = _runtime_store(context, f"{uid}_{bot_id}")
        ud["editing_media_msg_id"] = msg_id
        await safe_edit_message_text(q, f"{pe('🖼️')} Send the new media (photo/video/document):", parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([[btn("Cancel", f"ub_manage_messages_{bot_id}", "danger", "❌")]]))
        return

    if data.startswith(f"ubm_edbtn_{bot_id}_"):
        parts = data.split("_")
        msg_id = _extract_last_id(parts)
        ud = _runtime_store(context, f"{uid}_{bot_id}")
        ud["editing_buttons_msg_id"] = msg_id
        await safe_edit_message_text(q, INLINE_BUTTONS_HELP.format(icon=pe('🔘')),
            parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup([[btn("Cancel", f"ub_manage_messages_{bot_id}", "danger", "❌")]]))
        return

    if data.startswith(f"delmsg_{bot_id}_"):
        parts = data.split("_")
        msg_id = _extract_last_id(parts)
        db.delete_message(msg_id)
        await safe_edit_message_text(q, f"{pe('✅')} Message deleted.", parse_mode=ParseMode.HTML, reply_markup=bot_management_kb(bot_id, uid))
        return

    if data == f"setmsg_more_{bot_id}":
        clear_flow_state(context, uid, bot_id)
        ud = _runtime_store(context, f"{uid}_{bot_id}")
        ud["setting_message"] = True
        context.user_data[f"setting_message_{bot_id}"] = True
        await safe_edit_message_text(q, f"{pe('📝')} Send the next message:", parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([[btn("Done", f"setmsg_done_{bot_id}", "success", "✅"),
                                                btn("Cancel", f"setmsg_cancel_{bot_id}", "danger", "❌")]]))
        return

    if data == f"setmsg_done_{bot_id}":
        ud = _runtime_store(context, f"{uid}_{bot_id}")
        for key in ["setting_message", "messages", "pending_buttons", "waiting_buttons"]:
            ud.pop(key, None)
            context.user_data.pop(f"{key}_{bot_id}", None)
        await safe_edit_message_text(q, f"{pe('✅')} Messages saved successfully!", parse_mode=ParseMode.HTML, reply_markup=bot_management_kb(bot_id, uid))
        return

    if data == f"setmsg_cancel_{bot_id}":
        ud = _runtime_store(context, f"{uid}_{bot_id}")
        for key in ["setting_message", "messages", "pending_buttons", "waiting_buttons"]:
            ud.pop(key, None)
            context.user_data.pop(f"{key}_{bot_id}", None)
        await safe_edit_message_text(q, f"{pe('❌')} Cancelled.", parse_mode=ParseMode.HTML, reply_markup=bot_management_kb(bot_id, uid))
        return

    if data.startswith(f"setbtn_addmore_{bot_id}_"):
        parts = data.split("_")
        msg_id = _extract_last_id(parts)
        ud = _runtime_store(context, f"{uid}_{bot_id}")
        ud["waiting_buttons"] = {"msg_id": msg_id, "append": True}
        await safe_edit_message_text(q,
            f"{pe('🔘')} <b>Send more inline buttons to append</b>\n\n"
            "<code>Button Label|https://link</code>\n\n"
            "<i>Emoji aap jo likhoge wahi rahega.</i>",
            parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup([[btn("Back", f"manage_bot_{bot_id}", "primary", "🔙")]]))
        return

    if data.startswith(f"setbtng_addmore_{bot_id}"):
        ud = _runtime_store(context, f"{uid}_{bot_id}")
        grp = ud.get("pending_buttons_group_addmore")
        if grp:
            ud["waiting_buttons"] = {"msg_ids": grp.get("msg_ids"), "append": True}
        await safe_edit_message_text(q,
            f"{pe('🔘')} <b>Send more buttons to append</b>\n\n<code>Button Label|https://link</code>",
            parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup([[btn("Back", f"manage_bot_{bot_id}", "primary", "🔙")]]))
        return

    # Nothing matched. Never leave the owner staring at a button that does nothing:
    # tell him the menu below is the way forward.
    logging.warning(f"Unhandled userbot callback '{data}' for bot {bot_id} by {uid}")
    try:
        await safe_edit_message_text(q,
            f"{pe('⚠️')} Ye button purane session ka hai — kaam nahi karega.\nNeeche menu se dobara try karein.",
            parse_mode=ParseMode.HTML, reply_markup=bot_management_kb(bot_id, uid))
    except Exception:
        pass
    return


async def accept_all(q, bot_id: str, owner_id: int, context):
    try:
        channels = db.get_bot_channels(bot_id) or []
        for ch in channels:
            await sync_pending_join_requests_for_channel(bot_id, ch["channel_id"], bot_for(bot_id, context))
    except Exception:
        pass
    pending = db.get_pending_requests(bot_id)
    if not pending:
        await safe_edit_message_text(q, f"{pe('‼️')} No pending requests.", parse_mode=ParseMode.HTML, reply_markup=bot_management_kb(bot_id, owner_id))
        return
    ok = 0
    cleaned = 0
    ub_bot = bot_for(bot_id, context)
    for req in pending:
        try:
            await ub_bot.approve_chat_join_request(req["channel_id"], req["requester_id"])
            db.mark_request_status(req["id"], 'approved')
            ok += 1
        except Exception as ex:
            if 'User_already_participant' in str(ex):
                db.mark_request_status(req["id"], 'approved')
                cleaned += 1
    await safe_edit_message_text(q, f"<blockquote>{pp('✅')} <b>ACCEPT ALL COMPLETE</b></blockquote>\n\n{pe('✅')} Accepted: {ok}\n{pe('🧹')} Already approved: {cleaned}",
        parse_mode=ParseMode.HTML, reply_markup=bot_management_kb(bot_id, owner_id))


async def prompt_remove_channel(q, bot_id: str, owner_id: int):
    channels = db.get_bot_channels(bot_id)
    if not channels:
        await safe_edit_message_text(q, f"{pe('‼️')} No channels to remove.", parse_mode=ParseMode.HTML, reply_markup=bot_management_kb(bot_id, owner_id))
        return
    kb = [[btn(f"{pe('❌')} {ch['channel_title'][:25]}", f"removechan_{bot_id}_{ch['channel_id']}", "danger", "❌")] for ch in channels]
    kb.append([btn(f"{pe('🔙')} Back", f"manage_bot_{bot_id}", "primary", "🔙")])
    await safe_edit_message_text(q, "Select channel to remove:", parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup(kb))


async def _flush_media_group(bot_id: str, actor_uid: int, managed_uid: int, chat_id, context, media_group_id):
    if not bot_id or not actor_uid or not media_group_id:
        return
    ud = _runtime_store(context, f"{actor_uid}_{bot_id}")
    key = f"mg_{media_group_id}"
    items = ud.get(key, [])
    if not items:
        return
    channels = db.get_bot_channels(bot_id)
    if not channels:
        return
    channel_id = channels[0]["channel_id"]
    saved_ids = []
    for it in items:
        msg_id = db.add_message(bot_id, channel_id, it.get("text", ""), it.get("media_id"), it.get("media_type"),
                                media_group_id, it.get("entities_json"), it.get("file_name"), it.get("mime_type"),
                                it.get("telegram_message_id"))
        if msg_id and it.get("emoji_map"):
            db.save_user_emoji_map(bot_id, msg_id, it["emoji_map"])
        saved_ids.append(msg_id)
    ud.pop(key, None)
    ud["pending_buttons_group"] = {"msg_ids": saved_ids}
    await send_premium_message(context.bot, chat_id, f"{pe('✅')} Media group saved. Choose an option:", parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup([
        [btn("Set Inline Button", f"setbtng_{bot_id}", "primary", "🔘")],
        [btn("Set More Messages", f"setmsg_more_{bot_id}", "success", "➕")],
        [btn("Cancel", f"setmsg_cancel_{bot_id}", "danger", "❌")],
        [btn("Done", f"setmsg_done_{bot_id}", "success", "✅")],
    ]))


async def _flush_media_group_job(context: ContextTypes.DEFAULT_TYPE):
    data = context.job.data or {}
    await _flush_media_group(data.get("bot_id"), data.get("actor_uid"), data.get("managed_uid"),
                             data.get("chat_id"), context, data.get("media_group_id"))


async def handle_user_bot_message(update: Update, context: ContextTypes.DEFAULT_TYPE, bot_id: str, owner_id: int):
    user = update.effective_user
    if not user:
        return
    uid = user.id
    msg = update.message
    if not msg:
        return

    # Handle support reply from admin to user
    if msg.reply_to_message and (is_admin(uid) or uid == owner_id):
        # Check both maps for the reply
        key1 = f"{bot_id}:{uid}:{msg.reply_to_message.message_id}"
        target_uid = _get_support_uid(USERBOT_SUPPORT_REPLY_MAP, key1)
        if not target_uid:
            # Try main bot support map
            target_uid = _get_support_uid(SUPPORT_REPLY_MAP, msg.reply_to_message.message_id)

        if target_uid:
            try:
                await context.bot.copy_message(chat_id=target_uid, from_chat_id=msg.chat_id, message_id=msg.message_id)
                await send_ephemeral_reply(msg, f"{pe('✅')} Reply delivered to user {target_uid}", 2)
            except Exception as ex:
                await reply_premium_message(msg, f"{pe('❌')} Reply failed: {ex}", parse_mode=ParseMode.HTML)
            return

    # Handle user message (forward to admin)
    # Only forward if user is NOT the bot owner and NOT admin
    if uid != owner_id and not is_admin(uid):
        try:
            # Auto-start bot if needed
            if bot_id not in user_bot_applications:
                bot_data = db.get_user_bot(bot_id)
                if bot_data:
                    sub = db.get_subscription_for_bot(bot_id)
                    if sub:
                        try:
                            expiry = make_aware(sub["expiry_date"]) if isinstance(sub["expiry_date"], datetime) else sub["expiry_date"]
                            if isinstance(expiry, str):
                                expiry = datetime.fromisoformat(expiry.replace('+00:00', ''))
                                expiry = make_aware(expiry)
                            if expiry > now_aware():
                                await start_user_bot(bot_data["bot_token"], bot_id, bot_data["user_id"])
                                db.set_user_bot_active(bot_id, True)
                                logging.info(f"Auto-started userbot {bot_id} for incoming message")
                        except Exception as ex:
                            logging.error(f"Auto-start failed for message: {ex}")

            user_name = user.first_name or "N/A"
            user_username = user.username or "N/A"
            user_id_val = user.id

            # Send to owner AND all admins
            admin_list = list(ADMIN_USER_IDS) + [owner_id]

            if msg.text:
                support_text = format_support_msg(user_name, user_username, user_id_val, msg.text, clickable=True)
                for aid in set(admin_list):
                    try:
                        r = await context.bot.send_message(chat_id=aid, text=support_text,
                                                            parse_mode=ParseMode.HTML, disable_web_page_preview=True)
                        _store_support_map(USERBOT_SUPPORT_REPLY_MAP, f"{bot_id}:{aid}:{r.message_id}", uid)
                    except Exception as ex:
                        logging.error(f"Failed to send text to admin {aid}: {ex}")
            else:
                # Non-text message
                header_text = format_support_msg(user_name, user_username, user_id_val, clickable=True)
                media_type_label = getattr(msg, 'content_type', 'media').upper()
                fallback_notice = (
                    f"{header_text}\n\n"
                    f"<i>⚠️ User sent a {media_type_label} — could not forward due to content protection.</i>"
                )
                for aid in set(admin_list):
                    try:
                        await context.bot.send_message(chat_id=aid, text=header_text,
                                                        parse_mode=ParseMode.HTML, disable_web_page_preview=True)
                        r = await safe_copy_message(
                            context.bot, aid, msg.chat_id, msg.message_id,
                            fallback_text=fallback_notice
                        )
                        if r:
                            _store_support_map(USERBOT_SUPPORT_REPLY_MAP, f"{bot_id}:{aid}:{r.message_id}", uid)
                    except Exception as ex:
                        logging.error(f"Failed to send media to admin {aid}: {ex}")

            db.mark_reachable(bot_id, uid)
            await send_ephemeral_reply(msg, f"{pe('✅')} Message sent to support. You will receive a reply here.", 3)
        except Exception as ex:
            logging.error(f"Support message error: {ex}")
            await reply_premium_message(msg, f"{pe('⚠️')} Could not contact support right now. Please try again later.", parse_mode=ParseMode.HTML)
        return

    # Continue with existing message handling for owner/admin...
    ud = _runtime_store(context, f"{uid}_{bot_id}")
    extracted = MessageManager.extract_from_message(msg)

    if ud.get("editing_text_msg_id"):
        mid = ud.pop("editing_text_msg_id")
        row = db.get_message_by_id(mid)
        if row and row["bot_id"] == bot_id:
            db.update_message_text(mid, extracted["text"], extracted["entities_json"])
            if extracted["emoji_map"]:
                db.save_user_emoji_map(bot_id, mid, extracted["emoji_map"])
            await reply_premium_message(msg, f"{pe('✅')} Text updated.", parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup([[btn(f"{pe('🔙')} Back to Messages", f"ubmm_{bot_id}_{row['channel_id']}", "primary", "🔙")]]))
        else:
            await reply_premium_message(msg, f"{pe('❌')} Message not found. It may have been deleted.", parse_mode=ParseMode.HTML)
        return

    if ud.get("editing_buttons_msg_id"):
        mid = ud.pop("editing_buttons_msg_id")
        row = db.get_message_by_id(mid)
        if row and row["bot_id"] == bot_id:
            btn_json = buttons_json_from_text(msg.text or "")
            db.update_message_buttons(mid, btn_json or "[]")
            await reply_premium_message(msg, f"{pe('✅')} Buttons updated.", parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup([[btn(f"{pe('🔙')} Back to Messages", f"ubmm_{bot_id}_{row['channel_id']}", "primary", "🔙")]]))
        else:
            await reply_premium_message(msg, f"{pe('❌')} Message not found. It may have been deleted.", parse_mode=ParseMode.HTML)
        return

    if ud.get("editing_media_msg_id"):
        mid = ud.pop("editing_media_msg_id")
        row = db.get_message_by_id(mid)
        if row and row["bot_id"] == bot_id:
            db.update_message_media(mid, extracted["media_id"], extracted["media_type"], extracted["text"],
                                    extracted["entities_json"], extracted.get("file_name"), extracted.get("mime_type"),
                                    extracted.get("telegram_message_id"))
            if extracted["emoji_map"]:
                db.save_user_emoji_map(bot_id, mid, extracted["emoji_map"])
            await reply_premium_message(msg, f"{pe('✅')} Media updated.", parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup([[btn(f"{pe('🔙')} Back to Messages", f"ubmm_{bot_id}_{row['channel_id']}", "primary", "🔙")]]))
        else:
            await reply_premium_message(msg, f"{pe('❌')} Message not found. It may have been deleted.", parse_mode=ParseMode.HTML)
        return

    if ud.get("adding_channel") or context.user_data.get(f"adding_channel_{bot_id}"):
        channel_chat = None
        if hasattr(msg, 'forward_origin') and msg.forward_origin:
            try:
                if hasattr(msg.forward_origin, 'chat'):
                    channel_chat = msg.forward_origin.chat
            except:
                pass
        elif hasattr(msg, 'forward_from_chat') and msg.forward_from_chat:
            channel_chat = msg.forward_from_chat

        if channel_chat and channel_chat.type in ['channel', 'group', 'supergroup']:
            ch = channel_chat
            ub_bot = bot_for(bot_id, context)
            try:
                try:
                    member = await ub_bot.get_chat_member(ch.id, ub_bot.id)
                    if member.status not in ['administrator', 'creator']:
                        await reply_premium_message(msg, f"{pe('❌')} Bot is not an admin in this channel!\n\nPlease add bot as admin first, then try again.", parse_mode=ParseMode.HTML)
                        return
                except Exception as e:
                    await reply_premium_message(msg, f"{pe('❌')} Cannot verify bot admin status: {str(e)}\n\nMake sure bot is admin in the channel.", parse_mode=ParseMode.HTML)
                    return

                sub = db.get_subscription_for_bot(bot_id)
                if sub:
                    max_ch = sub.get("max_channels", 1)
                    current_ch = len(db.get_bot_channels(bot_id))
                    if current_ch >= max_ch:
                        await reply_premium_message(msg, f"{pe('❌')} Channel limit reached ({max_ch}). Upgrade to Pro for more channels.", parse_mode=ParseMode.HTML)
                        return

                db.add_channel(bot_id, ch.id, getattr(ch, 'username', None), ch.title or "Channel")
                await sync_pending_join_requests_for_channel(bot_id, ch.id, ub_bot)

                ud["adding_channel"] = False
                context.user_data.pop(f"adding_channel_{bot_id}", None)

                await reply_premium_message(msg, f"{pe('✅')} Channel '{ch.title}' added successfully!\n{pe('✨')} Existing pending requests have been synced.", parse_mode=ParseMode.HTML, reply_markup=bot_management_kb(bot_id, owner_id))
            except Exception as ex:
                await reply_premium_message(msg, f"{pe('❌')} Error adding channel: {str(ex)}", parse_mode=ParseMode.HTML)
        else:
            await reply_premium_message(msg, f"{pe('🔽')} <b>How to add a channel:</b>\n\n1. Make sure this bot is <b>admin</b> in your channel\n2. Go to your channel\n3. <b>Forward ANY message</b> from that channel to this bot\n4. The channel will be added automatically\n\n⚠️ The message must be forwarded from the channel!", parse_mode=ParseMode.HTML)
        return

    if ud.get("waiting_buttons"):
        info = ud.get("waiting_buttons")
        msg_id = info.get("msg_id")
        msg_ids = info.get("msg_ids") or ([] if msg_id is None else [msg_id])
        append_mode = info.get("append", False)
        btn_json = buttons_json_from_text(msg.text or "")
        if btn_json:
            for _id in msg_ids:
                if append_mode:
                    db.append_message_buttons(_id, btn_json)
                else:
                    db.update_message_buttons(_id, btn_json)
            preview_markup = buttons_to_markup(btn_json)
            if preview_markup:
                try:
                    await reply_premium_message(msg, f"{pe('👁')} <b>Button Preview</b> — yahi dikhega users ko:", parse_mode=ParseMode.HTML, reply_markup=preview_markup)
                except Exception:
                    pass
            if len(msg_ids) == 1:
                more_btn_cb = f"setbtn_addmore_{bot_id}_{msg_ids[0]}"
            else:
                more_btn_cb = f"setbtng_addmore_{bot_id}"
                ud["pending_buttons_group_addmore"] = {"msg_ids": msg_ids}
            await reply_premium_message(msg, f"{pe('✅')} Inline buttons saved! Choose next action:", parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup([
                [btn("Set More Inline Buttons", more_btn_cb, "primary", "🔘")],
                [btn("Set More Messages", f"setmsg_more_{bot_id}", "success", "➕")],
                [btn("Done", f"setmsg_done_{bot_id}", "success", "✅")],
                [btn("Back", f"manage_bot_{bot_id}", "primary", "🔙")],
            ]))
        else:
            await reply_premium_message(msg, f"{pe('❌')} No valid buttons parsed.\n\nFormat:\n• 1 button: <code>Button Label|https://link</code>\n• 2 per row: <code>Label One|https://link1 || Label Two|https://link2</code>", parse_mode=ParseMode.HTML, reply_markup=bot_management_kb(bot_id, owner_id))
        ud.pop("waiting_buttons", None)
        ud.pop("pending_buttons_group", None)
        return

    if ud.get("setting_message") or context.user_data.get(f"setting_message_{bot_id}"):
        if msg.text and msg.text.strip().lower() in ["done", "/done", "finish", "stop", "complete"]:
            for key in ["setting_message", "messages", "pending_buttons", "waiting_buttons"]:
                ud.pop(key, None)
                context.user_data.pop(f"{key}_{bot_id}", None)
            await reply_premium_message(msg, f"{pe('✅')} Messages saved successfully.", parse_mode=ParseMode.HTML, reply_markup=bot_management_kb(bot_id, owner_id))
            return
        channels = db.get_bot_channels(bot_id)
        if not channels:
            await reply_premium_message(msg, f"{pe('❌')} No channel added yet. Add a channel first using 'Add Channel' button.", parse_mode=ParseMode.HTML)
            return
        channel_id = channels[0]["channel_id"]
        media_group_id = extracted["media_group_id"]

        if media_group_id:
            key = f"mg_{media_group_id}"
            arr = ud.get(key, [])
            first_item = len(arr) == 0
            arr.append({"text": extracted["text"], "media_id": extracted["media_id"], "media_type": extracted["media_type"],
                        "entities_json": extracted["entities_json"], "emoji_map": extracted["emoji_map"],
                        "file_name": extracted.get("file_name"), "mime_type": extracted.get("mime_type"),
                        "telegram_message_id": extracted.get("telegram_message_id")})
            ud[key] = arr
            if first_item:
                try:
                    await reply_premium_message(msg, f"{pe('📸')} Album received, processing...", parse_mode=ParseMode.HTML)
                except Exception:
                    pass
            job_key = f"mg_job_{media_group_id}"
            old_job = ud.get(job_key)
            if old_job:
                try:
                    old_job.schedule_removal()
                except Exception:
                    pass
            j = context.job_queue.run_once(_flush_media_group_job, when=1.2,
                data={"bot_id": bot_id, "actor_uid": uid, "managed_uid": owner_id,
                      "chat_id": msg.chat_id, "media_group_id": media_group_id})
            ud[job_key] = j
            return

        msg_id = db.add_message(bot_id, channel_id, extracted["text"], extracted["media_id"],
                                extracted["media_type"], None, extracted["entities_json"],
                                extracted.get("file_name"), extracted.get("mime_type"),
                                extracted.get("telegram_message_id"))
        if msg_id and extracted["emoji_map"]:
            db.save_user_emoji_map(bot_id, msg_id, extracted["emoji_map"])
        ud["pending_buttons"] = {"msg_id": msg_id, "channel_id": channel_id}
        await reply_premium_message(msg, f"{pe('✅')} Message saved! Choose an option:", parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup([
            [btn("Set Inline Button", f"setbtn_{bot_id}_{msg_id}", "primary", "🔘")],
            [btn("Set More Messages", f"setmsg_more_{bot_id}", "success", "➕")],
            [btn("Cancel", f"setmsg_cancel_{bot_id}", "danger", "❌")],
            [btn("Done", f"setmsg_done_{bot_id}", "success", "✅")],
        ]))
        return

    if context.user_data.get(f"broadcast_stage_{bot_id}") == "await_message":
        draft = {"text": extracted["text"], "media": extracted["media_id"], "media_type": extracted["media_type"],
                 "emoji_map": extracted["emoji_map"], "entities_json": extracted["entities_json"],
                 "file_name": extracted.get("file_name"), "mime_type": extracted.get("mime_type")}
        context.user_data[f"broadcast_draft_{bot_id}"] = draft
        context.user_data[f"broadcast_stage_{bot_id}"] = "buttons_or_send"
        await reply_premium_message(msg, f"{pe('✅')} Broadcast draft saved. Add inline buttons or send now?", parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup([
            [btn("Add Inline Buttons", f"bcast_add_btns_{bot_id}", "primary", "🔘")],
            [btn("Send Now", f"bcast_send_{bot_id}", "success", "🚀")],
            [btn("Cancel", f"manage_bot_{bot_id}", "danger", "❌")],
        ]))
        return

    if context.user_data.get(f"broadcast_stage_{bot_id}") == "await_buttons":
        draft = context.user_data.get(f"broadcast_draft_{bot_id}", {})
        btn_json = buttons_json_from_text(msg.text or "")
        if btn_json:
            draft["buttons_json"] = btn_json
            context.user_data[f"broadcast_draft_{bot_id}"] = draft
            preview_markup = buttons_to_markup(btn_json)
            if preview_markup:
                try:
                    await reply_premium_message(msg, f"{pe('👁')} <b>Button Preview</b> — yahi dikhega users ko:", parse_mode=ParseMode.HTML, reply_markup=preview_markup)
                except Exception:
                    pass
            await reply_premium_message(msg, f"{pe('✅')} Buttons saved. Ready to send?", parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup([
                [btn("Send Now", f"bcast_send_{bot_id}", "success", "🚀")],
                [btn("Cancel", f"manage_bot_{bot_id}", "danger", "❌")],
            ]))
        else:
            await reply_premium_message(msg, f"{pe('❌')} No valid buttons.\n\nFormat:\n• 1 button: <code>Button Label|https://link</code>\n• 2 per row: <code>Label One|https://link1 || Label Two|https://link2</code>", parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup([[btn("Back", f"manage_bot_{bot_id}", "primary", "🔙")]]))
        context.user_data[f"broadcast_stage_{bot_id}"] = "buttons_or_send"
        return

    await reply_premium_message(msg,
        f"{pe('🔽')} <b>Koi setup flow active nahi hai.</b>\n\n"
        f"Naya message set karne ke liye pehle <b>Set Message(s)</b> dabao,\n"
        f"phir apna message yahan bhejo.\n\n"
        f"(Buttons se hi setup hota hai - direct message bhejne se save nahi hoga.)",
        parse_mode=ParseMode.HTML, reply_markup=bot_management_kb(bot_id, owner_id))


INLINE_BUTTONS_HELP = (
    "{icon} <b>Send Inline Buttons</b>\n\n"
    "• 1 button per row:\n  <code>Button Label|https://link</code>\n\n"
    "• 2 buttons per row:\n  <code>Label One|https://link1 || Label Two|https://link2</code>\n\n"
    "<b>Emoji:</b> aap label mein jo emoji likhoge, button par wahi dikhega —\n"
    "koi default icon force nahi hoga."
)


async def handle_set_buttons_callback(update: Update, context: ContextTypes.DEFAULT_TYPE, bot_id: str, owner_id: int):
    q = update.callback_query
    if not q or not q.from_user:
        return
    try:
        await q.answer()
    except Exception:
        pass
    uid = q.from_user.id
    ud = _runtime_store(context, f"{uid}_{bot_id}")
    data = q.data
    help_text = INLINE_BUTTONS_HELP.format(icon=pe("🔘"))

    if data == f"setbtng_{bot_id}":
        grp = ud.get("pending_buttons_group")
        if not grp or not grp.get("msg_ids"):
            await safe_edit_message_text(q, f"{pe('❌')} Group not found. Send media group again.", parse_mode=ParseMode.HTML, reply_markup=bot_management_kb(bot_id, owner_id))
            return
        # A media-group button request replaces any other pending input, so a stale
        # editing_/setting_message flag can't steal the button lines.
        clear_flow_state(context, uid, bot_id)
        ud = _runtime_store(context, f"{uid}_{bot_id}")
        ud["waiting_buttons"] = {"msg_ids": grp.get("msg_ids")}
        await safe_edit_message_text(q, help_text,
            parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup([[btn("Back", f"manage_bot_{bot_id}", "primary", "🔙")]]))
        return

    if data.startswith(f"setbtn_{bot_id}_"):
        parts = data.split("_")
        msg_id = _extract_last_id(parts)
        row = db.get_message_by_id(msg_id) if msg_id else None
        if not row or row.get("bot_id") != bot_id:
            await safe_edit_message_text(q, f"{pe('❌')} Message not found (it may have been deleted).",
                parse_mode=ParseMode.HTML, reply_markup=bot_management_kb(bot_id, owner_id))
            return
        clear_flow_state(context, uid, bot_id)
        ud = _runtime_store(context, f"{uid}_{bot_id}")
        ud["waiting_buttons"] = {"msg_id": msg_id}
        await safe_edit_message_text(q, help_text,
            parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup([[btn("Back", f"manage_bot_{bot_id}", "primary", "🔙")]]))
        return



async def delete_pending_leave_recovery_messages(bot_id: str, user_id: int, target_channel_id: int, bot: Bot) -> int:
    deleted = 0
    for row_id, message_id in db.get_pending_leave_recovery_messages(bot_id, user_id, target_channel_id):
        try:
            await bot.delete_message(chat_id=user_id, message_id=message_id)
            deleted += 1
        except Exception:
            pass
        finally:
            db.mark_leave_recovery_deleted(row_id)
    return deleted


async def handle_join_request(update: Update, context: ContextTypes.DEFAULT_TYPE, bot_id: str, owner_id: int):
    jr = update.chat_join_request
    if not jr:
        return
    requester = jr.from_user
    chat = jr.chat

    leave_cfg = db.get_leave_recovery_config()
    if (leave_cfg.get("enabled") and leave_cfg.get("target_channel_id") and int(leave_cfg["target_channel_id"]) == int(chat.id)):
        # NOTE: this is the TARGET (recovery) channel - cleaning up the old
        # "please rejoin" DMs must NOT depend on the per-source-channel switch
        # (that one only decides whether recovery DMs get SENT, default OFF).
        await delete_pending_leave_recovery_messages(bot_id, requester.id, int(chat.id), context.bot)
        try:
            await jr.approve()
        except Exception as ex:
            if 'User_already_participant' not in str(ex):
                logging.error(f"Leave recovery target approve error: {ex}")
        return

    channel_row = db.get_channel_owner_data(chat.id, bot_id)
    if not channel_row:
        try:
            member = await context.bot.get_chat_member(chat.id, context.bot.id)
            if member.status in ['administrator', 'creator']:
                db.add_channel(bot_id, chat.id, getattr(chat, 'username', None), chat.title or "Channel")
                channel_row = db.get_channel_owner_data(chat.id, bot_id)
        except Exception:
            pass
        if not channel_row:
            return

    auto = int(channel_row.get("auto_approve", 0)) == 1 if channel_row else False

    try:
        default_msg_text = db.get_default_first_message()
        default_msg_text = render_dynamic_text(default_msg_text, requester)
        await send_user_message(context.bot, requester.id, default_msg_text, parse_mode=ParseMode.HTML)
    except Exception as ex:
        logging.error(f"Default first message send error: {ex}")

    msgs = db.get_messages(chat.id, bot_id) if channel_row else []
    try:
        if msgs:
            await _send_messages_with_media_groups(requester.id, msgs, context, bot_id=bot_id, attach_start_button=True, placeholder_user=requester)
        else:
            wm = channel_row.get("welcome_message") if channel_row else DEFAULT_WELCOME_MESSAGE
            wm = render_dynamic_text(wm, requester)
            wid = channel_row.get("welcome_media_id") if channel_row else None
            wtype = channel_row.get("welcome_media_type") if channel_row else None
            markup = buttons_to_markup(buttons_json_from_text(wm) or None)
            if wid and wtype:
                await send_media(context, requester.id, wid, wtype, wm, markup)
            elif wm:
                await send_user_message(context.bot, requester.id, wm, parse_mode=ParseMode.HTML, reply_markup=markup)
        db.mark_reachable(bot_id, requester.id)
    except Exception as ex:
        logging.error(f"Send welcome error: {ex}")

    db.add_join_request(bot_id, requester.id, chat.id, 'approved' if auto else 'pending')
    if auto:
        try:
            await jr.approve()
        except Exception as ex:
            if 'User_already_participant' not in str(ex):
                logging.error(f"Approve error: {ex}")


async def handle_channel_member_update(update: Update, context: ContextTypes.DEFAULT_TYPE, bot_id: str, owner_id: int):
    cmu = update.chat_member
    if not cmu or not cmu.chat or not cmu.new_chat_member:
        return
    new_status = getattr(cmu.new_chat_member, "status", "")
    old_status = getattr(cmu.old_chat_member, "status", "") if cmu.old_chat_member else ""

    if new_status in {"member", "administrator", "creator"} and old_status in {"left", "kicked", "restricted", ""}:
        leave_cfg = db.get_leave_recovery_config()
        target_channel_id = leave_cfg.get("target_channel_id")
        if leave_cfg.get("enabled") and target_channel_id and int(target_channel_id) == int(cmu.chat.id):
            joined_user = getattr(cmu.new_chat_member, "user", None)
            if joined_user and not getattr(joined_user, "is_bot", False):
                await delete_pending_leave_recovery_messages(bot_id, joined_user.id, int(cmu.chat.id), context.bot)
        return

    if new_status not in {"left", "kicked"} or old_status in {"left", "kicked"}:
        return

    channel_row = db.get_channel_owner_data(cmu.chat.id, bot_id)
    if not channel_row:
        return
    member_user = getattr(cmu.new_chat_member, "user", None)
    if not member_user or getattr(member_user, "is_bot", False):
        return

    db.mark_unreachable(bot_id, member_user.id)
    leave_cfg = db.get_leave_recovery_config()
    target_channel_id = leave_cfg.get("target_channel_id")
    target_link = (leave_cfg.get("target_channel_link") or "").strip()

    if not leave_cfg.get("enabled") or not target_channel_id or not target_link or int(target_channel_id) == int(cmu.chat.id):
        return

    if not leave_recovery_channel_enabled(leave_cfg, cmu.chat.id):
        logging.info(f"Leave recovery disabled (default OFF) for channel {cmu.chat.id}, skipping.")
        return

    try:
        await delete_pending_leave_recovery_messages(bot_id, member_user.id, int(target_channel_id), context.bot)
        extra = {
            "source_channel_title": cmu.chat.title or str(cmu.chat.id),
            "source_channel_id": cmu.chat.id,
            "target_channel_link": target_link,
            "target_channel_id": target_channel_id
        }

        leave_messages = leave_cfg.get("messages", [])

        if not leave_messages and leave_cfg.get("message"):
            leave_messages = [{"text": leave_cfg["message"], "buttons_json": leave_cfg.get("buttons_json", "")}]

        if not leave_messages:
            leave_messages = [{"text": "Hello {first_name}, aap channel se leave ho gaye. Wapas access ke liye neeche wale channel par request bheje.", "buttons_json": ""}]

        for lm in leave_messages:
            text = render_dynamic_text(lm.get("text", ""), member_user, extra)
            lm_buttons = lm.get("buttons_json") or ""
            if lm_buttons:
                leave_markup = buttons_to_markup(lm_buttons)
            else:
                leave_markup = InlineKeyboardMarkup([[btn_url("Join Channel", target_link, "success", "🔔")]])

            sent = await send_user_message(context.bot, member_user.id, text,
                                               parse_mode=ParseMode.HTML, reply_markup=leave_markup)
            if sent:
                db.add_leave_recovery_message(bot_id, member_user.id, cmu.chat.id, int(target_channel_id), sent.message_id)

    except Exception as ex:
        logging.error(f"Leave recovery DM failed: {ex}")


# ================= USER BOT LIFECYCLE =================
async def stop_all_userbots():
    """Shut down every userbot Application (used before a restart, so the next
    attempt does not create a second polling session for the same token)."""
    for bid in list(user_bot_applications.keys()):
        try:
            await stop_user_bot(bid)
        except Exception as ex:
            logging.warning(f"stop_all_userbots: {bid}: {ex}")


async def start_user_bot(token: str, bot_id: str, owner_id: int):
    # Idempotent: if this userbot is already polling, stop the old Application first.
    # Without this, every retry of main() added a second poller for the same token
    # and the owner's in-memory setup flow (context.user_data) was lost.
    if bot_id in user_bot_applications:
        logging.info(f"userbot {bot_id} already running - restarting it cleanly")
        try:
            await stop_user_bot(bot_id)
        except Exception as ex:
            logging.warning(f"could not stop previous instance of {bot_id}: {ex}")
    app = ApplicationBuilder().token(token).concurrent_updates(True).build()
    app.bot_data["bot_id"] = bot_id
    app.bot_data["owner_id"] = owner_id
    bid = re.escape(bot_id)
    app.add_handler(CommandHandler("start", lambda u, c: user_bot_start(u, c, bot_id, owner_id)))
    app.add_handler(CallbackQueryHandler(lambda u, c: handle_public_userbot_callback(u, c, bot_id), pattern=r'^(start_now|live_chat_support)$'))
    # IMPORTANT (order matters): python-telegram-bot runs only the FIRST matching
    # handler inside a group. The "Set Inline Button" callbacks must be registered
    # BEFORE the generic one, otherwise the generic pattern swallows them and the
    # button silently does nothing.
    app.add_handler(CallbackQueryHandler(lambda u, c: handle_set_buttons_callback(u, c, bot_id, owner_id),
                                         pattern=f"^(setbtn_{bid}_|setbtng_{bid}$)"))
    app.add_handler(CallbackQueryHandler(lambda u, c: user_bot_callback(u, c, bot_id, owner_id), pattern=f"^(ub_|ubm_|ubmm_|delmsg_|setbtn_|setbtng|setmsg_|bcast_|removechan_|back_to_manage_|manage_bot_|toggleauto_|sub_for_bot_|sub_basic_|sub_pro_|setbtn_addmore_|setbtng_addmore_).*{bid}|^main_menu$"))
    app.add_handler(MessageHandler(filters.TEXT | filters.PHOTO | filters.VIDEO | filters.Document.ALL | filters.AUDIO | filters.VOICE | filters.Sticker.ALL, lambda u, c: handle_user_bot_message(u, c, bot_id, owner_id)))
    app.add_handler(ChatJoinRequestHandler(lambda u, c: handle_join_request(u, c, bot_id, owner_id)))
    app.add_handler(ChatMemberHandler(lambda u, c: handle_channel_member_update(u, c, bot_id, owner_id), ChatMemberHandler.CHAT_MEMBER))
    await app.initialize()
    await app.start()
    await app.updater.start_polling(allowed_updates=["message", "callback_query", "chat_member", "chat_join_request", "inline_query"])
    user_bot_applications[bot_id] = app
    try:
        for ch in db.get_bot_channels(bot_id) or []:
            await sync_pending_join_requests_for_channel(bot_id, ch["channel_id"], app.bot)
    except Exception as ex:
        logging.error(f"Startup pending sync failed: {ex}")
    return True


async def stop_user_bot(bot_id: str):
    if bot_id in user_bot_applications:
        try:
            app = user_bot_applications[bot_id]
            await app.updater.stop()
            await app.stop()
            await app.shutdown()
        except Exception:
            pass
        user_bot_applications.pop(bot_id, None)
        db.set_user_bot_active(bot_id, False)


# ================= BROADCAST FUNCTIONS =================
async def preview_user_broadcast(q, context: ContextTypes.DEFAULT_TYPE, bot_id: str, owner_id: int):
    draft = context.user_data.get(f"broadcast_draft_{bot_id}", {})
    if not draft:
        await safe_edit_message_text(q, f"{pe('❌')} No draft found.", parse_mode=ParseMode.HTML, reply_markup=bot_management_kb(bot_id, owner_id))
        return
    try:
        # bot_for(): when this panel is driven from the MAIN bot, context.bot would be
        # the main bot - the broadcast/preview has to come from the userbot itself.
        await send_media(bot_for(bot_id, context), owner_id, draft.get("media"), draft.get("media_type") or "text",
                         draft.get("text", ""), buttons_to_markup(draft.get("buttons_json")),
                         entities_json=draft.get("entities_json"), file_name=draft.get("file_name"), mime_type=draft.get("mime_type"))
    except Exception as ex:
        await safe_edit_message_text(q, f"{pe('❌')} Preview failed: {str(ex)}", parse_mode=ParseMode.HTML, reply_markup=bot_management_kb(bot_id, owner_id))
        return
    await safe_edit_message_text(q, f"{pe('✅')} Preview sent above. Confirm to broadcast?", parse_mode=ParseMode.HTML, reply_markup=confirm_kb(f"bcast_confirm_{bot_id}", f"manage_bot_{bot_id}"))


async def send_user_broadcast(q, context: ContextTypes.DEFAULT_TYPE, bot_id: str, owner_id: int):
    draft = context.user_data.get(f"broadcast_draft_{bot_id}", {})
    context.user_data.pop(f"broadcast_stage_{bot_id}", None)
    if not draft:
        await safe_edit_message_text(q, f"{pe('❌')} No draft to send.", parse_mode=ParseMode.HTML, reply_markup=bot_management_kb(bot_id, owner_id))
        return
    reqs = db.get_requesters_for_bot(bot_id)
    if not reqs:
        await safe_edit_message_text(q, f"{pe('❌')} No users to broadcast to.", parse_mode=ParseMode.HTML, reply_markup=bot_management_kb(bot_id, owner_id))
        return
    await safe_edit_message_text(q, f"{pe('✈️')} Broadcasting...", parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup([[btn("Back", f"manage_bot_{bot_id}", "primary", "🔙")]]))
    sent = 0
    fail = 0
    bc_bot = bot_for(bot_id, context)
    for r in reqs:
        try:
            await send_media(bc_bot, r, draft.get("media"), draft.get("media_type") or "text",
                             draft.get("text", ""), buttons_to_markup(draft.get("buttons_json")),
                             entities_json=draft.get("entities_json"), file_name=draft.get("file_name"), mime_type=draft.get("mime_type"))
            db.mark_reachable(bot_id, r)
            sent += 1
        except Forbidden:
            db.mark_unreachable(bot_id, r)
            fail += 1
        except Exception as ex:
            fail += 1
        if (sent + fail) % 30 == 0:
            try:
                await q.message.edit_text(f"{pe('✈️')} Broadcasting... Sent: {sent}, Failed: {fail}", parse_mode=ParseMode.HTML)
            except Exception:
                pass
    await safe_edit_message_text(q, UIFormatter.broadcast_confirm(sent, fail), parse_mode=ParseMode.HTML, reply_markup=bot_management_kb(bot_id, owner_id))
    context.user_data.pop(f"broadcast_draft_{bot_id}", None)


# ================= ADMIN PANEL FUNCTIONS =================
async def show_admin_userbot_control(q, context: ContextTypes.DEFAULT_TYPE):
    bots = db.get_all_user_bots()
    if not bots:
        await safe_edit_message_text(q, f"{pe('‼️')} No user bots found.", parse_mode=ParseMode.HTML, reply_markup=admin_kb())
        return

    running_count = sum(1 for bot in bots if bot["bot_id"] in user_bot_applications)
    premium_count = sum(1 for bot in bots if db.get_subscription_for_bot(bot["bot_id"]))
    stopped_count = max(len(bots) - running_count, 0)

    lines = [f"<blockquote>{pp('💎')} <b>USERBOT CONTROL CENTER</b></blockquote>", "",
             f"{pp('📊')} <b>Total Bots:</b> {len(bots)} | {pp('🟢')} <b>Running:</b> {running_count}",
             f"{pp('⭐️')} <b>Premium:</b> {premium_count} | {pp('🔴')} <b>Stopped:</b> {stopped_count}", "",
             f"{pp('📌')} <b>Bot List</b>"]

    for bot in bots:
        is_running = bot["bot_id"] in user_bot_applications
        sub = db.get_subscription_for_bot(bot["bot_id"])
        status_icon = pe('🟢') if is_running else pe('🔴')
        plan_text = sub["subscription_type"] if sub else "No active plan"
        plan_icon = pe('⭐️') if sub else pe('❌')
        lines.append(f"{status_icon} <b>@{bot['bot_username'] or 'N/A'}</b>\n   <code>{bot['bot_id']}</code> • {'Running' if is_running else 'Stopped'} • {plan_icon} {plan_text}")

    kb = []
    for bot in bots:
        is_running = bot["bot_id"] in user_bot_applications
        row = []
        if is_running:
            row.append(btn(f"Stop @{bot['bot_username'] or bot['bot_id']}", f"admin_ub_stop_{bot['bot_id']}", "danger", "🛑"))
        else:
            row.append(btn(f"Start @{bot['bot_username'] or bot['bot_id']}", f"admin_ub_start_{bot['bot_id']}", "success", "🚀"))
        row.append(btn("Info", f"admin_ub_info_{bot['bot_id']}", "primary", "📊"))
        kb.append(row)
        kb.append([btn(f"Manage @{bot['bot_username'] or bot['bot_id']}", f"manage_bot_{bot['bot_id']}", "primary", "⚙️")])
    kb.append([btn("Start All", "admin_start_all", "success", "🚀"), btn("Stop All", "admin_stop_all", "danger", "🛑")])
    kb.append([btn("Refresh", "admin_userbots", "primary", "🔄"), btn("Back to Admin", "admin_panel", "primary", "🔙")])
    await safe_edit_message_text(q, "\n".join(lines)[:4000], parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup(kb))


async def show_admin_ub_info(q, bot_id_target: str, context: ContextTypes.DEFAULT_TYPE):
    try:
        bot_data = db.get_user_bot(bot_id_target)
        if not bot_data:
            await safe_edit_message_text(q, f"{pe('❌')} UserBot not found.", parse_mode=ParseMode.HTML, reply_markup=admin_kb())
            return
        sub = db.get_subscription_for_bot(bot_id_target)
        user_doc = db.get_user(bot_data["user_id"]) or {}
        is_running = bot_id_target in user_bot_applications
        lines = [f"<blockquote>{pp('🔎')} <b>USERBOT INFO</b></blockquote>\n"]
        lines.append(f"{pp('👤')} <b>User:</b> {user_doc.get('first_name', '')} @{user_doc.get('username', '') or 'N/A'} ({bot_data['user_id']})")
        lines.append(f"{pp('🤖')} <b>Bot:</b> @{bot_data['bot_username'] or 'N/A'}")
        lines.append(f"{pp('⚡️')} <b>Running:</b> {'🟢 Yes' if is_running else '🔴 No'}")
        if sub:
            try:
                exp = make_aware(sub["expiry_date"]) if isinstance(sub["expiry_date"], datetime) else sub["expiry_date"]
                days_left = (exp - now_aware()).days
                lines.append(f"{pp('⭐️')} <b>Plan:</b> {sub['subscription_type']}")
                lines.append(f"{pp('📅')} <b>Expiry:</b> {exp.strftime('%d %b %Y')} ({days_left}d left)")
            except Exception:
                lines.append(f"{pp('⭐️')} <b>Plan:</b> {sub['subscription_type'] if sub else 'None'}")
        else:
            lines.append(f"{pp('⭐️')} <b>Subscription:</b> None")
        channels = db.get_bot_channels(bot_id_target) or []
        total_users = db.get_total_requesters_count(bot_id_target)
        reachable = db.get_reachable_requesters_count(bot_id_target)
        lines.append(f"{pp('✈️')} <b>Channels:</b> {len(channels)}")
        lines.append(f"{pp('👥')} <b>Total Users:</b> {total_users} | <b>Reachable:</b> {reachable}")
        kb = []
        # Admin ko client ke bot ka pura control yahan se milta hai (set message,
        # channel, buttons, broadcast...) - same panel jo client ko dikhta hai.
        kb.append([btn("Manage Panel (Full Control)", f"manage_bot_{bot_id_target}", "success", "⚙️")])
        if is_running:
            kb.append([btn("Stop Bot", f"admin_ub_stop_{bot_id_target}", "danger", "🛑")])
        else:
            kb.append([btn("Start Bot", f"admin_ub_start_{bot_id_target}", "success", "🚀")])
        kb.append([btn("Remove Bot", f"admin_remove_bot_{bot_id_target}", "danger", "🗑"), btn("Back", "admin_userbots", "primary", "🔙")])
        await safe_edit_message_text(q, "\n".join(lines), parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup(kb))
    except Exception as ex:
        # Never let a data/formatting issue for one bot crash the whole admin panel.
        logging.error(f"show_admin_ub_info error for {bot_id_target}: {ex}")
        await safe_edit_message_text(q, f"{pe('❌')} Could not load info: {str(ex)[:150]}", parse_mode=ParseMode.HTML, reply_markup=admin_kb())


async def show_all_users(q):
    users = db.get_all_users()
    lines = [f"<blockquote>{pp('📇')} <b>ALL USERS ({len(users)})</b></blockquote>\n"]
    for u in users:
        lines.append(f"• {u['user_id']} @{u.get('username', 'N/A')} {u.get('first_name', '')}")
    await safe_edit_message_text(q, "\n".join(lines)[:4000], parse_mode=ParseMode.HTML, reply_markup=admin_kb())


async def check_expiry(q):
    subs = db.get_all_subscriptions()
    now = now_aware()
    expiring = []
    for s in subs:
        if s.get("expiry_date"):
            exp = make_aware(s["expiry_date"]) if isinstance(s["expiry_date"], datetime) else s["expiry_date"]
            if isinstance(exp, str):
                try:
                    exp = datetime.fromisoformat(exp.replace('+00:00', ''))
                    exp = make_aware(exp)
                except:
                    continue
            days = (exp - now).days
            if 0 <= days <= 7:
                expiring.append(f"• {s['bot_username']} — {days} days left")
    text = f"<blockquote>{pp('⏰')} <b>EXPIRING SOON</b></blockquote>\n\n" + ("\n".join(expiring) if expiring else "None expiring within 7 days.")
    await safe_edit_message_text(q, text, parse_mode=ParseMode.HTML, reply_markup=admin_kb())


async def show_stats(q):
    users = db.get_all_users()
    bots = db.get_all_user_bots()
    subs = db.get_all_subscriptions()
    running = len(user_bot_applications)
    userbot_counts = db.get_userbot_user_counts()
    total_userbot_users = sum(row["users"] for row in userbot_counts)
    count_lines = []
    for row in userbot_counts[:25]:
        status = pe('🟢') if row["bot_id"] in user_bot_applications else pe('🔴')
        count_lines.append(f"{status} <b>@{row['bot_username'] or 'N/A'}</b> — <code>{row['bot_id']}</code> — <b>{row['users']}</b> users")
    if len(userbot_counts) > 25:
        count_lines.append(f"<i>…and {len(userbot_counts) - 25} more userbots</i>")
    per_bot_text = "\n".join(count_lines) if count_lines else "No userbots found."
    text = (f"<blockquote>{pp('📊')} <b>SYSTEM STATS</b></blockquote>\n\n"
            f"{pp('👀')} <b>Main Bot Users:</b> {len(users)}\n"
            f"{pp('💎')} <b>Total UserBots:</b> {len(bots)}\n"
            f"{pp('🟢')} <b>Running UserBots:</b> {running}\n"
            f"{pp('⭐️')} <b>Active Subscriptions:</b> {len(subs)}\n"
            f"{pp('📌')} <b>Total UserBot Users:</b> {total_userbot_users}\n\n"
            f"<b>UserBot Wise Users</b>\n{per_bot_text}")
    await safe_edit_message_text(q, text[:4000], parse_mode=ParseMode.HTML, reply_markup=admin_kb())


async def show_admin_sub_list(q, context: ContextTypes.DEFAULT_TYPE, page: int = 0):
    subs = db.get_all_subscriptions()
    page_size = 10
    total_pages = max(1, (len(subs) + page_size - 1) // page_size)
    page_subs = subs[page * page_size:(page + 1) * page_size]
    lines = [f"<blockquote>{pp('📋')} <b>SUBSCRIPTION LIST</b> (Page {page+1}/{total_pages})</blockquote>\n"]
    for s in page_subs:
        exp = s["expiry_date"].strftime("%d %b %Y") if s["expiry_date"] else "N/A"
        lines.append(f"• {s['bot_id']} @{s['bot_username'] or 'N/A'} — {s['subscription_type']} — {exp} — {'🟢' if s['bot_active'] else '🔴'}")
    kb = pagination_kb(page+1, total_pages, "admin_sublist")
    kb.append([btn("Back to Admin", "admin_panel", "primary", "🔙")])
    await safe_edit_message_text(q, "\n".join(lines)[:4000], parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup(kb) if kb else None)


async def start_all_userbots(q):
    bots = db.get_all_user_bots()
    started = 0
    failed = 0
    skipped = 0
    for bot in bots:
        bot_id = bot["bot_id"]
        if bot_id in user_bot_applications:
            skipped += 1
            continue
        sub = db.get_subscription_for_bot(bot_id)
        if not sub:
            continue
        try:
            exp = make_aware(sub["expiry_date"]) if isinstance(sub["expiry_date"], datetime) else sub["expiry_date"]
            if exp < now_aware():
                continue
        except Exception:
            continue
        try:
            await start_user_bot(bot["bot_token"], bot_id, bot["user_id"])
            db.set_user_bot_active(bot_id, True)
            started += 1
        except Exception:
            failed += 1
    await safe_edit_message_text(q, f"<blockquote>{pp('🚀')} <b>START ALL COMPLETE</b></blockquote>\n\n{pe('✅')} Started: {started}\n{pe('⏭️')} Already running: {skipped}\n{pe('❌')} Failed: {failed}", parse_mode=ParseMode.HTML, reply_markup=admin_kb())


# ================= LEAVE RECOVERY =================

def leave_recovery_status_text() -> str:
    cfg = db.get_leave_recovery_config()
    status = f"{pp('🟢')} ON" if cfg.get("enabled") else f"{pp('🔴')} OFF"
    target_id = cfg.get("target_channel_id") or "Not set"
    link = cfg.get("target_channel_link") or "Not set"
    messages = cfg.get("messages", [])
    msg_count = len(messages)

    channel_configs = cfg.get("channel_configs") or {}
    chan_lines = []
    for cid, enabled in channel_configs.items():
        chan_lines.append(f"  • <code>{cid}</code> → {'🟢 ON' if enabled else '🔴 OFF'}")
    chan_text = "\n".join(chan_lines) if chan_lines else "  (Koi channel ON nahi hai — default sabke liye OFF hai)"

    msgs_preview = ""
    for i, m in enumerate(messages[:3]):
        preview = (m.get("text") or "")[:60]
        has_btns = "✅ buttons" if m.get("buttons_json") else "no buttons"
        msgs_preview += f"\n  #{i+1} {EmojiManager._html_escape(preview)}… [{has_btns}]"
    if msg_count > 3:
        msgs_preview += f"\n  …and {msg_count - 3} more"
    if not msgs_preview:
        msgs_preview = "\n  (No messages set)"

    return (
        f"<blockquote>{pp('🔔')} <b>LEAVE RECOVERY</b></blockquote>\n\n"
        f"<b>Status:</b> {status}\n"
        f"<b>Target Channel ID:</b> <code>{target_id}</code>\n"
        f"<b>Target Link:</b> {EmojiManager._html_escape(str(link))}\n\n"
        f"<b>Messages ({msg_count}):</b>{msgs_preview}\n\n"
        f"<b>Per-Channel Config:</b>\n{chan_text}\n\n"
        f"<i>Global setting. All userbots use this config.</i>"
    )


def leave_recovery_kb() -> InlineKeyboardMarkup:
    cfg = db.get_leave_recovery_config()
    toggle_text = f"{pp('🛑')} Disable" if cfg.get("enabled") else f"{pp('✅')} Enable"
    toggle_style = "danger" if cfg.get("enabled") else "success"
    return InlineKeyboardMarkup([
        [btn(toggle_text, "admin_leave_toggle", toggle_style, "🔔")],
        [btn("Set Target Channel", "admin_leave_set_target", "primary", "🎯")],
        [btn("Manage Messages", "admin_leave_msgs", "primary", "💬")],
        [btn("Per-Channel Settings", "admin_leave_channels", "primary", "⚙️")],
        [btn("Clear Pending Records", "admin_leave_clear_pending", "danger", "🗑")],
        [btn("Back", "admin_panel", "primary", "🔙")],
    ])


def leave_recovery_msgs_kb() -> InlineKeyboardMarkup:
    cfg = db.get_leave_recovery_config()
    messages = cfg.get("messages", [])
    rows = []
    for i, m in enumerate(messages):
        preview = (m.get("text") or f"Message #{i+1}")[:20]
        has_btns = "🔘" if m.get("buttons_json") else "📄"
        rows.append([
            btn(f"{has_btns} #{i+1} {preview}", f"admin_leave_view_msg_{i}", "primary", "📝"),
            btn("🗑 Del", f"admin_leave_del_msg_{i}", "danger", "🗑"),
        ])
    rows.append([btn("➕ Add New Message", "admin_leave_add_msg", "success", "➕")])
    rows.append([btn("Back", "admin_leave_recovery", "primary", "🔙")])
    return InlineKeyboardMarkup(rows)


def leave_recovery_channels_kb() -> InlineKeyboardMarkup:
    cfg = db.get_leave_recovery_config()
    channel_configs = cfg.get("channel_configs", {})
    all_channels = {}
    for bot in db.get_all_user_bots():
        for ch in db.get_bot_channels(bot["bot_id"]):
            cid = str(ch["channel_id"])
            all_channels[cid] = ch.get("channel_title", cid)

    rows = []
    for cid, title in list(all_channels.items())[:20]:
        enabled = bool(channel_configs.get(cid, False))
        status_icon = "🟢" if enabled else "🔴"
        rows.append([btn(f"{status_icon} {title[:25]}", f"admin_leave_chan_toggle_{cid}", "primary", "⚙️")])

    if not rows:
        rows.append([btn("No channels found", "admin_leave_channels", "primary", "❌")])

    rows.append([btn("Back", "admin_leave_recovery", "primary", "🔙")])
    return InlineKeyboardMarkup(rows)


async def show_leave_recovery_panel(q):
    await safe_edit_message_text(q, leave_recovery_status_text(), parse_mode=ParseMode.HTML, reply_markup=leave_recovery_kb())


# ================= MAIN CALLBACK HANDLER =================
async def callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q or not q.from_user:
        return
    try:
        await q.answer()
    except Exception:
        pass
    uid = q.from_user.id
    data = q.data

    try:
        if data == "main_menu":
            user = q.from_user
            context.user_data.pop("managing_bot_id", None)
            await safe_edit_message_text(q, UIFormatter.main_menu(user.first_name), parse_mode=ParseMode.HTML, reply_markup=main_menu_kb(uid))
            for key in list(context.user_data.keys()):
                if key.startswith(("broadcast_stage_", "broadcast_draft_", "adding_channel_", "setting_message_")):
                    context.user_data.pop(key, None)
            return

        if data == "add_new_bot":
            # Bot add karne ka option sirf admin ke liye (clients admin se karwate hain).
            if not is_admin(uid):
                await safe_edit_message_text(q,
                    f"{pe('❌')} Bot add karne ka option sirf admin ke liye hai.\n\n"
                    f"Apna bot add karwane ke liye {ADMIN_USERNAME} se contact karein.",
                    parse_mode=ParseMode.HTML, reply_markup=main_menu_kb(uid))
                return
            await safe_edit_message_text(q, f"<blockquote>{pp('🔐')} <b>ADD YOUR BOT</b></blockquote>\n\nSend your BotFather API token to link your bot:", parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup([[btn("Back", "main_menu", "primary", "🔙")]]))
            context.user_data["waiting_token"] = True
            return

        if data.startswith("manage_bot_"):
            bot_id = data.replace("manage_bot_", "")
            bot_data = db.get_user_bot(bot_id)
            if not bot_data:
                await safe_edit_message_text(q, f"{pe('❌')} Bot not found!", parse_mode=ParseMode.HTML, reply_markup=main_menu_kb(uid))
                return
            # Check if user is owner OR admin
            if bot_data.get("user_id") != uid and not is_admin(uid):
                await safe_edit_message_text(q, f"{pe('❌')} You don't have permission to manage this bot.", parse_mode=ParseMode.HTML, reply_markup=main_menu_kb(uid))
                return
            # Remember which bot this panel belongs to, so the owner/admin can keep
            # working from the MAIN bot (set messages, add channel, buttons...).
            context.user_data["managing_bot_id"] = bot_id
            await safe_edit_message_text(q, f"<blockquote>{pp('🤖')} <b>MANAGE BOT</b></blockquote>\n\nBot: @{bot_data['bot_username']}\nBot ID: {bot_id}", parse_mode=ParseMode.HTML, reply_markup=bot_management_kb(bot_id, uid))
            return

        # ---- USERBOT MANAGE-PANEL CALLBACKS ON THE MAIN BOT -------------------
        # bot_management_kb() is shown here too, but none of its ub_*/setmsg_*/...
        # callbacks used to be handled by the main bot: clicking them did nothing
        # and any text the user typed only brought the menu back. Delegate them to
        # the same code the userbot itself uses.
        managed_bot = managed_bot_id_from_data(data, uid)
        if managed_bot:
            m_bot_data = db.get_user_bot(managed_bot)
            if not m_bot_data:
                await safe_edit_message_text(q, f"{pe('❌')} Bot not found!", parse_mode=ParseMode.HTML, reply_markup=main_menu_kb(uid))
                return
            if not is_bot_owner(managed_bot, uid):
                await safe_edit_message_text(q, f"{pe('❌')} You don't have permission to manage this bot.", parse_mode=ParseMode.HTML, reply_markup=main_menu_kb(uid))
                return
            context.user_data["managing_bot_id"] = managed_bot
            m_owner = m_bot_data.get("user_id") or uid
            if data.startswith(f"setbtn_{managed_bot}_") or data == f"setbtng_{managed_bot}":
                await handle_set_buttons_callback(update, context, managed_bot, m_owner)
            else:
                await user_bot_callback(update, context, managed_bot, m_owner)
            return

        if data.startswith("sub_for_bot_"):
            bot_id = data.replace("sub_for_bot_", "")
            await safe_edit_message_text(q, UIFormatter.subscription_required(), parse_mode=ParseMode.HTML, reply_markup=subscription_plans_kb(bot_id))
            return

        if data.startswith("sub_basic_") or data.startswith("sub_pro_"):
            parts = data.split("_")
            bot_id = parts[2] if len(parts) > 2 and parts[2] not in ["basic", "pro"] else None
            plan = "Basic" if "basic" in data else "Pro"
            await safe_edit_message_text(q, f"<blockquote>{pp('💰') if plan == 'Basic' else pp('⚡️')} <b>{plan} PLAN SELECTED</b></blockquote>\n\n{'Rs2599/month — 1 channel' if plan == 'Basic' else 'Rs3999/month — 5 channels'}\n\n{pp('📞')} Contact {ADMIN_USERNAME} to complete payment.", parse_mode=ParseMode.HTML, reply_markup=subscription_plans_kb(bot_id))
            return

        if data == "human_verify":
            db.mark_user_verified(uid)
            await safe_edit_message_text(q, UIFormatter.verification_success(q.from_user.first_name or ""),
                parse_mode=ParseMode.HTML, reply_markup=main_menu_kb(uid))
            return

        if data == "admin_panel":
            if not is_admin(uid):
                await safe_edit_message_text(q, f"{pe('❌')} Not authorized", parse_mode=ParseMode.HTML)
                return
            await safe_edit_message_text(q, f"<blockquote>{pp('👑')} <b>ADMIN PANEL</b></blockquote>", parse_mode=ParseMode.HTML, reply_markup=admin_kb())
            return

        if data == "admin_all_users":
            if not is_admin(uid):
                return
            await show_all_users(q)
            return

        if data == "admin_userbots":
            if not is_admin(uid):
                return
            await show_admin_userbot_control(q, context)
            return

        if data == "admin_add_userbot":
            if not is_admin(uid):
                return
            await safe_edit_message_text(q, f"<blockquote>{pp('🚀')} <b>ADD USERBOT</b></blockquote>\n\nSend: <code>user_id bot_token</code>\nExample: <code>123456789 123456:ABCdef...</code>", parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup([[btn("Back", "admin_panel", "primary", "🔙")]]))
            context.user_data["admin_add_userbot"] = True
            return

        if data == "admin_add_sub":
            if not is_admin(uid):
                return
            await safe_edit_message_text(q, f"<blockquote>{pp('⭐️')} <b>ADD SUBSCRIPTION</b></blockquote>\n\nSend: <code>@bot_username days Plan</code>\nExample: <code>@KALAKAAR_xBOT 30 Basic</code>\n\nOr: <code>bot_id days Plan</code>", parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup([[btn("Back", "admin_panel", "primary", "🔙")]]))
            context.user_data["admin_add_sub"] = True
            return

        if data == "admin_sub_list":
            if not is_admin(uid):
                return
            await show_admin_sub_list(q, context, page=0)
            return

        if data.startswith("admin_sublist_pg_"):
            if not is_admin(uid):
                return
            page = int(data.split("_")[-1])
            await show_admin_sub_list(q, context, page=page)
            return

        if data == "admin_check_expiry":
            if not is_admin(uid):
                return
            await check_expiry(q)
            return

        if data == "admin_stats":
            if not is_admin(uid):
                return
            await show_stats(q)
            return

        if data == "admin_start_all":
            if not is_admin(uid):
                return
            await start_all_userbots(q)
            return

        if data == "admin_stop_all":
            if not is_admin(uid):
                return
            stopped = 0
            for bot_id in list(user_bot_applications.keys()):
                await stop_user_bot(bot_id)
                stopped += 1
            await safe_edit_message_text(q, f"<blockquote>{pp('🛑')} <b>STOP ALL COMPLETE</b></blockquote>\n\n{pe('✅')} Stopped: {stopped}", parse_mode=ParseMode.HTML, reply_markup=admin_kb())
            return

        # LEAVE RECOVERY CALLBACKS
        if data == "admin_leave_recovery":
            if not is_admin(uid):
                return
            await show_leave_recovery_panel(q)
            return

        if data == "admin_leave_toggle":
            if not is_admin(uid):
                return
            cfg = db.get_leave_recovery_config()
            cfg["enabled"] = not bool(cfg.get("enabled"))
            db.set_leave_recovery_config(cfg)
            await show_leave_recovery_panel(q)
            return

        if data == "admin_leave_set_target":
            if not is_admin(uid):
                return
            context.user_data["admin_set_leave_target"] = True
            await safe_edit_message_text(q, f"<blockquote>{pp('🎯')} <b>SET LEAVE TARGET CHANNEL</b></blockquote>\n\nSend target as:\n<code>-1001234567890 https://t.me/+invite_or_public_link</code>", parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup([[btn("Back", "admin_leave_recovery", "primary", "🔙")]]))
            return

        if data == "admin_leave_msgs":
            if not is_admin(uid):
                return
            cfg = db.get_leave_recovery_config()
            msgs = cfg.get("messages", [])
            count = len(msgs)
            await safe_edit_message_text(q,
                f"<blockquote>{pp('💬')} <b>LEAVE RECOVERY MESSAGES ({count})</b></blockquote>\n\n"
                f"Jab user channel se leave kare tab ye messages DM mein jayenge.\n\n"
                f"Multiple messages add kar sakte hain — sab ek ke baad ek bheje jayenge.\n"
                f"Har message ke liye alag buttons set kar sakte hain.\n\n"
                f"Placeholders: <code>{{first_name}}</code> <code>{{username}}</code> <code>{{user_id}}</code>\n"
                f"<code>{{source_channel_title}}</code> <code>{{target_channel_link}}</code>",
                parse_mode=ParseMode.HTML, reply_markup=leave_recovery_msgs_kb())
            return

        if data == "admin_leave_add_msg":
            if not is_admin(uid):
                return
            context.user_data["admin_set_leave_msg"] = True
            await safe_edit_message_text(q,
                f"<blockquote>{pp('💬')} <b>ADD LEAVE RECOVERY MESSAGE</b></blockquote>\n\n"
                "Send text for DM. Supported placeholders:\n"
                "<code>{{first_name}}</code> <code>{{username}}</code> <code>{{user_id}}</code>\n"
                "<code>{{source_channel_title}}</code> <code>{{target_channel_link}}</code>",
                parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup([[btn("Back", "admin_leave_msgs", "primary", "🔙")]]))
            return

        if data.startswith("admin_leave_view_msg_"):
            if not is_admin(uid):
                return
            idx = int(data.replace("admin_leave_view_msg_", ""))
            cfg = db.get_leave_recovery_config()
            messages = cfg.get("messages", [])
            if idx >= len(messages):
                await show_leave_recovery_panel(q)
                return
            m = messages[idx]
            text_preview = EmojiManager._html_escape((m.get("text") or "")[:300])
            has_buttons = "✅ Yes" if m.get("buttons_json") else "❌ No"
            await safe_edit_message_text(q,
                f"<blockquote>{pp('📝')} <b>MESSAGE #{idx+1}</b></blockquote>\n\n"
                f"<b>Text:</b>\n{text_preview}\n\n"
                f"<b>Buttons:</b> {has_buttons}",
                parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup([
                    [btn("Edit Text", f"admin_leave_edit_text_{idx}", "primary", "📝"),
                     btn("Set Buttons", f"admin_leave_set_btns_{idx}", "primary", "🔘")],
                    [btn("🗑 Delete Message", f"admin_leave_del_msg_{idx}", "danger", "🗑")],
                    [btn("Back", "admin_leave_msgs", "primary", "🔙")],
                ]))
            return

        if data.startswith("admin_leave_del_msg_"):
            if not is_admin(uid):
                return
            idx = int(data.replace("admin_leave_del_msg_", ""))
            cfg = db.get_leave_recovery_config()
            messages = cfg.get("messages", [])
            if 0 <= idx < len(messages):
                messages.pop(idx)
                cfg["messages"] = messages
                db.set_leave_recovery_config(cfg)
            cfg2 = db.get_leave_recovery_config()
            count = len(cfg2.get("messages", []))
            await safe_edit_message_text(q, f"{pe('✅')} Message deleted. Total: {count}",
                parse_mode=ParseMode.HTML, reply_markup=leave_recovery_msgs_kb())
            return

        if data.startswith("admin_leave_edit_text_"):
            if not is_admin(uid):
                return
            idx = int(data.replace("admin_leave_edit_text_", ""))
            context.user_data["admin_edit_leave_msg_idx"] = idx
            await safe_edit_message_text(q,
                f"<blockquote>{pp('📝')} <b>EDIT MESSAGE #{idx+1} TEXT</b></blockquote>\n\nSend new text:",
                parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup([[btn("Back", "admin_leave_msgs", "primary", "🔙")]]))
            return

        if data.startswith("admin_leave_set_btns_"):
            if not is_admin(uid):
                return
            idx = int(data.replace("admin_leave_set_btns_", ""))
            context.user_data["admin_set_leave_btns_idx"] = idx
            await safe_edit_message_text(q,
                f"<blockquote>{pp('🔘')} <b>SET BUTTONS FOR MESSAGE #{idx+1}</b></blockquote>\n\n"
                "Send button lines.\n\nFormat:\n"
                "• <code>Button Label|https://link</code>\n"
                "• <code>Label1|https://url1 || Label2|https://url2</code>\n\n"
                "Multiple buttons per row use <code> || </code> separator.",
                parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup([[btn("Back", "admin_leave_msgs", "primary", "🔙")]]))
            return

        if data == "admin_leave_channels":
            if not is_admin(uid):
                return
            await safe_edit_message_text(q,
                f"<blockquote>{pp('⚙️')} <b>PER-CHANNEL LEAVE RECOVERY</b></blockquote>\n\n"
                "Toggle leave recovery ON/OFF for each channel.\n"
                "🟢 = Leave recovery active for this channel\n"
                "🔴 = Leave recovery disabled for this channel\n\n"
                "<i>Default: har channel ke liye OFF. Jisko chahiye usko manually ON karein.</i>",
                parse_mode=ParseMode.HTML, reply_markup=leave_recovery_channels_kb())
            return

        if data.startswith("admin_leave_chan_toggle_"):
            if not is_admin(uid):
                return
            chan_id = data.replace("admin_leave_chan_toggle_", "")
            cfg = db.get_leave_recovery_config()
            channel_configs = cfg.get("channel_configs") or {}
            current = bool(channel_configs.get(chan_id, False))
            channel_configs[chan_id] = not current
            cfg["channel_configs"] = channel_configs
            db.set_leave_recovery_config(cfg)
            new_status = "🟢 ON" if not current else "🔴 OFF"
            await safe_edit_message_text(q,
                f"{pe('✅')} Channel <code>{chan_id}</code> leave recovery set to <b>{new_status}</b>",
                parse_mode=ParseMode.HTML, reply_markup=leave_recovery_channels_kb())
            return

        if data == "admin_leave_clear_pending":
            if not is_admin(uid):
                return
            db._execute("UPDATE leave_recovery_messages SET deleted_at=now() WHERE deleted_at IS NULL")
            await show_leave_recovery_panel(q)
            return

        if data == "admin_default_first_msg":
            if not is_admin(uid):
                return
            current = db.get_default_first_message()
            context.user_data["admin_set_default_first_msg"] = True
            await safe_edit_message_text(q,
                f"<blockquote>{pp('💬')} <b>DEFAULT FIRST MESSAGE</b></blockquote>\n\n"
                f"Ye message har user ko <b>sabse pehle</b> jaata hai jab wo join request bhejta hai.\n\n"
                f"<b>Current message:</b>\n<blockquote>{EmojiManager._html_escape(current)}</blockquote>\n\n"
                f"Naya message bhejo:\n\n"
                f"Supported placeholders:\n<code>{{first_name}}</code> <code>{{username}}</code> <code>{{user_id}}</code>",
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup([[btn("Back", "admin_panel", "primary", "🔙")]]))
            return

        if data == "admin_broadcast":
            if not is_admin(uid):
                return
            await safe_edit_message_text(q, f"<blockquote>{pp('✈️')} <b>BROADCAST</b></blockquote>\n\nChoose target:", parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup([
                [btn("Specific UserBot", "admin_bcast_target_select", "primary", "🎯")],
                [btn("All UserBots", "admin_bcast_target_all", "success", "🌐")],
                [btn("Back", "admin_panel", "primary", "🔙")],
            ]))
            return

        if data == "admin_bcast_target_all":
            if not is_admin(uid):
                return
            context.user_data["admin_broadcast"] = True
            context.user_data["admin_broadcast_target"] = None
            await safe_edit_message_text(q, f"<blockquote>{pp('✈️')} <b>BROADCAST TO ALL</b></blockquote>\n\nSend text or media to broadcast to all userbots' users.", parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup([[btn("Back", "admin_panel", "primary", "🔙")]]))
            return

        if data == "admin_bcast_target_select":
            if not is_admin(uid):
                return
            bots = db.get_all_user_bots() or []
            if not bots:
                await safe_edit_message_text(q, f"{pe('❌')} No userbots found.", parse_mode=ParseMode.HTML, reply_markup=admin_kb())
                return
            kb = [[btn(f"@{bot['bot_username']} ({bot['bot_id']})", f"admin_bcast_pick_{bot['bot_id']}", "primary", "🤖")] for bot in bots]
            kb.append([btn("Back", "admin_broadcast", "primary", "🔙")])
            await safe_edit_message_text(q, f"{pe('🤖')} Select userbot:", parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup(kb))
            return

        if data.startswith("admin_bcast_pick_"):
            if not is_admin(uid):
                return
            bot_id = data.replace("admin_bcast_pick_", "")
            context.user_data["admin_broadcast"] = True
            context.user_data["admin_broadcast_target"] = bot_id
            await safe_edit_message_text(q, f"<blockquote>{pp('✈️')} <b>BROADCAST TO USERBOT {bot_id}</b></blockquote>\n\nSend text or media to broadcast.", parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup([[btn("Back", "admin_panel", "primary", "🔙")]]))
            return

        if data == "admin_bcast_add_btns":
            if not is_admin(uid):
                return
            draft = context.user_data.get("admin_broadcast_draft", {})
            if draft:
                context.user_data["admin_broadcast_stage"] = "await_buttons"
                await safe_edit_message_text(q, f"{pe('🔘')} Send button lines (Text|https://link per line):", parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup([[btn("Back", "admin_panel", "primary", "🔙")]]))
            else:
                await safe_edit_message_text(q, f"{pe('❌')} No draft found.", parse_mode=ParseMode.HTML, reply_markup=admin_kb())
            return

        if data == "admin_bcast_send":
            if not is_admin(uid):
                return
            await preview_admin_broadcast(q, context)
            return

        if data == "admin_bcast_confirm":
            if not is_admin(uid):
                return
            await send_admin_broadcast(q, context)
            return

        if data == "admin_send_reminders":
            if not is_admin(uid):
                return
            sent_count = 0
            for days_threshold in [3, 1]:
                expiring = db.get_expiring_subscriptions(days_threshold)
                for sub in expiring:
                    bot_id = sub["bot_id"]
                    sub_type = sub["subscription_type"]
                    bot_data = db.get_user_bot(bot_id)
                    if bot_data:
                        try:
                            expiry_dt = sub["expiry_date"]
                            if isinstance(expiry_dt, str):
                                expiry_dt = datetime.fromisoformat(expiry_dt.replace('+00:00', ''))
                            expiry_dt = make_aware(expiry_dt)
                            days_left = (expiry_dt - now_aware()).days
                            if days_threshold == 3:
                                msg_text = UIFormatter.expiry_reminder_3d(sub_type, expiry_dt, days_left)
                            else:
                                msg_text = UIFormatter.expiry_reminder_1d(sub_type, expiry_dt)
                            await send_premium_message(context.bot, bot_data["user_id"], msg_text, parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup([[
                                btn("Renew", f"https://t.me/{ADMIN_USERNAME.lstrip('@')}", "danger", "💰")
                            ]]))
                            db.mark_reminder_sent(bot_id, days_threshold)
                            sent_count += 1
                        except Exception as ex:
                            logging.error(f"Manual reminder failed: {ex}")
            await safe_edit_message_text(q, f"{pe('✅')} Reminders sent to {sent_count} users.", parse_mode=ParseMode.HTML, reply_markup=admin_kb())
            return

        if data.startswith("admin_ub_start_"):
            if not is_admin(uid):
                return
            bot_id = data.replace("admin_ub_start_", "")
            bot_data = db.get_user_bot(bot_id)
            if bot_data:
                sub = db.get_subscription_for_bot(bot_id)
                if sub:
                    try:
                        exp = make_aware(sub["expiry_date"]) if isinstance(sub["expiry_date"], datetime) else sub["expiry_date"]
                        if exp > now_aware():
                            await start_user_bot(bot_data["bot_token"], bot_id, bot_data["user_id"])
                            await safe_edit_message_text(q, f"{pe('✅')} Bot @{bot_data['bot_username']} started.", parse_mode=ParseMode.HTML, reply_markup=admin_kb())
                        else:
                            await safe_edit_message_text(q, f"{pe('❌')} Subscription expired.", parse_mode=ParseMode.HTML, reply_markup=admin_kb())
                    except Exception as e:
                        await safe_edit_message_text(q, f"{pe('❌')} Failed to start: {e}", parse_mode=ParseMode.HTML, reply_markup=admin_kb())
            return

        if data.startswith("admin_ub_stop_"):
            if not is_admin(uid):
                return
            bot_id = data.replace("admin_ub_stop_", "")
            await stop_user_bot(bot_id)
            await safe_edit_message_text(q, f"{pe('🛑')} Bot stopped.", parse_mode=ParseMode.HTML, reply_markup=admin_kb())
            return

        if data.startswith("admin_ub_info_"):
            if not is_admin(uid):
                return
            bot_id = data.replace("admin_ub_info_", "")
            await show_admin_ub_info(q, bot_id, context)
            return

        if data.startswith("admin_remove_bot_"):
            if not is_admin(uid):
                return
            if data.startswith("admin_remove_bot_confirm_"):
                bot_id = data.replace("admin_remove_bot_confirm_", "")
                await stop_user_bot(bot_id)
                db.remove_user_bot(bot_id)
                await safe_edit_message_text(q, f"{pe('✅')} UserBot removed.", parse_mode=ParseMode.HTML, reply_markup=admin_kb())
            else:
                bot_id = data.replace("admin_remove_bot_", "")
                await safe_edit_message_text(q, f"{pe('⚠️')} Confirm remove userbot {bot_id}?", parse_mode=ParseMode.HTML, reply_markup=confirm_kb(f"admin_remove_bot_confirm_{bot_id}", "admin_userbots"))
            return

    except Exception as ex:
        logging.error(f"Callback error: {ex}")
        try:
            fallback_kb = admin_kb() if is_admin(uid) else main_menu_kb(uid)
            await safe_edit_message_text(q, f"{pe('❌')} Error: {str(ex)[:100]}", parse_mode=ParseMode.HTML, reply_markup=fallback_kb)
        except Exception:
            pass


async def preview_admin_broadcast(q, context: ContextTypes.DEFAULT_TYPE):
    draft = context.user_data.get("admin_broadcast_draft", {})
    if not draft:
        await safe_edit_message_text(q, f"{pe('❌')} No draft found.", parse_mode=ParseMode.HTML, reply_markup=admin_kb())
        return
    uid = q.from_user.id
    try:
        await send_media(context, uid, draft.get("media"), draft.get("media_type") or "text",
                         draft.get("text", ""), buttons_to_markup(draft.get("buttons_json")),
                         entities_json=draft.get("entities_json"), file_name=draft.get("file_name"), mime_type=draft.get("mime_type"))
    except Exception as ex:
        await safe_edit_message_text(q, f"{pe('❌')} Preview failed: {str(ex)}", parse_mode=ParseMode.HTML, reply_markup=admin_kb())
        return
    target_bot = draft.get("target_bot")
    target_label = f"userbot {target_bot}" if target_bot else "ALL userbots"
    await safe_edit_message_text(q, f"{pe('✅')} Preview sent above.\n{pe('✈️')} Confirm broadcast to <b>{target_label}</b>?", parse_mode=ParseMode.HTML, reply_markup=confirm_kb("admin_bcast_confirm", "admin_panel"))


async def send_admin_broadcast(q, context: ContextTypes.DEFAULT_TYPE):
    draft = context.user_data.get("admin_broadcast_draft", {})
    for key in ["admin_broadcast", "admin_broadcast_stage", "admin_broadcast_target"]:
        context.user_data.pop(key, None)
    if not draft:
        await safe_edit_message_text(q, f"{pe('❌')} No draft to send.", parse_mode=ParseMode.HTML, reply_markup=admin_kb())
        return
    bots = db.get_all_user_bots()
    if draft.get("target_bot"):
        bots = [b for b in bots if b["bot_id"] == draft["target_bot"]]
    await safe_edit_message_text(q, f"{pe('✈️')} Admin broadcast started...", parse_mode=ParseMode.HTML, reply_markup=admin_kb())
    total_sent = 0
    total_fail = 0
    for bot in bots:
        bot_id = bot["bot_id"]
        sub = db.get_subscription_for_bot(bot_id)
        if not sub:
            continue
        try:
            exp = make_aware(sub["expiry_date"]) if isinstance(sub["expiry_date"], datetime) else sub["expiry_date"]
            if exp < now_aware():
                continue
        except Exception:
            continue
        if bot_id in user_bot_applications:
            bot_instance = user_bot_applications[bot_id].bot
        else:
            try:
                bot_instance = Bot(token=bot["bot_token"])
            except Exception:
                continue
        recipients = db.get_requesters_for_bot(bot_id)
        for r in recipients:
            try:
                await send_media(bot_instance, r, draft.get("media"), draft.get("media_type") or "text",
                                 draft.get("text", ""), buttons_to_markup(draft.get("buttons_json")),
                                 entities_json=draft.get("entities_json"), file_name=draft.get("file_name"), mime_type=draft.get("mime_type"))
                total_sent += 1
            except Forbidden:
                total_fail += 1
            except Exception:
                total_fail += 1
    target_label = f"userbot {draft['target_bot']}" if draft.get("target_bot") else "ALL userbots"
    await safe_edit_message_text(q, f"<blockquote>{pp('✅')} <b>ADMIN BROADCAST COMPLETE</b></blockquote>\n\n{pp('📤')} Target: {target_label}\n{pp('✅')} Sent: {total_sent}\n{pp('❌')} Failed: {total_fail}", parse_mode=ParseMode.HTML, reply_markup=admin_kb())
    context.user_data.pop("admin_broadcast_draft", None)


# ================= MAIN MESSAGE HANDLER =================
async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not user:
        return
    msg = update.message
    if not msg:
        return

    if context.user_data.get("admin_add_userbot") and is_admin(user.id):
        parts = msg.text.strip().split()
        if len(parts) == 2 and parts[0].isdigit():
            target = int(parts[0])
            token = parts[1]
            try:
                test_bot = Bot(token=token)
                bot_info = await test_bot.get_me()
                bot_id = db.add_user_bot(target, token, bot_info.username)
                context.user_data.pop("admin_add_userbot", None)
                await reply_premium_message(msg, f"{pe('✅')} Bot @{bot_info.username} linked to user {target}\nBot ID: {bot_id}", parse_mode=ParseMode.HTML, reply_markup=admin_kb())
            except Exception as ex:
                await reply_premium_message(msg, f"{pe('❌')} Error: {ex}", parse_mode=ParseMode.HTML, reply_markup=admin_kb())
        else:
            await reply_premium_message(msg, f"{pe('❌')} Format: <code>user_id bot_token</code>", parse_mode=ParseMode.HTML, reply_markup=admin_kb())
        return

    # FIX: "Add New Bot" used to be blocked for admins (`and not is_admin(...)`), so an
    # admin who pasted a token only ever got "Use menu" + the menu back - again and
    # again, because waiting_token was never cleared. Admins can add their own bot now.
    if context.user_data.get("waiting_token") and is_admin(user.id):
        token = (msg.text or "").strip()
        if ":" in token and len(token) > 10:
            try:
                test_bot = Bot(token=token)
                bot_info = await test_bot.get_me()
                bot_id = db.add_user_bot(user.id, token, bot_info.username)
                context.user_data.pop("waiting_token", None)
                await reply_premium_message(msg, f"<blockquote>{pp('✅')} <b>BOT ADDED SUCCESSFULLY</b></blockquote>\n\n{pp('🤖')} @{bot_info.username}\nBot ID: <code>{bot_id}</code>\n\n{pp('⚠️')} You need a subscription to activate your bot.\n{pp('📞')} Contact {ADMIN_USERNAME} to subscribe.", parse_mode=ParseMode.HTML, reply_markup=main_menu_kb(user.id))
            except Exception as ex:
                context.user_data.pop("waiting_token", None)
                await reply_premium_message(msg, f"{pe('❌')} Invalid token or bot error: {ex}\n\nPlease try again.", parse_mode=ParseMode.HTML)
        else:
            context.user_data.pop("waiting_token", None)
            await reply_premium_message(msg, f"{pe('❌')} Invalid token format. Please send the correct BotFather token.", parse_mode=ParseMode.HTML)
        return

    if context.user_data.get("admin_add_sub") and is_admin(user.id):
        parts = msg.text.strip().split()
        if len(parts) >= 3:
            bot_identifier = parts[0]
            days = int(parts[1])
            plan = parts[2].capitalize()
            if plan not in ("Basic", "Pro"):
                await reply_premium_message(msg, f"{pe('❌')} Plan must be Basic or Pro.", parse_mode=ParseMode.HTML, reply_markup=admin_kb())
                context.user_data.pop("admin_add_sub", None)
                return
            bot = None
            if bot_identifier.startswith("@"):
                bot = db.get_bot_by_username(bot_identifier.lstrip("@"))
            else:
                bot = db.get_user_bot(bot_identifier)
            if not bot:
                await reply_premium_message(msg, f"{pe('❌')} Bot {bot_identifier} not found!", parse_mode=ParseMode.HTML, reply_markup=admin_kb())
                context.user_data.pop("admin_add_sub", None)
                return
            db.add_subscription_for_bot(bot["bot_id"], plan, days)
            context.user_data.pop("admin_add_sub", None)
            await reply_premium_message(msg, f"{pe('✅')} Subscription added!\n{pp('🤖')} @{bot['bot_username']}\n{pp('⭐️')} {plan}\n{pp('📅')} {days} days", parse_mode=ParseMode.HTML, reply_markup=admin_kb())
            try:
                await start_user_bot(bot["bot_token"], bot["bot_id"], bot["user_id"])
                db.set_user_bot_active(bot["bot_id"], True)
                await send_premium_message(context.bot, bot["user_id"], f"<blockquote>{pp('✅')} <b>BOT ACTIVATED</b></blockquote>\n\n{pp('🤖')} @{bot['bot_username']}\n{pp('⭐️')} {plan}\n{pp('📅')} {days} days\n\nYour bot is now running!", parse_mode=ParseMode.HTML)
            except Exception as e:
                logging.error(f"Auto-start failed: {e}")
                await send_premium_message(context.bot, bot["user_id"], f"<blockquote>{pp('✅')} <b>SUBSCRIPTION ACTIVATED</b></blockquote>\n\n{pp('🤖')} @{bot['bot_username']}\n{pp('⭐️')} {plan}\n{pp('📅')} {days} days\n\nUse /start to access your bot panel.", parse_mode=ParseMode.HTML)
        else:
            await reply_premium_message(msg, f"{pe('❌')} Format: <code>@bot_username days Plan</code> or <code>bot_id days Plan</code>", parse_mode=ParseMode.HTML, reply_markup=admin_kb())
        return

    if context.user_data.get("admin_set_leave_target") and is_admin(user.id):
        parts = msg.text.strip().split()
        if len(parts) >= 2 and parts[0].lstrip("-").isdigit():
            cfg = db.get_leave_recovery_config()
            cfg["target_channel_id"] = int(parts[0])
            cfg["target_channel_link"] = parts[1]
            context.user_data.pop("admin_set_leave_target", None)
            db.set_leave_recovery_config(cfg)
            await reply_premium_message(msg, f"{pe('✅')} Leave target channel saved.", parse_mode=ParseMode.HTML, reply_markup=leave_recovery_kb())
        else:
            await reply_premium_message(msg, f"{pe('❌')} Format: <code>-1001234567890 https://t.me/+invite_or_public_link</code>", parse_mode=ParseMode.HTML, reply_markup=leave_recovery_kb())
        return

    if context.user_data.get("admin_set_leave_msg") and is_admin(user.id):
        text = msg.text or msg.caption or ""
        if text:
            cfg = db.get_leave_recovery_config()
            messages = cfg.get("messages", [])
            messages.append({"text": text, "buttons_json": ""})
            cfg["messages"] = messages
            context.user_data.pop("admin_set_leave_msg", None)
            db.set_leave_recovery_config(cfg)
            new_idx = len(messages) - 1
            await reply_premium_message(msg,
                f"{pe('✅')} Leave message #{new_idx+1} saved!\n\n"
                f"Ab is message ke liye buttons set karna chahte ho?",
                parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup([
                    [btn(f"Set Buttons for #{new_idx+1}", f"admin_leave_set_btns_{new_idx}", "primary", "🔘")],
                    [btn("Add Another Message", "admin_leave_add_msg", "success", "➕")],
                    [btn("Done", "admin_leave_msgs", "primary", "✅")],
                ]))
        else:
            await reply_premium_message(msg, f"{pe('❌')} Please send text message.", parse_mode=ParseMode.HTML, reply_markup=leave_recovery_kb())
        return

    if context.user_data.get("admin_edit_leave_msg_idx") is not None and is_admin(user.id):
        idx = context.user_data.pop("admin_edit_leave_msg_idx")
        text = msg.text or msg.caption or ""
        if text:
            cfg = db.get_leave_recovery_config()
            messages = cfg.get("messages", [])
            if 0 <= idx < len(messages):
                messages[idx]["text"] = text
                cfg["messages"] = messages
                db.set_leave_recovery_config(cfg)
                await reply_premium_message(msg, f"{pe('✅')} Message #{idx+1} text updated!", parse_mode=ParseMode.HTML, reply_markup=leave_recovery_msgs_kb())
            else:
                await reply_premium_message(msg, f"{pe('❌')} Message not found.", parse_mode=ParseMode.HTML, reply_markup=leave_recovery_msgs_kb())
        else:
            await reply_premium_message(msg, f"{pe('❌')} Please send text message.", parse_mode=ParseMode.HTML, reply_markup=leave_recovery_msgs_kb())
        return

    if context.user_data.get("admin_set_leave_btns_idx") is not None and is_admin(user.id):
        idx = context.user_data.pop("admin_set_leave_btns_idx")
        btn_json = buttons_json_from_text(msg.text or "")
        if btn_json:
            cfg = db.get_leave_recovery_config()
            messages = cfg.get("messages", [])
            if 0 <= idx < len(messages):
                messages[idx]["buttons_json"] = btn_json
                cfg["messages"] = messages
                db.set_leave_recovery_config(cfg)
                preview_markup = buttons_to_markup(btn_json)
                if preview_markup:
                    try:
                        await reply_premium_message(msg, f"{pe('👁')} <b>Button Preview:</b>", parse_mode=ParseMode.HTML, reply_markup=preview_markup)
                    except Exception:
                        pass
                await reply_premium_message(msg, f"{pe('✅')} Buttons for message #{idx+1} saved!", parse_mode=ParseMode.HTML, reply_markup=leave_recovery_msgs_kb())
            else:
                await reply_premium_message(msg, f"{pe('❌')} Message not found.", parse_mode=ParseMode.HTML, reply_markup=leave_recovery_msgs_kb())
        else:
            await reply_premium_message(msg,
                f"{pe('❌')} No valid buttons.\n\nFormat:\n"
                "• <code>Button Label|https://link</code>\n"
                "• <code>Label1|https://url1 || Label2|https://url2</code>",
                parse_mode=ParseMode.HTML, reply_markup=leave_recovery_msgs_kb())
        return

    if context.user_data.get("admin_set_default_first_msg") and is_admin(user.id):
        text = msg.text or msg.caption or ""
        if text.strip():
            db.set_default_first_message(text.strip())
            context.user_data.pop("admin_set_default_first_msg", None)
            await reply_premium_message(msg, f"{pe('✅')} Default first message saved!\n\n<blockquote>{EmojiManager._html_escape(text.strip())}</blockquote>", parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup([[btn("Back to Admin", "admin_panel", "primary", "🔙")]]))
        else:
            await reply_premium_message(msg, f"{pe('❌')} Please send a text message.", parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup([[btn("Back", "admin_default_first_msg", "primary", "🔙")]]))
        return

    if context.user_data.get("admin_broadcast") and is_admin(user.id):
        extracted = MessageManager.extract_from_message(msg)
        draft = {
            "text": msg.text or msg.caption or "",
            "media": msg.photo[-1].file_id if msg.photo else (msg.video.file_id if msg.video else (msg.document.file_id if msg.document else None)),
            "media_type": "photo" if msg.photo else ("video" if msg.video else ("document" if msg.document else "text")),
            "entities_json": extracted["entities_json"],
            "file_name": extracted.get("file_name"),
            "mime_type": extracted.get("mime_type"),
            "target_bot": context.user_data.get("admin_broadcast_target"),
        }
        context.user_data["admin_broadcast_draft"] = draft
        context.user_data["admin_broadcast_stage"] = "await_buttons"
        await reply_premium_message(msg, f"{pe('✅')} Broadcast draft saved. Add buttons or send now?", parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup([
            [btn("Add Inline Buttons", "admin_bcast_add_btns", "primary", "🔘")],
            [btn("Send Now", "admin_bcast_send", "success", "🚀")],
            [btn("Cancel", "admin_panel", "danger", "❌")],
        ]))
        return

    if context.user_data.get("admin_broadcast_stage") == "await_buttons" and is_admin(user.id):
        draft = context.user_data.get("admin_broadcast_draft", {})
        btn_json = buttons_json_from_text(msg.text or "")
        if btn_json:
            draft["buttons_json"] = btn_json
            context.user_data["admin_broadcast_draft"] = draft
            preview_markup = buttons_to_markup(btn_json)
            if preview_markup:
                try:
                    await reply_premium_message(msg, f"{pe('👁')} <b>Button Preview</b> — yahi dikhega users ko:", parse_mode=ParseMode.HTML, reply_markup=preview_markup)
                except Exception:
                    pass
            await reply_premium_message(msg, f"{pe('✅')} Buttons saved. Ready to send?", parse_mode=ParseMode.HTML, reply_markup=confirm_kb("admin_bcast_send", "admin_panel"))
        else:
            await reply_premium_message(msg, f"{pe('❌')} No valid buttons.\n\nFormat:\n• <code>Button Label|https://link</code>\n• <code>Label One|https://link1 || Label Two|https://link2</code>", parse_mode=ParseMode.HTML, reply_markup=admin_kb())
        context.user_data["admin_broadcast_stage"] = "await_send"
        return

    # ---- USERBOT SETUP FLOW DRIVEN FROM THE MAIN BOT -------------------------
    # The manage panel of a userbot is reachable from the main bot as well. While a
    # flow is active there (set message / set buttons / add channel / edit ...), the
    # typed message belongs to that flow - not to the support inbox and not to the
    # "Use menu" fallback.
    managing_bot_id = context.user_data.get("managing_bot_id")
    if managing_bot_id and is_bot_owner(managing_bot_id, user.id) and has_active_flow(context, user.id, managing_bot_id):
        admin_flow_active = (
            context.user_data.get("admin_add_userbot")
            or context.user_data.get("admin_add_sub")
            or context.user_data.get("admin_set_leave_target")
            or context.user_data.get("admin_set_leave_msg")
            or context.user_data.get("admin_set_default_first_msg")
            or context.user_data.get("admin_broadcast")
            or context.user_data.get("admin_broadcast_stage")
            or context.user_data.get("admin_edit_leave_msg_idx") is not None
            or context.user_data.get("admin_set_leave_btns_idx") is not None
        )
        if not admin_flow_active:
            m_bot_data = db.get_user_bot(managing_bot_id) or {}
            await handle_user_bot_message(update, context, managing_bot_id, m_bot_data.get("user_id") or user.id)
            return

    # Admin reply to support message
    if is_admin(user.id) and msg.reply_to_message:
        target_uid = _get_support_uid(SUPPORT_REPLY_MAP, msg.reply_to_message.message_id)
        if target_uid:
            try:
                await context.bot.copy_message(chat_id=target_uid, from_chat_id=msg.chat_id, message_id=msg.message_id)
                await send_ephemeral_reply(msg, f"{pe('✅')} Reply delivered to user {target_uid}", 2)
            except Exception as ex:
                await reply_premium_message(msg, f"{pe('❌')} Reply failed: {ex}", parse_mode=ParseMode.HTML)
            return

    # Non-admin user sending message to MAIN bot (support)
    if not is_admin(user.id):
        try:
            delivered = 0
            user_name = user.first_name or "N/A"
            user_username = user.username or "N/A"

            if msg.text:
                support_text = format_support_msg(user_name, user_username, user.id, msg.text, clickable=True)
                for admin_id in ADMIN_USER_IDS:
                    try:
                        relayed = await context.bot.send_message(chat_id=admin_id, text=support_text,
                                                                  parse_mode=ParseMode.HTML, disable_web_page_preview=True)
                        _store_support_map(SUPPORT_REPLY_MAP, relayed.message_id, user.id)
                        delivered += 1
                    except Exception:
                        pass
            else:
                header_text = format_support_msg(user_name, user_username, user.id, clickable=True)
                media_type_label = getattr(msg, 'content_type', 'media').upper()
                fallback_notice = (
                    f"{header_text}\n\n"
                    f"<i>⚠️ User sent a {media_type_label} — could not forward due to content protection.</i>"
                )
                for admin_id in ADMIN_USER_IDS:
                    try:
                        await context.bot.send_message(chat_id=admin_id, text=header_text,
                                                        parse_mode=ParseMode.HTML, disable_web_page_preview=True)
                        relayed = await safe_copy_message(
                            context.bot, admin_id, msg.chat_id, msg.message_id,
                            fallback_text=fallback_notice
                        )
                        if relayed:
                            _store_support_map(SUPPORT_REPLY_MAP, relayed.message_id, user.id)
                            delivered += 1
                    except Exception:
                        pass

            if delivered > 0:
                await send_ephemeral_reply(msg, f"{pe('✅')} Message sent to support. You will receive a reply here.", 3)
            else:
                await reply_premium_message(msg, f"{pe('⚠️')} Support is temporarily unavailable. Please try again later.", parse_mode=ParseMode.HTML)
        except Exception as ex:
            logging.error(f"Support message error: {ex}")
            await reply_premium_message(msg, f"{pe('⚠️')} Could not send to support right now. Please try again.", parse_mode=ParseMode.HTML)
        return

    await reply_premium_message(msg, f"{pe('🔽')} Use menu.", parse_mode=ParseMode.HTML, reply_markup=main_menu_kb(user.id))


async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    err_str = str(context.error)
    logging.error(f"Update {update} caused error {err_str}")
    if "Message is not modified" in err_str:
        return
    if "Query is too old" in err_str:
        return
    if "Forbidden" in err_str:
        return
    if "NetworkError" in err_str or "ReadError" in err_str:
        logging.warning(f"Network error (will retry later): {err_str}")
        return


# ================= START / ADMIN COMMANDS =================
async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not user:
        return
    db.add_user(user.id, user.username, user.first_name, user.last_name)
    # Admin gets admin panel directly, no welcome message
    if is_admin(user.id):
        await reply_premium_message(update.message, f"<blockquote>{pp('👑')} <b>ADMIN PANEL</b></blockquote>", parse_mode=ParseMode.HTML, reply_markup=admin_kb())
        return
    await reply_premium_message(update.message, UIFormatter.main_menu(user.first_name), parse_mode=ParseMode.HTML, reply_markup=main_menu_kb(user.id))


async def admin_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not user or not is_admin(user.id):
        return
    await reply_premium_message(update.message, f"<blockquote>{pp('👑')} <b>ADMIN PANEL</b></blockquote>", parse_mode=ParseMode.HTML, reply_markup=admin_kb())


async def proof_text_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not user or not is_admin(user.id):
        return
    await reply_premium_message(update.message, f"{pp('✅')} Bot is running fine.", parse_mode=ParseMode.HTML)


# ================= MAIN =================
async def _run_main():
    logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
    logging.info(f"{pp('🚀')} Starting Premium Bot System...")

    expired_bots = db.get_expired_subscriptions()
    for bot_id in expired_bots:
        bot_data = db.get_user_bot(bot_id)
        if bot_data and bot_data["is_active"] == 1:
            if bot_id in user_bot_applications:
                try:
                    app = user_bot_applications[bot_id]
                    await app.updater.stop()
                    await app.stop()
                    await app.shutdown()
                    logging.info(f"{pp('🛑')} Stopped expired bot on startup: {bot_id}")
                except Exception as ex:
                    logging.error(f"Error stopping expired bot {bot_id}: {ex}")
                user_bot_applications.pop(bot_id, None)
            db.set_user_bot_active(bot_id, False)

    bots = db.get_all_user_bots()
    if bots:
        logging.info(f"Found {len(bots)} user bots to start")
        for bot in bots:
            sub = db.get_subscription_for_bot(bot["bot_id"])
            if not sub:
                logging.info(f"Skipping {bot['bot_id']} - no subscription")
                db.set_user_bot_active(bot["bot_id"], False)
                continue
            try:
                expiry = make_aware(sub["expiry_date"]) if isinstance(sub["expiry_date"], datetime) else sub["expiry_date"]
                if expiry < now_aware():
                    logging.info(f"Skipping {bot['bot_id']} - subscription expired")
                    db.set_user_bot_active(bot["bot_id"], False)
                    continue
            except Exception as ex:
                logging.error(f"Error checking expiry for {bot['bot_id']}: {ex}")
                continue
            try:
                await start_user_bot(bot["bot_token"], bot["bot_id"], bot["user_id"])
                db.set_user_bot_active(bot["bot_id"], True)
                logging.info(f"{pp('✅')} Started user bot @{bot['bot_username']} for {bot['bot_id']}")
            except Exception as ex:
                logging.error(f"{pp('❌')} Failed to start user bot {bot['bot_id']}: {ex}")
    else:
        logging.info("No user bots found in database")

    from telegram.request import HTTPXRequest
    request = HTTPXRequest(
        connection_pool_size=100,
        connect_timeout=30.0,
        read_timeout=30.0,
        write_timeout=30.0,
        pool_timeout=10.0,
    )

    app = ApplicationBuilder().token(MAIN_BOT_TOKEN).concurrent_updates(True).request(request).build()

    app.add_handler(CommandHandler("start", start_command))
    app.add_handler(CommandHandler("admin", admin_command))
    app.add_handler(CommandHandler("proof", proof_text_command))
    app.add_handler(CommandHandler("prooftext", proof_text_command))
    app.add_handler(CallbackQueryHandler(callback_handler))
    app.add_handler(MessageHandler(filters.TEXT | filters.PHOTO | filters.VIDEO | filters.Document.ALL | filters.AUDIO | filters.VOICE | filters.Sticker.ALL, handle_message))
    app.add_error_handler(error_handler)

    app.job_queue.run_repeating(subscription_reminder_job, interval=43200, first=60, name="subscription_reminders")
    app.job_queue.run_repeating(check_expired_subscriptions_job, interval=3600, first=120, name="expired_subscriptions_check")

    try:
        await app.initialize()
        await app.start()
        await app.updater.start_polling(allowed_updates=["message", "callback_query", "chat_member", "chat_join_request", "inline_query"])
        logging.info(f"{pp('✅')} Main bot started successfully")
    except InvalidToken as ex:
        # Main bot ka token kharab ho to bhi clients ke userbots chalte rahenge -
        # sirf admin/client panel band rahega jab tak token fix nahi hota.
        logging.error(f"{pp('❌')} MAIN BOT TOKEN INVALID: {ex}")
        logging.error(
            "Naya token set karo aur restart karo:\n"
            "  1) @BotFather -> /mybots -> apna bot -> API Token\n"
            "  2) Bot folder me .env file me likho:  MAIN_BOT_TOKEN=<naya-token>\n"
            "  3) bash start"
        )
        logging.warning(f"{pp('⚠️')} Admin/client panel BAND rahega, lekin {len(user_bot_applications)} userbot(s) chal rahe hain.")

    try:
        await asyncio.Event().wait()
    except KeyboardInterrupt:
        logging.info(f"{pp('🛑')} Stopping bots...")
        for bot_id, user_app in user_bot_applications.items():
            try:
                await user_app.updater.stop()
                await user_app.stop()
                await user_app.shutdown()
                logging.info(f"{pp('✅')} Stopped user bot {bot_id}")
            except Exception:
                pass
        logging.info(f"{pp('✅')} All bots stopped")


async def main():
    """Runs the whole system. On ANY failure the userbots are stopped too, so the
    next attempt starts clean instead of leaving orphan pollers behind (those caused
    'Conflict: terminated by other getUpdates request' and made the owner's setup
    flow forget its state)."""
    try:
        await _run_main()
    except BaseException:
        try:
            await stop_all_userbots()
        except Exception as ex:
            logging.warning(f"cleanup after fatal error failed: {ex}")
        raise


MAX_START_RETRIES = 5


if __name__ == "__main__":
    attempts = 0
    while True:
        try:
            asyncio.run(main())
        except KeyboardInterrupt:
            logging.info(f"{pp('🛑')} Stopped by user")
            break
        except InvalidToken as ex:
            # Permanent: retrying can never succeed, it only floods bot.log.
            logging.error(f"{pp('❌')} MAIN BOT TOKEN INVALID: {ex}")
            logging.error(
                "Naya token set karo aur restart karo:\n"
                "  1) @BotFather -> /mybots -> apna bot -> API Token (revoke kiya hai to naya copy karo)\n"
                "  2) Bot folder me .env file banao (ya edit karo) aur likho:\n"
                "       MAIN_BOT_TOKEN=<naya-token>\n"
                "  3) bash start\n"
                "(.env .gitignore me hai - token git me nahi jayega.)"
            )
            break
        except Exception as ex:
            attempts += 1
            logging.error(f"{pp('❌')} Fatal error: {ex}")
            if attempts >= MAX_START_RETRIES:
                logging.error(f"{pp('🛑')} {MAX_START_RETRIES} baar fail hua - restart band. bot.log check karo.")
                break
            logging.info(f"{pp('🔄')} Restarting in 10 seconds... (attempt {attempts}/{MAX_START_RETRIES})")
            time.sleep(10)