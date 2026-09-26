import asyncio
import logging
import json
import time
import os
import re
import inspect
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

# ================= LOG SECURITY (secret masking) =================
TOKEN_RE = re.compile(r"\d{6,12}:[A-Za-z0-9_\-]{30,}")
LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'


def mask_secrets(text) -> str:
    """Bot tokens ko kabhi log/error me mat dikhao.

    Ek leaked token Telegram khud revoke kar deta hai (aur public repo me padha
    hua token poore internet ke liye open hota hai), isliye har log line se
    pehle token mask ho jata hai."""
    if not isinstance(text, str):
        text = str(text)
    return TOKEN_RE.sub(lambda m: f"{m.group(0).split(':', 1)[0]}:***MASKED***", text)


class MaskingFormatter(logging.Formatter):
    """Formatter jo traceback ke andar chhupe tokens ko bhi mask karta hai."""

    def format(self, record):
        return mask_secrets(super().format(record))


def install_log_masking():
    """Root logger ke saare handlers par masking formatter laga do."""
    root = logging.getLogger()
    if not root.handlers:
        logging.basicConfig(format=LOG_FORMAT, level=logging.INFO)
    for handler in root.handlers:
        handler.setFormatter(MaskingFormatter(LOG_FORMAT))


def load_env_file():
    """MAIN_BOT_TOKEN / DATABASE_URL ko .env se load karo (repo me secret commit na ho)."""
    candidates = [os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"), ".env"]
    for path in candidates:
        try:
            if not os.path.exists(path):
                continue
            with open(path, encoding="utf-8") as fh:
                for line in fh:
                    line = line.strip()
                    if not line or line.startswith("#") or "=" not in line:
                        continue
                    key, value = line.split("=", 1)
                    key = key.strip()
                    if key.startswith("export "):  # shell-style .env bhi chale
                        key = key[len("export "):].strip()
                    os.environ.setdefault(key, value.strip().strip('"').strip("'"))
        except Exception:
            continue


# ================= CONFIG =================
load_env_file()
MAIN_BOT_TOKEN = os.getenv("MAIN_BOT_TOKEN", "").strip()
MAIN_BOT_TOKEN_HINT = (
    "BotFather -> /mybots -> apna bot -> API Token -> Revoke -> NAYA token copy karo,\n"
    "phir server par:  cd ~/advanced && echo 'MAIN_BOT_TOKEN=<naya_token>' > .env && ./start\n"
    "(Naya token kabhi GitHub/chat me mat bhejo - Telegram use turant revoke kar deta hai.)")
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

# ================= STYLED / PREMIUM BUTTON CORE =================
# Telegram (Bot API 9.5+) lets a bot show a custom (premium/animated) emoji on an
# inline button through `icon_custom_emoji_id`, and colour it through `style`.
# python-telegram-bot < 22.7 has no dedicated parameters for those yet, so we
# detect support at runtime and otherwise pass them via `api_kwargs` (which is
# forwarded to the Bot API untouched by every PTB 20+/21+/22+ build).
STYLE_VALUES = ("primary", "success", "danger")
_BTN_PARAM_CACHE: Dict[str, bool] = {}


def _button_supports(field: str) -> bool:
    if field not in _BTN_PARAM_CACHE:
        try:
            _BTN_PARAM_CACHE[field] = field in inspect.signature(InlineKeyboardButton.__init__).parameters
        except Exception:
            _BTN_PARAM_CACHE[field] = False
    return _BTN_PARAM_CACHE[field]


# custom-emoji-id -> emoji character. We keep our own cache (filled while parsing
# user input) plus a reverse view of EMOJI_IDS so that, when Telegram refuses the
# animated emoji, the button still shows *some* emoji instead of nothing.
_CUSTOM_EMOJI_CHARS: Dict[str, str] = {}
_EMOJI_CHAR_BY_ID: Dict[str, str] = {}
for _emoji_char, _emoji_id in EMOJI_IDS.items():
    _EMOJI_CHAR_BY_ID.setdefault(_emoji_id, _emoji_char)


def remember_emoji_char(emoji_id: Optional[str], emoji_char: Optional[str]):
    """Remember which plain emoji belongs to a custom emoji id (fallback rendering)."""
    if not emoji_id or not emoji_char:
        return
    _CUSTOM_EMOJI_CHARS[str(emoji_id)] = str(emoji_char)[:8]


def emoji_char_for_id(emoji_id: Optional[str]) -> str:
    if not emoji_id:
        return ""
    return (_CUSTOM_EMOJI_CHARS.get(str(emoji_id))
            or _EMOJI_CHAR_BY_ID.get(str(emoji_id))
            or "")


def build_button(text: str, callback_data: Optional[str] = None, url: Optional[str] = None,
                 style: Optional[str] = None, icon_id: Optional[str] = None,
                 with_style: bool = True, with_icon: bool = True) -> InlineKeyboardButton:
    """Build an inline button with an optional colour (style) + premium emoji icon.

    `with_style=False` / `with_icon=False` are used by the automatic fallbacks when
    Telegram rejects styled buttons (unavailable custom emoji document, old client,
    bot owner without Premium, ...).
    """
    kwargs: Dict[str, Any] = {}
    api_kwargs: Dict[str, Any] = {}
    if style in STYLE_VALUES and with_style:
        if _button_supports("style"):
            kwargs["style"] = style
        else:
            api_kwargs["style"] = style
    if icon_id and with_icon:
        if _button_supports("icon_custom_emoji_id"):
            kwargs["icon_custom_emoji_id"] = str(icon_id)
        else:
            api_kwargs["icon_custom_emoji_id"] = str(icon_id)
    if callback_data is not None:
        kwargs["callback_data"] = callback_data
    else:
        kwargs["url"] = url
    if api_kwargs:
        kwargs["api_kwargs"] = api_kwargs
    return InlineKeyboardButton(text, **kwargs)


def button_icon_id(b) -> Optional[str]:
    """Read the custom-emoji icon id of a button, no matter how it was set."""
    try:
        value = getattr(b, "icon_custom_emoji_id", None)
        if value:
            return str(value)
        # NOTE: PTB stores unknown api_kwargs in a mappingproxy, not a dict
        api_kwargs = getattr(b, "api_kwargs", None)
        value = api_kwargs.get("icon_custom_emoji_id") if api_kwargs else None
        return str(value) if value else None
    except Exception:
        return None


def markup_has_icons(markup) -> bool:
    try:
        for row in (markup.inline_keyboard if markup else []):
            for b in row:
                if button_icon_id(b):
                    return True
    except Exception:
        pass
    return False


def _strip_leading_icon(text: str, emoji: Optional[str]) -> str:
    """Remove the icon emoji from the label *only* when it sits at the beginning,
    so meaningful emojis inside the label (🟢/🔴 status dots etc.) survive."""
    raw = text or ""
    stripped = raw.strip()
    if emoji and stripped.startswith(emoji):
        stripped = stripped[len(emoji):].strip()
    return stripped or raw.strip() or "Button"


def _degrade_markup(markup):
    """
    Fallback helper: strip style + custom-emoji icon from every button, keeping only
    text/url/callback_data, and restore the emoji as a *plain text* emoji so the
    button never loses its icon completely. Used when Telegram rejects a "styled"
    message (invalid/inaccessible custom-emoji document).
    """
    if not markup:
        return markup
    try:
        new_rows = []
        for row in markup.inline_keyboard:
            new_row = []
            for b in row:
                text = b.text or ""
                char = emoji_char_for_id(button_icon_id(b))
                if char and char not in text:
                    text = f"{char} {text}".strip()
                if getattr(b, "url", None):
                    new_row.append(InlineKeyboardButton(text, url=b.url))
                elif getattr(b, "callback_data", None):
                    new_row.append(InlineKeyboardButton(text, callback_data=b.callback_data))
                else:
                    new_row.append(b)
            new_rows.append(new_row)
        return InlineKeyboardMarkup(new_rows)
    except Exception:
        return markup


def btn(text: str, callback_data: str, style: str = "primary", emoji: str = None) -> InlineKeyboardButton:
    icon_id = EMOJI_IDS.get(emoji) if emoji else None
    return build_button(_strip_leading_icon(text, emoji), callback_data=callback_data,
                        style=style, icon_id=icon_id)


def btn_url(text: str, url: str, style: str = "primary", emoji: str = None) -> InlineKeyboardButton:
    icon_id = EMOJI_IDS.get(emoji) if emoji else None
    return build_button(_strip_leading_icon(text, emoji), url=url,
                        style=style, icon_id=icon_id)

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
        lines.append([btn("Add New Bot", "add_new_bot", "success", "➕")])
    else:
        lines.append([btn("Create New Bot", "add_new_bot", "success", "➕")])
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

# Callback prefixes of the userbot manage panel (they only carry the bot_id inside
# their payload, so the main bot has to work out which bot is meant).
USERBOT_PANEL_PREFIXES = ("ub_", "ubm_", "ubmm_", "delmsg_", "setmsg_", "setbtn_", "setbtng_",
                          "bcast_", "toggleauto_", "removechan_", "back_to_manage_")
READONLY_PANEL_PREFIXES = ("ub_stats_", "ub_list_channels_")


def resolve_managed_bot_id(user_id: int, data: str) -> Optional[str]:
    """Which userbot does this panel callback belong to?"""
    if not data:
        return None
    try:
        candidates = list(db.get_user_bots_by_owner(user_id) or [])
        known = {str(b.get("bot_id")) for b in candidates}
        if is_admin(user_id):
            for bot in (db.get_all_user_bots() or []):
                if str(bot.get("bot_id")) not in known:
                    candidates.append(bot)
                    known.add(str(bot.get("bot_id")))
        for bot in sorted(candidates, key=lambda b: -len(str(b.get("bot_id") or ""))):
            bot_id = str(bot.get("bot_id") or "")
            if bot_id and bot_id in data:
                return bot_id
    except Exception as ex:
        logging.error(f"resolve_managed_bot_id failed: {ex}")
    return None


async def show_manage_from_bot_help(q, bot_id: str):
    """The manage panel needs the userbot's own chat (that's where its handlers live)."""
    bot_data = db.get_user_bot(bot_id) or {}
    username = bot_data.get("bot_username")
    rows = []
    if username:
        rows.append([btn_url("Open My Bot", f"https://t.me/{username}", "success", "🚀")])
    rows.append([btn("Back", f"manage_bot_{bot_id}", "primary", "🔙")])
    await safe_edit_message_text(q,
        f"<blockquote>{pp('🤖')} <b>MANAGE FROM YOUR BOT</b></blockquote>\n\n"
        "Channels, welcome messages aur broadcast apne <b>bot ke andar</b> se manage hote hain.\n\n"
        f"👉 @{username or bot_id} ko <code>/start</code> bhejo aur panel kholo.",
        parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup(rows))


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

# ================= DATABASE =================
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/advanced_bot")

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

    def get_active_subscription(self, bot_id: str):
        """Latest subscription row that is still valid, or None."""
        row = self._fetchone("""SELECT * FROM bot_subscriptions\n               WHERE bot_id=%s AND expiry_date >= now()\n               ORDER BY expiry_date DESC LIMIT 1""", (bot_id,))
        return dict(row) if row else None

    def has_active_subscription(self, bot_id: str) -> bool:
        try:
            return self.get_active_subscription(bot_id) is not None
        except Exception:
            return False

    def grant_broadcast_subscription(self, bot_id: str, days: int = 1, sub_type: str = "Basic") -> bool:
        """Admin broadcast ke liye: agar koi active subscription nahi hai to 1 din ka
        Basic khud se add kar do, taaki broadcast us userbot ke users tak pahunch jaye."""
        try:
            if self.has_active_subscription(bot_id):
                return False
            self.add_subscription_for_bot(bot_id, sub_type, days)
            logging.info(f"Auto-added {days}-day {sub_type} subscription for {bot_id} (broadcast)")
            return True
        except Exception as ex:
            logging.error(f"grant_broadcast_subscription failed for {bot_id}: {ex}")
            return False

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
        """Everyone a broadcast should reach: previously reachable users first, then
        every approved join request (each id only once).

        The old query returned ONLY the reachable list whenever it was non-empty, so
        users who had approved the join request but were never messaged before never
        received a broadcast."""
        rows = self._fetchall("""SELECT requester_id, MAX(ok) AS ok FROM (
               SELECT requester_id, 1 AS ok FROM reachable_users WHERE bot_id=%s
               UNION ALL
               SELECT DISTINCT requester_id, 0 AS ok FROM join_requests WHERE bot_id=%s AND status='approved'
               ) AS all_users GROUP BY requester_id ORDER BY ok DESC""", (bot_id, bot_id))
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
    """Send user-facing messages (premium emoji tags allowed).

    Telegram can reject a message because of a custom-emoji document the bot may not
    use (Bot API: "Document_invalid") or because of broken HTML. Both cases used to
    end with the message simply NOT being delivered (broadcast buttons/emoji were lost
    that way), so we now retry with the emoji tags stripped and the button icons
    removed - a plain message is always better than no message.
    """
    try:
        return await bot.send_message(chat_id, text, *args, **kwargs)
    except Forbidden:
        logging.warning(f"Cannot send message to {chat_id}: bot blocked or can't initiate")
        return None
    except BadRequest as ex:
        logging.warning(f"send_user_message BadRequest for {chat_id}: {ex}; retrying plainly")
        plain_kwargs = dict(kwargs)
        if plain_kwargs.get("reply_markup") is not None:
            plain_kwargs["reply_markup"] = _degrade_markup(plain_kwargs["reply_markup"])
        candidates = []
        if kwargs.get("parse_mode") == ParseMode.HTML:
            candidates.append(escape_preserving_premium_emojis(text))
        candidates.append(strip_premium_emojis(text))
        for candidate in candidates:
            if not candidate or candidate == text:
                continue
            try:
                return await bot.send_message(chat_id, candidate, *args, **plain_kwargs)
            except Exception as retry_ex:
                logging.warning(f"send_user_message plain retry failed for {chat_id}: {retry_ex}")
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


# ================= INLINE BUTTON PARSING (PREMIUM-EMOJI AWARE) =================
# Telegram sends a premium (custom) emoji as: plain emoji character + a
# `custom_emoji` message entity that carries the emoji id. The old parser only
# looked at the raw text, so the id was thrown away and the premium emoji was
# downgraded to a normal emoji. We now read those entities and store the id in
# buttons_json["icon_id"], which is sent back as `icon_custom_emoji_id`.
BUTTON_ROW_SEPARATOR = "||"
EMOJI_KEYS_BY_LEN = sorted(EMOJI_IDS.keys(), key=len, reverse=True)


def _utf16_index(text: str) -> List[int]:
    """python char index -> Telegram utf-16 offset (entities use utf-16 units)."""
    out: List[int] = []
    pos = 0
    for ch in text:
        out.append(pos)
        pos += len(ch.encode("utf-16-le")) // 2
    return out


def custom_emoji_spans(text: Optional[str], entities: Optional[List]) -> List[dict]:
    """Custom-emoji entities as python-index spans: {start, end, emoji_id, char}."""
    if not text or not entities:
        return []
    try:
        u16 = _utf16_index(text)
        lookup = {offset: idx for idx, offset in enumerate(u16)}
        spans: List[dict] = []
        for entity in entities:
            if getattr(entity, "type", None) != "custom_emoji":
                continue
            emoji_id = getattr(entity, "custom_emoji_id", None)
            if not emoji_id:
                continue
            start = lookup.get(getattr(entity, "offset", -1))
            if start is None:
                continue
            end = lookup.get(getattr(entity, "offset", 0) + getattr(entity, "length", 0), len(text))
            char = text[start:end]
            remember_emoji_char(emoji_id, char)
            spans.append({"start": start, "end": end, "emoji_id": str(emoji_id), "char": char})
        return spans
    except Exception as ex:
        logging.warning(f"custom_emoji_spans failed: {ex}")
        return []


def _split_span(raw: str, start: int) -> Tuple[str, int, int]:
    """Strip a sub-string but keep its absolute offsets in sync."""
    stripped = raw.strip()
    left = len(raw) - len(raw.lstrip())
    peak = len(raw.rstrip())
    end = start + max(peak, left)
    return stripped, start + left, end


def _label_to_text_and_icon(label: str, abs_start: int, abs_end: int,
                            spans: List[dict]) -> Tuple[str, Optional[str], Optional[str]]:
    """Return (button text, icon_id, icon_char) for one button label.

    * a premium/custom emoji typed in the label wins and becomes the button icon
    * otherwise the first emoji of our premium set is promoted to the icon
    * the chosen icon emoji is removed from the visible text (it IS the icon now)
    """
    icon_id: Optional[str] = None
    icon_char: Optional[str] = None
    cuts: List[Tuple[int, int]] = []
    for span in spans:
        if span["start"] >= abs_start and span["end"] <= abs_end:
            if icon_id is None:
                icon_id, icon_char = span["emoji_id"], span["char"]
            cuts.append((span["start"] - abs_start, span["end"] - abs_start))
    if icon_id is None:
        for idx in range(len(label)):
            match = None
            for key in EMOJI_KEYS_BY_LEN:
                if label.startswith(key, idx):
                    match = key
                    break
            if match:
                icon_id, icon_char = EMOJI_IDS[match], match
                cuts.append((idx, idx + len(match)))
                break
    text = label
    for cut_start, cut_end in sorted(cuts, reverse=True):
        text = text[:cut_start] + text[cut_end:]
    return text.strip(), icon_id, icon_char


def _link_label(url: str) -> str:
    try:
        host = re.sub(r"^[a-z]+://", "", url or "").split("/")[0]
        return (host.replace("www.", "")[:30] or "Open Link")
    except Exception:
        return "Open Link"


def parse_button_lines(text: Optional[str], entities: Optional[List] = None) -> List[List[dict]]:
    """Parse the easy button syntax into rows of button dicts.

        Join Channel|https://t.me/channel              -> 1 button, own row
        Join|https://t.me/a || Site|https://site.com   -> 2 buttons on one row
        https://t.me/channel                           -> label = link host

    Premium emoji anywhere in the label is stored as the button's custom-emoji icon
    (icon_id + icon_char) so it stays premium after sending.
    """
    if not text:
        return []
    spans = custom_emoji_spans(text, entities)
    rows: List[List[dict]] = []
    cursor = 0
    for raw_line in text.split("\n"):
        line_start = cursor
        cursor += len(raw_line) + 1  # +1 for the "\n"
        parts: List[Tuple[str, int, int]] = []
        search = 0
        while True:
            idx = raw_line.find(BUTTON_ROW_SEPARATOR, search)
            if idx == -1:
                parts.append((raw_line[search:], line_start + search, line_start + len(raw_line)))
                break
            parts.append((raw_line[search:idx], line_start + search, line_start + idx))
            search = idx + len(BUTTON_ROW_SEPARATOR)
        row_buttons: List[dict] = []
        for part, part_start, part_end in parts:
            part_text, part_start, part_end = _split_span(part, part_start)
            if not part_text:
                continue
            sep = part_text.find("|")
            if sep == -1:
                if part_text.lower().startswith(("http://", "https://", "tg://")):
                    label_raw, label_start, label_end, url = "", part_start, part_start, part_text
                else:
                    continue
            else:
                label_raw, label_start, label_end = _split_span(part_text[:sep], part_start)
                url = part_text[sep + 1:].strip()
            if not url:
                continue
            label, icon_id, icon_char = _label_to_text_and_icon(label_raw, label_start, label_end, spans)
            if not label:
                label = _link_label(url) if not label_raw else (icon_char or "Open Link")
            item = {"text": label[:64], "icon_id": icon_id, "icon_char": icon_char, "style": "primary"}
            if url.lower().startswith("cb:"):
                item["cb"] = url[3:].strip()
                item["url"] = None
            else:
                item["url"] = url
                item["cb"] = None
            row_buttons.append(item)
        if row_buttons:
            rows.append(row_buttons)
    return rows


def rows_to_buttons_json(rows: Optional[List[List[dict]]]) -> Optional[str]:
    cleaned: List[List[dict]] = []
    for row in rows or []:
        clean_row: List[dict] = []
        for b in row or []:
            if not isinstance(b, dict):
                continue
            item = {"text": (b.get("text") or "Button").strip()[:64] or "Button"}
            if b.get("url"):
                item["url"] = b["url"]
            elif b.get("cb") or b.get("callback_data"):
                item["cb"] = b.get("cb") or b.get("callback_data")
            else:
                continue
            if b.get("icon_id"):
                item["icon_id"] = str(b["icon_id"])
            icon_char = b.get("icon_char") or emoji_char_for_id(b.get("icon_id"))
            if icon_char:
                item["icon_char"] = icon_char
            if b.get("style") in STYLE_VALUES:
                item["style"] = b["style"]
            clean_row.append(item)
        if clean_row:
            cleaned.append(clean_row)
    return json.dumps(cleaned, ensure_ascii=False) if cleaned else None


def parse_buttons_json(buttons_json) -> List[List[dict]]:
    """Tolerant buttons_json -> rows of dicts (always a list)."""
    if not buttons_json:
        return []
    if isinstance(buttons_json, (list, tuple)):
        return list(buttons_json)
    try:
        data = json.loads(buttons_json)
    except Exception:
        return []
    return data if isinstance(data, list) else []


def rows_from_buttons_json(buttons_json) -> List[List[dict]]:
    """Normalise stored json into the internal row format used by the button builder."""
    rows: List[List[dict]] = []
    for row in parse_buttons_json(buttons_json):
        if not isinstance(row, list):
            continue
        clean_row: List[dict] = []
        for b in row:
            if not isinstance(b, dict):
                continue
            item = {
                "text": b.get("text") or "Button",
                "url": b.get("url"),
                "cb": b.get("cb") or b.get("callback_data"),
                "icon_id": str(b.get("icon_id")) if b.get("icon_id") else None,
                "icon_char": b.get("icon_char") or emoji_char_for_id(b.get("icon_id")),
                "style": b.get("style") if b.get("style") in STYLE_VALUES else "primary",
            }
            if item["url"] or item["cb"]:
                clean_row.append(item)
        if clean_row:
            rows.append(clean_row)
    return rows


def markup_from_rows(rows: Optional[List[List[dict]]], use_icons: bool = True):
    if not rows:
        return None
    markup_rows = []
    for row in rows:
        btn_row = []
        for b in row or []:
            if not isinstance(b, dict):
                continue
            text = (b.get("text") or "Button")[:64]
            icon_id = b.get("icon_id")
            style = b.get("style") or "primary"
            if not use_icons:
                char = b.get("icon_char") or emoji_char_for_id(icon_id)
                if char and char not in text:
                    text = f"{char} {text}".strip()
            if b.get("url"):
                btn_row.append(build_button(text, url=b["url"], style=style,
                                            icon_id=icon_id, with_icon=use_icons))
            elif b.get("cb") or b.get("callback_data"):
                btn_row.append(build_button(text, callback_data=b.get("cb") or b.get("callback_data"),
                                            style=style, icon_id=icon_id, with_icon=use_icons))
        if btn_row:
            markup_rows.append(btn_row)
    return InlineKeyboardMarkup(markup_rows) if markup_rows else None


def buttons_to_markup(buttons_json, use_icons: bool = True):
    return markup_from_rows(rows_from_buttons_json(buttons_json) if not isinstance(buttons_json, list)
                            else buttons_json, use_icons=use_icons)


def buttons_to_plain_markup(buttons_json):
    rows = rows_from_buttons_json(buttons_json) if not isinstance(buttons_json, list) else buttons_json
    return markup_from_rows(rows, use_icons=False)


def button_count(buttons_json) -> int:
    rows = rows_from_buttons_json(buttons_json) if not isinstance(buttons_json, list) else buttons_json
    return sum(len(r) for r in rows or [])


def buttons_json_from_text(text: Optional[str], entities: Optional[List] = None) -> Optional[str]:
    return rows_to_buttons_json(parse_button_lines(text, entities))


def parse_buttons_text(text: Optional[str], entities: Optional[List] = None):
    """Legacy helper - returns PTB button rows (kept for compatibility)."""
    markup = markup_from_rows(parse_button_lines(text, entities))
    return markup.inline_keyboard if markup else []


def add_callback_button_to_json(buttons_json, text: str, cb: str, url: Optional[str] = None) -> str:
    rows = rows_from_buttons_json(buttons_json)
    for row in rows:
        for b in row:
            if (cb and b.get("cb") == cb) or (url and b.get("url") == url):
                return rows_to_buttons_json(rows) or "[]"
    rows.append([{"text": text, "url": url, "cb": None if url else cb,
                  "icon_id": None, "icon_char": None, "style": "primary"}])
    return rows_to_buttons_json(rows) or "[]"


# ================= EASY BUTTON BUILDER (WIZARD) =================
# New flow that any user can follow without learning any syntax:
#   ➕ Add Button  ->  send the button name  ->  send the link  ->
#   same row / new row?  ->  repeat  ->  ✅ Save
# The old "Label|link" bulk syntax stays available as "📄 Paste Many".
BUTTON_WIZARD_KEY = "button_wizard"
BUTTON_TARGET_KEY = "button_targets"
BUTTON_TARGET_TTL = 6 * 3600


def _target_store(context) -> dict:
    store = context.user_data.get(BUTTON_TARGET_KEY)
    if not isinstance(store, dict):
        store = {}
        context.user_data[BUTTON_TARGET_KEY] = store
    now = time.time()
    for key in [k for k, v in (list(store.items())) if now - v.get("ts", 0) > BUTTON_TARGET_TTL]:
        store.pop(key, None)
    return store


def register_button_target(context, target: dict) -> str:
    store = _target_store(context)
    tid = str(int(time.time() * 1000) % 1000000)
    while tid in store:
        tid = str((int(tid) + 1) % 1000000)
    store[tid] = {"target": target, "ts": time.time()}
    return tid


def get_button_target(context, tid) -> Optional[dict]:
    if tid is None or tid == "":
        return None
    entry = _target_store(context).get(str(tid))
    if not entry:
        return None
    entry["ts"] = time.time()
    return entry.get("target")


def button_builder_row(context, target: dict) -> List[InlineKeyboardButton]:
    """One row callers can drop into any keyboard: ➕ Add Button | 📄 Paste Many."""
    tid = register_button_target(context, target)
    return [btn("Add Button", f"bwz_start_{tid}", "success", "➕"),
            btn("Paste Many", f"bwz_bulk_{tid}", "primary", "📄")]


def _draft_for_target(context, target: dict) -> dict:
    kind = target.get("kind")
    if kind == "draft_user":
        return context.user_data.get(f"broadcast_draft_{target.get('bot_id')}") or {}
    if kind == "draft_admin":
        return context.user_data.get("admin_broadcast_draft") or {}
    return {}


def _target_title(target: dict) -> str:
    kind = target.get("kind")
    if kind == "message":
        return "Saved Message"
    if kind == "messages":
        return "Saved Album"
    if kind == "draft_user":
        return "Broadcast Message"
    if kind == "draft_admin":
        return "Admin Broadcast"
    if kind == "leave_msg":
        return f"Leave Message #{int(target.get('idx', 0)) + 1}"
    return "Message"


def target_rows(context, target: dict) -> List[List[dict]]:
    kind = target.get("kind")
    try:
        if kind == "message":
            row = db.get_message_by_id(target.get("msg_id"))
            return rows_from_buttons_json(row.get("buttons_json") if row else None)
        if kind == "messages":
            for mid in target.get("msg_ids") or []:
                row = db.get_message_by_id(mid)
                if row and row.get("buttons_json"):
                    return rows_from_buttons_json(row.get("buttons_json"))
            return []
        if kind in ("draft_user", "draft_admin"):
            return rows_from_buttons_json(_draft_for_target(context, target).get("buttons_json"))
        if kind == "leave_msg":
            messages = db.get_leave_recovery_config().get("messages", [])
            idx = int(target.get("idx", 0))
            if 0 <= idx < len(messages):
                return rows_from_buttons_json(messages[idx].get("buttons_json"))
        return []
    except Exception as ex:
        logging.error(f"target_rows failed: {ex}")
        return []


def target_save_rows(context, target: dict, rows) -> bool:
    payload = rows_to_buttons_json(rows) or "[]"
    kind = target.get("kind")
    try:
        if kind == "message":
            db.update_message_buttons(target.get("msg_id"), payload)
            return True
        if kind == "messages":
            msg_ids = target.get("msg_ids") or []
            for mid in msg_ids:
                db.update_message_buttons(mid, payload)
            return bool(msg_ids)
        if kind == "draft_user":
            draft = dict(_draft_for_target(context, target))
            draft["buttons_json"] = payload
            context.user_data[f"broadcast_draft_{target.get('bot_id')}"] = draft
            return True
        if kind == "draft_admin":
            draft = dict(_draft_for_target(context, target))
            draft["buttons_json"] = payload
            context.user_data["admin_broadcast_draft"] = draft
            return True
        if kind == "leave_msg":
            cfg = db.get_leave_recovery_config()
            messages = cfg.get("messages", [])
            idx = int(target.get("idx", 0))
            if 0 <= idx < len(messages):
                messages[idx]["buttons_json"] = payload
                cfg["messages"] = messages
                db.set_leave_recovery_config(cfg)
                return True
        return False
    except Exception as ex:
        logging.error(f"target_save_rows failed: {ex}")
        return False


def target_nav_rows(target: dict) -> List[List[InlineKeyboardButton]]:
    kind = target.get("kind")
    if kind == "draft_user":
        bot_id = target.get("bot_id")
        return [
            [btn("Send Broadcast", f"bcast_send_{bot_id}", "success", "🚀")],
            [btn("Cancel", f"manage_bot_{bot_id}", "danger", "❌")],
        ]
    if kind == "draft_admin":
        return [
            [btn("Send Broadcast", "admin_bcast_send", "success", "🚀")],
            [btn("Cancel", "admin_panel", "danger", "❌")],
        ]
    if kind == "leave_msg":
        return [[btn("Back", "admin_leave_msgs", "primary", "🔙")]]
    if target.get("back_cb"):
        return [[btn(target.get("back_text") or "Back", target["back_cb"], "primary", "🔙")]]
    return []


def _wizard_layout(rows) -> str:
    if not rows:
        return "<i>(abhi koi button nahi)</i>"
    lines = []
    for i, row in enumerate(rows, 1):
        parts = []
        for b in row or []:
            icon = (b.get("icon_char") or "").strip()
            label = (b.get("text") or "Button").strip()
            parts.append(f"[{icon} {label}]" if icon else f"[{label}]")
        lines.append(f"{i}. " + "  ".join(parts))
    return "\n".join(lines)


def _wizard_state(context) -> Optional[dict]:
    state = context.user_data.get(BUTTON_WIZARD_KEY)
    return state if isinstance(state, dict) else None


def _wizard_text(state: dict, target: dict) -> str:
    body = (f"{pe('🔘')} <b>BUTTON BUILDER</b> — {_target_title(target)}\n\n"
            f"<b>Layout:</b>\n{_wizard_layout(state.get('rows') or [])}\n\n")
    step = state.get("step")
    if step == "name":
        return body + (f"{pe('✏️')} <b>Button ka naam bhejo</b> — jo text button par dikhega.\n\n"
                       "Premium emoji bhi chalega: <code>💎 Join Now</code> bhejo to 💎 premium icon ban jayega.")
    if step == "url":
        name = (state.get("pending") or {}).get("text") or ""
        return body + (f"<b>Naam:</b> {EmojiManager._html_escape(name)}\n\n"
                       f"{pe('🔗')} <b>Ab is button ka link bhejo</b>\n"
                       "Example: <code>https://t.me/yourchannel</code>")
    if step == "bulk":
        return body + (f"{pe('📄')} <b>Bulk mode:</b> ek line me ek button bhejo\n\n"
                       "<code>Join Channel|https://t.me/channel</code>\n"
                       "<code>Join|https://t.me/a || Site|https://site.com</code>\n\n"
                       "<i>Do button ek hi line me chahiye to <code>||</code> lagao.</i>")
    return body + (f"{pe('➕')} <b>Naya button add karo</b> — Same Row = 2 button ek line me, "
                   "New Row = apni alag line me.")


def _wizard_kb(state: dict, target: dict) -> Optional[InlineKeyboardMarkup]:
    tid = state.get("tid")
    step = state.get("step")
    rows = state.get("rows") or []
    kb: List[List[InlineKeyboardButton]] = []
    if step in ("name", "url", "bulk"):
        if rows:
            kb.append([btn("Save & Done", f"bwz_done_{tid}", "success", "✅")])
        kb.append([btn("Cancel", f"bwz_cancel_{tid}", "danger", "❌")])
        return InlineKeyboardMarkup(kb)
    add_row: List[InlineKeyboardButton] = []
    if rows and len(rows[-1]) < 2:
        add_row.append(btn("Add Same Row", f"bwz_same_{tid}", "success", "↔️"))
    add_row.append(btn("Add New Row", f"bwz_row_{tid}", "success", "➕"))
    kb.append(add_row)
    kb.append([btn("Preview", f"bwz_prev_{tid}", "primary", "👀"),
               btn("Undo Last", f"bwz_undo_{tid}", "danger", "🗑")])
    kb.append([btn("Save & Done", f"bwz_done_{tid}", "success", "✅")])
    kb.append([btn("Cancel", f"bwz_cancel_{tid}", "danger", "❌")])
    return InlineKeyboardMarkup(kb)


async def _wizard_render(context, state: dict, target: dict, q=None, note: str = ""):
    text = _wizard_text(state, target)
    if note:
        text = f"{note}\n\n{text}"
    kb = _wizard_kb(state, target)
    if q is not None:
        await safe_edit_message_text(q, text, parse_mode=ParseMode.HTML, reply_markup=kb)
        return
    chat_id = state.get("chat_id")
    if chat_id:
        await send_premium_message(context.bot, chat_id, text, parse_mode=ParseMode.HTML, reply_markup=kb)


def label_from_message(msg) -> Tuple[str, Optional[str], Optional[str]]:
    """Extract a button label (+ premium emoji icon) from a user message."""
    raw = msg.text or msg.caption or ""
    if not raw.strip():
        return "", None, None
    first_line = raw.split("\n")[0]
    entities = list(msg.entities or msg.caption_entities or [])
    spans = custom_emoji_spans(raw, entities)
    label, icon_id, icon_char = _label_to_text_and_icon(first_line, 0, len(first_line), spans)
    if not label:
        label = icon_char or first_line.strip()
    return label.strip()[:64], icon_id, icon_char


async def start_button_wizard(q, context, tid, mode: str = "wizard"):
    target = get_button_target(context, tid)
    if not target:
        await safe_edit_message_text(q, f"{pe('❌')} Ye builder session purana ho gaya. Button dobara kholo.",
                                     parse_mode=ParseMode.HTML)
        return
    rows = target_rows(context, target)
    if mode == "bulk":
        step = "bulk"
    elif rows:
        step = "next"
    else:
        step = "name"
    state = {
        "tid": str(tid), "rows": rows, "pending": None, "placement": "new", "step": step,
        "chat_id": (q.message.chat_id if q.message else q.from_user.id), "ts": time.time(),
    }
    context.user_data[BUTTON_WIZARD_KEY] = state
    note = ""
    if rows:
        note = f"{pe('👀')} Pehle se {button_count(rows_to_buttons_json(rows))} button hain — aur add kar sakte ho."
    await _wizard_render(context, state, target, q=q, note=note)


async def handle_button_wizard_callback(q, context, data: str) -> bool:
    """Handles every `bwz_*` callback. Returns True when the update was consumed."""
    if not data or not data.startswith("bwz_"):
        return False
    parts = data.split("_")
    action = parts[1] if len(parts) > 1 else ""
    tid = parts[2] if len(parts) > 2 else ""
    if action in ("start", "bulk"):
        await start_button_wizard(q, context, tid, mode="bulk" if action == "bulk" else "wizard")
        return True
    state = _wizard_state(context)
    if not state or str(state.get("tid")) != str(tid):
        await safe_edit_message_text(q, f"{pe('❌')} Ye builder band ho chuka hai. Button dobara kholo.",
                                     parse_mode=ParseMode.HTML)
        return True
    target = get_button_target(context, tid)
    if not target:
        context.user_data.pop(BUTTON_WIZARD_KEY, None)
        await safe_edit_message_text(q, f"{pe('❌')} Target session expire ho gaya.", parse_mode=ParseMode.HTML)
        return True
    if action in ("same", "row"):
        state["step"] = "name"
        state["placement"] = "same" if action == "same" else "new"
        await _wizard_render(context, state, target, q=q)
        return True
    if action == "undo":
        rows = state.get("rows") or []
        if rows:
            rows[-1].pop()
            if not rows[-1]:
                rows.pop()
        state["rows"] = rows
        state["step"] = "next"
        await _wizard_render(context, state, target, q=q, note=f"{pe('🗑')} Last button hata diya.")
        return True
    if action == "prev":
        markup = markup_from_rows(state.get("rows") or [])
        if markup:
            try:
                await send_premium_message(context.bot, state.get("chat_id") or q.from_user.id,
                                           f"{pe('👀')} <b>Preview</b> — yahi buttons users ko dikhenge:",
                                           parse_mode=ParseMode.HTML, reply_markup=markup)
            except Exception as ex:
                logging.warning(f"button preview failed: {ex}")
        else:
            await send_ephemeral_reply(q.message, f"{pe('❌')} Abhi koi button nahi hai.", 2)
        return True
    if action == "done":
        rows = state.get("rows") or []
        ok = target_save_rows(context, target, rows)
        context.user_data.pop(BUTTON_WIZARD_KEY, None)
        nav = target_nav_rows(target)
        if not ok:
            await safe_edit_message_text(q, f"{pe('❌')} Save nahi ho paya. Dobara try karo.",
                                         parse_mode=ParseMode.HTML,
                                         reply_markup=InlineKeyboardMarkup(nav) if nav else None)
            return True
        extra: List[List[InlineKeyboardButton]] = []
        if target.get("kind") in ("message", "messages") and rows:
            extra.append(button_builder_row(context, target))
        await safe_edit_message_text(q,
                                     f"{pe('✅')} <b>Buttons saved!</b> ({button_count(rows_to_buttons_json(rows))})\n\n"
                                     f"{_wizard_layout(rows)}",
                                     parse_mode=ParseMode.HTML,
                                     reply_markup=InlineKeyboardMarkup(extra + nav) if (extra or nav) else None)
        return True
    if action == "cancel":
        context.user_data.pop(BUTTON_WIZARD_KEY, None)
        nav = target_nav_rows(target)
        await safe_edit_message_text(q, f"{pe('❌')} Button builder cancelled.", parse_mode=ParseMode.HTML,
                                     reply_markup=InlineKeyboardMarkup(nav) if nav else None)
        return True
    return True


async def userbot_wizard_callback(update, context, bot_id: str, owner_id: int):
    """`bwz_*` callbacks inside a userbot chat - owner/admin only, always answered."""
    q = update.callback_query
    if not q or not q.from_user:
        return
    try:
        await q.answer()
    except Exception:
        pass
    if not is_bot_owner(bot_id, q.from_user.id):
        return
    await handle_button_wizard_callback(q, context, q.data)


async def handle_button_wizard_message(msg, context) -> bool:
    """Consume text messages while the easy button builder is active."""
    state = _wizard_state(context)
    if not state:
        return False
    target = get_button_target(context, state.get("tid"))
    if not target:
        context.user_data.pop(BUTTON_WIZARD_KEY, None)
        return False
    has_media = any(getattr(msg, attr, None) for attr in
                    ("photo", "video", "document", "audio", "voice", "video_note", "sticker", "animation"))
    if has_media:
        await reply_premium_message(msg, f"{pe('⚠️')} Button builder khula hai — pehle <b>Save & Done</b> "
                                         f"ya <b>Cancel</b> dabao, phir media bhejo.",
                                    parse_mode=ParseMode.HTML)
        return True
    if not (msg.text or msg.caption):
        await reply_premium_message(msg, f"{pe('⚠️')} Text bhejo — button ka naam ya link.",
                                    parse_mode=ParseMode.HTML)
        return True
    state["chat_id"] = msg.chat_id
    step = state.get("step")
    if step == "name":
        label, icon_id, icon_char = label_from_message(msg)
        if not label:
            await reply_premium_message(msg, f"{pe('⚠️')} Button ka naam khali hai, dobara bhejo.",
                                        parse_mode=ParseMode.HTML)
            return True
        state["pending"] = {"text": label, "icon_id": icon_id, "icon_char": icon_char,
                            "url": None, "cb": None, "style": "primary"}
        state["step"] = "url"
        await _wizard_render(context, state, target)
        return True
    if step == "url":
        url = (msg.text or msg.caption or "").strip().split("\n")[0].strip()
        is_cb = url.lower().startswith("cb:")
        if not is_cb and not url.lower().startswith(("http://", "https://", "tg://")):
            await reply_premium_message(msg, f"{pe('⚠️')} Ye link valid nahi lag raha.\n\n"
                                             "<code>https://t.me/yourchannel</code> jaisa link bhejo.",
                                        parse_mode=ParseMode.HTML)
            return True
        pending = dict(state.get("pending") or {})
        pending["url"] = None if is_cb else url
        pending["cb"] = url[3:].strip() if is_cb else None
        rows = state.get("rows") or []
        if state.get("placement") == "same" and rows and len(rows[-1]) < 8:
            rows[-1].append(pending)
        else:
            rows.append([pending])
        state["rows"] = rows
        state["pending"] = None
        state["step"] = "next"
        await _wizard_render(context, state, target,
                            note=f"{pe('✅')} <b>{EmojiManager._html_escape(pending.get('text') or '')}</b> add ho gaya!")
        return True
    if step == "bulk":
        new_rows = parse_button_lines(msg.text or msg.caption or "", msg.entities or msg.caption_entities)
        if not new_rows:
            await reply_premium_message(msg, f"{pe('❌')} Kuch samajh nahi aaya.\n\n"
                                             "Ek line me ek button:\n<code>Join|https://t.me/channel</code>\n"
                                             "<code>A|https://a.com || B|https://b.com</code>",
                                        parse_mode=ParseMode.HTML)
            return True
        rows = state.get("rows") or []
        rows.extend(new_rows)
        state["rows"] = rows
        state["step"] = "next"
        await _wizard_render(context, state, target,
                            note=f"{pe('✅')} {sum(len(r) for r in new_rows)} button add ho gaye!")
        return True
    if step == "next":
        state["step"] = "name"
        state["placement"] = "new"
        await _wizard_render(context, state, target,
                            note=f"{pe('➕')} Naya button — pehle naam bhejo:")
        return True
    return False


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


def _resolve_bot(bot_or_context):
    """Accept a Bot/ExtBot or any context that carries one (context.bot)."""
    if bot_or_context is None:
        return None
    if hasattr(bot_or_context, "send_message") and hasattr(bot_or_context, "send_photo"):
        return bot_or_context
    return getattr(bot_or_context, "bot", bot_or_context)


@retry_async(max_retries=2, delay=0.5, backoff=1.5)
async def send_media(bot_or_context, chat_id: int, media_id, media_type: str,
                     text: str = "", markup=None, emoji_map: dict = None,
                     entities_json: Optional[str] = None, file_name: Optional[str] = None,
                     mime_type: Optional[str] = None):
    bot = _resolve_bot(bot_or_context)
    kwargs = {}
    if markup:
        kwargs["reply_markup"] = markup
    display_text = MessageManager.prepare_for_sending(text, entities_json, emoji_map) if text else None

    async def _do_send(send_kwargs, caption_override=None):
        caption = caption_override if caption_override is not None else display_text
        parse_mode = ParseMode.HTML if caption else None
        if media_type == "photo":
            await bot.send_photo(chat_id, media_id, caption=caption or None, parse_mode=parse_mode, **send_kwargs)
        elif media_type == "video":
            await bot.send_video(chat_id, media_id, caption=caption or None, parse_mode=parse_mode, **send_kwargs)
        elif media_type == "document":
            doc_kwargs = dict(send_kwargs)
            if file_name:
                doc_kwargs["filename"] = file_name
            await bot.send_document(chat_id, media_id, caption=caption or None, parse_mode=parse_mode, **doc_kwargs)
        elif media_type == "animation":
            await bot.send_animation(chat_id, media_id, caption=caption or None, parse_mode=parse_mode, **send_kwargs)
        elif media_type == "audio":
            await bot.send_audio(chat_id, media_id, caption=caption or None, parse_mode=parse_mode, **send_kwargs)
        elif media_type == "voice":
            await bot.send_voice(chat_id, media_id, caption=caption or None, parse_mode=parse_mode, **send_kwargs)
        elif media_type == "video_note":
            await bot.send_video_note(chat_id, media_id, **send_kwargs)
        elif media_type == "sticker":
            await bot.send_sticker(chat_id, media_id, **send_kwargs)
        else:
            if caption:
                await send_user_message(bot, chat_id, caption, parse_mode=ParseMode.HTML, **send_kwargs)

    try:
        await _do_send(kwargs)
    except Forbidden:
        logging.warning(f"Cannot send media to {chat_id}: bot blocked")
    except BadRequest as ex:
        # 1st retry: strip button icons + premium emoji from the caption. A single bad
        # custom-emoji document must never cost the whole media delivery.
        logging.warning(f"send_media BadRequest for {chat_id} ({media_type}): {ex}; retrying plainly")
        degraded_kwargs = dict(kwargs)
        if "reply_markup" in degraded_kwargs:
            degraded_kwargs["reply_markup"] = _degrade_markup(degraded_kwargs["reply_markup"])
        plain_caption = strip_premium_emojis(display_text) if display_text else None
        try:
            await _do_send(degraded_kwargs, caption_override=plain_caption)
        except Exception as ex2:
            logging.error(f"send_media plain retry also failed for {chat_id} ({media_type}): {ex2}")
            # 2nd retry: text only (so the user at least receives the caption + buttons)
            try:
                if plain_caption:
                    await send_user_message(bot, chat_id, plain_caption,
                                            parse_mode=ParseMode.HTML, **degraded_kwargs)
            except Exception:
                pass
    except (NetworkError, TimedOut) as ex:
        logging.warning(f"Network error sending to {chat_id}: {ex}")
        raise
    except Exception as ex:
        logging.error(f"send_media failed: {ex}")
        try:
            plain_text = strip_premium_emojis(display_text) if display_text else (text or "").strip()
            if plain_text:
                await send_user_message(bot, chat_id, plain_text,
                                        parse_mode=ParseMode.HTML, **kwargs)
        except Exception:
            pass


# ================= ALBUM / BROADCAST DRAFT SENDING =================
ALBUM_MEDIA_TYPES = ("photo", "video", "document", "audio")


def draft_items(draft: Optional[dict]) -> List[dict]:
    """Flatten a broadcast/album draft into an ordered list of media items."""
    draft = draft or {}
    album = draft.get("album")
    items: List[dict] = []
    if isinstance(album, list) and album:
        for it in album:
            if not isinstance(it, dict):
                continue
            items.append({
                "media": it.get("media") or it.get("media_id"),
                "media_type": it.get("media_type") or "text",
                "text": it.get("text") or "",
                "entities_json": it.get("entities_json"),
                "file_name": it.get("file_name"),
                "mime_type": it.get("mime_type"),
            })
        if items and not items[0]["text"] and draft.get("text"):
            items[0]["text"] = draft.get("text")
            items[0]["entities_json"] = draft.get("entities_json")
        if items:
            return items
    return [{
        "media": draft.get("media") or draft.get("media_id"),
        "media_type": draft.get("media_type") or "text",
        "text": draft.get("text") or "",
        "entities_json": draft.get("entities_json"),
        "file_name": draft.get("file_name"),
        "mime_type": draft.get("mime_type"),
    }]


def make_media_item(extracted: dict) -> dict:
    """One album/broadcast item from a MessageManager.extract_from_message() result."""
    return {
        "media": extracted.get("media_id"),
        "media_type": extracted.get("media_type"),
        "text": extracted.get("text") or "",
        "entities_json": extracted.get("entities_json"),
        "file_name": extracted.get("file_name"),
        "mime_type": extracted.get("mime_type"),
        "telegram_message_id": extracted.get("telegram_message_id"),
    }


async def send_buttons_after_album(bot, chat_id, text: str, markup):
    """Telegram does not allow reply_markup on an album, so the buttons go into a
    small follow-up message (this is how buttons on media groups work)."""
    body = text or f"{pe('🔗')} <b>Links</b>"
    try:
        await send_user_message(bot, chat_id, body, parse_mode=ParseMode.HTML, reply_markup=markup)
        return True
    except Exception as ex:
        logging.error(f"album buttons message failed for {chat_id}: {ex}")
        return False


def album_caption_index(items: List[dict]) -> int:
    """Album ka main caption = pehla item jiske paas text hai (-1 agar koi nahi)."""
    for idx, it in enumerate(items or []):
        if it.get("media") and it.get("media_type") in ALBUM_MEDIA_TYPES and (it.get("text") or "").strip():
            return idx
    return -1


async def send_album(bot, chat_id, items: List[dict], markup=None, button_text: str = "",
                     move_caption: bool = False) -> bool:
    """Send 2..10 media items as one album, then the buttons (album + caption+buttons).

    Telegram **album (media group) par inline buttons support nahi karta**
    (send_media_group me reply_markup parameter hi nahi hai). Isliye buttons ek chhote
    follow-up message me jate hain. `move_caption=True` hone par album ka main caption
    bhi usi message me chala jata hai - users ko album ke turant neeche EK message
    dikhta hai jisme text + buttons dono hote hain (orphan "Links" bubble nahi).
    """
    moved_caption = ""
    skip_caption = -1
    if move_caption and markup:
        skip_caption = album_caption_index(items)
        if skip_caption >= 0:
            moved_caption = MessageManager.prepare_for_sending(items[skip_caption].get("text") or "",
                                                               items[skip_caption].get("entities_json"))
    group = []
    for idx, it in enumerate(items):
        media_id = it.get("media")
        media_type = it.get("media_type")
        if not media_id or media_type not in ALBUM_MEDIA_TYPES:
            continue
        if idx == skip_caption:
            caption = None
        else:
            caption = MessageManager.prepare_for_sending(it.get("text") or "", it.get("entities_json")) if it.get("text") else None
        media_kwargs = {"caption": caption, "parse_mode": ParseMode.HTML if caption else None}
        if media_type == "photo":
            group.append(InputMediaPhoto(media=media_id, **media_kwargs))
        elif media_type == "video":
            group.append(InputMediaVideo(media=media_id, **media_kwargs))
        elif media_type == "document":
            group.append(InputMediaDocument(media=media_id, **media_kwargs))
        else:
            group.append(InputMediaAudio(media=media_id, **media_kwargs))
        if len(group) >= 10:
            break
    if len(group) < 2:
        return False
    try:
        await bot.send_media_group(chat_id=chat_id, media=group)
    except BadRequest as ex:
        # Permanent (jaise MEDIA_GROUPED_INVALID / media invalid) -> caller per-item
        # fallback karega. NOTE: BadRequest NetworkError ka subclass hai, isliye ye
        # check pehle hona zaroori hai warna neeche wala raise ise bhi le jayega.
        logging.warning(f"send_media_group failed for {chat_id}: {ex}; sending items separately")
        return False
    except (NetworkError, TimedOut):
        raise
    except Exception as ex:
        logging.warning(f"send_media_group failed for {chat_id}: {mask_secrets(ex)}; sending items separately")
        return False
    if markup:
        await send_buttons_after_album(bot, chat_id, moved_caption or button_text, markup)
    return True


async def send_draft_message(bot_or_context, chat_id, draft: Optional[dict], markup=None,
                             with_buttons: bool = True, button_text: str = "",
                             move_caption: Optional[bool] = None) -> bool:
    """Send a broadcast draft: single media/text, or the whole album + buttons.

    This is what makes grouped media (4-5 photos/videos with a caption) work in the
    broadcast flows - previously only the first item of the album was ever sent.

    Albums can't carry inline buttons (Telegram limit), so for an album the buttons
    travel in a follow-up message. By default the album caption moves into that
    message too (draft["caption_with_buttons"]) so text + buttons stay together.
    """
    bot = _resolve_bot(bot_or_context)
    draft = draft or {}
    items = draft_items(draft)
    if move_caption is None:
        move_caption = bool(draft.get("caption_with_buttons", True))
    usable = [it for it in items if it.get("media") and it.get("media_type") in ALBUM_MEDIA_TYPES]
    if len(usable) >= 2:
        sent = await send_album(bot, chat_id, items, markup=markup if with_buttons else None,
                                button_text=button_text, move_caption=bool(move_caption and with_buttons))
        if sent:
            return True
        # album failed -> deliver every item on its own, buttons (+ caption) after
        cap_idx = album_caption_index(items) if (move_caption and with_buttons) else -1
        moved = ""
        if cap_idx >= 0:
            moved = MessageManager.prepare_for_sending(items[cap_idx].get("text") or "",
                                                       items[cap_idx].get("entities_json"))
        cap_item = items[cap_idx] if 0 <= cap_idx < len(items) else None
        for it in usable:
            skip = it is cap_item   # identity compare (duplicate dicts par bhi sahi)
            await send_media(bot, chat_id, it.get("media"), it.get("media_type") or "text",
                             "" if skip else (it.get("text") or ""), None,
                             entities_json=None if skip else it.get("entities_json"),
                             file_name=it.get("file_name"), mime_type=it.get("mime_type"))
        if with_buttons and markup:
            await send_buttons_after_album(bot, chat_id, moved or button_text, markup)
        return True
    item = usable[0] if usable else (items[0] if items else {})
    await send_media(bot, chat_id, item.get("media"), item.get("media_type") or "text",
                     item.get("text") or "", markup if with_buttons else None,
                     entities_json=item.get("entities_json"),
                     file_name=item.get("file_name"), mime_type=item.get("mime_type"))
    return True


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
            raw_items = []          # har media ka raw data (caption baad me decide hota hai)
            group_buttons_json = None
            group_caption_text = None
            caption_idx = -1
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
                    if caption_idx < 0 and (g_text or "").strip():
                        caption_idx = len(raw_items)
                    raw_items.append({"media_id": g_media_id, "media_type": g_media_type,
                                      "text": g_text, "entities_json": g.get("entities_json"),
                                      "display": MessageManager.prepare_for_sending(g_text, g.get("entities_json")) if g_text else ""})
                j += 1
            if raw_items:
                group_markup = buttons_to_markup(group_buttons_json)
                # Telegram album (media group) par inline buttons support nahi hai, isliye
                # buttons ek chhote follow-up message me jate hain. Us message ka text
                # album ka caption hi rakhte hain (aur caption album se hata dete hain)
                # taaki users ko album ke turant neeche text + buttons ek saath dikhein.
                skip_idx = caption_idx if group_markup else -1
                group_items = []
                for k, item in enumerate(raw_items):
                    kwargs = {}
                    if item["display"] and k != skip_idx:
                        kwargs = {"caption": item["display"], "parse_mode": ParseMode.HTML}
                    cls = {"photo": InputMediaPhoto, "video": InputMediaVideo,
                           "document": InputMediaDocument, "audio": InputMediaAudio}[item["media_type"]]
                    group_items.append(cls(media=item["media_id"], **kwargs))
                try:
                    await context.bot.send_media_group(chat_id=chat_id, media=group_items)
                except BadRequest as ex:
                    # Fall back to sending the album items one by one instead of losing them
                    logging.error(f"send_media_group failed for {chat_id}: {ex}; sending items separately")
                    for k, item in enumerate(raw_items):
                        try:
                            text = "" if k == skip_idx else item["display"]
                            await send_media(context, chat_id, item["media_id"], item["media_type"],
                                             text or "", None, entities_json=None)
                        except Exception as inner_ex:
                            logging.error(f"album item fallback failed for {chat_id}: {inner_ex}")
                if group_markup:
                    await send_buttons_after_album(context.bot, chat_id, group_caption_text or "", group_markup)
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


class _UserDataContext:
    """Context proxy: sab kuch real context se, sirf user_data replace.

    PTB me JobQueue callback ko `CallbackContext.from_job()` wala context deta hai
    aur usme `user_data` **None** hota hai (job me user_id set nahi hota). Isi wajah
    se album flush job me `context.user_data.pop(...)` crash ho jata tha
    ("'NoneType' object has no attribute 'pop'") aur broadcast album save hi nahi hota.
    Ab schedule karte waqt asli user_data dict job ke data me jaata hai."""

    __slots__ = ("_wrapped", "_user_data")

    def __init__(self, context, user_data: dict):
        object.__setattr__(self, "_wrapped", context)
        object.__setattr__(self, "_user_data", user_data)

    @property
    def user_data(self) -> dict:
        return object.__getattribute__(self, "_user_data")

    def __getattr__(self, item):
        return getattr(object.__getattribute__(self, "_wrapped"), item)


def _context_with_user_data(context, user_data):
    """Job context (user_data None) ko live user_data ke saath usable banao."""
    if isinstance(user_data, dict) and getattr(context, "user_data", None) is not user_data:
        return _UserDataContext(context, user_data)
    return context


def _runtime_store(context: ContextTypes.DEFAULT_TYPE, key: str) -> dict:
    user_data = getattr(context, "user_data", None)
    if not isinstance(user_data, dict):
        # Never crash on a context without user_data (job contexts). Callers that can
        # only work with real storage get an empty dict and simply skip the work.
        logging.error("_runtime_store: is context me user_data available nahi hai")
        return {}
    if key not in user_data:
        user_data[key] = {}
    return user_data[key]


async def sync_pending_join_requests_for_channel(bot_id: str, channel_id: int, bot):
    try:
        if not hasattr(bot, 'get_chat_join_requests'):
            logging.info("get_chat_join_requests not available in this PTB version. Skipping sync.")
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

    # Easy button builder (➕ Add Button wizard)
    if data and data.startswith("bwz_"):
        await handle_button_wizard_callback(q, context, data)
        return

    if data == "main_menu":
        user = q.from_user
        await safe_edit_message_text(q, UIFormatter.main_menu(user.first_name), parse_mode=ParseMode.HTML,
            reply_markup=bot_management_kb(bot_id, uid))
        return

    if data == f"back_to_manage_{bot_id}" or data == f"manage_bot_{bot_id}":
        # leaving the panel drops any half-finished broadcast draft / button builder
        for stale_key in (f"broadcast_stage_{bot_id}", f"broadcast_draft_{bot_id}", BUTTON_WIZARD_KEY):
            context.user_data.pop(stale_key, None)
        await safe_edit_message_text(q, f"<blockquote>{pp('🤖')} <b>MANAGE BOT</b></blockquote>", parse_mode=ParseMode.HTML,
            reply_markup=bot_management_kb(bot_id, uid))
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
        ud = _runtime_store(context, f"{uid}_{bot_id}")
        ud["setting_message"] = True
        context.user_data[f"setting_message_{bot_id}"] = True
        await safe_edit_message_text(q,
            f"<blockquote>{pp('📝')} <b>SET MESSAGES</b></blockquote>\n\n"
            "Send messages one by one.\n\n"
            "Supported: text, photo, video, document, audio, sticker, media albums (4-5 media + caption)\n\n"
            f"{pp('🔘')} Har message ke baad <b>Add Button</b> se buttons bana sakte ho "
            "(naam bhejo → link bhejo → same row / new row).\n\n"
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
            f"<blockquote>{pp('✈️')} <b>BROADCAST</b></blockquote>\n\n"
            "Jo message bhejna hai wo bhejo — text, photo, video, document ya poora "
            "album (4-5 photo/video ek saath + caption).\n\n"
            f"{pe('🔘')} Buttons add karne ka option uske baad milega.",
            parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup([[btn("Cancel", f"manage_bot_{bot_id}", "danger", "❌")]]))
        return

    if data == f"bcast_add_btns_{bot_id}":
        if not context.user_data.get(f"broadcast_draft_{bot_id}"):
            await safe_edit_message_text(q, f"{pe('❌')} Pehle broadcast message bhejo.", parse_mode=ParseMode.HTML,
                                         reply_markup=bot_management_kb(bot_id, uid))
            return
        tid = register_button_target(context, user_broadcast_target(bot_id, uid))
        await start_button_wizard(q, context, tid)
        return

    if data == f"bcast_capmode_{bot_id}":
        draft = get_broadcast_draft(context, "user", bot_id)
        if not draft:
            await q.answer("Pehle album/message bhejo", show_alert=True)
            return
        draft["caption_with_buttons"] = not draft.get("caption_with_buttons", True)
        save_broadcast_draft(context, "user", bot_id, draft)
        await q.answer("Caption " + ("buttons ke saath (album ke neeche ek message me)" if draft["caption_with_buttons"] else "album par hi rahega"))
        try:
            await q.edit_message_reply_markup(reply_markup=user_broadcast_ready_kb(context, bot_id, uid))
        except Exception as ex:
            logging.warning(f"caption mode kb update failed: {ex}")
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
        kb = InlineKeyboardMarkup([
            button_builder_row(context, {"kind": "message", "msg_id": msg_id,
                                         "back_cb": f"ubmm_{bot_id}_{msg_id}"}),
            [btn("Cancel", f"ubmm_{bot_id}_{msg_id}", "danger", "❌")],
        ])
        await safe_edit_message_text(q,
            f"{pe('🔘')} <b>Edit Inline Buttons</b>\n\n"
            "<b>Add Button</b> = easy tarika (naam → link → same row / new row)\n"
            "<b>Paste Many</b> = purana format\n"
            "<code>Button Label|https://link</code>\n"
            "<code>Label One|https://link1 || Label Two|https://link2</code>",
            parse_mode=ParseMode.HTML, reply_markup=kb)
        return

    if data.startswith(f"delmsg_{bot_id}_"):
        parts = data.split("_")
        msg_id = _extract_last_id(parts)
        db.delete_message(msg_id)
        await safe_edit_message_text(q, f"{pe('✅')} Message deleted.", parse_mode=ParseMode.HTML, reply_markup=bot_management_kb(bot_id, uid))
        return

    if data == f"setmsg_more_{bot_id}":
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
        kb = InlineKeyboardMarkup([
            button_builder_row(context, {"kind": "message", "msg_id": msg_id,
                                         "back_cb": f"manage_bot_{bot_id}"}),
            [btn("Back", f"manage_bot_{bot_id}", "primary", "🔙")],
        ])
        await safe_edit_message_text(q,
            f"{pe('🔘')} <b>Aur buttons add karo</b> — purane buttons already load ho chuke hain.",
            parse_mode=ParseMode.HTML, reply_markup=kb)
        return

    if data.startswith(f"setbtng_addmore_{bot_id}"):
        ud = _runtime_store(context, f"{uid}_{bot_id}")
        grp = ud.get("pending_buttons_group_addmore") or {}
        msg_ids = grp.get("msg_ids") or []
        if not msg_ids:
            await safe_edit_message_text(q, f"{pe('❌')} Album session nahi mila, album dobara bhejo.",
                                         parse_mode=ParseMode.HTML, reply_markup=bot_management_kb(bot_id, uid))
            return
        kb = InlineKeyboardMarkup([
            button_builder_row(context, {"kind": "messages", "msg_ids": msg_ids,
                                         "back_cb": f"manage_bot_{bot_id}"}),
            [btn("Back", f"manage_bot_{bot_id}", "primary", "🔙")],
        ])
        await safe_edit_message_text(q,
            f"{pe('🔘')} <b>Aur buttons add karo</b> — purane buttons already load ho chuke hain.",
            parse_mode=ParseMode.HTML, reply_markup=kb)
        return

    if data == f"setbtng_{bot_id}" or data.startswith(f"setbtn_{bot_id}_"):
        ud = _runtime_store(context, f"{uid}_{bot_id}")
        if data == f"setbtng_{bot_id}":
            grp = ud.get("pending_buttons_group") or {}
            msg_ids = grp.get("msg_ids") or []
            target = {"kind": "messages", "msg_ids": msg_ids, "back_cb": f"manage_bot_{bot_id}"}
        else:
            msg_id = _extract_last_id(data.split("_"))
            target = {"kind": "message", "msg_id": msg_id, "back_cb": f"manage_bot_{bot_id}"}
        kb = InlineKeyboardMarkup([
            button_builder_row(context, target),
            [btn("Back", f"manage_bot_{bot_id}", "primary", "🔙")],
        ])
        await safe_edit_message_text(q,
            f"{pe('🔘')} <b>Inline Buttons</b>\n\n"
            "<b>Add Button</b> = easy (naam → link → row)\n"
            "<b>Paste Many</b> = bulk format",
            parse_mode=ParseMode.HTML, reply_markup=kb)
        return


async def accept_all(q, bot_id: str, owner_id: int, context):
    try:
        channels = db.get_bot_channels(bot_id) or []
        for ch in channels:
            await sync_pending_join_requests_for_channel(bot_id, ch["channel_id"], context.bot)
    except Exception:
        pass
    pending = db.get_pending_requests(bot_id)
    if not pending:
        await safe_edit_message_text(q, f"{pe('‼️')} No pending requests.", parse_mode=ParseMode.HTML, reply_markup=bot_management_kb(bot_id, owner_id))
        return
    ok = 0
    cleaned = 0
    for req in pending:
        try:
            await context.bot.approve_chat_join_request(req["channel_id"], req["requester_id"])
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
    builder = button_builder_row(context, {"kind": "messages", "msg_ids": saved_ids,
                                           "back_cb": f"manage_bot_{bot_id}"})
    await send_premium_message(context.bot, chat_id,
        f"{pe('✅')} <b>Album saved</b> ({len(saved_ids)} media).\n\n"
        f"{pe('🔘')} Buttons add karo (album ke neeche ek chhote message me dikhenge):",
        parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup([
            builder,
            [btn("Set More Messages", f"setmsg_more_{bot_id}", "success", "➕")],
            [btn("Done", f"setmsg_done_{bot_id}", "success", "✅"),
             btn("Cancel", f"setmsg_cancel_{bot_id}", "danger", "❌")],
        ]))


async def _flush_media_group_job(context: ContextTypes.DEFAULT_TYPE):
    data = context.job.data or {}
    # Job context me user_data None hota hai - captured dict se replace karo
    ctx = _context_with_user_data(context, data.get("user_data"))
    await _flush_media_group(data.get("bot_id"), data.get("actor_uid"), data.get("managed_uid"),
                             data.get("chat_id"), ctx, data.get("media_group_id"))


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

    # Easy button builder (➕ Add Button wizard) has priority over the other flows
    if await handle_button_wizard_message(msg, context):
        return

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
            btn_json = buttons_json_from_text(msg.text or "", msg.entities or msg.caption_entities)
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
            try:
                try:
                    member = await context.bot.get_chat_member(ch.id, context.bot.id)
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
                await sync_pending_join_requests_for_channel(bot_id, ch.id, context.bot)

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
        btn_json = buttons_json_from_text(msg.text or "", msg.entities or msg.caption_entities)
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
            builder_target = ({"kind": "message", "msg_id": msg_ids[0]} if len(msg_ids) == 1
                              else {"kind": "messages", "msg_ids": msg_ids})
            builder_target["back_cb"] = f"manage_bot_{bot_id}"
            await reply_premium_message(msg, f"{pe('✅')} Inline buttons saved!", parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup([
                    button_builder_row(context, builder_target),
                    [btn("Set More Inline Buttons", more_btn_cb, "primary", "🔘")],
                    [btn("Set More Messages", f"setmsg_more_{bot_id}", "success", "➕")],
                    [btn("Done", f"setmsg_done_{bot_id}", "success", "✅"),
                     btn("Back", f"manage_bot_{bot_id}", "primary", "🔙")],
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
                      "chat_id": msg.chat_id, "media_group_id": media_group_id,
                      "user_data": context.user_data})
            ud[job_key] = j
            return

        msg_id = db.add_message(bot_id, channel_id, extracted["text"], extracted["media_id"],
                                extracted["media_type"], None, extracted["entities_json"],
                                extracted.get("file_name"), extracted.get("mime_type"),
                                extracted.get("telegram_message_id"))
        if msg_id and extracted["emoji_map"]:
            db.save_user_emoji_map(bot_id, msg_id, extracted["emoji_map"])
        ud["pending_buttons"] = {"msg_id": msg_id, "channel_id": channel_id}
        builder = button_builder_row(context, {"kind": "message", "msg_id": msg_id,
                                               "back_cb": f"manage_bot_{bot_id}"})
        await reply_premium_message(msg,
            f"{pe('✅')} <b>Message saved!</b>\n\n"
            f"{pe('🔘')} Buttons add karne ke liye <b>Add Button</b> dabao (naam → link → row), "
            f"ya <b>Paste Many</b> se purana <code>Label|link</code> format use karo.",
            parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup([
                builder,
                [btn("Set More Messages", f"setmsg_more_{bot_id}", "success", "➕")],
                [btn("Done", f"setmsg_done_{bot_id}", "success", "✅"),
                 btn("Cancel", f"setmsg_cancel_{bot_id}", "danger", "❌")],
            ]))
        return

    if context.user_data.get(f"broadcast_stage_{bot_id}") == "await_message":
        # Albums arrive as separate updates - collect them all before saving the draft
        if await collect_broadcast_album(context, "user", bot_id, msg, extracted):
            return
        draft = {"text": extracted["text"], "media": extracted["media_id"], "media_type": extracted["media_type"],
                 "emoji_map": extracted["emoji_map"], "entities_json": extracted["entities_json"],
                 "file_name": extracted.get("file_name"), "mime_type": extracted.get("mime_type"),
                 "buttons_json": None, "caption_with_buttons": True, "target_bot": bot_id}
        context.user_data[f"broadcast_draft_{bot_id}"] = draft
        context.user_data[f"broadcast_stage_{bot_id}"] = "buttons_or_send"
        await reply_premium_message(msg,
            f"{pe('✅')} <b>Broadcast draft saved.</b>\n\n"
            f"{pe('🔘')} <b>Add Button</b> se buttons add karo, ya seedha <b>Send Now</b> dabao.",
            parse_mode=ParseMode.HTML, reply_markup=user_broadcast_ready_kb(context, bot_id, owner_id))
        return

    if context.user_data.get(f"broadcast_stage_{bot_id}") == "await_buttons":
        draft = context.user_data.get(f"broadcast_draft_{bot_id}", {})
        btn_json = buttons_json_from_text(msg.text or "", msg.entities or msg.caption_entities)
        if btn_json:
            draft["buttons_json"] = btn_json
            context.user_data[f"broadcast_draft_{bot_id}"] = draft
            preview_markup = buttons_to_markup(btn_json)
            if preview_markup:
                try:
                    await reply_premium_message(msg, f"{pe('👁')} <b>Button Preview</b> — yahi dikhega users ko:", parse_mode=ParseMode.HTML, reply_markup=preview_markup)
                except Exception:
                    pass
            await reply_premium_message(msg, f"{pe('✅')} Buttons saved. Ready to send?", parse_mode=ParseMode.HTML,
                                        reply_markup=user_broadcast_ready_kb(context, bot_id, owner_id))
        else:
            await reply_premium_message(msg, f"{pe('❌')} No valid buttons.\n\nFormat:\n• 1 button: <code>Button Label|https://link</code>\n• 2 per row: <code>Label One|https://link1 || Label Two|https://link2</code>", parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup([[btn("Back", f"manage_bot_{bot_id}", "primary", "🔙")]]))
        context.user_data[f"broadcast_stage_{bot_id}"] = "buttons_or_send"
        return

    await reply_premium_message(msg, f"{pe('🔽')} Use buttons to manage.", parse_mode=ParseMode.HTML, reply_markup=bot_management_kb(bot_id, owner_id))


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

    if data == f"setbtng_{bot_id}":
        grp = ud.get("pending_buttons_group")
        if not grp or not grp.get("msg_ids"):
            await safe_edit_message_text(q, f"{pe('❌')} Group not found. Send media group again.", parse_mode=ParseMode.HTML, reply_markup=bot_management_kb(bot_id, owner_id))
            return
        ud["waiting_buttons"] = {"msg_ids": grp.get("msg_ids")}
        await safe_edit_message_text(q,
            f"{pe('🔘')} <b>Send Inline Buttons</b>\n\n"
            "• 1 button per row:\n  <code>Button Label|https://link</code>\n\n"
            "• 2 buttons per row:\n  <code>Label One|https://link1 || Label Two|https://link2</code>\n\n"
            "Emoji in button text is preserved exactly as you type.",
            parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup([[btn("Back", f"manage_bot_{bot_id}", "primary", "🔙")]]))
        return

    if data.startswith(f"setbtn_{bot_id}_"):
        parts = data.split("_")
        msg_id = _extract_last_id(parts)
        ud["waiting_buttons"] = {"msg_id": msg_id}
        await safe_edit_message_text(q,
            f"{pe('🔘')} <b>Send Inline Buttons</b>\n\n"
            "• 1 button per row:\n  <code>Button Label|https://link</code>\n\n"
            "• 2 buttons per row:\n  <code>Label One|https://link1 || Label Two|https://link2</code>\n\n"
            "Emoji in button text is preserved exactly as you type.",
            parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup([[btn("Back", f"manage_bot_{bot_id}", "primary", "🔙")]]))


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
        channel_configs = leave_cfg.get("channel_configs", {})
        chan_key = str(chat.id)
        chan_enabled = channel_configs.get(chan_key, True)
        if chan_enabled:
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

    channel_configs = leave_cfg.get("channel_configs", {})
    source_chan_key = str(cmu.chat.id)
    if not channel_configs.get(source_chan_key, True):
        logging.info(f"Leave recovery disabled for channel {cmu.chat.id}, skipping.")
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
TOKEN_FAILURES: List[Dict[str, Any]] = []


def _tuned_request():
    """Userbot apps ke liye bhi wahi network profile (warna default 5s timeout par
    flaky VPS networks me httpx.ReadError aata rehta hai)."""
    from telegram.request import HTTPXRequest
    return HTTPXRequest(
        connection_pool_size=100,
        connect_timeout=30.0,
        read_timeout=30.0,
        write_timeout=30.0,
        pool_timeout=10.0,
    )


async def _cleanup_failed_app(app):
    """Aadhe initialize hue application ko safely band karo."""
    for step in ("updater", "stop", "shutdown"):
        try:
            if step == "updater":
                if app.updater:
                    await app.updater.stop()
            elif step == "stop":
                await app.stop()
            else:
                await app.shutdown()
        except Exception:
            pass


def remember_token_failure(bot_id: str, owner_id: int, reason: str = ""):
    """Ek hi bot ke liye ek baar record karo (restart loop me spam na ho)."""
    for fail in TOKEN_FAILURES:
        if fail["bot_id"] == bot_id:
            return
    TOKEN_FAILURES.append({"bot_id": bot_id, "owner_id": owner_id or 0, "reason": mask_secrets(reason)})


async def flush_token_failures(bot=None):
    """Dead userbot token ki khabar owner + admin ko do (ek-ek baar)."""
    if not bot or not TOKEN_FAILURES:
        return
    pending = list(TOKEN_FAILURES)
    TOKEN_FAILURES.clear()
    for fail in pending:
        text = (f"<blockquote>{pp('⚠️')} <b>USERBOT TOKEN INVALID</b></blockquote>\n\n"
                f"{pp('🤖')} Bot ID: <code>{fail['bot_id']}</code>\n"
                f"{pp('❌')} Telegram ne is bot ka token reject kar diya (revoke/delete ho gaya lagta hai).\n\n"
                "BotFather -> /mybots -> apna bot -> API Token -> <b>Revoke</b> se naya token lo,\n"
                "phir panel se purana bot hata ke naya token dobara add karo.")
        targets = {int(x) for x in ADMIN_USER_IDS}
        if fail.get("owner_id"):
            targets.add(int(fail["owner_id"]))
        for uid in targets:
            try:
                await bot.send_message(uid, text, parse_mode=ParseMode.HTML)
            except Exception as ex:
                logging.warning(f"Token warning owner {uid} ko nahi bhej paye: {mask_secrets(ex)}")


async def start_user_bot(token: str, bot_id: str, owner_id: int):
    try:
        app = ApplicationBuilder().token(token).concurrent_updates(True).request(_tuned_request()).build()
    except Exception as ex:
        logging.error(f"{pp('❌')} User bot {bot_id} bana nahi paya: {mask_secrets(ex)}")
        return False
    app.bot_data["bot_id"] = bot_id
    app.bot_data["owner_id"] = owner_id
    app.add_handler(CommandHandler("start", lambda u, c: user_bot_start(u, c, bot_id, owner_id)))
    app.add_handler(CallbackQueryHandler(lambda u, c: handle_public_userbot_callback(u, c, bot_id), pattern=r'^(start_now|live_chat_support)$'))
    app.add_handler(CallbackQueryHandler(lambda u, c: user_bot_callback(u, c, bot_id, owner_id), pattern=f"^(ub_|ubm_|ubmm_|delmsg_|setbtn_|setbtng|setmsg_|bcast_|bwz_|removechan_|back_to_manage_|manage_bot_|toggleauto_|setbtn_addmore_|setbtng_addmore_).*{bot_id}|^main_menu$"))
    app.add_handler(CallbackQueryHandler(lambda u, c: userbot_wizard_callback(u, c, bot_id, owner_id), pattern=r"^bwz_"))
    app.add_handler(CallbackQueryHandler(lambda u, c: handle_set_buttons_callback(u, c, bot_id, owner_id), pattern=f"^setbtn_{bot_id}_"))
    app.add_handler(MessageHandler(filters.TEXT | filters.PHOTO | filters.VIDEO | filters.Document.ALL | filters.AUDIO | filters.VOICE | filters.Sticker.ALL, lambda u, c: handle_user_bot_message(u, c, bot_id, owner_id)))
    app.add_handler(ChatJoinRequestHandler(lambda u, c: handle_join_request(u, c, bot_id, owner_id)))
    app.add_handler(ChatMemberHandler(lambda u, c: handle_channel_member_update(u, c, bot_id, owner_id), ChatMemberHandler.CHAT_MEMBER))
    try:
        await app.initialize()
        await app.start()
        await app.updater.start_polling(allowed_updates=["message", "callback_query", "chat_member", "chat_join_request", "inline_query"])
    except (InvalidToken, Forbidden) as ex:
        # Token revoke/delete ho gaya - retry karne ka koi fayda nahi. Sirf is bot ko
        # band karo, main bot chalta rahe (pehle ye poore bot ko restart loop me daal deta tha).
        logging.error(f"{pp('❌')} User bot {bot_id} ka token Telegram ne reject kar diya "
                      f"({mask_secrets(ex)}) - is bot ko band kiya, naya token chahiye")
        await _cleanup_failed_app(app)
        try:
            db.set_user_bot_active(bot_id, False)
        except Exception:
            pass
        remember_token_failure(bot_id, owner_id, str(ex))
        return False
    except Exception as ex:
        logging.error(f"{pp('❌')} Failed to start user bot {bot_id}: {mask_secrets(ex)}")
        await _cleanup_failed_app(app)
        return False
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
def _broadcast_draft_key(scope: str, bot_id: Optional[str] = None) -> str:
    return f"broadcast_draft_{bot_id}" if scope == "user" else "admin_broadcast_draft"


def _broadcast_stage_key(scope: str, bot_id: Optional[str] = None) -> str:
    return f"broadcast_stage_{bot_id}" if scope == "user" else "admin_broadcast_stage"


def get_broadcast_draft(context, scope: str, bot_id: Optional[str] = None) -> dict:
    draft = context.user_data.get(_broadcast_draft_key(scope, bot_id))
    return draft if isinstance(draft, dict) else {}


def save_broadcast_draft(context, scope: str, bot_id: Optional[str], draft: dict):
    context.user_data[_broadcast_draft_key(scope, bot_id)] = draft


def user_broadcast_target(bot_id: str, owner_id: int) -> dict:
    return {"kind": "draft_user", "bot_id": bot_id, "owner_id": owner_id,
            "back_cb": f"manage_bot_{bot_id}"}


def admin_broadcast_target() -> dict:
    return {"kind": "draft_admin", "back_cb": "admin_panel"}


def broadcast_caption_mode_label(draft: dict) -> str:
    return "Caption: Buttons ke saath" if draft.get("caption_with_buttons", True) else "Caption: Album par"


def broadcast_caption_mode_row(draft: dict, cb: str) -> List[InlineKeyboardButton]:
    """Album + buttons wale draft ke liye ek toggle row.

    Telegram albums par buttons attach nahi hote, isliye default me album ka caption
    buttons wale message me jata hai (dono ek saath dikhte hain). Jise caption album par
    hi chahiye wo is toggle se badal sakta hai."""
    if len(draft_items(draft)) < 2:
        return []
    return [btn(broadcast_caption_mode_label(draft), cb, "primary", "📝")]


def user_broadcast_ready_kb(context, bot_id: str, owner_id: int) -> InlineKeyboardMarkup:
    draft = get_broadcast_draft(context, "user", bot_id)
    rows = [button_builder_row(context, user_broadcast_target(bot_id, owner_id))]
    mode_row = broadcast_caption_mode_row(draft, f"bcast_capmode_{bot_id}")
    if mode_row:
        rows.append(mode_row)
    rows.append([btn("Send Now", f"bcast_send_{bot_id}", "success", "🚀"),
                 btn("Cancel", f"manage_bot_{bot_id}", "danger", "❌")])
    return InlineKeyboardMarkup(rows)


def admin_broadcast_ready_kb(context) -> InlineKeyboardMarkup:
    draft = get_broadcast_draft(context, "admin")
    rows = [button_builder_row(context, admin_broadcast_target())]
    mode_row = broadcast_caption_mode_row(draft, "admin_bcast_capmode")
    if mode_row:
        rows.append(mode_row)
    rows.append([btn("Send Now", "admin_bcast_send", "success", "🚀"),
                 btn("Cancel", "admin_panel", "danger", "❌")])
    return InlineKeyboardMarkup(rows)


def broadcast_selected_ids(context) -> List[str]:
    selected = context.user_data.get("admin_bcast_selected") or []
    return [str(b) for b in selected if b]


def broadcast_target_label(draft: dict) -> str:
    ids = [str(b) for b in (draft.get("target_bots") or []) if b]
    if not ids and draft.get("target_bot"):
        ids = [str(draft["target_bot"])]
    if not ids:
        return "ALL userbots"
    if len(ids) == 1:
        return f"userbot {ids[0]}"
    return f"{len(ids)} userbots ({', '.join(ids[:5])}{'…' if len(ids) > 5 else ''})"


def make_broadcast_draft(extracted: dict, target_bots: Optional[List[str]] = None,
                         target_bot: Optional[str] = None) -> dict:
    return {
        "text": extracted.get("text") or "",
        "media": extracted.get("media_id"),
        "media_type": extracted.get("media_type") or "text",
        "entities_json": extracted.get("entities_json"),
        "file_name": extracted.get("file_name"),
        "mime_type": extracted.get("mime_type"),
        "buttons_json": None,
        "caption_with_buttons": True,
        "target_bots": list(target_bots) if target_bots else None,
        "target_bot": target_bot,
    }


def _broadcast_album_key(scope: str, bot_id: Optional[str], media_group_id) -> str:
    return f"bcast_album_{scope}_{bot_id or 'admin'}_{media_group_id}"


# strong references so fire-and-forget flush tasks are never garbage collected
_PENDING_TASKS: set = set()


def _schedule_broadcast_flush(context, job_key: str, data: dict, when: float = 1.4):
    """Wait a moment for the rest of an album, then save the draft.

    Uses the JobQueue when available and falls back to an asyncio task, so album
    broadcasts keep working even without the job-queue extra.

    IMPORTANT: the live `user_data` dict is carried inside the job data. PTB's job
    context has `user_data = None`, so without this the flush crashed with
    "'NoneType' object has no attribute 'pop'" and the draft was never saved.
    """
    data = dict(data or {})
    if not isinstance(data.get("user_data"), dict):
        user_data = getattr(context, "user_data", None)
        if isinstance(user_data, dict):
            data["user_data"] = user_data

    if getattr(context, "job_queue", None):
        context.user_data[job_key] = context.job_queue.run_once(_flush_broadcast_album_job, when=when, data=data)
        return

    async def _runner():
        try:
            await asyncio.sleep(when)
            await flush_broadcast_album(context, data.get("scope"), data.get("bot_id"),
                                        data.get("chat_id"), data.get("media_group_id"),
                                        user_data=data.get("user_data"))
        except Exception as ex:
            logging.error(f"broadcast album flush task failed: {mask_secrets(ex)}")

    task = asyncio.ensure_future(_runner())
    _PENDING_TASKS.add(task)
    task.add_done_callback(_PENDING_TASKS.discard)
    context.user_data[job_key] = task


async def collect_broadcast_album(context, scope: str, bot_id: Optional[str], msg,
                                  extracted: dict) -> bool:
    """Collect an album (media group) sent while composing a broadcast.

    Telegram delivers every photo/video of an album as a separate update; before this
    only the first one ever reached the draft, so users got a single media instead of
    the whole album with its caption.
    """
    media_group_id = extracted.get("media_group_id")
    if not media_group_id:
        return False
    user_data = getattr(context, "user_data", None)
    if not isinstance(user_data, dict):
        logging.error("broadcast album collect: user_data available nahi hai")
        return False
    key = _broadcast_album_key(scope, bot_id, media_group_id)
    items = user_data.setdefault(key, [])
    first = len(items) == 0
    items.append(make_media_item(extracted))
    if first:
        try:
            await reply_premium_message(msg, f"{pe('📸')} Album mil gaya, process kar raha hoon…",
                                        parse_mode=ParseMode.HTML)
        except Exception:
            pass
    job_key = f"{key}_job"
    old_job = context.user_data.get(job_key)
    if old_job is not None:
        try:
            old_job.schedule_removal()
        except Exception:
            try:
                old_job.cancel()
            except Exception:
                pass
    _schedule_broadcast_flush(context, job_key,
                              {"scope": scope, "bot_id": bot_id, "chat_id": msg.chat_id,
                               "media_group_id": media_group_id})
    return True


async def _flush_broadcast_album_job(context: ContextTypes.DEFAULT_TYPE):
    data = context.job.data or {}
    await flush_broadcast_album(context, data.get("scope"), data.get("bot_id"),
                                data.get("chat_id"), data.get("media_group_id"),
                                user_data=data.get("user_data"))


async def flush_broadcast_album(context, scope: str, bot_id: Optional[str],
                                chat_id, media_group_id, user_data: Optional[dict] = None):
    ctx = _context_with_user_data(context, user_data)
    user_data = getattr(ctx, "user_data", None)
    if not isinstance(user_data, dict):
        logging.error(f"album flush: user_data available nahi hai (scope={scope}, bot={bot_id})")
        return
    key = _broadcast_album_key(scope, bot_id, media_group_id)
    items = user_data.pop(key, None) or []
    user_data.pop(f"{key}_job", None)
    if not items or not chat_id:
        return
    first = items[0]
    draft = {
        "text": first.get("text") or "",
        "media": first.get("media"),
        "media_type": first.get("media_type") or "text",
        "entities_json": first.get("entities_json"),
        "file_name": first.get("file_name"),
        "mime_type": first.get("mime_type"),
        "album": items,
        "buttons_json": None,
        "caption_with_buttons": True,
    }
    if scope == "admin":
        draft["target_bots"] = broadcast_selected_ids(ctx) or None
        draft["target_bot"] = None
        user_data["admin_broadcast_draft"] = draft
        user_data["admin_broadcast_stage"] = "buttons_or_send"
        user_data.pop("admin_broadcast", None)
        kb = admin_broadcast_ready_kb(ctx)
        label = broadcast_target_label(draft)
    else:
        draft["target_bot"] = bot_id
        user_data[f"broadcast_draft_{bot_id}"] = draft
        user_data[f"broadcast_stage_{bot_id}"] = "buttons_or_send"
        kb = user_broadcast_ready_kb(ctx, bot_id, 0)
        label = "your users"
    await send_premium_message(ctx.bot, chat_id,
                               f"{pe('✅')} <b>Album saved</b> ({len(items)} media) — target: {label}\n\n"
                               f"{pe('🔘')} Buttons add karo ya abhi send kar do.\n"
                               f"{pe('📝')} Telegram album par buttons attach nahi karta, "
                               f"isliye buttons album ke turant neeche ek message me jayenge "
                               f"(caption bhi usi ke saath — neeche wale button se badal sakte ho).",
                               parse_mode=ParseMode.HTML, reply_markup=kb)


async def preview_user_broadcast(q, context: ContextTypes.DEFAULT_TYPE, bot_id: str, owner_id: int):
    draft = context.user_data.get(f"broadcast_draft_{bot_id}", {})
    if not draft:
        await safe_edit_message_text(q, f"{pe('❌')} No draft found.", parse_mode=ParseMode.HTML, reply_markup=bot_management_kb(bot_id, owner_id))
        return
    try:
        await send_draft_message(context, owner_id, draft, markup=buttons_to_markup(draft.get("buttons_json")))
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
    reqs = list(dict.fromkeys(db.get_requesters_for_bot(bot_id) or []))
    if not reqs:
        await safe_edit_message_text(q, f"{pe('❌')} No users to broadcast to.", parse_mode=ParseMode.HTML, reply_markup=bot_management_kb(bot_id, owner_id))
        return
    await safe_edit_message_text(q, f"{pe('✈️')} Broadcasting...", parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup([[btn("Back", f"manage_bot_{bot_id}", "primary", "🔙")]]))
    markup = buttons_to_markup(draft.get("buttons_json"))
    sent = 0
    fail = 0
    for r in reqs:
        try:
            await send_draft_message(context, r, draft, markup=markup)
            db.mark_reachable(bot_id, r)
            sent += 1
        except Forbidden:
            db.mark_unreachable(bot_id, r)
            fail += 1
        except Exception as ex:
            logging.error(f"user broadcast delivery failed for {r}: {ex}")
            fail += 1
        if (sent + fail) % 30 == 0:
            try:
                await q.message.edit_text(f"{pe('✈️')} Broadcasting... Sent: {sent}, Failed: {fail}", parse_mode=ParseMode.HTML)
            except Exception:
                pass
    await safe_edit_message_text(q, UIFormatter.broadcast_confirm(sent, fail), parse_mode=ParseMode.HTML, reply_markup=bot_management_kb(bot_id, owner_id))
    context.user_data.pop(f"broadcast_draft_{bot_id}", None)


async def ensure_broadcast_subscription(bot_id: str, bot_token: Optional[str] = None,
                                        owner_id: Optional[int] = None) -> bool:
    """Agar userbot ke paas koi active subscription nahi hai to broadcast ke liye
    1 din ka Basic khud se add karo aur bot ko (best effort) chalu kar do."""
    added = db.grant_broadcast_subscription(bot_id, days=1, sub_type="Basic")
    if not added:
        return False
    try:
        if bot_token and bot_id not in user_bot_applications:
            if await start_user_bot(bot_token, bot_id, owner_id or 0):
                db.set_user_bot_active(bot_id, True)
            else:
                logging.warning(f"{bot_id}: trial subscription added but bot start nahi ho paya")
    except Exception as ex:
        logging.error(f"auto-start after trial subscription failed for {bot_id}: {mask_secrets(ex)}")
    return True


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
            if not await start_user_bot(bot["bot_token"], bot_id, bot["user_id"]):
                failed += 1
                continue
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

    channel_configs = cfg.get("channel_configs", {})
    chan_lines = []
    for cid, enabled in channel_configs.items():
        chan_lines.append(f"  • <code>{cid}</code> → {'🟢 ON' if enabled else '🔴 OFF'}")
    chan_text = "\n".join(chan_lines) if chan_lines else "  (Global setting applies to all)"

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
        enabled = channel_configs.get(cid, True)
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
        # Easy button builder (➕ Add Button wizard) - handled before everything else
        if data and data.startswith("bwz_"):
            await handle_button_wizard_callback(q, context, data)
            return

        if data == "main_menu":
            user = q.from_user
            await safe_edit_message_text(q, UIFormatter.main_menu(user.first_name), parse_mode=ParseMode.HTML, reply_markup=main_menu_kb(uid))
            for key in list(context.user_data.keys()):
                if key.startswith(("broadcast_stage_", "broadcast_draft_", "adding_channel_", "setting_message_")):
                    context.user_data.pop(key, None)
            return

        if data == "add_new_bot":
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
            await safe_edit_message_text(q, f"<blockquote>{pp('🤖')} <b>MANAGE BOT</b></blockquote>\n\nBot: @{bot_data['bot_username']}\nBot ID: {bot_id}", parse_mode=ParseMode.HTML, reply_markup=bot_management_kb(bot_id, uid))
            return

        # ---- userbot manage panel opened from the MAIN bot ----
        if data and data.startswith(USERBOT_PANEL_PREFIXES):
            panel_bot = resolve_managed_bot_id(uid, data)
            if not panel_bot:
                return
            if not is_bot_owner(panel_bot, uid):
                await safe_edit_message_text(q, f"{pe('❌')} You don't have permission to manage this bot.", parse_mode=ParseMode.HTML)
                return
            if data.startswith(READONLY_PANEL_PREFIXES):
                # read-only panels work from here too
                await user_bot_callback(update, context, panel_bot, uid)
            else:
                await show_manage_from_bot_help(q, panel_bot)
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

        if data == "admin_panel":
            if not is_admin(uid):
                await safe_edit_message_text(q, f"{pe('❌')} Not authorized", parse_mode=ParseMode.HTML)
                return
            # leaving the panel drops any half-finished broadcast (avoids a stale draft)
            for stale_key in ("admin_broadcast", "admin_broadcast_stage", "admin_broadcast_draft",
                              "admin_bcast_selected", "admin_broadcast_target"):
                context.user_data.pop(stale_key, None)
            context.user_data.pop(BUTTON_WIZARD_KEY, None)
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
            kb = InlineKeyboardMarkup([
                button_builder_row(context, {"kind": "leave_msg", "idx": idx,
                                             "back_cb": "admin_leave_msgs"}),
                [btn("Back", "admin_leave_msgs", "primary", "🔙")],
            ])
            await safe_edit_message_text(q,
                f"<blockquote>{pp('🔘')} <b>BUTTONS FOR MESSAGE #{idx+1}</b></blockquote>\n\n"
                "<b>Add Button</b> = easy tarika (naam → link → same row / new row)\n"
                "<b>Paste Many</b> = bulk format (premium emoji supported)\n"
                "<code>Button Label|https://link</code>\n"
                "<code>Label1|https://url1 || Label2|https://url2</code>",
                parse_mode=ParseMode.HTML, reply_markup=kb)
            return

        if data == "admin_leave_channels":
            if not is_admin(uid):
                return
            await safe_edit_message_text(q,
                f"<blockquote>{pp('⚙️')} <b>PER-CHANNEL LEAVE RECOVERY</b></blockquote>\n\n"
                "Toggle leave recovery ON/OFF for each channel.\n"
                "🟢 = Leave recovery active for this channel\n"
                "🔴 = Leave recovery disabled for this channel\n\n"
                "<i>By default all channels are ON.</i>",
                parse_mode=ParseMode.HTML, reply_markup=leave_recovery_channels_kb())
            return

        if data.startswith("admin_leave_chan_toggle_"):
            if not is_admin(uid):
                return
            chan_id = data.replace("admin_leave_chan_toggle_", "")
            cfg = db.get_leave_recovery_config()
            channel_configs = cfg.get("channel_configs", {})
            current = channel_configs.get(chan_id, True)
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
                [btn("Select UserBots (multi)", "admin_bcast_target_select", "primary", "🎯")],
                [btn("All UserBots", "admin_bcast_target_all", "success", "🌐")],
                [btn("Back", "admin_panel", "primary", "🔙")],
            ]))
            return

        if data == "admin_bcast_target_all":
            if not is_admin(uid):
                return
            context.user_data["admin_broadcast"] = True
            context.user_data["admin_broadcast_target"] = None
            context.user_data.pop("admin_bcast_selected", None)
            await safe_edit_message_text(q, f"<blockquote>{pp('✈️')} <b>BROADCAST TO ALL</b></blockquote>\n\nSend text, media ya album (4-5 photo/video + caption) — buttons baad me add kar sakte ho.", parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup([[btn("Back", "admin_panel", "primary", "🔙")]]))
            return

        if data == "admin_bcast_target_select":
            await render_admin_bcast_targets(q, context)
            return

        if data.startswith("admin_bcast_tog_"):
            if not is_admin(uid):
                return
            bot_id = data.replace("admin_bcast_tog_", "")
            selected = broadcast_selected_ids(context)
            if bot_id in selected:
                selected = [b for b in selected if b != bot_id]
            else:
                selected.append(bot_id)
            context.user_data["admin_bcast_selected"] = selected
            await render_admin_bcast_targets(q, context)
            return

        if data == "admin_bcast_sel_all":
            if not is_admin(uid):
                return
            context.user_data["admin_bcast_selected"] = [str(b["bot_id"]) for b in (db.get_all_user_bots() or [])]
            await render_admin_bcast_targets(q, context)
            return

        if data == "admin_bcast_sel_none":
            if not is_admin(uid):
                return
            context.user_data["admin_bcast_selected"] = []
            await render_admin_bcast_targets(q, context)
            return

        if data == "admin_bcast_sel_done":
            if not is_admin(uid):
                return
            selected = broadcast_selected_ids(context)
            if not selected:
                await safe_edit_message_text(q, f"{pe('⚠️')} Kam se kam ek userbot select karo.", parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup([[btn("Back", "admin_bcast_target_select", "primary", "🔙")]]))
                return
            context.user_data["admin_broadcast"] = True
            context.user_data["admin_broadcast_target"] = None
            await safe_edit_message_text(q,
                f"<blockquote>{pp('✈️')} <b>BROADCAST → {len(selected)} USERBOT(S)</b></blockquote>\n\n"
                f"Ab message bhejo — text, photo, video, document ya album (4-5 media + caption).\n\n"
                f"{pe('🔘')} Buttons add karne ka option draft save hone ke baad milega.\n"
                f"{pe('⚠️')} Jis userbot ka subscription nahi hai, usme 1 din ka Basic khud add ho jayega.",
                parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup([[btn("Back", "admin_bcast_target_select", "primary", "🔙")]]))
            return

        if data.startswith("admin_bcast_pick_"):
            if not is_admin(uid):
                return
            bot_id = data.replace("admin_bcast_pick_", "")
            context.user_data["admin_broadcast"] = True
            context.user_data["admin_broadcast_target"] = bot_id
            context.user_data["admin_bcast_selected"] = [bot_id]
            await safe_edit_message_text(q, f"<blockquote>{pp('✈️')} <b>BROADCAST TO USERBOT {bot_id}</b></blockquote>\n\nSend text or media to broadcast.", parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup([[btn("Back", "admin_panel", "primary", "🔙")]]))
            return

        if data == "admin_bcast_add_btns":
            if not is_admin(uid):
                return
            draft = context.user_data.get("admin_broadcast_draft", {})
            if not draft:
                await safe_edit_message_text(q, f"{pe('❌')} No draft found.", parse_mode=ParseMode.HTML, reply_markup=admin_kb())
                return
            tid = register_button_target(context, admin_broadcast_target())
            await start_button_wizard(q, context, tid)
            return

        if data == "admin_bcast_capmode":
            if not is_admin(uid):
                return
            draft = get_broadcast_draft(context, "admin")
            if not draft:
                await q.answer("Pehle album/message bhejo", show_alert=True)
                return
            draft["caption_with_buttons"] = not draft.get("caption_with_buttons", True)
            save_broadcast_draft(context, "admin", None, draft)
            await q.answer("Caption " + ("buttons ke saath (album ke neeche ek message me)" if draft["caption_with_buttons"] else "album par hi rahega"))
            try:
                await q.edit_message_reply_markup(reply_markup=admin_broadcast_ready_kb(context))
            except Exception as ex:
                logging.warning(f"caption mode kb update failed: {ex}")
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
                            if not await start_user_bot(bot_data["bot_token"], bot_id, bot_data["user_id"]):
                                await safe_edit_message_text(q, f"{pe('❌')} Bot start nahi ho paya - token invalid lagta hai (naya token add karo).", parse_mode=ParseMode.HTML, reply_markup=admin_kb())
                                return
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
        await send_draft_message(context, uid, draft, markup=buttons_to_markup(draft.get("buttons_json")))
    except Exception as ex:
        await safe_edit_message_text(q, f"{pe('❌')} Preview failed: {str(ex)}", parse_mode=ParseMode.HTML, reply_markup=admin_kb())
        return
    await safe_edit_message_text(q, f"{pe('✅')} Preview sent above.\n{pe('✈️')} Confirm broadcast to <b>{broadcast_target_label(draft)}</b>?",
                                 parse_mode=ParseMode.HTML, reply_markup=confirm_kb("admin_bcast_confirm", "admin_panel"))


async def send_admin_broadcast(q, context: ContextTypes.DEFAULT_TYPE):
    draft = context.user_data.get("admin_broadcast_draft", {})
    selected = [str(b) for b in ((draft.get("target_bots") or broadcast_selected_ids(context))) if b]
    if not selected and draft.get("target_bot"):
        selected = [str(draft["target_bot"])]
    for key in ["admin_broadcast", "admin_broadcast_stage", "admin_broadcast_target", "admin_bcast_selected"]:
        context.user_data.pop(key, None)
    if not draft:
        await safe_edit_message_text(q, f"{pe('❌')} No draft to send.", parse_mode=ParseMode.HTML, reply_markup=admin_kb())
        return
    all_bots = db.get_all_user_bots() or []
    bots = [b for b in all_bots if str(b["bot_id"]) in selected] if selected else all_bots
    if not bots:
        await safe_edit_message_text(q, f"{pe('❌')} Selected userbot(s) not found.", parse_mode=ParseMode.HTML, reply_markup=admin_kb())
        context.user_data.pop("admin_broadcast_draft", None)
        return
    await safe_edit_message_text(q, f"{pe('✈️')} Admin broadcast started — {len(bots)} userbot(s)…", parse_mode=ParseMode.HTML, reply_markup=admin_kb())
    markup = buttons_to_markup(draft.get("buttons_json"))
    total_sent = 0
    total_fail = 0
    auto_subs: List[str] = []
    per_bot_lines: List[str] = []
    for bot in bots:
        bot_id = bot["bot_id"]
        # Agar subscription nahi hai to 1 din ka Basic khud se add karo, warna broadcast
        # us userbot ke users tak kabhi nahi pahunchta.
        if await ensure_broadcast_subscription(bot_id, bot.get("bot_token"), bot.get("user_id")):
            auto_subs.append(bot_id)
        if bot_id in user_bot_applications:
            bot_instance = user_bot_applications[bot_id].bot
        else:
            try:
                bot_instance = Bot(token=bot["bot_token"])
            except Exception as ex:
                logging.error(f"admin broadcast: bad token for {bot_id}: {ex}")
                per_bot_lines.append(f"❌ @{bot.get('bot_username') or bot_id}: token error")
                continue
        recipients = list(dict.fromkeys(db.get_requesters_for_bot(bot_id) or []))
        bot_sent = 0
        bot_fail = 0
        for r in recipients:
            try:
                await send_draft_message(bot_instance, r, draft, markup=markup)
                bot_sent += 1
            except Forbidden:
                bot_fail += 1
            except Exception as ex:
                logging.error(f"admin broadcast delivery failed for {r} via {bot_id}: {ex}")
                bot_fail += 1
        total_sent += bot_sent
        total_fail += bot_fail
        per_bot_lines.append(f"• @{bot.get('bot_username') or bot_id}: {bot_sent} sent, {bot_fail} failed"
                             + ("  (1d Basic auto-added)" if bot_id in auto_subs else ""))
    summary = "\n".join(per_bot_lines[-15:])
    auto_line = ""
    if auto_subs:
        auto_line = f"\n{pp('⭐️')} 1-day Basic auto-added: {len(auto_subs)} userbot(s)"
    await safe_edit_message_text(q,
                                 f"<blockquote>{pp('✅')} <b>ADMIN BROADCAST COMPLETE</b></blockquote>\n\n"
                                 f"{pp('📤')} Target: {broadcast_target_label(draft)}\n"
                                 f"{pp('✅')} Sent: {total_sent}\n"
                                 f"{pp('❌')} Failed: {total_fail}{auto_line}\n\n{summary}",
                                 parse_mode=ParseMode.HTML, reply_markup=admin_kb())
    context.user_data.pop("admin_broadcast_draft", None)


async def render_admin_bcast_targets(q, context: ContextTypes.DEFAULT_TYPE):
    """Multi-select list: har userbot ko tick karke Done dabao."""
    if not is_admin(q.from_user.id):
        return
    bots = db.get_all_user_bots() or []
    if not bots:
        await safe_edit_message_text(q, f"{pe('❌')} No userbots found.", parse_mode=ParseMode.HTML, reply_markup=admin_kb())
        return
    selected = set(broadcast_selected_ids(context))
    rows: List[List[InlineKeyboardButton]] = [[
        btn("Select All", "admin_bcast_sel_all", "primary", "✅"),
        btn("Clear", "admin_bcast_sel_none", "danger", "🗑"),
    ]]
    for bot in bots:
        bot_id = str(bot["bot_id"])
        sub = db.get_active_subscription(bot_id)
        if sub:
            try:
                expiry = make_aware(sub["expiry_date"]) if isinstance(sub["expiry_date"], datetime) else sub["expiry_date"]
                days_left = max((expiry - now_aware()).days, 0)
            except Exception:
                days_left = 0
            status = f"{sub['subscription_type']} {days_left}d"
        else:
            status = "no sub → 1d Basic auto"
        mark = "☑️" if bot_id in selected else "⬜"
        label = f"{mark} @{bot.get('bot_username') or bot_id} • {status}"
        rows.append([btn(label[:60], f"admin_bcast_tog_{bot_id}", "primary", "🤖")])
    rows.append([btn(f"Done ({len(selected)} selected)", "admin_bcast_sel_done", "success", "✅")])
    rows.append([btn("Back", "admin_broadcast", "primary", "🔙")])
    await safe_edit_message_text(q,
                                 f"<blockquote>{pp('✈️')} <b>SELECT USERBOTS</b></blockquote>\n\n"
                                 "Jitne userbots ko select karna hai unhe tick karo (ek-ek karke, ya Select All), "
                                 "phir <b>Done</b> dabao.\n\n"
                                 f"{pe('ℹ️')} Jis userbot ka subscription nahi hai, usme 1 din ka Basic khud add ho jayega.\n\n"
                                 f"<b>Selected:</b> {len(selected)}",
                                 parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup(rows))


# ================= MAIN MESSAGE HANDLER =================
async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not user:
        return
    msg = update.message
    if not msg:
        return

    # Easy button builder (➕ Add Button wizard) has priority over everything else
    if await handle_button_wizard_message(msg, context):
        return

    if context.user_data.get("waiting_token") and not is_admin(user.id):
        token = msg.text.strip()
        if ":" in token and len(token) > 10:
            try:
                test_bot = Bot(token=token)
                bot_info = await test_bot.get_me()
                bot_id = db.add_user_bot(user.id, token, bot_info.username)
                context.user_data.pop("waiting_token", None)
                await reply_premium_message(msg, f"<blockquote>{pp('✅')} <b>BOT ADDED SUCCESSFULLY</b></blockquote>\n\n{pp('🤖')} @{bot_info.username}\nBot ID: <code>{bot_id}</code>\n\n{pp('⚠️')} You need a subscription to activate your bot.\n{pp('📞')} Contact {ADMIN_USERNAME} to subscribe.", parse_mode=ParseMode.HTML, reply_markup=main_menu_kb(user.id))
            except Exception as ex:
                await reply_premium_message(msg, f"{pe('❌')} Invalid token or bot error: {ex}\n\nPlease try again.", parse_mode=ParseMode.HTML)
        else:
            await reply_premium_message(msg, f"{pe('❌')} Invalid token format. Please send the correct BotFather token.", parse_mode=ParseMode.HTML)
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
                started = await start_user_bot(bot["bot_token"], bot["bot_id"], bot["user_id"])
                if started is False:
                    raise RuntimeError("token invalid - naya token add karo")
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
        btn_json = buttons_json_from_text(msg.text or "", msg.entities or msg.caption_entities)
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
        # An album arrives as many updates - collect all of them first
        if await collect_broadcast_album(context, "admin", None, msg, extracted):
            return
        if not (msg.text or msg.caption or extracted.get("media_id")):
            await reply_premium_message(msg, f"{pe('⚠️')} Text, photo, video, document ya album bhejo.", parse_mode=ParseMode.HTML)
            return
        draft = make_broadcast_draft(extracted,
                                     target_bots=broadcast_selected_ids(context) or None,
                                     target_bot=context.user_data.get("admin_broadcast_target"))
        context.user_data["admin_broadcast_draft"] = draft
        context.user_data["admin_broadcast_stage"] = "buttons_or_send"
        context.user_data.pop("admin_broadcast", None)
        await reply_premium_message(msg,
            f"{pe('✅')} <b>Broadcast draft saved.</b>\n"
            f"{pe('📤')} Target: {broadcast_target_label(draft)}\n\n"
            f"{pe('🔘')} <b>Add Button</b> se buttons banao, ya <b>Send Now</b> dabao.",
            parse_mode=ParseMode.HTML, reply_markup=admin_broadcast_ready_kb(context))
        return

    if context.user_data.get("admin_broadcast_stage") == "await_buttons" and is_admin(user.id):
        draft = context.user_data.get("admin_broadcast_draft", {})
        btn_json = buttons_json_from_text(msg.text or "", msg.entities or msg.caption_entities)
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
    err_str = mask_secrets(str(context.error))
    if "Message is not modified" in err_str or "Query is too old" in err_str:
        return
    if "Forbidden" in err_str:
        logging.warning(f"Forbidden (bot blocked / no rights): {err_str}")
        return
    if "NetworkError" in err_str or "ReadError" in err_str:
        logging.warning(f"Network error (will retry later): {err_str}")
        return
    # Real bug: full traceback log karo, warna sirf "Update None caused error xxx"
    # dikhta hai aur debug karna mushkil ho jata hai.
    logging.error(f"Update {update} caused error {err_str}", exc_info=context.error)


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
async def start_bots_on_boot():
    """Boot par pehle se saved userbots ko chalu karo (isolated - koi bhi error
    main bot ko nahi rokta)."""
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
                if not await start_user_bot(bot["bot_token"], bot["bot_id"], bot["user_id"]):
                    continue
                db.set_user_bot_active(bot["bot_id"], True)
                logging.info(f"{pp('✅')} Started user bot @{bot['bot_username']} for {bot['bot_id']}")
            except Exception as ex:
                logging.error(f"{pp('❌')} Failed to start user bot {bot['bot_id']}: {mask_secrets(ex)}")
    else:
        logging.info("No user bots found in database")


async def main():
    logging.basicConfig(format=LOG_FORMAT, level=logging.INFO)
    install_log_masking()
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

    try:
        await start_bots_on_boot()
    except Exception as ex:  # userbot problem se main bot kabhi rukna nahi chahiye
        logging.error(f"{pp('❌')} Userbot startup error (main bot phir bhi chalu hoga): {mask_secrets(ex)}")

    app = ApplicationBuilder().token(MAIN_BOT_TOKEN).concurrent_updates(True).request(_tuned_request()).build()

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
    except (InvalidToken, Forbidden) as ex:
        # Token revoked/leaked -> restart loop me bot ko jalao mat, seedha saaf message do.
        logging.error(f"{pp('❌')} MAIN BOT TOKEN reject ho gaya ({mask_secrets(ex)}).\n"
                      "Ye token leak ho chuka hai (public repo/chat), isliye Telegram ne revoke kar diya.\n"
                      f"{MAIN_BOT_TOKEN_HINT}")
        raise SystemExit(2)
    logging.info(f"{pp('✅')} Main bot started successfully")
    await flush_token_failures(app.bot)

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


if __name__ == "__main__":
    if not MAIN_BOT_TOKEN:
        logging.basicConfig(format=LOG_FORMAT, level=logging.INFO)
        install_log_masking()
        logging.error(f"{pp('❌')} MAIN_BOT_TOKEN set nahi hai!\n{MAIN_BOT_TOKEN_HINT}")
        raise SystemExit(1)

    while True:
        try:
            asyncio.run(main())
        except KeyboardInterrupt:
            logging.info(f"{pp('🛑')} Stopped by user")
            break
        except SystemExit as ex:
            logging.error(f"{pp('❌')} Bot band (exit code {ex.code}) - upar wala message padho, "
                          "config theek karke ./start dobara chalao")
            # Non-zero exit: launcher/monitoring ko pata chale ki config galat hai
            raise SystemExit(ex.code if isinstance(ex.code, int) and ex.code else 1)
        except Exception as ex:
            logging.exception(f"{pp('❌')} Fatal error: {mask_secrets(ex)}")
            logging.info(f"{pp('🔄')} Restarting in 10 seconds...")
            time.sleep(10)