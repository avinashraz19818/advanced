"""advanced.py ka verification harness (bot ke bahar chalta hai).

* psycopg2 ko stub karta hai, isliye PostgreSQL ke bina bhi advanced.py import hota hai
* Fake DB / Bot / Context / Query use karta hai, aur jahan zaroori ho wahan **asli**
  python-telegram-bot objects (JobQueue, CallbackContext.from_job) chalata hai
  - isi tarah "job context me user_data None" wala bug pakda gaya tha
* Chalane ka tarika:
      python -m venv /tmp/v && /tmp/v/bin/pip install "python-telegram-bot[job-queue]"
      /tmp/v/bin/python tests/verify_advanced.py
  (PTB 22.x aur 21.x dono par pass hona chahiye)
"""
import asyncio
import io
import logging
import os
import re
import sys
import tempfile
import types
from contextlib import redirect_stdout
from datetime import timedelta
from types import SimpleNamespace

# ---------------------------------------------------------------- psycopg2 stub
psycopg2 = types.ModuleType("psycopg2")


class DummyCursor:
    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def execute(self, *a, **k):
        return None

    def fetchone(self):
        return None

    def fetchall(self):
        return []


class DummyConn:
    autocommit = True

    def cursor(self, *a, **k):
        return DummyCursor()

    def close(self):
        pass


psycopg2.connect = lambda *a, **k: DummyConn()
extras = types.ModuleType("psycopg2.extras")


class RealDictCursor:
    pass


class Json:
    def __init__(self, value):
        self.value = value


extras.RealDictCursor = RealDictCursor
extras.Json = Json
psycopg2.extras = extras
sys.modules["psycopg2"] = psycopg2
sys.modules["psycopg2.extras"] = extras

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO)
import advanced as A  # noqa: E402
from telegram import InlineKeyboardMarkup  # noqa: E402

try:  # user-account mode ke error classes (telethon optional hai)
    from telethon.errors import (SessionPasswordNeededError, PhoneCodeInvalidError,
                                 PhoneCodeExpiredError, PasswordHashInvalidError)
    HAS_TELETHON = True
except Exception:  # pragma: no cover
    HAS_TELETHON = False

PASS, FAIL = [], []
_silent = io.StringIO()
with redirect_stdout(_silent):
    A.logging.getLogger().setLevel(A.logging.CRITICAL)


def check(name, cond, extra=""):
    (PASS if cond else FAIL).append(name)
    print(("  ok   " if cond else "  FAIL ") + name + (f"  -> {extra}" if (extra and not cond) else ""))


_SHARED_LOOP = None


def _loop():
    global _SHARED_LOOP
    if _SHARED_LOOP is None or _SHARED_LOOP.is_closed():
        _SHARED_LOOP = asyncio.new_event_loop()
        asyncio.set_event_loop(_SHARED_LOOP)
    return _SHARED_LOOP


def run(coro):
    return _loop().run_until_complete(coro)


# ---------------------------------------------------------------- fakes
class EmojiEntity:
    def __init__(self, offset, length, emoji_id, etype="custom_emoji"):
        self.type = etype
        self.offset = offset
        self.length = length
        self.custom_emoji_id = emoji_id


class FakeSent:
    message_id = 4242

    async def delete(self):
        return True


class RemoteFile:
    def __init__(self, file_path):
        self.file_path = file_path


class FakeBot:
    def __init__(self, name="fake", token=None, file_path="photos/file_1.jpg"):
        self.name = name
        self.token = token or f"token_{name}"
        self.file_path = file_path
        self.get_file_calls = []
        self.calls = []

    async def get_file(self, file_id, **kw):
        self.get_file_calls.append(file_id)
        return RemoteFile(self.file_path)

    def _log(self, kind, chat_id, media, kw):
        self.calls.append((kind, chat_id, media, kw))

    async def send_message(self, chat_id, text, **kw):
        self._log("send_message", chat_id, text, kw)
        return FakeSent()

    async def send_photo(self, chat_id, media, **kw):
        self._log("send_photo", chat_id, media, kw)
        return FakeSent()

    async def send_video(self, chat_id, media, **kw):
        self._log("send_video", chat_id, media, kw)
        return FakeSent()

    async def send_document(self, chat_id, media, **kw):
        self._log("send_document", chat_id, media, kw)
        return FakeSent()

    async def send_audio(self, chat_id, media, **kw):
        self._log("send_audio", chat_id, media, kw)
        return FakeSent()

    async def send_animation(self, chat_id, media, **kw):
        self._log("send_animation", chat_id, media, kw)
        return FakeSent()

    async def send_voice(self, chat_id, media, **kw):
        self._log("send_voice", chat_id, media, kw)
        return FakeSent()

    async def send_sticker(self, chat_id, media, **kw):
        self._log("send_sticker", chat_id, media, kw)
        return FakeSent()

    async def send_media_group(self, chat_id, media, **kw):
        self._log("send_media_group", chat_id, media, kw)
        return [FakeSent() for _ in media]


class FlakyBot(FakeBot):
    """Telegram jaisa reject: premium emoji document / styled buttons allowed nahi."""

    async def send_photo(self, chat_id, media, **kw):
        markup = kw.get("reply_markup")
        if markup is not None and A.markup_has_icons(markup):
            raise A.BadRequest("Bad Request: Document_invalid")
        if "tg-emoji" in str(kw.get("caption") or ""):
            raise A.BadRequest("Bad Request: can't parse entities")
        return await super().send_photo(chat_id, media, **kw)

    async def send_message(self, chat_id, text, **kw):
        if "tg-emoji" in str(text):
            raise A.BadRequest("Bad Request: Document_invalid")
        return await super().send_message(chat_id, text, **kw)


class CantInitiateBot(FakeBot):
    """Bot ne user ko /start nahi kiya - Telegram DM shuru karne hi nahi deta."""

    async def send_message(self, chat_id, text, **kw):
        self._log("send_message", chat_id, text, kw)
        if int(chat_id) == getattr(self, "allowed_chat", None):
            return FakeSent()
        raise A.Forbidden("Forbidden: bot can't initiate conversation with a user")

    async def send_photo(self, chat_id, media, **kw):
        self._log("send_photo", chat_id, media, kw)
        raise A.Forbidden("Forbidden: bot can't initiate conversation with a user")

    async def send_media_group(self, chat_id, media, **kw):
        self._log("send_media_group", chat_id, media, kw)
        raise A.Forbidden("Forbidden: bot can't initiate conversation with a user")


class BlockedBot(FakeBot):
    """User ne bot block kiya."""

    async def send_media_group(self, chat_id, media, **kw):
        self._log("send_media_group", chat_id, media, kw)
        raise A.Forbidden("Forbidden: bot was blocked by the user")

    async def send_photo(self, chat_id, media, **kw):
        self._log("send_photo", chat_id, media, kw)
        raise A.Forbidden("Forbidden: bot was blocked by the user")

    async def send_message(self, chat_id, text, **kw):
        self._log("send_message", chat_id, text, kw)
        raise A.Forbidden("Forbidden: bot was blocked by the user")


class PerUserBot(FakeBot):
    """Har recipient ke liye alag natija (broadcast robustness)."""

    def __init__(self, results, **kw):
        super().__init__(**kw)
        self.results = results

    def _maybe_fail(self, chat_id):
        kind = self.results.get(chat_id, "ok")
        if kind == "blocked":
            raise A.Forbidden("Forbidden: bot was blocked by the user")
        if kind == "chat_not_found":
            raise A.BadRequest("Bad Request: chat not found")
        if kind == "bad_media":
            raise A.BadRequest("Bad Request: wrong file identifier/http url specified")

    async def send_media_group(self, chat_id, media, **kw):
        self._log("send_media_group", chat_id, media, kw)
        self._maybe_fail(chat_id)
        return [FakeSent() for _ in media]

    async def send_message(self, chat_id, text, **kw):
        self._log("send_message", chat_id, text, kw)
        kind = self.results.get(chat_id, "ok")
        if kind == "blocked":
            raise A.Forbidden("Forbidden: bot was blocked by the user")
        if kind == "chat_not_found":
            raise A.BadRequest("Bad Request: chat not found")
        return FakeSent()   # text send media error se fail nahi hota

    async def send_photo(self, chat_id, media, **kw):
        self._log("send_photo", chat_id, media, kw)
        self._maybe_fail(chat_id)
        return FakeSent()


class FakeCtx:
    def __init__(self):
        self.bot = FakeBot()
        self.user_data = {}
        self.job_queue = None


class FakeQuery:
    def __init__(self, ctx, uid=999):
        self.context = ctx
        self.data = ""
        self.from_user = SimpleNamespace(id=uid, first_name="Tester", username="tester")
        self.message = SimpleNamespace(chat_id=uid)
        self.edits = []
        self.markup_edits = []
        self.answers = []

    async def answer(self, text=None, **kw):
        self.answers.append(text)
        return True

    async def edit_message_text(self, text, **kw):
        self.edits.append((text, kw))
        return FakeSent()

    async def edit_message_reply_markup(self, reply_markup=None, **kw):
        self.markup_edits.append(reply_markup)
        return FakeSent()


class FakeMsg:
    def __init__(self, text=None, entities=None, chat_id=999, caption=None, from_user=None):
        self.text = text
        self.from_user = from_user if from_user is not None else SimpleNamespace(
            id=chat_id, first_name="Tester", last_name="", username="tester", is_bot=False)
        self.caption = caption
        self.entities = entities or []
        self.caption_entities = []
        self.chat_id = chat_id
        self.message_id = 77
        self.replies = []
        self.media_group_id = None
        self.deleted = False
        self.reply_to_message = None
        # PTB Message ke media fields (jab media nahi hota to None hote hain)
        self.photo = None
        self.video = None
        self.document = None
        self.animation = None
        self.audio = None
        self.voice = None
        self.video_note = None
        self.sticker = None
        self.contact = None
        self.location = None
        self.poll = None
        self.forward_origin = None
        self.forward_from_chat = None
        self.caption = caption
        self.caption_entities = []
        self.chat = SimpleNamespace(id=chat_id, type="private", title=None, username=None)

    def __getattr__(self, item):
        # PTB Message me har optional field hoti hai (unset = None) - isliye jo field
        # harness me nahi likhi, uske liye None do (AttributeError nahi).
        if item.startswith("__"):
            raise AttributeError(item)
        return None

    async def delete(self):
        self.deleted = True
        return True

    async def reply_text(self, text, **kw):
        self.replies.append((text, kw))
        return FakeSent()


_ALLOWED_TAGS = {"b", "strong", "i", "em", "u", "ins", "s", "strike", "del", "span",
                 "tg-spoiler", "tg-emoji", "a", "code", "pre", "blockquote", "br", "tg-mention"}
_STRAY_TAG_RE = re.compile(r"<(?!/?(" + "|".join(sorted(_ALLOWED_TAGS)) + r")\b)[^>]*>")


class HtmlStrictMsg(FakeMsg):
    """FakeMsg jo Telegram jaisa HTML check karta hai (unsupported tag -> BadRequest)."""

    async def reply_text(self, text, **kw):
        if kw.get("parse_mode") == "HTML" or kw.get("parse_mode") == A.ParseMode.HTML:
            stray = _STRAY_TAG_RE.search(text or "")
            if stray:
                raise A.BadRequest(
                    "Can't parse entities: unsupported start tag "
                    f"\"{stray.group(0)[1:-1].split()[0]}\" at byte offset {stray.start()}")
        return await super().reply_text(text, **kw)


class FakeDB:
    def __init__(self):
        self.messages = {}
        self.subs = {}
        self.user_bots = []
        self.leave = {"messages": []}
        self.reachable_calls = []
        self.unreachable_calls = []
        self.gone_calls = []
        self.active_calls = []

    # --- messages
    def get_message_by_id(self, mid):
        return self.messages.get(int(mid))

    def update_message_buttons(self, mid, payload):
        self.messages.setdefault(int(mid), {"id": int(mid), "bot_id": "b1"})["buttons_json"] = payload

    def add_message(self, *a, **k):
        self.messages[len(self.messages) + 1] = {"id": len(self.messages) + 1}
        return len(self.messages)

    def save_user_emoji_map(self, *a, **k):
        pass

    # --- leave recovery
    def get_leave_recovery_config(self):
        return self.leave

    def set_leave_recovery_config(self, cfg):
        self.leave = cfg

    def add_leave_recovery_message(self, *a, **k):
        self.leave.setdefault("sent", []).append(a)

    def get_pending_leave_recovery_messages(self, bot_id, user_id, target_channel_id):
        self.leave.setdefault("pending_queries", []).append((bot_id, user_id, target_channel_id))
        return []

    def mark_leave_recovery_deleted(self, row_id):
        pass

    def get_channel_owner_data(self, channel_id, bot_id):
        return {"channel_id": channel_id, "bot_id": bot_id, "auto_approve": 1,
                "channel_title": "Test Channel", "channel_username": None,
                "welcome_message": None, "welcome_media_id": None, "welcome_media_type": None}

    # --- subscriptions
    def get_subscription_for_bot(self, bot_id):
        return self.subs.get(bot_id)

    def get_active_subscription(self, bot_id):
        return self.subs.get(bot_id)

    def has_active_subscription(self, bot_id):
        return bot_id in self.subs

    def add_subscription_for_bot(self, bot_id, sub_type, days):
        self.subs[bot_id] = {"subscription_type": sub_type,
                             "expiry_date": A.now_aware() + timedelta(days=days), "max_channels": 1}

    def grant_broadcast_subscription(self, bot_id, days=1, sub_type="Basic"):
        if bot_id in self.subs:
            return False
        self.add_subscription_for_bot(bot_id, sub_type, days)
        return True

    # --- bots / users
    def get_all_user_bots(self):
        return self.user_bots

    def get_user_bots_by_owner(self, user_id):
        return [b for b in self.user_bots if int(b.get("user_id", 0)) == int(user_id)]

    def get_user_bot(self, bot_id):
        for b in self.user_bots:
            if b["bot_id"] == bot_id:
                return b
        return None

    def set_user_bot_active(self, bot_id, active):
        self.active_calls.append((bot_id, bool(active)))

    def get_user(self, user_id):
        return getattr(self, "users", {}).get(int(user_id))

    def add_user(self, *a, **k):
        pass

    def remember_user_entity(self, bot_id, user_id, access_hash):
        self.entity_cache = getattr(self, "entity_cache", {})
        self.entity_cache[(bot_id, int(user_id))] = int(access_hash)
        return True

    def remember_user_entities(self, bot_id, users):
        saved = 0
        for u in users or []:
            uid = int(getattr(u, "id", 0) or 0)
            ah = getattr(u, "access_hash", None)
            if not uid or ah is None:
                continue
            self.remember_user_entity(bot_id, uid, ah)
            saved += 1
        return saved

    def get_user_access_hash(self, bot_id, user_id):
        try:
            return (getattr(self, "entity_cache", {}) or {}).get((bot_id, int(user_id)))
        except Exception:
            return None

    def add_user_bot(self, owner_id, token, username):
        bot_id = f"b{len(self.user_bots) + 1}"
        self.user_bots = self.user_bots + [{"bot_id": bot_id, "user_id": owner_id,
                                            "bot_token": token, "bot_username": username,
                                            "account_type": "bot", "is_active": 1}]
        self.added_bots = getattr(self, "added_bots", [])
        self.added_bots.append((owner_id, token, username, bot_id))
        return bot_id

    def get_bot_by_username(self, username):
        for b in self.user_bots:
            if b.get("bot_username") == username:
                return b
        return None

    def get_bot_channels(self, bot_id):
        if hasattr(self, "bot_channels"):
            return list(self.bot_channels)
        return [{"channel_id": -100123, "channel_title": "Test Channel", "auto_approve": 0}]

    def get_total_requesters_count(self, bot_id):
        return 7

    def get_reachable_requesters_count(self, bot_id):
        return 5

    def get_pending_count(self, bot_id):
        return 1

    def get_requesters_for_bot(self, bot_id):
        return list(getattr(self, "requesters", {}).get(bot_id, [111, 222]))

    # --- reachability
    def mark_reachable(self, *a):
        self.reachable_calls.append(tuple(a))

    def mark_unreachable(self, *a):
        self.unreachable_calls.append(tuple(a))

    def mark_permanently_unreachable(self, *a):
        self.gone_calls.append(tuple(a))

    def is_permanently_unreachable(self, bot_id, uid):
        return any(c[0] == bot_id and c[1] == uid for c in self.gone_calls)

    def clear_initiate_blocked_unreachable(self):
        keep = [c for c in self.gone_calls if "initiate" not in str(c[2] if len(c) > 2 else "").lower()]
        cleared = len(self.gone_calls) - len(keep)
        self.gone_calls = keep
        self.cleared_calls = getattr(self, "cleared_calls", 0) + cleared
        return cleared

    def count_permanently_unreachable(self, bot_id):
        return sum(1 for c in self.gone_calls if c[0] == bot_id)

    # --- user accounts (MTProto)
    def add_user_account(self, owner_id, account_user_id, phone, session_string,
                         username=None, api_id=None, api_hash=None):
        bot_id = f"ua{account_user_id}"
        self.user_account_calls = getattr(self, "user_account_calls", [])
        self.user_account_calls.append({"bot_id": bot_id, "owner_id": owner_id, "phone": phone,
                                        "session": session_string, "username": username,
                                        "api_id": api_id, "api_hash": api_hash})
        row = {"bot_id": bot_id, "user_id": owner_id, "bot_username": username, "phone": phone,
               "session_string": session_string, "api_id": api_id, "api_hash": api_hash,
               "account_type": "user", "bot_token": None, "is_active": 0}
        if not session_string or len(str(session_string)) < 100:
            row["session_string"] = FAKE_SESSION  # start_user_account decode kar sake
        self.user_bots = [b for b in self.user_bots if b.get("bot_id") != bot_id] + [row]
        return bot_id

    def set_user_account_session(self, bot_id, session_string, phone=None):
        for b in self.user_bots:
            if b.get("bot_id") == bot_id:
                b["session_string"] = session_string
                b["account_type"] = "user"
                if phone:
                    b["phone"] = phone

    def add_join_request(self, *a, **k):
        self.join_requests = getattr(self, "join_requests", [])
        self.join_requests.append(tuple(a))

    def add_channel(self, *a, **k):
        self.channels = getattr(self, "channels", [])
        self.channels.append(tuple(a))

    def set_auto_approve(self, *a, **k):
        pass

    def get_messages(self, channel_id=None, bot_id=None):
        return []

    def get_default_first_message(self):
        return "Hi {first_name}!"

    def get_pending_requests(self, bot_id):
        return []

    def mark_request_status(self, *a, **k):
        pass


def _fake_session_string() -> str:
    """Asli format ka StringSession (warna start_user_account decode par fail karta hai)."""
    if not HAS_TELETHON:
        return "FAKE-SESSION"
    from telethon.sessions import StringSession
    from telethon.crypto import AuthKey
    sess = StringSession()
    sess.set_dc(2, "149.154.167.51", 443)
    sess.auth_key = AuthKey(bytes(range(256)))
    return sess.save()


FAKE_SESSION = _fake_session_string()


class FakeTLClient:
    """Telethon client ka chhota jhootha version (koi network nahi)."""

    def __init__(self, authorized=True, me_id=123, me_username="acct", mode="ok", me_premium=None):
        self.authorized = authorized
        self.me = SimpleNamespace(id=me_id, username=me_username, bot=False, premium=me_premium)
        self.emoji_rejects = 0   # custom (premium) emoji reject kitni baar hua
        self.mode = mode          # ok | password | bad_code | bad_password | blocked | flood
        self.connected = False
        self.disconnected = False
        self.code_requests = []
        self.sign_ins = []
        self.sent_messages = []
        self.sent_files = []
        self.forwards = []       # Saved Messages -> forward
        self.raw_calls = []
        self.known_entities = {}  # user_id -> access_hash (Telethon session jaisa)
        self.invite_users = []    # GetChatInviteImporters ka jhootha jawab
        self.input_entity_calls = []
        self.handlers = []
        self.session = SimpleNamespace(save=lambda: FAKE_SESSION)
        self._stop = asyncio.Event()

    # --- lifecycle
    async def connect(self):
        self.connected = True

    def is_connected(self):
        return self.connected

    async def disconnect(self):
        self.connected = False
        self.disconnected = True

    async def is_user_authorized(self):
        return self.authorized

    async def get_me(self):
        return self.me

    async def run_until_disconnected(self):
        await self._stop.wait()

    def on(self, event):
        def deco(fn):
            self.handlers.append(fn)
            return fn
        return deco

    # --- login
    async def send_code_request(self, phone):
        self.code_requests.append(phone)
        return SimpleNamespace(phone_code_hash="hash-1")

    async def sign_in(self, phone=None, code=None, phone_code_hash=None, password=None):
        self.sign_ins.append({"phone": phone, "code": code, "password": password})
        if self.mode == "password" and password is None:
            raise SessionPasswordNeededError(None)
        if self.mode == "password" and password is not None and password != "rightpass":
            raise PasswordHashInvalidError(None)
        if self.mode == "bad_code" and code is not None:
            raise PhoneCodeInvalidError(None)
        if self.mode == "expired_code" and code is not None:
            raise PhoneCodeExpiredError(None)
        if self.mode == "bad_password" and password is not None:
            raise PasswordHashInvalidError(None)
        return self.me

    # --- sending
    def _maybe_direct_fail(self, chat_id):
        """direct send fail (jaise entity nahi mili / Telegram ne rok di) par 'me' allowed."""
        if self.mode != "direct_fail":
            return
        uid = getattr(chat_id, "user_id", None)
        if uid is None:
            try:
                int(chat_id)
            except Exception:
                return          # "me" = Saved Messages -> allowed
        raise ValueError("Could not find the input entity for "
                         f"PeerUser(user_id={uid if uid is not None else chat_id})")

    async def get_input_entity(self, chat_id):
        """Telethon jaisa entity resolve: session me ho to peer, warna error."""
        self.input_entity_calls.append(chat_id)
        try:
            uid = int(chat_id)
        except Exception:
            return chat_id   # "me" vagera
        if uid in self.known_entities:
            return SimpleNamespace(user_id=uid, access_hash=self.known_entities[uid])
        if self.mode == "strict_entity":
            # server log wali asli error: account ne user ko kabhi dekha hi nahi
            raise ValueError(f"Could not find the input entity for PeerUser(user_id={uid})")
        # default: Telethon session/dialogs se khud resolve kar leta hai
        return SimpleNamespace(user_id=uid, access_hash=900000 + (uid % 1000))

    async def forward_messages(self, entity, messages, from_peer=None, **kw):
        self.forwards.append({"to": entity, "msg": messages, "from_peer": from_peer})
        return SimpleNamespace(id=900 + len(self.forwards))

    def _maybe_reject_emoji(self, text):
        """Server log wali error: custom emoji (tg-emoji) bhejne par poora message fail."""
        if self.mode == "doc_invalid" and "tg-emoji" in (text or ""):
            self.emoji_rejects += 1
            raise type("DocumentInvalidError", (Exception,), {})(
                "The document file was invalid and can't be used in inline mode "
                "(caused by SendMessageRequest)")

    async def send_message(self, chat_id, text, **kw):
        self._maybe_reject_emoji(text)
        self._maybe_direct_fail(chat_id)
        if self.mode == "blocked":
            raise type("UserIsBlockedError", (Exception,), {})("You blocked this user")
        if self.mode == "flood":
            raise type("PeerFloodError", (Exception,), {})("Too many requests")
        self.sent_messages.append({"chat_id": chat_id, "text": text, **kw})
        return SimpleNamespace(id=len(self.sent_messages))

    async def send_file(self, chat_id, file, **kw):
        self._maybe_reject_emoji(kw.get("caption"))
        self._maybe_direct_fail(chat_id)
        if self.mode == "flood":
            raise type("PeerFloodError", (Exception,), {})("Too many requests")
        if self.mode == "blocked":
            raise type("UserIsBlockedError", (Exception,), {})("You blocked this user")
        self.sent_files.append({"chat_id": chat_id, "file": file, **kw})
        return SimpleNamespace(id=len(self.sent_files))

    async def get_permissions(self, chat_id, user=None):
        return SimpleNamespace(is_admin=True, is_creator=False)

    async def delete_messages(self, chat_id, message_ids):
        self.deleted_messages = getattr(self, "deleted_messages", [])
        self.deleted_messages.append((chat_id, list(message_ids)))
        return True

    async def edit_message(self, chat_id, message_id, text, **kw):
        self.edited = getattr(self, "edited", [])
        self.edited.append((chat_id, message_id, text))
        return SimpleNamespace(id=message_id)

    async def __call__(self, request):
        self.raw_calls.append(request)
        if type(request).__name__ == "GetChatInviteImportersRequest":
            users = list(self.invite_users)
            importers = [SimpleNamespace(user_id=int(getattr(u, "id", 0) or 0)) for u in users]
            return SimpleNamespace(users=users, importers=importers)
        return SimpleNamespace(users=[], importers=[])


A.db = FakeDB()


# ---------------------------------------------------------------- helpers
def _fake_update(q=None, uid=999, msg=None):
    return SimpleNamespace(callback_query=q,
                           effective_user=SimpleNamespace(id=uid, first_name="T", last_name="",
                                                          username="t", is_bot=False),
                           message=msg or FakeMsg(chat_id=uid), effective_chat=SimpleNamespace(id=uid),
                           chat_member=None, chat_join_request=None, inline_query=None)


def _kb_labels(markup):
    data = markup.to_dict() if hasattr(markup, "to_dict") else markup
    return [b.get("text", "") for row in data.get("inline_keyboard", []) for b in row]


def _peer_uid(x):
    """Telethon peer (InputPeerUser) ho ya seedha int - user_id nikaalo."""
    return int(getattr(x, "user_id", x))


class _LogCapture(logging.Handler):
    def __init__(self):
        super().__init__()
        self.records = []

    def emit(self, record):
        self.records.append(record)


def _capture_logs(level=logging.DEBUG):
    root = logging.getLogger()
    old = root.level
    root.setLevel(level)
    handler = _LogCapture()
    handler.setLevel(level)
    root.addHandler(handler)
    return handler, root, old


def _stop_capture(handler, root, old):
    root.removeHandler(handler)
    root.setLevel(old)


# ================================================================ tests
def test_premium_button_parsing():
    print("\n[1] premium emoji buttons")
    entities = [EmojiEntity(0, 2, "5000000001")]
    payload = A.buttons_json_from_text("💎 Join|https://t.me/a", entities)
    rows = A.rows_from_buttons_json(payload)
    check("premium emoji captured as icon", rows[0][0]["icon_id"] == "5000000001", str(rows))
    to_dict = A.buttons_to_markup(payload).to_dict()["inline_keyboard"]
    check("markup serializes icon", to_dict[0][0].get("icon_custom_emoji_id") == "5000000001", str(to_dict))
    plain = A.buttons_to_plain_markup(payload).to_dict()["inline_keyboard"]
    check("plain fallback keeps emoji text", plain[0][0]["text"].startswith("💎") and
          "icon_custom_emoji_id" not in plain[0][0], str(plain))
    degraded = A._degrade_markup(A.buttons_to_markup(payload)).to_dict()["inline_keyboard"]
    check("degrade strips icon", "icon_custom_emoji_id" not in degraded[0][0] and
          degraded[0][0]["text"].startswith("💎"), str(degraded))

    two = A.buttons_json_from_text("A|https://a.com || B|https://b.com")
    check("'||' = 2 buttons in one row", len(A.rows_from_buttons_json(two)[0]) == 2, str(two))
    check("garbage -> no rows", A.parse_button_lines("hello world") == [])

    b = A.btn("🟢 @mybot", "manage_bot_x", "primary", "🤖")
    check("meaningful emoji kept in label", b.text.startswith("🟢"), b.text)
    b2 = A.btn("🔙 Back", "x", "primary", "🔙")
    check("leading icon stripped from label", b2.text == "Back", b2.text)


def test_button_wizard():
    print("\n[2] ➕ Add Button wizard (naam -> link -> same/new row -> save)")
    A.db.messages[5] = {"id": 5, "bot_id": "b1", "channel_id": -100, "buttons_json": None}
    ctx = FakeCtx()
    tid = A.register_button_target(ctx, {"kind": "message", "msg_id": 5, "back_cb": "manage_bot_b1"})
    q = FakeQuery(ctx)
    run(A.start_button_wizard(q, ctx, tid))
    state = ctx.user_data[A.BUTTON_WIZARD_KEY]
    check("wizard starts at name step", state["step"] == "name", str(state.get("step")))
    check("wizard prompt shown", bool(q.edits) and "BUTTON BUILDER" in q.edits[-1][0], str(q.edits[-1:] or None))

    msg = FakeMsg("💎 Join Now", entities=[EmojiEntity(0, 2, "5000000001")])
    check("name step consumed", run(A.handle_button_wizard_message(msg, ctx)) is True)
    check("premium icon captured", state["pending"]["icon_id"] == "5000000001", str(state["pending"]))
    run(A.handle_button_wizard_message(FakeMsg("https://t.me/join"), ctx))
    check("button stored in row 1", len(state["rows"]) == 1 and len(state["rows"][0]) == 1, str(state["rows"]))

    run(A.handle_button_wizard_callback(FakeQuery(ctx), ctx, f"bwz_same_{tid}"))
    check("same-row placement", state["placement"] == "same" and state["step"] == "name", str(state))
    run(A.handle_button_wizard_message(FakeMsg("Website"), ctx))
    run(A.handle_button_wizard_message(FakeMsg("not-a-link"), ctx))
    check("invalid link rejected", state["step"] == "url", str(state.get("step")))
    run(A.handle_button_wizard_message(FakeMsg("https://site.com"), ctx))
    check("2 buttons in one row", len(state["rows"][0]) == 2, str(state["rows"]))

    qd = FakeQuery(ctx)
    run(A.handle_button_wizard_callback(qd, ctx, f"bwz_done_{tid}"))
    saved = A.rows_from_buttons_json(A.db.messages[5]["buttons_json"])
    check("buttons saved to DB", len(saved) == 1 and len(saved[0]) == 2, str(saved))
    check("premium icon persisted", saved[0][0]["icon_id"] == "5000000001", str(saved))
    check("wizard state cleared", A.BUTTON_WIZARD_KEY not in ctx.user_data)

    # bulk paste mode
    ctx2 = FakeCtx()
    ctx2.user_data["admin_broadcast_draft"] = {"text": "hi", "buttons_json": None}
    tid2 = A.register_button_target(ctx2, A.admin_broadcast_target())
    run(A.start_button_wizard(FakeQuery(ctx2), ctx2, tid2, mode="bulk"))
    st2 = ctx2.user_data[A.BUTTON_WIZARD_KEY]
    check("bulk mode step", st2["step"] == "bulk", str(st2.get("step")))
    run(A.handle_button_wizard_message(FakeMsg("A|https://a.com || B|https://b.com"), ctx2))
    run(A.handle_button_wizard_callback(FakeQuery(ctx2), ctx2, f"bwz_done_{tid2}"))
    check("bulk saved to draft", A.button_count(ctx2.user_data["admin_broadcast_draft"]["buttons_json"]) == 2,
          str(ctx2.user_data["admin_broadcast_draft"]))


def test_album_layout():
    print("\n[3] album + caption + buttons placement")
    payload = A.buttons_json_from_text("💎 JOIN VIP|https://t.me/join")
    markup = A.buttons_to_markup(payload)

    ctx = FakeCtx()
    draft = {"album": [{"media": "p1", "media_type": "photo", "text": "20K TO 4L TARGET", "entities_json": None},
                       {"media": "p2", "media_type": "photo"},
                       {"media": "p3", "media_type": "video"}], "text": "20K TO 4L TARGET"}
    run(A.send_draft_message(ctx, 555, draft, markup=markup))
    kinds = [c[0] for c in ctx.bot.calls]
    group = [c for c in ctx.bot.calls if c[0] == "send_media_group"][0]
    follow = [c for c in ctx.bot.calls if c[0] == "send_message"][0]
    check("album sent as one media group", kinds.count("send_media_group") == 1 and len(group[2]) == 3, str(kinds))
    check("default: caption album par hi", group[2][0].caption == "20K TO 4L TARGET", str(group[2][0].caption))
    check("buttons album ke neeche alag message me", "reply_markup" in follow[3] and follow[2] != "20K TO 4L TARGET",
          f"{follow[2]!r}")

    ctx2 = FakeCtx()
    run(A.send_draft_message(ctx2, 556, dict(draft, caption_with_buttons=True), markup=markup))
    group2 = [c for c in ctx2.bot.calls if c[0] == "send_media_group"][0]
    follow2 = [c for c in ctx2.bot.calls if c[0] == "send_message"][0]
    check("toggle: caption album se hat jata hai", all((m.caption or "") == "" for m in group2[2]),
          str([m.caption for m in group2[2]]))
    check("toggle: caption + buttons ek message me",
          follow2[2] == "20K TO 4L TARGET" and "reply_markup" in follow2[3], f"{follow2[2]!r}")

    for mtype, kind in (("photo", "send_photo"), ("video", "send_video"), ("document", "send_document")):
        ctx3 = FakeCtx()
        run(A.send_draft_message(ctx3, 557, {"media": "single", "media_type": mtype, "text": "Caption"}, markup=markup))
        check(f"1 {mtype}: caption + buttons usi message par",
              [c[0] for c in ctx3.bot.calls] == [kind] and ctx3.bot.calls[0][3].get("caption") == "Caption"
              and "reply_markup" in ctx3.bot.calls[0][3], str([c[0] for c in ctx3.bot.calls]))

    ctx4 = FakeCtx()
    run(A.send_draft_message(ctx4, 558, {"media": None, "media_type": "text", "text": "Plain"}, markup=markup))
    check("text: buttons usi text message par",
          [c[0] for c in ctx4.bot.calls] == ["send_message"] and "reply_markup" in ctx4.bot.calls[0][3],
          str(ctx4.bot.calls))


def test_album_flush_real_jobqueue():
    print("\n[4] album flush (asli JobQueue + asli from_job context)")
    from telegram.ext import ApplicationBuilder, CallbackContext

    app = ApplicationBuilder().token("123456789:" + "A" * 35).build()
    probe = app.job_queue.run_once(lambda ctx: None, when=30, data={})
    check("premise: job context me user_data None hota hai",
          CallbackContext.from_job(probe, app).user_data is None)

    class _JobApp:
        def __init__(self, bot):
            self.bot = bot
            self.user_data = {}

    def job_ctx(job, bot=None):
        bot = bot or FakeBot("jobbot")
        return CallbackContext.from_job(job, _JobApp(bot)), bot

    ctx = FakeCtx()
    ctx.job_queue = app.job_queue
    mg = "MG100"
    key = A._broadcast_album_key("user", "b1", mg)
    ctx.user_data[key] = [A.make_media_item({"text": "caption", "media": "f1", "media_type": "photo"}),
                          A.make_media_item({"text": "caption", "media": "f2", "media_type": "photo"})]
    A._schedule_broadcast_flush(ctx, f"{key}_job", {"scope": "user", "bot_id": "b1", "chat_id": 999,
                                                    "media_group_id": mg})
    job = ctx.user_data[f"{key}_job"]
    jctx, job_bot = job_ctx(job)
    check("job context really has no user_data", jctx.user_data is None)
    run(job.callback(jctx))
    draft = ctx.user_data.get("broadcast_draft_b1") or {}
    check("album draft saved (no 'NoneType.pop' crash)", len(draft.get("album") or []) == 2, str(draft))
    check("stage ready", ctx.user_data.get("broadcast_stage_b1") == "buttons_or_send")
    check("temp keys cleaned", key not in ctx.user_data and f"{key}_job" not in ctx.user_data)
    check("ready screen sent", any("Album saved" in str(c[2]) for c in job_bot.calls),
          str(job_bot.calls)[:1])

    # poora user flow: 2 media collect -> flush -> ready
    fctx = FakeCtx()
    fctx.job_queue = app.job_queue
    fmsg = FakeMsg(chat_id=999)
    item = {"text": "20K TO 4L TARGET", "media": "shot1.jpg", "media_type": "photo", "media_group_id": "MGFLOW"}
    ok1 = run(A.collect_broadcast_album(fctx, "user", "b1", fmsg, item))
    ok2 = run(A.collect_broadcast_album(fctx, "user", "b1", fmsg, dict(item, media="shot2.jpg")))
    check("album collected", ok1 is True and ok2 is True)
    check("sirf ek 'Album mil gaya' prompt", sum(1 for r in fmsg.replies if "Album mil gaya" in str(r[0])) == 1,
          str(fmsg.replies))
    fkey = A._broadcast_album_key("user", "b1", "MGFLOW")
    fjob = fctx.user_data[f"{fkey}_job"]
    fjctx, flow_bot = job_ctx(fjob)
    run(fjob.callback(fjctx))
    fdraft = fctx.user_data.get("broadcast_draft_b1") or {}
    check("flow: draft me dono items + caption", len(fdraft.get("album") or []) == 2 and
          fdraft.get("text") == "20K TO 4L TARGET", str(fdraft))
    check("flow: ready screen aaya", any("Album saved" in str(c[2]) for c in flow_bot.calls))


def test_admin_multiselect():
    print("\n[5] admin broadcast: multi-select + auto 1-day Basic")
    A.db.user_bots = [{"bot_id": "b1", "bot_username": "one", "bot_token": "t1", "user_id": 1},
                      {"bot_id": "b2", "bot_username": "two", "bot_token": "t2", "user_id": 2},
                      {"bot_id": "b3", "bot_username": "three", "bot_token": "t3", "user_id": 3}]
    A.db.subs = {"b1": {"subscription_type": "Pro", "expiry_date": A.now_aware() + timedelta(days=10)}}
    ctx = FakeCtx()
    q = FakeQuery(ctx, uid=A.ADMIN_USER_ID)
    run(A.render_admin_bcast_targets(q, ctx))
    labels = _kb_labels(q.edits[-1][1]["reply_markup"])
    check("userbot list rendered", any("one" in l for l in labels), str(labels))
    check("no-sub hint", any("1d Basic auto" in l for l in labels), str(labels))

    q.data = "admin_bcast_sel_all"
    run(A.callback_handler(_fake_update(q=q, uid=A.ADMIN_USER_ID), ctx))
    check("select all", sorted(A.broadcast_selected_ids(ctx)) == ["b1", "b2", "b3"], str(A.broadcast_selected_ids(ctx)))
    q2 = FakeQuery(ctx, uid=A.ADMIN_USER_ID)
    q2.data = "admin_bcast_tog_b2"
    run(A.callback_handler(_fake_update(q=q2, uid=A.ADMIN_USER_ID), ctx))
    check("toggle off", A.broadcast_selected_ids(ctx) == ["b1", "b3"], str(A.broadcast_selected_ids(ctx)))
    q3 = FakeQuery(ctx, uid=A.ADMIN_USER_ID)
    q3.data = "admin_bcast_sel_none"
    run(A.callback_handler(_fake_update(q=q3, uid=A.ADMIN_USER_ID), ctx))
    check("clear", A.broadcast_selected_ids(ctx) == [], str(A.broadcast_selected_ids(ctx)))

    auto = run(A.ensure_broadcast_subscription("b2"))
    check("auto 1-day Basic added", auto is True and "b2" in A.db.subs and
          abs((A.db.subs["b2"]["expiry_date"] - A.now_aware()).days) <= 1, str(A.db.subs.get("b2")))
    check("already-subscribed bot untouched", run(A.ensure_broadcast_subscription("b1")) is False)


def test_delivery_robustness():
    print("\n[6] broadcast delivery: cross-bot media, blocked users, dead users")
    check("classify: chat not found = user gone",
          A.is_user_gone_error(A.BadRequest("Bad Request: chat not found")) is True)
    check("classify: blocked = user gone",
          A.is_user_gone_error(A.Forbidden("Forbidden: bot can't initiate conversation with a user")) is True)
    check("classify: wrong file id = media error only",
          A.is_media_error(A.BadRequest("Bad Request: wrong file identifier/http url specified")) is True and
          A.is_user_gone_error(A.BadRequest("Bad Request: wrong file identifier/http url specified")) is False)

    main_bot = FakeBot("main", token="111:MAIN", file_path="photos/a.jpg")
    ubot = FakeBot("ubot", token="222:UBOT")
    A._MEDIA_REF_CACHE.clear()
    ref = run(A.sendable_media_id(ubot, "FILEID_ABC", main_bot))
    check("cross-bot: file_id -> file url", ref == "https://api.telegram.org/file/bot111:MAIN/photos/a.jpg", ref)
    run(A.sendable_media_id(ubot, "FILEID_ABC", main_bot))
    check("cross-bot: cached", len(main_bot.get_file_calls) == 1, str(main_bot.get_file_calls))
    check("cross-bot: same bot untouched",
          run(A.sendable_media_id(main_bot, "FILEID_ABC", main_bot)) == "FILEID_ABC")
    check("cross-bot: http url passthrough",
          run(A.sendable_media_id(ubot, "https://x/y.jpg", main_bot)) == "https://x/y.jpg")
    draft = {"media": "FILEID_ABC", "media_type": "photo",
             "album": [{"media": "FILEID_ABC", "media_type": "photo"}]}
    translated = run(A.translate_draft_for_bot(draft, ubot, main_bot))
    check("draft translate: single + album",
          translated["media"].startswith("https://api.telegram.org/file/bot111:MAIN/") and
          translated["album"][0]["media"].startswith("https://"), str(translated))
    check("draft translate: original untouched", draft["media"] == "FILEID_ABC")

    # blocked user: album Forbidden -> koi per-item retry nahi
    ctxb = FakeCtx()
    ctxb.bot = BlockedBot("blocked")
    album_draft = {"album": [{"media": f"f{i}", "media_type": "photo"} for i in range(5)], "text": ""}
    raised = False
    try:
        run(A.send_draft_message(ctxb, 4242, album_draft))
    except A.Forbidden:
        raised = True
    check("album Forbidden propagates", raised is True)
    check("blocked par per-item retry nahi", [c[0] for c in ctxb.bot.calls] == ["send_media_group"],
          str([c[0] for c in ctxb.bot.calls]))

    # admin broadcast with mixed results
    A._MEDIA_REF_CACHE.clear()
    A.db.user_bots = [{"bot_id": "b1", "bot_username": "one", "bot_token": "t1", "user_id": 999}]
    A.db.subs = {"b1": {"subscription_type": "Pro", "expiry_date": A.now_aware() + timedelta(days=5)}}
    A.db.unreachable_calls, A.db.gone_calls, A.db.reachable_calls = [], [], []
    recipients = [101, 102, 103, 104]
    A.db.requesters = {"b1": recipients}
    mixed = PerUserBot({101: "ok", 102: "blocked", 103: "chat_not_found", 104: "bad_media"},
                       name="mixed", token="333:MIXED")
    A.user_bot_applications["b1"] = SimpleNamespace(bot=mixed)
    actx = FakeCtx()
    actx.bot = FakeBot("main2", token="111:MAIN2")
    actx.user_data["admin_broadcast_draft"] = {"text": "hello", "media": "FILEID_XYZ", "media_type": "photo",
                                              "album": None, "buttons_json": None, "target_bots": ["b1"],
                                              "caption_with_buttons": False}
    actx.user_data["admin_bcast_selected"] = ["b1"]
    handler, root, old_level = _capture_logs()
    try:
        q = FakeQuery(actx, uid=A.ADMIN_USER_ID)
        run(A.send_admin_broadcast(q, actx))
    finally:
        _stop_capture(handler, root, old_level)
        A.user_bot_applications.pop("b1", None)
    errors = [r.getMessage() for r in handler.records if r.levelno >= logging.ERROR]
    infos = [r.getMessage() for r in handler.records if r.levelno == logging.INFO]
    warns = [r.getMessage() for r in handler.records if r.levelno == logging.WARNING]
    check("no per-recipient ERROR spam", not errors, str(errors[:2]))
    check("one summary line", any("recipients=4 sent=2 unreachable=2 failed=0 media_issues=1" in m for m in infos),
          str([m for m in infos if "broadcast b1" in m]))
    check("media issue grouped (ek warning)", any("media ke bina caption+buttons" in m for m in warns), str(warns))
    check("blocked + chat-not-found permanently marked",
          ("b1", 102) in [(c[0], c[1]) for c in A.db.unreachable_calls] and
          ("b1", 103) in [(c[0], c[1]) for c in A.db.gone_calls], str(A.db.gone_calls))
    check("success reachable marked", ("b1", 101) in [(c[0], c[1]) for c in A.db.reachable_calls])
    check("media translated before sending", any(
        isinstance(c[2], str) and c[2].startswith("https://api.telegram.org/file/bot") for c in mixed.calls
        if c[0] == "send_photo"), str(mixed.calls[:1]))

    import sqlite3
    con = sqlite3.connect(":memory:")
    con.executescript("""
        CREATE TABLE reachable_users (bot_id TEXT, requester_id INT, last_ok_at TEXT);
        CREATE TABLE join_requests (bot_id TEXT, requester_id INT, status TEXT);
        CREATE TABLE unreachable_users (bot_id TEXT, requester_id INT, reason TEXT);
        INSERT INTO reachable_users VALUES ('b1', 11, '2026-01-01'), ('b1', 12, '2026-01-02');
        INSERT INTO join_requests VALUES ('b1', 13, 'approved'), ('b1', 14, 'approved'), ('b1', 15, 'pending');
        INSERT INTO unreachable_users VALUES ('b1', 12, 'chat not found'), ('b1', 14, 'blocked');
    """)
    got = [r[0] for r in con.execute(A.REQUESTERS_SQL.replace("%s", "?"), ("b1", "b1", "b1")).fetchall()]
    check("audience SQL: dead users excluded", sorted(got) == [11, 13], str(got))
    check("audience SQL: reachable pehle", got[0] == 11, str(got))


def test_security_and_startup():
    print("\n[7] token masking + startup guards")
    import telegram.error as terr

    RAW = "7687421668:AAFzEsDO2L2EVkCm4MxhzSo8oGD0-8t5GKE"
    masked = A.mask_secrets(f"The token `{RAW}` was rejected by the server.")
    check("mask: secret hidden", RAW not in masked and "7687421668:" in masked, masked)
    check("mask: plain text untouched", A.mask_secrets("Document_invalid") == "Document_invalid")
    fmt = A.MaskingFormatter("%(message)s")
    rec = logging.LogRecord("t", logging.ERROR, __file__, 1, "boom %s", (RAW,), None)
    check("formatter: message masked", RAW not in fmt.format(rec))
    try:
        raise terr.InvalidToken(f"The token `{RAW}` was rejected by the server.")
    except terr.InvalidToken:
        rec2 = logging.LogRecord("t", logging.ERROR, __file__, 1, "fatal", (), sys.exc_info())
    check("formatter: traceback masked", RAW not in fmt.format(rec2))

    tmp = tempfile.mkdtemp()
    with open(os.path.join(tmp, ".env"), "w") as fh:
        fh.write("# c\nTEST_ENV_SECRET=hello123\nQUOTED='quoted-value'\n")
    os.environ.pop("TEST_ENV_SECRET", None)
    os.environ.pop("QUOTED", None)
    cwd = os.getcwd()
    try:
        os.chdir(tmp)
        A.load_env_file()
    finally:
        os.chdir(cwd)
    check("env: .env loaded", os.environ.get("TEST_ENV_SECRET") == "hello123")
    check("env: quotes stripped", os.environ.get("QUOTED") == "quoted-value")

    source = open(A.__file__, encoding="utf-8").read()
    check("source: leaked token gone", "AAFzEsDO2L2EVkCm4MxhzSo8oGD0-8t5GKE" not in source)
    defaults = re.findall(r'os\.getenv\("MAIN_BOT_TOKEN",\s*"([^"]*)"\)', source)
    # (configured_secrets() me bhi wahi getenv hai, isliye saare defaults empty hone chahiye)
    check("source: token env default empty",
          len(defaults) >= 1 and all(d == "" for d in defaults), str(defaults))
    check("source: bad token -> clean exit (no restart loop)",
          "raise SystemExit(2)" in source and "MAIN BOT TOKEN reject ho gaya" in source)
    check("source: userbot boot isolated", "async def start_bots_on_boot()" in source)
    check("source: log masking installed", "install_log_masking()" in source)


def test_network_noise_and_retry():
    print("\n[8] network noise filter + userbot retry + FORCE_IPV4")
    # log filter
    fmt = A.MaskingFormatter("%(message)s")
    noisy = logging.LogRecord("telegram.ext.Updater", logging.ERROR, __file__, 1,
                              "Exception happened while polling for updates.", (), None)
    noisy.exc_info = (A.NetworkError, A.NetworkError("httpx.ReadError"), None)
    noisy.exc_text = "Traceback ... 40 lines"
    filt = A.TransientNetworkFilter()
    check("filter: record allowed", filt.filter(noisy) is True)
    check("filter: downgraded to WARNING", noisy.levelno == logging.WARNING and noisy.exc_info is None)
    text = fmt.format(noisy)
    check("filter: ek chhoti line", "network hiccup" in text and len(text) < 260, text)
    real_bug = logging.LogRecord("root", logging.ERROR, __file__, 1, "Update None caused error 'NoneType' object has no attribute 'pop'", (), None)
    real_bug.exc_info = (AttributeError, AttributeError("pop"), None)
    check("filter: asli bug untouched", filt.filter(real_bug) is True and real_bug.exc_info is not None)
    A.install_network_log_filter()
    check("filter: PTB updater logger par laga",
          any(isinstance(f, A.TransientNetworkFilter) for f in logging.getLogger("telegram.ext.Updater").filters))

    # retry_async ab DEBUG par retry karta hai
    source = open(A.__file__, encoding="utf-8").read()
    check("source: per-attempt retry DEBUG par", 'logging.debug(f"Retry {retries}/{max_retries}' in source)

    # FORCE_IPV4
    import socket as _socket
    old_env = os.environ.get("FORCE_IPV4")
    real_gai = _socket.getaddrinfo
    try:
        os.environ.pop("FORCE_IPV4", None)
        check("ipv4: default off", A.force_ipv4_enabled() is False)
        os.environ["FORCE_IPV4"] = "1"
        check("ipv4: env on", A.force_ipv4_enabled() is True)
        sample = [(_socket.AF_INET6, 1, 6, "", ("2a02:c207::1", 443)),
                  (_socket.AF_INET, 1, 6, "", ("149.154.167.220", 443))]
        _socket.getaddrinfo = lambda *a, **k: list(sample)
        _socket._advanced_force_ipv4 = False
        check("ipv4: patch applied", A.install_force_ipv4() is True)
        resolved = _socket.getaddrinfo("api.telegram.org", 443)
        check("ipv4: ipv6 filtered", [r[0] for r in resolved] == [_socket.AF_INET], str(resolved))
    finally:
        _socket.getaddrinfo = real_gai
        if hasattr(_socket, "_advanced_force_ipv4"):
            del _socket._advanced_force_ipv4
        if old_env is None:
            os.environ.pop("FORCE_IPV4", None)
        else:
            os.environ["FORCE_IPV4"] = old_env

    # userbot start retry
    class _FakeUserbotApp:
        def __init__(self, exc=None):
            self.exc = exc
            self.handlers = []
            self.bot_data = {}
            self.bot = SimpleNamespace(name="fake-userbot", id=1)
            self.updater = SimpleNamespace(stop=self._noop, start_polling=self._noop)

        async def _noop(self, *a, **k):
            return None

        def add_handler(self, h):
            self.handlers.append(h)

        async def initialize(self):
            if self.exc is not None:
                raise self.exc

        async def start(self):
            return None

        async def stop(self):
            return None

        async def shutdown(self):
            return None

    class _Builder:
        def __init__(self, app):
            self.app = app

        def token(self, *a, **k):
            return self

        def concurrent_updates(self, *a, **k):
            return self

        def request(self, *a, **k):
            return self

        def build(self):
            return self.app

    real_builder = A.ApplicationBuilder
    real_delay = A.USERBOT_RETRY_DELAY
    A.USERBOT_RETRY_DELAY = 0.01
    RAW = "123456789:" + "A" * 35
    A.TOKEN_FAILURES.clear()
    attempts = {"n": 0}

    def flaky_then_ok():
        attempts["n"] += 1
        if attempts["n"] == 1:
            return _Builder(_FakeUserbotApp(A.NetworkError("httpx.ReadError")))
        return _Builder(_FakeUserbotApp())

    A.ApplicationBuilder = flaky_then_ok
    A.user_bot_applications.pop("retrybot_1", None)
    ok = run(A.start_user_bot(RAW, "retrybot_1", 555))
    check("userbot: 2nd attempt par start ho jata hai", ok is True and "retrybot_1" in A.user_bot_applications,
          f"{ok} attempts={attempts['n']}")
    A.user_bot_applications.pop("retrybot_1", None)

    attempts["n"] = 0

    def always_flaky():
        attempts["n"] += 1
        return _Builder(_FakeUserbotApp(A.NetworkError("httpx.ReadError")))

    A.ApplicationBuilder = always_flaky
    ok2 = run(A.start_user_bot(RAW, "flakybot_1", 555))
    check("userbot: 3 attempts ke baad False (no token flag)",
          ok2 is False and attempts["n"] == A.USERBOT_START_ATTEMPTS and not A.TOKEN_FAILURES,
          f"{ok2} attempts={attempts['n']}")

    def revoked():
        return _Builder(_FakeUserbotApp(A.InvalidToken(f"The token `{RAW}` was rejected by the server.")))

    A.db.active_calls.clear()
    A.ApplicationBuilder = revoked
    ok3 = run(A.start_user_bot(RAW, "deadbot_1", 555))
    check("userbot: revoked token -> False + inactive + owner warning",
          ok3 is False and ("deadbot_1", False) in A.db.active_calls and len(A.TOKEN_FAILURES) == 1,
          str(A.TOKEN_FAILURES))
    main_bot = FakeBot("main")
    run(A.flush_token_failures(main_bot))
    check("token warning masked", RAW not in " ".join(str(c[2]) for c in main_bot.calls) and
          any(c[1] == 555 for c in main_bot.calls))
    A.TOKEN_FAILURES.clear()
    A.ApplicationBuilder = real_builder
    A.USERBOT_RETRY_DELAY = real_delay

    # retry job
    A.db.user_bots = [{"bot_id": "b1", "bot_username": "one", "bot_token": "t1", "user_id": 999},
                      {"bot_id": "nosub", "bot_username": "two", "bot_token": "t2", "user_id": 998}]
    A.db.subs = {"b1": {"subscription_type": "Pro", "expiry_date": A.now_aware() + timedelta(days=3)}}
    A.user_bot_applications.pop("b1", None)
    started = []
    real_start = A.start_user_bot

    async def spy(token, bot_id, owner_id, quiet=False):
        started.append((bot_id, quiet))
        return True

    A.start_user_bot = spy
    A.ApplicationBuilder = lambda: _Builder(_FakeUserbotApp())
    try:
        run(A.retry_inactive_userbots_job(SimpleNamespace()))
    finally:
        A.start_user_bot = real_start
        A.ApplicationBuilder = real_builder
    check("retry job: inactive+subscribed start", ("b1", True) in started, str(started))
    check("retry job: no-sub skip", all(b != "nosub" for b, _ in started), str(started))
    A.user_bot_applications["b1"] = SimpleNamespace(bot=FakeBot("running"))
    started.clear()
    A.start_user_bot = spy
    try:
        run(A.retry_inactive_userbots_job(SimpleNamespace()))
    finally:
        A.start_user_bot = real_start
        A.user_bot_applications.pop("b1", None)
    check("retry job: already running skip", started == [], str(started))
    check("source: retry job registered", "retry_inactive_userbots_job, interval=600" in source)
    check("source: ipv4 install main me", "install_force_ipv4()" in source)


def test_leave_recovery():
    print("\n[9] leave recovery DM (blocked users)")
    A.db.leave = {"enabled": True, "target_channel_id": -100999, "target_channel_link": "https://t.me/joinchat/x",
                  "messages": [{"text": "Hello {first_name}, wapas aao", "buttons_json": ""},
                               {"text": "Second message", "buttons_json": ""}]}
    A.db.gone_calls = []
    blocked_ctx = FakeCtx()
    blocked_ctx.bot = BlockedBot("blocked")

    member = SimpleNamespace(id=5551, first_name="Ravi", is_bot=False)
    update = SimpleNamespace(chat_member=SimpleNamespace(
        chat=SimpleNamespace(id=-100123, title="Chan"), new_chat_member=SimpleNamespace(status="left", user=member),
        old_chat_member=SimpleNamespace(status="member")))
    handler, root, old_level = _capture_logs()
    try:
        run(A.handle_channel_member_update(update, blocked_ctx, "b1", 999))
    finally:
        _stop_capture(handler, root, old_level)

    errors = [r.getMessage() for r in handler.records if r.levelno >= logging.ERROR]
    warns = [r.getMessage() for r in handler.records if r.levelno == logging.WARNING]
    check("blocked par ERROR spam nahi", not errors, str(errors[:2]))
    check("ek hi warning line", sum(1 for m in warns if "leave recovery DM skip" in m) == 1, str(warns))
    check("user permanently unreachable mark hua",
          ("b1", 5551) in [(c[0], c[1]) for c in A.db.gone_calls], str(A.db.gone_calls))

    # dobara leave -> pehle hi skip (API call hi nahi)
    handler2, root2, old2 = _capture_logs()
    try:
        run(A.handle_channel_member_update(update, blocked_ctx, "b1", 999))
    finally:
        _stop_capture(handler2, root2, old2)
    infos = [r.getMessage() for r in handler2.records if r.levelno == logging.INFO]
    check("dobara try nahi (skip log)", any("pehle hi unreachable" in m for m in infos), str(infos))
    check("koi ERROR nahi", not [r for r in handler2.records if r.levelno >= logging.ERROR])

    # normal user -> DM jaata hai
    A.db.gone_calls = []
    good_ctx = FakeCtx()
    good_ctx.bot = FakeBot("good")
    member2 = SimpleNamespace(id=5552, first_name="New", is_bot=False)
    update2 = SimpleNamespace(chat_member=SimpleNamespace(
        chat=SimpleNamespace(id=-100123, title="Chan"), new_chat_member=SimpleNamespace(status="left", user=member2),
        old_chat_member=SimpleNamespace(status="member")))
    run(A.handle_channel_member_update(update2, good_ctx, "b1", 999))
    check("normal user ko DM gaya", bool(A.db.leave.get("sent")), str(A.db.leave.get("sent")))
    check("normal user permanently mark nahi hua", all(c[1] != 5552 for c in A.db.gone_calls), str(A.db.gone_calls))


def test_panel_routing():
    print("\n[10] main-bot se userbot panel routing")
    A.db.user_bots = [{"bot_id": "b1", "bot_username": "one", "bot_token": "t1", "user_id": 999}]
    check("own bot resolve hota hai", A.resolve_managed_bot_id(999, "ub_stats_b1") == "b1")
    check("foreign bot nahi", A.resolve_managed_bot_id(4242, "ub_stats_b1") is None)

    ctx = FakeCtx()
    q = FakeQuery(ctx, uid=999)
    q.data = "ub_stats_b1"
    run(A.callback_handler(_fake_update(q=q, uid=999), ctx))
    check("read-only stats render", bool(q.edits), str(q.edits[-1:]))

    q2 = FakeQuery(ctx, uid=999)
    q2.data = "ub_delete_messages_b1"
    run(A.callback_handler(_fake_update(q=q2, uid=999), ctx))
    text = q2.edits[-1][0] if q2.edits else ""
    check("composing panel -> manage-from-bot help", "MANAGE FROM YOUR BOT" in text, text[:80])
    check("help me bot link hai", "Open My Bot" in _kb_labels(q2.edits[-1][1].get("reply_markup")),
          str(_kb_labels(q2.edits[-1][1].get("reply_markup"))))


def test_app_wiring():
    print("\n[11] application wiring")
    source = open(A.__file__, encoding="utf-8").read()
    for needle in ('CommandHandler("start", start_command)', "CallbackQueryHandler(callback_handler)",
                   "MessageHandler(", "app.add_error_handler(error_handler)",
                   'allowed_updates=["message", "callback_query"'):
        check(f"wiring: {needle[:42]}", needle in source)
    check("wiring: album flush fallback (JobQueue ke bina)", "_PENDING_TASKS" in source and
          "_schedule_broadcast_flush" in source)
    check("wiring: builder har jagah", source.count("button_builder_row(") >= 5,
          str(source.count("button_builder_row(")))


def test_user_account_mode():
    print("\n[12] user account (MTProto) mode")
    A.db.user_bots = []
    A.db.gone_calls = []
    A.db.leave = {"messages": []}
    A.db.user_account_calls = []

    # --- phone normalization + account detect
    check("phone: spaces/dashes hatta hai", A.normalize_phone("+91 98765-43210") == "+919876543210")
    check("phone: galat input reject", A.normalize_phone("hello") == "")
    user_row = {"bot_id": "ua123", "user_id": 999, "account_type": "user", "phone": "+919876543210"}
    bot_row = {"bot_id": "b1", "user_id": 999, "account_type": "bot", "bot_token": "t1"}
    check("detect: user account", A.account_type_from_row(user_row) == "user")
    check("detect: bot account", A.account_type_from_row(bot_row) == "bot")
    check("display: phone masked", "***" in A.account_display_name(user_row, "ua123"))

    if HAS_TELETHON:
        A._UA_LOGINS.clear()
        A.TELEGRAM_API_ID, A.TELEGRAM_API_HASH = "12345", "abcdef123456"
        login_client = FakeTLClient(mode="password")
        status = run(A.ua_login_start(777, "+91 98765-43210", client_factory=lambda: login_client))
        check("login: OTP bheja (code step)", status == "code", status)
        check("login: phone normalize hokar gaya", login_client.code_requests == ["+919876543210"],
              str(login_client.code_requests))
        check("login: 2FA maanga", run(A.ua_login_submit_code(777, "12345")) == "password")
        check("login: password galat", run(A.ua_login_submit_password(777, "wrong")) == "error:password_invalid")
        status = run(A.ua_login_submit_password(777, "rightpass"))
        check("login: session save + DB row", status == "ok:ua123", status)
        calls = A.db.user_account_calls[-1]
        check("login: session DB me gaya", calls["session"] == FAKE_SESSION
              and calls["session"].startswith("1"), str(calls)[:90])
        check("login: account_type=user row", (A.db.get_user_bot("ua123") or {}).get("account_type") == "user")
        check("login: client band hua", login_client.disconnected is True)
        check("login: logout pe koi state nahi bacha", A.ua_login_state(777) is None)

        bad_client = FakeTLClient(mode="bad_code")
        run(A.ua_login_start(778, "+919876543210", client_factory=lambda: bad_client))
        check("login: galat code pakda", run(A.ua_login_submit_code(778, "00000")) == "error:code_invalid")
        run(A.ua_login_cancel(778))

        saved_id, saved_hash = A.TELEGRAM_API_ID, A.TELEGRAM_API_HASH
        A.TELEGRAM_API_ID, A.TELEGRAM_API_HASH = "", ""
        check("login: api_id/hash missing par hint", run(A.ua_login_start(779, "+919876543210")) == "error:api")
        A.TELEGRAM_API_ID, A.TELEGRAM_API_HASH = saved_id, saved_hash

    # --- .env se credentials (panel nahi) + startup status log
    saved_pair = (A.TELEGRAM_API_ID, A.TELEGRAM_API_HASH)
    A.TELEGRAM_API_ID, A.TELEGRAM_API_HASH = "12380656", "d927c13beaaf5110f25c505b7c071273"
    check("env creds: ready mila", A.ua_credentials_available() is True
          and A.user_account_credentials()[0] == 12380656, str(A.user_account_credentials()))
    A.TELEGRAM_API_ID, A.TELEGRAM_API_HASH = "", ""
    check("env creds: missing -> off", A.ua_credentials_available() is False)
    A.TELEGRAM_API_ID, A.TELEGRAM_API_HASH = saved_pair
    src_now = open(A.__file__, encoding="utf-8").read()
    check("startup log: ready line", "user account mode ready" in src_now)
    check("startup log: off line + hint", "user account mode OFF" in src_now and "TELEGRAM_API_HINT" in src_now)

    # --- adapter: text (premium emoji + buttons -> links)
    client = FakeTLClient()
    sender = A.UserAccountSender("ua123", 999, client, phone="+919876543210",
                                 username="acct", account_user_id=123)
    markup = InlineKeyboardMarkup([[
        A.btn_url("Join Channel", "https://t.me/test", "success", "🔔"),
        A.btn("Approve", "some_callback", "primary", "✅")]])
    premium = 'Hi <tg-emoji emoji-id="5000000001">💎</tg-emoji> user'
    run(sender.send_message(555, premium, parse_mode=A.ParseMode.HTML, reply_markup=markup))
    sent = client.sent_messages[-1]
    check("adapter: premium emoji intact", '<tg-emoji emoji-id="5000000001">' in sent["text"], sent["text"][:90])
    check("adapter: URL button link ban gaya", '<a href="https://t.me/test">' in sent["text"], sent["text"][:120])
    check("adapter: buttons kwarg nahi bheja", "buttons" not in sent, str(sorted(sent.keys())))

    # --- adapter: media translate + album
    async def _fake_materialize(media, *a, **k):
        return "/tmp/ua_media_test.jpg"
    real_materialize = A.materialize_media
    A.materialize_media = _fake_materialize
    try:
        run(sender.send_photo(556, "BOT-FILE-ID", caption="Photo caption"))
        check("adapter: media file translate hui", client.sent_files[-1]["file"] == "/tmp/ua_media_test.jpg",
              str(client.sent_files[-1]))
        from telegram import InputMediaPhoto
        run(sender.send_media_group(557, [InputMediaPhoto(media="F1"),
                                          InputMediaPhoto(media="F2", caption="Album caption")]))
        got = client.sent_files[-1]
        check("adapter: album ek saath (2 files)", isinstance(got["file"], list) and len(got["file"]) == 2, str(got)[:120])
        check("adapter: album caption saath gaya", got.get("caption") == "Album caption", str(got)[:120])
    finally:
        A.materialize_media = real_materialize

    # --- adapter: error translation
    blocked_client = FakeTLClient(mode="blocked")
    blocked_sender = A.UserAccountSender("ua123", 999, blocked_client, account_user_id=123)
    try:
        run(blocked_sender.send_message(555, "hi"))
        check("adapter: blocked -> Forbidden", False, "exception nahi aaya")
    except A.Forbidden as ex:
        check("adapter: blocked -> Forbidden", "blocked" in str(ex).lower(), str(ex))
    except Exception as ex:  # noqa: BLE001
        check("adapter: blocked -> Forbidden", False, f"{type(ex).__name__}: {ex}")

    flood_client = FakeTLClient(mode="flood")
    flood_sender = A.UserAccountSender("ua123", 999, flood_client, account_user_id=123)
    try:
        run(flood_sender.send_message(555, "hi"))
        check("adapter: flood -> AccountLimitedError", False, "exception nahi aaya")
    except A.AccountLimitedError:
        check("adapter: flood -> AccountLimitedError", True)
    except Exception as ex:  # noqa: BLE001
        check("adapter: flood -> AccountLimitedError", False, f"{type(ex).__name__}: {ex}")

    # --- throttle (bulk DM se account bachao)
    import time as _time
    old_delay = A.USER_ACCOUNT_SEND_DELAY
    A.USER_ACCOUNT_SEND_DELAY = 0.05
    A._UA_LAST_SEND.pop("ua123", None)
    start = _time.monotonic()
    run(sender.send_message(560, "one"))
    run(sender.send_message(561, "two"))
    run(sender.send_message(562, "three"))
    elapsed = _time.monotonic() - start
    A.USER_ACCOUNT_SEND_DELAY = old_delay
    check("adapter: per-message throttle", elapsed >= 0.09, f"{elapsed:.3f}s")

    # --- start / stop lifecycle
    A.db.user_bots = [{"bot_id": "ua123", "user_id": 999, "account_type": "user",
                       "session_string": "SESS", "api_id": 111, "api_hash": "hash",
                       "phone": "+919876543210", "bot_username": "acct", "bot_token": None}]
    live_client = FakeTLClient()
    ok = run(A.start_user_account("ua123", 999, client_factory=lambda: live_client))
    check("start: account client chalu", ok is True and live_client.connected is True, str(ok))
    check("start: registry me sender", isinstance(A.user_account_clients.get("ua123"), A.UserAccountSender))
    check("start: handlers lage", len(live_client.handlers) >= 2, str(len(live_client.handlers)))
    check("start: is_account_running True", A.is_account_running("ua123") is True)
    run(A.stop_user_account("ua123"))
    check("stop: client band", live_client.disconnected is True)
    check("stop: registry clean", "ua123" not in A.user_account_clients)

    # start_user_bot user account par delegate karta hai
    calls = []
    real_start_account = A.start_user_account

    async def _rec_start(bot_id, owner_id=0, row=None, quiet=False, client_factory=None):
        calls.append((bot_id, owner_id))
        return True
    A.start_user_account = _rec_start
    try:
        check("start_user_bot: user account delegate", run(A.start_user_bot(None, "ua123", 999)) is True)
        check("start_user_bot: sahi bot_id", calls == [("ua123", 999)], str(calls))
    finally:
        A.start_user_account = real_start_account

    # --- join request user account se
    client2 = FakeTLClient()
    sender2 = A.UserAccountSender("ua123", 999, client2, phone="+91", username="acct", account_user_id=123)
    approved = []

    async def _approve():
        approved.append(True)
    user = SimpleNamespace(id=555, first_name="Ravi", username="ravi", is_bot=False, last_name="")
    A.db.join_requests = []
    A.db.leave = {"messages": []}
    run(A.process_join_request("ua123", 999, user, -100123, "Test Channel", None,
                               sender=sender2, approve=_approve, auto=True))
    texts = [m["text"] for m in client2.sent_messages]
    check("join: welcome account se gaya", any("Ravi" in t for t in texts), str(texts)[:140])
    check("join: auto approve hua", approved == [True], str(approved))
    check("join: DB me request approved", A.db.join_requests and A.db.join_requests[-1][-1] == "approved",
          str(A.db.join_requests))

    # --- pending join requests (UpdatePendingJoinRequests -> importers)
    client3 = FakeTLClient()
    sender3 = A.UserAccountSender("ua123", 999, client3, account_user_id=123)

    async def _list(chat_id):
        return [SimpleNamespace(id=777, first_name="Neha", username="neha", is_bot=False, last_name="")]
    sender3.list_pending_join_requesters = _list
    A.db.join_requests = []
    run(A._ua_process_pending_join_requests("ua123", 999, -100123, sender3))
    check("pending: welcome gaya", any("Neha" in m["text"] for m in client3.sent_messages),
          str([m["text"] for m in client3.sent_messages])[:140])
    check("pending: raw approve request gayi",
          any(type(r).__name__ == "HideChatJoinRequestRequest" for r in client3.raw_calls),
          str([type(r).__name__ for r in client3.raw_calls]))
    check("pending: DB me pending entry", bool(A.db.join_requests), str(A.db.join_requests))

    # --- draft translate: media local file me materialize ho (token URL nahi)
    async def _fake_materialize2(media, *a, **k):
        return "/tmp/ua_translated.jpg"
    real_mat = A.materialize_media
    A.materialize_media = _fake_materialize2
    try:
        draft = {"media": "BOT-FILE-ID", "media_type": "photo", "album": None}
        translated = run(A.translate_draft_for_bot(draft, sender2, object()))
        check("draft translate: user account -> local file",
              translated.get("media") == "/tmp/ua_translated.jpg", str(translated))
    finally:
        A.materialize_media = real_mat

    # --- poori journey: login -> subscription -> account chalu -> stop
    A.db.user_bots = []
    A.db.subs = {}
    A.db.user_account_calls = []
    if HAS_TELETHON:
        journey_client = FakeTLClient()
        st = run(A.ua_login_start(555, "+919000000000", client_factory=lambda: journey_client))
        run(A.ua_login_submit_code(555, "55555"))
        jrow = A.db.get_user_bot("ua123") or {}
        check("journey: login -> account row", st == "code" and jrow.get("account_type") == "user", str(jrow)[:80])
        # admin subscription deta hai -> account chalu ho jata hai
        A.db.add_subscription_for_bot("ua123", "Basic", 30)
        live = FakeTLClient()
        real_tc = A.TelegramClient
        A.TelegramClient = lambda *a, **k: live
        try:
            started = run(A.start_user_bot(None, "ua123", 999, quiet=True))
        finally:
            A.TelegramClient = real_tc
        check("journey: subscription -> account chalu", started is True and live.connected, str(started))
        check("journey: registry me account", isinstance(A.user_account_clients.get("ua123"), A.UserAccountSender))
        run(A.stop_user_account("ua123"))
        check("journey: stop ke baad inactive", any(c[0] == "ua123" and c[1] is False for c in A.db.active_calls),
              str(A.db.active_calls[-3:]))

    # --- connection toot jaye to self-heal (registry clean + inactive + retry job)
    A.db.user_bots = [{"bot_id": "ua123", "user_id": 999, "account_type": "user",
                       "session_string": "SESS", "api_id": 111, "api_hash": "h",
                       "phone": "+9199", "bot_username": "acct", "bot_token": None}]
    A.db.active_calls = []
    A.user_account_clients["ua123"] = A.UserAccountSender("ua123", 999, FakeTLClient(), account_user_id=123)

    async def _finished():
        return None
    gone_task = _loop().create_task(_finished())
    run(asyncio.sleep(0))
    run(A._on_account_disconnected("ua123", gone_task))
    check("account toota -> registry clean", "ua123" not in A.user_account_clients)
    check("account toota -> inactive mark hua",
          any(c[0] == "ua123" and c[1] is False for c in A.db.active_calls), str(A.db.active_calls))

    # --- leave recovery user account se
    A.db.leave = {"enabled": True, "target_channel_id": -100999,
                  "target_channel_link": "https://t.me/+abc", "messages": []}
    A.db.gone_calls = []
    blocked_client2 = FakeTLClient(mode="blocked")
    blocked_sender2 = A.UserAccountSender("ua123", 999, blocked_client2, account_user_id=123)
    member = SimpleNamespace(id=888, first_name="Amit", username="amit", is_bot=False, last_name="")
    handler, root, old_level = _capture_logs()
    try:
        run(A.process_member_left("ua123", member, -100123, "Test Channel", sender=blocked_sender2))
    finally:
        _stop_capture(handler, root, old_level)
    warnings = [r for r in handler.records if r.levelno == logging.WARNING]
    errors = [r for r in handler.records if r.levelno >= logging.ERROR]
    check("leave: blocked par 1 warning", len(warnings) == 1, str([r.getMessage()[:60] for r in warnings]))
    check("leave: koi ERROR nahi", not errors, str([r.getMessage()[:60] for r in errors]))
    check("leave: user permanently mark hua", any(c[1] == 888 for c in A.db.gone_calls), str(A.db.gone_calls))

    good_client = FakeTLClient()
    good_sender = A.UserAccountSender("ua123", 999, good_client, account_user_id=123)
    member2 = SimpleNamespace(id=889, first_name="Sita", username="sita", is_bot=False, last_name="")
    run(A.process_member_left("ua123", member2, -100123, "Test Channel", sender=good_sender))
    check("leave: reachable ko DM gaya", any(_peer_uid(m["chat_id"]) == 889 for m in good_client.sent_messages),
          str(good_client.sent_messages)[:140])

    # --- panel + routing
    A.db.user_bots = [{"bot_id": "ua123", "user_id": 999, "account_type": "user",
                       "bot_username": "acct", "phone": "+919876543210", "bot_token": None}]
    labels = _kb_labels(A.main_menu_kb(999))
    check("panel: user account label", any("acct" in l for l in labels), str(labels))
    check("panel: Account Info button", any("Account Info" in l for l in _kb_labels(A.bot_management_kb("ua123", 999))),
          str(_kb_labels(A.bot_management_kb("ua123", 999))))

    ctx = FakeCtx()
    q = FakeQuery(ctx, uid=999)
    q.data = "ub_delete_messages_ua123"
    ctx.user_bots = A.db.user_bots
    run(A.callback_handler(_fake_update(q=q, uid=999), ctx))
    text = q.edits[-1][0] if q.edits else ""
    check("panel routing: main bot se khul gaya", "MANAGE FROM YOUR BOT" not in text, text[:80])

    # setbtn_ callbacks bhi main bot se handle hone chahiye (panel ke andar buttons lagana)
    setbtn_calls = []
    real_setbtn = A.handle_set_buttons_callback

    async def _rec_setbtn(update, context, bot_id, owner_id):
        setbtn_calls.append((bot_id, owner_id))
    A.handle_set_buttons_callback = _rec_setbtn
    try:
        ctx4 = FakeCtx()
        q4 = FakeQuery(ctx4, uid=999)
        q4.data = "setbtn_ua123_55"
        run(A.callback_handler(_fake_update(q=q4, uid=999), ctx4))
        check("panel routing: setbtn_ bhi chala", setbtn_calls == [("ua123", 999)], str(setbtn_calls))
    finally:
        A.handle_set_buttons_callback = real_setbtn

    # editing state (_runtime_store) bhi route ho
    routed2 = []
    real_handler2 = A.handle_user_bot_message

    async def _rec2(msg, context, bot_id, owner_id):
        routed2.append(bot_id)
    A.handle_user_bot_message = _rec2
    try:
        ctx5 = FakeCtx()
        ctx5.user_data["999_ua123"] = {"editing_text_msg_id": 55}
        ok2 = run(A._route_user_account_owner_message(FakeMsg(), ctx5, 999))
        check("owner flow: editing state route", ok2 is True and routed2 == ["ua123"], str(routed2))
    finally:
        A.handle_user_bot_message = real_handler2

    # stale login GC
    A._UA_LOGINS[1234] = {"step": "code", "client": FakeTLClient(),
                          "started_at": A.time.monotonic() - 5000}
    closed = run(A.ua_login_gc())
    check("login GC: adhura login band", closed >= 1 and A.ua_login_state(1234) is None, str(closed))

    routed = []
    real_handler = A.handle_user_bot_message

    async def _rec_handler(msg, context, bot_id, owner_id):
        routed.append((bot_id, owner_id))
    A.handle_user_bot_message = _rec_handler
    try:
        ctx2 = FakeCtx()
        ctx2.user_data["adding_channel_ua123"] = True
        ok = run(A._route_user_account_owner_message(FakeMsg(), ctx2, 999))
        check("owner flow: adding_channel route", ok is True and routed == [("ua123", 999)], str(routed))
        ctx3 = FakeCtx()
        ctx3.user_data["adding_channel_b1"] = True
        check("owner flow: bot account route nahi", run(A._route_user_account_owner_message(FakeMsg(), ctx3, 999)) is False)
    finally:
        A.handle_user_bot_message = real_handler


class FakeBotFactory:
    """A.Bot(token=...) ki jagah - token valid ho to get_me() deta hai."""

    def __init__(self, valid_tokens=("123456789:ABCdefGHIjklMNOpqrSTUvwxYZ123456789",)):
        self.valid_tokens = tuple(valid_tokens)

    def __call__(self, token=None, **kw):
        outer = self

        class _TB:
            def __init__(self):
                self.token = token

            async def get_me(self):
                if token in outer.valid_tokens:
                    return SimpleNamespace(username="clientbot", id=555)
                raise Exception("Unauthorized")
        return _TB()


def test_admin_add_account_wizard():
    print("\n[13] admin ADD ACCOUNT wizard (step-by-step)")
    A.db.user_bots = []
    A.db.added_bots = []
    ctx = FakeCtx()
    ctx.user_data.clear()
    admin = A.ADMIN_USER_ID  # harness me yahi id admin hai
    _saved_creds = (A.TELEGRAM_API_ID, A.TELEGRAM_API_HASH)
    A.TELEGRAM_API_ID, A.TELEGRAM_API_HASH = "12345", "abcdef123456"

    # --- chooser
    q = FakeQuery(ctx, uid=admin)
    q.data = "admin_add_userbot"
    run(A.callback_handler(_fake_update(q=q, uid=admin), ctx))
    labels = _kb_labels(q.edits[-1][1].get("reply_markup"))
    check("chooser: dono option dikhe", any("Bot Account" in l for l in labels) and
          any("User Account" in l for l in labels), str(labels))
    check("chooser: purana state clear", A._admin_add_state(ctx) is None)

    # --- bot path: chooser -> user_id -> token
    q2 = FakeQuery(ctx, uid=admin)
    q2.data = "admin_add_bot"
    run(A.callback_handler(_fake_update(q=q2, uid=admin), ctx))
    check("bot: user_id maanga", "USER ID (1/3)" in q2.edits[-1][0], q2.edits[-1][0][:60])
    check("bot: state set", (A._admin_add_state(ctx) or {}).get("step") == "user_id", str(A._admin_add_state(ctx)))

    msg = FakeMsg(text="123456789")
    run(A.handle_message(_fake_update(msg=msg, uid=admin), ctx))
    check("bot: token maanga", any("BOT TOKEN (2/3)" in (t or "") for t, _ in msg.replies),
          str([t[:40] for t, _ in msg.replies]))
    check("bot: user_id yaad rahi", (A._admin_add_state(ctx) or {}).get("user_id") == 123456789,
          str(A._admin_add_state(ctx)))

    msg_bad = FakeMsg(text="galat-token")
    run(A.handle_message(_fake_update(msg=msg_bad, uid=admin), ctx))
    check("bot: galat token par dobara maanga", any("Token galat format" in (t or "") for t, _ in msg_bad.replies),
          str([t[:50] for t, _ in msg_bad.replies]))
    check("bot: state token par hi rahi", (A._admin_add_state(ctx) or {}).get("step") == "token",
          str(A._admin_add_state(ctx)))

    real_bot = A.Bot
    A.Bot = FakeBotFactory()
    try:
        msg_token = FakeMsg(text="123456789:ABCdefGHIjklMNOpqrSTUvwxYZ123456789")
        run(A.handle_message(_fake_update(msg=msg_token, uid=admin), ctx))
    finally:
        A.Bot = real_bot
    check("bot: account add hua", any(b[0] == 123456789 and b[2] == "clientbot" for b in A.db.added_bots),
          str(A.db.added_bots))
    check("bot: token message delete hua", getattr(msg_token, "deleted", False) is True,
          str(getattr(msg_token, "deleted", None)))
    check("bot: success me subscription button",
          any("Subscription" in l for l in _kb_labels(msg_token.replies[-1][1].get("reply_markup"))),
          str(_kb_labels(msg_token.replies[-1][1].get("reply_markup"))))
    check("bot: state clear ho gayi", A._admin_add_state(ctx) is None)

    # --- quick subscription: "30 Basic" kaafi hai
    bot_id = A.db.added_bots[-1][3]
    qs = FakeQuery(ctx, uid=admin)
    qs.data = f"admin_quick_sub_{bot_id}"
    run(A.callback_handler(_fake_update(q=qs, uid=admin), ctx))
    check("quick sub: bot prefill set", ctx.user_data.get("admin_add_sub_bot") == bot_id,
          str(ctx.user_data.get("admin_add_sub_bot")))
    db_test = A.db
    real_start_bot = A.start_user_bot
    started_bots = []

    async def _fake_start(bot_token, bot_id, owner_id, quiet=False):
        started_bots.append(bot_id)
        return True
    A.start_user_bot = _fake_start
    msg_sub = FakeMsg(text="30 Basic")
    try:
        run(A.handle_message(_fake_update(msg=msg_sub, uid=admin), ctx))
    finally:
        A.start_user_bot = real_start_bot
    check("quick sub: 30 Basic se subscription lag gayi", db_test.subs.get(bot_id) is not None,
          str(db_test.subs))
    check("quick sub: prefill clear", ctx.user_data.get("admin_add_sub_bot") is None)

    # --- user account path: chooser -> user_id -> phone -> OTP
    ctx.user_data.clear()
    A.db.user_account_calls = []
    q3 = FakeQuery(ctx, uid=admin)
    q3.data = "admin_add_ua"
    run(A.callback_handler(_fake_update(q=q3, uid=admin), ctx))
    check("user: user_id maanga", "USER ID (1/3)" in q3.edits[-1][0], q3.edits[-1][0][:60])

    msg_uid = FakeMsg(text="not-a-number")
    run(A.handle_message(_fake_update(msg=msg_uid, uid=admin), ctx))
    check("user: galat user_id reject", any("user ID nahi lag rahi" in (t or "") for t, _ in msg_uid.replies),
          str([t[:50] for t, _ in msg_uid.replies]))

    msg_uid2 = FakeMsg(text="987654321")
    run(A.handle_message(_fake_update(msg=msg_uid2, uid=admin), ctx))
    check("user: phone maanga", any("PHONE NUMBER (2/3)" in (t or "") for t, _ in msg_uid2.replies),
          str([t[:45] for t, _ in msg_uid2.replies]))

    A._UA_LOGINS.clear()
    real_tc_admin = A.TelegramClient
    A.TelegramClient = lambda *a, **k: FakeTLClient()
    msg_phone = FakeMsg(text="+91 98765-43210")
    try:
        run(A.handle_message(_fake_update(msg=msg_phone, uid=admin), ctx))
    finally:
        A.TelegramClient = real_tc_admin
    check("user: phone normalize hokar login shuru", any("OTP BHEJ DIYA" in (t or "") or "OTP" in (t or "")
                                                        for t, _ in msg_phone.replies),
          str([t[:45] for t, _ in msg_phone.replies]))
    login_state = A.ua_login_state(admin)
    check("user: login wizard chalu", bool(login_state) and login_state.get("step") == "code",
          str(login_state))
    check("user: target owner yaad hai", (login_state or {}).get("owner_id") == 987654321,
          str(login_state))
    check("user: wizard state handover ke baad clear", A._admin_add_state(ctx) is None)

    # OTP -> account save
    msg_code = FakeMsg(text="55555")
    real_start_after_login = A.start_user_bot
    after_login_started = []

    async def _fake_after_login(bot_token, bot_id, owner_id, quiet=False):
        after_login_started.append(bot_id)
        return True
    A.start_user_bot = _fake_after_login
    try:
        run(A.handle_message(_fake_update(msg=msg_code, uid=admin), ctx))
    finally:
        A.start_user_bot = real_start_after_login
    check("user: login ke baad account start hua", after_login_started == ["ua123"], str(after_login_started))
    row = A.db.get_user_bot("ua123")
    check("user: account DB me aa gaya", bool(row) and row.get("account_type") == "user", str(row))
    check("user: admin ko subscription hint",
          any("subscription" in (t or "").lower() for t, _ in msg_code.replies),
          str([t[:60] for t, _ in msg_code.replies]))

    # --- cancel + purana ek-line format (backward compat)
    ctx.user_data.clear()
    q4 = FakeQuery(ctx, uid=admin)
    q4.data = "admin_add_bot"
    run(A.callback_handler(_fake_update(q=q4, uid=admin), ctx))
    q5 = FakeQuery(ctx, uid=admin)
    q5.data = "admin_add_cancel"
    run(A.callback_handler(_fake_update(q=q5, uid=admin), ctx))
    check("cancel: state clear", A._admin_add_state(ctx) is None)

    ctx.user_data.clear()
    q6 = FakeQuery(ctx, uid=admin)
    q6.data = "admin_add_bot"
    run(A.callback_handler(_fake_update(q=q6, uid=admin), ctx))
    real_bot2 = A.Bot
    A.Bot = FakeBotFactory()
    try:
        legacy = FakeMsg(text="444555666 123456789:ABCdefGHIjklMNOpqrSTUvwxYZ123456789")
        run(A.handle_message(_fake_update(msg=legacy, uid=admin), ctx))
    finally:
        A.Bot = real_bot2
    check("legacy: ek line format ab bhi chalta hai",
          any(b[0] == 444555666 for b in A.db.added_bots), str(A.db.added_bots[-2:]))

    # --- bot token ke bina (khaali text) par crash nahi
    ctx.user_data.clear()
    q7 = FakeQuery(ctx, uid=admin)
    q7.data = "admin_add_bot"
    run(A.callback_handler(_fake_update(q=q7, uid=admin), ctx))
    empty = FakeMsg(text=None)
    run(A.handle_message(_fake_update(msg=empty, uid=admin), ctx))
    check("empty text: soft error", any("Text bhejo" in (t or "") for t, _ in empty.replies),
          str([t[:40] for t, _ in empty.replies]))
    A._admin_add_clear(ctx)

    # --- non-admin ke liye wizard chalega hi nahi
    ctx2 = FakeCtx()
    ctx2.user_data.clear()
    A._admin_add_set(ctx2, "bot", "user_id")
    other = FakeMsg(text="123456789")
    run(A.handle_message(_fake_update(msg=other, uid=A.ADMIN_USER_ID + 777), ctx2))
    check("non-admin: wizard ignore", (A._admin_add_state(ctx2) or {}).get("step") == "user_id",
          str(A._admin_add_state(ctx2)))
    A._admin_add_clear(ctx2)
    A.TELEGRAM_API_ID, A.TELEGRAM_API_HASH = _saved_creds


def test_html_safety_and_welcome_spam():
    print("\n[14] HTML safety net + welcome ERROR spam")

    # --- sanitize: allowed tags rehne chahiye
    check("sanitize: tg-emoji safe", '<tg-emoji emoji-id="5000000001">' in
          A.sanitize_telegram_html('<tg-emoji emoji-id="5000000001">💎</tg-emoji>'))
    check("sanitize: blockquote/b/code safe", A.sanitize_telegram_html("<blockquote><b>x</b></blockquote>")
          == "<blockquote><b>x</b></blockquote>")
    check("sanitize: a href safe",
          A.sanitize_telegram_html('<a href="https://t.me/x">x</a>')
          == '<a href="https://t.me/x">x</a>')
    check("sanitize: plain <id> escape", '&lt;id>' in A.sanitize_telegram_html("printf 'X=<id>'"))
    check("sanitize: lone < escape", '&lt;' in A.sanitize_telegram_html("5 < 6"))
    check("sanitize: bina tag wala text untouched (double-escape nahi)",
          A.sanitize_telegram_html("a & b, 100% ok") == "a & b, 100% ok")
    check("sanitize: khaali text", A.sanitize_telegram_html("") == "")

    # --- asli bug: TELEGRAM_API_HINT HTML me bhejne par reject nahi hona chahiye
    for text, label in ((A.TELEGRAM_API_HINT, "hint"), (A.ua_login_error_text_check()
                                                        if hasattr(A, "ua_login_error_text_check") else
                                                        A.TELEGRAM_API_HINT, "login error")):
        prem = A.sanitize_telegram_html(A.premiumize_ui_emojis(text))
        stray = _STRAY_TAG_RE.search(prem)
        check(f"telegram-safe: {label}", stray is None, str(stray.group(0) if stray else ""))

    # hint me koi raw <placeholder> nahi (asli wajah)
    raw = re.findall(r"<[a-zA-Z_][a-zA-Z0-9_]*>", A.TELEGRAM_API_HINT)
    check("hint me raw <id>/<hash> nahi", not raw, str(raw))
    check("hint me asli command hai", "TELEGRAM_API_ID=" in A.TELEGRAM_API_HINT)

    # --- purane DB ka NOT NULL constraint (user account insert fail ho jata tha)
    src_now = open(A.__file__, encoding="utf-8").read()
    check("migration: bot_token NOT NULL hataya",
          "ALTER TABLE user_bots ALTER COLUMN bot_token DROP NOT NULL" in src_now)
    check("migration: insert me bot_token NULL jata hai",
          "(bot_id, user_id, bot_token, bot_username, is_active," in src_now
          and "VALUES (%s,%s,NULL,%s,0,'user'" in src_now)

    # --- api_hash jaise literal secrets bhi mask hone chahiye
    import os as _os
    _os.environ["TELEGRAM_API_HASH"] = "d927c13beaaf5110f25c505b7c071273"
    masked = A.mask_secrets("fail: DETAIL: Failing row contains (ua1, 1, null, d927c13beaaf5110f25c505b7c071273)")
    check("mask: api_hash chhupa", "d927c13beaaf5110f25c505b7c071273" not in masked, masked[:80])
    _os.environ.pop("TELEGRAM_API_HASH", None)

    # --- example/dummy api values par saaf message (Telegram ka asli error)
    fake_status = ("error:The api_id/api_hash combination is invalid "
                   "(caused by SendCodeRequest)")
    friendly = A._ua_login_error(fake_status)
    check("api invalid: asli values ka rasta batata hai", "my.telegram.org" in friendly and "asli" in friendly,
          friendly[:90])
    check("api invalid: HTML safe", _STRAY_TAG_RE.search(A.sanitize_telegram_html(friendly)) is None,
          str(_STRAY_TAG_RE.search(A.sanitize_telegram_html(friendly))))
    check("api invalid: example values ka zikr", "example/dummy" in friendly, friendly[:90])

    # --- strict bot par poora login error path (jaise user ko dikhta hai)
    ctx = FakeCtx()
    ctx.user_data.clear()
    admin = A.ADMIN_USER_ID
    A._UA_LOGINS.clear()
    A._admin_add_set(ctx, "user", "user_id")
    m0 = HtmlStrictMsg(text="987654321")
    run(A.handle_message(_fake_update(msg=m0, uid=admin), ctx))
    check("strict: phone step message gaya", any("PHONE NUMBER" in (t or "") for t, _ in m0.replies),
          str([t[:40] for t, _ in m0.replies]))

    saved_pair = (A.TELEGRAM_API_ID, A.TELEGRAM_API_HASH)
    A.TELEGRAM_API_ID, A.TELEGRAM_API_HASH = "", ""
    handler, root, old_level = _capture_logs()
    try:
        m1 = HtmlStrictMsg(text="+19312837172")
        run(A.handle_message(_fake_update(msg=m1, uid=admin), ctx))
    finally:
        _stop_capture(handler, root, old_level)
    A.TELEGRAM_API_ID, A.TELEGRAM_API_HASH = saved_pair
    check("strict: creds missing par reply aaya", bool(m1.replies), str(len(m1.replies)))
    err_logs = [r for r in handler.records if r.levelno >= logging.ERROR]
    check("strict: koi ERROR nahi (parse fail nahi hua)", not err_logs,
          str([r.getMessage()[:70] for r in err_logs]))
    warns = [r for r in handler.records if r.levelno == logging.WARNING]
    check("strict: ek saaf warning log hui",
          any("TELEGRAM_API_ID" in r.getMessage() or "credentials" in r.getMessage().lower() for r in warns),
          str([r.getMessage()[:70] for r in warns]))
    check("strict: state phone par hi rahi (dobara try kar sakta hai)",
          (A._UA_LOGINS.get(admin) or {}).get("step") == "phone", str(A._UA_LOGINS.get(admin)))
    A._UA_LOGINS.pop(admin, None)
    A._admin_add_clear(ctx)

    # --- welcome: blocked user par ERROR spam band + permanently mark
    A.db.gone_calls = []
    A.db.unreachable_calls = []
    A.db.join_requests = []
    A.db.leave = {"messages": []}
    A.db.subs = {}
    blocked = FakeBot("blocked")

    async def _boom(*a, **k):
        raise A.Forbidden("Forbidden: bot was blocked by the user")
    blocked.send_message = _boom
    blocked.send_photo = _boom
    blocked.send_document = _boom
    requester = SimpleNamespace(id=4242, first_name="Blocked", username="b", last_name="", is_bot=False)
    handler2, root2, old2 = _capture_logs()
    try:
        run(A.process_join_request("b1", 999, requester, -100123, "Test Channel", None,
                                   sender=blocked, approve=None, auto=True))
    finally:
        _stop_capture(handler2, root2, old2)
    errs = [r for r in handler2.records if r.levelno >= logging.ERROR]
    wrns = [r for r in handler2.records if r.levelno == logging.WARNING]
    check("welcome: koi ERROR nahi", not errs, str([r.getMessage()[:70] for r in errs]))
    check("welcome: ek warning (skip)", len(wrns) == 1, str([r.getMessage()[:70] for r in wrns]))
    check("welcome: user permanently mark hua", any(c[1] == 4242 for c in A.db.gone_calls),
          str(A.db.gone_calls))
    check("welcome: blocked par baar-baar try nahi (1 hi warning)",
          sum(1 for r in wrns if "welcome DM skip" in r.getMessage()) == 1,
          str([r.getMessage()[:60] for r in wrns]))

    # --- normal user par welcome phir bhi jata hai
    A.db.gone_calls = []
    good = FakeBot("good")
    handler3, root3, old3 = _capture_logs()
    try:
        run(A.process_join_request("b1", 999, SimpleNamespace(id=4343, first_name="Ravi",
                                                              username="ravi", last_name="", is_bot=False),
                                   -100123, "Test Channel", None, sender=good, approve=None, auto=True))
    finally:
        _stop_capture(handler3, root3, old3)
    check("welcome: normal user ko message gaya", bool(good.calls), str(good.calls[:1])[:110])
    check("welcome: normal user ERROR free",
          not [r for r in handler3.records if r.levelno >= logging.ERROR],
          str([r.getMessage()[:60] for r in handler3.records if r.levelno >= logging.ERROR]))


def test_subscription_picker_and_style_memory():
    print("\n[15] subscription picker + premium styling memory")
    A.reset_premium_styling_state()

    # --- picker list: sab accounts buttons me
    A.db.user_bots = [
        {"bot_id": "b1", "bot_username": "one", "bot_token": "t1", "user_id": 1, "account_type": "bot"},
        {"bot_id": "ua777", "bot_username": None, "phone": "+919876543210", "user_id": 2,
         "account_type": "user", "bot_token": None},
    ]
    A.db.subs = {"b1": {"subscription_type": "Pro", "expiry_date": A.now_aware() + timedelta(days=5)}}
    ctx = FakeCtx()
    q = FakeQuery(ctx, uid=A.ADMIN_USER_ID)
    q.data = "admin_add_sub"
    run(A.callback_handler(_fake_update(q=q, uid=A.ADMIN_USER_ID), ctx))
    labels = _kb_labels(q.edits[-1][1].get("reply_markup"))
    check("picker: dono account dikhe", any("one" in l for l in labels) and any("919876543210"[-10:] in l for l in labels),
          str(labels))
    check("picker: active plan dikhe", any("Pro" in l for l in labels), str(labels))
    check("picker: manual option", any("Khud type" in l for l in labels), str(labels))
    check("picker: text me days/plan hint", "30 Basic" in q.edits[-1][0], q.edits[-1][0][:80])

    # --- choose karke sirf "30 Basic" bhejna
    q2 = FakeQuery(ctx, uid=A.ADMIN_USER_ID)
    q2.data = "admin_quick_sub_ua777"
    run(A.callback_handler(_fake_update(q=q2, uid=A.ADMIN_USER_ID), ctx))
    check("picker: bot prefill set", ctx.user_data.get("admin_add_sub_bot") == "ua777",
          str(ctx.user_data.get("admin_add_sub_bot")))
    real_start_bot = A.start_user_bot
    started = []

    async def _fake_start(bot_token, bot_id, owner_id, quiet=False):
        started.append(bot_id)
        return True
    A.start_user_bot = _fake_start
    msg = FakeMsg(text="30 Basic")
    try:
        run(A.handle_message(_fake_update(msg=msg, uid=A.ADMIN_USER_ID), ctx))
    finally:
        A.start_user_bot = real_start_bot
    check("picker: user account ko subscription lag gayi", A.db.subs.get("ua777") is not None, str(A.db.subs))
    check("picker: account auto-start hua", started == ["ua777"], str(started))
    check("picker: prefill clear", ctx.user_data.get("admin_add_sub_bot") is None)

    # --- manual option purana format bacha rahe
    q3 = FakeQuery(ctx, uid=A.ADMIN_USER_ID)
    q3.data = "admin_add_sub_manual"
    run(A.callback_handler(_fake_update(q=q3, uid=A.ADMIN_USER_ID), ctx))
    check("manual: state set + hint", ctx.user_data.get("admin_add_sub") is True
          and "bot_id days Plan" in q3.edits[-1][0], q3.edits[-1][0][:70])
    ctx.user_data.clear()

    # --- login ke baad admin ko subscription button (apne hi account par bhi)
    A._UA_LOGINS.clear()
    A.db.user_bots = []
    A.db.user_account_calls = []
    ctx2 = FakeCtx()
    ctx2.user_data.clear()
    A.db.add_subscription_for_bot = A.db.add_subscription_for_bot  # (no-op)
    A.db.subs = {}
    st = run(A.ua_login_start(777, "+919000000000", client_factory=lambda: FakeTLClient()))
    run(A.ua_login_submit_code(777, "55555"))
    check("login: account add hua", st == "code" and A.db.get_user_bot("ua123") is not None)

    # --- premium styling: 2 fail ke baad disabled + no warning spam
    A.reset_premium_styling_state()
    check("style: default on", A.premium_styling_disabled() is False)
    check("style: document_invalid note", A.note_premium_failure(A.BadRequest("Document_invalid")) is True)
    check("style: 1 fail par abhi on", A.premium_styling_disabled() is False)
    A.note_premium_failure(A.BadRequest("Document_invalid"))
    check("style: 2 fail ke baad disabled", A.premium_styling_disabled() is True)
    check("style: unrelated error note nahi", A.note_premium_failure(A.BadRequest("chat not found")) is False)

    # disabled state me warning log nahi aani chahiye (sirf plain send)
    handler, root, old_level = _capture_logs()
    try:
        sent = run(A.send_premium_message(FakeBot("style"), 555, "Hello 💎 premium"))
    finally:
        _stop_capture(handler, root, old_level)
    warns = [r for r in handler.records if r.levelno >= logging.WARNING]
    check("style: disabled par koi styled warning nahi", not warns,
          str([r.getMessage()[:60] for r in warns]))
    check("style: plain message chala gaya", sent is not None)
    A.reset_premium_styling_state()
    check("style: reset ke baad on", A.premium_styling_disabled() is False)


def test_user_account_dm_start():
    print("\n[17] user account ke DM me /start -> panel wapas (\"kuch nhi aa rha\" fix)")
    A.reset_premium_styling_state()
    A.db.user_bots = [{"bot_id": "ua8394878310", "user_id": A.ADMIN_USER_ID, "account_type": "user",
                       "bot_username": None, "phone": "+19312837172", "bot_token": None,
                       "session_string": FAKE_SESSION, "api_id": 1, "api_hash": "h"}]
    A.db.subs = {"ua8394878310": {"subscription_type": "Basic",
                                  "expiry_date": A.now_aware() + timedelta(days=30), "max_channels": 1}}
    A.db.bot_channels = []
    A._UA_SUPPORT_GREETED.clear()

    fake = FakeTLClient(me_id=8394878310)
    sender = A.UserAccountSender("ua8394878310", A.ADMIN_USER_ID, fake,
                                 phone="+19312837172", account_user_id=8394878310)

    handled = run(A._ua_handle_owner_start(sender, "ua8394878310", A.ADMIN_USER_ID,
                                          FakeMsg(text="/start"), "/start"))
    check("dm start: handled", handled is True)
    texts = [m["text"] for m in fake.sent_messages]
    check("dm start: panel DM gaya", any("MANAGE USER ACCOUNT" in t for t in texts), str(texts)[:180])
    check("dm start: panel me account id", any("ua8394878310" in t for t in texts))
    check("dm start: plan dikhaya", any("Basic" in t for t in texts))
    check("dm start: panel manager ke DM me", _peer_uid(fake.sent_messages[0]["chat_id"]) == 8394878310)
    check("dm start: /start wale message par reply", fake.sent_messages[-1].get("reply_to") is not None)
    check("dm start: page ke baad hint", any("Panel upar bhej diya" in t for t in texts))

    fake.sent_messages.clear()
    check("dm: normal text ignore", run(A._ua_handle_owner_start(
        sender, "ua8394878310", A.ADMIN_USER_ID, FakeMsg(text="hello"), "hello")) is False)
    check("dm: normal text par koi message nahi", fake.sent_messages == [], str(fake.sent_messages))

    if A.tl_events is None:
        print("  skip telethon events (telethon install nahi)")
        return
    fake2 = FakeTLClient(me_id=8394878310)
    sender2 = A.UserAccountSender("ua8394878310", A.ADMIN_USER_ID, fake2,
                                  phone="+19312837172", account_user_id=8394878310)
    A._register_user_account_handlers(fake2, sender2, "ua8394878310", A.ADMIN_USER_ID)
    check("dm: handlers register hue (action + DM + raw)", len(fake2.handlers) >= 3, str(len(fake2.handlers)))

    class _DMEvent:
        is_private = True
        raw_text = "/start"
        sender_id = A.ADMIN_USER_ID
        message = FakeMsg(text="/start")

        async def get_chat(self):
            return None

        async def get_user(self):
            return None

    handler, root, old_level = _capture_logs()
    try:
        for h in fake2.handlers:
            run(h(_DMEvent()))
    finally:
        _stop_capture(handler, root, old_level)
    errors = [r for r in handler.records if r.levelno >= logging.ERROR]
    check("dm: handler chain me koi ERROR nahi", not errors,
          str([r.getMessage() for r in errors])[:200])
    check("dm: /start par panel gaya", any("MANAGE USER ACCOUNT" in m["text"] for m in fake2.sent_messages),
          str([m["text"][:50] for m in fake2.sent_messages]))
    check("dm: /start log me dikha",
          any("DM me /start aaya" in r.getMessage() for r in handler.records),
          str([r.getMessage() for r in handler.records if "DM" in r.getMessage()])[:160])

    class _StrangerEvent(_DMEvent):
        sender_id = 999888777

    fake2.sent_messages.clear()
    for h in fake2.handlers:
        run(h(_StrangerEvent()))
    check("dm: stranger ko ek support reply",
          any("support account" in m["text"] for m in fake2.sent_messages),
          str([m["text"][:60] for m in fake2.sent_messages]))
    fake2.sent_messages.clear()
    for h in fake2.handlers:
        run(h(_StrangerEvent()))
    check("dm: same stranger ko dobara nahi (spam band)", fake2.sent_messages == [])


def test_user_account_premium_emoji_fallback():
    print("\n[18] custom emoji reject -> plain emoji fallback (\"DM me panel nahi ja paya\" fix)")
    A.reset_premium_styling_state()
    A._UA_PREMIUM_EMOJI_OK.clear()
    A.db.user_bots = [{"bot_id": "ua8394878310", "user_id": A.ADMIN_USER_ID, "account_type": "user",
                       "bot_username": None, "phone": "+19312837172", "bot_token": None,
                       "session_string": FAKE_SESSION, "api_id": 1, "api_hash": "h"}]
    A.db.subs = {"ua8394878310": {"subscription_type": "Basic",
                                  "expiry_date": A.now_aware() + timedelta(days=30), "max_channels": 1}}
    A.db.bot_channels = []
    A._UA_SUPPORT_GREETED.clear()
    premium_tag = A.pp("👤")
    check("setup: pp() premium tag deta hai", "tg-emoji" in premium_tag, premium_tag[:60])

    # --- case 1: account Premium nahi hai -> pehle se plain (koi fail nahi)
    fake = FakeTLClient(me_id=8394878310, mode="doc_invalid", me_premium=False)
    sender = A.UserAccountSender("ua8394878310", A.ADMIN_USER_ID, fake, phone="+19312837172",
                                 account_user_id=8394878310, premium=False)
    check("non-premium: flag set", A._UA_PREMIUM_EMOJI_OK["ua8394878310"] is False)
    run(sender.send_message(8394878310, f"{A.pp('👤')} <b>Hi</b>", parse_mode="HTML"))
    check("non-premium: pehli koshish me hi chala", fake.emoji_rejects == 0, str(fake.emoji_rejects))
    sent = fake.sent_messages[-1]["text"]
    check("non-premium: tg-emoji hata", "tg-emoji" not in sent, sent[:80])
    check("non-premium: emoji bacha", "👤" in sent and "<b>Hi</b>" in sent, sent[:80])

    # --- case 2: premium pata nahi -> pehli koshish fail, plain se retry chalta hai
    A._UA_PREMIUM_EMOJI_OK.clear()
    fake2 = FakeTLClient(me_id=8394878310, mode="doc_invalid")
    sender2 = A.UserAccountSender("ua8394878310", A.ADMIN_USER_ID, fake2,
                                  account_user_id=8394878310)
    check("unknown: flag shuru me True", A._UA_PREMIUM_EMOJI_OK.get("ua8394878310") is None)
    res = run(sender2.send_message(8394878310, f"{A.pp('🔔')} <b>Welcome</b>", parse_mode="HTML"))
    check("unknown: retry se message gaya", res is not None and len(fake2.sent_messages) == 1,
          str(fake2.sent_messages)[:120])
    check("unknown: ek hi reject (log spam nahi)", fake2.emoji_rejects == 1, str(fake2.emoji_rejects))
    final = fake2.sent_messages[-1]["text"]
    check("unknown: plain emoji wala text", "tg-emoji" not in final and "🔔" in final, final[:80])
    check("unknown: note ek baar log hua", A._UA_PREMIUM_EMOJI_OK["ua8394878310"] is False)

    # agli baar sidha plain - dobara fail nahi
    fake2.sent_messages.clear()
    run(sender2.send_message(8394878310, f"{A.pp('🔔')} dobara", parse_mode="HTML"))
    check("unknown: agli baar bina fail", fake2.emoji_rejects == 1 and len(fake2.sent_messages) == 1,
          f"rejects={fake2.emoji_rejects} sent={len(fake2.sent_messages)}")

    # --- case 3: /start ka panel DM bhi pahunchta hai (yehi user ki problem thi)
    A._UA_PREMIUM_EMOJI_OK.clear()
    fake3 = FakeTLClient(me_id=8394878310, mode="doc_invalid")
    sender3 = A.UserAccountSender("ua8394878310", A.ADMIN_USER_ID, fake3,
                                  account_user_id=8394878310)
    handled = run(A._ua_handle_owner_start(sender3, "ua8394878310", A.ADMIN_USER_ID,
                                           FakeMsg(text="/start"), "/start"))
    check("dm panel: handled", handled is True)
    check("dm panel: panel pahunch gaya", any("MANAGE USER ACCOUNT" in m["text"] for m in fake3.sent_messages),
          str([m["text"][:50] for m in fake3.sent_messages]))
    check("dm panel: panel me tg-emoji nahi", all("tg-emoji" not in m["text"] for m in fake3.sent_messages))
    check("dm panel: reply bhi gaya", fake3.sent_messages[-1].get("reply_to") is not None)

    # --- case 4: caption (media) aur album caption me bhi fallback
    A._UA_PREMIUM_EMOJI_OK.clear()
    fake4 = FakeTLClient(me_id=8394878310, mode="doc_invalid")
    sender4 = A.UserAccountSender("ua8394878310", A.ADMIN_USER_ID, fake4,
                                  account_user_id=8394878310)
    run(sender4.send_photo(8394878310, "/tmp/does-not-exist.jpg",
                           caption=f"{A.pp('📊')} Report", parse_mode="HTML"))
    check("caption: fallback se gaya", len(fake4.sent_files) == 1, str(fake4.sent_files)[:100])
    check("caption: tg-emoji nahi",
          "tg-emoji" not in (fake4.sent_files[-1].get("caption") or ""),
          str(fake4.sent_files[-1].get("caption"))[:80])

    # --- case 5: code expire ho jaye to saaf message (PhoneCodeExpiredError -> rasta saaf)
    A._UA_LOGINS[4242] = {"step": "code", "client": FakeTLClient(mode="expired_code"),
                          "phone": "+19312837172", "phone_code_hash": "h1", "operator": 4242}
    status = run(A.ua_login_submit_code(4242, "12345"))
    check("expired code: status sahi", status == "error:code_expired", str(status))
    check("expired code: message me kya karna hai",
          "expire" in A._ua_login_error(status).lower(), A._ua_login_error(status)[:80])
    A._UA_LOGINS.pop(4242, None)

    # --- case 6: koi aur error (network) ho to waisa hi raise ho (chhupana nahi)
    A._UA_PREMIUM_EMOJI_OK.clear()
    fake5 = FakeTLClient(me_id=8394878310, mode="flood")
    sender5 = A.UserAccountSender("ua8394878310", A.ADMIN_USER_ID, fake5,
                                  account_user_id=8394878310)
    raised = None
    try:
        run(sender5.send_message(8394878310, f"{A.pp('🔔')} x", parse_mode="HTML"))
    except Exception as ex:
        raised = ex
    check("flood: fallback nahi (error raise hua)", raised is not None, str(raised))
    check("flood: flag waisa hi", A._UA_PREMIUM_EMOJI_OK.get("ua8394878310") is not False)


def test_user_account_boot_without_subscription():
    print("\n[20] user account bina subscription bhi boot par chalu (\"restart ke baad chup\" fix)")
    A.reset_premium_styling_state()
    A.db.user_bots = [{"bot_id": "ua8394878310", "user_id": A.ADMIN_USER_ID, "account_type": "user",
                       "bot_username": None, "phone": "+19312837172", "bot_token": None,
                       "session_string": FAKE_SESSION, "api_id": 1, "api_hash": "h"},
                      {"bot_id": "b9", "user_id": A.ADMIN_USER_ID, "account_type": "bot",
                       "bot_username": "nobot", "bot_token": "t9", "is_active": 1}]
    A.db.subs = {}          # koi subscription nahi
    A.db.active_calls = []
    A.user_account_clients.pop("ua8394878310", None)
    started = []

    async def _fake_start(token, bot_id, owner_id, quiet=False):
        started.append(bot_id)
        return True

    orig_start = A.start_user_bot
    A.start_user_bot = _fake_start
    handler, root, old_level = _capture_logs()
    try:
        run(A.start_bots_on_boot())
    finally:
        _stop_capture(handler, root, old_level)
        A.start_user_bot = orig_start
    msgs = [r.getMessage() for r in handler.records]
    check("boot: user account start hua (sub ke bina)", "ua8394878310" in started, str(started))
    check("boot: bot account skip hua (sub ke bina)", "b9" not in started, str(started))
    check("boot: wajah log me saaf", any("user account no subscription hone par bhi chalu" in m for m in msgs),
          str(msgs)[:220])
    check("boot: bot ka skip log", any("Skipping b9" in m for m in msgs), str(msgs)[:220])
    check("boot: active mark hua", ("ua8394878310", True) in A.db.active_calls, str(A.db.active_calls)[:120])

    # --- retry job bhi sub ke bina account ko chalu kare
    started.clear()
    A.user_account_clients.pop("ua8394878310", None)
    A.start_user_bot = _fake_start
    try:
        run(A.retry_inactive_userbots_job(FakeCtx()))
    finally:
        A.start_user_bot = orig_start
    check("retry: account dobara start hua", "ua8394878310" in started, str(started))

    # --- setting off karo to purana behaviour (skip) wapas aa jaye
    A.UA_START_WITHOUT_SUBSCRIPTION = False
    started.clear()
    A.user_account_clients.pop("ua8394878310", None)
    A.start_user_bot = _fake_start
    try:
        run(A.start_bots_on_boot())
    finally:
        A.start_user_bot = orig_start
        A.UA_START_WITHOUT_SUBSCRIPTION = True
    check("setting off: purana skip behaviour", "ua8394878310" not in started, str(started))


def test_diagnostics():
    print("\n[19] diagnostics: build tag, /diag report, test DM, disconnect watchdog")
    A.reset_premium_styling_state()
    check("build tag set", bool(A.BUILD_TAG) and len(A.BUILD_TAG) > 5, A.BUILD_TAG)
    A.db.user_bots = [{"bot_id": "ua8394878310", "user_id": A.ADMIN_USER_ID, "account_type": "user",
                       "bot_username": None, "phone": "+19312837172", "bot_token": None,
                       "session_string": FAKE_SESSION, "api_id": 1, "api_hash": "h"},
                      {"bot_id": "b7", "user_id": A.ADMIN_USER_ID, "account_type": "bot",
                       "bot_username": "mybot", "bot_token": "t", "is_active": 1}]
    A.db.subs = {"ua8394878310": {"subscription_type": "Basic",
                                  "expiry_date": A.now_aware() + timedelta(days=30), "max_channels": 1}}
    A.db.bot_channels = [{"channel_id": -1001, "channel_title": "C1", "auto_approve": 0}]
    A.db.active_calls = []
    A._UA_LOGINS.clear()
    A._UA_DM_SEEN.clear()

    # --- account band hai: diag me saaf dikhe
    A.user_account_clients.pop("ua8394878310", None)
    text = A.strip_premium_emojis(A.build_diag_text())
    check("diag: build tag dikhta hai", A.BUILD_TAG in text, text[:120])
    check("diag: account id dikhta hai", "ua8394878310" in text)
    check("diag: band account saaf likha", "account band hai" in text, text)
    check("diag: plan dikhta hai", "Basic" in text)

    # --- account chalu: counters dikhein
    fake = FakeTLClient(me_id=8394878310)
    fake.connected = True
    sender = A.UserAccountSender("ua8394878310", A.ADMIN_USER_ID, fake,
                                 account_user_id=8394878310, premium=False)
    A.user_account_clients["ua8394878310"] = sender
    sender.connected_at = A.time.time()
    sender.raw_updates = 5
    sender.dm_count = 2
    sender.last_dm_at = A.time.time() - 90
    sender.last_dm_from = 8015937475
    sender.last_dm_text = "/start"
    text = A.strip_premium_emojis(A.build_diag_text())
    check("diag: chalu account", "chalu: 🟢" in text, text)
    check("diag: connected", "connected: 🟢" in text or "connected: haan" in text, text)
    check("diag: DM count", "DM mile: 2" in text, text)
    check("diag: aakhri DM", "aakhri: 1m" in text, text)
    check("diag: raw updates", "raw updates: 5" in text, text)
    check("diag: plain emoji mode", "plain emoji" in text, text)

    # --- /diag command admin ko report bhejta hai
    msg = FakeMsg(text="/diag", chat_id=A.ADMIN_USER_ID, from_user=A._RouterUser(A.ADMIN_USER_ID, "Admin"))
    handler, root, old_level = _capture_logs()
    try:
        run(A.diag_command(_fake_update(msg=msg, uid=A.ADMIN_USER_ID), FakeCtx()))
    finally:
        _stop_capture(handler, root, old_level)
    replies = [t or "" for t, _ in msg.replies]
    check("diag cmd: report gaya", any("DIAGNOSTICS" in t for t in replies), str([t[:40] for t in replies]))
    check("diag cmd: log me bhi gaya", any("DIAGNOSTICS" in r.getMessage() for r in handler.records),
          str([r.getMessage()[:40] for r in handler.records])[:120])
    check("diag cmd: non-admin ko nahi", _check_non_admin_diag())

    # --- admin panel button
    mctx = FakeCtx()
    mq = FakeQuery(mctx, uid=A.ADMIN_USER_ID)
    mq.data = "admin_diag"
    run(A.callback_handler(_fake_update(q=mq, uid=A.ADMIN_USER_ID), mctx))
    check("diag button: panel se khula", any("DIAGNOSTICS" in (t or "") for t, _ in mq.edits),
          str([t[:40] for t, _ in mq.edits]))
    check("diag button: test DM button bhi", any("diag_test_ua8394878310" in
          [b.get("callback_data") for row in (kw.get("reply_markup").to_dict()["inline_keyboard"] if kw.get("reply_markup") else []) for b in row]
          for _t, kw in mq.edits), "test DM button missing")

    # --- test DM button: account se asli DM jata hai
    fake.sent_messages.clear()
    tq = FakeQuery(FakeCtx(), uid=A.ADMIN_USER_ID)
    tq.data = "diag_test_ua8394878310"
    handler, root, old_level = _capture_logs()
    try:
        run(A.callback_handler(_fake_update(q=tq, uid=A.ADMIN_USER_ID), FakeCtx()))
    finally:
        _stop_capture(handler, root, old_level)
    check("test dm: DM gaya", len(fake.sent_messages) == 1, str(fake.sent_messages)[:80])
    check("test dm: test text", "Test DM" in fake.sent_messages[-1]["text"], fake.sent_messages[-1]["text"][:60])
    check("test dm: manager ko gaya", _peer_uid(fake.sent_messages[-1]["chat_id"]) == A.ADMIN_USER_ID)
    check("test dm: log me ok", any("test DM (auto) -> " in r.getMessage() and ": ok" in r.getMessage()
                                    for r in handler.records),
          str([r.getMessage() for r in handler.records if "test DM" in r.getMessage()])[:120])
    check("test dm: reply me confirm", any("Test DM bhej diya" in (t or "") for t, _ in tq.edits),
          str([t[:50] for t, _ in tq.edits]))

    # --- watchdog: active par disconnected client -> registry saaf + dobara start try
    dead = FakeTLClient(me_id=8394878310)
    dead.connected = False
    dead_sender = A.UserAccountSender("ua8394878310", A.ADMIN_USER_ID, dead, account_user_id=8394878310)
    A.user_account_clients["ua8394878310"] = dead_sender
    started = []

    async def _fake_start(token, bot_id, owner_id, quiet=False):
        started.append(bot_id)      # asli network connection test me nahi chahiye
        return True

    orig_start = A.start_user_bot
    A.start_user_bot = _fake_start
    handler, root, old_level = _capture_logs()
    try:
        run(A.retry_inactive_userbots_job(FakeCtx()))
    finally:
        _stop_capture(handler, root, old_level)
        A.start_user_bot = orig_start
    msgs = [r.getMessage() for r in handler.records]
    check("watchdog: disconnect pakda", any("disconnect mila" in m for m in msgs), str(msgs)[:200])
    check("watchdog: dead client registry se hata", A.user_account_clients.get("ua8394878310") is not dead_sender)
    check("watchdog: account dobara start try hua", "ua8394878310" in started, str(started))
    A.user_account_clients.pop("ua8394878310", None)


def _check_non_admin_diag():
    """Non-admin /diag bheje to kuch na aaye."""
    msg = FakeMsg(text="/diag", chat_id=555, from_user=A._RouterUser(555, "Random"))
    run(A.diag_command(_fake_update(msg=msg, uid=555), FakeCtx()))
    return msg.replies == []


def test_dm_user_account_fallback():
    print("\n[21] bot DM na bhej paye to user account se (\"bot can't initiate conversation\" fix)")
    A.reset_premium_styling_state()
    A._DM_INITIATE_WARNED.clear()
    A.db.leave = {"enabled": True, "target_channel_id": -100999,
                  "target_channel_link": "https://t.me/joinchat/x",
                  "messages": [{"text": "Hello {first_name}, wapas aao", "buttons_json": ""}]}
    A.db.gone_calls = []
    A.db.unreachable_calls = []
    A.db.reachable_calls = []
    A.db.user_bots = [{"bot_id": "b1", "user_id": 999, "account_type": "bot",
                       "bot_username": "chanbot", "bot_token": "t1", "is_active": 1},
                      {"bot_id": "ua8394878310", "user_id": 999, "account_type": "user",
                       "bot_username": None, "phone": "+19312837172", "bot_token": None,
                       "session_string": FAKE_SESSION, "api_id": 1, "api_hash": "h"}]
    A.db.bot_channels = [{"channel_id": -100123, "channel_title": "Chan", "auto_approve": 0}]
    A.db.subs = {"b1": {"subscription_type": "Basic",
                        "expiry_date": A.now_aware() + timedelta(days=30), "max_channels": 1}}
    A.db.messages = {}

    # owner ka user account chalu hai -> fallback ready
    acct_client = FakeTLClient(me_id=8394878310)
    acct = A.UserAccountSender("ua8394878310", 999, acct_client, account_user_id=8394878310)
    A.user_account_clients["ua8394878310"] = acct
    check("fallback: candidates me account shamil", A.dm_sender_candidates("b1") == [acct],
          str(A.dm_sender_candidates("b1")))

    # --- join request: bot "can't initiate" -> welcome user account se
    ctx = FakeCtx()
    ctx.bot = CantInitiateBot("chanbot")
    A.db.messages = {}
    member = SimpleNamespace(id=777001, first_name="Ravi", last_name="", username="ravi", is_bot=False)
    jr_update = SimpleNamespace(chat_join_request=SimpleNamespace(
        from_user=member, chat=SimpleNamespace(id=-100123, title="Chan", username="chan"),
        approve=AsyncNoop()))
    handler, root, old_level = _capture_logs()
    try:
        run(A.handle_join_request(jr_update, ctx, "b1", 999))
    finally:
        _stop_capture(handler, root, old_level)
    msgs = [r.getMessage() for r in handler.records]
    errors = [r.getMessage() for r in handler.records if r.levelno >= logging.ERROR]
    check("join: koi ERROR nahi", not errors, str(errors[:2]))
    sent_texts = [(m.get("text") or "") for m in acct_client.sent_messages]
    check("join: default msg + welcome dono account se", len(sent_texts) == 2, str(sent_texts)[:150])
    check("join: welcome text account se gaya",
          any("Aapki request mil gayi hai" in t or "Ravi" in t for t in sent_texts), str(sent_texts)[:150])
    check("join: fallback log saaf", any("user account" in m and "chali gayi" in m for m in msgs),
          str(msgs)[:200])
    check("join: permanent mark NAHI hua", A.db.gone_calls == [], str(A.db.gone_calls))

    # --- leave recovery: bot fail -> account se, phir bhi record hota hai
    A.db.gone_calls = []
    acct_client.sent_messages.clear()
    leave_update = SimpleNamespace(chat_member=SimpleNamespace(
        chat=SimpleNamespace(id=-100123, title="Chan"),
        new_chat_member=SimpleNamespace(status="left", user=member),
        old_chat_member=SimpleNamespace(status="member")))
    handler, root, old_level = _capture_logs()
    try:
        run(A.handle_channel_member_update(leave_update, ctx, "b1", 999))
    finally:
        _stop_capture(handler, root, old_level)
    msgs = [r.getMessage() for r in handler.records]
    errors = [r.getMessage() for r in handler.records if r.levelno >= logging.ERROR]
    check("leave: koi ERROR nahi", not errors, str(errors[:2]))
    check("leave: DM account se gayi", len(acct_client.sent_messages) == 1,
          str(acct_client.sent_messages)[:150])
    check("leave: sent message me message_id (PTB jaisa)",
          getattr(acct_client.sent_messages and FakeSent(), "message_id", None) is not None)
    check("leave: message text sahi", "wapas aao" in (acct_client.sent_messages[-1]["text"] or ""),
          str(acct_client.sent_messages[-1]["text"])[:80])
    recorded = A.db.leave.get("sent") or []
    check("leave: recovery message record hua", bool(recorded), str(recorded)[:120])
    check("leave: record me bot_id + message_id aaya",
          bool(recorded) and recorded[-1][0] == "b1" and int(recorded[-1][-1]) > 0, str(recorded)[:120])
    check("leave: permanent mark NAHI hua", A.db.gone_calls == [], str(A.db.gone_calls))

    # --- koi user account nahi: permanent mark NAHI, sirf ek warning
    A.user_account_clients.pop("ua8394878310", None)
    A._DM_INITIATE_WARNED.clear()
    A.db.gone_calls = []
    A.db.messages = {}
    ctx2 = FakeCtx()
    ctx2.bot = CantInitiateBot("chanbot")
    handler, root, old_level = _capture_logs()
    try:
        run(A.handle_join_request(jr_update, ctx2, "b1", 999))
    finally:
        _stop_capture(handler, root, old_level)
    warns = [r.getMessage() for r in handler.records if r.levelno == logging.WARNING]
    check("no-account: permanent mark NAHI", A.db.gone_calls == [], str(A.db.gone_calls))
    check("no-account: ek hi warning", sum(1 for m in warns if "welcome DM skip" in m) == 1, str(warns))
    check("no-account: warning me rasta bataya",
          any("Add Account" in m or "user account add" in m for m in warns), str(warns)[:200])
    # dobara wahi user -> duplicate warning nahi
    handler, root, old_level = _capture_logs()
    try:
        run(A.handle_join_request(jr_update, ctx2, "b1", 999))
    finally:
        _stop_capture(handler, root, old_level)
    warns2 = [r.getMessage() for r in handler.records if r.levelno == logging.WARNING]
    check("no-account: dobara spam nahi", not [w for w in warns2 if "welcome DM skip" in w], str(warns2))

    # --- user ne bot BLOCK kiya: account se bhejne ki koshish nahi (respect block), mark ho jata hai
    A.user_account_clients["ua8394878310"] = acct
    acct_client.sent_messages.clear()
    A.db.gone_calls = []
    A.db.messages = {}
    ctx3 = FakeCtx()
    ctx3.bot = BlockedBot("blocked")
    handler, root, old_level = _capture_logs()
    try:
        run(A.handle_join_request(jr_update, ctx3, "b1", 999))
    finally:
        _stop_capture(handler, root, old_level)
    check("blocked: account se spam nahi", acct_client.sent_messages == [], str(acct_client.sent_messages))
    check("blocked: permanent mark hua", ("b1", 777001) in [(c[0], c[1]) for c in A.db.gone_calls],
          str(A.db.gone_calls))
    check("blocked: ek hi warning", True)

    # --- boot cleanup: purane initiate wale marks saaf, blocked wale rahen
    A.db.gone_calls = [("b1", 1, "Forbidden: bot can't initiate conversation with a user"),
                       ("b1", 2, "Forbidden: bot was blocked by the user"),
                       ("b1", 3, "Bad Request: chat not found")]
    cleared = A.db.clear_initiate_blocked_unreachable()
    check("cleanup: initiate mark saaf", cleared == 1, str(cleared))
    left = [(c[0], c[1]) for c in A.db.gone_calls]
    check("cleanup: blocked/gone marks rahe", sorted(left) == [("b1", 2), ("b1", 3)], str(left))

    # --- diag me fallback + unreachable count dikhe
    A.db.gone_calls = []
    txt = A.strip_premium_emojis(A.build_diag_text())
    check("diag: bot row me fallback status", "user-account fallback:" in txt, txt[:300])
    check("diag: unreachable count", "unreachable" in txt)

    # --- sender chain: primary == account khud -> duplicate nahi
    chain = A.dm_sender_candidates("ua8394878310", primary=acct, owner_id=999)
    check("chain: duplicate nahi", chain.count(acct) == 1, str(chain))
    A.user_account_clients.pop("ua8394878310", None)


class AsyncNoop:
    async def __call__(self):
        return True


def test_saved_messages_forward():
    print("\n[22] account se DM: direct -> Saved Messages -> forward (user ka idea)")
    A.reset_premium_styling_state()
    A._UA_SEND_MODE.clear()
    uid = 8394878310

    # --- 1. direct chalti hai to Saved Messages use nahi hoti
    fake = FakeTLClient(me_id=uid)
    sender = A.UserAccountSender("ua8394878310", 999, fake, account_user_id=uid)
    res = run(sender.send_message(777001, "Hello direct", parse_mode="HTML"))
    check("direct: message gaya", res is not None and res.message_id > 0, str(res))
    check("direct: forward nahi hua", fake.forwards == [], str(fake.forwards))
    check("direct: rasta yaad rakha", A._UA_SEND_MODE.get("ua8394878310") == "direct",
          str(A._UA_SEND_MODE))

    # --- 2. direct fail -> Saved Messages me daal kar forward
    A._UA_SEND_MODE.clear()
    fake2 = FakeTLClient(me_id=uid, mode="direct_fail")
    sender2 = A.UserAccountSender("ua8394878310", 999, fake2, account_user_id=uid)
    handler, root, old_level = _capture_logs()
    try:
        res = run(sender2.send_message(777001, "Hello saved", parse_mode="HTML"))
    finally:
        _stop_capture(handler, root, old_level)
    check("saved: message gaya", res is not None, str(res))
    check("saved: pehle Saved Messages me daala",
          bool(fake2.sent_messages) and fake2.sent_messages[0]["chat_id"] == "me",
          str(fake2.sent_messages)[:120])
    check("saved: phir user ko forward hua",
          bool(fake2.forwards) and _peer_uid(fake2.forwards[-1]["to"]) == 777001
          and fake2.forwards[-1]["from_peer"] == "me", str(fake2.forwards)[:150])
    check("saved: forward ka message_id PTB jaisa", res.message_id == 901, str(res.message_id))
    check("saved: rasta yaad rakha (agli baar saved pehle)",
          A._UA_SEND_MODE.get("ua8394878310") == "saved", str(A._UA_SEND_MODE))
    infos = [r.getMessage() for r in handler.records if r.levelno == logging.INFO]
    check("saved: saaf log line", any("Saved Messages me daal kar forward" in m for m in infos),
          str(infos)[:200])

    # --- 3. agli baar saved pehle try hoti hai (direct dobara fail nahi karti)
    fake2.sent_messages.clear()
    fake2.forwards.clear()
    run(sender2.send_message(777002, "Second", parse_mode="HTML"))
    check("saved: agli baar bhi forward se", len(fake2.forwards) == 1, str(fake2.forwards)[:120])

    # --- 4. media bhi saved se forward (file dobara upload nahi)
    A._UA_SEND_MODE.clear()
    fake3 = FakeTLClient(me_id=uid, mode="direct_fail")
    sender3 = A.UserAccountSender("ua8394878310", 999, fake3, account_user_id=uid)
    run(sender3.send_photo(777003, "/tmp/does-not-exist.jpg", caption="Photo caption"))
    check("media: Saved Messages me file gayi",
          bool(fake3.sent_files) and fake3.sent_files[0]["chat_id"] == "me",
          str(fake3.sent_files)[:120])
    check("media: forward hui", len(fake3.forwards) == 1, str(fake3.forwards)[:120])

    # --- 5. blocked user -> Saved Messages ki koshish bhi nahi (respect + spam se bacha)
    A._UA_SEND_MODE.clear()
    fake4 = FakeTLClient(me_id=uid, mode="blocked")
    sender4 = A.UserAccountSender("ua8394878310", 999, fake4, account_user_id=uid)
    raised = None
    try:
        run(sender4.send_message(777004, "Hi", parse_mode="HTML"))
    except Exception as ex:
        raised = ex
    check("blocked: error aaya", raised is not None, str(raised))
    check("blocked: forward nahi kiya", fake4.forwards == [], str(fake4.forwards))
    check("blocked: Saved Messages me bhi nahi daala", fake4.sent_messages == [], str(fake4.sent_messages))
    check("blocked: strategy switch nahi",
          A._UA_SEND_MODE.get("ua8394878310") in (None, "direct"), str(A._UA_SEND_MODE))

    # --- 6. flood -> koi doosra rasta nahi
    A._UA_SEND_MODE.clear()
    fake5 = FakeTLClient(me_id=uid, mode="flood")
    sender5 = A.UserAccountSender("ua8394878310", 999, fake5, account_user_id=uid)
    raised = None
    try:
        run(sender5.send_message(777005, "Hi", parse_mode="HTML"))
    except Exception as ex:
        raised = ex
    check("flood: error aaya", raised is not None, str(raised))
    check("flood: forward nahi", fake5.forwards == [], str(fake5.forwards))

    # --- 7. error ke saath rasta (dm_error_note) + helpers
    check("note: flood ka rasta", "flood" in A.dm_error_note(A.AccountLimitedError("PEER_FLOOD")).lower(),
          A.dm_error_note(A.AccountLimitedError("PEER_FLOOD")))
    check("note: mutual-contact ka rasta",
          "warm" in A.dm_error_note(A.BadRequest("You can only send messages to mutual contacts")).lower())
    check("note: entity ka rasta",
          "join request" in A.dm_error_note(A.BadRequest("Could not find the input entity for PeerUser")).lower())
    check("switch: Forbidden par nahi",
          A._ua_should_try_other_strategy(A.Forbidden("Forbidden: bot was blocked by the user")) is False)
    check("switch: flood par nahi",
          A._ua_should_try_other_strategy(A.AccountLimitedError("PeerFlood")) is False)
    check("switch: entity error par haan",
          A._ua_should_try_other_strategy(A.BadRequest("Could not find the input entity")) is True)

    # --- 8. /diag me dono test button + aakhri error dikhta hai
    A.user_account_clients["ua8394878310"] = sender2
    sender2.last_error = "BadRequest: Could not find the input entity for PeerUser(user_id=777001)"
    mctx = FakeCtx()
    mq = FakeQuery(mctx, uid=A.ADMIN_USER_ID)
    mq.data = "admin_diag"
    run(A.callback_handler(_fake_update(q=mq, uid=A.ADMIN_USER_ID), mctx))
    datas = [b.get("callback_data") for _t, kw in mq.edits if kw.get("reply_markup")
             for row in kw["reply_markup"].to_dict()["inline_keyboard"] for b in row]
    check("diag: direct test button", "diag_dm_ua8394878310_direct" in datas, str(datas)[:200])
    check("diag: saved test button", "diag_dm_ua8394878310_saved" in datas, str(datas)[:200])
    txt = A.strip_premium_emojis(A.build_diag_text())
    check("diag: bhejne ka rasta dikhta hai", "bhejne ka rasta" in txt, txt[:400])
    check("diag: aakhri error dikhta hai", "aakhri error" in txt and "PeerUser" in txt, txt[:400])

    # --- 9. saved button dabane par saved rasta hi test hota hai
    A._UA_SEND_MODE["ua8394878310"] = "direct"
    fake2.sent_messages.clear()
    fake2.forwards.clear()
    tq = FakeQuery(FakeCtx(), uid=A.ADMIN_USER_ID)
    tq.data = "diag_dm_ua8394878310_saved"
    handler, root, old_level = _capture_logs()
    try:
        run(A.callback_handler(_fake_update(q=tq, uid=A.ADMIN_USER_ID), FakeCtx()))
    finally:
        _stop_capture(handler, root, old_level)
    check("saved button: Saved Messages rasta use hua",
          bool(fake2.forwards) and fake2.sent_messages and fake2.sent_messages[0]["chat_id"] == "me",
          f"fwd={fake2.forwards} sent={fake2.sent_messages}"[:160])
    check("saved button: rasta wapas direct hi raha (test ne badla nahi)",
          A._UA_SEND_MODE.get("ua8394878310") == "direct", str(A._UA_SEND_MODE))
    check("saved button: reply me confirm",
          any("Test DM bhej diya" in (t or "") for t, _ in tq.edits), str([t[:40] for t, _ in tq.edits]))
    check("saved button: log me label", any("diag test DM (Saved Messages" in r.getMessage()
                                            for r in handler.records),
          str([r.getMessage() for r in handler.records if "diag test" in r.getMessage()])[:160])
    A.user_account_clients.pop("ua8394878310", None)
    A._UA_SEND_MODE.clear()


def test_entity_and_media_fixes():
    print("\n[23] entity (access_hash) + media file_id + PeerUser(0) fix (server log)")
    A.reset_premium_styling_state()
    A._UA_SEND_MODE.clear()
    A._WARN_ONCE_KEYS.clear()
    A.db.entity_cache = {}
    uid = 8394878310

    # --- 1. pending requesters: offset_user=0 nahi, InputUserEmpty (PeerUser(0) fix)
    fake = FakeTLClient(me_id=uid)
    fake.invite_users = [SimpleNamespace(id=777001, access_hash=111001),
                         SimpleNamespace(id=777002, access_hash=111002)]
    sender = A.UserAccountSender("ua8394878310", 999, fake, account_user_id=uid)
    reqs = run(sender.list_pending_join_requesters(-100123))
    check("pending: requesters mile", [u.id for u in reqs] == [777001, 777002], str(reqs))
    last_req = fake.raw_calls[-1]
    check("pending: offset_user InputUserEmpty (0 nahi)",
          type(getattr(last_req, "offset_user", None)).__name__ == "InputUserEmpty",
          str(type(getattr(last_req, "offset_user", None)))[:120])
    check("pending: access_hash memory me yaad",
          A._UA_ENTITY_MEM.get("ua8394878310", {}).get(777001) == 111001,
          str(A._UA_ENTITY_MEM)[:120])
    check("pending: access_hash DB me yaad",
          A.db.get_user_access_hash("ua8394878310", 777002) == 111002)

    # --- 2. _resolve_peer: cache hit -> sahi access_hash wala peer, dobara lookup nahi
    peer = run(sender._resolve_peer(777001))
    check("resolve: cache se peer",
          _peer_uid(peer) == 777001 and int(peer.access_hash) == 111001, str(peer)[:120])
    check("resolve: get_input_entity call nahi (cache hit)", fake.input_entity_calls == [],
          str(fake.input_entity_calls))

    # --- 3. DM me peer jata hai (Telethon ko access_hash milta hai)
    fake.sent_messages.clear()
    res = run(sender.send_message(777001, "Hello peer", parse_mode="HTML"))
    got = fake.sent_messages[-1]["chat_id"]
    check("send: peer object gaya",
          _peer_uid(got) == 777001 and int(got.access_hash) == 111001, str(got)[:120])
    check("send: message_id mila", res is not None and res.message_id > 0, str(res))

    # --- 4. cache miss + strict entity: channel hint se join-request list se resolve
    A._UA_ENTITY_MEM.clear()
    A.db.entity_cache = {}
    fake2 = FakeTLClient(me_id=uid, mode="strict_entity")
    fake2.invite_users = [SimpleNamespace(id=777010, access_hash=222010)]
    sender2 = A.UserAccountSender("ua8394878310", 999, fake2, account_user_id=uid)
    tok = A._UA_CHANNEL_HINT.set(-100123)
    try:
        peer2 = run(sender2._resolve_peer(777010))
    finally:
        A._UA_CHANNEL_HINT.reset(tok)
    check("hint: channel list se resolve",
          _peer_uid(peer2) == 777010 and int(peer2.access_hash) == 222010, str(peer2)[:120])
    check("hint: yaad ho gaya (agli baar seedha)",
          A._UA_ENTITY_MEM.get("ua8394878310", {}).get(777010) == 222010)

    # --- 4b. send_dm_fallback hints do_send tak pahunchata hai
    seen = {}

    async def _do_send2(snd):
        seen["chan"] = A._UA_CHANNEL_HINT.get()
        seen["mediabot"] = A._UA_MEDIA_BOT_HINT.get()

    ok, err, _used = run(A.send_dm_fallback("ua8394878310", 777010, _do_send2, primary=sender2,
                                            owner_id=999, channel_id=-100123))
    check("hint: channel hint do_send tak pahuncha", seen.get("chan") == -100123, str(seen))
    check("hint: media bot hint do_send tak pahuncha",
          seen.get("mediabot") == "ua8394878310", str(seen))
    check("hint: ok", ok is True and err is None, str((ok, err)))

    # --- 5. media: channel wale bot ka token pehle, phir main bot
    A.db.user_bots = [{"bot_id": "b1", "user_id": 999, "account_type": "bot",
                       "bot_username": "chanbot", "bot_token": "CHAN-TOKEN", "is_active": 1}]
    tried = []

    async def _fake_dl(file_id, tokens):
        tried.extend(tokens or [])
        return "/tmp/ua_chan_media.bin" if "CHAN-TOKEN" in (tokens or []) else None

    real_dl = A.bot_api_download_file
    A.bot_api_download_file = _fake_dl
    try:
        got_path = run(A.materialize_media("SOME-FILE-ID", None,
                                           extra_tokens=["CHAN-TOKEN", "MAIN-TOKEN"]))
    finally:
        A.bot_api_download_file = real_dl
    check("media: channel token se download hua", got_path == "/tmp/ua_chan_media.bin", str(got_path))
    check("media: dono token try hue", tried == ["CHAN-TOKEN", "MAIN-TOKEN"], str(tried))

    mtok = A._UA_MEDIA_BOT_HINT.set("b1")
    try:
        mt = sender._media_tokens()
    finally:
        A._UA_MEDIA_BOT_HINT.reset(mtok)
    check("media: channel token pehle", mt and mt[0] == "CHAN-TOKEN", str(mt))

    # cache hit par download dobara nahi
    fd, tmpp = tempfile.mkstemp(prefix="ua_cache_")
    os.close(fd)
    A._UA_MEDIA_CACHE[("CHAN-TOKEN", "CACHED-ID")] = tmpp
    dl_calls = []

    async def _fake_dl2(file_id, tokens):
        dl_calls.append(file_id)
        return None

    A.bot_api_download_file = _fake_dl2
    try:
        hit = run(A.materialize_media("CACHED-ID", None, extra_tokens=["CHAN-TOKEN", "MAIN"]))
    finally:
        A.bot_api_download_file = real_dl
        os.unlink(tmpp)
    check("media: cache hit par download nahi", hit == tmpp and dl_calls == [],
          f"{hit} {dl_calls}")

    # --- 6. log shor band: entity/file error par "retrying plainly" nahi, ek line
    check("retry: entity par plain retry nahi",
          A._should_plain_retry(A.BadRequest("Could not find the input entity for PeerUser")) is False)
    check("retry: file error par plain retry nahi",
          A._should_plain_retry(A.BadRequest("Failed to convert BAACxxx to media")) is False)
    check("retry: html error par plain retry haan",
          A._should_plain_retry(A.BadRequest("Can't parse entities: end of tag")) is True)

    class _EntityFailBot:
        async def send_message(self, chat_id, text, *a, **k):
            raise A.BadRequest("Could not find the input entity for PeerUser(user_id=1)")

    handler, root, old_level = _capture_logs()
    try:
        res_none = run(A.send_user_message(_EntityFailBot(), 777020, "Hi"))
    finally:
        _stop_capture(handler, root, old_level)
    warns = [r.getMessage() for r in handler.records if r.levelno == logging.WARNING]
    check("retry: entity par sirf 1 warning", len(warns) == 1, str(warns)[:200])
    check("retry: 'retrying plainly' nahi", not any("retrying plainly" in w for w in warns),
          str(warns)[:200])
    check("retry: None wapas (raise nahi)", res_none is None)

    handler2, root2, old2 = _capture_logs()
    try:
        A._warn_once("test:key", "pehli warning")
        A._warn_once("test:key", "pehli warning")
    finally:
        _stop_capture(handler2, root2, old2)
    w2 = [r.getMessage() for r in handler2.records if r.levelno == logging.WARNING]
    check("warn-once: ek hi baar", w2 == ["pehli warning"], str(w2))


def test_user_account_owner_flow_adapter():
    print("\n[16] user account owner flows (main bot ke messages se)")
    A.reset_premium_styling_state()
    A.db.user_bots = [{"bot_id": "ua8394878310", "user_id": A.ADMIN_USER_ID, "account_type": "user",
                       "bot_username": None, "phone": "+19312837172", "bot_token": None,
                       "session_string": FAKE_SESSION, "api_id": 1, "api_hash": "h"}]
    A.db.subs = {"ua8394878310": {"subscription_type": "Basic",
                                  "expiry_date": A.now_aware() + timedelta(days=30), "max_channels": 1}}
    A.db.gone_calls = []
    A.db.join_requests = []
    A.db.leave = {"messages": []}
    A.db.channels = []
    A.db.bot_channels = []
    uid = A.ADMIN_USER_ID

    ctx = FakeCtx()
    ctx.user_data.clear()

    # --- regression: adapter ke bina "'Message' object has no attribute 'effective_user'"
    owner = SimpleNamespace(id=uid, first_name="Owner", last_name="", username="owner", is_bot=False)
    raw = FakeMsg(text="hello", from_user=owner)
    adapted = A._RouterUpdate(raw, A._RouterUser(uid))
    check("adapter: effective_user milta hai", adapted.effective_user.id == uid, str(adapted.effective_user))
    check("adapter: message wahi hai", adapted.message is raw)
    check("adapter: unknown attr message se aata hai", adapted.chat_id == raw.chat_id)
    check("adapter: user object me full_name", A._RouterUser(5, "A", "B").full_name == "A B")

    # --- channel add flow (owner channel se message forward karta hai)
    A.user_account_clients["ua8394878310"] = A.UserAccountSender("ua8394878310", uid, FakeTLClient(),
                                                                 phone="+19312837172", account_user_id=8394878310)
    ctx.user_data.clear()
    ctx.user_data["adding_channel_ua8394878310"] = True
    forwarded = FakeMsg(text=None, from_user=owner)
    forwarded.forward_origin = SimpleNamespace(chat=SimpleNamespace(
        id=-1001234567890, type="channel", title="My Channel", username="mychannel"))
    handler, root, old_level = _capture_logs()
    try:
        run(A.handle_message(_fake_update(msg=forwarded, uid=uid), ctx))
    finally:
        _stop_capture(handler, root, old_level)
    errors = [r for r in handler.records if r.levelno >= logging.ERROR]
    check("channel add: koi ERROR nahi", not errors, str([r.getMessage()[:80] for r in errors]))
    check("channel add: channel DB me gaya", any(c[1] == -1001234567890 for c in A.db.channels),
          str(A.db.channels))
    check("channel add: state clear", not ctx.user_data.get("adding_channel_ua8394878310"),
          str(ctx.user_data.get("adding_channel_ua8394878310")))
    replies = [(t or "") for t, _ in forwarded.replies]
    check("channel add: success reply aaya", any("added successfully" in t or "Channel" in t for t in replies),
          str([t[:60] for t in replies]))
    check("channel add: limit/error message nahi", not any("limit" in t.lower() or "admin" in t.lower()
                                                          for t in replies), str([t[:60] for t in replies]))

    # --- welcome set flow (setting_message_ state)
    ctx.user_data.clear()
    ctx.user_data["setting_message_ua8394878310"] = True
    welcome = FakeMsg(text="Hello {first_name}, welcome!", from_user=owner)
    handler2, root2, old2 = _capture_logs()
    try:
        run(A.handle_message(_fake_update(msg=welcome, uid=uid), ctx))
    finally:
        _stop_capture(handler2, root2, old2)
    errs2 = [r for r in handler2.records if r.levelno >= logging.ERROR]
    check("welcome set: koi ERROR nahi", not errs2, str([r.getMessage()[:80] for r in errs2]))

    # --- non-owner ka message route nahi hona chahiye
    ctx.user_data.clear()
    ctx.user_data["adding_channel_ua8394878310"] = True
    stranger = SimpleNamespace(id=424242, first_name="Stranger", last_name="", username=None, is_bot=False)
    msg_stranger = FakeMsg(text="hi", from_user=stranger)
    check("router: non-owner skip", run(A._route_user_account_owner_message(msg_stranger, ctx, 424242)) is False)
    ctx.user_data.pop("adding_channel_ua8394878310", None)

    # --- bot account ka flow pehle jaisa (main bot se nahi chalega)
    ctx.user_data.clear()
    ctx.user_data["adding_channel_b1"] = True
    check("router: bot account skip", run(A._route_user_account_owner_message(FakeMsg(), ctx, uid)) is False)
    ctx.user_data.pop("adding_channel_b1", None)

    # --- account ka apna id bhi owner hai (account se /start par panel aaye)
    acc_uid = 8394878310
    check("self-owner: account ka apna id owner", A.is_bot_owner("ua8394878310", acc_uid) is True)
    check("self-owner: koi aur nahi", A.is_bot_owner("ua8394878310", 555555) is False)
    check("self-owner: bot account par nahi", A.is_bot_owner("b1", acc_uid) is False)
    check("self-owner: ua id parse", A.account_self_uid("ua8394878310") == 8394878310
          and A.account_self_uid("b1") is None)

    labels = _kb_labels(A.main_menu_kb(acc_uid))
    check("self-owner: main menu me account dikhe",
          any("8394878310" in l or "19312837172" in l for l in labels), str(labels))

    acc_msg = FakeMsg(text="/start", chat_id=acc_uid, from_user=A._RouterUser(acc_uid, "Acct"))
    run(A.start_command(_fake_update(msg=acc_msg, uid=acc_uid), FakeCtx()))
    texts = [t or "" for t, _ in acc_msg.replies]
    check("self-owner: /start par panel aaya", any("MANAGE USER ACCOUNT" in t for t in texts),
          str([t[:60] for t in texts]))
    check("self-owner: panel me channel button",
          any("Add Channel" in l for l in _kb_labels(acc_msg.replies[-1][1].get("reply_markup"))),
          str(_kb_labels(acc_msg.replies[-1][1].get("reply_markup"))))

    # --- account se manage button dabane par permission mile
    mctx = FakeCtx()
    mq = FakeQuery(mctx, uid=acc_uid)
    mq.data = "manage_bot_ua8394878310"
    run(A.callback_handler(_fake_update(q=mq, uid=acc_uid), mctx))
    qtexts = [t or "" for t, _ in mq.edits]
    check("self-owner: manage_bot_ panel khula", any("MANAGE USER ACCOUNT" in t for t in qtexts),
          str([t[:70] for t in qtexts]))
    check("self-owner: permission error nahi", not any("permission" in t.lower() for t in qtexts))

    # --- UserAccountSender ke panel helpers
    sender = A.user_account_clients["ua8394878310"]
    check("sender: delete_message hai", callable(getattr(sender, "delete_message", None)))
    check("sender: edit_message_text hai", callable(getattr(sender, "edit_message_text", None)))
    check("sender: delete_message chalta hai",
          run(sender.delete_message(555, 77)) is True)
    A.user_account_clients.pop("ua8394878310", None)
    A.db.user_bots = []


def main():
    # Har group se pehle styling state saaf (FlakyBot tests disable kar dete hain)
    A.reset_premium_styling_state()
    test_premium_button_parsing()
    A.reset_premium_styling_state()
    test_button_wizard()
    A.reset_premium_styling_state()
    test_album_layout()
    A.reset_premium_styling_state()
    test_album_flush_real_jobqueue()
    A.reset_premium_styling_state()
    test_admin_multiselect()
    A.reset_premium_styling_state()
    test_delivery_robustness()
    A.reset_premium_styling_state()
    test_security_and_startup()
    A.reset_premium_styling_state()
    test_network_noise_and_retry()
    A.reset_premium_styling_state()
    test_leave_recovery()
    A.reset_premium_styling_state()
    test_panel_routing()
    A.reset_premium_styling_state()
    test_app_wiring()
    A.reset_premium_styling_state()
    test_user_account_mode()
    A.reset_premium_styling_state()
    test_admin_add_account_wizard()
    A.reset_premium_styling_state()
    A.reset_premium_styling_state()
    test_html_safety_and_welcome_spam()
    A.reset_premium_styling_state()
    A.reset_premium_styling_state()
    test_subscription_picker_and_style_memory()
    test_user_account_owner_flow_adapter()
    test_user_account_dm_start()
    test_user_account_premium_emoji_fallback()
    test_diagnostics()
    test_user_account_boot_without_subscription()
    test_dm_user_account_fallback()
    test_saved_messages_forward()
    test_entity_and_media_fixes()
    print(f"\n==== tests: {len(PASS)} passed, {len(FAIL)} failed ====")
    if FAIL:
        for f in FAIL:
            print("  failed:", f)
        sys.exit(1)


if __name__ == "__main__":
    main()
