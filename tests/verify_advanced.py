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
        raise A.Forbidden("Forbidden: can't initiate conversation with a user")


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
    def __init__(self, text=None, entities=None, chat_id=999, caption=None):
        self.text = text
        self.caption = caption
        self.entities = entities or []
        self.caption_entities = []
        self.chat_id = chat_id
        self.message_id = 77
        self.replies = []
        self.media_group_id = None
        self.deleted = False

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

    def __init__(self, authorized=True, me_id=123, me_username="acct", mode="ok"):
        self.authorized = authorized
        self.me = SimpleNamespace(id=me_id, username=me_username, bot=False)
        self.mode = mode          # ok | password | bad_code | bad_password | blocked | flood
        self.connected = False
        self.disconnected = False
        self.code_requests = []
        self.sign_ins = []
        self.sent_messages = []
        self.sent_files = []
        self.raw_calls = []
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
        if self.mode == "bad_password" and password is not None:
            raise PasswordHashInvalidError(None)
        return self.me

    # --- sending
    async def send_message(self, chat_id, text, **kw):
        if self.mode == "blocked":
            raise type("UserIsBlockedError", (Exception,), {})("You blocked this user")
        if self.mode == "flood":
            raise type("PeerFloodError", (Exception,), {})("Too many requests")
        self.sent_messages.append({"chat_id": chat_id, "text": text, **kw})
        return SimpleNamespace(id=len(self.sent_messages))

    async def send_file(self, chat_id, file, **kw):
        if self.mode == "flood":
            raise type("PeerFloodError", (Exception,), {})("Too many requests")
        if self.mode == "blocked":
            raise type("UserIsBlockedError", (Exception,), {})("You blocked this user")
        self.sent_files.append({"chat_id": chat_id, "file": file, **kw})
        return SimpleNamespace(id=len(self.sent_files))

    async def get_permissions(self, chat_id, user=None):
        return SimpleNamespace(is_admin=True, is_creator=False)

    async def __call__(self, request):
        self.raw_calls.append(request)
        return SimpleNamespace(users=[], importers=[])


A.db = FakeDB()


# ---------------------------------------------------------------- helpers
def _fake_update(q=None, uid=999, msg=None):
    return SimpleNamespace(callback_query=q, effective_user=SimpleNamespace(id=uid, first_name="T", username="t"),
                           message=msg or FakeMsg(chat_id=uid), effective_chat=SimpleNamespace(id=uid),
                           chat_member=None, chat_join_request=None, inline_query=None)


def _kb_labels(markup):
    data = markup.to_dict() if hasattr(markup, "to_dict") else markup
    return [b.get("text", "") for row in data.get("inline_keyboard", []) for b in row]


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
    check("leave: reachable ko DM gaya", any(m["chat_id"] == 889 for m in good_client.sent_messages),
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


def main():
    test_premium_button_parsing()
    test_button_wizard()
    test_album_layout()
    test_album_flush_real_jobqueue()
    test_admin_multiselect()
    test_delivery_robustness()
    test_security_and_startup()
    test_network_noise_and_retry()
    test_leave_recovery()
    test_panel_routing()
    test_app_wiring()
    test_user_account_mode()
    test_admin_add_account_wizard()
    test_html_safety_and_welcome_spam()
    print(f"\n==== tests: {len(PASS)} passed, {len(FAIL)} failed ====")
    if FAIL:
        for f in FAIL:
            print("  failed:", f)
        sys.exit(1)


if __name__ == "__main__":
    main()
