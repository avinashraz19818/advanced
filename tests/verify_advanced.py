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

    def add_leave_recovery_pending(self, bot_id, user_id, source_channel_id, target_channel_id):
        rows = self.leave.setdefault("pending_rows", {})
        rows[(bot_id, user_id, target_channel_id)] = {
            "bot_id": bot_id, "user_id": user_id,
            "source_channel_id": source_channel_id, "target_channel_id": target_channel_id}
        self.leave.setdefault("pending_adds", []).append(
            (bot_id, user_id, source_channel_id, target_channel_id))

    def get_leave_recovery_pending(self, bot_id, user_id):
        rows = self.leave.get("pending_rows", {})
        return [dict(v) for k, v in rows.items() if k[0] == bot_id and k[1] == user_id]

    def clear_leave_recovery_pending(self, bot_id, user_id, target_channel_id=None):
        rows = self.leave.setdefault("pending_rows", {})
        for k in list(rows):
            if k[0] == bot_id and k[1] == user_id and (
                    target_channel_id is None or k[2] == target_channel_id):
                rows.pop(k, None)
        self.leave.setdefault("pending_clears", []).append((bot_id, user_id, target_channel_id))

    def _execute(self, sql, params=()):
        # real DB jaisa: raw SQL calls record karo (test me kaam ke liye)
        self.sql_calls = getattr(self, "sql_calls", [])
        self.sql_calls.append(sql.strip().split()[0] if sql and sql.strip() else sql)
        if "leave_recovery_messages" in (sql or "") and "UPDATE" in (sql or ""):
            self.leave["messages_cleared"] = True
        return None

    def clear_all_leave_recovery_pending(self):
        rows = self.leave.setdefault("pending_rows", {})
        count = len(rows)
        rows.clear()
        return count

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
        return [b for b in self.user_bots if (b.get("account_type") or "bot") == "bot"]

    def get_user_bots_by_owner(self, user_id):
        return [b for b in self.user_bots if int(b.get("user_id", 0)) == int(user_id)
                and (b.get("account_type") or "bot") == "bot"]

    def get_user_bot(self, bot_id):
        for b in self.user_bots:
            if b["bot_id"] == bot_id and (b.get("account_type") or "bot") == "bot":
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
            if b.get("bot_username") == username and (b.get("account_type") or "bot") == "bot":
                return b
        return None

    def get_bot_channels(self, bot_id):
        bc = getattr(self, "bot_channels", None)
        if isinstance(bc, dict):          # per-bot channels
            return list(bc.get(bot_id, []))
        if bc is not None:
            return list(bc)
        return [{"channel_id": -100123, "channel_title": "Test Channel", "auto_approve": 0}]

    def get_setting(self, key, default=None):
        return getattr(self, "settings", {}).get(key, default)

    def set_setting(self, key, value):
        self.settings = getattr(self, "settings", {})
        self.settings[key] = value

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
        # real DB: mark_reachable() purana soft (initiate) mark hata deta hai
        self.gone_calls = [c for c in self.gone_calls if not (
            len(c) > 1 and c[0] == a[0] and c[1] == a[1]
            and "initiate" in str(c[2] if len(c) > 2 else "").lower())]

    def mark_unreachable(self, *a):
        self.unreachable_calls.append(tuple(a))

    def mark_permanently_unreachable(self, *a):
        self.gone_calls.append(tuple(a))

    def mark_initiate_blocked(self, bot_id, uid, reason=""):
        self.gone_calls.append((bot_id, uid, f"initiate: {reason}"))
        self.initiate_calls = getattr(self, "initiate_calls", [])
        self.initiate_calls.append((bot_id, uid, str(reason)))

    def is_initiate_blocked(self, bot_id, uid):
        return any(c[0] == bot_id and c[1] == uid and "initiate" in str(c[2] if len(c) > 2 else "").lower()
                   for c in self.gone_calls)

    def count_initiate_blocked(self, bot_id):
        return sum(1 for c in self.gone_calls if c[0] == bot_id
                   and "initiate" in str(c[2] if len(c) > 2 else "").lower())

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


A.db = FakeDB()


# ---------------------------------------------------------------- helpers
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


def _fake_update(q=None, uid=999, msg=None):
    return SimpleNamespace(callback_query=q,
                           effective_user=SimpleNamespace(id=uid, first_name="T", last_name="",
                                                          username="t", is_bot=False),
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
    check("link ke baad color step", state["step"] == "color")
    choices = A._wizard_kb(state, A.get_button_target(ctx, tid)).to_dict()["inline_keyboard"]
    check("color: real colored buttons", [choices[0][0].get("style"),choices[0][1].get("style"),choices[1][0].get("style")]
          == ["primary","success","danger"])
    check("color: default has no style", "style" not in choices[1][1])
    check("color: actual label + premium icon", "Join Now" in choices[0][0]["text"] and choices[0][0].get("icon_custom_emoji_id") == "5000000001")
    green_callback = choices[0][1]["callback_data"]
    run(A.handle_button_wizard_callback(FakeQuery(ctx), ctx, green_callback))
    check("button stored in row 1", len(state["rows"]) == 1 and len(state["rows"][0]) == 1, str(state["rows"]))

    run(A.handle_button_wizard_callback(FakeQuery(ctx), ctx, f"bwz_same_{tid}"))
    check("same-row placement", state["placement"] == "same" and state["step"] == "name", str(state))
    run(A.handle_button_wizard_message(FakeMsg("Website"), ctx))
    run(A.handle_button_wizard_message(FakeMsg("not-a-link"), ctx))
    check("invalid link rejected", state["step"] == "url", str(state.get("step")))
    run(A.handle_button_wizard_message(FakeMsg("https://site.com"), ctx))
    run(A.handle_button_wizard_callback(FakeQuery(ctx), ctx, green_callback))
    check("old color tap ignored for next button", state["step"] == "color" and len(state["rows"][0]) == 1)
    run(A.handle_button_wizard_callback(FakeQuery(ctx), ctx, f"bwz_done_{tid}"))
    check("cannot skip pending color using stale Done", state["step"] == "color")
    run(A.handle_button_wizard_message(FakeMsg("Red please"), ctx))
    check("typed color does not become another label", state["step"] == "color")
    run(A.handle_button_wizard_callback(FakeQuery(ctx), ctx, f"bwz_color_{tid}_red_{state['color_nonce']}"))
    run(A.handle_button_wizard_callback(FakeQuery(ctx), ctx, green_callback))
    check("double tap cannot duplicate row", len(state["rows"][0]) == 2)
    check("2 buttons in one row", len(state["rows"][0]) == 2, str(state["rows"]))

    qd = FakeQuery(ctx)
    run(A.handle_button_wizard_callback(qd, ctx, f"bwz_done_{tid}"))
    saved = A.rows_from_buttons_json(A.db.messages[5]["buttons_json"])
    check("buttons saved to DB", len(saved) == 1 and len(saved[0]) == 2, str(saved))
    check("premium icon persisted", saved[0][0]["icon_id"] == "5000000001", str(saved))
    check("wizard state cleared", A.BUTTON_WIZARD_KEY not in ctx.user_data)
    check("chosen green/red persist in DB", [b["style"] for b in saved[0]] == ["success","danger"])
    markup = A.markup_from_rows(saved).to_dict()["inline_keyboard"]
    check("saved markup uses chosen colors", [b.get("style") for b in markup[0]] == ["success","danger"])
    for style in (None, "primary", "success", "danger"):
        original = [[{"text":"Test", "url":"https://t.me/test", "style":style}]]
        restored = A.rows_from_buttons_json(A.rows_to_buttons_json(original))
        check(f"style roundtrip: {style}", restored[0][0]["style"] == style)
        check(f"render style: {style}", A.markup_from_rows(restored).to_dict()["inline_keyboard"][0][0].get("style") == style)
    panel = A.button_builder_row(FakeCtx(), {"kind":"draft_admin"})
    check("old Add Button + Paste Many restored", len(panel) == 2 and all(b.web_app is None for b in panel))
    source = open(A.__file__, encoding="utf-8").read()
    check("Mini App backend removed", "miniapp_bridge" not in source and "WEBAPP_API_URL" not in source)


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
                  "channel_configs": {"-100123": True},   # default OFF hai, isliye explicitly ON
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


def test_admin_add_account_wizard():
    print("\n[12] admin ADD ACCOUNT wizard (step-by-step)")
    A.db.user_bots = []
    A.db.added_bots = []
    ctx = FakeCtx()
    ctx.user_data.clear()
    admin = A.ADMIN_USER_ID  # harness me yahi id admin hai

    # --- add account: seedha user_id step (sirf bot, chooser nahi)
    q = FakeQuery(ctx, uid=admin)
    q.data = "admin_add_userbot"
    run(A.callback_handler(_fake_update(q=q, uid=admin), ctx))
    check("add: seedha user_id maanga", "USER ID (1/3)" in q.edits[-1][0], q.edits[-1][0][:60])
    check("add: purana state clear + bot step",
          (A._admin_add_state(ctx) or {}).get("step") == "user_id", str(A._admin_add_state(ctx)))

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


def test_html_safety_and_welcome_spam():
    print("\n[13] HTML safety net + welcome ERROR spam")

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
    print("\n[14] subscription picker + premium styling memory")
    A.reset_premium_styling_state()

    # --- picker list: sab accounts buttons me
    A.db.user_bots = [
        {"bot_id": "b1", "bot_username": "one", "bot_token": "t1", "user_id": 1, "account_type": "bot"},
        {"bot_id": "b2", "bot_username": "two", "bot_token": "t2", "user_id": 1, "account_type": "bot"},
    ]
    A.db.subs = {"b1": {"subscription_type": "Pro", "expiry_date": A.now_aware() + timedelta(days=5)}}
    ctx = FakeCtx()
    q = FakeQuery(ctx, uid=A.ADMIN_USER_ID)
    q.data = "admin_add_sub"
    run(A.callback_handler(_fake_update(q=q, uid=A.ADMIN_USER_ID), ctx))
    labels = _kb_labels(q.edits[-1][1].get("reply_markup"))
    check("picker: dono bot dikhe", any("one" in l for l in labels) and any("two" in l for l in labels),
          str(labels))
    check("picker: active plan dikhe", any("Pro" in l for l in labels), str(labels))
    check("picker: manual option", any("Khud type" in l for l in labels), str(labels))
    check("picker: text me days/plan hint", "30 Basic" in q.edits[-1][0], q.edits[-1][0][:80])

    # --- choose karke sirf "30 Basic" bhejna
    q2 = FakeQuery(ctx, uid=A.ADMIN_USER_ID)
    q2.data = "admin_quick_sub_b2"
    run(A.callback_handler(_fake_update(q=q2, uid=A.ADMIN_USER_ID), ctx))
    check("picker: bot prefill set", ctx.user_data.get("admin_add_sub_bot") == "b2",
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
    check("picker: bot ko subscription lag gayi", A.db.subs.get("b2") is not None, str(A.db.subs))
    check("picker: bot auto-start hua", started == ["b2"], str(started))
    check("picker: prefill clear", ctx.user_data.get("admin_add_sub_bot") is None)

    # --- manual option purana format bacha rahe
    q3 = FakeQuery(ctx, uid=A.ADMIN_USER_ID)
    q3.data = "admin_add_sub_manual"
    run(A.callback_handler(_fake_update(q=q3, uid=A.ADMIN_USER_ID), ctx))
    check("manual: state set + hint", ctx.user_data.get("admin_add_sub") is True
          and "bot_id days Plan" in q3.edits[-1][0], q3.edits[-1][0][:70])
    ctx.user_data.clear()

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


def test_diagnostics():
    print("\n[15] diagnostics: build tag, /diag report, unreachable reset")
    A.reset_premium_styling_state()
    check("build tag set", bool(A.BUILD_TAG) and len(A.BUILD_TAG) > 5, A.BUILD_TAG)
    A.db.user_bots = [{"bot_id": "b7", "user_id": A.ADMIN_USER_ID, "account_type": "bot",
                       "bot_username": "mybot", "bot_token": "t", "is_active": 1}]
    A.db.subs = {"b7": {"subscription_type": "Basic",
                        "expiry_date": A.now_aware() + timedelta(days=30), "max_channels": 1}}
    A.db.bot_channels = [{"channel_id": -1001, "channel_title": "C1", "auto_approve": 0}]
    A.db.gone_calls = []

    # --- bot band hai: diag me saaf dikhe
    A.user_bot_applications.pop("b7", None)
    text = A.strip_premium_emojis(A.build_diag_text())
    check("diag: build tag dikhta hai", A.BUILD_TAG in text, text[:120])
    check("diag: bot id dikhta hai", "b7" in text, text[:200])
    check("diag: band bot saaf likha", "NAHI" in text, text[:300])
    check("diag: plan dikhta hai", "Basic" in text)

    # --- bot chalu: status dikhe
    A.user_bot_applications["b7"] = SimpleNamespace(bot=SimpleNamespace(id=1))
    try:
        text2 = A.strip_premium_emojis(A.build_diag_text())
    finally:
        A.user_bot_applications.pop("b7", None)
    check("diag: chalu bot", "chalu: \U0001F7E2" in text2, text2[:300])

    # --- /diag command admin ko report bhejta hai
    msg = FakeMsg(text="/diag", chat_id=A.ADMIN_USER_ID,
                  from_user=SimpleNamespace(id=A.ADMIN_USER_ID, first_name="Admin"))
    handler, root, old_level = _capture_logs()
    try:
        run(A.diag_command(_fake_update(msg=msg, uid=A.ADMIN_USER_ID), FakeCtx()))
    finally:
        _stop_capture(handler, root, old_level)
    replies = [t or "" for t, _ in msg.replies]
    check("diag cmd: report gaya", any("DIAGNOSTICS" in t for t in replies),
          str([t[:40] for t in replies]))
    check("diag cmd: log me bhi gaya", any("DIAGNOSTICS" in r.getMessage() for r in handler.records),
          str([r.getMessage()[:40] for r in handler.records])[:120])
    check("diag cmd: non-admin ko nahi", _check_non_admin_diag())

    # --- admin panel button + unreachable reset
    mctx = FakeCtx()
    mq = FakeQuery(mctx, uid=A.ADMIN_USER_ID)
    mq.data = "admin_diag"
    run(A.callback_handler(_fake_update(q=mq, uid=A.ADMIN_USER_ID), mctx))
    check("diag button: panel se khula", any("DIAGNOSTICS" in (t or "") for t, _ in mq.edits),
          str([t[:40] for t, _ in mq.edits]))
    A.db.gone_calls = [("b7", 111, "Forbidden: bot can't initiate conversation with a user")]
    rq = FakeQuery(FakeCtx(), uid=A.ADMIN_USER_ID)
    rq.data = "diag_reset_unreachable"
    run(A.callback_handler(_fake_update(q=rq, uid=A.ADMIN_USER_ID), FakeCtx()))
    check("diag reset: 1 mark saaf", A.db.gone_calls == [], str(A.db.gone_calls))
    check("diag reset: reply me confirm", any("saaf kiye" in (t or "") for t, _ in rq.edits),
          str([t[:60] for t, _ in rq.edits]))


class InitiateBlockedBot(FakeBot):
    """User ne bot ko /start nahi kiya -> Telegram 403 deta hai (permanent NAHI)."""

    async def send_message(self, chat_id, text, **kw):
        self._log("send_message", chat_id, text, kw)
        raise A.Forbidden("Forbidden: bot can't initiate conversation with a user")

    async def send_photo(self, chat_id, media, **kw):
        self._log("send_photo", chat_id, media, kw)
        raise A.Forbidden("Forbidden: bot can't initiate conversation with a user")


def test_initiate_blocked_flow():
    print("\n[16] initiate-blocked: soft mark + pending DM + /start par delivery")
    A._DM_INITIATE_WARNED.clear()
    A.db.user_bots = [{"bot_id": "b1", "bot_username": "one", "bot_token": "t1", "user_id": 999}]
    A.db.gone_calls = []
    A.db.reachable_calls = []
    A.db.leave = {"enabled": True, "target_channel_id": -100999,
                  "target_channel_link": "https://t.me/joinchat/x",
                  "channel_configs": {"-100123": True},   # default OFF hai, isliye explicitly ON
                  "messages": [{"text": "Hello {first_name}, wapas aao", "buttons_json": ""}]}

    bot = InitiateBlockedBot("initiate")
    ctx = FakeCtx()
    ctx.bot = bot
    member = SimpleNamespace(id=6661, first_name="NoStart", is_bot=False)
    update = SimpleNamespace(chat_member=SimpleNamespace(
        chat=SimpleNamespace(id=-100123, title="Chan"),
        new_chat_member=SimpleNamespace(status="left", user=member),
        old_chat_member=SimpleNamespace(status="member")))

    handler, root, old = _capture_logs()
    try:
        run(A.handle_channel_member_update(update, ctx, "b1", 999))
    finally:
        _stop_capture(handler, root, old)
    warns = [r.getMessage() for r in handler.records if r.levelno == logging.WARNING]
    infos = [r.getMessage() for r in handler.records if r.levelno == logging.INFO]
    check("initiate: koi WARNING nahi (ye normal Telegram rule hai)", not warns, str(warns))
    check("initiate: ek INFO line", sum(1 for m in infos if "/start nahi kiya" in m) == 1, str(infos))
    check("initiate: soft mark mila", A.db.is_initiate_blocked("b1", 6661), str(A.db.gone_calls))
    check("initiate: hard (permanent) mark nahi mila",
          not any(c[1] == 6661 and "initiate" not in str(c[2]).lower() for c in A.db.gone_calls),
          str(A.db.gone_calls))
    check("initiate: DM pending me save hui",
          ("b1", 6661, -100123, -100999) in A.db.leave.get("pending_adds", []),
          str(A.db.leave.get("pending_adds")))

    # dobara leave -> koi API call nahi, koi nayi warning nahi
    bot.calls = []
    handler2, root2, old2 = _capture_logs()
    try:
        run(A.handle_channel_member_update(update, ctx, "b1", 999))
    finally:
        _stop_capture(handler2, root2, old2)
    check("initiate: dobara API call nahi", bot.calls == [], str(bot.calls))
    check("initiate: dobara warning/log spam nahi",
          not [r for r in handler2.records if r.levelno >= logging.WARNING],
          str([r.getMessage()[:60] for r in handler2.records if r.levelno >= logging.WARNING]))

    # user ne /start kiya -> pending leave-recovery DM apne aap chali jaye
    good = FakeBot("good")
    ctx2 = FakeCtx()
    ctx2.bot = good
    ctx2.args = []
    run(A.user_bot_start(_fake_update(uid=6661, msg=FakeMsg(chat_id=6661)), ctx2, "b1", 999))
    texts = [c[2] for c in good.calls if c[0] == "send_message" and c[1] == 6661]
    check("start: pending leave-recovery DM chali", any("wapas aao" in str(t) for t in texts), str(texts[:2]))
    check("start: soft mark clear ho gaya", not A.db.is_initiate_blocked("b1", 6661), str(A.db.gone_calls))
    check("start: pending row delete ho gayi", not A.db.leave.get("pending_rows"),
          str(A.db.leave.get("pending_rows")))
    check("start: bheji hui message history me save hui", bool(A.db.leave.get("sent")), str(A.db.leave.get("sent")))

    # target channel ki join request -> pending clear (user wapas aa gaya)
    A.db.leave["pending_rows"] = {("b1", 7772, -100999): {"bot_id": "b1", "user_id": 7772,
                                                          "source_channel_id": -100123,
                                                          "target_channel_id": -100999}}
    requester = SimpleNamespace(id=7772, first_name="Back", username="back", last_name="", is_bot=False)
    run(A.process_join_request("b1", 999, requester, -100999, "Target", None,
                               sender=good, approve=None, auto=True))
    check("target join: pending row clear", not A.db.leave.get("pending_rows"),
          str(A.db.leave.get("pending_rows")))

    # broadcast: initiate-blocked user hard-drop na ho (pehle yahi bug tha)
    A.db.leave = {"messages": []}
    A.db.gone_calls = []
    A.db.requesters = {"b2": [7771]}
    A.db.user_bots = A.db.user_bots + [{"bot_id": "b2", "bot_username": "two", "bot_token": "t2", "user_id": 999}]
    A.user_bot_applications["b2"] = SimpleNamespace(bot=InitiateBlockedBot("ib2"))
    bctx = FakeCtx()
    bctx.user_data["broadcast_draft_b2"] = {"text": "hi", "album": None, "buttons_json": None,
                                            "media": None, "media_type": None}
    handler3, root3, old3 = _capture_logs()
    try:
        run(A.send_user_broadcast(FakeQuery(bctx, uid=999), bctx, "b2", 999))
    finally:
        _stop_capture(handler3, root3, old3)
        A.user_bot_applications.pop("b2", None)
    infos3 = [r.getMessage() for r in handler3.records if r.levelno == logging.INFO]
    check("broadcast: initiate user hard mark nahi hua",
          not any(c[1] == 7771 and "initiate" not in str(c[2]).lower() for c in A.db.gone_calls),
          str(A.db.gone_calls))
    check("broadcast: soft mark mila", A.db.is_initiate_blocked("b2", 7771), str(A.db.gone_calls))
    check("broadcast: summary me start_baaki dikha",
          any("start_baaki=1" in m for m in infos3), str([m for m in infos3 if "broadcast b2" in m]))

    # source-level: nayi cheezein waqai code me hain (regression guard)
    source = open(A.__file__, encoding="utf-8").read()
    for needle in ("def mark_initiate_blocked", "def is_initiate_blocked",
                   "CREATE TABLE IF NOT EXISTS leave_recovery_pending",
                   "def deliver_pending_leave_recovery", "def record_dm_failure"):
        check(f"source: {needle}", needle in source)


def test_network_hiccup_throttle():
    print("\n[17] network hiccup log throttle")
    A._NETWORK_HICCUP_STATE.clear()
    fmt = A.MaskingFormatter("%(message)s")
    filt = A.TransientNetworkFilter()
    recs = []
    for i in range(3):
        rec = logging.LogRecord("telegram.ext.Updater.test", logging.ERROR, __file__, 1,
                                "Exception happened while polling for updates.", (), None)
        rec.exc_info = (A.NetworkError, A.NetworkError("httpx.ReadError"), None)
        rec.exc_text = "Traceback ... 40 lines"
        filt.filter(rec)
        recs.append(rec)
    check("hiccup: pehli line WARNING", recs[0].levelno == logging.WARNING, str(recs[0].levelno))
    check("hiccup: baaki lines DEBUG (throttle)",
          all(r.levelno == logging.DEBUG for r in recs[1:]), str([r.levelno for r in recs]))
    text = fmt.format(recs[0])
    check("hiccup: line me FORCE_IPV4 hint + chhoti line",
          "FORCE_IPV4" in text and len(text) < 300, text)
    # window khatam -> agli hiccup phir WARNING par (repeat count ke saath)
    A._NETWORK_HICCUP_STATE[("telegram.ext.Updater.test", "NetworkError")] = [0.0, 3]
    rec4 = logging.LogRecord("telegram.ext.Updater.test", logging.ERROR, __file__, 1,
                             "Exception happened while polling for updates.", (), None)
    rec4.exc_info = (A.NetworkError, A.NetworkError("httpx.ReadError"), None)
    filt.filter(rec4)
    check("hiccup: naye window par summary me repeat count",
          rec4.levelno == logging.WARNING and "min me" in fmt.format(rec4), fmt.format(rec4))


def _lr_admin_q(ctx, data):
    q = FakeQuery(ctx, uid=A.ADMIN_USER_ID)
    q.data = data
    run(A.callback_handler(_fake_update(q=q, uid=A.ADMIN_USER_ID), ctx))
    return q


def _lr_kb(q):
    """Aakhri edit ka reply_markup (safe_edit_message_text -> q.edits)."""
    return (q.edits[-1][1] or {}).get("reply_markup")


def test_leave_recovery_channels_panel():
    print("\n[18] leave recovery: saare channels + default OFF + Sab ON/OFF")
    # 25 channels (2 bots) - pehle panel sirf pehle 20 dikhata tha
    A.db.user_bots = [{"bot_id": "b1", "bot_username": "one", "bot_token": "t1", "user_id": 999},
                      {"bot_id": "b2", "bot_username": "two", "bot_token": "t2", "user_id": 999}]
    A.db.bot_channels = {
        "b1": [{"channel_id": -100000 - i, "channel_title": f"Chan {i:02d}", "auto_approve": 0}
               for i in range(15)],
        "b2": [{"channel_id": -100100 - i, "channel_title": f"Chan {i + 15}", "auto_approve": 0}
               for i in range(10)],
    }
    A.db.leave = {"enabled": True, "target_channel_id": -100999,
                  "target_channel_link": "https://t.me/x", "messages": [], "channel_configs": {}}
    A.db.settings = {}

    all_channels = A.leave_recovery_all_channels()
    check("panel: saare 25 channels milte hain (koi 20-cap nahi)", len(all_channels) == 25, str(len(all_channels)))

    ctx = FakeCtx()
    q = _lr_admin_q(ctx, "admin_leave_channels")
    labels = _kb_labels(_lr_kb(q))
    check("panel: default sab OFF (🔴)", all(l.startswith("🔴") for l in labels if "Chan" in l), str(labels))
    check("panel: Sab OFF / Sab ON buttons", any("Sab OFF" in l for l in labels) and any("Sab ON" in l for l in labels), str(labels))
    check("panel: text me default OFF likha hai", "Default sab channels OFF" in q.edits[-1][0], q.edits[-1][0][:80])
    check("panel: page 1/4 (8 per page, 25 channels)",
          "Page 1/4" in q.edits[-1][0], q.edits[-1][0][-120:])

    # pagination -> aage ke channels bhi dikhein
    q2 = _lr_admin_q(ctx, "admin_leave_chan_page_3")
    labels3 = _kb_labels(_lr_kb(q2))
    check("panel: page 4 par aakhri channel dikhta hai", any("Chan 24" in l for l in labels3), str(labels3))
    check("panel: page 4 text", "Page 4/4" in q2.edits[-1][0], q2.edits[-1][0][-120:])

    # toggle: default OFF -> ek tap me ON
    q3 = _lr_admin_q(ctx, "admin_leave_chan_toggle_-100000")
    check("toggle: channel ON ho gaya", A.db.leave["channel_configs"].get("-100000") is True,
          str(A.db.leave["channel_configs"]))
    check("toggle: toggle ke baad usi page par wapas", "Page 1/4" in q3.edits[-1][0], q3.edits[-1][0][-120:])
    check("toggle: current ON channels list me",
          A.leave_recovery_on_channels(A.db.leave) == ["-100000"],
          str(A.leave_recovery_on_channels(A.db.leave)))

    # status text: sirf ON channels dikhein + default OFF ka note
    status = A.leave_recovery_status_text()
    plain_status = A.strip_premium_emojis(status)
    check("status: ON channel dikhta hai", "-100000" in status and "Chan 00" in status, plain_status[:220])
    check("status: counts sahi (total 25, ON 1, OFF 24)",
          "total 25 | 🟢 ON 1 | 🔴 OFF 24" in plain_status, plain_status[:260])

    # Sab ON / Sab OFF
    q4 = _lr_admin_q(ctx, "admin_leave_all_on")
    on_ids = A.leave_recovery_on_channels(A.db.leave)
    check("sab ON: 25 channels ON", len(on_ids) == 25, str(len(on_ids)))
    check("sab ON: text me sab ON",
          "ON: 25" in A.strip_premium_emojis(q4.edits[-1][0]), A.strip_premium_emojis(q4.edits[-1][0])[:200])
    q5 = _lr_admin_q(ctx, "admin_leave_all_off")
    check("sab OFF: sab OFF ho gaye", A.leave_recovery_on_channels(A.db.leave) == [],
          str(A.db.leave["channel_configs"]))
    check("sab OFF: text me sab OFF",
          "ON: 0" in A.strip_premium_emojis(q5.edits[-1][0]), A.strip_premium_emojis(q5.edits[-1][0])[:200])

    # default OFF par leave recovery bilkul nahi chalti
    member = SimpleNamespace(id=9991, first_name="NoDM", is_bot=False)
    update = SimpleNamespace(chat_member=SimpleNamespace(
        chat=SimpleNamespace(id=-100000, title="Chan 00"),
        new_chat_member=SimpleNamespace(status="left", user=member),
        old_chat_member=SimpleNamespace(status="member")))
    lctx = FakeCtx()
    lctx.bot = FakeBot("lr")
    run(A.handle_channel_member_update(update, lctx, "b1", 999))
    check("default OFF: koi DM nahi jati", not lctx.bot.calls, str(lctx.bot.calls))

    # channel ON karne par DM chalti hai
    A.db.leave["channel_configs"]["-100000"] = True
    A.db.leave["messages"] = [{"text": "Hello {first_name}, wapas aao", "buttons_json": ""}]
    lctx2 = FakeCtx()
    lctx2.bot = FakeBot("lr2")
    run(A.handle_channel_member_update(update, lctx2, "b1", 999))
    check("channel ON: DM chali", any(c[0] == "send_message" and c[1] == 9991 for c in lctx2.bot.calls),
          str(lctx2.bot.calls[:2]))

    # one-time migration: purane ON channels OFF + dobara restart par kuch na chhedo
    A.db.leave = {"enabled": True, "target_channel_id": -100999, "target_channel_link": "https://t.me/x",
                  "messages": [], "channel_configs": {"-100000": True, "-100001": True}}
    A.db.settings = {}
    changed = A.migrate_leave_recovery_default_off()
    check("migration: purane ON channels OFF", changed >= 2 and A.leave_recovery_on_channels(A.db.leave) == [],
          f"changed={changed} cfg={A.db.leave['channel_configs']}")
    A.db.leave["channel_configs"]["-100002"] = True     # admin ne khud ON kiya
    changed2 = A.migrate_leave_recovery_default_off()
    check("migration: dobara restart par admin ka ON safe",
          changed2 == 0 and A.db.leave["channel_configs"].get("-100002") is True,
          f"changed2={changed2} cfg={A.db.leave['channel_configs']}")
    # Clear Pending Records -> queued DMs bhi saaf
    A.db.leave["pending_rows"] = {("b1", 5, -100999): {"bot_id": "b1", "user_id": 5,
                                                       "source_channel_id": -100000,
                                                       "target_channel_id": -100999}}
    A.db.bot_channels = {}
    _lr_admin_q(ctx, "admin_leave_clear_pending")
    check("clear pending: queued recovery DMs bhi clear",
          not A.db.leave.get("pending_rows"), str(A.db.leave.get("pending_rows")))

    source = open(A.__file__, encoding="utf-8").read()
    check("source: migration main() me chalti hai",
          "migrate_leave_recovery_default_off()" in source and "turned_off = migrate" in source)


def _check_non_admin_diag():
    """Non-admin /diag bheje to kuch na aaye."""
    msg = FakeMsg(text="/diag", chat_id=555,
                  from_user=SimpleNamespace(id=555, first_name="Random"))
    run(A.diag_command(_fake_update(msg=msg, uid=555), FakeCtx()))
    return msg.replies == []


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
    A.reset_premium_styling_state()
    test_admin_add_account_wizard()
    A.reset_premium_styling_state()
    A.reset_premium_styling_state()
    test_html_safety_and_welcome_spam()
    A.reset_premium_styling_state()
    A.reset_premium_styling_state()
    test_subscription_picker_and_style_memory()
    test_initiate_blocked_flow()
    test_leave_recovery_channels_panel()
    test_network_hiccup_throttle()
    test_diagnostics()
    print(f"\n==== tests: {len(PASS)} passed, {len(FAIL)} failed ====")
    if FAIL:
        for f in FAIL:
            print("  failed:", f)
        sys.exit(1)


if __name__ == "__main__":
    main()
