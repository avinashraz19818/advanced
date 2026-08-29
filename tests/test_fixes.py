"""Regression tests for the 4 fixes (runs WITHOUT Telegram / PostgreSQL).

Run:  python3 tests/test_fixes.py     (or: .venv/bin/python -m pytest tests -q)

The module under test is the real advanced.py: we only stub the psycopg2 driver
(so `db = Database()` can be constructed) and swap `advanced.db` for an in-memory
fake. Every assertion below drives the real production functions.
"""
import asyncio
import json
import os
import sys
import types

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# ---------------------------------------------------------------- psycopg2 stub
if "psycopg2" not in sys.modules:
    stub = types.ModuleType("psycopg2")
    extras = types.ModuleType("psycopg2.extras")

    class _Json:
        def __init__(self, obj):
            self.obj = obj

    extras.Json = _Json
    extras.RealDictCursor = object
    stub.extras = extras

    class _Cur:
        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def execute(self, *a, **k):
            pass

        def fetchone(self):
            return None

        def fetchall(self):
            return []

    class _Conn:
        autocommit = True

        def cursor(self, *a, **k):
            return _Cur()

    stub.connect = lambda *a, **k: _Conn()
    sys.modules["psycopg2"] = stub
    sys.modules["psycopg2.extras"] = extras

import advanced  # noqa: E402
from telegram.error import InvalidToken  # noqa: E402
from telegram import Update  # noqa: E402  (only for isinstance-free duck typing)


# ------------------------------------------------------------------ fakes -----
class FakeUser:
    def __init__(self, uid, first_name="Test", username="tester"):
        self.id = uid
        self.first_name = first_name
        self.last_name = ""
        self.username = username
        self.is_bot = False


class SentMessage:
    _counter = 500

    def __init__(self, text="", reply_markup=None, chat_id=0):
        SentMessage._counter += 1
        self.message_id = SentMessage._counter
        self.text = text
        self.reply_markup = reply_markup
        self.chat_id = chat_id
        self.deleted = False

    async def delete(self):
        self.deleted = True


class FakeBot:
    def __init__(self, bot_id=1):
        self.id = bot_id
        self.sent = []
        self.deleted = []

    async def send_message(self, chat_id, text, *a, **k):
        m = SentMessage(text, k.get("reply_markup"), chat_id)
        self.sent.append(m)
        return m

    async def get_chat_member(self, chat_id, user_id):
        return types.SimpleNamespace(status="administrator")

    async def delete_message(self, chat_id, message_id):
        self.deleted.append((chat_id, message_id))


class FakeQuery:
    def __init__(self, data, user):
        self.data = data
        self.from_user = user
        self.answered = 0
        self.edits = []
        self.message = types.SimpleNamespace(
            message_id=1,
            chat=types.SimpleNamespace(send_message=lambda *a, **k: SentMessage()),
            delete=lambda: asyncio.sleep(0),
        )

    async def answer(self, *a, **k):
        self.answered += 1

    async def edit_message_text(self, text=None, *a, **k):
        self.edits.append({"text": text, "reply_markup": k.get("reply_markup")})
        return SentMessage(text or "", k.get("reply_markup"))


class FakeMessage:
    def __init__(self, text=None, user=None, chat_id=111, message_id=42):
        self.text = text
        self.caption = None
        self.entities = None
        self.caption_entities = None
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
        self.venue = None
        self.poll = None
        self.dice = None
        self.media_group_id = None
        self.forward_origin = None
        self.forward_from_chat = None
        self.reply_to_message = None
        self.message_id = message_id
        self.chat_id = chat_id
        self.from_user = user
        self.replies = []

    async def reply_text(self, text, *a, **k):
        m = SentMessage(text, k.get("reply_markup"), self.chat_id)
        self.replies.append(m)
        return m


class FakeUpdate:
    def __init__(self, user, message=None, query=None):
        self.effective_user = user
        self.message = message
        self.callback_query = query
        self.effective_message = message
        self.chat_join_request = None
        self.chat_member = None


class FakeContext:
    def __init__(self, bot=None):
        self.user_data = {}
        self.bot_data = {}
        self.chat_data = {}
        self.bot = bot or FakeBot()
        self.args = []
        self.job_queue = types.SimpleNamespace(run_once=lambda *a, **k: None)


class FakeDB(advanced.Database):
    """In-memory stand-in: only the methods the tested paths touch."""

    def __init__(self):  # deliberately does NOT call super().__init__
        self.bots = {}
        self.channels = {}
        self.messages = {}
        self.settings = {}
        self.reachable = set()
        self.join_requests = []
        self.leave_msgs = []
        self._next_msg_id = 1

    # -- bots
    def get_user_bot(self, bot_id):
        return self.bots.get(bot_id)

    def get_user_bots_by_owner(self, user_id):
        return [b for b in self.bots.values() if b["user_id"] == user_id]

    def get_all_user_bots(self):
        return list(self.bots.values())

    def get_subscription_for_bot(self, bot_id):
        return {"subscription_type": "Pro", "expiry_date": advanced.now_aware() + advanced.timedelta(days=30), "max_channels": 5}

    # -- channels
    def get_bot_channels(self, bot_id):
        return [c for c in self.channels.values() if c["bot_id"] == bot_id]

    def get_channel_owner_data(self, channel_id, bot_id=None):
        for c in self.channels.values():
            if c["channel_id"] == channel_id and (bot_id is None or c["bot_id"] == bot_id):
                return c
        return None

    def add_channel(self, bot_id, channel_id, username, title):
        self.channels[(bot_id, channel_id)] = {
            "bot_id": bot_id, "channel_id": channel_id, "channel_username": username,
            "channel_title": title, "welcome_message": None, "welcome_media_id": None,
            "welcome_media_type": None, "auto_approve": 0,
        }

    # -- messages
    def add_message(self, bot_id, channel_id, text, media_id, media_type, media_group_id=None,
                    entities_json=None, file_name=None, mime_type=None, telegram_message_id=None):
        mid = self._next_msg_id
        self._next_msg_id += 1
        self.messages[mid] = {
            "id": mid, "bot_id": bot_id, "channel_id": channel_id, "content_text": text,
            "media_id": media_id, "media_type": media_type, "media_group_id": media_group_id,
            "buttons_json": None, "entities_json": entities_json, "file_name": file_name,
            "mime_type": mime_type, "telegram_message_id": telegram_message_id,
        }
        return mid

    def get_messages(self, channel_id, bot_id=None):
        return [m for m in self.messages.values()
                if m["channel_id"] == channel_id and (bot_id is None or m["bot_id"] == bot_id)]

    def get_message_by_id(self, msg_id):
        return self.messages.get(int(msg_id))

    def update_message_buttons(self, msg_id, buttons_json):
        self.messages[int(msg_id)]["buttons_json"] = buttons_json

    def append_message_buttons(self, msg_id, new_buttons_json):
        row = self.messages.get(int(msg_id))
        existing = []
        if row and row.get("buttons_json"):
            try:
                existing = json.loads(row["buttons_json"]) or []
            except Exception:
                existing = []
        try:
            extra = json.loads(new_buttons_json) or []
        except Exception:
            extra = []
        row["buttons_json"] = json.dumps(existing + extra)

    def save_user_emoji_map(self, *a, **k):
        pass

    def mark_reachable(self, bot_id, requester_id):
        self.reachable.add((bot_id, requester_id))

    def mark_unreachable(self, bot_id, requester_id):
        self.reachable.discard((bot_id, requester_id))

    def add_join_request(self, bot_id, requester_id, channel_id, status):
        self.join_requests.append({"bot_id": bot_id, "requester_id": requester_id,
                                   "channel_id": channel_id, "status": status})

    def get_pending_leave_recovery_messages(self, bot_id, user_id, target_channel_id):
        return [(r["id"], r["message_id"]) for r in self.leave_msgs
                if r["bot_id"] == bot_id and r["user_id"] == user_id
                and r["target_channel_id"] == target_channel_id and not r["deleted"]]

    def add_leave_recovery_message(self, bot_id, user_id, source_channel_id, target_channel_id, message_id):
        self.leave_msgs.append({"id": len(self.leave_msgs) + 1, "bot_id": bot_id, "user_id": user_id,
                                "source_channel_id": source_channel_id,
                                "target_channel_id": target_channel_id,
                                "message_id": message_id, "deleted": False})

    def mark_leave_recovery_deleted(self, row_id):
        for r in self.leave_msgs:
            if r["id"] == row_id:
                r["deleted"] = True

    def get_default_first_message(self):
        return "Hello {first_name}"

    # -- misc used by the admin userbot screens / lifecycle
    def get_user(self, user_id):
        return {"user_id": user_id, "username": "someone", "first_name": "Someone"}

    def get_total_requesters_count(self, bot_id):
        return len(self.join_requests)

    def get_reachable_requesters_count(self, bot_id):
        return len([r for r in self.reachable if r[0] == bot_id])

    def set_user_bot_active(self, bot_id, active):
        if bot_id in self.bots:
            self.bots[bot_id]["is_active"] = 1 if active else 0

    def get_requesters_for_bot(self, bot_id):
        return []

    def get_pending_requests(self, bot_id):
        return []

    def get_expired_subscriptions(self):
        return []

    def get_all_subscriptions(self):
        return []

    # -- settings (leave recovery config lives here)
    def get_setting(self, key, default=None):
        return self.settings.get(key, default)

    def set_setting(self, key, value):
        self.settings[key] = value


# ------------------------------------------------------------------ fixtures --
ADMIN = 8015937475
CLIENT = 555000111
CHANNEL = -1001234567890
BOT_ID = f"{CLIENT}_1"


def make_world():
    db = FakeDB()
    db.bots[BOT_ID] = {"bot_id": BOT_ID, "user_id": CLIENT, "bot_username": "client_bot",
                       "bot_token": "tok", "is_active": 1}
    db.add_channel(BOT_ID, CHANNEL, None, "Client Channel")
    advanced.db = db
    return db


def last_text(edits_or_msgs):
    return (edits_or_msgs[-1].get("text") if isinstance(edits_or_msgs[-1], dict)
            else edits_or_msgs[-1].text) or ""


# ------------------------------------------------------------------ tests -----
def test_emoji_preserved_in_inline_buttons():
    """Issue 4: owner's own emoji must survive into the button label."""
    js = advanced.buttons_json_from_text("🔥 Join Now|https://t.me/abc")
    data = json.loads(js)
    assert data[0][0]["text"] == "🔥 Join Now", data
    assert data[0][0]["url"] == "https://t.me/abc", data
    assert "icon_id" not in data[0][0], "new buttons must not force a default icon"

    markup = advanced.buttons_to_markup(js)
    b = markup.inline_keyboard[0][0]
    assert b.text == "🔥 Join Now" and b.url == "https://t.me/abc"
    assert not getattr(b, "icon_custom_emoji_id", None), b.to_dict()

    # two buttons in one row + a plain (non mapped) emoji
    js2 = advanced.buttons_json_from_text("🚀 Open|https://a.t || ⭐️ Star|https://b.t")
    row = advanced.buttons_to_markup(js2).inline_keyboard[0]
    assert [x.text for x in row] == ["🚀 Open", "⭐️ Star"], [x.text for x in row]
    assert [x.url for x in row] == ["https://a.t", "https://b.t"]


def test_legacy_buttons_still_render_with_icon():
    """Old rows (stripped label + icon_id) must look exactly like before."""
    legacy = json.dumps([[{"text": "Join Now", "url": "https://t.me/abc",
                           "icon_id": "5042101437237036298"}]])
    b = advanced.buttons_to_markup(legacy).inline_keyboard[0][0]
    assert b.text == "Join Now"
    assert b.api_kwargs.get("icon_custom_emoji_id") == "5042101437237036298", b.to_dict()


def test_set_inline_button_callback_is_not_dead_anymore():
    """Issue 1: 'Set Inline Button' must arm the waiting_buttons state."""
    db = make_world()
    client = FakeUser(CLIENT, "Client")
    ctx = FakeContext()
    msg_id = db.add_message(BOT_ID, CHANNEL, "Hello {first_name}", None, None)

    q = FakeQuery(f"setbtn_{BOT_ID}_{msg_id}", client)
    upd = FakeUpdate(client, query=q)
    asyncio.run(advanced.user_bot_callback(upd, ctx, BOT_ID, CLIENT))

    ud = ctx.user_data[f"{CLIENT}_{BOT_ID}"]
    assert ud.get("waiting_buttons") == {"msg_id": msg_id}, ud
    assert "Send Inline Buttons" in last_text(q.edits), q.edits

    # ...and the button lines that follow are stored on that message
    m = FakeMessage("🎬 Watch Video|https://t.me/v || 💰 Buy|https://t.me/b", client)
    asyncio.run(advanced.handle_user_bot_message(FakeUpdate(client, message=m), ctx, BOT_ID, CLIENT))
    saved = json.loads(db.messages[msg_id]["buttons_json"])
    assert saved[0][0]["text"] == "🎬 Watch Video", saved
    assert saved[0][1]["text"] == "💰 Buy", saved
    assert ud.get("waiting_buttons") is None, "state must be cleared after saving"


def test_set_inline_button_group_callback():
    """setbtng_<bot> (media album) must arm waiting_buttons with msg_ids."""
    db = make_world()
    client = FakeUser(CLIENT, "Client")
    ctx = FakeContext()
    m1 = db.add_message(BOT_ID, CHANNEL, "album 1", "f1", "photo", "grp1")
    m2 = db.add_message(BOT_ID, CHANNEL, "album 2", "f2", "photo", "grp1")
    ud = advanced._runtime_store(ctx, f"{CLIENT}_{BOT_ID}")
    ud["pending_buttons_group"] = {"msg_ids": [m1, m2]}

    q = FakeQuery(f"setbtng_{BOT_ID}", client)
    asyncio.run(advanced.user_bot_callback(FakeUpdate(client, query=q), ctx, BOT_ID, CLIENT))
    assert ud.get("waiting_buttons") == {"msg_ids": [m1, m2]}, ud


def test_main_bot_manage_panel_buttons_work():
    """Issue 2: ub_*/setmsg_* callbacks on the MAIN bot used to be dead."""
    db = make_world()
    client = FakeUser(CLIENT, "Client")
    ctx = FakeContext()

    # open the manage panel from the main bot
    q = FakeQuery(f"manage_bot_{BOT_ID}", client)
    asyncio.run(advanced.callback_handler(FakeUpdate(client, query=q), ctx))
    assert ctx.user_data.get("managing_bot_id") == BOT_ID

    # click "Set Message(s)" on the MAIN bot -> flow must start
    q2 = FakeQuery(f"ub_set_message_{BOT_ID}", client)
    asyncio.run(advanced.callback_handler(FakeUpdate(client, query=q2), ctx))
    assert advanced.has_active_flow(ctx, CLIENT, BOT_ID), ctx.user_data
    assert "SET MESSAGES" in last_text(q2.edits), q2.edits

    # now typing a message must SAVE it (not answer "Use menu")
    m = FakeMessage("Welcome {first_name}!", client)
    asyncio.run(advanced.handle_message(FakeUpdate(client, message=m), ctx))
    msgs = db.get_messages(CHANNEL, BOT_ID)
    assert len(msgs) == 1 and msgs[0]["content_text"] == "Welcome {first_name}!", msgs
    assert not any("Use menu" in (x.text or "") for x in m.replies), [x.text for x in m.replies]

    # "Set Inline Button" from the main bot works too
    q3 = FakeQuery(f"setbtn_{BOT_ID}_{msgs[0]['id']}", client)
    asyncio.run(advanced.callback_handler(FakeUpdate(client, query=q3), ctx))
    ud = ctx.user_data[f"{CLIENT}_{BOT_ID}"]
    assert ud.get("waiting_buttons") == {"msg_id": msgs[0]["id"]}, ud

    m2 = FakeMessage("🔗 Link|https://t.me/x", client)
    asyncio.run(advanced.handle_message(FakeUpdate(client, message=m2), ctx))
    assert json.loads(db.messages[msgs[0]["id"]]["buttons_json"])[0][0]["text"] == "🔗 Link"


def test_second_message_can_be_set_after_done():
    """Issue 2: after 'done', starting a new message flow must work again."""
    db = make_world()
    client = FakeUser(CLIENT, "Client")
    ctx = FakeContext()

    async def flow(label, text):
        q = FakeQuery(f"ub_set_message_{BOT_ID}", client)
        await advanced.user_bot_callback(FakeUpdate(client, query=q), ctx, BOT_ID, CLIENT)
        m = FakeMessage(text, client)
        await advanced.handle_user_bot_message(FakeUpdate(client, message=m), ctx, BOT_ID, CLIENT)
        assert label in (m.replies[-1].text or ""), m.replies[-1].text
        qd = FakeQuery(f"setmsg_done_{BOT_ID}", client)
        await advanced.user_bot_callback(FakeUpdate(client, query=qd), ctx, BOT_ID, CLIENT)

    asyncio.run(flow("Message saved", "First message"))
    asyncio.run(flow("Message saved", "Second message"))
    texts = [m["content_text"] for m in db.get_messages(CHANNEL, BOT_ID)]
    assert texts == ["First message", "Second message"], texts


def test_stale_state_does_not_swallow_new_message():
    """A leftover editing_/waiting_buttons flag must not eat the next message."""
    db = make_world()
    client = FakeUser(CLIENT, "Client")
    ctx = FakeContext()
    stale = db.add_message(BOT_ID, CHANNEL, "old", None, None)
    ud = advanced._runtime_store(ctx, f"{CLIENT}_{BOT_ID}")
    ud["waiting_buttons"] = {"msg_id": stale}   # abandoned button prompt
    ud["editing_buttons_msg_id"] = stale

    q = FakeQuery(f"ub_set_message_{BOT_ID}", client)
    asyncio.run(advanced.user_bot_callback(FakeUpdate(client, query=q), ctx, BOT_ID, CLIENT))
    assert ud.get("waiting_buttons") is None and ud.get("editing_buttons_msg_id") is None, ud

    m = FakeMessage("Brand new welcome", client)
    asyncio.run(advanced.handle_user_bot_message(FakeUpdate(client, message=m), ctx, BOT_ID, CLIENT))
    assert "Message saved" in (m.replies[-1].text or ""), m.replies[-1].text
    assert db.messages[stale]["buttons_json"] is None, "stale message must stay untouched"


def test_back_to_menu_ends_flow():
    db = make_world()
    client = FakeUser(CLIENT, "Client")
    ctx = FakeContext()
    q = FakeQuery(f"ub_set_message_{BOT_ID}", client)
    asyncio.run(advanced.user_bot_callback(FakeUpdate(client, query=q), ctx, BOT_ID, CLIENT))
    assert advanced.has_active_flow(ctx, CLIENT, BOT_ID)
    q2 = FakeQuery(f"manage_bot_{BOT_ID}", client)
    asyncio.run(advanced.user_bot_callback(FakeUpdate(client, query=q2), ctx, BOT_ID, CLIENT))
    assert not advanced.has_active_flow(ctx, CLIENT, BOT_ID), ctx.user_data


def test_unknown_callback_is_not_silent():
    db = make_world()
    client = FakeUser(CLIENT, "Client")
    ctx = FakeContext()
    q = FakeQuery(f"ub_something_new_{BOT_ID}", client)
    asyncio.run(advanced.user_bot_callback(FakeUpdate(client, query=q), ctx, BOT_ID, CLIENT))
    assert q.edits and "purane session" in (q.edits[-1]["text"] or ""), q.edits


def test_handler_order_setbtn_first():
    """The userbot registers the setbtn handler BEFORE the catch-all one."""
    src = open(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                            "advanced.py"), encoding="utf-8").read()
    body = src[src.index("async def start_user_bot("):]
    body = body[:body.index("async def stop_user_bot(")]
    i_specific = body.index("handle_set_buttons_callback(u, c, bot_id, owner_id)")
    i_generic = body.index("user_bot_callback(u, c, bot_id, owner_id)")
    assert i_specific < i_generic, "specific setbtn handler must be registered first"

    from telegram.ext import CallbackQueryHandler
    bid = BOT_ID
    specific = CallbackQueryHandler(lambda u, c: None, pattern=f"^(setbtn_{bid}_|setbtng_{bid}$)")
    generic = CallbackQueryHandler(lambda u, c: None,
                                   pattern=f"^(ub_|ubm_|ubmm_|delmsg_|setbtn_|setbtng|setmsg_|bcast_|"
                                           f"removechan_|back_to_manage_|manage_bot_|toggleauto_|sub_for_bot_|"
                                           f"sub_basic_|sub_pro_|setbtn_addmore_|setbtng_addmore_).*{bid}|^main_menu$")
    for data in (f"setbtn_{bid}_7", f"setbtng_{bid}"):
        assert specific.pattern.match(data), data
    for data in (f"setbtn_addmore_{bid}_7", f"setbtng_addmore_{bid}", f"setmsg_done_{bid}"):
        assert not specific.pattern.match(data), data
        assert generic.pattern.match(data), data
    assert generic.pattern.match(f"sub_for_bot_{bid}")
    assert generic.pattern.match("main_menu")


def test_leave_recovery_default_off():
    """Issue 3: leave recovery is OFF by default - globally and per channel."""
    db = make_world()
    cfg = db.get_leave_recovery_config()
    assert cfg["enabled"] is False, cfg
    assert advanced.leave_recovery_channel_enabled(cfg, CHANNEL) is False
    assert advanced.leave_recovery_channel_enabled(cfg, "-100999") is False

    # explicitly enabled channel keeps working (old setups unaffected)
    cfg["enabled"] = True
    cfg["channel_configs"] = {str(CHANNEL): True}
    db.set_setting("leave_recovery", cfg)
    cfg2 = db.get_leave_recovery_config()
    assert advanced.leave_recovery_channel_enabled(cfg2, CHANNEL) is True
    assert advanced.leave_recovery_channel_enabled(cfg2, -100555) is False


def test_admin_can_add_own_bot_with_token():
    """The 'Add New Bot' token flow no longer dead-ends for admins."""
    db = make_world()
    admin = FakeUser(ADMIN, "Admin")
    ctx = FakeContext()
    advanced.ADMIN_USER_IDS.add(ADMIN)

    q = FakeQuery("add_new_bot", admin)
    asyncio.run(advanced.callback_handler(FakeUpdate(admin, query=q), ctx))
    assert ctx.user_data.get("waiting_token") is True

    m = FakeMessage("not-a-token", admin)
    asyncio.run(advanced.handle_message(FakeUpdate(admin, message=m), ctx))
    assert "Invalid token format" in (m.replies[-1].text or ""), m.replies[-1].text
    assert ctx.user_data.get("waiting_token") is None, "flag must be cleared, no menu spam"
    assert not any("Use menu" in (x.text or "") for x in m.replies), [x.text for x in m.replies]


def test_start_user_bot_registers_setbtn_handler_first():
    """Runs the real start_user_bot(): the setbtn handler must come first."""
    from telegram.ext import CallbackQueryHandler
    db = make_world()
    built = {}

    class FakeUpdater:
        async def start_polling(self, *a, **k):
            return None

        async def stop(self):
            pass

    class FakeApp:
        def __init__(self):
            self.handlers = {}
            self.bot_data = {}
            self.updater = FakeUpdater()
            self.bot = FakeBot()

        def add_handler(self, h, group=0):
            self.handlers.setdefault(group, []).append(h)

        async def initialize(self):
            pass

        async def start(self):
            pass

        async def stop(self):
            pass

        async def shutdown(self):
            pass

    class FakeBuilder:
        def token(self, t):
            return self

        def concurrent_updates(self, v):
            return self

        def request(self, r):
            return self

        def build(self):
            app = FakeApp()
            built["app"] = app
            return app

    old_builder = advanced.ApplicationBuilder
    advanced.ApplicationBuilder = FakeBuilder
    try:
        asyncio.run(advanced.start_user_bot("1234:fake-token", BOT_ID, CLIENT))
    finally:
        advanced.ApplicationBuilder = old_builder
        advanced.user_bot_applications.pop(BOT_ID, None)

    app = built["app"]
    cq = [h for h in app.handlers[0] if isinstance(h, CallbackQueryHandler)]
    assert len(cq) == 3, [type(h).__name__ for h in app.handlers[0]]
    assert cq[1].pattern.match(f"setbtn_{BOT_ID}_9"), "setbtn handler must be 2nd (first callback one)"
    assert cq[1].pattern.match(f"setbtng_{BOT_ID}")
    assert not cq[1].pattern.match(f"setbtn_addmore_{BOT_ID}_9")
    assert not cq[1].pattern.match(f"setbtng_addmore_{BOT_ID}")
    # the generic one still claims everything else
    for data in (f"ub_set_message_{BOT_ID}", f"setmsg_done_{BOT_ID}", f"setbtn_addmore_{BOT_ID}_9",
                 f"setbtng_addmore_{BOT_ID}", f"sub_for_bot_{BOT_ID}", "main_menu",
                 f"ubmm_{BOT_ID}_{CHANNEL}_9", f"toggleauto_{BOT_ID}_{CHANNEL}"):
        assert cq[2].pattern.match(data), data


def test_managed_bot_id_resolver():
    db = make_world()
    assert advanced.managed_bot_id_from_data(f"ub_set_message_{BOT_ID}", CLIENT) == BOT_ID
    assert advanced.managed_bot_id_from_data(f"ubmm_{BOT_ID}_{CHANNEL}_9", CLIENT) == BOT_ID
    assert advanced.managed_bot_id_from_data(f"setbtn_{BOT_ID}_9", CLIENT) == BOT_ID
    assert advanced.managed_bot_id_from_data(f"setbtng_addmore_{BOT_ID}", CLIENT) == BOT_ID
    assert advanced.managed_bot_id_from_data(f"toggleauto_{BOT_ID}_{CHANNEL}", CLIENT) == BOT_ID
    assert advanced.managed_bot_id_from_data("admin_panel", CLIENT) is None
    assert advanced.managed_bot_id_from_data("main_menu", CLIENT) is None
    # admin may resolve any bot, a stranger may not
    assert advanced.managed_bot_id_from_data(f"ub_stats_{BOT_ID}", ADMIN) == BOT_ID
    assert advanced.managed_bot_id_from_data(f"ub_stats_{BOT_ID}", 777) is None


def test_permission_denied_for_stranger():
    db = make_world()
    stranger = FakeUser(999888, "Stranger")
    ctx = FakeContext()
    q = FakeQuery(f"ub_set_message_{BOT_ID}", stranger)
    asyncio.run(advanced.user_bot_callback(FakeUpdate(stranger, query=q), ctx, BOT_ID, CLIENT))
    assert "permission" in (last_text(q.edits) or "").lower(), q.edits
    assert not advanced.has_active_flow(ctx, 999888, BOT_ID)



TARGET = -1009998887776


def _leave_cfg(enabled=True, channels=None):
    return {"enabled": enabled, "target_channel_id": TARGET,
            "target_channel_link": "https://t.me/+recover",
            "messages": [{"text": "Aap leave ho gaye {first_name}", "buttons_json": ""}],
            "channel_configs": channels or {}}


def _member_update(new_status, old_status, user, chat_id=CHANNEL, title="Client Channel"):
    return types.SimpleNamespace(
        chat=types.SimpleNamespace(id=chat_id, title=title),
        new_chat_member=types.SimpleNamespace(status=new_status, user=user),
        old_chat_member=types.SimpleNamespace(status=old_status, user=user),
    )


def _leave_update(user, chat_id=CHANNEL):
    """A real chat_member update: member -> left."""
    return types.SimpleNamespace(
        chat_member=_member_update("left", "member", user, chat_id=chat_id),
        effective_user=user, message=None, callback_query=None, chat_join_request=None)


def test_leave_recovery_not_sent_when_channel_not_enabled():
    """Issue 3: user leaves -> NO recovery DM unless the channel was switched ON."""
    db = make_world()
    db.set_setting("leave_recovery", _leave_cfg(enabled=True))
    ctx = FakeContext()

    asyncio.run(advanced.handle_channel_member_update(
        _leave_update(FakeUser(4242, "Leaver")), ctx, BOT_ID, CLIENT))
    assert db.leave_msgs == [], db.leave_msgs
    assert all("leave ho gaye" not in (m.text or "") for m in ctx.bot.sent), [m.text for m in ctx.bot.sent]


def test_leave_recovery_not_sent_when_globally_disabled():
    db = make_world()
    db.set_setting("leave_recovery", _leave_cfg(enabled=False, channels={str(CHANNEL): True}))
    ctx = FakeContext()
    asyncio.run(advanced.handle_channel_member_update(
        _leave_update(FakeUser(4242, "Leaver")), ctx, BOT_ID, CLIENT))
    assert db.leave_msgs == []


def test_leave_recovery_sent_when_channel_explicitly_on():
    """Old setups keep working: an explicitly enabled channel still sends the DM."""
    db = make_world()
    db.set_setting("leave_recovery", _leave_cfg(enabled=True, channels={str(CHANNEL): True}))
    ctx = FakeContext()
    asyncio.run(advanced.handle_channel_member_update(
        _leave_update(FakeUser(4242, "Leaver")), ctx, BOT_ID, CLIENT))
    assert len(db.leave_msgs) == 1, db.leave_msgs
    assert any("leave ho gaye" in (m.text or "") for m in ctx.bot.sent), [m.text for m in ctx.bot.sent]


def test_rejoin_clears_pending_recovery_dms():
    """Rejoining the TARGET channel deletes the old DMs even though the per-channel
    switch defaults to OFF."""
    db = make_world()
    db.set_setting("leave_recovery", _leave_cfg(enabled=True))
    db.leave_msgs.append({"id": 1, "bot_id": BOT_ID, "user_id": 4242,
                          "source_channel_id": CHANNEL, "target_channel_id": TARGET,
                          "message_id": 777, "deleted": False})
    ctx = FakeContext()
    upd = types.SimpleNamespace(chat_member=_member_update("member", "left", FakeUser(4242, "Back"),
                                                           chat_id=TARGET),
                                effective_user=None, message=None, callback_query=None,
                                chat_join_request=None)
    asyncio.run(advanced.handle_channel_member_update(upd, ctx, BOT_ID, CLIENT))
    assert db.leave_msgs[0]["deleted"] is True, db.leave_msgs
    assert (4242, 777) in ctx.bot.deleted, ctx.bot.deleted


def test_join_request_cleanup_on_target_channel():
    db = make_world()
    db.set_setting("leave_recovery", _leave_cfg(enabled=True))
    db.leave_msgs.append({"id": 1, "bot_id": BOT_ID, "user_id": 4242,
                          "source_channel_id": CHANNEL, "target_channel_id": TARGET,
                          "message_id": 888, "deleted": False})
    ctx = FakeContext()
    approved = []

    class JR:
        from_user = FakeUser(4242, "Back")
        chat = types.SimpleNamespace(id=TARGET, title="Recovery")

        async def approve(self):
            approved.append(4242)

    upd = types.SimpleNamespace(chat_join_request=JR(), effective_user=FakeUser(4242, "Back"),
                                message=None, callback_query=None, chat_member=None)
    asyncio.run(advanced.handle_join_request(upd, ctx, BOT_ID, CLIENT))
    assert db.leave_msgs[0]["deleted"] is True, db.leave_msgs
    assert approved == [4242]



def test_bot_add_token_option_is_admin_only():
    """Token/bot-add option sirf admin panel me - client ke panel se hat gaya."""
    db = make_world()
    advanced.ADMIN_USER_IDS.add(ADMIN)

    client_kb = advanced.main_menu_kb(CLIENT)
    client_cbs = [b.callback_data for row in client_kb.inline_keyboard for b in row]
    assert "add_new_bot" not in client_cbs, client_cbs

    admin_kb = advanced.main_menu_kb(ADMIN)
    admin_cbs = [b.callback_data for row in admin_kb.inline_keyboard for b in row]
    assert "add_new_bot" in admin_cbs, admin_cbs

    # client callback kare to refuse ho, waiting_token set na ho
    ctx = FakeContext()
    stranger = FakeUser(CLIENT, "Client")
    q = FakeQuery("add_new_bot", stranger)
    asyncio.run(advanced.callback_handler(FakeUpdate(stranger, query=q), ctx))
    assert ctx.user_data.get("waiting_token") is None, ctx.user_data
    assert "sirf admin" in (last_text(q.edits) or ""), q.edits


def test_admin_opens_client_bot_panel_from_admin_panel():
    """Admin ko client ke bot ka full control milta hai (manage_bot_<client bot>)."""
    db = make_world()
    admin = FakeUser(ADMIN, "Admin")
    advanced.ADMIN_USER_IDS.add(ADMIN)
    ctx = FakeContext()

    q = FakeQuery(f"manage_bot_{BOT_ID}", admin)
    asyncio.run(advanced.callback_handler(FakeUpdate(admin, query=q), ctx))
    assert ctx.user_data.get("managing_bot_id") == BOT_ID
    assert "MANAGE BOT" in (last_text(q.edits) or ""), q.edits

    # admin sets a message for the client's bot from the main bot
    q2 = FakeQuery(f"ub_set_message_{BOT_ID}", admin)
    asyncio.run(advanced.callback_handler(FakeUpdate(admin, query=q2), ctx))
    m = FakeMessage("Admin ne set kiya", admin)
    asyncio.run(advanced.handle_message(FakeUpdate(admin, message=m), ctx))
    msgs = db.get_messages(CHANNEL, BOT_ID)
    assert len(msgs) == 1 and msgs[0]["content_text"] == "Admin ne set kiya", msgs

    # admin ke manage panel me "Manage Panel" button hota hai
    info_kb = None
    q3 = FakeQuery(f"admin_ub_info_{BOT_ID}", admin)
    asyncio.run(advanced.callback_handler(FakeUpdate(admin, query=q3), ctx))
    info_kb = q3.edits[-1]["reply_markup"]
    cbs = [b.callback_data for row in info_kb.inline_keyboard for b in row]
    assert f"manage_bot_{BOT_ID}" in cbs, cbs


def test_client_message_goes_to_setup_flow_not_support():
    """Client (non-admin) ka message support inbox me nahi, apne flow me jaana chahiye."""
    db = make_world()
    client = FakeUser(CLIENT, "Client")
    ctx = FakeContext()
    q = FakeQuery(f"manage_bot_{BOT_ID}", client)
    asyncio.run(advanced.callback_handler(FakeUpdate(client, query=q), ctx))
    q2 = FakeQuery(f"ub_set_message_{BOT_ID}", client)
    asyncio.run(advanced.callback_handler(FakeUpdate(client, query=q2), ctx))

    m = FakeMessage("Client ka message", client)
    asyncio.run(advanced.handle_message(FakeUpdate(client, message=m), ctx))
    assert len(db.get_messages(CHANNEL, BOT_ID)) == 1
    assert not any("Message sent to support" in (x.text or "") for x in m.replies), [x.text for x in m.replies]


def test_start_user_bot_is_idempotent():
    """Do baar start karne par purana poller band hona chahiye (getUpdates conflict fix)."""
    db = make_world()
    built = []

    class FakeUpdater:
        def __init__(self):
            self.stopped = False

        async def start_polling(self, *a, **k):
            pass

        async def stop(self):
            self.stopped = True

    class FakeApp:
        def __init__(self):
            self.handlers = {}
            self.bot_data = {}
            self.updater = FakeUpdater()
            self.bot = FakeBot()
            self.stopped = False
            self.shutdown_called = False

        def add_handler(self, h, group=0):
            self.handlers.setdefault(group, []).append(h)

        async def initialize(self):
            pass

        async def start(self):
            pass

        async def stop(self):
            self.stopped = True

        async def shutdown(self):
            self.shutdown_called = True

    class FakeBuilder:
        def token(self, t):
            return self

        def concurrent_updates(self, v):
            return self

        def request(self, r):
            return self

        def build(self):
            app = FakeApp()
            built.append(app)
            return app

    old_builder = advanced.ApplicationBuilder
    advanced.ApplicationBuilder = FakeBuilder
    try:
        asyncio.run(advanced.start_user_bot("1234:fake", BOT_ID, CLIENT))
        asyncio.run(advanced.start_user_bot("1234:fake", BOT_ID, CLIENT))
    finally:
        advanced.ApplicationBuilder = old_builder
        advanced.user_bot_applications.pop(BOT_ID, None)

    assert len(built) == 2, built
    assert built[0].stopped and built[0].shutdown_called, "old instance must be shut down"
    assert not built[1].stopped


def test_main_stops_userbots_on_fatal_error():
    """main() failure par userbots band hone chahiye (orphan pollers = setup flow loss)."""
    calls = []

    async def boom():
        raise InvalidToken("The token was rejected by the server")

    async def fake_stop():
        calls.append("stopped")

    old_run, old_stop = advanced._run_main, advanced.stop_all_userbots
    advanced._run_main, advanced.stop_all_userbots = boom, fake_stop
    try:
        try:
            asyncio.run(advanced.main())
            raise AssertionError("main() must re-raise")
        except InvalidToken:
            pass
    finally:
        advanced._run_main, advanced.stop_all_userbots = old_run, old_stop
    assert calls == ["stopped"], calls


def test_dotenv_is_loaded():
    path = "/tmp/_adv_test.env"
    with open(path, "w", encoding="utf-8") as fh:
        fh.write("# comment\nMAIN_BOT_TOKEN=111:from-dotenv\nQUOTED=\"abc\"\n")
    os.environ.pop("QUOTED", None)
    advanced._load_dotenv(path)
    assert os.environ.get("MAIN_BOT_TOKEN") == "111:from-dotenv" or os.environ.get("QUOTED") == "abc"
    assert os.environ.get("QUOTED") == "abc", os.environ.get("QUOTED")



def test_userbots_survive_invalid_main_token():
    """Real _run_main(): main token kharab ho to userbots chalte rahein, process na mare."""
    db = make_world()
    db.bots[BOT_ID]["bot_token"] = "1234:userbot-token"
    created = []

    class FakeJobQueue:
        def run_repeating(self, *a, **k):
            return None

    class FakeUpdater:
        async def start_polling(self, *a, **k):
            pass

        async def stop(self):
            pass

    class FakeApp:
        def __init__(self, token, bad):
            self.token, self.bad = token, bad
            self.handlers = {}
            self.bot_data = {}
            self.updater = FakeUpdater()
            self.bot = FakeBot()
            self.job_queue = FakeJobQueue()

        def add_handler(self, h, group=0):
            self.handlers.setdefault(group, []).append(h)

        def add_error_handler(self, h, *a, **k):
            self.error_handler = h

        async def initialize(self):
            if self.bad:
                raise InvalidToken("The token was rejected by the server")

        async def start(self):
            pass

        async def stop(self):
            pass

        async def shutdown(self):
            pass

    class FakeBuilder:
        def __init__(self):
            self._token = None

        def token(self, t):
            self._token = t
            return self

        def concurrent_updates(self, v):
            return self

        def request(self, r):
            return self

        def build(self):
            bad = (self._token == advanced.MAIN_BOT_TOKEN)
            app = FakeApp(self._token, bad)
            created.append(app)
            return app

    old_builder = advanced.ApplicationBuilder
    advanced.ApplicationBuilder = FakeBuilder
    task = None
    try:
        task = asyncio.get_event_loop_policy().new_event_loop().create_task(advanced._run_main())
        loop = task.get_loop()
        loop.run_until_complete(asyncio.sleep(0.2))
        assert not task.done(), "process must keep running (userbots alive)"
        assert BOT_ID in advanced.user_bot_applications, advanced.user_bot_applications
        # main bot app ban gaya tha aur uska initialize fail hua
        assert any(a.bad for a in created)
    finally:
        if task is not None:
            task.cancel()
            try:
                task.get_loop().run_until_complete(task)
            except (asyncio.CancelledError, Exception):
                pass
        advanced.ApplicationBuilder = old_builder
        advanced.user_bot_applications.clear()


TESTS = [v for k, v in sorted(globals().items()) if k.startswith("test_") and callable(v)]

if __name__ == "__main__":
    failed = 0
    for t in TESTS:
        try:
            t()
            print(f"PASS  {t.__name__}")
        except Exception as ex:  # noqa: BLE001
            failed += 1
            import traceback
            print(f"FAIL  {t.__name__}: {ex}")
            traceback.print_exc()
    print(f"\n{len(TESTS) - failed}/{len(TESTS)} passed")
    sys.exit(1 if failed else 0)
