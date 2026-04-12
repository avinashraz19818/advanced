import asyncio
import logging
import json
import time
import os
import re
import html as html_lib
import urllib.request
import urllib.parse
from html.parser import HTMLParser
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any, Tuple

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
    ContextTypes,
    filters,
)
from telegram.constants import ParseMode

# ================= CONFIG =================
MAIN_BOT_TOKEN = "7687421668:AAF3h5GTbqBUVZoXe6OExF5NV3dUwjy5WoU"
ADMIN_USER_ID = 8089603563
ADMIN_USERNAME = "@aviii566"
_ADMIN_IDS_RAW = os.getenv("ADMIN_USER_IDS", "").strip()
ADMIN_USER_IDS = {ADMIN_USER_ID}
if _ADMIN_IDS_RAW:
    for _x in _ADMIN_IDS_RAW.split(","):
        _x = _x.strip()
        if _x.isdigit():
            ADMIN_USER_IDS.add(int(_x))

# Support reply maps
SUPPORT_REPLY_MAP: Dict[int, Dict] = {}
USERBOT_SUPPORT_REPLY_MAP: Dict[str, Dict] = {}
SUPPORT_MAP_TTL = 86400

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

# ================= PREMIUM EMOJI MAPPING =================
# Map of emoji to premium ID
PREMIUM_EMOJI_MAP = {
    "❤️‍🔥": "5042225965518816316",
    "👀": "5039623284056917259",
    "⚡️": "5042334757040423886",
    "🔮": "5042302287087666158",
    "🎁": "5041975203853239332",
    "🆒": "5039686583284925280",
    "🥂": "5039774728898741491",
    "💀": "5042209657527993345",
    "🔔": "5042111805288089118",
    "⭐️": "5042176294222037888",
    "🥸": "5039645755325809616",
    "📍": "5039775669496579510",
    "🆗": "5042020176455795565",
    "🎄": "5039670898064360513",
    "☃️": "5039936198194234403",
    "🍪": "5040071919160788440",
    "🛷": "5042298743739647195",
    "💎": "5042050649248760772",
    "🗿": "5040045238823945201",
    "➡️": "5042341817966658405",
    "🪩": "5039670228049462451",
    "👁": "5039984684080038649",
    "🧝": "5039531487720899631",
    "🗣️": "5039643534827717738",
    "🛡": "5042328396193864923",
    "💙": "5039560388555834382",
    "🔗": "5042101437237036298",
    "🧠": "5040030395416969985",
    "👑": "5039727497143387500",
    "ℹ️": "5042306247047513767",
    "👍": "5039544445637231745",
    "💯": "5042297717242463211",
    "🐦‍⬛️": "5041866884778034602",
    "🔝": "5042102141611672423",
    "😥": "5040018571372004272",
    "‼️": "5042003580702164014",
    "🍾": "5042274086332400375",
    "🪅": "5039685805895844805",
    "🤭": "5039607036195636113",
    "🌕": "5039727604517570274",
    "▶️": "5039753786638205957",
    "☄️": "5041992177563993101",
    "🚗": "5039955091755369148",
    "⚠️": "5039665997506675838",
    "🦌": "5040030124834030529",
    "🍭": "5041902056265221148",
    "☕": "5042264186432783816",
    "🌲": "5039849968135832729",
    "🆕": "5041852827350074289",
    "🍨": "5039790268090418048",
    "🎤": "5039905162760553480",
    "🕷": "5039542049045480714",
    "✔️": "5039844895779455925",
    "🚫": "5039671744172917707",
    "🔽": "5042069744673358669",
    "🧤": "5041833220824368077",
    "🎀": "5039953030171067177",
    "©": "5039810295522919687",
    "🫐": "5041866717274309621",
    "🗑": "5039614900280754969",
    "🦄": "5041857809512137718",
    "🌙": "5041906342642582678",
    "💬": "5040036030414062506",
    "🎩": "5042329873662609701",
    "❌": "5040042498634810056",
    "💫": "5042200814190330758",
    "🟢": "5039928501612839813",
    "⚰️": "5039808607600771831",
    "🧦": "5039785612345869872",
    "💵": "5040025580758631490",
    "🧹": "5039751080808809534",
    "🔄": "5041837837914211014",
    "🔴": "5042042652019655612",
    "👋": "5040033797031070992",
    "🕯": "5041976440803820473",
    "🌟": "5039555114335994885",
    "🍲": "5041847450051019704",
    "📊": "5042290883949495533",
    "💰": "5039789890133296083",
    "🙂": "5039613856603702817",
    "🎉": "5039778134807806727",
    "🌛": "5042180842592404417",
    "🍃": "5042148333984941105",
    "📣": "5041888071851705019",
    "🌈": "5041955142060999726",
    "🍽": "5039834781131474002",
    "🍑": "5041988947748586361",
    "📌": "5039600026809009149",
    "💍": "5042151130008650810",
    "🫦": "5039661745489052379",
    "✨": "5040016479722931047",
    "⛸": "5042179807505286093",
    "👎": "5042067236412458007",
    "🤡": "5042127383134470945",
    "💌": "5041784013384057970",
    "🔍": "5039649904264217620",
    "🪦": "5042303713016808496",
    "⏸": "5042036407137207122",
    "🔼": "5041845079229072486",
    "💋": "5041921924783932671",
    "🥶": "5041892104825996365",
    "☠️": "5042167377869932162",
    "❓": "5042094496569885750",
    "🗓": "5039534051816375152",
    "🎙": "5041798349984891898",
    "📉": "5039759318556083411",
    "😂": "5042169306310247330",
    "💐": "5039778826297542004",
    "➕": "5039891861246838069",
    "💃": "5039960709572592619",
    "🛍": "5039730920232322000",
    "🌺": "5042005857034831000",
    "🌩": "5039980479307055915",
    "😱": "5042125029492392868",
    "🦋": "5042131347389285520",
    "🍄": "5041978437963613455",
    "🌐": "5042186567783809934",
    "🔥": "5039644681583985437",
    "😵‍💫": "5040032289497547835",
    "❗": "5040034664614462519",
    "👅": "5039927453640819586",
    "💡": "5042264341051605743",
    "✅": "5039793437776282663",
    "💝": "5042148626042717135",
    "🎏": "5039725422674183126",
    "🧶": "5042077862161548006",
    "🎆": "5039865760730579942",
    "💕": "5040071927750722576",
    "🪟": "5042223702071050992",
    "💩": "5039586205604250698",
    "🧩": "5039673964671009665",
    "❄️": "5039672577396573147",
    "🚩": "5042289131602838715",
    "🔖": "5039826874096681939",
    "🏡": "5042022053356504092",
    "🖥": "5039579582764680065",
    "📈": "5039808285478224750",
    "❤️": "5040072842578756396",
    "💖": "5039643719511311434",
    "🍰": "5042314635118642120",
    "🧸": "5042192219960771668",
    "👈": "5042156073516008537",
    "🦴": "5041858483822003471",
    "🧪": "5039551936060195684",
    "👼": "5042027727008301916",
    "👻": "5042097984083330584",
    "🥳": "5041869131045929880",
    "🐸": "5042295196096662988",
    "🐈‍⬛": "5039653765439816618",
    "🍬": "5042271178639541327",
    "🤫": "5042062739581699323",
    "🎈": "5039532814865794117",
    "🎶": "5039771357349413873",
    "🌹": "5039850573726221609",
}

# ================= DATABASE =================
MONGO_URI = "mongodb+srv://avinash:avinash12@cluster0.wnwd1fv.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
MONGO_DB_NAME = "advanced_bot"

from pymongo import MongoClient
from pymongo import ReturnDocument
import certifi

# ================= DATABASE CLASS =================
class Database:
    def __init__(self):
        self.use_mongo = True
        try:
            self.mongo_client = MongoClient(MONGO_URI, tlsCAFile=certifi.where())
            self.mongo = self.mongo_client[MONGO_DB_NAME]
            self.init_db()
            logging.info("✅ MongoDB connected successfully")
        except Exception as ex:
            logging.error(f"❌ MongoDB connection failed: {ex}")
            raise

    def _next_id(self, key: str) -> int:
        try:
            result = self.mongo.counters.find_one_and_update(
                {"_id": key},
                {"$inc": {"seq": 1}},
                upsert=True,
                return_document=ReturnDocument.AFTER,
            )
            return int(result.get("seq", 1))
        except Exception as ex:
            logging.error(f"Error in _next_id: {ex}")
            return 1

    def init_db(self):
        try:
            collections = self.mongo.list_collection_names()
            needed = [
                "users", "user_bots", "subscriptions", "user_bot_channels",
                "user_bot_messages", "join_requests", "reachable_users",
                "counters", "banned_users"
            ]
            for col in needed:
                if col not in collections:
                    self.mongo.create_collection(col)

            self.mongo.users.create_index("user_id", unique=True)
            self.mongo.user_bots.create_index("user_id", unique=True)
            self.mongo.user_bots.create_index("bot_token", unique=True)
            self.mongo.subscriptions.create_index("user_id", unique=True)
            self.mongo.user_bot_channels.create_index([("user_id", 1), ("channel_id", 1)], unique=True)
            self.mongo.user_bot_messages.create_index([("channel_id", 1), ("_id", 1)])
            self.mongo.user_bot_messages.create_index("media_group_id")
            self.mongo.join_requests.create_index(
                [("owner_user_id", 1), ("requester_id", 1), ("channel_id", 1)], unique=True)
            self.mongo.join_requests.create_index("status")
            self.mongo.join_requests.create_index("request_date")
            self.mongo.reachable_users.create_index(
                [("owner_user_id", 1), ("requester_id", 1)], unique=True)
            self.mongo.reachable_users.create_index("last_ok_at")
            self.mongo.banned_users.create_index([("owner_user_id", 1), ("banned_user_id", 1)], unique=True)
            logging.info("✅ MongoDB indexes created successfully")
        except Exception as ex:
            logging.error(f"❌ Error creating indexes: {ex}")

    # User CRUD
    def add_user(self, user_id, username, first_name, last_name):
        try:
            self.mongo.users.update_one(
                {"user_id": user_id},
                {"$set": {"username": username, "first_name": first_name, "last_name": last_name},
                 "$setOnInsert": {"created_at": datetime.utcnow().isoformat()}},
                upsert=True
            )
        except Exception as ex:
            logging.error(f"Error adding user {user_id}: {ex}")

    def get_all_users(self):
        users = []
        try:
            for user in self.mongo.users.find({}, {"_id": 0}):
                sub = self.mongo.subscriptions.find_one({"user_id": user["user_id"]}) or {}
                users.append((
                    user.get("user_id"), user.get("username"), user.get("first_name"),
                    user.get("last_name"), user.get("created_at"),
                    sub.get("subscription_type"), sub.get("expiry_date"), sub.get("max_channels")
                ))
        except Exception as ex:
            logging.error(f"Error getting all users: {ex}")
        return users

    # UserBot CRUD
    def add_user_bot(self, user_id, token, username):
        try:
            self.mongo.user_bots.update_one(
                {"user_id": user_id},
                {"$set": {"bot_token": token, "bot_username": username, "is_active": 1},
                 "$setOnInsert": {"created_at": datetime.utcnow().isoformat()}},
                upsert=True
            )
        except Exception as ex:
            logging.error(f"Error adding user bot for {user_id}: {ex}")

    def get_user_bot(self, user_id):
        try:
            bot = self.mongo.user_bots.find_one({"user_id": user_id})
            if bot:
                return (bot.get("user_id"), bot.get("bot_token"), bot.get("bot_username"),
                        bot.get("is_active", 0), bot.get("created_at"))
        except Exception as ex:
            logging.error(f"Error getting user bot for {user_id}: {ex}")
        return None

    def get_all_user_bots(self):
        bots = []
        try:
            for bot in self.mongo.user_bots.find({}):
                bots.append((bot.get("user_id"), bot.get("bot_token"),
                              bot.get("bot_username"), bot.get("is_active", 0)))
        except Exception as ex:
            logging.error(f"Error getting all user bots: {ex}")
        return bots

    def set_user_bot_active(self, user_id, active):
        try:
            self.mongo.user_bots.update_one(
                {"user_id": user_id},
                {"$set": {"is_active": 1 if active else 0}}
            )
        except Exception as ex:
            logging.error(f"Error setting bot active for {user_id}: {ex}")

    def remove_user_bot(self, user_id):
        try:
            self.mongo.user_bots.delete_many({"user_id": user_id})
        except Exception as ex:
            logging.error(f"Error removing user bot for {user_id}: {ex}")

    # Subscription CRUD
    def get_subscription(self, user_id):
        try:
            sub = self.mongo.subscriptions.find_one({"user_id": user_id})
            if sub:
                return (sub.get("user_id"), sub.get("subscription_type"),
                        sub.get("expiry_date"), sub.get("max_channels"), sub.get("created_at"))
        except Exception as ex:
            logging.error(f"Error getting subscription for {user_id}: {ex}")
        return None

    def get_all_subscriptions(self):
        """Get all subscriptions with user info."""
        results = []
        try:
            for sub in self.mongo.subscriptions.find({}):
                user = self.mongo.users.find_one({"user_id": sub.get("user_id")}) or {}
                bot = self.mongo.user_bots.find_one({"user_id": sub.get("user_id")}) or {}
                results.append({
                    "user_id": sub.get("user_id"),
                    "username": user.get("username", ""),
                    "first_name": user.get("first_name", ""),
                    "subscription_type": sub.get("subscription_type"),
                    "expiry_date": sub.get("expiry_date"),
                    "max_channels": sub.get("max_channels"),
                    "bot_username": bot.get("bot_username", ""),
                    "bot_active": bot.get("is_active", 0),
                })
        except Exception as ex:
            logging.error(f"Error get_all_subscriptions: {ex}")
        return results

    def add_subscription(self, user_id, sub_type, days):
        try:
            expiry = (datetime.now() + timedelta(days=days)).isoformat()
            max_channels = 1 if sub_type.lower() == "basic" else 5
            self.mongo.subscriptions.update_one(
                {"user_id": user_id},
                {"$set": {"subscription_type": sub_type, "expiry_date": expiry,
                          "max_channels": max_channels, "reminder_3d_sent": False,
                          "reminder_1d_sent": False},
                 "$setOnInsert": {"created_at": datetime.utcnow().isoformat()}},
                upsert=True
            )
            logging.info(f"✅ Added/Updated subscription for user {user_id}: {sub_type}, expires {expiry}")
        except Exception as ex:
            logging.error(f"Error adding subscription for {user_id}: {ex}")

    def update_subscription_expiry(self, user_id, new_expiry):
        try:
            self.mongo.subscriptions.update_one(
                {"user_id": user_id},
                {"$set": {"expiry_date": new_expiry.isoformat(),
                          "reminder_3d_sent": False,
                          "reminder_1d_sent": False}}
            )
        except Exception as ex:
            logging.error(f"Error updating subscription expiry: {ex}")

    def get_expiring_subscriptions(self, days_threshold: int):
        results = []
        try:
            now = datetime.now()
            cutoff = (now + timedelta(days=days_threshold)).isoformat()
            reminder_field = f"reminder_{days_threshold}d_sent"
            for sub in self.mongo.subscriptions.find({
                "expiry_date": {"$lte": cutoff, "$gte": now.isoformat()},
                reminder_field: {"$ne": True}
            }):
                results.append({
                    "user_id": sub.get("user_id"),
                    "subscription_type": sub.get("subscription_type"),
                    "expiry_date": sub.get("expiry_date"),
                })
        except Exception as ex:
            logging.error(f"Error get_expiring_subscriptions: {ex}")
        return results

    def get_expired_subscriptions(self):
        results = []
        try:
            now = datetime.now().isoformat()
            for sub in self.mongo.subscriptions.find({
                "expiry_date": {"$lt": now}
            }):
                results.append(sub.get("user_id"))
        except Exception as ex:
            logging.error(f"Error get_expired_subscriptions: {ex}")
        return results

    def mark_reminder_sent(self, user_id: int, days: int):
        try:
            self.mongo.subscriptions.update_one(
                {"user_id": user_id},
                {"$set": {f"reminder_{days}d_sent": True}}
            )
        except Exception as ex:
            logging.error(f"Error mark_reminder_sent: {ex}")

    # Channel CRUD
    def add_channel(self, user_id, channel_id, username, title):
        try:
            self.mongo.user_bot_channels.update_one(
                {"user_id": user_id, "channel_id": channel_id},
                {"$set": {"channel_username": username, "channel_title": title},
                 "$setOnInsert": {"created_at": datetime.utcnow().isoformat(), "auto_approve": 1}},
                upsert=True
            )
        except Exception as ex:
            logging.error(f"Error adding channel for {user_id}: {ex}")

    def get_user_channels(self, user_id):
        channels = []
        try:
            for ch in self.mongo.user_bot_channels.find({"user_id": user_id}).sort("channel_id", 1):
                channels.append((
                    ch.get("user_id"), ch.get("channel_id"), ch.get("channel_username"),
                    ch.get("channel_title"), ch.get("welcome_message"), ch.get("welcome_media_id"),
                    ch.get("welcome_media_type"), int(ch.get("auto_approve", 1))
                ))
        except Exception as ex:
            logging.error(f"Error getting user channels for {user_id}: {ex}")
        return channels

    def set_auto_approve(self, user_id, channel_id, val):
        try:
            self.mongo.user_bot_channels.update_one(
                {"user_id": user_id, "channel_id": channel_id},
                {"$set": {"auto_approve": 1 if val else 0}}
            )
        except Exception as ex:
            logging.error(f"Error setting auto approve: {ex}")

    def get_channel_owner_data(self, channel_id):
        try:
            ch = self.mongo.user_bot_channels.find_one({"channel_id": channel_id})
            if ch:
                return (ch.get("user_id"), ch.get("channel_id"), ch.get("channel_username"),
                        ch.get("channel_title"), ch.get("welcome_message"), ch.get("welcome_media_id"),
                        ch.get("welcome_media_type"), int(ch.get("auto_approve", 1)))
        except Exception as ex:
            logging.error(f"Error getting channel owner data for {channel_id}: {ex}")
        return None

    def clear_messages(self, user_id, channel_id):
        try:
            self.mongo.user_bot_messages.delete_many({"user_id": user_id, "channel_id": channel_id})
            self.mongo.user_bot_channels.update_one(
                {"user_id": user_id, "channel_id": channel_id},
                {"$set": {"welcome_message": None, "welcome_media_id": None, "welcome_media_type": None}}
            )
        except Exception as ex:
            logging.error(f"Error clearing messages: {ex}")

    def remove_channel(self, user_id, channel_id):
        try:
            self.mongo.user_bot_messages.delete_many({"user_id": user_id, "channel_id": channel_id})
            self.mongo.user_bot_channels.delete_many({"user_id": user_id, "channel_id": channel_id})
            self.mongo.join_requests.delete_many({"owner_user_id": user_id, "channel_id": channel_id})
            return True
        except Exception as ex:
            logging.error(f"Error removing channel: {ex}")
            return False

    # Message CRUD
    def add_message(self, user_id, channel_id, text, media_id, media_type, media_group_id=None):
        try:
            msg_id = self._next_id("user_bot_messages")
            # Store the raw text exactly as received
            self.mongo.user_bot_messages.insert_one({
                "_id": msg_id, "user_id": user_id, "channel_id": channel_id,
                "content_text": text, "media_id": media_id, "media_type": media_type,
                "media_group_id": media_group_id, "buttons_json": None,
                "created_at": datetime.utcnow().isoformat()
            })
            existing_msgs = list(self.mongo.user_bot_messages.find({"user_id": user_id, "channel_id": channel_id}))
            if len(existing_msgs) == 1:
                self.mongo.user_bot_channels.update_one(
                    {"user_id": user_id, "channel_id": channel_id},
                    {"$set": {"welcome_message": text,
                              "welcome_media_id": media_id,
                              "welcome_media_type": media_type}}
                )
            return msg_id
        except Exception as ex:
            logging.error(f"Error adding message: {ex}")
            return 0

    def update_message_text(self, msg_id, text):
        try:
            row = self.get_message_by_id(msg_id)
            if row:
                user_id = row[1]
                self.mongo.user_bot_messages.update_one(
                    {"_id": int(msg_id)}, {"$set": {"content_text": text}})
                channel_id = row[2]
                msgs = list(self.mongo.user_bot_messages.find({"channel_id": channel_id}).sort("_id", 1))
                if msgs and msgs[0]["_id"] == msg_id:
                    self.mongo.user_bot_channels.update_one(
                        {"user_id": user_id, "channel_id": channel_id},
                        {"$set": {"welcome_message": text}}
                    )
        except Exception as ex:
            logging.error(f"Error updating message text: {ex}")

    def update_message_media(self, msg_id, media_id, media_type, text=None):
        try:
            update = {"media_id": media_id, "media_type": media_type}
            row = self.get_message_by_id(msg_id)
            if text is not None and row:
                update["content_text"] = text
            self.mongo.user_bot_messages.update_one({"_id": int(msg_id)}, {"$set": update})
            if row:
                channel_id = row[2]
                msgs = list(self.mongo.user_bot_messages.find({"channel_id": channel_id}).sort("_id", 1))
                if msgs and msgs[0]["_id"] == msg_id:
                    self.mongo.user_bot_channels.update_one(
                        {"user_id": row[1], "channel_id": channel_id},
                        {"$set": {"welcome_media_id": media_id, "welcome_media_type": media_type}}
                    )
        except Exception as ex:
            logging.error(f"Error updating message media: {ex}")

    def delete_message(self, msg_id):
        try:
            row = self.get_message_by_id(msg_id)
            if row:
                user_id = row[1]
                channel_id = row[2]
                self.mongo.user_bot_messages.delete_one({"_id": int(msg_id)})
                remaining = list(self.mongo.user_bot_messages.find(
                    {"channel_id": channel_id}).sort("_id", 1))
                if remaining:
                    first = remaining[0]
                    self.mongo.user_bot_channels.update_one(
                        {"user_id": user_id, "channel_id": channel_id},
                        {"$set": {"welcome_message": first.get("content_text"),
                                  "welcome_media_id": first.get("media_id"),
                                  "welcome_media_type": first.get("media_type")}}
                    )
                else:
                    self.mongo.user_bot_channels.update_one(
                        {"user_id": user_id, "channel_id": channel_id},
                        {"$set": {"welcome_message": None,
                                  "welcome_media_id": None,
                                  "welcome_media_type": None}}
                    )
        except Exception as ex:
            logging.error(f"Error deleting message: {ex}")

    def delete_media_group_messages(self, media_group_id):
        try:
            msgs = list(self.mongo.user_bot_messages.find({"media_group_id": media_group_id}))
            if msgs:
                user_id = msgs[0].get("user_id")
                channel_id = msgs[0].get("channel_id")
                self.mongo.user_bot_messages.delete_many({"media_group_id": media_group_id})
                remaining = list(self.mongo.user_bot_messages.find(
                    {"channel_id": channel_id}).sort("_id", 1))
                if remaining:
                    first = remaining[0]
                    self.mongo.user_bot_channels.update_one(
                        {"user_id": user_id, "channel_id": channel_id},
                        {"$set": {"welcome_message": first.get("content_text"),
                                  "welcome_media_id": first.get("media_id"),
                                  "welcome_media_type": first.get("media_type")}}
                    )
                else:
                    self.mongo.user_bot_channels.update_one(
                        {"user_id": user_id, "channel_id": channel_id},
                        {"$set": {"welcome_message": None,
                                  "welcome_media_id": None,
                                  "welcome_media_type": None}}
                    )
        except Exception as ex:
            logging.error(f"Error deleting media group: {ex}")

    def get_message_count(self, channel_id):
        try:
            return self.mongo.user_bot_messages.count_documents({"channel_id": channel_id})
        except Exception as ex:
            logging.error(f"Error getting message count: {ex}")
            return 0

    def get_messages(self, channel_id):
        messages = []
        try:
            for msg in self.mongo.user_bot_messages.find({"channel_id": channel_id}).sort("_id", 1):
                messages.append((msg.get("_id"), msg.get("content_text"), msg.get("media_id"),
                                  msg.get("media_type"), msg.get("media_group_id"), msg.get("buttons_json")))
        except Exception as ex:
            logging.error(f"Error getting messages: {ex}")
        return messages

    def get_message_by_id(self, msg_id):
        try:
            msg = self.mongo.user_bot_messages.find_one({"_id": int(msg_id)})
            if msg:
                return (msg.get("_id"), msg.get("user_id"), msg.get("channel_id"),
                        msg.get("content_text"), msg.get("media_id"), msg.get("media_type"),
                        msg.get("media_group_id"), msg.get("buttons_json"))
        except Exception as ex:
            logging.error(f"Error getting message by ID: {ex}")
        return None

    def update_message_buttons(self, msg_id, buttons_json):
        try:
            self.mongo.user_bot_messages.update_one(
                {"_id": int(msg_id)}, {"$set": {"buttons_json": buttons_json}})
        except Exception as ex:
            logging.error(f"Error updating message buttons: {ex}")

    # Join Request CRUD
    def add_join_request(self, owner_user_id, requester_id, channel_id, status):
        try:
            existing = self.mongo.join_requests.find_one({
                "owner_user_id": owner_user_id,
                "requester_id": requester_id,
                "channel_id": channel_id
            })
            if existing:
                if existing.get("status") == "approved" and status == "pending":
                    return
                self.mongo.join_requests.update_one(
                    {"_id": existing["_id"]}, {"$set": {"status": status}})
            else:
                req_id = self._next_id("join_requests")
                self.mongo.join_requests.insert_one({
                    "_id": req_id, "owner_user_id": owner_user_id,
                    "requester_id": requester_id, "channel_id": channel_id,
                    "status": status, "request_date": datetime.utcnow().isoformat(),
                    "approved_date": None
                })
        except Exception as ex:
            logging.error(f"Error adding join request: {ex}")

    def get_pending_requests(self, owner_user_id):
        requests = []
        try:
            for req in self.mongo.join_requests.find(
                    {"owner_user_id": owner_user_id, "status": "pending"}):
                requests.append((req.get("_id"), req.get("requester_id"), req.get("channel_id")))
        except Exception as ex:
            logging.error(f"Error getting pending requests: {ex}")
        return requests

    def mark_request_status(self, request_id, status):
        try:
            update = {"status": status}
            if status == "approved":
                update["approved_date"] = datetime.utcnow().isoformat()
            self.mongo.join_requests.update_one(
                {"_id": int(request_id)}, {"$set": update})
        except Exception as ex:
            logging.error(f"Error marking request status: {ex}")

    def get_pending_count(self, owner_user_id):
        try:
            return self.mongo.join_requests.count_documents(
                {"owner_user_id": owner_user_id, "status": "pending"})
        except Exception as ex:
            logging.error(f"Error getting pending count: {ex}")
            return 0

    def mark_reachable(self, owner_user_id, requester_id):
        try:
            self.mongo.reachable_users.update_one(
                {"owner_user_id": owner_user_id, "requester_id": requester_id},
                {"$set": {"last_ok_at": datetime.utcnow().isoformat()}},
                upsert=True
            )
        except Exception as ex:
            logging.error(f"Error marking reachable: {ex}")

    def mark_unreachable(self, owner_user_id, requester_id):
        try:
            self.mongo.reachable_users.delete_one(
                {"owner_user_id": owner_user_id, "requester_id": requester_id})
        except Exception as ex:
            logging.error(f"Error marking unreachable: {ex}")

    def get_requesters_for_owner(self, owner_user_id):
        try:
            reachable = list(self.mongo.reachable_users.find(
                {"owner_user_id": owner_user_id}).sort("last_ok_at", -1))
            if reachable:
                return [r.get("requester_id") for r in reachable if r.get("requester_id")]
            requests = self.mongo.join_requests.distinct(
                "requester_id", {"owner_user_id": owner_user_id, "status": "approved"})
            return [r for r in requests if r]
        except Exception as ex:
            logging.error(f"Error getting requesters for owner: {ex}")
            return []

    def get_total_requesters_count(self, owner_user_id):
        try:
            return len(self.mongo.join_requests.distinct(
                "requester_id", {"owner_user_id": owner_user_id}))
        except Exception as ex:
            logging.error(f"Error getting total requesters count: {ex}")
            return 0

    def get_reachable_requesters_count(self, owner_user_id):
        try:
            return len(self.mongo.reachable_users.distinct(
                "requester_id", {"owner_user_id": owner_user_id}))
        except Exception as ex:
            logging.error(f"Error getting reachable requesters count: {ex}")
            return 0

    def get_userbot_user_counts(self):
        rows = []
        try:
            for bot in self.mongo.user_bots.find({}, {"_id": 0}):
                users = len(self.mongo.join_requests.distinct(
                    "requester_id", {"owner_user_id": bot.get("user_id")}))
                rows.append((bot.get("user_id"), bot.get("bot_username"), users))
        except Exception as ex:
            logging.error(f"Error getting userbot user counts: {ex}")
        return rows

    # Ban/Unban
    def ban_user(self, owner_user_id: int, banned_user_id: int):
        try:
            self.mongo.banned_users.update_one(
                {"owner_user_id": owner_user_id, "banned_user_id": banned_user_id},
                {"$set": {"banned_at": datetime.utcnow().isoformat()}},
                upsert=True
            )
        except Exception as ex:
            logging.error(f"Error banning user: {ex}")

    def unban_user(self, owner_user_id: int, banned_user_id: int):
        try:
            self.mongo.banned_users.delete_one(
                {"owner_user_id": owner_user_id, "banned_user_id": banned_user_id})
        except Exception as ex:
            logging.error(f"Error unbanning user: {ex}")

    def is_banned(self, owner_user_id: int, user_id: int) -> bool:
        try:
            return bool(self.mongo.banned_users.find_one(
                {"owner_user_id": owner_user_id, "banned_user_id": user_id}))
        except Exception as ex:
            logging.error(f"Error checking ban: {ex}")
            return False


# ================= GLOBALS =================
db = Database()
user_bot_applications: Dict[int, Application] = {}


# ================= PREMIUM EMOJI PROCESSING =================
def extract_custom_emoji_entities(text: str, entities: Optional[List[MessageEntity]] = None) -> Dict[str, str]:
    """
    Extract custom emoji entities and return a mapping of emoji character to premium ID.
    This preserves the user's original premium emojis.
    """
    if not entities:
        return {}
    
    custom_emoji_map = {}
    for entity in entities:
        if entity.type == "custom_emoji":
            emoji_char = text[entity.offset:entity.offset + entity.length]
            custom_emoji_map[emoji_char] = entity.custom_emoji_id
    
    return custom_emoji_map


def convert_to_premium_format(text: str, custom_emojis: Dict[str, str] = None) -> str:
    """
    Convert emojis to Telegram premium format for sending to end users.
    Preserves user's custom premium emojis and converts standard emojis.
    """
    if not text:
        return text
    
    result = text
    
    # First, handle user's custom premium emojis (preserve them as is with their IDs)
    if custom_emojis:
        for emoji_char, premium_id in custom_emojis.items():
            # Wrap the emoji with tg-emoji tag using its original ID
            premium_tag = f'<tg-emoji emoji-id="{premium_id}">{emoji_char}</tg-emoji>'
            result = result.replace(emoji_char, premium_tag)
    
    # Then, convert standard emojis to premium format
    # Sort by length descending to handle multi-character emojis correctly
    sorted_emojis = sorted(PREMIUM_EMOJI_MAP.items(), key=lambda x: len(x[0]), reverse=True)
    
    for emoji_char, premium_id in sorted_emojis:
        # Skip if this emoji was already handled as custom
        if custom_emojis and emoji_char in custom_emojis:
            continue
        premium_tag = f'<tg-emoji emoji-id="{premium_id}">{emoji_char}</tg-emoji>'
        result = result.replace(emoji_char, premium_tag)
    
    return result


def process_for_display(text: str, entities: Optional[List[MessageEntity]] = None) -> str:
    """
    Process text for display in bot control panel.
    Shows emojis as normal (without conversion).
    """
    if not text:
        return ""
    
    # Just return the raw text - no conversion needed for control panel
    return text


def process_for_sending(text: str, entities: Optional[List[MessageEntity]] = None) -> str:
    """
    Process text for sending to end users.
    Converts emojis to premium format.
    """
    if not text:
        return ""
    
    # Extract custom emoji IDs from entities (if provided)
    custom_emojis = extract_custom_emoji_entities(text, entities) if entities else {}
    
    # Convert to premium format
    return convert_to_premium_format(text, custom_emojis)


# ================= HELPER FUNCTIONS =================
def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_USER_IDS


def get_managed_user_id(context: ContextTypes.DEFAULT_TYPE, requester_uid: int) -> int:
    owner_uid = context.application.bot_data.get("owner_id")
    if requester_uid in ADMIN_USER_IDS and owner_uid:
        return owner_uid
    return requester_uid


def parse_buttons_text(text: Optional[str]):
    if not text:
        return None
    buttons = []
    current_row = []
    for line in text.splitlines():
        sep = '|' if '|' in line else ('-' if '-' in line else None)
        if sep:
            parts = line.split(sep, 1)
            if len(parts) == 2:
                label, url = parts[0].strip(), parts[1].strip()
                if label and url:
                    current_row.append(InlineKeyboardButton(label, url=url))
                    if len(current_row) == 2:
                        buttons.append(current_row)
                        current_row = []
    if current_row:
        buttons.append(current_row)
    return buttons if buttons else None


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
            for btn in row:
                if btn.get('url'):
                    row_btns.append(InlineKeyboardButton(btn['text'], url=btn['url']))
                elif btn.get('cb'):
                    row_btns.append(InlineKeyboardButton(btn['text'], callback_data=btn['cb']))
                elif btn.get('callback_data'):
                    row_btns.append(InlineKeyboardButton(btn['text'], callback_data=btn['callback_data']))
            if row_btns:
                rows.append(row_btns)
        return InlineKeyboardMarkup(rows) if rows else None
    except Exception:
        return None


def buttons_json_from_text(text: str):
    rows = parse_buttons_text(text)
    if not rows:
        return None
    json_rows = []
    for row in rows:
        json_rows.append([{"text": btn.text, "url": btn.url} for btn in row])
    return json.dumps(json_rows)


def add_callback_button_to_json(buttons_json: Optional[str], text: str, cb: str,
                                 url: Optional[str] = None) -> str:
    data = []
    if buttons_json:
        try:
            data = json.loads(buttons_json) or []
        except Exception:
            data = []
    for row in data:
        for btn in row:
            if (btn.get('cb') == cb or btn.get('callback_data') == cb
                    or (url and btn.get('url') == url)):
                return json.dumps(data)
    if url:
        data.append([{"text": text, "url": url}])
    else:
        data.append([{"text": text, "cb": cb}])
    return json.dumps(data)


async def send_ephemeral_reply(msg, text: str, seconds: int = 2):
    try:
        sent = await msg.reply_text(text, parse_mode=ParseMode.HTML)
        await asyncio.sleep(seconds)
        try:
            await sent.delete()
        except Exception:
            pass
    except Exception:
        pass


def extract_media_from_msg(msg):
    text = msg.text or msg.caption or ""
    entities = msg.entities or msg.caption_entities or []
    media_id = None
    media_type = None
    if msg.photo:
        media_id = msg.photo[-1].file_id
        media_type = "photo"
    elif msg.video:
        media_id = msg.video.file_id
        media_type = "video"
    elif msg.document:
        media_id = msg.document.file_id
        media_type = "document"
    elif msg.animation:
        media_id = msg.animation.file_id
        media_type = "animation"
    elif msg.audio:
        media_id = msg.audio.file_id
        media_type = "audio"
    elif msg.voice:
        media_id = msg.voice.file_id
        media_type = "voice"
    elif msg.sticker:
        media_id = msg.sticker.file_id
        media_type = "sticker"
    return media_id, media_type, text, entities


def get_entities_from_message(msg):
    if msg.text:
        return msg.entities
    elif msg.caption:
        return msg.caption_entities
    return []


async def send_media(bot_or_context, chat_id: int, media_id, media_type: str,
                     text: str = "", markup=None, entities=None):
    bot = bot_or_context if isinstance(bot_or_context, Bot) else bot_or_context.bot
    kwargs = {}
    if markup:
        kwargs["reply_markup"] = markup
    try:
        # Process text for sending - convert emojis to premium format
        display_text = process_for_sending(text, entities) if text else None
        
        if media_type == "photo":
            await bot.send_photo(chat_id, media_id, caption=display_text, parse_mode=ParseMode.HTML, **kwargs)
        elif media_type == "video":
            await bot.send_video(chat_id, media_id, caption=display_text, parse_mode=ParseMode.HTML, **kwargs)
        elif media_type == "document":
            await bot.send_document(chat_id, media_id, caption=display_text, parse_mode=ParseMode.HTML, **kwargs)
        elif media_type == "animation":
            await bot.send_animation(chat_id, media_id, caption=display_text, parse_mode=ParseMode.HTML, **kwargs)
        elif media_type == "audio":
            await bot.send_audio(chat_id, media_id, caption=display_text, parse_mode=ParseMode.HTML, **kwargs)
        elif media_type == "voice":
            await bot.send_voice(chat_id, media_id, caption=display_text, parse_mode=ParseMode.HTML, **kwargs)
        elif media_type == "sticker":
            await bot.send_sticker(chat_id, media_id)
            if display_text:
                await bot.send_message(chat_id, display_text, parse_mode=ParseMode.HTML, **kwargs)
        else:
            await bot.send_message(chat_id, display_text or ".", parse_mode=ParseMode.HTML, **kwargs)
    except Exception as ex:
        logging.error(f"send_media error: {ex}")
        # Fallback without HTML parsing
        try:
            if media_type == "photo":
                await bot.send_photo(chat_id, media_id, caption=text or None, **kwargs)
            elif media_type == "video":
                await bot.send_video(chat_id, media_id, caption=text or None, **kwargs)
            elif media_type == "document":
                await bot.send_document(chat_id, media_id, caption=text or None, **kwargs)
            elif media_type == "animation":
                await bot.send_animation(chat_id, media_id, caption=text or None, **kwargs)
            elif media_type == "audio":
                await bot.send_audio(chat_id, media_id, caption=text or None, **kwargs)
            elif media_type == "voice":
                await bot.send_voice(chat_id, media_id, caption=text or None, **kwargs)
            elif media_type == "sticker":
                await bot.send_sticker(chat_id, media_id)
                if text:
                    await bot.send_message(chat_id, text, **kwargs)
            else:
                await bot.send_message(chat_id, text or ".", **kwargs)
        except Exception as fallback_ex:
            logging.error(f"send_media fallback also failed: {fallback_ex}")
            raise


# ================= KEYBOARDS =================
def main_menu_kb(user_id: int):
    kb = [
        [InlineKeyboardButton("🤖 Setup Your Bot", callback_data="setup_bot")],
        [InlineKeyboardButton("📊 My Subscription", callback_data="my_subscription")],
        [InlineKeyboardButton("📞 Contact Admin",
                              url=f"https://t.me/{ADMIN_USERNAME.lstrip('@')}")],
    ]
    if user_id in ADMIN_USER_IDS:
        kb.append([InlineKeyboardButton("👑 Admin Panel", callback_data="admin_panel")])
    return InlineKeyboardMarkup(kb)


def subscription_kb():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("💰 Basic - ₹2599/mo", callback_data="sub_basic")],
        [InlineKeyboardButton("⚡ Pro - ₹3999/mo", callback_data="sub_pro")],
        [InlineKeyboardButton("🔙 Back", callback_data="main_menu")],
    ])


def admin_kb():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📇 All Users", callback_data="admin_all_users"),
         InlineKeyboardButton("🤖 Manage UserBots", callback_data="admin_userbots")],
        [InlineKeyboardButton("➕ Add UserBot", callback_data="admin_add_userbot"),
         InlineKeyboardButton("➕ Add Subscription", callback_data="admin_add_sub")],
        [InlineKeyboardButton("📋 Subscription List", callback_data="admin_sub_list")],
        [InlineKeyboardButton("🔄 Check Expiry", callback_data="admin_check_expiry"),
         InlineKeyboardButton("📊 Stats", callback_data="admin_stats")],
        [InlineKeyboardButton("🚀 Start All Userbots", callback_data="admin_start_all"),
         InlineKeyboardButton("🛑 Stop All Userbots", callback_data="admin_stop_all")],
        [InlineKeyboardButton("✈️ Broadcast", callback_data="admin_broadcast"),
         InlineKeyboardButton("🔔 Send Reminders", callback_data="admin_send_reminders")],
        [InlineKeyboardButton("🔙 Main Menu", callback_data="main_menu")],
    ])


def userbot_kb(user_id: int):
    sub = db.get_subscription(user_id)
    lines = [
        [InlineKeyboardButton("✈️ Add Channel", callback_data="ub_add_channel"),
         InlineKeyboardButton("📝 Set Message(s)", callback_data="ub_set_message")],
        [InlineKeyboardButton("👀 Preview/Edit", callback_data="ub_manage_messages"),
         InlineKeyboardButton("🗑️ Delete Msgs", callback_data="ub_delete_messages")],
        [InlineKeyboardButton("❌ Remove Channel", callback_data="ub_remove_channel"),
         InlineKeyboardButton("⚙️ Auto-Approve", callback_data="ub_toggle_auto")],
        [InlineKeyboardButton("📋 My Channels", callback_data="ub_list_channels"),
         InlineKeyboardButton("📈 Bot Stats", callback_data="ub_stats")],
        [InlineKeyboardButton("📊 Pending", callback_data="ub_pending_requests"),
         InlineKeyboardButton("✅ Accept All", callback_data="ub_accept_all")],
        [InlineKeyboardButton("✈️ Broadcast to Users", callback_data="ub_broadcast")],
    ]
    if sub:
        try:
            expiry = datetime.fromisoformat(sub[2])
            days_left = (expiry - datetime.now()).days
            lines.append([InlineKeyboardButton(
                f"📅 {sub[1]} - {days_left} days", callback_data="ub_subscription")])
        except Exception:
            pass
    lines.append([
        InlineKeyboardButton("📞 Contact Admin",
                             url=f"https://t.me/{ADMIN_USERNAME.lstrip('@')}"),
        InlineKeyboardButton("🔙 Back to Main", callback_data="main_menu")
    ])
    return InlineKeyboardMarkup(lines)


# ================= SUBSCRIPTION EXPIRY CHECK JOB =================
async def check_expired_subscriptions_job(context: ContextTypes.DEFAULT_TYPE):
    expired_users = db.get_expired_subscriptions()
    for user_id in expired_users:
        bot_data = db.get_user_bot(user_id)
        if bot_data and bot_data[3] == 1:
            if user_id in user_bot_applications:
                try:
                    app = user_bot_applications[user_id]
                    await app.updater.stop()
                    await app.stop()
                    await app.shutdown()
                    logging.info(f"🛑 Stopped user bot for expired subscription: {user_id}")
                except Exception as ex:
                    logging.error(f"Error stopping expired bot {user_id}: {ex}")
                user_bot_applications.pop(user_id, None)
            db.set_user_bot_active(user_id, False)
            try:
                await context.bot.send_message(
                    user_id,
                    "‼️ Your subscription has expired!\n\nYour bot has been paused.\n📞 Contact @aviii566 to renew and reactivate.",
                    parse_mode=ParseMode.HTML,
                    reply_markup=InlineKeyboardMarkup([[
                        InlineKeyboardButton("🔄 Renew Now", url=f"https://t.me/{ADMIN_USERNAME.lstrip('@')}")
                    ]])
                )
            except Exception as ex:
                logging.error(f"Failed to notify user {user_id} about expiry: {ex}")


async def subscription_reminder_job(context: ContextTypes.DEFAULT_TYPE):
    _cleanup_support_maps()
    for days_threshold in [3, 1]:
        expiring = db.get_expiring_subscriptions(days_threshold)
        for sub in expiring:
            user_id = sub["user_id"]
            sub_type = sub["subscription_type"]
            try:
                expiry_dt = datetime.fromisoformat(sub["expiry_date"])
                days_left = (expiry_dt - datetime.now()).days
                if days_threshold == 3:
                    msg = (
                        f"🔔 Subscription Expiry Reminder\n\n"
                        f"⭐️ Plan: {sub_type}\n"
                        f"‼️ Your subscription expires in {days_left} days "
                        f"({expiry_dt.strftime('%d %b %Y')}).\n\n"
                        f"🔥 Renew now to avoid interruption!\n"
                        f"📞 Contact {ADMIN_USERNAME} to renew."
                    )
                else:
                    msg = (
                        f"‼️ Last Day Reminder!\n\n"
                        f"⭐️ Plan: {sub_type}\n"
                        f"‼️ Your subscription expires TOMORROW "
                        f"({expiry_dt.strftime('%d %b %Y')})!\n\n"
                        f"🚀 Renew immediately to keep your bot running!\n"
                        f"📞 Contact {ADMIN_USERNAME} NOW."
                    )
                try:
                    await context.bot.send_message(
                        user_id, msg, parse_mode=ParseMode.HTML,
                        reply_markup=InlineKeyboardMarkup([[
                            InlineKeyboardButton(
                                "🔄 Renew Subscription",
                                url=f"https://t.me/{ADMIN_USERNAME.lstrip('@')}"
                            )
                        ]])
                    )
                    db.mark_reminder_sent(user_id, days_threshold)
                    logging.info(f"✅ Reminder sent to user {user_id} ({days_threshold}d)")
                except Exception as send_err:
                    logging.error(f"Reminder send failed for {user_id}: {send_err}")
            except Exception as ex:
                logging.error(f"Reminder job error for {user_id}: {ex}")


async def _send_messages_with_media_groups(chat_id: int, msgs: List[tuple],
                                            context: ContextTypes.DEFAULT_TYPE,
                                            attach_start_button: bool = True):
    if msgs and attach_start_button:
        try:
            bot_username = None
            owner_uid = context.application.bot_data.get("owner_id")
            if owner_uid:
                row = db.get_user_bot(owner_uid)
                if row:
                    bot_username = row[2] if len(row) > 2 else None
            if bot_username:
                deep_link = f"https://t.me/{bot_username}?start=from_start_now"
                msgs = list(msgs)
                last = msgs[-1]
                updated = add_callback_button_to_json(
                    last[5] if len(last) > 5 else None,
                    "Start now 💰💰", "start_now", url=deep_link
                )
                msgs[-1] = (last[0], last[1], last[2], last[3], last[4], updated)
        except Exception as ex:
            logging.error(f"attach start button failed: {ex}")

    i = 0
    while i < len(msgs):
        mid, text, media_id, media_type, media_group_id, buttons_json = msgs[i]

        if media_group_id:
            group_items = []
            group_buttons_json = None
            group_caption_text = None
            j = i
            while j < len(msgs) and msgs[j][4] == media_group_id:
                _, g_text, g_mid, g_mtype, _, g_btnj = msgs[j]
                if not group_buttons_json and g_btnj:
                    group_buttons_json = g_btnj
                if not group_caption_text and g_text:
                    group_caption_text = g_text
                if g_mid and g_mtype in ("photo", "video", "document", "audio"):
                    if g_mtype == "photo":
                        group_items.append(InputMediaPhoto(
                            media=g_mid, caption=g_text or None, parse_mode=ParseMode.HTML))
                    elif g_mtype == "video":
                        group_items.append(InputMediaVideo(
                            media=g_mid, caption=g_text or None, parse_mode=ParseMode.HTML))
                    elif g_mtype == "document":
                        group_items.append(InputMediaDocument(
                            media=g_mid, caption=g_text or None, parse_mode=ParseMode.HTML))
                    elif g_mtype == "audio":
                        group_items.append(InputMediaAudio(
                            media=g_mid, caption=g_text or None, parse_mode=ParseMode.HTML))
                else:
                    markup = buttons_to_markup(g_btnj)
                    await send_media(context, chat_id, g_mid, g_mtype or "text", g_text or "", markup)
                j += 1
            if group_items:
                await context.bot.send_media_group(chat_id=chat_id, media=group_items)
                group_markup = buttons_to_markup(group_buttons_json)
                if group_markup:
                    await context.bot.send_message(
                        chat_id, group_caption_text or "Open links:",
                        parse_mode=ParseMode.HTML,
                        reply_markup=group_markup)
            i = j
            continue

        markup = buttons_to_markup(buttons_json)
        await send_media(context, chat_id, media_id, media_type or "text", text or "", markup)
        i += 1


async def send_saved_welcome(owner_uid: int, chat_id: int, context: ContextTypes.DEFAULT_TYPE):
    try:
        channels = db.get_user_channels(owner_uid) or []
        if not channels:
            await context.bot.send_message(chat_id, "Hello! Contact the channel owner for details.", parse_mode=ParseMode.HTML)
            return
        channel_id = channels[0][1]
        msgs = db.get_messages(channel_id) or []
        if not msgs:
            ch_row = db.get_channel_owner_data(channel_id)
            welcome_text = ch_row[4] if ch_row else None
            if welcome_text:
                await context.bot.send_message(
                    chat_id, welcome_text, parse_mode=ParseMode.HTML)
            else:
                await context.bot.send_message(chat_id, "Hello! Contact the channel owner for details.", parse_mode=ParseMode.HTML)
            return
        await _send_messages_with_media_groups(chat_id, msgs, context, attach_start_button=True)
    except Exception as ex:
        logging.error(f"send_saved_welcome error: {ex}")
        try:
            await context.bot.send_message(chat_id, "Hello! Contact the channel owner for details.", parse_mode=ParseMode.HTML)
        except Exception:
            pass


def _runtime_store(context: ContextTypes.DEFAULT_TYPE, uid: int) -> Dict[str, Any]:
    store = context.application.bot_data.setdefault("runtime_store", {})
    if uid not in store:
        store[uid] = {}
    return store[uid]


async def sync_pending_join_requests_for_channel(owner_uid: int, channel_id: int, bot: Bot):
    try:
        url = f"https://api.telegram.org/bot{bot.token}/getChatJoinRequests"
        payload = urllib.parse.urlencode({"chat_id": channel_id, "limit": 200}).encode("utf-8")
        req = urllib.request.Request(url, data=payload, method="POST")
        with urllib.request.urlopen(req, timeout=20) as resp:
            data = json.loads(resp.read().decode("utf-8"))
    except Exception as ex:
        logging.info(f"Pending sync API unavailable for channel {channel_id}: {ex}")
        return
    if not data.get("ok"):
        return
    synced = 0
    for jr in data.get("result", []) or []:
        try:
            requester_id = jr.get("user_chat_id") or (jr.get("from", {}) or {}).get("id")
            if requester_id:
                db.add_join_request(owner_uid, int(requester_id), channel_id, 'pending')
                synced += 1
        except Exception as ex:
            logging.error(f"pending sync item failed {channel_id}: {ex}")
    if synced:
        logging.info(f"Synced {synced} pending requests for owner={owner_uid} channel={channel_id}")


# ================= USER BOT HANDLERS =================
async def user_bot_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not user:
        return
    owner_uid = context.application.bot_data.get("owner_id")

    if context.args and any("from_start_now" in a for a in context.args):
        try:
            if owner_uid:
                db.mark_reachable(owner_uid, user.id)
            await send_live_chat_message(owner_uid, user.id, context)
            return
        except Exception as ex:
            logging.error(f"deep link live chat failed: {ex}")

    if owner_uid and user.id not in (owner_uid, ADMIN_USER_ID):
        if db.is_banned(owner_uid, user.id):
            await context.bot.send_message(user.id, "⛔ You are not allowed to use this bot.", parse_mode=ParseMode.HTML)
            return
        await send_saved_welcome(owner_uid, user.id, context)
        return

    target_uid = owner_uid if user.id == ADMIN_USER_ID and owner_uid else user.id
    await context.bot.send_message(
        user.id,
        "🤖 Welcome to your Auto Join Request Bot!\n\n🔽 Use the buttons below to configure your bot.",
        parse_mode=ParseMode.HTML,
        reply_markup=userbot_kb(target_uid)
    )


async def send_live_chat_message(owner_uid: int, chat_id: int, context: ContextTypes.DEFAULT_TYPE):
    channels = db.get_user_channels(owner_uid) if owner_uid else []
    if not channels:
        await context.bot.send_message(chat_id, "⚠️ No channel configured yet.", parse_mode=ParseMode.HTML)
        return
    channel_id = channels[0][1]
    msgs = db.get_messages(channel_id) or []
    if not msgs:
        await context.bot.send_message(chat_id, "⚠️ No message saved yet.", parse_mode=ParseMode.HTML)
        return
    last = msgs[-1]
    text = last[1] if len(last) > 1 else ""
    media_id = last[2] if len(last) > 2 else None
    media_type = last[3] if len(last) > 3 else None

    support_markup = InlineKeyboardMarkup([
        [InlineKeyboardButton("Live Chat Support 💬", callback_data="live_chat_support")]
    ])
    await send_media(context, chat_id, media_id, media_type or "text", text or "", support_markup)


async def handle_public_userbot_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q or not q.from_user:
        return
    uid = q.from_user.id
    owner_uid = context.application.bot_data.get("owner_id")
    data = q.data
    chat_id = q.message.chat_id if q.message else uid

    if data == "start_now":
        try:
            await q.answer()
        except Exception:
            pass
        if owner_uid:
            db.mark_reachable(owner_uid, uid)
        try:
            await send_live_chat_message(owner_uid, chat_id, context)
        except Exception as ex:
            logging.error(f"start_now send failed: {ex}")
            try:
                await q.answer("Please /start the bot first, then click Start now.", show_alert=True)
            except Exception:
                pass
        return

    if data == "live_chat_support":
        try:
            await q.answer("Live chat support is enabled. Please send your message here.", show_alert=True)
        except Exception:
            pass
        return


async def user_bot_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q or not q.from_user:
        return
    try:
        await q.answer()
    except Exception:
        pass
    uid = q.from_user.id
    owner_uid = context.application.bot_data.get("owner_id")

    if owner_uid and uid not in (owner_uid, ADMIN_USER_ID):
        await q.edit_message_text("Not authorized.", parse_mode=ParseMode.HTML)
        return

    managed_uid = get_managed_user_id(context, uid)
    ud = _runtime_store(context, uid)
    data = q.data

    try:
        if data == "ub_add_channel":
            await q.edit_message_text(
                "✈️ Add Channel\n\n🔽 Add this bot as admin in your channel, then forward any message from that channel here.",
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="main_menu")]]),
            )
            context.user_data["adding_channel"] = True
            ud["adding_channel"] = True

        elif data == "ub_set_message":
            await q.edit_message_text(
                "✨ Set Welcome Messages\n\nSend one or multiple messages (text/media + caption).\nType <code>done</code> when finished.\n\n🔔 Optional inline buttons: send lines\nText|https://link\n\n🔥 Tip: Use <b>HTML formatting</b> for bold, italic, links, etc.\n\n💎 Premium emojis are supported! Use any emoji and it will be preserved exactly as you send it.",
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="main_menu")]]),
            )
            context.user_data["setting_message"] = True
            context.user_data["messages"] = []
            ud["setting_message"] = True
            ud["messages"] = []

        elif data == "ub_delete_messages":
            channels = db.get_user_channels(managed_uid)
            if not channels:
                await q.edit_message_text("‼️ No channels yet.", parse_mode=ParseMode.HTML, reply_markup=userbot_kb(managed_uid))
                return
            kb = [[InlineKeyboardButton(f"🗑️ {c[3][:18]}", callback_data=f"delmsg_{c[1]}")]
                  for c in channels]
            kb.append([InlineKeyboardButton("🔙 Back", callback_data="main_menu")])
            await q.edit_message_text("‼️ Choose channel to delete saved welcome messages.",
                                      parse_mode=ParseMode.HTML,
                                      reply_markup=InlineKeyboardMarkup(kb))

        elif data == "ub_list_channels":
            await show_channels(q, managed_uid)

        elif data == "ub_pending_requests":
            await show_pending(q, managed_uid, context)

        elif data == "ub_subscription":
            await show_sub_userbot(q, managed_uid)

        elif data == "ub_toggle_auto":
            await show_toggle(q, managed_uid)

        elif data == "ub_accept_all":
            await accept_all(q, managed_uid, context)

        elif data == "ub_stats":
            channels = db.get_user_channels(managed_uid) or []
            pending = db.get_pending_count(managed_uid)
            total_users = db.get_total_requesters_count(managed_uid)
            reachable_users = db.get_reachable_requesters_count(managed_uid)
            stats_text = (
                f"📊 Bot Stats\n\n"
                f"✈️ Channels: {len(channels)}\n"
                f"📇 Total Users: {total_users}\n"
                f"🔘 Reachable Users: {reachable_users}\n"
                f"🔔 Pending Requests: {pending}"
            )
            await q.edit_message_text(stats_text, parse_mode=ParseMode.HTML, reply_markup=userbot_kb(managed_uid))

        elif data == "ub_manage_messages":
            channels = db.get_user_channels(managed_uid)
            if not channels:
                await q.edit_message_text("‼️ No channels yet.", parse_mode=ParseMode.HTML, reply_markup=userbot_kb(managed_uid))
                return
            kb = [[InlineKeyboardButton(
                f"🧩 {c[3][:22]} ({db.get_message_count(c[1])})",
                callback_data=f"ubmm_{c[1]}")] for c in channels]
            kb.append([InlineKeyboardButton("🔙 Back", callback_data="main_menu")])
            await q.edit_message_text("✨ Select channel for message preview/edit:",
                                      parse_mode=ParseMode.HTML,
                                      reply_markup=InlineKeyboardMarkup(kb))

        elif data == "ub_remove_channel":
            await prompt_remove_channel(q, managed_uid)

        elif data == "ub_broadcast":
            await q.edit_message_text(
                "✈️ Broadcast to Your Users\n\nSend text/media to broadcast to your users.\n🔔 Buttons format: Text|https://link\n\n🔥 Tip: Use <b>HTML formatting</b> for rich text.\n\n💎 Premium emojis will be preserved and converted automatically when sending!",
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="main_menu")]]),
            )
            context.user_data["broadcast_stage"] = "await_message"
            context.user_data.pop("broadcast_draft", None)

        elif data.startswith("toggleauto_"):
            await handle_toggle_callback(update, context)

        elif data.startswith("delmsg_"):
            cid = int(data.split("_")[1])
            db.clear_messages(managed_uid, cid)
            await q.edit_message_text("✅ Messages cleared.", parse_mode=ParseMode.HTML, reply_markup=userbot_kb(managed_uid))

        elif data.startswith("ubmm_"):
            cid = int(data.split("_")[1])
            msgs = db.get_messages(cid) or []
            if not msgs:
                await q.edit_message_text("‼️ No messages in this channel.",
                                          parse_mode=ParseMode.HTML,
                                          reply_markup=userbot_kb(managed_uid))
                return
            lines = ["✨ Saved Messages:\n"]
            kb = []
            seen_groups = set()
            for m in msgs[:50]:
                mid, text, media_id, media_type, mgid, btnj = m
                if mgid:
                    if mgid in seen_groups:
                        continue
                    g_items = [x for x in msgs if x[4] == mgid]
                    seen_groups.add(mgid)
                    lines.append(f"• Album {mgid[-6:]} | {len(g_items)} items | {(text or '')[:30]}")
                else:
                    lines.append(f"• ID {mid} | {media_type or 'text'} | {(text or '')[:30]}")
                kb.append([
                    InlineKeyboardButton(f"🔎 Preview {mid}", callback_data=f"ubm_preview_{mid}"),
                    InlineKeyboardButton(f"✏️ Edit {mid}", callback_data=f"ubm_edit_{mid}"),
                    InlineKeyboardButton(f"🗑️ Delete {mid}", callback_data=f"ubm_del_{mid}")
                ])
            kb.append([InlineKeyboardButton("🔙 Back", callback_data="main_menu")])
            # Show raw text for control panel (no premium conversion)
            await q.edit_message_text("\n".join(lines)[:3900], parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup(kb))

        elif data.startswith("ubm_preview_"):
            mid = int(data.split("_")[2])
            row = db.get_message_by_id(mid)
            if not row:
                await q.edit_message_text("‼️ Message not found.", parse_mode=ParseMode.HTML, reply_markup=userbot_kb(managed_uid))
                return
            _, owner, cid, text, media_id, media_type, mgid, btnj = row
            if owner != managed_uid:
                await q.edit_message_text("Not allowed.", parse_mode=ParseMode.HTML)
                return
            try:
                if mgid:
                    all_msgs = db.get_messages(cid) or []
                    group_msgs = [m for m in all_msgs if m[4] == mgid]
                    await _send_messages_with_media_groups(uid, group_msgs, context,
                                                           attach_start_button=False)
                else:
                    markup = buttons_to_markup(btnj)
                    await send_media(context, uid, media_id, media_type or "text", text or "", markup)
                await q.edit_message_text("✅ Preview sent above.", parse_mode=ParseMode.HTML, reply_markup=userbot_kb(managed_uid))
            except Exception as ex:
                await q.edit_message_text(f"‼️ Preview failed: {ex}", parse_mode=ParseMode.HTML, reply_markup=userbot_kb(managed_uid))

        elif data.startswith("ubm_edit_"):
            mid = int(data.split("_")[2])
            row = db.get_message_by_id(mid)
            if not row or row[1] != managed_uid:
                await q.edit_message_text("‼️ Message not found.", parse_mode=ParseMode.HTML, reply_markup=userbot_kb(managed_uid))
                return
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("📝 Edit Text", callback_data=f"ubm_etxt_{mid}"),
                 InlineKeyboardButton("🎬 Replace Media", callback_data=f"ubm_emedia_{mid}")],
                [InlineKeyboardButton("🔘 Edit Buttons", callback_data=f"ubm_ebtn_{mid}"),
                 InlineKeyboardButton("🗑️ Delete", callback_data=f"ubm_del_{mid}")],
                [InlineKeyboardButton("🔙 Back", callback_data="main_menu")],
            ])
            await q.edit_message_text(f"✨ Edit message ID {mid}", parse_mode=ParseMode.HTML, reply_markup=kb)

        elif data.startswith("ubm_etxt_"):
            mid = int(data.split("_")[2])
            ud["editing_text_msg_id"] = mid
            await q.edit_message_text("🔽 Send new text/caption for this message.", parse_mode=ParseMode.HTML)

        elif data.startswith("ubm_emedia_"):
            mid = int(data.split("_")[2])
            ud["editing_media_msg_id"] = mid
            await q.edit_message_text("🔽 Send new media (photo/video/document/audio/voice/sticker). Caption allowed.", parse_mode=ParseMode.HTML)

        elif data.startswith("ubm_ebtn_"):
            mid = int(data.split("_")[2])
            ud["editing_buttons_msg_id"] = mid
            await q.edit_message_text("🔽 Send button lines: Text|https://link", parse_mode=ParseMode.HTML)

        elif data.startswith("ubm_del_"):
            mid = int(data.split("_")[2])
            row = db.get_message_by_id(mid)
            if row and row[1] == managed_uid:
                cid = row[2]
                if row[6]:
                    db.delete_media_group_messages(row[6])
                else:
                    db.delete_message(mid)
                msgs = db.get_messages(cid) or []
                remaining = len(msgs)
                await q.edit_message_text(
                    f"✅ Message deleted. {remaining} message(s) remaining.",
                    parse_mode=ParseMode.HTML,
                    reply_markup=userbot_kb(managed_uid)
                )
            else:
                await q.edit_message_text("‼️ Not found.", parse_mode=ParseMode.HTML, reply_markup=userbot_kb(managed_uid))

        elif data.startswith("setbtn_") or data == "setbtng":
            await handle_set_buttons_callback(update, context)

        elif data == "setmsg_more":
            context.user_data["setting_message"] = True
            ud["setting_message"] = True
            await q.edit_message_text(
                "🔽 Send next welcome message (or type <code>done</code>).",
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="main_menu")]]))

        elif data == "setmsg_cancel":
            removed = 0
            p = ud.get("pending_buttons")
            if p and p.get("msg_id"):
                db.delete_message(p.get("msg_id"))
                removed += 1
            pg = ud.get("pending_buttons_group")
            if pg and pg.get("msg_ids"):
                for _id in pg.get("msg_ids"):
                    db.delete_message(_id)
                    removed += 1
            ud.pop("pending_buttons", None)
            ud.pop("pending_buttons_group", None)
            await q.edit_message_text(
                f"❌ Cancelled. Removed {removed} message(s).",
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("Set More Message", callback_data="setmsg_more")],
                    [InlineKeyboardButton("Done", callback_data="setmsg_done")],
                    [InlineKeyboardButton("🔙 Back", callback_data="main_menu")]
                ]))

        elif data == "setmsg_done":
            for key in ["setting_message", "messages", "pending_buttons", "waiting_buttons"]:
                context.user_data.pop(key, None)
                ud.pop(key, None)
            ud.pop("pending_buttons_group", None)
            await q.edit_message_text(
                "✅ Messages saved! Start button will appear on last message when sent to users.",
                parse_mode=ParseMode.HTML,
                reply_markup=userbot_kb(managed_uid)
            )

        elif data == "bcast_add_btns":
            context.user_data["broadcast_stage"] = "await_buttons"
            await q.edit_message_text(
                "🔽 Send inline buttons lines (Text|https://link).",
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="main_menu")]]))

        elif data == "bcast_send":
            await preview_user_broadcast(q, context)

        elif data == "bcast_confirm":
            await send_user_broadcast(q, context)

        elif data == "main_menu":
            await user_bot_start_from_callback(q, managed_uid)

        elif data.startswith("removechan_"):
            cid = int(data.split("_")[1])
            db.remove_channel(managed_uid, cid)
            await q.edit_message_text("✅ Channel removed.", parse_mode=ParseMode.HTML, reply_markup=userbot_kb(managed_uid))

    except Exception as ex:
        logging.error(f"Error in user_bot_callback: {ex}")
        try:
            await q.edit_message_text(f"‼️ Error: {str(ex)}", parse_mode=ParseMode.HTML, reply_markup=userbot_kb(managed_uid))
        except Exception:
            pass


async def user_bot_start_from_callback(q, uid):
    try:
        await q.edit_message_text(
            "🤖 Welcome to your Auto Join Request Bot!\n\n🔽 Use the buttons below to configure your bot.",
            parse_mode=ParseMode.HTML,
            reply_markup=userbot_kb(uid)
        )
    except Exception as ex:
        if "Message is not modified" not in str(ex):
            logging.error(f"Error in user_bot_start_from_callback: {ex}")


async def show_channels(q, uid):
    channels = db.get_user_channels(uid)
    if not channels:
        await q.edit_message_text("‼️ No channels yet.", parse_mode=ParseMode.HTML, reply_markup=userbot_kb(uid))
        return
    lines = ["✈️ Your Channels:\n"]
    for c in channels:
        cid = c[1]; uname = c[2] or "Private"; title = c[3]
        auto_raw = c[7] if len(c) > 7 else 0
        auto = bool(int(auto_raw)) if auto_raw is not None else False
        mcount = db.get_message_count(cid)
        lines.append(f"• {title} (@{uname}) — msgs: {mcount}, auto: {'ON' if auto else 'OFF'}")
    await q.edit_message_text("\n".join(lines), parse_mode=ParseMode.HTML, reply_markup=userbot_kb(uid))


async def _cleanup_stale_pending(uid: int, context: ContextTypes.DEFAULT_TYPE) -> int:
    cleaned = 0
    pending = db.get_pending_requests(uid) or []
    for rid, requester, channel_id in pending:
        try:
            member = await context.bot.get_chat_member(channel_id, requester)
            st = getattr(member, 'status', None)
            if st in ('member', 'administrator', 'creator'):
                db.mark_request_status(rid, 'approved')
                cleaned += 1
        except Exception as ex:
            msg_str = str(ex)
            if ('Hide_requester_missing' in msg_str or 'USER_NOT_PARTICIPANT' in msg_str
                    or 'user not found' in msg_str.lower()):
                db.mark_request_status(rid, 'approved')
                cleaned += 1
    return cleaned


async def show_pending(q, uid, context: ContextTypes.DEFAULT_TYPE):
    try:
        channels = db.get_user_channels(uid) or []
        for ch in channels:
            await sync_pending_join_requests_for_channel(uid, ch[1], context.bot)
    except Exception:
        pass
    cleaned = await _cleanup_stale_pending(uid, context)
    count = db.get_pending_count(uid)
    extra = f"\n🧹 Cleaned stale: {cleaned}" if cleaned else ""
    await q.edit_message_text(
        f"📊 Pending Requests: {count}{extra}\n\n🔽 Use Accept All to approve.",
        parse_mode=ParseMode.HTML,
        reply_markup=userbot_kb(uid),
    )


async def show_sub_userbot(q, uid):
    sub = db.get_subscription(uid)
    if not sub:
        await q.edit_message_text("‼️ No active subscription.", parse_mode=ParseMode.HTML, reply_markup=userbot_kb(uid))
        return
    expiry = datetime.fromisoformat(sub[2])
    days = (expiry - datetime.now()).days
    sub_text = (
        f"👑 Subscription Details\n\n"
        f"⭐️ Type: {sub[1]}\n"
        f"💎 Max channels: {sub[3]}\n"
        f"🔔 Expiry: {expiry.date()} ({days} days)"
    )
    await q.edit_message_text(sub_text, parse_mode=ParseMode.HTML, reply_markup=userbot_kb(uid))


async def show_toggle(q, uid):
    channels = db.get_user_channels(uid)
    if not channels:
        await q.edit_message_text("‼️ No channels yet.", parse_mode=ParseMode.HTML, reply_markup=userbot_kb(uid))
        return
    kb = []
    for c in channels:
        cid = c[1]; title = c[3]
        auto_raw = c[7] if len(c) > 7 else 0
        auto = bool(int(auto_raw)) if auto_raw is not None else False
        kb.append([InlineKeyboardButton(
            f"{'🟢' if auto else '🔴'} {title[:18]}",
            callback_data=f"toggleauto_{cid}")])
    kb.append([InlineKeyboardButton("🔙 Back", callback_data="main_menu")])
    await q.edit_message_text("🟢 Auto-Approve ON/OFF per channel.",
                              parse_mode=ParseMode.HTML,
                              reply_markup=InlineKeyboardMarkup(kb))


async def handle_toggle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q or not q.from_user:
        return
    try:
        await q.answer()
    except Exception:
        pass
    uid = q.from_user.id
    managed_uid = get_managed_user_id(context, uid)
    data = q.data
    if not data.startswith("toggleauto_"):
        return
    cid = int(data.split("_")[1])
    channel = db.get_channel_owner_data(cid)
    if not channel or channel[0] != managed_uid:
        await q.edit_message_text("‼️ Channel not found.", parse_mode=ParseMode.HTML, reply_markup=userbot_kb(managed_uid))
        return
    auto_raw = channel[7] if len(channel) > 7 else 0
    current = bool(int(auto_raw)) if auto_raw is not None else False
    db.set_auto_approve(managed_uid, cid, not current)
    await show_toggle(q, managed_uid)


async def accept_all(q, uid, context):
    try:
        channels = db.get_user_channels(uid) or []
        for ch in channels:
            await sync_pending_join_requests_for_channel(uid, ch[1], context.bot)
    except Exception:
        pass
    await _cleanup_stale_pending(uid, context)
    pending = db.get_pending_requests(uid)
    if not pending:
        await q.edit_message_text("‼️ No pending requests.", parse_mode=ParseMode.HTML, reply_markup=userbot_kb(uid))
        return
    ok = 0; fail = 0; cleaned = 0
    for rid, requester, channel_id in pending:
        try:
            await context.bot.approve_chat_join_request(channel_id, requester)
            db.mark_request_status(rid, 'approved')
            ok += 1
        except Exception as ex:
            msg_str = str(ex)
            if 'User_already_participant' in msg_str or 'Hide_requester_missing' in msg_str:
                db.mark_request_status(rid, 'approved')
                cleaned += 1
            else:
                logging.error(f"accept_all error {requester} {channel_id}: {ex}")
                fail += 1
    result_text = (
        f"✅ Accepted: {ok}\n"
        f"✨ Cleaned: {cleaned}\n"
        f"‼️ Failed: {fail}"
    )
    await q.edit_message_text(result_text, parse_mode=ParseMode.HTML, reply_markup=userbot_kb(uid))


async def prompt_remove_channel(q, uid):
    channels = db.get_user_channels(uid)
    if not channels:
        await q.edit_message_text("‼️ No channels to remove.", parse_mode=ParseMode.HTML, reply_markup=userbot_kb(uid))
        return
    kb = [[InlineKeyboardButton(f"❌ {c[3][:22]}", callback_data=f"removechan_{c[1]}")]
          for c in channels]
    kb.append([InlineKeyboardButton("🔙 Back", callback_data="main_menu")])
    await q.edit_message_text("‼️ Select channel to remove:",
                              parse_mode=ParseMode.HTML,
                              reply_markup=InlineKeyboardMarkup(kb))


async def _flush_media_group(actor_uid, managed_uid, chat_id, context, media_group_id):
    if not actor_uid or not managed_uid or not media_group_id:
        return
    ud = _runtime_store(context, actor_uid)
    key = f"mg_{media_group_id}"
    items = ud.get(key, [])
    if not items:
        return
    channels = db.get_user_channels(managed_uid)
    if not channels:
        return
    channel_id = channels[0][1]
    saved_ids = []
    for it in items:
        saved_ids.append(
            db.add_message(managed_uid, channel_id,
                           it.get("text", ""), it.get("media_id"),
                           it.get("media_type"), media_group_id)
        )
    ud.pop(key, None)
    ud.pop("manual_album_buffer", None)
    ud["pending_buttons_group"] = {"msg_ids": saved_ids}
    await context.bot.send_message(
        chat_id,
        "Media group saved. Choose an option:",
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("➕ Set Inline Button", callback_data="setbtng")],
            [InlineKeyboardButton("Set More Message", callback_data="setmsg_more")],
            [InlineKeyboardButton("❌ Cancel This Message", callback_data="setmsg_cancel")],
            [InlineKeyboardButton("Done", callback_data="setmsg_done")],
        ]),
    )


async def _flush_media_group_job(context: ContextTypes.DEFAULT_TYPE):
    data = context.job.data or {}
    await _flush_media_group(
        data.get("actor_uid"), data.get("managed_uid"),
        data.get("chat_id"), context, data.get("media_group_id"),
    )


async def handle_user_bot_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not user:
        return
    uid = user.id
    msg = update.message
    if not msg:
        return

    owner_uid = context.application.bot_data.get("owner_id")

    if msg.reply_to_message and owner_uid and (uid == owner_uid or uid in ADMIN_USER_IDS):
        key = f"{uid}:{msg.reply_to_message.message_id}"
        target_uid = _get_support_uid(USERBOT_SUPPORT_REPLY_MAP, key)
        if target_uid:
            try:
                await context.bot.copy_message(
                    chat_id=target_uid, from_chat_id=msg.chat_id, message_id=msg.message_id)
                if uid != ADMIN_USER_ID:
                    try:
                        await context.bot.send_message(
                            ADMIN_USER_ID,
                            f"↪️ Admin {uid} replied to user {target_uid}",
                            parse_mode=ParseMode.HTML)
                        await context.bot.copy_message(
                            chat_id=ADMIN_USER_ID,
                            from_chat_id=msg.chat_id,
                            message_id=msg.message_id)
                    except Exception as mirror_err:
                        logging.error(f"support mirror failed: {mirror_err}")
                await send_ephemeral_reply(msg, f"✅ Reply delivered to user {target_uid}", 2)
            except Exception as ex:
                await msg.reply_text(f"❌ Reply failed for {target_uid}: {ex}", parse_mode=ParseMode.HTML)
            return

    if owner_uid and uid not in ({owner_uid} | ADMIN_USER_IDS):
        if db.is_banned(owner_uid, uid):
            return
        try:
            inbox_ids = set(ADMIN_USER_IDS)
            inbox_ids.add(owner_uid)
            delivered = 0
            for aid in inbox_ids:
                try:
                    r = await context.bot.forward_message(
                        chat_id=aid, from_chat_id=msg.chat_id, message_id=msg.message_id)
                    _store_support_map(USERBOT_SUPPORT_REPLY_MAP, f"{aid}:{r.message_id}", uid)
                    delivered += 1
                except Exception as e2:
                    logging.error(f"userbot support relay failed to {aid}: {e2}")
            if delivered > 0:
                await send_ephemeral_reply(msg, "✅ Message sent to support. Please wait for reply.", 2)
            else:
                await msg.reply_text("⚠️ Support unavailable right now. Try later.", parse_mode=ParseMode.HTML)
        except Exception as ex:
            logging.error(f"userbot support relay failed for user {uid}: {ex}")
            await msg.reply_text("⚠️ Could not contact support right now.", parse_mode=ParseMode.HTML)
        return

    managed_uid = get_managed_user_id(context, uid)
    ud = _runtime_store(context, uid)

    if ud.get("editing_text_msg_id"):
        mid = ud.pop("editing_text_msg_id")
        row = db.get_message_by_id(mid)
        if row and row[1] == managed_uid:
            # Store the raw text exactly as received (preserve emojis)
            processed_text = msg.text or msg.caption or ""
            db.update_message_text(mid, processed_text)
            await msg.reply_text("✅ Text updated.", parse_mode=ParseMode.HTML, reply_markup=userbot_kb(managed_uid))
        return

    if ud.get("editing_buttons_msg_id"):
        mid = ud.pop("editing_buttons_msg_id")
        row = db.get_message_by_id(mid)
        if row and row[1] == managed_uid:
            btn_json = buttons_json_from_text(msg.text or "")
            db.update_message_buttons(mid, btn_json or "[]")
            await msg.reply_text("✅ Buttons updated.", parse_mode=ParseMode.HTML, reply_markup=userbot_kb(managed_uid))
        return

    if ud.get("editing_media_msg_id"):
        mid = ud.pop("editing_media_msg_id")
        row = db.get_message_by_id(mid)
        if row and row[1] == managed_uid:
            media_id, media_type, text, entities = extract_media_from_msg(msg)
            # Store the raw text exactly as received
            processed_text = text
            db.update_message_media(mid, media_id, media_type, processed_text)
            await msg.reply_text("✅ Media updated.", parse_mode=ParseMode.HTML, reply_markup=userbot_kb(managed_uid))
        return

    if ud.get("adding_channel") or context.user_data.get("adding_channel"):
        if (msg.forward_from_chat
                and msg.forward_from_chat.type in ['channel', 'group', 'supergroup']):
            ch = msg.forward_from_chat
            try:
                member = await context.bot.get_chat_member(ch.id, context.bot.id)
                if member.status not in ['administrator', 'creator']:
                    await msg.reply_text("‼️ Bot not admin in that channel.", parse_mode=ParseMode.HTML)
                    return
                sub = db.get_subscription(managed_uid)
                if sub:
                    max_ch = sub[3] or 1
                    current_ch = len(db.get_user_channels(managed_uid))
                    if current_ch >= max_ch:
                        await msg.reply_text(
                            f"‼️ Channel limit reached ({max_ch}). Upgrade to Pro for more.",
                            parse_mode=ParseMode.HTML)
                        return
                db.add_channel(managed_uid, ch.id,
                               getattr(ch, 'username', None), ch.title or "Channel")
                await sync_pending_join_requests_for_channel(managed_uid, ch.id, context.bot)
                ud["adding_channel"] = False
                context.user_data["adding_channel"] = False
                await msg.reply_text(
                    f"✅ Channel added!\n✨ Old pending requests also synced.",
                    parse_mode=ParseMode.HTML,
                    reply_markup=userbot_kb(managed_uid))
            except Exception as ex:
                await msg.reply_text(f"Error: {ex}", parse_mode=ParseMode.HTML)
        else:
            await msg.reply_text("🔽 Forward a message from the channel.", parse_mode=ParseMode.HTML)
        return

    if ud.get("waiting_buttons"):
        info = ud.get("waiting_buttons")
        msg_id = info.get("msg_id")
        msg_ids = info.get("msg_ids") or ([] if msg_id is None else [msg_id])
        btn_json = buttons_json_from_text(msg.text or "")
        if btn_json:
            for _id in msg_ids:
                db.update_message_buttons(_id, btn_json)
            await msg.reply_text(
                "✅ Inline buttons saved.",
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("Set More Message", callback_data="setmsg_more")],
                    [InlineKeyboardButton("Done", callback_data="setmsg_done")],
                    [InlineKeyboardButton("🔙 Back", callback_data="main_menu")],
                ]))
        else:
            await msg.reply_text(
                "‼️ No valid buttons parsed (use Text|https://link per line).",
                parse_mode=ParseMode.HTML,
                reply_markup=userbot_kb(managed_uid))
        ud.pop("waiting_buttons", None)
        ud.pop("pending_buttons_group", None)
        return

    if ud.get("setting_message") or context.user_data.get("setting_message"):
        if msg.text and msg.text.strip().lower() in ["done", "/done", "finish", "stop", "complete"]:
            for key in ["setting_message", "messages", "pending_buttons", "waiting_buttons"]:
                ud.pop(key, None)
                context.user_data.pop(key, None)
            await msg.reply_text("✅ Saved messages.", parse_mode=ParseMode.HTML, reply_markup=userbot_kb(managed_uid))
            return
        channels = db.get_user_channels(managed_uid)
        if not channels:
            await msg.reply_text("‼️ No channel added.", parse_mode=ParseMode.HTML)
            return
        channel_id = channels[0][1]
        media_id, media_type, text, entities = extract_media_from_msg(msg)
        media_group_id = getattr(msg, "media_group_id", None)

        if media_group_id:
            key = f"mg_{media_group_id}"
            arr = ud.get(key, [])
            first_item = len(arr) == 0
            # Store the raw text exactly as received (preserve emojis)
            processed_text = text
            arr.append({"text": processed_text, "media_id": media_id, "media_type": media_type})
            ud[key] = arr
            if first_item:
                try:
                    await msg.reply_text("Album received, processing...", parse_mode=ParseMode.HTML)
                except Exception:
                    pass
            job_key = f"mg_job_{media_group_id}"
            old_job = ud.get(job_key)
            if old_job:
                try:
                    old_job.schedule_removal()
                except Exception:
                    pass
            j = context.job_queue.run_once(
                _flush_media_group_job, when=1.2,
                data={"actor_uid": uid, "managed_uid": managed_uid,
                      "chat_id": msg.chat_id, "media_group_id": media_group_id},
            )
            ud[job_key] = j
            return

        # Store the raw text exactly as received (preserve emojis)
        processed_text = text
        msg_id = db.add_message(managed_uid, channel_id, processed_text, media_id, media_type, None)
        ud["pending_buttons"] = {"msg_id": msg_id, "channel_id": channel_id}
        await msg.reply_text(
            "✅ Saved! Choose an option:",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("➕ Set Inline Button",
                                      callback_data=f"setbtn_{msg_id}")],
                [InlineKeyboardButton("Skip Inline", callback_data="setmsg_more")],
                [InlineKeyboardButton("Set More Message", callback_data="setmsg_more")],
                [InlineKeyboardButton("❌ Cancel This Message",
                                      callback_data="setmsg_cancel")],
                [InlineKeyboardButton("Done", callback_data="setmsg_done")],
            ]))
        return

    if context.user_data.get("broadcast_stage") == "await_message":
        media_id, media_type, text, entities = extract_media_from_msg(msg)
        # Store the raw text exactly as received
        processed_text = text
        draft = {"text": processed_text, "media": media_id, "media_type": media_type}
        context.user_data["broadcast_draft"] = draft
        context.user_data["broadcast_stage"] = "buttons_or_send"
        await msg.reply_text(
            "✅ Broadcast draft saved. Add inline buttons or send now?",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("➕ Add Inline Buttons",
                                      callback_data="bcast_add_btns")],
                [InlineKeyboardButton("🚀 Send Now", callback_data="bcast_send")],
                [InlineKeyboardButton("❌ Cancel", callback_data="main_menu")],
            ]))
        return

    if context.user_data.get("broadcast_stage") == "await_buttons":
        draft = context.user_data.get("broadcast_draft", {})
        btn_json = buttons_json_from_text(msg.text or "")
        if btn_json:
            draft["buttons_json"] = btn_json
            context.user_data["broadcast_draft"] = draft
            await msg.reply_text(
                "✅ Inline buttons saved. Send now?",
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🚀 Send Now", callback_data="bcast_send")],
                    [InlineKeyboardButton("❌ Cancel", callback_data="main_menu")],
                ]))
        else:
            await msg.reply_text(
                "‼️ No valid buttons (use Text|https://link per line).",
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="main_menu")]]))
        context.user_data["broadcast_stage"] = "buttons_or_send"
        return

    await msg.reply_text("🔽 Use buttons to manage.", parse_mode=ParseMode.HTML, reply_markup=userbot_kb(managed_uid))


async def handle_set_buttons_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q or not q.from_user:
        return
    try:
        await q.answer()
    except Exception:
        pass
    uid = q.from_user.id
    ud = _runtime_store(context, uid)
    data = q.data
    if data == "setbtng":
        grp = ud.get("pending_buttons_group")
        if not grp or not grp.get("msg_ids"):
            await q.edit_message_text("‼️ Group not found. Send media group again.",
                                      parse_mode=ParseMode.HTML,
                                      reply_markup=userbot_kb(get_managed_user_id(context, uid)))
            return
        ud["waiting_buttons"] = {"msg_ids": grp.get("msg_ids")}
        await q.edit_message_text(
            "🔽 Send inline buttons lines (Text|https://link).",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="main_menu")]]))
        return
    if not data.startswith("setbtn_"):
        return
    msg_id = int(data.split("_")[1])
    ud["waiting_buttons"] = {"msg_id": msg_id}
    await q.edit_message_text(
        "🔽 Send inline buttons lines (Text|https://link) for this message.",
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="main_menu")]]))


async def handle_join_request(update: Update, context: ContextTypes.DEFAULT_TYPE):
    jr = update.chat_join_request
    if not jr:
        return
    requester = jr.from_user
    chat = jr.chat
    channel_row = db.get_channel_owner_data(chat.id)
    if not channel_row:
        logging.warning(f"Join request for unknown channel {chat.id}")
        return
    owner_uid = channel_row[0]
    auto_raw = channel_row[7] if len(channel_row) > 7 else 0
    auto = bool(int(auto_raw)) if auto_raw is not None else False

    if db.is_banned(owner_uid, requester.id):
        try:
            await jr.decline()
        except Exception:
            pass
        return

    msgs = db.get_messages(chat.id)
    try:
        if msgs:
            await _send_messages_with_media_groups(
                requester.id, msgs, context, attach_start_button=True)
        else:
            wm = channel_row[4] or ""
            wid = channel_row[5]
            wtype = channel_row[6]
            markup = buttons_to_markup(buttons_json_from_text(wm) or None)
            if wid and wtype:
                await send_media(context, requester.id, wid, wtype, wm, markup)
            elif wm:
                await context.bot.send_message(
                    requester.id, wm, parse_mode=ParseMode.HTML,
                    reply_markup=markup)
        db.mark_reachable(owner_uid, requester.id)
    except Exception as ex:
        logging.error(f"Send welcome error: {ex}")

    db.add_join_request(owner_uid, requester.id, chat.id, 'approved' if auto else 'pending')
    if auto:
        try:
            await jr.approve()
        except Exception as ex:
            logging.error(f"Approve error: {ex}")


async def start_user_bot(token: str, owner_id: int):
    app = ApplicationBuilder().token(token).concurrent_updates(True).connect_timeout(30).read_timeout(30).write_timeout(30).pool_timeout(30).build()
    app.bot_data["owner_id"] = owner_id
    app.add_handler(CommandHandler("start", user_bot_start))
    app.add_handler(CallbackQueryHandler(
        handle_public_userbot_callback, pattern=r'^(start_now|live_chat_support)$'))
    app.add_handler(CallbackQueryHandler(
        user_bot_callback,
        pattern="^(ub_|ubm_|ubmm_|toggleauto_|delmsg_|setbtn_|setbtng|setmsg_|main_menu|bcast_|removechan_)"))
    app.add_handler(CallbackQueryHandler(handle_toggle_callback, pattern="^toggleauto_"))
    app.add_handler(CallbackQueryHandler(handle_set_buttons_callback, pattern="^setbtn_"))
    app.add_handler(MessageHandler(
        ~filters.COMMAND & (filters.TEXT | filters.PHOTO | filters.VIDEO |
                            filters.Document.ALL | filters.AUDIO | filters.VOICE |
                            filters.ANIMATION | filters.Sticker.ALL),
        handle_user_bot_message
    ))
    app.add_handler(ChatJoinRequestHandler(handle_join_request))
    await app.initialize()
    await app.start()
    await app.updater.start_polling()
    user_bot_applications[owner_id] = app
    try:
        for ch in db.get_user_channels(owner_id) or []:
            await sync_pending_join_requests_for_channel(owner_id, ch[1], app.bot)
    except Exception as ex:
        logging.error(f"Startup pending sync failed for owner {owner_id}: {ex}")
    return True


async def stop_user_bot(owner_id: int):
    if owner_id in user_bot_applications:
        try:
            app = user_bot_applications[owner_id]
            await app.updater.stop()
            await app.stop()
            await app.shutdown()
            logging.info(f"🛑 Stopped user bot for {owner_id}")
        except Exception as ex:
            logging.error(f"Error stopping user bot {owner_id}: {ex}")
        user_bot_applications.pop(owner_id, None)
    db.set_user_bot_active(owner_id, False)


async def remove_my_bot(q, uid):
    await stop_user_bot(uid)
    db.remove_user_bot(uid)
    await q.edit_message_text("✅ Bot removed.", parse_mode=ParseMode.HTML, reply_markup=main_menu_kb(uid))


# ================= MAIN BOT HANDLERS =================
async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not user:
        return
    db.add_user(user.id, user.username or "", user.first_name or "", user.last_name or "")
    welcome_text = (
        "👋 Welcome to Auto Join Request Bot Manager!\n\n"
        "🚀 Click Setup Bot to begin.\n"
        "⭐️ Manage your channels with ease!\n"
        "💎 Premium emojis are supported automatically!"
    )
    await update.message.reply_text(
        welcome_text,
        parse_mode=ParseMode.HTML,
        reply_markup=main_menu_kb(user.id)
    )


async def admin_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not user or not is_admin(user.id):
        await update.message.reply_text("Not authorized.", parse_mode=ParseMode.HTML)
        return
    await update.message.reply_text("👑 Admin Panel", parse_mode=ParseMode.HTML, reply_markup=admin_kb())


async def proof_text_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not user or not is_admin(user.id):
        await update.message.reply_text("Not authorized.", parse_mode=ParseMode.HTML)
        return
    proof_text = (
        "✅ USERBOT ACTIVATION CONFIRMATION\n\n"
        "Your UserBot setup has been completed successfully and is now active.\n\n"
        "Details:\n"
        "• Service: Advanced UserBot Setup\n"
        "• Plan: Monthly Subscription\n"
        "• Amount: ₹2500\n"
        f"• Start Date: {datetime.utcnow().strftime('%d %B %Y')}\n"
        "• Status: Active\n\n"
        "Your bot credentials have been configured from the admin panel, and your setup is ready to use.\n"
        "For any support or changes, feel free to contact us anytime.\n\n"
        f"Proof ID: PRF-{datetime.utcnow().strftime('%d%m%y')}-001"
    )
    await update.message.reply_text(proof_text, parse_mode=ParseMode.HTML)


# ================= ADMIN SUBSCRIPTION LIST =================
async def show_admin_sub_list(q, context: ContextTypes.DEFAULT_TYPE, page: int = 0):
    """Show paginated subscription list with status indicators."""
    subs = db.get_all_subscriptions()
    if not subs:
        await q.edit_message_text(
            "‼️ No subscriptions found.",
            parse_mode=ParseMode.HTML,
            reply_markup=admin_kb()
        )
        return

    now = datetime.now()
    PAGE_SIZE = 8
    total = len(subs)
    total_pages = max(1, (total + PAGE_SIZE - 1) // PAGE_SIZE)
    page = max(0, min(page, total_pages - 1))
    start = page * PAGE_SIZE
    page_subs = subs[start:start + PAGE_SIZE]

    lines = [f"📋 <b>Subscription List</b> ({total} total) — Page {page+1}/{total_pages}\n"]
    for s in page_subs:
        uid = s["user_id"]
        uname = f"@{s['username']}" if s.get("username") else s.get("first_name", "Unknown")
        plan = s.get("subscription_type", "?")
        expiry_str = s.get("expiry_date", "")
        bot_uname = s.get("bot_username", "")
        is_running = uid in user_bot_applications
        bot_active_db = s.get("bot_active", 0)

        try:
            exp_dt = datetime.fromisoformat(expiry_str)
            days_left = (exp_dt - now).days
            if days_left < 0:
                status = "❌ Expired"
                exp_display = f"Expired {abs(days_left)}d ago"
            elif days_left == 0:
                status = "⚠️ Today"
                exp_display = "Expires TODAY"
            elif days_left <= 3:
                status = "🔴 Critical"
                exp_display = f"{days_left}d left"
            elif days_left <= 7:
                status = "🟡 Soon"
                exp_display = f"{days_left}d left"
            else:
                status = "🟢 Active"
                exp_display = f"{days_left}d left"
        except Exception:
            status = "❓ Unknown"
            exp_display = expiry_str[:10] if expiry_str else "N/A"

        bot_status = "🤖🟢" if is_running else ("🤖🔴" if bot_uname else "🚫")
        bot_info = f"@{bot_uname}" if bot_uname else "No bot"

        lines.append(
            f"\n<b>{uname}</b> ({uid})\n"
            f"  ⭐️ {plan} | {status} | {exp_display}\n"
            f"  {bot_status} {bot_info}"
        )

    text = "\n".join(lines)

    kb = []
    # Pagination row
    nav_row = []
    if page > 0:
        nav_row.append(InlineKeyboardButton("◀️ Prev", callback_data=f"admin_sublist_pg_{page-1}"))
    if page < total_pages - 1:
        nav_row.append(InlineKeyboardButton("Next ▶️", callback_data=f"admin_sublist_pg_{page+1}"))
    if nav_row:
        kb.append(nav_row)

    kb.append([InlineKeyboardButton("🔙 Back to Admin", callback_data="admin_panel")])

    await q.edit_message_text(
        text[:4000],
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(kb)
    )


# ================= ADMIN USERBOT CONTROL PANEL =================
async def show_admin_userbot_control(q, context: ContextTypes.DEFAULT_TYPE):
    """Advanced userbot control panel — start/stop individual or all."""
    bots = db.get_all_user_bots() or []
    if not bots:
        await q.edit_message_text(
            "‼️ No userbots found.",
            parse_mode=ParseMode.HTML,
            reply_markup=admin_kb()
        )
        return

    running_count = len(user_bot_applications)
    total_count = len(bots)

    lines = [
        f"🤖 <b>UserBot Control Panel</b>\n",
        f"🟢 Running: {running_count} / {total_count}\n",
        "─────────────────────────\n"
    ]
    for uid, token, buser, active in bots:
        is_running = uid in user_bot_applications
        sub = db.get_subscription(uid)
        has_valid_sub = False
        if sub:
            try:
                exp = datetime.fromisoformat(sub[2])
                has_valid_sub = exp > datetime.now()
            except Exception:
                pass
        status_icon = "🟢" if is_running else "🔴"
        sub_icon = "✅" if has_valid_sub else "❌"
        lines.append(f"{status_icon} @{buser or 'N/A'} ({uid}) {sub_icon}")

    kb = []
    # Individual bot controls
    for uid, token, buser, active in bots:
        is_running = uid in user_bot_applications
        row = []
        if is_running:
            row.append(InlineKeyboardButton(
                f"🛑 Stop @{buser or uid}", callback_data=f"admin_ub_stop_{uid}"))
        else:
            row.append(InlineKeyboardButton(
                f"▶️ Start @{buser or uid}", callback_data=f"admin_ub_start_{uid}"))
        row.append(InlineKeyboardButton("🔎 Info", callback_data=f"admin_ub_info_{uid}"))
        kb.append(row)

    # Bulk controls
    kb.append([
        InlineKeyboardButton("🚀 Start All", callback_data="admin_start_all"),
        InlineKeyboardButton("🛑 Stop All", callback_data="admin_stop_all"),
    ])
    kb.append([InlineKeyboardButton("🔄 Refresh", callback_data="admin_userbots")])
    kb.append([InlineKeyboardButton("🔙 Back to Admin", callback_data="admin_panel")])

    await q.edit_message_text(
        "\n".join(lines)[:4000],
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(kb)
    )


async def show_admin_ub_info(q, uid_target: int, context: ContextTypes.DEFAULT_TYPE):
    """Show detailed info about a specific userbot."""
    bot_data = db.get_user_bot(uid_target)
    sub = db.get_subscription(uid_target)
    user_doc = db.mongo.users.find_one({"user_id": uid_target}) or {}
    is_running = uid_target in user_bot_applications

    uname = user_doc.get("username", "")
    fname = user_doc.get("first_name", "")

    lines = [f"🔎 <b>UserBot Info</b>\n"]
    lines.append(f"👤 User: {fname} {'@'+uname if uname else ''} ({uid_target})")

    if bot_data:
        lines.append(f"🤖 Bot: @{bot_data[2] or 'N/A'}")
        lines.append(f"🔘 DB Active Flag: {'Yes' if bot_data[3] else 'No'}")
        lines.append(f"⚡️ Actually Running: {'🟢 Yes' if is_running else '🔴 No'}")
    else:
        lines.append("🤖 Bot: Not configured")

    if sub:
        try:
            exp = datetime.fromisoformat(sub[2])
            days_left = (exp - datetime.now()).days
            lines.append(f"⭐️ Plan: {sub[1]}")
            lines.append(f"📅 Expiry: {exp.strftime('%d %b %Y')} ({days_left}d left)")
        except Exception:
            lines.append(f"⭐️ Plan: {sub[1] if sub else 'None'}")
    else:
        lines.append("⭐️ Subscription: None")

    channels = db.get_user_channels(uid_target) or []
    total_users = db.get_total_requesters_count(uid_target)
    reachable = db.get_reachable_requesters_count(uid_target)
    lines.append(f"✈️ Channels: {len(channels)}")
    lines.append(f"👥 Total Users: {total_users} | Reachable: {reachable}")

    kb = []
    if bot_data:
        if is_running:
            kb.append([InlineKeyboardButton("🛑 Stop Bot", callback_data=f"admin_ub_stop_{uid_target}")])
        else:
            kb.append([InlineKeyboardButton("▶️ Start Bot", callback_data=f"admin_ub_start_{uid_target}")])
        kb.append([InlineKeyboardButton("🗑️ Remove Bot", callback_data=f"admin_remove_bot_{uid_target}")])

    kb.append([InlineKeyboardButton("🔙 Back", callback_data="admin_userbots")])

    await q.edit_message_text(
        "\n".join(lines),
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(kb)
    )


# ================= CALLBACK HANDLER =================
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
            await q.edit_message_text("👋 Main menu", parse_mode=ParseMode.HTML, reply_markup=main_menu_kb(uid))
            for key in ["broadcast_stage", "broadcast_draft", "waiting_buttons",
                        "admin_broadcast_draft", "admin_broadcast",
                        "admin_broadcast_stage", "admin_broadcast_target"]:
                context.user_data.pop(key, None)

        elif data == "setup_bot":
            sub = db.get_subscription(uid)
            if not sub:
                await q.edit_message_text("‼️ Subscription required.", parse_mode=ParseMode.HTML, reply_markup=subscription_kb())
                return
            try:
                expiry = datetime.fromisoformat(sub[2])
                if expiry < datetime.now():
                    await q.edit_message_text(
                        f"‼️ Your subscription has expired!\n📞 Contact {ADMIN_USERNAME} to renew.",
                        parse_mode=ParseMode.HTML,
                        reply_markup=subscription_kb()
                    )
                    return
            except Exception:
                pass
            user_bot = db.get_user_bot(uid)
            if user_bot:
                bot_username = user_bot[2]
                is_running = uid in user_bot_applications
                status_text = "🟢 Running" if is_running else "🔴 Stopped"
                await q.edit_message_text(
                    f"🤖 Bot already linked: @{bot_username}\n🔘 Status: {status_text}\n🔽 Tap below to manage it.",
                    parse_mode=ParseMode.HTML,
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("🔗 Open My Bot",
                                              url=f"https://t.me/{bot_username}")],
                        [InlineKeyboardButton("🗑️ Remove My Bot",
                                              callback_data="remove_my_bot_confirm")],
                        [InlineKeyboardButton("🔙 Back", callback_data="main_menu")],
                    ]),
                )
            else:
                await q.edit_message_text(
                    "🔐 Send your BotFather token:",
                    parse_mode=ParseMode.HTML,
                    reply_markup=InlineKeyboardMarkup(
                        [[InlineKeyboardButton("🔙 Back", callback_data="main_menu")]]))
                context.user_data["waiting_token"] = True

        elif data == "my_subscription":
            await show_subscription(q, uid)

        elif data.startswith("sub_"):
            await handle_sub_purchase(q, data)

        elif data == "admin_panel":
            if not is_admin(uid):
                await q.edit_message_text("‼️ Not authorized", parse_mode=ParseMode.HTML)
                return
            for key in ["admin_broadcast_draft", "admin_broadcast",
                        "admin_broadcast_stage", "admin_broadcast_target"]:
                context.user_data.pop(key, None)
            await q.edit_message_text("👑 Admin Panel", parse_mode=ParseMode.HTML, reply_markup=admin_kb())

        elif data == "admin_all_users":
            await show_all_users(q)

        elif data == "admin_sub_list":
            if not is_admin(uid):
                return
            await show_admin_sub_list(q, context, page=0)

        elif data.startswith("admin_sublist_pg_"):
            if not is_admin(uid):
                return
            page = int(data.split("_")[-1])
            await show_admin_sub_list(q, context, page=page)

        elif data == "admin_userbots":
            if not is_admin(uid):
                return
            await show_admin_userbot_control(q, context)

        elif data == "admin_add_userbot":
            await q.edit_message_text(
                "🚀 Add UserBot via Admin\n\n🔽 Send: user_id bot_token\nExample: 123456789 123456:ABCdef...",
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup(
                    [[InlineKeyboardButton("🔙 Back", callback_data="admin_panel")]]),
            )
            context.user_data["admin_add_userbot"] = True

        elif data.startswith("admin_ub_info_"):
            if not is_admin(uid):
                return
            tuid = int(data.split("_")[-1])
            await show_admin_ub_info(q, tuid, context)

        elif data.startswith("admin_ub_start_"):
            if not is_admin(uid):
                return
            tuid = int(data.split("_")[-1])
            bot_data = db.get_user_bot(tuid)
            if not bot_data or not bot_data[1]:
                await q.edit_message_text("‼️ UserBot not found.", parse_mode=ParseMode.HTML, reply_markup=admin_kb())
                return
            sub = db.get_subscription(tuid)
            if not sub:
                await q.edit_message_text(
                    f"‼️ User {tuid} has no subscription.",
                    parse_mode=ParseMode.HTML, reply_markup=admin_kb())
                return
            try:
                exp = datetime.fromisoformat(sub[2])
                if exp < datetime.now():
                    await q.edit_message_text(
                        f"‼️ User {tuid} subscription expired.",
                        parse_mode=ParseMode.HTML, reply_markup=admin_kb())
                    return
            except Exception:
                pass
            if tuid in user_bot_applications:
                await q.edit_message_text(
                    f"ℹ️ Bot @{bot_data[2]} already running.",
                    parse_mode=ParseMode.HTML,
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="admin_userbots")]]))
                return
            try:
                await start_user_bot(bot_data[1], tuid)
                db.set_user_bot_active(tuid, True)
                await q.edit_message_text(
                    f"✅ Bot @{bot_data[2]} started for user {tuid}",
                    parse_mode=ParseMode.HTML,
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="admin_userbots")]]))
            except Exception as ex:
                await q.edit_message_text(
                    f"‼️ Failed to start: {ex}",
                    parse_mode=ParseMode.HTML,
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="admin_userbots")]]))

        elif data.startswith("admin_ub_stop_"):
            if not is_admin(uid):
                return
            tuid = int(data.split("_")[-1])
            bot_data = db.get_user_bot(tuid)
            buser = bot_data[2] if bot_data else str(tuid)
            if tuid not in user_bot_applications:
                await q.edit_message_text(
                    f"ℹ️ Bot @{buser} is not running.",
                    parse_mode=ParseMode.HTML,
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="admin_userbots")]]))
                return
            try:
                await stop_user_bot(tuid)
                await q.edit_message_text(
                    f"🛑 Bot @{buser} stopped for user {tuid}",
                    parse_mode=ParseMode.HTML,
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="admin_userbots")]]))
            except Exception as ex:
                await q.edit_message_text(
                    f"‼️ Failed to stop: {ex}",
                    parse_mode=ParseMode.HTML,
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="admin_userbots")]]))

        elif data.startswith("admin_take_"):
            tuid = int(data.split("_")[2])
            ub = db.get_user_bot(tuid)
            if not ub or not ub[2]:
                await q.edit_message_text("‼️ Userbot not found.", parse_mode=ParseMode.HTML, reply_markup=admin_kb())
                return
            await q.edit_message_text(
                f"👑 Admin attached to user {tuid}\n🤖 Bot: @{ub[2]}\n🔽 Now open bot and control.",
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔗 Open UserBot",
                                          url=f"https://t.me/{ub[2]}")],
                    [InlineKeyboardButton("🗑️ Remove UserBot",
                                          callback_data=f"admin_remove_bot_{tuid}")],
                    [InlineKeyboardButton("🔙 Back", callback_data="admin_userbots")],
                ])
            )

        elif (data.startswith("admin_remove_bot_")
              and not data.startswith("admin_remove_bot_confirm_")):
            tuid = int(data.split("_")[3])
            ub = db.get_user_bot(tuid)
            if not ub:
                await q.edit_message_text("‼️ Userbot not found.", parse_mode=ParseMode.HTML, reply_markup=admin_kb())
                return
            buser = ub[2] or "unknown"
            await q.edit_message_text(
                f"‼️ Confirm remove userbot?\n📇 User: {tuid}\n🤖 Bot: @{buser}",
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("✅ Yes, Remove",
                                          callback_data=f"admin_remove_bot_confirm_{tuid}")],
                    [InlineKeyboardButton("❌ Cancel",
                                          callback_data=f"admin_take_{tuid}")],
                ])
            )

        elif data.startswith("admin_remove_bot_confirm_"):
            tuid = int(data.split("_")[4])
            await stop_user_bot(tuid)
            db.remove_user_bot(tuid)
            await q.edit_message_text(f"✅ UserBot removed for user {tuid}",
                                      parse_mode=ParseMode.HTML,
                                      reply_markup=InlineKeyboardMarkup(
                                          [[InlineKeyboardButton("🔙 Back",
                                                                 callback_data="admin_userbots")]]))

        elif data == "admin_add_sub":
            await q.edit_message_text(
                "⭐️ Send: user_id days plan(Basic/Pro)",
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup(
                    [[InlineKeyboardButton("🔙 Back", callback_data="admin_panel")]]))
            context.user_data["admin_add_sub"] = True

        elif data == "admin_check_expiry":
            await check_expiry(q)

        elif data == "admin_stats":
            await show_stats(q)

        elif data == "admin_start_all":
            await start_all_userbots(q)

        elif data == "admin_stop_all":
            if not is_admin(uid):
                return
            stopped = 0
            failed = 0
            for owner_uid in list(user_bot_applications.keys()):
                try:
                    await stop_user_bot(owner_uid)
                    stopped += 1
                except Exception as ex:
                    logging.error(f"stop_all error {owner_uid}: {ex}")
                    failed += 1
            result_text = f"🛑 Stopped: {stopped}\n‼️ Failed: {failed}"
            await q.edit_message_text(result_text, parse_mode=ParseMode.HTML, reply_markup=admin_kb())

        elif data == "admin_broadcast":
            await q.edit_message_text("✈️ Choose broadcast target:",
                                      parse_mode=ParseMode.HTML,
                                      reply_markup=InlineKeyboardMarkup([
                                          [InlineKeyboardButton("🎯 Specific UserBot",
                                                                callback_data="admin_bcast_target_select")],
                                          [InlineKeyboardButton("🌐 All UserBots",
                                                                callback_data="admin_bcast_target_all")],
                                          [InlineKeyboardButton("🔙 Back", callback_data="admin_panel")],
                                      ]))

        elif data == "admin_bcast_target_all":
            context.user_data["admin_broadcast"] = True
            context.user_data["admin_broadcast_target"] = None
            await q.edit_message_text(
                "✈️ Send broadcast text/media for ALL userbots users.\n\n🔥 Use <b>HTML formatting</b> for rich text.\n\n💎 Premium emojis will be preserved and converted automatically when sending!",
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup(
                    [[InlineKeyboardButton("🔙 Back", callback_data="admin_panel")]]))

        elif data == "admin_bcast_target_select":
            bots = db.get_all_user_bots() or []
            if not bots:
                await q.edit_message_text("‼️ No userbots found.", parse_mode=ParseMode.HTML, reply_markup=admin_kb())
                return
            kb = []
            for b in bots:
                tuid, _, buser, active = b
                kb.append([InlineKeyboardButton(
                    f"@{buser} ({tuid})", callback_data=f"admin_bcast_pick_{tuid}")])
            kb.append([InlineKeyboardButton("🔙 Back", callback_data="admin_broadcast")])
            await q.edit_message_text("🤖 Select userbot:",
                                      parse_mode=ParseMode.HTML,
                                      reply_markup=InlineKeyboardMarkup(kb))

        elif data.startswith("admin_bcast_pick_"):
            tuid = int(data.split("_")[3])
            context.user_data["admin_broadcast"] = True
            context.user_data["admin_broadcast_target"] = tuid
            await q.edit_message_text(
                f"✈️ Send broadcast text/media for userbot owner: {tuid}\n\n🔥 Use <b>HTML formatting</b> for rich text.\n\n💎 Premium emojis will be preserved and converted automatically when sending!",
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup(
                    [[InlineKeyboardButton("🔙 Back", callback_data="admin_panel")]]))

        elif data == "remove_my_bot_confirm":
            await remove_my_bot(q, uid)

        elif data == "admin_bcast_add_btns":
            draft = context.user_data.get("admin_broadcast_draft", {})
            if not draft:
                await q.edit_message_text("‼️ No draft found.", parse_mode=ParseMode.HTML, reply_markup=admin_kb())
            else:
                context.user_data["admin_broadcast"] = True
                context.user_data["admin_broadcast_stage"] = "await_buttons"
                await q.edit_message_text(
                    "🔽 Send inline buttons lines (Text|https://link).",
                    parse_mode=ParseMode.HTML,
                    reply_markup=InlineKeyboardMarkup(
                        [[InlineKeyboardButton("🔙 Back", callback_data="admin_panel")]]))

        elif data == "admin_bcast_send":
            await preview_admin_broadcast(q, context)

        elif data == "admin_bcast_confirm":
            await send_admin_broadcast(q, context)

        elif data == "admin_send_reminders":
            if not is_admin(uid):
                return
            sent_count = 0
            for days_threshold in [3, 1]:
                expiring = db.get_expiring_subscriptions(days_threshold)
                for sub in expiring:
                    sub_uid = sub["user_id"]
                    sub_type = sub["subscription_type"]
                    try:
                        expiry_dt = datetime.fromisoformat(sub["expiry_date"])
                        days_left = (expiry_dt - datetime.now()).days
                        msg_text = (
                            f"🔔 Subscription Expiry Reminder\n\n"
                            f"⭐️ Plan: {sub_type}\n"
                            f"‼️ Expires in {days_left} day(s) "
                            f"({expiry_dt.strftime('%d %b %Y')}).\n\n"
                            f"📞 Contact {ADMIN_USERNAME} to renew."
                        )
                        await context.bot.send_message(
                            sub_uid, msg_text, parse_mode=ParseMode.HTML,
                            reply_markup=InlineKeyboardMarkup([[
                                InlineKeyboardButton(
                                    "🔄 Renew",
                                    url=f"https://t.me/{ADMIN_USERNAME.lstrip('@')}"
                                )
                            ]])
                        )
                        db.mark_reminder_sent(sub_uid, days_threshold)
                        sent_count += 1
                    except Exception as ex:
                        logging.error(f"Manual reminder failed for {sub_uid}: {ex}")
            await q.edit_message_text(
                f"✅ Reminders sent to {sent_count} users.",
                parse_mode=ParseMode.HTML,
                reply_markup=admin_kb()
            )

    except Exception as ex:
        logging.error(f"Error in callback_handler: {ex}")
        if "Message is not modified" not in str(ex):
            try:
                await q.edit_message_text(f"‼️ Error: {str(ex)}", parse_mode=ParseMode.HTML, reply_markup=main_menu_kb(uid))
            except Exception:
                pass


async def show_subscription(q, uid):
    sub = db.get_subscription(uid)
    if not sub:
        await q.edit_message_text("‼️ No active subscription.", parse_mode=ParseMode.HTML, reply_markup=subscription_kb())
        return
    try:
        expiry = datetime.fromisoformat(sub[2])
        days = (expiry - datetime.now()).days
        status = "✅ Active" if days > 0 else "❌ Expired"
        sub_text = (
            f"👑 Subscription Details\n\n"
            f"⭐️ Type: {sub[1]}\n"
            f"💎 Max channels: {sub[3]}\n"
            f"🔔 Expiry: {expiry.date()} ({days} days)\n"
            f"🔘 Status: {status}"
        )
        await q.edit_message_text(sub_text, parse_mode=ParseMode.HTML, reply_markup=subscription_kb())
    except Exception:
        await q.edit_message_text("‼️ Subscription data error.", parse_mode=ParseMode.HTML, reply_markup=subscription_kb())


async def handle_sub_purchase(q, data):
    if data == "sub_basic":
        await q.edit_message_text(
            "💰 Selected Basic Rs.2599/mo (1 channel).\n📞 Contact @aviii566 to pay.",
            parse_mode=ParseMode.HTML,
            reply_markup=subscription_kb())
    elif data == "sub_pro":
        await q.edit_message_text(
            "💎 Selected Pro Rs.3999/mo (5 channels).\n📞 Contact @aviii566 to pay.",
            parse_mode=ParseMode.HTML,
            reply_markup=subscription_kb())
    elif data == "sub_renew":
        await q.edit_message_text(
            "📞 Contact @aviii566 to renew.",
            parse_mode=ParseMode.HTML,
            reply_markup=subscription_kb())


async def show_all_users(q):
    users = db.get_all_users()
    lines = [f"📇 Users ({len(users)}):\n"]
    for u in users:
        uid, uname, fname = u[0], u[1], u[2]
        sub = u[5] or "None"
        exp = u[6] or ""
        lines.append(
            f"• {uid} @{uname or 'N/A'} {fname or ''} — {sub} {exp[:10] if exp else ''}")
    text = "\n".join(lines)
    await q.edit_message_text(text[:4000], parse_mode=ParseMode.HTML, reply_markup=admin_kb())


async def check_expiry(q):
    users = db.get_all_users()
    now = datetime.now()
    expiring = []
    for u in users:
        if u[6]:
            try:
                exp = datetime.fromisoformat(u[6])
                days = (exp - now).days
                if days <= 7:
                    expiring.append(f"@{u[1] or 'N/A'} — {days} days left")
            except Exception:
                pass
    text = f"🔔 Expiring within 7 days:\n" + ("\n".join(expiring) if expiring else "None")
    await q.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=admin_kb())


async def show_stats(q):
    users = db.get_all_users()
    total_users = len(users)
    active_subs = sum(1 for u in users if u[5])
    running = len(user_bot_applications)
    total_bots = len(db.get_all_user_bots())
    counts = db.get_userbot_user_counts()
    per = "\n".join([f"@{b or 'N/A'} users:{c}" for _, b, c in counts]) or "(none)"
    total_userbot_users = sum(c for _, _, c in counts)
    text = (
        f"📊 Stats\n\n"
        f"📇 Total users: {total_users}\n"
        f"⭐️ Active subs: {active_subs}\n"
        f"🤖 Userbots (total): {total_bots}\n"
        f"🔘 Userbots running: {running}\n\n"
        f"✈️ Per userbot users:\n{per}\n\n"
        f"🔥 Total userbot users: {total_userbot_users}"
    )
    await q.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=admin_kb())


async def start_all_userbots(q):
    bots = db.get_all_user_bots()
    started = 0; failed = 0
    for uid, token, buser, active in bots:
        sub = db.get_subscription(uid)
        if not sub:
            logging.info(f"Skipping {uid} - no subscription")
            continue
        try:
            expiry = datetime.fromisoformat(sub[2])
            if expiry < datetime.now():
                logging.info(f"Skipping {uid} - subscription expired")
                continue
        except Exception as ex:
            logging.error(f"Error checking expiry for {uid}: {ex}")
            continue
        if uid in user_bot_applications:
            logging.info(f"Bot for {uid} already running")
            continue
        try:
            await start_user_bot(token, uid)
            db.set_user_bot_active(uid, True)
            started += 1
            logging.info(f"✅ Started user bot @{buser} for user {uid}")
        except Exception as ex:
            logging.error(f"start_all error {uid}: {ex}")
            failed += 1
    result_text = f"✅ Started: {started}\n‼️ Failed: {failed}"
    await q.edit_message_text(result_text, parse_mode=ParseMode.HTML, reply_markup=admin_kb())


async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not user:
        return
    msg = update.message
    if not msg:
        return

    if context.user_data.get("admin_add_userbot") and is_admin(user.id):
        text_content = msg.text or ""
        parts = text_content.split()
        if len(parts) >= 2:
            try:
                target_id = int(parts[0])
                token = parts[1]
                test_bot = Bot(token=token, request=None)
                me = await test_bot.get_me()

                sub = db.get_subscription(target_id)
                if not sub:
                    await msg.reply_text(
                        f"‼️ User {target_id} has no subscription. Add subscription first.",
                        parse_mode=ParseMode.HTML,
                        reply_markup=admin_kb())
                    context.user_data.pop("admin_add_userbot", None)
                    return

                expiry = datetime.fromisoformat(sub[2])
                if expiry < datetime.now():
                    await msg.reply_text(
                        f"‼️ User {target_id} subscription expired. Renew first.",
                        parse_mode=ParseMode.HTML,
                        reply_markup=admin_kb())
                    context.user_data.pop("admin_add_userbot", None)
                    return

                db.add_user(target_id, "", "", "")
                db.add_user_bot(target_id, token, me.username)
                await start_user_bot(token, target_id)
                db.set_user_bot_active(target_id, True)

                notice = (
                    "✅ USERBOT ACTIVATION CONFIRMATION\n\n"
                    "Your UserBot setup has been completed successfully and is now active.\n\n"
                    "Details:\n"
                    f"• Service: Advanced UserBot Setup\n"
                    f"• Plan: {sub[1]} (Active)\n"
                    f"• Bot: @{me.username}\n"
                    f"• Start: {datetime.utcnow().strftime('%d %B %Y')}\n"
                    f"• Expiry: {expiry.strftime('%d %B %Y')}\n"
                    "• Status: Active\n\n"
                    "Open your bot and click Start to begin.\n\n"
                    "💎 Premium emojis will be preserved exactly as you set them!"
                )
                try:
                    await test_bot.send_message(chat_id=target_id, text=notice, parse_mode=ParseMode.HTML)
                except Exception as e1:
                    logging.warning(f"userbot notice failed: {e1}")
                    try:
                        await context.bot.send_message(chat_id=target_id, text=notice, parse_mode=ParseMode.HTML)
                    except Exception as e2:
                        logging.warning(f"userbot notice fallback failed: {e2}")

                await msg.reply_text(
                    f"✅ UserBot added for user {target_id}\n🤖 Bot: @{me.username}\n⭐️ Subscription: {sub[1]}",
                    parse_mode=ParseMode.HTML,
                    reply_markup=admin_kb())
            except ValueError:
                await msg.reply_text("‼️ Invalid user ID format", parse_mode=ParseMode.HTML, reply_markup=admin_kb())
            except Exception as ex:
                await msg.reply_text(f"‼️ Error: {str(ex)}", parse_mode=ParseMode.HTML, reply_markup=admin_kb())
        else:
            await msg.reply_text("‼️ Format: user_id bot_token", parse_mode=ParseMode.HTML, reply_markup=admin_kb())
        context.user_data.pop("admin_add_userbot", None)
        return

    if context.user_data.get("waiting_token"):
        token = (msg.text or "").strip()
        try:
            test_bot = Bot(token=token, request=None)
            me = await test_bot.get_me()

            sub = db.get_subscription(user.id)
            if not sub:
                await msg.reply_text(
                    "‼️ You don't have an active subscription. Contact @aviii566 to purchase.",
                    parse_mode=ParseMode.HTML)
                context.user_data.pop("waiting_token", None)
                return

            expiry = datetime.fromisoformat(sub[2])
            if expiry < datetime.now():
                await msg.reply_text(
                    "‼️ Your subscription has expired. Contact @aviii566 to renew.",
                    parse_mode=ParseMode.HTML)
                context.user_data.pop("waiting_token", None)
                return

            db.add_user_bot(user.id, token, me.username)
            await start_user_bot(token, user.id)
            db.set_user_bot_active(user.id, True)
            await msg.reply_text(f"✅ Bot set: @{me.username}\n\n💎 Premium emojis will be preserved exactly as you set them!", parse_mode=ParseMode.HTML, reply_markup=userbot_kb(user.id))
        except Exception as ex:
            await msg.reply_text(f"Invalid token: {ex}", parse_mode=ParseMode.HTML)
        finally:
            context.user_data.pop("waiting_token", None)
        return

    if context.user_data.get("admin_add_sub") and is_admin(user.id):
        parts = (msg.text or "").split()
        if len(parts) >= 3:
            try:
                target = int(parts[0])
                days = int(parts[1])
                plan = parts[2].capitalize()
                db.add_subscription(target, plan, days)

                bot_data = db.get_user_bot(target)
                bot_started = False
                if bot_data and bot_data[1] and bot_data[2]:
                    if target in user_bot_applications:
                        try:
                            await stop_user_bot(target)
                        except Exception as stop_ex:
                            logging.error(f"Stop before restart failed for {target}: {stop_ex}")

                    try:
                        await start_user_bot(bot_data[1], target)
                        db.set_user_bot_active(target, True)
                        bot_started = True
                        logging.info(f"✅ Auto-started bot for {target} after subscription add")
                    except Exception as start_ex:
                        logging.error(f"Auto-start failed for {target}: {start_ex}")

                try:
                    bot_notice = ""
                    if bot_data and bot_started:
                        bot_notice = f"\n🤖 Bot @{bot_data[2]} has been restarted automatically."
                    elif bot_data and not bot_started:
                        bot_notice = "\n⚠️ Bot restart failed. Please contact admin."
                    else:
                        bot_notice = "\n📌 Add your bot token via Setup Bot to activate."

                    await context.bot.send_message(
                        target,
                        f"✅ Your subscription has been renewed/activated!\n\n"
                        f"⭐️ Plan: {plan}\n"
                        f"📅 Duration: {days} days{bot_notice}\n\n"
                        f"Click /start to use your bot.\n\n"
                        f"💎 Premium emojis will be preserved exactly as you set them!",
                        parse_mode=ParseMode.HTML
                    )
                except Exception as e:
                    logging.error(f"Failed to notify user {target}: {e}")

                status_msg = f"✅ Added {plan} {days}d for {target}"
                if bot_data and bot_started:
                    status_msg += f"\n🚀 Bot @{bot_data[2]} auto-started!"
                elif bot_data:
                    status_msg += f"\n⚠️ Bot auto-start failed — check logs."
                else:
                    status_msg += "\n📌 No bot configured for this user yet."

                await msg.reply_text(
                    status_msg,
                    parse_mode=ParseMode.HTML,
                    reply_markup=admin_kb())
            except Exception as ex:
                await msg.reply_text(f"‼️ Error: {ex}", parse_mode=ParseMode.HTML, reply_markup=admin_kb())
        else:
            await msg.reply_text("‼️ Format: user_id days Plan", parse_mode=ParseMode.HTML, reply_markup=admin_kb())
        context.user_data.pop("admin_add_sub", None)
        return

    if context.user_data.get("admin_broadcast_stage") == "await_buttons" and is_admin(user.id):
        draft = context.user_data.get("admin_broadcast_draft", {})
        btn_json = buttons_json_from_text(msg.text or "")
        if btn_json:
            draft["buttons_json"] = btn_json
            context.user_data["admin_broadcast_draft"] = draft
            await msg.reply_text(
                "✅ Buttons saved. Review preview and send?",
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🚀 Send Now", callback_data="admin_bcast_send")],
                    [InlineKeyboardButton("❌ Cancel", callback_data="admin_panel")],
                ]))
        else:
            await msg.reply_text(
                "‼️ No valid buttons (use Text|https://link).",
                parse_mode=ParseMode.HTML,
                reply_markup=admin_kb())
        context.user_data["admin_broadcast_stage"] = "await_send"
        return

    if context.user_data.get("admin_broadcast") and is_admin(user.id):
        media_id, media_type, text, entities = extract_media_from_msg(msg)
        # Store the raw text exactly as received
        processed_text = text
        draft = {
            "text": processed_text, "media": media_id, "media_type": media_type,
            "target_uid": context.user_data.get("admin_broadcast_target"),
        }
        context.user_data["admin_broadcast_draft"] = draft
        context.user_data["admin_broadcast_stage"] = "await_buttons"
        await msg.reply_text(
            "✅ Broadcast draft saved. Add inline buttons or send now?",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("➕ Add Inline Buttons",
                                      callback_data="admin_bcast_add_btns")],
                [InlineKeyboardButton("🚀 Send Now", callback_data="admin_bcast_send")],
                [InlineKeyboardButton("❌ Cancel", callback_data="admin_panel")],
            ]))
        return

    if is_admin(user.id) and msg.reply_to_message:
        target_uid = _get_support_uid(SUPPORT_REPLY_MAP, msg.reply_to_message.message_id)
        if target_uid:
            try:
                await context.bot.copy_message(
                    chat_id=target_uid, from_chat_id=msg.chat_id,
                    message_id=msg.message_id)
                if user.id != ADMIN_USER_ID:
                    try:
                        await context.bot.send_message(
                            ADMIN_USER_ID,
                            f"↪️ Admin {user.id} replied to user {target_uid}",
                            parse_mode=ParseMode.HTML)
                        await context.bot.copy_message(
                            chat_id=ADMIN_USER_ID, from_chat_id=msg.chat_id,
                            message_id=msg.message_id)
                    except Exception as mirror_err:
                        logging.error(f"main support mirror failed: {mirror_err}")
                await send_ephemeral_reply(msg, f"✅ Sent to user {target_uid}", 2)
            except Exception as ex:
                await msg.reply_text(f"❌ Failed to send to user {target_uid}: {ex}", parse_mode=ParseMode.HTML)
            return

    if not is_admin(user.id):
        try:
            delivered = 0
            for admin_id in ADMIN_USER_IDS:
                try:
                    relayed = await context.bot.forward_message(
                        chat_id=admin_id, from_chat_id=msg.chat_id,
                        message_id=msg.message_id)
                    _store_support_map(SUPPORT_REPLY_MAP, relayed.message_id, user.id)
                    delivered += 1
                except Exception as e_admin:
                    logging.error(
                        f"support relay to admin {admin_id} failed for {user.id}: {e_admin}")
            if delivered > 0:
                await send_ephemeral_reply(
                    msg, "✅ Message sent to support. You will get a reply here.", 2)
            else:
                await msg.reply_text("⚠️ Support is temporarily unavailable. Please try again.", parse_mode=ParseMode.HTML)
        except Exception as ex:
            logging.error(f"support relay failed for {user.id}: {ex}")
            await msg.reply_text("⚠️ Could not send to support right now. Please try again.", parse_mode=ParseMode.HTML)
        return

    await msg.reply_text("🔽 Use menu.", parse_mode=ParseMode.HTML, reply_markup=main_menu_kb(user.id))


async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    err_str = str(context.error)
    logging.error(f"Update {update} caused error {err_str}")
    if "Message is not modified" in err_str or "Query is too old" in err_str:
        return
    try:
        if update and hasattr(update, 'effective_chat') and update.effective_chat:
            await context.bot.send_message(
                update.effective_chat.id,
                "❌ An error occurred. Please try again or contact admin.",
                parse_mode=ParseMode.HTML
            )
    except Exception:
        pass


async def preview_user_broadcast(q, context: ContextTypes.DEFAULT_TYPE):
    draft = context.user_data.get("broadcast_draft", {})
    uid = q.from_user.id
    if not draft:
        await q.edit_message_text("‼️ No draft found.", parse_mode=ParseMode.HTML, reply_markup=userbot_kb(uid))
        return
    broadcast_text = draft.get("text", "")
    buttons = buttons_to_markup(draft.get("buttons_json")) if draft.get("buttons_json") else None
    try:
        await send_media(context, uid, draft.get("media"), draft.get("media_type") or "text",
                         broadcast_text, buttons)
    except Exception as ex:
        logging.error(f"Preview send error: {ex}")
        await q.edit_message_text(f"‼️ Preview failed: {str(ex)}", parse_mode=ParseMode.HTML, reply_markup=userbot_kb(uid))
        return
    await q.edit_message_text(
        "✅ Preview sent above. Confirm to send broadcast?",
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("✅ Confirm Send", callback_data="bcast_confirm")],
            [InlineKeyboardButton("❌ Cancel", callback_data="main_menu")],
        ]),
    )


async def send_user_broadcast(q, context: ContextTypes.DEFAULT_TYPE):
    uid = q.from_user.id
    managed_uid = get_managed_user_id(context, uid)
    draft = context.user_data.get("broadcast_draft", {})
    context.user_data.pop("broadcast_stage", None)
    if not draft:
        await q.edit_message_text("‼️ No draft to send.", parse_mode=ParseMode.HTML, reply_markup=userbot_kb(managed_uid))
        return
    reqs = db.get_requesters_for_owner(managed_uid)
    if not reqs:
        await q.edit_message_text("‼️ No users to broadcast.", parse_mode=ParseMode.HTML, reply_markup=userbot_kb(managed_uid))
        return
    broadcast_text = draft.get("text", "")
    buttons = buttons_to_markup(draft.get("buttons_json")) if draft.get("buttons_json") else None
    await q.edit_message_text("✈️ Broadcasting...",
                              parse_mode=ParseMode.HTML,
                              reply_markup=InlineKeyboardMarkup(
                                  [[InlineKeyboardButton("🔙 Back", callback_data="main_menu")]]))
    sent = 0; fail = 0
    for r in reqs:
        try:
            await send_media(context, r, draft.get("media"), draft.get("media_type") or "text",
                             broadcast_text, buttons)
            db.mark_reachable(managed_uid, r)
            sent += 1
        except Exception as ex:
            emsg = str(ex).lower()
            if ("bot was blocked by the user" in emsg
                    or "bot can't initiate conversation with a user" in emsg):
                db.mark_unreachable(managed_uid, r)
            logging.error(f"broadcast to {r}: {ex}")
            fail += 1
        if (sent + fail) % 30 == 0:
            try:
                await q.message.edit_text(
                    f"✈️ Broadcasting... Sent: {sent}, Failed: {fail}",
                    parse_mode=ParseMode.HTML)
            except Exception:
                pass
    try:
        await q.message.edit_text(
            f"✅ Broadcast Complete!\n\n🟢 Sent: {sent}\n‼️ Failed: {fail}",
            parse_mode=ParseMode.HTML,
            reply_markup=userbot_kb(managed_uid)
        )
    except Exception:
        pass
    context.user_data.pop("broadcast_draft", None)


async def preview_admin_broadcast(q, context: ContextTypes.DEFAULT_TYPE):
    draft = context.user_data.get("admin_broadcast_draft", {})
    if not draft:
        await q.edit_message_text("‼️ No draft found.", parse_mode=ParseMode.HTML, reply_markup=admin_kb())
        return
    broadcast_text = draft.get("text", "")
    btns = buttons_to_markup(draft.get("buttons_json")) if draft.get("buttons_json") else None
    uid = q.from_user.id
    try:
        await send_media(context, uid, draft.get("media"), draft.get("media_type") or "text",
                         broadcast_text, btns)
    except Exception as ex:
        logging.error(f"Admin preview send error: {ex}")
        await q.edit_message_text(f"‼️ Preview failed: {str(ex)}", parse_mode=ParseMode.HTML, reply_markup=admin_kb())
        return
    target_uid = draft.get("target_uid")
    target_label = f"userbot {target_uid}" if target_uid else "ALL userbots"
    await q.edit_message_text(
        f"✅ Preview sent above.\n✈️ Confirm broadcast to {target_label}?",
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("✅ Confirm Send", callback_data="admin_bcast_confirm")],
            [InlineKeyboardButton("❌ Cancel", callback_data="admin_panel")],
        ]),
    )


async def send_admin_broadcast(q, context: ContextTypes.DEFAULT_TYPE):
    draft = context.user_data.get("admin_broadcast_draft", {})
    for key in ["admin_broadcast", "admin_broadcast_stage", "admin_broadcast_target"]:
        context.user_data.pop(key, None)
    if not draft:
        await q.edit_message_text("‼️ No draft to send.", parse_mode=ParseMode.HTML, reply_markup=admin_kb())
        return
    bots = db.get_all_user_bots()
    if draft.get("target_uid"):
        bots = [b for b in bots if b[0] == draft["target_uid"]]
    broadcast_text = draft.get("text", "")
    btns = buttons_to_markup(draft.get("buttons_json")) if draft.get("buttons_json") else None
    await q.edit_message_text("✈️ Admin broadcast started...", parse_mode=ParseMode.HTML, reply_markup=admin_kb())
    total_sent = 0; total_fail = 0

    for uid, token, buser, active in bots:
        sub = db.get_subscription(uid)
        if not sub:
            continue
        try:
            expiry = datetime.fromisoformat(sub[2])
            if expiry < datetime.now():
                continue
        except Exception:
            continue

        if uid in user_bot_applications:
            bot_instance = user_bot_applications[uid].bot
        else:
            bot_instance = Bot(token=token, request=None)

        recipients = db.get_requesters_for_owner(uid)
        for r in recipients:
            try:
                await send_media(bot_instance, r, draft.get("media"),
                                 draft.get("media_type") or "text", broadcast_text, btns)
                db.mark_reachable(uid, r)
                total_sent += 1
            except Exception as ex:
                emsg = str(ex).lower()
                if ("bot was blocked by the user" in emsg
                        or "bot can't initiate conversation with a user" in emsg):
                    db.mark_unreachable(uid, r)
                logging.error(f"admin broadcast via @{buser} to {r}: {ex}")
                total_fail += 1
            if (total_sent + total_fail) % 50 == 0:
                try:
                    await q.message.edit_text(
                        f"✈️ Admin broadcast progress: Sent {total_sent}, Failed {total_fail}",
                        parse_mode=ParseMode.HTML)
                except Exception:
                    pass

    target_label = (f"to userbot {draft['target_uid']}"
                    if draft.get("target_uid") else "to ALL userbots")
    try:
        await q.message.edit_text(
            f"✅ Admin broadcast complete {target_label}!\n\n📊 Statistics:\n• Sent: {total_sent}\n• Failed: {total_fail}",
            parse_mode=ParseMode.HTML,
            reply_markup=admin_kb()
        )
    except Exception:
        pass
    context.user_data.pop("admin_broadcast_draft", None)


async def handle_channel_delete_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q:
        return
    try:
        await q.answer()
    except Exception:
        pass
    data = q.data or ''
    if not (data.startswith('del_ch_') or data.startswith('rm_ch_')):
        return
    uid = q.from_user.id
    managed_uid = get_managed_user_id(context, uid)
    raw = data.split('_', 2)[-1]
    removed = False
    try:
        if db.remove_channel(managed_uid, int(raw)):
            removed = True
    except Exception:
        pass
    channels = db.get_user_channels(managed_uid) or []
    kb = [[InlineKeyboardButton(f"❌ {ch[2]}", callback_data=f"del_ch_{ch[1]}")]
          for ch in channels]
    kb.append([InlineKeyboardButton("🔙 Back", callback_data="my_channels")])
    txt = f"✅ Channel removed successfully." if removed else f"‼️ Channel remove failed: {raw}"
    try:
        await q.edit_message_text(txt, parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup(kb))
    except Exception:
        await q.message.reply_text(txt, parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup(kb))


async def main():
    logging.basicConfig(
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        level=logging.INFO
    )
    logging.info("🚀 Starting bot system...")
    logging.info(f"💎 Premium emojis loaded: {len(PREMIUM_EMOJI_MAP)}")

    # Stop bots for expired subscriptions on startup
    expired_users = db.get_expired_subscriptions()
    for user_id in expired_users:
        bot_data = db.get_user_bot(user_id)
        if bot_data and bot_data[3] == 1:
            if user_id in user_bot_applications:
                try:
                    app = user_bot_applications[user_id]
                    await app.updater.stop()
                    await app.stop()
                    await app.shutdown()
                    logging.info(f"🛑 Stopped expired bot on startup: {user_id}")
                except Exception as ex:
                    logging.error(f"Error stopping expired bot {user_id}: {ex}")
                user_bot_applications.pop(user_id, None)
            db.set_user_bot_active(user_id, False)

    # Start all valid bots
    bots = db.get_all_user_bots()
    if bots:
        logging.info(f"Found {len(bots)} user bots to start")
        for uid, token, buser, active in bots:
            sub = db.get_subscription(uid)
            if not sub:
                logging.info(f"Skipping {uid} - no subscription")
                db.set_user_bot_active(uid, False)
                continue
            try:
                expiry = datetime.fromisoformat(sub[2])
                if expiry < datetime.now():
                    logging.info(f"Skipping {uid} - subscription expired")
                    db.set_user_bot_active(uid, False)
                    continue
            except Exception as ex:
                logging.error(f"Error checking expiry for {uid}: {ex}")
                continue
            try:
                await start_user_bot(token, uid)
                db.set_user_bot_active(uid, True)
                logging.info(f"✅ Started user bot @{buser} for user {uid}")
            except Exception as ex:
                logging.error(f"❌ Failed to start user bot {uid}: {ex}")
    else:
        logging.info("No user bots found in database")

    # Main bot setup
    app = ApplicationBuilder().token(MAIN_BOT_TOKEN).concurrent_updates(True).connect_timeout(30).read_timeout(30).write_timeout(30).pool_timeout(30).build()

    app.add_handler(CommandHandler("start", start_command))
    app.add_handler(CommandHandler("admin", admin_command))
    app.add_handler(CommandHandler("proof", proof_text_command))
    app.add_handler(CommandHandler("prooftext", proof_text_command))
    app.add_handler(CallbackQueryHandler(callback_handler))

    app.add_handler(MessageHandler(
        ~filters.COMMAND & (filters.TEXT | filters.PHOTO | filters.VIDEO |
                            filters.Document.ALL | filters.AUDIO | filters.VOICE |
                            filters.ANIMATION | filters.Sticker.ALL),
        handle_message
    ))
    app.add_error_handler(error_handler)

    # Schedule jobs
    app.job_queue.run_repeating(
        subscription_reminder_job,
        interval=43200,
        first=60,
        name="subscription_reminders"
    )
    app.job_queue.run_repeating(
        check_expired_subscriptions_job,
        interval=3600,
        first=120,
        name="expired_subscriptions_check"
    )

    await app.initialize()
    await app.start()
    await app.updater.start_polling()
    logging.info("✅ Main bot started successfully")
    logging.info("🔔 Subscription reminder job scheduled (every 12 hours)")
    logging.info("🕐 Expired subscription check job scheduled (every 1 hour)")
    logging.info(f"💎 Premium emoji support active with {len(PREMIUM_EMOJI_MAP)} emojis")

    try:
        await asyncio.Event().wait()
    except KeyboardInterrupt:
        logging.info("🛑 Stopping bots...")
        for owner_uid, user_app in user_bot_applications.items():
            try:
                await user_app.updater.stop()
                await user_app.stop()
                await user_app.shutdown()
                logging.info(f"✅ Stopped user bot {owner_uid}")
            except Exception:
                pass
        logging.info("✅ All bots stopped")


if __name__ == "__main__":
    while True:
        try:
            asyncio.run(main())
        except KeyboardInterrupt:
            logging.info("🛑 Stopped by user")
            break
        except Exception as ex:
            logging.error(f"❌ Fatal error: {ex}")
            logging.info("🔄 Restarting in 10 seconds...")
            time.sleep(10)