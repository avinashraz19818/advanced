import asyncio
import logging
import json
import time
import os
import re
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
from telegram.error import BadRequest

# ================= CONFIG =================
MAIN_BOT_TOKEN = os.getenv("MAIN_BOT_TOKEN", "7687421668:AAF3h5GTbqBUVZoXe6OExF5NV3dUwjy5WoU")
ADMIN_USER_ID = 8089603563
ADMIN_USERNAME = "@aviii566"
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


# ================= DATABASE =================
MONGO_URI = os.getenv("MONGO_URI", "mongodb+srv://avinash:avinash12@cluster0.wnwd1fv.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")
MONGO_DB_NAME = "advanced_bot"

from pymongo import MongoClient, ReturnDocument
import certifi


# ================= DATABASE CLASS =================
class Database:
    def __init__(self):
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
                "counters", "user_emoji_maps"
            ]
            for col in needed:
                if col not in collections:
                    self.mongo.create_collection(col)

            self.mongo.users.create_index("user_id", unique=True)
            self.mongo.user_bots.create_index([("user_id", 1), ("bot_id", 1)], unique=True)
            self.mongo.user_bots.create_index("bot_token", unique=True)
            self.mongo.subscriptions.create_index([("user_id", 1), ("bot_id", 1)], unique=True)
            self.mongo.user_bot_channels.create_index([("user_id", 1), ("bot_id", 1), ("channel_id", 1)], unique=True)
            self.mongo.user_bot_messages.create_index([("bot_id", 1), ("channel_id", 1), ("_id", 1)])
            self.mongo.user_bot_messages.create_index("media_group_id")
            self.mongo.join_requests.create_index(
                [("owner_user_id", 1), ("bot_id", 1), ("requester_id", 1), ("channel_id", 1)], unique=True)
            self.mongo.join_requests.create_index("status")
            self.mongo.join_requests.create_index("request_date")
            self.mongo.reachable_users.create_index(
                [("owner_user_id", 1), ("bot_id", 1), ("requester_id", 1)], unique=True)
            self.mongo.reachable_users.create_index("last_ok_at")
            self.mongo.user_emoji_maps.create_index([("user_id", 1), ("msg_id", 1)], unique=True)
            logging.info("✅ MongoDB indexes created successfully")
        except Exception as ex:
            logging.error(f"❌ Error creating indexes: {ex}")

    # ---- User CRUD ----
    def add_user(self, user_id, username, first_name, last_name):
        try:
            self.mongo.users.update_one(
                {"user_id": user_id},
                {"$set": {"username": username, "first_name": first_name, "last_name": last_name},
                 "$setOnInsert": {"created_at": datetime.utcnow().isoformat(), "verified": False}},
                upsert=True
            )
        except Exception as ex:
            logging.error(f"Error adding user {user_id}: {ex}")

    def mark_user_verified(self, user_id: int):
        try:
            self.mongo.users.update_one(
                {"user_id": user_id},
                {"$set": {"verified": True}}
            )
        except Exception as ex:
            logging.error(f"Error marking user verified {user_id}: {ex}")

    def is_user_verified(self, user_id: int) -> bool:
        try:
            u = self.mongo.users.find_one({"user_id": user_id})
            return bool(u and u.get("verified"))
        except Exception as ex:
            logging.error(f"Error checking verification {user_id}: {ex}")
            return False

    def get_all_users(self):
        users = []
        try:
            for user in self.mongo.users.find({}, {"_id": 0}):
                users.append((
                    user.get("user_id"), user.get("username"), user.get("first_name"),
                    user.get("last_name"), user.get("created_at")
                ))
        except Exception as ex:
            logging.error(f"Error getting all users: {ex}")
        return users

    # ---- UserBot CRUD (Multiple bots per user) ----
    def add_user_bot(self, user_id, bot_id, token, username):
        try:
            self.mongo.user_bots.update_one(
                {"user_id": user_id, "bot_id": bot_id},
                {"$set": {"bot_token": token, "bot_username": username, "is_active": 1},
                 "$setOnInsert": {"created_at": datetime.utcnow().isoformat()}},
                upsert=True
            )
        except Exception as ex:
            logging.error(f"Error adding user bot for {user_id}: {ex}")

    def get_user_bot(self, user_id, bot_id):
        try:
            bot = self.mongo.user_bots.find_one({"user_id": user_id, "bot_id": bot_id})
            if bot:
                return (bot.get("user_id"), bot.get("bot_id"), bot.get("bot_token"), 
                        bot.get("bot_username"), bot.get("is_active", 0), bot.get("created_at"))
        except Exception as ex:
            logging.error(f"Error getting user bot for {user_id}: {ex}")
        return None

    def get_user_bots(self, user_id):
        bots = []
        try:
            for bot in self.mongo.user_bots.find({"user_id": user_id}):
                bots.append((bot.get("user_id"), bot.get("bot_id"), bot.get("bot_token"),
                              bot.get("bot_username"), bot.get("is_active", 0)))
        except Exception as ex:
            logging.error(f"Error getting user bots for {user_id}: {ex}")
        return bots

    def get_all_user_bots(self):
        bots = []
        try:
            for bot in self.mongo.user_bots.find({}):
                bots.append((bot.get("user_id"), bot.get("bot_id"), bot.get("bot_token"),
                              bot.get("bot_username"), bot.get("is_active", 0)))
        except Exception as ex:
            logging.error(f"Error getting all user bots: {ex}")
        return bots

    def set_user_bot_active(self, user_id, bot_id, active):
        try:
            self.mongo.user_bots.update_one(
                {"user_id": user_id, "bot_id": bot_id},
                {"$set": {"is_active": 1 if active else 0}}
            )
        except Exception as ex:
            logging.error(f"Error setting bot active for {user_id}: {ex}")

    def remove_user_bot(self, user_id, bot_id):
        try:
            # Remove all related data
            self.mongo.user_bots.delete_many({"user_id": user_id, "bot_id": bot_id})
            self.mongo.subscriptions.delete_many({"user_id": user_id, "bot_id": bot_id})
            self.mongo.user_bot_channels.delete_many({"user_id": user_id, "bot_id": bot_id})
            self.mongo.user_bot_messages.delete_many({"user_id": user_id, "bot_id": bot_id})
            self.mongo.join_requests.delete_many({"owner_user_id": user_id, "bot_id": bot_id})
            self.mongo.reachable_users.delete_many({"owner_user_id": user_id, "bot_id": bot_id})
        except Exception as ex:
            logging.error(f"Error removing user bot for {user_id}: {ex}")

    def remove_all_user_bots(self, user_id):
        try:
            bots = self.get_user_bots(user_id)
            for bot in bots:
                self.remove_user_bot(user_id, bot[1])
        except Exception as ex:
            logging.error(f"Error removing all user bots for {user_id}: {ex}")

    # ---- Subscription CRUD (Per bot) ----
    def get_subscription(self, user_id, bot_id=None):
        try:
            query = {"user_id": user_id}
            if bot_id is not None:
                query["bot_id"] = bot_id
            sub = self.mongo.subscriptions.find_one(query)
            if sub:
                return (sub.get("user_id"), sub.get("bot_id"), sub.get("subscription_type"),
                        sub.get("expiry_date"), sub.get("max_channels"), sub.get("created_at"))
        except Exception as ex:
            logging.error(f"Error getting subscription for {user_id}: {ex}")
        return None

    def get_user_subscriptions(self, user_id):
        subs = []
        try:
            for sub in self.mongo.subscriptions.find({"user_id": user_id}):
                subs.append((sub.get("user_id"), sub.get("bot_id"), sub.get("subscription_type"),
                             sub.get("expiry_date"), sub.get("max_channels")))
        except Exception as ex:
            logging.error(f"Error getting user subscriptions: {ex}")
        return subs

    def get_all_subscriptions(self):
        results = []
        try:
            for sub in self.mongo.subscriptions.find({}):
                user = self.mongo.users.find_one({"user_id": sub.get("user_id")}) or {}
                bot = self.mongo.user_bots.find_one({"user_id": sub.get("user_id"), "bot_id": sub.get("bot_id")}) or {}
                results.append({
                    "user_id": sub.get("user_id"),
                    "bot_id": sub.get("bot_id"),
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

    def add_subscription(self, user_id, bot_id, sub_type, days):
        try:
            expiry = (datetime.now() + timedelta(days=days)).isoformat()
            max_channels = 1 if sub_type.lower() == "basic" else 5
            self.mongo.subscriptions.update_one(
                {"user_id": user_id, "bot_id": bot_id},
                {"$set": {"subscription_type": sub_type, "expiry_date": expiry,
                          "max_channels": max_channels, "reminder_3d_sent": False,
                          "reminder_1d_sent": False},
                 "$setOnInsert": {"created_at": datetime.utcnow().isoformat()}},
                upsert=True
            )
        except Exception as ex:
            logging.error(f"Error adding subscription for {user_id}: {ex}")

    def remove_subscription(self, user_id, bot_id):
        try:
            self.mongo.subscriptions.delete_one({"user_id": user_id, "bot_id": bot_id})
        except Exception as ex:
            logging.error(f"Error removing subscription for {user_id}: {ex}")

    def update_subscription_expiry(self, user_id, bot_id, new_expiry):
        try:
            self.mongo.subscriptions.update_one(
                {"user_id": user_id, "bot_id": bot_id},
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
                    "bot_id": sub.get("bot_id"),
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
            for sub in self.mongo.subscriptions.find({"expiry_date": {"$lt": now}}):
                results.append((sub.get("user_id"), sub.get("bot_id")))
        except Exception as ex:
            logging.error(f"Error get_expired_subscriptions: {ex}")
        return results

    def mark_reminder_sent(self, user_id, bot_id, days: int):
        try:
            self.mongo.subscriptions.update_one(
                {"user_id": user_id, "bot_id": bot_id},
                {"$set": {f"reminder_{days}d_sent": True}}
            )
        except Exception as ex:
            logging.error(f"Error mark_reminder_sent: {ex}")

    # ---- Channel CRUD (Per bot) ----
    def add_channel(self, user_id, bot_id, channel_id, username, title):
        try:
            self.mongo.user_bot_channels.update_one(
                {"user_id": user_id, "bot_id": bot_id, "channel_id": channel_id},
                {"$set": {"channel_username": username, "channel_title": title},
                 "$setOnInsert": {"created_at": datetime.utcnow().isoformat(), "auto_approve": 1}},
                upsert=True
            )
        except Exception as ex:
            logging.error(f"Error adding channel for {user_id}: {ex}")

    def get_user_channels(self, user_id, bot_id):
        channels = []
        try:
            for ch in self.mongo.user_bot_channels.find({"user_id": user_id, "bot_id": bot_id}).sort("channel_id", 1):
                channels.append((
                    ch.get("user_id"), ch.get("bot_id"), ch.get("channel_id"), ch.get("channel_username"),
                    ch.get("channel_title"), ch.get("welcome_message"), ch.get("welcome_media_id"),
                    ch.get("welcome_media_type"), int(ch.get("auto_approve", 1))
                ))
        except Exception as ex:
            logging.error(f"Error getting user channels for {user_id}: {ex}")
        return channels

    def set_auto_approve(self, user_id, bot_id, channel_id, val):
        try:
            self.mongo.user_bot_channels.update_one(
                {"user_id": user_id, "bot_id": bot_id, "channel_id": channel_id},
                {"$set": {"auto_approve": 1 if val else 0}}
            )
        except Exception as ex:
            logging.error(f"Error setting auto approve: {ex}")

    def get_channel_owner_data(self, channel_id):
        try:
            ch = self.mongo.user_bot_channels.find_one({"channel_id": channel_id})
            if ch:
                return (ch.get("user_id"), ch.get("bot_id"), ch.get("channel_id"), ch.get("channel_username"),
                        ch.get("channel_title"), ch.get("welcome_message"), ch.get("welcome_media_id"),
                        ch.get("welcome_media_type"), int(ch.get("auto_approve", 1)))
        except Exception as ex:
            logging.error(f"Error getting channel owner data for {channel_id}: {ex}")
        return None

    def clear_messages(self, user_id, bot_id, channel_id):
        try:
            self.mongo.user_bot_messages.delete_many({"user_id": user_id, "bot_id": bot_id, "channel_id": channel_id})
            self.mongo.user_bot_channels.update_one(
                {"user_id": user_id, "bot_id": bot_id, "channel_id": channel_id},
                {"$set": {"welcome_message": None, "welcome_media_id": None, "welcome_media_type": None}}
            )
        except Exception as ex:
            logging.error(f"Error clearing messages: {ex}")

    def remove_channel(self, user_id, bot_id, channel_id):
        try:
            self.mongo.user_bot_messages.delete_many({"user_id": user_id, "bot_id": bot_id, "channel_id": channel_id})
            self.mongo.user_bot_channels.delete_many({"user_id": user_id, "bot_id": bot_id, "channel_id": channel_id})
            self.mongo.join_requests.delete_many({"owner_user_id": user_id, "bot_id": bot_id, "channel_id": channel_id})
            return True
        except Exception as ex:
            logging.error(f"Error removing channel: {ex}")
            return False

    # ---- Message CRUD (Per bot) ----
    def add_message(self, user_id, bot_id, channel_id, text, media_id, media_type,
                    media_group_id=None, entities_json=None):
        try:
            msg_id = self._next_id("user_bot_messages")
            self.mongo.user_bot_messages.insert_one({
                "_id": msg_id, "user_id": user_id, "bot_id": bot_id, "channel_id": channel_id,
                "content_text": text, "media_id": media_id, "media_type": media_type,
                "media_group_id": media_group_id, "buttons_json": None,
                "entities_json": entities_json,
                "created_at": datetime.utcnow().isoformat()
            })
            existing_msgs = list(self.mongo.user_bot_messages.find(
                {"user_id": user_id, "bot_id": bot_id, "channel_id": channel_id}))
            if len(existing_msgs) == 1:
                self.mongo.user_bot_channels.update_one(
                    {"user_id": user_id, "bot_id": bot_id, "channel_id": channel_id},
                    {"$set": {"welcome_message": text,
                              "welcome_media_id": media_id,
                              "welcome_media_type": media_type}}
                )
            return msg_id
        except Exception as ex:
            logging.error(f"Error adding message: {ex}")
            return 0

    def update_message_text(self, msg_id, text, entities_json=None):
        try:
            row = self.get_message_by_id(msg_id)
            if row:
                user_id = row[1]
                bot_id = row[2]
                update_doc = {"content_text": text}
                if entities_json is not None:
                    update_doc["entities_json"] = entities_json
                self.mongo.user_bot_messages.update_one(
                    {"_id": int(msg_id)}, {"$set": update_doc})
                channel_id = row[3]
                msgs = list(self.mongo.user_bot_messages.find(
                    {"user_id": user_id, "bot_id": bot_id, "channel_id": channel_id}).sort("_id", 1))
                if msgs and msgs[0]["_id"] == msg_id:
                    self.mongo.user_bot_channels.update_one(
                        {"user_id": user_id, "bot_id": bot_id, "channel_id": channel_id},
                        {"$set": {"welcome_message": text}}
                    )
        except Exception as ex:
            logging.error(f"Error updating message text: {ex}")

    def update_message_media(self, msg_id, media_id, media_type, text=None, entities_json=None):
        try:
            update = {"media_id": media_id, "media_type": media_type}
            row = self.get_message_by_id(msg_id)
            if text is not None and row:
                update["content_text"] = text
            if entities_json is not None:
                update["entities_json"] = entities_json
            self.mongo.user_bot_messages.update_one({"_id": int(msg_id)}, {"$set": update})
            if row:
                user_id = row[1]
                bot_id = row[2]
                channel_id = row[3]
                msgs = list(self.mongo.user_bot_messages.find(
                    {"user_id": user_id, "bot_id": bot_id, "channel_id": channel_id}).sort("_id", 1))
                if msgs and msgs[0]["_id"] == msg_id:
                    self.mongo.user_bot_channels.update_one(
                        {"user_id": user_id, "bot_id": bot_id, "channel_id": channel_id},
                        {"$set": {"welcome_media_id": media_id, "welcome_media_type": media_type}}
                    )
        except Exception as ex:
            logging.error(f"Error updating message media: {ex}")

    def delete_message(self, msg_id):
        try:
            row = self.get_message_by_id(msg_id)
            if row:
                user_id = row[1]
                bot_id = row[2]
                channel_id = row[3]
                self.mongo.user_bot_messages.delete_one({"_id": int(msg_id)})
                self.mongo.user_emoji_maps.delete_many({"msg_id": int(msg_id)})
                remaining = list(self.mongo.user_bot_messages.find(
                    {"user_id": user_id, "bot_id": bot_id, "channel_id": channel_id}).sort("_id", 1))
                if remaining:
                    first = remaining[0]
                    self.mongo.user_bot_channels.update_one(
                        {"user_id": user_id, "bot_id": bot_id, "channel_id": channel_id},
                        {"$set": {"welcome_message": first.get("content_text"),
                                  "welcome_media_id": first.get("media_id"),
                                  "welcome_media_type": first.get("media_type")}}
                    )
                else:
                    self.mongo.user_bot_channels.update_one(
                        {"user_id": user_id, "bot_id": bot_id, "channel_id": channel_id},
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
                bot_id = msgs[0].get("bot_id")
                channel_id = msgs[0].get("channel_id")
                msg_ids = [m["_id"] for m in msgs]
                self.mongo.user_bot_messages.delete_many({"media_group_id": media_group_id})
                self.mongo.user_emoji_maps.delete_many({"msg_id": {"$in": msg_ids}})
                remaining = list(self.mongo.user_bot_messages.find(
                    {"user_id": user_id, "bot_id": bot_id, "channel_id": channel_id}).sort("_id", 1))
                if remaining:
                    first = remaining[0]
                    self.mongo.user_bot_channels.update_one(
                        {"user_id": user_id, "bot_id": bot_id, "channel_id": channel_id},
                        {"$set": {"welcome_message": first.get("content_text"),
                                  "welcome_media_id": first.get("media_id"),
                                  "welcome_media_type": first.get("media_type")}}
                    )
                else:
                    self.mongo.user_bot_channels.update_one(
                        {"user_id": user_id, "bot_id": bot_id, "channel_id": channel_id},
                        {"$set": {"welcome_message": None,
                                  "welcome_media_id": None,
                                  "welcome_media_type": None}}
                    )
        except Exception as ex:
            logging.error(f"Error deleting media group: {ex}")

    def get_message_count(self, user_id, bot_id, channel_id):
        try:
            return self.mongo.user_bot_messages.count_documents({"user_id": user_id, "bot_id": bot_id, "channel_id": channel_id})
        except Exception as ex:
            logging.error(f"Error getting message count: {ex}")
            return 0

    def get_messages(self, user_id, bot_id, channel_id):
        messages = []
        try:
            for msg in self.mongo.user_bot_messages.find(
                    {"user_id": user_id, "bot_id": bot_id, "channel_id": channel_id}).sort("_id", 1):
                messages.append((
                    msg.get("_id"), msg.get("content_text"), msg.get("media_id"),
                    msg.get("media_type"), msg.get("media_group_id"), msg.get("buttons_json"),
                    msg.get("entities_json")
                ))
        except Exception as ex:
            logging.error(f"Error getting messages: {ex}")
        return messages

    def get_message_by_id(self, msg_id):
        try:
            msg = self.mongo.user_bot_messages.find_one({"_id": int(msg_id)})
            if msg:
                return (msg.get("_id"), msg.get("user_id"), msg.get("bot_id"), msg.get("channel_id"),
                        msg.get("content_text"), msg.get("media_id"), msg.get("media_type"),
                        msg.get("media_group_id"), msg.get("buttons_json"),
                        msg.get("entities_json"))
        except Exception as ex:
            logging.error(f"Error getting message by ID: {ex}")
        return None

    def update_message_buttons(self, msg_id, buttons_json):
        try:
            self.mongo.user_bot_messages.update_one(
                {"_id": int(msg_id)}, {"$set": {"buttons_json": buttons_json}})
        except Exception as ex:
            logging.error(f"Error updating message buttons: {ex}")

    # ---- Per-User Emoji Map CRUD ----
    def save_user_emoji_map(self, user_id: int, msg_id: int, emoji_map: dict):
        try:
            if not emoji_map:
                return
            self.mongo.user_emoji_maps.update_one(
                {"user_id": user_id, "msg_id": msg_id},
                {"$set": {"emoji_map": emoji_map, "updated_at": datetime.utcnow().isoformat()}},
                upsert=True
            )
        except Exception as ex:
            logging.error(f"Error saving emoji map for msg {msg_id}: {ex}")

    def get_user_emoji_map(self, user_id: int, msg_id: int) -> dict:
        try:
            doc = self.mongo.user_emoji_maps.find_one({"user_id": user_id, "msg_id": msg_id})
            if doc:
                return doc.get("emoji_map", {})
        except Exception as ex:
            logging.error(f"Error getting emoji map for msg {msg_id}: {ex}")
        return {}

    def delete_user_emoji_map(self, msg_id: int):
        try:
            self.mongo.user_emoji_maps.delete_many({"msg_id": msg_id})
        except Exception as ex:
            logging.error(f"Error deleting emoji map for msg {msg_id}: {ex}")

    # ---- Join Request CRUD (Per bot) ----
    def add_join_request(self, owner_user_id, bot_id, requester_id, channel_id, status):
        try:
            existing = self.mongo.join_requests.find_one({
                "owner_user_id": owner_user_id,
                "bot_id": bot_id,
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
                    "bot_id": bot_id, "requester_id": requester_id, "channel_id": channel_id,
                    "status": status, "request_date": datetime.utcnow().isoformat(),
                    "approved_date": None
                })
        except Exception as ex:
            logging.error(f"Error adding join request: {ex}")

    def get_pending_requests(self, owner_user_id, bot_id):
        requests = []
        try:
            for req in self.mongo.join_requests.find(
                    {"owner_user_id": owner_user_id, "bot_id": bot_id, "status": "pending"}):
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

    def get_pending_count(self, owner_user_id, bot_id):
        try:
            return self.mongo.join_requests.count_documents(
                {"owner_user_id": owner_user_id, "bot_id": bot_id, "status": "pending"})
        except Exception as ex:
            logging.error(f"Error getting pending count: {ex}")
            return 0

    def mark_reachable(self, owner_user_id, bot_id, requester_id):
        try:
            self.mongo.reachable_users.update_one(
                {"owner_user_id": owner_user_id, "bot_id": bot_id, "requester_id": requester_id},
                {"$set": {"last_ok_at": datetime.utcnow().isoformat()}},
                upsert=True
            )
        except Exception as ex:
            logging.error(f"Error marking reachable: {ex}")

    def mark_unreachable(self, owner_user_id, bot_id, requester_id):
        try:
            self.mongo.reachable_users.delete_one(
                {"owner_user_id": owner_user_id, "bot_id": bot_id, "requester_id": requester_id})
        except Exception as ex:
            logging.error(f"Error marking unreachable: {ex}")

    def get_requesters_for_owner(self, owner_user_id, bot_id):
        try:
            reachable = list(self.mongo.reachable_users.find(
                {"owner_user_id": owner_user_id, "bot_id": bot_id}).sort("last_ok_at", -1))
            if reachable:
                return [r.get("requester_id") for r in reachable if r.get("requester_id")]
            requests = self.mongo.join_requests.distinct(
                "requester_id", {"owner_user_id": owner_user_id, "bot_id": bot_id, "status": "approved"})
            return [r for r in requests if r]
        except Exception as ex:
            logging.error(f"Error getting requesters for owner: {ex}")
            return []

    def get_total_requesters_count(self, owner_user_id, bot_id):
        try:
            return len(self.mongo.join_requests.distinct(
                "requester_id", {"owner_user_id": owner_user_id, "bot_id": bot_id}))
        except Exception as ex:
            logging.error(f"Error getting total requesters count: {ex}")
            return 0

    def get_reachable_requesters_count(self, owner_user_id, bot_id):
        try:
            return len(self.mongo.reachable_users.distinct(
                "requester_id", {"owner_user_id": owner_user_id, "bot_id": bot_id}))
        except Exception as ex:
            logging.error(f"Error getting reachable requesters count: {ex}")
            return 0

    def get_userbot_user_counts(self):
        rows = []
        try:
            for bot in self.mongo.user_bots.find({}, {"_id": 0}):
                users = len(self.mongo.join_requests.distinct(
                    "requester_id", {"owner_user_id": bot.get("user_id"), "bot_id": bot.get("bot_id")}))
                rows.append((bot.get("user_id"), bot.get("bot_id"), bot.get("bot_username"), users))
        except Exception as ex:
            logging.error(f"Error getting userbot user counts: {ex}")
        return rows



# ================= GLOBALS =================
db = Database()
# Key: (user_id, bot_id) -> Application
user_bot_applications: Dict[Tuple[int, int], Application] = {}


# ================= EMOJI MANAGER (Per-User) =================
class EmojiManager:
    """
    Handles per-user premium emoji extraction, storage, and reconstruction.
    Each user has their own emoji map stored per-message in MongoDB.
    NO global emoji mapping is used.
    Uses single-pass UTF-16 entity rendering to avoid cascading replacement bugs.
    """

    @staticmethod
    def extract_from_entities(text: str, entities: Optional[List]) -> dict:
        """
        Extract custom emoji IDs from message entities.
        Returns {emoji_char: emoji_id} per this specific message.
        """
        if not entities or not text:
            return {}
        emoji_map = {}
        # Build UTF-16 position map for correct slicing
        char_to_utf16 = []
        utf16_pos = 0
        for ch in text:
            char_to_utf16.append(utf16_pos)
            utf16_pos += len(ch.encode('utf-16-le')) // 2

        for entity in entities:
            if entity.type == "custom_emoji" and entity.custom_emoji_id:
                try:
                    # Find which python chars correspond to this utf16 offset+length range
                    start_utf16 = entity.offset
                    end_utf16 = entity.offset + entity.length
                    # Find python char indices
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
        """Serialize message entities to JSON string for storage."""
        if not entities:
            return None
        try:
            serialized = []
            for e in entities:
                d = {
                    "type": e.type,
                    "offset": e.offset,
                    "length": e.length,
                }
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
        """
        Single-pass UTF-16 aware entity-to-HTML renderer.
        Handles bold, italic, custom_emoji, links, code, etc.
        Never does cascading string replacement — processes each character once.
        """
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

        # Build UTF-16 position map: python char index -> utf16 code unit start
        char_to_utf16: List[int] = []
        utf16_pos = 0
        for ch in text:
            char_to_utf16.append(utf16_pos)
            utf16_pos += len(ch.encode('utf-16-le')) // 2
        total_utf16 = utf16_pos

        # Build open/close event maps keyed on UTF-16 code unit position
        opens: Dict[int, List[str]] = {}
        closes: Dict[int, List[str]] = {}

        for e in sorted(entities,
                        key=lambda x: (x.get("offset", 0), -(x.get("length", 0)))):
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

        # Single pass: iterate python chars, emit tags at correct UTF-16 boundaries
        result = []
        for i, ch in enumerate(text):
            u16 = char_to_utf16[i]

            # Emit close tags that end at this position (reverse order for proper nesting)
            if u16 in closes:
                for ct in reversed(closes[u16]):
                    result.append(ct)

            # Emit open tags that start at this position
            if u16 in opens:
                for ot in opens[u16]:
                    result.append(ot)

            # HTML-escape the character itself
            if ch == '<':
                result.append('&lt;')
            elif ch == '>':
                result.append('&gt;')
            elif ch == '&':
                result.append('&amp;')
            else:
                result.append(ch)

        # Emit any close tags that end at the very end of the string
        if total_utf16 in closes:
            for ct in reversed(closes[total_utf16]):
                result.append(ct)

        return "".join(result)

    @staticmethod
    def _html_escape(text: str) -> str:
        return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


# ================= MESSAGE MANAGER =================
class MessageManager:
    """
    Handles exact message storage and reconstruction.
    Stores raw text + entities together, reconstructs on send.
    NEVER modifies user content during storage.
    """

    @staticmethod
    def extract_from_message(msg) -> dict:
        """
        Extract all relevant fields from a Telegram message.
        Returns a dict with raw text, entities, media info.
        """
        text = msg.text or msg.caption or ""
        entities = list(msg.entities or msg.caption_entities or [])
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

        emoji_map = EmojiManager.extract_from_entities(text, entities)
        entities_json = EmojiManager.entities_to_json(entities)

        return {
            "text": text,
            "entities": entities,
            "entities_json": entities_json,
            "emoji_map": emoji_map,
            "media_id": media_id,
            "media_type": media_type,
            "media_group_id": getattr(msg, "media_group_id", None),
        }

    @staticmethod
    def prepare_for_sending(text: str, entities_json: Optional[str] = None,
                            emoji_map: Optional[dict] = None) -> str:
        """
        Prepare text for sending to end users.
        Priority: entities_json (single-pass renderer) > emoji_map fallback > plain text.
        """
        if not text:
            return ""
        if entities_json:
            return EmojiManager.render_entities_html(text, entities_json)
        return EmojiManager._html_escape(text)


# ================= PREMIUM EMOJI UI HELPER =================
# Known Telegram premium animated emoji IDs used in bot UI messages
_UI_PE: dict = {
    "💎": "5042050649248760772",
    "⭐️": "5042176294222037888",
    "⚡️": "5042334757040423886",
    "👑": "5039727497143387500",
    "✅": "5039844895779455925",
    "❌": "5040042498634810056",
    "‼️": "5042003580702164014",
    "🔔": "5042111805288089118",
    "📊": "5042290883949495533",
    "💬": "5040036030414062506",
    "🔄": "5041837837914211014",
    "🎉": "5039778134807806727",
    "🔍": "5039649904264217620",
    "👀": "5039623284056917259",
    "💫": "5042200814190330758",
    "✨": "5040016479722931047",
    "🛡": "5042328396193864923",
    "🔴": "5042042652019655612",
    "🟢": "5039928501612839813",
    "📌": "5039600026809009149",
    "📣": "5041888071851705019",
    "🗑": "5039614900280754969",
    "🔝": "5042102141611672423",
    "💰": "5039789890133296083",
    "💙": "5039560388555834382",
    "👍": "5039544445637231745",
    "👎": "5042067236412458007",
    "🚫": "5039671744172917707",
    "⚠️": "5039665997506675838",
    "🔗": "5042101437237036298",
    "🧠": "5040030395416969985",
    "💡": "5039660273953853888",
}
_UI_PE.update({
    "⭐": _UI_PE["⭐️"],
    "⚡": _UI_PE["⚡️"],
    "‼": _UI_PE["‼️"],
    "⚠": _UI_PE["⚠️"],
    "🗑️": _UI_PE["🗑"],
})


def pe(emoji: str) -> str:
    """Wrap a UI emoji with its premium <tg-emoji> tag if ID is known, else return as-is."""
    eid = _UI_PE.get(emoji)
    if eid:
        return f'<tg-emoji emoji-id="{eid}">{emoji}</tg-emoji>'
    return emoji


def premiumize_ui_emojis(text: Optional[str]) -> str:
    if not text:
        return ""
    if "<tg-emoji" not in text:
        result = text
        for emoji in sorted(_UI_PE.keys(), key=len, reverse=True):
            result = result.replace(emoji, pe(emoji))
        return result

    parts = re.split(r'(<tg-emoji\b[^>]*>.*?</tg-emoji>)', text)
    output = []
    for part in parts:
        if part.startswith("<tg-emoji"):
            output.append(part)
            continue
        for emoji in sorted(_UI_PE.keys(), key=len, reverse=True):
            part = part.replace(emoji, pe(emoji))
        output.append(part)
    return "".join(output)


def premiumize_if_html(text: Optional[str], kwargs: dict) -> str:
    raw_text = text or ""
    parse_mode = kwargs.get("parse_mode")
    if parse_mode == ParseMode.HTML or parse_mode == "HTML":
        return premiumize_ui_emojis(raw_text)
    if any(emoji in raw_text for emoji in _UI_PE.keys()):
        kwargs["parse_mode"] = ParseMode.HTML
        return premiumize_ui_emojis(EmojiManager._html_escape(raw_text))
    return raw_text


class UIFormatter:
    """
    Formats all bot messages with premium UI design.
    Uses blockquotes, bold headings, premium emojis, clean spacing.
    """

    @staticmethod
    def main_menu(user_name: str = "") -> str:
        name_part = f" <b>{user_name}</b>" if user_name else ""
        return (
            f"<blockquote>{pe('💎')} <b>WELCOME{name_part}</b></blockquote>\n\n"
            f"<b>Your Premium Bot Panel</b>\n\n"
            f"{pe('💎')} Manage your bots\n"
            f"{pe('⚡️')} Set welcome messages\n"
            f"{pe('📊')} Track your users\n"
            f"{pe('📣')} Broadcast to audience\n\n"
            f"<i>Select an option below to get started:</i>"
        )

    @staticmethod
    def verification_prompt() -> str:
        return (
            f"<blockquote>🔐 <b>HUMAN VERIFICATION REQUIRED</b></blockquote>\n\n"
            f"To ensure you are a real person and protect our system,\n"
            f"please complete the quick verification below.\n\n"
            f"<i>Click the button to verify and access your panel.</i>"
        )

    @staticmethod
    def verification_success(first_name: str) -> str:
        return (
            f"<blockquote>{pe('✅')} <b>VERIFICATION COMPLETE</b></blockquote>\n\n"
            f"Welcome, <b>{first_name}</b>! {pe('🎉')}\n\n"
            f"You now have access to the full bot panel.\n"
            f"<i>Use the buttons below to get started:</i>"
        )

    @staticmethod
    def subscription_required() -> str:
        return (
            f"<blockquote>{pe('💎')} <b>SUBSCRIPTION REQUIRED</b></blockquote>\n\n"
            f"You need an active subscription to access this feature.\n\n"
            f"<b>Available Plans:</b>\n"
            f"• {pe('💰')} <b>Basic</b> — ₹2599/mo — 1 channel\n"
            f"• {pe('⚡️')} <b>Pro</b> — ₹3999/mo — 5 channels\n\n"
            f"Contact {ADMIN_USERNAME} to subscribe."
        )

    @staticmethod
    def subscription_details(sub_type: str, expiry: datetime, days: int, max_ch: int, bot_username: str = "") -> str:
        status = f"{pe('✅')} Active" if days > 0 else f"{pe('❌')} Expired"
        bot_line = f"\n{pe('🤖')} <b>Bot:</b> @{bot_username}" if bot_username else ""
        return (
            f"<blockquote>{pe('👑')} <b>YOUR SUBSCRIPTION</b></blockquote>\n\n"
            f"{pe('⭐️')} <b>Plan:</b> {sub_type}\n"
            f"{pe('💎')} <b>Max Channels:</b> {max_ch}"
            f"{bot_line}\n"
            f"📅 <b>Expiry:</b> {expiry.strftime('%d %b %Y')}\n"
            f"⏳ <b>Days Remaining:</b> {days}\n"
            f"🔘 <b>Status:</b> {status}"
        )

    @staticmethod
    def bot_stats(channels: int, total_users: int, reachable: int, pending: int) -> str:
        return (
            f"<blockquote>{pe('📊')} <b>BOT STATISTICS</b></blockquote>\n\n"
            f"{pe('📣')} <b>Channels:</b> {channels}\n"
            f"{pe('👀')} <b>Total Users:</b> {total_users}\n"
            f"{pe('✅')} <b>Reachable Users:</b> {reachable}\n"
            f"{pe('🔔')} <b>Pending Requests:</b> {pending}"
        )

    @staticmethod
    def live_chat_header() -> str:
        return (
            f"<blockquote>{pe('💬')} <b>LIVE CHAT SUPPORT</b></blockquote>\n\n"
            f"You are now connected to support.\n"
            f"Please type your message and our team will respond shortly."
        )

    @staticmethod
    def broadcast_confirm(sent: int, failed: int) -> str:
        return (
            f"<blockquote>{pe('✅')} <b>BROADCAST COMPLETE</b></blockquote>\n\n"
            f"📤 <b>Sent:</b> {sent}\n"
            f"{pe('❌')} <b>Failed:</b> {failed}"
        )

    @staticmethod
    def expiry_reminder_3d(sub_type: str, expiry: datetime, days: int) -> str:
        return (
            f"<blockquote>{pe('🔔')} <b>SUBSCRIPTION EXPIRY REMINDER</b></blockquote>\n\n"
            f"{pe('⭐️')} <b>Plan:</b> {sub_type}\n"
            f"📅 <b>Expires on:</b> {expiry.strftime('%d %b %Y')}\n"
            f"⏳ <b>Days left:</b> {days}\n\n"
            f"🔥 Renew now to avoid any service interruption!\n"
            f"📞 Contact {ADMIN_USERNAME} to renew."
        )

    @staticmethod
    def expiry_reminder_1d(sub_type: str, expiry: datetime) -> str:
        return (
            f"<blockquote>{pe('‼️')} <b>LAST DAY REMINDER</b></blockquote>\n\n"
            f"{pe('⭐️')} <b>Plan:</b> {sub_type}\n"
            f"📅 <b>Expires TOMORROW:</b> {expiry.strftime('%d %b %Y')}\n\n"
            f"🚨 <b>Renew immediately</b> to keep your bot running!\n"
            f"📞 Contact {ADMIN_USERNAME} <b>NOW</b>."
        )

    @staticmethod
    def subscription_expired() -> str:
        return (
            f"<blockquote>{pe('❌')} <b>SUBSCRIPTION EXPIRED</b></blockquote>\n\n"
            f"Your bot has been paused due to subscription expiry.\n\n"
            f"📞 Contact {ADMIN_USERNAME} to renew and reactivate your bot."
        )


# ================= USER FLOW MANAGER =================
class UserFlowManager:
    """Manages the human verification start flow and user registration."""

    @staticmethod
    def needs_verification(user_id: int) -> bool:
        return not db.is_user_verified(user_id)

    @staticmethod
    def verification_button() -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup([
            [InlineKeyboardButton("👉 Verify Now", callback_data="human_verify")]
        ])


# ================= HELPER FUNCTIONS =================
def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_USER_IDS


def get_managed_user_id(context: ContextTypes.DEFAULT_TYPE, requester_uid: int) -> int:
    owner_uid = context.application.bot_data.get("owner_id")
    if requester_uid in ADMIN_USER_IDS and owner_uid:
        return owner_uid
    return requester_uid


def get_current_bot_id(context: ContextTypes.DEFAULT_TYPE) -> int:
    return context.application.bot_data.get("bot_id", 0)


async def safe_edit_message_text(q, *args, **kwargs):
    try:
        args = list(args)
        if args:
            args[0] = premiumize_if_html(args[0], kwargs)
        elif "text" in kwargs:
            kwargs["text"] = premiumize_if_html(kwargs.get("text"), kwargs)
        return await q.edit_message_text(*args, **kwargs)
    except BadRequest as ex:
        if "Message is not modified" in str(ex):
            try:
                await q.answer()
            except Exception:
                pass
            return None
        raise


async def send_premium_message(bot, chat_id, text, *args, **kwargs):
    text = premiumize_if_html(text, kwargs)
    return await bot.send_message(chat_id, text, *args, **kwargs)


async def reply_premium_message(message, text, *args, **kwargs):
    text = premiumize_if_html(text, kwargs)
    return await message.reply_text(text, *args, **kwargs)


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
        sent = await reply_premium_message(msg, text, parse_mode=ParseMode.HTML)
        await asyncio.sleep(seconds)
        try:
            await sent.delete()
        except Exception:
            pass
    except Exception:
        pass


async def send_media(bot_or_context, chat_id: int, media_id, media_type: str,
                     text: str = "", markup=None, emoji_map: dict = None,
                     entities_json: Optional[str] = None):
    """
    Send media/text to user.
    Uses entities_json for single-pass rendering (premium emojis, bold, etc).
    Falls back to plain text if neither is available.
    """
    bot = bot_or_context if isinstance(bot_or_context, Bot) else bot_or_context.bot
    kwargs = {}
    if markup:
        kwargs["reply_markup"] = markup

    # Render text with proper entity formatting
    if text:
        display_text = MessageManager.prepare_for_sending(text, entities_json, emoji_map)
        display_text = premiumize_ui_emojis(display_text)
    else:
        display_text = None

    try:
        if media_type == "photo":
            await bot.send_photo(chat_id, media_id,
                                 caption=display_text or None,
                                 parse_mode=ParseMode.HTML if display_text else None,
                                 **kwargs)
        elif media_type == "video":
            await bot.send_video(chat_id, media_id,
                                 caption=display_text or None,
                                 parse_mode=ParseMode.HTML if display_text else None,
                                 **kwargs)
        elif media_type == "document":
            await bot.send_document(chat_id, media_id,
                                    caption=display_text or None,
                                    parse_mode=ParseMode.HTML if display_text else None,
                                    **kwargs)
        elif media_type == "animation":
            await bot.send_animation(chat_id, media_id,
                                     caption=display_text or None,
                                     parse_mode=ParseMode.HTML if display_text else None,
                                     **kwargs)
        elif media_type == "audio":
            await bot.send_audio(chat_id, media_id,
                                 caption=display_text or None,
                                 parse_mode=ParseMode.HTML if display_text else None,
                                 **kwargs)
        elif media_type == "voice":
            await bot.send_voice(chat_id, media_id,
                                 caption=display_text or None,
                                 parse_mode=ParseMode.HTML if display_text else None,
                                 **kwargs)
        elif media_type == "sticker":
            await bot.send_sticker(chat_id, media_id, **kwargs)
        else:
            if display_text:
                await send_premium_message(bot, chat_id, display_text,
                                       parse_mode=ParseMode.HTML, **kwargs)
    except Exception as ex:
        logging.error(f"send_media failed chat={chat_id} type={media_type}: {ex}")
        # Fallback: try sending plain text only (strip all formatting)
        try:
            plain_text = (text or "").strip()
            if plain_text:
                fb_kwargs = {}
                if markup:
                    fb_kwargs["reply_markup"] = markup
                await send_premium_message(bot, chat_id, plain_text, **fb_kwargs)
        except Exception:
            pass


# ================= KEYBOARD BUILDERS =================
def main_menu_kb(uid: int) -> InlineKeyboardMarkup:
    lines = []
    user_bots = db.get_user_bots(uid)
    if user_bots:
        lines.append([InlineKeyboardButton("🤖 My Bots", callback_data="my_bots")])
        lines.append([InlineKeyboardButton("➕ Add New Bot", callback_data="add_new_bot")])
    else:
        lines.append([InlineKeyboardButton("➕ Add Your First Bot", callback_data="add_new_bot")])
    
    # Check if user has any active subscription
    has_active = False
    for bot in user_bots:
        sub = db.get_subscription(uid, bot[1])
        if sub:
            try:
                expiry = datetime.fromisoformat(sub[3])
                if expiry > datetime.now():
                    has_active = True
                    break
            except:
                pass
    
    if has_active:
        lines.append([InlineKeyboardButton("🔍 Manage Bots", callback_data="manage_bots")])
    else:
        lines.append([InlineKeyboardButton("💎 Get Subscription", callback_data="my_subscriptions")])
    
    lines.append([InlineKeyboardButton("📞 Contact Admin",
                                       url=f"https://t.me/{ADMIN_USERNAME.lstrip('@')}")])
    if is_admin(uid):
        lines.append([InlineKeyboardButton("👑 Admin Panel", callback_data="admin_panel")])
    return InlineKeyboardMarkup(lines)


def my_bots_kb(uid: int) -> InlineKeyboardMarkup:
    user_bots = db.get_user_bots(uid)
    kb = []
    for bot in user_bots:
        _, bot_id, _, username, active = bot
        sub = db.get_subscription(uid, bot_id)
        status_icon = "🟢" if active else "🔴"
        plan_icon = "⭐️" if sub else "❌"
        kb.append([InlineKeyboardButton(
            f"{status_icon} @{username} {plan_icon}",
            callback_data=f"select_bot_{bot_id}")])
    kb.append([InlineKeyboardButton("➕ Add New Bot", callback_data="add_new_bot")])
    kb.append([InlineKeyboardButton("🔙 Main Menu", callback_data="main_menu")])
    return InlineKeyboardMarkup(kb)


def bot_manage_kb(uid: int, bot_id: int, bot_username: str, has_active_sub: bool) -> InlineKeyboardMarkup:
    kb = []
    if has_active_sub:
        kb.append([InlineKeyboardButton("⚙️ Setup Bot", callback_data=f"setup_bot_{bot_id}")])
        kb.append([InlineKeyboardButton("📊 Bot Stats", callback_data=f"bot_stats_{bot_id}")])
        kb.append([InlineKeyboardButton("🔗 Open Bot", url=f"https://t.me/{bot_username}")])
    kb.append([InlineKeyboardButton(f"⭐️ Subscription Info", callback_data=f"bot_sub_{bot_id}")])
    kb.append([InlineKeyboardButton("🗑️ Remove Bot", callback_data=f"remove_bot_{bot_id}")])
    kb.append([InlineKeyboardButton("🔙 Back to My Bots", callback_data="my_bots")])
    kb.append([InlineKeyboardButton("🔙 Main Menu", callback_data="main_menu")])
    return InlineKeyboardMarkup(kb)


def subscriptions_list_kb(uid: int) -> InlineKeyboardMarkup:
    user_bots = db.get_user_bots(uid)
    kb = []
    for bot in user_bots:
        _, bot_id, _, username, _ = bot
        sub = db.get_subscription(uid, bot_id)
        if sub:
            try:
                expiry = datetime.fromisoformat(sub[3])
                days = (expiry - datetime.now()).days
                status = "🟢 Active" if days > 0 else "🔴 Expired"
                kb.append([InlineKeyboardButton(
                    f"@{username} - {sub[2]} ({status})",
                    callback_data=f"view_sub_{bot_id}")])
            except:
                kb.append([InlineKeyboardButton(f"@{username}", callback_data=f"view_sub_{bot_id}")])
        else:
            kb.append([InlineKeyboardButton(f"@{username} - No Subscription", callback_data=f"view_sub_{bot_id}")])
    kb.append([InlineKeyboardButton("➕ Add Subscription", callback_data="admin_add_sub_from_user")])
    kb.append([InlineKeyboardButton("🔙 Main Menu", callback_data="main_menu")])
    return InlineKeyboardMarkup(kb)


def subscription_kb(uid: int, bot_id: int = None) -> InlineKeyboardMarkup:
    back_cb = f"select_bot_{bot_id}" if bot_id else "my_subscriptions"
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("💰 Basic — ₹2599/mo (1 channel)", callback_data=f"sub_basic_{bot_id}" if bot_id else "sub_basic")],
        [InlineKeyboardButton("⚡️ Pro — ₹3999/mo (5 channels)", callback_data=f"sub_pro_{bot_id}" if bot_id else "sub_pro")],
        [InlineKeyboardButton("🔙 Back", callback_data=back_cb)],
    ])


def admin_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📇 All Users", callback_data="admin_all_users"),
         InlineKeyboardButton("🤖 Manage UserBots", callback_data="admin_userbots")],
        [InlineKeyboardButton("➕ Add UserBot", callback_data="admin_add_userbot"),
         InlineKeyboardButton("⭐️ Add/Remove Subscription", callback_data="admin_manage_subs")],
        [InlineKeyboardButton("📋 Subscription List", callback_data="admin_sub_list")],
        [InlineKeyboardButton("🔄 Check Expiry", callback_data="admin_check_expiry"),
         InlineKeyboardButton("📊 Stats", callback_data="admin_stats")],
        [InlineKeyboardButton("🚀 Start All Bots", callback_data="admin_start_all"),
         InlineKeyboardButton("🛑 Stop All Bots", callback_data="admin_stop_all")],
        [InlineKeyboardButton("✈️ Broadcast", callback_data="admin_broadcast"),
         InlineKeyboardButton("🔔 Send Reminders", callback_data="admin_send_reminders")],
        [InlineKeyboardButton("🔙 Main Menu", callback_data="main_menu")],
    ])


def admin_manage_subs_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("➕ Add Subscription", callback_data="admin_add_sub")],
        [InlineKeyboardButton("🗑️ Remove Subscription", callback_data="admin_remove_sub")],
        [InlineKeyboardButton("🔙 Back to Admin", callback_data="admin_panel")],
    ])


def userbot_kb(user_id: int, bot_id: int, bot_username: str) -> InlineKeyboardMarkup:
    sub = db.get_subscription(user_id, bot_id)
    lines = [
        [InlineKeyboardButton("✈️ Add Channel", callback_data=f"ub_add_channel_{bot_id}"),
         InlineKeyboardButton("📝 Set Message(s)", callback_data=f"ub_set_message_{bot_id}")],
        [InlineKeyboardButton("👀 Preview/Edit", callback_data=f"ub_manage_messages_{bot_id}"),
         InlineKeyboardButton("🗑️ Delete Msgs", callback_data=f"ub_delete_messages_{bot_id}")],
        [InlineKeyboardButton("❌ Remove Channel", callback_data=f"ub_remove_channel_{bot_id}"),
         InlineKeyboardButton("⚙️ Auto-Approve", callback_data=f"ub_toggle_auto_{bot_id}")],
        [InlineKeyboardButton("📋 My Channels", callback_data=f"ub_list_channels_{bot_id}"),
         InlineKeyboardButton("📈 Bot Stats", callback_data=f"ub_stats_{bot_id}")],
        [InlineKeyboardButton("📊 Pending", callback_data=f"ub_pending_requests_{bot_id}"),
         InlineKeyboardButton("✅ Accept All", callback_data=f"ub_accept_all_{bot_id}")],
        [InlineKeyboardButton("✈️ Broadcast to Users", callback_data=f"ub_broadcast_{bot_id}")],
    ]
    if sub:
        try:
            expiry = datetime.fromisoformat(sub[3])
            days_left = (expiry - datetime.now()).days
            lines.append([InlineKeyboardButton(
                f"📅 {sub[2]} — {days_left}d left", callback_data=f"ub_subscription_{bot_id}")])
        except Exception:
            pass
    lines.append([
        InlineKeyboardButton("📞 Contact Admin",
                             url=f"https://t.me/{ADMIN_USERNAME.lstrip('@')}"),
        InlineKeyboardButton("🔙 My Bots", callback_data="my_bots")
    ])
    return InlineKeyboardMarkup(lines)


# ================= SUBSCRIPTION JOBS =================
async def check_expired_subscriptions_job(context: ContextTypes.DEFAULT_TYPE):
    expired = db.get_expired_subscriptions()
    for user_id, bot_id in expired:
        bot_data = db.get_user_bot(user_id, bot_id)
        if bot_data and bot_data[4] == 1:
            key = (user_id, bot_id)
            if key in user_bot_applications:
                try:
                    app = user_bot_applications[key]
                    await app.updater.stop()
                    await app.stop()
                    await app.shutdown()
                    logging.info(f"🛑 Stopped expired bot: {user_id}/{bot_id}")
                except Exception as ex:
                    logging.error(f"Error stopping expired bot {user_id}/{bot_id}: {ex}")
                user_bot_applications.pop(key, None)
            db.set_user_bot_active(user_id, bot_id, False)
            try:
                await send_premium_message(context.bot, 
                    user_id,
                    UIFormatter.subscription_expired(),
                    parse_mode=ParseMode.HTML,
                    reply_markup=InlineKeyboardMarkup([[
                        InlineKeyboardButton("🔄 Renew Now",
                                             url=f"https://t.me/{ADMIN_USERNAME.lstrip('@')}")
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
            bot_id = sub["bot_id"]
            sub_type = sub["subscription_type"]
            try:
                expiry_dt = datetime.fromisoformat(sub["expiry_date"])
                days_left = (expiry_dt - datetime.now()).days
                if days_threshold == 3:
                    msg_text = UIFormatter.expiry_reminder_3d(sub_type, expiry_dt, days_left)
                else:
                    msg_text = UIFormatter.expiry_reminder_1d(sub_type, expiry_dt)
                try:
                    await send_premium_message(context.bot, 
                        user_id, msg_text, parse_mode=ParseMode.HTML,
                        reply_markup=InlineKeyboardMarkup([[
                            InlineKeyboardButton(
                                "🔄 Renew Subscription",
                                url=f"https://t.me/{ADMIN_USERNAME.lstrip('@')}"
                            )
                        ]])
                    )
                    db.mark_reminder_sent(user_id, bot_id, days_threshold)
                    logging.info(f"✅ Reminder sent to {user_id}/{bot_id} ({days_threshold}d)")
                except Exception as send_err:
                    logging.error(f"Reminder send failed for {user_id}/{bot_id}: {send_err}")
            except Exception as ex:
                logging.error(f"Reminder job error for {user_id}/{bot_id}: {ex}")


# ================= MESSAGE SENDING WITH EMOJI RECONSTRUCTION =================
async def _send_messages_with_media_groups(chat_id: int, msgs: List[tuple],
                                            context: ContextTypes.DEFAULT_TYPE,
                                            owner_uid: int = None,
                                            bot_id: int = None,
                                            attach_start_button: bool = True):
    """
    Send stored messages to a user, reconstructing premium emojis per-message.
    msgs tuple format: (id, text, media_id, media_type, media_group_id, buttons_json, entities_json)
    """
    if msgs and attach_start_button:
        try:
            msgs = list(msgs)
            last = msgs[-1]
            updated = add_callback_button_to_json(
                last[5] if len(last) > 5 else None,
                "💬 Live Chat Support", "live_chat_support"
            )
            msgs[-1] = last[:5] + (updated,) + last[6:]
        except Exception as ex:
            logging.error(f"attach live chat button failed: {ex}")

    i = 0
    while i < len(msgs):
        row = msgs[i]
        mid = row[0]
        text = row[1] if len(row) > 1 else ""
        media_id = row[2] if len(row) > 2 else None
        media_type = row[3] if len(row) > 3 else None
        media_group_id = row[4] if len(row) > 4 else None
        buttons_json = row[5] if len(row) > 5 else None
        entities_json = row[6] if len(row) > 6 else None

        if media_group_id:
            group_items = []
            group_buttons_json = None
            group_caption_text = None
            group_caption_entities_json = None
            j = i
            while j < len(msgs) and msgs[j][4] == media_group_id:
                g = msgs[j]
                g_mid = g[0]
                g_text = g[1] if len(g) > 1 else ""
                g_media_id = g[2] if len(g) > 2 else None
                g_media_type = g[3] if len(g) > 3 else None
                g_btnj = g[5] if len(g) > 5 else None
                g_ej = g[6] if len(g) > 6 else None

                if not group_buttons_json and g_btnj:
                    group_buttons_json = g_btnj
                if not group_caption_text and g_text:
                    group_caption_text = g_text
                    group_caption_entities_json = g_ej

                if g_media_id and g_media_type in ("photo", "video", "document", "audio"):
                    display_text = (MessageManager.prepare_for_sending(g_text, g_ej)
                                    if g_text else None)
                    pm = ParseMode.HTML if display_text else None
                    if g_media_type == "photo":
                        group_items.append(InputMediaPhoto(
                            media=g_media_id, caption=display_text or None, parse_mode=pm))
                    elif g_media_type == "video":
                        group_items.append(InputMediaVideo(
                            media=g_media_id, caption=display_text or None, parse_mode=pm))
                    elif g_media_type == "document":
                        group_items.append(InputMediaDocument(
                            media=g_media_id, caption=display_text or None, parse_mode=pm))
                    elif g_media_type == "audio":
                        group_items.append(InputMediaAudio(
                            media=g_media_id, caption=display_text or None, parse_mode=pm))
                else:
                    markup = buttons_to_markup(g_btnj)
                    await send_media(context, chat_id, g_media_id, g_media_type or "text",
                                     g_text or "", markup, entities_json=g_ej)
                j += 1

            if group_items:
                await context.bot.send_media_group(chat_id=chat_id, media=group_items)
                group_markup = buttons_to_markup(group_buttons_json)
                if group_markup:
                    display_caption = MessageManager.prepare_for_sending(
                        group_caption_text or "", group_caption_entities_json)
                    await send_premium_message(context.bot, 
                        chat_id,
                        display_caption or "Open links:",
                        parse_mode=ParseMode.HTML,
                        reply_markup=group_markup)
            i = j
            continue

        markup = buttons_to_markup(buttons_json)
        await send_media(context, chat_id, media_id, media_type or "text",
                         text or "", markup, entities_json=entities_json)
        i += 1


async def send_saved_welcome(owner_uid: int, bot_id: int, chat_id: int, context: ContextTypes.DEFAULT_TYPE):
    try:
        channels = db.get_user_channels(owner_uid, bot_id) or []
        if not channels:
            await send_premium_message(context.bot, chat_id,
                                           "Hello! Contact the channel owner for details.",
                                           parse_mode=ParseMode.HTML)
            return
        channel_id = channels[0][2]
        msgs = db.get_messages(owner_uid, bot_id, channel_id) or []
        if not msgs:
            ch_row = db.get_channel_owner_data(channel_id)
            welcome_text = ch_row[5] if ch_row else None
            if welcome_text:
                await send_premium_message(context.bot, 
                    chat_id, welcome_text, parse_mode=ParseMode.HTML)
            else:
                await send_premium_message(context.bot, chat_id,
                                               "Hello! Contact the channel owner for details.",
                                               parse_mode=ParseMode.HTML)
            return
        await _send_messages_with_media_groups(chat_id, msgs, context,
                                               owner_uid=owner_uid, bot_id=bot_id, attach_start_button=True)
    except Exception as ex:
        logging.error(f"send_saved_welcome error: {ex}")
        try:
            await send_premium_message(context.bot, chat_id,
                                           "Hello! Contact the channel owner for details.",
                                           parse_mode=ParseMode.HTML)
        except Exception:
            pass


def _runtime_store(context: ContextTypes.DEFAULT_TYPE, uid: int) -> Dict[str, Any]:
    store = context.application.bot_data.setdefault("runtime_store", {})
    if uid not in store:
        store[uid] = {}
    return store[uid]


import urllib.request
import urllib.parse


async def sync_pending_join_requests_for_channel(owner_uid: int, bot_id: int, channel_id: int, bot: Bot):
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
                db.add_join_request(owner_uid, bot_id, int(requester_id), channel_id, 'pending')
                synced += 1
        except Exception as ex:
            logging.error(f"pending sync item failed {channel_id}: {ex}")
    if synced:
        logging.info(f"Synced {synced} pending requests for owner={owner_uid} bot={bot_id} channel={channel_id}")


# ================= MAIN BOT COMMAND HANDLERS =================
async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not user:
        return

    # Always register user (but may not be "verified" yet)
    db.add_user(user.id, user.username, user.first_name, user.last_name)

    if is_admin(user.id):
        await send_premium_message(context.bot, 
            user.id,
            UIFormatter.main_menu(user.first_name),
            parse_mode=ParseMode.HTML,
            reply_markup=main_menu_kb(user.id)
        )
        return

    # Human verification flow
    if UserFlowManager.needs_verification(user.id):
        await send_premium_message(context.bot, 
            user.id,
            UIFormatter.verification_prompt(),
            parse_mode=ParseMode.HTML,
            reply_markup=UserFlowManager.verification_button()
        )
        return

    # Verified user — show main menu
    await send_premium_message(context.bot, 
        user.id,
        UIFormatter.main_menu(user.first_name),
        parse_mode=ParseMode.HTML,
        reply_markup=main_menu_kb(user.id)
    )


async def admin_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not user or not is_admin(user.id):
        return
    await reply_premium_message(update.message, 
        "<blockquote>👑 <b>ADMIN PANEL</b></blockquote>",
        parse_mode=ParseMode.HTML,
        reply_markup=admin_kb()
    )


async def proof_text_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await reply_premium_message(update.message, 
        "<blockquote>📸 <b>PROOF</b></blockquote>\n\nContact admin for proof and testimonials.",
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("📞 Contact Admin",
                                 url=f"https://t.me/{ADMIN_USERNAME.lstrip('@')}")
        ]])
    )


# ================= USER BOT HANDLERS =================
async def user_bot_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not user:
        return
    owner_uid = context.application.bot_data.get("owner_id")
    bot_id = context.application.bot_data.get("bot_id")

    if context.args and any("from_start_now" in a for a in context.args):
        try:
            if owner_uid and bot_id:
                db.mark_reachable(owner_uid, bot_id, user.id)
            await send_live_chat_message(owner_uid, bot_id, user.id, context)
            return
        except Exception as ex:
            logging.error(f"deep link live chat failed: {ex}")

    if owner_uid and user.id not in (owner_uid, ADMIN_USER_ID):
        await send_saved_welcome(owner_uid, bot_id, user.id, context)
        return

    target_uid = owner_uid if user.id == ADMIN_USER_ID and owner_uid else user.id
    target_bot_id = bot_id
    bot_data = db.get_user_bot(target_uid, target_bot_id)
    bot_username = bot_data[3] if bot_data else "bot"
    
    await send_premium_message(context.bot, 
        user.id,
        (f"<blockquote>🤖 <b>WELCOME TO YOUR BOT</b></blockquote>\n\n"
         f"Your Auto Join Request Bot is ready.\n"
         f"Use the buttons below to configure and manage it."),
        parse_mode=ParseMode.HTML,
        reply_markup=userbot_kb(target_uid, target_bot_id, bot_username)
    )


async def send_live_chat_message(owner_uid: int, bot_id: int, chat_id: int, context: ContextTypes.DEFAULT_TYPE):
    channels = db.get_user_channels(owner_uid, bot_id) if owner_uid else []
    if not channels:
        await send_premium_message(context.bot, chat_id,
                                       "⚠️ No channel configured yet.",
                                       parse_mode=ParseMode.HTML)
        return
    channel_id = channels[0][2]
    msgs = db.get_messages(owner_uid, bot_id, channel_id) or []
    if not msgs:
        await send_premium_message(context.bot, chat_id, "⚠️ No message saved yet.",
                                       parse_mode=ParseMode.HTML)
        return
    last = msgs[-1]
    text = last[1] if len(last) > 1 else ""
    media_id = last[2] if len(last) > 2 else None
    media_type = last[3] if len(last) > 3 else None
    entities_json = last[6] if len(last) > 6 else None

    support_markup = InlineKeyboardMarkup([
        [InlineKeyboardButton("💬 Live Chat Support", callback_data="live_chat_support")]
    ])
    await send_media(context, chat_id, media_id, media_type or "text",
                     text or "", support_markup, entities_json=entities_json)


async def handle_public_userbot_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q or not q.from_user:
        return
    uid = q.from_user.id
    owner_uid = context.application.bot_data.get("owner_id")
    bot_id = context.application.bot_data.get("bot_id")
    data = q.data
    chat_id = q.message.chat_id if q.message else uid

    if data == "start_now":
        try:
            await q.answer()
        except Exception:
            pass
        if owner_uid and bot_id:
            db.mark_reachable(owner_uid, bot_id, uid)
        try:
            await send_live_chat_message(owner_uid, bot_id, chat_id, context)
        except Exception as ex:
            logging.error(f"start_now send failed: {ex}")
            try:
                await q.answer("Please /start the bot first, then click Start now.",
                               show_alert=True)
            except Exception:
                pass
        return

    if data == "live_chat_support":
        try:
            await q.answer()
        except Exception:
            pass
        # Mark user as reachable so they receive future broadcasts
        if owner_uid and bot_id:
            try:
                db.mark_reachable(owner_uid, bot_id, uid)
            except Exception:
                pass
        # Send live chat header to user
        try:
            await send_premium_message(context.bot,
                chat_id,
                UIFormatter.live_chat_header(),
                parse_mode=ParseMode.HTML
            )
        except Exception:
            pass
        # Notify owner and admin that user wants live chat support
        if owner_uid and bot_id:
            try:
                user_obj = q.from_user
                user_mention = f"@{user_obj.username}" if user_obj.username else f"{user_obj.first_name} (ID: {uid})"
                notify_text = (
                    f"<blockquote>\U0001f4ac <b>LIVE CHAT REQUEST</b></blockquote>\n\n"
                    f"User {user_mention} ne Live Chat Support click kiya hai.\n"
                    f"User ID: <code>{uid}</code>"
                )
                inbox_ids = set(ADMIN_USER_IDS)
                inbox_ids.add(owner_uid)
                for aid in inbox_ids:
                    try:
                        await send_premium_message(context.bot, aid, notify_text, parse_mode=ParseMode.HTML)
                    except Exception as e2:
                        logging.error(f"live_chat notify failed to {aid}: {e2}")
            except Exception as ex:
                logging.error(f"live_chat_support notify error: {ex}")
        return


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
        # Human verification callback
        if data == "human_verify":
            user = q.from_user
            db.add_user(user.id, user.username, user.first_name, user.last_name)
            db.mark_user_verified(user.id)
            await safe_edit_message_text(q, 
                UIFormatter.verification_success(user.first_name),
                parse_mode=ParseMode.HTML,
                reply_markup=main_menu_kb(user.id)
            )
            return

        if data == "main_menu":
            user = q.from_user
            await safe_edit_message_text(q, 
                UIFormatter.main_menu(user.first_name),
                parse_mode=ParseMode.HTML,
                reply_markup=main_menu_kb(uid))
            for key in ["broadcast_stage", "broadcast_draft", "waiting_buttons",
                        "admin_broadcast_draft", "admin_broadcast",
                        "admin_broadcast_stage", "admin_broadcast_target",
                        "adding_channel", "setting_message", "messages"]:
                context.user_data.pop(key, None)

        elif data == "my_bots":
            await safe_edit_message_text(q, 
                "<blockquote>🤖 <b>YOUR BOTS</b></blockquote>\n\nSelect a bot to manage:",
                parse_mode=ParseMode.HTML,
                reply_markup=my_bots_kb(uid))

        elif data == "my_subscriptions":
            subs = db.get_user_subscriptions(uid)
            if not subs:
                await safe_edit_message_text(q, 
                    "<blockquote>📋 <b>YOUR SUBSCRIPTIONS</b></blockquote>\n\nNo subscriptions found.\nContact admin to purchase.",
                    parse_mode=ParseMode.HTML,
                    reply_markup=main_menu_kb(uid))
            else:
                await safe_edit_message_text(q, 
                    "<blockquote>📋 <b>YOUR SUBSCRIPTIONS</b></blockquote>\n\nSelect a bot to view its subscription:",
                    parse_mode=ParseMode.HTML,
                    reply_markup=subscriptions_list_kb(uid))

        elif data == "manage_bots":
            user_bots = db.get_user_bots(uid)
            if not user_bots:
                await safe_edit_message_text(q, 
                    "No bots found. Add one first!",
                    parse_mode=ParseMode.HTML,
                    reply_markup=my_bots_kb(uid))
            else:
                await safe_edit_message_text(q, 
                    "<blockquote>⚙️ <b>MANAGE YOUR BOTS</b></blockquote>\n\nSelect a bot to manage:",
                    parse_mode=ParseMode.HTML,
                    reply_markup=my_bots_kb(uid))

        elif data.startswith("select_bot_"):
            bot_id = int(data.split("_")[2])
            bot_data = db.get_user_bot(uid, bot_id)
            if not bot_data:
                await safe_edit_message_text(q, "Bot not found!", parse_mode=ParseMode.HTML,
                                          reply_markup=my_bots_kb(uid))
                return
            _, _, _, username, active = bot_data
            sub = db.get_subscription(uid, bot_id)
            has_active = False
            if sub:
                try:
                    expiry = datetime.fromisoformat(sub[3])
                    if expiry > datetime.now():
                        has_active = True
                except:
                    pass
            await safe_edit_message_text(q, 
                f"<blockquote>🤖 <b>@{username}</b></blockquote>\n\n"
                f"Status: {'🟢 Active' if active else '🔴 Stopped'}\n"
                f"Subscription: {'✅ Active' if has_active else '❌ None/Expired'}",
                parse_mode=ParseMode.HTML,
                reply_markup=bot_manage_kb(uid, bot_id, username, has_active))

        elif data.startswith("setup_bot_"):
            bot_id = int(data.split("_")[2])
            sub = db.get_subscription(uid, bot_id)
            if not sub:
                await safe_edit_message_text(q, 
                    UIFormatter.subscription_required(),
                    parse_mode=ParseMode.HTML,
                    reply_markup=subscription_kb(uid, bot_id))
                return
            try:
                expiry = datetime.fromisoformat(sub[3])
                if expiry < datetime.now():
                    await safe_edit_message_text(q, 
                        (f"<blockquote>❌ <b>SUBSCRIPTION EXPIRED</b></blockquote>\n\n"
                         f"📞 Contact {ADMIN_USERNAME} to renew."),
                        parse_mode=ParseMode.HTML,
                        reply_markup=subscription_kb(uid, bot_id))
                    return
            except Exception:
                pass
            await safe_edit_message_text(q, 
                f"<blockquote>⚙️ <b>SETUP YOUR BOT</b></blockquote>\n\n"
                f"Use the buttons below to configure your bot:",
                parse_mode=ParseMode.HTML,
                reply_markup=userbot_kb(uid, bot_id, sub[3] if len(sub) > 3 else "bot"))

        elif data.startswith("bot_stats_"):
            bot_id = int(data.split("_")[2])
            channels = db.get_user_channels(uid, bot_id) or []
            pending = db.get_pending_count(uid, bot_id)
            total_users = db.get_total_requesters_count(uid, bot_id)
            reachable_users = db.get_reachable_requesters_count(uid, bot_id)
            await safe_edit_message_text(q, 
                UIFormatter.bot_stats(len(channels), total_users, reachable_users, pending),
                parse_mode=ParseMode.HTML,
                reply_markup=bot_manage_kb(uid, bot_id, "", True))

        elif data.startswith("bot_sub_"):
            bot_id = int(data.split("_")[2])
            sub = db.get_subscription(uid, bot_id)
            if not sub:
                await safe_edit_message_text(q, 
                    "<blockquote>⭐️ <b>SUBSCRIPTION</b></blockquote>\n\nNo active subscription.",
                    parse_mode=ParseMode.HTML,
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("💎 Buy Subscription", callback_data=f"sub_basic_{bot_id}")],
                        [InlineKeyboardButton("🔙 Back", callback_data=f"select_bot_{bot_id}")]
                    ]))
                return
            try:
                expiry = datetime.fromisoformat(sub[3])
                days = (expiry - datetime.now()).days
                bot_data = db.get_user_bot(uid, bot_id)
                bot_username = bot_data[3] if bot_data else ""
                await safe_edit_message_text(q, 
                    UIFormatter.subscription_details(sub[2], expiry, days, sub[4] or 1, bot_username),
                    parse_mode=ParseMode.HTML,
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("🔄 Renew", callback_data=f"sub_renew_{bot_id}")],
                        [InlineKeyboardButton("🔙 Back", callback_data=f"select_bot_{bot_id}")]
                    ]))
            except Exception as e:
                await safe_edit_message_text(q, f"Error: {e}", parse_mode=ParseMode.HTML,
                                          reply_markup=bot_manage_kb(uid, bot_id, "", False))

        elif data.startswith("remove_bot_"):
            bot_id = int(data.split("_")[2])
            bot_data = db.get_user_bot(uid, bot_id)
            if not bot_data:
                await safe_edit_message_text(q, "Bot not found!", parse_mode=ParseMode.HTML,
                                          reply_markup=my_bots_kb(uid))
                return
            username = bot_data[3]
            await safe_edit_message_text(q, 
                f"<blockquote>⚠️ <b>REMOVE BOT</b></blockquote>\n\n"
                f"Are you sure you want to remove @{username}?\n"
                f"This will delete all channels, messages, and user data.\n\n"
                f"<b>This action cannot be undone!</b>",
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("✅ Yes, Remove", callback_data=f"confirm_remove_bot_{bot_id}")],
                    [InlineKeyboardButton("❌ Cancel", callback_data=f"select_bot_{bot_id}")]
                ]))

        elif data.startswith("confirm_remove_bot_"):
            bot_id = int(data.split("_")[3])
            key = (uid, bot_id)
            if key in user_bot_applications:
                await stop_user_bot(uid, bot_id)
            db.remove_user_bot(uid, bot_id)
            await safe_edit_message_text(q, 
                "✅ Bot removed successfully.",
                parse_mode=ParseMode.HTML,
                reply_markup=my_bots_kb(uid))

        elif data == "add_new_bot":
            await safe_edit_message_text(q, 
                ("<blockquote>🤖 <b>ADD NEW BOT</b></blockquote>\n\n"
                 "Send your BotFather API token to add a new bot:\n\n"
                 "<code>1234567890:ABCdefGHIjklMNOpqrsTUVwxyz</code>"),
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup(
                    [[InlineKeyboardButton("🔙 Back", callback_data="my_bots")]]))
            context.user_data["waiting_token_for_bot"] = True

        elif data.startswith("view_sub_"):
            bot_id = int(data.split("_")[2])
            sub = db.get_subscription(uid, bot_id)
            bot_data = db.get_user_bot(uid, bot_id)
            if not sub:
                await safe_edit_message_text(q, 
                    "No subscription for this bot.",
                    parse_mode=ParseMode.HTML,
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("💎 Buy", callback_data=f"sub_basic_{bot_id}")],
                        [InlineKeyboardButton("🔙 Back", callback_data="my_subscriptions")]
                    ]))
                return
            try:
                expiry = datetime.fromisoformat(sub[3])
                days = (expiry - datetime.now()).days
                await safe_edit_message_text(q, 
                    UIFormatter.subscription_details(sub[2], expiry, days, sub[4] or 1, bot_data[3] if bot_data else ""),
                    parse_mode=ParseMode.HTML,
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("🔄 Renew", callback_data=f"sub_renew_{bot_id}")],
                        [InlineKeyboardButton("🗑️ Remove Subscription", callback_data=f"remove_sub_{bot_id}")],
                        [InlineKeyboardButton("🔙 Back", callback_data="my_subscriptions")]
                    ]))
            except Exception as e:
                await safe_edit_message_text(q, f"Error: {e}", parse_mode=ParseMode.HTML,
                                          reply_markup=subscriptions_list_kb(uid))

        elif data.startswith("remove_sub_"):
            bot_id = int(data.split("_")[2])
            db.remove_subscription(uid, bot_id)
            # Also stop the bot if it was running
            key = (uid, bot_id)
            if key in user_bot_applications:
                await stop_user_bot(uid, bot_id)
            await safe_edit_message_text(q, 
                "✅ Subscription removed successfully.",
                parse_mode=ParseMode.HTML,
                reply_markup=my_bots_kb(uid))

        elif data.startswith("sub_basic_") or data.startswith("sub_pro_") or data.startswith("sub_renew_"):
            parts = data.split("_")
            sub_type = parts[0].replace("sub_", "")
            bot_id = int(parts[2]) if len(parts) > 2 else None
            await safe_edit_message_text(q, 
                (f"<blockquote>💰 <b>{sub_type.upper()} PLAN SELECTED</b></blockquote>\n\n"
                 f"{'₹2599/month — 1 channel' if sub_type == 'basic' else '₹3999/month — 5 channels'}\n\n"
                 f"📞 Contact {ADMIN_USERNAME} to complete payment.\n"
                 f"Provide your User ID: <code>{uid}</code>\n"
                 f"Bot ID: <code>{bot_id if bot_id else 'New Bot'}</code>"),
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔙 Back", callback_data=f"select_bot_{bot_id}" if bot_id else "my_bots")]
                ]))

        elif data == "my_subscriptions":
            subs = db.get_user_subscriptions(uid)
            if not subs:
                await safe_edit_message_text(q, 
                    "<blockquote>📋 <b>YOUR SUBSCRIPTIONS</b></blockquote>\n\nNo subscriptions found.",
                    parse_mode=ParseMode.HTML,
                    reply_markup=main_menu_kb(uid))
            else:
                await safe_edit_message_text(q, 
                    "<blockquote>📋 <b>YOUR SUBSCRIPTIONS</b></blockquote>\n\nSelect a bot:",
                    parse_mode=ParseMode.HTML,
                    reply_markup=subscriptions_list_kb(uid))

        elif data == "admin_manage_subs":
            if not is_admin(uid):
                return
            await safe_edit_message_text(q, 
                "<blockquote>⭐️ <b>MANAGE SUBSCRIPTIONS</b></blockquote>",
                parse_mode=ParseMode.HTML,
                reply_markup=admin_manage_subs_kb())

        elif data == "admin_remove_sub":
            if not is_admin(uid):
                return
            await safe_edit_message_text(q, 
                ("<blockquote>🗑️ <b>REMOVE SUBSCRIPTION</b></blockquote>\n\n"
                 "Send: <code>user_id bot_id</code>\n"
                 "Example: <code>123456789 1</code>\n\n"
                 "To find bot_id, check /admin_userbots"),
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup(
                    [[InlineKeyboardButton("🔙 Back", callback_data="admin_manage_subs")]]))
            context.user_data["admin_remove_sub_mode"] = True

        elif data == "admin_add_sub_from_user":
            if not is_admin(uid):
                return
            await safe_edit_message_text(q, 
                ("<blockquote>⭐️ <b>ADD SUBSCRIPTION TO USER</b></blockquote>\n\n"
                 "Send: <code>user_id bot_id days plan</code>\n"
                 "Example: <code>123456789 1 30 Basic</code>\n\n"
                 "Plan: Basic or Pro"),
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup(
                    [[InlineKeyboardButton("🔙 Back", callback_data="my_bots")]]))
            context.user_data["admin_add_sub_to_user"] = True

        elif data == "admin_panel":
            if not is_admin(uid):
                await safe_edit_message_text(q, "‼️ Not authorized", parse_mode=ParseMode.HTML)
                return
            for key in ["admin_broadcast_draft", "admin_broadcast",
                        "admin_broadcast_stage", "admin_broadcast_target",
                        "admin_remove_sub_mode", "admin_add_sub_to_user"]:
                context.user_data.pop(key, None)
            await safe_edit_message_text(q, 
                "<blockquote>👑 <b>ADMIN PANEL</b></blockquote>",
                parse_mode=ParseMode.HTML,
                reply_markup=admin_kb())

        elif data == "admin_all_users":
            if not is_admin(uid):
                return
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
            if not is_admin(uid):
                return
            await safe_edit_message_text(q, 
                ("<blockquote>🚀 <b>ADD USERBOT</b></blockquote>\n\n"
                 "Send: <code>user_id bot_token</code>\n"
                 "Example: <code>123456789 123456:ABCdef...</code>\n\n"
                 "The bot will be assigned bot_id automatically."),
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup(
                    [[InlineKeyboardButton("🔙 Back", callback_data="admin_panel")]]),
            )
            context.user_data["admin_add_userbot"] = True

        elif data.startswith("admin_ub_info_"):
            if not is_admin(uid):
                return
            parts = data.split("_")
            tuid = int(parts[3])
            tbot_id = int(parts[4]) if len(parts) > 4 else None
            await show_admin_ub_info(q, tuid, tbot_id, context)

        elif data.startswith("admin_ub_start_"):
            if not is_admin(uid):
                return
            parts = data.split("_")
            tuid = int(parts[3])
            tbot_id = int(parts[4]) if len(parts) > 4 else None
            bot_data = db.get_user_bot(tuid, tbot_id)
            if not bot_data or not bot_data[2]:
                await safe_edit_message_text(q, "‼️ UserBot not found.", parse_mode=ParseMode.HTML,
                                          reply_markup=admin_kb())
                return
            sub = db.get_subscription(tuid, tbot_id)
            if not sub:
                await safe_edit_message_text(q, f"‼️ User {tuid} bot {tbot_id} has no subscription.",
                                          parse_mode=ParseMode.HTML, reply_markup=admin_kb())
                return
            try:
                exp = datetime.fromisoformat(sub[3])
                if exp < datetime.now():
                    await safe_edit_message_text(q, f"‼️ User {tuid} bot {tbot_id} subscription expired.",
                                              parse_mode=ParseMode.HTML, reply_markup=admin_kb())
                    return
            except Exception:
                pass
            key = (tuid, tbot_id)
            if key in user_bot_applications:
                await safe_edit_message_text(q, 
                    f"ℹ️ Bot @{bot_data[3]} is already running.",
                    parse_mode=ParseMode.HTML,
                    reply_markup=InlineKeyboardMarkup(
                        [[InlineKeyboardButton("🔙 Back", callback_data="admin_userbots")]]))
                return
            try:
                await start_user_bot(bot_data[2], tuid, tbot_id)
                db.set_user_bot_active(tuid, tbot_id, True)
                await safe_edit_message_text(q, 
                    f"✅ Bot @{bot_data[3]} started for user {tuid}",
                    parse_mode=ParseMode.HTML,
                    reply_markup=InlineKeyboardMarkup(
                        [[InlineKeyboardButton("🔙 Back", callback_data="admin_userbots")]]))
            except Exception as ex:
                await safe_edit_message_text(q, f"‼️ Failed to start: {ex}",
                                          parse_mode=ParseMode.HTML,
                                          reply_markup=InlineKeyboardMarkup(
                                              [[InlineKeyboardButton("🔙 Back",
                                                                     callback_data="admin_userbots")]]))

        elif data.startswith("admin_ub_stop_"):
            if not is_admin(uid):
                return
            parts = data.split("_")
            tuid = int(parts[3])
            tbot_id = int(parts[4]) if len(parts) > 4 else None
            bot_data = db.get_user_bot(tuid, tbot_id)
            buser = bot_data[3] if bot_data else str(tuid)
            key = (tuid, tbot_id) if tbot_id else (tuid, 0)
            if key not in user_bot_applications:
                await safe_edit_message_text(q, 
                    f"ℹ️ Bot @{buser} is not running.",
                    parse_mode=ParseMode.HTML,
                    reply_markup=InlineKeyboardMarkup(
                        [[InlineKeyboardButton("🔙 Back", callback_data="admin_userbots")]]))
                return
            try:
                await stop_user_bot(tuid, tbot_id)
                await safe_edit_message_text(q, 
                    f"🛑 Bot @{buser} stopped for user {tuid}",
                    parse_mode=ParseMode.HTML,
                    reply_markup=InlineKeyboardMarkup(
                        [[InlineKeyboardButton("🔙 Back", callback_data="admin_userbots")]]))
            except Exception as ex:
                await safe_edit_message_text(q, f"‼️ Failed to stop: {ex}",
                                          parse_mode=ParseMode.HTML,
                                          reply_markup=InlineKeyboardMarkup(
                                              [[InlineKeyboardButton("🔙 Back",
                                                                     callback_data="admin_userbots")]]))

        elif data.startswith("admin_remove_bot_"):
            if not is_admin(uid):
                return
            parts = data.split("_")
            tuid = int(parts[3])
            tbot_id = int(parts[4]) if len(parts) > 4 else None
            bot_data = db.get_user_bot(tuid, tbot_id)
            if not bot_data:
                await safe_edit_message_text(q, "‼️ UserBot not found.", parse_mode=ParseMode.HTML,
                                          reply_markup=admin_kb())
                return
            buser = bot_data[3] or "unknown"
            await safe_edit_message_text(q, 
                (f"‼️ Confirm remove userbot?\n"
                 f"👤 User: {tuid}\n"
                 f"🤖 Bot: @{buser}\n"
                 f"🤖 Bot ID: {tbot_id}"),
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("✅ Yes, Remove",
                                          callback_data=f"admin_remove_bot_confirm_{tuid}_{tbot_id}")],
                    [InlineKeyboardButton("❌ Cancel",
                                          callback_data="admin_userbots")],
                ])
            )

        elif data.startswith("admin_remove_bot_confirm_"):
            if not is_admin(uid):
                return
            parts = data.split("_")
            tuid = int(parts[4])
            tbot_id = int(parts[5]) if len(parts) > 5 else None
            await stop_user_bot(tuid, tbot_id)
            db.remove_user_bot(tuid, tbot_id)
            await safe_edit_message_text(q, f"✅ UserBot removed for user {tuid} bot {tbot_id}",
                                      parse_mode=ParseMode.HTML,
                                      reply_markup=InlineKeyboardMarkup(
                                          [[InlineKeyboardButton("🔙 Back",
                                                                 callback_data="admin_userbots")]]))

        elif data == "admin_add_sub":
            if not is_admin(uid):
                return
            await safe_edit_message_text(q, 
                ("<blockquote>⭐️ <b>ADD SUBSCRIPTION</b></blockquote>\n\n"
                 "Send: <code>user_id bot_id days plan</code>\n"
                 "Example: <code>123456789 1 30 Pro</code>\n\n"
                 "Plan: Basic or Pro\n"
                 "To find bot_id, check /admin_userbots"),
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup(
                    [[InlineKeyboardButton("🔙 Back", callback_data="admin_manage_subs")]]))
            context.user_data["admin_add_sub"] = True

        elif data == "admin_check_expiry":
            if not is_admin(uid):
                return
            await check_expiry(q)

        elif data == "admin_stats":
            if not is_admin(uid):
                return
            await show_stats(q)

        elif data == "admin_start_all":
            if not is_admin(uid):
                return
            await start_all_userbots(q)

        elif data == "admin_stop_all":
            if not is_admin(uid):
                return
            stopped = 0
            failed = 0
            for key in list(user_bot_applications.keys()):
                try:
                    user_id, bot_id = key
                    await stop_user_bot(user_id, bot_id)
                    stopped += 1
                except Exception as ex:
                    logging.error(f"stop_all error {key}: {ex}")
                    failed += 1
            await safe_edit_message_text(q, 
                (f"<blockquote>🛑 <b>STOP ALL COMPLETE</b></blockquote>\n\n"
                 f"✅ Stopped: {stopped}\n❌ Failed: {failed}"),
                parse_mode=ParseMode.HTML, reply_markup=admin_kb())

        elif data == "admin_broadcast":
            if not is_admin(uid):
                return
            await safe_edit_message_text(q, 
                "<blockquote>✈️ <b>BROADCAST</b></blockquote>\n\nChoose target:",
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🎯 Specific UserBot",
                                          callback_data="admin_bcast_target_select")],
                    [InlineKeyboardButton("🌐 All UserBots",
                                          callback_data="admin_bcast_target_all")],
                    [InlineKeyboardButton("🔙 Back", callback_data="admin_panel")],
                ]))

        elif data == "admin_bcast_target_all":
            if not is_admin(uid):
                return
            context.user_data["admin_broadcast"] = True
            context.user_data["admin_broadcast_target"] = None
            await safe_edit_message_text(q, 
                ("<blockquote>✈️ <b>BROADCAST TO ALL</b></blockquote>\n\n"
                 "Send text or media to broadcast to all userbots' users.\n\n"
                 "• HTML formatting supported\n"
                 "• Premium emojis preserved exactly as sent"),
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup(
                    [[InlineKeyboardButton("🔙 Back", callback_data="admin_panel")]]))

        elif data == "admin_bcast_target_select":
            if not is_admin(uid):
                return
            bots = db.get_all_user_bots() or []
            if not bots:
                await safe_edit_message_text(q, "‼️ No userbots found.", parse_mode=ParseMode.HTML,
                                          reply_markup=admin_kb())
                return
            kb = []
            for b in bots:
                tuid, tbot_id, _, buser, _ = b
                kb.append([InlineKeyboardButton(
                    f"@{buser} (User:{tuid} Bot:{tbot_id})", callback_data=f"admin_bcast_pick_{tuid}_{tbot_id}")])
            kb.append([InlineKeyboardButton("🔙 Back", callback_data="admin_broadcast")])
            await safe_edit_message_text(q, "🤖 Select userbot:", parse_mode=ParseMode.HTML,
                                      reply_markup=InlineKeyboardMarkup(kb))

        elif data.startswith("admin_bcast_pick_"):
            if not is_admin(uid):
                return
            parts = data.split("_")
            tuid = int(parts[3])
            tbot_id = int(parts[4]) if len(parts) > 4 else None
            context.user_data["admin_broadcast"] = True
            context.user_data["admin_broadcast_target"] = (tuid, tbot_id)
            await safe_edit_message_text(q, 
                (f"<blockquote>✈️ <b>BROADCAST TO USERBOT {tuid}/{tbot_id}</b></blockquote>\n\n"
                 "Send text or media to broadcast.\n\n"
                 "• HTML formatting supported\n"
                 "• Premium emojis preserved exactly"),
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup(
                    [[InlineKeyboardButton("🔙 Back", callback_data="admin_panel")]]))

        elif data == "admin_send_reminders":
            if not is_admin(uid):
                return
            sent_count = 0
            for days_threshold in [3, 1]:
                expiring = db.get_expiring_subscriptions(days_threshold)
                for sub in expiring:
                    sub_uid = sub["user_id"]
                    bot_id = sub["bot_id"]
                    sub_type = sub["subscription_type"]
                    try:
                        expiry_dt = datetime.fromisoformat(sub["expiry_date"])
                        days_left = (expiry_dt - datetime.now()).days
                        if days_threshold == 3:
                            msg_text = UIFormatter.expiry_reminder_3d(sub_type, expiry_dt, days_left)
                        else:
                            msg_text = UIFormatter.expiry_reminder_1d(sub_type, expiry_dt)
                        await send_premium_message(context.bot, 
                            sub_uid, msg_text, parse_mode=ParseMode.HTML,
                            reply_markup=InlineKeyboardMarkup([[
                                InlineKeyboardButton(
                                    "🔄 Renew",
                                    url=f"https://t.me/{ADMIN_USERNAME.lstrip('@')}")
                            ]]))
                        db.mark_reminder_sent(sub_uid, bot_id, days_threshold)
                        sent_count += 1
                    except Exception as ex:
                        logging.error(f"Manual reminder failed for {sub_uid}/{bot_id}: {ex}")
            await safe_edit_message_text(q, 
                f"✅ Reminders sent to {sent_count} users.",
                parse_mode=ParseMode.HTML, reply_markup=admin_kb())

        # User bot operation callbacks (with bot_id)
        elif data.startswith("ub_"):
            await handle_user_bot_callback(update, context)

    except Exception as ex:
        logging.error(f"Error in callback_handler: {ex}")
        if "Message is not modified" not in str(ex):
            try:
                await safe_edit_message_text(q, f"‼️ Error: {str(ex)}", parse_mode=ParseMode.HTML,
                                          reply_markup=main_menu_kb(uid))
            except Exception:
                pass


async def handle_user_bot_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q or not q.from_user:
        return
    try:
        await q.answer()
    except Exception:
        pass
    uid = q.from_user.id
    data = q.data
    owner_uid = context.application.bot_data.get("owner_id") if hasattr(context.application.bot_data, "get") else uid
    current_bot_id = get_current_bot_id(context)
    
    # Parse bot_id from callback data
    parts = data.split("_")
    bot_id = None
    for part in parts:
        if part.isdigit() and len(part) > 2:
            bot_id = int(part)
            break
    
    if not bot_id and "_" in data:
        try:
            bot_id = int(parts[-1]) if parts[-1].isdigit() else None
        except:
            pass
    
    if not bot_id:
        bot_id = current_bot_id
    
    if not bot_id:
        await safe_edit_message_text(q, "Bot ID not found.", parse_mode=ParseMode.HTML,
                                  reply_markup=main_menu_kb(uid))
        return
    
    managed_uid = get_managed_user_id(context, uid)
    ud = _runtime_store(context, uid)
    
    try:
        if data.startswith("ub_add_channel_"):
            await safe_edit_message_text(q, 
                ("<blockquote>✈️ <b>ADD CHANNEL</b></blockquote>\n\n"
                 "Add this bot as <b>admin</b> in your channel, then forward any message "
                 "from that channel here."),
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup(
                    [[InlineKeyboardButton("🔙 Back", callback_data=f"setup_bot_{bot_id}")]]),
            )
            context.user_data["adding_channel"] = True
            context.user_data["adding_channel_bot_id"] = bot_id
            ud["adding_channel"] = True

        elif data.startswith("ub_set_message_"):
            await safe_edit_message_text(q, 
                ("<blockquote>📝 <b>SET WELCOME MESSAGES</b></blockquote>\n\n"
                 "Send one or multiple messages (text, photo, video, etc.)\n\n"
                 "• HTML formatting supported: <code>&lt;b&gt;bold&lt;/b&gt;</code>, "
                 "<code>&lt;i&gt;italic&lt;/i&gt;</code>\n"
                 "• Inline buttons: <code>Text|https://link</code>\n"
                 "• Premium emojis are preserved exactly as you send them\n\n"
                 "Type <code>done</code> when finished."),
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup(
                    [[InlineKeyboardButton("🔙 Back", callback_data=f"setup_bot_{bot_id}")]]),
            )
            context.user_data["setting_message"] = True
            context.user_data["setting_message_bot_id"] = bot_id
            context.user_data["messages"] = []
            ud["setting_message"] = True
            ud["messages"] = []

        elif data.startswith("ub_delete_messages_"):
            channels = db.get_user_channels(managed_uid, bot_id)
            if not channels:
                await safe_edit_message_text(q, "‼️ No channels yet.", parse_mode=ParseMode.HTML,
                                          reply_markup=userbot_kb(managed_uid, bot_id, ""))
                return
            kb = [[InlineKeyboardButton(f"🗑️ {c[4][:18]}", callback_data=f"delmsg_{bot_id}_{c[2]}")]
                  for c in channels]
            kb.append([InlineKeyboardButton("🔙 Back", callback_data=f"setup_bot_{bot_id}")])
            await safe_edit_message_text(q, "Select channel to clear welcome messages:",
                                      parse_mode=ParseMode.HTML,
                                      reply_markup=InlineKeyboardMarkup(kb))

        elif data.startswith("ub_list_channels_"):
            await show_channels(q, managed_uid, bot_id)

        elif data.startswith("ub_pending_requests_"):
            await show_pending(q, managed_uid, bot_id)

        elif data.startswith("ub_subscription_"):
            await show_sub_userbot(q, managed_uid, bot_id)

        elif data.startswith("ub_toggle_auto_"):
            await show_toggle(q, managed_uid, bot_id)

        elif data.startswith("ub_accept_all_"):
            await accept_all(q, managed_uid, bot_id, context)

        elif data.startswith("ub_stats_"):
            channels = db.get_user_channels(managed_uid, bot_id) or []
            pending = db.get_pending_count(managed_uid, bot_id)
            total_users = db.get_total_requesters_count(managed_uid, bot_id)
            reachable_users = db.get_reachable_requesters_count(managed_uid, bot_id)
            await safe_edit_message_text(q, 
                UIFormatter.bot_stats(len(channels), total_users, reachable_users, pending),
                parse_mode=ParseMode.HTML,
                reply_markup=userbot_kb(managed_uid, bot_id, ""))

        elif data.startswith("ub_manage_messages_"):
            channels = db.get_user_channels(managed_uid, bot_id)
            if not channels:
                await safe_edit_message_text(q, "‼️ No channels yet.", parse_mode=ParseMode.HTML,
                                          reply_markup=userbot_kb(managed_uid, bot_id, ""))
                return
            kb = [[InlineKeyboardButton(
                f"🧩 {c[4][:22]} ({db.get_message_count(managed_uid, bot_id, c[2])} msgs)",
                callback_data=f"ubmm_{bot_id}_{c[2]}")] for c in channels]
            kb.append([InlineKeyboardButton("🔙 Back", callback_data=f"setup_bot_{bot_id}")])
            await safe_edit_message_text(q, 
                "<blockquote>✨ <b>MESSAGE MANAGER</b></blockquote>\n\nSelect a channel:",
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup(kb))

        elif data.startswith("ub_remove_channel_"):
            await prompt_remove_channel(q, managed_uid, bot_id)

        elif data.startswith("ub_broadcast_"):
            await safe_edit_message_text(q, 
                ("<blockquote>✈️ <b>BROADCAST TO USERS</b></blockquote>\n\n"
                 "Send text or media to broadcast to all your users.\n\n"
                 "• HTML formatting supported\n"
                 "• Inline buttons: <code>Text|https://link</code>\n"
                 "• Premium emojis preserved exactly as sent"),
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup(
                    [[InlineKeyboardButton("🔙 Back", callback_data=f"setup_bot_{bot_id}")]]),
            )
            context.user_data["broadcast_stage"] = "await_message"
            context.user_data["broadcast_bot_id"] = bot_id
            context.user_data.pop("broadcast_draft", None)

        elif data.startswith("toggleauto_"):
            await handle_toggle_callback(update, context, bot_id)

        elif data.startswith("delmsg_"):
            parts = data.split("_")
            cid = int(parts[2])
            db.clear_messages(managed_uid, bot_id, cid)
            await safe_edit_message_text(q, "✅ Messages cleared.",
                                      parse_mode=ParseMode.HTML,
                                      reply_markup=userbot_kb(managed_uid, bot_id, ""))

        elif data.startswith("ubmm_"):
            parts = data.split("_")
            cid = int(parts[2])
            msgs = db.get_messages(managed_uid, bot_id, cid) or []
            if not msgs:
                await safe_edit_message_text(q, "‼️ No messages in this channel.",
                                          parse_mode=ParseMode.HTML,
                                          reply_markup=userbot_kb(managed_uid, bot_id, ""))
                return
            lines = ["<blockquote>✨ <b>SAVED MESSAGES</b></blockquote>\n"]
            kb = []
            seen_groups = set()
            for m in msgs[:50]:
                mid = m[0]; text = m[1] if len(m) > 1 else ""
                media_type = m[3] if len(m) > 3 else None
                mgid = m[4] if len(m) > 4 else None
                if mgid:
                    if mgid in seen_groups:
                        continue
                    g_items = [x for x in msgs if x[4] == mgid]
                    seen_groups.add(mgid)
                    lines.append(f"• Album {mgid[-6:]} | {len(g_items)} items | {(text or '')[:30]}")
                else:
                    lines.append(f"• ID {mid} | {media_type or 'text'} | {(text or '')[:30]}")
                kb.append([
                    InlineKeyboardButton(f"🔎 Preview {mid}", callback_data=f"ubm_preview_{bot_id}_{mid}"),
                    InlineKeyboardButton(f"✏️ Edit {mid}", callback_data=f"ubm_edit_{bot_id}_{mid}"),
                    InlineKeyboardButton(f"🗑️ Del {mid}", callback_data=f"ubm_del_{bot_id}_{mid}")
                ])
            kb.append([InlineKeyboardButton("🔙 Back", callback_data=f"setup_bot_{bot_id}")])
            await safe_edit_message_text(q, "\n".join(lines)[:3900],
                                      parse_mode=ParseMode.HTML,
                                      reply_markup=InlineKeyboardMarkup(kb))

        elif data.startswith("ubm_preview_"):
            parts = data.split("_")
            mid = int(parts[3]) if len(parts) > 3 else None
            if not mid:
                return
            row = db.get_message_by_id(mid)
            if not row:
                await safe_edit_message_text(q, "‼️ Message not found.", parse_mode=ParseMode.HTML,
                                          reply_markup=userbot_kb(managed_uid, bot_id, ""))
                return
            msg_text = row[4] if len(row) > 4 else ""
            msg_media_id = row[5] if len(row) > 5 else None
            msg_media_type = row[6] if len(row) > 6 else None
            msg_btnj = row[8] if len(row) > 8 else None
            msg_ej = row[9] if len(row) > 9 else None
            markup = buttons_to_markup(msg_btnj)
            try:
                await send_media(context, q.message.chat_id, msg_media_id,
                                 msg_media_type or "text", msg_text or "",
                                 markup, entities_json=msg_ej)
            except Exception as ex:
                await safe_edit_message_text(q, f"‼️ Preview failed: {ex}",
                                          parse_mode=ParseMode.HTML,
                                          reply_markup=userbot_kb(managed_uid, bot_id, ""))

        elif data.startswith("ubm_edit_"):
            parts = data.split("_")
            mid = int(parts[3]) if len(parts) > 3 else None
            if not mid:
                return
            ud["editing_msg_id"] = mid
            ud["editing_bot_id"] = bot_id
            await safe_edit_message_text(q, 
                ("<blockquote>✏️ <b>EDIT MESSAGE</b></blockquote>\n\n"
                 "What do you want to edit?"),
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("📝 Edit Text", callback_data=f"ubm_edittext_{bot_id}_{mid}")],
                    [InlineKeyboardButton("🖼️ Edit Media", callback_data=f"ubm_editmedia_{bot_id}_{mid}")],
                    [InlineKeyboardButton("🔘 Edit Buttons", callback_data=f"ubm_editbtns_{bot_id}_{mid}")],
                    [InlineKeyboardButton("🔙 Back", callback_data=f"setup_bot_{bot_id}")],
                ])
            )

        elif data.startswith("ubm_del_"):
            parts = data.split("_")
            mid = int(parts[3]) if len(parts) > 3 else None
            if not mid:
                return
            row = db.get_message_by_id(mid)
            if row:
                mgid = row[7] if len(row) > 7 else None
                if mgid:
                    db.delete_media_group_messages(mgid)
                else:
                    db.delete_message(mid)
            await safe_edit_message_text(q, "✅ Message deleted.",
                                      parse_mode=ParseMode.HTML,
                                      reply_markup=userbot_kb(managed_uid, bot_id, ""))

        elif data.startswith("removechan_"):
            parts = data.split("_")
            cid = int(parts[2]) if len(parts) > 2 else None
            if cid:
                ok = db.remove_channel(managed_uid, bot_id, cid)
                if ok:
                    await safe_edit_message_text(q, "✅ Channel removed.",
                                              parse_mode=ParseMode.HTML,
                                              reply_markup=userbot_kb(managed_uid, bot_id, ""))
                else:
                    await safe_edit_message_text(q, "‼️ Failed to remove channel.",
                                              parse_mode=ParseMode.HTML,
                                              reply_markup=userbot_kb(managed_uid, bot_id, ""))

        elif data in ("setmsg_more", "setmsg_done", "setmsg_cancel"):
            if data == "setmsg_done" or data == "setmsg_cancel":
                for key in ["setting_message", "messages", "pending_buttons", "waiting_buttons"]:
                    ud.pop(key, None)
                    context.user_data.pop(key, None)
                await safe_edit_message_text(q, "✅ Done.", parse_mode=ParseMode.HTML,
                                          reply_markup=userbot_kb(managed_uid, bot_id, ""))
            else:
                await safe_edit_message_text(q, 
                    "✅ Send your next message, or type <code>done</code> to finish.",
                    parse_mode=ParseMode.HTML,
                    reply_markup=InlineKeyboardMarkup(
                        [[InlineKeyboardButton("🔙 Back", callback_data=f"setup_bot_{bot_id}")]]))

    except Exception as ex:
        logging.error(f"Error in handle_user_bot_callback: {ex}")
        try:
            await safe_edit_message_text(q, f"‼️ Error: {str(ex)}", parse_mode=ParseMode.HTML,
                                      reply_markup=userbot_kb(managed_uid, bot_id, ""))
        except Exception:
            pass


async def show_channels(q, uid, bot_id):
    channels = db.get_user_channels(uid, bot_id)
    if not channels:
        await safe_edit_message_text(q, "‼️ No channels added yet.",
                                  parse_mode=ParseMode.HTML,
                                  reply_markup=userbot_kb(uid, bot_id, ""))
        return
    lines = ["<blockquote>📋 <b>MY CHANNELS</b></blockquote>\n"]
    for c in channels:
        auto = "🟢 Auto" if c[8] else "🔴 Manual"
        lines.append(f"• {c[4]} | {auto} | ID: {c[2]}")
    await safe_edit_message_text(q, "\n".join(lines), parse_mode=ParseMode.HTML,
                              reply_markup=userbot_kb(uid, bot_id, ""))


async def show_pending(q, uid, bot_id):
    pending = db.get_pending_requests(uid, bot_id)
    text = (f"<blockquote>📊 <b>PENDING REQUESTS</b></blockquote>\n\n"
            f"🔔 <b>Total Pending:</b> {len(pending)}")
    await safe_edit_message_text(q, text, parse_mode=ParseMode.HTML, reply_markup=userbot_kb(uid, bot_id, ""))


async def show_sub_userbot(q, uid, bot_id):
    sub = db.get_subscription(uid, bot_id)
    if not sub:
        await safe_edit_message_text(q, "‼️ No active subscription.",
                                  parse_mode=ParseMode.HTML,
                                  reply_markup=userbot_kb(uid, bot_id, ""))
        return
    expiry = datetime.fromisoformat(sub[3])
    days = (expiry - datetime.now()).days
    bot_data = db.get_user_bot(uid, bot_id)
    bot_username = bot_data[3] if bot_data else ""
    await safe_edit_message_text(q, 
        UIFormatter.subscription_details(sub[2], expiry, days, sub[4] or 1, bot_username),
        parse_mode=ParseMode.HTML,
        reply_markup=userbot_kb(uid, bot_id, bot_username))


async def show_toggle(q, uid, bot_id):
    channels = db.get_user_channels(uid, bot_id)
    if not channels:
        await safe_edit_message_text(q, "‼️ No channels yet.", parse_mode=ParseMode.HTML,
                                  reply_markup=userbot_kb(uid, bot_id, ""))
        return
    kb = []
    for c in channels:
        cid = c[2]
        title = c[4]
        auto_raw = c[8] if len(c) > 8 else 0
        auto = bool(int(auto_raw)) if auto_raw is not None else False
        kb.append([InlineKeyboardButton(
            f"{'🟢 ON' if auto else '🔴 OFF'} — {title[:20]}",
            callback_data=f"toggleauto_{bot_id}_{cid}")])
    kb.append([InlineKeyboardButton("🔙 Back", callback_data=f"setup_bot_{bot_id}")])
    await safe_edit_message_text(q, 
        "<blockquote>⚙️ <b>AUTO-APPROVE SETTINGS</b></blockquote>\n\nTap channel to toggle:",
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(kb))


async def handle_toggle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE, bot_id: int):
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
    parts = data.split("_")
    cid = int(parts[2]) if len(parts) > 2 else None
    if not cid:
        return
    channel = db.get_channel_owner_data(cid)
    if not channel or channel[0] != managed_uid:
        await safe_edit_message_text(q, "‼️ Channel not found.", parse_mode=ParseMode.HTML,
                                  reply_markup=userbot_kb(managed_uid, bot_id, ""))
        return
    auto_raw = channel[8] if len(channel) > 8 else 0
    current = bool(int(auto_raw)) if auto_raw is not None else False
    db.set_auto_approve(managed_uid, bot_id, cid, not current)
    await show_toggle(q, managed_uid, bot_id)


async def accept_all(q, uid, bot_id, context):
    try:
        channels = db.get_user_channels(uid, bot_id) or []
        for ch in channels:
            bot_data = db.get_user_bot(uid, bot_id)
            if bot_data:
                test_bot = Bot(token=bot_data[2])
                await sync_pending_join_requests_for_channel(uid, bot_id, ch[2], test_bot)
    except Exception:
        pass
    pending = db.get_pending_requests(uid, bot_id)
    if not pending:
        await safe_edit_message_text(q, "‼️ No pending requests.", parse_mode=ParseMode.HTML,
                                  reply_markup=userbot_kb(uid, bot_id, ""))
        return
    ok = 0
    fail = 0
    cleaned = 0
    bot_data = db.get_user_bot(uid, bot_id)
    bot_instance = None
    if bot_data:
        bot_instance = Bot(token=bot_data[2])
    for rid, requester, channel_id in pending:
        try:
            await bot_instance.approve_chat_join_request(channel_id, requester)
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
    await safe_edit_message_text(q, 
        (f"<blockquote>✅ <b>ACCEPT ALL COMPLETE</b></blockquote>\n\n"
         f"✅ Accepted: {ok}\n"
         f"🧹 Already approved: {cleaned}\n"
         f"❌ Failed: {fail}"),
        parse_mode=ParseMode.HTML,
        reply_markup=userbot_kb(uid, bot_id, ""))


async def prompt_remove_channel(q, uid, bot_id):
    channels = db.get_user_channels(uid, bot_id)
    if not channels:
        await safe_edit_message_text(q, "‼️ No channels to remove.", parse_mode=ParseMode.HTML,
                                  reply_markup=userbot_kb(uid, bot_id, ""))
        return
    kb = [[InlineKeyboardButton(f"❌ {c[4][:22]}", callback_data=f"removechan_{bot_id}_{c[2]}")]
          for c in channels]
    kb.append([InlineKeyboardButton("🔙 Back", callback_data=f"setup_bot_{bot_id}")])
    await safe_edit_message_text(q, "Select channel to remove:",
                              parse_mode=ParseMode.HTML,
                              reply_markup=InlineKeyboardMarkup(kb))


async def flush_media_group(actor_uid, managed_uid, bot_id, chat_id, context, media_group_id):
    if not actor_uid or not managed_uid or not bot_id or not media_group_id:
        return
    ud = _runtime_store(context, actor_uid)
    key = f"mg_{media_group_id}"
    items = ud.get(key, [])
    if not items:
        return
    channels = db.get_user_channels(managed_uid, bot_id)
    if not channels:
        return
    channel_id = channels[0][2]
    saved_ids = []
    for it in items:
        msg_id = db.add_message(
            managed_uid, bot_id, channel_id,
            it.get("text", ""), it.get("media_id"),
            it.get("media_type"), media_group_id,
            it.get("entities_json")
        )
        if msg_id and it.get("emoji_map"):
            db.save_user_emoji_map(managed_uid, msg_id, it["emoji_map"])
        saved_ids.append(msg_id)
    ud.pop(key, None)
    ud.pop("manual_album_buffer", None)
    ud["pending_buttons_group"] = {"msg_ids": saved_ids}
    await send_premium_message(context.bot, 
        chat_id,
        "✅ Media group saved. Choose an option:",
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("➕ Set Inline Button", callback_data=f"setbtng_{bot_id}")],
            [InlineKeyboardButton("Set More Messages", callback_data="setmsg_more")],
            [InlineKeyboardButton("❌ Cancel", callback_data="setmsg_cancel")],
            [InlineKeyboardButton("✅ Done", callback_data="setmsg_done")],
        ]),
    )


async def flush_media_group_job(context: ContextTypes.DEFAULT_TYPE):
    data = context.job.data or {}
    await flush_media_group(
        data.get("actor_uid"), data.get("managed_uid"),
        data.get("bot_id"), data.get("chat_id"), context, data.get("media_group_id"),
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
    bot_id = context.application.bot_data.get("bot_id")

    # Owner/admin replying to forwarded support message
    if msg.reply_to_message and owner_uid and (uid == owner_uid or uid in ADMIN_USER_IDS):
        key = f"{uid}:{bot_id}:{msg.reply_to_message.message_id}"
        target_uid = _get_support_uid(USERBOT_SUPPORT_REPLY_MAP, key)
        if target_uid:
            try:
                await context.bot.copy_message(
                    chat_id=target_uid, from_chat_id=msg.chat_id,
                    message_id=msg.message_id)
                if uid != ADMIN_USER_ID:
                    try:
                        await send_premium_message(context.bot, 
                            ADMIN_USER_ID,
                            f"↪️ Admin {uid} replied to user {target_uid}",
                            parse_mode=ParseMode.HTML)
                        await context.bot.copy_message(
                            chat_id=ADMIN_USER_ID, from_chat_id=msg.chat_id,
                            message_id=msg.message_id)
                    except Exception as mirror_err:
                        logging.error(f"support mirror failed: {mirror_err}")
                await send_ephemeral_reply(msg, f"✅ Reply delivered to user {target_uid}", 2)
            except Exception as ex:
                await reply_premium_message(msg, f"❌ Reply failed: {ex}", parse_mode=ParseMode.HTML)
            return

    # Public user message — relay to support
    if owner_uid and bot_id and uid not in ({owner_uid} | ADMIN_USER_IDS):
        try:
            inbox_ids = set(ADMIN_USER_IDS)
            inbox_ids.add(owner_uid)
            delivered = 0
            for aid in inbox_ids:
                try:
                    r = await context.bot.forward_message(
                        chat_id=aid, from_chat_id=msg.chat_id,
                        message_id=msg.message_id)
                    _store_support_map(USERBOT_SUPPORT_REPLY_MAP, f"{aid}:{bot_id}:{r.message_id}", uid)
                    delivered += 1
                except Exception as e2:
                    logging.error(f"userbot support relay failed to {aid}: {e2}")
            if delivered > 0:
                # Mark user as reachable so future broadcasts reach them
                try:
                    db.mark_reachable(owner_uid, bot_id, uid)
                except Exception:
                    pass
                await send_ephemeral_reply(msg,
                                           "✅ Message sent to support. Please wait for reply.", 2)
            else:
                await reply_premium_message(msg, "⚠️ Support unavailable right now. Try later.",
                                     parse_mode=ParseMode.HTML)
        except Exception as ex:
            logging.error(f"userbot support relay failed for {uid}: {ex}")
            await reply_premium_message(msg, "⚠️ Could not contact support right now.",
                                 parse_mode=ParseMode.HTML)
        return

    managed_uid = get_managed_user_id(context, uid)
    ud = _runtime_store(context, uid)
    extracted = MessageManager.extract_from_message(msg)

    # Get current bot_id from context or user_data
    current_bot_id = bot_id or context.user_data.get("current_bot_id", 0)
    
    # Adding channel flow
    if ud.get("adding_channel") or context.user_data.get("adding_channel"):
        add_bot_id = context.user_data.get("adding_channel_bot_id", current_bot_id)
        if (msg.forward_from_chat
                and msg.forward_from_chat.type in ['channel', 'group', 'supergroup']):
            ch = msg.forward_from_chat
            try:
                bot_info = db.get_user_bot(managed_uid, add_bot_id)
                if not bot_info:
                    await reply_premium_message(msg, "‼️ Bot not found. Please add bot first.",
                                         parse_mode=ParseMode.HTML)
                    return
                test_bot = Bot(token=bot_info[2])
                member = await test_bot.get_chat_member(ch.id, test_bot.id)
                if member.status not in ['administrator', 'creator']:
                    await reply_premium_message(msg, "‼️ Bot is not admin in that channel.",
                                         parse_mode=ParseMode.HTML)
                    return
                sub = db.get_subscription(managed_uid, add_bot_id)
                if sub:
                    max_ch = sub[4] or 1
                    current_ch = len(db.get_user_channels(managed_uid, add_bot_id))
                    if current_ch >= max_ch:
                        await reply_premium_message(msg, 
                            f"‼️ Channel limit reached ({max_ch}). Upgrade to Pro for more.",
                            parse_mode=ParseMode.HTML)
                        return
                db.add_channel(managed_uid, add_bot_id, ch.id,
                               getattr(ch, 'username', None), ch.title or "Channel")
                await sync_pending_join_requests_for_channel(managed_uid, add_bot_id, ch.id, test_bot)
                ud["adding_channel"] = False
                context.user_data["adding_channel"] = False
                await reply_premium_message(msg, 
                    "✅ Channel added!\n✨ Existing pending requests have been synced.",
                    parse_mode=ParseMode.HTML,
                    reply_markup=userbot_kb(managed_uid, add_bot_id, bot_info[3]))
            except Exception as ex:
                await reply_premium_message(msg, f"Error: {ex}", parse_mode=ParseMode.HTML)
        else:
            await reply_premium_message(msg, "🔽 Forward a message from the channel.",
                                 parse_mode=ParseMode.HTML)
        return

    # Edit text mode
    if ud.get("editing_text_msg_id"):
        mid = ud.pop("editing_text_msg_id")
        edit_bot_id = ud.get("editing_bot_id", current_bot_id)
        row = db.get_message_by_id(mid)
        if row and row[1] == managed_uid and row[2] == edit_bot_id:
            db.update_message_text(mid, extracted["text"], extracted["entities_json"])
            if extracted["emoji_map"]:
                db.save_user_emoji_map(managed_uid, mid, extracted["emoji_map"])
            await reply_premium_message(msg, "✅ Text updated.", parse_mode=ParseMode.HTML,
                                 reply_markup=userbot_kb(managed_uid, edit_bot_id, ""))
        return

    # Edit buttons mode
    if ud.get("editing_buttons_msg_id"):
        mid = ud.pop("editing_buttons_msg_id")
        edit_bot_id = ud.get("editing_bot_id", current_bot_id)
        row = db.get_message_by_id(mid)
        if row and row[1] == managed_uid and row[2] == edit_bot_id:
            btn_json = buttons_json_from_text(msg.text or "")
            db.update_message_buttons(mid, btn_json or "[]")
            await reply_premium_message(msg, "✅ Buttons updated.", parse_mode=ParseMode.HTML,
                                 reply_markup=userbot_kb(managed_uid, edit_bot_id, ""))
        return

    # Edit media mode
    if ud.get("editing_media_msg_id"):
        mid = ud.pop("editing_media_msg_id")
        edit_bot_id = ud.get("editing_bot_id", current_bot_id)
        row = db.get_message_by_id(mid)
        if row and row[1] == managed_uid and row[2] == edit_bot_id:
            db.update_message_media(mid, extracted["media_id"], extracted["media_type"],
                                    extracted["text"], extracted["entities_json"])
            if extracted["emoji_map"]:
                db.save_user_emoji_map(managed_uid, mid, extracted["emoji_map"])
            await reply_premium_message(msg, "✅ Media updated.", parse_mode=ParseMode.HTML,
                                 reply_markup=userbot_kb(managed_uid, edit_bot_id, ""))
        return

    # Setting messages
    setting_bot_id = context.user_data.get("setting_message_bot_id", current_bot_id)
    if ud.get("setting_message") or context.user_data.get("setting_message"):
        if msg.text and msg.text.strip().lower() in ["done", "/done", "finish", "stop", "complete"]:
            for key in ["setting_message", "messages", "pending_buttons", "waiting_buttons"]:
                ud.pop(key, None)
                context.user_data.pop(key, None)
            await reply_premium_message(msg, "✅ Messages saved successfully.",
                                 parse_mode=ParseMode.HTML,
                                 reply_markup=userbot_kb(managed_uid, setting_bot_id, ""))
            return
        channels = db.get_user_channels(managed_uid, setting_bot_id)
        if not channels:
            await reply_premium_message(msg, "‼️ No channel added yet.", parse_mode=ParseMode.HTML)
            return
        channel_id = channels[0][2]
        media_group_id = extracted["media_group_id"]

        if media_group_id:
            key = f"mg_{media_group_id}"
            arr = ud.get(key, [])
            first_item = len(arr) == 0
            arr.append({
                "text": extracted["text"],
                "media_id": extracted["media_id"],
                "media_type": extracted["media_type"],
                "entities_json": extracted["entities_json"],
                "emoji_map": extracted["emoji_map"],
            })
            ud[key] = arr
            if first_item:
                try:
                    await reply_premium_message(msg, "📸 Album received, processing...",
                                         parse_mode=ParseMode.HTML)
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
                flush_media_group_job, when=1.2,
                data={"actor_uid": uid, "managed_uid": managed_uid,
                      "bot_id": setting_bot_id,
                      "chat_id": msg.chat_id, "media_group_id": media_group_id},
            )
            ud[job_key] = j
            return

        msg_id = db.add_message(
            managed_uid, setting_bot_id, channel_id,
            extracted["text"], extracted["media_id"],
            extracted["media_type"], None, extracted["entities_json"]
        )
        # Save per-user emoji map for this message
        if msg_id and extracted["emoji_map"]:
            db.save_user_emoji_map(managed_uid, msg_id, extracted["emoji_map"])

        ud["pending_buttons"] = {"msg_id": msg_id, "channel_id": channel_id}
        await reply_premium_message(msg, 
            "✅ Message saved! Choose an option:",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("➕ Set Inline Button",
                                      callback_data=f"setbtn_{msg_id}")],
                [InlineKeyboardButton("➡️ Set More Messages", callback_data="setmsg_more")],
                [InlineKeyboardButton("❌ Cancel", callback_data="setmsg_cancel")],
                [InlineKeyboardButton("✅ Done", callback_data="setmsg_done")],
            ]))
        return

    # Broadcast stage
    if context.user_data.get("broadcast_stage") == "await_message":
        broadcast_bot_id = context.user_data.get("broadcast_bot_id", current_bot_id)
        draft = {
            "text": extracted["text"],
            "media": extracted["media_id"],
            "media_type": extracted["media_type"],
            "emoji_map": extracted["emoji_map"],
            "entities_json": extracted["entities_json"],
            "bot_id": broadcast_bot_id,
        }
        context.user_data["broadcast_draft"] = draft
        context.user_data["broadcast_stage"] = "buttons_or_send"
        await reply_premium_message(msg, 
            "✅ Broadcast draft saved. Add inline buttons or send now?",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("➕ Add Inline Buttons", callback_data=f"bcast_add_btns_{broadcast_bot_id}")],
                [InlineKeyboardButton("🚀 Send Now", callback_data=f"bcast_send_{broadcast_bot_id}")],
                [InlineKeyboardButton("❌ Cancel", callback_data=f"setup_bot_{broadcast_bot_id}")],
            ]))
        return

    if context.user_data.get("broadcast_stage") == "await_buttons":
        draft = context.user_data.get("broadcast_draft", {})
        btn_json = buttons_json_from_text(msg.text or "")
        if btn_json:
            draft["buttons_json"] = btn_json
            context.user_data["broadcast_draft"] = draft
            await reply_premium_message(msg, 
                "✅ Buttons saved. Ready to send?",
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🚀 Send Now", callback_data="bcast_send")],
                    [InlineKeyboardButton("❌ Cancel", callback_data=f"setup_bot_{draft.get('bot_id', 0)}")],
                ]))
        else:
            await reply_premium_message(msg, 
                "‼️ No valid buttons. Use: <code>Text|https://link</code> per line.",
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup(
                    [[InlineKeyboardButton("🔙 Back", callback_data=f"setup_bot_{draft.get('bot_id', 0)}")]]))
        context.user_data["broadcast_stage"] = "buttons_or_send"
        return

    await reply_premium_message(msg, "🔽 Use buttons to manage.", parse_mode=ParseMode.HTML,
                         reply_markup=main_menu_kb(uid))


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
    if data.startswith("setbtng_"):
        parts = data.split("_")
        bot_id = int(parts[1]) if len(parts) > 1 else 0
        grp = ud.get("pending_buttons_group")
        if not grp or not grp.get("msg_ids"):
            await safe_edit_message_text(q, "‼️ Group not found. Send media group again.",
                                      parse_mode=ParseMode.HTML,
                                      reply_markup=userbot_kb(uid, bot_id, ""))
            return
        ud["waiting_buttons"] = {"msg_ids": grp.get("msg_ids")}
        await safe_edit_message_text(q, 
            "🔘 Send button lines (Text|https://link per line):",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("🔙 Back", callback_data=f"setup_bot_{bot_id}")]]))
        return
    if not data.startswith("setbtn_"):
        return
    msg_id = int(data.split("_")[1])
    ud["waiting_buttons"] = {"msg_id": msg_id}
    await safe_edit_message_text(q, 
        "🔘 Send button lines (Text|https://link per line):",
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(
            [[InlineKeyboardButton("🔙 Back", callback_data="main_menu")]]))


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
    bot_id = channel_row[1]
    auto_raw = channel_row[8] if len(channel_row) > 8 else 0
    auto = bool(int(auto_raw)) if auto_raw is not None else False

    msgs = db.get_messages(owner_uid, bot_id, chat.id)
    bot_data = db.get_user_bot(owner_uid, bot_id)
    bot_instance = Bot(token=bot_data[2]) if bot_data else None
    
    try:
        if msgs:
            await _send_messages_with_media_groups(
                requester.id, msgs, context,
                owner_uid=owner_uid, bot_id=bot_id, attach_start_button=True)
        else:
            wm = channel_row[5] or ""
            wid = channel_row[6]
            wtype = channel_row[7]
            markup = buttons_to_markup(buttons_json_from_text(wm) or None)
            if wid and wtype:
                await send_media(bot_instance, requester.id, wid, wtype, wm, markup)
            elif wm:
                await send_premium_message(bot_instance, 
                    requester.id, wm, parse_mode=ParseMode.HTML, reply_markup=markup)
        db.mark_reachable(owner_uid, bot_id, requester.id)
    except Exception as ex:
        logging.error(f"Send welcome error: {ex}")

    db.add_join_request(owner_uid, bot_id, requester.id, chat.id, 'approved' if auto else 'pending')
    if auto and bot_instance:
        try:
            await jr.approve()
        except Exception as ex:
            logging.error(f"Approve error: {ex}")


# ================= USER BOT LIFECYCLE =================
async def start_user_bot(token: str, owner_id: int, bot_id: int):
    app = (ApplicationBuilder()
           .token(token)
           .concurrent_updates(True)
           .connect_timeout(30)
           .read_timeout(30)
           .write_timeout(30)
           .pool_timeout(30)
           .build())
    app.bot_data["owner_id"] = owner_id
    app.bot_data["bot_id"] = bot_id
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
    user_bot_applications[(owner_id, bot_id)] = app
    try:
        for ch in db.get_user_channels(owner_id, bot_id) or []:
            bot_instance = Bot(token=token)
            await sync_pending_join_requests_for_channel(owner_id, bot_id, ch[2], bot_instance)
    except Exception as ex:
        logging.error(f"Startup pending sync failed for owner {owner_id} bot {bot_id}: {ex}")
    return True


async def stop_user_bot(owner_id: int, bot_id: int):
    key = (owner_id, bot_id)
    if key in user_bot_applications:
        try:
            app = user_bot_applications[key]
            await app.updater.stop()
            await app.stop()
            await app.shutdown()
            logging.info(f"🛑 Stopped user bot for {owner_id}/{bot_id}")
        except Exception as ex:
            logging.error(f"Error stopping user bot {owner_id}/{bot_id}: {ex}")
        user_bot_applications.pop(key, None)
        db.set_user_bot_active(owner_id, bot_id, False)


# ================= ADMIN PANEL FUNCTIONS =================
async def show_admin_userbot_control(q, context: ContextTypes.DEFAULT_TYPE):
    bots = db.get_all_user_bots() or []
    if not bots:
        await safe_edit_message_text(q, 
            f"{pe('‼️')} No user bots found.",
            parse_mode=ParseMode.HTML,
            reply_markup=admin_kb(),
        )
        return

    running_count = sum(1 for uid, bid, *_ in bots if (uid, bid) in user_bot_applications)
    premium_count = sum(1 for uid, bid, *_ in bots if db.get_subscription(uid, bid))
    stopped_count = max(len(bots) - running_count, 0)

    lines = [
        f"<blockquote>{pe('💎')} <b>USERBOT CONTROL CENTER</b></blockquote>",
        "",
        f"{pe('📊')} <b>Total Bots:</b> {len(bots)} | {pe('🟢')} <b>Running:</b> {running_count}",
        f"{pe('⭐️')} <b>Premium:</b> {premium_count} | {pe('🔴')} <b>Stopped:</b> {stopped_count}",
        "",
        f"{pe('📌')} <b>Bot List</b>",
    ]

    for uid, bid, token, buser, active in bots:
        is_running = (uid, bid) in user_bot_applications
        sub = db.get_subscription(uid, bid)
        status_icon = pe('🟢') if is_running else pe('🔴')
        status_text = "Running" if is_running else "Stopped"
        plan_text = sub[2] if sub else "No active plan"
        plan_icon = pe('⭐️') if sub else pe('❌')
        lines.append(
            f"{status_icon} <b>@{buser or 'N/A'}</b>\n"
            f"   <code>{uid}/{bid}</code> • {status_text} • {plan_icon} {plan_text}"
        )

    kb = []
    for uid, bid, token, buser, active in bots:
        is_running = (uid, bid) in user_bot_applications
        row = []
        if is_running:
            row.append(InlineKeyboardButton(
                f"Stop @{buser or uid}", callback_data=f"admin_ub_stop_{uid}_{bid}"))
        else:
            row.append(InlineKeyboardButton(
                f"Start @{buser or uid}", callback_data=f"admin_ub_start_{uid}_{bid}"))
        row.append(InlineKeyboardButton("Info", callback_data=f"admin_ub_info_{uid}_{bid}"))
        kb.append(row)
    kb.append([InlineKeyboardButton("Start All", callback_data="admin_start_all"),
               InlineKeyboardButton("Stop All", callback_data="admin_stop_all")])
    kb.append([InlineKeyboardButton("Refresh", callback_data="admin_userbots")])
    kb.append([InlineKeyboardButton("Back to Admin", callback_data="admin_panel")])
    await safe_edit_message_text(q, 
        "\n".join(lines)[:4000],
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(kb)
    )


async def show_admin_ub_info(q, uid_target: int, bot_id_target: int, context: ContextTypes.DEFAULT_TYPE):
    bot_data = db.get_user_bot(uid_target, bot_id_target)
    sub = db.get_subscription(uid_target, bot_id_target)
    user_doc = db.mongo.users.find_one({"user_id": uid_target}) or {}
    is_running = (uid_target, bot_id_target) in user_bot_applications
    uname = user_doc.get("username", "")
    fname = user_doc.get("first_name", "")
    lines = [f"<blockquote>🔎 <b>USERBOT INFO</b></blockquote>\n"]
    lines.append(f"👤 <b>User:</b> {fname} {'@'+uname if uname else ''} ({uid_target})")
    lines.append(f"🤖 <b>Bot ID:</b> {bot_id_target}")
    if bot_data:
        lines.append(f"🤖 <b>Bot:</b> @{bot_data[3] or 'N/A'}")
        lines.append(f"⚡️ <b>Running:</b> {'🟢 Yes' if is_running else '🔴 No'}")
    else:
        lines.append("🤖 <b>Bot:</b> Not configured")
    if sub:
        try:
            exp = datetime.fromisoformat(sub[3])
            days_left = (exp - datetime.now()).days
            lines.append(f"⭐️ <b>Plan:</b> {sub[2]}")
            lines.append(f"📅 <b>Expiry:</b> {exp.strftime('%d %b %Y')} ({days_left}d left)")
        except Exception:
            lines.append(f"⭐️ <b>Plan:</b> {sub[2] if sub else 'None'}")
    else:
        lines.append("⭐️ <b>Subscription:</b> None")
    channels = db.get_user_channels(uid_target, bot_id_target) or []
    total_users = db.get_total_requesters_count(uid_target, bot_id_target)
    reachable = db.get_reachable_requesters_count(uid_target, bot_id_target)
    lines.append(f"✈️ <b>Channels:</b> {len(channels)}")
    lines.append(f"👥 <b>Total Users:</b> {total_users} | <b>Reachable:</b> {reachable}")
    kb = []
    if bot_data:
        if is_running:
            kb.append([InlineKeyboardButton("🛑 Stop Bot",
                                             callback_data=f"admin_ub_stop_{uid_target}_{bot_id_target}")])
        else:
            kb.append([InlineKeyboardButton("▶️ Start Bot",
                                             callback_data=f"admin_ub_start_{uid_target}_{bot_id_target}")])
        kb.append([InlineKeyboardButton("🗑️ Remove Bot",
                                         callback_data=f"admin_remove_bot_{uid_target}_{bot_id_target}")])
    kb.append([InlineKeyboardButton("🔙 Back", callback_data="admin_userbots")])
    await safe_edit_message_text(q, 
        "\n".join(lines), parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup(kb))


async def show_all_users(q):
    users = db.get_all_users()
    lines = [f"<blockquote>📇 <b>ALL USERS ({len(users)})</b></blockquote>\n"]
    for u in users:
        uid, uname, fname = u[0], u[1], u[2]
        lines.append(f"• {uid} @{uname or 'N/A'} {fname or ''}")
    text = "\n".join(lines)
    await safe_edit_message_text(q, text[:4000], parse_mode=ParseMode.HTML, reply_markup=admin_kb())


async def show_admin_sub_list(q, context: ContextTypes.DEFAULT_TYPE, page: int = 0):
    subs = db.get_all_subscriptions()
    page_size = 10
    total_pages = max(1, (len(subs) + page_size - 1) // page_size)
    page_subs = subs[page * page_size:(page + 1) * page_size]
    lines = [f"<blockquote>📋 <b>SUBSCRIPTION LIST</b> (Page {page+1}/{total_pages})</blockquote>\n"]
    for s in page_subs:
        exp = s.get("expiry_date", "")[:10]
        lines.append(
            f"• {s['user_id']}/{s['bot_id']} @{s['username'] or 'N/A'} — {s['subscription_type']} "
            f"— {exp} — {'🟢' if s['bot_active'] else '🔴'}")
    kb = []
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton("⬅️ Prev", callback_data=f"admin_sublist_pg_{page-1}"))
    if page < total_pages - 1:
        nav.append(InlineKeyboardButton("➡️ Next", callback_data=f"admin_sublist_pg_{page+1}"))
    if nav:
        kb.append(nav)
    kb.append([InlineKeyboardButton("🔙 Back to Admin", callback_data="admin_panel")])
    await safe_edit_message_text(q, "\n".join(lines)[:4000], parse_mode=ParseMode.HTML,
                              reply_markup=InlineKeyboardMarkup(kb))


async def start_all_userbots(q):
    bots = db.get_all_user_bots() or []
    started = 0
    failed = 0
    skipped = 0
    for uid, bid, token, buser, active in bots:
        key = (uid, bid)
        if key in user_bot_applications:
            skipped += 1
            continue
        sub = db.get_subscription(uid, bid)
        if not sub:
            continue
        try:
            exp = datetime.fromisoformat(sub[3])
            if exp < datetime.now():
                continue
        except Exception:
            continue
        try:
            await start_user_bot(token, uid, bid)
            db.set_user_bot_active(uid, bid, True)
            started += 1
        except Exception as ex:
            logging.error(f"start_all error {uid}/{bid}: {ex}")
            failed += 1
    await safe_edit_message_text(q, 
        (f"<blockquote>🚀 <b>START ALL COMPLETE</b></blockquote>\n\n"
         f"✅ Started: {started}\n"
         f"⏭️ Already running: {skipped}\n"
         f"❌ Failed: {failed}"),
        parse_mode=ParseMode.HTML,
        reply_markup=admin_kb())


async def check_expiry(q):
    subs = db.get_all_subscriptions()
    now = datetime.now()
    expiring = []
    for s in subs:
        if s.get("expiry_date"):
            try:
                exp = datetime.fromisoformat(s["expiry_date"])
                days = (exp - now).days
                if days <= 7:
                    expiring.append(f"@{s['username'] or 'N/A'} ({s['user_id']}/{s['bot_id']}) — {days} days left")
            except Exception:
                pass
    text = (f"<blockquote>⏰ <b>EXPIRING SOON</b></blockquote>\n\n"
            + ("\n".join(expiring) if expiring else "None expiring within 7 days."))
    await safe_edit_message_text(q, text, parse_mode=ParseMode.HTML, reply_markup=admin_kb())


async def show_stats(q):
    users = db.get_all_users()
    bots = db.get_all_user_bots()
    subs = db.get_all_subscriptions()
    running = len(user_bot_applications)
    userbot_counts = db.get_userbot_user_counts()
    total_userbot_users = sum(row[3] for row in userbot_counts)
    count_lines = []
    for owner_id, bot_id, bot_username, user_count in userbot_counts[:25]:
        status = pe('🟢') if (owner_id, bot_id) in user_bot_applications else pe('🔴')
        count_lines.append(
            f"{status} <b>@{bot_username or 'N/A'}</b> — <code>{owner_id}/{bot_id}</code> — <b>{user_count}</b> users"
        )
    if len(userbot_counts) > 25:
        count_lines.append(f"<i>…and {len(userbot_counts) - 25} more userbots</i>")
    per_bot_text = "\n".join(count_lines) if count_lines else "No userbots found."
    text = (
        f"<blockquote>{pe('📊')} <b>SYSTEM STATS</b></blockquote>\n\n"
        f"{pe('👀')} <b>Main Bot Users:</b> {len(users)}\n"
        f"{pe('💎')} <b>Total UserBots:</b> {len(bots)}\n"
        f"{pe('🟢')} <b>Running UserBots:</b> {running}\n"
        f"{pe('⭐️')} <b>Active Subscriptions:</b> {len(subs)}\n"
        f"{pe('📌')} <b>Total UserBot Users:</b> {total_userbot_users}\n\n"
        f"<b>UserBot Wise Users</b>\n"
        f"{per_bot_text}"
    )
    await safe_edit_message_text(q, text[:4000], parse_mode=ParseMode.HTML, reply_markup=admin_kb())


# ================= BROADCAST FUNCTIONS =================
async def preview_user_broadcast(q, context: ContextTypes.DEFAULT_TYPE):
    draft = context.user_data.get("broadcast_draft", {})
    uid = q.from_user.id
    if not draft:
        await safe_edit_message_text(q, "‼️ No draft found.", parse_mode=ParseMode.HTML,
                                  reply_markup=main_menu_kb(uid))
        return
    bot_id = draft.get("bot_id", 0)
    ej = draft.get("entities_json")
    btns = buttons_to_markup(draft.get("buttons_json")) if draft.get("buttons_json") else None
    try:
        await send_media(context, uid, draft.get("media"),
                         draft.get("media_type") or "text",
                         draft.get("text", ""), btns, entities_json=ej)
    except Exception as ex:
        logging.error(f"Preview send error: {ex}")
        await safe_edit_message_text(q, f"‼️ Preview failed: {str(ex)}", parse_mode=ParseMode.HTML,
                                  reply_markup=userbot_kb(uid, bot_id, ""))
        return
    await safe_edit_message_text(q, 
        "✅ Preview sent above. Confirm to broadcast?",
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("✅ Confirm Send", callback_data=f"bcast_confirm_{bot_id}")],
            [InlineKeyboardButton("❌ Cancel", callback_data=f"setup_bot_{bot_id}")],
        ]),
    )


async def send_user_broadcast(q, context: ContextTypes.DEFAULT_TYPE):
    uid = q.from_user.id
    draft = context.user_data.get("broadcast_draft", {})
    bot_id = draft.get("bot_id", 0)
    context.user_data.pop("broadcast_stage", None)
    if not draft:
        await safe_edit_message_text(q, "‼️ No draft to send.", parse_mode=ParseMode.HTML,
                                  reply_markup=userbot_kb(uid, bot_id, ""))
        return
    reqs = db.get_requesters_for_owner(uid, bot_id)
    if not reqs:
        await safe_edit_message_text(q, "‼️ No users to broadcast to.", parse_mode=ParseMode.HTML,
                                  reply_markup=userbot_kb(uid, bot_id, ""))
        return
    ej = draft.get("entities_json")
    btns = buttons_to_markup(draft.get("buttons_json")) if draft.get("buttons_json") else None
    await safe_edit_message_text(q, "✈️ Broadcasting...", parse_mode=ParseMode.HTML,
                              reply_markup=InlineKeyboardMarkup(
                                  [[InlineKeyboardButton("🔙 Back", callback_data=f"setup_bot_{bot_id}")]]))
    bot_data = db.get_user_bot(uid, bot_id)
    bot_instance = None
    if bot_data:
        bot_instance = Bot(token=bot_data[2])
    sent = 0
    fail = 0
    for r in reqs:
        try:
            await send_media(bot_instance, r, draft.get("media"),
                             draft.get("media_type") or "text",
                             draft.get("text", ""), btns, entities_json=ej)
            db.mark_reachable(uid, bot_id, r)
            sent += 1
        except Exception as ex:
            emsg = str(ex).lower()
            if ("bot was blocked by the user" in emsg
                    or "bot can't initiate conversation with a user" in emsg):
                db.mark_unreachable(uid, bot_id, r)
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
            UIFormatter.broadcast_confirm(sent, fail),
            parse_mode=ParseMode.HTML,
            reply_markup=userbot_kb(uid, bot_id, ""))
    except Exception:
        pass
    context.user_data.pop("broadcast_draft", None)


async def preview_admin_broadcast(q, context: ContextTypes.DEFAULT_TYPE):
    draft = context.user_data.get("admin_broadcast_draft", {})
    if not draft:
        await safe_edit_message_text(q, "‼️ No draft found.", parse_mode=ParseMode.HTML,
                                  reply_markup=admin_kb())
        return
    ej = draft.get("entities_json")
    btns = buttons_to_markup(draft.get("buttons_json")) if draft.get("buttons_json") else None
    uid = q.from_user.id
    try:
        await send_media(context, uid, draft.get("media"),
                         draft.get("media_type") or "text",
                         draft.get("text", ""), btns, entities_json=ej)
    except Exception as ex:
        logging.error(f"Admin preview send error: {ex}")
        await safe_edit_message_text(q, f"‼️ Preview failed: {str(ex)}", parse_mode=ParseMode.HTML,
                                  reply_markup=admin_kb())
        return
    target = draft.get("target_uid")
    target_label = f"userbot {target[0]}/{target[1]}" if target else "ALL userbots"
    await safe_edit_message_text(q, 
        f"✅ Preview sent above.\n✈️ Confirm broadcast to <b>{target_label}</b>?",
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
        await safe_edit_message_text(q, "‼️ No draft to send.", parse_mode=ParseMode.HTML,
                                  reply_markup=admin_kb())
        return
    bots = db.get_all_user_bots()
    target = draft.get("target_uid")
    if target:
        bots = [b for b in bots if b[0] == target[0] and b[1] == target[1]]
    ej = draft.get("entities_json")
    btns = buttons_to_markup(draft.get("buttons_json")) if draft.get("buttons_json") else None
    await safe_edit_message_text(q, "✈️ Admin broadcast started...", parse_mode=ParseMode.HTML,
                              reply_markup=admin_kb())
    total_sent = 0
    total_fail = 0
    for uid, bid, token, buser, active in bots:
        sub = db.get_subscription(uid, bid)
        if not sub:
            continue
        try:
            expiry = datetime.fromisoformat(sub[3])
            if expiry < datetime.now():
                continue
        except Exception:
            continue
        key = (uid, bid)
        if key in user_bot_applications:
            bot_instance = user_bot_applications[key].bot
        else:
            try:
                bot_instance = Bot(token=token)
            except Exception:
                continue
        recipients = db.get_requesters_for_owner(uid, bid)
        for r in recipients:
            try:
                await send_media(bot_instance, r, draft.get("media"),
                                 draft.get("media_type") or "text",
                                 draft.get("text", ""), btns, entities_json=ej)
                db.mark_reachable(uid, bid, r)
                total_sent += 1
            except Exception as ex:
                emsg = str(ex).lower()
                if ("bot was blocked by the user" in emsg
                        or "bot can't initiate conversation with a user" in emsg):
                    db.mark_unreachable(uid, bid, r)
                logging.error(f"admin broadcast via @{buser} to {r}: {ex}")
                total_fail += 1
            if (total_sent + total_fail) % 50 == 0:
                try:
                    await q.message.edit_text(
                        f"✈️ Admin broadcast: Sent {total_sent}, Failed {total_fail}",
                        parse_mode=ParseMode.HTML)
                except Exception:
                    pass
    target_label = f"userbot {target[0]}/{target[1]}" if target else "ALL userbots"
    try:
        await q.message.edit_text(
            (f"<blockquote>✅ <b>ADMIN BROADCAST COMPLETE</b></blockquote>\n\n"
             f"📤 Target: {target_label}\n"
             f"✅ Sent: {total_sent}\n"
             f"❌ Failed: {total_fail}"),
            parse_mode=ParseMode.HTML,
            reply_markup=admin_kb())
    except Exception:
        pass
    context.user_data.pop("admin_broadcast_draft", None)


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
    if data.startswith("setbtng_"):
        parts = data.split("_")
        bot_id = int(parts[1]) if len(parts) > 1 else 0
        grp = ud.get("pending_buttons_group")
        if not grp or not grp.get("msg_ids"):
            await safe_edit_message_text(q, "‼️ Group not found. Send media group again.",
                                      parse_mode=ParseMode.HTML,
                                      reply_markup=userbot_kb(uid, bot_id, ""))
            return
        ud["waiting_buttons"] = {"msg_ids": grp.get("msg_ids")}
        await safe_edit_message_text(q, 
            "🔘 Send button lines (Text|https://link per line):",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("🔙 Back", callback_data=f"setup_bot_{bot_id}")]]))
        return
    if not data.startswith("setbtn_"):
        return
    msg_id = int(data.split("_")[1])
    ud["waiting_buttons"] = {"msg_id": msg_id}
    await safe_edit_message_text(q, 
        "🔘 Send button lines (Text|https://link per line):",
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(
            [[InlineKeyboardButton("🔙 Back", callback_data="main_menu")]]))


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
    bot_id = channel_row[1]
    auto_raw = channel_row[8] if len(channel_row) > 8 else 0
    auto = bool(int(auto_raw)) if auto_raw is not None else False

    msgs = db.get_messages(owner_uid, bot_id, chat.id)
    bot_data = db.get_user_bot(owner_uid, bot_id)
    bot_instance = Bot(token=bot_data[2]) if bot_data else None
    
    try:
        if msgs:
            await _send_messages_with_media_groups(
                requester.id, msgs, context,
                owner_uid=owner_uid, bot_id=bot_id, attach_start_button=True)
        else:
            wm = channel_row[5] or ""
            wid = channel_row[6]
            wtype = channel_row[7]
            markup = buttons_to_markup(buttons_json_from_text(wm) or None)
            if wid and wtype:
                await send_media(bot_instance, requester.id, wid, wtype, wm, markup)
            elif wm:
                await send_premium_message(bot_instance, 
                    requester.id, wm, parse_mode=ParseMode.HTML, reply_markup=markup)
        db.mark_reachable(owner_uid, bot_id, requester.id)
    except Exception as ex:
        logging.error(f"Send welcome error: {ex}")

    db.add_join_request(owner_uid, bot_id, requester.id, chat.id, 'approved' if auto else 'pending')
    if auto and bot_instance:
        try:
            await jr.approve()
        except Exception as ex:
            logging.error(f"Approve error: {ex}")


# ================= MAIN MESSAGE HANDLER =================
async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not user:
        return
    msg = update.message
    if not msg:
        return

    extracted = MessageManager.extract_from_message(msg)

    # Waiting for bot token (add new bot)
    if context.user_data.get("waiting_token_for_bot") and not is_admin(user.id):
        token_text = (msg.text or "").strip()
        if ":" in token_text and len(token_text) > 10:
            try:
                test_bot = Bot(token=token_text)
                bot_info = await test_bot.get_me()
                # Get next bot_id for this user
                user_bots = db.get_user_bots(user.id)
                next_bot_id = max([b[1] for b in user_bots], default=0) + 1
                db.add_user_bot(user.id, next_bot_id, token_text, bot_info.username)
                context.user_data.pop("waiting_token_for_bot", None)
                
                # Check subscription
                sub = db.get_subscription(user.id, next_bot_id)
                if sub:
                    try:
                        await start_user_bot(token_text, user.id, next_bot_id)
                        db.set_user_bot_active(user.id, next_bot_id, True)
                        await reply_premium_message(msg, 
                            (f"<blockquote>✅ <b>BOT LINKED SUCCESSFULLY</b></blockquote>\n\n"
                             f"🤖 @{bot_info.username} is now running!\n\n"
                             f"Use the panel to configure channels and messages."),
                            parse_mode=ParseMode.HTML,
                            reply_markup=bot_manage_kb(user.id, next_bot_id, bot_info.username, True))
                    except Exception as start_ex:
                        await reply_premium_message(msg, 
                            (f"✅ Bot token saved but failed to start: {start_ex}\n"
                             "Contact admin for help."),
                            parse_mode=ParseMode.HTML,
                            reply_markup=my_bots_kb(user.id))
                else:
                    await reply_premium_message(msg, 
                        (f"<blockquote>✅ <b>BOT TOKEN SAVED</b></blockquote>\n\n"
                         f"🤖 @{bot_info.username}\n\n"
                         f"⚠️ You need a subscription to activate your bot.\n"
                         f"📞 Contact {ADMIN_USERNAME} to subscribe."),
                        parse_mode=ParseMode.HTML,
                        reply_markup=my_bots_kb(user.id))
            except Exception as ex:
                await reply_premium_message(msg, 
                    f"‼️ Invalid token or bot error: {ex}\n\nPlease try again.",
                    parse_mode=ParseMode.HTML)
        else:
            await reply_premium_message(msg, "‼️ Invalid token format. Please send the correct BotFather token.",
                                  parse_mode=ParseMode.HTML)
        return

    # Admin: add userbot via admin panel
    if context.user_data.get("admin_add_userbot") and is_admin(user.id):
        parts = (msg.text or "").strip().split()
        if len(parts) == 2 and parts[0].isdigit():
            target = int(parts[0])
            token = parts[1]
            try:
                test_bot = Bot(token=token)
                bot_info = await test_bot.get_me()
                db.add_user(target, None, f"User{target}", None)
                user_bots = db.get_user_bots(target)
                next_bot_id = max([b[1] for b in user_bots], default=0) + 1
                db.add_user_bot(target, next_bot_id, token, bot_info.username)
                context.user_data.pop("admin_add_userbot", None)
                await reply_premium_message(msg, 
                    f"✅ Bot @{bot_info.username} (ID:{next_bot_id}) linked to user {target}",
                    parse_mode=ParseMode.HTML, reply_markup=admin_kb())
            except Exception as ex:
                await reply_premium_message(msg, f"‼️ Error: {ex}", parse_mode=ParseMode.HTML,
                                     reply_markup=admin_kb())
        else:
            await reply_premium_message(msg, "‼️ Format: <code>user_id bot_token</code>",
                                  parse_mode=ParseMode.HTML, reply_markup=admin_kb())
        return

    # Admin: remove subscription
    if context.user_data.get("admin_remove_sub_mode") and is_admin(user.id):
        parts = (msg.text or "").strip().split()
        if len(parts) >= 2 and parts[0].isdigit() and parts[1].isdigit():
            target = int(parts[0])
            bot_id = int(parts[1])
            db.remove_subscription(target, bot_id)
            key = (target, bot_id)
            if key in user_bot_applications:
                await stop_user_bot(target, bot_id)
            context.user_data.pop("admin_remove_sub_mode", None)
            await reply_premium_message(msg, 
                f"✅ Subscription removed for user {target} bot {bot_id}",
                parse_mode=ParseMode.HTML, reply_markup=admin_kb())
        else:
            await reply_premium_message(msg, "‼️ Format: <code>user_id bot_id</code>",
                                  parse_mode=ParseMode.HTML, reply_markup=admin_kb())
        return

    # Admin: add subscription to specific user
    if context.user_data.get("admin_add_sub_to_user") and is_admin(user.id):
        parts = (msg.text or "").strip().split()
        if len(parts) >= 4 and parts[0].isdigit() and parts[1].isdigit() and parts[2].isdigit():
            target = int(parts[0])
            bot_id = int(parts[1])
            days = int(parts[2])
            plan = parts[3].capitalize()
            if plan not in ("Basic", "Pro"):
                await reply_premium_message(msg, "‼️ Plan must be Basic or Pro.",
                                      parse_mode=ParseMode.HTML, reply_markup=admin_kb())
                context.user_data.pop("admin_add_sub_to_user", None)
                return
            try:
                db.add_subscription(target, bot_id, plan, days)
                context.user_data.pop("admin_add_sub_to_user", None)
                bot_started = False
                bot_data = db.get_user_bot(target, bot_id)
                key = (target, bot_id)
                if bot_data and key not in user_bot_applications:
                    try:
                        await start_user_bot(bot_data[2], target, bot_id)
                        db.set_user_bot_active(target, bot_id, True)
                        bot_started = True
                    except Exception as start_ex:
                        logging.error(f"Auto-start failed for {target}/{bot_id}: {start_ex}")
                try:
                    bot_notice = ""
                    if bot_data and bot_started:
                        bot_notice = f"\n🤖 Bot @{bot_data[3]} auto-restarted!"
                    elif bot_data:
                        bot_notice = "\n⚠️ Bot restart failed. Contact admin."
                    else:
                        bot_notice = "\n📌 Add bot token to activate."
                    await send_premium_message(context.bot, 
                        target,
                        (f"<blockquote>✅ <b>SUBSCRIPTION ACTIVATED</b></blockquote>\n\n"
                         f"⭐️ Bot ID: {bot_id}\n"
                         f"📋 Plan: {plan}\n"
                         f"📅 Duration: {days} days{bot_notice}\n\n"
                         f"Use /start to access your panel."),
                        parse_mode=ParseMode.HTML)
                except Exception as e:
                    logging.error(f"Failed to notify user {target}: {e}")
                status_msg = f"✅ Added {plan} {days}d for {target}/{bot_id}"
                if bot_data and bot_started:
                    status_msg += f"\n🚀 Bot @{bot_data[3]} auto-started!"
                await reply_premium_message(msg, status_msg, parse_mode=ParseMode.HTML, reply_markup=admin_kb())
            except Exception as ex:
                await reply_premium_message(msg, f"‼️ Error: {ex}", parse_mode=ParseMode.HTML,
                                     reply_markup=admin_kb())
        else:
            await reply_premium_message(msg, "‼️ Format: <code>user_id bot_id days Plan</code>",
                                  parse_mode=ParseMode.HTML, reply_markup=admin_kb())
        context.user_data.pop("admin_add_sub_to_user", None)
        return

    # Admin: add subscription (old format with single bot)
    if context.user_data.get("admin_add_sub") and is_admin(user.id):
        parts = (msg.text or "").strip().split()
        if len(parts) >= 3 and parts[0].isdigit() and parts[1].isdigit():
            target = int(parts[0])
            bot_id = int(parts[1])
            days = int(parts[2])
            plan = parts[3].capitalize() if len(parts) > 3 else "Basic"
            if plan not in ("Basic", "Pro"):
                plan = "Basic"
            try:
                db.add_subscription(target, bot_id, plan, days)
                context.user_data.pop("admin_add_sub", None)
                bot_started = False
                bot_data = db.get_user_bot(target, bot_id)
                key = (target, bot_id)
                if bot_data and key not in user_bot_applications:
                    try:
                        await start_user_bot(bot_data[2], target, bot_id)
                        db.set_user_bot_active(target, bot_id, True)
                        bot_started = True
                    except Exception as start_ex:
                        logging.error(f"Auto-start failed for {target}/{bot_id}: {start_ex}")
                try:
                    bot_notice = ""
                    if bot_data and bot_started:
                        bot_notice = f"\n🤖 Bot @{bot_data[3]} auto-restarted!"
                    elif bot_data:
                        bot_notice = "\n⚠️ Bot restart failed. Contact admin."
                    else:
                        bot_notice = "\n📌 Add bot token to activate."
                    await send_premium_message(context.bot, 
                        target,
                        (f"<blockquote>✅ <b>SUBSCRIPTION ACTIVATED</b></blockquote>\n\n"
                         f"⭐️ Bot ID: {bot_id}\n"
                         f"📋 Plan: {plan}\n"
                         f"📅 Duration: {days} days{bot_notice}\n\n"
                         f"Use /start to access your panel."),
                        parse_mode=ParseMode.HTML)
                except Exception as e:
                    logging.error(f"Failed to notify user {target}: {e}")
                status_msg = f"✅ Added {plan} {days}d for {target}/{bot_id}"
                if bot_data and bot_started:
                    status_msg += f"\n🚀 Bot @{bot_data[3]} auto-started!"
                await reply_premium_message(msg, status_msg, parse_mode=ParseMode.HTML, reply_markup=admin_kb())
            except Exception as ex:
                await reply_premium_message(msg, f"‼️ Error: {ex}", parse_mode=ParseMode.HTML,
                                     reply_markup=admin_kb())
        else:
            await reply_premium_message(msg, "‼️ Format: <code>user_id bot_id days plan</code>",
                                  parse_mode=ParseMode.HTML, reply_markup=admin_kb())
        context.user_data.pop("admin_add_sub", None)
        return

    # Admin broadcast stage: await buttons
    if context.user_data.get("admin_broadcast_stage") == "await_buttons" and is_admin(user.id):
        draft = context.user_data.get("admin_broadcast_draft", {})
        btn_json = buttons_json_from_text(msg.text or "")
        if btn_json:
            draft["buttons_json"] = btn_json
            context.user_data["admin_broadcast_draft"] = draft
            await reply_premium_message(msg, 
                "✅ Buttons saved. Ready to send?",
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🚀 Send Now", callback_data="admin_bcast_send")],
                    [InlineKeyboardButton("❌ Cancel", callback_data="admin_panel")],
                ]))
        else:
            await reply_premium_message(msg, "‼️ No valid buttons (use Text|https://link).",
                                  parse_mode=ParseMode.HTML, reply_markup=admin_kb())
        context.user_data["admin_broadcast_stage"] = "await_send"
        return

    # Admin broadcast: capture message
    if context.user_data.get("admin_broadcast") and is_admin(user.id):
        target = context.user_data.get("admin_broadcast_target")
        draft = {
            "text": extracted["text"],
            "media": extracted["media_id"],
            "media_type": extracted["media_type"],
            "emoji_map": extracted["emoji_map"],
            "entities_json": extracted["entities_json"],
            "target_uid": target,
        }
        context.user_data["admin_broadcast_draft"] = draft
        context.user_data["admin_broadcast_stage"] = "await_buttons"
        await reply_premium_message(msg, 
            "✅ Broadcast draft saved. Add buttons or send now?",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("➕ Add Inline Buttons", callback_data="admin_bcast_add_btns")],
                [InlineKeyboardButton("🚀 Send Now", callback_data="admin_bcast_send")],
                [InlineKeyboardButton("❌ Cancel", callback_data="admin_panel")],
            ]))
        return

    # Admin support reply
    if is_admin(user.id) and msg.reply_to_message:
        target_uid = _get_support_uid(SUPPORT_REPLY_MAP, msg.reply_to_message.message_id)
        if target_uid:
            try:
                await context.bot.copy_message(
                    chat_id=target_uid, from_chat_id=msg.chat_id, message_id=msg.message_id)
                if user.id != ADMIN_USER_ID:
                    try:
                        await send_premium_message(context.bot, 
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
                await reply_premium_message(msg, f"❌ Failed to send to user {target_uid}: {ex}",
                                      parse_mode=ParseMode.HTML)
            return

    # Non-admin: relay to support
    if not is_admin(user.id):
        try:
            delivered = 0
            for admin_id in ADMIN_USER_IDS:
                try:
                    relayed = await context.bot.forward_message(
                        chat_id=admin_id, from_chat_id=msg.chat_id, message_id=msg.message_id)
                    _store_support_map(SUPPORT_REPLY_MAP, relayed.message_id, user.id)
                    delivered += 1
                except Exception as e_admin:
                    logging.error(f"support relay to admin {admin_id} failed: {e_admin}")
            if delivered > 0:
                await send_ephemeral_reply(
                    msg, "✅ Message sent to support. You will receive a reply here.", 2)
            else:
                await reply_premium_message(msg, "⚠️ Support is temporarily unavailable. Please try again.",
                                      parse_mode=ParseMode.HTML)
        except Exception as ex:
            logging.error(f"support relay failed for {user.id}: {ex}")
            await reply_premium_message(msg, "⚠️ Could not send to support right now.",
                                  parse_mode=ParseMode.HTML)
        return

    await reply_premium_message(msg, "🔽 Use menu.", parse_mode=ParseMode.HTML,
                         reply_markup=main_menu_kb(user.id))


async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    err_str = str(context.error)
    logging.error(f"Update {update} caused error {err_str}")
    if "Message is not modified" in err_str or "Query is too old" in err_str:
        return
    try:
        if update and hasattr(update, 'effective_chat') and update.effective_chat:
            await send_premium_message(context.bot, 
                update.effective_chat.id,
                "❌ An error occurred. Please try again or contact admin.",
                parse_mode=ParseMode.HTML
            )
    except Exception:
        pass


# ================= MAIN =================
async def main():
    logging.basicConfig(
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        level=logging.INFO
    )
    logging.info("🚀 Starting Premium Bot System with Multi-Bot Support...")

    # Stop bots for expired subscriptions on startup
    expired = db.get_expired_subscriptions()
    for user_id, bot_id in expired:
        bot_data = db.get_user_bot(user_id, bot_id)
        if bot_data and bot_data[4] == 1:
            key = (user_id, bot_id)
            if key in user_bot_applications:
                try:
                    app = user_bot_applications[key]
                    await app.updater.stop()
                    await app.stop()
                    await app.shutdown()
                    logging.info(f"🛑 Stopped expired bot on startup: {user_id}/{bot_id}")
                except Exception as ex:
                    logging.error(f"Error stopping expired bot {user_id}/{bot_id}: {ex}")
                user_bot_applications.pop(key, None)
            db.set_user_bot_active(user_id, bot_id, False)

    # Start all valid user bots
    bots = db.get_all_user_bots()
    if bots:
        logging.info(f"Found {len(bots)} user bots to start")
        for uid, bid, token, buser, active in bots:
            sub = db.get_subscription(uid, bid)
            if not sub:
                logging.info(f"Skipping {uid}/{bid} - no subscription")
                db.set_user_bot_active(uid, bid, False)
                continue
            try:
                expiry = datetime.fromisoformat(sub[3])
                if expiry < datetime.now():
                    logging.info(f"Skipping {uid}/{bid} - subscription expired")
                    db.set_user_bot_active(uid, bid, False)
                    continue
            except Exception as ex:
                logging.error(f"Error checking expiry for {uid}/{bid}: {ex}")
                continue
            try:
                await start_user_bot(token, uid, bid)
                db.set_user_bot_active(uid, bid, True)
                logging.info(f"✅ Started user bot @{buser} for user {uid} (bot {bid})")
            except Exception as ex:
                logging.error(f"❌ Failed to start user bot {uid}/{bid}: {ex}")
    else:
        logging.info("No user bots found in database")

    # Main bot setup
    app = (ApplicationBuilder()
           .token(MAIN_BOT_TOKEN)
           .concurrent_updates(True)
           .connect_timeout(30)
           .read_timeout(30)
           .write_timeout(30)
           .pool_timeout(30)
           .build())

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
        subscription_reminder_job, interval=43200, first=60,
        name="subscription_reminders")
    app.job_queue.run_repeating(
        check_expired_subscriptions_job, interval=3600, first=120,
        name="expired_subscriptions_check")

    await app.initialize()
    await app.start()
    await app.updater.start_polling()
    logging.info("✅ Main bot started successfully")
    logging.info("🔔 Subscription reminder job scheduled (every 12 hours)")
    logging.info("🕐 Expired subscription check job scheduled (every 1 hour)")
    logging.info("🔐 Human verification flow active")
    logging.info("💎 Per-user premium emoji system active")
    logging.info("🤖 Multi-bot per user support enabled")
    logging.info("🗑️ Subscription removal button available for admin")

    try:
        await asyncio.Event().wait()
    except KeyboardInterrupt:
        logging.info("🛑 Stopping bots...")
        for key, user_app in user_bot_applications.items():
            try:
                await user_app.updater.stop()
                await user_app.stop()
                await user_app.shutdown()
                logging.info(f"✅ Stopped user bot {key}")
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
