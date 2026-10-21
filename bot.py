"""TEKIN ALMAZ BOT 💎 — secure, async Telegram referral and reward bot.

Python 3.11+, aiogram 3.x, SQLite. All operational logic intentionally lives
in this file so the admin can send a complete, standalone bot.py to themselves.
"""

from __future__ import annotations

import asyncio
import html
import logging
import os
import re
import sqlite3
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, AsyncIterator
from urllib.parse import urlparse
from uuid import uuid4

import aiosqlite
from aiogram import BaseMiddleware, Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ChatType
from aiogram.exceptions import (
    TelegramAPIError,
    TelegramForbiddenError,
    TelegramNetworkError,
    TelegramRetryAfter,
)
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    BotCommand,
    CallbackQuery,
    ChatMemberUpdated,
    FSInputFile,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    Message,
    ReplyKeyboardMarkup,
    Update,
)
from aiogram.utils.keyboard import InlineKeyboardBuilder
from dotenv import load_dotenv


APP_NAME = "TEKIN ALMAZ BOT 💎"
INVITE_REWARD = 5
EXIT_PENALTY = 7
CHANNEL_LEAVE_PENALTY = 1
REWARD_COST = 105
MAX_REASON_LENGTH = 300
PROJECT_DIR = Path(__file__).resolve().parent
load_dotenv(PROJECT_DIR / ".env")

os.environ.setdefault("BOT_TOKEN", "8936811970:AAFu93zy4cOxIgxYv1h_MgNH0DCJIwetV9o")
os.environ.setdefault("ADMIN_IDS", "7075772956")
os.environ.setdefault("BOT_USERNAME", "kingfire_almaz_bot")
os.environ.setdefault("ADMIN_CONTACT", "@kingfire_amaki")
os.environ.setdefault("CHANNEL_1_ID", "@kanal1")
os.environ.setdefault("CHANNEL_1_TITLE", "1-kanal")
os.environ.setdefault("CHANNEL_1_URL", "https://t.me/kanal1")
os.environ.setdefault("CHANNEL_2_ID", "@kanal2")
os.environ.setdefault("CHANNEL_2_TITLE", "2-kanal")
os.environ.setdefault("CHANNEL_2_URL", "https://t.me/kanal2")
os.environ.setdefault("CHANNEL_3_ID", "@kanal3")
os.environ.setdefault("CHANNEL_3_TITLE", "3-kanal")
os.environ.setdefault("CHANNEL_3_URL", "https://t.me/kanal3")
os.environ.setdefault("YOUTUBE_URL", "https://youtube.com/@kanal")

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger("tekin_almaz_bot")


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def parse_admin_ids(value: str | None) -> frozenset[int]:
    if not value or not value.strip():
        return frozenset()
    ids: set[int] = set()
    for item in value.split(","):
        item = item.strip()
        if not item:
            continue
        try:
            admin_id = int(item)
        except ValueError as exc:
            raise ValueError("ADMIN_IDS vergul bilan ajratilgan Telegram ID raqamlari bo‘lishi kerak.") from exc
        if admin_id <= 0:
            raise ValueError("ADMIN_IDS ichidagi barcha ID raqamlar musbat bo‘lishi kerak.")
        ids.add(admin_id)
    return frozenset(ids)


@dataclass(frozen=True)
class Config:
    token: str
    admin_ids: frozenset[int]
    db_path: Path
    bot_username: str
    admin_contact: str
    channels: tuple[dict[str, str], ...]
    youtube_url: str
    news_channel_ids: tuple[str, ...]

    @classmethod
    def from_env(cls, require_token: bool = True) -> "Config":
        token = os.getenv("BOT_TOKEN", "").strip()
        if require_token and not token:
            raise RuntimeError("BOT_TOKEN topilmadi. .env faylini sozlang.")
        admin_ids = parse_admin_ids(os.getenv("ADMIN_IDS"))
        if require_token and not admin_ids:
            raise RuntimeError("ADMIN_IDS topilmadi. Kamida bitta admin Telegram ID sini kiriting.")
        channels: list[dict[str, str]] = []
        for index in range(1, 4):
            channels.append(
                {
                    "id": os.getenv(f"CHANNEL_{index}_ID", "").strip(),
                    "title": os.getenv(f"CHANNEL_{index}_TITLE", f"{index}-kanal").strip(),
                    "url": os.getenv(f"CHANNEL_{index}_URL", "").strip(),
                }
            )
        news_ids = tuple(
            value.strip()
            for value in os.getenv("NEWS_CHANNEL_IDS", "").split(",")
            if value.strip()
        )
        db_path = Path(os.getenv("BOT_DB_PATH", str(PROJECT_DIR / "data" / "bot.sqlite3"))).expanduser()
        return cls(
            token=token,
            admin_ids=admin_ids,
            db_path=db_path,
            bot_username=os.getenv("BOT_USERNAME", "").strip().lstrip("@"),
            admin_contact=os.getenv("ADMIN_CONTACT", "").strip(),
            channels=tuple(channels),
            youtube_url=os.getenv("YOUTUBE_URL", "").strip(),
            news_channel_ids=news_ids,
        )


class Database:
    """SQLite storage with serialized BEGIN IMMEDIATE write transactions."""

    def __init__(self, path: Path | str):
        self.path = Path(path)
        self.connection: aiosqlite.Connection | None = None
        self.write_lock = asyncio.Lock()

    async def initialize(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.connection = await aiosqlite.connect(self.path, timeout=15)
        self.connection.row_factory = aiosqlite.Row
        await self.connection.execute("PRAGMA journal_mode=WAL")
        await self.connection.execute("PRAGMA foreign_keys=ON")
        await self.connection.execute("PRAGMA busy_timeout=15000")
        await self.connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS users (
                telegram_id INTEGER PRIMARY KEY,
                username TEXT,
                first_name TEXT NOT NULL DEFAULT '',
                joined_at TEXT NOT NULL,
                inviter_id INTEGER REFERENCES users(telegram_id),
                total_referrals INTEGER NOT NULL DEFAULT 0 CHECK(total_referrals >= 0),
                active_referrals INTEGER NOT NULL DEFAULT 0 CHECK(active_referrals >= 0),
                balance INTEGER NOT NULL DEFAULT 0 CHECK(balance >= 0),
                spent_points INTEGER NOT NULL DEFAULT 0 CHECK(spent_points >= 0),
                debt INTEGER NOT NULL DEFAULT 0 CHECK(debt >= 0),
                status TEXT NOT NULL DEFAULT 'active' CHECK(status IN ('active','blocked')),
                last_active TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_users_active_top
                ON users(active_referrals DESC, balance DESC);
            CREATE INDEX IF NOT EXISTS idx_users_total_top
                ON users(total_referrals DESC, balance DESC);

            CREATE TABLE IF NOT EXISTS referrals (
                invited_user_id INTEGER PRIMARY KEY REFERENCES users(telegram_id),
                inviter_id INTEGER NOT NULL REFERENCES users(telegram_id),
                created_at TEXT NOT NULL,
                qualified INTEGER NOT NULL DEFAULT 0 CHECK(qualified IN (0,1)),
                is_active INTEGER NOT NULL DEFAULT 0 CHECK(is_active IN (0,1)),
                exit_count INTEGER NOT NULL DEFAULT 0,
                FOREIGN KEY(inviter_id) REFERENCES users(telegram_id)
            );
            CREATE INDEX IF NOT EXISTS idx_referrals_inviter ON referrals(inviter_id);

            CREATE TABLE IF NOT EXISTS transactions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL REFERENCES users(telegram_id),
                amount INTEGER NOT NULL,
                balance_after INTEGER NOT NULL CHECK(balance_after >= 0),
                kind TEXT NOT NULL,
                description TEXT NOT NULL,
                admin_id INTEGER,
                reward_request_id INTEGER,
                idempotency_key TEXT UNIQUE,
                created_at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_transactions_user
                ON transactions(user_id, id DESC);

            CREATE TABLE IF NOT EXISTS penalties (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL REFERENCES users(telegram_id),
                penalty_kind TEXT NOT NULL,
                amount INTEGER NOT NULL CHECK(amount > 0),
                deducted INTEGER NOT NULL CHECK(deducted >= 0),
                debt_added INTEGER NOT NULL CHECK(debt_added >= 0),
                idempotency_key TEXT NOT NULL UNIQUE,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS reward_requests (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL REFERENCES users(telegram_id),
                free_fire_uid TEXT NOT NULL,
                cost INTEGER NOT NULL DEFAULT 105,
                status TEXT NOT NULL DEFAULT 'pending'
                    CHECK(status IN ('pending','approved','rejected','completed')),
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                admin_id INTEGER,
                admin_note TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_reward_status
                ON reward_requests(status, id DESC);
            CREATE UNIQUE INDEX IF NOT EXISTS idx_one_open_reward_per_user
                ON reward_requests(user_id) WHERE status IN ('pending','approved');

            CREATE TABLE IF NOT EXISTS admin_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                admin_id INTEGER NOT NULL,
                action TEXT NOT NULL,
                details TEXT NOT NULL,
                created_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS bot_settings (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                updated_by INTEGER
            );
            CREATE TABLE IF NOT EXISTS processed_updates (
                update_id INTEGER PRIMARY KEY,
                status TEXT NOT NULL CHECK(status IN ('processing','completed')),
                updated_at REAL NOT NULL
            );
            CREATE TABLE IF NOT EXISTS channel_memberships (
                user_id INTEGER NOT NULL REFERENCES users(telegram_id),
                channel_key TEXT NOT NULL,
                is_subscribed INTEGER NOT NULL CHECK(is_subscribed IN (0,1)),
                lapse_count INTEGER NOT NULL DEFAULT 0,
                updated_at TEXT NOT NULL,
                PRIMARY KEY(user_id, channel_key)
            );
            CREATE TABLE IF NOT EXISTS group_chats (
                chat_id INTEGER PRIMARY KEY,
                title TEXT NOT NULL DEFAULT '',
                is_active INTEGER NOT NULL DEFAULT 1 CHECK(is_active IN (0,1)),
                updated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS news_drafts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                admin_id INTEGER NOT NULL,
                source_chat_id INTEGER NOT NULL,
                source_message_id INTEGER NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending'
                    CHECK(status IN ('pending','sending','sent','partial','cancelled')),
                created_at TEXT NOT NULL,
                result TEXT
            );
            """
        )
        await self.connection.execute(
            "DELETE FROM processed_updates WHERE status='completed' AND updated_at<?",
            (time.time() - 30 * 24 * 60 * 60,),
        )
        await self.connection.commit()

    async def close(self) -> None:
        if self.connection is not None:
            await self.connection.close()
            self.connection = None

    def _conn(self) -> aiosqlite.Connection:
        if self.connection is None:
            raise RuntimeError("Database.initialize() chaqirilmagan.")
        return self.connection

    @asynccontextmanager
    async def write(self) -> AsyncIterator[aiosqlite.Connection]:
        conn = self._conn()
        async with self.write_lock:
            await conn.execute("BEGIN IMMEDIATE")
            try:
                yield conn
                await conn.commit()
            except BaseException:
                await conn.rollback()
                raise

    async def seed_settings(self, defaults: dict[str, str]) -> None:
        async with self.write() as conn:
            for key, value in defaults.items():
                await conn.execute(
                    "INSERT OR IGNORE INTO bot_settings(key,value,updated_at) VALUES(?,?,?)",
                    (key, value, now_iso()),
                )

    async def get_setting(self, key: str, default: str = "") -> str:
        cursor = await self._conn().execute(
            "SELECT value FROM bot_settings WHERE key=?", (key,)
        )
        row = await cursor.fetchone()
        return str(row["value"]) if row else default

    async def set_setting(self, key: str, value: str, admin_id: int) -> None:
        async with self.write() as conn:
            await conn.execute(
                """INSERT INTO bot_settings(key,value,updated_at,updated_by)
                   VALUES(?,?,?,?)
                   ON CONFLICT(key) DO UPDATE SET
                     value=excluded.value, updated_at=excluded.updated_at,
                     updated_by=excluded.updated_by""",
                (key, value, now_iso(), admin_id),
            )
            await self._log_admin(conn, admin_id, "setting_update", f"{key}={value[:160]}")

    async def register_user(
        self, telegram_id: int, username: str | None, first_name: str, inviter_id: int | None
    ) -> tuple[bool, bool]:
        """Create a first-time profile and permanently attach a valid inviter.

        Referral bonus is intentionally deferred until mandatory Telegram
        subscriptions are verified.
        """
        created = False
        inviter_attached = False
        async with self.write() as conn:
            existing = await (
                await conn.execute("SELECT * FROM users WHERE telegram_id=?", (telegram_id,))
            ).fetchone()
            if existing is None:
                if inviter_id == telegram_id:
                    inviter_id = None
                if inviter_id is not None:
                    inviter = await (
                        await conn.execute(
                            "SELECT telegram_id FROM users WHERE telegram_id=?", (inviter_id,)
                        )
                    ).fetchone()
                    if inviter is None:
                        inviter_id = None
                stamp = now_iso()
                await conn.execute(
                    """INSERT INTO users
                       (telegram_id,username,first_name,joined_at,inviter_id,last_active)
                       VALUES(?,?,?,?,?,?)""",
                    (telegram_id, username, first_name[:128], stamp, inviter_id, stamp),
                )
                created = True
                if inviter_id is not None:
                    await conn.execute(
                        """INSERT INTO referrals
                           (invited_user_id,inviter_id,created_at,qualified,is_active)
                           VALUES(?,?,?,0,0)""",
                        (telegram_id, inviter_id, stamp),
                    )
                    inviter_attached = True
            else:
                await conn.execute(
                    """UPDATE users SET username=?,first_name=?,status='active',last_active=?
                       WHERE telegram_id=?""",
                    (username, first_name[:128], now_iso(), telegram_id),
                )
                referral = await (
                    await conn.execute(
                        "SELECT inviter_id,qualified,is_active FROM referrals WHERE invited_user_id=?",
                        (telegram_id,),
                    )
                ).fetchone()
                if referral and referral["qualified"] and not referral["is_active"]:
                    await conn.execute(
                        "UPDATE referrals SET is_active=1 WHERE invited_user_id=?",
                        (telegram_id,),
                    )
                    await conn.execute(
                        """UPDATE users SET active_referrals=active_referrals+1
                           WHERE telegram_id=?""",
                        (referral["inviter_id"],),
                    )
        return created, inviter_attached

    async def qualify_referral(self, invited_user_id: int) -> bool:
        """Credit +5 once after a new invitee meets all required channel checks."""
        async with self.write() as conn:
            referral = await (
                await conn.execute(
                    "SELECT * FROM referrals WHERE invited_user_id=?", (invited_user_id,)
                )
            ).fetchone()
            if not referral or referral["qualified"]:
                return False
            inviter_id = int(referral["inviter_id"])
            await conn.execute(
                """UPDATE referrals SET qualified=1,is_active=1
                   WHERE invited_user_id=? AND qualified=0""",
                (invited_user_id,),
            )
            await conn.execute(
                """UPDATE users SET total_referrals=total_referrals+1,
                   active_referrals=active_referrals+1,balance=balance+?
                   WHERE telegram_id=?""",
                (INVITE_REWARD, inviter_id),
            )
            balance = await (
                await conn.execute("SELECT balance FROM users WHERE telegram_id=?", (inviter_id,))
            ).fetchone()
            await self._insert_transaction(
                conn,
                inviter_id,
                INVITE_REWARD,
                int(balance["balance"]),
                "referral_bonus",
                f"Yangi taklif (ID {invited_user_id}) uchun +{INVITE_REWARD} 💎",
                f"referral-bonus:{invited_user_id}",
            )
            return True

    async def reactivate_user(self, telegram_id: int) -> None:
        async with self.write() as conn:
            await conn.execute(
                "UPDATE users SET status='active',last_active=? WHERE telegram_id=?",
                (now_iso(), telegram_id),
            )
            referral = await (
                await conn.execute(
                    "SELECT inviter_id,qualified,is_active FROM referrals WHERE invited_user_id=?",
                    (telegram_id,),
                )
            ).fetchone()
            if referral and referral["qualified"] and not referral["is_active"]:
                await conn.execute(
                    "UPDATE referrals SET is_active=1 WHERE invited_user_id=?", (telegram_id,)
                )
                await conn.execute(
                    "UPDATE users SET active_referrals=active_referrals+1 WHERE telegram_id=?",
                    (referral["inviter_id"],),
                )

    async def mark_stopped(self, telegram_id: int, detected_by: str) -> None:
        """Record /stop or a Telegram delivery failure, without double charging."""
        async with self.write() as conn:
            user = await (
                await conn.execute("SELECT status FROM users WHERE telegram_id=?", (telegram_id,))
            ).fetchone()
            if user is None:
                return
            was_active = user["status"] == "active"
            await conn.execute(
                "UPDATE users SET status='blocked',last_active=? WHERE telegram_id=?",
                (now_iso(), telegram_id),
            )
            if not was_active:
                return
            referral = await (
                await conn.execute(
                    """SELECT inviter_id,qualified,is_active,exit_count
                       FROM referrals WHERE invited_user_id=?""",
                    (telegram_id,),
                )
            ).fetchone()
            if referral and referral["qualified"] and referral["is_active"]:
                exit_count = int(referral["exit_count"]) + 1
                await conn.execute(
                    """UPDATE referrals SET is_active=0,exit_count=?
                       WHERE invited_user_id=?""",
                    (exit_count, telegram_id),
                )
                await conn.execute(
                    """UPDATE users SET active_referrals=MAX(0,active_referrals-1)
                       WHERE telegram_id=?""",
                    (referral["inviter_id"],),
                )
                await self._apply_penalty(
                    conn,
                    int(referral["inviter_id"]),
                    EXIT_PENALTY,
                    "referral_exit",
                    f"Taklif qilingan foydalanuvchi botdan chiqdi ({detected_by}); ID {telegram_id}",
                    f"referral-exit:{telegram_id}:{exit_count}",
                )

    async def record_membership(self, user_id: int, channel_key: str, is_subscribed: bool) -> bool:
        """Remember observed status; charge -1 only on an observed subscribed→left change."""
        async with self.write() as conn:
            row = await (
                await conn.execute(
                    """SELECT is_subscribed,lapse_count FROM channel_memberships
                       WHERE user_id=? AND channel_key=?""",
                    (user_id, channel_key),
                )
            ).fetchone()
            penalized = False
            if row is None:
                await conn.execute(
                    """INSERT INTO channel_memberships
                       (user_id,channel_key,is_subscribed,lapse_count,updated_at)
                       VALUES(?,?,?,?,?)""",
                    (user_id, channel_key, int(is_subscribed), 0, now_iso()),
                )
            elif is_subscribed:
                await conn.execute(
                    """UPDATE channel_memberships SET is_subscribed=1,updated_at=?
                       WHERE user_id=? AND channel_key=?""",
                    (now_iso(), user_id, channel_key),
                )
            elif row["is_subscribed"]:
                lapse_count = int(row["lapse_count"]) + 1
                await conn.execute(
                    """UPDATE channel_memberships SET is_subscribed=0,lapse_count=?,updated_at=?
                       WHERE user_id=? AND channel_key=?""",
                    (lapse_count, now_iso(), user_id, channel_key),
                )
                await self._apply_penalty(
                    conn,
                    user_id,
                    CHANNEL_LEAVE_PENALTY,
                    "channel_leave",
                    f"Majburiy obunadan chiqqani faoliyat paytida aniqlandi: {channel_key}",
                    f"channel-leave:{user_id}:{channel_key}:{lapse_count}",
                )
                penalized = True
            return penalized

    async def _apply_penalty(
        self,
        conn: aiosqlite.Connection,
        user_id: int,
        amount: int,
        kind: str,
        description: str,
        idempotency_key: str,
    ) -> bool:
        exists = await (
            await conn.execute(
                "SELECT id FROM penalties WHERE idempotency_key=?", (idempotency_key,)
            )
        ).fetchone()
        if exists:
            return False
        row = await (
            await conn.execute("SELECT balance,debt FROM users WHERE telegram_id=?", (user_id,))
        ).fetchone()
        if row is None:
            return False
        balance = int(row["balance"])
        deducted = min(balance, amount)
        shortfall = amount - deducted
        new_balance = balance - deducted
        await conn.execute(
            "UPDATE users SET balance=?,debt=debt+? WHERE telegram_id=?",
            (new_balance, shortfall, user_id),
        )
        await conn.execute(
            """INSERT INTO penalties
               (user_id,penalty_kind,amount,deducted,debt_added,idempotency_key,created_at)
               VALUES(?,?,?,?,?,?,?)""",
            (user_id, kind, amount, deducted, shortfall, idempotency_key, now_iso()),
        )
        if deducted:
            await self._insert_transaction(
                conn, user_id, -deducted, new_balance, kind, description, idempotency_key
            )
        return True

    async def get_user(self, telegram_id: int) -> aiosqlite.Row | None:
        return await (
            await self._conn().execute(
                "SELECT * FROM users WHERE telegram_id=?", (telegram_id,)
            )
        ).fetchone()

    async def get_users_count(self) -> int:
        row = await (await self._conn().execute("SELECT COUNT(*) AS n FROM users")).fetchone()
        return int(row["n"])

    async def top_users(self, order_by: str, limit: int = 10, offset: int = 0) -> list[aiosqlite.Row]:
        if order_by not in {"active_referrals", "total_referrals"}:
            raise ValueError("Noma’lum reyting turi.")
        cursor = await self._conn().execute(
            f"""SELECT telegram_id,username,first_name,total_referrals,active_referrals,balance
                FROM users ORDER BY {order_by} DESC,balance DESC,telegram_id ASC
                LIMIT ? OFFSET ?""",
            (limit, offset),
        )
        return list(await cursor.fetchall())

    async def list_users(self, limit: int = 10, offset: int = 0) -> list[aiosqlite.Row]:
        cursor = await self._conn().execute(
            """SELECT telegram_id,username,first_name,balance,status,joined_at
               FROM users ORDER BY joined_at DESC LIMIT ? OFFSET ?""",
            (limit, offset),
        )
        return list(await cursor.fetchall())

    async def user_transactions(self, user_id: int, limit: int = 15) -> list[aiosqlite.Row]:
        cursor = await self._conn().execute(
            """SELECT amount,balance_after,kind,description,created_at
               FROM transactions WHERE user_id=? ORDER BY id DESC LIMIT ?""",
            (user_id, limit),
        )
        return list(await cursor.fetchall())

    async def adjust_balance(
        self, user_id: int, amount: int, reason: str, admin_id: int
    ) -> tuple[bool, int]:
        if amount == 0:
            return False, 0
        async with self.write() as conn:
            user = await (
                await conn.execute(
                    "SELECT balance,debt FROM users WHERE telegram_id=?", (user_id,)
                )
            ).fetchone()
            if user is None:
                return False, 0
            if amount > 0:
                new_balance = int(user["balance"]) + amount
                await conn.execute(
                    "UPDATE users SET balance=? WHERE telegram_id=?", (new_balance, user_id)
                )
                applied = amount
            else:
                requested = abs(amount)
                applied = min(int(user["balance"]), requested)
                shortfall = requested - applied
                new_balance = int(user["balance"]) - applied
                await conn.execute(
                    """UPDATE users SET balance=?,debt=debt+? WHERE telegram_id=?""",
                    (new_balance, shortfall, user_id),
                )
                if shortfall:
                    await conn.execute(
                        """INSERT INTO penalties
                           (user_id,penalty_kind,amount,deducted,debt_added,idempotency_key,created_at)
                           VALUES(?,?,?,?,?,?,?)""",
                        (
                            user_id,
                            "admin_adjustment",
                            requested,
                            applied,
                            shortfall,
                            f"admin-adjust:{uuid4()}",
                            now_iso(),
                        ),
                    )
            await self._insert_transaction(
                conn,
                user_id,
                applied if amount > 0 else -applied,
                new_balance,
                "admin_adjustment",
                reason[:MAX_REASON_LENGTH],
                f"admin-adjust:{uuid4()}",
                admin_id=admin_id,
            )
            await self._log_admin(
                conn,
                admin_id,
                "balance_adjustment",
                f"user={user_id}; requested={amount}; applied={applied}; reason={reason[:MAX_REASON_LENGTH]}",
            )
            return True, new_balance

    async def open_reward_id(self, user_id: int) -> int | None:
        cursor = await self._conn().execute(
            """SELECT id FROM reward_requests WHERE user_id=?
               AND status IN ('pending','approved')""",
            (user_id,),
        )
        row = await cursor.fetchone()
        return int(row["id"]) if row else None

    async def create_reward_request(self, user_id: int, ff_uid: str) -> tuple[str, int | None]:
        """Return (status, request_id): created, insufficient, open_request, unknown_user."""
        async with self.write() as conn:
            user = await (
                await conn.execute(
                    "SELECT balance FROM users WHERE telegram_id=?", (user_id,)
                )
            ).fetchone()
            if user is None:
                return "unknown_user", None
            current = await (
                await conn.execute(
                    """SELECT id FROM reward_requests WHERE user_id=?
                       AND status IN ('pending','approved')""",
                    (user_id,),
                )
            ).fetchone()
            if current:
                return "open_request", int(current["id"])
            if int(user["balance"]) < REWARD_COST:
                return "insufficient", None
            stamp = now_iso()
            try:
                cursor = await conn.execute(
                    """INSERT INTO reward_requests
                       (user_id,free_fire_uid,cost,status,created_at,updated_at)
                       VALUES(?,?,?,'pending',?,?)""",
                    (user_id, ff_uid, REWARD_COST, stamp, stamp),
                )
            except sqlite3.IntegrityError:
                current = await (
                    await conn.execute(
                        """SELECT id FROM reward_requests WHERE user_id=?
                           AND status IN ('pending','approved')""",
                        (user_id,),
                    )
                ).fetchone()
                return "open_request", int(current["id"]) if current else None
            request_id = int(cursor.lastrowid)
            new_balance = int(user["balance"]) - REWARD_COST
            await conn.execute(
                """UPDATE users SET balance=?,spent_points=spent_points+?
                   WHERE telegram_id=?""",
                (new_balance, REWARD_COST, user_id),
            )
            await self._insert_transaction(
                conn,
                user_id,
                -REWARD_COST,
                new_balance,
                "reward_spend",
                f"Olmos mukofoti so‘rovi #{request_id}",
                f"reward:{request_id}:spend",
                reward_request_id=request_id,
            )
            return "created", request_id

    async def transition_reward(
        self, request_id: int, action: str, admin_id: int, note: str = ""
    ) -> tuple[bool, str, int | None]:
        allowed = {"approve", "reject", "complete"}
        if action not in allowed:
            return False, "invalid_action", None
        async with self.write() as conn:
            request = await (
                await conn.execute(
                    "SELECT * FROM reward_requests WHERE id=?", (request_id,)
                )
            ).fetchone()
            if request is None:
                return False, "not_found", None
            old_status = str(request["status"])
            if action == "approve" and old_status != "pending":
                return False, "wrong_status", int(request["user_id"])
            if action == "reject" and old_status not in {"pending", "approved"}:
                return False, "wrong_status", int(request["user_id"])
            if action == "complete" and old_status != "approved":
                return False, "wrong_status", int(request["user_id"])
            new_status = {"approve": "approved", "reject": "rejected", "complete": "completed"}[
                action
            ]
            user_id = int(request["user_id"])
            if action == "reject":
                exists = await (
                    await conn.execute(
                        """SELECT id FROM transactions WHERE idempotency_key=?""",
                        (f"reward:{request_id}:refund",),
                    )
                ).fetchone()
                if exists:
                    return False, "already_refunded", user_id
            await conn.execute(
                """UPDATE reward_requests SET status=?,updated_at=?,admin_id=?,admin_note=?
                   WHERE id=?""",
                (new_status, now_iso(), admin_id, note[:MAX_REASON_LENGTH], request_id),
            )
            if action == "reject":
                user = await (
                    await conn.execute(
                        "SELECT balance,spent_points FROM users WHERE telegram_id=?", (user_id,)
                    )
                ).fetchone()
                new_balance = int(user["balance"]) + int(request["cost"])
                await conn.execute(
                    """UPDATE users SET balance=?,spent_points=MAX(0,spent_points-?)
                       WHERE telegram_id=?""",
                    (new_balance, request["cost"], user_id),
                )
                await self._insert_transaction(
                    conn,
                    user_id,
                    int(request["cost"]),
                    new_balance,
                    "reward_refund",
                    f"Rad etilgan #{request_id} arizasi uchun ball qaytarildi",
                    f"reward:{request_id}:refund",
                    admin_id=admin_id,
                    reward_request_id=request_id,
                )
            await self._log_admin(
                conn, admin_id, f"reward_{action}", f"request={request_id}; note={note[:MAX_REASON_LENGTH]}"
            )
            return True, new_status, user_id

    async def reward_requests(self, limit: int = 10, offset: int = 0) -> list[aiosqlite.Row]:
        cursor = await self._conn().execute(
            """SELECT r.*,u.username,u.first_name FROM reward_requests r
               JOIN users u ON u.telegram_id=r.user_id
               ORDER BY r.id DESC LIMIT ? OFFSET ?""",
            (limit, offset),
        )
        return list(await cursor.fetchall())

    async def active_user_ids(self) -> list[int]:
        cursor = await self._conn().execute(
            "SELECT telegram_id FROM users WHERE status='active' ORDER BY telegram_id"
        )
        return [int(row["telegram_id"]) for row in await cursor.fetchall()]

    async def stats(self) -> dict[str, int]:
        row = await (
            await self._conn().execute(
                """SELECT COUNT(*) AS users,
                   COALESCE(SUM(balance),0) AS balance,
                   COALESCE(SUM(spent_points),0) AS spent,
                   COALESCE(SUM(debt),0) AS debt,
                   COALESCE(SUM(total_referrals),0) AS referrals
                   FROM users"""
            )
        ).fetchone()
        rewards = await (
            await self._conn().execute(
                """SELECT COUNT(*) AS total,
                   SUM(CASE WHEN status='pending' THEN 1 ELSE 0 END) AS pending,
                   SUM(CASE WHEN status='completed' THEN 1 ELSE 0 END) AS completed
                   FROM reward_requests"""
            )
        ).fetchone()
        return {
            "users": int(row["users"]),
            "balance": int(row["balance"]),
            "spent": int(row["spent"]),
            "debt": int(row["debt"]),
            "referrals": int(row["referrals"]),
            "rewards": int(rewards["total"] or 0),
            "pending": int(rewards["pending"] or 0),
            "completed": int(rewards["completed"] or 0),
        }

    async def claim_update(self, update_id: int) -> bool:
        async with self.write() as conn:
            row = await (
                await conn.execute(
                    "SELECT status,updated_at FROM processed_updates WHERE update_id=?",
                    (update_id,),
                )
            ).fetchone()
            if row and (
                row["status"] == "completed" or time.time() - float(row["updated_at"]) < 120
            ):
                return False
            await conn.execute(
                """INSERT INTO processed_updates(update_id,status,updated_at)
                   VALUES(?,'processing',?)
                   ON CONFLICT(update_id) DO UPDATE SET status='processing',updated_at=excluded.updated_at""",
                (update_id, time.time()),
            )
            return True

    async def finish_update(self, update_id: int) -> None:
        async with self.write() as conn:
            await conn.execute(
                """UPDATE processed_updates SET status='completed',updated_at=?
                   WHERE update_id=?""",
                (time.time(), update_id),
            )

    async def release_update(self, update_id: int) -> None:
        async with self.write() as conn:
            await conn.execute("DELETE FROM processed_updates WHERE update_id=?", (update_id,))

    async def log_admin(self, admin_id: int, action: str, details: str) -> None:
        async with self.write() as conn:
            await self._log_admin(conn, admin_id, action, details)

    async def _log_admin(
        self, conn: aiosqlite.Connection, admin_id: int, action: str, details: str
    ) -> None:
        await conn.execute(
            """INSERT INTO admin_logs(admin_id,action,details,created_at)
               VALUES(?,?,?,?)""",
            (admin_id, action[:80], details[:1000], now_iso()),
        )

    async def save_group(self, chat_id: int, title: str, is_active: bool) -> None:
        async with self.write() as conn:
            await conn.execute(
                """INSERT INTO group_chats(chat_id,title,is_active,updated_at)
                   VALUES(?,?,?,?)
                   ON CONFLICT(chat_id) DO UPDATE SET title=excluded.title,
                     is_active=excluded.is_active,updated_at=excluded.updated_at""",
                (chat_id, title[:200], int(is_active), now_iso()),
            )

    async def active_groups(self) -> list[int]:
        cursor = await self._conn().execute(
            "SELECT chat_id FROM group_chats WHERE is_active=1"
        )
        return [int(row["chat_id"]) for row in await cursor.fetchall()]

    async def create_news_draft(
        self, admin_id: int, source_chat_id: int, source_message_id: int
    ) -> int:
        async with self.write() as conn:
            cursor = await conn.execute(
                """INSERT INTO news_drafts
                   (admin_id,source_chat_id,source_message_id,status,created_at)
                   VALUES(?,?,?,'pending',?)""",
                (admin_id, source_chat_id, source_message_id, now_iso()),
            )
            return int(cursor.lastrowid)

    async def claim_news_draft(self, draft_id: int, admin_id: int) -> aiosqlite.Row | None:
        async with self.write() as conn:
            cursor = await conn.execute(
                """UPDATE news_drafts SET status='sending'
                   WHERE id=? AND admin_id=? AND status='pending'""",
                (draft_id, admin_id),
            )
            if cursor.rowcount != 1:
                return None
            return await (
                await conn.execute("SELECT * FROM news_drafts WHERE id=?", (draft_id,))
            ).fetchone()

    async def finish_news_draft(self, draft_id: int, status: str, result: str) -> None:
        async with self.write() as conn:
            await conn.execute(
                "UPDATE news_drafts SET status=?,result=? WHERE id=?",
                (status, result[:2000], draft_id),
            )

    @staticmethod
    async def _insert_transaction(
        conn: aiosqlite.Connection,
        user_id: int,
        amount: int,
        balance_after: int,
        kind: str,
        description: str,
        idempotency_key: str,
        admin_id: int | None = None,
        reward_request_id: int | None = None,
    ) -> None:
        await conn.execute(
            """INSERT OR IGNORE INTO transactions
               (user_id,amount,balance_after,kind,description,admin_id,
                reward_request_id,idempotency_key,created_at)
               VALUES(?,?,?,?,?,?,?,?,?)""",
            (
                user_id,
                amount,
                balance_after,
                kind[:80],
                description[:500],
                admin_id,
                reward_request_id,
                idempotency_key,
                now_iso(),
            ),
        )


class IdempotencyMiddleware(BaseMiddleware):
    def __init__(self, database: Database):
        self.database = database

    async def __call__(self, handler: Any, event: Update, data: dict[str, Any]) -> Any:
        update_id = int(event.update_id)
        if not await self.database.claim_update(update_id):
            logger.info("Takroriy Telegram hodisasi e’tiborsiz qoldirildi (update_id=%s)", update_id)
            return None
        try:
            result = await handler(event, data)
        except BaseException:
            await self.database.release_update(update_id)
            raise
        await self.database.finish_update(update_id)
        return result


class SubscriptionGateMiddleware(BaseMiddleware):
    def __init__(self, database: Database, config: Config):
        self.database = database
        self.config = config

    async def __call__(self, handler: Any, event: Any, data: dict[str, Any]) -> Any:
        user = getattr(event, "from_user", None)
        if user is None:
            return await handler(event, data)
        chat = getattr(event, "chat", None)
        if chat is None and getattr(event, "message", None):
            chat = event.message.chat
        if chat is not None and chat.type != ChatType.PRIVATE:
            return await handler(event, data)
        if user.id in self.config.admin_ids:
            return await handler(event, data)
        if isinstance(event, Message):
            text = event.text or ""
            if text.startswith("/start") or text.startswith("/stop"):
                return await handler(event, data)
        if isinstance(event, CallbackQuery) and event.data == "subscription:check":
            return await handler(event, data)
        profile = await self.database.get_user(user.id)
        if profile is None:
            return await handler(event, data)
        bot: Bot = data["bot"]
        ok, missing, failure = await check_subscriptions(bot, self.database, user.id)
        if failure:
            notice = "Obunani hozir tekshira olmadim. Birozdan so‘ng qayta urinib ko‘ring."
        elif not ok:
            notice = subscription_text(missing)
        else:
            await self.database.qualify_referral(user.id)
            return await handler(event, data)
        keyboard = subscription_keyboard(await required_channels(self.database), self.config)
        if isinstance(event, Message):
            await event.answer(notice, reply_markup=keyboard)
        elif isinstance(event, CallbackQuery):
            await event.answer("Avval obunani tekshiring.", show_alert=False)
            if event.message:
                await event.message.answer(notice, reply_markup=keyboard)
        return None


class ThrottlingMiddleware(BaseMiddleware):
    """Small per-process flood guard for repeated private-chat events."""

    def __init__(self, minimum_interval: float = 0.45):
        self.minimum_interval = minimum_interval
        self.last_seen: dict[int, float] = {}

    async def __call__(self, handler: Any, event: Any, data: dict[str, Any]) -> Any:
        user = getattr(event, "from_user", None)
        if user is None:
            return await handler(event, data)
        chat = getattr(event, "chat", None)
        if chat is None and getattr(event, "message", None):
            chat = event.message.chat
        if chat is not None and chat.type != ChatType.PRIVATE:
            return await handler(event, data)
        current = time.monotonic()
        previous = self.last_seen.get(user.id, 0.0)
        self.last_seen[user.id] = current
        if len(self.last_seen) > 10_000:
            cutoff = current - 60
            self.last_seen = {
                user_id: timestamp
                for user_id, timestamp in self.last_seen.items()
                if timestamp >= cutoff
            }
        if current - previous < self.minimum_interval:
            if isinstance(event, CallbackQuery):
                await event.answer("Biroz kutib, qayta urinib ko‘ring.", show_alert=False)
            elif isinstance(event, Message):
                await event.answer("Iltimos, juda tez-tez xabar yubormang.")
            return None
        return await handler(event, data)


class UserFlow(StatesGroup):
    reward_uid = State()


class AdminFlow(StatesGroup):
    lookup_id = State()
    balance_id = State()
    balance_change = State()
    setting_key = State()
    setting_value = State()
    broadcast_content = State()
    news_content = State()


router = Router()
_db: Database
_config = Config.from_env(require_token=False)
_bot_username = ""


def main_keyboard(is_admin: bool = False) -> ReplyKeyboardMarkup:
    rows = [
        [KeyboardButton(text="💎 Mening balansim"), KeyboardButton(text="👥 Do‘stlarni taklif qilish")],
        [KeyboardButton(text="📊 Takliflar statistikasi"), KeyboardButton(text="🏆 TOP foydalanuvchilar")],
        [KeyboardButton(text="🎁 Olmoslarni olish"), KeyboardButton(text="📜 Tranzaksiyalar tarixi")],
        [KeyboardButton(text="ℹ️ Bot qoidalari"), KeyboardButton(text="📞 Administrator bilan aloqa")],
        [KeyboardButton(text="➕ Guruhga qo‘shish")],
    ]
    if is_admin:
        rows.append([KeyboardButton(text="🛠 Admin panel")])
    return ReplyKeyboardMarkup(keyboard=rows, resize_keyboard=True, input_field_placeholder="Bo‘limni tanlang")


def admin_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="👥 Foydalanuvchilar", callback_data="admin:users")
    builder.button(text="🔎 ID orqali qidirish", callback_data="admin:lookup")
    builder.button(text="💎 Balansni o‘zgartirish", callback_data="admin:balance")
    builder.button(text="🎁 Mukofot arizalari", callback_data="admin:rewards")
    builder.button(text="📊 Umumiy statistika", callback_data="admin:stats")
    builder.button(text="🏆 Reyting", callback_data="admin:top")
    builder.button(text="📢 Xabar yuborish", callback_data="admin:broadcast")
    builder.button(text="📰 Yangilik yuborish", callback_data="admin:news")
    builder.button(text="⚙️ Obuna va sozlamalar", callback_data="admin:settings")
    builder.button(text="📦 bot.py va requirements.txt", callback_data="admin:source")
    builder.adjust(2, 2, 2, 2, 1, 1)
    return builder.as_markup()


def subscription_keyboard(
    channels: list[dict[str, str]], config: Config
) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    for channel in channels:
        title = channel.get("title") or "Telegram kanal"
        link = channel.get("url") or (
            f"https://t.me/{channel['id'].lstrip('@')}"
            if channel.get("id", "").startswith("@")
            else ""
        )
        if link and is_http_url(link):
            builder.row(InlineKeyboardButton(text=f"📢 {title}ga obuna bo‘lish", url=link))
    youtube_url = config.youtube_url
    if youtube_url and is_http_url(youtube_url):
        builder.row(InlineKeyboardButton(text="▶️ YouTube kanaliga o‘tish", url=youtube_url))
    builder.row(InlineKeyboardButton(text="✅ Obunani tekshirish", callback_data="subscription:check"))
    return builder.as_markup()


def is_http_url(value: str) -> bool:
    try:
        parsed = urlparse(value)
    except ValueError:
        return False
    return parsed.scheme == "https" and bool(parsed.netloc)


async def required_channels(database: Database) -> list[dict[str, str]]:
    channels: list[dict[str, str]] = []
    for index in range(1, 4):
        channels.append(
            {
                "key": f"channel_{index}",
                "id": await database.get_setting(
                    f"channel_{index}_chat_id", _config.channels[index - 1]["id"]
                ),
                "title": await database.get_setting(
                    f"channel_{index}_title", _config.channels[index - 1]["title"]
                ),
                "url": await database.get_setting(
                    f"channel_{index}_url", _config.channels[index - 1]["url"]
                ),
            }
        )
    return channels


async def check_subscriptions(
    bot: Bot, database: Database, user_id: int
) -> tuple[bool, list[str], bool]:
    """Return (all_subscribed, missing_channel_titles, temporary/config error)."""
    missing: list[str] = []
    channels = await required_channels(database)
    for channel in channels:
        chat_id = channel["id"].strip()
        if not chat_id:
            logger.error("Majburiy kanal sozlanmagan: %s", channel["key"])
            return False, [channel["title"]], True
        try:
            member = await bot.get_chat_member(chat_id=chat_id, user_id=user_id)
        except (TelegramAPIError, TelegramNetworkError) as exc:
            # API errors (including bot not being an admin) are not evidence of
            # leaving; do not issue a penalty based on an unverifiable result.
            logger.warning("Obuna tekshiruvi bajarilmadi (%s): %s", channel["key"], type(exc).__name__)
            return False, [], True
        status = getattr(member, "status", "")
        is_member = status in {"creator", "administrator", "member"}
        if status == "restricted":
            is_member = bool(getattr(member, "is_member", False))
        await database.record_membership(user_id, channel["key"], is_member)
        if not is_member:
            missing.append(channel["title"])
    return not missing, missing, False


def subscription_text(missing: list[str]) -> str:
    if missing:
        names = "\n".join(f"• {html.escape(name)}" for name in missing)
        return (
            "Asosiy menyuni ochish uchun avval quyidagi Telegram kanallariga obuna bo‘ling:\n"
            f"{names}\n\nSo‘ng «✅ Obunani tekshirish» tugmasini bosing."
        )
    return (
        "Obunani tekshirish hozircha imkonsiz. Bot uchta Telegram kanalida administrator "
        "ekanini va kanal ID/username sozlanganini tekshiring."
    )


def rules_text() -> str:
    return (
        "ℹ️ <b>Bot qoidalari</b>\n\n"
        "• Birinchi marta ro‘yxatdan o‘tgan, o‘zini taklif qilmagan foydalanuvchi uchala "
        "Telegram kanaliga obuna bo‘lgach taklif haqiqiy hisoblanadi: taklif qiluvchiga +5 💎.\n"
        "• Bir foydalanuvchining taklif qiluvchisi o‘zgarmaydi; qayta kirish bonus bermaydi.\n"
        "• Taklif qilingan foydalanuvchi /stop yuborsa yoki botni bloklagani xabar yuborishda "
        "aniqlansa, taklif qiluvchidan 7 💎 yechiladi. Bloklashni bot darhol ko‘ra olmaydi.\n"
        "• Majburiy Telegram kanalidan chiqish faoliyat paytidagi tekshiruvda aniqlansa, "
        "har bir kuzatilgan chiqish uchun 1 💎 yechiladi.\n"
        "• Balans manfiy bo‘lmaydi; yetmagan jarima qarzdorlik sifatida alohida qayd qilinadi.\n"
        "• Mukofot so‘rovi 105 💎 turadi. Ariza rad etilsa 105 💎 avtomatik qaytariladi.\n"
        "• Admin mukofot yuborganini tasdiqlamaguncha ariza bajarilgan hisoblanmaydi.\n"
        "• YouTube havolasi beriladi, lekin YouTube obunasini Telegram Bot API orqali tekshirib bo‘lmaydi.\n"
        "• Taklif bonuslari faqat yangi Telegram akkauntlari va obuna tekshiruviga asoslanadi; "
        "bot alohida odam ekanini yoki ko‘p akkaunt ishlatilmaganini to‘liq isbotlay olmaydi."
    )


async def show_main_menu(message: Message, telegram_id: int) -> None:
    await _db.qualify_referral(telegram_id)
    await message.answer(
        "💎 <b>TEKIN ALMAZ BOT</b>\n"
        "Free Fire hamjamiyatiga xush kelibsiz! Bo‘limni tanlang:",
        reply_markup=main_keyboard(telegram_id in _config.admin_ids),
    )


async def send_subscription_gate(message: Message, telegram_id: int, bot: Bot) -> None:
    if telegram_id in _config.admin_ids:
        await show_main_menu(message, telegram_id)
        return
    ok, missing, failure = await check_subscriptions(bot, _db, telegram_id)
    if ok:
        await show_main_menu(message, telegram_id)
        return
    text = (
        "Obuna tekshiruvi vaqtincha ishlamadi. Bot kanal administrator ekanini tekshirib, yana urinib ko‘ring."
        if failure
        else subscription_text(missing)
    )
    await message.answer(text, reply_markup=subscription_keyboard(await required_channels(_db), _config))


def format_user(row: aiosqlite.Row) -> str:
    return (
        "👤 <b>Profilingiz</b>\n"
        f"💎 Balans: <b>{row['balance']}</b>\n"
        f"👥 Jami takliflar: <b>{row['total_referrals']}</b>\n"
        f"✅ Faol takliflar: <b>{row['active_referrals']}</b>\n"
        f"🎁 Mukofot uchun sarflangan: <b>{row['spent_points']}</b>\n"
        f"⚠️ Qarzdorlik/jarima: <b>{row['debt']}</b>"
    )


def reward_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🆔 Free Fire UID kiritish", callback_data="reward:start")]
        ]
    )


def admin_only(telegram_id: int) -> bool:
    return telegram_id in _config.admin_ids


def is_admin_user(message: Message) -> bool:
    return message.from_user is not None and message.from_user.id in _config.admin_ids


async def deny_admin(event: Message | CallbackQuery) -> bool:
    actor = event.from_user
    if actor is None or not admin_only(actor.id):
        if isinstance(event, Message):
            await event.answer("⛔ Bu bo‘lim faqat administratorlar uchun.")
        else:
            await event.answer("⛔ Ruxsat yo‘q.", show_alert=True)
        return True
    return False


def safe_user_label(row: aiosqlite.Row) -> str:
    name = html.escape(row["first_name"] or "")
    username = html.escape(row["username"] or "")
    return f"{name} (@{username})" if username else name


async def make_top_text(order_by: str) -> str:
    rows = await _db.top_users(order_by, 10)
    title = "Faol takliflar bo‘yicha TOP 10" if order_by == "active_referrals" else "Jami takliflar bo‘yicha TOP 10"
    if not rows:
        return f"🏆 <b>{title}</b>\n\nHozircha foydalanuvchilar yo‘q."
    lines = [f"🏆 <b>{title}</b>\n"]
    for index, row in enumerate(rows, start=1):
        label = safe_user_label(row) or f"ID {row['telegram_id']}"
        count = row[order_by]
        lines.append(f"{index}. {label} — <b>{count}</b> ta · {row['balance']} 💎")
    return "\n".join(lines)


def top_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="✅ Faol takliflar", callback_data="top:active"),
                InlineKeyboardButton(text="👥 Jami takliflar", callback_data="top:total"),
            ]
        ]
    )


@router.message(CommandStart())
async def start_handler(message: Message, command: Any, bot: Bot) -> None:
    if message.from_user is None:
        return
    payload = command.args or ""
    inviter_id: int | None = None
    if payload.isdigit():
        candidate = int(payload)
        if candidate > 0 and candidate != message.from_user.id:
            inviter_id = candidate
    await _db.register_user(
        message.from_user.id,
        message.from_user.username,
        message.from_user.first_name or "",
        inviter_id,
    )
    await send_subscription_gate(message, message.from_user.id, bot)


@router.message(Command("stop"))
async def stop_handler(message: Message, bot: Bot) -> None:
    if message.from_user is None:
        return
    await _db.mark_stopped(message.from_user.id, "/stop")
    await message.answer(
        "Botdan foydalanish to‘xtatildi. Qayta boshlash uchun /start yuboring.",
        reply_markup=ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text="/start")]], resize_keyboard=True
        ),
    )


@router.callback_query(F.data == "subscription:check")
async def subscription_check_handler(callback: CallbackQuery, bot: Bot) -> None:
    if callback.from_user is None or callback.message is None:
        await callback.answer()
        return
    ok, missing, failure = await check_subscriptions(bot, _db, callback.from_user.id)
    if failure:
        await callback.answer("Tekshiruv bajarilmadi. Bot kanal admini ekanini tekshiring.", show_alert=True)
        return
    if not ok:
        await callback.answer("Hali barcha Telegram kanallariga obuna bo‘lmagansiz.", show_alert=True)
        await callback.message.answer(
            subscription_text(missing),
            reply_markup=subscription_keyboard(await required_channels(_db), _config),
        )
        return
    await _db.qualify_referral(callback.from_user.id)
    await callback.answer("Obuna tasdiqlandi!")
    await show_main_menu(callback.message, callback.from_user.id)


@router.message(Command("admin"))
async def admin_command(message: Message) -> None:
    if message.from_user is None or await deny_admin(message):
        return
    await message.answer("🛠 <b>Administrator paneli</b>", reply_markup=admin_keyboard())


@router.message(F.text == "🛠 Admin panel")
async def admin_menu_button(message: Message) -> None:
    if message.from_user is None or await deny_admin(message):
        return
    await message.answer("🛠 <b>Administrator paneli</b>", reply_markup=admin_keyboard())


@router.message(F.text == "💎 Mening balansim")
async def profile_handler(message: Message) -> None:
    if message.from_user is None:
        return
    row = await _db.get_user(message.from_user.id)
    if row:
        await message.answer(format_user(row))
    else:
        await message.answer("Profil topilmadi. /start buyrug‘ini yuboring.")


@router.message(F.text == "👥 Do‘stlarni taklif qilish")
async def invite_handler(message: Message) -> None:
    if message.from_user is None:
        return
    username = _bot_username or _config.bot_username
    if not username:
        await message.answer("Taklif havolasi hozircha tayyor emas. Bot sozlamalarini tekshiring.")
        return
    link = f"https://t.me/{username}?start={message.from_user.id}"
    row = await _db.get_user(message.from_user.id)
    if not row:
        await message.answer("Avval /start orqali ro‘yxatdan o‘ting.")
        return
    await message.answer(
        "👥 <b>Do‘stlaringizni taklif qiling</b>\n\n"
        f"Shaxsiy havolangiz:\n<code>{html.escape(link)}</code>\n\n"
        "Do‘stingiz uchta majburiy Telegram kanaliga obuna bo‘lgach, taklif haqiqiy bo‘ladi "
        f"va sizga +{INVITE_REWARD} 💎 beriladi.",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[[InlineKeyboardButton(text="Havolani ochish", url=link)]]
        ),
    )


@router.message(F.text == "📊 Takliflar statistikasi")
async def referral_stats_handler(message: Message) -> None:
    if message.from_user is None:
        return
    row = await _db.get_user(message.from_user.id)
    if not row:
        await message.answer("Avval /start orqali ro‘yxatdan o‘ting.")
        return
    await message.answer(
        "📊 <b>Takliflar statistikasi</b>\n"
        f"Jami haqiqiy takliflar: {row['total_referrals']}\n"
        f"Faol takliflar: {row['active_referrals']}\n"
        f"Balansga olingan bonuslar: {row['balance']} 💎"
    )


@router.message(F.text == "🏆 TOP foydalanuvchilar")
async def top_handler(message: Message) -> None:
    await message.answer(await make_top_text("active_referrals"), reply_markup=top_keyboard())


@router.callback_query(F.data == "top:active")
async def top_active_callback(callback: CallbackQuery) -> None:
    await callback.answer()
    if callback.message:
        await callback.message.answer(
            await make_top_text("active_referrals"), reply_markup=top_keyboard()
        )


@router.callback_query(F.data == "top:total")
async def top_total_callback(callback: CallbackQuery) -> None:
    await callback.answer()
    if callback.message:
        await callback.message.answer(
            await make_top_text("total_referrals"), reply_markup=top_keyboard()
        )


@router.message(F.text == "🎁 Olmoslarni olish")
async def reward_menu_handler(message: Message) -> None:
    if message.from_user is None:
        return
    row = await _db.get_user(message.from_user.id)
    if not row:
        await message.answer("Avval /start orqali ro‘yxatdan o‘ting.")
        return
    if int(row["balance"]) < REWARD_COST:
        missing = REWARD_COST - int(row["balance"])
        await message.answer(
            f"🎁 Mukofot olish uchun kamida <b>{REWARD_COST} 💎</b> kerak.\n"
            f"Sizga yana <b>{missing} 💎</b> yetishmayapti."
        )
        return
    await message.answer(
        f"🎁 Balansingiz yetarli. Mukofot arizasi <b>{REWARD_COST} 💎</b> turadi.\n"
        "Ariza rad etilsa ballar qaytariladi.",
        reply_markup=reward_keyboard(),
    )


@router.callback_query(F.data == "reward:start")
async def reward_start_handler(callback: CallbackQuery, state: FSMContext) -> None:
    if callback.from_user is None or callback.message is None:
        await callback.answer()
        return
    row = await _db.get_user(callback.from_user.id)
    if not row or int(row["balance"]) < REWARD_COST:
        await callback.answer(f"Balansingiz {REWARD_COST} 💎 dan kam.", show_alert=True)
        return
    open_id = await _db.open_reward_id(callback.from_user.id)
    if open_id is not None:
        await callback.answer(f"Sizda #{open_id} raqamli ochiq ariza bor.", show_alert=True)
        return
    await state.set_state(UserFlow.reward_uid)
    await callback.answer()
    await callback.message.answer(
        "Free Fire UID raqamingizni yuboring (faqat 6–12 ta raqam). Bekor qilish uchun /cancel."
    )


@router.message(UserFlow.reward_uid, Command("cancel"))
async def cancel_reward_handler(message: Message, state: FSMContext) -> None:
    await state.clear()
    await message.answer(
        "Mukofot so‘rovi bekor qilindi.",
        reply_markup=main_keyboard(is_admin_user(message)),
    )


@router.message(UserFlow.reward_uid)
async def reward_uid_handler(message: Message, state: FSMContext, bot: Bot) -> None:
    if message.from_user is None:
        return
    ff_uid = (message.text or "").strip()
    if not re.fullmatch(r"\d{6,12}", ff_uid):
        await message.answer("UID noto‘g‘ri. 6–12 ta raqam kiriting yoki /cancel yuboring.")
        return
    status, request_id = await _db.create_reward_request(message.from_user.id, ff_uid)
    await state.clear()
    if status == "insufficient":
        row = await _db.get_user(message.from_user.id)
        missing = REWARD_COST - int(row["balance"]) if row else REWARD_COST
        await message.answer(f"Balans yetarli emas. Yana {missing} 💎 kerak.")
        return
    if status == "open_request":
        await message.answer(f"Sizda allaqachon #{request_id} raqamli ochiq ariza bor.")
        return
    if status != "created" or request_id is None:
        await message.answer("Ariza yaratilmadi. Birozdan so‘ng qayta urinib ko‘ring.")
        return
    await message.answer(
        f"✅ Arizangiz qabul qilindi. Raqami: <b>#{request_id}</b>\n"
        f"{REWARD_COST} 💎 balansdan yechildi. Admin tasdiqlagach holati yangilanadi.",
        reply_markup=main_keyboard(is_admin_user(message)),
    )
    for admin_id in _config.admin_ids:
        try:
            await bot.send_message(
                admin_id,
                f"🎁 Yangi mukofot arizasi <b>#{request_id}</b>\n"
                f"Foydalanuvchi: <code>{message.from_user.id}</code>\n"
                f"Free Fire UID: <code>{html.escape(ff_uid)}</code>",
                reply_markup=reward_admin_keyboard(request_id, "pending"),
            )
        except TelegramAPIError as exc:
            logger.warning("Admin xabari yuborilmadi (ID=%s): %s", admin_id, type(exc).__name__)


def reward_admin_keyboard(request_id: int, status: str) -> InlineKeyboardMarkup | None:
    if status == "pending":
        return InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(text="✅ Tasdiqlash", callback_data=f"admin:reward:approve:{request_id}"),
                    InlineKeyboardButton(text="❌ Rad etish", callback_data=f"admin:reward:reject:{request_id}"),
                ]
            ]
        )
    if status == "approved":
        return InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="📦 Yuborildi deb belgilash", callback_data=f"admin:reward:complete:{request_id}")]
            ]
        )
    return None


@router.message(F.text == "📜 Tranzaksiyalar tarixi")
async def transactions_handler(message: Message) -> None:
    if message.from_user is None:
        return
    rows = await _db.user_transactions(message.from_user.id)
    if not rows:
        await message.answer("📜 Hozircha tranzaksiyalar yo‘q.")
        return
    lines = ["📜 <b>Oxirgi tranzaksiyalar</b>"]
    for row in rows:
        mark = "+" if row["amount"] > 0 else ""
        lines.append(
            f"• {mark}{row['amount']} 💎 — {html.escape(row['description'])} "
            f"(balans: {row['balance_after']})"
        )
    await message.answer("\n".join(lines))


@router.message(F.text == "ℹ️ Bot qoidalari")
async def rules_handler(message: Message) -> None:
    await message.answer(rules_text())


@router.message(F.text == "📞 Administrator bilan aloqa")
async def contact_handler(message: Message) -> None:
    contact = await _db.get_setting("admin_contact", _config.admin_contact)
    if contact.startswith("@") and re.fullmatch(r"@[A-Za-z0-9_]{5,32}", contact):
        await message.answer(
            "📞 Administrator bilan bog‘lanish:",
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[[InlineKeyboardButton(text="✉️ Xabar yozish", url=f"https://t.me/{contact[1:]}")]]
            ),
        )
    elif is_http_url(contact):
        await message.answer(
            "📞 Administrator bilan bog‘lanish:",
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[[InlineKeyboardButton(text="✉️ Bog‘lanish", url=contact)]]
            ),
        )
    else:
        await message.answer("Admin bilan bog‘lanish manzili hali sozlanmagan.")


@router.message(F.text == "➕ Guruhga qo‘shish")
async def add_group_handler(message: Message) -> None:
    username = _bot_username or _config.bot_username
    if not username:
        await message.answer("Bot havolasi hozircha tayyor emas.")
        return
    link = f"https://t.me/{username}?startgroup=true"
    await message.answer(
        "AGAR SEN MENI GURUXINGA QUSHSANG MEN UZIM FREE FIREDAGI YANGILIKLARNI "
        "GURUXINGA YUBORIB TURAMAN ALBATTA XECHQANAQA REKLAMALARSIZ\n\n"
        "Bot qo‘shilgan guruhga admin tasdiqlagan Free Fire yangiliklari yuboriladi. "
        "Botga guruhda xabar yuborish ruxsati kerak.",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[[InlineKeyboardButton(text="➕ Botni guruhga qo‘shish", url=link)]]
        ),
    )


@router.message(Command("cancel"))
async def cancel_any_state(message: Message, state: FSMContext) -> None:
    await state.clear()
    await message.answer("Amal bekor qilindi.", reply_markup=main_keyboard(is_admin_user(message)))


@router.callback_query(F.data.startswith("admin:"))
async def admin_callback_dispatch(callback: CallbackQuery, state: FSMContext, bot: Bot) -> None:
    if await deny_admin(callback):
        return
    data = callback.data or ""
    admin_id = callback.from_user.id
    if data == "admin:users":
        await _db.log_admin(admin_id, "users_list", "offset=0")
        await callback.answer()
        await send_users_page(callback.message, 0)
    elif data.startswith("admin:users:"):
        await callback.answer()
        try:
            offset = max(0, int(data.rsplit(":", 1)[1]))
        except ValueError:
            offset = 0
        await _db.log_admin(admin_id, "users_list", f"offset={offset}")
        await send_users_page(callback.message, offset)
    elif data == "admin:lookup":
        await state.set_state(AdminFlow.lookup_id)
        await callback.answer()
        if callback.message:
            await callback.message.answer("Qidiriladigan foydalanuvchining Telegram ID sini yuboring.")
    elif data == "admin:balance":
        await state.set_state(AdminFlow.balance_id)
        await callback.answer()
        if callback.message:
            await callback.message.answer("Balansi o‘zgaradigan foydalanuvchining Telegram ID sini yuboring.")
    elif data == "admin:rewards":
        await _db.log_admin(admin_id, "rewards_list", "offset=0")
        await callback.answer()
        await send_reward_page(callback.message, 0)
    elif data.startswith("admin:rewards:"):
        await callback.answer()
        try:
            offset = max(0, int(data.rsplit(":", 1)[1]))
        except ValueError:
            offset = 0
        await _db.log_admin(admin_id, "rewards_list", f"offset={offset}")
        await send_reward_page(callback.message, offset)
    elif data == "admin:stats":
        await _db.log_admin(admin_id, "stats_view", "overview")
        await callback.answer()
        await send_admin_stats(callback.message)
    elif data == "admin:top":
        await _db.log_admin(admin_id, "ranking_view", "active_referrals")
        await callback.answer()
        await send_admin_top(callback.message, "active_referrals")
    elif data.startswith("admin:top:"):
        await callback.answer()
        order = data.rsplit(":", 1)[1]
        await _db.log_admin(admin_id, "ranking_view", order)
        await send_admin_top(
            callback.message,
            "total_referrals" if order == "total" else "active_referrals",
        )
    elif data == "admin:broadcast":
        await _db.log_admin(admin_id, "broadcast_compose", "started")
        await state.set_state(AdminFlow.broadcast_content)
        await callback.answer()
        if callback.message:
            await callback.message.answer("Foydalanuvchilarga yuboriladigan xabarni (matn/media) yuboring. Bekor qilish: /cancel")
    elif data == "admin:news":
        await _db.log_admin(admin_id, "news_compose", "started")
        await state.set_state(AdminFlow.news_content)
        await callback.answer()
        if callback.message:
            await callback.message.answer("Yangilik postini (matn, rasm yoki video) yuboring. Tarqatishdan oldin tasdiq so‘raladi. Bekor qilish: /cancel")
    elif data == "admin:settings":
        await _db.log_admin(admin_id, "settings_view", "opened")
        await callback.answer()
        await send_settings(callback.message)
    elif data == "admin:settings:edit":
        await state.set_state(AdminFlow.setting_key)
        await callback.answer()
        if callback.message:
            await callback.message.answer(
                "O‘zgartirish uchun kalitni yuboring:\n"
                "channel_1_chat_id / channel_1_title / channel_1_url (1–3), youtube_url, admin_contact, news_channel_ids"
            )
    elif data.startswith("admin:reward:"):
        parts = data.split(":")
        if len(parts) != 4:
            await callback.answer("Ariza tugmasi noto‘g‘ri.", show_alert=True)
            return
        action, request_text = parts[2], parts[3]
        if action not in {"approve", "reject", "complete"} or not request_text.isdigit():
            await callback.answer("Ariza tugmasi noto‘g‘ri.", show_alert=True)
            return
        success, status, user_id = await _db.transition_reward(
            int(request_text), action, admin_id
        )
        if not success:
            await callback.answer("Ariza holati o‘zgartirilmadi; u avval qayta ishlangan bo‘lishi mumkin.", show_alert=True)
            return
        await callback.answer("Ariza holati yangilandi.")
        if callback.message:
            try:
                await callback.message.edit_reply_markup(
                    reply_markup=reward_admin_keyboard(int(request_text), status)
                )
            except TelegramAPIError:
                pass
        if user_id:
            user_messages = {
                "approved": f"✅ #{request_text} arizangiz tasdiqlandi. Mukofot yuborilgach xabar beramiz.",
                "rejected": f"❌ #{request_text} arizangiz rad etildi. {REWARD_COST} 💎 balansingizga qaytarildi.",
                "completed": f"📦 #{request_text} mukofotingiz administrator tomonidan yuborilgan deb belgilandi.",
            }
            await safe_send_user(bot, user_id, user_messages[status])
    elif data.startswith("admin:broadcast:confirm:"):
        try:
            source_message_id = int(data.rsplit(":", 1)[1])
        except ValueError:
            await callback.answer("Xabar ID noto‘g‘ri.", show_alert=True)
            return
        await callback.answer("Yuborish boshlandi.")
        if callback.message:
            # Remove the buttons first so a double tap cannot send twice.
            try:
                await callback.message.edit_reply_markup(reply_markup=None)
            except TelegramAPIError:
                pass
            await confirm_broadcast(callback.message, admin_id, source_message_id, bot)
    elif data.startswith("admin:broadcast:cancel:"):
        await callback.answer("Yuborish bekor qilindi.")
        if callback.message:
            await callback.message.edit_text("Xabar yuborish bekor qilindi.")
        await _db.log_admin(admin_id, "broadcast_cancel", data)
    elif data.startswith("admin:news:confirm:"):
        try:
            draft_id = int(data.rsplit(":", 1)[1])
        except ValueError:
            await callback.answer("Post ID noto‘g‘ri.", show_alert=True)
            return
        await callback.answer("Yuborish boshlandi.")
        if callback.message:
            try:
                await callback.message.edit_reply_markup(reply_markup=None)
            except TelegramAPIError:
                pass
            await confirm_news(callback.message, admin_id, draft_id, bot)
    elif data.startswith("admin:news:cancel:"):
        await callback.answer("Yangilik yuborish bekor qilindi.")
        if callback.message:
            await callback.message.edit_text("Yangilik yuborish bekor qilindi.")
        await _db.log_admin(admin_id, "news_cancel", data)
    elif data == "admin:source":
        await callback.answer()
        if callback.message:
            await send_source_files(callback.message, bot, admin_id)
    else:
        await callback.answer("Noma’lum admin amali.", show_alert=True)


async def send_users_page(message: Message | None, offset: int) -> None:
    if message is None:
        return
    rows = await _db.list_users(10, offset)
    total = await _db.get_users_count()
    if not rows:
        await message.answer("Foydalanuvchilar topilmadi.")
        return
    lines = [f"👥 <b>Foydalanuvchilar</b> ({offset + 1}–{offset + len(rows)} / {total})"]
    for row in rows:
        label = safe_user_label(row) or "Ismsiz"
        lines.append(
            f"• <code>{row['telegram_id']}</code> {label} · {row['balance']} 💎 · {row['status']}"
        )
    buttons = []
    if offset > 0:
        buttons.append(InlineKeyboardButton(text="⬅️ Oldingi", callback_data=f"admin:users:{max(0, offset - 10)}"))
    if offset + 10 < total:
        buttons.append(InlineKeyboardButton(text="Keyingi ➡️", callback_data=f"admin:users:{offset + 10}"))
    await message.answer("\n".join(lines), reply_markup=InlineKeyboardMarkup(inline_keyboard=[buttons] if buttons else []))


async def send_reward_page(message: Message | None, offset: int) -> None:
    if message is None:
        return
    rows = await _db.reward_requests(10, offset)
    if not rows:
        await message.answer("🎁 Mukofot arizalari topilmadi.")
        return
    for row in rows:
        body = (
            f"🎁 <b>Ariza #{row['id']}</b> — {html.escape(row['status'])}\n"
            f"Foydalanuvchi: <code>{row['user_id']}</code> {html.escape(row['username'] or '')}\n"
            f"Free Fire UID: <code>{html.escape(row['free_fire_uid'])}</code>\n"
            f"Ball: {row['cost']} 💎 · Sana: {html.escape(row['created_at'])}"
        )
        await message.answer(body, reply_markup=reward_admin_keyboard(int(row["id"]), str(row["status"])))
    buttons: list[InlineKeyboardButton] = []
    if offset:
        buttons.append(InlineKeyboardButton(text="⬅️ Oldingi", callback_data=f"admin:rewards:{max(0, offset - 10)}"))
    if len(rows) == 10:
        buttons.append(InlineKeyboardButton(text="Keyingi ➡️", callback_data=f"admin:rewards:{offset + 10}"))
    if buttons:
        await message.answer("Arizalar sahifasi:", reply_markup=InlineKeyboardMarkup(inline_keyboard=[buttons]))


async def send_admin_stats(message: Message | None) -> None:
    if message is None:
        return
    stats = await _db.stats()
    await message.answer(
        "📊 <b>Umumiy statistika</b>\n"
        f"Foydalanuvchilar: {stats['users']}\n"
        f"Haqiqiy jami takliflar: {stats['referrals']}\n"
        f"Foydalanuvchilar balanslari: {stats['balance']} 💎\n"
        f"Sarflangan ballar: {stats['spent']} 💎\n"
        f"Qayd etilgan qarzdorlik: {stats['debt']} 💎\n"
        f"Mukofot arizalari: {stats['rewards']} (kutilmoqda: {stats['pending']}, bajarilgan: {stats['completed']})"
    )


async def send_admin_top(message: Message | None, order_by: str) -> None:
    if message is None:
        return
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="Faol takliflar", callback_data="admin:top:active"),
                InlineKeyboardButton(text="Jami takliflar", callback_data="admin:top:total"),
            ]
        ]
    )
    await message.answer(await make_top_text(order_by), reply_markup=keyboard)


async def send_settings(message: Message | None) -> None:
    if message is None:
        return
    channels = await required_channels(_db)
    lines = ["⚙️ <b>Joriy sozlamalar</b>"]
    for channel in channels:
        lines.append(
            f"{html.escape(channel['title'])}: ID=<code>{html.escape(channel['id'] or 'sozlanmagan')}</code>, "
            f"havola={html.escape(channel['url'] or 'sozlanmagan')}"
        )
    lines.append(f"YouTube: {html.escape(await _db.get_setting('youtube_url', _config.youtube_url) or 'sozlanmagan')}")
    lines.append(f"Admin aloqa: {html.escape(await _db.get_setting('admin_contact', _config.admin_contact) or 'sozlanmagan')}")
    await message.answer(
        "\n".join(lines),
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[[InlineKeyboardButton(text="✏️ Sozlamani o‘zgartirish", callback_data="admin:settings:edit")]]
        ),
    )


@router.message(AdminFlow.lookup_id)
async def admin_lookup_id(message: Message, state: FSMContext) -> None:
    if message.from_user is None or await deny_admin(message):
        return
    if not (message.text or "").strip().isdigit():
        await message.answer("Telegram ID faqat raqam bo‘lishi kerak.")
        return
    user_id = int(message.text.strip())
    row = await _db.get_user(user_id)
    await state.clear()
    if not row:
        await message.answer("Foydalanuvchi topilmadi.")
        return
    await message.answer(
        format_user(row)
        + f"\n🆔 ID: <code>{row['telegram_id']}</code>\n"
        + f"Username: @{html.escape(row['username'] or 'yo‘q')}\n"
        + f"Holat: {html.escape(row['status'])}"
    )
    await _db.log_admin(message.from_user.id, "user_lookup", f"user={user_id}")


@router.message(AdminFlow.balance_id)
async def admin_balance_user(message: Message, state: FSMContext) -> None:
    if message.from_user is None or await deny_admin(message):
        return
    text = (message.text or "").strip()
    if not text.isdigit():
        await message.answer("Telegram ID faqat raqam bo‘lishi kerak.")
        return
    user_id = int(text)
    row = await _db.get_user(user_id)
    if not row:
        await message.answer("Foydalanuvchi topilmadi. Boshqa ID yuboring yoki /cancel.")
        return
    await state.update_data(target_user_id=user_id)
    await state.set_state(AdminFlow.balance_change)
    await message.answer(
        "Miqdor va sababni bitta qatorda kiriting:\n"
        "<code>+10 Taklif bonusi</code> yoki <code>-5 Qoidabuzarlik</code>\n"
        "Sabab majburiy."
    )


@router.message(AdminFlow.balance_change)
async def admin_balance_change(message: Message, state: FSMContext) -> None:
    if message.from_user is None or await deny_admin(message):
        return
    match = re.fullmatch(r"\s*([+-]\d{1,7})\s+(.{3,300})\s*", message.text or "", re.S)
    if not match:
        await message.answer("Format xato. Masalan: <code>+10 Musobaqa mukofoti</code>. Sababni kiriting.")
        return
    amount = int(match.group(1))
    reason = match.group(2).strip()
    if amount == 0 or len(reason) < 3:
        await message.answer("Miqdor nol bo‘lmasin va sabab kamida 3 ta belgidan iborat bo‘lsin.")
        return
    state_data = await state.get_data()
    target_id = int(state_data["target_user_id"])
    success, balance = await _db.adjust_balance(target_id, amount, reason, message.from_user.id)
    await state.clear()
    if success:
        await message.answer(f"✅ ID <code>{target_id}</code> balans yangilandi: <b>{balance} 💎</b>.")
    else:
        await message.answer("Balans o‘zgartirilmadi: foydalanuvchi topilmadi.")


SETTING_KEYS = {
    "channel_1_chat_id",
    "channel_1_title",
    "channel_1_url",
    "channel_2_chat_id",
    "channel_2_title",
    "channel_2_url",
    "channel_3_chat_id",
    "channel_3_title",
    "channel_3_url",
    "youtube_url",
    "admin_contact",
    "news_channel_ids",
}


@router.message(AdminFlow.setting_key)
async def admin_setting_key(message: Message, state: FSMContext) -> None:
    if message.from_user is None or await deny_admin(message):
        return
    key = (message.text or "").strip()
    if key not in SETTING_KEYS:
        await message.answer("Bu kalit ruxsat etilmagan. Kalitlardan birini aynan yuboring yoki /cancel.")
        return
    await state.update_data(setting_key=key)
    await state.set_state(AdminFlow.setting_value)
    await message.answer(f"<code>{html.escape(key)}</code> uchun yangi qiymatni yuboring. Bo‘shatish uchun - yuboring.")


def validate_setting(key: str, value: str) -> bool:
    if len(value) > 300:
        return False
    if key.endswith("_url") or key in {"youtube_url", "admin_contact"}:
        return (
            not value
            or re.fullmatch(r"@[A-Za-z0-9_]{5,32}", value) is not None
            or is_http_url(value)
        )
    if key.endswith("_chat_id"):
        return not value or bool(re.fullmatch(r"@?[A-Za-z0-9_-]{3,80}|-?\d{5,20}", value))
    if key == "news_channel_ids":
        return not value or all(
            re.fullmatch(r"@?[A-Za-z0-9_-]{3,80}|-?\d{5,20}", item.strip())
            for item in value.split(",")
        )
    return True


@router.message(AdminFlow.setting_value)
async def admin_setting_value(message: Message, state: FSMContext) -> None:
    if message.from_user is None or await deny_admin(message):
        return
    data = await state.get_data()
    key = str(data["setting_key"])
    value = (message.text or "").strip()
    if value == "-":
        value = ""
    if not validate_setting(key, value):
        await message.answer("Qiymat noto‘g‘ri. URL https:// bilan boshlansin, kanal ID/username to‘g‘ri bo‘lsin.")
        return
    await _db.set_setting(key, value, message.from_user.id)
    await state.clear()
    await message.answer("✅ Sozlama saqlandi. Kanal o‘zgarishlarini keyingi obuna tekshiruvda ishlatadi.")


@router.message(AdminFlow.broadcast_content)
async def admin_broadcast_content(message: Message, state: FSMContext, bot: Bot) -> None:
    if message.from_user is None or await deny_admin(message):
        return
    await state.clear()
    target_count = await _db.get_users_count()
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text=f"✅ {target_count} foydalanuvchiga yuborish",
                    callback_data=f"admin:broadcast:confirm:{message.message_id}",
                ),
                InlineKeyboardButton(text="❌ Bekor qilish", callback_data=f"admin:broadcast:cancel:{message.message_id}"),
            ]
        ]
    )
    try:
        await bot.copy_message(message.chat.id, message.chat.id, message.message_id)
    except TelegramAPIError as exc:
        logger.info("Broadcast preview nusxalanmadi: %s", type(exc).__name__)
    await message.answer("Yuqoridagi xabarni barcha faol bot foydalanuvchilariga yuborishni tasdiqlaysizmi?", reply_markup=keyboard)


async def confirm_broadcast(
    message: Message | None, admin_id: int, source_message_id: int, bot: Bot
) -> None:
    if message is None:
        return
    user_ids = await _db.active_user_ids()
    sent = 0
    failed = 0
    semaphore = asyncio.Semaphore(12)

    async def deliver(user_id: int) -> None:
        nonlocal sent, failed
        async with semaphore:
            try:
                await bot.copy_message(user_id, admin_id, source_message_id)
                sent += 1
            except TelegramForbiddenError:
                failed += 1
                await _db.mark_stopped(user_id, "broadcast blocked")
            except TelegramRetryAfter as exc:
                await asyncio.sleep(float(exc.retry_after))
                try:
                    await bot.copy_message(user_id, admin_id, source_message_id)
                    sent += 1
                except TelegramAPIError:
                    failed += 1
            except TelegramAPIError:
                failed += 1
            await asyncio.sleep(0.04)

    await asyncio.gather(*(deliver(user_id) for user_id in user_ids))
    await _db.log_admin(admin_id, "broadcast", f"source_message={source_message_id}; sent={sent}; failed={failed}")
    await message.answer(f"📢 Xabar yakunlandi. Yuborildi: {sent}, yetkazilmadi: {failed}.")


@router.message(AdminFlow.news_content)
async def admin_news_content(message: Message, state: FSMContext, bot: Bot) -> None:
    if message.from_user is None or await deny_admin(message):
        return
    draft_id = await _db.create_news_draft(
        message.from_user.id, message.chat.id, message.message_id
    )
    await state.clear()
    try:
        await bot.copy_message(message.chat.id, message.chat.id, message.message_id)
    except TelegramAPIError as exc:
        logger.info("Yangilik preview nusxalanmadi: %s", type(exc).__name__)
    await message.answer(
        "Ushbu yangilikni sozlangan Telegram kanallari va bot qo‘shilgan guruhlarga yuborishni tasdiqlaysizmi?",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(text="✅ Tasdiqlash", callback_data=f"admin:news:confirm:{draft_id}"),
                    InlineKeyboardButton(text="❌ Bekor qilish", callback_data=f"admin:news:cancel:{draft_id}"),
                ]
            ]
        ),
    )


async def news_targets(bot: Bot, database: Database) -> tuple[list[str], list[int]]:
    channels = await required_channels(database)
    ids = [channel["id"] for channel in channels if channel["id"]]
    raw_extra = await database.get_setting("news_channel_ids", ",".join(_config.news_channel_ids))
    ids.extend(item.strip() for item in raw_extra.split(",") if item.strip())
    unique_channels = list(dict.fromkeys(ids))
    groups = await database.active_groups()
    # Only deliver to configured channels where the bot is actually an admin,
    # and to recorded groups where Telegram currently permits sending.
    valid_channels: list[str] = []
    for chat_id in unique_channels:
        try:
            member = await bot.get_chat_member(chat_id, bot.id)
            if getattr(member, "status", "") in {"creator", "administrator"}:
                valid_channels.append(chat_id)
        except TelegramAPIError:
            continue
    valid_groups: list[int] = []
    for chat_id in groups:
        try:
            member = await bot.get_chat_member(chat_id, bot.id)
            if getattr(member, "status", "") not in {"left", "kicked"}:
                valid_groups.append(chat_id)
        except TelegramAPIError:
            continue
    return valid_channels, valid_groups


async def confirm_news(message: Message | None, admin_id: int, draft_id: int, bot: Bot) -> None:
    if message is None:
        return
    draft = await _db.claim_news_draft(draft_id, admin_id)
    if draft is None:
        await message.answer("Bu yangilik avval yuborilgan, bekor qilingan yoki topilmadi.")
        return
    channels, groups = await news_targets(bot, _db)
    targets: list[int | str] = [*channels, *groups]
    sent = 0
    failed_targets: list[str] = []
    for target in targets:
        try:
            await bot.copy_message(
                chat_id=target,
                from_chat_id=int(draft["source_chat_id"]),
                message_id=int(draft["source_message_id"]),
            )
            sent += 1
        except TelegramAPIError as exc:
            failed_targets.append(f"{target}:{type(exc).__name__}")
    final_status = "sent" if targets and not failed_targets else "partial"
    result = f"sent={sent}; failed={','.join(failed_targets)}"
    await _db.finish_news_draft(draft_id, final_status, result)
    await _db.log_admin(admin_id, "news_publish", f"draft={draft_id}; {result}")
    await message.answer(
        f"📰 Yangilik tarqatildi. Yuborildi: {sent}, yuborilmagan: {len(failed_targets)}."
        + ("\nHech bir ruxsatli manzil topilmadi." if not targets else "")
    )


async def safe_send_user(bot: Bot, user_id: int, text: str) -> bool:
    try:
        await bot.send_message(user_id, text)
        return True
    except TelegramForbiddenError:
        await _db.mark_stopped(user_id, "message blocked")
        return False
    except TelegramAPIError as exc:
        logger.warning("Foydalanuvchiga xabar yetkazilmadi (ID=%s): %s", user_id, type(exc).__name__)
        return False


async def send_source_files(message: Message, bot: Bot, admin_id: int) -> None:
    await _db.log_admin(admin_id, "source_files_sent", "bot.py,requirements.txt")
    for filename, caption in (
        ("bot.py", "To‘liq bot kodi — bitta faylda."),
        ("requirements.txt", "Bot uchun Python paketlari."),
    ):
        path = PROJECT_DIR / filename
        if not path.is_file():
            await message.answer(f"{filename} fayli topilmadi.")
            continue
        try:
            await bot.send_document(
                message.chat.id,
                FSInputFile(path, filename=filename),
                caption=caption,
            )
        except TelegramAPIError as exc:
            logger.warning("Manba fayli yuborilmadi (%s): %s", filename, type(exc).__name__)
            await message.answer(f"{filename} yuborilmadi; xatolik: {type(exc).__name__}.")


@router.my_chat_member()
async def group_membership_handler(event: ChatMemberUpdated) -> None:
    chat = event.chat
    if chat.type not in {ChatType.GROUP, ChatType.SUPERGROUP}:
        return
    status = event.new_chat_member.status
    is_active = status not in {"left", "kicked"}
    await _db.save_group(chat.id, chat.title or "", is_active)


@router.errors()
async def errors_handler(event: Any) -> bool:
    exception = event.exception
    update = getattr(event, "update", None)
    actor_id: int | None = None
    if update is not None:
        if update.message and update.message.from_user:
            actor_id = update.message.from_user.id
        elif update.callback_query and update.callback_query.from_user:
            actor_id = update.callback_query.from_user.id
        elif update.edited_message and update.edited_message.from_user:
            actor_id = update.edited_message.from_user.id
    if isinstance(exception, TelegramForbiddenError) and actor_id:
        await _db.mark_stopped(actor_id, "Telegram blocked bot")
        logger.info("Bot bloklangani xabar yuborishda aniqlandi (ID=%s)", actor_id)
        return True
    logger.error("Update ishlovida xatolik: %s", type(exception).__name__, exc_info=exception)
    if actor_id and update and update.message:
        try:
            await update.message.answer("Xatolik yuz berdi. Birozdan so‘ng qayta urinib ko‘ring.")
        except TelegramAPIError:
            pass
    return True


@router.message()
async def fallback_handler(message: Message) -> None:
    if message.chat.type != ChatType.PRIVATE:
        return
    if message.text and message.text.startswith("/"):
        await message.answer("Buyruq topilmadi. Menyudan bo‘lim tanlang.", reply_markup=main_keyboard(is_admin_user(message)))
        return
    await message.answer("Menyudagi tugmalardan birini tanlang.", reply_markup=main_keyboard(is_admin_user(message)))


async def startup(bot: Bot) -> None:
    global _bot_username
    me = await bot.get_me()
    _bot_username = _config.bot_username or (me.username or "")
    try:
        await bot.set_my_name(name=APP_NAME)
    except TelegramAPIError as exc:
        logger.warning("Bot profilidagi ko‘rinadigan nom yangilanmadi: %s", type(exc).__name__)
    if not _bot_username:
        logger.warning("BOT_USERNAME sozlanmagan va Telegram profilida username yo‘q; referral havolalari ishlamaydi.")
    await bot.set_my_commands(
        [
            BotCommand(command="start", description="Botni boshlash"),
            BotCommand(command="stop", description="Botdan chiqish"),
            BotCommand(command="admin", description="Administrator paneli"),
        ]
    )
    logger.info("%s ishga tushdi (bot=@%s)", APP_NAME, _bot_username or "username yo‘q")
    logger.info("Bazasi: %s; adminlar soni: %d", _config.db_path, len(_config.admin_ids))
    channels = await required_channels(_db)
    for channel in channels:
        if not channel["id"]:
            logger.warning("%s uchun chat ID sozlanmagan; admin sozlamasida kiriting.", channel["key"])


async def main() -> None:
    global _db, _config
    _config = Config.from_env(require_token=True)
    _db = Database(_config.db_path)
    await _db.initialize()
    defaults: dict[str, str] = {
        "admin_contact": _config.admin_contact,
        "youtube_url": _config.youtube_url,
        "news_channel_ids": ",".join(_config.news_channel_ids),
    }
    for index, channel in enumerate(_config.channels, start=1):
        defaults[f"channel_{index}_chat_id"] = channel["id"]
        defaults[f"channel_{index}_title"] = channel["title"]
        defaults[f"channel_{index}_url"] = channel["url"]
    await _db.seed_settings(defaults)
    bot = Bot(_config.token, default=DefaultBotProperties(parse_mode="HTML"))
    dispatcher = Dispatcher(storage=MemoryStorage())
    dispatcher.update.outer_middleware(IdempotencyMiddleware(_db))
    dispatcher.message.outer_middleware(ThrottlingMiddleware())
    dispatcher.callback_query.outer_middleware(ThrottlingMiddleware())
    dispatcher.message.outer_middleware(SubscriptionGateMiddleware(_db, _config))
    dispatcher.callback_query.outer_middleware(SubscriptionGateMiddleware(_db, _config))
    dispatcher.include_router(router)
    try:
        await startup(bot)
        await dispatcher.start_polling(
            bot,
            allowed_updates=dispatcher.resolve_used_update_types(),
            close_bot_session=True,
        )
    finally:
        await _db.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot to‘xtatildi.")
    except RuntimeError as exc:
        logger.error("%s", exc)
        raise SystemExit(1) from exc
