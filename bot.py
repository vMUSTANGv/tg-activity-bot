# -*- coding: utf-8 -*-
import os
import base64
import asyncio
import sqlite3
import logging

import httpx
from aiogram import Bot, Dispatcher, Router
from aiogram.types import Message, PollAnswer, MessageReactionUpdated, BotCommand, BotCommandScopeAllGroupChats, BotCommandScopeAllPrivateChats
from aiogram.filters import Command, CommandStart
from aiogram.webhook.aiohttp_server import SimpleRequestHandler, setup_application
from aiohttp import web

from groq import Groq

BOT_TOKEN = os.getenv("BOT_TOKEN", "")
GROQ_API_KEY = os.getenv("GROQ_API_KEY", "")
RENDER_URL = os.getenv("RENDER_URL", "")
WEBHOOK_PATH = f"/webhook/{BOT_TOKEN}"

# Turso (libSQL) — облачный SQLite, переживает рестарты Render.
# Если переменные не заданы — fallback на локальный sqlite (для разработки).
TURSO_URL = os.getenv("TURSO_DATABASE_URL", "")
TURSO_TOKEN = os.getenv("TURSO_AUTH_TOKEN", "")
DB_PATH = os.getenv("DB_PATH", "activity.db")

USE_TURSO = bool(TURSO_URL)

SUMMARY_MSG_COUNT = 300
PORT = int(os.getenv("PORT", "10000"))

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

groq_client = None
if GROQ_API_KEY:
    groq_client = Groq(api_key=GROQ_API_KEY)

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()
router = Router()
dp.include_router(router)


# ---------- DB layer (Turso HTTP / sqlite sync) ----------

class TursoHTTP:
    """Минимальный async-клиент к Turso v2 pipeline API.
    Используется вместо libsql-client (он сломан с актуальным сервером)."""

    def __init__(self, url: str, token: str):
        if url.startswith("libsql://"):
            url = "https://" + url[len("libsql://"):]
        self.url = url.rstrip("/") + "/v2/pipeline"
        self.headers = {"Authorization": f"Bearer {token}"}
        self._client = httpx.AsyncClient(timeout=30.0)

    @staticmethod
    def _arg(v):
        if v is None:
            return {"type": "null", "value": None}
        if isinstance(v, bool):
            return {"type": "integer", "value": str(int(v))}
        if isinstance(v, int):
            return {"type": "integer", "value": str(v)}
        if isinstance(v, float):
            return {"type": "float", "value": v}
        if isinstance(v, (bytes, bytearray)):
            return {"type": "blob", "base64": base64.b64encode(v).decode()}
        return {"type": "text", "value": str(v)}

    @staticmethod
    def _decode_cell(cell):
        t = cell.get("type")
        v = cell.get("value")
        if t == "null":
            return None
        if t == "integer":
            return int(v) if v is not None else None
        if t == "float":
            return float(v) if v is not None else None
        return v

    async def execute(self, sql: str, params=()):
        payload = {
            "requests": [
                {"type": "execute", "stmt": {
                    "sql": sql,
                    "args": [self._arg(p) for p in params],
                }},
                {"type": "close"},
            ]
        }
        r = await self._client.post(self.url, headers=self.headers, json=payload)
        r.raise_for_status()
        data = r.json()
        results = data.get("results", [])
        if not results:
            return []
        first = results[0]
        if first.get("type") == "error":
            raise RuntimeError(f"Turso error: {first.get('error')}")
        result = first.get("response", {}).get("result", {})
        rows = []
        for row in result.get("rows", []):
            rows.append(tuple(self._decode_cell(c) for c in row))
        return rows


_turso: TursoHTTP | None = None


async def db_connect():
    global _turso
    if USE_TURSO and _turso is None:
        _turso = TursoHTTP(TURSO_URL, TURSO_TOKEN)


CREATE_TABLES = [
    """CREATE TABLE IF NOT EXISTS messages (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        chat_id INTEGER NOT NULL,
        message_id INTEGER,
        user_id INTEGER NOT NULL,
        username TEXT,
        full_name TEXT,
        text TEXT,
        ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )""",
    """CREATE TABLE IF NOT EXISTS reactions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        chat_id INTEGER NOT NULL,
        message_id INTEGER,
        user_id INTEGER NOT NULL,
        username TEXT,
        full_name TEXT,
        target_user_id INTEGER,
        emoji TEXT,
        direction TEXT DEFAULT 'given',
        ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )""",
    """CREATE TABLE IF NOT EXISTS poll_votes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        chat_id INTEGER,
        user_id INTEGER NOT NULL,
        username TEXT,
        full_name TEXT,
        poll_id TEXT,
        ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )""",
    """CREATE TABLE IF NOT EXISTS polls (
        poll_id TEXT PRIMARY KEY,
        chat_id INTEGER NOT NULL,
        ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )""",
    """CREATE TABLE IF NOT EXISTS achievements (
        chat_id INTEGER NOT NULL,
        user_id INTEGER NOT NULL,
        badge_key TEXT NOT NULL,
        ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (chat_id, user_id, badge_key)
    )""",
]

CREATE_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_msg_chat ON messages(chat_id)",
    "CREATE INDEX IF NOT EXISTS idx_msg_chat_msgid ON messages(chat_id, message_id)",
    "CREATE INDEX IF NOT EXISTS idx_react_chat ON reactions(chat_id)",
    "CREATE INDEX IF NOT EXISTS idx_react_target ON reactions(chat_id, target_user_id)",
]

# Миграции для существующих БД (добавление колонок). ALTER без IF NOT EXISTS,
# поэтому ловим ошибку "duplicate column".
MIGRATIONS = [
    "ALTER TABLE messages ADD COLUMN message_id INTEGER",
    "ALTER TABLE reactions ADD COLUMN message_id INTEGER",
    "ALTER TABLE reactions ADD COLUMN target_user_id INTEGER",
]


async def _safe_migrate(stmt: str):
    try:
        if USE_TURSO:
            await _turso.execute(stmt)
        else:
            con = sqlite3.connect(DB_PATH)
            con.execute(stmt)
            con.commit()
            con.close()
    except Exception as e:
        msg = str(e).lower()
        if "duplicate column" in msg or "already exists" in msg:
            return
        log.warning(f"Migration skipped ({stmt}): {e}")


async def init_db():
    if USE_TURSO:
        await db_connect()
        # Порядок важен: таблицы → миграции колонок → индексы (индексы могут
        # ссылаться на колонки, добавленные миграциями).
        for stmt in CREATE_TABLES:
            await _turso.execute(stmt)
        for stmt in MIGRATIONS:
            await _safe_migrate(stmt)
        for stmt in CREATE_INDEXES:
            await _turso.execute(stmt)
        log.info("Turso DB initialized")
    else:
        db_dir = os.path.dirname(DB_PATH)
        if db_dir:
            os.makedirs(db_dir, exist_ok=True)
        con = sqlite3.connect(DB_PATH)
        for stmt in CREATE_TABLES:
            con.execute(stmt)
        con.commit()
        con.close()
        for stmt in MIGRATIONS:
            await _safe_migrate(stmt)
        con = sqlite3.connect(DB_PATH)
        for stmt in CREATE_INDEXES:
            con.execute(stmt)
        con.commit()
        con.close()
        log.info(f"Local SQLite initialized at {DB_PATH}")


async def db_execute(sql: str, params=()):
    """INSERT/UPDATE — без возврата."""
    if USE_TURSO:
        await _turso.execute(sql, list(params))
    else:
        con = sqlite3.connect(DB_PATH)
        con.execute(sql, params)
        con.commit()
        con.close()


async def db_fetchall(sql: str, params=()):
    """SELECT — список tuple."""
    if USE_TURSO:
        return await _turso.execute(sql, list(params))
    else:
        con = sqlite3.connect(DB_PATH)
        rows = con.execute(sql, params).fetchall()
        con.close()
        return rows


async def db_fetchone(sql: str, params=()):
    rows = await db_fetchall(sql, params)
    return rows[0] if rows else None


# ---------- helpers ----------

def user_label(user_id, username, full_name):
    if username:
        return f"@{username}"
    return f"id:{user_id}"


def is_group(msg: Message) -> bool:
    return msg.chat.type in ("group", "supergroup")


def period_filter(period: str):
    """Возвращает (sql_clause, params) для фильтрации по ts."""
    if period == "week":
        return " AND ts >= datetime('now','-7 days')", ()
    if period == "month":
        return " AND ts >= datetime('now','-30 days')", ()
    return "", ()


# ---------- achievements ----------

MSG_THRESHOLDS = [100, 500, 1000, 5000, 10000]
REACT_GIVEN_THRESHOLDS = [50, 200, 1000]
REACT_RECV_THRESHOLDS = [50, 200, 1000]
POLL_THRESHOLDS = [10, 50, 200]

BADGE_LABELS = {
    "msg": "💬 {n} сообщений",
    "react_given": "👍 {n} реакций поставлено",
    "react_recv": "❤️ {n} реакций получено",
    "poll": "🗳 {n} голосов в опросах",
}


async def check_and_award(chat_id: int, user_id: int, category: str, count: int, thresholds: list[int]):
    """Если count точно совпадает с порогом — выдаёт бейдж (один раз). Возвращает текст бейджа или None."""
    if count not in thresholds:
        return None
    badge_key = f"{category}_{count}"
    existing = await db_fetchall(
        "SELECT 1 FROM achievements WHERE chat_id=? AND user_id=? AND badge_key=?",
        (chat_id, user_id, badge_key),
    )
    if existing:
        return None
    await db_execute(
        "INSERT INTO achievements (chat_id, user_id, badge_key) VALUES (?,?,?)",
        (chat_id, user_id, badge_key),
    )
    return BADGE_LABELS[category].format(n=count)


# ---------- handlers ----------

@router.message(~Command("stats", "summary", "start", "help", "silent", "digest"))
async def on_message(msg: Message):
    if not msg.from_user or not is_group(msg):
        return
    await db_execute(
        "INSERT INTO messages (chat_id, message_id, user_id, username, full_name, text) VALUES (?,?,?,?,?,?)",
        (msg.chat.id, msg.message_id, msg.from_user.id, msg.from_user.username,
         msg.from_user.full_name, (msg.text or msg.caption or "")[:4000]),
    )
    # Если в сообщении опрос — запоминаем привязку poll_id → chat_id.
    if msg.poll:
        await db_execute(
            "INSERT OR IGNORE INTO polls (poll_id, chat_id) VALUES (?,?)",
            (msg.poll.id, msg.chat.id),
        )

    # Проверка достижений по числу сообщений.
    row = await db_fetchall(
        "SELECT COUNT(*) FROM messages WHERE chat_id=? AND user_id=?",
        (msg.chat.id, msg.from_user.id),
    )
    cnt = row[0][0] if row else 0
    badge = await check_and_award(msg.chat.id, msg.from_user.id, "msg", cnt, MSG_THRESHOLDS)
    if badge:
        await msg.answer(
            f"🎉 {user_label(msg.from_user.id, msg.from_user.username, msg.from_user.full_name)} получает достижение: {badge}!"
        )


@router.message_reaction()
async def on_reaction(event: MessageReactionUpdated):
    if not event.user:
        return

    # Автор сообщения, на которое ставится реакция.
    target_row = await db_fetchone(
        "SELECT user_id FROM messages WHERE chat_id=? AND message_id=? LIMIT 1",
        (event.chat.id, event.message_id),
    )
    target_user_id = target_row[0] if target_row else None

    new_emojis = {
        getattr(r, "emoji", None) or getattr(r, "custom_emoji_id", "custom")
        for r in (event.new_reaction or [])
    }
    old_emojis = {
        getattr(r, "emoji", None) or getattr(r, "custom_emoji_id", "custom")
        for r in (event.old_reaction or [])
    }

    # Снятые реакции — удаляем соответствующие записи (по одной на эмодзи).
    for emoji in old_emojis - new_emojis:
        await db_execute(
            """DELETE FROM reactions WHERE id = (
                SELECT id FROM reactions
                WHERE chat_id=? AND user_id=? AND message_id=? AND emoji=?
                LIMIT 1
            )""",
            (event.chat.id, event.user.id, event.message_id, emoji),
        )

    # Новые реакции — добавляем.
    new_added = new_emojis - old_emojis
    for emoji in new_added:
        await db_execute(
            "INSERT INTO reactions (chat_id, message_id, user_id, username, full_name, target_user_id, emoji, direction) VALUES (?,?,?,?,?,?,?,'given')",
            (event.chat.id, event.message_id, event.user.id, event.user.username,
             event.user.full_name, target_user_id, emoji),
        )

    if not new_added:
        return

    # Достижение «поставлено реакций»
    given_row = await db_fetchall(
        "SELECT COUNT(*) FROM reactions WHERE chat_id=? AND user_id=?",
        (event.chat.id, event.user.id),
    )
    given_cnt = given_row[0][0] if given_row else 0
    badge = await check_and_award(event.chat.id, event.user.id, "react_given", given_cnt, REACT_GIVEN_THRESHOLDS)
    if badge:
        await bot.send_message(
            event.chat.id,
            f"🎉 {user_label(event.user.id, event.user.username, event.user.full_name)} получает достижение: {badge}!",
        )

    # Достижение «получено реакций» — для автора сообщения
    if target_user_id:
        recv_row = await db_fetchall(
            "SELECT COUNT(*) FROM reactions WHERE chat_id=? AND target_user_id=?",
            (event.chat.id, target_user_id),
        )
        recv_cnt = recv_row[0][0] if recv_row else 0
        badge = await check_and_award(event.chat.id, target_user_id, "react_recv", recv_cnt, REACT_RECV_THRESHOLDS)
        if badge:
            # Подтянем username автора из messages
            author = await db_fetchall(
                "SELECT username, full_name FROM messages WHERE chat_id=? AND user_id=? ORDER BY id DESC LIMIT 1",
                (event.chat.id, target_user_id),
            )
            uname = author[0][0] if author else None
            fname = author[0][1] if author else None
            await bot.send_message(
                event.chat.id,
                f"🎉 {user_label(target_user_id, uname, fname)} получает достижение: {badge}!",
            )


@router.poll_answer()
async def on_poll_answer(answer: PollAnswer):
    if not answer.user:
        return
    # Достаём chat_id из таблицы polls (заполняется при появлении опроса в чате).
    poll_row = await db_fetchone(
        "SELECT chat_id FROM polls WHERE poll_id=?",
        (answer.poll_id,),
    )
    chat_id = poll_row[0] if poll_row else None
    await db_execute(
        "INSERT INTO poll_votes (chat_id, user_id, username, full_name, poll_id) VALUES (?,?,?,?,?)",
        (chat_id, answer.user.id, answer.user.username, answer.user.full_name, answer.poll_id),
    )

    if chat_id is None:
        return
    poll_row2 = await db_fetchall(
        "SELECT COUNT(*) FROM poll_votes WHERE chat_id=? AND user_id=?",
        (chat_id, answer.user.id),
    )
    poll_cnt = poll_row2[0][0] if poll_row2 else 0
    badge = await check_and_award(chat_id, answer.user.id, "poll", poll_cnt, POLL_THRESHOLDS)
    if badge:
        await bot.send_message(
            chat_id,
            f"🎉 {user_label(answer.user.id, answer.user.username, answer.user.full_name)} получает достижение: {badge}!",
        )


@router.message(CommandStart())
async def cmd_start(msg: Message):
    await msg.answer(
        "Activity Bot\n\n"
        "Отслеживаю активность в чате:\n"
        "- сообщения\n"
        "- реакции (лайки)\n"
        "- голоса в опросах\n\n"
        "/stats - топ активности\n"
        "/summary - выжимка 300 сообщений\n"
        "/help - справка"
    )


@router.message(Command("help"))
async def cmd_help(msg: Message):
    await msg.answer(
        "Настройка:\n\n"
        "1. Добавь бота в группу\n"
        "2. Назначь администратором\n"
        "3. @BotFather - Bot Settings - Group Privacy - OFF"
    )


@router.message(Command("stats"))
async def cmd_stats(msg: Message):
    if not is_group(msg):
        return
    chat_id = msg.chat.id

    # Парсим аргумент: /stats, /stats week, /stats month
    args = (msg.text or "").split()
    period = args[1].lower() if len(args) > 1 else "all"
    if period not in ("all", "week", "month"):
        period = "all"
    pf, _ = period_filter(period)
    period_label = {"all": "за всё время", "week": "за неделю", "month": "за месяц"}[period]

    rows_msg = await db_fetchall(
        f"SELECT user_id, username, full_name, COUNT(*) cnt FROM messages WHERE chat_id=?{pf} GROUP BY user_id ORDER BY cnt DESC LIMIT 15",
        (chat_id,),
    )

    rows_react_given = await db_fetchall(
        f"SELECT user_id, username, full_name, COUNT(*) cnt FROM reactions WHERE chat_id=? AND direction='given'{pf} GROUP BY user_id ORDER BY cnt DESC LIMIT 10",
        (chat_id,),
    )

    # Топ полученных реакций — джойним с messages, чтобы достать ник автора.
    rows_react_recv = await db_fetchall(
        f"""SELECT r.target_user_id,
                  (SELECT username FROM messages m WHERE m.user_id=r.target_user_id AND m.chat_id=r.chat_id ORDER BY m.id DESC LIMIT 1) un,
                  (SELECT full_name FROM messages m WHERE m.user_id=r.target_user_id AND m.chat_id=r.chat_id ORDER BY m.id DESC LIMIT 1) fn,
                  COUNT(*) cnt
            FROM reactions r
            WHERE r.chat_id=? AND r.target_user_id IS NOT NULL{pf.replace('ts', 'r.ts')}
            GROUP BY r.target_user_id
            ORDER BY cnt DESC LIMIT 10""",
        (chat_id,),
    )

    rows_polls = await db_fetchall(
        f"SELECT user_id, username, full_name, COUNT(*) cnt FROM poll_votes WHERE chat_id=?{pf} GROUP BY user_id ORDER BY cnt DESC LIMIT 10",
        (chat_id,),
    )

    total_msg_row = await db_fetchone(f"SELECT COUNT(*) FROM messages WHERE chat_id=?{pf}", (chat_id,))
    total_react_row = await db_fetchone(f"SELECT COUNT(*) FROM reactions WHERE chat_id=?{pf}", (chat_id,))
    total_msg = total_msg_row[0] if total_msg_row else 0
    total_react = total_react_row[0] if total_react_row else 0

    lines = [
        f"Активность чата ({period_label})\n",
        f"Сообщений: {total_msg} | Реакций: {total_react}\n",
    ]

    if rows_msg:
        lines.append("Топ по сообщениям:")
        for i, (uid, uname, fname, cnt) in enumerate(rows_msg, 1):
            lines.append(f"  {i}. {user_label(uid, uname, fname)} - {cnt}")

    if rows_react_given:
        lines.append("\nТоп по поставленным реакциям:")
        for i, (uid, uname, fname, cnt) in enumerate(rows_react_given, 1):
            lines.append(f"  {i}. {user_label(uid, uname, fname)} - {cnt}")

    if rows_react_recv:
        lines.append("\nТоп по полученным реакциям:")
        for i, (uid, uname, fname, cnt) in enumerate(rows_react_recv, 1):
            lines.append(f"  {i}. {user_label(uid, uname, fname)} - {cnt}")

    if rows_polls:
        lines.append("\nТоп по голосованиям:")
        for i, (uid, uname, fname, cnt) in enumerate(rows_polls, 1):
            lines.append(f"  {i}. {user_label(uid, uname, fname)} - {cnt}")

    if not rows_msg and not rows_react_given:
        lines.append("Пока нет данных.")

    lines.append("\nИспользование: /stats [week|month]")
    await msg.answer("\n".join(lines))


@router.message(Command("silent"))
async def cmd_silent(msg: Message):
    if not is_group(msg):
        return
    args = (msg.text or "").split()
    days = 14
    if len(args) > 1 and args[1].isdigit():
        days = max(1, min(int(args[1]), 365))

    rows = await db_fetchall(
        """SELECT user_id,
                  (SELECT username FROM messages WHERE chat_id=? AND user_id=m.user_id ORDER BY id DESC LIMIT 1) un,
                  (SELECT full_name FROM messages WHERE chat_id=? AND user_id=m.user_id ORDER BY id DESC LIMIT 1) fn,
                  MAX(ts) last_ts,
                  COUNT(*) total
           FROM messages m
           WHERE chat_id=?
           GROUP BY user_id
           HAVING MAX(ts) < datetime('now','-' || ? || ' days')
           ORDER BY last_ts ASC
           LIMIT 30""",
        (msg.chat.id, msg.chat.id, msg.chat.id, days),
    )

    if not rows:
        await msg.answer(f"😎 Молчунов за {days} дней нет — все на связи.")
        return

    lines = [f"😴 *Молчуны* (нет сообщений {days}+ дней)\n"]
    for uid, uname, fname, last_ts, total in rows:
        last_short = (last_ts or "")[:10]
        lines.append(f"• {user_label(uid, uname, fname)} — последний раз {last_short} (всего: {total})")
    lines.append(f"\n_Использование: /silent [дней]_")
    await msg.answer("\n".join(lines), parse_mode="Markdown")


@router.message(Command("digest"))
async def cmd_digest(msg: Message):
    if not is_group(msg):
        return
    chat_id = msg.chat.id

    total_msgs_row = await db_fetchall(
        "SELECT COUNT(*) FROM messages WHERE chat_id=? AND ts >= datetime('now','-7 days')",
        (chat_id,),
    )
    total_react_row = await db_fetchall(
        "SELECT COUNT(*) FROM reactions WHERE chat_id=? AND ts >= datetime('now','-7 days')",
        (chat_id,),
    )
    total_polls_row = await db_fetchall(
        "SELECT COUNT(*) FROM poll_votes WHERE chat_id=? AND ts >= datetime('now','-7 days')",
        (chat_id,),
    )
    active_users_row = await db_fetchall(
        "SELECT COUNT(DISTINCT user_id) FROM messages WHERE chat_id=? AND ts >= datetime('now','-7 days')",
        (chat_id,),
    )
    total_msgs = total_msgs_row[0][0] if total_msgs_row else 0
    total_react = total_react_row[0][0] if total_react_row else 0
    total_polls = total_polls_row[0][0] if total_polls_row else 0
    active_users = active_users_row[0][0] if active_users_row else 0

    if total_msgs < 5:
        await msg.answer("Слишком мало сообщений за неделю для дайджеста.")
        return

    top_msg = await db_fetchall(
        "SELECT user_id, username, full_name, COUNT(*) cnt FROM messages WHERE chat_id=? AND ts >= datetime('now','-7 days') GROUP BY user_id ORDER BY cnt DESC LIMIT 3",
        (chat_id,),
    )
    top_recv = await db_fetchall(
        """SELECT r.target_user_id,
                  (SELECT username FROM messages m WHERE m.user_id=r.target_user_id AND m.chat_id=r.chat_id ORDER BY m.id DESC LIMIT 1),
                  (SELECT full_name FROM messages m WHERE m.user_id=r.target_user_id AND m.chat_id=r.chat_id ORDER BY m.id DESC LIMIT 1),
                  COUNT(*) cnt
           FROM reactions r
           WHERE r.chat_id=? AND r.target_user_id IS NOT NULL AND r.ts >= datetime('now','-7 days')
           GROUP BY r.target_user_id ORDER BY cnt DESC LIMIT 3""",
        (chat_id,),
    )

    # Сообщения для AI-выжимки тем
    log_rows = await db_fetchall(
        "SELECT user_id, username, text FROM messages WHERE chat_id=? AND text!='' AND ts >= datetime('now','-7 days') ORDER BY id DESC LIMIT 400",
        (chat_id,),
    )
    log_rows.reverse()
    chat_log = "\n".join(
        f"{('@' + un) if un else f'id:{uid}'}: {txt}"
        for uid, un, txt in log_rows
    )

    topics = "—"
    if groq_client and chat_log:
        try:
            response = await asyncio.to_thread(
                groq_client.chat.completions.create,
                model="llama-3.3-70b-versatile",
                messages=[
                    {"role": "system", "content": "Ты выделяешь главные темы недельного чата. Отвечай по-русски, очень кратко."},
                    {"role": "user", "content": f"Лог за неделю:\n\n{chat_log}\n\nВыдели 3-5 главных тем недели маркированным списком. Каждая тема — одна строка с эмодзи в начале. Никаких пояснений, никаких имён, только темы."},
                ],
                max_tokens=400,
            )
            topics = (response.choices[0].message.content or "—").strip()
        except Exception as e:
            log.exception("Groq digest error")
            topics = f"_не удалось получить темы: {e}_"

    lines = [
        "📊 *Дайджест за неделю*\n",
        f"💬 Сообщений: *{total_msgs}*",
        f"❤️ Реакций: *{total_react}*",
        f"🗳 Голосов в опросах: *{total_polls}*",
        f"👥 Активных участников: *{active_users}*",
        "",
        "🔥 *Главные темы:*",
        topics,
        "",
    ]

    if top_msg:
        lines.append("🏆 *Топ авторов:*")
        for i, (uid, un, fn, cnt) in enumerate(top_msg, 1):
            lines.append(f"  {i}. {user_label(uid, un, fn)} — {cnt}")
        lines.append("")

    if top_recv:
        lines.append("❤️ *Топ по полученным реакциям:*")
        for i, (uid, un, fn, cnt) in enumerate(top_recv, 1):
            lines.append(f"  {i}. {user_label(uid, un, fn)} — {cnt}")

    await msg.answer("\n".join(lines), parse_mode="Markdown")


@router.message(Command("summary"))
async def cmd_summary(msg: Message):
    if not is_group(msg):
        return
    if not groq_client:
        await msg.answer("GROQ_API_KEY не настроен.")
        return

    chat_id = msg.chat.id
    rows = await db_fetchall(
        "SELECT user_id, username, text, ts FROM messages WHERE chat_id=? AND text!='' ORDER BY id DESC LIMIT ?",
        (chat_id, SUMMARY_MSG_COUNT),
    )

    if len(rows) < 5:
        await msg.answer("Мало сообщений для саммари (нужно 5+).")
        return

    rows.reverse()
    chat_log = "\n".join(
        f"[{ts}] {('@' + uname) if uname else f'id:{uid}'}: {text}"
        for uid, uname, text, ts in rows
    )

    wait_msg = await msg.answer("Генерирую выжимку...")

    try:
        response = await asyncio.to_thread(
            groq_client.chat.completions.create,
            model="llama-3.3-70b-versatile",
            messages=[
                {"role": "system", "content": (
                    "Ты помощник для кратких выжимок групповых чатов. Отвечай на русском. "
                    "Когда упоминаешь участников — используй их Telegram-ники в формате @username (как они указаны в логе). "
                    "Никогда не используй имена и фамилии. "
                    "Пиши коротко и по делу, без воды. Используй эмодзи для оживления и читабельности (умеренно: 1-2 на блок). "
                    "Форматируй ответ как разделы с заголовками-эмодзи."
                )},
                {"role": "user", "content": (
                    f"Лог последних {len(rows)} сообщений (участники указаны как @username):\n\n{chat_log}\n\n"
                    "Сделай выжимку строго в таком формате:\n\n"
                    "💬 *Главные темы*\n— коротко, маркированный список\n\n"
                    "✅ *Решения и договорённости*\n— только если есть, иначе «нет»\n\n"
                    "🔗 *Важные ссылки*\n— только реально полезные, иначе «нет»\n\n"
                    "👥 *Активные участники*\n— @username — кратко чем отметился\n\n"
                    "Не более 400 слов. Никаких имён — только @username."
                )},
            ],
            max_tokens=2000,
        )
        text = (response.choices[0].message.content or "Не удалось.")[:4000]
        await wait_msg.edit_text(f"Выжимка ({len(rows)} сообщений):\n\n{text}")
    except Exception as e:
        log.exception("Groq error")
        await wait_msg.edit_text(f"Ошибка: {e}")


GROUP_COMMANDS = [
    BotCommand(command="stats",   description="📊 Топ активности (week|month)"),
    BotCommand(command="digest",  description="🗞 Дайджест за неделю"),
    BotCommand(command="summary", description="📝 AI-выжимка чата"),
    BotCommand(command="silent",  description="😴 Кто давно молчит"),
    BotCommand(command="help",    description="❓ Справка"),
]

PRIVATE_COMMANDS = [
    BotCommand(command="start", description="🚀 Запустить бота"),
    BotCommand(command="help",  description="❓ Справка"),
]


async def on_startup(app_or_bot=None):
    await init_db()
    await bot.delete_my_commands()
    await bot.set_my_commands(GROUP_COMMANDS)  # default scope
    await bot.set_my_commands(GROUP_COMMANDS, scope=BotCommandScopeAllGroupChats())
    await bot.set_my_commands(PRIVATE_COMMANDS, scope=BotCommandScopeAllPrivateChats())
    url = f"{RENDER_URL}{WEBHOOK_PATH}"
    await bot.set_webhook(url, allowed_updates=["message", "message_reaction", "poll_answer"])
    log.info(f"Webhook set: {url}")


async def health(request):
    return web.Response(text="OK")


if __name__ == "__main__":
    app = web.Application()
    app.router.add_get("/", health)
    webhook_handler = SimpleRequestHandler(dispatcher=dp, bot=bot)
    webhook_handler.register(app, path=WEBHOOK_PATH)
    setup_application(app, dp, bot=bot)
    dp.startup.register(on_startup)
    log.info("Starting server...")
    web.run_app(app, host="0.0.0.0", port=PORT)
