# -*- coding: utf-8 -*-
import os
import base64
import asyncio
import sqlite3
import logging

import httpx
from aiogram import Bot, Dispatcher, Router
from aiogram.types import (
    Message, PollAnswer, MessageReactionUpdated, CallbackQuery, ChatMemberUpdated,
    BotCommand, BotCommandScopeAllGroupChats, BotCommandScopeAllPrivateChats, BotCommandScopeChat,
    InlineKeyboardMarkup, InlineKeyboardButton,
)
from aiogram.filters import Command, CommandStart
from aiogram.webhook.aiohttp_server import SimpleRequestHandler, setup_application
from aiohttp import web

from groq import Groq

BOT_TOKEN = os.getenv("BOT_TOKEN", "")
GROQ_API_KEY = os.getenv("GROQ_API_KEY", "")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")
OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY", "")
ADMIN_USER_ID = int(os.getenv("ADMIN_USER_ID", "0") or "0")
_raw_allowed = os.getenv("ALLOWED_CHAT_IDS", "").strip()
ALLOWED_CHAT_IDS: set[int] = set()
if _raw_allowed:
    for x in _raw_allowed.replace(";", ",").split(","):
        x = x.strip()
        if x:
            try:
                ALLOWED_CHAT_IDS.add(int(x))
            except ValueError:
                pass
RENDER_URL = os.getenv("RENDER_URL", "")
WEBHOOK_PATH = f"/webhook/{BOT_TOKEN}"

# Turso (libSQL) — облачный SQLite, переживает рестарты Render.
# Если переменные не заданы — fallback на локальный sqlite (для разработки).
TURSO_URL = os.getenv("TURSO_DATABASE_URL", "")
TURSO_TOKEN = os.getenv("TURSO_AUTH_TOKEN", "")
DB_PATH = os.getenv("DB_PATH", "activity.db")

USE_TURSO = bool(TURSO_URL)

SUMMARY_MSG_COUNT = 300
SUMMARY_MAX_TEXT_LEN = 280
SUMMARY_COOLDOWN_SEC = 5 * 60
PORT = int(os.getenv("PORT", "10000"))

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

groq_client = None
if GROQ_API_KEY:
    groq_client = Groq(api_key=GROQ_API_KEY)


# ---------- LLM провайдеры ----------

GEMINI_URL = "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent"
OPENROUTER_URL = "https://openrouter.ai/api/v1/chat/completions"


async def _call_gemini(system: str, user: str, max_tokens: int) -> str | None:
    """Gemini 2.0 Flash через REST. Возвращает текст или None при ошибке."""
    if not GEMINI_API_KEY:
        return None
    try:
        async with httpx.AsyncClient(timeout=60.0) as client:
            resp = await client.post(
                GEMINI_URL,
                headers={"x-goog-api-key": GEMINI_API_KEY},
                json={
                    "systemInstruction": {"parts": [{"text": system}]},
                    "contents": [{"role": "user", "parts": [{"text": user}]}],
                    "generationConfig": {
                        "maxOutputTokens": max_tokens,
                        "temperature": 0.7,
                    },
                },
            )
            resp.raise_for_status()
            data = resp.json()
            cands = data.get("candidates", [])
            if cands:
                parts = cands[0].get("content", {}).get("parts", [])
                text = "".join(p.get("text", "") for p in parts).strip()
                if text:
                    return text
            log.warning(f"Gemini returned empty: {data}")
    except Exception as e:
        log.warning(f"Gemini failed: {e}")
    return None


def _strip_cot(text: str) -> str:
    """Вырезает chain-of-thought из ответа модели.
    1. Убирает блоки <think>...</think>
    2. Если текст начинается с английского reasoning — ищет первую строку с кириллицей/эмодзи
    3. Возвращает полезную часть или пустую строку если ничего не осталось."""
    # 1. <think> блоки (DeepSeek R1, Qwen и другие)
    text = _re.sub(r"<think>.*?</think>", "", text, flags=_re.DOTALL | _re.IGNORECASE).strip()

    # 2. Если начинается с английского reasoning — ищем начало русского текста
    cot_markers = ("let's ", "we need to", "i need to", "let me ", "i'll ", "i will ",
                   "first,", "okay,", "now,", "here is", "here's", "the log", "looking at")
    if text and text[:80].lower().startswith(tuple(cot_markers)):
        # Ищем первую строку содержащую кириллицу или эмодзи-заголовок
        lines = text.split("\n")
        for i, line in enumerate(lines):
            if _re.search(r"[а-яА-ЯёЁ]", line) or _re.search(r"[📌📅✅💡🔗⚠️]", line):
                text = "\n".join(lines[i:]).strip()
                break
        else:
            return ""  # Вообще нет русского текста
    return text


OPENROUTER_MODELS = [
    "meta-llama/llama-3.3-70b-instruct:free",   # 65k, инструктивная, не думает вслух
    "nousresearch/hermes-3-llama-3.1-405b:free", # 131k, 405B, инструктивная
    "google/gemma-3-27b-it:free",              # 131k, запасная
]


async def _call_openrouter(system: str, user: str, max_tokens: int) -> str | None:
    """OpenRouter — пробует несколько бесплатных моделей по очереди. Возвращает текст или None."""
    if not OPENROUTER_API_KEY:
        return None

    async with httpx.AsyncClient(timeout=90.0) as client:
        for model in OPENROUTER_MODELS:
            try:
                resp = await client.post(
                    OPENROUTER_URL,
                    headers={
                        "Authorization": f"Bearer {OPENROUTER_API_KEY}",
                        "HTTP-Referer": "https://tg-activity-bot.onrender.com",
                        "X-Title": "tg-activity-bot",
                    },
                    json={
                        "model": model,
                        "messages": [
                            {"role": "system", "content": system},
                            {"role": "user", "content": user},
                        ],
                        "max_tokens": max_tokens,
                        "temperature": 0.7,
                    },
                )
                resp.raise_for_status()
                data = resp.json()
                choices = data.get("choices", [])
                if choices:
                    text = (choices[0].get("message", {}).get("content") or "").strip()
                    if text:
                        text = _strip_cot(text)
                        if text:
                            log.info(f"OpenRouter model {model} succeeded")
                            return text
                        log.warning(f"OpenRouter {model} returned only CoT, skipping")
                log.warning(f"OpenRouter {model} returned empty: {data}")
            except Exception as e:
                log.warning(f"OpenRouter {model} failed: {e}")
                continue
    return None


async def llm_complete(system: str, user: str, max_tokens: int = 1500) -> str:
    """Универсальная функция вызова LLM. Цепочка: Gemini → OpenRouter (DeepSeek) → Groq (аварийный).
    Каждый следующий пробуется только если предыдущий не ответил."""

    # 1. OpenRouter (основной, бесплатные модели)
    result = await _call_openrouter(system, user, max_tokens)
    if result:
        return result

    # 2. Gemini (если OpenRouter не справился)
    result = await _call_gemini(system, user, max_tokens)
    if result:
        return result

    # 3. Groq (аварийный, маленький лимит — обрезаем вход)
    if groq_client:
        GROQ_USER_CHAR_BUDGET = 7000  # Русский текст: ~2 символа/токен
        if len(user) > GROQ_USER_CHAR_BUDGET:
            user_compact = "[лог обрезан из-за лимита]\n\n" + user[-GROQ_USER_CHAR_BUDGET:]
        else:
            user_compact = user
        try:
            response = await asyncio.to_thread(
                groq_client.chat.completions.create,
                model="llama-3.1-8b-instant",
                messages=[
                    {"role": "system", "content": system},
                    {"role": "user", "content": user_compact},
                ],
                max_tokens=min(max_tokens, 1000),
            )
            return (response.choices[0].message.content or "").strip()
        except Exception as e:
            log.warning(f"Groq failed: {e}")

    raise RuntimeError("Все LLM-провайдеры недоступны (Gemini, OpenRouter, Groq).")

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
    """CREATE TABLE IF NOT EXISTS chat_members (
        chat_id INTEGER NOT NULL,
        user_id INTEGER NOT NULL,
        username TEXT,
        full_name TEXT,
        joined_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        last_seen_ts TIMESTAMP,
        left_ts TIMESTAMP,
        PRIMARY KEY (chat_id, user_id)
    )""",
]

CREATE_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_msg_chat ON messages(chat_id)",
    "CREATE INDEX IF NOT EXISTS idx_msg_chat_msgid ON messages(chat_id, message_id)",
    "CREATE INDEX IF NOT EXISTS idx_react_chat ON reactions(chat_id)",
    "CREATE INDEX IF NOT EXISTS idx_react_target ON reactions(chat_id, target_user_id)",
    "CREATE INDEX IF NOT EXISTS idx_members_chat_active ON chat_members(chat_id, left_ts)",
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

# Zero-width non-joiner ломает парсер упоминаний Telegram, но визуально незаметен:
# участник видит @username как обычно, но не получает пуша.
_ZWNJ = "\u200c"


def user_label(user_id, username, full_name):
    if username:
        return f"@{_ZWNJ}{username}"
    return f"id:{user_id}"


import re as _re

_MENTION_RE = _re.compile(r"@(\w+)")


def desensitize_mentions(text: str) -> str:
    """Вставляет ZWNJ после @, чтобы упоминания не пинговали участников."""
    if not text:
        return text
    return _MENTION_RE.sub(lambda m: f"@{_ZWNJ}{m.group(1)}", text)


# Telegram HTML поддерживает только узкий набор тегов. LLM любят генерить
# <ul>/<li>/<p>/<br>/<h1> — это валит парсер. Чистим.
_ALLOWED_TAGS = {"b", "strong", "i", "em", "u", "s", "strike", "del",
                 "code", "pre", "a", "tg-spoiler", "blockquote"}


def sanitize_telegram_html(text: str) -> str:
    if not text:
        return text
    # Списки и параграфы → переводы строк и маркеры
    text = _re.sub(r"<\s*li\s*>", "• ", text, flags=_re.IGNORECASE)
    text = _re.sub(r"<\s*/\s*li\s*>", "\n", text, flags=_re.IGNORECASE)
    text = _re.sub(r"<\s*/?\s*ul\s*>", "\n", text, flags=_re.IGNORECASE)
    text = _re.sub(r"<\s*/?\s*ol\s*>", "\n", text, flags=_re.IGNORECASE)
    text = _re.sub(r"<\s*/?\s*p\s*>", "\n", text, flags=_re.IGNORECASE)
    text = _re.sub(r"<\s*br\s*/?\s*>", "\n", text, flags=_re.IGNORECASE)
    text = _re.sub(r"<\s*h[1-6]\s*>", "<b>", text, flags=_re.IGNORECASE)
    text = _re.sub(r"<\s*/\s*h[1-6]\s*>", "</b>\n", text, flags=_re.IGNORECASE)

    # Любые остальные теги, не входящие в whitelist, удаляем (открывающие/закрывающие/самозакрывающие)
    def _strip(match):
        tag = match.group(1).lower()
        return match.group(0) if tag in _ALLOWED_TAGS else ""
    text = _re.sub(r"</?\s*([a-zA-Z][a-zA-Z0-9-]*)[^>]*>", _strip, text)

    # Лишние пустые строки
    text = _re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def is_allowed_chat(chat_id: int) -> bool:
    """Если whitelist не задан — разрешаем все чаты. Иначе только из списка."""
    if not ALLOWED_CHAT_IDS:
        return True
    return chat_id in ALLOWED_CHAT_IDS


def is_group(msg: Message) -> bool:
    """Чат должен быть группой И входить в whitelist (если он задан)."""
    return msg.chat.type in ("group", "supergroup") and is_allowed_chat(msg.chat.id)


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

def main_menu_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="📊 Статистика", callback_data="stats:all"),
            InlineKeyboardButton(text="🗞 Дайджест", callback_data="digest"),
        ],
        [
            InlineKeyboardButton(text="📅 За неделю", callback_data="stats:week"),
            InlineKeyboardButton(text="🗓 За месяц", callback_data="stats:month"),
        ],
        [
            InlineKeyboardButton(text="📝 AI-выжимка", callback_data="summary"),
        ],
        [
            InlineKeyboardButton(text="😴 Молчуны 14д", callback_data="silent:14"),
            InlineKeyboardButton(text="😴 Молчуны 7д", callback_data="silent:7"),
        ],
        [
            InlineKeyboardButton(text="❓ Помощь", callback_data="help"),
        ],
    ])


async def upsert_member_seen(chat_id: int, user_id: int, username: str | None, full_name: str | None):
    """Отметить участника как живого и видимого. Создаёт запись если её ещё нет."""
    existing = await db_fetchall(
        "SELECT 1 FROM chat_members WHERE chat_id=? AND user_id=?",
        (chat_id, user_id),
    )
    if existing:
        await db_execute(
            "UPDATE chat_members SET username=?, full_name=?, last_seen_ts=CURRENT_TIMESTAMP, left_ts=NULL WHERE chat_id=? AND user_id=?",
            (username, full_name, chat_id, user_id),
        )
    else:
        await db_execute(
            "INSERT INTO chat_members (chat_id, user_id, username, full_name, last_seen_ts) VALUES (?,?,?,?,CURRENT_TIMESTAMP)",
            (chat_id, user_id, username, full_name),
        )


@router.edited_message()
async def on_edited_message(msg: Message):
    """Если пользователь отредактировал сообщение — обновляем текст в БД."""
    if not msg.from_user or not is_group(msg):
        return
    new_text = (msg.text or msg.caption or "")[:4000]
    await db_execute(
        "UPDATE messages SET text=? WHERE chat_id=? AND message_id=? AND user_id=?",
        (new_text, msg.chat.id, msg.message_id, msg.from_user.id),
    )


@router.message(Command("chats"))
async def cmd_chats(msg: Message):
    """Список всех чатов где бот видел сообщения. Только для админа."""
    if not msg.from_user or (ADMIN_USER_ID and msg.from_user.id != ADMIN_USER_ID):
        return
    rows = await db_fetchall(
        """SELECT chat_id, COUNT(*) cnt, MAX(ts) last_ts, COUNT(DISTINCT user_id) users
           FROM messages
           GROUP BY chat_id
           ORDER BY cnt DESC""",
    )
    if not rows:
        await msg.answer("В БД нет ни одного чата.", disable_notification=True)
        return
    lines = ["📋 <b>Все чаты бота</b>\n"]
    for chat_id, cnt, last_ts, users in rows:
        in_wl = "✅" if is_allowed_chat(chat_id) else "⚠️"
        last_short = (last_ts or "")[:16]
        lines.append(f"{in_wl} <code>{chat_id}</code> — {cnt} сообщ., {users} чел., посл.: {last_short}")
    lines.append(f"\nВсего чатов: <b>{len(rows)}</b>")
    if ALLOWED_CHAT_IDS:
        lines.append(f"В whitelist: {len(ALLOWED_CHAT_IDS)}")
    else:
        lines.append("⚠️ <b>Whitelist пуст — бот работает во всех чатах.</b>")
    await msg.answer("\n".join(lines), parse_mode="HTML", disable_notification=True)
    await try_delete(msg.chat.id, msg.message_id)


@router.message(Command("chatid"))
async def cmd_chatid(msg: Message):
    """Показывает chat_id текущего чата (для добавления в ALLOWED_CHAT_IDS).
    Доступна только админу из ADMIN_USER_ID, чтобы левые юзеры не светили id."""
    if not msg.from_user or (ADMIN_USER_ID and msg.from_user.id != ADMIN_USER_ID):
        return
    await msg.answer(
        f"<code>chat_id = {msg.chat.id}</code>\n"
        f"тип: {msg.chat.type}\n"
        f"в whitelist: {'да' if is_allowed_chat(msg.chat.id) else 'нет'}",
        parse_mode="HTML",
        disable_notification=True,
    )
    await try_delete(msg.chat.id, msg.message_id)


@router.chat_member()
async def on_chat_member(event: ChatMemberUpdated):
    """Отслеживает входы/выходы участников группы."""
    if event.chat.type not in ("group", "supergroup"):
        return
    if not is_allowed_chat(event.chat.id):
        return
    member = event.new_chat_member
    user = member.user
    if not user or user.is_bot:
        return
    status = member.status
    if status in ("member", "administrator", "creator", "restricted"):
        existing = await db_fetchall(
            "SELECT 1 FROM chat_members WHERE chat_id=? AND user_id=?",
            (event.chat.id, user.id),
        )
        if existing:
            await db_execute(
                "UPDATE chat_members SET username=?, full_name=?, left_ts=NULL WHERE chat_id=? AND user_id=?",
                (user.username, user.full_name, event.chat.id, user.id),
            )
        else:
            await db_execute(
                "INSERT INTO chat_members (chat_id, user_id, username, full_name) VALUES (?,?,?,?)",
                (event.chat.id, user.id, user.username, user.full_name),
            )
    elif status in ("left", "kicked"):
        await db_execute(
            "UPDATE chat_members SET left_ts=CURRENT_TIMESTAMP WHERE chat_id=? AND user_id=?",
            (event.chat.id, user.id),
        )


async def try_delete(chat_id: int, message_id: int):
    """Лучшее усилие удалить сообщение. Молча игнорим если у бота нет прав."""
    try:
        await bot.delete_message(chat_id, message_id)
    except Exception:
        pass


async def notify_admin(text: str):
    """Шлёт сообщение об ошибке только админу из ADMIN_USER_ID. Если не задан — пишет в лог."""
    if not ADMIN_USER_ID:
        log.error(f"[admin notify, no ADMIN_USER_ID set] {text}")
        return
    try:
        await bot.send_message(ADMIN_USER_ID, text, parse_mode="HTML", disable_notification=True)
    except Exception as e:
        log.warning(f"Admin DM to {ADMIN_USER_ID} failed: {e}")


async def is_chat_admin(chat_id: int, user_id: int) -> bool:
    try:
        member = await bot.get_chat_member(chat_id, user_id)
        return member.status in ("creator", "administrator")
    except Exception:
        return False


@router.message(Command("all", "everyone"))
async def cmd_all(msg: Message):
    if not is_group(msg) or not msg.from_user:
        return
    if not await is_chat_admin(msg.chat.id, msg.from_user.id):
        await msg.answer("🚫 Команда доступна только админам чата.", disable_notification=True)
        return

    # Текст-приписка (всё после команды)
    parts = (msg.text or "").split(maxsplit=1)
    note = parts[1].strip() if len(parts) > 1 else "Внимание!"

    rows = await db_fetchall(
        """SELECT user_id, username, full_name FROM chat_members
           WHERE chat_id=? AND left_ts IS NULL""",
        (msg.chat.id,),
    )
    if not rows:
        await msg.answer("Пока некого звать — бот ещё не видел участников.")
        return

    def html_escape(s: str) -> str:
        return (s or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

    mentions = []
    for uid, un, fn in rows:
        name = html_escape(un or fn or f"id{uid}")
        mentions.append(f'<a href="tg://user?id={uid}">{name}</a>')

    # Telegram лимит ~4096 символов на сообщение и ~50 уникальных упоминаний
    # на одно сообщение, чтоб уведомления реально доходили — режем по 50.
    CHUNK = 50
    header = f"📣 <b>{html_escape(note)}</b>\n\n"
    chunks = [mentions[i:i+CHUNK] for i in range(0, len(mentions), CHUNK)]
    for i, chunk in enumerate(chunks):
        prefix = header if i == 0 else ""
        await msg.answer(prefix + " ".join(chunk), parse_mode="HTML")
    await try_delete(msg.chat.id, msg.message_id)


@router.message(~Command("stats", "summary", "start", "help", "silent", "digest", "menu", "all", "everyone", "chatid", "chats"))
async def on_message(msg: Message):
    if not msg.from_user or not is_group(msg):
        return
    await ensure_chat_commands(msg.chat.id)
    await db_execute(
        "INSERT INTO messages (chat_id, message_id, user_id, username, full_name, text) VALUES (?,?,?,?,?,?)",
        (msg.chat.id, msg.message_id, msg.from_user.id, msg.from_user.username,
         msg.from_user.full_name, (msg.text or msg.caption or "")[:4000]),
    )
    await upsert_member_seen(msg.chat.id, msg.from_user.id, msg.from_user.username, msg.from_user.full_name)
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
        "👋 *Activity Bot*\n\n"
        "Веду статистику по чату: сообщения, реакции, опросы. "
        "Делаю AI-выжимки и дайджесты, выдаю достижения.\n\n"
        "Нажми кнопку или используй /menu в любой момент.",
        reply_markup=main_menu_kb(),
        parse_mode="Markdown",
    )


@router.message(Command("menu"))
async def cmd_menu(msg: Message):
    await msg.answer("📋 *Меню бота*", reply_markup=main_menu_kb(), parse_mode="Markdown")
    await try_delete(msg.chat.id, msg.message_id)


# ---------- callback handlers ----------

async def _render_stats(chat_id: int, period: str) -> str:
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
    total_msg_row = await db_fetchall(f"SELECT COUNT(*) FROM messages WHERE chat_id=?{pf}", (chat_id,))
    total_react_row = await db_fetchall(f"SELECT COUNT(*) FROM reactions WHERE chat_id=?{pf}", (chat_id,))
    total_msg = total_msg_row[0][0] if total_msg_row else 0
    total_react = total_react_row[0][0] if total_react_row else 0

    lines = [
        f"📊 <b>Активность чата ({period_label})</b>\n",
        f"💬 Сообщений: <b>{total_msg}</b> | ❤️ Реакций: <b>{total_react}</b>\n",
    ]
    if rows_msg:
        lines.append("🏆 <b>Топ по сообщениям:</b>")
        for i, (uid, un, fn, cnt) in enumerate(rows_msg, 1):
            lines.append(f"  {i}. {user_label(uid, un, fn)} — {cnt}")
    if rows_react_given:
        lines.append("\n👍 <b>Топ по поставленным реакциям:</b>")
        for i, (uid, un, fn, cnt) in enumerate(rows_react_given, 1):
            lines.append(f"  {i}. {user_label(uid, un, fn)} — {cnt}")
    if rows_react_recv:
        lines.append("\n❤️ <b>Топ по полученным реакциям:</b>")
        for i, (uid, un, fn, cnt) in enumerate(rows_react_recv, 1):
            lines.append(f"  {i}. {user_label(uid, un, fn)} — {cnt}")
    if rows_polls:
        lines.append("\n🗳 <b>Топ по голосованиям:</b>")
        for i, (uid, un, fn, cnt) in enumerate(rows_polls, 1):
            lines.append(f"  {i}. {user_label(uid, un, fn)} — {cnt}")
    if not rows_msg and not rows_react_given:
        lines.append("Пока нет данных.")
    return "\n".join(lines)


@router.callback_query()
async def on_callback(cq: CallbackQuery):
    data = cq.data or ""
    chat_id = cq.message.chat.id if cq.message else None
    if not chat_id:
        await cq.answer()
        return

    try:
        if data.startswith("stats:"):
            period = data.split(":", 1)[1]
            text = await _render_stats(chat_id, period)
            await cq.message.answer(text, parse_mode="HTML")
        elif data == "digest":
            await cq.answer("Готовлю дайджест…")
            await cmd_digest(cq.message)
            return
        elif data == "summary":
            await cq.answer("Генерирую выжимку…")
            await cmd_summary(cq.message)
            return
        elif data.startswith("silent:"):
            try:
                days = int(data.split(":", 1)[1])
            except ValueError:
                days = 14
            text = await _render_silent(chat_id, days)
            await cq.message.answer(text, parse_mode="Markdown")
        elif data == "help":
            await cmd_help(cq.message)
    finally:
        await cq.answer()


HELP_TEXT = (
    "🤖 <b>Activity Bot — справка</b>\n\n"
    "Слежу за активностью в группе и помогаю её осмыслить: считаю сообщения, реакции, "
    "опросы, делаю AI-выжимки и дайджесты, вручаю достижения.\n\n"
    "━━━━━━━━━━━━━━━\n"
    "📋 <b>Команды</b>\n\n"
    "📊 /stats — топ участников за всё время\n"
    "    └ <code>/stats week</code> — за 7 дней\n"
    "    └ <code>/stats month</code> — за 30 дней\n\n"
    "🗞 /digest — дайджест за неделю: цифры + AI-темы + топы\n\n"
    "📝 /summary — AI-выжимка последних 300 сообщений: о чём говорили, какие договорённости, планы и ссылки\n\n"
    "😴 /silent — кто давно не писал (по умолчанию 14+ дней)\n"
    "    └ <code>/silent 7</code> — за 7 дней\n"
    "    └ показывает в т.ч. вступивших, но молчащих\n\n"
    "📣 /all — позвать всех участников (с пушем)\n"
    "    └ <code>/all встречаемся в 19:00</code> — добавит подпись\n"
    "    └ доступно только админам чата\n\n"
    "📋 /menu — кнопочное меню (для тех, кто не любит слэши)\n\n"
    "❓ /help — это сообщение\n\n"
    "━━━━━━━━━━━━━━━\n"
    "🏆 <b>Достижения</b>\n\n"
    "Бот сам поздравляет в чате, когда участник достигает рубежа:\n"
    "• 💬 100 / 500 / 1000 / 5000 / 10000 сообщений\n"
    "• 👍 50 / 200 / 1000 поставленных реакций\n"
    "• ❤️ 50 / 200 / 1000 полученных реакций\n"
    "• 🗳 10 / 50 / 200 голосов в опросах\n\n"
    "━━━━━━━━━━━━━━━\n"
    "⚙️ <b>Настройка для админа</b>\n\n"
    "1. Добавь бота в группу\n"
    "2. Назначь <b>администратором</b> (нужно для чтения всех сообщений и реакций)\n"
    "3. У @BotFather: <code>Bot Settings → Group Privacy → OFF</code>\n\n"
    "━━━━━━━━━━━━━━━\n"
    "ℹ️ <b>Что важно знать</b>\n\n"
    "• Бот хранит только сообщения, написанные после установки\n"
    "• Молчунов бот «знает» только тех, кто хоть раз написал или вошёл при включённом боте — это ограничение Telegram API\n"
    "• Все ответы бота приватны для группы, наружу ничего не уходит"
)


@router.message(Command("help"))
async def cmd_help(msg: Message):
    await msg.answer(HELP_TEXT, parse_mode="HTML", disable_web_page_preview=True)
    await try_delete(msg.chat.id, msg.message_id)


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
    await try_delete(msg.chat.id, msg.message_id)


async def _render_silent(chat_id: int, days: int) -> str:
    rows = await db_fetchall(
        """SELECT user_id, username, full_name, last_seen_ts, joined_ts
           FROM chat_members
           WHERE chat_id=?
             AND left_ts IS NULL
             AND (last_seen_ts IS NULL OR last_seen_ts < datetime('now','-' || ? || ' days'))
           ORDER BY (last_seen_ts IS NULL) DESC, last_seen_ts ASC
           LIMIT 50""",
        (chat_id, days),
    )
    if not rows:
        return f"😎 Молчунов за {days} дней нет — все на связи."

    never_wrote = [r for r in rows if r[3] is None]
    silent = [r for r in rows if r[3] is not None]

    lines = [f"😴 *Молчуны* (нет сообщений {days}+ дней)\n"]
    if silent:
        for uid, un, fn, last_seen, _ in silent:
            last_short = (last_seen or "")[:10]
            lines.append(f"• {user_label(uid, un, fn)} — последний раз {last_short}")
    if never_wrote:
        if silent:
            lines.append("")
        lines.append("👻 *Ни разу не писали:*")
        for uid, un, fn, _, joined in never_wrote:
            joined_short = (joined or "")[:10]
            lines.append(f"• {user_label(uid, un, fn)} — в чате с {joined_short}")
    lines.append("\n_Использование: /silent [дней]_")
    return "\n".join(lines)


@router.message(Command("silent"))
async def cmd_silent(msg: Message):
    if not is_group(msg):
        return
    args = (msg.text or "").split()
    days = 14
    if len(args) > 1 and args[1].isdigit():
        days = max(1, min(int(args[1]), 365))
    await msg.answer(await _render_silent(msg.chat.id, days), parse_mode="Markdown")
    await try_delete(msg.chat.id, msg.message_id)


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
        "SELECT user_id, username, text FROM messages WHERE chat_id=? AND text!='' AND ts >= datetime('now','-7 days') ORDER BY id DESC LIMIT 200",
        (chat_id,),
    )
    log_rows.reverse()
    chat_log = "\n".join(
        f"{('@' + un) if un else f'id:{uid}'}: {(txt or '')[:SUMMARY_MAX_TEXT_LEN]}"
        for uid, un, txt in log_rows
    )

    topics = "—"
    if (GEMINI_API_KEY or OPENROUTER_API_KEY or groq_client) and chat_log:
        try:
            topics_raw = await llm_complete(
                "Ты выделяешь главные темы недельного чата. Отвечай по-русски, очень кратко.",
                f"Лог за неделю:\n\n{chat_log}\n\nВыдели 3-5 главных тем недели маркированным списком. Каждая тема — одна строка с эмодзи в начале. Никаких пояснений, никаких имён, только темы.",
                max_tokens=400,
            )
            topics = desensitize_mentions(topics_raw or "—")
        except Exception as e:
            log.exception("LLM digest error")
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

    await msg.answer("\n".join(lines), parse_mode="Markdown", disable_notification=True)
    await try_delete(msg.chat.id, msg.message_id)


_summary_last_call: dict[int, float] = {}
_chat_commands_pushed: set[int] = set()


async def ensure_chat_commands(chat_id: int):
    """Один раз за время жизни процесса заталкиваем команды в конкретный чат,
    чтобы сбросить агрессивный кэш Telegram-клиента."""
    if chat_id in _chat_commands_pushed:
        return
    _chat_commands_pushed.add(chat_id)
    try:
        await bot.set_my_commands(GROUP_COMMANDS, scope=BotCommandScopeChat(chat_id=chat_id))
    except Exception as e:
        log.warning(f"set chat commands failed for {chat_id}: {e}")


@router.message(Command("summary"))
async def cmd_summary(msg: Message):
    if not is_group(msg):
        return

    import time as _time
    now = _time.monotonic()
    last = _summary_last_call.get(msg.chat.id, 0)
    elapsed = now - last
    if elapsed < SUMMARY_COOLDOWN_SEC:
        wait_sec = int(SUMMARY_COOLDOWN_SEC - elapsed)
        await msg.answer(
            f"⏳ Подожди ещё {wait_sec // 60} мин {wait_sec % 60} сек — выжимку можно делать раз в {SUMMARY_COOLDOWN_SEC // 60} минут.",
            disable_notification=True,
        )
        return
    _summary_last_call[msg.chat.id] = now

    if not GEMINI_API_KEY and not OPENROUTER_API_KEY and not groq_client:
        await msg.answer("LLM не настроен (нужен GEMINI_API_KEY, OPENROUTER_API_KEY или GROQ_API_KEY).")
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
        f"[{ts}] {('@' + uname) if uname else f'id:{uid}'}: {(text or '')[:SUMMARY_MAX_TEXT_LEN]}"
        for uid, uname, text, ts in rows
    )

    wait_msg = await msg.answer("Генерирую выжимку...")

    system = (
        "Ты — внимательный читатель Telegram-чата встреч по интересам. Тебе дают лог сообщений, "
        "ты пишешь короткую человеческую сводку: что реально происходило, кто что предложил, "
        "о чём договорились, что осталось висеть.\n\n"
        "КРИТИЧЕСКИЕ ПРАВИЛА:\n"
        "— НИКОГДА не выводи свои мысли, рассуждения, reasoning, chain-of-thought. ТОЛЬКО итоговую сводку.\n"
        "— Пиши ТОЛЬКО на русском языке. НИ ОДНОГО слова на английском.\n"
        "— Пиши только то, что прямо есть в логе. Никаких выдумок, никаких «возможно», «вероятно».\n"
        "— Никаких имён и фамилий — только @username из лога.\n"
        "— Каждый @username форматируй как <b><u>@username</u></b> (жирный + подчёркнутый).\n"
        "— Отвечай в HTML. Начинай сразу с 📌, без преамбул."
    )
    user_prompt = (
        f"Лог последних {len(rows)} сообщений:\n\n{chat_log}\n\n"
        "Напиши сводку в таком виде:\n\n"
        "<b>📌 Главное</b>\n"
        "Один абзац (3-5 предложений) живым языком: что обсуждали в основном, какое настроение, "
        "к чему пришли. Без формальностей.\n\n"
        "<b>📅 Про встречи</b>\n"
        "Только если в логе реально говорили про встречи. Перечисли по пунктам: кто что предложил, "
        "где, когда, кто согласился, кто отказался. Каждый пункт — конкретное предложение или решение, "
        "со ссылкой на @username автора. Если про встречи не говорили — этот раздел не пиши вообще, пропусти.\n\n"
        "<b>✅ Договорённости</b>\n"
        "Только если реально что-то решили вместе. Каждая строка — одно конкретное решение. "
        "Если ничего не решали — этот раздел не пиши вообще.\n\n"
        "<b>🔗 Ссылки и материалы</b>\n"
        "Только если в логе были полезные ссылки (не мемы, не картинки). Если не было — пропусти раздел.\n\n"
        "Правила:\n"
        "— Пропускай разделы где нет контента, не пиши «—» и не придумывай.\n"
        "— Никаких выдуманных «открытых вопросов» и риторики.\n"
        "— Никаких «возможно», «вероятно», «утверждают ли». Только факты из лога.\n"
        "— Если в логе вообще ничего интересного — напиши одну строку: «Чат жил своей жизнью без важных тем».\n"
        "— Максимум 300 слов.\n"
        "— Помни: @username всегда как <b><u>@username</u></b>."
    )
    try:
        text = await llm_complete(system, user_prompt, max_tokens=1500)
        text = sanitize_telegram_html(text or "Не удалось.")
        text = desensitize_mentions(text[:4000])
        await wait_msg.edit_text(
            f"📝 <b>Выжимка чата</b> ({len(rows)} сообщений)\n\n{text}",
            parse_mode="HTML",
            disable_notification=True,
        )
        await try_delete(msg.chat.id, msg.message_id)
    except Exception as e:
        log.exception("LLM error")
        # Чистим следы из чата, ошибку — в личку админу
        await try_delete(msg.chat.id, wait_msg.message_id)
        await try_delete(msg.chat.id, msg.message_id)
        await notify_admin(
            f"⚠️ <b>Ошибка /summary в чате</b> <code>{msg.chat.id}</code>\n\n<code>{str(e)[:1500]}</code>"
        )


GROUP_COMMANDS = [
    BotCommand(command="menu",    description="📋 Открыть меню"),
    BotCommand(command="summary", description="📝 Пересказать чат"),
    BotCommand(command="all",     description="📣 Позвать всех (админы)"),
]

PRIVATE_COMMANDS = [
    BotCommand(command="start", description="🚀 Запустить бота"),
    BotCommand(command="help",  description="❓ Справка"),
]


async def on_startup(app_or_bot=None):
    # Render free tier часто глючит с DNS при холодном старте.
    # Пробуем до 5 раз с паузой.
    for attempt in range(1, 6):
        try:
            await init_db()
            await bot.delete_my_commands()
            await bot.set_my_commands(GROUP_COMMANDS)
            await bot.set_my_commands(GROUP_COMMANDS, scope=BotCommandScopeAllGroupChats())
            await bot.set_my_commands(PRIVATE_COMMANDS, scope=BotCommandScopeAllPrivateChats())
            url = f"{RENDER_URL}{WEBHOOK_PATH}"
            await bot.set_webhook(url, allowed_updates=["message", "edited_message", "message_reaction", "poll_answer", "callback_query", "chat_member"])
            log.info(f"Webhook set: {url}")
            return
        except Exception as e:
            log.warning(f"Startup attempt {attempt}/5 failed: {e}")
            if attempt < 5:
                await asyncio.sleep(3)
            else:
                log.error("All startup attempts failed, proceeding anyway")
                raise


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
