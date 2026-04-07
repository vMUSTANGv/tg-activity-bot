"""
Telegram Activity Bot (Render / Webhook)
"""

import os
import asyncio
import sqlite3
import logging
from typing import Optional

from aiogram import Bot, Dispatcher, Router
from aiogram.types import Message, PollAnswer, MessageReactionUpdated
from aiogram.filters import Command, CommandStart
from aiogram.enums import ParseMode
from aiogram.webhook.aiohttp_server import SimpleRequestHandler, setup_application
from aiohttp import web

from google import genai

# ── Config ───────────────────────────────────────────────────────────
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")
RENDER_URL = os.getenv("RENDER_URL", "")
WEBHOOK_PATH = f"/webhook/{BOT_TOKEN}"
DB_PATH = "activity.db"
SUMMARY_MSG_COUNT = 300
PORT = int(os.getenv("PORT", "10000"))

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

gemini_client = None
if GEMINI_API_KEY:
    gemini_client = genai.Client(api_key=GEMINI_API_KEY)

# ── Database ─────────────────────────────────────────────────────────
def init_db():
    con = sqlite3.connect(DB_PATH)
    con.executescript("""
        CREATE TABLE IF NOT EXISTS messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            chat_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            username TEXT,
            full_name TEXT,
            text TEXT,
            ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS reactions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            chat_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            username TEXT,
            full_name TEXT,
            emoji TEXT,
            direction TEXT DEFAULT 'given',
            ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS poll_votes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            chat_id INTEGER,
            user_id INTEGER NOT NULL,
            username TEXT,
            full_name TEXT,
            poll_id TEXT,
            ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE INDEX IF NOT EXISTS idx_msg_chat ON messages(chat_id);
        CREATE INDEX IF NOT EXISTS idx_react_chat ON reactions(chat_id);
    """)
    con.commit()
    con.close()

def get_db():
    return sqlite3.connect(DB_PATH)

def user_label(user_id: int, username: Optional[str], full_name: Optional[str]) -> str:
    if username:
        return f"@{username}"
    if full_name:
        return full_name
    return f"id:{user_id}"

# ── Router ───────────────────────────────────────────────────────────
router = Router()

@router.message(~Command("stats", "summary", "start", "help"))
async def on_message(msg: Message):
    if not msg.from_user:
        return
    con = get_db()
    con.execute(
        "INSERT INTO messages (chat_id, user_id, username, full_name, text) VALUES (?, ?, ?, ?, ?)",
        (msg.chat.id, msg.from_user.id, msg.from_user.username,
         msg.from_user.full_name, (msg.text or msg.caption or "")[:4000]),
    )
    con.commit()
    con.close()

@router.message_reaction()
async def on_reaction(event: MessageReactionUpdated):
    if not event.user:
        return
    con = get_db()
    for r in (event.new_reaction or []):
        emoji = getattr(r, "emoji", None) or getattr(r, "custom_emoji_id", "custom")
        con.execute(
            "INSERT INTO reactions (chat_id, user_id, username, full_name, emoji, direction) "
            "VALUES (?, ?, ?, ?, ?, 'given')",
            (event.chat.id, event.user.id, event.user.username,
             event.user.full_name, emoji),
        )
    con.commit()
    con.close()

@router.poll_answer()
async def on_poll_answer(answer: PollAnswer):
    if not answer.user:
        return
    con = get_db()
    con.execute(
        "INSERT INTO poll_votes (user_id, username, full_name, poll_id) VALUES (?, ?, ?, ?)",
        (answer.user.id, answer.user.username, answer.user.full_name, answer.poll_id),
    )
    con.commit()
    con.close()

# ── Commands ─────────────────────────────────────────────────────────
@router.message(CommandStart())
async def cmd_start(msg: Message):
    await msg.answer(
        "👋 <b>Activity Bot</b>\n\n"
        "Отслеживаю активность:\n"
        "• сообщения\n• реакции\n• голоса в опросах\n\n"
        "/stats — топ активности\n"
        "/summary — выжимка 300 сообщений\n"
        "/help — справка",
        parse_mode=ParseMode.HTML,
    )

@router.message(Command("help"))
async def cmd_help(msg: Message):
    await msg.answer(
        "<b>Настройка:</b>\n\n"
        "1. Добавь бота в группу\n"
        "2. Назначь администратором\n"
        "3. @BotFather → Bot Settings → Group Privacy → <b>OFF</b>",
        parse_mode=ParseMode.HTML,
    )

@router.message(Command("stats"))
async def cmd_stats(msg: Message):
    chat_id = msg.chat.id
    con = get_db()

    rows_msg = con.execute(
        """SELECT user_id, username, full_name, COUNT(*) cnt
           FROM messages WHERE chat_id = ?
           GROUP BY user_id ORDER BY cnt DESC LIMIT 15""",
        (chat_id,),
    ).fetchall()

    rows_react = con.execute(
        """SELECT user_id, username, full_name, COUNT(*) cnt
           FROM reactions WHERE chat_id = ? AND direction='given'
           GROUP BY user_id ORDER BY cnt DESC LIMIT 10""",
        (chat_id,),
    ).fetchall()

    rows_polls = con.execute(
        """SELECT user_id, username, full_name, COUNT(*) cnt
           FROM poll_votes GROUP BY user_id ORDER BY cnt DESC LIMIT 10""",
    ).fetchall()

    total_msg = con.execute(
        "SELECT COUNT(*) FROM messages WHERE chat_id = ?", (chat_id,)
    ).fetchone()[0]
    total_react = con.execute(
        "SELECT COUNT(*) FROM reactions WHERE chat_id = ?", (chat_id,)
    ).fetchone()[0]
    con.close()

    lines = [f"📊 <b>Активность чата</b>\n",
             f"Сообщений: <b>{total_msg}</b>  •  Реакций: <b>{total_react}</b>\n"]

    if rows_msg:
        lines.append("<b>💬 Топ по сообщениям:</b>")
        for i, (uid, uname, fname, cnt) in enumerate(rows_msg, 1):
            bar = "█" * min(cnt * 10 // (rows_msg[0][3] or 1), 10)
            lines.append(f"  {i}. {user_label(uid, uname, fname)} — <b>{cnt}</b>  {bar}")

    if rows_react:
        lines.append("\n<b>❤️ Топ по реакциям:</b>")
        for i, (uid, uname, fname, cnt) in enumerate(rows_react, 1):
            lines.append(f"  {i}. {user_label(uid, uname, fname)} — {cnt}")

    if rows_polls:
        lines.append("\n<b>📋 Топ по голосованиям:</b>")
        for i, (uid, uname, fname, cnt) in enumerate(rows_polls, 1):
            lines.append(f"  {i}. {user_label(uid, uname, fname)} — {cnt}")

    if not rows_msg and not rows_react:
        lines.append("Пока нет данных.")

    await msg.answer("\n".join(lines), parse_mode=ParseMode.HTML)

@router.message(Command("summary"))
async def cmd_summary(msg: Message):
    if not gemini_client:
        await msg.answer("⚠️ GEMINI_API_KEY не настроен.")
        return

    chat_id = msg.chat.id
    con = get_db()
    rows = con.execute(
        """SELECT full_name, username, text, ts FROM messages
           WHERE chat_id = ? AND text != ''
           ORDER BY id DESC LIMIT ?""",
        (chat_id, SUMMARY_MSG_COUNT),
    ).fetchall()
    con.close()

    if len(rows) < 5:
        await msg.answer("Мало сообщений для саммари (нужно ≥ 5).")
        return

    rows.reverse()
    chat_log = "\n".join(
        f"[{ts}] {fname or uname or 'Аноним'}: {text}"
        for fname, uname, text, ts in rows
    )

    wait_msg = await msg.answer("⏳ Генерирую выжимку...")

    try:
        response = await asyncio.to_thread(
            gemini_client.models.generate_content,
            model="gemini-2.0-flash",
            contents=f"""Ты — помощник для кратких выжимок групповых чатов.

Лог последних {len(rows)} сообщений:

{chat_log}

Сделай выжимку на русском:
1. Главные темы
2. Решения / договорённости
3. Важные ссылки
4. Самые активные участники

Не более 500 слов.""",
        )

        text = (response.text or "Не удалось.")[:4000]
        await wait_msg.edit_text(
            f"📝 <b>Выжимка ({len(rows)} сообщений):</b>\n\n{text}",
            parse_mode=ParseMode.HTML,
        )
    except Exception as e:
        log.exception("Gemini error")
        await wait_msg.edit_text(f"❌ Ошибка: {e}")

# ── Health check ─────────────────────────────────────────────────────
async def health(request):
    return web.Response(text="OK")

# ── Main ─────────────────────────────────────────────────────────────
async def on_startup(bot: Bot):
    url = f"{RENDER_URL}{WEBHOOK_PATH}"
    await bot.set_webhook(
        url,
        allowed_updates=["message", "message_reaction", "poll_answer"],
    )
    log.info(f"Webhook set: {url}")

def main():
    if not BOT_TOKEN:
        raise ValueError("Set BOT_TOKEN env var")
    if not RENDER_URL:
        raise ValueError("Set RENDER_URL env var")

    init_db()

    bot = Bot(token=BOT_TOKEN)
    dp = Dispatcher()
    dp.include_router(router)
    dp.startup.register(on_startup)

    app = web.Application()
    app.router.add_get("/", health)

    webhook_handler = SimpleRequestHandler(dispatcher=dp, bot=bot)
    webhook_handler.register(app, path=WEBHOOK_PATH)
    setup_application(app, dp, bot=bot)

    log.info("Starting webhook server...")
    web.run_app(app, host="0.0.0.0", port=PORT)
