# -*- coding: utf-8 -*-
import os
import asyncio
import sqlite3
import logging

from aiogram import Bot, Dispatcher, Router
from aiogram.types import Message, PollAnswer, MessageReactionUpdated
from aiogram.filters import Command, CommandStart
from aiogram.enums import ParseMode
from aiogram.webhook.aiohttp_server import SimpleRequestHandler, setup_application
from aiohttp import web

from google import genai

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

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()
router = Router()
dp.include_router(router)


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


def user_label(user_id, username, full_name):
    if username:
        return f"@{username}"
    if full_name:
        return full_name
    return f"id:{user_id}"


@router.message(~Command("stats", "summary", "start", "help"))
async def on_message(msg: Message):
    if not msg.from_user:
        return
    con = get_db()
    con.execute(
        "INSERT INTO messages (chat_id, user_id, username, full_name, text) VALUES (?,?,?,?,?)",
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
            "INSERT INTO reactions (chat_id, user_id, username, full_name, emoji, direction) VALUES (?,?,?,?,?,'given')",
            (event.chat.id, event.user.id, event.user.username, event.user.full_name, emoji),
        )
    con.commit()
    con.close()


@router.poll_answer()
async def on_poll_answer(answer: PollAnswer):
    if not answer.user:
        return
    con = get_db()
    con.execute(
        "INSERT INTO poll_votes (user_id, username, full_name, poll_id) VALUES (?,?,?,?)",
        (answer.user.id, answer.user.username, answer.user.full_name, answer.poll_id),
    )
    con.commit()
    con.close()


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
    chat_id = msg.chat.id
    con = get_db()

    rows_msg = con.execute(
        "SELECT user_id, username, full_name, COUNT(*) cnt FROM messages WHERE chat_id=? GROUP BY user_id ORDER BY cnt DESC LIMIT 15",
        (chat_id,),
    ).fetchall()

    rows_react = con.execute(
        "SELECT user_id, username, full_name, COUNT(*) cnt FROM reactions WHERE chat_id=? AND direction='given' GROUP BY user_id ORDER BY cnt DESC LIMIT 10",
        (chat_id,),
    ).fetchall()

    rows_polls = con.execute(
        "SELECT user_id, username, full_name, COUNT(*) cnt FROM poll_votes GROUP BY user_id ORDER BY cnt DESC LIMIT 10",
    ).fetchall()

    total_msg = con.execute("SELECT COUNT(*) FROM messages WHERE chat_id=?", (chat_id,)).fetchone()[0]
    total_react = con.execute("SELECT COUNT(*) FROM reactions WHERE chat_id=?", (chat_id,)).fetchone()[0]
    con.close()

    lines = [
        "Активность чата\n",
        f"Сообщений: {total_msg} | Реакций: {total_react}\n",
    ]

    if rows_msg:
        lines.append("Топ по сообщениям:")
        for i, (uid, uname, fname, cnt) in enumerate(rows_msg, 1):
            lines.append(f"  {i}. {user_label(uid, uname, fname)} - {cnt}")

    if rows_react:
        lines.append("\nТоп по реакциям:")
        for i, (uid, uname, fname, cnt) in enumerate(rows_react, 1):
            lines.append(f"  {i}. {user_label(uid, uname, fname)} - {cnt}")

    if rows_polls:
        lines.append("\nТоп по голосованиям:")
        for i, (uid, uname, fname, cnt) in enumerate(rows_polls, 1):
            lines.append(f"  {i}. {user_label(uid, uname, fname)} - {cnt}")

    if not rows_msg and not rows_react:
        lines.append("Пока нет данных.")

    await msg.answer("\n".join(lines))


@router.message(Command("summary"))
async def cmd_summary(msg: Message):
    if not gemini_client:
        await msg.answer("GEMINI_API_KEY не настроен.")
        return

    chat_id = msg.chat.id
    con = get_db()
    rows = con.execute(
        "SELECT full_name, username, text, ts FROM messages WHERE chat_id=? AND text!='' ORDER BY id DESC LIMIT ?",
        (chat_id, SUMMARY_MSG_COUNT),
    ).fetchall()
    con.close()

    if len(rows) < 5:
        await msg.answer("Мало сообщений для саммари (нужно 5+).")
        return

    rows.reverse()
    chat_log = "\n".join(
        f"[{ts}] {fname or uname or 'Аноним'}: {text}"
        for fname, uname, text, ts in rows
    )

    wait_msg = await msg.answer("Генерирую выжимку...")

    try:
        response = await asyncio.to_thread(
            gemini_client.models.generate_content,
            model="gemini-2.0-flash-lite",
            contents=f"Ты помощник для кратких выжимок групповых чатов.\n\nЛог последних {len(rows)} сообщений:\n\n{chat_log}\n\nСделай выжимку на русском:\n1. Главные темы\n2. Решения и договорённости\n3. Важные ссылки\n4. Активные участники\n\nНе более 500 слов.",
        )
        text = (response.text or "Не удалось.")[:4000]
        await wait_msg.edit_text(f"Выжимка ({len(rows)} сообщений):\n\n{text}")
    except Exception as e:
        log.exception("Gemini error")
        await wait_msg.edit_text(f"Ошибка: {e}")


async def on_startup(app_or_bot=None):
    url = f"{RENDER_URL}{WEBHOOK_PATH}"
    await bot.set_webhook(url, allowed_updates=["message", "message_reaction", "poll_answer"])
    log.info(f"Webhook set: {url}")


async def health(request):
    return web.Response(text="OK")


if __name__ == "__main__":
    init_db()
    app = web.Application()
    app.router.add_get("/", health)
    webhook_handler = SimpleRequestHandler(dispatcher=dp, bot=bot)
    webhook_handler.register(app, path=WEBHOOK_PATH)
    setup_application(app, dp, bot=bot)
    dp.startup.register(on_startup)
    log.info("Starting server...")
    web.run_app(app, host="0.0.0.0", port=PORT)
