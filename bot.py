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
    text = (
        "\U0001f44b <b>Activity Bot</b>\n\n"
        "\U0001f4ca \u041e\u0442\u0441\u043b\u0435\u0436\u0438\u0432\u0430\u044e \u0430\u043a\u0442\u0438\u0432\u043d\u043e\u0441\u0442\u044c \u0432 \u0447\u0430\u0442\u0435:\n"
        "\u2022 \u0441\u043e\u043e\u0431\u0449\u0435\u043d\u0438\u044f\n"
        "\u2022 \u0440\u0435\u0430\u043a\u0446\u0438\u0438 (\u043b\u0430\u0439\u043a\u0438)\n"
        "\u2022 \u0433\u043e\u043b\u043e\u0441\u0430 \u0432 \u043e\u043f\u0440\u043e\u0441\u0430\u0445\n\n"
        "/stats \u2014 \u0442\u043e\u043f \u0430\u043a\u0442\u0438\u0432\u043d\u043e\u0441\u0442\u0438\n"
        "/summary \u2014 \u0432\u044b\u0436\u0438\u043c\u043a\u0430 300 \u0441\u043e\u043e\u0431\u0449\u0435\u043d\u0438\u0439\n"
        "/help \u2014 \u0441\u043f\u0440\u0430\u0432\u043a\u0430"
    )
    await msg.answer(text, parse_mode=ParseMode.HTML)


@router.message(Command("help"))
async def cmd_help(msg: Message):
    text = (
        "<b>\u041d\u0430\u0441\u0442\u0440\u043e\u0439\u043a\u0430:</b>\n\n"
        "1. \u0414\u043e\u0431\u0430\u0432\u044c \u0431\u043e\u0442\u0430 \u0432 \u0433\u0440\u0443\u043f\u043f\u0443\n"
        "2. \u041d\u0430\u0437\u043d\u0430\u0447\u044c \u0430\u0434\u043c\u0438\u043d\u0438\u0441\u0442\u0440\u0430\u0442\u043e\u0440\u043e\u043c\n"
        "3. @BotFather \u2192 Bot Settings \u2192 Group Privacy \u2192 <b>OFF</b>"
    )
    await msg.answer(text, parse_mode=ParseMode.HTML)


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
        "\U0001f4ca <b>\u0410\u043a\u0442\u0438\u0432\u043d\u043e\u0441\u0442\u044c \u0447\u0430\u0442\u0430</b>\n",
        f"\u0421\u043e\u043e\u0431\u0449\u0435\u043d\u0438\u0439: <b>{total_msg}</b>  \u2022  \u0420\u0435\u0430\u043a\u0446\u0438\u0439: <b>{total_react}</b>\n",
    ]

    if rows_msg:
        lines.append("<b>\U0001f4ac \u0422\u043e\u043f \u043f\u043e \u0441\u043e\u043e\u0431\u0449\u0435\u043d\u0438\u044f\u043c:</b>")
        for i, (uid, uname, fname, cnt) in enumerate(rows_msg, 1):
            bar = "\u2588" * min(cnt * 10 // (rows_msg[0][3] or 1), 10)
            lines.append(f"  {i}. {user_label(uid, uname, fname)} \u2014 <b>{cnt}</b>  {bar}")

    if rows_react:
        lines.append(f"\n<b>\u2764\ufe0f \u0422\u043e\u043f \u043f\u043e \u0440\u0435\u0430\u043a\u0446\u0438\u044f\u043c:</b>")
        for i, (uid, uname, fname, cnt) in enumerate(rows_react, 1):
            lines.append(f"  {i}. {user_label(uid, uname, fname)} \u2014 {cnt}")

    if rows_polls:
        lines.append(f"\n<b>\U0001f4cb \u0422\u043e\u043f \u043f\u043e \u0433\u043e\u043b\u043e\u0441\u043e\u0432\u0430\u043d\u0438\u044f\u043c:</b>")
        for i, (uid, uname, fname, cnt) in enumerate(rows_polls, 1):
            lines.append(f"  {i}. {user_label(uid, uname, fname)} \u2014 {cnt}")

    if not rows_msg and not rows_react:
        lines.append("\u041f\u043e\u043a\u0430 \u043d\u0435\u0442 \u0434\u0430\u043d\u043d\u044b\u0445.")

    await msg.answer("\n".join(lines), parse_mode=ParseMode.HTML)


@router.message(Command("summary"))
async def cmd_summary(msg: Message):
    if not gemini_client:
        await msg.answer("\u26a0\ufe0f GEMINI_API_KEY \u043d\u0435 \u043d\u0430\u0441\u0442\u0440\u043e\u0435\u043d.")
        return

    chat_id = msg.chat.id
    con = get_db()
    rows = con.execute(
        "SELECT full_name, username, text, ts FROM messages WHERE chat_id=? AND text!='' ORDER BY id DESC LIMIT ?",
        (chat_id, SUMMARY_MSG_COUNT),
    ).fetchall()
    con.close()

    if len(rows) < 5:
        await msg.answer("\u041c\u0430\u043b\u043e \u0441\u043e\u043e\u0431\u0449\u0435\u043d\u0438\u0439 \u0434\u043b\u044f \u0441\u0430\u043c\u043c\u0430\u0440\u0438 (\u043d\u0443\u0436\u043d\u043e 5+).")
        return

    rows.reverse()
    chat_log = "\n".join(
        f"[{ts}] {fname or uname or 'Anonim'}: {text}"
        for fname, uname, text, ts in rows
    )

    wait_msg = await msg.answer("\u23f3 \u0413\u0435\u043d\u0435\u0440\u0438\u0440\u0443\u044e \u0432\u044b\u0436\u0438\u043c\u043a\u0443...")

    try:
        response = await asyncio.to_thread(
            gemini_client.models.generate_content,
            model="gemini-2.0-flash-lite",
            contents=f"\u0422\u044b \u043f\u043e\u043c\u043e\u0449\u043d\u0438\u043a \u0434\u043b\u044f \u043a\u0440\u0430\u0442\u043a\u0438\u0445 \u0432\u044b\u0436\u0438\u043c\u043e\u043a \u0433\u0440\u0443\u043f\u043f\u043e\u0432\u044b\u0445 \u0447\u0430\u0442\u043e\u0432.\n\n\u041b\u043e\u0433 \u043f\u043e\u0441\u043b\u0435\u0434\u043d\u0438\u0445 {len(rows)} \u0441\u043e\u043e\u0431\u0449\u0435\u043d\u0438\u0439:\n\n{chat_log}\n\n\u0421\u0434\u0435\u043b\u0430\u0439 \u0432\u044b\u0436\u0438\u043c\u043a\u0443 \u043d\u0430 \u0440\u0443\u0441\u0441\u043a\u043e\u043c:\n1. \u0413\u043b\u0430\u0432\u043d\u044b\u0435 \u0442\u0435\u043c\u044b\n2. \u0420\u0435\u0448\u0435\u043d\u0438\u044f / \u0434\u043e\u0433\u043e\u0432\u043e\u0440\u0451\u043d\u043d\u043e\u0441\u0442\u0438\n3. \u0412\u0430\u0436\u043d\u044b\u0435 \u0441\u0441\u044b\u043b\u043a\u0438\n4. \u0410\u043a\u0442\u0438\u0432\u043d\u044b\u0435 \u0443\u0447\u0430\u0441\u0442\u043d\u0438\u043a\u0438\n\n\u041d\u0435 \u0431\u043e\u043b\u0435\u0435 500 \u0441\u043b\u043e\u0432.",
        )
        text = (response.text or "\u041d\u0435 \u0443\u0434\u0430\u043b\u043e\u0441\u044c.")[:4000]
        await wait_msg.edit_text(
            f"\U0001f4dd <b>\u0412\u044b\u0436\u0438\u043c\u043a\u0430 ({len(rows)} \u0441\u043e\u043e\u0431\u0449\u0435\u043d\u0438\u0439):</b>\n\n{text}",
            parse_mode=ParseMode.HTML,
        )
    except Exception as e:
        log.exception("Gemini error")
        await wait_msg.edit_text(f"\u274c \u041e\u0448\u0438\u0431\u043a\u0430: {e}")


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
