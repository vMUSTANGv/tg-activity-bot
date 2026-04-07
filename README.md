# tg-activity-bot

Telegram-бот для учёта активности в группе: сообщения, реакции, голоса в опросах + AI-выжимка через Groq.

## Команды

- `/stats` — топ участников по сообщениям/реакциям/опросам
- `/summary` — выжимка последних 300 сообщений (Groq, llama-3.3-70b)
- `/help` — настройка

## Переменные окружения

| Переменная | Назначение |
|---|---|
| `BOT_TOKEN` | Токен бота от @BotFather |
| `GROQ_API_KEY` | Ключ Groq для `/summary` |
| `RENDER_URL` | Публичный URL сервиса (например `https://my-bot.onrender.com`) |
| `TURSO_DATABASE_URL` | URL базы Turso (`libsql://...turso.io`) |
| `TURSO_AUTH_TOKEN` | Токен Turso (Full Access, без срока) |
| `PORT` | Порт (Render задаёт сам, по умолчанию `10000`) |

Если `TURSO_DATABASE_URL` не задана, бот падает в локальный SQLite (`activity.db`) — удобно для разработки, но на бесплатном Render файл не переживёт рестарт.

## Деплой на Render (бесплатный план + Turso)

1. Зарегистрируйся на https://turso.tech, создай БД, скопируй `Database URL` и сгенерируй `Auth Token` (Full Access, Never expires).
2. На Render открой сервис → **Environment** → добавь:
   - `BOT_TOKEN`
   - `GROQ_API_KEY`
   - `RENDER_URL` (например `https://tg-activity-bot.onrender.com`)
   - `TURSO_DATABASE_URL`
   - `TURSO_AUTH_TOKEN`
3. Build: `pip install -r requirements.txt`
4. Start: `python bot.py`

История сообщений хранится в Turso и переживает любые рестарты/редеплои.

## Настройка бота в Telegram

1. Добавь бота в группу
2. Назначь администратором (нужно для чтения всех сообщений и реакций)
3. У @BotFather: `Bot Settings → Group Privacy → OFF`
