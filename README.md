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
| `DB_PATH` | Путь к SQLite-файлу. **На Render укажи `/var/data/activity.db`** |
| `PORT` | Порт (Render задаёт сам, по умолчанию `10000`) |

## Деплой на Render (с сохранением истории)

1. Создай Web Service из этого репо.
2. **Disks → Add Disk**: mount path `/var/data`, размер от 1 GB.
3. Environment: задай переменные выше, обязательно `DB_PATH=/var/data/activity.db`.
4. Build: `pip install -r requirements.txt`
5. Start: `python bot.py`

Без persistent disk SQLite-файл стирается при каждом редеплое.

## Настройка бота в Telegram

1. Добавь бота в группу
2. Назначь администратором (нужно для чтения всех сообщений и реакций)
3. У @BotFather: `Bot Settings → Group Privacy → OFF`
