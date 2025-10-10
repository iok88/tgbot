# merged_bot.py
# Объединённый пример: Telegram bot + Together AI + Google Sheets
import os
import re
import logging
import requests
import asyncio
from datetime import datetime

from telegram import Update
from telegram.ext import ApplicationBuilder, MessageHandler, filters, ContextTypes, CommandHandler

import gspread
from google.oauth2.service_account import Credentials

import time
from gspread import Client
from gspread.auth import AuthorizedSession

# Подключение к Google Sheets с таймаутом
creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
session = AuthorizedSession(creds)
session.requests_session.timeout = 10  # таймаут 10 секунд
gc = Client(None, session)
sheet = gc.open_by_key(SPREADSHEET_ID).sheet1

def append_row_with_retry(row, retries=3, delay=2):
    """
    Добавляет строку в Google Sheets с несколькими попытками и паузой между ними.
    """
    for attempt in range(1, retries + 1):
        try:
            sheet.append_row(row, value_input_option='USER_ENTERED')
            logger.info(f"Строка успешно записана в Google Sheets с {attempt}-й попытки.")
            return True
        except Exception as e:
            logger.error(f"Ошибка при записи в Google Sheets (попытка {attempt}): {e}")
            if attempt < retries:
                time.sleep(delay)
    return False

BOT_TOKEN = os.environ.get("BOT_TOKEN") or "7597740350:AAH1eqNGlHTbjkE7UUp80A9x6oqhINdz4Hs"
TOGETHER_API_KEY = os.environ.get("TOGETHER_API_KEY") or "4631235db33d35753063e6404b846beea5387a4324d8491a46d85f9fc743dd4a"
SERVICE_ACCOUNT_FILE = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS") or "D:\my cods\папка 5\causal-folder-463113-a8-b44594f9e475.json"
SPREADSHEET_ID = os.environ.get("SPREADSHEET_ID") or "1gGJTFWY4N3koNOHo-ZsNYC-7_pJ9Bv5S-9jauVsU4KA"
SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)
try:
    creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
    gc = gspread.authorize(creds)
    sheet = gc.open_by_key(SPREADSHEET_ID).sheet1  # первый лист
    logger.info("Успешно подключились к Google Sheets.")
except Exception as e:
    logger.exception("Ошибка при подключении к Google Sheets. Проверь SERVICE_ACCOUNT_FILE и SPREADSHEET_ID.")
    raise
HEADERS = [
    "Timestamp",             # 1 — время обращения
    "Имя",                   # 2 — имя пользователя (Telegram full name)
    "Username",              # 3 — Telegram username (если есть)
    "Где случилась проблема",# 4 — парсинг места/организации
    "Описание",              # 5 — подробности проблемы
    "TogetherAI ответ",      # 6 — что вернул LLM
    "Telegram user_id",      # 7 — ID пользователя (для отладки)
    "Message id"             # 8 — id сообщения
]
try:
    first_row = sheet.row_values(1)
    # простая проверка — если первой строки нет или это не наши заголовки, вставим
    if not first_row or first_row[0] != HEADERS[0]:
        sheet.insert_row(HEADERS, index=1)
        logger.info("Добавлены заголовки в таблицу.")
except Exception as e:
    logger.warning("Не удалось проверить/вставить заголовки: %s", e)

# -------------------------
# 5) Вспомогательные функции (блокировка вызывается асинхронно в executor)
# -------------------------
def call_together_api(prompt: str) -> str:
    """Вызывает Together AI (синхронно, поэтому мы запускаем в executor)."""
    try:
        response = requests.post(
            "https://api.together.xyz/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {TOGETHER_API_KEY}",
                "Content-Type": "application/json",
            },
            json={
                "model": "meta-llama/Llama-3.3-70B-Instruct-Turbo-Free",
                "messages": [{"role": "user", "content": prompt}],
                "temperature": 0.4,
                "max_tokens": 1500
            },
            timeout=30
        )
        response.raise_for_status()
        data = response.json()
        reply = data["choices"][0]["message"]["content"]
        return reply.strip()
    except Exception as e:
        logger.exception("Ошибка при вызове Together API")
        return "Ошибка при получении ответа от AI."

def append_row_to_sheet(row):
    """Добавляет строку в Google Sheet (синхронно)."""
    # value_input_option 'USER_ENTERED' позволит Excel-like форматирование, если нужно
    return sheet.append_row(row, value_input_option='USER_ENTERED')

# -------------------------
# 6) Telegram handlers
# -------------------------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Добрый день! Напишите ваше обращение в одном из форматов:\n"
        "  — \"ООО 'Ромашка' - проблемы с гидравликой экскаватора\"\n"
        "  — или просто: \"Потекла труба на складе\"\n\n"
        "Я запишу сообщение в таблицу и дам краткий ответ."
    )

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_input = (update.message.text or "").strip()
    user = update.message.from_user
    user_name = user.full_name or ""
    username = f"@{user.username}" if user.username else ""

    # Разбор сообщения: пытаемся отделить 'Где' от 'Описание' через дефис/двоеточие
    parts = re.split(r'\s*[-–—:]\s*', user_input, maxsplit=1)
    if len(parts) == 2:
        location = parts[0].strip()
        description = parts[1].strip()
    else:
        location = "Не указано"
        description = user_input

    # Формируем промпт для Together AI (как у тебя в оригинале)
    prompt = (
        f"Пользователь написал: '{user_input}'. "
        "Общайся как будто ты школьник из 7 класса, но если у тебя спросят чтобы ты скинул дз — ответь, "
        "что я не списывал и что родители тебя наругают, и отвечай только краткими ответами."
    )

    loop = asyncio.get_running_loop()

    # 1) Получаем ответ LLM в отдельном потоке (чтобы не блокировать event loop)
    try:
        reply = await loop.run_in_executor(None, call_together_api, prompt)
    except Exception as e:
        logger.exception("Ошибка при вызове LLM в executor")
        reply = "Извините, временная ошибка при обработке запроса."

    # 2) Готовим строку для записи в таблицу
    timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    row = [timestamp, user_name, username, location, description, reply, str(user.id), str(update.message.message_id)]

    # 3) Записываем строку в таблицу (также в executor)
    try:
        success = await loop.run_in_executor(None, append_row_with_retry, row)
        if not success:
            await update.message.reply_text("⚠ Не удалось записать данные в Google Таблицу. Попробуйте ещё раз позже.")
        
    except Exception as e:
        logger.exception("Не удалось записать данные в Google Sheets")

    # 4) Отвечаем пользователю
    try:
        await update.message.reply_text(reply)
    except Exception as e:
        logger.exception("Не удалось отправить ответ пользователю")

# -------------------------
# 7) Запуск бота
# -------------------------
if __name__ == "__main__":
    app = ApplicationBuilder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    logger.info("Бот запущен...")
    app.run_polling()
