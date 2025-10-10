# merged_bot.py
# Telegram bot + Together AI + Google Sheets (устойчивый вариант)
import os
import re
import logging
import requests
import asyncio
from datetime import datetime
import time

from telegram import Update
from telegram.ext import ApplicationBuilder, MessageHandler, filters, ContextTypes, CommandHandler

import gspread
from google.oauth2.service_account import Credentials

# -------------------------
# 1) Настройки
# -------------------------
BOT_TOKEN = os.environ.get("BOT_TOKEN") or "7597740350:AAH1eqNGlHTbjkE7UUp80A9x6oqhINdz4Hs"
TOGETHER_API_KEY = os.environ.get("TOGETHER_API_KEY") or "4631235db33d35753063e6404b846beea5387a4324d8491a46d85f9fc743dd4a"
SERVICE_ACCOUNT_FILE = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS") or r"D:\my cods\папка 5\causal-folder-463113-a8-b44594f9e475.json"
SPREADSHEET_ID = os.environ.get("SPREADSHEET_ID") or "1gGJTFWY4N3koNOHo-ZsNYC-7_pJ9Bv5S-9jauVsU4KA"
SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']

# -------------------------
# 2) Логирование
# -------------------------
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# -------------------------
# 3) Подключение к Google Sheets
# -------------------------
try:
    creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
    gc = gspread.authorize(creds)
    sheet = gc.open_by_key(SPREADSHEET_ID).sheet1
    logger.info("Успешно подключились к Google Sheets.")
except Exception as e:
    logger.exception("Ошибка при подключении к Google Sheets. Проверь SERVICE_ACCOUNT_FILE и SPREADSHEET_ID.")
    raise

# -------------------------
# 4) Заголовки
# -------------------------
HEADERS = [
    "Timestamp", "Имя", "Username", "Где случилась проблема",
    "Описание", "TogetherAI ответ", "Telegram user_id", "Message id"
]
try:
    first_row = sheet.row_values(1)
    if not first_row or first_row[0] != HEADERS[0]:
        sheet.insert_row(HEADERS, index=1)
        logger.info("Добавлены заголовки в таблицу.")
except Exception as e:
    logger.warning("Не удалось проверить/вставить заголовки: %s", e)

# -------------------------
# 5) Функции
# -------------------------
def call_together_api(prompt: str) -> str:
    """Вызов Together AI."""
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
        return data["choices"][0]["message"]["content"].strip()
    except Exception as e:
        logger.exception("Ошибка при вызове Together API")
        return "Ошибка при получении ответа от AI."

def append_row_with_retry(row, retries=3, delay=2):
    """Запись строки с несколькими попытками."""
    for attempt in range(1, retries + 1):
        try:
            sheet.append_row(row, value_input_option='USER_ENTERED')
            logger.info(f"Строка записана с {attempt}-й попытки.")
            return True
        except Exception as e:
            logger.error(f"Ошибка при записи в Google Sheets (попытка {attempt}): {e}")
            if attempt < retries:
                time.sleep(delay)
    return False

# -------------------------
# 6) Telegram handlers
# -------------------------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Добрый день! Напишите ваше обращение:\n"
        "  — \"ООО 'Ромашка' - проблемы с гидравликой экскаватора\"\n"
        "  — или: \"Потекла труба на складе\"\n\n"
        "Я запишу сообщение в таблицу и дам ответ."
    )

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_input = (update.message.text or "").strip()
    user = update.message.from_user
    user_name = user.full_name or ""
    username = f"@{user.username}" if user.username else ""

    parts = re.split(r'\s*[-–—:]\s*', user_input, maxsplit=1)
    if len(parts) == 2:
        location, description = parts[0].strip(), parts[1].strip()
    else:
        location, description = "Не указано", user_input

    prompt = (
        f"Пользователь написал: '{user_input}'. "
        "Общайся как будто ты школьник из 7 класса, но если у тебя спросят скинуть дз — ответь, "
        "что я не списывал и что родители тебя наругают, и отвечай кратко."
    )

    loop = asyncio.get_running_loop()

    try:
        reply = await loop.run_in_executor(None, call_together_api, prompt)
    except Exception:
        reply = "Извините, временная ошибка при обработке запроса."

    timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    row = [timestamp, user_name, username, location, description, reply, str(user.id), str(update.message.message_id)]

    try:
        success = await loop.run_in_executor(None, append_row_with_retry, row)
        if not success:
            await update.message.reply_text("⚠ Не удалось записать данные в Google Таблицу. Попробуйте ещё раз позже.")
    except Exception as e:
        logger.exception("Не удалось записать данные в Google Sheets")

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
