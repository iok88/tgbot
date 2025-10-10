# my_parsing_bot.py
# Telegram bot: разбирает технические сообщения и записывает структурированные данные в Google Sheets

import os
import re
import time
import logging
import asyncio
from datetime import datetime, timezone

from telegram import Update
from telegram.ext import ApplicationBuilder, MessageHandler, filters, ContextTypes, CommandHandler

import gspread
from google.oauth2.service_account import Credentials
import requests  # оставил, если захочешь использовать Together AI позже

# -------------------------
# Настройки (отредактируй по необходимости)
# -------------------------
BOT_TOKEN = os.environ.get("BOT_TOKEN") or "7597740350:AAH1eqNGlHTbjkE7UUp80A9x6oqhINdz4Hs"
# если не используешь Together AI, можешь оставить пустым
TOGETHER_API_KEY = os.environ.get("TOGETHER_API_KEY") or "4631235db33d35753063e6404b846beea5387a4324d8491a46d85f9fc743dd4a"
# обязательно укажи путь к JSON-ключу (или используй переменную окружения GOOGLE_APPLICATION_CREDENTIALS)
SERVICE_ACCOUNT_FILE = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS") or r"D:\my cods\папка 5\causal-folder-463113-a8-b44594f9e475.json"
SPREADSHEET_ID = os.environ.get("SPREADSHEET_ID") or "1-Zwh6d30h7rIb9HNXycDnZwFe6OVCp89SBJW_Yqywas"

SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']

# -------------------------
# Логирование
# -------------------------
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    level=logging.INFO)
logger = logging.getLogger(__name__)

# -------------------------
# Подключение к Google Sheets
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
# Заголовки: порядок столбцов в таблице (именно такой вывел пользователь)
# -------------------------
HEADERS = [
    "Организация",
    "Дата",
    "Номер шасси",
    "Модель самосвала",
    "Что вышло из строя",
    "Описание проблемы",
    "Пробег / Моточасы"
]
try:
    first_row = sheet.row_values(1)
    # если таблица новая или первая ячейка не совпадает — вставим заголовки
    if not first_row or first_row[:len(HEADERS)] != HEADERS:
        # Если там уже есть данные, insert_row сдвинет их вниз
        sheet.insert_row(HEADERS, index=1)
        logger.info("Добавлены заголовки в таблицу.")
except Exception as e:
    logger.warning("Не удалось проверить/вставить заголовки: %s", e)

# -------------------------
# Парсер: извлечение нужных полей из сообщения
# -------------------------
def parse_message(text: str):
    text_orig = text.strip()
    text_l = text_orig.lower()

    # 1) Организация: попробуем взять кусок до "шасси" или до первого " - " / новой строки / " на "
    org = ""
    m_org = re.search(r"^(.*?)\s*(?:шасси|на\s|—|-|–|:|,?\s*где|,?\s*в)\b", text_l, re.IGNORECASE)
    if m_org:
        org = m_org.group(1).strip()
    else:
        # иначе: текст до первой запятой (если подходит)
        m = re.match(r"^(.*?)[,;]", text_orig)
        org = m.group(1).strip() if m else ""

    # Сохраним организацию в исходном регистре, если нашли по lower
    if org:
        # найдём оригинальную подстроку в исходном тексте (регистрозависимо)
        pattern = re.compile(re.escape(org), re.IGNORECASE)
        m_orig = pattern.search(text_orig)
        org = m_orig.group(0).strip() if m_orig else org

    # 2) Дата — текущее время (UTC с часовым поясом)
    date_str = datetime.now(timezone.utc).astimezone().strftime("%Y-%m-%d %H:%M:%S %Z")

    # 3) Номер шасси
    m_chassis = re.search(r"шасси\s*[:\s]?\s*(\d+)", text_l, re.IGNORECASE)
    chassis = m_chassis.group(1) if m_chassis else ""

    # 4) Модель самосвала — часто в тексте нет; попробуем найти слова "БелАЗ", "CAT", "Volvo" и др.
    model = ""
    model_candidates = ["белаз", "cat", "volvo", "komatsu", "dumper", "камаз", "shacman", "moxy", "terex"]
    for cand in model_candidates:
        if re.search(r"\b" + re.escape(cand) + r"\b", text_l, re.IGNORECASE):
            # вернём встреченный фрагмент в оригинальном регистре
            m_mod = re.search(r"\b(" + re.escape(cand) + r")\b", text_orig, re.IGNORECASE)
            model = m_mod.group(1) if m_mod else cand
            break

    # 5) Пробег и моточасы
    km = None
    hours = None
    # ищем варианты вида "23310км" или "23310 км"
    m_km = re.search(r"(\d{2,7})\s*км\b", text_l, re.IGNORECASE)
    if m_km:
        km = m_km.group(1)
    m_h = re.search(r"(\d{1,6})\s*ч\b", text_l, re.IGNORECASE)
    if m_h:
        hours = m_h.group(1)

    mileage_hours = ""
    if km:
        mileage_hours += f"{km} км"
    if hours:
        if mileage_hours:
            mileage_hours += ", "
        mileage_hours += f"{hours} ч"

    # 6) Что вышло из строя — собираем все упоминания "защита: ...", "ошибка ...", "отказ ..." и т.п.
    failures = []
    for pat in [r"защита[:\s]*([^\.,;]+)", r"ошибка[:\s]*([^\.,;]+)", r"ошибка\s+([^\.,;]+)", r"отказ[:\s]*([^\.,;]+)"]:
        for m in re.finditer(pat, text_l, re.IGNORECASE):
            failures.append(m.group(0).strip())
    # Удалим дубли и объединим
    failures = list(dict.fromkeys(failures))
    failure_text = "; ".join(failures)

    # 7) Описание проблемы — оставим весь текст, но можно попытаться убрать уже распарсенные куски
    description = text_orig
    # Попробуем убрать организацию и пробег/часы коротко из описания, чтобы не дублировать
    if org:
        description = re.sub(re.compile(re.escape(org), re.IGNORECASE), "", description, count=1).strip()
    if chassis:
        description = re.sub(re.compile(r"шасси\s*[:\s]?\s*" + re.escape(chassis), re.IGNORECASE), "", description)
    if km:
        description = re.sub(re.compile(re.escape(km) + r"\s*км", re.IGNORECASE), "", description)
    if hours:
        description = re.sub(re.compile(re.escape(hours) + r"\s*ч", re.IGNORECASE), "", description)

    description = re.sub(r"^[\s,;:-]+", "", description).strip()

    return {
        "organization": org,
        "date": date_str,
        "chassis": chassis,
        "model": model,
        "failure": failure_text,
        "description": description,
        "mileage_hours": mileage_hours
    }

# -------------------------
# Запись в Google Sheets с retry
# -------------------------
def append_row_with_retry(row, retries=3, delay=2):
    """
    Попытаться записать строку в Google Sheet. Возвращает True при успехе.
    """
    for attempt in range(1, retries + 1):
        try:
            sheet.append_row(row, value_input_option='USER_ENTERED')
            logger.info("Строка успешно записана (попытка %d).", attempt)
            return True
        except Exception as e:
            logger.error("Ошибка записи в Google Sheets (попытка %d): %s", attempt, e)
            if attempt < retries:
                time.sleep(delay)
    return False

# -------------------------
# Telegram handlers
# -------------------------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Привет! Пришли сообщение с данными (пример):\n"
        "Маломырский рудник , Амурская область шасси 773 23310км, 2245ч на спуске ...\n\n"
        "Я распарсю данные и запишу их в таблицу."
    )

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_input = (update.message.text or "").strip()
    if not user_input:
        await update.message.reply_text("Пожалуйста, отправь текстовое сообщение с данными.")
        return

    parsed = parse_message(user_input)

    # Формируем строку в порядке: организация, дата, номер шасси, модель, что вышло, описание, пробег/моточасы
    row = [
        parsed["organization"],
        parsed["date"],
        parsed["chassis"],
        parsed["model"],
        parsed["failure"],
        parsed["description"],
        parsed["mileage_hours"]
    ]

    loop = asyncio.get_running_loop()
    success = await loop.run_in_executor(None, append_row_with_retry, row)

    # Ответ пользователю: краткое подтверждение + распарсенные ключевые поля
    if success:
        reply = (
            "✅ Данные сохранены в таблицу.\n\n"
            f"Организация: {parsed['organization'] or '—'}\n"
            f"Шасси: {parsed['chassis'] or '—'}\n"
            f"Что вышло из строя: {parsed['failure'] or '—'}\n"
            f"Пробег/моточасы: {parsed['mileage_hours'] or '—'}"
        )
    else:
        reply = "⚠ Не удалось записать данные в Google Таблицу. Попробуй ещё раз позже."

    try:
        await update.message.reply_text(reply)
    except Exception as e:
        logger.exception("Не удалось отправить сообщение пользователю: %s", e)

# -------------------------
# Запуск бота
# -------------------------
if __name__ == "__main__":
    app = ApplicationBuilder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    logger.info("Бот запущен...")
    app.run_polling()
