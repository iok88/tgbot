import os
import re
import time
import queue
import asyncio
import atexit
import json
import threading
import logging
import tkinter as tk
from pydub import AudioSegment
from datetime import datetime, timezone


# ---------- Telegram ----------
from telegram import Update
from telegram.ext import ApplicationBuilder, MessageHandler, filters, ContextTypes, CommandHandler

# ---------- Google Sheets ----------
import gspread
from google.oauth2.service_account import Credentials

# ---------- Speech / Voice ----------
import speech_recognition as sr
from pydub import AudioSegment
import tempfile

# Укажи путь к ffmpeg.exe
AudioSegment.converter = r"C:\\Users\\user\\Downloads\\ffmpeg-2025-09-10-git-c1dc2e2b7c-essentials_build\\ffmpeg-2025-09-10-git-c1dc2e2b7c-essentials_build\\bin\\ffmpeg.exe"

# ---------- Tk / UI ----------
try:
    import ttkbootstrap as tb
    from ttkbootstrap.constants import *
    TK_HAS_BOOTSTRAP = True
except Exception:
    from tkinter import ttk, messagebox
    TK_HAS_BOOTSTRAP = False
    
# ===========================
# НАСТРОЙКИ
# ===========================
CONFIG_FILE = "config.json"
BOT_TOKEN = ""
SERVICE_ACCOUNT_FILE = ""
SPREADSHEET_ID = ""
SEND_TO_CHAT_ID = None  # можно задать ID чата для дублирования

SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
]

HEADERS = [
    "Организация",
    "Дата",
    "Номер шасси",
    "Модель самосвала",
    "Что вышло из строя",
    "Описание проблемы",
    "Пробег / Моточасы"
]
# ===========================
# ЛОГИРОВАНИЕ
# ===========================
log_queue = queue.Queue()

class QueueHandler(logging.Handler):
    def emit(self, record):
        try:
            msg = self.format(record)
            log_queue.put(msg)
        except Exception:
            pass

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)
logger.handlers.clear()
queue_handler = QueueHandler()
queue_handler.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))
logger.addHandler(queue_handler)

# ===========================
# CONFIG JSON
# ===========================
def load_config():
    if os.path.exists(CONFIG_FILE):
        with open(CONFIG_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}

def save_config(cfg):
    with open(CONFIG_FILE, "w", encoding="utf-8") as f:
        json.dump(cfg, f, ensure_ascii=False, indent=2)
        
# ===========================
# GOOGLE SHEETS
# ===========================
def connect_sheets():
    creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(SPREADSHEET_ID)
    ws = sh.sheet1
    return ws

def append_row_with_retry(row, retries=3, delay=2):
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


# ===========================
# ПАРСЕР СООБЩЕНИЙ
# ===========================
def parse_message(text: str):
    text_orig = (text or "").strip()
    text_l = text_orig.lower()

    org = ""
    m_org = re.search(r"^(.*?)\s*(?:шасси|на\s|—|-|–|:|,?\s*где|,?\s*в)\b", text_l)
    if m_org:
        org = m_org.group(1).strip()
    else:
        m = re.match(r"^(.*?)[,;]", text_orig)
        org = m.group(1).strip() if m else ""

    if org:
        pattern = re.compile(re.escape(org), re.IGNORECASE)
        m_orig = pattern.search(text_orig)
        org = m_orig.group(0).strip() if m_orig else org

    date_str = datetime.now(timezone.utc).astimezone().strftime("%Y-%m-%d %H:%M:%S %Z")

    m_chassis = re.search(r"шасси\s*[:\s]?\s*(\d+)", text_l)
    chassis = m_chassis.group(1) if m_chassis else ""

    model = ""
    model_candidates = ["белаз", "cat", "volvo", "komatsu", "dumper", "камаз", "shacman", "moxy", "terex"]
    for cand in model_candidates:
        if re.search(r"\b" + re.escape(cand) + r"\b", text_l):
            m_mod = re.search(r"\b(" + re.escape(cand) + r")\b", text_orig, re.IGNORECASE)
            model = m_mod.group(1) if m_mod else cand
            break

    km = None
    hours = None
    m_km = re.search(r"(\d{2,7})\s*км\b", text_l)
    if m_km:
        km = m_km.group(1)
    m_h = re.search(r"(\d{1,6})\s*ч\b", text_l)
    if m_h:
        hours = m_h.group(1)

    mileage_hours = ""
    if km:
        mileage_hours += f"{km} км"
    if hours:
        if mileage_hours:
            mileage_hours += ", "
        mileage_hours += f"{hours} ч"

    failures = []
    for pat in [r"защита[:\s]*([^\.,;]+)", r"ошибка[:\s]*([^\.,;]+)", r"ошибка\s+([^\.,;]+)", r"отказ[:\s]*([^\.,;]+)"]:
        for m in re.finditer(pat, text_l):
            failures.append(m.group(0).strip())
    failures = list(dict.fromkeys(failures))
    failure_text = "; ".join(failures)

    description = text_orig
    if org:
        description = re.sub(re.compile(re.escape(org), re.IGNORECASE), "", description, count=1).strip()
    if chassis:
        description = re.sub(re.compile(r"шасси\s*[:\s]?\s*" + re.escape(chassis), re.IGNORECASE), "", description)
    if km:
        description = re.sub(re.compile(re.escape(km) + r"\s*км", re.IGNORECASE), "", description)
    if hours:
        description = re.sub(re.compile(re.escape(hours) + r"\s*ч", re.IGNORECASE), "", description)
    description = re.sub(r"^[\s,;:\-]+", "", description).strip()

    return {
        "organization": org,
        "date": date_str,
        "chassis": chassis,
        "model": model,
        "failure": failure_text,
        "description": description,
        "mileage_hours": mileage_hours
    }

def make_row(parsed: dict):
    return [
        parsed["organization"],
        parsed["date"],
        parsed["chassis"],
        parsed["model"],
        parsed["failure"],
        parsed["description"],
        parsed["mileage_hours"]
    ]

# ===========================
# TELEGRAM BOT
# ===========================
async def tg_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Привет! Пришли сообщение (текст или голосовое).")


async def process_text(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str):
    if not text:
        await update.message.reply_text("Пожалуйста, пришлите текст или голосовое сообщение.")
        return
    parsed = parse_message(text)
    row = make_row(parsed)
    loop = asyncio.get_running_loop()
    ok = await loop.run_in_executor(None, append_row_with_retry, row)
    if ok:
        await update.message.reply_text(f"✅ Данные записаны.\nРаспознанный текст: {text}")
    else:
        await update.message.reply_text("⚠ Ошибка записи в таблицу.")


async def tg_handle(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (update.message.text or "").strip()
    await process_text(update, context, text)

async def tg_handle_voice(update: Update, context: ContextTypes.DEFAULT_TYPE):
    voice = update.message.voice
    file = await context.bot.get_file(voice.file_id)

    # временный файл
    with tempfile.NamedTemporaryFile(delete=False, suffix=".ogg") as f:
        ogg_path = f.name
        await file.download_to_drive(ogg_path)

    wav_path = ogg_path.replace(".ogg", ".wav")
    AudioSegment.from_file(ogg_path).export(wav_path, format="wav")

    recognizer = sr.Recognizer()
    try:
        with sr.AudioFile(wav_path) as source:
            audio_data = recognizer.record(source)
            text = recognizer.recognize_google(audio_data, language="ru-RU")
    except Exception as e:
        text = f"Ошибка распознавания: {e}"

    os.remove(ogg_path)
    os.remove(wav_path)

    # 🔹 нормализация текста перед обработкой
    text = normalize_recognized_text(text)

    await process_text(update, context, text)


# ===========================
# НОРМАЛИЗАЦИЯ РАСПОЗНАННОГО ТЕКСТА
# ===========================
def normalize_recognized_text(text: str) -> str:
    text = text.lower()

    # исправляем типичные ошибки распознавания
    text = re.sub(r"\bчасо[в]*\b", "ч", text)
    text = re.sub(r"\bкилометр[а-я]*\b", "км", text)

    # восстанавливаем бренды с большой буквы
    text = re.sub(r"\bбелаз\b", "БелАЗ", text, flags=re.IGNORECASE)
    text = re.sub(r"\bкамаз\b", "КамАЗ", text, flags=re.IGNORECASE)
    text = re.sub(r"\bvolvo\b", "Volvo", text, flags=re.IGNORECASE)
    text = re.sub(r"\bkomatsu\b", "Komatsu", text, flags=re.IGNORECASE)
    text = re.sub(r"\bcat\b", "CAT", text, flags=re.IGNORECASE)

    return text.strip()



def run_telegram_bot():
    import asyncio
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    app = ApplicationBuilder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", tg_start))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, tg_handle))
    app.add_handler(MessageHandler(filters.VOICE, tg_handle_voice))
    logger.info("Бот запущен...")
    loop.run_until_complete(app.run_polling())

# ===========================
# TK GUI
# ===========================
class AppUI:
    def __init__(self):
        if TK_HAS_BOOTSTRAP:
            self.root = tb.Window(themename="flatly")
            self.ttk = tb
        else:
            self.root = tk.Tk()
            self.ttk = ttk
        self.root.geometry("980x680")
        self.root.title("Чат-бот → Google Sheets")

        frm_top = self.ttk.Frame(self.root, padding=10)
        frm_top.pack(fill="x")
        self.txt_input = self.ttk.Text(frm_top, height=6, wrap="word")
        self.txt_input.pack(fill="x", pady=6)
        frm_btns = self.ttk.Frame(frm_top)
        frm_btns.pack(fill="x", pady=4)

        if TK_HAS_BOOTSTRAP:
            self.ttk.Button(frm_btns, text="Проверить", bootstyle=INFO, command=self.on_check).pack(side="left", padx=4)
            self.ttk.Button(frm_btns, text="Отправить", bootstyle=SUCCESS, command=self.on_send).pack(side="left", padx=4)
            self.ttk.Button(frm_btns, text="Очистить", bootstyle=SECONDARY, command=self.on_clear).pack(side="left", padx=4)
            self.ttk.Button(frm_btns, text="Настройки", bootstyle=WARNING, command=self.open_settings_window).pack(side="left", padx=4)
        else:
            self.ttk.Button(frm_btns, text="Проверить", command=self.on_check).pack(side="left", padx=4)
            self.ttk.Button(frm_btns, text="Отправить", command=self.on_send).pack(side="left", padx=4)
            self.ttk.Button(frm_btns, text="Очистить", command=self.on_clear).pack(side="left", padx=4)
            self.ttk.Button(frm_btns, text="Настройки", command=self.open_settings_window).pack(side="left", padx=4)

        frm_mid = self.ttk.Labelframe(self.root, text="Предпросмотр", padding=10)
        frm_mid.pack(fill="x", padx=10, pady=8)
        self.var_org   = self._add_kv(frm_mid, "Организация")
        self.var_date  = self._add_kv(frm_mid, "Дата")
        self.var_chas  = self._add_kv(frm_mid, "Номер шасси")
        self.var_model = self._add_kv(frm_mid, "Модель")
        self.var_fail  = self._add_kv(frm_mid, "Что вышло из строя")
        self.var_mh    = self._add_kv(frm_mid, "Пробег / Моточасы")

        lbl_desc = self.ttk.Label(frm_mid, text="Описание:")
        lbl_desc.pack(anchor="w", pady=(8, 2))
        self.txt_desc = self.ttk.Text(frm_mid, height=5, wrap="word", state="disabled")
        self.txt_desc.pack(fill="x")

        frm_logs = self.ttk.Labelframe(self.root, text="Логи", padding=10)
        frm_logs.pack(fill="both", expand=True, padx=10, pady=(8, 10))
        self.txt_logs = self.ttk.Text(frm_logs, height=12, wrap="none", state="disabled")
        self.txt_logs.pack(fill="both", expand=True)
        self.root.after(200, self.poll_logs)

    def _add_kv(self, parent, key):
        frm = self.ttk.Frame(parent)
        frm.pack(fill="x", pady=2)
        lbl_key = self.ttk.Label(frm, text=f"{key}:", width=20, anchor="w")
        lbl_key.pack(side="left")
        var = tk.StringVar(value="")
        ent = self.ttk.Entry(frm, textvariable=var, state="readonly")
        ent.pack(side="left", fill="x", expand=True)
        return var

    def poll_logs(self):
        try:
            while True:
                line = log_queue.get_nowait()
                self._append_log(line + "\n")
        except queue.Empty:
            pass
        self.root.after(300, self.poll_logs)

    def _append_log(self, text):
        self.txt_logs.configure(state="normal")
        self.txt_logs.insert("end", text)
        self.txt_logs.see("end")
        self.txt_logs.configure(state="disabled")

    def on_clear(self):
        self.txt_input.delete("1.0", "end")

    def on_check(self):
        text = self.txt_input.get("1.0", "end").strip()
        parsed = parse_message(text)
        self._fill_preview(parsed)

    def _fill_preview(self, parsed):
        self.var_org.set(parsed["organization"])
        self.var_date.set(parsed["date"])
        self.var_chas.set(parsed["chassis"])
        self.var_model.set(parsed["model"])
        self.var_fail.set(parsed["failure"])
        self.var_mh.set(parsed["mileage_hours"])
        self.txt_desc.configure(state="normal")
        self.txt_desc.delete("1.0", "end")
        self.txt_desc.insert("1.0", parsed["description"])
        self.txt_desc.configure(state="disabled")

    def on_send(self):
        text = self.txt_input.get("1.0", "end").strip()
        if not text:
            self._append_log("Введите сообщение.\n")
            return
        parsed = parse_message(text)
        self._fill_preview(parsed)
        row = make_row(parsed)
        ok = append_row_with_retry(row)
        if ok:
            self._append_log("✅ Запись добавлена в Google Sheets.\n")
        else:
            self._append_log("⚠ Не удалось записать.\n")

    def open_settings_window(self):
        win = tk.Toplevel(self.root)
        win.title("Настройки")
        win.geometry("500x300")

        tk.Label(win, text="BOT_TOKEN:").pack(anchor="w")
        ent_token = tk.Entry(win)
        ent_token.insert(0, BOT_TOKEN)
        ent_token.pack(fill="x")

        tk.Label(win, text="GOOGLE_APPLICATION_CREDENTIALS:").pack(anchor="w")
        ent_cred = tk.Entry(win)
        ent_cred.insert(0, SERVICE_ACCOUNT_FILE)
        ent_cred.pack(fill="x")

        tk.Label(win, text="SPREADSHEET (ссылка или ID):").pack(anchor="w")
        ent_sheet = tk.Entry(win)
        ent_sheet.insert(0, SPREADSHEET_ID)
        ent_sheet.pack(fill="x")

        def save_and_apply():
            global BOT_TOKEN, SERVICE_ACCOUNT_FILE, SPREADSHEET_ID, sheet
            BOT_TOKEN = ent_token.get().strip()
            SERVICE_ACCOUNT_FILE = ent_cred.get().strip()
            link = ent_sheet.get().strip()
            if "/d/" in link:
                try:
                    SPREADSHEET_ID = link.split("/d/")[1].split("/")[0]
                except Exception:
                    SPREADSHEET_ID = link
            else:
                SPREADSHEET_ID = link
            save_config({
                "BOT_TOKEN": BOT_TOKEN,
                "GOOGLE_APPLICATION_CREDENTIALS": SERVICE_ACCOUNT_FILE,
                "SPREADSHEET_ID": SPREADSHEET_ID
            })
            try:
                sheet = connect_sheets()
                self._append_log("✅ Настройки сохранены и подключение обновлено.\n")
            except Exception as e:
                self._append_log(f"⚠ Ошибка подключения: {e}\n")
            win.destroy()

        tk.Button(win, text="Сохранить", command=save_and_apply).pack(pady=10)

    def run(self):
        self.root.mainloop()

cfg = load_config()
BOT_TOKEN = cfg.get("BOT_TOKEN", BOT_TOKEN)
SERVICE_ACCOUNT_FILE = cfg.get("GOOGLE_APPLICATION_CREDENTIALS", SERVICE_ACCOUNT_FILE)
SPREADSHEET_ID = cfg.get("SPREADSHEET_ID", SPREADSHEET_ID)

sheet = connect_sheets()
try:
    first_row = sheet.row_values(1)
    if not first_row or first_row[:len(HEADERS)] != HEADERS:
        sheet.insert_row(HEADERS, index=1)
except Exception as e:
    logger.warning("Не удалось проверить заголовки: %s", e)

def start_threads():
    t = threading.Thread(target=run_telegram_bot, name="TelegramBotThread", daemon=True)
    t.start()

def main():
    start_threads()
    app = AppUI()
    app.run()

atexit.register(lambda: logger.info("Завершение работы..."))

if __name__ == "__main__":
    main()
