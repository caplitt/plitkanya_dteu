import os, logging, asyncio, time, sqlite3, io, csv
from collections import defaultdict, deque
from contextlib import closing
from datetime import datetime, timezone
from dotenv import load_dotenv
from telegram import Update, InputFile
from telegram.ext import Application, CommandHandler, MessageHandler, ContextTypes, filters

# ---------- ЛОГИ ----------
logging.basicConfig(format="%(asctime)s [%(levelname)s] %(message)s",
                    level=logging.INFO)
log = logging.getLogger("anonbot")

# ---------- ENV ----------
load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN")
TARGET_CHAT_ID = os.getenv("TARGET_CHAT_ID")
OWNER_ID = os.getenv("OWNER_ID")  # обов'язково постав свій числовий Telegram ID

if not BOT_TOKEN:
    raise SystemExit("BOT_TOKEN не заданий")
if not TARGET_CHAT_ID:
    raise SystemExit("TARGET_CHAT_ID не заданий (наприклад -1001234567890)")
if not OWNER_ID:
    log.warning("OWNER_ID не заданий — команди для власника будуть недоступні.")

# ---------- БД (SQLite) ----------
DB_PATH = os.getenv("DB_PATH", "messages.db")

def db_init():
    with closing(sqlite3.connect(DB_PATH)) as conn, conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                username TEXT,
                first_name TEXT,
                ts INTEGER NOT NULL,
                mtype TEXT NOT NULL,      -- 'text' | 'photo' | 'document'
                text TEXT,
                caption TEXT,
                file_id TEXT
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_user_ts ON messages(user_id, ts DESC)")

def db_insert_message(user_id: int, username: str | None, first_name: str | None,
                      ts: int, mtype: str, text: str | None,
                      caption: str | None, file_id: str | None):
    with closing(sqlite3.connect(DB_PATH)) as conn, conn:
        conn.execute("""
            INSERT INTO messages (user_id, username, first_name, ts, mtype, text, caption, file_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (user_id, username, first_name, ts, mtype, text, caption, file_id))

def db_fetch_user_by_username(username: str):
    with closing(sqlite3.connect(DB_PATH)) as conn:
        row = conn.execute("""
            SELECT user_id, username, first_name, MAX(ts) AS last_ts
            FROM messages
            WHERE LOWER(COALESCE(username,'')) = LOWER(?)
        """, (username,)).fetchone()
        return row

def db_fetch_messages_by_user(user_id: int, limit: int = 50):
    with closing(sqlite3.connect(DB_PATH)) as conn:
        rows = conn.execute("""
            SELECT ts, mtype, COALESCE(text, caption, '') AS content, file_id
            FROM messages
            WHERE user_id = ?
            ORDER BY ts DESC
            LIMIT ?
        """, (user_id, limit)).fetchall()
        return rows

db_init()

# ---------- АНТИСПАМ ----------
LIMIT = 5
WINDOW = 30.0  # сек
rate_buckets: dict[int, deque[float]] = defaultdict(lambda: deque(maxlen=LIMIT))

def check_rate_limit(user_id: int) -> float:
    now = time.monotonic()
    bucket = rate_buckets[user_id]
    while bucket and now - bucket[0] > WINDOW:
        bucket.popleft()
    if len(bucket) >= LIMIT:
        return max(0.0, WINDOW - (now - bucket[0]))
    bucket.append(now)
    return 0.0

# ---------- ХЕЛПЕРИ ----------
def is_owner(update: Update) -> bool:
    try:
        return OWNER_ID and update.effective_user and str(update.effective_user.id) == str(OWNER_ID)
    except Exception:
        return False

def ts_to_str(ts: int) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).astimezone().strftime("%Y-%m-%d %H:%M:%S")

HELP_TEXT = "Надішліть мені повідомлення — я перепошлю його в канал/групу."

# ---------- ХЕНДЛЕРИ КОМАНД ----------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message:
        await update.message.reply_text(HELP_TEXT)

async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message:
        await update.message.reply_text(HELP_TEXT)

async def finduser(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Тільки для власника: /finduser <username> -> user_id"""
    if not is_owner(update):
        return
    if not update.message:
        return
    args = (update.message.text or "").split()
    if len(args) < 2:
        await update.message.reply_text("Використання: /finduser <username> (без @)")
        return
    username = args[1].lstrip("@")
    row = db_fetch_user_by_username(username)
    if not row or not row[0]:
        await update.message.reply_text("Не знайдено такого користувача в логах.")
        return
    user_id, uname, first_name, last_ts = row
    last_seen = ts_to_str(last_ts) if last_ts else "невідомо"
    await update.message.reply_text(f"user_id: {user_id}\nusername: @{uname or ''}\nname: {first_name or ''}\nlast_seen: {last_seen}")

async def userlog(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Тільки для власника: /userlog <user_id> [N] -> файл з останніми N повідомленнями"""
    if not is_owner(update) or not update.message:
        return
    args = (update.message.text or "").split()
    if len(args) < 2:
        await update.message.reply_text("Використання: /userlog <user_id> [N=50]")
        return
    try:
        user_id = int(args[1])
    except ValueError:
        await update.message.reply_text("user_id має бути числом.")
        return
    limit = 50
    if len(args) >= 3:
        try:
            limit = max(1, min(1000, int(args[2])))
        except ValueError:
            pass

    rows = db_fetch_messages_by_user(user_id, limit)
    if not rows:
        await update.message.reply_text("Немає повідомлень для цього користувача.")
        return

    # Збираємо CSV у пам'яті
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(["timestamp_local", "type", "content", "file_id"])
    for ts, mtype, content, file_id in rows:
        writer.writerow([ts_to_str(ts), mtype, (content or "").replace("\n", "\\n"), file_id or ""])
    data = buf.getvalue().encode("utf-8")
    buf.close()

    fname = f"user_{user_id}_last_{limit}.csv"
    await update.message.reply_document(document=InputFile(io.BytesIO(data), filename=fname),
                                        caption=f"Останні {len(rows)} повідомлень користувача {user_id}")

# ---------- ПРИВАТНІ ПОВІДОМЛЕННЯ ----------
async def private_msg(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Приймає приватні повідомлення і публікує їх у канал/групу без додаткового тексту + логування в SQLite."""
    if update.effective_chat.type != "private" or not update.message:
        return

    user = update.effective_user
    # антиспам
    wait = check_rate_limit(user.id)
    if wait > 0:
        wait_rounded = max(1, int(wait + 0.5))
        await update.message.reply_text(f"⏳ Забагато повідомлень. Спробуйте знову через ~{wait_rounded} сек.")
        return

    ts = int(time.time())
    username = (user.username or None)
    first_name = (user.first_name or None)

    try:
        if update.message.text:
            text = update.message.text
            # лог у БД
            db_insert_message(user.id, username, first_name, ts, "text", text, None, None)
            # публікація
            await context.bot.send_message(chat_id=int(TARGET_CHAT_ID), text=text, disable_web_page_preview=True)

        elif update.message.photo:
            file_id = update.message.photo[-1].file_id
            caption = update.message.caption or None
            db_insert_message(user.id, username, first_name, ts, "photo", None, caption, file_id)
            await context.bot.send_photo(chat_id=int(TARGET_CHAT_ID), photo=file_id, caption=caption)

        elif update.message.document:
            file_id = update.message.document.file_id
            caption = update.message.caption or None
            db_insert_message(user.id, username, first_name, ts, "document", None, caption, file_id)
            await context.bot.send_document(chat_id=int(TARGET_CHAT_ID), document=file_id, caption=caption)

        else:
            await update.message.reply_text("Підтримується тільки текст, фото або документи.")
            return

        await update.message.reply_text("✅ Повідомлення відправлено.")
        log.info("Forwarded & logged")
    except Exception:
        log.exception("Send error")
        await update.message.reply_text("Не вдалося надіслати в канал/групу. Перевірте TARGET_CHAT_ID та права бота.")

# ---------- APP ----------
def build_app() -> Application:
    app = Application.builder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_cmd))
    if OWNER_ID:
        app.add_handler(CommandHandler("finduser", finduser))
        app.add_handler(CommandHandler("userlog", userlog))
    app.add_handler(MessageHandler(filters.ChatType.PRIVATE, private_msg))
    return app

def main():
    app = build_app()
    log.info("Starting long polling…")
    app.run_polling(allowed_updates=Update.ALL_TYPES, drop_pending_updates=True)

if __name__ == "__main__":
    asyncio.run(main())
