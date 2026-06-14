"""
core_bot.py — Cloud Core (Koyeb Deployment)
============================================

TUGAS:
  - Terima perintah user via Telegram
  - Fetch metadata URL (yt-dlp --dump-json minimal, TANPA download)
  - Insert job ke PostgreSQL queue
  - Poll status job & update pesan Telegram
  - TIDAK ADA subprocess download, TIDAK ADA yt-dlp berat, TIDAK ADA ffmpeg

FLOW:
  User → /start | URL → Core → queue INSERT → Worker picks up
  Core polling loop → edit message saat status berubah
"""

from __future__ import annotations

import asyncio
import io
import json
import logging
import os
import random
import string
import time
from logging.handlers import RotatingFileHandler

import aiohttp
import qrcode
import telegram.error
from telegram import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Update,
)
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)
from telegram.request import HTTPXRequest

# Import shared modules
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from shared import database as db
from shared.utils import (
    detect_type_from_url,
    format_duration,
    format_size,
    get_uptime,
    is_allowed_domain,
    normalize_url,
    safe_html,
)

# ─── Config ────────────────────────────────────────────────────────────────────

TOKEN: str   = os.environ["BOT_TOKEN"]
MY_ID: int   = int(os.environ["MY_ID"])
LOG_FILE: str = os.getenv("LOG_FILE", "/tmp/core_bot.log")

# Polling: seberapa sering Core mengecek status job untuk tiap user
JOB_POLL_INTERVAL: float = float(os.getenv("JOB_POLL_INTERVAL", "5.0"))

# Max metadata fetch timeout (yt-dlp --dump-json, bukan download)
META_FETCH_TIMEOUT: int = int(os.getenv("META_FETCH_TIMEOUT", "30"))

# Bot startup time
START_TIME = time.time()

# ─── Logging ───────────────────────────────────────────────────────────────────

logging.basicConfig(
    handlers=[RotatingFileHandler(LOG_FILE, maxBytes=2_000_000, backupCount=3)],
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("core_bot")

# ─── Global state (minimal, semua real state ada di DB) ───────────────────────

# Active polling tasks: job_id → asyncio.Task
# Core poll DB tiap JOB_POLL_INTERVAL detik untuk update pesan Telegram
_active_polls: dict[str, asyncio.Task] = {}

# Rate limit per user (simple in-memory, stateless per restart)
_rate_limits: dict[int, float] = {}

BACKGROUND_TASKS: set[asyncio.Task] = set()


def fire(coro):
    task = asyncio.create_task(coro)
    BACKGROUND_TASKS.add(task)
    task.add_done_callback(BACKGROUND_TASKS.discard)


# ─── Telegram Helpers ──────────────────────────────────────────────────────────

async def safe_edit(
    app: Application,
    chat_id: int,
    message_id: int,
    text: str,
    reply_markup=None,
) -> None:
    if len(text) > 3900:
        text = text[:3900] + "\n\n<i>... [truncated]</i>"
    try:
        await app.bot.edit_message_text(
            chat_id=chat_id,
            message_id=message_id,
            text=text,
            parse_mode="HTML",
            reply_markup=reply_markup,
        )
    except telegram.error.BadRequest as e:
        msg = str(e).lower()
        if "message is not modified" in msg:
            return
        if "not found" in msg or "deleted" in msg:
            try:
                await app.bot.send_message(
                    chat_id=chat_id, text=text,
                    parse_mode="HTML", reply_markup=reply_markup,
                )
            except Exception:
                pass
            return
        logger.exception("BadRequest editing message")
    except telegram.error.RetryAfter as e:
        logger.warning("FloodWait: sleeping %ds", e.retry_after)
        await asyncio.sleep(e.retry_after + 1)
        try:
            await app.bot.edit_message_text(
                chat_id=chat_id, message_id=message_id,
                text=text, parse_mode="HTML", reply_markup=reply_markup,
            )
        except Exception:
            pass
    except Exception:
        logger.exception("Unexpected error in safe_edit")


# ─── Metadata Fetch (lightweight, HANYA --dump-json) ──────────────────────────

async def fetch_media_metadata(url: str, job_type: str) -> tuple[bool, dict | str]:
    """
    Jalankan yt-dlp --dump-json untuk ambil metadata TANPA download.
    Ini satu-satunya subprocess yang boleh ada di Core.
    Timeout ketat: 30 detik.
    """
    cmd = [
        "yt-dlp",
        "--dump-json",
        "--quiet",
        "--no-warnings",
        "--no-playlist",
        "--restrict-filenames",
        "--",
        url,
    ]
    if job_type == "audio":
        cmd = [
            "yt-dlp",
            "--dump-json",
            "--quiet",
            "--no-warnings",
            "--no-playlist",
            "-f", "bestaudio",
            "--restrict-filenames",
            "--",
            url,
        ]

    process = None
    try:
        process = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            stdout, stderr = await asyncio.wait_for(
                process.communicate(), timeout=META_FETCH_TIMEOUT
            )
        except asyncio.TimeoutError:
            if process.returncode is None:
                process.kill()
                await process.wait()
            return False, "Timeout: gagal ambil metadata (>30 detik)."

        if process.returncode == 0:
            try:
                lines = stdout.decode().strip().split("\n")
                data = json.loads(lines[-1] if lines else "{}")
                filesize = data.get("filesize") or data.get("filesize_approx") or 524_288_000
                return True, {
                    "title":    data.get("title", "Media_File"),
                    "filesize": filesize,
                    "duration": data.get("duration"),
                    "url":      url,
                    "type":     job_type,
                    "size_human": format_size(filesize),
                }
            except (json.JSONDecodeError, IndexError):
                return False, "Metadata tidak valid dari yt-dlp."

        stderr_text = stderr.decode(errors="ignore")[:200]
        return False, f"URL salah atau tidak didukung. ({stderr_text})"

    except FileNotFoundError:
        return False, "yt-dlp tidak ditemukan di server. Hubungi admin."
    except Exception as exc:
        logger.exception("fetch_media_metadata error")
        return False, str(exc)
    finally:
        if process and process.returncode is None:
            try:
                process.kill()
                await process.wait()
            except Exception:
                pass


# ─── Job Status Polling ────────────────────────────────────────────────────────

async def poll_job_until_done(
    app: Application,
    job_id: str,
    chat_id: int,
    message_id: int,
    safe_title: str,
) -> None:
    """
    Loop poll DB setiap JOB_POLL_INTERVAL detik.
    Update pesan Telegram saat status berubah.
    Berhenti saat done/failed/dead.
    """
    last_status: str = ""
    last_retry: int  = -1
    max_poll_time    = 7200  # 2 jam hard limit per job
    poll_start       = time.time()

    try:
        while True:
            if time.time() - poll_start > max_poll_time:
                await safe_edit(
                    app, chat_id, message_id,
                    "⏰ <b>Timeout monitoring.</b>\nJob mungkin masih berjalan. Cek /status.",
                    reply_markup=_kb_back(),
                )
                break

            job = await db.get_job_status(job_id)
            if not job:
                logger.warning("poll_job: job %s not found in DB", job_id)
                break

            status      = job["status"]
            retry_count = job.get("retry_count", 0)

            # Hanya edit pesan jika ada perubahan
            if status != last_status or retry_count != last_retry:
                last_status = status
                last_retry  = retry_count
                text = _build_status_text(job, safe_title)
                markup = _kb_back() if status in ("done", "failed", "dead") else None
                await safe_edit(app, chat_id, message_id, text, reply_markup=markup)

            if status in ("done", "failed", "dead"):
                break

            await asyncio.sleep(JOB_POLL_INTERVAL)

    except asyncio.CancelledError:
        pass
    except Exception:
        logger.exception("poll_job_until_done error for job %s", job_id)
    finally:
        _active_polls.pop(job_id, None)


def _build_status_text(job: dict, safe_title: str) -> str:
    status      = job["status"]
    retry_count = job.get("retry_count", 0)
    meta        = job.get("result_meta") or {}

    if isinstance(meta, str):
        try:
            meta = json.loads(meta)
        except Exception:
            meta = {}

    if status == "pending":
        return (
            f"⏳ <b>DALAM ANTREAN</b>\n"
            f"🎬 <code>{safe_title}</code>\n"
            f"<i>Worker akan memproses sebentar lagi...</i>"
        )
    elif status == "running":
        retry_text = f"\n🔄 Retry: {retry_count}" if retry_count > 0 else ""
        return (
            f"📥 <b>SEDANG DIPROSES</b>\n"
            f"🎬 <code>{safe_title}</code>\n"
            f"⚙️ <i>Worker sedang mendownload...{retry_text}</i>"
        )
    elif status == "done":
        filename    = safe_html(str(job.get("result_filename", ""))[:50])
        size_human  = safe_html(meta.get("size_human", format_size(job.get("result_size", 0))))
        title       = safe_html(meta.get("title", safe_title)[:40])
        folder_map  = {"video": "Video", "audio": "Music", "tiktok": "TikTok"}
        folder      = folder_map.get(job.get("type", "video"), "Download")
        return (
            f"✅ <b>SELESAI!</b>\n\n"
            f"🎬 <code>{title}</code>\n"
            f"📁 <i>{filename}</i>\n"
            f"💾 {size_human}\n"
            f"📂 Folder: <b>{folder}</b>"
        )
    elif status in ("failed", "dead"):
        err = safe_html(str(job.get("error_msg", "Unknown error"))[:150])
        return (
            f"❌ <b>GAGAL</b> (retry: {retry_count})\n\n"
            f"🎬 <code>{safe_title}</code>\n"
            f"<i>{err}</i>"
        )
    return f"<i>Status: {status}</i>"


# ─── Keyboards ─────────────────────────────────────────────────────────────────

def _kb_main() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📥 DOWNLOADER", callback_data="menu_download")],
        [InlineKeyboardButton("📊 DASHBOARD", callback_data="sys_info"),
         InlineKeyboardButton("📜 HISTORY", callback_data="menu_history")],
        [InlineKeyboardButton("🔍 QUEUE INSPECTOR", callback_data="menu_inspector"),
         InlineKeyboardButton("🛠 UTILITIES", callback_data="menu_util")],
    ])


def _kb_downloader() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📹 YT Video", callback_data="act_yt_video"),
         InlineKeyboardButton("🎵 YT Audio", callback_data="act_yt_audio")],
        [InlineKeyboardButton("📱 TikTok", callback_data="act_tiktok"),
         InlineKeyboardButton("ℹ️ Info URL", callback_data="act_info")],
        [InlineKeyboardButton("🔙 BACK", callback_data="back_main")],
    ])


def _kb_util() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🔐 Password", callback_data="util_pass"),
         InlineKeyboardButton("📱 QR Code", callback_data="act_qr")],
        [InlineKeyboardButton("🔙 BACK", callback_data="back_main")],
    ])


def _kb_confirm() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ LANJUT DOWNLOAD", callback_data="confirm_download")],
        [InlineKeyboardButton("❌ BATAL", callback_data="back_main")],
    ])


def _kb_back() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("🔙 BACK", callback_data="back_main")]])


def _kb_dashboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🔄 REFRESH", callback_data="sys_info_refresh")],
        [InlineKeyboardButton("🔙 BACK", callback_data="back_main")],
    ])


# ─── Dashboard ─────────────────────────────────────────────────────────────────

async def build_dashboard(chat_id: int) -> str:
    try:
        stats      = await db.get_global_stats()
        workers    = await db.get_active_workers(stale_minutes=5)
        recent     = await db.get_recent_history(chat_id, limit=1)
        pending_jobs = await db.get_pending_jobs_for_chat(chat_id, limit=5)
    except Exception:
        logger.exception("build_dashboard error")
        stats, workers, recent, pending_jobs = {}, [], [], []

    worker_text = f"{len(workers)} aktif" if workers else "Tidak ada worker online"

    text = (
        f"📊 <b>CLOUD DASHBOARD</b>\n\n"
        f"⏱ <b>Core Uptime:</b> <code>{get_uptime(START_TIME)}</code>\n"
        f"⚙️ <b>Workers:</b> {worker_text}\n\n"
        f"📈 <b>STATS:</b>\n"
        f"• Total Downloads: <code>{stats.get('total_downloads', 0)}</code>\n"
        f"• Total Data: <code>{format_size(stats.get('total_bytes', 0))}</code>\n"
        f"• Total Gagal: <code>{stats.get('total_failures', 0)}</code>\n"
    )

    if pending_jobs:
        text += f"\n📌 <b>Job kamu ({len(pending_jobs)}):</b>\n"
        for j in pending_jobs:
            text += f"  • <code>{str(j['id'])[:8]}...</code> [{j['status'].upper()}]\n"

    if recent:
        r = recent[0]
        text += (
            f"\n📥 <b>Last Download:</b>\n"
            f"  <code>{safe_html(str(r.get('title', ''))[:35])}</code> "
            f"({format_size(r.get('size_bytes', 0))})"
        )

    return text


# ─── Handlers ──────────────────────────────────────────────────────────────────

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_user.id != MY_ID:
        return
    context.user_data.clear()
    await update.message.reply_text(
        "🤖 <b>BOT ASISTEN — CLOUD EDITION</b>\n<i>Powered by Neon + Koyeb</i>",
        parse_mode="HTML",
        reply_markup=_kb_main(),
    )


async def callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_user.id != MY_ID:
        return

    # Rate limit: 1 aksi per detik
    now = time.time()
    if now - _rate_limits.get(MY_ID, 0.0) < 1.0:
        return
    _rate_limits[MY_ID] = now

    query = update.callback_query
    try:
        await query.answer()
    except Exception:
        pass

    data    = query.data
    app     = context.application
    chat_id = query.message.chat.id
    msg_id  = query.message.message_id

    # ── Navigation ──
    if data == "back_main":
        context.user_data.clear()
        return await safe_edit(app, chat_id, msg_id,
            "🤖 <b>BOT ASISTEN — CLOUD EDITION</b>\n<i>Pilih modul:</i>",
            reply_markup=_kb_main())

    if data == "menu_download":
        return await safe_edit(app, chat_id, msg_id,
            "📥 <b>DOWNLOAD MANAGER</b>", reply_markup=_kb_downloader())

    if data == "menu_util":
        return await safe_edit(app, chat_id, msg_id,
            "🛠 <b>UTILITIES</b>", reply_markup=_kb_util())

    # ── Dashboard ──
    if data in ("sys_info", "sys_info_refresh"):
        if data == "sys_info_refresh":
            try:
                await query.edit_message_text("⏳ <i>Refreshing...</i>", parse_mode="HTML")
            except Exception:
                pass
        text = await build_dashboard(chat_id)
        return await safe_edit(app, chat_id, msg_id, text, reply_markup=_kb_dashboard())

    # ── Queue Inspector ──
    if data == "menu_inspector":
        try:
            jobs = await db.get_pending_jobs_for_chat(chat_id, limit=10)
            workers = await db.get_active_workers(stale_minutes=5)
        except Exception:
            jobs, workers = [], []

        if not jobs and not workers:
            return await safe_edit(app, chat_id, msg_id,
                "ℹ️ <b>Antrean kosong & tidak ada worker aktif.</b>",
                reply_markup=_kb_back())

        text = "🔍 <b>QUEUE INSPECTOR</b>\n\n"
        if workers:
            text += f"⚙️ <b>Workers Aktif ({len(workers)}):</b>\n"
            for w in workers:
                text += f"  • <code>{w['worker_id']}</code> [{w['status'].upper()}]\n"
            text += "\n"
        if jobs:
            text += f"📌 <b>Jobs kamu ({len(jobs)}):</b>\n"
            for j in jobs:
                text += (
                    f"  • <code>{str(j['id'])[:8]}...</code> "
                    f"[{j['status'].upper()}] {j.get('type','')}\n"
                )
        return await safe_edit(app, chat_id, msg_id, text, reply_markup=_kb_back())

    # ── History ──
    if data == "menu_history":
        try:
            hist = await db.get_recent_history(chat_id, limit=10)
        except Exception:
            hist = []

        if not hist:
            return await safe_edit(app, chat_id, msg_id,
                "📜 <b>History kosong.</b>", reply_markup=_kb_back())

        text = "📜 <b>RIWAYAT DOWNLOAD (10 terakhir)</b>\n\n"
        for idx, r in enumerate(hist, 1):
            text += (
                f"{idx}. <code>{safe_html(str(r.get('title',''))[:30])}</code>\n"
                f"   {format_size(r.get('size_bytes',0))} — {r.get('type','')}\n"
            )
        return await safe_edit(app, chat_id, msg_id, text, reply_markup=_kb_back())

    # ── Downloader actions — set state, minta URL ──
    action_map = {
        "act_yt_video": ("yt_video", "📹 <b>YouTube Video</b>\nKirim URL video:"),
        "act_yt_audio": ("yt_audio", "🎵 <b>YouTube Audio (MP3)</b>\nKirim URL video:"),
        "act_tiktok":   ("tiktok",   "📱 <b>TikTok</b>\nKirim URL TikTok:"),
        "act_info":     ("info",     "ℹ️ <b>Info Media</b>\nKirim URL:"),
        "act_qr":       ("qr",       "📱 <b>QR Code</b>\nKirim teks atau URL:"),
    }
    if data in action_map:
        context.user_data.clear()
        action, prompt = action_map[data]
        context.user_data["action"] = action
        return await safe_edit(app, chat_id, msg_id, prompt, reply_markup=_kb_back())

    # ── Utilities ──
    if data == "util_pass":
        pwd = "".join(
            random.choice(string.ascii_letters + string.digits + "!@#$%&*?")
            for _ in range(16)
        )
        return await safe_edit(app, chat_id, msg_id,
            f"🔐 <b>Password Generator:</b>\n\n<code>{safe_html(pwd)}</code>",
            reply_markup=_kb_back())

    # ── Confirm download ──
    if data == "confirm_download":
        meta = context.user_data.get("media_info")
        if not meta:
            return await safe_edit(app, chat_id, msg_id,
                "❌ Sesi habis. Mulai ulang dari menu.", reply_markup=_kb_back())

        url            = meta["url"]
        normalized     = normalize_url(url)
        job_type       = meta["type"]

        # Cek duplikat
        try:
            is_dup = await db.check_duplicate_url(chat_id, normalized)
        except Exception:
            is_dup = False
        if is_dup:
            return await safe_edit(app, chat_id, msg_id,
                "⚠️ <b>DUPLIKAT:</b> URL ini sudah pernah didownload.",
                reply_markup=_kb_back())

        # Insert ke queue
        try:
            job = await db.insert_job(
                chat_id=chat_id,
                message_id=msg_id,
                url=url,
                normalized_url=normalized,
                job_type=job_type,
                result_meta=meta,
            )
        except Exception as exc:
            logger.exception("insert_job failed")
            return await safe_edit(app, chat_id, msg_id,
                f"❌ Gagal memasukkan ke queue: {safe_html(str(exc)[:100])}",
                reply_markup=_kb_back())

        if job is None:
            return await safe_edit(app, chat_id, msg_id,
                "ℹ️ Job ini sudah ada di queue sebelumnya.", reply_markup=_kb_back())

        job_id     = str(job["id"])
        safe_title = safe_html(str(meta.get("title", ""))[:35])
        context.user_data.clear()

        await safe_edit(
            app, chat_id, msg_id,
            f"⏳ <b>MASUK ANTREAN</b>\n"
            f"🎬 <code>{safe_title}</code>\n"
            f"🆔 <code>{job_id[:8]}...</code>\n"
            f"<i>Worker akan segera memproses...</i>",
        )

        # Mulai polling task
        if job_id not in _active_polls:
            task = asyncio.create_task(
                poll_job_until_done(app, job_id, chat_id, msg_id, safe_title)
            )
            _active_polls[job_id] = task
            BACKGROUND_TASKS.add(task)
            task.add_done_callback(BACKGROUND_TASKS.discard)

        return


async def message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_user.id != MY_ID:
        return
    if not update.message or not update.message.text:
        return

    # Rate limit
    now = time.time()
    if now - _rate_limits.get(MY_ID, 0.0) < 1.0:
        return
    _rate_limits[MY_ID] = now

    text   = update.message.text.strip()
    action = context.user_data.get("action")
    app    = context.application

    if not action:
        return await update.message.reply_text(
            "🤖 Gunakan menu.", reply_markup=_kb_main()
        )

    # ── QR Code ──
    if action == "qr":
        if len(text) > 1000:
            return await update.message.reply_text("❌ Max 1000 karakter.")
        try:
            def build_qr():
                qr = qrcode.QRCode(version=None, box_size=10, border=4)
                qr.add_data(text)
                qr.make(fit=True)
                img = qr.make_image(fill_color="black", back_color="white")
                bio = io.BytesIO()
                img.save(bio, "PNG")
                bio.seek(0)
                return bio

            bio = await asyncio.to_thread(build_qr)
            await update.message.reply_photo(
                photo=bio, caption="📱 <b>QR Code</b>", parse_mode="HTML"
            )
            bio.close()
        except Exception:
            logger.exception("QR build failed")
            await update.message.reply_text("❌ Gagal membuat QR.", reply_markup=_kb_back())
        context.user_data.clear()
        return

    # ── Download actions ──
    if action in ("yt_video", "yt_audio", "tiktok", "info"):
        url = normalize_url(text)

        if not is_allowed_domain(url):
            return await update.message.reply_text(
                "❌ Domain tidak didukung. Hanya YouTube & TikTok.",
                reply_markup=_kb_back(),
            )

        msg = await update.message.reply_text(
            "⏳ <i>Mengambil metadata...</i>", parse_mode="HTML"
        )
        type_map = {"yt_video": "video", "yt_audio": "audio",
                    "tiktok": "tiktok", "info": detect_type_from_url(url)}
        job_type = type_map.get(action, "video")

        ok, data = await fetch_media_metadata(url, job_type)

        if not ok:
            await safe_edit(app, msg.chat.id, msg.message_id,
                f"❌ <b>Gagal:</b>\n{safe_html(str(data))}",
                reply_markup=_kb_back())
            context.user_data.clear()
            return

        safe_title = safe_html(str(data["title"])[:40])
        info_text = (
            f"🎬 <b>{safe_title}</b>\n"
            f"⏱ <b>Durasi:</b> {format_duration(data.get('duration'))}\n"
            f"💾 <b>Size:</b> ~{format_size(data.get('filesize', 0))}\n"
            f"🏷 <b>Tipe:</b> {job_type}"
        )

        if action == "info":
            await safe_edit(app, msg.chat.id, msg.message_id,
                f"📊 <b>METADATA:</b>\n\n{info_text}", reply_markup=_kb_back())
            context.user_data.clear()
            return

        context.user_data["media_info"] = data
        await safe_edit(app, msg.chat.id, msg.message_id,
            info_text + "\n\n<i>Lanjut download?</i>",
            reply_markup=_kb_confirm())
        return


# ─── Lifecycle ─────────────────────────────────────────────────────────────────

async def on_startup(app: Application) -> None:
    """Post-init: buka DB pool, jalankan background maintenance."""
    logger.info("Core bot starting up...")

    # Pemanasan DB pool
    try:
        await db.get_pool()
        logger.info("DB pool ready")
    except Exception:
        logger.exception("FATAL: DB pool init failed — check DATABASE_URL")
        raise

    # Jadwal zombie reaper setiap 10 menit
    async def zombie_reaper_loop():
        while True:
            try:
                n = await db.reap_zombie_jobs()
                m = await db.mark_dead_workers()
                if n or m:
                    logger.info("Reaper: %d zombie jobs, %d dead workers", n, m)
            except Exception:
                logger.exception("zombie_reaper_loop error")
            await asyncio.sleep(600)

    # Jadwal prune job lama setiap 12 jam
    async def prune_loop():
        while True:
            try:
                n = await db.prune_old_jobs(days=7)
                if n:
                    logger.info("Pruned %d old jobs", n)
            except Exception:
                logger.exception("prune_loop error")
            await asyncio.sleep(43200)

    fire(zombie_reaper_loop())
    fire(prune_loop())
    logger.info("Core bot startup complete")


async def on_shutdown(app: Application) -> None:
    """Graceful shutdown."""
    logger.info("Core bot shutting down...")

    # Cancel semua polling tasks
    for task in list(_active_polls.values()):
        task.cancel()
    for task in list(BACKGROUND_TASKS):
        task.cancel()
    await asyncio.gather(*BACKGROUND_TASKS, return_exceptions=True)

    await db.close_pool()
    logger.info("Core bot shutdown complete")


# ─── Entry Point ───────────────────────────────────────────────────────────────

def main() -> None:
    if not TOKEN or TOKEN == "GANTI_TOKEN_DI_SINI":
        print("❌ BOT_TOKEN tidak valid!")
        return
    if MY_ID <= 0:
        print("❌ MY_ID tidak valid!")
        return

    req = HTTPXRequest(read_timeout=60, write_timeout=60, connect_timeout=30)
    app = (
        Application.builder()
        .token(TOKEN)
        .request(req)
        .post_init(on_startup)
        .post_shutdown(on_shutdown)
        .build()
    )

    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CallbackQueryHandler(callback_handler))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, message_handler))

    print("=" * 55)
    print("🚀 CORE BOT — CLOUD EDITION (Koyeb)")
    print("=" * 55)
    print("• DB Backend    : Neon PostgreSQL (asyncpg pool)")
    print("• Download Logic: ❌ TIDAK ADA (di worker)")
    print("• Orchestration : ✅ Queue insert + status poll")
    print("• Zombie Reaper : ✅ Aktif (setiap 10 menit)")
    print("• Job Prune     : ✅ Aktif (setiap 12 jam)")
    print("=" * 55)

    try:
        app.run_polling(drop_pending_updates=True)
    except Exception:
        logger.exception("Core bot polling crashed")


if __name__ == "__main__":
    main()
