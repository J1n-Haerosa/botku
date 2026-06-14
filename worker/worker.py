"""
worker.py — Download Worker Service
====================================

TUGAS:
  - Poll Neon PostgreSQL queue setiap X detik
  - Ambil job status = 'pending' dengan SELECT FOR UPDATE SKIP LOCKED
  - Jalankan yt-dlp + ffmpeg
  - Upload hasil ke local storage (atau S3 opsional)
  - Update status job ke database
  - Kirim heartbeat ke worker_heartbeat table

PRINSIP:
  - Stateless — semua state ada di database
  - Multi-worker aman — tidak ada race condition
  - Idempotent — job yang sama tidak akan diproses dua kali
  - Resilient — exponential backoff, zombie detection, graceful shutdown

ENVIRONMENT VARIABLES:
  DATABASE_URL        Neon PostgreSQL DSN
  WORKER_ID           ID unik worker ini (default: hostname-pid)
  WORKER_CONCURRENCY  Jumlah job paralel (default: 1)
  POLL_INTERVAL       Detik antar polling (default: 5)
  DOWNLOAD_DIR        Direktori output lokal (default: /tmp/downloads)
  HEARTBEAT_INTERVAL  Detik antar heartbeat (default: 30)
  YTDLP_TIMEOUT       Timeout yt-dlp dalam detik (default: 3600)
"""

from __future__ import annotations

import asyncio
import logging
import os
import platform
import shutil
import socket
import time
import uuid
from logging.handlers import RotatingFileHandler
from pathlib import Path

import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from shared import database as db
from shared.utils import format_size

# ─── Config ────────────────────────────────────────────────────────────────────

_hostname = socket.gethostname()
_pid      = os.getpid()

WORKER_ID: str = os.getenv(
    "WORKER_ID", f"{_hostname}-{_pid}"
)
WORKER_CONCURRENCY: int  = int(os.getenv("WORKER_CONCURRENCY", "1"))
POLL_INTERVAL: float     = float(os.getenv("POLL_INTERVAL", "5.0"))
HEARTBEAT_INTERVAL: float = float(os.getenv("HEARTBEAT_INTERVAL", "30.0"))
YTDLP_TIMEOUT: int       = int(os.getenv("YTDLP_TIMEOUT", "3600"))

DOWNLOAD_DIR = Path(os.getenv("DOWNLOAD_DIR", "/tmp/downloads"))
DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)

DIR_VIDEO  = DOWNLOAD_DIR / "Video"
DIR_AUDIO  = DOWNLOAD_DIR / "Music"
DIR_TIKTOK = DOWNLOAD_DIR / "TikTok"
for d in [DIR_VIDEO, DIR_AUDIO, DIR_TIKTOK]:
    d.mkdir(parents=True, exist_ok=True)

LOG_FILE = os.getenv("LOG_FILE", "/tmp/worker.log")

ALLOWED_MEDIA_EXT = {".mp4", ".m4a", ".mp3", ".webm", ".mkv", ".aac", ".opus", ".flac", ".wav", ".m4v"}

# Exponential backoff config untuk retry
RETRY_BASE_DELAY = 3.0      # detik
RETRY_MAX_DELAY  = 60.0     # detik

# ─── Logging ───────────────────────────────────────────────────────────────────

logging.basicConfig(
    handlers=[RotatingFileHandler(LOG_FILE, maxBytes=2_000_000, backupCount=3)],
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(f"worker.{WORKER_ID}")

# ─── Worker State ──────────────────────────────────────────────────────────────

_jobs_processed = 0
_jobs_failed    = 0
_is_running     = True        # set False oleh signal handler
_active_semas: asyncio.Semaphore | None = None


# ─── Download Engine ───────────────────────────────────────────────────────────

def _unique_path(path: Path) -> Path:
    """Hindari overwrite file yang sudah ada."""
    if not path.exists():
        return path
    stem, suffix = path.stem, path.suffix
    n = 1
    while True:
        candidate = path.with_name(f"{stem} ({n}){suffix}")
        if not candidate.exists():
            return candidate
        n += 1


async def run_download(job: dict) -> tuple[bool, str, str, int, bool]:
    """
    Jalankan yt-dlp + ffmpeg untuk satu job.

    Return: (success, result_filename, error_msg, file_size_bytes, should_retry)
    """
    job_id   = str(job["id"])
    url      = job["url"]
    job_type = job["type"]
    meta     = job.get("result_meta") or {}
    if isinstance(meta, str):
        import json
        try:
            meta = json.loads(meta)
        except Exception:
            meta = {}

    # Buat temp dir terisolasi per job
    job_dir = DOWNLOAD_DIR / f".temp_job_{job_id[:8]}"
    job_dir.mkdir(parents=True, exist_ok=True)

    target_dir = {
        "video":  DIR_VIDEO,
        "audio":  DIR_AUDIO,
        "tiktok": DIR_TIKTOK,
    }.get(job_type, DIR_VIDEO)

    # ── Bangun command yt-dlp ──
    base_cmd = [
        "yt-dlp",
        "--newline",
        "--no-playlist",
        "--restrict-filenames",
        "-c",
        "--no-warnings",
    ]

    if job_type == "audio":
        cmd = base_cmd + [
            "-x", "--audio-format", "mp3",
            "-o", str(job_dir / "%(title)s.%(ext)s"),
            "--", url,
        ]
    elif job_type == "tiktok":
        cmd = base_cmd + [
            "-o", str(job_dir / "%(title)s.%(ext)s"),
            "--", url,
        ]
    else:   # video
        cmd = base_cmd + [
            "-f", "bestvideo[ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]/best",
            "-o", str(job_dir / "%(title)s.%(ext)s"),
            "--", url,
        ]

    logger.info("[%s] Starting download: type=%s url=%s", job_id[:8], job_type, url)

    process = None
    try:
        process = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
        )

        # Baca output yt-dlp — log progress tanpa blocking
        async def drain_stdout():
            while True:
                line_bytes = await process.stdout.readline()
                if not line_bytes:
                    break
                line = line_bytes.decode("utf-8", errors="ignore").strip()
                if line:
                    logger.debug("[%s] ytdlp: %s", job_id[:8], line)

        try:
            await asyncio.wait_for(
                asyncio.gather(drain_stdout(), process.wait()),
                timeout=YTDLP_TIMEOUT,
            )
        except asyncio.TimeoutError:
            logger.error("[%s] yt-dlp timeout after %ds", job_id[:8], YTDLP_TIMEOUT)
            return False, "", "Timeout yt-dlp melebihi batas waktu.", 0, True

        if process.returncode != 0:
            err = f"yt-dlp exit code {process.returncode}"
            logger.warning("[%s] %s", job_id[:8], err)
            return False, "", err, 0, True

        # ── Temukan file hasil ──
        all_files = list(job_dir.iterdir())
        media_files = [f for f in all_files if f.is_file() and f.suffix.lower() in ALLOWED_MEDIA_EXT]

        if media_files:
            best = max(media_files, key=lambda f: f.stat().st_size)
        elif all_files:
            best = max([f for f in all_files if f.is_file()], key=lambda f: f.stat().st_size, default=None)
            if not best:
                return False, "", "Tidak ada file output.", 0, False
        else:
            return False, "", "Direktori temp kosong setelah download.", 0, False

        file_size = best.stat().st_size
        if file_size == 0:
            return False, "", "File output berukuran 0 byte (file korup).", 0, True

        # ── Pindahkan ke target folder ──
        final_path = _unique_path(target_dir / best.name)
        shutil.move(str(best), str(final_path))

        logger.info(
            "[%s] Done: %s (%s)",
            job_id[:8], final_path.name, format_size(file_size)
        )
        return True, final_path.name, "", file_size, False

    except FileNotFoundError:
        return False, "", "yt-dlp tidak ditemukan. Install dulu.", 0, False
    except Exception as exc:
        logger.exception("[%s] run_download unexpected error", job_id[:8])
        return False, "", str(exc)[:500], 0, True
    finally:
        # Cleanup temp dir selalu
        if process and process.returncode is None:
            try:
                process.kill()
                await process.wait()
            except Exception:
                pass
        try:
            shutil.rmtree(str(job_dir), ignore_errors=True)
        except Exception:
            pass


# ─── Job Processor ─────────────────────────────────────────────────────────────

async def process_job(job: dict) -> None:
    """
    Proses satu job: retry loop + update DB.
    """
    global _jobs_processed, _jobs_failed

    job_id      = str(job["id"])
    retry_count = job.get("retry_count", 0)
    max_retries = job.get("max_retries", 3)
    job_type    = job.get("type", "video")
    meta        = job.get("result_meta") or {}
    if isinstance(meta, str):
        import json
        try:
            meta = json.loads(meta)
        except Exception:
            meta = {}

    title = meta.get("title", "Unknown")
    logger.info("[%s] Processing: '%s' (type=%s, retry=%d/%d)",
                job_id[:8], title[:40], job_type, retry_count, max_retries)

    success, filename, error_msg, file_size, should_retry = await run_download(job)

    if success:
        result_meta = {
            **meta,
            "size_human": format_size(file_size),
            "worker_id": WORKER_ID,
        }
        try:
            await db.complete_job(
                job_id=job_id,
                result_filename=filename,
                result_size=file_size,
                result_meta=result_meta,
            )
            _jobs_processed += 1
            logger.info("[%s] Marked as DONE", job_id[:8])
        except Exception:
            logger.exception("[%s] complete_job DB error", job_id[:8])
    else:
        try:
            final_status = await db.fail_job(
                job_id=job_id,
                error_msg=error_msg,
                should_retry=should_retry,
            )
            _jobs_failed += 1
            logger.warning("[%s] Marked as %s: %s", job_id[:8], final_status.upper(), error_msg[:100])
        except Exception:
            logger.exception("[%s] fail_job DB error", job_id[:8])


# ─── Poll Loop ─────────────────────────────────────────────────────────────────

async def poll_loop(semaphore: asyncio.Semaphore) -> None:
    """
    Main loop: poll DB, ambil job, proses secara concurrent.
    Semaphore membatasi jumlah job paralel = WORKER_CONCURRENCY.
    """
    consecutive_empty = 0
    base_sleep        = POLL_INTERVAL

    while _is_running:
        try:
            # Ambil slot semaphore dulu — jika penuh, tunggu
            # Ini memastikan tidak lebih dari CONCURRENCY job paralel
            async with semaphore:
                jobs = await db.claim_next_job(WORKER_ID, batch_size=1)

            if not jobs:
                # Tidak ada job — adaptive backoff (max 60 detik)
                consecutive_empty += 1
                sleep_time = min(base_sleep * (1 + consecutive_empty * 0.5), 60.0)
                await asyncio.sleep(sleep_time)
                continue

            consecutive_empty = 0
            job = jobs[0]

            # Proses di background agar loop bisa ambil job berikutnya
            async def _run(j):
                async with semaphore:
                    await process_job(j)

            asyncio.create_task(_run(job))

        except asyncio.CancelledError:
            break
        except Exception:
            logger.exception("poll_loop error — akan retry")
            await asyncio.sleep(base_sleep)


# ─── Heartbeat ─────────────────────────────────────────────────────────────────

async def heartbeat_loop() -> None:
    """Kirim heartbeat ke DB setiap HEARTBEAT_INTERVAL detik."""
    while _is_running:
        try:
            await db.upsert_worker_heartbeat(
                worker_id=WORKER_ID,
                hostname=_hostname,
                pid=_pid,
                status="busy" if _jobs_processed > 0 else "idle",
                jobs_processed=_jobs_processed,
                jobs_failed=_jobs_failed,
                metadata={
                    "python": platform.python_version(),
                    "concurrency": WORKER_CONCURRENCY,
                    "download_dir": str(DOWNLOAD_DIR),
                },
            )
        except Exception:
            logger.warning("Heartbeat failed — DB mungkin unreachable")
        await asyncio.sleep(HEARTBEAT_INTERVAL)


# ─── Entry Point ───────────────────────────────────────────────────────────────

async def main() -> None:
    global _is_running

    logger.info("=" * 55)
    logger.info("Worker starting: id=%s concurrency=%d", WORKER_ID, WORKER_CONCURRENCY)
    logger.info("Download dir: %s", DOWNLOAD_DIR)
    logger.info("=" * 55)

    # Validasi environment
    if not os.environ.get("DATABASE_URL"):
        logger.critical("DATABASE_URL tidak di-set!")
        return

    # Validasi yt-dlp & ffmpeg
    if not shutil.which("yt-dlp"):
        logger.critical("yt-dlp tidak ditemukan. Install: pip install yt-dlp")
        return
    if not shutil.which("ffmpeg"):
        logger.warning("ffmpeg tidak ditemukan — audio conversion mungkin gagal")

    # Init DB pool
    try:
        await db.get_pool()
        logger.info("DB pool ready")
    except Exception:
        logger.exception("FATAL: DB pool init failed")
        return

    # Release stale locks dari sesi sebelumnya (jika worker crash)
    try:
        await db.release_stale_locks(WORKER_ID)
    except Exception:
        logger.warning("release_stale_locks failed (non-fatal)")

    # Kirim heartbeat awal
    try:
        await db.upsert_worker_heartbeat(
            worker_id=WORKER_ID,
            hostname=_hostname,
            pid=_pid,
            status="idle",
            metadata={"started_at": time.time()},
        )
    except Exception:
        logger.warning("Initial heartbeat failed")

    semaphore = asyncio.Semaphore(WORKER_CONCURRENCY)

    # Shutdown handler (SIGTERM / SIGINT)
    import signal

    def _handle_signal(sig, _frame):
        global _is_running
        logger.info("Received signal %s — graceful shutdown...", sig)
        _is_running = False

    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, _handle_signal)

    # Jalankan poll + heartbeat loop secara concurrent
    try:
        await asyncio.gather(
            poll_loop(semaphore),
            heartbeat_loop(),
        )
    except asyncio.CancelledError:
        pass
    finally:
        # Release semua lock yang dipegang saat shutdown
        try:
            await db.release_stale_locks(WORKER_ID)
        except Exception:
            pass

        # Update status worker ke idle/dead
        try:
            await db.upsert_worker_heartbeat(
                worker_id=WORKER_ID,
                hostname=_hostname,
                pid=_pid,
                status="dead",
                jobs_processed=_jobs_processed,
                jobs_failed=_jobs_failed,
            )
        except Exception:
            pass

        await db.close_pool()
        logger.info("Worker %s shutdown complete. Processed: %d, Failed: %d",
                    WORKER_ID, _jobs_processed, _jobs_failed)


if __name__ == "__main__":
    asyncio.run(main())
