"""
database.py — Neon PostgreSQL Connector
Dipakai bersama oleh core_bot.py dan worker.py

Features:
- Async connection pool via asyncpg
- Context manager untuk transaction
- SELECT ... FOR UPDATE SKIP LOCKED (worker job locking)
- Connection retry + exponential backoff
- Zombie job detection
- Prepared statement helpers
"""

from __future__ import annotations

import asyncio
import logging
import os
import time
from contextlib import asynccontextmanager
from typing import Any, Optional
from uuid import UUID

import asyncpg

logger = logging.getLogger(__name__)

# ─── Config ───────────────────────────────────────────────────────────────────

DATABASE_URL: str = os.environ["DATABASE_URL"]   # wajib di-set via env

# Pool sizing — sesuaikan dengan tier Neon kamu
POOL_MIN_SIZE: int  = int(os.getenv("DB_POOL_MIN", "2"))
POOL_MAX_SIZE: int  = int(os.getenv("DB_POOL_MAX", "10"))

# Timeout untuk acquire connection dari pool
POOL_ACQUIRE_TIMEOUT: float = float(os.getenv("DB_ACQUIRE_TIMEOUT", "10.0"))

# Zombie job: job "running" lebih dari X menit dianggap mati
ZOMBIE_LOCK_TIMEOUT_MINUTES: int = int(os.getenv("ZOMBIE_TIMEOUT_MIN", "90"))


# ─── Pool Singleton ────────────────────────────────────────────────────────────

_pool: Optional[asyncpg.Pool] = None
_pool_lock = asyncio.Lock()


async def get_pool() -> asyncpg.Pool:
    """Return singleton connection pool, buat jika belum ada."""
    global _pool
    if _pool is not None:
        return _pool

    async with _pool_lock:
        if _pool is not None:   # double-check setelah lock
            return _pool

        for attempt in range(1, 6):
            try:
                _pool = await asyncpg.create_pool(
                    dsn=DATABASE_URL,
                    min_size=POOL_MIN_SIZE,
                    max_size=POOL_MAX_SIZE,
                    command_timeout=30,
                    # ssl wajib untuk Neon
                    ssl="require",
                )
                logger.info("DB pool created (min=%d, max=%d)", POOL_MIN_SIZE, POOL_MAX_SIZE)
                return _pool
            except Exception as exc:
                wait = 2 ** attempt
                logger.warning("DB pool init attempt %d failed: %s — retry in %ds", attempt, exc, wait)
                if attempt == 5:
                    raise
                await asyncio.sleep(wait)

    raise RuntimeError("DB pool could not be created")  # unreachable tapi memuaskan type checker


async def close_pool() -> None:
    """Tutup pool saat shutdown."""
    global _pool
    if _pool:
        await _pool.close()
        _pool = None
        logger.info("DB pool closed")


# ─── Low-level helpers ─────────────────────────────────────────────────────────

@asynccontextmanager
async def acquire():
    """Context manager: acquire satu koneksi dari pool."""
    pool = await get_pool()
    async with pool.acquire(timeout=POOL_ACQUIRE_TIMEOUT) as conn:
        yield conn


@asynccontextmanager
async def transaction():
    """Context manager: acquire koneksi + buka transaction."""
    async with acquire() as conn:
        async with conn.transaction():
            yield conn


# ─── Queue CRUD ────────────────────────────────────────────────────────────────

async def insert_job(
    chat_id: int,
    message_id: int,
    url: str,
    normalized_url: str,
    job_type: str,
    result_meta: dict,
) -> Optional[dict]:
    """
    Insert job baru ke queue.
    Return row jika berhasil, None jika duplicate (ON CONFLICT DO NOTHING).
    """
    async with acquire() as conn:
        row = await conn.fetchrow(
            """
            INSERT INTO queue (chat_id, message_id, url, normalized_url, type, result_meta)
            VALUES ($1, $2, $3, $4, $5, $6::jsonb)
            ON CONFLICT (chat_id, normalized_url) DO NOTHING
            RETURNING *
            """,
            chat_id, message_id, url, normalized_url, job_type,
            # asyncpg butuh dict → json string untuk JSONB
            _encode_json(result_meta),
        )
        return dict(row) if row else None


async def get_job_status(job_id: str) -> Optional[dict]:
    """Ambil status + metadata satu job by UUID."""
    async with acquire() as conn:
        row = await conn.fetchrow(
            "SELECT * FROM queue WHERE id = $1",
            UUID(job_id),
        )
        return dict(row) if row else None


async def get_pending_jobs_for_chat(chat_id: int, limit: int = 10) -> list[dict]:
    """Core: list job pending/running untuk satu chat_id (polling status)."""
    async with acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT id, status, type, url, error_msg, result_filename, result_meta, updated_at
            FROM queue
            WHERE chat_id = $1
              AND status IN ('pending', 'running')
            ORDER BY created_at ASC
            LIMIT $2
            """,
            chat_id, limit,
        )
        return [dict(r) for r in rows]


# ─── Worker Job Locking ─────────────────────────────────────────────────────────

async def claim_next_job(worker_id: str, batch_size: int = 1) -> list[dict]:
    """
    Worker: ambil dan lock job pending menggunakan
    SELECT ... FOR UPDATE SKIP LOCKED — atomic, aman multi-worker.

    Return list job yang berhasil di-claim (max batch_size).
    """
    async with transaction() as conn:
        rows = await conn.fetch(
            """
            SELECT *
            FROM queue
            WHERE status = 'pending'
              AND retry_count < max_retries
            ORDER BY created_at ASC
            LIMIT $1
            FOR UPDATE SKIP LOCKED
            """,
            batch_size,
        )
        if not rows:
            return []

        job_ids = [r["id"] for r in rows]
        now = asyncio.get_event_loop().time()

        # Lock semua job yang diklaim sekaligus
        await conn.execute(
            """
            UPDATE queue
            SET status    = 'running',
                locked_by = $1,
                lock_time = NOW()
            WHERE id = ANY($2::uuid[])
            """,
            worker_id, job_ids,
        )
        return [dict(r) for r in rows]


async def complete_job(
    job_id: str,
    result_filename: str,
    result_size: int,
    result_meta: dict,
) -> None:
    """Worker: tandai job selesai, simpan hasil."""
    async with acquire() as conn:
        await conn.execute(
            """
            UPDATE queue
            SET status          = 'done',
                result_filename = $2,
                result_size     = $3,
                result_meta     = $4::jsonb,
                locked_by       = NULL,
                lock_time       = NULL
            WHERE id = $1
            """,
            UUID(job_id), result_filename, result_size,
            _encode_json(result_meta),
        )


async def fail_job(
    job_id: str,
    error_msg: str,
    should_retry: bool = True,
) -> str:
    """
    Worker: tandai job gagal.
    - Jika retry_count < max_retries DAN should_retry=True → status kembali 'pending'
    - Jika tidak → status 'failed'
    Return status akhir job.
    """
    async with acquire() as conn:
        row = await conn.fetchrow(
            """
            UPDATE queue
            SET retry_count = retry_count + 1,
                error_msg   = $2,
                locked_by   = NULL,
                lock_time   = NULL,
                status      = CASE
                    WHEN $3 = TRUE AND (retry_count + 1) < max_retries THEN 'pending'
                    ELSE 'failed'
                END
            WHERE id = $1
            RETURNING status, retry_count
            """,
            UUID(job_id), error_msg[:1000], should_retry,
        )
        return row["status"] if row else "failed"


async def release_stale_locks(worker_id: str) -> None:
    """
    Worker: saat startup/shutdown, lepas semua lock milik worker ini
    agar job tidak zombie selamanya.
    """
    async with acquire() as conn:
        released = await conn.execute(
            """
            UPDATE queue
            SET status    = 'pending',
                locked_by = NULL,
                lock_time = NULL,
                error_msg = 'Released: worker restarted'
            WHERE locked_by = $1
              AND status = 'running'
            """,
            worker_id,
        )
        count = int(released.split()[-1])
        if count:
            logger.info("Released %d stale locks owned by %s", count, worker_id)


# ─── Zombie Job Reaper ──────────────────────────────────────────────────────────

async def reap_zombie_jobs() -> int:
    """
    Jadwal periodik: job 'running' yang lock_time-nya sudah terlalu lama
    dikembalikan ke 'pending' (jika masih bisa retry) atau 'dead'.
    Return jumlah job yang di-reap.
    """
    async with acquire() as conn:
        result = await conn.execute(
            """
            UPDATE queue
            SET status    = CASE
                                WHEN retry_count < max_retries THEN 'pending'
                                ELSE 'dead'
                            END,
                locked_by = NULL,
                lock_time = NULL,
                error_msg = COALESCE(error_msg, '') || ' [ZOMBIE: lock expired]'
            WHERE status = 'running'
              AND lock_time < NOW() - ($1 || ' minutes')::INTERVAL
            """,
            str(ZOMBIE_LOCK_TIMEOUT_MINUTES),
        )
        count = int(result.split()[-1])
        if count:
            logger.warning("Reaped %d zombie jobs", count)
        return count


# ─── History ───────────────────────────────────────────────────────────────────

async def get_recent_history(chat_id: int, limit: int = 10) -> list[dict]:
    async with acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT title, url, type, size_bytes, size_human, duration_sec, created_at
            FROM history
            WHERE chat_id = $1
            ORDER BY created_at DESC
            LIMIT $2
            """,
            chat_id, limit,
        )
        return [dict(r) for r in rows]


async def check_duplicate_url(chat_id: int, normalized_url: str) -> bool:
    """True jika URL sudah pernah di-download sukses oleh chat ini."""
    async with acquire() as conn:
        row = await conn.fetchrow(
            "SELECT 1 FROM history WHERE chat_id = $1 AND normalized_url = $2 LIMIT 1",
            chat_id, normalized_url,
        )
        return row is not None


# ─── Stats ─────────────────────────────────────────────────────────────────────

async def get_global_stats() -> dict:
    async with acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM stats WHERE id = 1")
        return dict(row) if row else {}


# ─── Worker Heartbeat ───────────────────────────────────────────────────────────

async def upsert_worker_heartbeat(
    worker_id: str,
    hostname: str,
    pid: int,
    status: str,
    jobs_processed: int = 0,
    jobs_failed: int = 0,
    metadata: Optional[dict] = None,
) -> None:
    async with acquire() as conn:
        await conn.execute(
            """
            INSERT INTO worker_heartbeat
                (worker_id, hostname, pid, status, jobs_processed, jobs_failed, last_heartbeat, metadata)
            VALUES ($1, $2, $3, $4, $5, $6, NOW(), $7::jsonb)
            ON CONFLICT (worker_id) DO UPDATE SET
                hostname       = EXCLUDED.hostname,
                pid            = EXCLUDED.pid,
                status         = EXCLUDED.status,
                jobs_processed = EXCLUDED.jobs_processed,
                jobs_failed    = EXCLUDED.jobs_failed,
                last_heartbeat = NOW(),
                metadata       = EXCLUDED.metadata
            """,
            worker_id, hostname, pid, status,
            jobs_processed, jobs_failed,
            _encode_json(metadata or {}),
        )


async def get_active_workers(stale_minutes: int = 5) -> list[dict]:
    """Return worker yang heartbeat-nya masih fresh."""
    async with acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT *
            FROM worker_heartbeat
            WHERE last_heartbeat > NOW() - ($1 || ' minutes')::INTERVAL
            ORDER BY last_heartbeat DESC
            """,
            str(stale_minutes),
        )
        return [dict(r) for r in rows]


async def mark_dead_workers(stale_minutes: int = 10) -> int:
    """Tandai worker yang tidak heartbeat sebagai 'dead'."""
    async with acquire() as conn:
        result = await conn.execute(
            """
            UPDATE worker_heartbeat
            SET status = 'dead'
            WHERE last_heartbeat < NOW() - ($1 || ' minutes')::INTERVAL
              AND status != 'dead'
            """,
            str(stale_minutes),
        )
        return int(result.split()[-1])


# ─── Maintenance ──────────────────────────────────────────────────────────────

async def prune_old_jobs(days: int = 7) -> int:
    """Hapus job done/failed/dead yang lebih dari N hari."""
    async with acquire() as conn:
        result = await conn.execute(
            """
            DELETE FROM queue
            WHERE status IN ('done', 'failed', 'dead')
              AND updated_at < NOW() - ($1 || ' days')::INTERVAL
            """,
            str(days),
        )
        return int(result.split()[-1])


# ─── Internals ────────────────────────────────────────────────────────────────

def _encode_json(data: dict) -> str:
    """asyncpg perlu string JSON untuk kolom JSONB."""
    import json
    return json.dumps(data, default=str)
