"""
utils.py — Shared utilities untuk Core Bot dan Worker
URL normalization, formatting, domain validation
"""

from __future__ import annotations

import html
import time
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

# ─── Domain Config ─────────────────────────────────────────────────────────────

ALLOWED_DOMAINS: set[str] = {
    "youtube.com",
    "youtu.be",
    "tiktok.com",
    "youtube-nocookie.com",
    "www.youtube.com",
    "www.tiktok.com",
    "m.youtube.com",
    "vm.tiktok.com",
}

# Query params YouTube yang dipertahankan setelah normalisasi
YOUTUBE_KEEP_PARAMS: set[str] = {"v", "shorts"}


# ─── URL Utils ─────────────────────────────────────────────────────────────────

def is_allowed_domain(url: str) -> bool:
    try:
        host = urlparse(url).netloc.lower().lstrip("www.")
        return any(host == d or host.endswith("." + d) for d in ALLOWED_DOMAINS)
    except Exception:
        return False


def normalize_url(url: str) -> str:
    """
    Normalisasi URL:
    - Strip whitespace
    - Hapus semua tracking params YouTube (si=, pp=, feature=, t=, ab_channel=, dll)
    - Hanya pertahankan param 'v' untuk youtube.com
    - youtu.be: pertahankan hanya path (video ID)
    - TikTok: hapus semua query string
    """
    url = url.strip()
    try:
        parsed = urlparse(url)
        host = parsed.netloc.lower()

        if "youtu.be" in host:
            return urlunparse((parsed.scheme, parsed.netloc, parsed.path, "", "", ""))

        if "youtube.com" in host or "youtube-nocookie.com" in host:
            params = parse_qs(parsed.query, keep_blank_values=False)
            clean_params = {k: v for k, v in params.items() if k in YOUTUBE_KEEP_PARAMS}
            new_query = urlencode({k: v[0] for k, v in clean_params.items()})
            return urlunparse((parsed.scheme, parsed.netloc, parsed.path, "", new_query, ""))

        if "tiktok.com" in host:
            return urlunparse((parsed.scheme, parsed.netloc, parsed.path, "", "", ""))

    except Exception:
        pass

    # Fallback: strip list param
    url = url.split("&list=")[0].split("?list=")[0]
    return url


def detect_type_from_url(url: str) -> str:
    """Deteksi tipe media dari URL (fallback jika user tidak eksplisit pilih)."""
    host = urlparse(url).netloc.lower()
    if "tiktok.com" in host:
        return "tiktok"
    return "video"


# ─── Text Formatting ───────────────────────────────────────────────────────────

def safe_html(text: str) -> str:
    return html.escape(str(text))


def format_size(size_bytes: int | None) -> str:
    if not size_bytes:
        return "0 B"
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if size_bytes < 1024.0:
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024.0
    return f"{size_bytes:.2f} PB"


def format_duration(seconds: int | None) -> str:
    if not seconds:
        return "Unknown"
    return time.strftime("%H:%M:%S", time.gmtime(seconds))


def get_uptime(start_time: float) -> str:
    elapsed = int(time.time() - start_time)
    days, rem = divmod(elapsed, 86400)
    hours, rem = divmod(rem, 3600)
    minutes, seconds = divmod(rem, 60)
    return f"{days}d {hours}h {minutes}m {seconds}s"
