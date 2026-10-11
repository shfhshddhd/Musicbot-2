"""Shared download/cache helpers for reliable playback and queue prefetching."""

import asyncio
from pathlib import Path

from AloneX import logger, yt

_download_locks: dict[tuple[str, bool], asyncio.Lock] = {}


def _valid_file(path: str | None) -> bool:
    if not path:
        return False
    try:
        candidate = Path(path)
        return candidate.is_file() and candidate.stat().st_size > 0
    except (OSError, TypeError, ValueError):
        return False


async def download_track(media):
    """Return a complete local file, coalescing concurrent downloads per track."""
    if _valid_file(media.file_path):
        return media.file_path

    key = (str(media.id), bool(media.video))
    lock = _download_locks.setdefault(key, asyncio.Lock())
    async with lock:
        if _valid_file(media.file_path):
            return media.file_path

        try:
            result = await yt.download(media.id, video=media.video)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Download failed for track %s", media.id)
            return None

        if _valid_file(result):
            media.file_path = str(result)
            return media.file_path

        logger.warning("Downloader returned no complete file for track %s", media.id)
        return None
