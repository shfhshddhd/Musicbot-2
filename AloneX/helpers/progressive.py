"""Progressive audio pipe for starting playback before the API download finishes.

Audio only: video, seek, and unsupported environments keep the established downloader.
The completed MP3 is atomically moved into the normal cache after the stream ends.
"""
import asyncio
import os
from pathlib import Path
import tempfile

import aiohttp

from AloneX import logger

API_URL = os.environ.get("SHRUTI_API_URL", "https://api01.shrutibots.site").rstrip("/")
API_KEY = os.environ.get("SHRUTI_API_KEY", "")
DOWNLOAD_DIR = Path("downloads")
START_BUFFER_BYTES = max(64 * 1024, int(os.environ.get("PROGRESSIVE_START_BUFFER", str(256 * 1024))))
_active_tasks: set[asyncio.Task] = set()


def _keep_task(task: asyncio.Task) -> None:
    _active_tasks.add(task)
    task.add_done_callback(_active_tasks.discard)


async def prepare_progressive_audio(media):
    """Return (fifo_path, task) once enough bytes are buffered, or (None, None).

    The returned FIFO is only intended for an initial, audio-only play. The
    producer continues downloading into a temporary file while FFmpeg reads
    the FIFO, then atomically publishes the complete MP3 cache.
    """
    if getattr(media, "video", False) or os.name != "posix" or not hasattr(os, "mkfifo"):
        return None, None
    if not API_KEY:
        logger.info("[progressive] SHRUTI_API_KEY missing; using regular download")
        return None, None

    DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
    video_id = str(media.id)
    cache_path = DOWNLOAD_DIR / f"{video_id}.mp3"
    if cache_path.is_file() and cache_path.stat().st_size > 0:
        media.file_path = str(cache_path)
        return None, None

    fifo_path = DOWNLOAD_DIR / f".progressive_{os.getpid()}_{video_id}.mp3"
    partial_path = DOWNLOAD_DIR / f".{video_id}.{os.getpid()}.part"
    for stale in (fifo_path, partial_path):
        try:
            stale.unlink()
        except FileNotFoundError:
            pass
    try:
        os.mkfifo(fifo_path, 0o600)
    except OSError as exc:
        logger.warning("[progressive] FIFO unavailable; regular download fallback: %s", exc)
        return None, None

    ready = asyncio.Event()
    failed = asyncio.Event()
    state = {"error": None, "started": False}

    async def produce():
        buffered = []
        buffered_size = 0
        total = 0
        writer = None
        try:
            timeout = aiohttp.ClientTimeout(total=300, connect=20, sock_read=60)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(
                    f"{API_URL}/download",
                    params={"url": video_id, "type": "audio", "api_key": API_KEY},
                ) as response:
                    if response.status != 200:
                        raise RuntimeError(f"download API returned HTTP {response.status}")
                    with partial_path.open("wb") as cache_file:
                        async for chunk in response.content.iter_chunked(64 * 1024):
                            if not chunk:
                                continue
                            cache_file.write(chunk)
                            cache_file.flush()
                            total += len(chunk)
                            if not state["started"]:
                                buffered.append(chunk)
                                buffered_size += len(chunk)
                                if buffered_size < START_BUFFER_BYTES:
                                    continue
                                ready.set()
                                # Opening a FIFO writer blocks until FFmpeg opens
                                # its reader. Move that blocking open off the loop.
                                writer = await asyncio.to_thread(open, fifo_path, "wb", buffering=0)
                                state["started"] = True
                                try:
                                    os.unlink(fifo_path)
                                except FileNotFoundError:
                                    pass
                                for item in buffered:
                                    await asyncio.to_thread(writer.write, item)
                                buffered.clear()
                            else:
                                await asyncio.to_thread(writer.write, chunk)
                        cache_file.flush()
                        os.fsync(cache_file.fileno())

            if not state["started"]:
                # Very short files still need enough bytes to be useful; don't
                # start the player on an empty/undersized stream.
                if total >= 16 * 1024:
                    ready.set()
                    writer = await asyncio.to_thread(open, fifo_path, "wb", buffering=0)
                    state["started"] = True
                    try:
                        os.unlink(fifo_path)
                    except FileNotFoundError:
                        pass
                    for item in buffered:
                        await asyncio.to_thread(writer.write, item)
                else:
                    raise RuntimeError(f"audio response too small ({total} bytes)")

            if total <= 0 or not partial_path.is_file():
                raise RuntimeError("download API returned an empty audio file")
            os.replace(partial_path, cache_path)
            media.file_path = str(cache_path)
            logger.info("[progressive] download complete: %s (%s bytes)", video_id, total)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            state["error"] = exc
            failed.set()
            logger.warning("[progressive] stream failed for %s: %s", video_id, exc)
        finally:
            if writer is not None:
                try:
                    await asyncio.to_thread(writer.close)
                except Exception:
                    pass
            try:
                partial_path.unlink()
            except FileNotFoundError:
                pass
            if not state["started"]:
                try:
                    fifo_path.unlink()
                except FileNotFoundError:
                    pass

    task = asyncio.create_task(produce(), name=f"progressive-audio-{video_id}")
    _keep_task(task)
    # Wait for the startup buffer or an early API error, but never block on the
    # entire download. A timeout falls back to the known-good full downloader.
    try:
        await asyncio.wait_for(
            asyncio.gather(ready.wait(), failed.wait(), return_exceptions=True),
            timeout=25,
        )
    except asyncio.TimeoutError:
        logger.warning("[progressive] startup buffer timeout for %s; using regular download", video_id)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        return None, None

    if failed.is_set() or not ready.is_set():
        try:
            await task
        except asyncio.CancelledError:
            pass
        return None, None
    return str(fifo_path), task
