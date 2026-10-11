"""Integration test: FIFO becomes readable before the simulated API finishes."""
import asyncio
import importlib.util
import logging
import os
from pathlib import Path
import sys
import tempfile
import types

from aiohttp import web

ROOT = Path(__file__).resolve().parents[1]
sys.modules["AloneX"] = types.SimpleNamespace(logger=logging.getLogger("progressive-test"))
os.environ["SHRUTI_API_KEY"] = "test-key"
os.environ["SHRUTI_API_URL"] = "http://127.0.0.1:0"
os.environ["PROGRESSIVE_START_BUFFER"] = "65536"

spec = importlib.util.spec_from_file_location(
    "progressive_under_test", ROOT / "AloneX/helpers/progressive.py"
)
progressive = importlib.util.module_from_spec(spec)
spec.loader.exec_module(progressive)


async def main():
    payload_chunks = [bytes([65 + (i % 20)]) * 32768 for i in range(12)]
    server_finished = asyncio.Event()

    async def download(request):
        response = web.StreamResponse(status=200, headers={"Content-Type": "audio/mpeg"})
        await response.prepare(request)
        for chunk in payload_chunks:
            await response.write(chunk)
            await asyncio.sleep(0.12)
        await response.write_eof()
        server_finished.set()
        return response

    app = web.Application()
    app.router.add_get("/download", download)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    port = site._server.sockets[0].getsockname()[1]
    progressive.API_URL = f"http://127.0.0.1:{port}"

    with tempfile.TemporaryDirectory() as tmp:
        old_cwd = os.getcwd()
        os.chdir(tmp)
        try:
            media = types.SimpleNamespace(id="fixture-track", video=False, file_path=None)
            fifo, task = await progressive.prepare_progressive_audio(media)
            assert fifo and task, "progressive preparation should return a FIFO and task"
            assert not server_finished.is_set(), "helper waited for the full download"

            def consume_pipe(path):
                with open(path, "rb", buffering=0) as stream:
                    return stream.read()

            consumer = asyncio.create_task(asyncio.to_thread(consume_pipe, fifo))
            data = await asyncio.wait_for(consumer, timeout=10)
            await asyncio.wait_for(task, timeout=10)
            assert data == b"".join(payload_chunks), "FIFO stream bytes were corrupted"
            assert server_finished.is_set(), "test server should finish after playback consumer"
            assert Path(media.file_path).is_file(), "completed MP3 cache was not published"
            assert Path(media.file_path).read_bytes() == b"".join(payload_chunks)
            assert not Path(fifo).exists(), "temporary FIFO should be removed"
            print("PASS: FIFO became available before API completion; stream and cache bytes match.")
        finally:
            os.chdir(old_cwd)
    await runner.cleanup()


if __name__ == "__main__":
    asyncio.run(main())
