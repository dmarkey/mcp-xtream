"""End to end: detached worker process against a fake Xtream server."""

import json
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import ClassVar

import pytest

from mcp_xtream import downloader

PACKETS = 20_000
BODY = b"".join(bytes([0x47, i % 256]) + bytes(186) for i in range(PACKETS))
PAYLOAD = b"\x99" * 92 + BODY


class _FakeXtream(BaseHTTPRequestHandler):
    paths: ClassVar[list[str]] = []
    slow = False

    def do_GET(self):
        type(self).paths.append(self.path)
        if self.path.startswith("/player_api.php"):
            body = json.dumps({
                "user_info": {"auth": 1, "status": "Active", "max_connections": "1", "active_cons": "0"},
                "server_info": {"timezone": "Europe/Amsterdam",
                                "time_now": "2026-10-18 13:02:52", "timestamp_now": 1792321372},
            }).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        self.send_response(200)
        self.send_header("Content-Length", str(len(PAYLOAD)))
        self.end_headers()
        step = 64 * 1024
        try:
            for i in range(0, len(PAYLOAD), step):
                self.wfile.write(PAYLOAD[i : i + step])
                if type(self).slow:
                    time.sleep(0.2)
        except (BrokenPipeError, ConnectionResetError):
            pass

    def log_message(self, *args):
        pass


@pytest.fixture
def fake_server(tmp_path, monkeypatch):
    _FakeXtream.paths = []
    server = ThreadingHTTPServer(("127.0.0.1", 0), _FakeXtream)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    # Inherited by the spawned worker process.
    monkeypatch.setenv("XTREAM_SERVER", f"http://127.0.0.1:{server.server_address[1]}")
    monkeypatch.setenv("XTREAM_USERNAME", "user")
    monkeypatch.setenv("XTREAM_PASSWORD", "pass")
    monkeypatch.setenv("XTREAM_CACHE_DIR", str(tmp_path / "cache"))
    monkeypatch.setenv("XTREAM_DOWNLOAD_DIR", str(tmp_path / "downloads"))
    yield _FakeXtream
    server.shutdown()


def _new_job(fmt="ts"):
    # 2026-10-18 09:00 UTC -> 11:00 on the Amsterdam server.
    with downloader.spawn_lock():
        return downloader.start_job(
            stream_id=524967, channel_name="BBC One NI", title="Sunday Politics",
            start=1792314000, stop=1792315800, fmt=fmt, server_tz=None, server_offset=None,
        )


def _wait(job_id, states, timeout=20):
    deadline = time.time() + timeout
    while time.time() < deadline:
        job = downloader.refresh_state(downloader.read_job(job_id))
        if job["state"] in states:
            return job
        time.sleep(0.1)
    raise AssertionError(f"job stuck in {job['state']}: {job}")


def test_detached_download_completes(fake_server):
    job = _new_job()
    done = _wait(job["id"], {"done", "failed"})
    assert done["state"] == "done", done.get("error")
    assert Path(done["output"]).read_bytes() == BODY
    assert any(p == "/timeshift/user/pass/30/2026-10-18:11-00/524967.ts" for p in fake_server.paths)
    assert not Path(done["part"]).exists()


def test_cancel_removes_partial(fake_server):
    fake_server.slow = True
    try:
        job = _new_job()
        _wait(job["id"], {"downloading"})
        time.sleep(0.5)
        assert downloader.active_job()["id"] == job["id"]
        downloader.cancel_job(job["id"])
        cancelled = _wait(job["id"], {"cancelled", "failed"})
    finally:
        fake_server.slow = False
    assert cancelled["state"] == "cancelled"
    assert not Path(cancelled["part"]).exists()
    assert downloader.active_job() is None
