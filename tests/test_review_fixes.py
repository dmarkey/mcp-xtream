"""Regression tests for the issues agreed in the three-reviewer review (U1–U17)."""

import os
import signal
import subprocess
import sys
import threading
import time
from datetime import UTC, datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import ClassVar

import httpx
import pytest

from mcp_xtream import client as client_mod
from mcp_xtream import data, downloader, timeutil
from mcp_xtream.client import XtreamClient, XtreamError
from mcp_xtream.config import Credentials

NASTY = "p@ss word&1/#?%"
PACKET = lambda i: bytes([0x47, i % 256]) + bytes(186)
BODY = b"".join(PACKET(i) for i in range(12_000))  # ~2.2 MB
PAYLOAD = b"\x99" * 92 + BODY


@pytest.fixture(autouse=True)
def isolated(tmp_path, monkeypatch):
    monkeypatch.setenv("XTREAM_CACHE_DIR", str(tmp_path / "cache"))
    monkeypatch.setenv("XTREAM_DOWNLOAD_DIR", str(tmp_path / "downloads"))
    timeutil.local_tz.cache_clear()
    yield
    timeutil.local_tz.cache_clear()


class _Server(BaseHTTPRequestHandler):
    """Fake Xtream server; behaviour switched by class attributes."""

    protocol_version = "HTTP/1.1"
    xmltv_status = 200
    xmltv_body = b""
    slow = 0.0
    no_length = False
    drop_first_at: int | None = None
    paths: ClassVar[list[str]] = []
    ranges: ClassVar[list[str]] = []

    def _send(self, status, body=b"", headers=()):
        self.send_response(status)
        for k, v in headers:
            self.send_header(k, v)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        cls = type(self)
        cls.paths.append(self.path)
        if self.path.startswith("/player_api.php"):
            body = (b'{"user_info":{"auth":1,"status":"Active","max_connections":"1",'
                    b'"active_cons":"0"},"server_info":{}}')
            return self._send(200, body)
        if self.path.startswith("/xmltv.php"):
            return self._send(cls.xmltv_status, cls.xmltv_body)
        rng = self.headers.get("Range")
        if rng:
            cls.ranges.append(rng)
        start = int(rng.split("=")[1].rstrip("-")) if rng else 0
        if start >= len(PAYLOAD):
            return self._send(416)
        data_ = PAYLOAD[start:]
        if rng:
            self.send_response(206)
            self.send_header("Content-Range", f"bytes {start}-{len(PAYLOAD) - 1}/{len(PAYLOAD)}")
        else:
            self.send_response(200)
        if cls.no_length:
            self.send_header("Connection", "close")
        else:
            self.send_header("Content-Length", str(len(data_)))
        self.end_headers()
        if cls.drop_first_at is not None and not rng:
            cut, cls.drop_first_at = cls.drop_first_at, None
            data_ = data_[:cut]
        try:
            for i in range(0, len(data_), 64 * 1024):
                self.wfile.write(data_[i : i + 64 * 1024])
                if cls.slow:
                    time.sleep(cls.slow)
        except (BrokenPipeError, ConnectionResetError):
            pass
        if cls.no_length:
            self.close_connection = True

    def log_message(self, *args):
        pass


@pytest.fixture
def server(monkeypatch):
    for attr, value in {"xmltv_status": 200, "xmltv_body": b"", "slow": 0.0,
                        "no_length": False, "drop_first_at": None}.items():
        setattr(_Server, attr, value)
    _Server.paths = []
    _Server.ranges = []
    srv = ThreadingHTTPServer(("127.0.0.1", 0), _Server)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    url = f"http://127.0.0.1:{srv.server_address[1]}"
    monkeypatch.setenv("XTREAM_SERVER", url)
    monkeypatch.setenv("XTREAM_USERNAME", "user")
    monkeypatch.setenv("XTREAM_PASSWORD", "pass")
    yield url
    srv.shutdown()


def _job_kwargs(**over):
    kw = {"stream_id": 1, "channel_name": "Chan", "title": "Show", "start": 1792314000,
          "stop": 1792315800, "fmt": "ts", "server_tz": None, "server_offset": None}
    kw.update(over)
    return kw


def _start(**over):
    with downloader.spawn_lock():
        return downloader.start_job(**_job_kwargs(**over))


def _wait(job_id, states, timeout=20):
    deadline = time.time() + timeout
    job = None
    while time.time() < deadline:
        job = downloader.refresh_state(downloader.read_job(job_id))
        if job["state"] in states:
            return job
        time.sleep(0.05)
    raise AssertionError(f"stuck: {job}")


# U1 ---------------------------------------------------------------------------


def test_u1_guide_http_error_never_contains_password(server):
    _Server.xmltv_status = 503
    creds = Credentials(server, "user", NASTY, "test")
    with pytest.raises(XtreamError) as exc:
        XtreamClient(creds).xmltv()
    msg = str(exc.value)
    assert "503" in msg and "password" not in msg and "http" not in msg
    assert "p%40ss" not in msg and "p@ss" not in msg


def test_u1_redact_covers_encoded_forms():
    creds = Credentials("http://s", "u", NASTY, "test")
    from urllib.parse import quote, quote_plus

    text = f"a {NASTY} b {quote(NASTY)} c {quote(NASTY, safe='')} d {quote_plus(NASTY)}"
    assert "p@ss" not in creds.redact(text) and "p%40ss" not in creds.redact(text)


# U2 ---------------------------------------------------------------------------


def test_u2_catchup_url_escapes_credentials():
    creds = Credentials("http://s", "us/er", NASTY, "test")
    url = XtreamClient(creds).catchup_url(42, 1792314000, 30, "Europe/Amsterdam", None)
    parsed = httpx.URL(url)
    assert parsed.fragment == "" and parsed.query == b""
    assert parsed.path.endswith("/30/2026-10-18:11-00/42.ts")
    segments = parsed.raw_path.decode().split("/")
    assert segments[2:4] == ["us%2Fer", "p%40ss%20word%261%2F%23%3F%25"]


# U3 / U4 --------------------------------------------------------------------------


def test_u3_local_tz_is_a_real_zone(monkeypatch):
    monkeypatch.delenv("XTREAM_TZ", raising=False)
    monkeypatch.setenv("TZ", "Europe/London")
    tz = timeutil.local_tz()
    assert str(tz) == "Europe/London"
    # 26 Oct (after the clocks go back) 20:00 GMT must show as 20:00.
    ts = int(datetime(2026, 10, 26, 20, 0, tzinfo=UTC).timestamp())
    assert timeutil.fmt_time(ts).endswith("2026-10-26 20:00")


def test_u4_schedule_day_is_25h_on_fall_back_day(monkeypatch):
    monkeypatch.setenv("XTREAM_TZ", "Europe/Dublin")
    from fastmcp import Client

    from mcp_xtream import server as server_mod
    from mcp_xtream.data import Channel
    from mcp_xtream.epg import Programme

    # 25 Oct 2026: midnight IST (+1) to midnight GMT is 25 hours.
    start = int(datetime(2026, 10, 24, 23, 0, tzinfo=UTC).timestamp())
    progs = [Programme(start + h * 3600, start + (h + 1) * 3600, f"h{h}", "") for h in range(26)]
    ch = Channel(1, "Chan", "Cat", "c", 3, "ts")

    class Stub:
        warnings: ClassVar[dict] = {}
        by_stream: ClassVar[dict] = {1: ch}

        def programmes_for(self, c):
            return progs

    monkeypatch.setattr(server_mod, "store", lambda: Stub())

    async def run():
        async with Client(server_mod.mcp) as c:
            return (await c.call_tool("channel_schedule", {"stream_id": 1, "date": "2026-10-25"})).data

    import asyncio

    titles = [p["title"] for p in asyncio.run(run())["programmes"]]
    assert titles == [f"h{h}" for h in range(25)]


# U5 / U6 / U10 / U13: liveness via lock --------------------------------------------


def test_u5_killed_worker_does_not_block_downloads(server):
    _Server.slow = 0.2
    job = _start()
    running = _wait(job["id"], {"downloading"})
    os.kill(running["pid"], signal.SIGKILL)
    failed = _wait(job["id"], {"failed"})
    assert "exited unexpectedly" in failed["error"]
    assert not Path(failed["part"]).exists()
    assert downloader.active_job() is None


def test_u5_cancel_immediately_after_start(server):
    _Server.slow = 0.2
    job = _start()
    downloader.cancel_job(job["id"])  # likely before the worker's handler/imports
    done = _wait(job["id"], {"cancelled", "failed"})
    assert done["state"] == "cancelled"
    assert downloader.active_job() is None


def test_u6_live_but_unrelated_pid_is_not_mistaken_for_worker():
    job = downloader.new_job(**_job_kwargs())
    job.update(state="downloading", pid=os.getpid())  # alive, but not a worker
    downloader.write_job(job)
    assert downloader.refresh_state(job)["state"] == "failed"
    result = downloader.cancel_job(job["id"])  # must not signal our own pid
    assert result is not None and result[0]["state"] == "failed"


def test_u10_refresh_rereads_before_failing():
    job = downloader.new_job(**_job_kwargs())
    job["state"] = "done"
    downloader.write_job(job)
    stale = dict(job, state="converting")
    assert downloader.refresh_state(stale)["state"] == "done"


def test_u13_spawn_failure_does_not_leave_a_blocking_job(monkeypatch):
    def boom(*a, **k):
        raise OSError("no fork for you")

    monkeypatch.setattr(downloader.subprocess, "Popen", boom)
    with pytest.raises(OSError):
        _start()
    assert downloader.active_job() is None
    assert downloader.list_jobs()[0]["state"] == "failed"


# U7 / U8 -------------------------------------------------------------------------


def test_u7_parallel_starts_only_launch_one(server):
    _Server.slow = 0.2
    started, refused = [], []

    def attempt():
        with downloader.spawn_lock():
            if downloader.active_job():
                refused.append(1)
                return
            time.sleep(0.2)  # widen the window, like the auth round trip
            started.append(downloader.start_job(**_job_kwargs()))

    threads = [threading.Thread(target=attempt) for _ in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert len(started) == 1 and len(refused) == 3
    downloader.cancel_job(started[0]["id"])
    _wait(started[0]["id"], {"cancelled", "failed"})


def test_u8_concurrent_job_writes():
    job = downloader.new_job(**_job_kwargs())
    errors = []

    def writer():
        try:
            for _ in range(50):
                downloader.write_job(dict(job))
        except Exception as e:  # noqa: BLE001
            errors.append(e)

    threads = [threading.Thread(target=writer) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert not errors
    assert not list(Path(os.environ["XTREAM_CACHE_DIR"], "jobs").glob("*.tmp"))


# U9 ------------------------------------------------------------------------------


def test_u9_cancel_stops_conversion(tmp_path):
    fake_ffmpeg = tmp_path / "ffmpeg"
    fake_ffmpeg.write_text(f"#!{sys.executable}\nimport time\ntime.sleep(60)\n")
    fake_ffmpeg.chmod(0o755)
    job = downloader.new_job(**_job_kwargs(fmt="mkv"))
    Path(job["part"]).write_bytes(b"x" * 2_000_000)
    worker = downloader.Worker(job)
    threading.Timer(0.5, lambda: setattr(worker, "cancelled", True)).start()
    t0 = time.time()
    with pytest.raises(downloader._Cancelled):
        worker._remux(str(fake_ffmpeg), tmp_path / "out.tmp.mkv")
    assert time.time() - t0 < 5


def test_u9_cancel_reports_finished_jobs_honestly():
    job = downloader.new_job(**_job_kwargs())
    job["state"] = "done"
    downloader.write_job(job)
    _, message = downloader.cancel_job(job["id"])
    assert "already done" in message


# U11 -----------------------------------------------------------------------------


@pytest.mark.parametrize("body", [b"", b"<html>502 Bad Gateway</html>"])
def test_u11_empty_guide_is_a_failed_refresh(server, body):
    _Server.xmltv_body = body
    store = data.DataStore(Credentials(server, "user", "pass", "test"))
    with pytest.raises(XtreamError, match="empty or not valid"):
        store._fetch_epg(previous=None)


def test_u11_truncated_guide_fails_only_when_a_copy_exists(server):
    _Server.xmltv_body = (b'<tv><programme channel="c" start="20261018090000 +0000" '
                          b'stop="20261018100000 +0000"><title>x</title></programme><programme ch')
    store = data.DataStore(Credentials(server, "user", "pass", "test"))
    assert "c" in store._fetch_epg(previous=None)["epg"]
    with pytest.raises(XtreamError, match="cut off"):
        store._fetch_epg(previous={"epg": {}})


# U12 ------------------------------------------------------------------------------


def test_u12_failure_removes_partial_file(monkeypatch, server):
    job = downloader.new_job(**_job_kwargs())
    downloader.write_job(job)
    Path(job["part"]).write_bytes(b"x" * 1000)
    worker = downloader.Worker(job)
    monkeypatch.setattr(worker, "_catchup_url", lambda creds: (_ for _ in ()).throw(RuntimeError("boom")))
    with pytest.raises(RuntimeError):
        worker.run()
    assert not Path(job["part"]).exists()
    assert downloader.read_job(job["id"])["state"] == "failed"


# U14 ------------------------------------------------------------------------------


def test_u14_worker_ignores_modules_in_launch_dir(server, tmp_path, monkeypatch):
    launch = tmp_path / "launch"
    launch.mkdir()
    (launch / "httpx.py").write_text("raise SystemExit('hijacked')\n")
    monkeypatch.chdir(launch)
    job = _start()
    done = _wait(job["id"], {"done", "failed"})
    assert done["state"] == "done", done.get("error")


# U16 ------------------------------------------------------------------------------


def test_u16_length_less_body_cut_short_is_resumed(server):
    _Server.no_length = True
    _Server.drop_first_at = 1_000_000
    job = downloader.new_job(**_job_kwargs())
    downloader.write_job(job)
    worker = downloader.Worker(job)
    worker._download(f"{server}/timeshift/user/pass/30/x/1.ts", Credentials(server, "u", "p", "t"))
    assert Path(job["part"]).read_bytes() == BODY
    assert _Server.ranges and _Server.ranges[0] == f"bytes={1_000_000}-"


# U17 ------------------------------------------------------------------------------


def test_u17_fresh_zero_offset_beats_cached(monkeypatch):
    info = {"user_info": {}, "server_info": {"time_now": "2026-10-18 09:00:00",
                                             "timestamp_now": 1792314000}}
    monkeypatch.setattr(client_mod.XtreamClient, "authenticate", lambda self: info)
    job = downloader.new_job(**_job_kwargs(server_offset=3600))
    url = downloader.Worker(job)._catchup_url(Credentials("http://s", "u", "p", "t"))
    assert "/2026-10-18:09-00/" in url  # UTC server: offset 0, not the cached +1h


def test_worker_subprocess_entrypoint_is_importable():
    out = subprocess.run([sys.executable, "-I", "-c", "import mcp_xtream.downloader"],
                         capture_output=True, text=True, check=False)
    assert out.returncode == 0, out.stderr


# Round 3 ----------------------------------------------------------------------


def test_r3_cold_start_without_guide_keeps_channel_tools_working(server):
    _Server.xmltv_body = b"<tv></tv>"
    store = data.DataStore(Credentials(server, "user", "pass", "test"))
    store.ensure_loaded()  # must not raise
    assert store.epg == {} and "unavailable" in store.warnings["epg"]
    fetches = sum(p.startswith("/xmltv.php") for p in _Server.paths)
    store.ensure_loaded()  # no refetch until the retry delay passes
    assert sum(p.startswith("/xmltv.php") for p in _Server.paths) == fetches


def test_r3_cancel_never_writes_the_job_file(server):
    _Server.slow = 0.2
    job = _start()
    _wait(job["id"], {"downloading"})
    path = Path(os.environ["XTREAM_CACHE_DIR"], "jobs", f"{job['id']}.json")
    before = path.stat().st_mtime_ns
    downloader.cancel_job(job["id"])
    # Only the marker is written by the server; the worker owns the job file.
    assert path.with_suffix(".cancel").exists()
    assert downloader.read_job(job["id"]).get("cancel_requested") is None
    assert _wait(job["id"], {"cancelled", "failed"})["state"] == "cancelled"
    assert path.stat().st_mtime_ns >= before


def test_r3_marker_cancels_worker_without_a_signal(server):
    _Server.slow = 0.2
    job = _start()
    running = _wait(job["id"], {"downloading"})
    Path(os.environ["XTREAM_CACHE_DIR"], "jobs", f"{job['id']}.cancel").touch()
    assert _wait(job["id"], {"cancelled", "failed"}, timeout=10)["state"] == "cancelled"
    assert running["pid"]


def test_r3_probe_keeps_file_on_unexpected_status(server, monkeypatch):
    _Server.no_length = True
    job = downloader.new_job(**_job_kwargs())
    downloader.write_job(job)
    real_send = _Server._send

    def odd_416(self, status, body=b"", headers=()):
        real_send(self, 404 if status == 416 else status, body, headers)

    monkeypatch.setattr(_Server, "_send", odd_416)
    worker = downloader.Worker(job)
    worker._download(f"{server}/timeshift/u/p/30/x/1.ts", Credentials(server, "u", "p", "t"))
    assert Path(job["part"]).read_bytes() == BODY
    assert "couldn't be checked" in downloader.read_job(job["id"])["warning"]


def test_r3_legacy_job_without_lock_uses_pid():
    job = downloader.new_job(**_job_kwargs())
    job.update(state="downloading", pid=os.getpid())
    del job["uses_lock"]  # written by a version without lock files
    downloader.write_job(job)
    assert downloader.refresh_state(job)["state"] == "downloading"


def test_r3_stubborn_ffmpeg_still_ends_cancelled(tmp_path):
    fake = tmp_path / "ffmpeg"
    fake.write_text(f"#!{sys.executable}\nimport signal, time\n"
                    "signal.signal(signal.SIGTERM, signal.SIG_IGN)\ntime.sleep(60)\n")
    fake.chmod(0o755)
    job = downloader.new_job(**_job_kwargs(fmt="mkv"))
    worker = downloader.Worker(job)
    worker.cancelled = True
    real_popen = subprocess.Popen

    class FastWait(real_popen):
        def wait(self, timeout=None):
            return super().wait(timeout=0.2 if timeout else None)

    import mcp_xtream.downloader as d

    d.subprocess.Popen, saved = FastWait, d.subprocess.Popen
    try:
        with pytest.raises(downloader._Cancelled):
            worker._remux(str(fake), tmp_path / "o.tmp.mkv")
    finally:
        d.subprocess.Popen = saved


def test_r3_tz_absolute_path(monkeypatch):
    monkeypatch.delenv("XTREAM_TZ", raising=False)
    monkeypatch.setenv("TZ", "/usr/share/zoneinfo/America/New_York")
    assert str(timeutil.local_tz()) == "America/New_York"
