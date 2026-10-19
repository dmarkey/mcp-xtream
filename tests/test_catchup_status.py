"""Graded catch-up status, best-first channel ordering and check_recording."""

import asyncio
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import ClassVar

import pytest
from fastmcp import Client

from mcp_xtream import downloader, server, timeutil
from mcp_xtream.client import XtreamClient
from mcp_xtream.config import Credentials
from mcp_xtream.data import Channel, DataStore
from mcp_xtream.epg import Programme

H = 3600
NOW = int(time.time())


@pytest.fixture(autouse=True)
def isolated(tmp_path, monkeypatch):
    monkeypatch.setenv("XTREAM_CACHE_DIR", str(tmp_path / "cache"))
    monkeypatch.setenv("XTREAM_DOWNLOAD_DIR", str(tmp_path / "downloads"))
    monkeypatch.setenv("XTREAM_TZ", "Europe/Dublin")
    timeutil.local_tz.cache_clear()
    yield
    timeutil.local_tz.cache_clear()


CH = Channel(1, "UK: BBC One NI", "UK", "bbc", 3, "ts")
NOCATCH = Channel(2, "UK: BBC One NI HEVC", "UK", "bbc", 0, "ts")


@pytest.mark.parametrize(("start", "stop", "channel", "expected"), [
    (NOW - 10 * H, NOW - 9 * H, CH, "in_window"),
    (NOW - 2 * H, NOW - 1 * H, CH, "just_aired"),
    (NOW - 5 * 86400, NOW - 5 * 86400 + H, CH, "expired"),
    (NOW - 600, NOW + 600, CH, "airing"),
    (NOW + H, NOW + 2 * H, CH, "not_aired_yet"),
    (NOW - 10 * H, NOW - 9 * H, NOCATCH, "no_catchup"),
])
def test_catchup_status(start, stop, channel, expected):
    assert server._catchup_status(channel, Programme(start, stop, "x", ""), NOW) == expected


OLD = Programme(NOW - 10 * H, NOW - 9 * H, "Sunday Politics", "Politics.")
RECENT = Programme(NOW - 2 * H, NOW - H, "Sunday Politics", "Politics.")
OTHER = Channel(3, "IRL: BBC ONE NORTHERN 4K", "IE", "bbc", 3, "ts")


class _Stub(DataStore):
    def __init__(self, url="http://127.0.0.1:9"):
        self.warnings = {}
        self.channels = [NOCATCH, CH, OTHER]
        self.by_stream = {c.stream_id: c for c in self.channels}
        self.by_epg = {"bbc": [NOCATCH, CH, OTHER]}
        self.epg = {"bbc": [OLD, RECENT]}
        self.server_tz, self.server_offset = "Europe/Amsterdam", 7200
        self.creds = Credentials(url, "u", "p", "test")


def call(tool, args, raise_on_error=True):
    async def run():
        async with Client(server.mcp) as c:
            return await c.call_tool(tool, args, raise_on_error=raise_on_error)
    return asyncio.run(run())


@pytest.fixture
def stub(monkeypatch):
    store = _Stub()
    monkeypatch.setattr(server, "store", lambda: store)
    monkeypatch.setattr(downloader, "active_job", lambda: None)
    return store


def test_search_lists_all_channels_best_first(stub):
    progs = call("search_programmes", {"query": "sunday politics", "when": "past"}).data["programmes"]
    old = next(p for p in progs if p["programme_id"] == f"bbc@{OLD.start}")
    statuses = [c["catchup_status"] for c in old["channels"]]
    assert statuses == ["in_window", "in_window", "no_catchup"]  # no-catchup channel kept, last
    assert [c["catchup_available"] for c in old["channels"]] == [True, True, False]


def test_catchup_only_drops_unavailable_channels(stub):
    progs = call("search_programmes", {"query": "sunday politics", "catchup_only": True}).data["programmes"]
    assert all(c["catchup_status"] != "no_catchup" for p in progs for c in p["channels"])


def test_list_channels_keeps_channels_without_catchup(stub):
    names = {c["name"] for c in call("list_channels", {"query": "bbc"}).data["channels"]}
    assert NOCATCH.name in names
    names = {c["name"] for c in call("list_channels", {"query": "bbc", "catchup_only": True}).data["channels"]}
    assert NOCATCH.name not in names


class _Provider(BaseHTTPRequestHandler):
    status = 206
    paths: ClassVar[list[str]] = []

    def do_GET(self):
        type(self).paths.append(self.path)
        if self.path.startswith("/player_api.php"):
            body = b'{"user_info":{"auth":1,"status":"Active","max_connections":"1","active_cons":"0"},"server_info":{}}'
            self.send_response(200)
        elif type(self).status == 206:
            body = b"\x47" * 1024
            self.send_response(206)
            self.send_header("Content-Range", "bytes 0-1023/1288599318")
        else:
            body = b""
            self.send_response(type(self).status)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):
        pass


@pytest.fixture
def provider(monkeypatch):
    _Provider.status, _Provider.paths = 206, []
    srv = ThreadingHTTPServer(("127.0.0.1", 0), _Provider)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    url = f"http://127.0.0.1:{srv.server_address[1]}"
    store = _Stub(url)
    monkeypatch.setattr(server, "store", lambda: store)
    monkeypatch.setattr(downloader, "active_job", lambda: None)
    yield url
    srv.shutdown()


def test_probe_recording_reports_size(provider):
    status, size = XtreamClient(Credentials(provider, "u", "p", "t")).probe_recording(f"{provider}/timeshift/x.ts")
    assert (status, size) == (206, 1288599318)


def test_check_recording_available(provider):
    r = call("check_recording", {"stream_id": 1, "programme_id": f"bbc@{OLD.start}"}).data
    assert r["available"] and r["size_mb"] == 1288.6 and "1.3 GB" in r["message"]
    assert any(p.startswith("/timeshift/u/p/") for p in _Provider.paths)


def test_check_recording_not_ready_vs_missing(provider):
    _Provider.status = 404
    recent = call("check_recording", {"stream_id": 1, "programme_id": f"bbc@{RECENT.start}"}).data
    assert not recent["available"] and "Not available yet" in recent["message"]
    old = call("check_recording", {"stream_id": 1, "programme_id": f"bbc@{OLD.start}"}).data
    assert "another channel" in old["message"]


def test_check_recording_skips_channels_without_catchup(provider):
    r = call("check_recording", {"stream_id": 2, "programme_id": f"bbc@{OLD.start}"}).data
    assert not r["available"] and "no catch-up archive" in r["message"]
    assert not any(p.startswith("/timeshift") for p in _Provider.paths)  # no stream opened


def test_check_recording_respects_busy_connection(provider, monkeypatch):
    def busy(_s):
        raise server.ToolError("All 1 connection(s) are in use")

    monkeypatch.setattr(server, "_require_free_connection", busy)
    r = call("check_recording", {"stream_id": 1, "programme_id": f"bbc@{OLD.start}"}, raise_on_error=False)
    assert r.is_error and not any(p.startswith("/timeshift") for p in _Provider.paths)
    ok = call("check_recording", {"stream_id": 1, "programme_id": f"bbc@{OLD.start}", "force": True}).data
    assert ok["available"]


def test_check_recording_waits_for_running_download(provider, monkeypatch):
    monkeypatch.setattr(downloader, "active_job", lambda: {"id": "abc", "title": "Gogglebox"})
    r = call("check_recording", {"stream_id": 1, "programme_id": f"bbc@{OLD.start}"}, raise_on_error=False)
    assert r.is_error and "Gogglebox" in r.content[0].text
