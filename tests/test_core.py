import json
import threading
from datetime import UTC, datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import ClassVar

import pytest

from mcp_xtream import config, downloader
from mcp_xtream.client import server_local_time, server_utc_offset
from mcp_xtream.epg import Programme, clean_text, parse_xmltv, resolve_overlaps


@pytest.fixture(autouse=True)
def isolated_dirs(tmp_path, monkeypatch):
    monkeypatch.setenv("XTREAM_CACHE_DIR", str(tmp_path / "cache"))
    monkeypatch.setenv("XTREAM_DOWNLOAD_DIR", str(tmp_path / "downloads"))
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path / "config"))
    for var in ("XTREAM_SERVER", "XTREAM_USERNAME", "XTREAM_PASSWORD"):
        monkeypatch.delenv(var, raising=False)


# -- EPG -----------------------------------------------------------------------


def test_parse_xmltv_resolves_overlaps_and_cleans_titles():
    xml = b"""<tv>
    <programme channel="a" start="20261018090000 +0000" stop="20261018100000 +0000"><title>A &amp;amp; B</title></programme>
    <programme channel="a" start="20261018090500 +0000" stop="20261018100500 +0000"><title>competing</title></programme>
    <programme channel="a" start="20261018095800 +0000" stop="20261018110000 +0000"><title>next
      line</title></programme>
    </tv>"""
    progs = parse_xmltv(xml)["a"]
    assert [p.title for p in progs] == ["A & B", "next line"]
    assert progs[0].stop == progs[1].start  # 2-minute overlap clipped
    assert datetime.fromtimestamp(progs[0].stop, tz=UTC).minute == 58


def test_resolve_overlaps_keeps_first_on_equal_start():
    progs = [Programme(0, 3600, "first", ""), Programme(0, 3600, "second", "")]
    assert [p.title for p in resolve_overlaps(progs)] == ["first"]


def test_clean_text():
    assert clean_text("Law &amp; Order\n \n  SVU") == "Law & Order SVU"


def test_parse_xmltv_datetime_offset():
    xml = b'<tv><programme channel="c" start="20261018100000 +0100" stop="20261018103000 +0100"><title>x</title></programme></tv>'
    start = parse_xmltv(xml)["c"][0].start
    assert datetime.fromtimestamp(start, tz=UTC).hour == 9


# -- timezones -----------------------------------------------------------------


def test_catchup_time_uses_server_zone():
    ts = int(datetime(2026, 10, 18, 9, 0, tzinfo=UTC).timestamp())
    assert server_local_time(ts, "Europe/Amsterdam").strftime("%H-%M") == "11-00"
    assert server_local_time(ts, "Bogus/Zone", 7200).strftime("%H-%M") == "11-00"
    assert server_local_time(ts).strftime("%H-%M") == "09-00"
    winter = int(datetime(2026, 12, 6, 9, 0, tzinfo=UTC).timestamp())
    assert server_local_time(winter, "Europe/Amsterdam").strftime("%H-%M") == "10-00"


def test_server_utc_offset():
    info = {"time_now": "2026-10-18 13:02:52", "timestamp_now": 1792321372}
    assert server_utc_offset(info) == 7200
    assert server_utc_offset({}) is None


# -- config ----------------------------------------------------------------------


def test_credentials_from_env(monkeypatch):
    monkeypatch.setenv("XTREAM_SERVER", "http://s")
    monkeypatch.setenv("XTREAM_USERNAME", "u")
    monkeypatch.setenv("XTREAM_PASSWORD", "secret")
    creds = config.load_credentials()
    assert creds.source == "environment"
    assert creds.redact("http://s/live/u/secret/1.ts") == "http://s/live/u/***/1.ts"


def test_missing_credentials(monkeypatch):
    monkeypatch.setenv("XTREAM_SERVER", "http://s")
    with pytest.raises(config.ConfigError, match="XTREAM_USERNAME, XTREAM_PASSWORD"):
        config.load_credentials()


def test_pyiptv_config_is_ignored(tmp_path):
    d = tmp_path / "config" / "PyIPTV"
    d.mkdir(parents=True)
    (d / "playlists.json").write_text(json.dumps({"playlists": {"1": {
        "name": "Main", "source_type": "xtream", "server_url": "http://x",
        "username": "u", "password": "p"}}}))
    with pytest.raises(config.ConfigError):
        config.load_credentials()


# -- downloader ------------------------------------------------------------------


def test_find_ts_sync():
    packet = b"\x47" + b"\x00" * 187
    assert downloader.find_ts_sync(b"\x99" * 92 + packet * 5) == 92
    assert downloader.find_ts_sync(b"\x00" * 2000) == -1


JUNK = b"\x40\x34\x13" + b"\x99" * 89  # 92 bytes, like the real provider
BODY = b"".join(bytes([0x47, i % 256]) + bytes(186) for i in range(30_000))  # ~5.6 MB
PAYLOAD = JUNK + BODY


class _FlakyServer(BaseHTTPRequestHandler):
    """Serves PAYLOAD; the first full request is cut off partway."""

    drops_left = 1
    ranges_seen: ClassVar[list[str]] = []

    def do_GET(self):
        rng = self.headers.get("Range")
        start = 0
        if rng:
            type(self).ranges_seen.append(rng)
            start = int(rng.split("=")[1].rstrip("-"))
            self.send_response(206)
            self.send_header("Content-Range", f"bytes {start}-{len(PAYLOAD) - 1}/{len(PAYLOAD)}")
        else:
            self.send_response(200)
        self.send_header("Content-Length", str(len(PAYLOAD) - start))
        self.end_headers()
        data = PAYLOAD[start:]
        if type(self).drops_left and not rng:
            type(self).drops_left -= 1
            self.wfile.write(data[:2_000_000])
            self.wfile.flush()
            self.connection.close()  # premature end
            return
        self.wfile.write(data)

    def log_message(self, *args):
        pass


def test_worker_trims_and_resumes_after_drop(monkeypatch):
    server = ThreadingHTTPServer(("127.0.0.1", 0), _FlakyServer)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    url = f"http://127.0.0.1:{server.server_address[1]}/timeshift/x.ts"
    monkeypatch.setattr(downloader.time, "sleep", lambda s: None)
    try:
        job = downloader.new_job(
            stream_id=1, channel_name="Chan", title="Show", start=1_792_314_000,
            stop=1_792_315_800, fmt="ts", server_tz=None, server_offset=None,
        )
        downloader.write_job(job)
        worker = downloader.Worker(job)
        creds = config.Credentials("http://x", "u", "p", "test")
        worker._download(url, creds)
        worker._finish()
    finally:
        server.shutdown()

    saved = downloader.read_job(job["id"])
    assert saved["state"] == "done"
    assert saved["retries"] == 1
    # Resumes from the last byte actually written (<= what was sent before the drop).
    assert len(_FlakyServer.ranges_seen) == 1
    assert 0 < int(_FlakyServer.ranges_seen[0][6:-1]) <= 2_000_000
    assert Path(saved["output"]).read_bytes() == BODY
    assert saved["bytes_done"] == len(BODY)


def test_new_job_safe_unique_names():
    kw = {"stream_id": 1, "channel_name": "A/B", "title": "Show: 1", "start": 0,
          "stop": 60, "fmt": "mkv", "server_tz": None, "server_offset": None}
    first = downloader.new_job(**kw)
    name = Path(first["output"]).name
    assert name.startswith("Show 1 - A B - ") and name.endswith(".mkv")
    Path(first["output"]).write_bytes(b"x")
    second = downloader.new_job(**kw)
    assert second["output"] != first["output"]
    # A name whose .part.ts is still being written is taken too.
    Path(second["part"]).write_bytes(b"x")
    assert downloader.new_job(**kw)["output"] not in (first["output"], second["output"])


def test_output_name_fits_255_bytes_and_keeps_channel_and_date(monkeypatch):
    monkeypatch.setenv("XTREAM_TZ", "Europe/Dublin")
    from mcp_xtream.timeutil import local_tz

    local_tz.cache_clear()
    stem = downloader.output_stem("Ελληνικά " * 40, "Канал " * 30, 1792314000)
    assert len((stem + " (12).part.ts").encode()) <= 255
    assert stem.endswith(" - 2026-10-18 1000")  # Irish time, not system time
    assert " - Канал" in stem
    local_tz.cache_clear()
