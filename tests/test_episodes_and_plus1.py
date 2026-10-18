"""+1 channel guide shifting, programme_details, and agent-supplied
season/episode naming for Jellyfin."""

import asyncio
import time
import xml.etree.ElementTree as ET
from pathlib import Path

import pytest
from fastmcp import Client

from mcp_xtream import downloader, server, timeutil
from mcp_xtream.data import Channel, DataStore, guide_shift
from mcp_xtream.epg import Programme


@pytest.fixture(autouse=True)
def isolated(tmp_path, monkeypatch):
    monkeypatch.setenv("XTREAM_CACHE_DIR", str(tmp_path / "cache"))
    monkeypatch.setenv("XTREAM_DOWNLOAD_DIR", str(tmp_path / "downloads"))
    monkeypatch.setenv("XTREAM_TZ", "Europe/Dublin")
    timeutil.local_tz.cache_clear()
    yield
    timeutil.local_tz.cache_clear()


@pytest.mark.parametrize(("name", "epg_id", "shift"), [
    ("UK: E4 +1 ◉", "E4.uk", 3600),
    ("UK: ITV 2+1 ◉", "ITV2.uk", 3600),
    ("UK: Quest Red +1 ◉", "QuestRedPlus1.uk", 0),  # has its own +1 guide
    ("UK: E4 ◉", "E4.uk", 0),
    ("UK: E4 +1 ◉", "", 0),
    ("UK: 5 STAR+1 ◉", "5Star+1.uk", 0),
])
def test_guide_shift(name, epg_id, shift):
    assert guide_shift(name, epg_id) == shift


# A guide entry that ended two hours ago, on E4 and E4 +1.
NOW = int(time.time())
PROG = Programme(NOW - 3 * 3600, NOW - 2 * 3600, "Gogglebox", "Rob and Stephen watch the news.")
E4 = Channel(1, "UK: E4 ◉", "UK General", "E4.uk", 3, "ts")
E4P1 = Channel(2, "UK: E4 +1 ◉", "UK General", "E4.uk", 3, "ts", epg_shift=3600)


class _StubStore(DataStore):
    def __init__(self):
        self.warnings = {}
        self.channels = [E4, E4P1]
        self.by_stream = {1: E4, 2: E4P1}
        self.by_epg = {"E4.uk": [E4, E4P1]}
        self.epg = {"E4.uk": [PROG]}
        self.server_tz, self.server_offset = "Europe/Amsterdam", 7200
        self.creds = None


@pytest.fixture
def stub(monkeypatch):
    started: list[dict] = []
    monkeypatch.setattr(server, "store", lambda: _StubStore())
    monkeypatch.setattr(downloader, "active_job", lambda: None)

    def fake_start(**kw):
        started.append(kw)
        return downloader.new_job(**kw)

    monkeypatch.setattr(downloader, "start_job", fake_start)
    return started


def call(tool, args, raise_on_error=True):
    async def run():
        async with Client(server.mcp) as c:
            return await c.call_tool(tool, args, raise_on_error=raise_on_error)
    return asyncio.run(run())


PID = f"E4.uk@{PROG.start}"


def test_programmes_for_shifts_plus1():
    store = _StubStore()
    assert store.programmes_for(E4P1)[0].start == PROG.start + 3600
    assert store.programmes_for(E4)[0].start == PROG.start


def test_search_reports_plus1_airing_time(stub):
    res = call("search_programmes", {"query": "gogglebox"}).data
    prog = res["programmes"][0]
    assert prog["programme_id"] == PID
    by_id = {c["stream_id"]: c for c in prog["channels"]}
    assert "start" not in by_id[1]
    assert by_id[2]["start"] == timeutil.fmt_time(PROG.start + 3600)
    # E4 aired 2h ago; E4 +1 finished only 1h ago — both downloadable.
    assert by_id[1]["catchup_available"] and by_id[2]["catchup_available"]


def test_download_from_plus1_uses_shifted_time(stub):
    call("download_programme", {"stream_id": 2, "programme_id": PID, "force": True})
    assert stub[0]["start"] == PROG.start + 3600 and stub[0]["stop"] == PROG.stop + 3600


def test_schedule_ids_round_trip_for_plus1(stub):
    from datetime import datetime

    day = datetime.fromtimestamp(PROG.start + 3600, tz=timeutil.local_tz()).date().isoformat()
    sched = call("channel_schedule", {"stream_id": 2, "date": day}).data
    entry = next(p for p in sched["programmes"] if p["title"] == "Gogglebox")
    assert entry["programme_id"] == PID  # guide-time id, usable with download_programme
    assert entry["start"] == timeutil.fmt_time(PROG.start + 3600)


def test_programme_details(stub):
    d = call("programme_details", {"programme_id": PID}).data
    assert d["description"] == PROG.desc
    assert "Not in the guide" in d["episode_numbering"]
    assert {c["stream_id"] for c in d["channels"]} == {1, 2}


def test_download_with_season_episode_uses_jellyfin_layout(stub):
    call("download_programme", {"stream_id": 1, "programme_id": PID, "force": True,
                                "show": "Gogglebox", "season": 24, "episode": 10,
                                "episode_title": "Episode 10"})
    kw = stub[0]
    assert kw["folder"] == "Gogglebox/Season 24"
    assert kw["stem"] == "Gogglebox - S24E10 - Episode 10"
    assert kw["nfo"]["plot"] == PROG.desc and kw["nfo"]["season"] == 24


def test_download_show_only_uses_air_date(stub):
    call("download_programme", {"stream_id": 1, "programme_id": PID, "force": True,
                                "show": "Gogglebox"})
    kw = stub[0]
    assert kw["folder"] == "Gogglebox"
    assert kw["stem"].startswith("Gogglebox - 20") and kw["nfo"]["season"] is None


@pytest.mark.parametrize("args", [
    {"season": 1},
    {"season": 1, "episode": 2},           # no show
    {"show": "X", "season": -1, "episode": 2},
])
def test_download_naming_validation(stub, args):
    res = call("download_programme", {"stream_id": 1, "programme_id": PID, "force": True, **args},
               raise_on_error=False)
    assert res.is_error and not stub


def test_worker_writes_nfo(tmp_path):
    folder, stem = downloader.jellyfin_layout("Show/With:Bad*Chars", 2, 5, "Pilot", PROG.start)
    assert folder == "Show With Bad Chars/Season 02" and stem.endswith("S02E05 - Pilot")
    job = downloader.new_job(stream_id=1, channel_name="C", title="T", start=PROG.start,
                             stop=PROG.stop, fmt="ts", server_tz=None, server_offset=None,
                             folder=folder, stem=stem,
                             nfo={"title": "Pilot", "showtitle": "Show", "season": 2,
                                  "episode": 5, "aired": "2026-10-18", "plot": "p & q"})
    downloader.write_job(job)
    Path(job["part"]).write_bytes(b"\x47" * 2_000_000)
    downloader.Worker(job)._finish()
    out = Path(downloader.read_job(job["id"])["output"])
    assert out.parent.name == "Season 02" and out.suffix == ".ts"
    nfo = ET.parse(out.with_suffix(".nfo")).getroot()
    assert nfo.tag == "episodedetails"
    assert nfo.findtext("season") == "2" and nfo.findtext("plot") == "p & q"


def test_no_nfo_without_show(tmp_path):
    job = downloader.new_job(stream_id=1, channel_name="C", title="T", start=PROG.start,
                             stop=PROG.stop, fmt="ts", server_tz=None, server_offset=None)
    downloader.write_job(job)
    Path(job["part"]).write_bytes(b"\x47" * 2_000_000)
    downloader.Worker(job)._finish()
    out = Path(downloader.read_job(job["id"])["output"])
    assert not out.with_suffix(".nfo").exists()
