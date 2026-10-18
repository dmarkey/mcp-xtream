import time

import pytest

from mcp_xtream import data
from mcp_xtream.client import XtreamError
from mcp_xtream.config import Credentials
from mcp_xtream.data import DataStore, merge_history
from mcp_xtream.epg import Programme

H = 3600


@pytest.fixture(autouse=True)
def isolated_cache(tmp_path, monkeypatch):
    monkeypatch.setenv("XTREAM_CACHE_DIR", str(tmp_path / "cache"))


def P(start, stop, title="x"):
    return Programme(start, stop, title, "")


def test_merge_keeps_history_the_new_feed_dropped():
    now = 100 * H
    old = {"bbc": [P(now - 48 * H, now - 47 * H, "two days ago"),
                   P(now - 25 * H, now - 23 * H, "straddles"),
                   P(now - 23 * H, now - 22 * H, "also in new feed")]}
    new = {"bbc": [P(now - 24 * H, now - 22 * H, "new first"), P(now, now + H, "now")]}
    merged = merge_history(new, old, keep_after=now - 7 * 24 * H)["bbc"]
    assert [p.title for p in merged] == ["two days ago", "straddles", "new first", "now"]
    assert merged[1].stop == now - 24 * H  # clipped to the new feed's first start


def test_merge_drops_expired_and_keeps_vanished_channels():
    now = 100 * H
    old = {"gone": [P(now - 30 * H, now - 29 * H)], "ancient": [P(now - 99 * H, now - 98 * H)]}
    merged = merge_history({}, old, keep_after=now - 72 * H)
    assert "gone" in merged and "ancient" not in merged


class _Store(DataStore):
    """DataStore with scripted fetches instead of network calls."""

    def __init__(self):
        super().__init__(Credentials("http://s", "u", "secret", "test"))
        self.epg_fetches = 0
        self.fail = False

    def _fetch_channels(self):
        if self.fail:
            raise XtreamError("Cannot reach server: secret timeout")
        return {"channels": [], "server_tz": "Europe/Amsterdam", "server_offset": 7200, "account": {}}

    def _fetch_epg(self, previous):
        if self.fail:
            raise XtreamError("Cannot reach server")
        self.epg_fetches += 1
        start = int(time.time()) - H
        return {"epg": {"bbc": [[start, start + 2 * H, f"fetch {self.epg_fetches}", ""]]}}


def test_stale_copy_used_when_refresh_fails(monkeypatch):
    clock = [time.time()]
    monkeypatch.setattr(data.time, "time", lambda: clock[0])
    s = _Store()
    s.ensure_loaded()
    assert s.epg["bbc"][0].title == "fetch 1"

    # Guide expires while the provider is unreachable: keep the old copy.
    clock[0] += data.EPG_TTL + 60
    s.fail = True
    s.ensure_loaded()
    assert s.epg["bbc"][0].title == "fetch 1"
    assert "TV guide" in s.warnings["epg"]
    assert "secret" not in " ".join(s.warnings.values())  # password redacted

    # No refetch on every call...
    clock[0] += 60
    s.ensure_loaded()
    assert s.epg_fetches == 1
    # ...but it retries after RETRY_AFTER_FAILURE and clears the warning.
    clock[0] += data.RETRY_AFTER_FAILURE
    s.fail = False
    s.ensure_loaded()
    assert s.epg_fetches == 2 and not s.warnings


def test_no_copy_and_fetch_fails_raises():
    s = _Store()
    s.fail = True
    with pytest.raises(XtreamError):
        s.ensure_loaded()


def test_disk_cache_age_counts_from_fetch(monkeypatch):
    s = _Store()
    s.ensure_loaded()
    # A new process 2.5h later reuses the disk copy...
    later = time.time() + 2.5 * H
    monkeypatch.setattr(data.time, "time", lambda: later)
    s2 = _Store()
    s2.ensure_loaded()
    assert s2.epg_fetches == 0
    # ...but it still expires 3h after the original fetch, not 3h after loading.
    monkeypatch.setattr(data.time, "time", lambda: later + 0.6 * H)
    s2.ensure_loaded()
    assert s2.epg_fetches == 1
