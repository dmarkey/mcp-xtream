"""Channel list + EPG, fetched from the provider and cached on disk as gzip JSON."""

from __future__ import annotations

import gzip
import hashlib
import json
import os
import re
import threading
import time
from dataclasses import asdict, dataclass

import httpx

from .client import XtreamClient, XtreamError, server_utc_offset
from .config import Credentials, cache_dir
from .epg import EpgIndex, Programme, parse_xmltv_checked
from .timeutil import fmt_time, local_tz  # noqa: F401 - re-exported for server

CHANNELS_TTL = 6 * 3600
EPG_TTL = 3 * 3600
# Providers' XMLTV usually only reaches ~24h back, but catchup keeps days.
# Past programmes from earlier fetches are kept this long so they stay
# searchable for the whole catchup window.
EPG_HISTORY = 7 * 86400
# After a failed refresh, keep serving the old copy and retry after this.
RETRY_AFTER_FAILURE = 10 * 60
SCHEMA_VERSION = 1


@dataclass
class Channel:
    stream_id: int
    name: str
    category: str
    epg_id: str
    catchup_days: int  # 0 = no catchup
    container: str
    # Seconds to add to this channel's guide times. Providers often point
    # "+1" channels at the main channel's guide, so its times are an hour early.
    epg_shift: int = 0


_PLUS_ONE_NAME = re.compile(r"\+\s*1\b")
_PLUS_ONE_GUIDE = re.compile(r"(plus\s*1|\+\s*1)", re.IGNORECASE)


def guide_shift(name: str, epg_id: str) -> int:
    """3600 for a '+1' channel that uses the main channel's guide, else 0."""
    if epg_id and _PLUS_ONE_NAME.search(name) and not _PLUS_ONE_GUIDE.search(epg_id):
        return 3600
    return 0


def shifted(prog: Programme, seconds: int) -> Programme:
    if not seconds:
        return prog
    return prog._replace(start=prog.start + seconds, stop=prog.stop + seconds)


def _read_cache(name: str, ttl: int | None) -> dict | None:
    """Cached payload if present and younger than ttl (None = any age)."""
    path = cache_dir() / name
    try:
        with gzip.open(path, "rt", encoding="utf-8") as f:
            data = json.load(f)
    except (OSError, ValueError):
        return None
    if data.get("schema") != SCHEMA_VERSION:
        return None
    if ttl is not None and time.time() - data.get("saved_at", 0) > ttl:
        return None
    return data


def _write_cache(name: str, data: dict) -> None:
    path = cache_dir() / name
    tmp = path.with_suffix(".tmp")
    with gzip.open(tmp, "wt", encoding="utf-8") as f:
        json.dump({"schema": SCHEMA_VERSION, "saved_at": time.time(), **data}, f)
    os.replace(tmp, path)


class DataStore:
    def __init__(self, creds: Credentials):
        self.creds = creds
        self._lock = threading.Lock()
        self.channels: list[Channel] = []
        self.by_stream: dict[int, Channel] = {}
        self.by_epg: dict[str, list[Channel]] = {}
        self.epg: EpgIndex = {}
        self.server_tz: str | None = None
        self.server_offset: int | None = None
        self.account: dict = {}
        self._channels_loaded_at = 0.0
        self._epg_loaded_at = 0.0
        self.warnings: dict[str, str] = {}  # kind -> stale-data notice

    def _key(self) -> str:
        raw = f"{self.creds.server_url}|{self.creds.username}".encode()
        return hashlib.sha256(raw).hexdigest()[:16]

    # -- loading -----------------------------------------------------------

    def ensure_loaded(self, force: bool = False) -> None:
        with self._lock:
            now = time.time()
            if force or now - self._channels_loaded_at > CHANNELS_TTL:
                self._refresh("channels", force)
            if force or now - self._epg_loaded_at > EPG_TTL:
                self._refresh("epg", force)

    def _refresh(self, kind: str, force: bool) -> None:
        """Load from a fresh disk cache, else fetch. If fetching fails, fall
        back to the old copy (any age) and retry after a short delay."""
        name = f"{kind}_{self._key()}.json.gz"
        ttl = CHANNELS_TTL if kind == "channels" else EPG_TTL
        data = None if force else _read_cache(name, ttl)
        if data is None:
            previous = _read_cache(name, None)
            try:
                payload = self._fetch_channels() if kind == "channels" else self._fetch_epg(previous)
            except (XtreamError, httpx.HTTPError) as e:
                if previous is None and kind == "channels":
                    raise  # nothing useful works without channels
                if previous is None:
                    # No guide at all yet: carry on with an empty one so the
                    # channel tools still work, and retry later.
                    previous = {"saved_at": 0, "epg": {}}
                what = "channel list" if kind == "channels" else "TV guide"
                reason = self.creds.redact(str(e))
                if previous.get("saved_at"):
                    age_h = (time.time() - previous["saved_at"]) / 3600
                    self.warnings[kind] = (
                        f"Couldn't refresh the {what} ({reason}); using a copy from {age_h:.1f}h ago."
                    )
                else:
                    self.warnings[kind] = f"The {what} is unavailable ({reason}); will retry."
                data = previous
                # Pretend it was loaded so we don't refetch on every call.
                loaded_at = time.time() - ttl + RETRY_AFTER_FAILURE
            else:
                _write_cache(name, payload)
                data = {"saved_at": time.time(), **payload}
                self.warnings.pop(kind, None)
                loaded_at = data["saved_at"]
        else:
            self.warnings.pop(kind, None)
            loaded_at = data["saved_at"]  # age counts from when it was fetched

        if kind == "channels":
            self._apply_channels(data)
            self._channels_loaded_at = loaded_at
        else:
            self.epg = {
                ch: [Programme(*p) for p in progs] for ch, progs in data["epg"].items()
            }
            self._epg_loaded_at = loaded_at

    def _fetch_channels(self) -> dict:
        with XtreamClient(self.creds) as client:
            auth = client.authenticate()
            categories = {
                str(c.get("category_id")): c.get("category_name") or "Uncategorized"
                for c in client.live_categories()
            }
            streams = client.live_streams()
        server_info = auth.get("server_info") or {}
        return {
            "channels": [
                asdict(ch) for s in streams if (ch := _to_channel(s, categories)) is not None
            ],
            "server_tz": server_info.get("timezone"),
            "server_offset": server_utc_offset(server_info),
            "account": _account_summary(auth.get("user_info") or {}),
        }

    def _fetch_epg(self, previous: dict | None) -> dict:
        with XtreamClient(self.creds) as client:
            index, complete = parse_xmltv_checked(client.xmltv())
        # A 200 with an empty body or an HTML error page parses to nothing;
        # treat that (or a truncated feed, when we have an older copy to fall
        # back on) as a failed refresh so the stale-copy warning path runs.
        if not index:
            raise XtreamError("TV guide response was empty or not valid XMLTV")
        if not complete and previous is not None:
            raise XtreamError("TV guide download was cut off part-way")
        old = {
            ch: [Programme(*p) for p in progs]
            for ch, progs in (previous or {}).get("epg", {}).items()
        }
        merged = merge_history(index, old, time.time() - EPG_HISTORY)
        return {"epg": {ch: [list(p) for p in progs] for ch, progs in merged.items()}}

    def _apply_channels(self, data: dict) -> None:
        self.channels = [Channel(**c) for c in data["channels"]]
        for c in self.channels:
            c.epg_shift = guide_shift(c.name, c.epg_id)
        self.by_stream = {c.stream_id: c for c in self.channels}
        self.by_epg = {}
        for c in self.channels:
            if c.epg_id:
                self.by_epg.setdefault(c.epg_id, []).append(c)
        self.server_tz = data.get("server_tz")
        self.server_offset = data.get("server_offset")
        self.account = data.get("account") or {}

    # -- lookups -----------------------------------------------------------

    def find_programme(self, epg_id: str, start: int) -> Programme | None:
        for p in self.epg.get(epg_id, []):
            if p.start == start:
                return p
        return None

    def programmes_for(self, channel: Channel) -> list[Programme]:
        """The channel's programmes, at the times *this* channel airs them."""
        progs = self.epg.get(channel.epg_id, []) if channel.epg_id else []
        return [shifted(p, channel.epg_shift) for p in progs] if channel.epg_shift else progs


def merge_history(new: EpgIndex, old: EpgIndex, keep_after: float) -> EpgIndex:
    """Add past programmes from an earlier fetch that the new feed no longer
    includes, so the whole catchup window stays searchable.

    For each channel, old programmes starting before the new feed's first
    programme are prepended (clipped so they don't overlap it). Anything that
    ended before keep_after is dropped.
    """
    merged: EpgIndex = {}
    for ch in new.keys() | old.keys():
        fresh = new.get(ch, [])
        cutoff = fresh[0].start if fresh else float("inf")
        history = []
        for p in old.get(ch, []):
            if p.start >= cutoff:
                break
            if p.stop > keep_after:
                history.append(p._replace(stop=min(p.stop, cutoff)))
        programmes = history + [p for p in fresh if p.stop > keep_after]
        if programmes:
            merged[ch] = programmes
    return merged


def _to_channel(stream: dict, categories: dict[str, str]) -> Channel | None:
    try:
        stream_id = int(stream["stream_id"])
    except (KeyError, TypeError, ValueError):
        return None

    def as_int(v) -> int:
        try:
            return int(v or 0)
        except (TypeError, ValueError):
            return 0

    has_archive = as_int(stream.get("tv_archive")) > 0
    return Channel(
        stream_id=stream_id,
        name=(stream.get("name") or "").strip(),
        category=categories.get(str(stream.get("category_id")), "Uncategorized"),
        epg_id=(stream.get("epg_channel_id") or "").strip(),
        catchup_days=as_int(stream.get("tv_archive_duration")) if has_archive else 0,
        container=stream.get("container_extension") or "ts",
    )


def _account_summary(user_info: dict) -> dict:
    """The safe subset of user_info (it also echoes the password back)."""
    keys = ("status", "exp_date", "max_connections", "active_cons", "is_trial", "message")
    return {k: user_info.get(k) for k in keys}
