"""The user's local timezone, as a real IANA zone so DST is handled."""

from __future__ import annotations

import os
from datetime import datetime, tzinfo
from functools import cache
from pathlib import Path
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError


def _zone(name: str | None) -> ZoneInfo | None:
    if not name:
        return None
    name = name.removeprefix(":")
    if "zoneinfo/" in name:  # TZ=/usr/share/zoneinfo/Europe/Dublin
        name = name.split("zoneinfo/", 1)[1]
    try:
        return ZoneInfo(name)
    except (ZoneInfoNotFoundError, ValueError):
        return None


@cache
def local_tz() -> tzinfo:
    """XTREAM_TZ, else TZ, else the zone /etc/localtime points at.

    `datetime.now().astimezone().tzinfo` is only a fixed offset (e.g. +01:00
    "IST"), which shows times on the other side of a DST change an hour out,
    so it's the last resort.
    """
    for name in (os.environ.get("XTREAM_TZ"), os.environ.get("TZ")):
        if zone := _zone(name):
            return zone
    try:
        target = str(Path("/etc/localtime").resolve())
        if "zoneinfo/" in target and (zone := _zone(target.split("zoneinfo/", 1)[1])):
            return zone
    except OSError:
        pass
    return datetime.now().astimezone().tzinfo  # type: ignore[return-value]


def fmt_time(ts: int) -> str:
    return datetime.fromtimestamp(ts, tz=local_tz()).strftime("%a %Y-%m-%d %H:%M")
