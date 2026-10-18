"""XMLTV parsing into a per-channel, non-overlapping programme index."""

from __future__ import annotations

import html
import io
import xml.etree.ElementTree as ET
from datetime import UTC, datetime, timedelta
from typing import NamedTuple


class Programme(NamedTuple):
    start: int  # unix ts
    stop: int
    title: str
    desc: str


EpgIndex = dict[str, list[Programme]]

# Overlaps up to this long are sloppy boundaries and get clipped; longer ones
# mean the feed merged a competing schedule and the later entry is dropped.
MAX_CLIP_SECONDS = 5 * 60


def parse_xmltv_datetime(s: str) -> int:
    """'20261018100000 +0100' -> unix ts. 0 if unparseable."""
    parts = (s or "").strip().split()
    if not parts:
        return 0
    offset = parts[1] if len(parts) > 1 else "+0000"
    try:
        dt = datetime.strptime(parts[0][:14], "%Y%m%d%H%M%S")
        sign = 1 if offset.startswith("+") else -1
        delta = timedelta(hours=int(offset[1:3]), minutes=int(offset[3:5]))
        return int((dt - sign * delta).replace(tzinfo=UTC).timestamp())
    except (ValueError, IndexError):
        return 0


def clean_text(s: str) -> str:
    """Decode entities some feeds double-escape and collapse whitespace."""
    if "&" in s:
        s = html.unescape(s)
    return " ".join(s.split())


def resolve_overlaps(programmes: list[Programme]) -> list[Programme]:
    """Sort and make non-overlapping. Stable sort: on equal starts the entry
    that appeared first in the feed wins."""
    programmes.sort(key=lambda p: p.start)
    result: list[Programme] = []
    for prog in programmes:
        if prog.stop <= prog.start:
            continue
        if result:
            prev = result[-1]
            overlap = prev.stop - prog.start
            if overlap > 0:
                shorter = min(prev.stop - prev.start, prog.stop - prog.start)
                if prog.start <= prev.start or overlap > min(MAX_CLIP_SECONDS, shorter // 2):
                    continue
                result[-1] = prev._replace(stop=prog.start)
        result.append(prog)
    return result


def parse_xmltv(content: bytes) -> EpgIndex:
    return parse_xmltv_checked(content)[0]


def parse_xmltv_checked(content: bytes) -> tuple[EpgIndex, bool]:
    """Parse XMLTV; the bool is False if the document was malformed or cut
    off (whatever parsed before that point is still returned)."""
    index: EpgIndex = {}
    complete = True
    try:
        for _event, elem in ET.iterparse(io.BytesIO(content), events=("end",)):
            if elem.tag != "programme":
                continue
            channel = elem.get("channel", "")
            start = parse_xmltv_datetime(elem.get("start", ""))
            stop = parse_xmltv_datetime(elem.get("stop", ""))
            if channel and start and stop:
                title_el, desc_el = elem.find("title"), elem.find("desc")
                title = clean_text(title_el.text or "") if title_el is not None else ""
                desc = (desc_el.text or "").strip() if desc_el is not None else ""
                if "&" in desc:
                    desc = html.unescape(desc)
                index.setdefault(channel, []).append(Programme(start, stop, title, desc))
            elem.clear()
    except ET.ParseError:
        complete = False
    return {ch: resolve_overlaps(progs) for ch, progs in index.items()}, complete
