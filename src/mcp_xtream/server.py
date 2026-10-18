"""MCP server: search an Xtream provider's TV guide and download catchup."""

from __future__ import annotations

import asyncio
import time
from datetime import datetime, timedelta
from importlib.metadata import PackageNotFoundError, version
from typing import Literal

from fastmcp import Context, FastMCP
from fastmcp.exceptions import ToolError
from fastmcp.server.lifespan import lifespan

from . import downloader
from .client import XtreamClient, XtreamError
from .config import ConfigError, load_credentials
from .data import Channel, DataStore, fmt_time, local_tz, shifted
from .epg import Programme
from .events import EventHub

INSTRUCTIONS = """\
Search an IPTV provider's TV guide (EPG) and download past programmes via
catchup. Typical flow: search_programmes -> pick a channel that has
catchup_available -> download_programme -> download_status.

Notes:
- Several channels often share one guide (same epg_id). Provider channel
  names can be wrong (e.g. a regional feed carrying another region's guide),
  so prefer channels whose name/category matches the programme's region.
- "+1" channels that reuse the main channel's guide are shifted an hour
  later automatically; such channel entries carry their own start/end.
- The account allows a limited number of simultaneous connections; a
  download uses one, so it may stop the user's TV playback (and vice versa).
- Times are shown in the user's local timezone.
- Clients that support server-push events can call subscribe_events once
  after connecting; they're then told when each download finishes, fails or
  is cancelled (MCP log notifications with logger="events"), so there's no
  need to poll download_status.

Episode numbers (for Jellyfin/Plex) are YOUR job, and best effort:
- The guide has no season/episode data, and repeats air long after their
  original broadcast, so the guide alone can't tell you SxxEyy.
- Use programme_details for the full description, then identify the episode
  yourself (web search, TheTVDB/TMDB/TVmaze, the broadcaster's site), matching
  on the description rather than the air date.
- Numbering differs between broadcasters and databases (e.g. "series 18" vs
  "season 28"); prefer the numbering of the user's media server database
  (usually TheTVDB or TMDB) and ask the user if unsure.
- Pass show/season/episode to download_programme only when you're confident;
  otherwise pass just show to get date-based naming, which Jellyfin also
  understands. Tell the user the numbers are a best guess and how you got them.
"""

try:
    _VERSION = version("mcp-xtream")
except PackageNotFoundError:  # running from a source tree without install
    _VERSION = "0.0.0"

_events: EventHub | None = None


@lifespan
async def _app_lifespan(server):
    """Run the download-event poller for the life of the server."""
    global _events
    _events = EventHub()
    task = asyncio.create_task(_events.run())
    try:
        yield {}
    finally:
        task.cancel()


mcp = FastMCP("Xtream IPTV", instructions=INSTRUCTIONS, version=_VERSION, lifespan=_app_lifespan)

_store: DataStore | None = None


def store() -> DataStore:
    global _store
    if _store is None:
        try:
            _store = DataStore(load_credentials())
        except ConfigError as e:
            raise ToolError(str(e)) from None
    try:
        _store.ensure_loaded()
    except XtreamError as e:
        raise ToolError(str(e)) from None
    return _store


def _with_warnings(result: dict) -> dict:
    """Flag results built from an old copy because a refresh failed."""
    if _store is not None and _store.warnings:
        result["warning"] = " ".join(_store.warnings.values())
    return result


def _programme_id(epg_id: str, prog: Programme) -> str:
    return f"{epg_id}@{prog.start}"


def _parse_programme_id(pid: str) -> tuple[str, int]:
    epg_id, _, start = pid.rpartition("@")
    if not epg_id or not start.isdigit():
        raise ToolError(f"Invalid programme_id: {pid!r}")
    return epg_id, int(start)


def _catchup_available(ch: Channel, prog: Programme, now: float) -> bool:
    return bool(ch.catchup_days) and prog.stop <= now and prog.start >= now - ch.catchup_days * 86400


def _status(prog: Programme, now: float) -> str:
    if prog.stop <= now:
        return "aired"
    if prog.start <= now:
        return "airing"
    return "upcoming"


def _channel_dict(ch: Channel) -> dict:
    return {
        "stream_id": ch.stream_id,
        "name": ch.name,
        "category": ch.category,
        "epg_id": ch.epg_id or None,
        "catchup_days": ch.catchup_days,
    }


def _programme_dict(
    epg_id: str, prog: Programme, now: float, with_desc: bool, shift: int = 0
) -> dict:
    """prog may be shifted for a +1 channel; the id always uses guide time."""
    out = {
        "programme_id": f"{epg_id}@{prog.start - shift}",
        "title": prog.title,
        "start": fmt_time(prog.start),
        "end": fmt_time(prog.stop),
        "minutes": (prog.stop - prog.start) // 60,
        "status": _status(prog, now),
    }
    if with_desc and prog.desc:
        out["description"] = prog.desc
    return out


def _parse_local(value: str) -> int:
    try:
        dt = datetime.fromisoformat(value)
    except ValueError:
        raise ToolError(f"Not an ISO datetime: {value!r} (e.g. 2026-10-18T10:00)") from None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=local_tz())
    return int(dt.timestamp())


def _client_name(ctx: Context) -> str:
    """Stable identity for the connected client, so subscriptions survive a
    reconnect (a new session for the same client takes over delivery)."""
    try:
        return ctx.session.client_params.clientInfo.name or "unknown"  # type: ignore[union-attr]
    except Exception:  # noqa: BLE001
        return "unknown"


@mcp.tool
async def subscribe_events(ctx: Context) -> str:
    """Register this session for server-push download events.

    Call once after connecting (and again after reconnecting; repeat calls are
    harmless). When a download finishes, fails or is cancelled, the server
    pushes an MCP log notification with logger="events" and a payload like
    {"event": "download_finished", "title": ..., "output": ..., "message": ...}.
    The first subscription from a client covers downloads that end from then
    on; later ones also catch up on any that ended while disconnected.
    Requires a persistent session (stdio).
    """
    if _events is None:
        raise ToolError("Events aren't available (server lifespan not running)")
    _events.subscribe(_client_name(ctx), ctx.session)
    return ("Subscribed: download_finished / download_failed / download_cancelled events "
            "will be delivered to this session as log notifications (logger='events').")


@mcp.tool
def account_info() -> dict:
    """Account status: expiry, connection limit and current active connections
    (fetched live)."""
    s = store()
    with XtreamClient(s.creds) as client:
        auth = client.authenticate()
    user = auth.get("user_info") or {}
    exp = str(user.get("exp_date") or "")
    return {
        "status": user.get("status"),
        "expires": fmt_time(int(exp)) if exp.isdigit() else None,
        "max_connections": user.get("max_connections"),
        "active_connections": user.get("active_cons"),
        "server_timezone": (auth.get("server_info") or {}).get("timezone"),
        "channels": len(s.channels),
        "channels_with_catchup": sum(1 for c in s.channels if c.catchup_days),
    }


@mcp.tool
def list_channels(query: str = "", catchup_only: bool = False, limit: int = 30) -> dict:
    """Find live channels by name or category (case-insensitive; all words must
    match). Each result includes stream_id, epg_id, catchup_days and what's on
    now."""
    s = store()
    words = query.lower().split()
    now = time.time()
    matches = []
    for ch in s.channels:
        hay = f"{ch.name} {ch.category}".lower()
        if words and not all(w in hay for w in words):
            continue
        if catchup_only and not ch.catchup_days:
            continue
        entry = _channel_dict(ch)
        current = next((p for p in s.programmes_for(ch) if p.start <= now < p.stop), None)
        entry["now_playing"] = current.title if current else None
        matches.append(entry)
    return _with_warnings({"total_matches": len(matches), "channels": matches[: max(1, limit)]})


@mcp.tool
def search_programmes(
    query: str,
    channel: str = "",
    when: Literal["any", "past", "now", "upcoming"] = "any",
    hours_back: int = 72,
    hours_ahead: int = 24,
    catchup_only: bool = False,
    search_descriptions: bool = False,
    limit: int = 25,
) -> dict:
    """Search the TV guide by programme title (all words must match).

    channel: optional channel-name filter (e.g. "bbc one"). The window is
    hours_back before now to hours_ahead after. Each result lists every
    channel carrying that guide entry, with catchup_available telling you
    whether it can be downloaded now. Results are newest first.
    """
    s = store()
    now = time.time()
    lo, hi = now - hours_back * 3600, now + hours_ahead * 3600
    words = query.lower().split()
    chan_words = channel.lower().split()

    epg_ids = s.by_epg.keys()
    if chan_words:
        epg_ids = {
            c.epg_id
            for c in s.channels
            if c.epg_id and all(w in f"{c.name} {c.category}".lower() for w in chan_words)
        }

    results = []
    for epg_id in epg_ids:
        streams = s.by_epg.get(epg_id, [])
        for prog in s.epg.get(epg_id, []):
            if prog.stop < lo or prog.start > hi:
                continue
            status = _status(prog, now)
            if when == "past" and status != "aired":
                continue
            if when == "now" and status != "airing":
                continue
            if when == "upcoming" and status != "upcoming":
                continue
            hay = prog.title.lower()
            if search_descriptions:
                hay += " " + prog.desc.lower()
            if words and not all(w in hay for w in words):
                continue
            chans = []
            for ch in streams:
                if chan_words and not all(w in f"{ch.name} {ch.category}".lower() for w in chan_words):
                    continue
                entry = _channel_dict(ch)
                airing = shifted(prog, ch.epg_shift)
                if ch.epg_shift:
                    entry["start"], entry["end"] = fmt_time(airing.start), fmt_time(airing.stop)
                entry["catchup_available"] = _catchup_available(ch, airing, now)
                chans.append(entry)
            if catchup_only:
                chans = [c for c in chans if c["catchup_available"]]
            if not chans:
                continue
            item = _programme_dict(epg_id, prog, now, search_descriptions)
            item["channels"] = chans
            results.append((prog.start, item))

    results.sort(key=lambda r: r[0], reverse=True)
    return _with_warnings(
        {"total_matches": len(results), "programmes": [r[1] for r in results[: max(1, limit)]]}
    )


@mcp.tool
def channel_schedule(stream_id: int, date: str = "today", include_descriptions: bool = False) -> dict:
    """A channel's guide for one day. date: 'today', 'yesterday', 'tomorrow'
    or YYYY-MM-DD (local time)."""
    s = store()
    ch = s.by_stream.get(stream_id)
    if ch is None:
        raise ToolError(f"No channel with stream_id {stream_id}")
    today = datetime.now(tz=local_tz()).date()
    named = {"today": 0, "yesterday": -1, "tomorrow": 1}
    if date in named:
        day = today + timedelta(days=named[date])
    else:
        try:
            day = datetime.fromisoformat(date).date()
        except ValueError:
            raise ToolError(f"Bad date {date!r}: use today, yesterday, tomorrow or YYYY-MM-DD") from None
    # Local midnight to the next local midnight: 23 or 25 hours on DST days.
    day_start = int(datetime.combine(day, datetime.min.time(), tzinfo=local_tz()).timestamp())
    next_day = day + timedelta(days=1)
    day_end = int(datetime.combine(next_day, datetime.min.time(), tzinfo=local_tz()).timestamp())
    now = time.time()
    progs = []
    for p in s.programmes_for(ch):
        if p.stop <= day_start or p.start >= day_end:
            continue
        item = _programme_dict(ch.epg_id, p, now, include_descriptions, ch.epg_shift)
        item["catchup_available"] = _catchup_available(ch, p, now)
        progs.append(item)
    return _with_warnings({"channel": _channel_dict(ch), "date": day.isoformat(), "programmes": progs})


@mcp.tool
def programme_details(programme_id: str) -> dict:
    """Everything the guide knows about one programme: full description, air
    times and every channel carrying it (with catchup availability).

    Use this to identify the episode yourself before downloading; the guide
    has no season/episode numbers.
    """
    s = store()
    epg_id, start_ts = _parse_programme_id(programme_id)
    prog = s.find_programme(epg_id, start_ts)
    if prog is None:
        raise ToolError("programme_id not found in the guide (try refresh_data)")
    now = time.time()
    out = _programme_dict(epg_id, prog, now, with_desc=True)
    out["description"] = prog.desc or None
    out["air_date"] = datetime.fromtimestamp(prog.start, tz=local_tz()).date().isoformat()
    chans = []
    for ch in s.by_epg.get(epg_id, []):
        entry = _channel_dict(ch)
        airing = shifted(prog, ch.epg_shift)
        entry["start"], entry["end"] = fmt_time(airing.start), fmt_time(airing.stop)
        entry["catchup_available"] = _catchup_available(ch, airing, now)
        chans.append(entry)
    out["channels"] = chans
    out["episode_numbering"] = (
        "Not in the guide. Identify it from the description (best effort) and pass "
        "show/season/episode to download_programme."
    )
    return _with_warnings(out)


@mcp.tool
def download_programme(
    stream_id: int,
    programme_id: str = "",
    start: str = "",
    end: str = "",
    title: str = "",
    format: Literal["mkv", "ts"] = "mkv",
    force: bool = False,
    show: str = "",
    season: int | None = None,
    episode: int | None = None,
    episode_title: str = "",
) -> dict:
    """Download a past programme from a channel's catchup archive in the
    background. Returns a job_id; poll download_status.

    Identify the programme with programme_id (from search_programmes /
    channel_schedule), or give start/end as ISO local datetimes
    (e.g. 2026-10-18T10:00) plus a title. format 'mkv' remuxes with ffmpeg
    (no re-encode, keeps subtitles); falls back to .ts if ffmpeg is missing.
    force=True skips the free-connection check.

    Media-server naming (optional): give show (the series name as your media
    server knows it) to save as Jellyfin/Plex TV layout with an .nfo file:
    with season+episode -> 'Show/Season 02/Show - S02E05 - <episode_title>',
    without them -> date-based 'Show/Show - 2026-10-18'. season/episode are
    your best-effort identification (see programme_details); only pass them
    when confident. Without show, files are named '<title> - <channel> - <time>'.
    """
    if (season is None) != (episode is None):
        raise ToolError("Give both season and episode, or neither")
    if season is not None and (season < 0 or episode < 0):
        raise ToolError("season and episode must be zero or positive")
    if season is not None and not show:
        raise ToolError("Give show (the series name) along with season/episode")
    s = store()
    ch = s.by_stream.get(stream_id)
    if ch is None:
        raise ToolError(f"No channel with stream_id {stream_id}")
    if not ch.catchup_days:
        raise ToolError(f"'{ch.name}' has no catchup archive")

    if programme_id:
        epg_id, start_ts = _parse_programme_id(programme_id)
        prog = s.find_programme(epg_id, start_ts)
        if prog is None:
            raise ToolError("programme_id not found in the guide (try refresh_data)")
        if epg_id != ch.epg_id:
            raise ToolError(
                f"That programme is from guide '{epg_id}', but '{ch.name}' uses "
                f"'{ch.epg_id or 'none'}'. Pick a channel listed with the programme."
            )
        airing = shifted(prog, ch.epg_shift)  # a +1 channel airs it an hour later
        start_ts, stop_ts, prog_title = airing.start, airing.stop, title or prog.title
        description = prog.desc
    elif start and end:
        start_ts, stop_ts = _parse_local(start), _parse_local(end)
        prog_title = title or f"{ch.name} recording"
        description = ""
    else:
        raise ToolError("Give programme_id, or start and end")

    now = time.time()
    if stop_ts <= start_ts:
        raise ToolError("end must be after start")
    if stop_ts > now:
        raise ToolError("That programme hasn't finished airing yet")
    if start_ts < now - ch.catchup_days * 86400:
        raise ToolError(f"Outside the {ch.catchup_days}-day catchup window for '{ch.name}'")

    # Held across the check, the connection probe and the spawn, so parallel
    # tool calls (or two server processes) can't both start a download.
    with downloader.spawn_lock():
        if active := downloader.active_job():
            raise ToolError(
                f"Download {active['id']} ('{active['title']}') is still running; "
                "wait for it or cancel it first"
            )
        if not force:
            try:
                with XtreamClient(s.creds) as client:
                    user = client.authenticate().get("user_info") or {}
            except XtreamError as e:
                raise ToolError(str(e)) from None
            try:
                active_cons = int(user.get("active_cons") or 0)
                max_cons = int(user.get("max_connections") or 0)
            except ValueError:
                active_cons, max_cons = 0, 0
            if max_cons and active_cons >= max_cons:
                raise ToolError(
                    f"All {max_cons} connection(s) are in use (probably TV playback). "
                    "Stop playback first, or pass force=True to try anyway."
                )
        layout: dict = {}
        if show:
            folder, stem = downloader.jellyfin_layout(show, season, episode, episode_title, start_ts)
            layout = {
                "folder": folder,
                "stem": stem,
                "nfo": {
                    "title": episode_title or prog_title,
                    "showtitle": show,
                    "season": season,
                    "episode": episode,
                    "aired": datetime.fromtimestamp(start_ts, tz=local_tz()).date().isoformat(),
                    "plot": description,
                    "studio": ch.name,
                },
            }
        try:
            job = downloader.start_job(
                stream_id=stream_id,
                channel_name=ch.name,
                title=prog_title,
                start=start_ts,
                stop=stop_ts,
                fmt=format,
                server_tz=s.server_tz,
                server_offset=s.server_offset,
                **layout,
            )
        except OSError as e:
            raise ToolError(f"Couldn't start the download: {e.strerror or e}") from None
    return _job_summary(job)


def _job_summary(job: dict) -> dict:
    done, total = job.get("bytes_done") or 0, job.get("bytes_total")
    out = {
        "job_id": job["id"],
        "state": job["state"],
        "title": job["title"],
        "channel": job["channel_name"],
        "programme_start": fmt_time(job["start"]),
        "programme_end": fmt_time(job["stop"]),
        "output": job["output"],
        "downloaded_mb": round(done / 1e6, 1),
        "total_mb": round(total / 1e6, 1) if total else None,
        "percent": round(100 * done / total, 1) if total else None,
        "retries": job.get("retries", 0),
    }
    if job.get("error"):
        out["error"] = job["error"]
    if job.get("warning"):
        out["warning"] = job["warning"]
    return out


@mcp.tool
def download_status(job_id: str) -> dict:
    """Progress of a download job: state is queued, downloading, converting,
    done, failed or cancelled."""
    job = downloader.read_job(job_id)
    if job is None:
        raise ToolError(f"No download job {job_id}")
    return _job_summary(downloader.refresh_state(job))


@mcp.tool
def list_downloads(limit: int = 10) -> dict:
    """Recent download jobs, newest first."""
    return {"downloads": [_job_summary(j) for j in downloader.list_jobs()[: max(1, limit)]]}


@mcp.tool
def cancel_download(job_id: str) -> dict:
    """Cancel a running download (including its MKV conversion) and delete
    its partial file. Poll download_status to see it reach 'cancelled'."""
    result = downloader.cancel_job(job_id)
    if result is None:
        raise ToolError(f"No download job {job_id}")
    job, message = result
    return {"job_id": job_id, "state": job["state"], "message": message}


@mcp.tool
def refresh_data() -> dict:
    """Re-fetch the channel list and TV guide from the provider now. They
    refresh automatically (guide every 3h, channels every 6h) when a tool is
    used; past programmes are kept for 7 days so catchup stays searchable."""
    s = store()
    try:
        s.ensure_loaded(force=True)
    except XtreamError as e:
        raise ToolError(str(e)) from None
    starts = [progs[0].start for progs in s.epg.values() if progs]
    return _with_warnings({
        "channels": len(s.channels),
        "guide_channels": len(s.epg),
        "programmes": sum(len(p) for p in s.epg.values()),
        "guide_starts": fmt_time(min(starts)) if starts else None,
    })


def main() -> None:
    mcp.run(show_banner=False)
