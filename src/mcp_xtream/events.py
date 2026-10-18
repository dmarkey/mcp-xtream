"""Server-push events: tell a subscribed client when a download finishes.

Delivery uses MCP's server->client log-notification channel
(`session.send_log_message(..., logger="events")`), so the event reaches the
client even when it isn't waiting on a tool call. That needs a session that
outlives the request: true for stdio (one session per process).

Downloads run in separate worker processes, so a single background task polls
the job files and announces each job that reaches a final state (done, failed,
cancelled). What has been announced is persisted per client in the cache
directory, so a download that finishes while the server is restarting (e.g.
the client restarted it) is still announced once it resubscribes, and a
client's first subscription only covers jobs that end after it.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import tempfile
import time
import warnings
from pathlib import Path

from . import downloader
from .config import cache_dir

log = logging.getLogger(__name__)

POLL_SECONDS = 5
FINAL_STATES = ("done", "failed", "cancelled")
EVENT_NAMES = {
    "done": "download_finished",
    "failed": "download_failed",
    "cancelled": "download_cancelled",
}


def _state_path() -> Path:
    return cache_dir() / "events.json"


def describe(job: dict) -> dict:
    """The event payload for a job in a final state."""
    state = job["state"]
    payload = {
        "event": EVENT_NAMES[state],
        "job_id": job["id"],
        "title": job.get("title"),
        "channel": job.get("channel_name"),
    }
    if state == "done":
        size = job.get("size") or 0
        payload.update(output=job.get("output"), size_mb=round(size / 1e6, 1))
        message = f"Download finished: '{job.get('title')}' ({size / 1e9:.1f} GB) saved to {job.get('output')}."
        if job.get("warning"):
            payload["warning"] = job["warning"]
            message += f" Note: {job['warning']}"
    elif state == "failed":
        payload["error"] = job.get("error")
        message = f"Download failed: '{job.get('title')}': {job.get('error') or 'unknown error'}."
    else:
        message = f"Download cancelled: '{job.get('title')}'."
    payload["message"] = message
    return payload


class EventHub:
    """Tracks subscribed sessions and announces finished downloads to them."""

    def __init__(self) -> None:
        # Latest session per client name, so delivery survives a reconnect.
        self._sessions: dict[str, object] = {}
        self._state = self._load()

    # -- persistence -----------------------------------------------------------

    def _load(self) -> dict:
        try:
            data = json.loads(_state_path().read_text(encoding="utf-8"))
            return data if isinstance(data, dict) else {}
        except (OSError, ValueError):
            return {}

    def _save(self) -> None:
        path = _state_path()
        fd, tmp = tempfile.mkstemp(dir=path.parent, prefix=".events.", suffix=".tmp")
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump(self._state, f)
            os.replace(tmp, path)
        except BaseException:
            Path(tmp).unlink(missing_ok=True)
            raise

    # -- subscriptions ---------------------------------------------------------

    def subscribe(self, client: str, session: object) -> None:
        self._sessions[client] = session
        if client not in self._state:
            # First ever subscription: only announce jobs that end from now on.
            self._state[client] = {"since": time.time(), "announced": []}
            self._save()

    @property
    def subscribed(self) -> bool:
        return bool(self._sessions)

    # -- delivery --------------------------------------------------------------

    async def tick(self) -> int:
        """Announce newly finished jobs to every subscriber. Returns how many
        events were delivered."""
        if not self._sessions:
            return 0
        jobs = await asyncio.to_thread(downloader.list_jobs)
        final = [j for j in jobs if j.get("state") in FINAL_STATES]
        existing = {j["id"] for j in jobs}
        delivered = 0
        changed = False
        for client, session in list(self._sessions.items()):
            entry = self._state.setdefault(client, {"since": time.time(), "announced": []})
            announced = set(entry["announced"])
            for job in sorted(final, key=lambda j: j.get("updated_at", 0)):
                if job["id"] in announced or job.get("updated_at", 0) < entry["since"]:
                    continue
                try:
                    with warnings.catch_warnings():
                        # MCP marks the logging capability deprecated (SEP-2577),
                        # but it's the channel event-aware clients listen on.
                        warnings.simplefilter("ignore")
                        await session.send_log_message(  # type: ignore[attr-defined]
                            level="notice", data=describe(job), logger="events"
                        )
                except Exception:  # noqa: BLE001 - client gone; retry next tick
                    log.warning("event delivery to %s failed; will retry", client)
                    break
                announced.add(job["id"])
                delivered += 1
                changed = True
            # Forget jobs whose files are gone so the list doesn't grow forever.
            pruned = sorted(announced & existing)
            if pruned != sorted(entry["announced"]):
                entry["announced"] = pruned
                changed = True
        if changed:
            self._save()
        return delivered

    async def run(self) -> None:
        while True:
            try:
                await self.tick()
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("event poll failed")
            await asyncio.sleep(POLL_SECONDS)
