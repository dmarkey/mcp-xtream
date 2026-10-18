"""Server-push download events (subscribe_events + EventHub)."""

import asyncio
import time

import pytest

from mcp_xtream import downloader, events


@pytest.fixture(autouse=True)
def isolated(tmp_path, monkeypatch):
    monkeypatch.setenv("XTREAM_CACHE_DIR", str(tmp_path / "cache"))
    monkeypatch.setenv("XTREAM_DOWNLOAD_DIR", str(tmp_path / "downloads"))


class FakeSession:
    def __init__(self, fail=False):
        self.sent: list[dict] = []
        self.fail = fail

    async def send_log_message(self, level, data, logger):
        if self.fail:
            raise ConnectionError("client gone")
        assert logger == "events" and level == "notice"
        self.sent.append(data)


def finished_job(state="done", **extra):
    job = downloader.new_job(stream_id=1, channel_name="BBC One NI", title="Sunday Politics",
                             start=1792314000, stop=1792315800, fmt="mkv",
                             server_tz=None, server_offset=None)
    job.update(state=state, size=1_288_599_318, **extra)
    downloader.write_job(job)  # stamps updated_at = now
    return job


def run(coro):
    return asyncio.run(coro)


def test_announces_jobs_that_finish_after_subscribing():
    old = finished_job()
    time.sleep(0.01)
    hub, session = events.EventHub(), FakeSession()
    hub.subscribe("markeybot", session)
    time.sleep(0.01)
    new = finished_job()
    assert run(hub.tick()) == 1
    assert [e["job_id"] for e in session.sent] == [new["id"]]  # not the old one
    assert run(hub.tick()) == 0  # announced once only
    assert old["id"] not in {e["job_id"] for e in session.sent}


def test_payloads():
    done = events.describe(dict(finished_job(), warning="ffmpeg not found"))
    assert done["event"] == "download_finished" and done["size_mb"] == 1288.6
    assert "1.3 GB" in done["message"] and "ffmpeg not found" in done["message"]
    failed = events.describe(dict(finished_job("failed", error="Server returned HTTP 404")))
    assert failed["event"] == "download_failed" and "HTTP 404" in failed["message"]
    assert events.describe(finished_job("cancelled"))["event"] == "download_cancelled"


def test_running_jobs_are_not_announced():
    hub, session = events.EventHub(), FakeSession()
    hub.subscribe("bot", session)
    time.sleep(0.01)
    job = downloader.new_job(stream_id=1, channel_name="C", title="T", start=0, stop=60,
                             fmt="ts", server_tz=None, server_offset=None)
    job["state"] = "done"  # final, but never written -> invisible
    run(hub.tick())
    assert session.sent == []


def test_failed_delivery_is_retried():
    hub, bad = events.EventHub(), FakeSession(fail=True)
    hub.subscribe("bot", bad)
    time.sleep(0.01)
    job = finished_job()
    assert run(hub.tick()) == 0
    good = FakeSession()
    hub.subscribe("bot", good)  # client reconnected
    assert run(hub.tick()) == 1 and good.sent[0]["job_id"] == job["id"]


def test_catches_up_after_server_restart():
    hub = events.EventHub()
    hub.subscribe("bot", FakeSession())
    time.sleep(0.01)
    first = finished_job()
    run(hub.tick())
    # Server restarts (e.g. the bot restarted it); a download finishes meanwhile.
    time.sleep(0.01)
    missed = finished_job()
    hub2, session2 = events.EventHub(), FakeSession()
    hub2.subscribe("bot", session2)
    run(hub2.tick())
    ids = [e["job_id"] for e in session2.sent]
    assert ids == [missed["id"]]  # caught up, and no repeat of the first
    assert first["id"] not in ids


def test_no_polling_without_subscribers(monkeypatch):
    calls = []
    monkeypatch.setattr(downloader, "list_jobs", lambda: calls.append(1) or [])
    assert run(events.EventHub().tick()) == 0 and not calls


def test_end_to_end_over_stdio(tmp_path):
    """The way markeybot connects: an MCP ClientSession over stdio with a
    logging callback, receiving logger="events" notifications."""
    import json
    import os
    import sys
    import uuid
    from pathlib import Path

    from mcp import ClientSession, StdioServerParameters
    from mcp.client.stdio import stdio_client

    exe = Path(sys.executable).with_name("mcp-xtream")
    params = StdioServerParameters(command=str(exe), env={
        "XTREAM_SERVER": "http://127.0.0.1:9", "XTREAM_USERNAME": "x", "XTREAM_PASSWORD": "x",
        "XTREAM_CACHE_DIR": os.environ["XTREAM_CACHE_DIR"],
        "XTREAM_DOWNLOAD_DIR": os.environ["XTREAM_DOWNLOAD_DIR"],
    })
    received = []

    async def on_log(message):
        if message.logger == "events":
            received.append(message.data)

    async def scenario():
        async with (
            stdio_client(params) as (r, w),
            ClientSession(r, w, logging_callback=on_log) as session,
        ):
            await session.initialize()
            await session.call_tool("subscribe_events", {})
            await asyncio.sleep(0.2)
            jid = uuid.uuid4().hex[:12]
            jobs = Path(os.environ["XTREAM_CACHE_DIR"], "jobs")
            jobs.mkdir(parents=True, exist_ok=True)
            (jobs / f"{jid}.json").write_text(json.dumps({
                "id": jid, "state": "done", "title": "Sunday Politics",
                "channel_name": "BBC One NI", "output": "/x.mkv", "size": 1_288_599_318,
                "uses_lock": True, "created_at": time.time(), "updated_at": time.time(),
            }))
            for _ in range(40):  # the poller runs every POLL_SECONDS
                if received:
                    break
                await asyncio.sleep(0.5)
            return jid

    jid = run(scenario())
    assert received, "no event delivered"
    assert received[0]["event"] == "download_finished" and received[0]["job_id"] == jid
    assert "Sunday Politics" in received[0]["message"]
