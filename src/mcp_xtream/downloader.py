"""Catchup downloads as detached background processes.

The MCP server only lives as long as the client session, so each download
runs in its own process (`python -m mcp_xtream.downloader <job_id> <lock_fd>`)
and reports progress through a JSON job file that the server reads back.

Ownership: only the worker writes a job's file once it's running. The server
requests cancellation by creating `<id>.cancel` (plus SIGTERM to hurry it), so
the two never overwrite each other's updates.

Liveness: every job has a lock file. The server takes an exclusive flock on it
before writing the job and hands the locked descriptor to the worker, which
keeps it until it exits. The kernel drops the lock however the worker dies
(crash, SIGKILL, reboot), so "lock is free" reliably means "no worker" —
no PIDs to go stale or be reused, and no zombies to mistake for live workers.

Provider quirks handled here:
  * The .ts body starts with a partial packet, so leading bytes up to the
    first MPEG-TS sync (0x47 every 188 bytes) are trimmed.
  * Connections can drop mid-transfer; we resume with an HTTP Range request
    from the exact byte reached (the server answers 206 with the same total).
"""

from __future__ import annotations

import fcntl
import json
import os
import re
import shutil
import signal
import subprocess
import sys
import tempfile
import threading
import time
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path

import httpx

from .client import (
    STREAM_USER_AGENT,
    XtreamClient,
    describe_http_error,
    server_utc_offset,
)
from .config import download_dir, jobs_dir, load_credentials
from .timeutil import local_tz

ACTIVE_STATES = ("queued", "downloading", "converting")
MAX_RETRIES = 6
TS_PACKET = 188
TITLE_MAX_BYTES = 120
CHANNEL_MAX_BYTES = 60

# Serialises read-modify-write of job files between the server's tool threads.
_write_lock = threading.Lock()
# Workers we spawned; polled so they don't linger as zombies.
_children: dict[str, subprocess.Popen] = {}


# -- job files ---------------------------------------------------------------


def _job_path(job_id: str) -> Path:
    if not re.fullmatch(r"[0-9a-f]{12}", job_id):
        raise ValueError(f"Invalid job id: {job_id}")
    return jobs_dir() / f"{job_id}.json"


def _lock_path(job_id: str) -> Path:
    return _job_path(job_id).with_suffix(".lock")


def _cancel_path(job_id: str) -> Path:
    return _job_path(job_id).with_suffix(".cancel")


def cancel_requested(job_id: str) -> bool:
    return _cancel_path(job_id).exists()


def read_job(job_id: str) -> dict | None:
    try:
        return json.loads(_job_path(job_id).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None


def write_job(job: dict) -> None:
    job["updated_at"] = time.time()
    path = _job_path(job["id"])
    # Unique temp name per write so concurrent writers can't clobber each
    # other's temp file mid-replace.
    fd, tmp = tempfile.mkstemp(dir=path.parent, prefix=f".{job['id']}.", suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(job, f, indent=2)
        os.replace(tmp, path)
    except BaseException:
        Path(tmp).unlink(missing_ok=True)
        raise


def _reap() -> None:
    for job_id, proc in list(_children.items()):
        if proc.poll() is not None:
            del _children[job_id]


def _legacy_pid_alive(pid: int | None) -> bool:
    if not pid:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def worker_alive(job: dict) -> bool:
    """True while something holds the job's lock (its worker, or the server
    between creating the job and spawning the worker)."""
    _reap()
    try:
        fd = os.open(_lock_path(job["id"]), os.O_RDWR)
    except FileNotFoundError:
        if job.get("uses_lock"):
            return False
        # Job from a version without lock files whose worker may still be
        # running after an upgrade: fall back to the old pid check.
        return _legacy_pid_alive(job.get("pid"))
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        return True
    else:
        fcntl.flock(fd, fcntl.LOCK_UN)
        return False
    finally:
        os.close(fd)


def refresh_state(job: dict) -> dict:
    """Settle jobs whose worker is gone but which never reached a final state
    (crashed, killed, or cancelled before it could clean up)."""
    if job.get("state") not in ACTIVE_STATES or worker_alive(job):
        return job
    with _write_lock:
        # Re-read: the worker writes its final state before releasing the
        # lock, so the file is authoritative now.
        fresh = read_job(job["id"]) or job
        if fresh.get("state") in ACTIVE_STATES:
            if fresh.get("part"):
                Path(fresh["part"]).unlink(missing_ok=True)
            if cancel_requested(fresh["id"]) or fresh.get("cancel_requested"):
                fresh["state"] = "cancelled"
            else:
                fresh["state"] = "failed"
                fresh["error"] = fresh.get("error") or "Download process exited unexpectedly"
            write_job(fresh)
        return fresh


def list_jobs() -> list[dict]:
    jobs = []
    for path in jobs_dir().glob("*.json"):
        job = read_job(path.stem)
        if job:
            jobs.append(refresh_state(job))
    return sorted(jobs, key=lambda j: j.get("created_at", 0), reverse=True)


def active_job() -> dict | None:
    for job in list_jobs():
        if job.get("state") in ACTIVE_STATES:
            return job
    return None


@contextmanager
def spawn_lock() -> Iterator[None]:
    """Serialise check-for-active-job → create → spawn, across the server's
    tool threads and across separate server processes."""
    fd = os.open(jobs_dir() / "spawn.lock", os.O_RDWR | os.O_CREAT, 0o600)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX)
        yield
    finally:
        fcntl.flock(fd, fcntl.LOCK_UN)
        os.close(fd)


# -- creating, starting and cancelling ------------------------------------------


def _clean_name(text: str) -> str:
    text = re.sub(r'[\\/:*?"<>|\x00-\x1f]', " ", text)
    return " ".join(text.split())


def _truncate_bytes(text: str, limit: int) -> str:
    """Cut to at most `limit` UTF-8 bytes without splitting a character."""
    return text.encode("utf-8")[:limit].decode("utf-8", "ignore").rstrip()


def output_stem(title: str, channel_name: str, start: int) -> str:
    """'<title> - <channel> - <YYYY-MM-DD HHMM>' in the user's timezone,
    each part capped in bytes so the name fits filesystem limits (255 bytes)
    even for non-Latin titles, while keeping the channel and date."""
    title = _truncate_bytes(_clean_name(title), TITLE_MAX_BYTES) or "programme"
    channel = _truncate_bytes(_clean_name(channel_name), CHANNEL_MAX_BYTES) or "channel"
    when = datetime.fromtimestamp(start, tz=local_tz()).strftime("%Y-%m-%d %H%M")
    return f"{title} - {channel} - {when}"


def jellyfin_layout(
    show: str, season: int | None, episode: int | None, episode_title: str, start: int
) -> tuple[str, str]:
    """(folder, stem) in Jellyfin's TV layout.

    With season+episode: 'Show/Season 02/Show - S02E05 - Title'. Without:
    Jellyfin's air-date naming, 'Show/Show - 2026-10-18'.
    """
    show = _truncate_bytes(_clean_name(show), 80) or "Unknown show"
    if season is not None and episode is not None:
        stem = f"{show} - S{season:02d}E{episode:02d}"
        if title := _truncate_bytes(_clean_name(episode_title), 80):
            stem += f" - {title}"
        return f"{show}/Season {season:02d}", stem
    when = datetime.fromtimestamp(start, tz=local_tz()).strftime("%Y-%m-%d")
    return show, f"{show} - {when}"


def write_nfo(path: Path, nfo: dict) -> None:
    """Jellyfin/Kodi <episodedetails> sidecar."""
    import xml.etree.ElementTree as ET

    root = ET.Element("episodedetails")
    for key in ("title", "showtitle", "season", "episode", "aired", "plot", "studio"):
        if nfo.get(key) not in (None, ""):
            ET.SubElement(root, key).text = str(nfo[key])
    root.append(ET.Comment(
        " Season/episode were supplied by the requesting agent (best effort), "
        "not by the TV guide. "
    ))
    ET.indent(root)
    path.write_bytes(ET.tostring(root, encoding="utf-8", xml_declaration=True))


def _unique_path(path: Path) -> Path:
    n = 2
    candidate = path
    while candidate.exists() or candidate.with_suffix(".part.ts").exists():
        candidate = path.with_name(f"{path.stem} ({n}){path.suffix}")
        n += 1
    return candidate


def new_job(
    *,
    stream_id: int,
    channel_name: str,
    title: str,
    start: int,
    stop: int,
    fmt: str,
    server_tz: str | None,
    server_offset: int | None,
    folder: str | None = None,
    stem: str | None = None,
    nfo: dict | None = None,
) -> dict:
    """Build (but don't write or start) a job, choosing a free output name.

    folder/stem override the default flat '<title> - <channel> - <time>'
    name (used for Jellyfin layout); nfo is written next to the finished file.
    """
    ext = "mkv" if fmt == "mkv" else "ts"
    base = download_dir()
    if folder:
        base = base.joinpath(*folder.split("/"))
        base.mkdir(parents=True, exist_ok=True)
    output = _unique_path(base / f"{stem or output_stem(title, channel_name, start)}.{ext}")
    job = {
        "id": uuid.uuid4().hex[:12],
        "state": "queued",
        "stream_id": stream_id,
        "channel_name": channel_name,
        "title": title,
        "start": start,
        "stop": stop,
        "format": ext,
        "server_tz": server_tz,
        "server_offset": server_offset,
        "output": str(output),
        "part": str(output.with_suffix(".part.ts")),
        "bytes_done": 0,
        "bytes_total": None,
        "retries": 0,
        "error": None,
        "warning": None,
        "pid": None,
        "uses_lock": True,  # liveness via <id>.lock, never the pid
        "nfo": nfo,
        "created_at": time.time(),
    }
    return job


def start_job(**kwargs) -> dict:
    """Create the job file and launch its worker. Call under spawn_lock().
    Takes the same keyword arguments as new_job()."""
    job = new_job(**kwargs)
    # Hold the job's lock from before it's visible until the worker owns it.
    lock_fd = os.open(_lock_path(job["id"]), os.O_RDWR | os.O_CREAT, 0o600)
    try:
        fcntl.flock(lock_fd, fcntl.LOCK_EX)
        write_job(job)
        log = open(jobs_dir() / f"{job['id']}.log", "ab")  # noqa: SIM115 - handed to child
        try:
            proc = subprocess.Popen(
                # -P: don't import stray modules from the working directory
                # (unlike -I this keeps PYTHONPATH / user site installs working).
                [sys.executable, "-P", "-m", "mcp_xtream.downloader", job["id"], str(lock_fd)],
                stdin=subprocess.DEVNULL,
                stdout=log,
                stderr=log,
                cwd=jobs_dir(),
                pass_fds=(lock_fd,),
                start_new_session=True,  # survive the MCP server exiting
            )
        except OSError as e:
            job.update(state="failed", error=f"Couldn't start the download process: {e}")
            write_job(job)
            raise
        finally:
            log.close()
    finally:
        os.close(lock_fd)  # the worker keeps its inherited copy
    # The worker records its own pid; we keep the handle to reap it and to
    # signal it before it has.
    _children[job["id"]] = proc
    return job


def cancel_job(job_id: str) -> tuple[dict, str] | None:
    """Request cancellation. Returns (job, message) or None if unknown."""
    job = read_job(job_id)
    if job is None:
        return None
    job = refresh_state(job)
    if job.get("state") not in ACTIVE_STATES:
        return job, f"Nothing to cancel: the download is already {job['state']}"
    # The worker polls for this marker; refresh_state uses it if the worker
    # dies first. Not written into the job file, which the worker owns.
    _cancel_path(job_id).touch()
    child = _children.get(job_id)
    pid = job.get("pid") or (child.pid if child else None)
    if pid and worker_alive(job):
        # Safe to signal: the lock proves the worker that owns this pid is alive.
        try:
            os.kill(pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
    return job, "Cancelling; the partial file will be removed"


# -- worker ------------------------------------------------------------------


def find_ts_sync(buf: bytes) -> int:
    """Offset of the first byte that starts three consecutive TS packets."""
    for i in range(min(len(buf) - 2 * TS_PACKET, TS_PACKET * 4)):
        if buf[i] == 0x47 and buf[i + TS_PACKET] == 0x47 and buf[i + 2 * TS_PACKET] == 0x47:
            return i
    return -1


class _Cancelled(Exception):
    pass


class _HttpClientError(RuntimeError):
    """A 4xx from the provider: retrying won't help."""


# Providers archive catchup with a delay; a 404 this soon after a programme
# ends most likely means "not ready yet" rather than "doesn't exist".
RECENT_SECONDS = 6 * 3600


class Worker:
    def __init__(self, job: dict):
        self.job = job
        self.cancelled = False
        self.part = Path(job["part"])

    def _cancel_wanted(self) -> bool:
        if not self.cancelled and cancel_requested(self.job["id"]):
            self.cancelled = True
        return self.cancelled

    def _check_cancel(self) -> None:
        if self._cancel_wanted():
            raise _Cancelled

    def _save(self, **fields) -> None:
        self.job.update(fields)
        write_job(self.job)

    def run(self) -> None:
        creds = None
        try:
            self._check_cancel()
            self._save(state="downloading", pid=os.getpid())
            creds = load_credentials()
            url = self._catchup_url(creds)
            self._download(url, creds)
            self._check_cancel()
            self._finish()
        except _Cancelled:
            self.part.unlink(missing_ok=True)
            self._save(state="cancelled")
        except Exception as e:
            # A partial file can't be resumed by a later job, so don't leave
            # gigabytes of it in the user's video folder.
            self.part.unlink(missing_ok=True)
            message = str(e) or type(e).__name__
            self._save(state="failed", error=creds.redact(message) if creds else message)
            raise

    def _http_error(self, status: int) -> Exception:
        if status == 404:
            if time.time() - self.job["stop"] < RECENT_SECONDS:
                return _HttpClientError(
                    "The provider has no catchup recording for this yet (HTTP 404). It "
                    "ended recently, and providers often take a few hours to make "
                    "recordings available, so try again later or try another channel "
                    "carrying the programme."
                )
            return _HttpClientError(
                "The provider has no catchup recording for this slot (HTTP 404); "
                "try another channel that carries the programme."
            )
        if 400 <= status < 500:
            return _HttpClientError(f"Server returned HTTP {status}")
        return RuntimeError(f"Server returned HTTP {status}")

    def _catchup_url(self, creds) -> str:
        job = self.job
        tz, offset = job.get("server_tz"), job.get("server_offset")
        with XtreamClient(creds) as client:
            server_info = client.authenticate().get("server_info") or {}
        tz = server_info.get("timezone") or tz
        fresh_offset = server_utc_offset(server_info)
        if fresh_offset is not None:  # 0 (a UTC server) is a valid offset
            offset = fresh_offset
        minutes = max(1, -(-(job["stop"] - job["start"]) // 60))
        return client.catchup_url(job["stream_id"], job["start"], minutes, tz, offset)

    def _download(self, url: str, creds) -> None:
        # server_pos counts bytes received from the server; the file holds
        # server_pos - trimmed bytes (leading partial packet dropped). Both are
        # updated per chunk so a mid-transfer failure resumes at the right byte.
        self.part.unlink(missing_ok=True)
        self.server_pos, self.trimmed = 0, 0
        total = None
        verifying = False  # probing whether a length-less body really ended
        failures = 0
        with httpx.Client(
            follow_redirects=True,
            timeout=httpx.Timeout(30, read=60),
            headers={"User-Agent": STREAM_USER_AGENT},
        ) as http:
            while True:
                self._check_cancel()
                progress_at_start = self.server_pos
                try:
                    pos = self.server_pos
                    headers = {"Range": f"bytes={pos}-"} if pos else {}
                    with http.stream("GET", url, headers=headers) as resp:
                        if verifying and resp.status_code == 416:
                            break  # nothing past pos: the download was complete
                        if verifying and resp.status_code != 206:
                            # Can't tell (Range ignored, or an odd error for an
                            # out-of-range request): keep what we have, flagged.
                            self._save(warning="The server didn't report the recording size, "
                                       "so the download couldn't be checked for completeness")
                            break
                        if pos and resp.status_code == 206:
                            m = re.match(r"bytes (\d+)-\d+/(\d+)", resp.headers.get("content-range", ""))
                            if not m or int(m.group(1)) != pos:
                                raise RuntimeError("Server resumed at the wrong offset")
                            total = int(m.group(2))
                        elif resp.status_code == 200:
                            # Fresh body (first attempt, or Range ignored): start over.
                            self.server_pos, self.trimmed = 0, 0
                            self.part.unlink(missing_ok=True)
                            length = resp.headers.get("content-length")
                            total = int(length) if length else None
                        else:
                            raise self._http_error(resp.status_code)
                        verifying = False
                        self._save(bytes_total=total)
                        self._stream_body(resp)
                except (httpx.HTTPError, RuntimeError) as e:
                    if isinstance(e, _HttpClientError):
                        raise
                    failures = 0 if self.server_pos - progress_at_start > 1_000_000 else failures + 1
                    if failures > MAX_RETRIES:
                        reason = describe_http_error(e) if isinstance(e, httpx.HTTPError) else str(e)
                        raise RuntimeError(f"Download kept failing: {creds.redact(reason)}") from None
                    self._save(retries=self.job["retries"] + 1)
                    time.sleep(min(30, 2**failures))
                    continue
                if total is None:
                    # A clean close without a length can't be told apart from a
                    # dropped connection; ask for anything past this point.
                    verifying = True
                    continue
                if self.server_pos >= total:
                    break
                # Connection closed early without an error: resume.
                failures = 0 if self.server_pos - progress_at_start > 1_000_000 else failures + 1
                if failures > MAX_RETRIES:
                    raise RuntimeError("Server keeps closing the connection early")
                self._save(retries=self.job["retries"] + 1)
                time.sleep(2)

    def _stream_body(self, resp) -> None:
        head = b"" if self.server_pos == 0 else None
        last_save = 0.0
        try:
            with open(self.part, "ab") as f:
                for chunk in resp.iter_bytes(256 * 1024):
                    self._check_cancel()
                    if head is not None:
                        # Buffer the start until we can find the first TS sync.
                        head += chunk
                        if len(head) < TS_PACKET * 8:
                            continue
                        self.trimmed = max(find_ts_sync(head), 0)
                        f.write(head[self.trimmed :])
                        self.server_pos += len(head)
                        head = None
                    else:
                        f.write(chunk)
                        self.server_pos += len(chunk)
                    now = time.monotonic()
                    if now - last_save > 1:
                        last_save = now
                        self._save(bytes_done=self.server_pos - self.trimmed, state="downloading")
                if head:  # tiny body
                    f.write(head)
                    self.server_pos += len(head)
        finally:
            self._save(bytes_done=self.server_pos - self.trimmed)

    def _remux(self, ffmpeg: str, tmp: Path) -> bool:
        """ffmpeg -c copy to MKV; killed promptly if the job is cancelled."""
        proc = subprocess.Popen(
            [ffmpeg, "-nostdin", "-loglevel", "error", "-y",
             "-fflags", "+discardcorrupt+genpts", "-i", str(self.part),
             "-map", "0:v?", "-map", "0:a?", "-map", "0:s?",
             "-c", "copy", str(tmp)],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        try:
            while proc.poll() is None:
                if self._cancel_wanted():
                    proc.terminate()
                    try:
                        proc.wait(timeout=10)
                    except subprocess.TimeoutExpired:
                        proc.kill()
                        proc.wait()
                    tmp.unlink(missing_ok=True)
                    raise _Cancelled
                time.sleep(0.25)
        finally:
            if proc.poll() is None:
                proc.kill()
        return proc.returncode == 0 and tmp.exists() and tmp.stat().st_size > 0

    def _finish(self) -> None:
        output = Path(self.job["output"])
        if self.part.stat().st_size < 1_000_000:
            raise RuntimeError("Downloaded file is too small; the recording may not exist")
        if self.job["format"] == "mkv":
            ffmpeg = shutil.which("ffmpeg")
            if ffmpeg:
                self._save(state="converting")
                tmp = output.with_suffix(".tmp.mkv")
                if self._remux(ffmpeg, tmp):
                    os.replace(tmp, output)
                    self.part.unlink(missing_ok=True)
                    self._done(output)
                    return
                tmp.unlink(missing_ok=True)
                warning = "MKV conversion failed; kept the original .ts"
            else:
                warning = "ffmpeg not found; saved as .ts instead of .mkv"
            output = _unique_path(output.with_suffix(".ts"))
            self.job["warning"] = warning
        os.replace(self.part, output)
        self._done(output)

    def _done(self, output: Path) -> None:
        if self.job.get("nfo"):
            try:
                write_nfo(output.with_suffix(".nfo"), self.job["nfo"])
            except OSError as e:
                self.job["warning"] = f"Couldn't write the .nfo file: {e.strerror or e}"
        self._save(state="done", output=str(output), size=output.stat().st_size)


def main() -> None:
    worker: Worker | None = None
    pending = []

    # Install the handler before anything slow so an early cancel isn't lost.
    def on_term(*_):
        if worker is None:
            pending.append(True)
        else:
            worker.cancelled = True

    signal.signal(signal.SIGTERM, on_term)
    job_id = sys.argv[1]
    # argv[2] is the inherited, already-locked descriptor for the job's lock
    # file; keep it open (unused) so the lock lives exactly as long as we do.
    _lock_fd = int(sys.argv[2]) if len(sys.argv) > 2 else None
    job = read_job(job_id)
    if not job:
        sys.exit(f"Job not found: {job_id}")
    worker = Worker(job)
    worker.cancelled = bool(pending)
    worker.run()


if __name__ == "__main__":
    main()
