"""Xtream Codes panel API client (player_api.php, xmltv.php, timeshift URLs)."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta, timezone
from typing import Self
from urllib.parse import quote, urlparse, urlunparse
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import httpx

from .config import Credentials

# Some panels reject unknown user agents on the API; streams accept anything.
API_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)
STREAM_USER_AGENT = "Lavf/61.7.100"


class XtreamError(RuntimeError):
    pass


def describe_http_error(e: httpx.HTTPError) -> str:
    """A short description of a transport error that never includes the
    request URL (it carries the password in the query string)."""
    return type(e).__name__


def normalize_server_url(url: str) -> str:
    url = url.strip()
    if "://" not in url:
        url = "http://" + url
    parsed = urlparse(url)
    if not parsed.netloc:
        raise XtreamError(f"Invalid server URL: {url}")
    return urlunparse((parsed.scheme, parsed.netloc, "", "", "", ""))


def server_utc_offset(server_info: dict) -> int | None:
    """Server's UTC offset in seconds, from its wall clock vs unix time."""
    try:
        local = datetime.strptime(str(server_info["time_now"]), "%Y-%m-%d %H:%M:%S")
        utc = datetime.fromtimestamp(int(server_info["timestamp_now"]), tz=UTC)
    except (KeyError, ValueError, TypeError, OSError):
        return None
    offset = (local - utc.replace(tzinfo=None)).total_seconds()
    return int(round(offset / 900) * 900)


def server_local_time(
    ts: int, tz_name: str | None = None, utc_offset: int | None = None
) -> datetime:
    """Timeshift URLs take the start time in the *server's* local time.

    Prefer the IANA zone (correct across DST), then a fixed offset, then UTC.
    """
    if tz_name:
        try:
            return datetime.fromtimestamp(ts, tz=ZoneInfo(tz_name))
        except (ZoneInfoNotFoundError, ValueError):
            pass  # Unknown zone name; fall back to the offset.
    if utc_offset is not None:
        return datetime.fromtimestamp(ts, tz=timezone(timedelta(seconds=utc_offset)))
    return datetime.fromtimestamp(ts, tz=UTC)


class XtreamClient:
    def __init__(self, creds: Credentials, timeout: float = 30):
        self.creds = creds
        self.server_url = normalize_server_url(creds.server_url)
        self._http = httpx.Client(
            timeout=timeout,
            follow_redirects=True,
            headers={"User-Agent": API_USER_AGENT},
        )

    def close(self) -> None:
        self._http.close()

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    def _api(self, **params) -> object:
        query = {"username": self.creds.username, "password": self.creds.password}
        query.update({k: str(v) for k, v in params.items()})
        try:
            resp = self._http.get(f"{self.server_url}/player_api.php", params=query)
        except httpx.HTTPError as e:
            raise XtreamError(f"Cannot reach server ({describe_http_error(e)})") from None
        if resp.status_code in (401, 403):
            raise XtreamError(f"Server rejected credentials (HTTP {resp.status_code})")
        if resp.status_code != 200:
            raise XtreamError(f"Server returned HTTP {resp.status_code}")
        try:
            return resp.json()
        except ValueError:
            raise XtreamError("Server returned a non-JSON response") from None

    def authenticate(self) -> dict:
        """Return {'user_info': ..., 'server_info': ...}; raise if not usable."""
        data = self._api()
        if not isinstance(data, dict):
            raise XtreamError("Unexpected auth response")
        user = data.get("user_info") or {}
        if str(user.get("auth")) != "1":
            raise XtreamError(f"Login rejected: {user.get('message') or 'auth failed'}")
        status = str(user.get("status", "")).strip()
        if status and status.lower() != "active":
            raise XtreamError(f"Account status is '{status}'")
        return data

    def live_categories(self) -> list[dict]:
        data = self._api(action="get_live_categories")
        return data if isinstance(data, list) else []

    def live_streams(self) -> list[dict]:
        data = self._api(action="get_live_streams")
        return data if isinstance(data, list) else []

    def xmltv(self) -> bytes:
        try:
            resp = self._http.get(
                f"{self.server_url}/xmltv.php",
                params={"username": self.creds.username, "password": self.creds.password},
                timeout=180,
            )
        except httpx.HTTPError as e:
            raise XtreamError(f"TV guide download failed ({describe_http_error(e)})") from None
        # Not raise_for_status(): its message includes the URL.
        if resp.status_code != 200:
            raise XtreamError(f"TV guide download failed: server returned HTTP {resp.status_code}")
        return resp.content

    def catchup_url(
        self,
        stream_id: int,
        start_ts: int,
        duration_minutes: int,
        tz_name: str | None,
        utc_offset: int | None,
        container: str = "ts",
    ) -> str:
        """Path-style timeshift URL. Contains the password — never return it
        from a tool."""
        start = server_local_time(start_ts, tz_name, utc_offset).strftime(
            "%Y-%m-%d:%H-%M"
        )
        # Escape the credentials: '#', '?', '/', '%' or spaces would otherwise
        # change how the path is parsed.
        user = quote(self.creds.username, safe="")
        password = quote(self.creds.password, safe="")
        return (
            f"{self.server_url}/timeshift/{user}/{password}"
            f"/{duration_minutes}/{start}/{stream_id}.{container}"
        )
