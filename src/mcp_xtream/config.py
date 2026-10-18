"""Credentials and paths, configured through environment variables.

XTREAM_SERVER, XTREAM_USERNAME and XTREAM_PASSWORD are required. The password
never leaves this process except in requests to the provider itself.
"""

from __future__ import annotations

import os
import sys
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import quote, quote_plus

REQUIRED_VARS = ("XTREAM_SERVER", "XTREAM_USERNAME", "XTREAM_PASSWORD")


@dataclass(frozen=True)
class Credentials:
    server_url: str
    username: str
    password: str
    source: str  # where they were loaded from, for diagnostics

    def redact(self, text: str) -> str:
        """Strip the password from text that might be shown to the model,
        including the percent-encoded forms it takes inside URLs."""
        if not self.password:
            return text
        forms = {self.password, quote(self.password), quote(self.password, safe=""),
                 quote_plus(self.password)}
        for form in sorted(forms, key=len, reverse=True):
            text = text.replace(form, "***")
        return text


class ConfigError(RuntimeError):
    pass


def load_credentials() -> Credentials:
    values = {var: os.environ.get(var, "").strip() for var in REQUIRED_VARS}
    missing = [var for var, value in values.items() if not value]
    if missing:
        raise ConfigError(
            f"Missing environment variable(s): {', '.join(missing)}. "
            "Set XTREAM_SERVER (e.g. http://provider.example:80), "
            "XTREAM_USERNAME and XTREAM_PASSWORD."
        )
    return Credentials(
        values["XTREAM_SERVER"],
        values["XTREAM_USERNAME"],
        values["XTREAM_PASSWORD"],
        "environment",
    )


def cache_dir() -> Path:
    if env := os.environ.get("XTREAM_CACHE_DIR"):
        path = Path(env)
    elif sys.platform == "darwin":
        path = Path.home() / "Library" / "Caches" / "mcp-xtream"
    else:
        base = os.environ.get("XDG_CACHE_HOME") or Path.home() / ".cache"
        path = Path(base) / "mcp-xtream"
    path.mkdir(parents=True, exist_ok=True)
    return path


def jobs_dir() -> Path:
    path = cache_dir() / "jobs"
    path.mkdir(parents=True, exist_ok=True)
    return path


def download_dir() -> Path:
    if env := os.environ.get("XTREAM_DOWNLOAD_DIR"):
        path = Path(env).expanduser()
    elif sys.platform == "darwin":
        path = Path.home() / "Movies" / "IPTV"
    else:
        path = Path.home() / "Videos" / "IPTV"
    path.mkdir(parents=True, exist_ok=True)
    return path
