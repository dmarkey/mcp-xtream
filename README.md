# mcp-xtream

[![CI](https://github.com/dmarkey/mcp-xtream/actions/workflows/ci.yml/badge.svg)](https://github.com/dmarkey/mcp-xtream/actions/workflows/ci.yml)
[![Python 3.13+](https://img.shields.io/badge/python-3.13%2B-blue.svg)](https://www.python.org/)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

An [MCP](https://modelcontextprotocol.io) server for **Xtream Codes** IPTV
subscriptions. It lets an AI assistant search your provider's TV guide and
download past programmes from its catchup archive, saved ready for Jellyfin or
Plex.

> **You:** Download Sunday Politics from this morning, BBC One NI.
>
> **Assistant:** Found it: *Sunday Politics*, 10:00–10:30 on IRL: BBC ONE
> NORTHERN 4K (catchup available). Downloading in the background… done:
> `~/Movies/IPTV/Sunday Politics/Sunday Politics - 2026-10-18.mkv` (1.3 GB, 29:44).

Other things to ask:

- *"Is there any snooker in the last three days?"*
- *"What's on RTÉ One tonight?"*
- *"Get last night's Gogglebox for my Jellyfin library."*
- *"How long is left on my IPTV subscription?"*

## Features

- **Guide search** across thousands of channels, past and upcoming, with
  every channel carrying a programme and whether it can still be downloaded.
- **Background catchup downloads** that keep going after the chat ends,
  resume dropped connections from the exact byte, and convert to `.mkv`
  (no re-encode, keeps subtitles) when `ffmpeg` is installed.
- **Jellyfin/Plex naming** (`Show/Season 02/Show - S02E05.mkv` plus an
  `.nfo` file) when the assistant has identified the episode, or date-based
  naming when it hasn't.
- **Handles provider quirks**: catchup times in the server's own timezone,
  "+1" channels that reuse the main channel's guide, duplicated or overlapping
  guide entries, and guides that only reach a day back (past programmes are
  kept for 7 days so the whole catchup window stays searchable).
- **Tells you when a download is done**: clients that support server-push
  events get a `download_finished` / `download_failed` notification instead of
  having to poll.
- **Respects your connection limit**: won't start a download while all of
  your account's connections are in use (e.g. you're watching TV).
- **Keeps your password private**: stream URLs contain it, so no tool ever
  returns one, and errors never include it.

## Quick start

You need [uv](https://docs.astral.sh/uv/getting-started/installation/).
`uvx` downloads and runs the server in an isolated environment, fetching
Python 3.13 if needed; nothing else to install. `ffmpeg` is optional (for
`.mkv` output).

### Claude Code

```bash
claude mcp add xtream \
  -e XTREAM_SERVER=http://your-provider.example:80 \
  -e XTREAM_USERNAME=your-username \
  -e XTREAM_PASSWORD=your-password \
  -- uvx --from git+https://github.com/dmarkey/mcp-xtream mcp-xtream
```

### Claude Desktop and other MCP clients

Add to your client's MCP configuration (for Claude Desktop,
`claude_desktop_config.json`):

```json
{
  "mcpServers": {
    "xtream": {
      "command": "uvx",
      "args": ["--from", "git+https://github.com/dmarkey/mcp-xtream", "mcp-xtream"],
      "env": {
        "XTREAM_SERVER": "http://your-provider.example:80",
        "XTREAM_USERNAME": "your-username",
        "XTREAM_PASSWORD": "your-password"
      }
    }
  }
}
```

### Try it from a terminal

```bash
XTREAM_SERVER=http://your-provider.example:80 \
XTREAM_USERNAME=your-username XTREAM_PASSWORD=your-password \
uvx --from git+https://github.com/dmarkey/mcp-xtream mcp-xtream
```

It speaks MCP over stdio, so it waits silently for a client; use
[MCP Inspector](https://github.com/modelcontextprotocol/inspector) to poke at
the tools by hand.

## Configuration

Everything is set with environment variables.

| Variable | Required | Default |
|----------|----------|---------|
| `XTREAM_SERVER` | yes | — e.g. `http://provider.example:80` (the address your IPTV app uses) |
| `XTREAM_USERNAME` | yes | — |
| `XTREAM_PASSWORD` | yes | — |
| `XTREAM_DOWNLOAD_DIR` | no | `~/Movies/IPTV` (macOS), `~/Videos/IPTV` (Linux) |
| `XTREAM_CACHE_DIR` | no | `~/Library/Caches/mcp-xtream` (macOS), `~/.cache/mcp-xtream` (Linux) |
| `XTREAM_TZ` | no | your system timezone, e.g. `Europe/Dublin` (used to display times) |

The `-e` / `env` settings above store your password in plain text in the
client's config file. If you'd rather not, export the variables from your
shell profile instead and leave them out of the MCP config.

## Tools

| Tool | What it does |
|------|--------------|
| `search_programmes` | Search the guide by title (optionally descriptions or channel), past, now or upcoming. Lists every channel carrying each programme and whether catchup is available |
| `list_channels` | Find channels by name or category, with catchup days and what's on now |
| `channel_schedule` | One channel's guide for a day |
| `check_recording` | Ask the provider whether a channel's recording of a programme actually exists right now (briefly uses a connection) |
| `programme_details` | Full description, air times and channels for one programme, for identifying the episode |
| `download_programme` | Download a past programme in the background, optionally named for Jellyfin/Plex |
| `download_status` / `list_downloads` / `cancel_download` | Track and manage downloads |
| `subscribe_events` | Get pushed a notification when each download finishes, fails or is cancelled |
| `account_info` | Subscription status, expiry, connection limit, active connections |
| `refresh_data` | Re-fetch the channel list and guide now |

## Catch-up availability is a hint

Every channel stays listed (channels without catch-up are still useful for
live TV). Each programme lists the channels carrying it, best bet first, with
a `catchup_status`:

| Status | Meaning |
|--------|---------|
| `in_window` | Within the channel's advertised catch-up window |
| `just_aired` | Ended in the last few hours; the provider may not have the recording yet |
| `airing` / `not_aired_yet` | Not finished yet |
| `expired` | Older than the channel's archive |
| `no_catchup` | The channel has no archive (live only) |

The advertised window is only a hint: one channel's recording can be missing
while another channel carrying the same programme has it, and providers
sometimes lose recordings early. `check_recording` asks the provider directly
(a 1 KB request, refused while your connections are all in use), and the
assistant is told to try other channels before giving up.

## Download notifications

Clients that support server-push events can call `subscribe_events` once after
connecting. Each download that ends is then announced as an MCP log
notification with `logger="events"`:

```json
{"event": "download_finished", "job_id": "c7cba935dee2", "title": "Sunday Politics",
 "channel": "IRL: BBC ONE NORTHERN 4K", "output": "/media/tv/Sunday Politics/Sunday Politics - 2026-10-18.mkv",
 "size_mb": 1288.6, "message": "Download finished: 'Sunday Politics' (1.3 GB) saved to …"}
```

`download_failed` (with `error`) and `download_cancelled` work the same way.
Announcements are tracked per client on disk, so a download that ends while
the server is restarting is still announced when the client resubscribes.
For [markeybot](https://github.com/dmarkey/markeybot), set `"events": true` on
the server entry and it subscribes automatically.

Note: MCP's logging capability, which these notifications ride on, is marked
deprecated in the 2026-07-28 spec (SEP-2577). Clients that still deliver log
notifications (the MCP Python SDK's `ClientSession` with a `logging_callback`,
markeybot) work; others just won't see the events, and `download_status` still
works for everyone.

## Episode numbers are best effort (and the assistant's job)

Xtream guides almost never carry season/episode numbers (one provider tested
had them for 0.1% of programmes), and repeats air long after the original
broadcast, so the server doesn't guess. Instead the assistant reads the
description with `programme_details`, identifies the episode itself (web
search, TheTVDB, TMDB, TVmaze or the broadcaster's site) and passes what it
found to `download_programme`:

| Arguments | Saved as |
|-----------|----------|
| `show`, `season`, `episode` (+ optional `episode_title`) | `Show/Season 02/Show - S02E05 - Title.mkv` + `.nfo` |
| `show` only | `Show/Show - 2026-10-18.mkv` + `.nfo` (Jellyfin's air-date naming) |
| neither | `<title> - <channel> - <date time>.mkv` |

Treat the numbers as a best guess: broadcasters and databases often number the
same episode differently (e.g. "series 18" vs "season 28"), so tell the
assistant which database your media server uses. The `.nfo` file records the
description and air date and notes that the numbering came from the assistant.

## How it works

**The guide.** The channel list and XMLTV guide are fetched from your
provider and cached on disk (refreshed when over 6 hours and 3 hours old
respectively, the next time a tool is used; `refresh_data` forces it).
Overlapping entries are resolved, mangled titles cleaned up, and past
programmes kept for 7 days. If a refresh fails, the previous copy is used and
results carry a `warning`.

**"+1" channels.** Providers often point "+1" channels at the main channel's
guide. When a channel's name contains "+1" but its guide doesn't, its times are
shifted an hour later everywhere, so the right hour is downloaded.

**Downloads.** Each download runs as a detached process, so it survives the
assistant session ending, and holds a lock that the operating system releases
however it exits, so a crashed download never blocks the next one. Catchup URLs
use the start time in the *provider's* timezone (many providers aren't on
UTC). Dropped connections resume with an HTTP Range request from the last byte
written. Only one download runs at a time, and cancelling also stops an
in-progress `.mkv` conversion and removes partial files.

## Requirements and limitations

- macOS or Linux (downloads use POSIX file locking; Windows isn't supported).
- Python 3.13+ (`uvx` fetches it automatically).
- Your provider must support Xtream Codes catchup (`tv_archive`) for downloads;
  how far back depends on the channel (often 1–7 days).
- A download uses one of your account's connections. With a one-connection
  account you can't watch and download at the same time.

## Development

```bash
git clone https://github.com/dmarkey/mcp-xtream
cd mcp-xtream
uv sync
uv run pytest            # unit + end-to-end tests against local fake servers
uv run ruff check src tests
```

The tests never contact a real provider.

## Disclaimer

This is an unofficial client for the Xtream Codes API. Use it only with IPTV
services you are entitled to use, and respect your provider's terms and the
rights of content owners. Downloads are for personal, time-shifted viewing.

## License

[MIT](LICENSE)
