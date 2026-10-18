"""MCP server for Xtream Codes IPTV: TV guide search and catchup downloads."""


def main() -> None:
    from .server import main as run

    run()
