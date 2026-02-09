"""
Control panel UI — served from gateway/ui.html at /_gateway/ui.

The HTML is read from disk on every request so you can edit ui.html
and refresh the browser without restarting the gateway.
"""

from pathlib import Path

_UI_FILE = Path(__file__).parent / "ui.html"


def load_html() -> str:
    """Read the UI HTML from disk (hot-reloadable)."""
    return _UI_FILE.read_text(encoding="utf-8")
