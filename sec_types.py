"""Shared datatypes for the SEC downloader (kept separate to avoid import cycles)."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass
class DownloadResult:
    """Outcome of :func:`download_sec_filings.download_sec_filings`."""

    ok: bool
    exit_code: int
    cik10: str
    entity_name: str
    out_dir: Path | None = None
    error_message: str | None = None
    artifacts: dict[str, bytes] | None = None
