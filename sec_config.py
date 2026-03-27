"""SEC downloader constants, User-Agent, and Windows asyncio policy for Playwright."""

from __future__ import annotations

import asyncio
import os
import sys
import warnings
from typing import Literal

OutputMode = Literal["local", "memory", "auto"]


def _windows_asyncio_subprocess_policy() -> None:
    """Windows + Playwright: SelectorEventLoop cannot create subprocesses; Proactor can."""
    if sys.platform == "win32":
        try:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", DeprecationWarning)
                asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
        except Exception:
            pass


_windows_asyncio_subprocess_policy()

# ---------------------------------------------------------------------------
# SEC compliance: set your identity (required by SEC for programmatic access).
# Format must be descriptive, e.g. "Example Corp admin@example.com".
# Override without editing code: environment variable SEC_USER_AGENT.
# ---------------------------------------------------------------------------
_DEFAULT_SEC_USER_AGENT = "Jakob jakobsinvestmentnewsletter@gmail.com"


def sec_user_agent() -> str:
    """User-Agent string for all SEC HTTP requests (env ``SEC_USER_AGENT`` or default)."""
    return (os.environ.get("SEC_USER_AGENT") or _DEFAULT_SEC_USER_AGENT).strip()


# Backward-compatible alias (import time); prefer :func:`sec_user_agent` after env changes.
SEC_USER_AGENT = sec_user_agent()

USE_SCRIPT_DIR_AS_BASE = True
DEFAULT_CIK: str | int | None = None

MAX_10K = 5
MAX_10Q = 20
MAX_REQUESTS_PER_SECOND = 10
PARALLEL_PDF_WORKERS = 1
PARALLEL_HTML_WORKERS = 4
PDF_SKIP_IMAGE_SUBRESOURCES = False
PDF_GOTO_WAIT_UNTIL = "domcontentloaded"
FILING_OUTPUT_FORMAT: str = "pdf"

SUBMISSIONS_URL = "https://data.sec.gov/submissions/CIK{cik}.json"
SUBMISSIONS_HISTORY_FILE_URL = "https://data.sec.gov/submissions/{name}"
COMPANY_FACTS_URL = "https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json"
ARCHIVES_BASE = "https://www.sec.gov/Archives/edgar/data"

STRUCTURED_FILENAME = "structured_data.json"
DEFAULT_MAX_FILINGS_PUBLIC = 80
