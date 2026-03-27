"""HTTP session, rate limiting, and SEC document GETs."""

from __future__ import annotations

import sys
import threading
import time
from collections import deque
from pathlib import Path
from typing import Callable

import requests

from sec_config import sec_user_agent


class RateLimiter:
    """Sliding-window limiter: at most `max_per_second` calls per 1-second window."""

    def __init__(self, max_per_second: int) -> None:
        self.max_per_second = max_per_second
        self._times: deque[float] = deque()
        self._lock = threading.Lock()

    def wait(self) -> None:
        while True:
            with self._lock:
                now = time.monotonic()
                window_start = now - 1.0
                while self._times and self._times[0] < window_start:
                    self._times.popleft()
                if len(self._times) < self.max_per_second:
                    self._times.append(time.monotonic())
                    return
                sleep_for = max(0.0, self._times[0] + 1.0 - now)
            time.sleep(sleep_for)


def session_with_headers() -> requests.Session:
    ua = sec_user_agent()
    if "@" not in ua or len(ua) < 10:
        print(
            "Warning: SEC_USER_AGENT should be a real contact string like "
            "'Company Name admin@company.com' (see SEC developer FAQs). "
            "Set the SEC_USER_AGENT environment variable or edit _DEFAULT_SEC_USER_AGENT.",
            file=sys.stderr,
        )
    s = requests.Session()
    s.headers.update(
        {
            "User-Agent": ua,
            "Accept-Encoding": "gzip, deflate",
        }
    )
    return s


def rate_limited_get(
    session: requests.Session,
    limiter: RateLimiter,
    url: str,
    *,
    timeout: float = 60.0,
) -> requests.Response:
    limiter.wait()
    return session.get(url, timeout=timeout)


def download_filing_html_bytes(
    session: requests.Session,
    limiter: RateLimiter,
    url: str,
    log: Callable[[str], None] = print,
) -> bytes:
    log(f"  GET {url}")
    resp = rate_limited_get(session, limiter, url)
    resp.raise_for_status()
    return resp.content


def download_filing_html(
    session: requests.Session,
    limiter: RateLimiter,
    url: str,
    dest: Path,
    log: Callable[[str], None] = print,
) -> None:
    dest.write_bytes(download_filing_html_bytes(session, limiter, url, log=log))
