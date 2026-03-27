"""Playwright PDF rendering with requests-backed network (SEC policy)."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Callable, cast
from urllib.parse import urljoin, urlparse

import requests

from sec_config import PDF_GOTO_WAIT_UNTIL, _windows_asyncio_subprocess_policy, sec_user_agent
from sec_http import RateLimiter


def _response_headers_for_fulfill(resp: requests.Response) -> dict[str, str]:
    """Strip hop-by-hop / encoding headers; body is already decompressed by requests."""
    skip = {
        "content-encoding",
        "transfer-encoding",
        "content-length",
        "connection",
    }
    return {k: v for k, v in resp.headers.items() if k.lower() not in skip}


def _is_sec_gov_host(host: str | None) -> bool:
    """True for ``sec.gov`` and official subdomains (not arbitrary *sec.gov registrable domains)."""
    h = (host or "").lower()
    return h == "sec.gov" or h.endswith(".sec.gov")


def _fetch_sec_url_for_playwright(
    session: requests.Session,
    limiter: RateLimiter,
    req_url: str,
    *,
    timeout: float = 120.0,
    max_hops: int = 15,
) -> requests.Response | None:
    """Follow redirects manually; each hop must stay on ``*.sec.gov``."""
    current = req_url
    for _ in range(max_hops):
        parsed = urlparse(current)
        if parsed.scheme not in ("http", "https"):
            return None
        if not _is_sec_gov_host(parsed.hostname):
            return None
        limiter.wait()
        r = session.get(current, timeout=timeout, allow_redirects=False)
        if r.status_code in (301, 302, 303, 307, 308) and r.headers.get("Location"):
            current = urljoin(current, r.headers["Location"])
            continue
        return r
    return None


class PlaywrightPdfRenderer:
    """One Chromium process for many PDFs; network fulfilled via ``requests`` (SEC policy)."""

    def __init__(
        self,
        session: requests.Session,
        limiter: RateLimiter,
        *,
        skip_images: bool = True,
        log: Callable[[str], None] = print,
    ) -> None:
        self._session = session
        self._limiter = limiter
        self._skip_images = skip_images
        self._log = log
        self._pw: Any = None
        self._browser: Any = None
        self._context: Any = None

    def __enter__(self) -> PlaywrightPdfRenderer:
        try:
            from playwright.sync_api import sync_playwright
        except ImportError as e:
            raise RuntimeError(
                "PDF output requires Playwright. Install with: pip install playwright && "
                "playwright install chromium"
            ) from e

        try:
            self._pw = sync_playwright().start()
        except NotImplementedError:
            _windows_asyncio_subprocess_policy()
            self._pw = sync_playwright().start()
        try:
            self._browser = self._pw.chromium.launch(
                headless=True,
                args=["--disable-blink-features=AutomationControlled"],
            )
            self._context = self._browser.new_context(user_agent=sec_user_agent())
        except Exception as e:
            self._pw.stop()
            self._pw = None
            err = str(e).lower()
            if "executable doesn't exist" in err or "browsertype.launch" in err:
                raise RuntimeError(
                    "Chromium is not installed for Playwright. Run: playwright install chromium"
                ) from e
            raise
        return self

    def __exit__(self, *args: object) -> None:
        if self._context:
            self._context.close()
            self._context = None
        if self._browser:
            self._browser.close()
            self._browser = None
        if self._pw:
            self._pw.stop()
            self._pw = None

    def render_filing_pdf_bytes(self, url: str) -> bytes:
        session = self._session
        limiter = self._limiter

        def handle_route(route: Any) -> None:
            if self._skip_images and route.request.resource_type == "image":
                route.abort()
                return
            req_url = route.request.url
            if not req_url.startswith(("http://", "https://")):
                route.continue_()
                return
            if not _is_sec_gov_host(urlparse(req_url).hostname):
                route.abort()
                return
            try:
                r = _fetch_sec_url_for_playwright(session, limiter, req_url)
                if r is None:
                    route.abort()
                    return
                route.fulfill(
                    status=r.status_code,
                    headers=_response_headers_for_fulfill(r),
                    body=r.content,
                )
            except requests.RequestException:
                route.abort()
            except Exception:
                try:
                    route.abort()
                except Exception:
                    pass

        self._log(f"  PDF {url}")
        page = self._context.new_page()
        try:
            page.route("**/*", handle_route)
            page.goto(url, wait_until=PDF_GOTO_WAIT_UNTIL, timeout=180_000)
            page.unroute("**/*", handle_route)
            return cast(
                bytes,
                page.pdf(
                    format="Letter",
                    print_background=True,
                    margin={
                        "top": "0.45in",
                        "bottom": "0.45in",
                        "left": "0.45in",
                        "right": "0.45in",
                    },
                ),
            )
        finally:
            page.close()

    def save_filing_pdf(self, url: str, dest: Path) -> None:
        dest.write_bytes(self.render_filing_pdf_bytes(url))
