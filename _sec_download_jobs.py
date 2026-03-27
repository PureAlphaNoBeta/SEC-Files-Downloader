"""HTML/PDF filing workers and company-facts fetch (used by ``download_sec_filings``)."""

from __future__ import annotations

import json
import threading
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Callable

import requests

from sec_config import (
    COMPANY_FACTS_URL,
    MAX_REQUESTS_PER_SECOND,
    PARALLEL_HTML_WORKERS,
    PARALLEL_PDF_WORKERS,
    PDF_SKIP_IMAGE_SUBRESOURCES,
    STRUCTURED_FILENAME,
)
from sec_http import (
    RateLimiter,
    download_filing_html,
    download_filing_html_bytes,
    rate_limited_get,
    session_with_headers,
)
from sec_pdf import PlaywrightPdfRenderer
from sec_types import DownloadResult
from sec_zip_io import save_json

# One Session per worker thread (connection pooling); main thread has its own slot.
_thread_local = threading.local()


def _thread_session() -> requests.Session:
    s = getattr(_thread_local, "session", None)
    if s is None:
        s = session_with_headers()
        _thread_local.session = s
    return s


def _exc_detail(exc: BaseException) -> str:
    msg = str(exc).strip() or "(no message)"
    return f"{type(exc).__name__}: {msg}\n{traceback.format_exc()}"


def _split_balanced(items: list[Any], n_parts: int) -> list[list[Any]]:
    """Partition items into up to ``n_parts`` non-empty sublists of near-equal size."""
    if not items:
        return []
    n_parts = max(1, min(n_parts, len(items)))
    base, extra = divmod(len(items), n_parts)
    out: list[list[Any]] = []
    idx = 0
    for i in range(n_parts):
        sz = base + (1 if i < extra else 0)
        out.append(items[idx : idx + sz])
        idx += sz
    return out


def run_html_jobs(
    *,
    memory_mode: bool,
    html_jobs_mem: list[tuple[dict[str, Any], str, str]],
    html_jobs_disk: list[tuple[dict[str, Any], str, str, Path]],
    limiter: RateLimiter,
    artifacts: dict[str, bytes] | None,
    mem_lock: threading.Lock,
    log: Callable[[str], None],
    err: Callable[[str], None],
    cik10: str,
    entity_name: str,
    out_dir: Path | None,
) -> tuple[DownloadResult | None, bool]:
    """Run parallel HTML downloads. Returns ``(error, any_downloaded)``."""
    html_jobs = html_jobs_mem if memory_mode else html_jobs_disk
    if not html_jobs:
        return None, False

    def _html_job_disk(job: tuple[dict[str, Any], str, str, Path]) -> None:
        row, fname, url, dest = job
        download_filing_html(_thread_session(), limiter, url, dest, log=log)
        log(f"Saved: {fname} [{row['form']} filed {row['filing_date']}]")

    def _html_job_mem(job: tuple[dict[str, Any], str, str]) -> None:
        row, fname, url = job
        data = download_filing_html_bytes(_thread_session(), limiter, url, log=log)
        with mem_lock:
            assert artifacts is not None
            artifacts[fname] = data
        log(f"Saved: {fname} [{row['form']} filed {row['filing_date']}] (memory)")

    hw = min(PARALLEL_HTML_WORKERS, len(html_jobs))
    try:
        if memory_mode:
            if hw <= 1:
                for job in html_jobs_mem:
                    _html_job_mem(job)
            else:
                with ThreadPoolExecutor(max_workers=hw) as ex:
                    list(ex.map(_html_job_mem, html_jobs_mem))
        else:
            if hw <= 1:
                for job in html_jobs_disk:
                    _html_job_disk(job)
            else:
                with ThreadPoolExecutor(max_workers=hw) as ex:
                    list(ex.map(_html_job_disk, html_jobs_disk))
    except requests.RequestException as e:
        err(f"Error: download failed: {e}")
        return (
            DownloadResult(
                ok=False,
                exit_code=1,
                cik10=cik10,
                entity_name=entity_name,
                out_dir=out_dir,
                error_message=str(e),
                artifacts=artifacts,
            ),
            False,
        )
    except OSError as e:
        err(f"Error: could not write file: {e}")
        return (
            DownloadResult(
                ok=False,
                exit_code=1,
                cik10=cik10,
                entity_name=entity_name,
                out_dir=out_dir,
                error_message=str(e),
                artifacts=artifacts,
            ),
            False,
        )
    return None, True


def run_pdf_jobs(
    *,
    memory_mode: bool,
    pdf_jobs_mem: list[tuple[dict[str, Any], str, str]],
    pdf_jobs_disk: list[tuple[dict[str, Any], str, str, Path]],
    limiter: RateLimiter,
    artifacts: dict[str, bytes] | None,
    mem_lock: threading.Lock,
    log: Callable[[str], None],
    err: Callable[[str], None],
    cik10: str,
    entity_name: str,
    out_dir: Path | None,
) -> tuple[DownloadResult | None, bool]:
    """Run PDF rendering jobs. Returns ``(error, any_downloaded)``."""
    pdf_jobs = pdf_jobs_mem if memory_mode else pdf_jobs_disk
    if not pdf_jobs:
        return None, False

    def _pdf_worker_chunk_disk(chunk: list[tuple[dict[str, Any], str, str, Path]]) -> None:
        with PlaywrightPdfRenderer(
            _thread_session(),
            limiter,
            skip_images=PDF_SKIP_IMAGE_SUBRESOURCES,
            log=log,
        ) as renderer:
            for row, fname, url, dest in chunk:
                renderer.save_filing_pdf(url, dest)
                log(f"Saved: {fname} [{row['form']} filed {row['filing_date']}]")

    def _pdf_worker_chunk_mem(chunk: list[tuple[dict[str, Any], str, str]]) -> None:
        with PlaywrightPdfRenderer(
            _thread_session(),
            limiter,
            skip_images=PDF_SKIP_IMAGE_SUBRESOURCES,
            log=log,
        ) as renderer:
            for row, fname, url in chunk:
                pdf_bytes = renderer.render_filing_pdf_bytes(url)
                with mem_lock:
                    assert artifacts is not None
                    artifacts[fname] = pdf_bytes
                log(f"Saved: {fname} [{row['form']} filed {row['filing_date']}] (memory)")

    pw = min(PARALLEL_PDF_WORKERS, len(pdf_jobs))
    try:
        if pw <= 1:
            log("Starting Chromium once for all PDFs (single worker).")
            if memory_mode:
                _pdf_worker_chunk_mem(pdf_jobs_mem)
            else:
                _pdf_worker_chunk_disk(pdf_jobs_disk)
        else:
            chunks = _split_balanced(pdf_jobs, pw)
            log(
                f"Starting {len(chunks)} Chromium workers for PDFs "
                f"(shared SEC cap: {MAX_REQUESTS_PER_SECOND} req/s)."
            )
            with ThreadPoolExecutor(max_workers=len(chunks)) as ex:
                if memory_mode:
                    futures = [ex.submit(_pdf_worker_chunk_mem, ch) for ch in chunks]
                else:
                    futures = [ex.submit(_pdf_worker_chunk_disk, ch) for ch in chunks]
                for fut in as_completed(futures):
                    fut.result()
    except requests.RequestException as e:
        err(f"Error: download failed: {e}")
        return (
            DownloadResult(
                ok=False,
                exit_code=1,
                cik10=cik10,
                entity_name=entity_name,
                out_dir=out_dir,
                error_message=str(e),
                artifacts=artifacts,
            ),
            False,
        )
    except RuntimeError as e:
        err(f"Error: {_exc_detail(e)}")
        return (
            DownloadResult(
                ok=False,
                exit_code=1,
                cik10=cik10,
                entity_name=entity_name,
                out_dir=out_dir,
                error_message=str(e) or f"{type(e).__name__} (see log)",
                artifacts=artifacts,
            ),
            False,
        )
    except OSError as e:
        err(f"Error: could not write file: {e}")
        return (
            DownloadResult(
                ok=False,
                exit_code=1,
                cik10=cik10,
                entity_name=entity_name,
                out_dir=out_dir,
                error_message=str(e),
                artifacts=artifacts,
            ),
            False,
        )
    except Exception as e:
        err(f"Error: {_exc_detail(e)}")
        return (
            DownloadResult(
                ok=False,
                exit_code=1,
                cik10=cik10,
                entity_name=entity_name,
                out_dir=out_dir,
                error_message=str(e) or f"{type(e).__name__} (see log)",
                artifacts=artifacts,
            ),
            False,
        )
    return None, True


def fetch_company_facts(
    *,
    include_structured_data: bool,
    memory_mode: bool,
    structured_existed_at_start: bool,
    any_filing_downloaded: bool,
    cik10: str,
    entity_name: str,
    session: requests.Session,
    limiter: RateLimiter,
    artifacts: dict[str, bytes] | None,
    out_dir: Path | None,
    structured_path: Path,
    log: Callable[[str], None],
    err: Callable[[str], None],
) -> DownloadResult | None:
    """Download/write company facts JSON, or skip. Returns error result or ``None``."""
    if not include_structured_data:
        return None

    skip_facts = (
        not memory_mode
        and structured_existed_at_start
        and not any_filing_downloaded
    )
    if skip_facts:
        log(f"Skip company facts (unchanged): {structured_path.name}")
        return None

    facts_url = COMPANY_FACTS_URL.format(cik=cik10)
    log(f"Downloading company facts: {facts_url}")
    try:
        resp = rate_limited_get(session, limiter, facts_url)
    except requests.RequestException as e:
        err(f"Error: company facts request failed: {e}")
        return DownloadResult(
            ok=False,
            exit_code=1,
            cik10=cik10,
            entity_name=entity_name,
            out_dir=out_dir,
            error_message=str(e),
            artifacts=artifacts,
        )

    if resp.status_code == 404:
        log("Skip company facts: SEC returned 404 (no aggregated XBRL facts for this CIK).")
        return None
    if not resp.ok:
        err(f"Error: company facts HTTP {resp.status_code} for {facts_url}")
        return DownloadResult(
            ok=False,
            exit_code=1,
            cik10=cik10,
            entity_name=entity_name,
            out_dir=out_dir,
            error_message=f"HTTP {resp.status_code}",
            artifacts=artifacts,
        )

    try:
        payload = resp.json()
    except json.JSONDecodeError as e:
        err(f"Error: invalid JSON from company facts API: {e}")
        return DownloadResult(
            ok=False,
            exit_code=1,
            cik10=cik10,
            entity_name=entity_name,
            out_dir=out_dir,
            error_message=str(e),
            artifacts=artifacts,
        )

    json_bytes = json.dumps(payload, indent=2).encode("utf-8")
    if memory_mode:
        assert artifacts is not None
        artifacts[STRUCTURED_FILENAME] = json_bytes
        log(f"Stored structured data in memory: {STRUCTURED_FILENAME}")
    else:
        assert out_dir is not None
        try:
            save_json(out_dir / STRUCTURED_FILENAME, payload)
        except OSError as e:
            err(f"Error: could not write {structured_path}: {e}")
            return DownloadResult(
                ok=False,
                exit_code=1,
                cik10=cik10,
                entity_name=entity_name,
                out_dir=out_dir,
                error_message=str(e),
                artifacts=artifacts,
            )
        if structured_existed_at_start and any_filing_downloaded:
            reason = "refresh after new filings"
        elif not structured_existed_at_start:
            reason = "initial download"
        else:
            reason = "missing or stale file"
        log(f"Wrote structured data ({reason}): {STRUCTURED_FILENAME}")
    return None
