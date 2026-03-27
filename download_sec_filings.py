#!/usr/bin/env python3
"""
Download recent SEC 10-K / 10-Q filings and company-facts JSON for a given CIK.

Implementation is split across ``sec_*.py``, ``sec_types.py``, and ``_sec_download_jobs.py``;
this file is the CLI entry point and stable import path for :func:`download_sec_filings`
and related constants.
"""

from __future__ import annotations

import argparse
import json
import sys
import threading
from pathlib import Path
from typing import Any, Callable, Literal, cast

import requests

from _sec_download_jobs import fetch_company_facts, run_html_jobs, run_pdf_jobs
from sec_config import (
    ARCHIVES_BASE,
    DEFAULT_CIK,
    DEFAULT_MAX_FILINGS_PUBLIC,
    FILING_OUTPUT_FORMAT,
    MAX_10K,
    MAX_10Q,
    MAX_REQUESTS_PER_SECOND,
    OutputMode,
    PARALLEL_HTML_WORKERS,
    PARALLEL_PDF_WORKERS,
    PDF_SKIP_IMAGE_SUBRESOURCES,
    SEC_USER_AGENT,
    STRUCTURED_FILENAME,
    SUBMISSIONS_HISTORY_FILE_URL,
    SUBMISSIONS_URL,
    USE_SCRIPT_DIR_AS_BASE,
    _DEFAULT_SEC_USER_AGENT,
    sec_user_agent,
)
from sec_filings_paths import (
    build_target_filename,
    filing_document_url,
    filing_save_suffix,
    pad_cik_10,
)
from sec_http import RateLimiter, rate_limited_get, session_with_headers
from sec_submissions import (
    classify_period_form,
    load_submissions,
    merge_submissions_recent_for_year_window,
    select_filings,
    select_filings_within_years,
)
from sec_types import DownloadResult
from sec_zip_io import zip_artifacts_bytes, zip_artifacts_spooled, zip_pdfs_from_directory


def detect_streamlit_runtime() -> bool:
    """True when this Python process is executing a Streamlit script run."""
    try:
        from streamlit.runtime.scriptrunner import get_script_run_ctx

        return get_script_run_ctx() is not None
    except Exception:
        return False


def _resolve_output_mode(mode: OutputMode) -> Literal["local", "memory"]:
    if mode == "auto":
        return "memory" if detect_streamlit_runtime() else "local"
    return cast(Literal["local", "memory"], mode)


def download_sec_filings(
    cik: str | int,
    *,
    years_back: int | None = None,
    format_mode: str | None = None,
    base_dir: Path | None = None,
    output_mode: OutputMode = "local",
    include_structured_data: bool = True,
    max_filings: int | None = None,
    log: Callable[[str], None] = print,
    err_log: Callable[[str], None] | None = None,
) -> DownloadResult:
    """Download 10-K / 10-Q (and optionally company facts) for ``cik``.

    If ``years_back`` is set, filings are filtered so effective report/filing year
    is >= ``(current calendar year - years_back)``. If ``None``, uses
    :data:`MAX_10K` and :data:`MAX_10Q` caps on the recent submissions feed.

    ``output_mode``:
    - ``local`` — write under ``base_dir`` / ``<CIK>/`` (script dir or cwd; same as CLI).
    - ``memory`` — keep filings only in RAM; set ``DownloadResult.artifacts`` (for ZIP /
      ``st.download_button`` on Streamlit Cloud). No persistent server folder.
    - ``auto`` — ``memory`` when running inside Streamlit, else ``local``.

    If ``max_filings`` is set, abort before downloading when the planned filing count
    exceeds it (useful for public web UIs).
    """
    err = err_log or (lambda m: print(m, file=sys.stderr))
    resolved = _resolve_output_mode(output_mode)
    memory_mode = resolved == "memory"

    if cik is None or str(cik).strip() == "":
        err("Error: CIK is required.")
        return DownloadResult(
            ok=False,
            exit_code=2,
            cik10="",
            entity_name="Unknown",
            out_dir=None,
            error_message="CIK is required.",
            artifacts=None,
        )

    try:
        cik10 = pad_cik_10(cik)
    except (ValueError, TypeError) as e:
        err(f"Error: invalid CIK: {e}")
        return DownloadResult(
            ok=False,
            exit_code=2,
            cik10="",
            entity_name="Unknown",
            out_dir=None,
            error_message=str(e),
            artifacts=None,
        )

    cik_folder = str(int(cik10, 10))
    root = base_dir
    if root is None:
        root = Path(__file__).resolve().parent if USE_SCRIPT_DIR_AS_BASE else Path.cwd()

    out_dir: Path | None = None
    artifacts: dict[str, bytes] | None = {} if memory_mode else None
    mem_lock = threading.Lock()

    if memory_mode:
        log(
            "Output mode: in-memory (no CIK folder on server disk; "
            "use DownloadResult.artifacts or zip_artifacts_bytes for download)."
        )
    else:
        out_dir = root / cik_folder
        try:
            out_dir.mkdir(parents=True, exist_ok=True)
        except OSError as e:
            err(f"Error: could not create output directory {out_dir}: {e}")
            return DownloadResult(
                ok=False,
                exit_code=1,
                cik10=cik10,
                entity_name="Unknown",
                out_dir=out_dir,
                error_message=str(e),
                artifacts=None,
            )
        log(f"Output directory: {out_dir}")

    structured_path = (out_dir / STRUCTURED_FILENAME) if out_dir else Path(STRUCTURED_FILENAME)
    structured_existed_at_start = bool(out_dir and structured_path.exists())

    mode = format_mode if format_mode is not None else FILING_OUTPUT_FORMAT
    if mode not in ("pdf", "html"):
        err(f"Error: format_mode must be 'pdf' or 'html', got {mode!r}.")
        return DownloadResult(
            ok=False,
            exit_code=2,
            cik10=cik10,
            entity_name="Unknown",
            out_dir=out_dir,
            error_message="Invalid format_mode.",
            artifacts=None,
        )

    limiter = RateLimiter(MAX_REQUESTS_PER_SECOND)
    session = session_with_headers()

    try:
        submissions = load_submissions(session, limiter, cik10, log=log)
        if years_back is not None:
            merge_submissions_recent_for_year_window(
                session, limiter, submissions, int(years_back), log=log
            )
    except requests.RequestException as e:
        err(f"Error: failed to load submissions: {e}")
        return DownloadResult(
            ok=False,
            exit_code=1,
            cik10=cik10,
            entity_name="Unknown",
            out_dir=out_dir,
            error_message=str(e),
            artifacts=None,
        )
    except json.JSONDecodeError as e:
        err(f"Error: invalid JSON from submissions API: {e}")
        return DownloadResult(
            ok=False,
            exit_code=1,
            cik10=cik10,
            entity_name="Unknown",
            out_dir=out_dir,
            error_message=str(e),
            artifacts=None,
        )

    name = submissions.get("name") or "Unknown"
    log(f"Entity: {name} (CIK {cik10})")

    if years_back is not None:
        rows = select_filings_within_years(submissions, int(years_back))
    else:
        rows = select_filings(submissions, MAX_10K, MAX_10Q)

    log(
        f"Target list: {len([r for r in rows if r['period_form'] == '10-K'])} 10-K, "
        f"{len([r for r in rows if r['period_form'] == '10-Q'])} 10-Q "
        f"({mode.upper()} output)"
    )

    occupied_names: set[str] = set()
    planned: list[tuple[dict[str, Any], str]] = []
    for row in rows:
        suffix = filing_save_suffix(row["primary_document"], mode)
        fname = build_target_filename(
            row["period_form"],
            row["report_date"],
            row["filing_date"],
            row["accession"],
            occupied_names,
            suffix,
        )
        occupied_names.add(fname)
        planned.append((row, fname))

    if max_filings is not None and len(planned) > max_filings:
        err(
            f"Error: too many filings in this run ({len(planned)}), "
            f"maximum allowed is {max_filings}. Reduce the year range or raise the limit."
        )
        return DownloadResult(
            ok=False,
            exit_code=1,
            cik10=cik10,
            entity_name=name,
            out_dir=out_dir,
            error_message=f"Too many filings ({len(planned)} > {max_filings}).",
            artifacts=artifacts,
        )

    pdf_jobs_disk: list[tuple[dict[str, Any], str, str, Path]] = []
    html_jobs_disk: list[tuple[dict[str, Any], str, str, Path]] = []
    pdf_jobs_mem: list[tuple[dict[str, Any], str, str]] = []
    html_jobs_mem: list[tuple[dict[str, Any], str, str]] = []

    assert artifacts is not None or out_dir is not None

    for row, fname in planned:
        url = filing_document_url(cik10, row["accession"], row["primary_document"])
        if memory_mode:
            if mode == "pdf" and fname.lower().endswith(".pdf"):
                pdf_jobs_mem.append((row, fname, url))
            else:
                html_jobs_mem.append((row, fname, url))
            continue
        assert out_dir is not None
        dest = out_dir / fname
        if dest.exists():
            log(f"Skip (exists): {fname} [{row['form']} filed {row['filing_date']}]")
            continue
        if mode == "pdf" and dest.suffix.lower() == ".pdf":
            pdf_jobs_disk.append((row, fname, url, dest))
        else:
            html_jobs_disk.append((row, fname, url, dest))

    any_filing_downloaded = False

    err_result, touched = run_html_jobs(
        memory_mode=memory_mode,
        html_jobs_mem=html_jobs_mem,
        html_jobs_disk=html_jobs_disk,
        limiter=limiter,
        artifacts=artifacts,
        mem_lock=mem_lock,
        log=log,
        err=err,
        cik10=cik10,
        entity_name=name,
        out_dir=out_dir,
    )
    if err_result is not None:
        return err_result
    any_filing_downloaded = any_filing_downloaded or touched

    err_result, touched = run_pdf_jobs(
        memory_mode=memory_mode,
        pdf_jobs_mem=pdf_jobs_mem,
        pdf_jobs_disk=pdf_jobs_disk,
        limiter=limiter,
        artifacts=artifacts,
        mem_lock=mem_lock,
        log=log,
        err=err,
        cik10=cik10,
        entity_name=name,
        out_dir=out_dir,
    )
    if err_result is not None:
        return err_result
    any_filing_downloaded = any_filing_downloaded or touched

    err_result = fetch_company_facts(
        include_structured_data=include_structured_data,
        memory_mode=memory_mode,
        structured_existed_at_start=structured_existed_at_start,
        any_filing_downloaded=any_filing_downloaded,
        cik10=cik10,
        entity_name=name,
        session=session,
        limiter=limiter,
        artifacts=artifacts,
        out_dir=out_dir,
        structured_path=structured_path,
        log=log,
        err=err,
    )
    if err_result is not None:
        return err_result

    log("Done.")
    return DownloadResult(
        ok=True,
        exit_code=0,
        cik10=cik10,
        entity_name=name,
        out_dir=out_dir,
        error_message=None,
        artifacts=artifacts if memory_mode else None,
    )


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Download SEC 10-K, 10-Q, and company facts JSON for a CIK."
    )
    parser.add_argument(
        "cik",
        nargs="?",
        default=DEFAULT_CIK,
        help="Central Index Key (with or without leading zeros)",
    )
    parser.add_argument(
        "--html",
        action="store_true",
        help="Save filings as HTML instead of PDF (ignores FILING_OUTPUT_FORMAT).",
    )
    parser.add_argument(
        "--years",
        type=int,
        default=None,
        metavar="N",
        help=(
            "Include 10-K/10-Q whose report/filing year >= (current year - N). "
            "Default: use fixed caps MAX_10K / MAX_10Q on the recent feed."
        ),
    )
    args = parser.parse_args()
    if args.cik is None or str(args.cik).strip() == "":
        print("Error: provide a CIK as an argument or set DEFAULT_CIK.", file=sys.stderr)
        return 2

    if args.years is not None and args.years < 1:
        print("Error: --years must be >= 1", file=sys.stderr)
        return 2

    fmt = "html" if args.html else FILING_OUTPUT_FORMAT
    res = download_sec_filings(
        args.cik,
        years_back=args.years,
        format_mode=fmt,
        output_mode="local",
        include_structured_data=True,
    )
    return res.exit_code


__all__ = [
    "ARCHIVES_BASE",
    "DEFAULT_MAX_FILINGS_PUBLIC",
    "DownloadResult",
    "FILING_OUTPUT_FORMAT",
    "MAX_10K",
    "MAX_10Q",
    "MAX_REQUESTS_PER_SECOND",
    "OutputMode",
    "PARALLEL_HTML_WORKERS",
    "PARALLEL_PDF_WORKERS",
    "PDF_SKIP_IMAGE_SUBRESOURCES",
    "RateLimiter",
    "SEC_USER_AGENT",
    "STRUCTURED_FILENAME",
    "SUBMISSIONS_HISTORY_FILE_URL",
    "SUBMISSIONS_URL",
    "USE_SCRIPT_DIR_AS_BASE",
    "_DEFAULT_SEC_USER_AGENT",
    "classify_period_form",
    "detect_streamlit_runtime",
    "download_sec_filings",
    "merge_submissions_recent_for_year_window",
    "pad_cik_10",
    "sec_user_agent",
    "session_with_headers",
    "zip_artifacts_bytes",
    "zip_artifacts_spooled",
    "zip_pdfs_from_directory",
]
