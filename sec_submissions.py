"""Load SEC company submissions JSON, merge history files, and select 10-K / 10-Q rows."""

from __future__ import annotations

from datetime import date
from typing import Any, Callable, cast

import requests

from sec_config import SUBMISSIONS_HISTORY_FILE_URL, SUBMISSIONS_URL
from sec_http import RateLimiter, rate_limited_get


def classify_period_form(form: str | None) -> str | None:
    """Map raw submission form to '10-K' or '10-Q' (includes common variants)."""
    if not form:
        return None
    root = form.split("/")[0].upper()
    if root in ("10-K", "10-K405", "10-KT"):
        return "10-K" if root != "10-KT" else None
    if root == "10-Q":
        return "10-Q"
    return None


def load_submissions(
    session: requests.Session,
    limiter: RateLimiter,
    cik10: str,
    log: Callable[[str], None] = print,
) -> dict[str, Any]:
    url = SUBMISSIONS_URL.format(cik=cik10)
    log(f"Fetching submissions index: {url}")
    resp = rate_limited_get(session, limiter, url)
    resp.raise_for_status()
    return resp.json()


def _recent_as_mutable_lists(recent: dict[str, Any]) -> dict[str, list[Any]]:
    """Deep-copy list columns from a SEC ``recent`` block for in-place appends."""
    out: dict[str, list[Any]] = {}
    for k, v in recent.items():
        out[k] = list(v) if isinstance(v, list) else []
    return out


def _append_recent_chunk_dedupe(
    merged: dict[str, list[Any]],
    seen_acc: set[str],
    chunk: dict[str, Any],
) -> None:
    """Append rows from ``chunk`` into ``merged``, skipping duplicate accessions."""
    keys = list(merged.keys())
    forms = chunk.get("form") or []
    accs = chunk.get("accessionNumber") or []
    filing_dates = chunk.get("filingDate") or []
    primary_docs = chunk.get("primaryDocument") or []
    n = min(len(forms), len(accs), len(filing_dates), len(primary_docs))
    for i in range(n):
        acc = accs[i]
        if acc in seen_acc:
            continue
        seen_acc.add(acc)
        for k in keys:
            arr = chunk.get(k) or []
            merged[k].append(arr[i] if i < len(arr) else None)


def _oldest_filing_date_year(recent: dict[str, Any]) -> int | None:
    """Calendar year of the oldest ``filingDate`` in a newest-first ``recent`` block."""
    fds = recent.get("filingDate") or []
    if not fds:
        return None
    tail = fds[-1]
    if len(tail) >= 4 and tail[:4].isdigit():
        return int(tail[:4])
    return None


def merge_submissions_recent_for_year_window(
    session: requests.Session,
    limiter: RateLimiter,
    submissions: dict[str, Any],
    years_back: int,
    log: Callable[[str], None] = print,
) -> None:
    """Pull linked ``filings.files`` chunks until merged data is old enough for ``years_back``.

    Stops extending when the oldest ``filingDate`` year is below ``current_year - years_back``.
    """
    if years_back < 1:
        years_back = 1
    min_year = date.today().year - years_back

    filings = submissions.get("filings")
    if not isinstance(filings, dict):
        return
    recent = filings.get("recent")
    if not isinstance(recent, dict):
        return
    files_meta = filings.get("files")
    if not files_meta:
        return

    merged_lists = _recent_as_mutable_lists(recent)
    seen_acc = set(merged_lists.get("accessionNumber") or [])

    for entry in files_meta:
        if not isinstance(entry, dict):
            continue
        oy = _oldest_filing_date_year(cast(dict[str, Any], merged_lists))
        if oy is None:
            break
        if oy < min_year:
            break
        name = entry.get("name")
        if not name or not isinstance(name, str):
            continue
        url = SUBMISSIONS_HISTORY_FILE_URL.format(name=name)
        log(f"Fetching submissions history: {url}")
        resp = rate_limited_get(session, limiter, url)
        resp.raise_for_status()
        chunk = resp.json()
        if not isinstance(chunk, dict):
            continue
        _append_recent_chunk_dedupe(merged_lists, seen_acc, chunk)

    filings["recent"] = cast(dict[str, Any], merged_lists)


def _filing_effective_year(report_date: str, filing_date: str) -> int:
    d = (report_date or "").strip() or (filing_date or "").strip()
    if len(d) >= 4 and d[:4].isdigit():
        return int(d[:4])
    return date.today().year


def select_filings_within_years(
    data: dict[str, Any],
    years_back: int,
) -> list[dict[str, Any]]:
    """10-K / 10-Q where effective report/filing year >= ``current_year - years_back``."""
    if years_back < 1:
        years_back = 1
    min_year = date.today().year - years_back

    recent = (data.get("filings") or {}).get("recent") or {}
    forms: list[str] = recent.get("form") or []
    filing_dates: list[str] = recent.get("filingDate") or []
    report_dates: list[str] = recent.get("reportDate") or []
    accessions: list[str] = recent.get("accessionNumber") or []
    primary_docs: list[str] = recent.get("primaryDocument") or []

    n = min(len(forms), len(filing_dates), len(accessions), len(primary_docs))

    picked_k: list[dict[str, Any]] = []
    picked_q: list[dict[str, Any]] = []

    for i in range(n):
        form_raw = forms[i]
        kind = classify_period_form(form_raw)
        rd = report_dates[i] if i < len(report_dates) else ""
        fd = filing_dates[i]
        if _filing_effective_year(rd, fd) < min_year:
            continue
        if kind == "10-K":
            picked_k.append(
                {
                    "form": form_raw,
                    "period_form": "10-K",
                    "filing_date": fd,
                    "report_date": rd,
                    "accession": accessions[i],
                    "primary_document": primary_docs[i],
                }
            )
        elif kind == "10-Q":
            picked_q.append(
                {
                    "form": form_raw,
                    "period_form": "10-Q",
                    "filing_date": fd,
                    "report_date": rd,
                    "accession": accessions[i],
                    "primary_document": primary_docs[i],
                }
            )

    return picked_k + picked_q


def select_filings(
    data: dict[str, Any],
    max_10k: int,
    max_10q: int,
) -> list[dict[str, Any]]:
    recent = (data.get("filings") or {}).get("recent") or {}
    forms: list[str] = recent.get("form") or []
    filing_dates: list[str] = recent.get("filingDate") or []
    report_dates: list[str] = recent.get("reportDate") or []
    accessions: list[str] = recent.get("accessionNumber") or []
    primary_docs: list[str] = recent.get("primaryDocument") or []

    n = min(len(forms), len(filing_dates), len(accessions), len(primary_docs))

    picked_k: list[dict[str, Any]] = []
    picked_q: list[dict[str, Any]] = []

    for i in range(n):
        form_raw = forms[i]
        kind = classify_period_form(form_raw)
        rd = report_dates[i] if i < len(report_dates) else ""
        if kind == "10-K" and len(picked_k) < max_10k:
            picked_k.append(
                {
                    "form": form_raw,
                    "period_form": "10-K",
                    "filing_date": filing_dates[i],
                    "report_date": rd,
                    "accession": accessions[i],
                    "primary_document": primary_docs[i],
                }
            )
        elif kind == "10-Q" and len(picked_q) < max_10q:
            picked_q.append(
                {
                    "form": form_raw,
                    "period_form": "10-Q",
                    "filing_date": filing_dates[i],
                    "report_date": rd,
                    "accession": accessions[i],
                    "primary_document": primary_docs[i],
                }
            )
        if len(picked_k) >= max_10k and len(picked_q) >= max_10q:
            break

    return picked_k + picked_q
