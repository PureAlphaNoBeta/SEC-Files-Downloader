"""CIK normalization, filing filenames, and EDGAR document URLs."""

from __future__ import annotations

import os
from pathlib import Path
from urllib.parse import quote

from sec_config import ARCHIVES_BASE


def pad_cik_10(cik: str | int) -> str:
    """Return CIK as a 10-digit zero-padded string for SEC API paths."""
    raw = str(cik).strip().replace(" ", "")
    if not raw.isdigit():
        raise ValueError("CIK must contain only digits (optional leading zeros).")
    core = raw.lstrip("0") or "0"
    if len(core) > 10:
        raise ValueError("CIK exceeds 10 digits.")
    return f"{int(core):010d}"


def cik_numeric_for_archives(cik10: str) -> str:
    """CIK without leading zeros for /Archives/edgar/data/{cik}/..."""
    return str(int(cik10, 10))


def accession_no_dashes(accession: str) -> str:
    return accession.replace("-", "")


def quarter_from_ymd(ymd: str) -> int:
    if len(ymd) < 10:
        return 1
    try:
        month = int(ymd[5:7])
    except ValueError:
        return 1
    if not 1 <= month <= 12:
        return 1
    return (month - 1) // 3 + 1


def unique_target_name(
    base_name: str,
    occupied: set[str],
    accession: str,
) -> str:
    """Ensure filename does not collide in our plan or on disk."""
    if base_name not in occupied:
        return base_name
    tail = accession_no_dashes(accession)[-6:]
    stem, ext = os.path.splitext(base_name)
    alt = f"{stem}_{tail}{ext}"
    if alt not in occupied:
        return alt
    return f"{stem}_{accession_no_dashes(accession)}{ext}"


def filing_save_suffix(primary_document: str, format_mode: str) -> str:
    """Extension for saved filing: .pdf, .html, or original suffix for non-HTML primaries."""
    if format_mode == "html":
        return ".html"
    low = primary_document.lower()
    if low.endswith((".htm", ".html")):
        return ".pdf"
    suf = Path(primary_document).suffix.lower()
    return suf if suf else ".html"


def build_target_filename(
    period_form: str,
    report_date: str,
    filing_date: str,
    accession: str,
    occupied: set[str],
    save_suffix: str,
) -> str:
    rd = (report_date or "").strip() or filing_date
    if len(rd) < 10:
        rd = filing_date
    year = rd[:4]
    if period_form == "10-K":
        base = f"{year}_10-K{save_suffix}"
    else:
        q = quarter_from_ymd(rd)
        base = f"{year}_Q{q}_10-Q{save_suffix}"
    return unique_target_name(base, occupied, accession)


def filing_document_url(cik10: str, accession: str, primary_document: str) -> str:
    cik_num = cik_numeric_for_archives(cik10)
    acc = accession_no_dashes(accession)
    doc = quote(primary_document, safe="/-._~")
    return f"{ARCHIVES_BASE}/{cik_num}/{acc}/{doc}"
