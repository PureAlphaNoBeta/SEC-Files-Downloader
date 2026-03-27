"""
Streamlit UI for SEC 10-K / 10-Q filings (wraps download_sec_filings).

Choose PDF (Playwright/Chromium) or HTML (primary document from EDGAR). Uses in-memory
output (no CIK folder on the server) so it works on Streamlit Cloud. The ZIP is written
via a spooled temp buffer (RAM up to a cap, then disk) so building does not duplicate
the archive as ``bytes``; the finished file is then read once into ``bytes`` for
``st.download_button`` (Streamlit does not accept ``SpooledTemporaryFile`` as ``data``).

Run from the project directory:
  streamlit run streamlit_app.py
"""

from __future__ import annotations

import asyncio
import sys
import tempfile
import warnings

# Playwright launches Chromium via asyncio subprocesses. On Windows the default event loop
# does not implement subprocess transport → NotImplementedError. Set Proactor *before*
# importing Streamlit so its asyncio setup picks up a loop that can spawn processes.
# Python 3.14+ deprecates this policy API until a replacement lands; we still need it for Playwright.
if sys.platform == "win32":
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

from download_sec_filings import (
    DEFAULT_MAX_FILINGS_PUBLIC,
    download_sec_filings,
    zip_pdfs_from_directory,
)

from sec_zip_io import ZIP_SPOOL_MAX_IN_MEMORY, zip_artifacts_spooled

import streamlit as st

st.set_page_config(page_title="SEC Filings", layout="centered")

st.title("SEC filings (10-K / 10-Q)")
st.caption(
    "Downloads use your SEC `User-Agent` (set `SEC_USER_AGENT` env or `_DEFAULT_SEC_USER_AGENT` "
    "in `sec_config.py`) and respect rate limits. "
    "Filings are built in memory on this server and sent to your browser—nothing is saved to the "
    "app host disk (suitable for Streamlit Community Cloud). "
    f"At most **{DEFAULT_MAX_FILINGS_PUBLIC}** filings per run."
)

format_choice = st.radio(
    "Filing format",
    ("PDF", "HTML"),
    horizontal=True,
    help=(
        "**PDF** — render each filing in Chromium (Playwright); slower, print-like pages. "
        "**HTML** — save the primary filing document as fetched from EDGAR; faster, no browser render."
    ),
)
format_mode: str = "pdf" if format_choice == "PDF" else "html"

cik = st.text_input(
    "CIK",
    placeholder="e.g. 320193 or 0000320193",
    help="Central Index Key, with or without leading zeros.",
)
years = st.number_input(
    "Number of Years",
    min_value=1,
    max_value=30,
    value=5,
    step=1,
    help="Include filings whose report or filing date year >= (current year - N).",
)


def _close_spool(sp: object | None) -> None:
    if sp is not None and hasattr(sp, "close"):
        try:
            sp.close()
        except Exception:
            pass


if st.button("Generate Filings ZIP", type="primary"):
    cik_s = (cik or "").strip()

    if not cik_s:
        st.warning("Please enter a CIK.")
    else:
        _close_spool(st.session_state.pop("zip_spool", None))
        st.session_state.pop("zip_bytes", None)
        st.session_state.pop("zip_name", None)
        log_lines: list[str] = []

        def capture_log(msg: str) -> None:
            log_lines.append(msg)

        def capture_err(msg: str) -> None:
            log_lines.append(f"[error] {msg}")

        with st.spinner("Fetching filings and building ZIP archive…"):
            result = download_sec_filings(
                cik_s,
                years_back=int(years),
                format_mode=format_mode,
                output_mode="memory",
                include_structured_data=True,
                max_filings=DEFAULT_MAX_FILINGS_PUBLIC,
                log=capture_log,
                err_log=capture_err,
            )

        if result.ok:
            zip_spool: tempfile.SpooledTemporaryFile[bytes] | None = None
            zip_size = 0
            n_pdf = 0
            n_html = 0

            if result.artifacts:
                n_pdf = sum(1 for k in result.artifacts if k.lower().endswith(".pdf"))
                n_html = sum(
                    1 for k in result.artifacts if k.lower().endswith((".html", ".htm"))
                )
                zip_spool = zip_artifacts_spooled(result.artifacts)
                zip_spool.seek(0, 2)
                zip_size = zip_spool.tell()
                zip_spool.seek(0)
            elif result.out_dir is not None:
                raw = zip_pdfs_from_directory(result.out_dir)
                n_pdf = len(list(result.out_dir.glob("*.pdf")))
                n_html = len(list(result.out_dir.glob("*.html"))) + len(
                    list(result.out_dir.glob("*.htm"))
                )
                if raw:
                    zip_spool = tempfile.SpooledTemporaryFile(
                        max_size=ZIP_SPOOL_MAX_IN_MEMORY,
                        mode="w+b",
                    )
                    zip_spool.write(raw)
                    zip_spool.seek(0)
                    zip_size = len(raw)

            if not zip_spool or zip_size == 0:
                st.warning(
                    "No files were added to the ZIP. "
                    f"There may be no filings for this CIK and year window "
                    f"({n_pdf} PDF(s), {n_html} HTML on disk/paths)."
                )
                _close_spool(zip_spool)
            else:
                zip_spool.seek(0)
                st.session_state["zip_bytes"] = zip_spool.read()
                _close_spool(zip_spool)
                st.session_state["zip_name"] = (
                    f"SEC_{result.cik10}_filings_{format_mode}.zip"
                )
                kb = max(zip_size / 1024.0, 0.01)
                if format_mode == "pdf":
                    filing_summary = f"{n_pdf} PDF(s)"
                else:
                    filing_summary = f"{n_html} HTML file(s)"
                st.success(
                    f"{result.entity_name} (CIK {result.cik10}) — "
                    f"{filing_summary}; ZIP ready ({kb:.1f} KB)."
                )
        else:
            st.error(result.error_message or "Download failed.")

        with st.expander("Run log", expanded=not result.ok):
            st.code("\n".join(log_lines) if log_lines else "(no log lines)", language="text")

zip_bytes_sess = st.session_state.get("zip_bytes")
zip_name = st.session_state.get("zip_name") or "SEC_filings.zip"

if zip_bytes_sess is not None:
    st.download_button(
        label="Download filings ZIP",
        data=zip_bytes_sess,
        file_name=zip_name,
        mime="application/zip",
        type="secondary",
    )

