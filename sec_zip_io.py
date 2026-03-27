"""ZIP archives and JSON sidecar writes."""

from __future__ import annotations

import io
import json
import os
import tempfile
import zipfile
from collections.abc import Iterator
from pathlib import Path
from typing import Any

from sec_config import STRUCTURED_FILENAME

# Hold ZIP in RAM up to this size before spilling to disk (SpooledTemporaryFile).
ZIP_SPOOL_MAX_IN_MEMORY = 32 * 1024 * 1024


def save_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def zip_pdfs_from_directory(out_dir: Path) -> bytes:
    """Build a ZIP archive containing every ``*.pdf`` file in ``out_dir`` (names at archive root)."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for p in sorted(out_dir.glob("*.pdf")):
            if p.is_file():
                zf.write(p, arcname=p.name)
    return buf.getvalue()


def _zip_arcname_safe(name: str) -> str:
    base = os.path.basename(name.replace("\\", "/"))
    if not base or base in (".", "..") or base.startswith(".."):
        raise ValueError(f"Unsafe ZIP entry name: {name!r}")
    return base


def _is_zip_included_artifact(name: str) -> bool:
    if name == STRUCTURED_FILENAME:
        return True
    low = name.lower()
    return low.endswith((".pdf", ".html", ".htm"))


def _artifact_zip_members(artifacts: dict[str, bytes]) -> Iterator[tuple[str, bytes]]:
    for name in sorted(artifacts):
        if _is_zip_included_artifact(name):
            yield _zip_arcname_safe(name), artifacts[name]


def zip_artifacts_bytes(artifacts: dict[str, bytes]) -> bytes:
    """ZIP in memory: filings (``*.pdf`` and/or ``*.html``/``.htm``) and ``structured_data.json``."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for arc, data in _artifact_zip_members(artifacts):
            zf.writestr(arc, data)
    return buf.getvalue()


def zip_artifacts_spooled(
    artifacts: dict[str, bytes],
    *,
    max_in_memory: int = ZIP_SPOOL_MAX_IN_MEMORY,
) -> tempfile.SpooledTemporaryFile[bytes]:
    """Build ZIP on a spooled buffer (RAM until ``max_in_memory``, then temp disk).

    Caller must ``close()`` the returned file when done (or let it GC). Position is 0
    for reading (e.g. ``st.download_button``).
    """
    spool: tempfile.SpooledTemporaryFile[bytes] = tempfile.SpooledTemporaryFile(
        max_size=max_in_memory,
        mode="w+b",
    )
    try:
        with zipfile.ZipFile(spool, "w", zipfile.ZIP_DEFLATED) as zf:
            for arc, data in _artifact_zip_members(artifacts):
                zf.writestr(arc, data)
    except Exception:
        spool.close()
        raise
    spool.seek(0)
    return spool
