"""Projection/pruning over committed MBP-1 partitions without reopening Bronze."""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pyarrow as pa
import pyarrow.dataset as ds

from src.data.mbp1_paths import ROOT

DEFAULT_ROOT = ROOT / "data/processed/mbp1"


def mbp1_files(start: str, end: str, root: str | Path = DEFAULT_ROOT) -> list[str]:
    """Resolve committed months only; dates are inclusive, ISO calendar dates."""
    first, last = date.fromisoformat(start), date.fromisoformat(end)
    if first > last:
        raise ValueError("start must not exceed end")
    root = Path(root)
    catalog = root / "_SUCCESS.json"
    if not catalog.exists():
        raise FileNotFoundError("Store has no reconciled completion catalog; run ingestion first")
    manifest = json.loads(catalog.read_text())
    files = []
    for partition in manifest["partitions"]:
        month = partition["month"]
        if first.strftime("%Y-%m") <= month <= last.strftime("%Y-%m"):
            completion = root / partition["path"] / "_SUCCESS.json"
            checkpoint = json.loads(completion.read_text())
            if checkpoint["fingerprint"] != partition["fingerprint"]:
                raise ValueError("Partition identity differs from published catalog")
            for part in checkpoint["files"]:
                path = completion.parent / part["name"]
                if path.stat().st_size != part["bytes"]:
                    raise ValueError(f"Partition file size mismatch: {path}")
                files.append(str(path))
    if not files:
        raise ValueError("No completed partitions in requested range")
    return files


def scan_mbp1(start: str, end: str, columns: list[str] | None = None,
              root: str | Path = DEFAULT_ROOT, batch_size: int = 250_000,
              allow_final_test: bool = False) -> ds.Scanner:
    """Lazy Arrow scanner, inclusive session dates; chronological file ordering.

    Iterate ``to_batches()`` for bounded memory. ``to_table()`` materializes the
    selection; only then use ``to_pandas()`` or ``polars.from_arrow`` for small
    results. Use ``session_date_ny,event_idx_day`` as explicit order keys in
    downstream SQL/parallel engines, which need not retain scan order.

    Final-test reads require explicit authorization. The ingestion audit's
    full-period access is not general authorization for subsequent research.
    """
    if end >= "2025-01-01" and not allow_final_test:
        raise PermissionError("2025 onward is final test; explicit allow_final_test=True required")
    files = mbp1_files(start, end, root)
    options = ds.ParquetFragmentScanOptions(page_checksum_verification=True, pre_buffer=False)
    parquet_format = ds.ParquetFileFormat(default_fragment_scan_options=options)
    dataset = ds.dataset(files, format=parquet_format)
    predicate = ((ds.field("session_date_ny") >= pa.scalar(date.fromisoformat(start)))
                 & (ds.field("session_date_ny") <= pa.scalar(date.fromisoformat(end))))
    return dataset.scanner(columns=columns, filter=predicate, batch_size=batch_size,
                           batch_readahead=2, fragment_readahead=1, use_threads=False)
