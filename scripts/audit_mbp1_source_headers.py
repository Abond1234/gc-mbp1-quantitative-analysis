"""Reproduce marker-body and original-header byte checks without API access."""

from __future__ import annotations

import base64
import json
import sys
from importlib.metadata import version
from pathlib import Path
from zoneinfo import TZPATH

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import databento as db

from src.data.mbp1_ingest import META, json_write
from src.data.mbp1_paths import project_path


def audit_source_headers():
    rows = json.loads((META / "raw_inventory.json").read_text())
    header_mismatches, marker_mismatches = [], []
    for row in rows:
        store = db.DBNStore.from_file(project_path(row["raw_file"]))
        expected_header = base64.b64decode(row["dbn_metadata_b64"])
        reader = store.reader
        try:
            reader.seek(0)
            if reader.read(len(expected_header)) != expected_header:
                header_mismatches.append(row["session_date"])
        finally:
            reader.close()
        expected_marker = ["COMPLETE", "date=" + row["session_date"],
                           "window=07:00-12:00 America/New_York", "schema=mbp-1", "symbol=GC.v.0"]
        if project_path(row["raw_file"] + ".done").read_text().splitlines() != expected_marker:
            marker_mismatches.append(row["session_date"])
    json_write(META / "metadata_byte_audit.json", {
        "sources_checked": len(rows),
        "metadata_encoded_bytes_equal_original_header": not header_mismatches,
        "mismatch_dates": header_mismatches,
    })
    json_write(META / "done_marker_audit.json", {
        "markers_checked": len(rows), "content_matches_request": not marker_mismatches,
        "mismatch_dates": marker_mismatches,
    })
    json_write(META / "timezone_provenance.json", {
        "tzdata": version("tzdata"), "zoneinfo_TZPATH": list(TZPATH), "timezone": "America/New_York",
    })
    if header_mismatches or marker_mismatches:
        raise ValueError("Header/marker discrepancies found; inspect metadata audit files")
    print(f"PASS: {len(rows)} original metadata headers and completion-marker bodies")


if __name__ == "__main__":
    audit_source_headers()
