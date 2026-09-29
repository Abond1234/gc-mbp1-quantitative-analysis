"""Read-only data verification; writes only a separate migration audit report."""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import numpy as np
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq

from src.data.mbp1_access import mbp1_files, scan_mbp1
from src.data.mbp1_ingest import META, RAW, SILVER, json_write, reconcile_partition, sha256_file
from src.data.mbp1_paths import ROOT


def verify(hashes: bool = False):
    catalog = json.loads((SILVER / "_SUCCESS.json").read_text())
    original = json.loads((META / "migration/original_catalog.json").read_text())
    raw = json.loads((META / "raw_inventory.json").read_text())
    quality = json.loads((META / "mbp1_data_quality.json").read_text())
    assert catalog["partitions"] == original["partitions"]
    files = list(RAW.rglob("*.dbn.zst"))
    markers = list(RAW.rglob("*.done"))
    assert len(files) == len(markers) == len(raw) == 1305
    assert all(Path(str(p) + ".done").is_file() for p in files)
    assert sum(p.stat().st_size for p in files) == 17_232_496_150
    assert sum(r["record_count"] for r in raw) == 786_491_001
    checkpoints = []
    footer_rows = parquet_bytes = parquet_files = 0
    for partition in catalog["partitions"]:
        checkpoint = json.loads((SILVER / partition["path"] / "_SUCCESS.json").read_text())
        assert checkpoint["fingerprint"] == partition["fingerprint"]
        assert checkpoint["reconciliation"]["native_digest_match"]
        assert checkpoint["reconciliation"]["delivery_order_match"]
        checkpoints.append(checkpoint)
        for part in checkpoint["files"]:
            path = SILVER / partition["path"] / part["name"]
            assert path.stat().st_size == part["bytes"]
            with pq.ParquetFile(path) as pf:
                assert pf.metadata.num_rows == part["rows"]
                footer_rows += pf.metadata.num_rows
            if hashes:
                assert sha256_file(path) == part["sha256"]
            parquet_bytes += part["bytes"]
            parquet_files += 1
    if hashes:
        for row in raw:
            path = ROOT / row["raw_file"]
            assert sha256_file(path) == row["sha256"]
            assert sha256_file(Path(str(path) + ".done")) == row["done_sha256"]
    assert len(checkpoints) == 61 and parquet_files == 184
    assert footer_rows == sum(c["reconciliation"]["records"] for c in checkpoints) == 786_491_001
    assert parquet_bytes == 30_390_798_465
    assert len([r for r in quality if r["empty"]]) == 14
    assert sum(bool(r["warnings"]) for r in quality) == 61
    assert not any(r["errors"] for r in quality)
    assert sorted(r["session_date"] for r in raw if r["degraded_source"]) == sorted(original["degraded_dates"])
    replacement = next(r for r in raw if r["session_date"] == "2026-09-09")
    assert replacement["record_count"] == 914_441 and replacement["done_marker"]
    audit = json.loads((META / "silver_invariant_audit.json").read_text())
    assert audit["status"] == "PASS" and audit["records_checked"] == footer_rows
    assert audit["dataset_fingerprint"] == hashlib.sha256(
        json.dumps(catalog["partitions"], sort_keys=True).encode()).hexdigest()

    queries = []
    for start, end, columns in [
        ("2024-09-03", "2024-09-03", None),
        ("2024-09-01", "2024-09-30", ["session_date_ny", "price", "instrument_id"]),
    ]:
        scanner = scan_mbp1(start, end, columns)
        if columns:
            assert scanner.projected_schema.names == columns
        count = sum(b.num_rows for b in scanner.to_batches())
        expected = sum(r["record_count"] for r in raw if start <= r["session_date"] <= end)
        assert count == expected
        selected = mbp1_files(start, end)
        assert all("year=2024" in p and "month=09" in p for p in selected)
        queries.append({"start": start, "end": end, "rows": count, "candidate_files": len(selected)})
    dataset = ds.dataset(mbp1_files("2024-09-03", "2024-09-03"), format="parquet")
    predicate = ds.field("session_date_ny") == pa.scalar(date(2024, 9, 3))
    fragments = list(dataset.get_fragments())
    selected_row_groups = sum(len(list(f.split_by_row_group(filter=predicate))) for f in fragments)
    total_row_groups = sum(f.num_row_groups for f in fragments)
    assert 0 < selected_row_groups < total_row_groups
    segments = pq.read_table(META / "contract_segments.parquet").to_pylist()
    assert len({r["roll_segment"] for r in segments}) == 26
    transition_index = next(i for i, r in enumerate(segments) if r["contract_change"])
    previous, transition = segments[transition_index-1:transition_index+1]
    roll_queries = []
    for row in (previous, transition):
        day = row["session_date"]
        scanner = scan_mbp1(day, day, ["event_idx_day", "instrument_id", "roll_segment", "contract_change"])
        first = None
        count = 0
        for batch in scanner.to_batches():
            if batch.num_rows and first is None:
                first = batch.slice(0, 1).to_pylist()[0]
            count += batch.num_rows
        assert first["event_idx_day"] == 0
        assert first["instrument_id"] == row["instrument_id"]
        assert first["roll_segment"] == row["roll_segment"]
        assert first["contract_change"] == row["contract_change"]
        assert count == next(r["record_count"] for r in raw if r["session_date"] == day)
        roll_queries.append({"day": day, "rows": count, "first": first})
    try:
        scan_mbp1("2025-01-01", "2025-01-02")
    except PermissionError:
        guard = "PASS"
    else:
        raise AssertionError("Final-test guard did not reject access")
    schema = json.loads((META / "native_schema.json").read_text())
    dtype = np.dtype([tuple(field) for field in schema["dtype"]])
    month = checkpoints[0]
    native_check = reconcile_partition(SILVER / month["path"], month["files"], month["sessions"], dtype)
    result = {"status": "PASS", "raw_files": len(files), "markers": len(markers),
              "raw_records": sum(r["record_count"] for r in raw), "silver_footer_records": footer_rows,
              "months": len(checkpoints), "parquet_files": parquet_files, "parquet_bytes": parquet_bytes,
              "all_partition_fingerprints_preserved": True, "full_hashes_this_run": hashes,
              "queries": queries, "selected_row_groups": selected_row_groups,
              "total_month_row_groups": total_row_groups, "roll_boundary": roll_queries,
              "native_reconciliation_month": month["month"], "native_reconciliation": native_check,
              "final_test_guard": guard, "cwd": str(Path.cwd()), "interpreter": sys.executable,
              "source_import": str(ROOT)}
    json_write(META / "migration/validation.json", result)
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--hashes", action="store_true", help="Rehash every raw, marker and Parquet file")
    verify(parser.parse_args().hashes)
