"""Local-only Bronze inventory, streaming Silver conversion and reconciliation.

No historical client is constructed. The only Databento entry point used is
DBNStore.from_file. Publication occurs only after independent Parquet readback.
"""

from __future__ import annotations

import base64
import hashlib
import importlib.metadata
import json
import os
import platform
import re
import sys
import time
import uuid
import warnings
from collections import Counter
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import databento as db
import numpy as np
import psutil
import pyarrow as pa
import pyarrow.parquet as pq
import zstandard as zstd

from src.data.mbp1_migration import migration_fingerprint
from src.data.mbp1_paths import ROOT, project_path
from src.data.mbp1_validation import DEGRADED, NY, SessionAudit, session_bounds
from src.resources import machine_memory_gb

VERSION = "1.0.0"
CHUNK_ROWS = 250_000
PART_ROWS = 5_000_000
RAW = ROOT / "data/raw/DB MBP-1 DATA"
SILVER = ROOT / "data/processed/mbp1"
META = ROOT / "data/metadata/mbp1"
NAME = re.compile(r"GC\.v\.0_(\d{4}-\d{2}-\d{2})_0700-1200_NY_mbp-1\.dbn\.zst$")
PACKAGES = ("databento", "databento-dbn", "numpy", "pyarrow", "zstandard", "psutil")


def json_write(path: Path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(path.name + ".tmp")
    temp.write_text(json.dumps(value, indent=2, sort_keys=True, default=str), encoding="utf-8")
    os.replace(temp, path)


def sha256_file(path: Path) -> str:
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def identity() -> dict:
    paths = [Path(__file__), ROOT / "src/data/mbp1_validation.py",
             ROOT / "project_docs/mbp1_ingestion_contract.md",
             ROOT / "src/data/mbp1_paths.py", ROOT / "src/data/mbp1_migration.py",
             ROOT / "src/resources.py"]
    return {"pipeline_version": VERSION, "python": platform.python_version(),
            "libraries": {p: importlib.metadata.version(p) for p in PACKAGES},
            "code_sha256": {p.name: sha256_file(p) for p in paths},
            "chunk_rows": CHUNK_ROWS, "part_rows": PART_ROWS, "workers": 1}


def records_table(rows: list[dict]) -> pa.Table:
    """Metadata tables have JSON for nested values and exact int64 counters."""
    cleaned = [{k: json.dumps(v, sort_keys=True, default=str) if isinstance(v, (dict, list)) else v
                for k, v in row.items()} for row in rows]
    # from_pylist takes field names from the first row; union keys explicitly.
    keys = sorted({key for row in cleaned for key in row})
    return pa.Table.from_pylist([{key: row.get(key) for key in keys} for row in cleaned])


def write_metadata(name: str, rows: list[dict]):
    import pyarrow.csv as csv

    table = records_table(rows)
    for extension in ("parquet", "csv"):
        target = META / f"{name}.{extension}"
        temp = target.with_suffix(target.suffix + ".tmp")
        if extension == "parquet":
            pq.write_table(table, temp, compression="zstd")
        else:
            csv.write_csv(table, temp)
        os.replace(temp, target)


def inspect_source(path: Path, dtype: np.dtype | None) -> tuple[dict, np.dtype | None]:
    """Hash and verify complete Zstandard frames, including clean EOF."""
    path = project_path(path).resolve()
    stat = path.stat()
    match = NAME.fullmatch(path.name)
    recorded_path = path.relative_to(ROOT) if path.is_relative_to(ROOT) else path
    row = {"raw_file": recorded_path.as_posix(), "file_bytes": stat.st_size,
           "mtime_ns": stat.st_mtime_ns, "session_date": match[1] if match else None,
           "done_marker": Path(str(path) + ".done").is_file(), "readable": False,
           "processing_status": "INVENTORIED", "error": None}
    store = None
    try:
        if not match:
            raise ValueError("Unexpected raw filename")
        day = match[1]
        if path.parent.name != day[:4]:
            raise ValueError("Year directory disagrees with session date")
        if not row["done_marker"]:
            raise ValueError("Missing completion marker")
        marker = Path(str(path) + ".done")
        row["done_sha256"] = sha256_file(marker)
        row["done_bytes"] = marker.stat().st_size
        digest = hashlib.sha256()
        decoder = zstd.ZstdDecompressor().decompressobj()
        decoded_bytes = 0
        frame_count = 0
        with path.open("rb") as stream:
            while block := stream.read(64 * 1024):
                digest.update(block)
                pending = block
                while pending:
                    if decoder.eof:
                        decoder = zstd.ZstdDecompressor().decompressobj()
                    decoded_bytes += len(decoder.decompress(pending))
                    pending = decoder.unused_data
                    if decoder.eof:
                        frame_count += 1
        if not decoder.eof:
            raise ValueError("Incomplete Zstandard frame at EOF")
        row.update(sha256=digest.hexdigest(), uncompressed_bytes=decoded_bytes,
                   zstd_frames=frame_count)
        store = db.DBNStore.from_file(path)
        m = store.metadata
        # Preserve the exact metadata bytes in addition to human-readable values.
        encoded = m.encode()
        row.update(dbn_metadata_b64=base64.b64encode(encoded).decode(),
                   dbn_metadata_repr=str(m), dbn_version=m.version,
                   mappings=m.mappings, schema=str(m.schema), dataset=m.dataset,
                   metadata_start=m.start, metadata_end=m.end,
                   metadata_not_found=list(m.not_found), metadata_partial=list(m.partial))
        if (m.dataset != "GLBX.MDP3" or str(m.schema) != "mbp-1"
                or str(m.stype_in) != "continuous" or str(m.stype_out) != "instrument_id"
                or m.symbols != ["GC.v.0"] or m.ts_out or m.limit is not None):
            raise ValueError("DBN metadata differs from the frozen request")
        if (m.start, m.end) != session_bounds(day):
            raise ValueError("DBN request window differs from DST-aware session bounds")
        with warnings.catch_warnings():
            warnings.simplefilter("error")
            sample = next(iter(store.to_ndarray(count=1)), None)
        if sample is not None:
            if dtype is not None and sample.dtype != dtype:
                raise ValueError("Native dtype changed between files")
            dtype = sample.dtype
        if dtype is None:
            raise ValueError("Inspect a nonempty source before empty sources")
        payload_bytes = decoded_bytes - len(encoded)
        if payload_bytes < 0 or payload_bytes % dtype.itemsize:
            raise ValueError("Incomplete DBN record or metadata length mismatch")
        row.update(record_count=payload_bytes // dtype.itemsize,
                   empty=payload_bytes == 0, readable=True,
                   degraded_source=day in DEGRADED,
                   validation_status="INVENTORIED")
        after = path.stat()
        if (after.st_size, after.st_mtime_ns) != (stat.st_size, stat.st_mtime_ns):
            raise ValueError("Source changed during inventory")
    except Exception as exc:
        # Fail closed after persisting the error; this is not a numerical fallback.
        row.update(error=f"{type(exc).__name__}: {exc}", validation_status="ERROR")
    finally:
        if store is not None:
            store.reader.close()
    return row, dtype


def inventory(raw: Path = RAW) -> tuple[list[dict], np.dtype]:
    META.mkdir(parents=True, exist_ok=True)
    files = sorted(p for p in raw.rglob("*") if p.is_file())
    raw_files = [p for p in files if p.name.endswith(".dbn.zst")]
    extras = [p.as_posix() for p in files if not p.name.endswith((".dbn.zst", ".dbn.zst.done"))]
    orphans = [p.as_posix() for p in files if p.name.endswith(".done")
               and not Path(str(p)[:-5]).is_file()]
    rows, dtype = [], None
    for i, path in enumerate(raw_files):
        row, dtype = inspect_source(path, dtype)
        rows.append(row)
        if i % 100 == 0:
            print(f"INVENTORY {i + 1}/{len(raw_files)} {row['session_date']}", flush=True)
    expected = set()
    d = date(2021, 9, 27)
    while d <= date(2026, 9, 26):
        if d.weekday() < 5:
            expected.add(d.isoformat())
        d += timedelta(days=1)
    dates = Counter(row["session_date"] for row in rows)
    issues = {"unexpected_files": extras, "orphan_markers": orphans,
              "missing_dates": sorted(expected - dates.keys()),
              "unexpected_dates": sorted(str(x) for x in dates.keys() - expected),
              "duplicate_dates": [d for d, n in dates.items() if n > 1]}
    json_write(META / "inventory_issues.json", issues)
    json_write(META / "raw_inventory.json", rows)
    write_metadata("raw_manifest", rows)
    if any(issues.values()) or any(row["error"] for row in rows):
        raise ValueError("Raw inventory failed; inspect data/metadata/mbp1")
    if dtype is None:
        raise ValueError("No decodable source files")
    json_write(META / "native_schema.json", {"dtype": dtype.descr, "itemsize": dtype.itemsize,
               "price_scale": 1_000_000_000, "undef_price": 2**63 - 1,
               "undef_timestamp": 2**64 - 1, "timezone": str(NY)})
    return rows, dtype


def native_to_arrow(a: np.ndarray, day: str, offset: int, state: dict) -> pa.Table:
    """Native fields are lossless; all normalization fields are additive."""
    columns = {}
    for name in a.dtype.names:
        values = a[name].copy()
        if name in ("ts_event", "ts_recv"):
            columns[name + "_raw"] = pa.array(values)
            valid = values != 2**64 - 1
            if np.any(values[valid] > np.iinfo(np.int64).max):
                raise ValueError("Timestamp exceeds Arrow signed-nanosecond range")
            columns[name] = pa.array(values.astype(np.int64), mask=~valid,
                                      type=pa.timestamp("ns", "UTC"))
        elif values.dtype.kind == "S":
            columns[name] = pa.array(values.astype("U1"))
        else:
            columns[name] = pa.array(values)
    n = len(a)
    columns["session_date_ny"] = pa.array([date.fromisoformat(day)] * n, type=pa.date32())
    columns["event_idx_day"] = pa.array(np.arange(offset, offset + n, dtype=np.uint64))
    columns["ts_event_ny"] = columns["ts_event"].cast(pa.timestamp("ns", str(NY)))
    start, _ = session_bounds(day)
    delta = a["ts_event"].astype(np.int64) - start
    columns["ns_from_0700"] = pa.array(delta, mask=a["ts_event"] == 2**64 - 1)
    columns["minute_from_0700"] = pa.array(delta // (60 * 10**9), mask=a["ts_event"] == 2**64 - 1)
    ids = a["instrument_id"]
    changes = np.empty(n, dtype=bool)
    changes[0] = state["last_id"] is not None and int(ids[0]) != state["last_id"]
    changes[1:] = ids[1:] != ids[:-1]
    segments = state["roll_segment"] + np.cumsum(changes, dtype=np.uint32)
    columns["contract_change"] = pa.array(changes)
    columns["roll_segment"] = pa.array(segments)
    state["last_id"] = int(ids[-1])
    state["roll_segment"] = int(segments[-1])
    return pa.table(columns).replace_schema_metadata({
        b"mbp1_pipeline": VERSION.encode(), b"price_scale": b"1000000000",
        b"ordering": b"session_date_ny,event_idx_day; original delivery order",
        b"session_date_semantics": b"requested New York session, not inferred event date",
    })


def arrow_to_native(table: pa.Table, dtype: np.dtype) -> np.ndarray:
    """Independent reconstruction used to compare every native byte in order."""
    result = np.empty(table.num_rows, dtype=dtype)
    for name in dtype.names:
        source = name + "_raw" if name in ("ts_event", "ts_recv") else name
        values = table[source].to_numpy(zero_copy_only=False)
        result[name] = values
    return result


def reconcile_partition(folder: Path, files: list[dict], sessions: list[dict], dtype: np.dtype):
    expected = {s["session_date"]: s for s in sessions}
    counts = Counter()
    digests = {day: hashlib.sha256() for day in expected}
    previous_day = ""
    total = 0
    native_columns = [n + "_raw" if n in ("ts_event", "ts_recv") else n for n in dtype.names]
    for part in files:
        path = folder / part["name"]
        parquet = pq.ParquetFile(path, page_checksum_verification=True)
        if parquet.metadata.num_rows != part["rows"]:
            raise ValueError("Parquet footer differs from writer count")
        for batch in parquet.iter_batches(batch_size=CHUNK_ROWS, use_threads=False,
                columns=native_columns + ["session_date_ny", "event_idx_day"]):
            table = pa.Table.from_batches([batch])
            days = table["session_date_ny"].to_numpy()
            native = arrow_to_native(table, dtype)
            starts = np.r_[0, np.flatnonzero(days[1:] != days[:-1]) + 1, len(days)]
            for left, right in zip(starts[:-1], starts[1:], strict=True):
                day = str(days[left])
                if day < previous_day or day not in expected:
                    raise ValueError("Unrecognized or reordered Silver session")
                previous_day = day
                idx = table["event_idx_day"].slice(int(left), int(right - left)).to_numpy()
                if not np.array_equal(idx, np.arange(counts[day], counts[day] + len(idx))):
                    raise ValueError("Delivery order index is discontinuous")
                digests[day].update(native[left:right].tobytes())
                counts[day] += int(right - left)
            total += len(native)
        parquet.close(force=True)
    for day, session in expected.items():
        if counts[day] != session["record_count"] or digests[day].hexdigest() != session["native_sha256"]:
            raise ValueError(f"Raw/Silver native-byte reconciliation failed: {day}")
    return {"records": total, "sessions": len(expected), "native_digest_match": True,
            "delivery_order_match": True}


def publish_directory(source: Path, destination: Path):
    """Retry transient Windows scanner locks; never fall back to a partial copy."""
    import gc

    gc.collect()
    for attempt in range(10):
        try:
            os.replace(source, destination)
            return
        except PermissionError:
            if attempt == 9:
                raise
            time.sleep(min(0.25 * (attempt + 1), 2))


def convert_month(month: str, rows: list[dict], dtype: np.dtype, state: dict,
                  config: dict, root: Path = SILVER, *, reuse_only: bool = False) -> dict:
    started = time.perf_counter()
    fingerprint = hashlib.sha256(json.dumps({"config": config, "sources": rows,
                    "incoming_state": state}, sort_keys=True, default=str).encode()).hexdigest()
    relative = Path(f"year={month[:4]}") / f"month={month[5:]}"
    destination = root / relative
    completion = destination / "_SUCCESS.json"
    if completion.exists():
        old = json.loads(completion.read_text())
        migrated = migration_fingerprint(config, rows, state)
        if old["fingerprint"] in (fingerprint, migrated):
            for part in old["files"]:
                path = destination / part["name"]
                if path.stat().st_size != part["bytes"] or sha256_file(path) != part["sha256"]:
                    raise ValueError(f"Completed output was modified: {path}")
            state.update(old["outgoing_state"])
            print(f"REUSE {month}: {old['reconciliation']['records']:,} rows", flush=True)
            return old
    if reuse_only:
        raise ValueError(f"Reuse-only check failed for {month}; no partition was rebuilt")
    # Global catalog is invalidated before any replacement; access fails closed.
    (root / "_SUCCESS.json").unlink(missing_ok=True)
    stage = root / "_staging" / f"{month}-{uuid.uuid4().hex}"
    stage.mkdir(parents=True)
    files, sessions, segments = [], [], []
    writer = None
    part_rows = 0
    part_path = None
    try:
        for source in rows:
            day = source["session_date"]
            path = project_path(source["raw_file"])
            before = path.stat()
            if (before.st_size, before.st_mtime_ns) != (source["file_bytes"], source["mtime_ns"]):
                raise ValueError("Source identity changed after inventory")
            store = db.DBNStore.from_file(path)
            mappings = store.metadata.mappings.get("GC.v.0", [])
            mapped = {int(m["symbol"]) for m in mappings
                      if str(m["start_date"]) <= day < str(m["end_date"])}
            audit = SessionAudit(day, mapped)
            digest = hashlib.sha256()
            try:
                with warnings.catch_warnings():
                    warnings.simplefilter("error")
                    for a in store.to_ndarray(count=CHUNK_ROWS):
                        if not len(a):
                            continue
                        if a.dtype != dtype:
                            raise ValueError("Native dtype changed")
                        offset = audit.rows
                        audit.consume(a)
                        digest.update(a.tobytes())
                        table = native_to_arrow(a, day, offset, state)
                        changes = table["contract_change"].to_numpy()
                        indices = np.flatnonzero(changes)
                        if offset == 0:
                            indices = np.unique(np.r_[0, indices])
                        for index in indices:
                            segments.append({"session_date": day, "event_idx_day": int(offset + index),
                                "instrument_id": int(a["instrument_id"][index]),
                                "roll_segment": int(table["roll_segment"][int(index)].as_py()),
                                "contract_change": bool(changes[index]),
                                "ts_recv_ns": int(a["ts_recv"][index])})
                        if writer is None:
                            part_path = stage / f"part-{len(files):04d}.parquet"
                            writer = pq.ParquetWriter(part_path, table.schema, compression="zstd",
                                compression_level=3, use_dictionary=True, write_statistics=True,
                                write_page_checksum=True)
                        writer.write_table(table, row_group_size=CHUNK_ROWS)
                        part_rows += len(a)
                        if part_rows >= PART_ROWS:
                            writer.close()
                            writer = None
                            files.append({"name": part_path.name, "rows": part_rows})
                            part_rows = 0
            finally:
                store.reader.close()
            if audit.rows != source["record_count"]:
                raise ValueError(f"Structural/decoded count mismatch: {day}")
            after = path.stat()
            if (after.st_size, after.st_mtime_ns) != (before.st_size, before.st_mtime_ns):
                raise ValueError("Source changed during conversion")
            session = audit.result()
            session["native_sha256"] = digest.hexdigest()
            sessions.append(session)
        if writer is not None:
            writer.close()
            writer = None
            files.append({"name": part_path.name, "rows": part_rows})
        reconciliation = reconcile_partition(stage, files, sessions, dtype)
        for part in files:
            path = stage / part["name"]
            part.update(bytes=path.stat().st_size, sha256=sha256_file(path))
        result = {"month": month, "path": relative.as_posix(), "fingerprint": fingerprint,
                  "files": files, "sessions": sessions, "segments": segments,
                  "outgoing_state": dict(state), "reconciliation": reconciliation,
                  "seconds": time.perf_counter() - started,
                  "peak_working_set_bytes": getattr(psutil.Process().memory_info(), "peak_wset", 0)}
        json_write(stage / "_SUCCESS.json", result)
        destination.parent.mkdir(parents=True, exist_ok=True)
        if destination.exists():
            archive = root / "_superseded" / f"{month}-{uuid.uuid4().hex}"
            archive.parent.mkdir(exist_ok=True)
            publish_directory(destination, archive)
        publish_directory(stage, destination)
        print(f"COMPLETE {month}: {reconciliation['records']:,} rows, "
              f"{len(files)} parts, {result['seconds']:.1f}s", flush=True)
        return result
    finally:
        if writer is not None:
            writer.close()


def activity_flags(sessions: list[dict]):
    history = []
    for session in sessions:
        if len(history) >= 10:
            for field in ("record_count", "trade_volume"):
                median = float(np.median([s[field] for s in history[-20:]]))
                value = session[field]
                if median > 0 and (value < median * 0.1 or value > median * 10):
                    session["warnings"].append(field + "_activity_outlier")
                    if "WARNING" not in session["validation_status"]:
                        session["validation_status"] += " | WARNING"
        if not session["empty"]:
            history.append(session)


def run_ingestion(*, reuse_only: bool = False):
    """One writer, monthly checkpoints, persistent status on every failure."""
    META.mkdir(parents=True, exist_ok=True)
    SILVER.mkdir(parents=True, exist_ok=True)
    lock = SILVER / "_RUNNING.lock"
    # OS advisory locking releases automatically after a crash, unlike stale PID files.
    with lock.open("a+b") as lockfile:
        if os.name == "nt":
            import msvcrt
            lockfile.seek(0)
            if lockfile.read(1) == b"":
                lockfile.write(b"0")
                lockfile.flush()
            lockfile.seek(0)
            msvcrt.locking(lockfile.fileno(), msvcrt.LK_NBLCK, 1)
        else:
            import fcntl
            fcntl.flock(lockfile.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        started = time.perf_counter()
        config = identity()
        config["memory_gb"] = machine_memory_gb()[0]
        json_write(META / "run_status.json", {"state": "RUNNING", "pid": os.getpid(), "config": config})
        try:
            rows, dtype = inventory()
            months = sorted({r["session_date"][:7] for r in rows})
            state = {"last_id": None, "roll_segment": 1}
            partitions = []
            for month in months:
                subset = [r for r in rows if r["session_date"].startswith(month)]
                partitions.append(convert_month(month, subset, dtype, state, config,
                                                reuse_only=reuse_only))
                json_write(META / "run_status.json", {"state": "RUNNING", "last_month": month,
                           "completed_months": len(partitions), "total_months": len(months)})
            sessions = [s for p in partitions for s in p["sessions"]]
            activity_flags(sessions)
            by_day = {s["session_date"]: s for s in sessions}
            manifest = [dict(r, **{k: v for k, v in by_day[r["session_date"]].items()
                                  if k != "session_date"}, processing_status="RECONCILED") for r in rows]
            write_metadata("raw_manifest", manifest)
            write_metadata("mbp1_data_quality", sessions)
            json_write(META / "mbp1_data_quality.json", sessions)
            segments = [s for p in partitions for s in p["segments"]]
            write_metadata("contract_segments", segments)
            raw_count = sum(r["record_count"] for r in rows)
            silver_count = sum(p["reconciliation"]["records"] for p in partitions)
            if raw_count != silver_count:
                raise ValueError("Global count reconciliation failed")
            summary = {"config": config, "created_utc": datetime.now(timezone.utc).isoformat(),
                "operation": "VERIFIED_CHECKPOINT_REUSE" if reuse_only else "INGEST_OR_REUSE",
                "migration_record": "data/metadata/mbp1/migration/migration_provenance.json",
                "dataset": "GLBX.MDP3", "schema": "mbp-1", "symbol": "GC.v.0",
                "stype_in": "continuous", "start": "2021-09-27", "end": "2026-09-26",
                "session": "07:00 <= ts_recv NY < 12:00", "timezone": str(NY),
                "raw_files": len(rows), "raw_bytes": sum(r["file_bytes"] for r in rows),
                "raw_records": raw_count, "silver_records": silver_count,
                "excluded_records": 0, "months": len(partitions),
                "parquet_files": sum(len(p["files"]) for p in partitions),
                "parquet_bytes": sum(f["bytes"] for p in partitions for f in p["files"]),
                "degraded_dates": list(DEGRADED),
                "empty_dates": [s["session_date"] for s in sessions if s["empty"]],
                "error_sessions": [s["session_date"] for s in sessions if s["errors"]],
                "warning_sessions": sum(bool(s["warnings"]) for s in sessions),
                "incomplete_duplicate_audit_sessions": [s["session_date"] for s in sessions
                                                       if not s["duplicate_coverage_complete"]],
                "roll_segments": state["roll_segment"], "seconds": time.perf_counter() - started,
                "peak_working_set_bytes": getattr(psutil.Process().memory_info(), "peak_wset", 0),
                "native_reconciliation": "ALL_NATIVE_BYTES_AND_DELIVERY_ORDER_MATCH",
                "partitions": [{k: p[k] for k in ("month", "path", "fingerprint")} for p in partitions]}
            json_write(META / "provenance.json", summary)
            json_write(SILVER / "_SUCCESS.json", summary)
            json_write(META / "run_status.json", {"state": "COMPLETE", "records": raw_count})
            print(json.dumps({k: v for k, v in summary.items() if k != "partitions"}, indent=2), flush=True)
        except Exception as exc:
            json_write(META / "run_status.json", {"state": "FAILED", "error": repr(exc)})
            raise


if __name__ == "__main__":
    if sys.argv[1:] not in ([], ["--reuse-only"]):
        raise SystemExit("Usage: python -m src.data.mbp1_ingest [--reuse-only] (local files only)")
    run_ingestion(reuse_only="--reuse-only" in sys.argv[1:])
