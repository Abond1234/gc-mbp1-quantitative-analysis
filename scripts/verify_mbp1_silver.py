"""Independent global invariants for all additive Silver fields, without Bronze."""

from __future__ import annotations

import hashlib
import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import numpy as np
import psutil
import pyarrow as pa

from src.data.mbp1_access import scan_mbp1
from src.data.mbp1_ingest import META, json_write
from src.data.mbp1_validation import session_bounds


def verify_silver_fields():
    started = time.perf_counter()
    provenance = json.loads((META / "provenance.json").read_text())
    columns = ["ts_event_raw", "ts_event", "ts_event_ny", "ts_recv_raw", "ts_recv",
               "session_date_ny", "event_idx_day", "ns_from_0700", "minute_from_0700",
               "instrument_id", "roll_segment", "contract_change"]
    records = 0
    previous_id = None
    previous_day = None
    previous_segment = 1
    previous_idx = -1
    observed_peak = 0
    for batch in scan_mbp1("2021-09-27", "2026-09-25", columns,
                           allow_validation=True, allow_final_test=True).to_batches():
        if not batch.num_rows:
            continue
        table = pa.Table.from_batches([batch])
        for raw_name, views in (("ts_event_raw", ["ts_event", "ts_event_ny"]),
                                ("ts_recv_raw", ["ts_recv"])):
            raw = table[raw_name].to_numpy()
            valid = raw != np.iinfo(np.uint64).max
            for name in views:
                col = table[name]
                expected_tz = "America/New_York" if name.endswith("_ny") else "UTC"
                if col.type != pa.timestamp("ns", expected_tz):
                    raise ValueError(f"Timestamp type mismatch: {name}")
                if not np.array_equal(col.is_null().to_numpy(), ~valid):
                    raise ValueError(f"Timestamp null mismatch: {name}")
                integers = col.cast(pa.int64()).fill_null(0).to_numpy()
                if not np.array_equal(integers[valid].astype(np.uint64), raw[valid]):
                    raise ValueError(f"Timestamp value mismatch: {name}")
        ids = table["instrument_id"].to_numpy()
        changes = np.r_[previous_id is not None and int(ids[0]) != previous_id, ids[1:] != ids[:-1]]
        expected_segments = previous_segment + np.cumsum(changes, dtype=np.uint32)
        if not np.array_equal(table["contract_change"].to_numpy(), changes):
            raise ValueError("Contract change mismatch")
        if not np.array_equal(table["roll_segment"].to_numpy(), expected_segments):
            raise ValueError("Roll segment mismatch")
        previous_id, previous_segment = int(ids[-1]), int(expected_segments[-1])
        days = table["session_date_ny"].to_numpy()
        indices = table["event_idx_day"].to_numpy()
        event = table["ts_event_raw"].to_numpy()
        elapsed = table["ns_from_0700"].fill_null(0).to_numpy()
        minutes = table["minute_from_0700"].fill_null(0).to_numpy()
        boundaries = np.r_[0, np.flatnonzero(days[1:] != days[:-1]) + 1, len(days)]
        for left, right in zip(boundaries[:-1], boundaries[1:], strict=True):
            day = str(days[left])
            if previous_day is not None and day < previous_day:
                raise ValueError("Session ordering mismatch")
            start_idx = previous_idx + 1 if day == previous_day else 0
            if not np.array_equal(indices[left:right], np.arange(start_idx, start_idx + right - left)):
                raise ValueError("Event index mismatch")
            start_ns, _ = session_bounds(day)
            valid = event[left:right] != np.iinfo(np.uint64).max
            expected = event[left:right][valid].astype(np.int64) - start_ns
            if not np.array_equal(elapsed[left:right][valid], expected):
                raise ValueError("Session nanosecond offset mismatch")
            if not np.array_equal(minutes[left:right][valid], expected // (60 * 10**9)):
                raise ValueError("Session minute offset mismatch")
            previous_day, previous_idx = day, int(indices[right - 1])
        records += batch.num_rows
        observed_peak = max(observed_peak, psutil.Process().memory_info().rss)
    if records != provenance["silver_records"] or previous_segment != provenance["roll_segments"]:
        raise ValueError("Global record/segment totals disagree")
    result = {"status": "PASS", "records_checked": records, "last_session": previous_day,
              "dataset_fingerprint": hashlib.sha256(json.dumps(
                  provenance["partitions"], sort_keys=True).encode()).hexdigest(),
              "observed_id_segments": previous_segment,
              "checks": ["UTC_nanoseconds", "New_York_timezone", "undefined_timestamp_views",
                         "delivery_indices", "session_offsets", "contract_change", "roll_segment"],
              "seconds": time.perf_counter() - started, "observed_peak_rss_bytes": observed_peak}
    json_write(META / "silver_invariant_audit.json", result)
    print(json.dumps(result, indent=2), flush=True)


if __name__ == "__main__":
    verify_silver_fields()
