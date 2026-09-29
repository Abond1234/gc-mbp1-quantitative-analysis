"""Locate coverage gaps and summarize observed ID segments after reconciliation.

This is a source-quality audit, not a liquidity-selection or feature procedure.
It reads only projected Silver columns on days already flagged by frozen rules.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import numpy as np
import pyarrow.parquet as pq

from src.data.mbp1_access import scan_mbp1
from src.data.mbp1_ingest import META, json_write, write_metadata
from src.data.mbp1_validation import session_bounds


def warning_examples(sessions: list[dict]):
    """Save bounded local context around first non-coverage warning occurrences."""
    examples = []
    columns = ["event_idx_day", "ts_event", "ts_recv", "instrument_id", "sequence",
               "action", "side", "flags", "price", "size", "bid_px_00", "ask_px_00",
               "bid_sz_00", "ask_sz_00", "bid_ct_00", "ask_ct_00"]
    for session in sessions:
        targets = {name: session["first_example_event_idx"][name]
                   for name in session["warnings"]
                   if name in session["first_example_event_idx"] and "gap" not in name}
        if not targets:
            continue
        indices = sorted({i for value in targets.values() for i in range(max(0, value - 2), value + 3)})
        day = session["session_date"]
        records = []
        for batch in scan_mbp1(day, day, columns, allow_final_test=True).to_batches():
            mask = np.isin(batch.column(0).to_numpy(), indices)
            if mask.any():
                records.extend(batch.filter(mask).to_pylist())
        examples.append({"session_date": day, "first_warning_indices": targets, "context": records})
    json_write(META / "warning_examples.json", examples)


def clock_diagnostics(sessions: list[dict]):
    """Measure observed clock differences without aligning or correcting clocks."""
    rows = []
    for session in sessions:
        checks = session["checks"]
        if not any(checks.get(k) for k in ("negative_ts_in_delta", "recv_before_event", "event_outside_session")):
            continue
        day = session["session_date"]
        row = {"session_date": day, "negative_ts_in_delta_count": checks["negative_ts_in_delta"],
               "recv_before_event_count": checks["recv_before_event"],
               "event_outside_session_count": checks["event_outside_session"]}
        for batch in scan_mbp1(day, day, ["ts_event_raw", "ts_recv_raw", "ts_in_delta"],
                               allow_final_test=True).to_batches():
            if not batch.num_rows:
                continue
            event, recv = batch.column(0).to_numpy(), batch.column(1).to_numpy()
            valid = (event != np.iinfo(np.uint64).max) & (recv != np.iinfo(np.uint64).max)
            delay = recv[valid].astype(np.int64) - event[valid].astype(np.int64)
            publisher = batch.column(2).to_numpy()
            for name, values in (("recv_minus_event_ns", delay), ("ts_in_delta", publisher)):
                if not len(values):
                    continue
                low, high = int(values.min()), int(values.max())
                row[name + "_min"] = min(row.get(name + "_min", low), low)
                row[name + "_max"] = max(row.get(name + "_max", high), high)
        rows.append(row)
    if rows:
        write_metadata("clock_diagnostics", rows)
    json_write(META / "clock_diagnostics.json", rows)


def audit_details():
    sessions = json.loads((META / "mbp1_data_quality.json").read_text())
    gaps = []
    for session in sessions:
        day = session["session_date"]
        if session["empty"]:
            continue
        start, end = session_bounds(day)
        if session["checks"].get("leading_gap_over_60s"):
            gaps.append({"session_date": day, "kind": "leading", "left_recv_ns": start,
                         "right_recv_ns": session["first_recv_ns"], "right_event_idx_day": 0})
        if session["checks"].get("trailing_gap_over_60s"):
            gaps.append({"session_date": day, "kind": "trailing", "left_recv_ns": session["last_recv_ns"],
                         "right_recv_ns": end, "right_event_idx_day": session["record_count"]})
        if not session["checks"].get("recv_gap_over_60s"):
            continue
        scan = scan_mbp1(day, day, ["ts_recv_raw", "event_idx_day", "instrument_id"],
                         allow_final_test=True)
        previous = None
        detected = 0
        for batch in scan.to_batches():
            if not batch.num_rows:
                continue
            recv = batch.column(0).to_numpy()
            idx = batch.column(1).to_numpy()
            ids = batch.column(2).to_numpy()
            extended = recv if previous is None else np.r_[previous, recv]
            steps = np.diff(extended.astype(np.int64))
            for i in np.flatnonzero(steps > 60 * 10**9):
                right = i + 1 if previous is None else i
                gaps.append({"session_date": day, "kind": "internal",
                             "left_recv_ns": int(extended[i]), "right_recv_ns": int(extended[i + 1]),
                             "right_event_idx_day": int(idx[right]), "instrument_id": int(ids[right])})
                detected += 1
            previous = int(recv[-1])
        if detected != session["checks"]["recv_gap_over_60s"]:
            raise ValueError(f"Independent gap count differs: {day}")
    for gap in gaps:
        gap["duration_ns"] = gap["right_recv_ns"] - gap["left_recv_ns"]
    if gaps:
        write_metadata("coverage_gaps", gaps)
    json_write(META / "coverage_gaps.json", gaps)
    checkpoints = pq.read_table(META / "contract_segments.parquet").to_pylist()
    segments = {}
    for row in checkpoints:
        key = row["roll_segment"]
        if key not in segments:
            segments[key] = {"roll_segment": key, "instrument_id": row["instrument_id"],
                             "first_session": row["session_date"],
                             "first_event_idx_day": row["event_idx_day"],
                             "first_recv_ns": row["ts_recv_ns"], "contract_change": row["contract_change"]}
        segments[key]["last_session"] = row["session_date"]
    seen = set()
    for segment in segments.values():
        segment["previously_observed_id"] = segment["instrument_id"] in seen
        seen.add(segment["instrument_id"])
    write_metadata("roll_segment_summary", list(segments.values()))
    json_write(META / "quality_details_summary.json", {
        "coverage_gap_intervals": len(gaps),
        "gap_dates": sorted({g["session_date"] for g in gaps}),
        "gap_counts_independently_reconciled": True,
        "unique_observed_instrument_ids": len(seen), "observed_segments": len(segments),
        "id_reappearance_segments": [s for s in segments.values() if s["previously_observed_id"]],
    })
    print("Gap intervals:", len(gaps), "Observed ID segments:", len(segments))


if __name__ == "__main__":
    audit_details()
    completed_sessions = json.loads((META / "mbp1_data_quality.json").read_text())
    warning_examples(completed_sessions)
    clock_diagnostics(completed_sessions)
