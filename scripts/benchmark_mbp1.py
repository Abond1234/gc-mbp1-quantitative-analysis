"""Bounded Arrow queries and persistent audit report; run after ingestion."""

from __future__ import annotations

import json
import sys
import time
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import psutil
import pyarrow.compute as pc

from src.data.mbp1_access import mbp1_files, scan_mbp1
from src.data.mbp1_ingest import META, json_write, write_metadata


def query_benchmarks():
    sessions = json.loads((META / "mbp1_data_quality.json").read_text())
    cases = [
        ("one_day_all_columns", "2024-09-03", "2024-09-03", None, False),
        ("one_month_selected_columns", "2024-09-01", "2024-09-30",
         ["ts_event", "instrument_id", "price", "bid_px_00", "ask_px_00"], False),
        ("one_day_selected_columns", "2024-09-03", "2024-09-03",
         ["ts_event", "price"], False),
        ("date_range_two_months", "2024-08-26", "2024-09-06",
         ["ts_event", "sequence", "action", "size"], False),
        ("records_per_day", "2024-09-01", "2024-09-30", ["session_date_ny"], True),
    ]
    process = psutil.Process()
    output = []
    for label, start, end, columns, aggregate in cases:
        for repeat in range(2):
            started = time.perf_counter()
            baseline = process.memory_info().rss
            peak = baseline
            rows, batches, counts = 0, 0, Counter()
            scan = scan_mbp1(start, end, columns)
            for batch in scan.to_batches():
                rows += len(batch)
                batches += 1
                if aggregate:
                    values = pc.value_counts(batch.column(0)).to_pylist()
                    counts.update({str(v["values"]): v["counts"] for v in values})
                peak = max(peak, process.memory_info().rss)
            expected_rows = sum(s["record_count"] for s in sessions if start <= s["session_date"] <= end)
            if rows != expected_rows:
                raise ValueError(f"Query/manifest count mismatch: {label}")
            result = {"query": label, "repeat": repeat + 1, "start": start, "end": end,
                      "columns": columns or "ALL", "rows": rows, "batches": batches,
                      "expected_rows": expected_rows,
                      "candidate_files": len(mbp1_files(start, end)),
                      "seconds": time.perf_counter() - started,
                      "baseline_rss_bytes": baseline, "observed_peak_rss_bytes": peak,
                      "observed_rss_increase_bytes": peak - baseline}
            output.append(result)
            if aggregate:
                json_write(META / "benchmark_records_per_day.json", dict(counts))
            print(result, flush=True)
    json_write(META / "query_benchmarks.json", output)
    write_metadata("query_benchmarks", output)
    return output


def generate_report(benchmarks: list[dict]):
    provenance = json.loads((META / "provenance.json").read_text())
    sessions = json.loads((META / "mbp1_data_quality.json").read_text())
    counters = Counter()
    affected = Counter()
    for session in sessions:
        counters.update(session["checks"])
        affected.update(k for k, v in session["checks"].items() if v)
        for name in session["warnings"]:
            if name.endswith("_activity_outlier"):
                counters[name] += 1
                affected[name] += 1
    anomaly_rows = [{"check": k, "observations": counters[k], "sessions": affected[k]}
                    for k in sorted(counters) if counters[k]]
    write_metadata("anomaly_summary", anomaly_rows)
    lines = ["# MBP-1 local foundation: executed audit", "",
             "Generated operational report. No features, models or strategies were evaluated.", "",
             "## Reconciliation and storage", "",
             f"- Source files: {provenance['raw_files']:,}",
             f"- Raw records: {provenance['raw_records']:,}",
             f"- Silver records: {provenance['silver_records']:,}",
             "- Exclusions, deduplication and count discrepancies: 0",
             "- All native fields reconstructed from Parquet match ordered DBN SHA-256 digests.",
             f"- Compressed Bronze bytes: {provenance['raw_bytes']:,}",
             f"- Parquet bytes: {provenance['parquet_bytes']:,}",
             f"- Monthly partitions: {provenance['months']}; Parquet parts: {provenance['parquet_files']}",
             f"- Ingestion invocation elapsed seconds (including reuse, if any): {provenance['seconds']:.3f}",
             f"- Process peak working set bytes: {provenance['peak_working_set_bytes']:,}",
             "- Chunk/row group: at most 250,000 rows; part rotation at approximately 5 million rows.",
             "- Serial CPU conversion; each month is fully read back before atomic publication.",
             "", "## Source coverage", "",
             "Requested weekdays: 2021-09-27 through 2026-09-26; final requested weekday 2026-09-25.",
             "Receive-time window is 07:00 inclusive to 12:00 exclusive America/New_York.",
             "UTC nanoseconds remain canonical. session_date_ny is the requested session date.",
             "The pricing notebook was inspected, never executed; all work used local files.",
             "", "Known vendor-degraded sessions: " + ", ".join(provenance["degraded_dates"]), "",
             "Decoded empty sessions: " + ", ".join(provenance["empty_dates"]), "",
             "These are observed empty sources, not inferred holiday labels.",
             "", "## Validation findings", "",
             "| Check | Observations | Sessions |", "|---|---:|---:|"]
    lines += [f"| {r['check']} | {r['observations']:,} | {r['sessions']:,} |" for r in anomaly_rows]
    lines += ["", f"Error sessions: {len(provenance['error_sessions'])}; "
              f"warning sessions: {provenance['warning_sessions']}.",
              "Full per-session categories, counts, first example event indices, action/side/flag "
              "counts and timestamps are in mbp1_data_quality.csv/parquet/json. "
              "Raw file hashes, metadata mappings and completed download markers are in raw_manifest.",
              "", "## Interpretation and limitations", "",
              "Duplicate rows are observations, not automatic errors. None were removed. "
              "Equal/forward-jumping venue sequences are expected in a schema/symbol-filtered feed. "
              "Negative event-time steps do not authorize reordering. The quality contract distinguishes "
              "completed-event crossed/locked books from intermediate states using F_LAST.",
              "", "The session-level low/high activity thresholds were frozen before processing; "
              "they are diagnostics and do not exclude any records.",
              "", f"Observed ID segments: {provenance['roll_segments']}. The contract_segments table "
              "contains daily starts and actual ID transitions. Only contract_change=True marks a "
              "transition; the first dataset row starts segment 1 without implying an observed roll. "
              "Numeric IDs are not globally unique physical contract identifiers. Local mappings "
              "do not provide expiration symbols; no external mapping was requested or invented.",
              "", "Databento's public issue record reports incorrect non-trade action/price/size "
              "on some historical GLBX.MDP3 channel-flush events. Source bytes are retained. "
              "This constrains future action-sensitive research even if transport integrity passes. "
              "Do not infer corrections from apparent repetition.",
              "", "References: [MBP-1 schema](https://databento.com/docs/schemas-and-data-formats/mbp-1), "
              "[conventions](https://databento.com/docs/standards-and-conventions), "
              "[vendor issue](https://issues.databento.com/b/6vrl98vl/feature-ideas/mbp-110-side-field-is-only-filled-in-for-trades).",
              "", "## Query benchmarks", "",
              "Read-through Arrow batches, not materialization of a month in pandas. "
              "Filesystem cache is not flushed; first measurements are not claimed to be cold-cache. "
              "RSS is sampled after each batch; peaks between samples can be missed.", "",
              "| Query | Run | Rows | Seconds | Observed peak RSS MiB |",
              "|---|---:|---:|---:|---:|"]
    lines += [f"| {r['query']} | {r['repeat']} | {r['rows']:,} | {r['seconds']:.3f} | "
              f"{r['observed_peak_rss_bytes'] / 2**20:.1f} |" for r in benchmarks]
    lines += ["", "## Access and readiness", "",
              "See project_docs/mbp1_data_dictionary.md for schema, API, ordering and quality policy. "
              "The top-level _SUCCESS.json publishes reconciled data; it does not certify that "
              "every source session is suitable for every research question. Known degraded sessions, "
              "empty sessions, warnings, source-action semantics and unresolved contract identity "
              "must be handled in a predeclared research contract. Final test remains guarded by default."]
    (META / "validation_report.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


if __name__ == "__main__":
    generate_report(query_benchmarks())
