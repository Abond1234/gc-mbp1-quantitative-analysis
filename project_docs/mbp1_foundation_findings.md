# GC MBP-1 foundation: executed findings

## Result and scope

The local Bronze-to-Silver foundation is complete. Representation integrity is
verified; unrestricted microstructure research is **not** approved merely because
conversion succeeded. Eligibility, clock handling, roll boundaries and vendor
action semantics must be addressed in the next research contract.

The explicit user request authorized full-period ingestion/quality diagnostics,
including 2025 onward. This is an infrastructure exception, not authorization for
descriptive feature selection or repeated final-test research. No historical API
client, download, model, signal, feature, label, backtest or Gold layer was created.
Raw DBN and completion-marker files were read without modification.

## Reconciled dataset

| Measure | Verified result |
|---|---:|
| Expected weekdays / source files | 1,305 / 1,305 |
| Nonempty / empty sources | 1,291 / 14 |
| Raw DBN records | 786,491,001 |
| Silver records | 786,491,001 |
| Excluded records / count discrepancies | 0 / 0 |
| Native trade records, action T | 52,302,576 |
| Compressed Bronze bytes, excluding markers | 17,232,496,150 |
| Canonical Parquet bytes | 30,390,798,465 |
| Monthly partitions / Parquet parts | 61 / 184 |
| Observed instrument IDs / ID segments | 26 / 26 |
| Observed ID transitions | 25 |
| Sessions with warnings | 61 |
| Sessions with error-level findings | 0 |

Requested dates are 2021-09-27 through 2026-09-26 inclusive, weekdays only.
Actual source-session coverage ends on Friday 2026-09-25. Receive timestamps
are inside the requested 07:00 inclusive to 12:00 exclusive America/New_York
window, with EST/EDT verified against every file's UTC metadata.

The real decoder exposes DBN v3, 80-byte records and 20 native fields. All are
retained, including the record header, sequence, instrument, action/side, prices,
quantities, BBO sizes/order counts, timestamps and flags. Silver has 29 columns:
native data plus raw timestamp copies and documented convenience/order/segment
fields. Prices remain int64 units of 1e-9; undefined sentinels are preserved.

Every source has a SHA-256 identity, a complete compressed frame, aligned DBN
records, matching request metadata and a matching COMPLETE marker body. There
are no missing/extra dates, duplicate source dates, unexpected files or orphan
markers. All 1,305 retained metadata headers match the original header bytes.

Each Parquet month was read back independently. Reconstructed native records
match the per-session **ordered byte hashes**, and indices/counts match exactly.
A second whole-store pass verified all timestamp views, session offsets, delivery
indices and contract-change/segment fields across 786,491,001 rows. An unchanged
rerun rehashed inputs/outputs, reused all 61 partitions, rebuilt none, and left
all 184 Parquet files' sizes, modification times and partition fingerprints
unchanged. Source replacement 2026-09-09 passed with **914,441 records**, its
valid marker, and no error/warning findings.

## Complete anomaly summary

Counts below overlap. Repeated native records were preserved, not deduplicated.
The persistent table has every session, individual counters, categories, action/
side/flag distributions, timestamp endpoints and first example indices.

| Observation | Count | Affected sessions |
|---|---:|---:|
| Completed-event crossed BBO, F_LAST | 2,798 | 16 |
| Completed-event locked BBO, F_LAST | 4 | 2 |
| Intermediate-event crossed BBO | 26 | 16 |
| Intermediate-event locked BBO | 1 | 1 |
| Internal receive gaps over 60 seconds | 756 | 27 |
| Leading coverage gap over 60 seconds | 7 | 7 |
| Trailing coverage gap over 60 seconds | 7 | 7 |
| Record-count activity outlier | 40 sessions | 40 |
| Trade-size-sum activity outlier | 42 sessions | 42 |
| Exact duplicate occurrences beyond the first | 143,066 | 1,277 |
| Adjacent exact duplicates, included above | 135,628 | 1,276 |
| Equal event timestamps | 51,353,239 | 1,291 |
| Equal receive timestamps | 7,054,375 | 1,291 |
| Equal venue sequences | 7,054,375 | 1,291 |
| Forward venue-sequence jumps greater than one | 619,570,550 | 1,291 |
| Negative ts_in_delta | 2,013,329 | 129 |
| Receive timestamp before event timestamp | 1,296,225 | 126 |
| Event timestamp outside requested window | 9 | 4 |
| Fill/no-action records | 17,600 | 1,068 |
| Undefined non-trade event price | 6,625 | 16 |
| Zero non-trade size | 6,625 | 16 |
| Absent BBO | 1 | 1 |

Activity limits were frozen at less than 0.1 or greater than 10 times the prior
20 nonempty-session median, requiring 10 predecessors. These are warning flags,
not exclusions; empty sessions also trigger low-activity flags where eligible.

There are **zero** receive/event timestamp reversals, sequence reversals,
receive-window violations, undefined timestamps, invalid trade prices/sizes,
nonpositive defined BBO prices, quote size/order-count inconsistencies, unknown
actions/sides, malformed layouts, mapping mismatches or intraday ID changes.
No duplicate audit has incomplete coverage. The observed flag values are only
0 and 128; absence of native bad-book/bad-time flags does not establish quality.

Interpretation and investigated examples:

- The 770 coverage intervals were independently located and count-reconciled in
  `coverage_gaps`. The largest leading gap is 2,787.794031537 seconds on
  2025-11-28. The largest internal gap is 1,293.777568211 seconds on 2022-11-30.
  Sparse nonempty sessions include 2022-11-30 (878 records) and 2023-05-31
  (621 records). The local stream alone cannot distinguish all inactivity from
  missing upstream events or independently verify active-contract ranking.
- Crossed completed-event books are not silently accepted as executable quotes.
  The largest affected dates are 2026-09-11 (1,306 records) and 2026-02-02
  (1,037). Bounded event context is saved in `warning_examples.json`.
- Clock diagnostics show receive-minus-event time as low as **-126,512,767 ns**,
  and ts_in_delta as low as **-126,599,627 ns**. No clocks were shifted or
  records reordered. These differences matter to timing and cross-feed causality.
- The nine event-window exceptions occur on 2023-01-13, 2025-12-05, 2026-05-21
  and 2026-09-16. Their receive timestamps are all inside the request. Databento
  filters this schema using receive time; the distinct event times are retained.
- Undefined event prices belong to 6,624 N records and one R record. The sole
  absent BBO is the opening R/clear record on 2025-11-28, with zero quote sizes
  and undefined prices. These are not invalid trade prices or invented quotes.
- Equal/jumping venue sequences are not GC packet-loss counters. MBP-1 is a
  symbol/schema-filtered feed; identical trade records can represent distinct
  delivered trades. No automatic deduplication is justified here.

## Known degraded and empty dates

Vendor-degraded overlay, retained even when local checks produce no warning:
**2024-09-18, 2025-09-17, 2025-09-24, 2025-11-28, 2026-03-16, 2026-04-10**.
Of these, 2025-11-28 also has local book/coverage warnings. Across both categories,
66 distinct sessions have a warning and/or known degraded-source flag.

Decoded empty sources, established from complete local files rather than a
holiday assumption:

**2021-12-24; 2022-04-15; 2022-12-26; 2023-01-02; 2023-04-07; 2023-12-25;
2024-01-01; 2024-03-29; 2024-12-25; 2025-01-01; 2025-04-18; 2025-12-25;
2026-01-01; 2026-04-03.**

## Contract behavior and research constraints

All 25 observed ID changes occur at session starts. There are no intraday
changes or reappearing IDs in this source. Daily mappings and segment endpoints
are retained in `raw_manifest`, `contract_segments` and `roll_segment_summary`.
Local MBP-1 metadata maps GC.v.0 to numeric IDs; physical expiration symbols are
unresolved. No symbols, adjusted prices, roll-spanning changes or interpolated
events were fabricated. The vendor defines `.v.0` using previous-day volume;
the selected contract need not have uniform activity within the requested window.
[Databento symbology](https://databento.com/docs/standards-and-conventions/symbology).

A separate vendor-reported historical GLBX.MDP3 channel-flush issue affects
some non-trade action/price/size values. This audit cannot identify and correct
every affected record from MBP-1 alone. Action-sensitive feature work requires
vendor clarification/resolution or an explicitly justified scope that avoids
those semantics. [Vendor issue record](https://issues.databento.com/b/6vrl98vl/feature-ideas/mbp-110-side-field-is-only-filled-in-for-trades).

The next research contract must freeze session/record eligibility and sensitivity
analysis, treatment of completed-event invalid books and sparse/gapped sessions,
clock/information-availability rules, and session/roll resets before feature
outcomes are inspected. Final-test access remains guarded by default. The
storage foundation is usable; these research decisions are not silently made by
the loader. **There is no blanket approval to start unrestricted feature research.**

## Operation, performance and evidence

See [the schema/query guide](mbp1_data_dictionary.md) for exact ingestion,
verification and query commands. The scanner uses monthly partition pruning,
column projection, date predicate pushdown and page-checksum verification.
Chunks/row groups are at most 250,000 records; parts rotate around five million
records. No whole month or full dataset is materialized in pandas.

Runtime timings and memory measurements are deliberately kept in ignored local
artifacts rather than this tracked summary:

- `data/metadata/mbp1/validation_report.md`: executed findings and query timings.
- `provenance_initial_build.json`: full-build time, memory, versions and identities.
- `query_benchmarks.csv/parquet/json`: day, month, projected columns, date range
  and records-per-day aggregation; every result reconciles to the manifest.
- `silver_invariant_audit.json`, `restart_audit.json`, `metadata_byte_audit.json`,
  `done_marker_audit.json`: independent integrity evidence.
- `mbp1_data_quality.csv/parquet/json`, `anomaly_summary`, `coverage_gaps`,
  `clock_diagnostics`, `warning_examples.json`: complete quality findings.
- `raw_manifest.csv/parquet`, `native_schema.json`, `roll_segment_summary`:
  source provenance, schema and observed contract metadata.

The validation notebook executed end to end with its pinned Project 1 kernel.
The full unittest suite passed 716 tests, with 9 skipped; eight focused MBP-1
tests passed again after the scanner checksum option was added. Repository-wide
ruff checks passed. Notebook execution emitted Windows event-loop/local-kernel
transport environment warnings but had no cell failures. A transient Windows
directory-publication error during development was addressed with explicit
reader closure and bounded atomic-rename retries; incomplete output was never
published as a completed store.
