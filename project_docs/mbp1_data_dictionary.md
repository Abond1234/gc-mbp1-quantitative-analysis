# MBP-1 Silver store

This additive foundation is separate from the existing OHLCV research engines.
Current membership and access follow the
[research split policy](mbp1_research_split_policy.md): Development 2021-09-27 to
2024-09-30, Validation 2024-10-01 to 2025-09-30, Final Test 2025-10-01 to
2026-09-25, all inclusive New York session dates. The source of truth is
`configs/mbp1_research_splits.json`. The historical ingestion contract is unchanged.
Run using this project's independent virtual environment (see README.md for setup):

```powershell
.venv/Scripts/python.exe -m src.data.mbp1_ingest --reuse-only
.venv/Scripts/python.exe scripts/audit_mbp1_source_headers.py
.venv/Scripts/python.exe scripts/benchmark_mbp1.py
.venv/Scripts/python.exe scripts/audit_mbp1_quality_details.py
.venv/Scripts/python.exe scripts/verify_mbp1_silver.py
.venv/Scripts/python.exe -m unittest discover -s tests
.venv/Scripts/python.exe -m ruff check .
```

The original request notebook remains in Project 1. A sanitized, non-executable
transcript is preserved in [original_request_sanitized.md](original_request_sanitized.md).
It contains pricing and billable download/replacement cells and is never executed
by this pipeline. No API credentials are needed for local processing.
The existing pinned NumPy/PyArrow stack is reused. Runtime dependencies and code
hashes are recorded in `data/metadata/mbp1/provenance.json`; no core pins changed.
`requirements-mbp1.txt` pins the decoder, codec and timezone database used in
this run; install it with `python -m pip install -r requirements-mbp1.txt` when
reproducing in another environment. The existing numerical pins remain intact.

## Layout and publication

- Immutable Bronze: `data/raw/DB MBP-1 DATA/YYYY/*.dbn.zst` and `.done` files.
- Silver: `data/processed/mbp1/year=YYYY/month=MM/part-NNNN.parquet`.
- Audit: `data/metadata/mbp1/`, including CSV, Parquet and JSON metadata,
  `validation_report.md`, source schema, inventory errors, query benchmarks and provenance.
- Each month has `_SUCCESS.json`: ordered source identity, per-session native
  digests/diagnostics, part checksums/counts, roll carry state and reconciliation.
- The root `_SUCCESS.json` is the publication catalog. Readers fail closed until
  it exists, and ignore `_staging` and `_superseded`. Failed work remains local
  for inspection and is never scanned as completed data.

Each run rehashes sources and verifies compressed frame completion, DBN metadata
and record alignment. Completed months are reused only with identical source,
pipeline/configuration, dependencies and incoming roll state. Their Parquet
checksums are verified before reuse. Source mutation causes a rebuild; previous
partitions are retained under `_superseded`. A process lock prevents concurrent
writers and automatically releases on process exit. Interrupted months are
restarted, completed months are reusable. Do not edit a published store manually.

The single writer processes 250,000-record chunks. Zstandard level 3 Parquet has
statistics, dictionary encoding and page checksums. Row groups never exceed a
chunk and never cross a day. Parts rotate after reaching 5 million rows (at most
one chunk above the threshold). Monthly partitions contain several parts when
necessary. No full day, month or five-year concatenation is required.

## Discovered native schema

Local DBN v3 provides fixed 80-byte MBP-1 records with these 20 native fields.
Every field is preserved; there are no discarded padding fields in this layout.

| Field | Native type | Silver representation and meaning |
|---|---|---|
| length | uint8 | Native record length in four-byte units (20) |
| rtype | uint8 | Native record type (1 for MBP-1) |
| publisher_id | uint16 | Databento venue/dataset identifier |
| instrument_id | uint32 | Native instrument identifier, retained on every event |
| ts_event | uint64 | Original matching-engine event nanoseconds; UTC timestamp view plus exact `ts_event_raw` uint64 |
| price | int64 | Exact event price in 1e-9 price units |
| size | uint32 | Event quantity, interpreted with action |
| action | byte | Lossless one-character string; A/C/M/R/T/F/N |
| side | byte | Lossless one-character string; B bid/buy, A ask/sell, N unspecified |
| flags | uint8 | Native bit field, preserved without rewriting |
| depth | uint8 | Native book depth |
| ts_recv | uint64 | Original receive nanoseconds; UTC timestamp view plus exact `ts_recv_raw` uint64 |
| ts_in_delta | int32 | Native receive-minus-publisher-send interval in nanoseconds |
| sequence | uint32 | Venue sequence; not a consecutive GC event counter |
| bid_px_00, ask_px_00 | int64 each | Native BBO prices in 1e-9 units |
| bid_sz_00, ask_sz_00 | uint32 each | Native size at best price |
| bid_ct_00, ask_ct_00 | uint32 each | Native order count at best price |

Native price sentinel `9223372036854775807` remains an integer. Timestamp sentinel
`18446744073709551615` remains in `_raw`; only the added timestamp view becomes
null. Undefined order size is `4294967295`. Never subtract undefined prices.
No native price is converted to float. For GC, 0.1 price units corresponds to
100,000,000 native units. For any midpoint arithmetic retain a doubled midpoint
integer (bid plus ask) if half ticks must be represented exactly.
The GC minimum price increment is documented in the
[CME contract specification](https://www.cmegroup.com/market-regulation/files/gold-futures-and-options-fact-card.pdf).

`flags & 128` is F_LAST, `& 64` F_TOB, `& 32` F_SNAPSHOT, `& 16` F_MBP,
`& 8` F_BAD_TS_RECV, `& 4` F_MAYBE_BAD_BOOK, `& 2` publisher-specific.
Use bit masks, not equality. Book state on a partial event is not automatically
an executable completed book. No book reconstruction or action correction occurs.

Semantic references: [native schema](https://databento.com/docs/schemas-and-data-formats/mbp-1)
and [Databento conventions](https://databento.com/docs/standards-and-conventions).
The installed decoder's actual dtype and metadata are captured in `native_schema.json`
and `raw_manifest`; encoded metadata is also retained as base64 for exact recovery.

## Additive research fields

| Field | Definition |
|---|---|
| session_date_ny | Requested New York session date, derived from validated filename/window; includes events whose event time precedes the boundary |
| event_idx_day | uint64, zero-based original delivered position within that source session |
| ts_event_ny | Nanosecond timestamp view with America/New_York timezone |
| ns_from_0700 | Exact signed event-time nanoseconds relative to that session's 07:00 NY; negative boundary values retained |
| minute_from_0700 | Floor of ns_from_0700 / 60,000,000,000 |
| contract_change | True only when observed instrument ID differs from the preceding delivered record, including across nonempty days/months |
| roll_segment | Monotone observed-ID segment number starting at 1; not a physical-contract expiration identifier |

`seconds_from_0700` can be derived with integer quotient/remainder from the stored
nanoseconds; there is no floating-point canonical elapsed-time field. DST uses
IANA America/New_York rules. No timestamps are sorted or repaired. Empty sessions
produce metadata rows but no invented events or contract IDs.

Local metadata resolves GC.v.0 to daily numeric IDs only. It does not provide
physical GC expiration symbols. An unchanged ID cannot establish global physical
contract identity, and an ID transition is not an adjusted-price splice. Use
`contract_change`/`roll_segment` as mandatory boundaries; do not calculate across
them. `contract_segments` contains every nonempty day start and each intraday ID
transition; filter `contract_change` for observed changes.
The vendor defines `.v.0` as the highest-ranked expiration by the previous day's
volume. This does not guarantee that it is the most active contract in every
07:00-12:00 sample. Prices are unadjusted. See
[continuous symbology](https://databento.com/docs/standards-and-conventions/symbology).

## Querying

```python
from src.data.mbp1_access import scan_development

scan = scan_development(
    start="2024-09-01", end="2024-09-30",  # inclusive dates
    columns=["session_date_ny", "event_idx_day", "ts_event", "ts_recv",
             "instrument_id", "roll_segment", "contract_change", "sequence",
             "action", "side", "price", "size", "bid_px_00", "ask_px_00",
             "bid_sz_00", "ask_sz_00"],
)
for batch in scan.to_batches():
    # Process one bounded Arrow RecordBatch at a time.
    print(batch.num_rows)

# Materialize only a deliberately small selection:
day = scan_development("2024-09-03", "2024-09-03", ["ts_event", "price"]).to_table()
pandas_day = day.to_pandas()
```

The helper resolves only intersecting monthly partitions, selects columns and
pushes session-date filtering to Arrow/Parquet row-group statistics. Reads verify
Parquet page checksums and use bounded read-ahead without whole-fragment
pre-buffering. It does not
open Bronze. `root=` supports another copied Silver location. Canonical order is
`session_date_ny,event_idx_day`; SQL and parallel consumers must request that order
explicitly before sequential processing. Timestamp ordering is not a substitute.

Optional libraries are not required or installed by this pipeline:

```python
# Polars: consume guarded Development batches (if installed).
import polars as pl
for batch in scan_development("2024-09-01", "2024-09-30",
                              ["session_date_ny", "event_idx_day", "price"]).to_batches():
    frame = pl.from_arrow(batch)

# DuckDB streaming Arrow input (if installed):
import duckdb
reader = scan_development("2024-09-01", "2024-09-30", ["session_date_ny"]).to_reader()
connection = duckdb.connect()
connection.register("events", reader)
counts = connection.sql("SELECT session_date_ny, count(*) FROM events GROUP BY 1 ORDER BY 1")
print(counts.fetchall())
```

`scan_development` has no holdout override; omitted dates mean the whole frozen
Development range, lazily. The general `scan_mbp1` and monthly file resolver
`mbp1_files` reject either holdout unless its own `allow_validation=True` or
`allow_final_test=True` is explicitly supplied under a separately authorized
procedure. Ranges intersecting both need both flags. Noncanonical/reversed dates
and dates outside frozen coverage fail; future data cannot silently join a split.
`mbp1_files` returns whole months, without row filtering; use the scanner for
research. Direct file access is not a security boundary. Supplied benchmark
examples are now Development (historically Validation); the historical full-period
quality reports remain infrastructure evidence, not research entry points.

The ignored split ledger under `data/metadata/mbp1/research_splits/` records
session assignments, counts and policy/source metadata identities. It does not
add fields to or rewrite the canonical Silver schema. Reproduce/verify it with
`python scripts/freeze_mbp1_research_splits.py`. The Development starter is
`notebooks/02_mbp1_development.ipynb`; the historical audit notebook is locked by default.

## Quality policy and readiness

`mbp1_data_quality` has every requested session and separate errors, warnings,
unusual observations and known-source-degradation flags. Consult the frozen
`mbp1_ingestion_contract.md` for the rules. No session or record is automatically
removed by the scanner. A future research contract must specify its exclusion
and sensitivity policy before reading outcomes.

Exact duplicates are retained, including identical trades. Receive timestamps
must be monotone for the bounded duplicate detector to cover all nonadjacent
duplicates; exceptions are explicitly listed as incomplete duplicate audits.
Forward sequence gaps alone cannot establish loss in this filtered dataset.
No receive-time interval outside the requested window is silently accepted.
Event times can precede receive-time boundaries and are reported separately.

There is a vendor-reported historical GLBX.MDP3 channel-flush issue affecting
some non-trade action/price/size values. Its affected rows cannot reliably be
identified or repaired using this schema alone. Preserve source semantics and
obtain a vendor resolution before authorizing research dependent on exact
non-trade action attribution. [Vendor issue record](https://issues.databento.com/b/6vrl98vl/feature-ideas/mbp-110-side-field-is-only-filled-in-for-trades).

The generated validation report provides actual counts, anomalies and operational
benchmarks. A published, byte-reconciled store is necessary but insufficient for
unrestricted microstructure research. Known degraded sessions, warnings, gaps,
empty sources and unresolved physical-contract identities remain explicit
research constraints. This task creates no feature/Gold dataset.

## Migration operation

Runtime defaults resolve from the new project root, independently of the working directory.
The full environment snapshot and migration report supersede source-project setup instructions.
Use `--reuse-only` to verify all checkpoints and refuse a rebuild. Explicit compatibility
for the reviewed path changes is recorded in `configs/mbp1_migration_compatibility.json`;
original hashes and checkpoints are preserved, while each rerun records its actual runtime.
