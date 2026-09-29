# GC MBP-1 migration completion report

Completed: 2026-09-29T05:57:24.757333+00:00

Destination: `C:\Users\abond\Desktop\WORK FILES\Systemic\GC MBP-1 QUANTITATIVE ANALYSIS`

Source: `C:\Users\abond\Desktop\WORK FILES\Systemic\Project 1`

## Outcome

The MBP-1 foundation is independently usable in the destination, with one
authoritative Bronze/Silver store and its own virtual environment. The verified
source copies were removed after transfer and operational checks. OHLCV research,
its environment, shared helpers and unrelated pre-existing changes were preserved.
Project 1's README now links to the destination. No feature dataset, signal,
model or backtest was created; no Databento API request was made.

## Transfer and integrity

| Measure | Verified result |
|---|---:|
| Files selected and hash-verified | 2,925 |
| Daily raw files / completion markers | 1,305 / 1,305 |
| Compressed raw data bytes | 17,232,496,150 |
| Marker bytes | 121,365 |
| Raw directory bytes, including markers | 17,232,617,515 |
| Raw / Silver records | 786,491,001 / 786,491,001 |
| Exclusions / count discrepancies | 0 / 0 |
| Published monthly partitions / Parquet parts | 61 / 184 |
| Canonical Parquet bytes | 30,390,798,465 |
| Silver transfer bytes, including checkpoints/catalog | 30,395,014,726 |
| Original metadata files / bytes | 40 / 14,187,690 |
| Original code, docs, tests, notebook and logs / bytes | 29 / 168,213 |

Every copied file matched its source SHA-256. All raw files, markers and canonical
Parquet files additionally matched recorded build checksums. Immediately before
removal, selected source files were rehashed, destination data was rehashed again,
and data modification times were checked. All 61 partition checkpoints and
fingerprints were preserved. Parquet footers independently total 786,491,001 rows.
Existing whole-store native/derived-field audits remain applicable to identical
bytes; this migration did not rerun the entire native conversion or whole-store
derived-field audit. A fresh native-byte and delivery-order reconstruction of
September 2021 reconciled 2,357,126 rows across four sessions.

`python -m src.data.mbp1_ingest --reuse-only` re-inventoried all Bronze, verified
all output hashes, reused 61 months and rebuilt zero. It ran from an unrelated
working directory. The refreshed root catalog records checkpoint reuse and the
actual runtime; the original root catalog, metadata and initial build provenance
are preserved separately. This is a migration, not a new ingestion.

## Assets and portability

Migrated: the three MBP-1 modules; all five original MBP-1 scripts; both original
MBP-1 test modules; the validation notebook; contract, dictionary and findings;
requirements; ten useful MBP-1 logs; all canonical Bronze/Silver; and all 40
metadata, quality, roll, benchmark and provenance artifacts.

The required shared `src/resources.py`, its tests and package initializers were
copied and retained in Project 1. `src/data/__init__.py` did not exist in the source;
it was added in the destination. No cited MBP-1 asset was missing.

Data defaults and audit source paths now resolve from the destination package
root. The notebook and generator use `gc-mbp1-venv`, whose interpreter points to
the destination `.venv`. A local venv `.pth` configured by the setup script enables
imports from any working directory without Project 1 or `PYTHONPATH`.

Reviewed path/configuration compatibility is explicit in
`configs/mbp1_migration_compatibility.json`. It permits original checkpoint reuse
only for the exact reviewed runtime configuration and recomputed original source/
roll-state fingerprint. Hashes are not disabled. Native conversion, reconstruction,
quality validation, the frozen ingestion contract and shared memory helper were
left unchanged. Changes to code, dependencies, source hashes, source timestamps
or incoming state invalidate compatibility. Regression tests check those failures,
same-size Parquet corruption and refusal to build a missing partition.

The original request notebook was inspected but never run. It contains pricing,
billable downloads and replacement cells, correcting the old dictionary's
description of it as only pricing. A sanitized, non-executable transcript is in
`project_docs/original_request_sanitized.md`; outputs, notebook metadata and
credential/client lines were omitted. The original stays in Project 1 as history.

## Environment and checks

Python: 3.14.5, 64-bit AMD64, matching the source exactly. All **140 installed
distributions match by name and version**, including pip 26.1.1. No editable,
local-wheel or direct-URL dependencies were found. No package was dropped or
substituted. Source declarations and the older lockfile are preserved as historical
metadata; the installed set was authoritative for parity.

Key versions: databento 0.82.0, databento-dbn 0.63.0, numpy 2.5.1, pandas 3.0.3,
pyarrow 25.0.0, scipy 1.18.0, zstandard 0.25.0, psutil 7.2.2, tzdata 2026.3,
ruff 0.15.22, jupyter 1.1.1, ipykernel 7.3.0. DuckDB and pytest were not installed
in the source and were not added. Unittest is the test runner.

| Check | Result |
|---|---|
| Destination `python -m pip check` | PASS, no broken requirements |
| `python -m unittest discover -s tests` | PASS, all 22 tests |
| `python -m ruff check .` | PASS |
| `python scripts/audit_mbp1_source_headers.py` | PASS, 1,305 headers and marker bodies |
| `python -m src.data.mbp1_ingest --reuse-only` | PASS, 61 reused, zero rebuilt |
| `python scripts/verify_mbp1_migration.py` | PASS, counts, pruning, roll, guard, native sample |
| Sample Validation day, 2024-09-03 | 660,149 rows, all columns read |
| September 2024 projected scan | 9,930,084 rows, only two monthly files |
| Day row-group pruning | 3 selected of 50 month row groups |
| First roll boundary, 2021-11-30 to 2021-12-01 | Counts, delivery starts, IDs and segment transition pass |
| Default 2025+ scanner request | Rejected with PermissionError |
| Notebook with new kernel, foreign working directory | Six code cells, zero errors |
| Migrated audit/benchmark entry-point imports | All resolve within destination |

The read-only README query was executed and returned 660,149 rows. Setup,
requirements installation, import setup and kernel registration commands were
performed with the new interpreter. `py -3.14 --version` confirms Python 3.14.5.
The activation command is optional; verification used explicit interpreter paths.
An early test attempt before pip finished failed on missing imports; the completed
environment then passed all tests. Notebook execution emitted Windows event-loop
and local-kernel TCP transport warnings, with no cell failure.

## Source leftovers and pre-existing work

The three redundant directories remain unchanged in Project 1:

| Location under old `data/processed/mbp1/` | Files including checkpoint | Bytes |
|---|---:|---:|
| `_staging/2021-11-3a49abf4a4ed40c08bd7c3a470b562c0` | 3 | 371,719,418 |
| `_superseded/2021-09-4c2f72371add4418b530e440cd9626f9` | 2 | 92,603,359 |
| `_superseded/2021-10-f3b9257b1b6c431687c24e977fe37c56` | 4 | 498,401,815 |
| Total | 9 | 962,724,592 |

These are six Parquet files plus three checkpoint files. The byte total matches
the brief; the file counts above explicitly include checkpoints. Each leftover
checkpoint differs from the corresponding published checkpoint. They were not
copied into the destination or deleted. The old root completion catalog was
removed, so these leftovers cannot be mistaken for a published source store.

Pre-existing `.gitignore` and README changes, `.codex_tmp`, output/outputs and
unrelated research reports were recorded. Only the MBP-1 README links/addendum were
changed in source documentation. GitNexus's index inconsistency was repaired by a
full local reindex; its generated guidance-count edits were restored. The source
uses branch `migration/extract-gc-mbp1`; its unrelated changes remain uncommitted.
The destination is a separate local Git repository on `migration/gc-mbp1`, with
no remote or publication. Data, generated metadata, logs and the venv are ignored;
`src/data` is trackable. Original source Git commit: `90320ae4750d0802e1fdbfc17da7ff0550a06ce9`.

## Research readiness and limits

**Infrastructure is ready for governed MBP-1 microstructure feature research.**
Research execution still requires a frozen contract defining session/record
eligibility, clock/information-availability rules, completed-book validity,
sensitivity analysis and session/roll resets. No unrestricted feature research
is authorized by migration success. Action-sensitive research needs resolution
of the source-action limitations or an explicitly justified restricted scope.

Coverage is requested weekdays from 2021-09-27 through 2026-09-26, with actual
last source day 2026-09-25, and receive times 07:00-12:00 America/New_York.
Verified quality counts remain 14 empty sessions, 61 warning sessions, zero
error sessions, six vendor-degraded dates and 26 observed instrument-ID segments.
The degraded dates are 2024-09-18, 2025-09-17, 2025-09-24, 2025-11-28,
2026-03-16 and 2026-04-10. The 2026-09-09 replacement has its valid marker and
914,441 records. Physical contract symbols remain unresolved. Gaps, crossed BBO,
clock differences and non-trade price sentinels remain intact and documented;
the data is not claimed anomaly-free. Final Test from 2025 remains guarded.

No migration or package-parity blocker remains. Detailed machine-readable evidence
is under `data/metadata/mbp1/migration/`: preflight, transfer inventory, original
metadata/artifacts/catalog, environment parity, validation, notebook execution,
restart verification, removal plan and migration provenance. Runtime logs are
under `logs/migration_*.log`.
