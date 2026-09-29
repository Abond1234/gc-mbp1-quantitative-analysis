# GC MBP-1 QUANTITATIVE ANALYSIS

This work started with GC 1-minute OHLCV research in Project 1 and continues here
with event-level Databento GC MBP-1 data. This independent local project contains
immutable Bronze files, reconciled Silver partitions, ingestion, audits and
bounded research access. It contains no feature/Gold dataset, model or backtest.

## Verified foundation

| Item | Result |
|---|---|
| Source | GLBX.MDP3, mbp-1, GC.v.0 continuous |
| Requested coverage | Weekdays 2021-09-27 through 2026-09-26; last source day 2026-09-25 |
| Window | 07:00 inclusive to 12:00 exclusive America/New_York, receive time |
| Bronze | 1,305 DBN files and 1,305 markers; 17,232,496,150 compressed data bytes |
| Raw to Silver | 786,491,001 to 786,491,001 records; zero exclusions |
| Silver | 61 monthly partitions, 184 Parquet files, 30,390,798,465 bytes |
| Session quality | 14 empty, 61 with warnings, six vendor-degraded dates |
| Instruments | 26 observed ID segments; physical expiration symbols unresolved |

Read the [executed findings](project_docs/mbp1_foundation_findings.md),
[data dictionary](project_docs/mbp1_data_dictionary.md),
[ingestion contract](project_docs/mbp1_ingestion_contract.md) and
[migration report](project_docs/mbp1_migration_report.md).
Warnings include coverage gaps, crossed/locked BBO, event/receive clock differences
and undefined non-trade prices. Original findings also flag vendor non-trade
action semantics. Transport integrity does not make every observation research-eligible.
No trading edge has been established.

## Environment

The independent `.venv` uses Python 3.14.5, 64-bit, and the exact 140-distribution
Project 1 baseline. `requirements-project1-environment.txt` captures that complete
installed set; `requirements-mbp1.txt` is the concise runtime subset. The older
Project 1 lockfile differs from its actual installed environment and is preserved
as historical evidence under ignored migration metadata.

Use the existing environment:

```powershell
Set-Location 'C:\Users\abond\Desktop\WORK FILES\Systemic\GC MBP-1 QUANTITATIVE ANALYSIS'
& .\.venv\Scripts\Activate.ps1
& .\.venv\Scripts\python.exe -m pip check
```

To recreate a missing environment, use installed 64-bit Python 3.14.5:

```powershell
py -3.14 -m venv .venv
& .\.venv\Scripts\python.exe -m pip install -r requirements-project1-environment.txt
& .\.venv\Scripts\python.exe scripts\configure_environment.py
& .\.venv\Scripts\python.exe -m ipykernel install --user --name gc-mbp1-venv --display-name 'GC MBP-1 (.venv)'
```

The setup script adds this project's root to its own venv import path without
changing `PYTHONPATH` or relying on Project 1. Select **GC MBP-1 (.venv)** in Jupyter.
The raw downloader is not part of setup. No API key is needed.

## Read-only access

This command works with the new venv even when the working directory is elsewhere:

```powershell
& .\.venv\Scripts\python.exe -c "from src.data.mbp1_access import scan_mbp1; print(sum(b.num_rows for b in scan_mbp1('2024-09-03', '2024-09-03', ['event_idx_day', 'price']).to_batches()))"
```

Use the absolute path to `.venv\Scripts\python.exe` when outside the root.
Runtime data defaults resolve from the source package location. Process bounded
Arrow batches, preserve delivery order (`session_date_ny,event_idx_day`), and reset
at session/roll boundaries. Prices retain native integer precision; no stream
sorting, deduplication, interpolation or roll price adjustment is performed.

Development ends in 2023; Validation is 2024. The scanner rejects **2025 onward**
unless explicitly passed `allow_final_test=True`. Migration integrity checks are
not authorization to explore or tune on Final Test. Freeze a research contract
covering eligibility, clocks, book validity, source semantics and roll resets
before starting feature research.

## Verification and operation

```powershell
& .\.venv\Scripts\python.exe -m unittest discover -s tests
& .\.venv\Scripts\python.exe -m ruff check .
& .\.venv\Scripts\python.exe scripts\verify_mbp1_migration.py
& .\.venv\Scripts\python.exe scripts\audit_mbp1_source_headers.py
& .\.venv\Scripts\python.exe -m src.data.mbp1_ingest --reuse-only
```

`verify_mbp1_migration.py --hashes` additionally rehashes every raw file, marker and
Parquet part. The normal check reconciles all Parquet footer counts, queries a day,
a month and a roll boundary, tests pruning and the access guard, and reconstructs
native records for the first month. It writes a separate migration audit only.

`--reuse-only` re-inventories local Bronze and verifies every checkpoint/checksum,
refusing any rebuild. It refreshes run metadata while retaining partition files
and their original fingerprints. The reviewed path-only compatibility is explicit
in `configs/mbp1_migration_compatibility.json`; changes to the approved code,
configuration, dependencies or source identities invalidate that compatibility.
Do not regenerate this record to suppress a mismatch. The ordinary ingestion
command, without `--reuse-only`, may build/rebuild partitions and is intended only
when that work is deliberate.

Additional migrated entry points are `scripts/benchmark_mbp1.py`,
`scripts/audit_mbp1_quality_details.py`, `scripts/verify_mbp1_silver.py` and
`scripts/build_mbp1_notebook.py`. Full quality/derived-field audits include Final
Test infrastructure diagnostics; they are not research entry points. Original
outputs are preserved under `data/metadata/mbp1/migration/original_metadata/`.

The validation notebook reads existing audit metadata and queries a Validation
day; it performs no downloads or rebuilds. Its generator uses the independent kernel.

## Directory map

```text
src/data/             Ingestion, validation, access and migration compatibility
src/resources.py      Shared memory helper required by ingestion
scripts/              Audits, benchmarks, setup and notebook builder
tests/                MBP-1, migration integrity and memory-helper tests
notebooks/            Ingestion/validation notebook
project_docs/         Contracts, dictionary, findings and migration report
configs/              Reviewed checkpoint compatibility identity
data/raw/DB MBP-1 DATA/  Authoritative immutable Bronze
data/processed/mbp1/     Authoritative published Silver
data/metadata/mbp1/      Manifests, quality and build/migration provenance
logs/                 Original and migration verification logs
.venv/                Independent local environment
```

Top-level data, logs, environments, caches and secrets are Git-ignored; `src/data`
remains trackable. Unrelated OHLCV research stays in Project 1. Its redundant
`_staging`/`_superseded` output remains there, outside the published catalog;
see the migration report for its inventory and status.
