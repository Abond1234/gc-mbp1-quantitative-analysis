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
Current research membership is governed by the
[research split policy](project_docs/mbp1_research_split_policy.md).
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

## Frozen research splits

Approximately five years of source data are assigned chronologically, using
whole sessions and month-end boundaries. The policy was frozen on 2026-10-01;
it supersedes the original 2023/2024/2025 research cutoffs.

| Role | Inclusive New York session dates | Source sessions | Share |
|---|---|---:|---:|
| Development | 2021-09-27 to 2024-09-30 | 786 | 60.23% |
| Validation | 2024-10-01 to 2025-09-30 | 261 | 20.00% |
| Final Test | 2025-10-01 to 2026-09-25 | 258 | 19.77% |

This provides roughly three years for exploration/fitting, one year for candidate
validation and the latest year for final evaluation. Boundaries were chosen from
calendar coverage, not performance or event counts. Empty and flagged source
sessions retain their assignments. The splits are logical views of the original
store; no Bronze/Silver data or original build provenance was rewritten.

Start notebook work in
[02_mbp1_development.ipynb](notebooks/02_mbp1_development.ipynb). Complete its
research contract before analysis. Use chronological folds inside Development,
freeze the candidate before Validation, and freeze the complete procedure before
the authorized Final Test. The [full policy](project_docs/mbp1_research_split_policy.md)
covers horizon-dependent purging, prior audit exposure and the immutable split ledger.
The existing research-roadmap PDF has a dated addendum superseding its old split
dates and notebook references; the original proposal is preserved as historical context.

## Development-only access

This command works with the new venv even when the working directory is elsewhere:

```powershell
& .\.venv\Scripts\python.exe -c "from src.data.mbp1_access import scan_development; print(sum(b.num_rows for b in scan_development('2024-09-03', '2024-09-03', ['event_idx_day', 'price']).to_batches()))"
```

Use the absolute path to `.venv\Scripts\python.exe` when outside the root.
Runtime data defaults resolve from the source package location. Process bounded
Arrow batches, preserve delivery order (`session_date_ny,event_idx_day`), and reset
at session/roll boundaries. Prices retain native integer precision; no stream
sorting, deduplication, interpolation or roll price adjustment is performed.

`scan_development(columns=[...])` lazily selects all Development when dates are
omitted and exposes no holdout override. Both `scan_mbp1` and `mbp1_files` also
block Validation and Final Test by default. Explicit `allow_validation=True` and
`allow_final_test=True` are separate opt-ins for separately authorized procedures;
a cross-holdout request needs both. Out-of-coverage dates fail closed. Direct
filesystem access is not a security boundary; do not bypass these helpers for
research or use full-period audit metadata to select features.

## Verification and operation

```powershell
& .\.venv\Scripts\python.exe -m unittest discover -s tests
& .\.venv\Scripts\python.exe -m ruff check .
& .\.venv\Scripts\python.exe scripts\freeze_mbp1_research_splits.py
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

The historical ingestion/validation notebook reads full-period audit metadata and
queries a day now assigned to Development. It is locked by default and is only
for a separately authorized infrastructure task. Use the Development notebook for
research. Both generators use the independent kernel and save output-free notebooks.

## Directory map

```text
src/data/             Ingestion, validation, access and migration compatibility
src/resources.py      Shared memory helper required by ingestion
scripts/              Audits, benchmarks, setup and notebook builder
tests/                MBP-1, migration integrity and memory-helper tests
notebooks/            Development starter and locked historical audit notebook
project_docs/         Research split policy, contracts, dictionary and historical reports
configs/              Frozen research splits and reviewed checkpoint compatibility
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

GitNexus is required throughout this project for code discovery, symbol context,
pre-edit impact analysis and change-scope review. See [AGENTS.md](AGENTS.md).
