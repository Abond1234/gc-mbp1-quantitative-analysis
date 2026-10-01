# GC MBP-1 research split policy

Frozen on 2026-10-01 at the user's request, before feature research in this
repository. Policy ID: `gc-mbp1-chronological-v1`. The executable source of truth
is [the versioned configuration](../configs/mbp1_research_splits.json).
This supersedes the previous research assignment (Development through 2023,
Validation in 2024, Final Test from 2025). It does not revise the historical
ingestion contract, audit findings or checkpoint identities.

## Assignment and duration

Available source sessions span **2021-09-27 through 2026-09-25**: approximately
five years, 1,305 requested weekdays, 1,291 nonempty sources and 14 empty sources.
The original request ended on Saturday 2026-09-26; no source session exists for
that Saturday. The receive-time window remains 07:00 inclusive to 12:00 exclusive
America/New_York.

| Role | Inclusive New York source-session dates | Source sessions | Share of sessions | Records | Empty sources |
|---|---|---:|---:|---:|---:|
| Development | 2021-09-27 to 2024-09-30 | 786 | 60.23% | 465,090,823 | 8 |
| Validation | 2024-10-01 to 2025-09-30 | 261 | 20.00% | 126,136,708 | 3 |
| Final Test | 2025-10-01 to 2026-09-25 | 258 | 19.77% | 195,263,470 | 3 |
| Total | 2021-09-27 to 2026-09-25 | 1,305 | 100.00% | 786,491,001 | 14 |

The design targets **60/20/20 elapsed time**: roughly three years for discovery
and fitting, a year for candidate validation, and the latest year for the final
evaluation. Full month boundaries match Silver storage and keep any month from
straddling two roles. These proportions are a project design choice, not a
universal quant standard. They were selected from duration and calendar
boundaries, before aggregating record counts; event activity, returns, volatility,
quality outcomes and performance were not used to choose the cutoffs. The table's
percentages use source sessions, not event rows or statistically independent samples.

Assignment uses `session_date_ny`, the requested receive-window session date.
Every delivered event in a session has that session's role, even when `ts_event`
falls just outside its receive window. Empty, sparse and flagged sources stay
assigned. There are no shuffled rows, overlapping memberships or unassigned
source dates. Calendar dates between the bounds are contiguous; weekends do not
create observations. Split membership is separate from research eligibility.

## Access and notebooks

Start with [02_mbp1_development.ipynb](../notebooks/02_mbp1_development.ipynb),
using the **GC MBP-1 (.venv)** kernel. Its saved outputs are empty; the supplied
cells perform only a Development-day access/order check. Complete its research
contract before adding exploratory analysis.

```python
from src.data.mbp1_access import scan_development

# Small selection; dates are inclusive.
scan = scan_development("2024-09-03", "2024-09-03",
                        columns=["session_date_ny", "event_idx_day", "price"])
for batch in scan.to_batches():
    pass  # Apply the predeclared Development procedure here.

# Omitted dates select all Development lazily; never materialize it all in pandas.
scan = scan_development(columns=["session_date_ny", "event_idx_day", "price"])
```

`scan_development` exposes no holdout override. `scan_mbp1` and `mbp1_files` also
default to Development only. A request intersecting Validation needs
`allow_validation=True`; a request intersecting Final Test needs
`allow_final_test=True`. A request intersecting both needs both flags. Flags must
be booleans and are only technical opt-ins, not research approval. Guards run
before store/file access, including for copied roots. Reversed/noncanonical dates
and ranges outside frozen coverage fail. Future data does not silently extend a
split. Restart the notebook kernel after an explicitly reviewed policy revision.

These are application governance guards, not filesystem permissions. Raw files,
direct Parquet readers, Arrow fragment APIs and existing full-period metadata
remain accessible to the file owner. Use the guarded scanner for research;
`mbp1_files` returns whole monthly files and is intended for infrastructure tools.
Arrow readers retain bounded batches, column/date pruning, checksum verification
and deterministic single-threaded scanning. Native precision/order are unchanged.

The historical [ingestion notebook](../notebooks/01_mbp1_ingestion_and_validation.ipynb)
displays full-period infrastructure diagnostics. Its first code cell blocks
execution by default; enable it only for an explicitly authorized infrastructure
task. It is not an exploration template. Existing full-period quality reports and
audit scripts carry the same restriction; resegmentation does not renew audit
authorization or permit holdout feature selection.

## Research and evaluation protocol

1. Use Development for notebook exploration, feature design and fitting. Freeze
   each question, record/session eligibility, clock rules, lookbacks, labels,
   holding periods, costs, thresholds and success/failure criteria before its
   analysis. Log attempts and negative findings.
2. Use expanding or rolling chronological folds **within Development** for
   tuning and robustness checks. Fit preprocessing, normalization, imputation
   and feature selection on each training fold alone. Never shuffle event rows
   across folds; group all events from a session together.
3. Freeze the candidate and validation plan before accessing Validation. Use it
   for the predeclared selection/robustness decision and log every revision. If
   repeated feedback drives new ideas, Validation has become selection data;
   do not describe it as an untouched test. Any refit on Development plus
   Validation must be specified before Final Test is opened.
4. Freeze the complete procedure and obtain explicit Final Test authorization.
   Evaluate once against the predeclared criteria, report failures, and do not
   tune on the result or move the cutoff. A revised strategy needs genuinely
   unseen future data for another final claim.

Reset state at session, observed roll and split boundaries. Do not calculate
cross-roll price changes. Features, labels, execution/holding intervals and any
warm-up observations must stay within their permitted session/roll/split. Purge
samples whose information or outcome intervals cross a fold boundary; freeze any
additional embargo from the declared maximum dependence/holding horizon before
evaluation. No universal numeric gap is asserted here: no feature or label
horizon has been defined. The scanner enforces date access only; it does not
construct labels, folds or purges. A future multi-session design must explicitly
revise the session-reset protocol and specify interval-aware purging.

The chronology and training-only transformations follow the principles in the
[scikit-learn time-series validation documentation](https://scikit-learn.org/stable/modules/generated/sklearn.model_selection.TimeSeriesSplit.html)
and [data leakage guidance](https://scikit-learn.org/stable/common_pitfalls.html#data-leakage).
Do not apply an equal-row splitter directly to irregular MBP-1 events; construct
session/time-based folds appropriate to the frozen research question.

## Provenance and limitations

Run `python scripts/freeze_mbp1_research_splits.py` to create or verify the ignored
ledger `data/metadata/mbp1/research_splits/gc-mbp1-chronological-v1/assignments.json`.
It assigns every source weekday exactly once, reconciles per-session inventory
counts to the published monthly checkpoints, and records the policy, inventory
and catalog SHA-256 identities. Identical reruns leave the ledger untouched;
changed inputs fail instead of refreshing the record automatically. It uses
existing metadata only and does not claim a fresh native-byte audit.

This is a logical research partition over the existing immutable store. Bronze,
Silver, native timestamps/prices, original delivery indices, contract segments,
historical metadata, original build provenance and
`configs/mbp1_migration_compatibility.json` remain unchanged. The hash-bound
[ingestion contract](mbp1_ingestion_contract.md) remains historical version 1;
this document governs current research membership and access. No data was moved,
duplicated, rewritten, deleted or excluded to create the splits.

Earlier foundation/migration work inspected full-period integrity and quality
diagnostics and benchmarked September 2024 (then called Validation, now
Development). Those exposures remain documented; the new Final Test is not
claimed to be wholly unseen at an infrastructure level. The repository records
no prior feature/model/backtest research. Any research exposure outside these
records must be disclosed before claiming an untouched final evaluation.
Resegmentation cannot undo prior research exposure. The old 2025-01-01 through
2025-09-30 Final Test designation is explicitly retired and becomes Validation.

Source-action limitations, crossed books, gaps, clock discrepancies, empty
sessions and unresolved physical contracts remain as recorded in the findings.
No eligibility filter, feature, model, backtest or trading edge is established
by this change. New datasets and boundary changes require a versioned decision;
never recompute the cutoffs automatically as data grows.

## Implementation verification, 2026-10-01

- `python -m unittest discover -s tests`: all 28 tests passed, including synthetic
  split endpoints, crossing ranges, independent opt-ins, malformed dates, future
  coverage rejection, native precision/order and complete weekday assignment.
- `python -m ruff check .` and `git diff --check`: passed.
- The frozen ledger reconciled all 1,305 sessions and 786,491,001 records to
  existing Silver checkpoints; an identical rerun preserved its modification time.
- The new notebook executed both code cells using `gc-mbp1-venv` from a foreign
  working directory. Its Development sample reconciled 660,149 rows. Saved
  notebooks remain output-free. The historical notebook's default lock passed.
- No Validation or Final Test event rows were read during this change. Boundary
  access was exercised on synthetic data and checked against real default guards.
- Ingestion code/configuration/dependency identity still matches the exact
  reviewed migration compatibility record; it was not refreshed.
- GitNexus impact analysis preceded existing-symbol edits, the index was rebuilt,
  and `detect_changes` mapped the expected access/audit flows. Its high scope risk
  reflects the shared access gate; new policy/ledger/test symbols were also indexed.

Local machine-readable verification is in the same ignored policy directory as
the ledger, as `verification.json`. Notebook execution emitted the existing
Windows event-loop/local-kernel transport warnings, with no cell failures.

The existing `output/pdf/GC_MBP1_Research_Roadmap.pdf` now starts with a dated
policy addendum. Every original page is marked as historical, with its split
dates and notebook references superseded by the addendum. Original proposal
pagination, research suggestions and reference links are preserved. A verified
byte-identical archival copy and `roadmap_update.json` are retained in the ignored
policy directory. The addendum and amended layout were rendered and inspected.
