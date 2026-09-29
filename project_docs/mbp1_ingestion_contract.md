# MBP-1 foundation contract, version 1

Scope frozen before full decoding: local GLBX.MDP3, mbp-1, GC.v.0 continuous,
2021-09-27 through 2026-09-26 inclusive, weekdays, 07:00 inclusive to 12:00
exclusive America/New_York. The user's explicit full-period infrastructure audit
authorizes integrity diagnostics on 2025 onward for this task only. It does not
authorize fitting, feature selection, strategy research, or reuse of final-test
diagnostics to choose hypotheses. No historical API clients or download calls.

Bronze files are read-only. Preserve every native field, undefined sentinel,
duplicate and delivered row. No sorting, interpolation, deduplication, resampling,
price adjustment or forward fill. CPU only. Prices remain signed integer units
of 1e-9. Native uint64 timestamp values are retained alongside UTC timestamp
views; undefined timestamps become null only in the additional views.

Before conversion inventory all files, markers, SHA-256 identities, compressed
frame completeness, DBN metadata and structural record counts. Unexpected files,
missing weekdays, duplicate dates, metadata mismatches, incomplete frames or
records are errors; never silently omit them. A completed marker does not prove
content correctness. Actual decoded counts must match structural input counts.

Quality rules fixed in advance:

- ERROR: unreadable/truncated source, missing marker/file, invalid record layout,
  unexpected schema/request metadata, impossible record types, undefined times,
  receive timestamps outside request, nonpositive defined trade prices or zero
  trade sizes, failed field-level round trip/count reconciliation.
- WARNING: decreasing receive timestamps or sequences within an instrument,
  internal receive gaps over 60 seconds, leading/trailing coverage gaps over 60
  seconds, bad-receive/book flags, locked/crossed completed-event BBO,
  inconsistent present/absent quote sizes/counts, nonpositive defined BBO prices,
  unexpected intraday instrument changes or metadata mapping disagreement.
- VALID BUT UNUSUAL: empty source sessions (not automatically explained as a
  holiday), exact duplicate records, equal sequences, forward sequence gaps,
  event-time reversals, partial-event locked/crossed books, absent quotes,
  negative/clamped ts_in_delta, fill/no-action records, event time outside the
  request when receive time is inside. These observations are retained, and
  incomplete-event BBO is not assumed executable.
- KNOWN DEGRADED SOURCE DATA: 2024-09-18, 2025-09-17, 2025-09-24,
  2025-11-28, 2026-03-16, 2026-04-10, regardless of local diagnostics.
- Activity diagnostics: record count and action=T size sum below 0.1 times or
  above 10 times the median of the previous 20 nonempty sessions, requiring at
  least 10 predecessors. Warning only; never an exclusion or fitted threshold.

Exact duplicate checks compare all native record bytes within chunks and across
chunk boundaries with equal receive timestamps. Completeness of that method
requires nondecreasing receive timestamps; any violation explicitly invalidates
that duplicate-coverage claim and requires a supplementary external audit.
Sequence jumps are not evidence of lost GC events: venue sequences include
messages for other instruments and MBP-1 is a schema-filtered stream.

Instrument transitions define observed ID segments, including transitions across
session/month boundaries; IDs alone do not prove physical contract identity.
Empty dates carry no fabricated instrument or observations. No cross-roll price
calculation is allowed. Daily metadata mappings are retained verbatim.

Bounded chunks of 250,000 records, one deterministic writer, Zstandard Parquet,
monthly Hive partitions and parts capped at 5,000,000 rows. Each month is an
atomic checkpoint, with per-file checksums and counts. Readers use only published
completion manifests. Resume requires identical source, code/configuration and
dependency identities. Reconstruct native DBN records from Parquet and compare
ordered SHA-256 digests and per-session counts independently of writer counters.

Readiness requires integrity reconciliation and an explicit assessment of
warnings and vendor limitations, not a script-success flag. All data and runtime
reports live beneath ignored data/. No Gold layer is in scope.
