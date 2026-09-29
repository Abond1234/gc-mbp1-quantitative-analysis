"""Generate the MBP-1 audit notebook; heavy processing stays in reusable modules."""

from pathlib import Path

import nbformat as nbf

notebook = nbf.v4.new_notebook()
notebook.metadata["kernelspec"] = {
    "display_name": "GC MBP-1 (.venv)", "language": "python", "name": "gc-mbp1-venv"
}
notebook.cells = [
    nbf.v4.new_markdown_cell(
        "# MBP-1 ingestion and validation\n\n"
        "Infrastructure audit only: no features, labels, models or strategies. "
        "Read `project_docs/mbp1_ingestion_contract.md` before running. "
        "The full-period audit was expressly authorized; this does not authorize "
        "future descriptive or research access to the final-test partition.\n\n"
        "Run heavy processing from the repository root:\n"
        "```powershell\n.venv/Scripts/python.exe -m src.data.mbp1_ingest --reuse-only\n"
        ".venv/Scripts/python.exe scripts/benchmark_mbp1.py\n```\n"
        "Rerunning ingestion verifies identities and reuses completed monthly partitions. "
        "This notebook reads the resulting audit tables and a small Validation-day example."
    ),
    nbf.v4.new_code_cell(
        "import hashlib\nimport json\nimport sys\nfrom pathlib import Path\n"
        "from importlib.metadata import version\n"
        "from src.data.mbp1_paths import ROOT\n"
        "import pyarrow.parquet as pq\n"
        "from src.data.mbp1_access import scan_mbp1\n"
        "META = ROOT / 'data/metadata/mbp1'\n"
        "SILVER = ROOT / 'data/processed/mbp1'\n"
        "provenance = json.loads((META / 'provenance.json').read_text())\n"
        "for package, expected_version in provenance['config']['libraries'].items():\n"
        "    assert version(package) == expected_version, f'Environment mismatch: {package}'\n"
        "assert (SILVER / '_SUCCESS.json').is_file()\n"
        "assert provenance['raw_records'] == provenance['silver_records']\n"
        "assert provenance['excluded_records'] == 0\n"
        "derived_audit = json.loads((META / 'silver_invariant_audit.json').read_text())\n"
        "assert derived_audit['status'] == 'PASS'\n"
        "assert derived_audit['records_checked'] == provenance['silver_records']\n"
        "assert derived_audit['dataset_fingerprint'] == hashlib.sha256(\n"
        "    json.dumps(provenance['partitions'], sort_keys=True).encode()).hexdigest()\n"
        "{k: provenance[k] for k in ['raw_files', 'raw_records', 'silver_records', "
        "'months', 'parquet_files', 'parquet_bytes', 'native_reconciliation']}"
    ),
    nbf.v4.new_markdown_cell(
        "## Native schema and provenance\n\n"
        "Prices remain exact int64 fixed-point values. UTC nanosecond timestamps, "
        "raw uint64 values and delivery indices are preserved. The schema/data dictionary "
        "documents the added timezone and observed-ID segment fields."
    ),
    nbf.v4.new_code_cell(
        "schema = json.loads((META / 'native_schema.json').read_text())\n"
        "print(json.dumps(schema, indent=2))\n"
        "print(json.dumps(provenance['config'], indent=2))"
    ),
    nbf.v4.new_markdown_cell(
        "## Quality findings\n\n"
        "No record is silently removed. The quality table separates source degradation, "
        "errors, warnings and unusual but potentially valid events. "
        "Consult the full per-session table and frozen thresholds before declaring eligibility."
    ),
    nbf.v4.new_code_cell(
        "quality = pq.read_table(META / 'mbp1_data_quality.parquet').to_pandas()\n"
        "assert len(quality) == provenance['raw_files']\n"
        "print('Known degraded:', provenance['degraded_dates'])\n"
        "print('Decoded empty:', provenance['empty_dates'])\n"
        "print('Error sessions:', provenance['error_sessions'])\n"
        "print('Warning sessions:', provenance['warning_sessions'])\n"
        "print('Incomplete duplicate coverage:', provenance['incomplete_duplicate_audit_sessions'])\n"
        "pq.read_table(META / 'anomaly_summary.parquet').to_pandas()"
    ),
    nbf.v4.new_code_cell(
        "segments = pq.read_table(META / 'contract_segments.parquet').to_pandas()\n"
        "print('Observed ID segments:', provenance['roll_segments'])\n"
        "segments.loc[segments.contract_change, "
        "['session_date', 'event_idx_day', 'instrument_id', 'roll_segment']]"
    ),
    nbf.v4.new_markdown_cell(
        "## Research access\n\n"
        "The scanner prunes monthly partitions, projects columns and filters Parquet "
        "row groups. Iterate batches for large selections. Explicit order keys are "
        "session_date_ny and event_idx_day, not timestamp sorting. Physical GC expiration "
        "symbols were not recoverable from local metadata; numeric IDs are not fabricated symbols."
    ),
    nbf.v4.new_code_cell(
        "scan = scan_mbp1('2024-09-03', '2024-09-03', root=SILVER,\n"
        "    columns=['session_date_ny', 'event_idx_day', 'ts_event', 'ts_recv',\n"
        "             'instrument_id', 'roll_segment', 'price', 'bid_px_00', 'ask_px_00'])\n"
        "rows = sum(batch.num_rows for batch in scan.to_batches())\n"
        "expected = int(quality.loc[quality.session_date == '2024-09-03', 'record_count'].iloc[0])\n"
        "assert rows == expected\n"
        "print('Example day reconciled records:', rows)"
    ),
    nbf.v4.new_code_cell(
        "benchmarks = pq.read_table(META / 'query_benchmarks.parquet').to_pandas()\n"
        "benchmarks[['query', 'repeat', 'rows', 'seconds', 'observed_peak_rss_bytes']]"
    ),
    nbf.v4.new_markdown_cell(
        "## Readiness assessment\n\n"
        "Successful byte/count reconciliation certifies transport and representation integrity. "
        "It does not erase source quality limitations. Known degraded sessions, flagged gaps/book "
        "states, empty sessions, unresolved physical contract symbols and the vendor-reported "
        "historical non-trade channel-flush action issue require an explicit eligibility and "
        "sensitivity policy before feature research. No unrestricted research approval is implied.\n\n"
        "See `data/metadata/mbp1/validation_report.md` and "
        "`project_docs/mbp1_foundation_findings.md` for the completed assessment."
    ),
]
destination = Path(__file__).resolve().parents[1] / "notebooks/01_mbp1_ingestion_and_validation.ipynb"
nbf.write(notebook, destination)
print(destination)
