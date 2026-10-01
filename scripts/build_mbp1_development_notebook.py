"""Generate an output-free Development-only notebook with no holdout override."""

from pathlib import Path

import nbformat as nbf

notebook = nbf.v4.new_notebook()
notebook.metadata["kernelspec"] = {
    "display_name": "GC MBP-1 (.venv)", "language": "python", "name": "gc-mbp1-venv"
}
notebook.cells = [
    nbf.v4.new_markdown_cell(
        "# GC MBP-1: Development workspace\n\n"
        "Start here for notebook work. Read `project_docs/mbp1_research_split_policy.md`. "
        "Only Development is available through `scan_development`; it has no holdout override. "
        "The cells below verify access and delivery ordering on one Development day. "
        "They create no features, labels or models.\n\n"
        "Before exploration, write down the research question, eligibility rules, information "
        "clock, feature/label horizons and evaluation criteria. Treat the historical "
        "ingestion notebook and full-period quality reports as infrastructure evidence; "
        "do not use their holdout diagnostics to choose features."
    ),
    nbf.v4.new_code_cell(
        "import sys\nfrom src.data.mbp1_access import scan_development\n"
        "from src.data.mbp1_splits import POLICY_ID, POLICY_SHA256, SPLITS\n\n"
        "development = SPLITS['development']\n"
        "print('Interpreter:', sys.executable)\n"
        "print('Policy:', POLICY_ID, POLICY_SHA256)\n"
        "print('Development:', development.start, 'through', development.end)"
    ),
    nbf.v4.new_markdown_cell(
        "## Research contract (complete before analysis)\n\n"
        "- Question and falsifiable hypothesis: pending.\n"
        "- Session/record eligibility and sensitivity checks: pending.\n"
        "- Information clock, quote validity and source-action scope: pending.\n"
        "- Lookback, prediction horizon, holding period and boundary purging: pending.\n"
        "- Development walk-forward folds, costs and success/failure criteria: pending.\n\n"
        "Fit transformations only on each training fold. Reset at sessions, observed rolls "
        "and split boundaries; discard any feature/label interval crossing them. Validation "
        "requires a frozen candidate; Final Test requires the final frozen procedure."
    ),
    nbf.v4.new_code_cell(
        "# Infrastructure smoke check only: one Development session, bounded batches.\n"
        "day = '2024-09-03'\n"
        "scanner = scan_development(day, day,\n"
        "    columns=['session_date_ny', 'event_idx_day', 'instrument_id', 'roll_segment'])\n"
        "rows = 0\n"
        "for batch in scanner.to_batches():\n"
        "    assert all(development.start <= d <= development.end\n"
        "               for d in batch.column('session_date_ny').unique().to_pylist())\n"
        "    indices = batch.column('event_idx_day').to_numpy()\n"
        "    if len(indices):\n"
        "        assert int(indices[0]) == rows\n"
        "        assert (indices[1:] == indices[:-1] + 1).all()\n"
        "    rows += batch.num_rows\n"
        "print('Development day:', day, 'records:', rows)"
    ),
    nbf.v4.new_markdown_cell(
        "## Larger Development selections\n\n"
        "`scan_development(columns=[...])` selects the frozen Development range lazily. "
        "Iterate `.to_batches()`; do not materialize three years with `.to_table()` or pandas. "
        "A date range touching either holdout raises `PermissionError` before file access. "
        "Raw bytes, native integer prices and original delivery order remain intact. "
        "Split membership alone does not make a session eligible. No trading edge is established."
    ),
]
for index, cell in enumerate(notebook.cells):
    cell.id = f"development-{index:02d}"
destination = Path(__file__).resolve().parents[1] / "notebooks/02_mbp1_development.ipynb"
nbf.write(notebook, destination)
print(destination)
