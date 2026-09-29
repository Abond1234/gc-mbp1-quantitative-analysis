# GC MBP-1 research governance

Read README.md, project_docs/mbp1_ingestion_contract.md, the data dictionary,
foundation findings and migration report before changing the pipeline.

- Freeze research questions, eligibility, thresholds and horizons before analysis.
- Development ends in 2023; Validation is 2024; Final Test starts in 2025.
  Keep scanner access to Final Test blocked by default. Migration integrity
  checks do not authorize feature selection or descriptive Final Test research.
- Preserve raw bytes, native precision, delivery order, session and roll boundaries.
- Run deterministic CPU numerics. No credentials or billable API calls in this pipeline.
- Never commit data, generated metadata, environments, logs or secrets.
- Preserve original build provenance. The migration compatibility config applies
  only to its exact reviewed code/configuration; never refresh it automatically.
- Run python -m unittest discover -s tests and python -m ruff check . after changes.
- No AI attribution trailers or emojis. Keep negative findings explicit.

<!-- gitnexus:start -->
# GitNexus — Code Intelligence

This project is indexed by GitNexus as **GC MBP-1 QUANTITATIVE ANALYSIS** (275 symbols, 526 relationships, 17 execution flows). Use the GitNexus MCP tools to understand code, assess impact, and navigate safely.

> Index stale? Run `node .gitnexus/run.cjs analyze` from the project root — it auto-selects an available runner. No `.gitnexus/run.cjs` yet? `npx gitnexus analyze` (npm 11 crash → `npm i -g gitnexus`; #1939).

## Always Do

- **MUST run impact analysis before editing any symbol.** Before modifying a function, class, or method, run `impact({target: "symbolName", direction: "upstream"})` and report the blast radius (direct callers, affected processes, risk level) to the user.
- **MUST run `detect_changes()` before committing** to verify your changes only affect expected symbols and execution flows. For regression review, compare against the default branch: `detect_changes({scope: "compare", base_ref: "main"})`.
- **MUST warn the user** if impact analysis returns HIGH or CRITICAL risk before proceeding with edits.
- When exploring unfamiliar code, use `query({search_query: "concept"})` to find execution flows instead of grepping. It returns process-grouped results ranked by relevance.
- When you need full context on a specific symbol — callers, callees, which execution flows it participates in — use `context({name: "symbolName"})`.
- For security review, `explain({target: "fileOrSymbol"})` lists taint findings (source→sink flows; needs `analyze --pdg`).

## Never Do

- NEVER edit a function, class, or method without first running `impact` on it.
- NEVER ignore HIGH or CRITICAL risk warnings from impact analysis.
- NEVER rename symbols with find-and-replace — use `rename` which understands the call graph.
- NEVER commit changes without running `detect_changes()` to check affected scope.

## Resources

| Resource | Use for |
|----------|---------|
| `gitnexus://repo/GC MBP-1 QUANTITATIVE ANALYSIS/context` | Codebase overview, check index freshness |
| `gitnexus://repo/GC MBP-1 QUANTITATIVE ANALYSIS/clusters` | All functional areas |
| `gitnexus://repo/GC MBP-1 QUANTITATIVE ANALYSIS/processes` | All execution flows |
| `gitnexus://repo/GC MBP-1 QUANTITATIVE ANALYSIS/process/{name}` | Step-by-step execution trace |

## CLI

| Task | Read this skill file |
|------|---------------------|
| Understand architecture / "How does X work?" | `.claude/skills/gitnexus/gitnexus-exploring/SKILL.md` |
| Blast radius / "What breaks if I change X?" | `.claude/skills/gitnexus/gitnexus-impact-analysis/SKILL.md` |
| Trace bugs / "Why is X failing?" | `.claude/skills/gitnexus/gitnexus-debugging/SKILL.md` |
| Rename / extract / split / refactor | `.claude/skills/gitnexus/gitnexus-refactoring/SKILL.md` |
| Tools, resources, schema reference | `.claude/skills/gitnexus/gitnexus-guide/SKILL.md` |
| Index, status, clean, wiki CLI commands | `.claude/skills/gitnexus/gitnexus-cli/SKILL.md` |

<!-- gitnexus:end -->
