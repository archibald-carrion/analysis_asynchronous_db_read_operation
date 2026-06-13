# Legacy / archived scripts

These files are **not part of the live experiment flow**
(`run_full_experiment.sh` → `run_setup.sh` / `generate_experimental_design.py` →
`run_randomized_experiment.sh` → `run_tests.sh`). They are kept for reference/history.
None of them is referenced by any active script — verified before archiving.

| File | Why archived |
|---|---|
| `run_all_benchmarks.sh` | **Old orchestrator**, superseded by `run_randomized_experiment.sh`. Loops over the 3 modes directly (ITERATIONS=15 × 2 runs) instead of reading the randomized CRD schedule CSV. |
| `check_pg_mode.sh` | Standalone helper to print the current PostgreSQL I/O mode. Not invoked by the flow. |
| `show_running_queries.sh` | Standalone helper to inspect active queries during a run. Diagnostic only. |
| `database_export.sh` | One-off DB export/dump utility. Not part of setup or execution. |
| `fix_limit_queries.sh` | One-off patch script that rewrote `LIMIT` clauses in the query files. Already applied. |
| `fix_schedule_csv.py` | One-off repair script for a malformed schedule CSV. Produced `experimental_design_schedule_fixed.csv`. |
| `experimental_design_schedule_fixed.csv` | Stale, repaired copy of a schedule CSV. The live schedule is `../experimental_design_schedule.csv`. |
| `recompute_qphh.py` | Post-hoc recomputation of QphH from saved raw CSVs. Useful for re-analysis, but not part of a run. |
| `export_clean_results.py` | Post-hoc results cleaning/export. Analysis-time utility, not part of a run. |

If you need any of these, they still work from here (adjust relative paths to the parent
directory). To restore one to the active folder: `git mv legacy/<file> .`
