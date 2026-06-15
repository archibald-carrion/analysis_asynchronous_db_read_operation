# TPC-H Experiment — proof_of_concept

Scripts that set up TPC-H databases and run the I/O-method benchmark on **PostgreSQL 18**
(Linux VM with `systemd`). See `../README.md` for the high-level flow and
`TPCH_COMPLIANCE.md` for the TPC-H compliance audit.

## Entry points

### Full experiment (everything)

```bash
./run_full_experiment.sh
```

Orchestrates the whole cycle: cleans old DBs → regenerates data + queries for each scale →
generates the randomized CRD schedule → runs the randomized experiment. Calls, in order:
`cleanup_tpch.sh`, `run_setup.sh` (per scale), `generate_experimental_design.py`,
`run_randomized_experiment.sh`.

### Setup only (build databases for one scale)

```bash
SCALE_FACTOR=40   DB_NAME=tpch_db_40gb  ./run_setup.sh
SCALE_FACTOR=10   DB_NAME=tpch_db_10gb  ./run_setup.sh
SCALE_FACTOR=1    DB_NAME=tpch_db_1gb   ./run_setup.sh
SCALE_FACTOR=0.1  DB_NAME=tpch_db_100mb ./run_setup.sh

# Queries only, no DB changes:
SCALE_FACTOR=0.1 DB_NAME=tpch_db_100mb ./run_setup.sh --queries-only
```

`run_setup.sh` installs/uses PostgreSQL 18, builds **tpch-kit** (gregrahn), generates data
with `dbgen`, loads via `\copy`, and generates Q1–Q22 with `qgen`.
See `commands-to-start-project.txt` for the exact copy-paste sequence (including cleanup).

### Generate the experimental design

```bash
uv run generate_experimental_design.py \
  --db-sizes 0.1 1 10 40 --replicates 12 \
  --output experimental_design_schedule.csv \
  --cooldown 1 --randomize-databases
```

Produces a Completely Randomized Design (CRD): 3 I/O methods × 4 sizes × 12 replicates =
**144 runs** in randomized order.

### Run the randomized experiment

```bash
./run_randomized_experiment.sh
```

Reads `experimental_design_schedule.csv` and, for each run, switches database, applies the
I/O method via `toggle_pg_config.sh`, restarts PostgreSQL, clears caches, cools down, then
invokes `run_tests.sh` (power + throughput tests, QphH). Results land in
`randomized_results/` and the schedule CSV is updated in place (PENDING → COMPLETED/FAILED),
so the run is resumable.

## I/O method configuration

`toggle_pg_config.sh <sync|bgworkers|iouring>` rewrites `/etc/postgresql/18/main/postgresql.conf`
from a backup and appends the mode-specific config from `.../modes/*.conf`, then restarts
PostgreSQL. Those mode files and the original backup are created by `configure_pg_modes.sh`.

## Database details (defaults)

- Database user: `tpch_user` · password: `tpch_password_123` · port: `5432`
- Per-scale DB names: `tpch_db_100mb`, `tpch_db_1gb`, `tpch_db_10gb`, `tpch_db_40gb`

## Outputs

- `randomized_results/raw_data/` — per-run complete/refresh/interval CSVs
- `randomized_results/logs/` — per-run logs
- `experimental_design_schedule.csv` — schedule + status + QphH results
- `randomized_experiment.log` — master log

## Notes

- These scripts target a Linux VM (`systemctl`, `/proc/sys/vm/drop_caches`,
  `/etc/postgresql/18/...`). They will not run on macOS.
- Superseded / one-off helper scripts have been archived under `legacy/` — see
  `legacy/README.md`.
