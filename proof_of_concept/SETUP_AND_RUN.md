# Fresh VM → Run the Experiment (setup & run guide)

End-to-end commands to take a **brand-new Debian 13 (Trixie) Linux VM** to a completed TPC-H
I/O-method experiment. Run everything **inside the VM**, as a user with `sudo`.

> **Scope of this run:** the paper's experiment uses **three database scales — 100 MB, 1 GB,
> and 10 GB**. Those are the only databases built and benchmarked below. Larger scales
> (e.g. 40 GB, 100 GB, 1 TB) are **future-work references**, not part of this reproduction;
> see the note at the end of Step 4 if you want to add them later.

> **Target OS:** Debian 13 (Trixie). `run_setup.sh` assumes the PGDG `trixie-pgdg` repo and
> `/etc/postgresql/18/...` paths. On Ubuntu the package/repo steps differ slightly (the
> `apt.postgresql.org.sh` script handles both, but conf paths are still `/etc/postgresql/18/main`).
>
> **What installs what:**
> - PostgreSQL **server** → you install it (Step 2). `run_setup.sh` does *not* install the server,
>   it only waits for a running one.
> - PGDG repo, `git/gcc/make`, tpch-kit (dbgen/qgen), the role/DB, data & queries →
>   `run_setup.sh` (Step 4).
> - The three I/O-mode config files + original-conf backup → `configure_pg_modes.sh` (Step 5).

---

## 0. Get the code onto the VM

```bash
sudo apt-get update -y
sudo apt-get install -y git
git clone <YOUR_REPO_URL> ~/analysis_asynchronous_db_read_operation
cd ~/analysis_asynchronous_db_read_operation/proof_of_concept
chmod +x *.sh
```

## 1. Install prerequisites

`run_setup.sh` installs `git/gcc/make`, but install them up front so Step 2/3 work cleanly.

```bash
sudo apt-get update -y
sudo apt-get install -y ca-certificates curl gnupg lsb-release \
                        git gcc make python3 bc
```

`bc` and `python3` are used by the benchmark math; `curl`/`gnupg` by the PGDG repo script.

> **uv (optional but recommended)** — the experimental-design generator is invoked as
> `uv run generate_experimental_design.py`. Install uv, or see Step 7 for a plain-`python3`
> alternative.
> ```bash
> curl -LsSf https://astral.sh/uv/install.sh | sh
> source $HOME/.local/bin/env   # or restart the shell
> ```

## 2. Add the PGDG repo and install PostgreSQL 18

PGDG 18 packages are built **with io_uring support**, which the `iouring` mode needs.

```bash
# Official PGDG setup script (creates the trixie-pgdg apt source)
sudo apt-get install -y postgresql-common
sudo /usr/share/postgresql-common/pgdg/apt.postgresql.org.sh -y

# Install the server + client (v18)
sudo apt-get update -y
sudo apt-get install -y postgresql-18 postgresql-client-18

# Start it and enable on boot
sudo systemctl enable --now postgresql
sudo systemctl status postgresql --no-pager
```

Verify version and io_uring availability:

```bash
psql --version                                   # expect 18.x
uname -r                                          # kernel should be 5.1+ for io_uring
sudo -u postgres psql -c "SHOW server_version_num;"   # expect 180000+
```

## 3. Sanity-check the server responds

```bash
sudo -u postgres psql -c "SELECT 1;"
```

If this works, `run_setup.sh`'s `wait_for_postgres` will pass.

## 4. Build TPC-H + create databases (one per scale)

`run_setup.sh` per scale: builds tpch-kit (dbgen/qgen), runs `dbgen -r 1` (which also emits the
mandatory refresh data `dss.ri`/`dss.rd`), creates the `tpch_user` role and the database,
loads data, and generates Q1–Q22 **plus the per-stream query sets** (`stream0..streamN`).

```bash
cd ~/analysis_asynchronous_db_read_operation/proof_of_concept

# One command per scale factor. SCALE_FACTOR is ~GB; DB_NAME follows tpch_db_<n>gb.
# These three scales are the ones used in the paper.
SCALE_FACTOR=0.1  DB_NAME=tpch_db_100mb ./run_setup.sh
SCALE_FACTOR=1    DB_NAME=tpch_db_1gb   ./run_setup.sh
SCALE_FACTOR=10   DB_NAME=tpch_db_10gb  ./run_setup.sh
```

> These three scales (100 MB / 1 GB / 10 GB) **must match the `--db-sizes` in Step 7.**
> **Disk/time:** do the small ones first; SF=10 already takes a while and ~10 GB+ of disk.
>
> **Future references (optional, not part of the paper):** larger scales such as 40 GB or
> 100 GB are listed in the paper only as future work. To add one later, run another line —
> e.g. `SCALE_FACTOR=40 DB_NAME=tpch_db_40gb ./run_setup.sh` — and remember to also add `40`
> to `--db-sizes` in Step 7. SF=40 needs tens of GB; larger SFs grow fast.

DB credentials created by setup: user `tpch_user`, password `tpch_password_123`.

Quick check that a DB loaded:
```bash
export PGPASSWORD=tpch_password_123
psql -h localhost -U tpch_user -d tpch_db_1gb -c "\dt"
psql -h localhost -U tpch_user -d tpch_db_1gb -c "SELECT count(*) FROM orders;"
```

## 5. Set up the three PostgreSQL I/O modes (once)

Creates `/etc/postgresql/18/main/modes/{sync,bgworkers,iouring}.conf` and backs up the original
`postgresql.conf`. **Required before** `toggle_pg_config.sh` / the experiment can switch modes.

```bash
sudo ./configure_pg_modes.sh
```

Test switching works (and confirm io_uring is accepted):

```bash
sudo ./toggle_pg_config.sh iouring
sudo -u postgres psql -c "SHOW io_method;"     # expect io_uring
sudo ./toggle_pg_config.sh sync                # back to baseline
```

If `SHOW io_method;` errors or io_uring fails to start, your PostgreSQL build lacks io_uring
support — recheck Step 2 (use PGDG packages, not distro PostgreSQL).

## 6. Smoke-test one short run (strongly recommended)

Run the benchmark once against the smallest DB before committing to the full sweep. This
exercises the power + throughput tests and the QphH calculation end to end.

```bash
export PGPASSWORD=tpch_password_123
SCALE_FACTOR=0.1 DB_NAME=tpch_db_100mb IO_METHOD=sync \
  ITERATIONS=1 RUNS_PER_ITERATION=1 ./run_tests.sh sync

# Check outputs:
cat query_results/tpch_response_variable.csv     # should have a non-zero qphh_metric
ls -la query_errors.log                          # empty = clean run
```

If `dss.ri`/`dss.rd` are missing the run now **aborts loudly** (by design) — re-run Step 4 for
that scale so dbgen regenerates them.

## 7. Generate the experimental design (CRD schedule)

3 I/O methods × your DB sizes × replicates, randomized. **`--db-sizes` must match Step 4.**

```bash
uv run generate_experimental_design.py \
  --db-sizes 0.1 1 10 \
  --replicates 12 \
  --output experimental_design_schedule.csv \
  --cooldown 1 --randomize-databases
```

No uv? Use python3 with the project deps (pandas, numpy):
```bash
python3 -m pip install --user pandas numpy
python3 generate_experimental_design.py --db-sizes 0.1 1 10 --replicates 12 \
  --output experimental_design_schedule.csv --cooldown 1 --randomize-databases
```

## 8. Run the experiment

Two options:

**A) Full automated cycle** (cleans DBs, regenerates data+queries, regenerates the schedule,
then runs everything). Use this if you want one command to do Steps 4+7+8.
> ⚠️ It **drops and rebuilds** all the databases listed in its `SCALES=(...)` array. Make sure
> that array and the `--db-sizes` in its `generate_schedule()` are set to the paper's three
> scales (`0.1 1 10`) — edit them if they differ.
```bash
./run_full_experiment.sh
```

**B) Just run the schedule** you already generated in Step 7 (databases already built):
```bash
./run_randomized_experiment.sh
```

Either way it runs in randomized order, switching I/O method + restarting PostgreSQL + clearing
caches + cooling down before each run. It prompts for confirmation and shows an ETA. The run is
**resumable** — the schedule CSV tracks PENDING/COMPLETED/FAILED, so re-running continues where
it left off.

> **Long runs:** use `tmux`/`screen` so an SSH drop doesn't kill it:
> ```bash
> sudo apt-get install -y tmux
> tmux new -s tpch
> #   ... run ./run_randomized_experiment.sh inside ...
> #   detach: Ctrl-b then d   |   reattach: tmux attach -t tpch
> ```

## 9. Where the results are

All under `proof_of_concept/randomized_results/`:

| Path | Contents |
|---|---|
| `experimental_design_schedule.csv` | **Master results table** — `qphh_result` per run + status |
| `randomized_results/raw_data/run*_complete.csv` | 22 power-test query timings per run |
| `randomized_results/raw_data/run*_refresh.csv` | RF1/RF2 timings |
| `randomized_results/raw_data/run*_interval.csv` | throughput interval `Ts`, stream count |
| `randomized_results/logs/run_*.log` | full per-run log |
| `randomized_results/logs/run_*.errors.log` | **errors only** — exists only for runs that failed |
| `randomized_results/experiment_report_*.txt` | final summary |
| `randomized_experiment.log` | master log |

**QphH is computed automatically** during the run (written into the schedule CSV) — no separate
script needed. A `qphh_result` of `0.00` plus a `run_*.errors.log` means that run had query/RF
failures; open the errors log to see the actual Postgres error.

---

## Quick reference (TL;DR)

```bash
# 1. prerequisites
sudo apt-get update -y && sudo apt-get install -y ca-certificates curl gnupg git gcc make python3 bc
# 2. postgres 18 (PGDG, has io_uring)
sudo apt-get install -y postgresql-common
sudo /usr/share/postgresql-common/pgdg/apt.postgresql.org.sh -y
sudo apt-get update -y && sudo apt-get install -y postgresql-18 postgresql-client-18
sudo systemctl enable --now postgresql
# 3-4. build TPC-H + DBs (repeat per scale — paper uses 100MB/1GB/10GB)
cd ~/analysis_asynchronous_db_read_operation/proof_of_concept && chmod +x *.sh
SCALE_FACTOR=0.1 DB_NAME=tpch_db_100mb ./run_setup.sh
SCALE_FACTOR=1   DB_NAME=tpch_db_1gb   ./run_setup.sh
SCALE_FACTOR=10  DB_NAME=tpch_db_10gb  ./run_setup.sh
# 5. PG modes
sudo ./configure_pg_modes.sh
# 6. smoke test
export PGPASSWORD=tpch_password_123
SCALE_FACTOR=0.1 DB_NAME=tpch_db_100mb ITERATIONS=1 RUNS_PER_ITERATION=1 ./run_tests.sh sync
# 7. schedule
uv run generate_experimental_design.py --db-sizes 0.1 1 10 --replicates 12 \
  --output experimental_design_schedule.csv --cooldown 1 --randomize-databases
# 8. run (inside tmux)
./run_randomized_experiment.sh
```
