# BDA Out-of-Order IoT Stream Processing — Reproduction Guide

End-to-end pipeline: **Kafka** (event ingestion) → **CMiX** unified
processor with **ALOA** (latency-budget watermark), **MASO**
(memory-aware tier organization), **EARM** (guarded eviction &
retention) → **Spark** (independent aggregation ground truth).

The implementation targets a Snowflake-style pipeline-analytics
workload (full scan + group-by-aggregate) streaming over an
out-of-order IoT sensor stream.

---

## Environment

| Component | Path / version |
|---|---|
| Python | `.venv/bin/python` (kafka-python, pandas, pyarrow, matplotlib) |
| Spark | `$SPARK_HOME/bin/spark-submit` (local mode) |
| Hadoop HDFS | `hdfs://localhost:9000` |
| Kafka | `~/hadoop_Workspace/kafka/bin` (Bootstrap `localhost:9092`) |
| Topic | `bda-iot-events` (1 partition, RF 1, reset per scenario) |

Spark's bundled Python lacks `kafka`, so Spark jobs must run with
`PYSPARK_PYTHON` / `PYSPARK_DRIVER_PYTHON` pointing at the venv
Python; `spark-submit` injects `pyspark.zip` automatically.

## Data

- **Canonical stream:** HDFS `hdfs://localhost:9000/bda/canonical/iot_events`
  (79,230,217 events; local mirror `data/canonical/iot_events`).
- **Scenarios** (`data/workloads/scenarios.json`): disorder profiles
  `{name, max_delay_s, out_of_order_fraction}`
  `baseline {0, 0.0}`, `mild_ooo {2, 0.10}`, `moderate_ooo {5, 0.25}`,
  `heavy_ooo {10, 0.40}`, `burst_ooo {20, 0.50}`.

## Commands

### 1. Unit tests

```bash
.venv/bin/python -m unittest discover -s tests -p "test_*.py" -v
```

### 2. Experiment matrix

Primary matrix (all 8 mechanisms × 5 scenarios):

```bash
.venv/bin/python scripts/run_final_experiments.py \
  --limit 200000 --results-dir results/final/200k
```

Full pipeline of the orchestrator per scenario:
1. Reset Kafka topic (delete + recreate).
2. Inject scenario-limited slice from canonical via `spark-submit`
   `src/streaming/kafka_injector.py` (ordered by `start`, disorder
   applied by scenario).
3. Run each mechanism with `src/bda/cmix/runner.py` (`-m`,
   `--mechanism cmix|fixed|aloa|maso|earm|full|earm_agg`).
4. Verify each mechanism's emitted+hot+archive rows against the CMiX
   ground truth via `src/bda/eval/correctness.py` and write
   `comparison_summary.{json,csv}`.

Optional Spark cross-check (overwrites
`results/processing/spark_baseline`):

```bash
PYSPARK_PYTHON="$PWD/.venv/bin/python" \
PYSPARK_DRIVER_PYTHON="$PWD/.venv/bin/python" \
  scripts/regenerate_spark_crosscheck.py
```

### 3. Flagship high-volume run (5,000,000 canonical events)

```bash
.venv/bin/python scripts/run_flagship.py
```

Runs `cmix` (ground truth) and `full` (ALOA + MASO + EARM), plus an
independent Spark baseline over the same 5,000,000-event slice, and
cross-checks all three.

### 4. Analysis, plots, dashboard

```bash
.venv/bin/python scripts/analyze_results.py --results-dir results/final/200k
.venv/bin/python scripts/build_dashboard_data.py
.venv/bin/python frontend/dashboard.py --port 8765
# open http://127.0.0.1:8765
```

Plots are written to `results/plots/`; the dashboard is a static,
no-backend page served by the stdlib HTTP server.

## Mechanism semantics

- **CMiX** (`cmix`): reconcile late events into finalized state at
  stream end; the baseline / ground truth.
- **Fixed `Ns`** (`fixed{2,5,10}`): fixed watermark lateness budget.
- **ALOA** (`aloa`): latency-budget-aware adaptive watermark driven by
  scenario disorder statistics.
- **ALOA+MASO** (`maso`): tier organization & offloading; reports peak
  memory utilization and issued offload operations.
- **Full** (`full`): ALOA + MASO + EARM adaptive guard eviction.
- **EARM aggressive** (`earm_agg`): ALOA + MASO + EARM with fixed
  guard (4 s).

Key metrics:
- `evicted_state_entries` == `finalized_windows` (tier moves);
  `frozen_emissions` is the true count of windows fully released from
  hot state; `late_after_eviction` must be 0 for safe configs.
- Result rows = emitted + hot + archive (merged at end of run).

## Notes for long jobs

The experiment runner can take 30+ minutes for the full matrix.
Launch it detached and poll the log:

```bash
setsid nohup .venv/bin/python scripts/run_final_experiments.py \
  --limit 200000 --results-dir results/final/200k \
  > results/logs/phase_matrix_200k.log 2>&1 < /dev/null & disown
```