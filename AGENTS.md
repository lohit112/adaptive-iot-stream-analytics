# GEMINI.md — Project rules (read first, every conversation)

Project: Adaptive IoT stream processing (CMiX + ALOA + MASO + EARM) + Isolation Forest + live dashboard.
Full requirements: `docs/SPEC_SUMMARY.md` (and `docs/SPEC_FULL.md` if present). Build order: `docs/PLAN.md`.

## Hard rules (never violate)
1. Do NOT modify logic in `src/bda/cmix`, `src/bda/aloa`, `src/bda/maso`, `src/bda/earm`. Wrap and call them. If you believe a change is needed, STOP and ask me with evidence (failing test / line numbers).
2. Never sort or globally reorder the full dataset before streaming. Arrival order = file order. Event time comes from the Timestamp column.
3. Do not rewrite a working algorithm into a "simplified" one. Do not create a second CMiX/ALOA/MASO/EARM.
4. Never hard-code results in the dashboard. Live-demo numbers must come from the backend run on the uploaded file. Benchmark numbers (`results_bundle.json`) live in a separate "Research Results" tab.
5. Never invent anomaly labels, precision/recall/F1/accuracy, or performance numbers. Unsupervised metrics only (anomaly count/%, score distribution, seed stability).
6. Never claim the adaptive system is faster. Never claim "guarantees". Use: "zero late-after-eviction events were observed in the evaluated workloads".
7. Preserve: window=60s, slide=10s, tolerance=1e-6, comparison key (window_start, DeviceId, Sensor), metric names and meanings.
8. Correctness status must come from missing/extra/mismatch counts, never from "the run completed".
9. Kafka (localhost:9092, topic bda-iot-events) is the default transport. If unavailable, fall back to an in-process queue and LABEL it visibly in API + UI.
10. Python 3.12 / 3.13. Dependencies: scikit-learn, pyspark, py4j. Keep the stdlib http.server backend (no new web framework without asking).
11. BDA Architecture: Spark (PySpark 4.2.0) performs schema canonicalization, deduplication, and windowed baseline analytics; HDFS (port 9000) stores/replicates canonical Parquet data with non-faked status diagnostics.

## Working method
- Work ONE phase at a time from `docs/PLAN.md`. Do not start the next phase.
- Start in Planning mode: show a plan + assumptions, wait for my approval, then implement.
- Add tests with every phase. Run `pytest -q` and paste the real output. Never claim tests pass without running them.
- Finish each phase with: files changed, how to run, test output, known limitations.
- Large datasets and results stay out of Git (`data/canonical/`, `results/`).

## How to Run Live
1. Run Dashboard: `python scripts/run_dashboard.py` (or `python -m src.bda.api.server --port 8765`)
2. Open Browser: `http://localhost:8765`
3. Run PySpark Canonicalizer: `python src/data/canonicalize.py`
4. Run Spark Baseline: `python src/streaming/spark_baseline.py --limit 1000`
5. Run Tests: `python -m pytest -q`

