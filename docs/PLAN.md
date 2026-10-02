# Implementation plan (one phase per Antigravity conversation)

Already implemented (do not rebuild): CMiX processor, ALOA/MASO/EARM controllers, correctness comparison,
Kafka injector, Spark baseline/ground truth, experiment scripts, static results page.
Missing: pipeline/, ml/, api/, upload+profile, live dashboard, sample data, ML deps, e2e tests.

Known issues to fix:
- tests/test_cmix_correctness.py ~line 121: `rng.shuffle(positions[g:end])` shuffles a copy -> no disorder generated.
- src/bda/cmix/processor.py docstring says disorder bound "guarantees" zero late-after-eviction -> reword to "observed".

| Phase | Goal | Acceptance |
|---|---|---|
| 0 | Read runner.py, cmix_integrated.py, kafka_injector.py; write docs/ARCHITECTURE_NOTES.md. NO code changes | Notes name exact call order & functions to reuse |
| 1 | Fix shuffle test bug + docstring wording; assert test data is truly OOO | `pytest -q` green |
| 2 | `src/bda/pipeline/stream.py`: StreamPipeline (modes cmix/aloa/maso/full), Kafka or labeled in-process fallback, progress callback | 5-row sample matches independent pandas ground truth |
| 3 | `pipeline/ingest.py`: CSV/Parquet load, schema validation, column mapping (Time/Timestamp), profiling, OOO from file order, error cases | Unit test per error case |
| 4 | Live correctness: pandas ground truth (+ Spark if available), reuse eval.correctness.compare_rows | Status derived from missing/extra/mismatch |
| 5 | `src/bda/ml/`: features (groups A-D), Isolation Forest (seeded), config persistence, score+label | Insufficient-data error handled; deterministic with seed |
| 6 | `src/bda/api/`: stdlib server, endpoints, background job, raw-body upload | curl flow upload->start->status->results works |
| 7 | Dashboard sections A-I, Live Demo tab + Research Results tab, charts, export | Uploading a different file changes all live numbers |
| 8 | Stream ablation runner A-D (+ unsupervised ML ablation) | Table of measured values only |
| 9 | Tests incl. one end-to-end (CSV -> pipeline -> ML -> API) | `pytest -q` green |
| 10 | Docs: REPRODUCE.md, README, Kafka/Spark/HDFS setup, limitations | Fresh-clone instructions work |

Final claim allowed: "The evaluated architecture substantially reduces active state and observed memory while
preserving aggregate correctness on the evaluated OOO workloads, with processing overhead as the main trade-off."
