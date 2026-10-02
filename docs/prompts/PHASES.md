# Copy-paste prompts (one per new Antigravity conversation, Planning mode)

Common header for every prompt:
> Read GEMINI.md, docs/SPEC_SUMMARY.md, docs/PLAN.md (and docs/ARCHITECTURE_NOTES.md once it exists). Work ONLY on the phase below. Show a plan and assumptions first and wait for my approval. Do not modify cmix/aloa/maso/earm logic. Run `pytest -q` and paste real output. Finish with files changed, run commands, limitations.

## Phase 0 — Read and document
Read src/bda/cmix/runner.py, src/bda/cmix/processor.py, src/bda/aloa/cmix_integrated.py, controller.py in aloa/maso/earm, src/streaming/kafka_injector.py, src/bda/eval/correctness.py. Write docs/ARCHITECTURE_NOTES.md: per-event call order (process_event, update_watermark, finalize_windows, controller observe/record/run_eviction_cycle, freeze_and_emit), how runner.py builds each mechanism (cmix/aloa/maso/earm/full), how metrics and RSS are collected, constructor args of each class, and the exact functions a new pipeline module should reuse. NO code changes.

## Phase 1 — Fix baseline
Fix tests/test_cmix_correctness.py (~line 121): `rng.shuffle(positions[g:end])` shuffles a copy; modify the real list (slice assignment) and add an assertion that the generated sequence has inversions. Reword the processor.py docstring that says the disorder bound "guarantees" zero late-after-eviction to say it was observed in evaluated workloads. Run pytest; if other tests change behaviour because disorder is now real, report it before fixing anything.

## Phase 2 — Streaming pipeline
Create src/bda/pipeline/stream.py with StreamPipeline(mode in {cmix,aloa,maso,full}, window=60, slide=10, transport in {kafka,inprocess}, on_progress callback). It must reuse the existing classes exactly as runner.py does (extract shared logic by importing, not copying; if impossible, ask). Events carry event_time + arrival_sequence; never sort. Kafka producer sends to bda-iot-events at configurable rate; if broker unreachable, use in-process queue and set transport_used="inprocess_fallback". Emit progress snapshots every N events (events, ev/s, current event time, watermark, hot/finalized/frozen counts, reconciled, late_after_finalization, late_after_eviction, RSS). Return aggregates (schema: window_start, window_end, DeviceId, Sensor, count, sum, avg, min, max) + final metrics. Test: data/demo/sample_ooo.csv equals independent pandas ground truth in every mode.

## Phase 3 — Ingest, validation, profiling
Create src/bda/pipeline/ingest.py: load CSV/Parquet; map Time->Timestamp; validate columns, timestamps, numerics, empties, duplicates, malformed rows with actionable error messages (list of issues, not a crash); profile (events, devices, sensors, time range, missing values, OOO count/% and max lateness computed from FILE ORDER, lateness histogram); preview first N rows. Unit test every error case from SPEC_SUMMARY.

## Phase 4 — Live correctness
Create src/bda/pipeline/truth.py: independent pandas ground truth for 60/10 windows; Spark used only if available, otherwise report "spark: unavailable". Reuse eval.correctness.compare_rows with tolerance 1e-6. Output expected_rows, actual_rows, missing, extra, mismatches, tolerance, status ("exact" only when all zero). Tests for exact and deliberately corrupted results.

## Phase 5 — ML
Create src/bda/ml/{features.py,model.py,run.py}. Feature groups A-D as modular registry; features come from event-time aggregates only. IsolationForest with fixed random_state, optional StandardScaler, contamination configurable (default "auto"). Output anomaly_score, anomaly_label per row. Persist JSON config (model version, features, params, threshold, seed). Raise a clear InsufficientDataError for too few rows. Unsupervised ablation A-D: anomaly count/%, score distribution, stability across 5 seeds. No accuracy/precision/recall/F1. Add scikit-learn to requirements.txt.

## Phase 6 — API
Create src/bda/api/server.py on stdlib ThreadingHTTPServer, serving frontend/ too. Endpoints: POST /api/upload (raw body + X-Filename header), POST /api/profile, POST /api/start (mode, broker, topic, rate, ml group), GET /api/status, /api/metrics, /api/correctness, /api/results, /api/ml, /api/export (CSV). Pipeline runs in a background thread; keep a job object with progress history for charts. JSON errors with actionable messages. Test with http.client against an ephemeral port.

## Phase 7 — Dashboard
Extend frontend/ (vanilla JS, canvas or inline SVG charts, no new libs). Tabs: "Live Demo" (sections A-I from SPEC_SUMMARY) and "Research Results" (existing results_bundle.json, labelled as precomputed benchmark). Show arrival-order vs event-time view, transport actually used (Kafka vs fallback), honest correctness status, model config, anomaly drill-down, CSV export. Verify uploading sample_ooo_2k.csv then another file changes all live numbers.

## Phase 8 — Ablation
Create scripts/run_stream_ablation.py: modes A-D on a chosen file; table of measured throughput, peak hot, RSS, reconciliation, late_after_finalization, late_after_eviction, correctness. Output to results/ (git-ignored). Only measured values.

## Phase 9 — Tests
Fill gaps: window assignment, categories A/B/C, release, metric accounting, correctness, features, ML determinism, upload validation, API, plus one end-to-end test (sample CSV -> pipeline in-process -> aggregates -> ML -> /api/results).

## Phase 10 — Docs
Update REPRODUCE.md and README.md: install, Kafka/Spark/HDFS setup, dataset format, start backend/dashboard, tests, research experiments, live demo, module explanations, ML pipeline, dashboard architecture, validation results actually obtained, limitations and unimplemented items.
