# Spec summary (condensed from the master specification — all hard facts retained)

## Purpose
Upload IoT CSV/Parquet (possibly out-of-order) -> validate -> profile -> (HDFS/Parquet, Spark) -> Kafka producer ->
topic `bda-iot-events` @ localhost:9092 -> CMiX -> ALOA -> MASO -> EARM -> event-time aggregates ->
feature extraction -> Isolation Forest -> anomaly_score/anomaly_label -> dashboard.
Two modes: research/evaluation and interactive demo. Research claim = STATE/MEMORY efficiency trade-off, NOT speed.

## Data model
Columns: DeviceId, Sensor, Timestamp (event time), Value. Repo canonical data uses `Time`; accept both.
Never replace event time with arrival time; keep arrival sequence separately.

## Components
- HDFS = storage; Parquet = format (distinct things). Canonical dataset outside Git; keep data/workloads/scenarios.json.
- Spark: (A) large-scale processing, (B) INDEPENDENT ground truth (count,sum,avg,min,max per (window_start, DeviceId, Sensor)). Streaming system must not be its own oracle.
- Kafka: configurable injection speed; do not bypass in main demo path.
- CMiX: 60 s window, 10 s slide (NOT 5 s tumbling). State per (window_start, DeviceId, Sensor): count,sum,min,max; avg=sum/count.
  Lifecycle: HOT(state) -> finalize -> ARCHIVE(finalized_state) -> freeze+emit -> FROZEN(frozen_keys). Finalization != irreversible release;
  release happens at freeze_and_emit() (metric frozen_emissions). `evicted_state_entries` = hot->archive accounting, not physical deletion.
- ALOA: adaptive order/lateness control; observes lateness, OOO, state pressure, budget, reconciliation. NOT a global sort.
- MASO: state organization/accounting/lifecycle/offload planning.
- EARM: adaptive irreversible release. Critical metric late_after_eviction. 0 means "zero observed in evaluated workloads" — never a formal guarantee.

## Late events
A: arrives while HOT -> normal update. B: after finalization before release -> reconcile (late_after_finalization++, reconciled_events++).
C: after release -> late_after_eviction++ (may be dropped by config, never hidden from metrics).

## Metrics to keep (names unchanged)
peak_hot, final_hot, final_archive, frozen_emissions, reconciled_events, late_after_finalization, late_after_eviction,
over_budget_events, ooo_inversions, finalization_audit. Plus throughput, peak RSS, OOO rate, max lateness. Do not report unmeasured values.

## Correctness
Key (window_start, DeviceId, Sensor); compare count,sum,avg,min,max; tolerance 1e-6. Exact = same key sets, missing=0, extra=0, all fields within tol.
"Exact" is aggregate agreement, not byte equality. Comparison utility builds a ground-truth dict — not an O(1)-memory validator.

## Scenarios
baseline, mild_ooo, moderate_ooo, heavy_ooo, burst_ooo.

## Validated reference results (research tab only; never hard-code in live demo)
50K: GT rows 50,576; missing=extra=mismatch=0 (with Spark cross-check).
200K: reference rows 201,448; 7 mechanisms x 5 scenarios = 35 comparisons all 0/0/0. OOO: mild ~40-41% (max lateness ~8s), moderate ~64-65% (~9s), heavy ~69-70% (~9s), burst ~69% (~10s). Full/EARM late_after_eviction=0.
Reconciliation 200K (fixed2 vs adaptive): mild 3135/512, moderate 7277/27, heavy 10865/17, burst 14012/13. fixed10 reduces reconciliation but retains state longer (more memory).
5M flagship:
- CMiX: peak_hot 5,025,321; final_hot 5,025,321; archive 0; frozen 0; reconciled 0; 30,240.13 ev/s; peak RSS 1,678.0 MB.
- Full: peak_hot 469; final_hot 409; final_archive 312,576; frozen_emissions 4,712,336; reconciled 318; late_after_finalization 318; late_after_eviction 0; 10,220.46 ev/s; peak RSS 1,042.8 MB.
- Peak hot reduction ~99.9907%; RSS reduction ~37.9%; throughput ~66% LOWER for Full. CMiX vs Spark and Full vs CMiX: 0/0/0.

## Ablation (primary research ablation)
A CMiX; B +ALOA; C +MASO; D +EARM. Measure throughput, peak hot, RSS, reconciliation, late_after_finalization, late_after_eviction, correctness.

## ML
Isolation Forest (unsupervised; seeded). Features from EVENT-TIME aggregates (never raw arrival order). Modular feature groups:
A basic aggregates; B statistical (mean,min,max,std,var,range,count); C +temporal (rate of change, rolling mean/std, time-of-day, device activity); D +stream-aware (OOO stats).
Outputs anomaly_score, anomaly_label. Persist model config, feature list, params, threshold, version; show in dashboard.
Modes: Demo (train on uploaded/reference, score processed) and Research (separate train/test). NO labels -> NO accuracy/precision/recall/F1.
Report: anomaly count/%, score distribution, seed stability, qualitative cases.

## Dashboard (stdlib backend + HTML/CSS/JS)
Sections: A upload+schema validation+preview; B profile (events, devices, sensors, time range, missing, OOO count/%, max lateness);
C stream config (broker, topic, window 60, slide 10, mode, lateness policy); D live processing (events, ev/s, current event time, watermark,
active/finalized/frozen, reconciled, late-after-finalization/eviction); E memory/state (peak/current hot, archive, frozen_emissions, RSS, reduction %);
F correctness (expected/actual rows, missing, extra, mismatches, tolerance, honest status); G OOO analysis (OOO %, lateness histogram, reconciliation, late categories);
H ML (observations, anomaly count/%, score histogram, anomalous devices/sensors, timeline, drill-down); I final table + export.
Charts: events over time, throughput, active state, RSS, lateness distribution, reconciliation, lifecycle hot/finalized/frozen, score histogram, anomaly timeline, device/sensor summary.
Must show arrival order vs event-time order for the demo data.

## API
POST /api/upload, POST /api/profile, POST /api/start, GET /api/status, /api/metrics, /api/correctness, /api/results, /api/ml, /api/export.

## Layout
src/bda/{cmix,aloa,maso,earm,eval,ml,pipeline,api}. Existing: src/bda/cmix/{processor,kafka_processor,runner}.py, aloa/{policy,controller,cmix_integrated,compare_cmix}.py, eval/correctness.py, tests/test_cmix_correctness.py, scripts/*.

## Errors to handle (actionable messages)
missing columns, invalid timestamps/numerics, empty data, duplicates, Kafka/topic unavailable, Spark unavailable, malformed rows, missing DeviceId/Sensor/Value/Timestamp, model failure, insufficient ML data.

## Tests
CMiX aggregation, window assignment, OOO, reconciliation, finalization, irreversible release, metric accounting, correctness compare, features, ML, upload validation, API, + one end-to-end test.

## Demo data
data/demo/sample_ooo.csv (5 rows: 10:01:01, :05, :03, :02, :04) — dashboard must flag disorder then process by event time.
