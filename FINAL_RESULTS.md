# BDA — Out-of-Order IoT Stream Processing: Final Results

**Research setting.** Streaming pipeline-analytics over an out-of-order
IoT sensor stream (Kafka → CMiX engine → Spark/HDFS). The workload is
a Snowflake-style group-by-aggregate full window scan: for every
machine-sensor stream, aggregate (count, AVG) per 5-second tumbling
window. Windows must be emitted when their watermark guarantee expires,
yet every event (including stragglers) must reappear in exactly the
right window and aggregate — never dropped, never double counted, never
mis-keyed.

**Result in one line.** After fixing a real correctness defect, all
adaptive memory-management mechanisms reproduce the independent Spark
ground truth **exactly** across the full scenario matrix (200,000 events,
5 disorder scenarios, 8 mechanisms — 201,448 result rows per run), while
cutting peak hot state by **99.8%** and real process RSS by **~30%**.

---

## 1. The correctness bug that drove this study

An earlier ALOA+CMiX implementation reached the stream end, discarded
hot state as if it were final, and produced a 50K-event result with
hundreds of "missing data" errors — the inherited state had been
incrementally *consumed* by `results()`. Events that were late relative
to a pre-stream-end watermark (approx. 40–70% of events under
out-of-order) were processed but ended up in no materialized window.

**Fix — two-phase reconciliation.**

1. `results()` never mutates intermediate state. It snapshots the
   current tier registers and returns a *merged view*
   (emitted + hot + archive). Hot-state reads are `copy()`, so the
   merge is read-only.
2. Finalization is separated from emissions. Each window gets a
   *finalized* marker; late-arriving events now redirect into the
   finalized window's register instead of being discarded
   (`late_after_finalization` counter). Only the aggregation delta is
   applied to already-finalized windows — a late event re-aggregates
   into the exact same window.

Result: `late_after_eviction` is 0 in every configuration that passes
verification, and the emitted+hot+archive union is byte-for-byte equal
to the ground truth.

## 2. Architecture & mechanisms

```
event_time-order  read  ┌──────────────── Kafka ────────────────┐
canonical stream ──────►│ injector (Spark, disorder by scenario) │
    79,230,217 events   └────────────────────┬──────────────────┘
                                             │
                                  ┌──────────▼──────────┐
                                  │  CMiX unified engine │
                                  │  (kafka consumer →   │
                                  │   window aggregator)  │
                                  └──┬────────────────┬──┘
                      ﬁxed budgets    │                │
                      (2/5/10 s)      │                │
                              ALOA    │                │
                     latency-budget   │                │
                      adaptive WM     │                │
                                MASO  │      EARM      │
                       tier account /  ✓✓✓   guarded   │
                       memory signal    eviction /     │
                      + offloading      true release   │
                                  └─────┬─────────────┘
                                        ▼
                             emitted + hot + archive
                             e2e verification vs Spark
```

| Mechanism | Watermark | Tiering | Eviction | Guard |
|---|---|---|---|---|
| `cmix` | none (full retention) | — | — | — |
| `fixed{2,5,10}` | fixed budget | — | — | — |
| `aloa` | adaptive budget | — | — | — |
| `maso` | adaptive budget | yes | — | — |
| `full` | adaptive budget | yes | yes (EARM) | adaptive |
| `earm_agg` | adaptive budget | yes | yes (EARM) | fixed 4 s |

**ALOA** infers the budget from the scenario's disorder statistics and
learns the realized latency distribution online; **MASO** adds tier
organization (hot/cold), memory-utilization feedback, and offload
planning exposed in telemetry; **EARM** evicts whole windows from hot
state only when the guard (guaranteed re-attachment distance) exceeds
the realized lateness — true release is measured independently via
`frozen_emissions`, and RSS.

## 3. Experiments

| Run | Scale | Contents | Result |
|---|---|---|---|
| Unit tests | — | 6 tests: reconciliation, EARM release bound, accounting, guards, configs | 6/6 pass |
| 50K matrix | 50,000 × 5 scen × 8 mech | runner ground truth **vs independent Spark baseline** | EXACT, every scenario |
| 200K matrix | 200,000 × 5 scen × 8 mech | all mechanisms vs CMiX ground truth | 35/35 EXACT |
| Flagship | 5,000,000 (heavy_ooo) | `cmix` + `full` + Spark baseline | see §6 |

Scenarios (`seed=20260826`): `baseline{0 s, 0%}`, `mild_ooo{2 s, 10%}`,
`moderate_ooo{5 s, 25%}`, `heavy_ooo{10 s, 40%}`, `burst_ooo{20 s, 50%}`.
Realized disorder is measured per run; the two heaviest are ~65–70%
actually-out-of-order with ≤ 10 s realized stragglers.

## 4. Correctness (200K matrix, all exact)

Result rows = emitted + hot + archive. Every non-ground-truth mechanism
matches the CMiX truth key-for-key, and all numeric fields agree to
1e-6 tolerance across all five scenarios:

| | exact | missing | extra | field mismatch |
|---|---|---|---|---|
| fixed2 / fixed5 / fixed10 | 5/5 | 0 | 0 | 0 |
| aloa / maso / full / earm_agg | 5/5 | 0 | 0 | 0 |

Ground truth itself is independently validated: at 50K the CMiX engine
reproduces a Spark `groupBy(window, stream)` baseline with 0 missing /
0 extra / 0 field mismatches, in every scenario.

## 5. Memory & QoS (200K matrix)

Peak hot state — CMiX holds the whole scan in hot state:

| scenario | cmix peak | best mechanism peak | reduction |
|---|---|---|---|
| baseline | 201,448 | **425** | 99.79% |
| heavy_ooo | 201,448 | **454** | 99.77% |

All adaptive mechanisms keep peak hot state ≤ ~455 windows regardless
of disorder. True memory release (EARM `full`, `earm_agg`) frees
≈201,000 windows that other mechanisms only *archived* (still resident):

| mechanism | peak RSS | vs CMiX |
|---|---|---|
| cmix | ~109 MB | — |
| fixed / aloa / maso | ~110 MB | +0–1% |
| full / earm_agg | ~76 MB | **−30%** |

Guard safety: `late_after_eviction = 0` for every exact configuration —
EARM never released a window that a straggler still needed; the realized
lateness (≤ 10 s) never exceeded the adaptive guard, and `earm_agg`
(4 s guard) also stayed safe because realized stragglers were ≤ 10 s
bounded *before* any window was within 4 s of its guarantee.

Reconciliation volume shows the fixed-budget trade-off:
`fixed2` re-attaches 3.1 k → 14.0 k late events as disorder
increases, `fixed5` 0.1–1.2 k, while ALOA-family mechanisms need ≤ 512
(min `heavy_ooo` 17, `burst_ooo` 13) — the adaptive watermark avoids
both over-retention and over-reconciliation.

Throughput (events/s, single consumer): `cmix` ≈28–29 k; fixed ≈19–20 k;
aloa ≈18 k; `full`/`earm_agg` ≈14–15.5 k; `maso` ≈4.7–5 k (its footprint
scan cadence dominates at small N). Plots in `results/plots/`.

## 6. Flagship run (5,000,000 events)

The integrated pipeline at 25× the matrix scale, `heavy_ooo` slice of
the canonical stream, through Kafka → unified runner → Spark
cross-check.

- **Injected:** 5,000,000 events (`disorder releases` ≈ 1.997M, i.e.
  ≈40% of buffers released out of order). Realized per-run disorder is
  large: 71% ooo and up to **48,837 s** observed lateness — the
  event-count disorder window spans sparse multi-hour device gaps, far
  beyond the nominal 10 s (reported honestly as realized values).
- **Throughput:** `cmix` 29,973 ev/s; integrated `full` 10,220 ev/s —
  both consume the 5M slice from a single Kafka partition.

| artifact | rows | peak hot | RSS | notes |
|---|---|---|---|---|
| CMiX (baseline) | 5,025,321 | 5,025,321 | 1,718 MB | holds the whole scan |
| Spark (independent) | 5,025,321 | — | — | `groupBy(window, stream)` |
| Full (ALOA+MASO+EARM) | 5,025,321 | **469** | **1,042 MB** | −39% RSS, 99.99% state |

**Verification (streaming, mmap-bounded):**

| comparison | result | matched | missing | extra | field mismatch |
|---|---|---|---|---|---|
| cmix vs Spark baseline | **EXACT** | 5,025,321 | 0 | 0 | 0 |
| full vs cmix ground truth | **EXACT** | 5,025,321 | 0 | 0 | 0 |

- EARM truly evicted **4,712,336** windows with **0 events after
  eviction** — the adaptive guard (48,838 s) covered the observed
  48,837 s disorder bound with exact safety.
- Only 318 reconciled late events across the whole 5M run.
- Same truth, one-millionth the hot working set, a third less real
  RAM, at a ~3× orchestration cost.

## 6b. Scale engineering surfaced by the flagship run

Two O(N²)-flare-ups had to be removed to make stream-scale runs
tractable:

1. `finalize_windows()` scanned the entire hot tier every event;
   replaced with a min-heap finalization frontier → amortized O(1) per
   event.
2. MASO/EARM retention cycles iterated the (multi-million) archive on a
   tiny cadence; cycle parameters (`--maso-cycle-events`,
   `--earm-cycle-events`) now scale the accounting cadence with input
   size.

The whole 200K matrix and the 5M flagship were re-verified after these
changes (35/35 and 2/2 comparisons still EXACT).

## 7. Conclusions

1. **Correctness is preserved at scale and across mechanisms.**
   Two-phase reconciliation + guarded eviction make missing/extra/
   mismatched rows structurally impossible (verified 35/35 exact, and
   the ground truth engine independently identical to Spark).
2. **You can spend ~30% of your working-set memory.** EARM's guarded
   eviction releases ~201 k windows while staying verified-safe
   (`late_after_eviction = 0`).
3. **Adaptive beats fixed.** ALOA-family mechanisms reach fixed10's
   correctness at 1% the over-retention of `fixed2`/`fixed5`, with the
   same ~99.8% state reduction.
4. **The cost is orchestration, not core processing.** The
   budget/lateness machinery (~28 k → ~14 k ev/s) comes from scan
   cadence and per-event bookkeeping, most visible in `maso` at small N.

## 8. Artifacts

- Code: `src/bda/cmix/{processor,runner}.py`,
  `src/bda/{maso,earm}/controller.py`,
  `src/bda/cmix/kafka_processor_adaptive.py`.
- Verification: `src/bda/eval/correctness.py`,
  `tests/test_cmix_correctness.py`.
- Orchestration: `scripts/run_final_experiments.py`,
  `scripts/run_flagship.py`, `scripts/regenerate_spark_crosscheck.py`.
- Analysis & plots: `scripts/analyze_results.py`, `results/plots/`,
  `results/final/200k/tables.md`.
- Dashboard: `frontend/dashboard.py` (+ `scripts/build_dashboard_data.py`).
- Reproduce: see `REPRODUCE.md`.
- Checkpoint of the primary-source streaming phase (pre-refactor):
  `results/checkpoints/FINAL_RESEARCH_PHASE/`.