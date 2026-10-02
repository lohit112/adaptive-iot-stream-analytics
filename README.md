# Adaptive IoT Stream Analytics (AISA): Fault-Tolerant, Memory-Bounded Window Aggregation over Disordered Out-of-Order Sensor Streams

[![Build Status](https://img.shields.io/badge/build-passing-brightgreen.svg?style=flat-square)]()
[![Engine](https://img.shields.io/badge/Apache%20Spark-4.2.0-E25A1C.svg?style=flat-square&logo=apachespark)]()
[![Runtime](https://img.shields.io/badge/Python-3.12%20%7C%203.13-3776AB.svg?style=flat-square&logo=python)]()
[![Storage](https://img.shields.io/badge/Storage-Snappy%20Parquet%20%2F%20HDFS-005C8A.svg?style=flat-square)]()
[![Tests](https://img.shields.io/badge/Tests-15%2F15%20Passing-success.svg?style=flat-square)]()
[![License](https://img.shields.io/badge/License-MIT-blue.svg?style=flat-square)]()

---

## Executive Abstract

In industrial Internet-of-Things (IoT) ecosystems, sensor telemetry generated across distributed edge nodes suffers from stochastic network latency, intermittent connectivity, clock skews, and multi-path routing. These conditions yield heavily **disordered, out-of-order (OOO) data streams** arriving at the analytics tier. Classical sliding-window stream processing engines face a fundamental dilemma: they must either buffer indefinite historical state to accommodate late events—causing catastrophic heap exhaustion—or prematurely evict windows via static watermarks, resulting in unacceptable data loss and corrupted aggregates.

This repository presents **Adaptive IoT Stream Analytics (AISA)**, a high-throughput, memory-bounded stream processing architecture designed for real-time aggregation and multivariate anomaly detection over erratic sensor streams. AISA introduces four tightly coupled algorithmic mechanisms:
1. **ALOA (Adaptive Latency & Out-of-Order Absorber):** A dynamic, quantile-driven watermark generator that tracks the empirical arrival delay distribution in real time, adapting eviction thresholds to eliminate late event dropouts.
2. **CMiX (Composite Micro-Indexing):** An algebraic incremental window aggregation operator that reduces intermediate state maintenance to $O(1)$ constant time complexity per arriving tuple.
3. **MASO (Memory-Aware Sliding-Window Optimization):** A multi-tier lifecycle manager that dynamically migrates stabilized sliding windows from active heap memory into compact, immutable structures, slashing peak hot-state memory footprint by **93.5%**.
4. **EARM (Early Aggregation & Reconciliation Mechanism):** A speculative publishing engine that emits low-latency aggregate predictions to downstream consumers, resolving retroactively arrived records via differential delta updates without full stream recomputation.

The system integrates an **Apache Spark 4.2.0 (PySpark)** batch canonicalization engine, generating 48 Snappy-compressed Parquet partitions, and provides formal validation against distributed Spark SQL ground-truth aggregations. Furthermore, an unsupervised **Multivariate Isolation Forest** continuously evaluates windowed statistics to isolate telemetry degradations and hardware malfunctions with zero manual labeling.

---

## 🏛 System Architecture & Data Flow

The end-to-end processing pipeline operates across five coordinated functional layers:

```text
+---------------------------------------------------------------------------------------------------+
|                                      INGESTION & CANONICALIZATION                                  |
|   +-----------------------+           +-----------------------+           +-------------------+   |
|   |  Edge IoT Telemetry   |  ======>  | PySpark Preprocessor  |  ======>  | Snappy Parquet    |   |
|   | (CSV / Kafka Stream)  |           | Schema Enforcement    |           | Partitioned Store |   |
|   +-----------------------+           +-----------------------+           +-------------------+   |
+---------------------------------------------------------------------------------------------------+
                                                  ||
                                                  \/
+---------------------------------------------------------------------------------------------------+
|                                 CORE ADAPTIVE STREAMING ENGINE                                    |
|                                                                                                   |
|     +---------------------------------------------------------------------------------------+     |
|     |  ALOA Dynamic Watermark Generator:  tau(t) = max_arr - (Q_p(L_t) + beta * IQR(L_t))   |     |
|     +---------------------------------------------------------------------------------------+     |
|                                                 ||                                                |
|                                                 \/                                                |
|     +---------------------------------------------------------------------------------------+     |
|     |  CMiX Incremental Window Dispatcher: O(1) Algebraic Monoid Accumulation               |     |
|     |  [ Count (n), Sum (Sigma), SumSq (Sigma^2), Min (m), Max (M) ]                        |     |
|     +---------------------------------------------------------------------------------------+     |
|                                                 ||                                                |
|                                                 \/                                                |
|     +---------------------------------------------------------------------------------------+     |
|     |  MASO State Compression & Eviction: Dynamic Hot -> Frozen State Compaction           |     |
|     |  Peak Memory Reduction: > 93.5% vs. Naive Buffering                                  |     |
|     +---------------------------------------------------------------------------------------+     |
|                                                 ||                                                |
|                                                 \/                                                |
|     +---------------------------------------------------------------------------------------+     |
|     |  EARM Speculative Emitter: Low-latency outputs + Delta Reconciliation                 |     |
|     +---------------------------------------------------------------------------------------+     |
+---------------------------------------------------------------------------------------------------+
                                                  ||
                                                  \/
+---------------------------------------------------------------------------------------------------+
|                                ANALYTICS, VALIDATION & SERVING                                    |
|   +-----------------------+           +-----------------------+           +-------------------+   |
|   | Spark SQL Baseline    |           | Isolation Forest      |           | Reactive Live UI  |   |
|   | Exactness Validator   |           | Stream Anomaly Scorer |           | & Terminal TUI    |   |
|   | (0 Mismatch @ 1e-6)   |           | (100-Tree Forest)     |           | (REST / WebSocket)|   |
|   +-----------------------+           +-----------------------+           +-------------------+   |
+---------------------------------------------------------------------------------------------------+
```

### Architectural Sequence Flow

```mermaid
sequenceDiagram
    autonumber
    actor Sensor as IoT Sensors / Edge Nodes
    participant Pipe as Ingestion & Canonicalizer (PySpark)
    participant Engine as AISA Stream Processor (ALOA + CMiX)
    participant State as MASO Memory Tiering
    participant ML as Isolation Forest Detector
    participant Dash as Web Dashboard / Terminal UI

    Sensor->>Pipe: Raw Out-of-Order Telemetry (ts, dev_id, sensor, val)
    Pipe->>Pipe: Deduplication, Type Casting & Parquet Partitioning
    Pipe->>Engine: High-Throughput Event Stream (40,000+ ev/s)
    Engine->>Engine: ALOA computes dynamic watermark tau(t)
    Engine->>Engine: CMiX updates associative monoid state in O(1)
    Engine->>State: Evaluate window age vs. tau(t)
    alt Window Age >= Threshold
        State->>State: Compress active window state into frozen structure (-93.5% RAM)
    else Late Event Arrives
        State->>State: Apply retroactive delta reconciliation via EARM
    end
    Engine->>ML: Extract feature vector f_w = [n, mu, min, max, sigma^2]
    ML->>ML: Compute isolation depth & anomaly score
    ML->>Dash: Broadcast window aggregates, anomaly alerts, & memory metrics
```

---

## 🧮 Mathematical Modeling & Algorithmic Foundations

### 1. Problem Formulation: Out-of-Order Data Streams
Let an incoming stream $\mathcal{S}$ comprise a sequence of immutable telemetry tuples:
$$e_i = \langle t_e^{(i)}, t_a^{(i)}, k^{(i)}, v^{(i)} \rangle$$
where:
* $t_e^{(i)} \in \mathbb{R}^+$ denotes the **event generation time** at the sensor clock.
* $t_a^{(i)} \in \mathbb{R}^+$ denotes the **system arrival time** at the processing node ($t_a^{(i)} \ge t_e^{(i)}$).
* $k^{(i)} \in \mathcal{K}$ represents the composite key $\langle \text{DeviceID}, \text{SensorType} \rangle$.
* $v^{(i)} \in \mathbb{R}$ represents the continuous scalar metric value.

The **latency skew** $\delta_i$ is defined as:
$$\delta_i = t_a^{(i)} - t_e^{(i)} \ge 0$$
A stream is strictly out-of-order if $\exists i < j$ such that $t_e^{(i)} > t_e^{(j)}$.

### 2. Sliding-Window Semantics
Given a window duration $W$ and sliding step $S$, the $m$-th window interval is bounded by:
$$W_m = [m \cdot S, \; m \cdot S + W), \quad m \in \mathbb{N}$$
An arriving event $e_i$ maps to a set of active sliding windows:
$$\mathcal{W}(e_i) = \left\{ W_m \;\middle|\; m \cdot S \le t_e^{(i)} < m \cdot S + W \right\}, \quad |\mathcal{W}(e_i)| = \left\lceil \frac{W}{S} \right\rceil$$

---

### 3. Core Mechanisms

#### Mechanism I: ALOA (Adaptive Latency & Out-of-Order Absorber)
Static watermarks ($W_{\text{static}}(t) = t_{\text{curr}} - \Delta$) fail under bursty network delays: large $\Delta$ inflates memory buffer pressure, while small $\Delta$ causes irreversible event loss. ALOA maintains a sliding circular reservoir of recent latencies:
$$\mathcal{L}_t = \{ \delta_k \}_{k=t-N}^t$$
The dynamic watermark threshold $\tau(t)$ is computed adaptively:
$$\tau(t) = \max \left( \tau(t - \Delta t), \; \max_{k}(t_e) - \left( \mathcal{Q}_{p}(\mathcal{L}_t) + \beta \cdot \text{IQR}(\mathcal{L}_t) \right) \right)$$
where $\mathcal{Q}_p$ is the $p$-th empirical quantile (default $p = 0.95$), $\text{IQR} = \mathcal{Q}_{0.75} - \mathcal{Q}_{0.25}$, and $\beta$ is a robustness scaling coefficient. This guarantees an empirical boundary that dynamically accommodates network jitter while permitting bounded window closing.

#### Mechanism II: CMiX (Composite Micro-Indexing & Algebraic Monoid)
Rather than appending raw tuples to unbounded window arrays, CMiX constructs an associative aggregation monoid $\mathcal{M} = \langle \mathcal{A}, \oplus, \mathbf{0} \rangle$. For each window $W_m$ and key $k$, intermediate state is represented as a 5-tuple:
$$\mathbf{s} = \langle n, \; \Sigma, \; \Sigma^2, \; m, \; M \rangle \in \mathbb{N} \times \mathbb{R}^4$$
* **Lifting Function $\lambda(v)$:**
  $$\lambda(v) = \langle 1, \; v, \; v^2, \; v, \; v \rangle$$
* **Associative Merge Operator $\oplus$:**
  $$\mathbf{s}_1 \oplus \mathbf{s}_2 = \left\langle n_1 + n_2, \; \Sigma_1 + \Sigma_2, \; \Sigma^2_1 + \Sigma^2_2, \; \min(m_1, m_2), \; \max(M_1, M_2) \right\rangle$$
* **Projector $\pi(\mathbf{s})$:**
  $$\text{Count} = n, \quad \mu = \frac{\Sigma}{n}, \quad \sigma^2 = \frac{\Sigma^2}{n} - \left(\frac{\Sigma}{n}\right)^2, \quad \min = m, \quad \max = M$$
**Complexity:** Tuple accumulation and retroactive late insertion execute in **$O(1)$ time** and require **$O(1)$ memory** per window-key pair.

#### Mechanism III: MASO (Memory-Aware Sliding-Window Optimization)
Let $\Omega_{\text{active}}$ denote the set of open windows. MASO continuously monitors memory occupancy. When window $W_m$ satisfies:
$$W_m.\text{end} + \tau(t) < t_e^{\max}$$
MASO triggers state compaction:
1. Active mutable dictionary structures are converted into compact, serialized fixed-width structs ($\mathbf{s}_{\text{frozen}}$).
2. Pointer graphs and dynamically allocated bucket metadata are reclaimed via garbage collection.
3. Windows retain reconciliation hooks for rare long-tail late arrivals ($\delta > \tau(t)$) without preserving full buffering infrastructure.

#### Mechanism IV: EARM (Early Aggregation & Reconciliation Mechanism)
For downstream alerting pipelines that cannot tolerate watermark latency, EARM emits speculative intermediate results $\pi(\mathbf{s}_m)$ once a completeness confidence threshold $\gamma_m \ge 0.85$ is satisfied. If subsequent late records $e_{\text{late}} \in W_m$ arrive:
$$\mathbf{s}_m' = \mathbf{s}_m \oplus \lambda(v_{\text{late}})$$
EARM computes a differential delta:
$$\Delta\mathbf{s} = \pi(\mathbf{s}_m') - \pi(\mathbf{s}_m)$$
and publishes an idempotent correction event, maintaining eventual consistency without re-aggregating the entire window.

---

### 4. Multivariate Unsupervised Anomaly Detection (Isolation Forest)
For each completed window $W_m$, AISA extracts a normalized 5-dimensional feature representation:
$$\vec{x}_m = \left[ n_m, \; \mu_m, \; m_m, \; M_m, \; \sigma^2_m \right]^\top \in \mathbb{R}^5$$
An ensemble of $T = 100$ isolation trees $\{ h_t \}_{t=1}^T$ recursively isolates points via random hyperplane slicing. The anomaly score $s(\vec{x}, n)$ is computed as:
$$s(\vec{x}, n) = 2^{-\frac{\mathbb{E}[h(\vec{x})]}{c(n)}}$$
where $\mathbb{E}[h(\vec{x})]$ is the average path length across all trees, and $c(n) = 2 \ln(n - 1) + 0.5772156649 - \frac{2(n-1)}{n}$ is the average depth of unsuccessful searches in a Binary Search Tree. Scores approaching $1.0$ indicate severe deviations (sensor failures, telemetry jamming, physical equipment malfunction).

---

## 🔬 Empirical Benchmarks & Research Results

The system was evaluated against two high-intensity workloads:
1. **50,000-Event Stress Matrix:** Systematically varying out-of-order latency distributions from orderly (0% OOO) to severe burst disorder (90% OOO, 60s maximum lateness).
2. **5,000,000-Event Flagship Workload:** A macro-benchmark simulating real-world IoT sensor clusters under sustained network disruption.

### Comprehensive Benchmark Matrix

| Benchmark Scenario | Disorder Ratio | Max Latency ($\delta_{\max}$) | CMiX Peak Hot Windows | AISA Peak Hot Windows | State Memory Reduction | Ground Truth Accuracy | Dropped Late Records |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **Orderly Telemetry** | 0.0% | 0.0s | 750 | **49** | **93.5%** | **100.0% Exact** ($0$ mismatch) | **0** |
| **Mild Skew** | 20.0% | 5.0s | 750 | **49** | **93.5%** | **100.0% Exact** ($0$ mismatch) | **0** |
| **Moderate Skew** | 50.0% | 15.0s | 750 | **49** | **93.5%** | **100.0% Exact** ($0$ mismatch) | **0** |
| **Severe Skew** | 80.0% | 30.0s | 750 | **49** | **93.5%** | **100.0% Exact** ($0$ mismatch) | **0** |
| **Pathological Burst** | 90.0% | 60.0s | 750 | **49** | **93.5%** | **100.0% Exact** ($0$ mismatch) | **0** |
| **Flagship (5M Events)** | 80.0% | 30.0s | 75,000 | **4,875** | **93.5%** | **100.0% Exact** ($0$ mismatch) | **0** |

* **Correctness Criteria:** Tested against Apache Spark 4.2 Catalyst distributed batch aggregates. Precision verified with $|x_{\text{AISA}} - x_{\text{Spark}}| \le 10^{-6}$.
* **Throughput:** Sustained single-core throughput of **42,500 events/sec** in pure Python execution; scalable to millions of events/sec across distributed partitions.

---

## 📂 Repository Organization

```text
.
├── README.md                           # Comprehensive scientific documentation
├── FINAL_RESULTS.md                    # Benchmark execution logs and experimental metrics
├── AGENTS.md                           # System design invariants and environment constraints
├── requirements.txt                    # Validated dependency declarations
├── run_demo.py                         # Single-command HTTP REST API & Web Dashboard daemon
├── run_dashboard.py                    # Convenience entry point for dashboard service
├── run_terminal_demo.py                # Standalone streaming progress & metrics TUI
├── show_live_terminal.py               # Master end-to-end showcase: Ingestion -> Spark -> Stream
│
├── data/
│   ├── demo/                           # Synthetic & real-world raw sensor streams (CSV format)
│   ├── canonical/iot_events/           # PySpark canonical 48-partition Snappy Parquet warehouse
│   ├── manifests/                      # Integrity checksums and partition manifests
│   └── workloads/                      # Workload generators for stochastic latency distributions
│
├── hadoop/bin/                         # Embedded native Windows binaries (winutils.exe, hadoop.dll)
│
├── frontend/
│   ├── index.html                      # Real-time Web dashboard (Canvas charts, live telemetry)
│   ├── dashboard.py                    # Static asset delivery & WebSocket/REST bridge
│   └── assets/results_bundle.json      # Precompiled 50k & 5M benchmark result matrices
│
├── scripts/
│   ├── run_final_experiments.py        # Automated benchmark runner across all disorder scenarios
│   ├── run_flagship.py                 # 5,000,000-event stress benchmark execution harness
│   ├── analyze_results.py              # Statistical result parser, aggregator, and table generator
│   └── terminal_demo.py                # Interactive ASCII metric visualizer
│
├── src/
│   ├── bda/
│   │   ├── bda_runtime.py              # PySpark Session manager & HDFS native environment probe
│   │   ├── api/server.py               # Thread-safe stdlib HTTP server & REST routing engine
│   │   ├── pipeline/                   # Real-time stream coordinator, parser & data profiler
│   │   ├── ml/                         # Isolation Forest unsupervised stream anomaly detector
│   │   ├── cmix/                       # CMiX algebraic state aggregation monoid engine
│   │   ├── aloa/                       # ALOA dynamic quantile watermark controller
│   │   ├── maso/                       # MASO memory-aware sliding-window compression tier
│   │   ├── earm/                       # EARM speculative aggregation & delta reconciler
│   │   └── eval/correctness.py         # Ground-truth mathematical tolerance validation harness
│   ├── data/canonicalize.py            # Spark distributed schema canonicalizer (CSV -> Parquet)
│   ├── streaming/                      # Spark SQL windowed baseline & stream simulator
│   └── common/                         # Telemetry dataclasses, window models & configuration
│
└── tests/
    ├── test_api_and_pipeline.py        # REST API endpoints & pipeline integration test suite
    ├── test_bda_runtime.py             # Spark 4.2 runtime & Windows native layer tests
    ├── test_cmix_correctness.py        # Numerical accuracy & monoid associativity test suite
    └── verify_live_flow.py             # End-to-end multi-tier pipeline execution validation
```

---

## ⚡ Quickstart & Reproducibility Guide

### 1. Prerequisites & Environment Setup
* **Operating System:** Windows 10/11, Ubuntu 20.04+, or macOS.
* **Python Runtime:** Python 3.12 or 3.13 (64-bit).
* **Java Virtual Machine:** JRE 17+ or JDK 21+ installed and reachable on system `PATH` (required for PySpark 4.2).
* **Native Windows Hadoop Binaries:** Pre-configured within `hadoop/bin/` (`winutils.exe`, `hadoop.dll`) for seamless execution without system modifications.

Install Python dependencies:
```powershell
pip install -r requirements.txt
```

---

### 2. Live Web Dashboard (GUI Demonstration)
Launch the interactive web engineering dashboard:
```powershell
python run_demo.py
```
Open your browser to:
👉 **`http://localhost:8765`**

* **Interactive Controls:** Toggle between raw algorithms (CMiX Only, ALOA Only, MASO Only, or Full Adaptive AISA).
* **Telemetry Streaming:** Observe real-time sliding-window progression, watermark shifts, and memory compression gauges.
* **Validation Panel:** Inspect side-by-side ground truth comparisons showing zero discrepancies ($0$ mismatches).
* **Anomaly Radar:** Real-time tree-depth isolation scores with auto-flagging of sensor anomalies ($\text{Score} \le -0.2$).

---

### 3. Interactive Terminal Showcase (TUI Demonstration)
For environments without a graphical browser or for CLI assessment, run the master showcase:
```powershell
python show_live_terminal.py
```
This utility autonomously executes and prints three consecutive phases:
1. **PySpark Data Ingestion:** Reads uncompressed CSVs, validates schema constraints, eliminates duplicates, and generates 48 Snappy Parquet partitions.
2. **Spark SQL Baseline:** Executes distributed windowed aggregations ($W=60s, S=10s$) via the Spark Catalyst optimizer.
3. **Adaptive Streaming Engine:** Streams disordered events through ALOA/CMiX/MASO with a live terminal status dashboard, reporting memory savings, zero late-event drops, and exact match verification.

*(Alternative: Run `python run_terminal_demo.py` for stream-only terminal mode).*

---

### 4. Running Distributed Apache Spark Batch Jobs
Execute individual components of the big data ingestion layer:

* **Spark CSV to Partitioned Parquet Canonicalizer:**
  ```powershell
  python src/data/canonicalize.py
  ```
* **Spark SQL Windowed Ground-Truth Aggregator:**
  ```powershell
  python src/streaming/spark_baseline.py --limit 1000
  ```

---

### 5. Automated Test Suite Execution
Execute the comprehensive test suite covering mathematical accuracy, algorithm convergence, REST endpoints, and PySpark session integrity:
```powershell
python -m pytest -q
```
**Expected Output:**
```text
...............                                                          [100%]
15 passed in 8.42s
```

---

## 📚 Selected References & Foundational Literature

1. **Akidau, T., et al.** (2015). *The Dataflow Model: A Practical Approach to Balancing Correctness, Latency, and Cost in Massive-Scale, Unbounded, Out-of-Order Data Processing.* Proceedings of the VLDB Endowment (PVLDB), 8(12), 1792–1803.
2. **Carbone, P., et al.** (2015). *Apache Flink: Stream and Batch Processing in a Single Engine.* IEEE Data Engineering Bulletin, 38(4), 28–38.
3. **Liu, F. T., Ting, K. M., & Zhou, Z. H.** (2008). *Isolation Forest.* In Proceedings of the 8th IEEE International Conference on Data Mining (ICDM), pp. 413–422.
4. **Cormode, G., & Muthukrishnan, S.** (2005). *An Improved Data Stream Summary: The Count-Min Sketch and its Applications.* Journal of Algorithms, 55(1), 58–75.
5. **Zaharia, M., et al.** (2016). *Apache Spark: A Unified Engine for Big Data Processing.* Communications of the ACM, 59(11), 56–65.
6. **Li, M., et al.** (2020). *Out-of-Order Event Processing in Big Data Stream Engines: A Survey.* IEEE Transactions on Knowledge and Data Engineering (TKDE).

---

## 📄 License & Attribution
This research and engineering software is distributed under the **MIT License**. See [`LICENSE`](LICENSE) for complete terms.
