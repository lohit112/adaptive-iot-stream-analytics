# Adaptive IoT Stream Analytics (AISA)
### High-Throughput, Memory-Bounded Out-of-Order Stream Processing with Apache Spark, HDFS & Online Isolation Forest ML

[![Python 3.12+](https://img.shields.io/badge/Python-3.12%20%7C%203.13-blue.svg?logo=python&logoColor=white)](https://www.python.org/)
[![Apache Spark](https://img.shields.io/badge/Apache%20Spark-4.2.0-orange.svg?logo=apachespark&logoColor=white)](https://spark.apache.org/)
[![Apache Hadoop](https://img.shields.io/badge/Apache%20Hadoop-HDFS%20%2F%20Parquet-yellow.svg?logo=apachehadoop&logoColor=black)](https://hadoop.apache.org/)
[![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-Distributed%20Streaming-red.svg?logo=apachekafka&logoColor=white)](https://kafka.apache.org/)
[![Scikit-Learn](https://img.shields.io/badge/ML-Isolation%20Forest-green.svg?logo=scikitlearn&logoColor=white)](https://scikit-learn.org/)
[![Tests](https://img.shields.io/badge/Tests-15%20Passed%20%28100%25%29-brightgreen.svg)]()
[![License: MIT](https://img.shields.io/badge/License-MIT-purple.svg)](LICENSE)

---

## 📌 Executive Summary

Modern Internet of Things (IoT) deployments—spanning smart cities, industrial SCADA systems, and healthcare telemetry—generate continuous, high-volume sensor streams. Because of packet loss, multi-hop routing jitter, device sleep cycles, and intermittent network partitions, **sensor data frequently arrives severely delayed and out-of-order (OOO)**.

Traditional stream processing systems face a fundamental dilemma:
1. **Unbounded Buffering:** Waiting indefinitely for delayed events causes catastrophic **active-state memory explosion (Out-of-Memory / OOM)** under high-cardinality streams.
2. **Premature Eviction:** Aggressively dropping old windows to preserve RAM causes **inaccurate aggregations, corrupted metrics, and lost anomalies**.

**Adaptive IoT Stream Analytics (AISA)** solves this trade-off by combining a 4-tier adaptive streaming core (**CMiX + ALOA + MASO + EARM**) with a genuine Big Data architecture (**Apache Spark + HDFS**) and real-time **Unsupervised Isolation Forest Machine Learning**. 

### 🏆 Key Highlights
* **Up to 93.5% Hot-State Memory Reduction:** Dynamically freezes stabilized windows while holding active memory strictly bounded.
* **100% Mathematical Exactness:** Zero loss in aggregation fidelity (`missing = 0`, `extra = 0`, `mismatches = 0` vs. batch ground-truth with $10^{-6}$ float precision).
* **Zero Late-After-Eviction Losses:** 0 late events dropped across evaluated workloads.
* **Sub-Millisecond Processing Throughput:** Processes **40,000 to 90,000+ events/sec** on commodity single-node hardware.
* **Genuine Big Data Backbone:** Native PySpark 4.2.0 data canonicalizer, 48-partition Snappy Parquet storage, and socket-level HDFS diagnostics.

---

## 🏗️ End-to-End System Architecture

```
                       +-----------------------------------+
                       |         Raw IoT Datasets          |
                       | (Multi-Sensor CSV Streams / Tele) |
                       +-----------------------------------+
                                         │
                                         ▼
                       +-----------------------------------+
                       |    Apache Spark 4.2.0 Pipeline    |
                       |  - Strict Schema Enforcement      |
                       |  - Deterministic Deduplication    |
                       |  - ANSI Timestamp Normalization   |
                       +-----------------------------------+
                                         │
                                         ▼
                       +-----------------------------------+
                       |    Distributed Storage Layer      |
                       |  - HDFS (hdfs://localhost:9000)   |
                       |  - 48-Partition Canonical Parquet |
                       +-----------------------------------+
                                         │
                                         ▼
                       +-----------------------------------+
                       |    High-Throughput Transport      |
                       |  - Apache Kafka (localhost:9092)  |
                       |  - High-Speed In-Process Engine   |
                       +-----------------------------------+
                                         │
                                         ▼
                       +-----------------------------------+
                       |     Adaptive Streaming Core       |
                       | ┌───────────────────────────────┐ |
                       | │ CMiX: Matrix Incremental Slices│ |
                       | ├───────────────────────────────┤ |
                       | │ ALOA: Dynamic Lateness Watermk│ |
                       | ├───────────────────────────────┤ |
                       | │ MASO: Memory-Aware Eviction   │ |
                       | ├───────────────────────────────┤ |
                       | │ EARM: Early Result Reconciler │ |
                       | └───────────────────────────────┘ |
                       +-----------------------------------+
                                         │
                                         ▼
                       +-----------------------------------+
                       |   Unsupervised Machine Learning   |
                       |  - Scikit-Learn Isolation Forest  |
                       |  - Multivariate Window Anomaly    |
                       |    Scoring (Count, Mean, Min, Max)|
                       +-----------------------------------+
                                         │
                                         ▼
                       +-----------------------------------+
                       | Live Reactive Web & Terminal UI   |
                       |  - Sub-Second Telemetry Gauges    |
                       |  - Correctness Verifier vs Spark  |
                       |  - Dynamic Dataset Switcher       |
                       +-----------------------------------+
```

---

## ⚙️ Core Algorithmic Components

### 1. CMiX (Correctness-preserving Matrix Incremental eXecution)
Instead of recomputing window aggregates from scratch ($O(N \cdot W)$), CMiX slices event time into discrete chronological matrices ($O(1)$ amortized updates). It incrementally computes rolling window metrics (`count`, `sum`, `mean`, `min`, `max`) per `(DeviceId, Sensor)` stream slice without duplicate scanning.

### 2. ALOA (Adaptive Lateness-based Online Aggregation)
Static watermarks fail when network latency fluctuates. ALOA continuously tracks historical event disorder probabilities ($P(\text{delay} \le \delta)$) and adaptively adjusts the watermark delay. Under bursts of late arrivals, ALOA widens the safety window; under orderly arrivals, it shrinks the buffer to release memory immediately.

### 3. MASO (Memory-Aware Sliding-window Optimization)
MASO monitors the resident set size and active window count. As windows pass ALOA’s dynamic threshold, MASO transitions active "hot" state into immutable "frozen" state, compressing active window memory by **over 93%** without discarding late-arriving event reconciliations.

### 4. EARM (Early Aggregation & Reconciliation Mechanism)
For downstream low-latency dashboards and alert triggers, EARM publishes early speculative window outputs. If delayed events arrive after early emission, EARM applies state diff reconciliations without replaying the stream.

### 5. Isolation Forest Stream Anomaly Detector
An unsupervised ensemble model constructed from 100 decision trees. For each sliding window ($W=60s, S=10s$), it extracts multivariate features:
$$\vec{f}_w = \left[ \text{Count}, \mu_{\text{Value}}, \min_{\text{Value}}, \max_{\text{Value}}, \sigma^2_{\text{Value}} \right]$$
The model isolates anomalies (sensor degradation, abnormal telemetry spikes, hardware failures) by assigning normalized anomaly scores ($\le -0.2$ indicates high-severity anomaly).

---

## 📊 Empirical Benchmarks & Research Results

Evaluated across **50,000-event stress scenarios** and a **5,000,000-event flagship workload** under varying out-of-order delay distributions:

| Workload Scenario | Disorder Prob. | Max Lateness | CMiX Peak Hot | AISA Peak Hot | Memory Reduction | Correctness Verdict | Late Lost |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **Baseline (Orderly)** | 0% | 0s | 750 | **49** | **93.5%** | **EXACT (100%)** | **0** |
| **Mild OOO** | 20% | 5s | 750 | **49** | **93.5%** | **EXACT (100%)** | **0** |
| **Moderate OOO** | 50% | 15s | 750 | **49** | **93.5%** | **EXACT (100%)** | **0** |
| **Heavy OOO** | 80% | 30s | 750 | **49** | **93.5%** | **EXACT (100%)** | **0** |
| **Burst OOO** | 90% | 60s | 750 | **49** | **93.5%** | **EXACT (100%)** | **0** |
| **Flagship (5M Events)** | 80% | 30s | 75,000 | **4,875** | **93.5%** | **EXACT (100%)** | **0** |

* Ground-truth verification matches Spark batch outputs exactly (`mismatches = 0` at $10^{-6}$ float tolerance).

---

## 📁 Repository Structure

```text
├── AGENTS.md                   # Core project guidelines & runtime rules
├── FINAL_RESULTS.md            # Comprehensive research results & evaluation logs
├── README.md                   # Executive project documentation (this file)
├── requirements.txt            # Python dependencies (pyspark, scikit-learn, etc.)
├── run_demo.py                 # Live REST API & Web Dashboard Launcher
├── run_dashboard.py            # Dashboard launcher alias
├── run_terminal_demo.py        # Interactive Terminal Live Demo
├── show_live_terminal.py       # Master Spark + Streaming Terminal Showcase
│
├── data/
│   ├── demo/                   # Raw IoT sensor CSV datasets (sample_ooo.csv, 2k, etc.)
│   ├── canonical/iot_events/   # Spark 48-partition Snappy Parquet storage
│   ├── manifests/              # Parquet dataset validation manifests
│   └── workloads/              # Standard OOO scenario configurations
│
├── hadoop/bin/                 # Native Windows Hadoop layer (winutils.exe, hadoop.dll)
│
├── frontend/
│   ├── index.html              # Reactive zero-dependency live engineering UI
│   ├── dashboard.py            # Static asset & API serving router
│   └── assets/results_bundle.json # Precomputed 50k & 5M research benchmark outputs
│
├── scripts/
│   ├── run_final_experiments.py # Complete mechanism matrix benchmark runner
│   ├── run_flagship.py         # 5,000,000-event flagship evaluation runner
│   ├── analyze_results.py      # Statistical benchmark aggregator
│   └── terminal_demo.py        # Terminal live progress & metric renderer
│
├── src/
│   ├── bda/
│   │   ├── bda_runtime.py      # Genuine Spark & HDFS socket probing and sync
│   │   ├── api/server.py       # Thread-safe stdlib HTTP server & REST endpoints
│   │   ├── pipeline/           # Streaming coordinator, CSV parsing & data profiler
│   │   ├── ml/                 # Isolation Forest detector & feature extractor
│   │   ├── cmix/               # CMiX incremental window aggregation engine
│   │   ├── aloa/               # ALOA dynamic watermark & eviction controllers
│   │   ├── maso/               # MASO memory-aware state optimization manager
│   │   ├── earm/               # EARM early speculative aggregation & reconciler
│   │   └── eval/correctness.py # Mathematical tolerance verifier vs ground truth
│   ├── data/canonicalize.py    # PySpark CSV schema enforcer & Parquet builder
│   ├── streaming/              # Kafka injector & Spark SQL windowed baseline
│   └── common/                 # Configs, event dataclasses, and ground truth
│
└── tests/
    ├── test_api_and_pipeline.py# REST API & live streaming pipeline tests
    ├── test_bda_runtime.py     # Spark & HDFS infrastructure tests
    ├── test_cmix_correctness.py# Mathematical correctness validation tests
    └── verify_live_flow.py     # End-to-end multi-step live flow verification
```

---

## 🚀 Quickstart & How to Run

### 1. Prerequisites
* Python 3.12 or 3.13
* Java Runtime (JRE 17+ or JDK 24) installed and in `PATH` (for Apache Spark)
* Install dependencies:
  ```powershell
  pip install -r requirements.txt
  ```

---

### 2. Launch the Live Engineering Web Dashboard
Run the one-command server:
```powershell
python run_demo.py
```
Open your browser to:
👉 **[http://localhost:8765](http://localhost:8765)**

1. **Dataset Ingestion:** Explore the pre-loaded 2,400-event telemetry dataset or upload any custom IoT CSV.
2. **Dynamic Profiling:** Review real-time out-of-order percentage, device cardinality, and maximum lateness.
3. **Execution Modes:** Toggle between **CMiX**, **ALOA**, **MASO**, and **Full Adaptive**.
4. **Live Execution:** Observe real-time event throughput (40,000+ ev/s), watermark movement, and hot-state gauges.
5. **Exact Verification:** Verify exact correctness (`100% Exact`, 0 mismatches) and inspect the **Isolation Forest Anomaly Report**.

---

### 3. Run the Interactive Terminal Showcase
To demonstrate the complete pipeline in a terminal without a browser:
```powershell
python show_live_terminal.py
```
This runs the full 3-phase demonstration:
1. **PySpark Data Ingestion:** Reads raw CSV, enforces `(Time, DeviceId, Sensor, Value)` schema, deduplicates, and generates 48 Snappy Parquet partitions.
2. **Spark SQL Baseline:** Executes distributed 60s/10s sliding window aggregations via Spark Catalyst.
3. **Adaptive Streaming Engine:** Streams events with live terminal progress, displaying memory reduction, exact correctness verification, and ML anomaly scores.

*(Or run `python run_terminal_demo.py` for stream-only terminal mode).*

---

### 4. Execute Apache Spark Batch Jobs Individually
* **Run Spark Canonicalization (CSV ➔ Parquet):**
  ```powershell
  python src/data/canonicalize.py
  ```
* **Run Spark Windowed Ground-Truth Aggregation:**
  ```powershell
  python src/streaming/spark_baseline.py --limit 1000
  ```

---

### 5. Automated Test Suite
To verify the complete test suite across algorithms, API, Spark, and ML:
```powershell
python -m pytest -q
```
```text
...............                                                          [100%]
15 passed in 8.52s
```

---

## 🔬 Scientific Methodology & Reproducibility
* **Window Parameters:** Window Size ($W$) = 60s, Slide ($S$) = 10s.
* **Float Tolerance:** Numerical comparisons use $|x_{\text{test}} - x_{\text{gt}}| \le 10^{-6}$.
* **Hardware Portability:** Native Windows compatibility enabled via built-in `hadoop/bin/winutils.exe` & `hadoop.dll`.

---

## 📜 License
This project is licensed under the MIT License — see the [LICENSE](LICENSE) file for details.
