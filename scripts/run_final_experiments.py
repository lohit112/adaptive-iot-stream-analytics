"""
Final experiment orchestrator for the BDA OOO streaming research.

For every scenario this pipeline:
    1. resets the Kafka topic,
    2. injects the deterministic scenario workload (Spark, HDFS canonical),
    3. runs ground truth (cmix mechanism via the unified runner) and the
       configured mechanism matrix,
    4. cross-checks runner ground truth against the independent Spark
       baseline,
    5. compares every mechanism's result rows against ground truth,
    6. writes per-run metrics + a comparison summary (JSON and CSV).

Usage:
    python scripts/run_final_experiments.py \
        --limit 50000 \
        --spark-check \
        --results-dir results/final

Environment:
    SPARK_HOME   (spark-submit with bundled pyspark)
    venv python  (.venv/bin/python with kafka)
    PYTHONPATH   must include the project root
"""

import argparse
import csv
import json
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.bda.eval.correctness import (  # noqa: E402
    compare_files,
    compare_rows,
    load_rows,
)

KAFKA_BOOTSTRAP = "localhost:9092"
KAFKA_TOPIC = "bda-iot-events"
SCENARIO_FILE = PROJECT_ROOT / "data" / "workloads" / "scenarios.json"
SEED = "20260826"
BUFFER_SIZE = 20

VENV_PYTHON = Path(sys.executable)
SPARK_SUBMIT = Path(
    os.environ.get("SPARK_SUBMIT") or (
        str(Path(os.environ.get("SPARK_HOME", "")) / "bin" / ("spark-submit.cmd" if sys.platform == "win32" else "spark-submit"))
        if os.environ.get("SPARK_HOME") else "spark-submit"
    )
)
KAFKA_TOPICS = os.environ.get(
    "KAFKA_BIN",
    "kafka-topics.bat" if sys.platform == "win32" else "kafka-topics.sh",
)
HADOOP_HDFS = os.environ.get(
    "HDFS_BIN",
    "hdfs.cmd" if sys.platform == "win32" else "hdfs",
)

SCENARIOS = [
    "baseline",
    "mild_ooo",
    "moderate_ooo",
    "heavy_ooo",
    "burst_ooo",
]

MECHANISMS = {
    "cmix": {"mechanism": "cmix"},
    "fixed2": {"mechanism": "fixed", "fixed_lateness": 2},
    "fixed5": {"mechanism": "fixed", "fixed_lateness": 5},
    "fixed10": {"mechanism": "fixed", "fixed_lateness": 10},
    "aloa": {"mechanism": "aloa"},
    "maso": {"mechanism": "maso"},
    "full": {
        "mechanism": "full",
        "earm_guard_policy": "adaptive",
    },
    "earm_agg": {
        "mechanism": "earm_agg",
        "earm_guard_policy": "fixed",
        "earm_guard_seconds": 4,
    },
}

ABLATION_MAP = {
    "none": "cmix",
    "aloa": "aloa",
    "maso": "maso",
    "full": "full",
}


PYSPARK_PYTHON = str(VENV_PYTHON)


def spark_env():
    env = {
        "JAVA_HOME": os.environ.get("JAVA_HOME", ""),
        "PYSPARK_PYTHON": PYSPARK_PYTHON,
        "PYSPARK_DRIVER_PYTHON": PYSPARK_PYTHON,
    }
    return env


def run_shell(cmd, label, env=None, timeout=1800):
    print(f"\n[{label}]")
    print(f"$ {' '.join(cmd)}")

    merged_env = dict(os.environ)
    if env:
        merged_env.update(env)

    result = subprocess.run(
        cmd,
        cwd=str(PROJECT_ROOT),
        env=merged_env,
        capture_output=True,
        text=True,
        timeout=timeout,
    )

    if result.stdout:
        print(result.stdout[-4000:])

    if result.stderr:
        print(result.stderr[-4000:], file=sys.stderr)

    if result.returncode != 0:
        raise RuntimeError(
            f"{label} failed with exit code {result.returncode}"
        )

    return result


def reset_topic():
    env = {"JAVA_HOME": os.environ.get("JAVA_HOME", "")}

    print("[kafka] clearing topic", KAFKA_TOPIC)

    subprocess.run(
        [
            KAFKA_TOPICS,
            "--bootstrap-server",
            KAFKA_BOOTSTRAP,
            "--delete",
            "--topic",
            KAFKA_TOPIC,
        ],
        capture_output=True,
        text=True,
        env=env,
    )

    time.sleep(2)

    run_shell(
        [
            KAFKA_TOPICS,
            "--bootstrap-server",
            KAFKA_BOOTSTRAP,
            "--create",
            "--topic",
            KAFKA_TOPIC,
            "--partitions",
            "1",
            "--replication-factor",
            "1",
        ],
        "kafka create topic",
        env=env,
        timeout=120,
    )


def inject(scenario, limit, seed):
    run_shell(
        [
            str(SPARK_SUBMIT),
            "--master",
            "local[4]",
            "--driver-memory",
            "4g",
            "src/streaming/kafka_injector.py",
            "--scenario",
            scenario,
            "--limit",
            str(limit),
            "--seed",
            str(seed),
            "--buffer-size",
            str(BUFFER_SIZE),
        ],
        f"inject scenario={scenario} limit={limit}",
        env=spark_env(),
        timeout=2400,
    )


def run_mechanism(name, config, scenario, limit, results_dir, baseline_peak_hot):
    mechanism = config["mechanism"]

    prefix = results_dir / name
    prefix.parent.mkdir(parents=True, exist_ok=True)

    output_rows = prefix.with_name(f"{name}_rows.json")
    metrics = prefix.with_name(f"{name}_metrics.json")
    emitted = prefix.with_name(f"{name}_emitted.jsonl")

    cmd = [
        str(VENV_PYTHON),
        "-m",
        "src.bda.cmix.runner",
        "--expected",
        str(limit),
        "--output-rows",
        str(output_rows),
        "--metrics",
        str(metrics),
        "--emitted",
        str(emitted),
        "--mechanism",
        mechanism,
        "--scenario",
        scenario,
        "--event-count",
        str(limit),
        "--buffer-size",
        str(BUFFER_SIZE),
    ]

    if config.get("fixed_lateness") is not None:
        cmd += ["--fixed-lateness", str(config["fixed_lateness"])]

    if config.get("earm_guard_policy"):
        cmd += ["--earm-guard-policy", config["earm_guard_policy"]]

    if config.get("earm_guard_seconds") is not None:
        cmd += [
            "--earm-guard-seconds",
            str(config["earm_guard_seconds"]),
        ]

    if baseline_peak_hot is not None:
        cmd += [
            "--baseline-peak-hot",
            str(baseline_peak_hot),
        ]

    run_shell(
        cmd,
        f"run mechanism={name} scenario={scenario}",
        env={"PYTHONPATH": str(PROJECT_ROOT)},
        timeout=3600,
    )

    return output_rows, metrics


def spark_check(limit, scenario, results_dir):
    """
    Independent ground truth via Spark SQL over the canonical HDFS
    source, selecting the same deterministic N events.
    """
    baseline_dir = results_dir / scenario / "spark_baseline"

    run_shell(
        [
            str(SPARK_SUBMIT),
            "--master",
            "local[4]",
            "--driver-memory",
            "4g",
            "src/streaming/spark_baseline.py",
            "--limit",
            str(limit),
        ],
        f"spark baseline scenario={scenario}",
        env=spark_env(),
        timeout=3600,
    )

    parquet_dir = (
        PROJECT_ROOT
        / "results"
        / "processing"
        / "spark_baseline"
    )

    import pandas as pd

    frame = pd.read_parquet(str(parquet_dir))

    frame["start"] = (
        frame["start"].astype("int64") // 10**9
    ).astype("int64")
    frame["end"] = (
        frame["end"].astype("int64") // 10**9
    ).astype("int64")

    rows = frame.to_dict(orient="records")

    out = baseline_dir.with_name(f"{scenario}_spark_baseline.json")

    with open(out, "w") as f:
        json.dump(rows, f)

    return out


def baseline_peak_from(report):
    if report is None:
        return None

    return report.get("state", {}).get("peak_hot_entries")


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument("--limit", type=int, default=50000)
    parser.add_argument(
        "--scenarios",
        nargs="+",
        default=SCENARIOS,
    )
    parser.add_argument(
        "--mechanisms",
        nargs="+",
        default=list(MECHANISMS),
    )
    parser.add_argument(
        "--results-dir",
        default="results/final",
    )
    parser.add_argument(
        "--seed",
        default=SEED,
    )
    parser.add_argument(
        "--spark-check",
        action="store_true",
        help="cross-check runner ground truth vs Spark baseline",
    )
    parser.add_argument(
        "--topic-reset",
        action="store_true",
        default=True,
        help="delete and recreate the Kafka topic per scenario",
    )
    parser.add_argument(
        "--no-topic-reset",
        dest="topic_reset",
        action="store_false",
    )

    args = parser.parse_args()

    results_dir = PROJECT_ROOT / args.results_dir
    results_dir.mkdir(parents=True, exist_ok=True)

    invalid = [m for m in args.mechanisms if m not in MECHANISMS]

    if invalid:
        raise SystemExit(
            f"unknown mechanisms: {invalid}; "
            f"valid: {sorted(MECHANISMS)}"
        )

    summary = {"scenarios": {}, "comparison": []}

    for scenario in args.scenarios:
        scenario_dir = results_dir / scenario
        scenario_dir.mkdir(parents=True, exist_ok=True)

        print("\n" + "=" * 80)
        print(f"SCENARIO: {scenario}")
        print("=" * 80)

        if args.topic_reset:
            reset_topic()

        inject(scenario, args.limit, args.seed)

        if args.spark_check:
            try:
                _ = spark_check(args.limit, scenario, results_dir)
            except Exception as error:
                print(
                    f"[warn] spark baseline failed: {error}",
                    file=sys.stderr,
                )

        ground_truth_rows = None

        for name in args.mechanisms:
            config = MECHANISMS[name]
            mechanism = config["mechanism"]

            if name == "cmix":
                baseline_peak = None
            else:
                gt_path = (
                    scenario_dir
                    / "cmix_metrics.json"
                )
                baseline_peak = baseline_peak_from(
                    json.loads(
                        gt_path.read_text()
                    )
                    if gt_path.exists()
                    else None
                )

            output_rows, metrics_path = run_mechanism(
                name,
                config,
                scenario,
                args.limit,
                scenario_dir,
                baseline_peak,
            )

            if name == "cmix":
                ground_truth_rows = output_rows

        if ground_truth_rows is None:
            raise RuntimeError("cmix ground truth was not run")

        spark_gt_path = (
            scenario_dir
            / f"{scenario}_spark_baseline.json"
        )

        scenario_entry = {
            "ground_truth_rows": str(ground_truth_rows),
            "spark_baseline": str(spark_gt_path)
            if spark_gt_path.exists()
            else None,
            "mechanisms": {},
        }

        spark_report = None

        if spark_gt_path.exists():
            spark_report = compare_files(
                ground_truth_rows,
                spark_gt_path,
            )
            scenario_entry["spark_crosscheck"] = spark_report

            spark_report["scenario"] = scenario
            spark_report["comparison_type"] = (
                "runner_ground_truth_vs_spark_baseline"
            )
            summary["comparison"].append(spark_report)

            print(
                "\nRunner ground truth vs Spark baseline: "
                f"{'EXACT' if spark_report['exact'] else 'MISMATCH'}"
            )

        for name in args.mechanisms:
            output_rows = (
                scenario_dir
                / f"{name}_rows.json"
            )
            metrics_path = (
                scenario_dir
                / f"{name}_metrics.json"
            )

            if not output_rows.exists():
                continue

            if name == "cmix":
                continue

            compare = compare_files(
                ground_truth_rows,
                output_rows,
                tags={"scenario": scenario, "mechanism": name},
            )

            comparison_path = (
                scenario_dir
                / f"{name}_comparison.json"
            )

            with open(comparison_path, "w") as f:
                json.dump(compare, f, indent=2)

            scenario_entry["mechanisms"][name] = compare

            metrics = json.loads(metrics_path.read_text())
            metrics["correctness"] = compare

            with open(metrics_path, "w") as f:
                json.dump(metrics, f, indent=2)

            compare["scenario"] = scenario
            compare["mechanism"] = name
            compare["comparison_type"] = (
                "mechanism_vs_ground_truth"
            )
            summary["comparison"].append(compare)

            print(
                f"[verify] {name:>8} vs ground truth: "
                f"{'EXACT' if compare['exact'] else 'MISMATCH'}"
                f"  (rows={compare['result_rows']:,} "
                f"missing={compare['missing_in_result']} "
                f"extra={compare['extra_in_result']} "
                f"mismatch={compare['field_mismatches']})"
            )

        summary["scenarios"][scenario] = scenario_entry

    summary_path = results_dir / "comparison_summary.json"

    with open(summary_path, "w") as f:
        json.dump(summary, f, indent=2)

    _write_csv(results_dir, summary)

    print("\n" + "=" * 80)
    print("EXPERIMENTS COMPLETE")
    print(f"Results: {results_dir}")
    print(f"Summary JSON: {summary_path}")
    print("=" * 80)


def _write_csv(results_dir, summary):
    header = [
        "scenario",
        "comparison_type",
        "mechanism",
        "exact",
        "ground_truth_rows",
        "result_rows",
        "missing_in_result",
        "extra_in_result",
        "field_mismatches",
        "peak_hot",
        "final_hot",
        "final_archive",
        "frozen",
        "reconciled",
        "late_after_finalization",
        "late_after_eviction",
        "events_per_second",
        "peak_rss_kb",
        "peak_hot_reduction_percent",
    ]

    rows = []

    for item in summary["comparison"]:
        scenario = item.get("scenario", "")
        mechanism = item.get("mechanism", "")

        if item.get("comparison_type") == "runner_ground_truth_vs_spark_baseline":
            metrics = {}
        else:
            metrics = _load_metrics(results_dir, scenario, mechanism)

        rows.append(
            [
                scenario,
                item.get("comparison_type", ""),
                mechanism,
                str(bool(item.get("exact", False))),
                item.get("ground_truth_rows", 0),
                item.get("result_rows", 0),
                item.get("missing_in_result", 0),
                item.get("extra_in_result", 0),
                item.get("field_mismatches", 0),
                metrics.get("state", {}).get("peak_hot_entries", ""),
                metrics.get("state", {}).get("final_hot_entries", ""),
                metrics.get("state", {}).get(
                    "final_archive_entries", ""
                ),
                metrics.get("state", {}).get("frozen_emissions", ""),
                metrics.get("state", {}).get("reconciled_events", ""),
                metrics.get("state", {}).get(
                    "late_after_finalization", ""
                ),
                metrics.get("state", {}).get(
                    "late_after_eviction", ""
                ),
                metrics.get("runtime", {}).get(
                    "events_per_second", ""
                ),
                metrics.get("runtime", {}).get("peak_rss_kb", ""),
                metrics.get("state_reduction", {}).get(
                    "peak_hot_reduction_percent", ""
                ),
            ]
        )

    output = results_dir / "comparison_summary.csv"

    with open(output, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(rows)


def _load_metrics(results_dir, scenario, mechanism):
    path = (
        results_dir
        / scenario
        / f"{mechanism}_metrics.json"
    )

    if not path.exists():
        return {}

    return json.loads(path.read_text())


if __name__ == "__main__":
    main()