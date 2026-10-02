"""
Flagship high-volume run: 5,000,000 canonical events through the
integrated pipeline (Kafka inject -> unified runner), cross-checked
against the independent Spark baseline at the same slice.

Mechanisms run: cmix (ground truth), full (ALOA+MASO+EARM).
"""

import json
import os
import subprocess
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.bda.eval.correctness import (  # noqa: E402
    compare_streaming,
)

LIMIT = 5_000_000
SCENARIO = "heavy_ooo"
SEED = "20260826"
BUFFER_SIZE = 20

RESULTS_DIR = PROJECT_ROOT / "results" / "final" / "flagship_5m"

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

KAFKA_BOOTSTRAP = "localhost:9092"
KAFKA_TOPIC = "bda-iot-events"


def spark_env():
    return {
        "JAVA_HOME": os.environ.get(
            "JAVA_HOME", "/usr/lib/jvm/java-17-openjdk-amd64"
        ),
        "PYSPARK_PYTHON": str(VENV_PYTHON),
        "PYSPARK_DRIVER_PYTHON": str(VENV_PYTHON),
    }


def run(cmd, label, env=None, timeout=7200):
    print(f"\n[{label}]")
    print(f"$ {' '.join(cmd)}")
    merged = dict(os.environ)
    if env:
        merged.update(env)

    result = subprocess.run(
        cmd,
        cwd=str(PROJECT_ROOT),
        env=merged,
        capture_output=True,
        text=True,
        timeout=timeout,
    )

    if result.stdout:
        print(result.stdout[-3000:])
    if result.stderr:
        print(result.stderr[-3000:], file=sys.stderr)
    if result.returncode != 0:
        raise RuntimeError(
            f"{label} failed with code {result.returncode}"
        )
    return result


def reset_topic():
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
    )
    import time

    time.sleep(2)

    run(
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
        timeout=120,
    )


def inject():
    run(
        [
            str(SPARK_SUBMIT),
            "--master",
            "local[4]",
            "--driver-memory",
            "6g",
            "src/streaming/kafka_injector.py",
            "--scenario",
            SCENARIO,
            "--limit",
            str(LIMIT),
            "--seed",
            SEED,
"--buffer-size",
            str(BUFFER_SIZE),
            "--streaming",
        ],
        f"inject {SCENARIO} limit={LIMIT}",
        env=spark_env(),
        timeout=7200,
    )


def run_mechanism(name, mechanism, baseline_peak=None):
    cmd = [
        str(VENV_PYTHON),
        "-m",
        "src.bda.cmix.runner",
        "--expected",
        str(LIMIT),
        "--output-rows",
        str(RESULTS_DIR / f"{name}_rows.json"),
        "--metrics",
        str(RESULTS_DIR / f"{name}_metrics.json"),
        "--emitted",
        str(RESULTS_DIR / f"{name}_emitted.jsonl"),
        "--mechanism",
        mechanism,
        "--scenario",
        SCENARIO,
        "--event-count",
        str(LIMIT),
        "--buffer-size",
        str(BUFFER_SIZE),
    ]

    if baseline_peak is not None:
        cmd += ["--baseline-peak-hot", str(baseline_peak)]

    run(cmd, f"run {name}", timeout=7200)

    return (
        RESULTS_DIR / f"{name}_rows.json",
        RESULTS_DIR / f"{name}_metrics.json",
    )


def spark_check():
    run(
        [
            str(SPARK_SUBMIT),
            "--master",
            "local[4]",
            "--driver-memory",
            "6g",
            "src/streaming/spark_baseline.py",
            "--limit",
            str(LIMIT),
        ],
        f"spark baseline limit={LIMIT}",
        env=spark_env(),
        timeout=7200,
    )

    import pandas as pd

    parquet_dir = (
        PROJECT_ROOT
        / "results"
        / "processing"
        / "spark_baseline"
    )

    frame = pd.read_parquet(str(parquet_dir))
    frame["start"] = (
        frame["start"].astype("int64") // 10**9
    ).astype("int64")
    frame["end"] = (
        frame["end"].astype("int64") // 10**9
    ).astype("int64")

    out = RESULTS_DIR / f"{SCENARIO}_spark_baseline.json"
    with open(out, "w") as f:
        json.dump(frame.to_dict(orient="records"), f)

    return out


def main():
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)

    reset_topic()
    inject()

    cmix_rows, cmix_metrics = run_mechanism("cmix", "cmix")

    baseline_peak = json.loads(cmix_metrics.read_text())[
        "state"
    ]["peak_hot_entries"]

    full_rows, full_metrics = run_mechanism(
        "full", "full", baseline_peak=baseline_peak
    )

    # Independent ground truth at the same 5M slice.
    spark_path = spark_check()

    for name, rows_path in [
        ("cmix", cmix_rows),
        ("full", full_rows),
    ]:
        if name == "cmix":
            report = compare_streaming(
                rows_path, spark_path
            )
            report["comparison_type"] = (
                "cmix_vs_spark_baseline"
            )
            print(
                f"[crosscheck] cmix vs spark: "
                f"{'EXACT' if report['exact'] else 'MISMATCH'} "
                f"(missing={report['missing_in_result']} "
                f"extra={report['extra_in_result']} "
                f"mismatch={report['field_mismatches']})"
            )
        else:
            gt = cmix_rows
            report = compare_streaming(gt, rows_path)
            report["comparison_type"] = (
                "mechanism_vs_ground_truth"
            )
            print(
                f"[verify] full vs cmix ground truth: "
                f"{'EXACT' if report['exact'] else 'MISMATCH'} "
                f"(missing={report['missing_in_result']} "
                f"extra={report['extra_in_result']} "
                f"mismatch={report['field_mismatches']})"
            )

        (RESULTS_DIR / f"{name}_comparison.json").write_text(
            json.dumps(report, indent=2)
        )

    print("\nFLAGSHIP COMPLETE")
    print(f"Results: {RESULTS_DIR}")


if __name__ == "__main__":
    main()