import argparse
import json
import subprocess
import time
from pathlib import Path


import os
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[3]
RESULT_DIR = PROJECT_ROOT / "results" / "processing" / "cmix_benchmarks"
INJECTOR = PROJECT_ROOT / "src" / "streaming" / "kafka_injector.py"
PROCESSOR = PROJECT_ROOT / "src" / "bda" / "cmix" / "kafka_processor.py"

KAFKA = Path(os.environ.get("KAFKA_BIN", "kafka/bin"))
TOPIC = "bda-iot-events"


def run(cmd):
    print()
    print("RUNNING:")
    print(" ".join(map(str, cmd)))
    print()

    start = time.time()

    completed = subprocess.run(
        cmd,
        cwd=PROJECT_ROOT,
        text=True,
    )

    elapsed = time.time() - start

    if completed.returncode != 0:
        raise RuntimeError(
            f"Command failed with exit code {completed.returncode}"
        )

    return elapsed


def recreate_topic():
    subprocess.run(
        [
            str(KAFKA / "kafka-topics.sh"),
            "--bootstrap-server",
            "localhost:9092",
            "--delete",
            "--topic",
            TOPIC,
        ],
        cwd=PROJECT_ROOT,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )

    time.sleep(2)

    run(
        [
            str(KAFKA / "kafka-topics.sh"),
            "--bootstrap-server",
            "localhost:9092",
            "--create",
            "--topic",
            TOPIC,
            "--partitions",
            "1",
            "--replication-factor",
            "1",
        ]
    )


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--scenario",
        required=True,
        choices=[
            "baseline",
            "mild_ooo",
            "moderate_ooo",
            "heavy_ooo",
            "burst_ooo",
        ],
    )

    parser.add_argument(
        "--limit",
        type=int,
        required=True,
    )

    parser.add_argument(
        "--buffer-size",
        type=int,
        default=20,
    )

    args = parser.parse_args()

    RESULT_DIR.mkdir(parents=True, exist_ok=True)

    print("=" * 70)
    print("BDA CMiX BENCHMARK")
    print("=" * 70)
    print(f"Scenario    : {args.scenario}")
    print(f"Input limit : {args.limit:,}")
    print(f"Buffer size : {args.buffer_size}")
    print("=" * 70)

    recreate_topic()

    start = time.time()

    injector_time = run(
        [
            os.environ.get(
                "SPARK_SUBMIT",
                str(Path(os.environ.get("SPARK_HOME", "")) / "bin" / ("spark-submit.cmd" if sys.platform == "win32" else "spark-submit"))
                if os.environ.get("SPARK_HOME") else "spark-submit"
            ),
            "--master",
            "local[4]",
            "--driver-memory",
            "2g",
            str(INJECTOR),
            "--scenario",
            args.scenario,
            "--limit",
            str(args.limit),
            "--buffer-size",
            str(args.buffer_size),
        ]
    )

    output = (
        RESULT_DIR
        / f"{args.scenario}_{args.limit}.json"
    )

    processor_time = run(
        [
            "python",
            str(PROCESSOR),
            "--expected",
            str(args.limit),
            "--output",
            str(output),
        ]
    )

    total_time = time.time() - start

    metrics = {
        "algorithm": "CMiX",
        "scenario": args.scenario,
        "input_records": args.limit,
        "buffer_size": args.buffer_size,
        "injector_elapsed_seconds": injector_time,
        "processor_elapsed_seconds": processor_time,
        "end_to_end_elapsed_seconds": total_time,
        "input_throughput_events_per_second": (
            args.limit / total_time
            if total_time > 0
            else 0
        ),
    }

    metrics_path = (
        RESULT_DIR
        / f"{args.scenario}_{args.limit}_metrics.json"
    )

    with metrics_path.open("w") as f:
        json.dump(metrics, f, indent=2)

    print()
    print("=" * 70)
    print("CMiX BENCHMARK COMPLETE")
    print("=" * 70)
    print(f"Scenario       : {args.scenario}")
    print(f"Input records  : {args.limit:,}")
    print(f"Total time     : {total_time:.3f} sec")
    print(
        "Throughput     : "
        f"{metrics['input_throughput_events_per_second']:.2f} events/sec"
    )
    print(f"Metrics        : {metrics_path}")
    print("=" * 70)


if __name__ == "__main__":
    main()
