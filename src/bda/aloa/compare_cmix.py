import argparse
import json
import subprocess
import time
from pathlib import Path


import os
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[3]
KAFKA_BIN = Path(os.environ.get("KAFKA_BIN", "kafka/bin"))

TOPIC = "bda-iot-events"

CMIX_PROCESSOR = (
    PROJECT_ROOT / "src/bda/cmix/kafka_processor.py"
)

ALOA_PROCESSOR = (
    PROJECT_ROOT / "src/bda/aloa/cmix_integrated.py"
)

INJECTOR = (
    PROJECT_ROOT / "src/streaming/kafka_injector.py"
)

RESULT_DIR = (
    PROJECT_ROOT / "results/processing/aloa_comparison"
)


def run(cmd):
    start = time.time()

    completed = subprocess.run(
        cmd,
        cwd=PROJECT_ROOT,
        text=True,
    )

    elapsed = time.time() - start

    if completed.returncode != 0:
        raise RuntimeError(
            f"Command failed: {completed.returncode}"
        )

    return elapsed


def recreate_topic():
    subprocess.run(
        [
            str(KAFKA_BIN / "kafka-topics.sh"),
            "--bootstrap-server",
            "localhost:9092",
            "--delete",
            "--topic",
            TOPIC,
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )

    time.sleep(2)

    run(
        [
            str(KAFKA_BIN / "kafka-topics.sh"),
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


def inject(scenario, limit, buffer_size):
    return run(
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
            scenario,
            "--limit",
            str(limit),
            "--buffer-size",
            str(buffer_size),
        ]
    )


def run_processor(processor, limit, output, aloa_log=None):
    cmd = [
        "python",
        str(processor),
        "--expected",
        str(limit),
        "--output",
        str(output),
    ]

    if aloa_log is not None:
        cmd.extend(
            [
                "--aloa-log",
                str(aloa_log),
            ]
        )

    return run(cmd)


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
        default=10000,
    )

    parser.add_argument(
        "--buffer-size",
        type=int,
        default=20,
    )

    args = parser.parse_args()

    RESULT_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    print("=" * 70)
    print("CMiX vs ALOA + CMiX QUANTITATIVE COMPARISON")
    print("=" * 70)
    print(f"Scenario    : {args.scenario}")
    print(f"Input       : {args.limit:,}")
    print(f"Buffer      : {args.buffer_size}")
    print("=" * 70)

    # --------------------------------------------------------------
    # CMiX
    # --------------------------------------------------------------

    print()
    print(">>> PHASE 1: CMiX")
    print()

    recreate_topic()

    injection_time = inject(
        args.scenario,
        args.limit,
        args.buffer_size,
    )

    cmix_output = (
        RESULT_DIR
        / f"{args.scenario}_{args.limit}_cmix.json"
    )

    cmix_time = run_processor(
        CMIX_PROCESSOR,
        args.limit,
        cmix_output,
    )

    # --------------------------------------------------------------
    # ALOA + CMiX
    # --------------------------------------------------------------

    print()
    print(">>> PHASE 2: ALOA + CMiX")
    print()

    recreate_topic()

    injection_time_2 = inject(
        args.scenario,
        args.limit,
        args.buffer_size,
    )

    integrated_output = (
        RESULT_DIR
        / f"{args.scenario}_{args.limit}_aloa_cmix.json"
    )

    aloa_log = (
        RESULT_DIR
        / f"{args.scenario}_{args.limit}_aloa.json"
    )

    integrated_time = run_processor(
        ALOA_PROCESSOR,
        args.limit,
        integrated_output,
        aloa_log,
    )

    # --------------------------------------------------------------
    # Metrics
    # --------------------------------------------------------------

    cmix_total = injection_time + cmix_time
    integrated_total = injection_time_2 + integrated_time

    metrics = {
        "scenario": args.scenario,
        "input_records": args.limit,
        "buffer_size": args.buffer_size,

        "cmix": {
            "injection_seconds": injection_time,
            "processing_seconds": cmix_time,
            "end_to_end_seconds": cmix_total,
            "throughput_events_per_second": (
                args.limit / cmix_total
                if cmix_total > 0
                else 0
            ),
        },

        "aloa_cmix": {
            "injection_seconds": injection_time_2,
            "processing_seconds": integrated_time,
            "end_to_end_seconds": integrated_total,
            "throughput_events_per_second": (
                args.limit / integrated_total
                if integrated_total > 0
                else 0
            ),
        },
    }

    with open(aloa_log) as f:
        aloa_data = json.load(f)

    # --------------------------------------------------------------
    # ALOA metrics
    #
    # Current ALOA logs store metrics in nested sections.
    # --------------------------------------------------------------

    ordering = aloa_data.get("ordering", {})
    budget = aloa_data.get("budget", {})

    metrics["aloa"] = {
        "ooo_events": ordering.get(
            "ooo_events",
            aloa_data.get("ooo_events", 0),
        ),
        "ooo_ratio": ordering.get(
            "ooo_ratio",
            aloa_data.get("ooo_ratio", 0.0),
        ),
        "final_budget_seconds": budget.get(
            "final_seconds",
            aloa_data.get("final_budget_seconds", 0),
        ),
    }

    # Preserve adaptive-state metrics when available.
    correctness = aloa_data.get(
        "correctness_mode",
        aloa_data.get("correctness", {}),
    )

    state = aloa_data.get("state", {})

    metrics["aloa_cmix_state"] = {
        "maximum_active_entries": state.get(
            "maximum_entries", 0
        ),
        "final_active_entries": state.get(
            "final_entries", 0
        ),
        "average_active_entries": state.get(
            "average_entries", 0
        ),
        "all_events_processed": correctness.get(
            "all_events_processed_by_cmix",
            True,
        ),
        "events_discarded": correctness.get(
            "over_budget_events_discarded",
            False,
        ),
    }

    metrics["comparison"] = {
        "end_to_end_change_percent": (
            (
                integrated_total - cmix_total
            )
            / cmix_total
            * 100
        ),
        "processing_change_percent": (
            (
                integrated_time - cmix_time
            )
            / cmix_time
            * 100
        ),
        "throughput_change_percent": (
            (
                metrics["aloa_cmix"][
                    "throughput_events_per_second"
                ]
                - metrics["cmix"][
                    "throughput_events_per_second"
                ]
            )
            / metrics["cmix"][
                "throughput_events_per_second"
            ]
            * 100
        ),
    }

    metrics_path = (
        RESULT_DIR
        / f"{args.scenario}_{args.limit}_comparison.json"
    )

    with open(metrics_path, "w") as f:
        json.dump(
            metrics,
            f,
            indent=2,
        )

    print()
    print("=" * 70)
    print("QUANTITATIVE COMPARISON COMPLETE")
    print("=" * 70)

    print(
        f"CMiX end-to-end       : "
        f"{cmix_total:.3f}s"
    )

    print(
        f"ALOA + CMiX end-to-end: "
        f"{integrated_total:.3f}s"
    )

    print(
        f"CMiX throughput       : "
        f"{metrics['cmix']['throughput_events_per_second']:.2f} events/s"
    )

    print(
        f"ALOA + CMiX throughput: "
        f"{metrics['aloa_cmix']['throughput_events_per_second']:.2f} events/s"
    )

    print(
        f"ALOA final budget     : "
        f"{metrics['aloa']['final_budget_seconds']}s"
    )

    print(
        f"Processing change     : "
        f"{metrics['comparison']['processing_change_percent']:.2f}%"
    )

    print(
        f"Throughput change     : "
        f"{metrics['comparison']['throughput_change_percent']:.2f}%"
    )

    print(f"Metrics               : {metrics_path}")
    print("=" * 70)


if __name__ == "__main__":
    main()
