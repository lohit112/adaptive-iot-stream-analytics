import argparse
import json
import random
from pathlib import Path

from src.bda.aloa.controller import ALOAController


import os
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[3]
SCENARIO_FILE = PROJECT_ROOT / "data/workloads/scenarios.json"


def load_scenario(name):
    with SCENARIO_FILE.open() as f:
        config = json.load(f)

    return config["scenarios"][name]


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument("--scenario", required=True)
    parser.add_argument("--limit", type=int, default=10000)
    parser.add_argument("--buffer-size", type=int, default=20)
    parser.add_argument("--seed", type=int, default=20260826)

    args = parser.parse_args()

    scenario = load_scenario(args.scenario)
    rng = random.Random(args.seed)

    # Use the same deterministic source ordering as kafka_injector.py.
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder
        .appName("BDA-ALOA-Replay")
        .master("local[4]")
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    hdfs_uri = os.environ.get("HDFS_CANONICAL_URI", "hdfs://localhost:9000/bda/canonical/iot_events")
    local_path = str(PROJECT_ROOT / "data" / "canonical" / "iot_events")
    try:
        df = spark.read.parquet(hdfs_uri)
    except Exception:
        df = spark.read.parquet(local_path)
        .select(
            "Time",
            "DeviceId",
            "Sensor",
            "Value",
        )
        .orderBy(
            "Time",
            "DeviceId",
            "Sensor",
            "Value",
        )
        .limit(args.limit)
    )

    rows = df.collect()

    controller = ALOAController()

    buffer = []

    published = 0
    disorder_releases = 0
    max_lateness = 0.0

    budget_min = None
    budget_max = None

    print("=" * 70)
    print("ALOA REAL WORKLOAD REPLAY")
    print("=" * 70)
    print(f"Scenario       : {args.scenario}")
    print(f"Input records  : {len(rows):,}")
    print(f"Buffer size    : {args.buffer_size}")
    print(f"Disorder prob. : {scenario['disorder_probability']}")
    print(f"Max delay      : {scenario['max_delay_seconds']} sec")
    print("=" * 70)

    def observe(row):
        nonlocal published
        nonlocal max_lateness
        nonlocal budget_min
        nonlocal budget_max

        observation = controller.observe_event(
            event_time=int(row["Time"]),
            state_size=0,
            memory_utilization=0.0,
        )

        published += 1

        max_lateness = max(
            max_lateness,
            observation.lateness_seconds,
        )

        budget = observation.allowed_lateness_seconds

        if budget_min is None:
            budget_min = budget
            budget_max = budget
        else:
            budget_min = min(budget_min, budget)
            budget_max = max(budget_max, budget)

    for row in rows:
        buffer.append(row)

        if len(buffer) < args.buffer_size:
            continue

        if args.scenario == "baseline":
            index = 0
        else:
            if rng.random() < scenario["disorder_probability"]:
                index = rng.randrange(1, len(buffer))
                disorder_releases += 1
            else:
                index = 0

        row = buffer.pop(index)

        observe(row)

    while buffer:
        if args.scenario == "baseline":
            index = 0
        else:
            index = rng.randrange(len(buffer))

        row = buffer.pop(index)

        observe(row)

    print()
    print("=" * 70)
    print("ALOA REPLAY COMPLETE")
    print("=" * 70)
    print(f"Events observed       : {published:,}")
    print(f"OOO events             : {controller.ooo_events:,}")
    print(f"OOO ratio              : {controller.ooo_ratio:.4f}")
    print(f"Maximum observed late  : {max_lateness:.2f} sec")
    print(f"ALOA minimum budget    : {budget_min} sec")
    print(f"ALOA maximum budget    : {budget_max} sec")
    print(f"Final ALOA budget      : {controller.policy.allowed_lateness_seconds} sec")
    print("=" * 70)

    spark.stop()


if __name__ == "__main__":
    main()
