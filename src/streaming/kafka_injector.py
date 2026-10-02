import argparse
import json
import random
import time
from pathlib import Path

from kafka import KafkaProducer
from pyspark.sql import SparkSession


import os
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
SCENARIO_FILE = Path(os.environ.get("SCENARIO_FILE", str(PROJECT_ROOT / "data" / "workloads" / "scenarios.json")))

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "bda-iot-events")
HDFS_SOURCE = os.environ.get("HDFS_SOURCE", "hdfs://localhost:9000/bda/canonical/iot_events")


def load_scenario(name):
    with SCENARIO_FILE.open() as f:
        config = json.load(f)

    if name not in config["scenarios"]:
        raise ValueError(
            f"Unknown scenario: {name}"
        )

    return config["scenarios"][name]


def create_spark():
    import sys
    if "PYSPARK_PYTHON" not in os.environ:
        os.environ["PYSPARK_PYTHON"] = sys.executable
    if "PYSPARK_DRIVER_PYTHON" not in os.environ:
        os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    if "HADOOP_HOME" not in os.environ:
        local_hadoop = PROJECT_ROOT / "hadoop"
        if (local_hadoop / "bin" / "winutils.exe").exists():
            os.environ["HADOOP_HOME"] = str(local_hadoop)
            os.environ["PATH"] = str(local_hadoop / "bin") + os.pathsep + os.environ.get("PATH", "")

    return (
        SparkSession.builder
        .appName("BDA-Kafka-Injector")
        .master("local[4]")
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )


def create_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        acks="all",
        linger_ms=200,
        batch_size=65536,
        max_in_flight_requests_per_connection=5,
        value_serializer=lambda value: json.dumps(
            value,
            separators=(",", ":")
        ).encode("utf-8"),
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
        default=100,
    )

    parser.add_argument(
        "--seed",
        type=int,
        default=20260826,
    )

    parser.add_argument(
        "--buffer-size",
        type=int,
        default=20,
    )

    parser.add_argument(
        "--streaming",
        action="store_true",
        help="Iterate partitions incrementally instead of "
        "collecting the whole slice to the driver.",
    )

    args = parser.parse_args()

    scenario = load_scenario(args.scenario)
    rng = random.Random(args.seed)

    print("=" * 70)
    print("BDA OOO KAFKA INJECTOR")
    print("=" * 70)
    print(f"Scenario       : {args.scenario}")
    print(f"Limit          : {args.limit}")
    print(f"Buffer size    : {args.buffer_size}")
    print(f"Disorder prob. : {scenario['disorder_probability']}")
    print(f"Max delay      : {scenario['max_delay_seconds']} sec")
    print("=" * 70)

    spark = create_spark()
    spark.sparkContext.setLogLevel("WARN")

    producer = create_producer()

    try:
        # Read event-time ordered data.
        local_source = str(PROJECT_ROOT / "data" / "canonical" / "iot_events")
        try:
            raw_df = spark.read.parquet(HDFS_SOURCE)
        except Exception as e:
            print(f"HDFS source '{HDFS_SOURCE}' unavailable ({e}), using local canonical store: {local_source}")
            raw_df = spark.read.parquet(local_source)

        df = (
            raw_df
            .select(
                "Time",
                "DeviceId",
                "Sensor",
                "Value",
            )
            # Deterministic source selection.
            #
            # Time alone is not sufficient because many IoT events
            # share the same timestamp. The additional fields make
            # the selected N-event dataset reproducible across runs.
            .orderBy(
                "Time",
                "DeviceId",
                "Sensor",
                "Value",
            )
            .limit(args.limit)
        )

        if args.streaming:
            rows = df.rdd.toLocalIterator(prefetchPartitions=False)
        else:
            rows = df.collect()
            print(f"Loaded events: {len(rows):,}")

        buffer = []
        arrival_sequence = 0
        published = 0
        disorder_events = 0

        def publish(row):
            nonlocal arrival_sequence
            nonlocal published
            nonlocal disorder_events

            event_time = int(row["Time"])

            event = {
                "event_id": (
                    f"{event_time}-"
                    f"{row['DeviceId']}-"
                    f"{row['Sensor']}-"
                    f"{arrival_sequence}"
                ),
                "event_time": event_time,
                "device_id": row["DeviceId"],
                "sensor": row["Sensor"],
                "value": float(row["Value"]),
                "arrival_sequence": arrival_sequence,
                "scenario": args.scenario,
            }

            producer.send(
                KAFKA_TOPIC,
                value=event,
            )

            arrival_sequence += 1
            published += 1

        for row in rows:

            buffer.append(row)

            if len(buffer) < args.buffer_size:
                continue

            # Baseline preserves event-time order.
            if args.scenario == "baseline":
                index = 0

            else:
                # With the configured probability, select a later
                # event from the buffer before the earliest event.
                if rng.random() < scenario["disorder_probability"]:
                    index = rng.randrange(1, len(buffer))
                    disorder_events += 1
                else:
                    index = 0

            row_to_publish = buffer.pop(index)

            publish(row_to_publish)

        # Flush remaining buffered events.
        while buffer:
            if args.scenario == "baseline":
                index = 0
            else:
                index = rng.randrange(len(buffer))

            row_to_publish = buffer.pop(index)
            publish(row_to_publish)

        producer.flush()

        print()
        print("=" * 70)
        print("INJECTION COMPLETE")
        print("=" * 70)
        print(f"Published events : {published:,}")
        print(f"Disorder releases: {disorder_events:,}")
        print(f"Scenario         : {args.scenario}")
        print("=" * 70)

    finally:
        producer.close()
        spark.stop()


if __name__ == "__main__":
    main()
