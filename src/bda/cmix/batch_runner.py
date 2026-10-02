import argparse
import json
import time
from pathlib import Path

from pyspark.sql import SparkSession

from src.bda.cmix.processor import CMiXProcessor


import os
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent.parent
HDFS_SOURCE = os.environ.get("HDFS_SOURCE", "hdfs://localhost:9000/bda/canonical/iot_events")
RESULT_DIR = Path(os.environ.get("RESULT_DIR", str(PROJECT_ROOT / "results" / "processing")))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=1000)
    args = parser.parse_args()

    spark = (
        SparkSession.builder
        .appName("BDA-CMiX-Batch")
        .master("local[4]")
        .config("spark.driver.memory", "4g")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    start = time.time()

    print("=" * 70)
    print("BDA CMiX BATCH PROCESSOR")
    print("=" * 70)
    print(f"Input limit : {args.limit}")

    df = (
        spark.read
        .parquet(HDFS_SOURCE)
        .select("Time", "DeviceId", "Sensor", "Value")
        .orderBy(
            "Time",
            "DeviceId",
            "Sensor",
            "Value",
        )
        .limit(args.limit)
    )

    rows = df.collect()

    print(f"Loaded events: {len(rows):,}")

    processor = CMiXProcessor(
        window_size_seconds=60,
        slide_seconds=10,
    )

    for row in rows:
        processor.process_event(
            int(row["Time"]),
            row["DeviceId"],
            row["Sensor"],
            float(row["Value"]),
        )

    results = processor.results()

    output = RESULT_DIR / "cmix_baseline"
    output.mkdir(parents=True, exist_ok=True)

    output_file = output / "results.json"

    with output_file.open("w") as f:
        json.dump(results, f, indent=2)

    elapsed = time.time() - start

    metadata = {
        "algorithm": "CMiX",
        "input_limit": args.limit,
        "result_rows": len(results),
        "window_size_seconds": 60,
        "slide_seconds": 10,
        "elapsed_seconds": elapsed,
    }

    with (RESULT_DIR / "cmix_baseline_metadata.json").open("w") as f:
        json.dump(metadata, f, indent=2)

    print()
    print("=" * 70)
    print("CMiX BATCH COMPLETE")
    print("=" * 70)
    print(f"Input records : {len(rows):,}")
    print(f"Result rows   : {len(results):,}")
    print(f"Output        : {output_file}")
    print(f"Elapsed       : {elapsed:.2f} seconds")
    print("=" * 70)

    spark.stop()


if __name__ == "__main__":
    main()
