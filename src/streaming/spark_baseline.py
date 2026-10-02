import argparse
import time
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    count,
    max as spark_max,
    min as spark_min,
    sum as spark_sum,
    from_unixtime,
    col,
    window,
)


import os

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
HDFS_SOURCE = os.environ.get("HDFS_SOURCE", "hdfs://localhost:9000/bda/canonical/iot_events")
RESULT_DIR = Path(os.environ.get("RESULT_DIR", str(PROJECT_ROOT / "results" / "processing")))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=1000)
    args = parser.parse_args()

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

    spark = (
        SparkSession.builder
        .appName("BDA-Spark-Baseline")
        .master("local[4]")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.shuffle.partitions", "48")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    start = time.time()

    print("=" * 70)
    print("BDA SPARK BASELINE")
    print("=" * 70)

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
        .orderBy(
            "Time",
            "DeviceId",
            "Sensor",
            "Value",
        )
        .limit(args.limit)
    )

    input_count = df.count()

    print(f"Input records: {input_count:,}")

    df = df.withColumn(
        "event_timestamp",
        from_unixtime(col("Time")).cast("timestamp")
    )

    result = (
        df
        .groupBy(
            window(
                "event_timestamp",
                "60 seconds",
                "10 seconds",
            ),
            "DeviceId",
            "Sensor",
        )
        .agg(
            count("*").alias("count"),
            spark_sum("Value").alias("sum"),
            avg("Value").alias("avg"),
            spark_min("Value").alias("min"),
            spark_max("Value").alias("max"),
        )
        .select(
            "window.start",
            "window.end",
            "DeviceId",
            "Sensor",
            "count",
            "sum",
            "avg",
            "min",
            "max",
        )
        .orderBy(
            "start",
            "DeviceId",
            "Sensor",
        )
    )

    output = RESULT_DIR / "spark_baseline"

    (
        result
        .write
        .mode("overwrite")
        .parquet(str(output))
    )

    result_count = result.count()

    elapsed = time.time() - start

    print()
    print("=" * 70)
    print("SPARK BASELINE COMPLETE")
    print("=" * 70)
    print(f"Input records : {input_count:,}")
    print(f"Result rows   : {result_count:,}")
    print(f"Output        : {output}")
    print(f"Elapsed       : {elapsed:.2f} seconds")
    print("=" * 70)

    spark.stop()


if __name__ == "__main__":
    main()
