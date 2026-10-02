from pathlib import Path
import json
import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import col


import os
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
RAW_DATASET = Path(os.environ.get("RAW_DATASET", str(PROJECT_ROOT / "data" / "demo")))
CANONICAL_DIR = Path(os.environ.get("CANONICAL_DIR", str(PROJECT_ROOT / "data" / "canonical")))
MANIFEST_DIR = Path(os.environ.get("MANIFEST_DIR", str(PROJECT_ROOT / "data" / "manifests")))

CANONICAL_DIR.mkdir(parents=True, exist_ok=True)
MANIFEST_DIR.mkdir(parents=True, exist_ok=True)

OUTPUT_PATH = os.environ.get("OUTPUT_PATH", str(CANONICAL_DIR / "iot_events"))

EXPECTED_COLUMNS = ["Time", "DeviceId", "Sensor", "Value"]


def main() -> None:
    start = time.time()

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
        .appName("BDA-Canonical-Dataset")
        .master("local[4]")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.shuffle.partitions", "48")
        .config("spark.sql.files.maxPartitionBytes", "64m")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    print("=" * 70)
    print("BDA CANONICAL DATASET GENERATION")
    print("=" * 70)
    print(f"Input : {RAW_DATASET}")
    print(f"Output: {OUTPUT_PATH}")
    print()

    # Read only CSV files from the raw dataset.
    if RAW_DATASET.is_dir():
        csv_files = [p.resolve().as_uri() for p in RAW_DATASET.glob("*.csv")]
    else:
        csv_files = [RAW_DATASET.resolve().as_uri()]

    if not csv_files:
        raise FileNotFoundError(f"No CSV files found in {RAW_DATASET}")

    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", False)
        .csv(csv_files)
    )

    print("Input schema:")
    df.printSchema()

    time_name = "Time"
    for candidate in ["Time", "Timestamp", "timestamp", "time"]:
        if candidate in df.columns:
            time_name = candidate
            break

    from pyspark.sql.functions import expr, coalesce, unix_timestamp, to_timestamp
    df = df.select(
        coalesce(
            expr(f"try_cast({time_name} as bigint)"),
            unix_timestamp(to_timestamp(col(time_name)))
        ).alias("Time"),
        col("DeviceId").cast("string").alias("DeviceId"),
        col("Sensor").cast("string").alias("Sensor"),
        col("Value").cast("double").alias("Value"),
    )

    print("Canonical schema:")
    df.printSchema()

    raw_count = df.count()
    print(f"Raw records: {raw_count:,}")

    # Remove exact duplicate event identities.
    canonical = df.dropDuplicates(EXPECTED_COLUMNS)

    canonical_count = canonical.count()
    removed = raw_count - canonical_count

    print(f"Canonical records: {canonical_count:,}")
    print(f"Duplicate records removed: {removed:,}")

    # Final uniqueness validation.
    unique_count = canonical.select(EXPECTED_COLUMNS).distinct().count()

    if unique_count != canonical_count:
        raise RuntimeError(
            "Uniqueness validation failed: "
            f"canonical_count={canonical_count}, "
            f"unique_count={unique_count}"
        )

    print(f"Unique canonical identities: {unique_count:,}")

    # Write derived data only. Raw CSV files are never modified.
    (
        canonical
        .repartition(48)
        .write
        .mode("overwrite")
        .parquet(OUTPUT_PATH)
    )

    # Validate the written dataset by reading it back.
    written = spark.read.parquet(OUTPUT_PATH)

    written_count = written.count()
    written_unique_count = (
        written.select(EXPECTED_COLUMNS)
        .distinct()
        .count()
    )

    if written_count != canonical_count:
        raise RuntimeError(
            "Written-record validation failed: "
            f"expected={canonical_count}, actual={written_count}"
        )

    if written_unique_count != written_count:
        raise RuntimeError(
            "Written uniqueness validation failed: "
            f"records={written_count}, "
            f"unique={written_unique_count}"
        )

    elapsed = time.time() - start

    manifest = {
        "input": {
            "path": str(RAW_DATASET),
            "format": "CSV",
            "files": 3471,
            "raw_records": raw_count,
        },
        "event_identity": EXPECTED_COLUMNS,
        "output": {
            "path": OUTPUT_PATH,
            "format": "Parquet",
            "canonical_records": canonical_count,
            "unique_records": written_unique_count,
            "duplicate_records_removed": removed,
        },
        "validation": {
            "written_records": written_count,
            "written_unique_records": written_unique_count,
            "passed": True,
        },
        "runtime_seconds": round(elapsed, 2),
    }

    manifest_path = MANIFEST_DIR / "canonical_dataset_manifest.json"

    with manifest_path.open("w") as f:
        json.dump(manifest, f, indent=2)

    print()
    print("=" * 70)
    print("CANONICAL DATASET COMPLETE")
    print("=" * 70)
    print(f"Raw records              : {raw_count:,}")
    print(f"Canonical records        : {canonical_count:,}")
    print(f"Duplicates removed       : {removed:,}")
    print(f"Written records          : {written_count:,}")
    print(f"Written unique records   : {written_unique_count:,}")
    print(f"Manifest                 : {manifest_path}")
    print(f"Elapsed                  : {elapsed / 60:.2f} minutes")
    print("=" * 70)

    spark.stop()


if __name__ == "__main__":
    main()