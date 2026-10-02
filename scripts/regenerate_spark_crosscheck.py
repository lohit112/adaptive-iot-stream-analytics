"""
Regenerate runner-ground-truth vs Spark-baseline cross-checks using
already-computed cmix rows, and rebuild the comparison summary.

Used to repair the Spark timestamp unit conversion without re-running
the full mechanism matrix.
"""

import csv
import json
import os
import subprocess
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.bda.eval.correctness import compare_files  # noqa: E402

RESULTS_DIR = PROJECT_ROOT / "results" / "final" / "50k"
SPARK_SUBMIT = Path(
    os.environ.get("SPARK_SUBMIT") or (
        str(Path(os.environ.get("SPARK_HOME", "")) / "bin" / ("spark-submit.cmd" if sys.platform == "win32" else "spark-submit"))
        if os.environ.get("SPARK_HOME") else "spark-submit"
    )
)
VENV_PYTHON = Path(sys.executable)

SCENARIOS = [
    "baseline",
    "mild_ooo",
    "moderate_ooo",
    "heavy_ooo",
    "burst_ooo",
]

MECHANISMS = [
    "cmix",
    "fixed2",
    "fixed5",
    "fixed10",
    "aloa",
    "maso",
    "full",
    "earm_agg",
]


def spark_env():
    return {
        "JAVA_HOME": os.environ.get("JAVA_HOME", ""),
        "PYSPARK_PYTHON": str(VENV_PYTHON),
        "PYSPARK_DRIVER_PYTHON": str(VENV_PYTHON),
    }


def run_spark_baseline(limit):
    subprocess.run(
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
        cwd=str(PROJECT_ROOT),
        env=spark_env(),
        capture_output=True,
        text=True,
        check=True,
    )


def read_spark_json(parquet_dir):
    import pandas as pd

    frame = pd.read_parquet(str(parquet_dir))

    frame["start"] = (
        frame["start"].astype("int64") // 10**9
    ).astype("int64")
    frame["end"] = (
        frame["end"].astype("int64") // 10**9
    ).astype("int64")

    return frame.to_dict(orient="records")


def rebuild_summary():
    summary = {"scenarios": {}, "comparison": []}

    for scenario in SCENARIOS:
        scenario_dir = RESULTS_DIR / scenario

        entry = {
            "ground_truth_rows": (
                str(scenario_dir / "cmix_rows.json")
            ),
            "spark_baseline": (
                str(scenario_dir / f"{scenario}_spark_baseline.json")
                if (scenario_dir / f"{scenario}_spark_baseline.json")
                .exists()
                else None
            ),
            "mechanisms": {},
        }

        cross = scenario_dir / f"{scenario}_spark_crosscheck.json"

        if cross.exists():
            report = json.loads(cross.read_text())
            report["scenario"] = scenario
            report["comparison_type"] = (
                "runner_ground_truth_vs_spark_baseline"
            )
            entry["spark_crosscheck"] = report
            summary["comparison"].append(report)

        for mech in MECHANISMS:
            comparison_path = (
                scenario_dir / f"{mech}_comparison.json"
            )

            if not comparison_path.exists():
                continue

            report = json.loads(comparison_path.read_text())
            entry["mechanisms"][mech] = report

            report["scenario"] = scenario
            report["mechanism"] = mech
            report["comparison_type"] = (
                "mechanism_vs_ground_truth"
            )
            summary["comparison"].append(report)

        summary["scenarios"][scenario] = entry

    summary_path = RESULTS_DIR / "comparison_summary.json"

    with open(summary_path, "w") as f:
        json.dump(summary, f, indent=2)

    write_csv(summary, RESULTS_DIR / "comparison_summary.csv")

    print("summary rebuilt:", summary_path)


def write_csv(summary, output):
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

        metrics = {}

        if item.get("comparison_type") == (
            "mechanism_vs_ground_truth"
        ):
            metrics_path = (
                RESULTS_DIR
                / scenario
                / f"{mechanism}_metrics.json"
            )
            if metrics_path.exists():
                metrics = json.loads(metrics_path.read_text())

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
                metrics.get("state", {}).get("late_after_eviction", ""),
                metrics.get("runtime", {}).get(
                    "events_per_second", ""
                ),
                metrics.get("runtime", {}).get("peak_rss_kb", ""),
                metrics.get("state_reduction", {}).get(
                    "peak_hot_reduction_percent", ""
                ),
            ]
        )

    with open(output, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(rows)


def main():
    limit = 50000

    parquet_dir = (
        PROJECT_ROOT
        / "results"
        / "processing"
        / "spark_baseline"
    )

    for scenario in SCENARIOS:
        print(f"\nspark baseline: {scenario}")

        run_spark_baseline(limit)

        rows = read_spark_json(parquet_dir)

        out = RESULTS_DIR / scenario / f"{scenario}_spark_baseline.json"

        with open(out, "w") as f:
            json.dump(rows, f)

        cmix_rows = RESULTS_DIR / scenario / "cmix_rows.json"

        cross = compare_files(
            cmix_rows,
            out,
        )

        cross_path = (
            RESULTS_DIR
            / scenario
            / f"{scenario}_spark_crosscheck.json"
        )

        with open(cross_path, "w") as f:
            json.dump(cross, f, indent=2)

        print(
            f"  runner vs spark: "
            f"{'EXACT' if cross['exact'] else 'MISMATCH'} "
            f"(missing={cross['missing_in_result']} "
            f"extra={cross['extra_in_result']} "
            f"mismatch={cross['field_mismatches']})"
        )

    rebuild_summary()


if __name__ == "__main__":
    main()