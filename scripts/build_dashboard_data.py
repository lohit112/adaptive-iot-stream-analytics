"""Minimal static data snapshots served to the researcher dashboard.

Reads the experiment metrics/plots and writes a compact JSON bundle the
frontend can load without a backend.
"""

import json
import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

RESULTS_DIRS = [
    PROJECT_ROOT / "results" / "final" / "50k",
    PROJECT_ROOT / "results" / "final" / "200k",
    PROJECT_ROOT / "results" / "final" / "flagship_5m",
]

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

MECH_LABELS = {
    "cmix": "CMiX (baseline)",
    "fixed2": "Fixed 2s",
    "fixed5": "Fixed 5s",
    "fixed10": "Fixed 10s",
    "aloa": "ALOA",
    "maso": "ALOA+MASO",
    "full": "Full (ALOA+MASO+EARM)",
    "earm_agg": "EARM aggressive",
}


def build_result_set(results_dir):
    if not results_dir.exists():
        return None

    bundle = {"dir": str(results_dir), "scenarios": {}}

    top_level = json.loads(
        (results_dir / "full_metrics.json").read_text()
    ) if (results_dir / "full_metrics.json").exists() else None

    scenario_dirs = [
        d for d in SCENARIOS if (results_dir / d).exists()
    ]

    if not scenario_dirs and top_level is not None:
        scenario_dirs = ["heavy_ooo"]

    for scenario in scenario_dirs:
        scenario_dir = (
            results_dir if scenario == "heavy_ooo" and (
                results_dir / "full_metrics.json"
            ).exists() and not (results_dir / scenario).exists()
            else results_dir / scenario
        )

        if not scenario_dir.exists():
            continue

        entry = {"mechanisms": {}}

        spark_path = scenario_dir / f"{scenario}_spark_baseline.json"
        if spark_path.exists():
            entry["spark_baseline"] = True

        for mechanism in MECHANISMS:
            metrics_path = scenario_dir / (
                f"{mechanism}_metrics.json"
            )
            if not metrics_path.exists():
                continue

            metrics = json.loads(metrics_path.read_text())
            comparison = None

            comparison_name = (
                "full_vs_cmix"
                if mechanism == "full"
                and (scenario_dir / "full_vs_cmix_comparison.json").exists()
                else f"{mechanism}_comparison.json"
            )

            comparison_path = scenario_dir / comparison_name
            if comparison_path.exists():
                comparison = json.loads(
                    comparison_path.read_text()
                )

            entry["mechanisms"][mechanism] = {
                "state": metrics.get("state", {}),
                "state_reduction": metrics.get(
                    "state_reduction", {}
                ),
                "runtime": metrics.get("runtime", {}),
                "ordering": metrics.get("ordering", {}),
                "lateness": metrics.get("lateness", {}),
                "budget": metrics.get("budget", {}),
                "budget_evaluation": metrics.get(
                    "budget_evaluation", {}
                ),
                "aloa": metrics.get("aloa"),
                "maso": metrics.get("maso"),
                "earm": metrics.get("earm"),
                "correctness": comparison
                or metrics.get("correctness"),
                "samples": metrics.get("samples", [])[::20],
                "events_received": metrics.get(
                    "events_received", 0
                ),
                "result_rows": metrics.get("result_rows", 0),
                "label": MECH_LABELS.get(
                    mechanism, mechanism
                ),
            }

        bundle["scenarios"][scenario] = entry

    return bundle


def main():
    output = {}

    for result_dir in RESULTS_DIRS:
        bundle = build_result_set(result_dir)
        if bundle and bundle["scenarios"]:
            output[str(result_dir.relative_to(PROJECT_ROOT))] = (
                bundle
            )

    output["generated_at"] = time.strftime("%Y-%m-%d %H:%M:%S")

    target = (
        PROJECT_ROOT
        / "frontend"
        / "assets"
        / "results_bundle.json"
    )
    target.parent.mkdir(parents=True, exist_ok=True)

    target.write_text(json.dumps(output, indent=1))

    print("wrote", target)


if __name__ == "__main__":
    main()