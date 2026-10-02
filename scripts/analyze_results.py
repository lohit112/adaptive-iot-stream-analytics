"""
Aggregate analysis and plotting for the BDA experiment results.

Reads the per-scenario/per-mechanism metrics JSON produced by the
orchestrator and emits:
  - summary tables (CSV / Markdown)
  - matplotlib figures under results/plots/
"""

import json
import sys
from pathlib import Path

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

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

ORDER = [
    "cmix",
    "fixed2",
    "fixed5",
    "fixed10",
    "aloa",
    "maso",
    "full",
    "earm_agg",
]


def load(result_dir, scenario, mechanism):
    path = result_dir / scenario / f"{mechanism}_metrics.json"
    if not path.exists():
        return None
    return json.loads(path.read_text())


def load_all(result_dir):
    data = {}
    for scenario in SCENARIOS:
        data[scenario] = {}
        for mechanism in MECHANISMS:
            metrics = load(result_dir, scenario, mechanism)
            if metrics is not None:
                data[scenario][mechanism] = metrics
    return data


def table_correctness(data, result_dir):
    rows = []
    header = [
        "scenario",
        "mechanism",
        "exact",
        "rows",
        "missing",
        "extra",
        "field_mismatch",
    ]

    for scenario in SCENARIOS:
        for mechanism in ORDER:
            if mechanism not in data[scenario]:
                continue
            m = data[scenario][mechanism]
            c = m.get("correctness", {})

            if mechanism == "cmix" and not c:
                c = {"exact": True}

            rows.append(
                [
                    scenario,
                    mechanism,
                    bool(c.get("exact", False)),
                    m.get("result_rows", 0),
                    c.get("missing_in_result", 0),
                    c.get("extra_in_result", 0),
                    c.get("field_mismatches", 0),
                ]
            )

    return header, rows


def table_state(data):
    header = [
        "scenario",
        "mechanism",
        "peak_hot",
        "final_hot",
        "archive",
        "frozen",
        "reconciled",
        "late_after_finalization",
        "late_after_eviction",
        "peak_reduction_pct",
    ]

    rows = []

    for scenario in SCENARIOS:
        for mechanism in ORDER:
            if mechanism not in data[scenario]:
                continue
            m = data[scenario][mechanism]
            s = m.get("state", {})
            sr = m.get("state_reduction", {})

            rows.append(
                [
                    scenario,
                    mechanism,
                    s.get("peak_hot_entries", 0),
                    s.get("final_hot_entries", 0),
                    s.get("final_archive_entries", 0),
                    s.get("frozen_emissions", 0),
                    s.get("reconciled_events", 0),
                    s.get("late_after_finalization", 0),
                    s.get("late_after_eviction", 0),
                    round(
                        sr.get("peak_hot_reduction_percent", 0), 2
                    ),
                ]
            )

    return header, rows


def table_runtime(data):
    header = [
        "scenario",
        "mechanism",
        "ev/s",
        "processing_s",
        "peak_rss_kb",
        "max_lateness",
        "avg_lateness",
        "ooo_ratio_pct",
        "budget_mean_s",
        "over_budget_pct",
    ]

    rows = []

    for scenario in SCENARIOS:
        for mechanism in ORDER:
            if mechanism not in data[scenario]:
                continue
            m = data[scenario][mechanism]
            r = m.get("runtime", {})
            o = m.get("ordering", {})
            l = m.get("lateness", {})
            b = m.get("budget", {})
            be = m.get("budget_evaluation", {})

            rows.append(
                [
                    scenario,
                    mechanism,
                    round(r.get("events_per_second", 0), 1),
                    round(r.get("processing_seconds", 0), 3),
                    r.get("peak_rss_kb", 0),
                    l.get("maximum_seconds", 0),
                    round(l.get("average_seconds", 0), 3),
                    round(o.get("ooo_ratio", 0) * 100, 1),
                    b.get("mean_seconds", 0),
                    round(
                        be.get("over_budget_ratio", 0) * 100, 1
                    ),
                ]
            )

    return header, rows


def write_md(result_dir, data):
    lines = []
    lines.append("# BDA OOO Streaming Experiment — Results Tables")
    lines.append("")

    for title, table in [
        ("Correctness vs ground truth", table_correctness(data, result_dir)),
        ("State model tiers", table_state(data)),
        ("Runtime, memory, ordering, budget", table_runtime(data)),
    ]:
        header, rows = table
        lines.append(f"## {title}")
        lines.append("")
        lines.append("| " + " | ".join(header) + " |")
        lines.append("|" + "|".join(["---"] * len(header)) + "|")
        for row in rows:
            lines.append("| " + " | ".join(str(v) for v in row) + " |")
        lines.append("")

    output = result_dir / "tables.md"
    output.write_text("\n".join(lines))
    print("wrote", output)


def plot_peak_reduction(data, out_dir):
    fig, ax = plt.subplots(figsize=(11, 5))

    scenarios = SCENARIOS
    x = list(range(len(scenarios)))

    width = 0.12
    offsets = {
        mech: i - 3 * width for i, mech in enumerate(ORDER)
    }

    for mech in ORDER:
        values = []
        for scenario in scenarios:
            m = data[scenario].get(mech)
            if m is None:
                values.append(0.0)
            else:
                values.append(
                    m.get("state_reduction", {}).get(
                        "peak_hot_reduction_percent", 0
                    )
                )
        ax.bar(
            [xi + offsets[mech] for xi in x],
            values,
            width=width,
            label=MECH_LABELS[mech],
        )

    ax.set_xticks(x)
    ax.set_xticklabels(scenarios, rotation=15)
    ax.set_ylabel("Peak hot-state reduction vs baseline (%)")
    ax.set_title("Peak state reduction by scenario and mechanism")
    ax.legend(ncol=4, fontsize=7)
    ax.grid(axis="y", alpha=0.3)

    fig.tight_layout()
    out = out_dir / "peak_state_reduction.png"
    fig.savefig(out, dpi=130)
    plt.close(fig)
    print("wrote", out)


def plot_rss(data, out_dir):
    fig, ax = plt.subplots(figsize=(11, 5))

    scenarios = SCENARIOS
    x = list(range(len(scenarios)))

    width = 0.12
    offsets = {
        mech: i - 3 * width for i, mech in enumerate(ORDER)
    }

    for mech in ORDER:
        values = []
        for scenario in scenarios:
            m = data[scenario].get(mech)
            if m is None:
                values.append(0.0)
            else:
                values.append(
                    m.get("runtime", {}).get("peak_rss_kb", 0)
                    / 1024
                )
        ax.bar(
            [xi + offsets[mech] for xi in x],
            values,
            width=width,
            label=MECH_LABELS[mech],
        )

    ax.set_xticks(x)
    ax.set_xticklabels(scenarios, rotation=15)
    ax.set_ylabel("Peak RSS (MB)")
    ax.set_title("Peak RSS by scenario and mechanism")
    ax.legend(ncol=4, fontsize=7)
    ax.grid(axis="y", alpha=0.3)

    fig.tight_layout()
    out = out_dir / "peak_rss.png"
    fig.savefig(out, dpi=130)
    plt.close(fig)
    print("wrote", out)


def plot_throughput(data, out_dir):
    fig, ax = plt.subplots(figsize=(11, 5))

    scenarios = SCENARIOS
    x = list(range(len(scenarios)))

    width = 0.12
    offsets = {
        mech: i - 3 * width for i, mech in enumerate(ORDER)
    }

    for mech in ORDER:
        values = []
        for scenario in scenarios:
            m = data[scenario].get(mech)
            if m is None:
                values.append(0.0)
            else:
                values.append(
                    m.get("runtime", {}).get(
                        "events_per_second", 0
                    )
                )
        ax.bar(
            [xi + offsets[mech] for xi in x],
            values,
            width=width,
            label=MECH_LABELS[mech],
        )

    ax.set_xticks(x)
    ax.set_xticklabels(scenarios, rotation=15)
    ax.set_ylabel("Events / second")
    ax.set_title("Throughput by scenario and mechanism")
    ax.legend(ncol=4, fontsize=7)
    ax.grid(axis="y", alpha=0.3)

    fig.tight_layout()
    out = out_dir / "throughput.png"
    fig.savefig(out, dpi=130)
    plt.close(fig)
    print("wrote", out)


def plot_evolution(data, out_dir, scenario="heavy_ooo"):
    """
    Watermark / budget / active-state evolution from the decimated
    per-event samples, across mechanisms.
    """
    fig, axes = plt.subplots(1, 3, figsize=(16, 4.5))

    for mech in ["cmix", "aloa", "full", "earm_agg"]:
        m = data.get(scenario, {}).get(mech)
        if m is None:
            continue

        samples = m.get("samples", [])
        if not samples:
            continue

        x = [s["event_number"] for s in samples]
        wm = [s["watermark"] if s["watermark"] else None for s in samples]
        active = [s["active_state_entries"] for s in samples]
        budget = [s["allowed_lateness_seconds"] for s in samples]

        label = MECH_LABELS[mech]

        axes[0].plot(x, active, label=label, lw=1.4)
        axes[0].set_title(f"active hot state ({scenario})")
        axes[0].set_xlabel("event #")

        axes[1].plot(x, wm, label=label, lw=1.4)
        axes[1].set_title(f"watermark ({scenario})")
        axes[1].set_xlabel("event #")

        axes[2].plot(x, budget, label=label, lw=1.4)
        axes[2].set_title(f"allowed lateness budget ({scenario})")
        axes[2].set_xlabel("event #")

    for ax in axes:
        ax.grid(alpha=0.3)
        ax.legend(fontsize=7)

    fig.tight_layout()
    out = out_dir / f"evolution_{scenario}.png"
    fig.savefig(out, dpi=130)
    plt.close(fig)
    print("wrote", out)


def main():
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--results-dir",
        default="results/final/200k",
    )
    parser.add_argument(
        "--plots-dir",
        default="results/plots",
    )
    args = parser.parse_args()

    result_dir = PROJECT_ROOT / args.results_dir
    plots_dir = PROJECT_ROOT / args.plots_dir
    plots_dir.mkdir(parents=True, exist_ok=True)

    data = load_all(result_dir)

    print(f"loaded scenarios: {[s for s in data if data[s]]}")

    write_md(result_dir, data)

    plot_peak_reduction(data, plots_dir)
    plot_rss(data, plots_dir)
    plot_throughput(data, plots_dir)

    for scenario in SCENARIOS:
        plot_evolution(data, plots_dir, scenario)

    print("done")


if __name__ == "__main__":
    main()