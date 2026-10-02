"""
Correctness comparison for the BDA OOO experiment matrix.

Compares every mechanism's aggregated result rows against the ground
truth and produces both per-comparison verdicts and a summary table.
"""

import json
from pathlib import Path

AGG_FIELDS = ["count", "sum", "avg", "min", "max"]


def load_rows(path):
    data = json.loads(Path(path).read_text())

    if isinstance(data, dict) and "rows" in data:
        data = data["rows"]

    return data


def row_key(row):
    return (
        int(row["start"]),
        str(row["DeviceId"]),
        str(row["Sensor"]),
    )


def _close(a, b, tolerance):
    return abs(float(a) - float(b)) <= tolerance


def compare_rows(ground_truth_rows, result_rows, tolerance=1e-6):
    gt = {row_key(r): r for r in ground_truth_rows}
    out = {row_key(r): r for r in result_rows}

    all_keys = set(gt) | set(out)

    missing = sorted(set(gt) - set(out))
    extra = sorted(set(out) - set(gt))

    field_mismatches = []
    field_mismatch_counts = {c: 0 for c in AGG_FIELDS}

    for key in sorted(set(gt) & set(out)):
        expected = gt[key]
        actual = out[key]

        for field in AGG_FIELDS:
            if not _close(expected[field], actual[field], tolerance):
                field_mismatch_counts[field] += 1
                field_mismatches.append(
                    {
                        "key": list(key),
                        "field": field,
                        "expected": expected[field],
                        "actual": actual[field],
                        "abs_diff": abs(
                            float(expected[field])
                            - float(actual[field])
                        ),
                    }
                )

    correct = (
        not missing and not extra and not field_mismatches
    )

    return {
        "ground_truth_rows": len(gt),
        "result_rows": len(out),
        "matched_keys": len(set(gt) & set(out)),
        "missing_in_result": len(missing),
        "missing_keys": missing[:50],
        "extra_in_result": len(extra),
        "extra_keys": extra[:50],
        "field_mismatches": len(field_mismatches),
        "field_mismatch_counts": field_mismatch_counts,
        "mismatch_samples": field_mismatches[:50],
        "exact": correct,
        "tolerance": tolerance,
    }


def compare_files(ground_truth_path, result_path, tags=None):
    gt = load_rows(ground_truth_path)
    res = load_rows(result_path)

    report = compare_rows(gt, res)
    report["ground_truth_file"] = str(ground_truth_path)
    report["result_file"] = str(result_path)

    if tags:
        report.update(tags)

    return report


def iter_json_array(path):
    """Yield the top-level objects of a JSON array without loading
    the whole file into memory (single-pass, bounded RAM).

    Rows are expected as a JSON array (compact or pretty-printed).
    """
    import mmap

    with open(path, "rb") as f:
        with mmap.mmap(
            f.fileno(), 0, access=mmap.ACCESS_READ
        ) as buf:
            size = len(buf)
            pos = 0

            while pos < size and buf[pos] in b" \t\r\n":
                pos += 1

            if pos >= size or buf[pos : pos + 1] != b"[":
                raise ValueError(f"Not a JSON array: {path}")

            pos += 1

            decoder = json.JSONDecoder()

            while True:
                while pos < size and buf[pos] in b" \t\r\n,":
                    pos += 1

                if pos >= size or buf[pos : pos + 1] == b"]":
                    return

                window = buf[pos : pos + 8192]
                obj, end = decoder.raw_decode(
                    window.decode("utf-8")
                )
                yield obj
                pos += end


def compare_streaming(
    ground_truth_path, result_path, tolerance=1e-6, tags=None
):
    """Memory-bounded variant of compare_files for very large row
    sets (millions of rows).
    """
    gt = {}

    for record in iter_json_array(ground_truth_path):
        gt[row_key(record)] = tuple(
            record[field] for field in AGG_FIELDS
        )

    matched = 0
    field_mismatch_counts = {c: 0 for c in AGG_FIELDS}
    field_mismatches = []
    extra_keys = []

    for record in iter_json_array(result_path):
        key = row_key(record)

        if key not in gt:
            extra_keys.append(key)
            continue

        matched += 1

        for i, field in enumerate(AGG_FIELDS):
            if not _close(record[field], gt[key][i], tolerance):
                field_mismatch_counts[field] += 1

                if len(field_mismatches) < 50:
                    field_mismatches.append(
                        {
                            "key": list(key),
                            "field": field,
                            "expected": gt[key][i],
                            "actual": record[field],
                            "abs_diff": abs(
                                float(record[field])
                                - float(gt[key][i])
                            ),
                        }
                    )

        del gt[key]

    missing_keys = list(gt.keys())

    extra = len(extra_keys)
    missing = len(missing_keys)

    correct = (
        missing == 0 and extra == 0 and not field_mismatches
    )

    report = {
        "ground_truth_rows": matched + missing,
        "result_rows": matched + extra,
        "matched_keys": matched,
        "missing_in_result": missing,
        "missing_keys": missing_keys[:50],
        "extra_in_result": extra,
        "extra_keys": extra_keys[:50],
        "field_mismatches": len(field_mismatches),
        "field_mismatch_counts": field_mismatch_counts,
        "mismatch_samples": field_mismatches,
        "exact": correct,
        "tolerance": tolerance,
        "ground_truth_file": str(ground_truth_path),
        "result_file": str(result_path),
    }

    if tags:
        report.update(tags)

    return report


def summarize(results_dir, scenario, mechanisms):
    rows = []

    for mechanism in mechanisms:
        report_path = (
            Path(results_dir)
            / scenario
            / f"{mechanism}_metrics.json"
        )

        if not report_path.exists():
            continue

        report = json.loads(report_path.read_text())

        row = {
            "scenario": scenario,
            "mechanism": mechanism,
            "result_rows": report.get("result_rows", 0),
            "exact": report.get("correctness", {}).get("exact", False),
            "missing": report.get("correctness", {}).get(
                "missing_in_result", 0
            ),
            "extra": report.get("correctness", {}).get(
                "extra_in_result", 0
            ),
            "field_mismatches": report.get("correctness", {}).get(
                "field_mismatches", 0
            ),
            "peak_hot": report.get("state", {}).get(
                "peak_hot_entries", 0
            ),
            "final_hot": report.get("state", {}).get(
                "final_hot_entries", 0
            ),
            "final_archive": report.get("state", {}).get(
                "final_archive_entries", 0
            ),
            "evicted": report.get("state", {}).get(
                "evicted_state_entries", 0
            ),
            "frozen": report.get("state", {}).get(
                "frozen_emissions", 0
            ),
            "reconciled": report.get("state", {}).get(
                "reconciled_events", 0
            ),
            "late_after_finalization": report.get("state", {}).get(
                "late_after_finalization", 0
            ),
            "late_after_eviction": report.get("state", {}).get(
                "late_after_eviction", 0
            ),
            "events_per_second": report.get("runtime", {}).get(
                "events_per_second", 0
            ),
            "peak_rss_kb": report.get("runtime", {}).get(
                "peak_rss_kb", 0
            ),
            "peak_hot_reduction_percent": report.get(
                "state_reduction", {}
            ).get("peak_hot_reduction_percent", 0),
        }

        rows.append(row)

    return rows


def print_summary_table(rows):
    if not rows:
        print("no rows")
        return

    header = [
        "scenario",
        "mechanism",
        "result_rows",
        "exact",
        "missing",
        "extra",
        "mismatch",
        "peak_hot",
        "final_hot",
        "archive",
        "evicted",
        "frozen",
        "reconciled",
        "late_aft_fin",
        "late_aft_ev",
        "events/s",
        "peak_rss_kb",
        "reduction%",
    ]

    widths = {}

    for col in header:
        widths[col] = len(col)

    records = []

    for row in rows:
        record = [
            str(row["scenario"]),
            str(row["mechanism"]),
            f"{row['result_rows']:,}",
            str(bool(row["exact"])),
            f"{row['missing']:,}",
            f"{row['extra']:,}",
            f"{row['field_mismatches']:,}",
            f"{row['peak_hot']:,}",
            f"{row['final_hot']:,}",
            f"{row['final_archive']:,}",
            f"{row['evicted']:,}",
            f"{row['frozen']:,}",
            f"{row['reconciled']:,}",
            f"{row['late_after_finalization']:,}",
            f"{row['late_after_eviction']:,}",
            f"{row['events_per_second']:,.0f}",
            f"{row['peak_rss_kb']:,}",
            f"{row['peak_hot_reduction_percent']:.2f}",
        ]
        records.append(record)

        for col, value in zip(header, record):
            widths[col] = max(widths[col], len(value))

    print(
        " | ".join(
            col.ljust(widths[col]) for col in header
        )
    )
    print("-+-".join("-" * widths[col] for col in header))

    for record in records:
        print(
            " | ".join(
                col.ljust(widths[label])
                for label, col in zip(header, record)
            )
        )


def main():
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--gt", required=True)
    parser.add_argument("--result", required=True)
    parser.add_argument("--scenario", default="")
    parser.add_argument("--mechanism", default="")

    args = parser.parse_args()

    report = compare_files(
        args.gt,
        args.result,
        tags={
            "scenario": args.scenario,
            "mechanism": args.mechanism,
        },
    )

    print(json.dumps(report, indent=2))

    if not report["exact"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()