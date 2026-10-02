"""
Unified streaming-run engine for the final BDA experiment matrix.

Every configuration (ground truth, CMiX baseline, fixed-lateness
baselines, ALOA, MASO, EARM, full pipeline) is executed through this
same engine so that instrumentation, timing, memory and RSS measurement
are identical across configurations.
"""

import json
import os
import threading
import time

from kafka import KafkaConsumer

from src.bda.aloa.controller import ALOAController
from src.bda.cmix.processor import CMiXProcessor
from src.bda.earm.controller import EARMController
from src.bda.maso.controller import MASOController

KAFKA_BOOTSTRAP = "localhost:9092"
KAFKA_TOPIC = "bda-iot-events"
WINDOW_SIZE_SECONDS = 60
SLIDE_SECONDS = 10


class RSSMonitor:
    """
    Lightweight self-monitoring RSS sampler reading /proc/self/status.
    """

    def __init__(self, interval_seconds=0.05):
        self.interval = interval_seconds
        self.peak_rss_kb = 0
        self.last_rss_kb = 0
        self.samples = 0
        self._stop = False
        self._thread = None

    def _read(self):
        try:
            with open("/proc/self/status") as f:
                for line in f:
                    if line.startswith("VmRSS:"):
                        value = line.split()[1]
                        return int(value)
        except Exception:
            return 0

    def start(self):
        self._stop = False
        self._thread = threading.Thread(
            target=self._run,
            daemon=True,
        )
        self._thread.start()
        return self

    def _run(self):
        while not self._stop:
            value = self._read()
            if value:
                self.peak_rss_kb = max(self.peak_rss_kb, value)
                self.last_rss_kb = value
                self.samples += 1
            time.sleep(self.interval)

    def stop(self):
        self._stop = True
        if self._thread is not None:
            self._thread.join(timeout=1)


def build_consumer():
    return KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        group_id=None,
        value_deserializer=lambda value: json.loads(
            value.decode("utf-8")
        ),
    )


class StreamRunner:
    """
    Executes one configuration over the Kafka topic and produces:
      - result rows (JSON array) for correctness comparison,
      - a metrics report (JSON object) for tables/plots.
    """

    def __init__(
        self,
        mechanism="aloa",
        fixed_lateness=None,
        maso_enabled=False,
        earm_enabled=False,
        earm_guard_policy="adaptive",
        earm_guard_seconds=12,
        maso_cycle_events=20,
        earm_cycle_events=20,
        baseline_peak_hot=None,
        audit_limit=500,
    ):
        self.mechanism = mechanism
        self.fixed_lateness = fixed_lateness
        self.maso_enabled = maso_enabled
        self.earm_enabled = earm_enabled
        self.earm_guard_policy = earm_guard_policy
        self.earm_guard_seconds = earm_guard_seconds
        self.maso_cycle_events = maso_cycle_events
        self.earm_cycle_events = earm_cycle_events
        self.baseline_peak_hot = baseline_peak_hot
        self.audit_limit = audit_limit

    # ------------------------------------------------------------------
    # Processor construction
    # ------------------------------------------------------------------

    def _new_aloa(self):
        self.aloa = ALOAController(window_size_seconds=WINDOW_SIZE_SECONDS)
        return self.aloa

    def _build(self):
        self.processor = CMiXProcessor(
            window_size_seconds=WINDOW_SIZE_SECONDS,
            slide_seconds=SLIDE_SECONDS,
            audit_limit=self.audit_limit,
        )

        self.aloa = None
        self.maso = None
        self.earm = None

        if self.mechanism in ("aloa", "maso", "earm", "full", "earm_agg"):
            self.aloa = self._new_aloa()

        if self.maso_enabled or self.mechanism in (
            "maso",
            "earm",
            "full",
            "earm_agg",
        ):
            self.maso = MASOController(
                cycle_events=self.maso_cycle_events,
                retention_horizon_seconds=self.earm_guard_seconds * 2,
            )

        if self.earm_enabled or self.mechanism in (
            "earm",
            "full",
            "earm_agg",
        ):
            policy = self.earm_guard_policy
            guard = self.earm_guard_seconds

            if self.mechanism == "earm_agg":
                policy = "fixed"
                guard = min(guard, 4)

            self.earm = EARMController(
                guard_policy=policy,
                guard_seconds=guard,
                cycle_events=self.earm_cycle_events,
            )

    # ------------------------------------------------------------------
    # Main loop
    # ------------------------------------------------------------------

    def run(
        self,
        expected,
        emitted_path=None,
    ):
        consumer = build_consumer()

        self._build()
        self.current_max = None

        self.processor.drop_late_after_eviction = (
            self.mechanism in ("full", "earm", "earm_agg")
        )

        received = 0
        inversions = 0
        previous_event_time = None

        # Running statistics (bounded memory; no per-event storage).
        stats = {
            "lateness_sum": 0.0,
            "lateness_max": 0.0,
            "ooo_events": 0,
            "budget_first": None,
            "budget_last": None,
            "budget_min": None,
            "budget_max": None,
            "budget_sum": 0.0,
            "budget_count": 0,
            "budget_changes": 0,
            "peak_active": 0,
            "watermark_first": None,
            "watermark_last": None,
        }

        # Decimated per-event sample for downstream plots/analysis.
        sample_every = 500
        samples = []

        emitted_fh = None

        def emit(row):
            nonlocal emitted_fh
            if emitted_path is not None:
                if emitted_fh is None:
                    emitted_fh = open(emitted_path, "w")
                emitted_fh.write(
                    json.dumps(row, separators=(",", ":")) + "\n"
                )

        self.processor.emit_callback = emit

        rss = RSSMonitor(interval_seconds=0.05).start()

        start = time.time()

        try:
            while received < expected:
                records = consumer.poll(
                    timeout_ms=1000,
                    max_records=100,
                )

                if not records:
                    continue

                for _, messages in records.items():
                    for message in messages:
                        event = message.value
                        event_time = int(event["event_time"])

                        if (
                            previous_event_time is not None
                            and event_time < previous_event_time
                        ):
                            inversions += 1

                        previous_event_time = event_time

                        budget_now = None
                        lateness_now = 0.0

                        if self.aloa is not None:
                            observation = self.aloa.observe_event(
                                event_time=event_time,
                                state_size=len(self.processor.state),
                                memory_utilization=(
                                    self.maso.memory_utilization()
                                    if self.maso is not None
                                    else 0.0
                                ),
                            )
                            budget_now = (
                                observation.allowed_lateness_seconds
                            )
                            lateness_now = observation.lateness_seconds

                            self.processor.update_watermark(
                                max_event_time=self.aloa.max_event_time,
                                allowed_lateness_seconds=budget_now,
                            )

                        elif self.mechanism == "fixed":
                            budget_now = self.fixed_lateness
                            self.processor.update_watermark(
                                max_event_time=(
                                    self.current_max or event_time
                                ),
                                allowed_lateness_seconds=budget_now,
                            )
                            lateness_now = (
                                max(
                                    0,
                                    (self.current_max or event_time)
                                    - event_time,
                                )
                                if self.current_max is not None
                                else 0.0
                            )

                        elif self.mechanism == "cmix":
                            lateness_now = (
                                max(
                                    0,
                                    (self.current_max or event_time)
                                    - event_time,
                                )
                                if self.current_max is not None
                                else 0.0
                            )

                        self.current_max = (
                            max(self.current_max, event_time)
                            if self.current_max is not None
                            else event_time
                        )

                        self.processor.process_event(
                            event_time=event_time,
                            device_id=event["device_id"],
                            sensor=event["sensor"],
                            value=float(event["value"]),
                            lateness_seconds=lateness_now,
                            allowed_lateness_seconds=budget_now,
                        )

                        if self.mechanism != "cmix":
                            self.processor.finalize_windows()

                        if self.earm is not None:
                            self.earm.observe_event(
                                lateness_seconds=lateness_now,
                                allowed_lateness_seconds=(budget_now or 0),
                                over_budget=(
                                    lateness_now > (budget_now or 0)
                                ),
                            )

                        if (
                            self.maso is not None
                            and received % self.maso.cycle_events == 0
                        ):
                            if (
                                received
                                % (
                                    self.maso.cycle_events
                                    * self.maso.scan_every_cycles
                                )
                                == 0
                            ):
                                self.maso.record_footprint(
                                    self.processor.memory_footprint()
                                )
                            self.maso.record_cycle(
                                received,
                                self.processor,
                            )

                        if (
                            self.earm is not None
                            and received % self.earm.cycle_events == 0
                        ):
                            self.earm.run_eviction_cycle(
                                received,
                                self.processor,
                                emit,
                            )

                        stats["lateness_sum"] += lateness_now
                        stats["lateness_max"] = max(
                            stats["lateness_max"], lateness_now
                        )
                        ooo_now = lateness_now > 0
                        if ooo_now:
                            stats["ooo_events"] += 1

                        if budget_now is not None:
                            if stats["budget_first"] is None:
                                stats["budget_first"] = budget_now
                            elif budget_now != stats["budget_last"]:
                                stats["budget_changes"] += 1

                            stats["budget_last"] = budget_now
                            stats["budget_min"] = (
                                budget_now
                                if stats["budget_min"] is None
                                else min(
                                    stats["budget_min"], budget_now
                                )
                            )
                            stats["budget_max"] = (
                                budget_now
                                if stats["budget_max"] is None
                                else max(
                                    stats["budget_max"], budget_now
                                )
                            )
                            stats["budget_sum"] += budget_now
                            stats["budget_count"] += 1

                        active = len(self.processor.state)
                        stats["peak_active"] = max(
                            stats["peak_active"], active
                        )

                        if self.processor.watermark is not None:
                            if stats["watermark_first"] is None:
                                stats["watermark_first"] = (
                                    self.processor.watermark
                                )
                            stats["watermark_last"] = (
                                self.processor.watermark
                            )

                        received += 1

                        if received % sample_every == 0:
                            samples.append(
                                {
                                    "event_number": received,
                                    "event_time": event_time,
                                    "lateness_seconds": lateness_now,
                                    "allowed_lateness_seconds": (
                                        budget_now
                                    ),
                                    "watermark": (
                                        self.processor.watermark
                                    ),
                                    "active_state_entries": active,
                                    "out_of_order": ooo_now,
                                }
                            )

                        if received >= expected:
                            break

                    if received >= expected:
                        break

            # Final accounting cycles: refresh MASO offloadable report
            # and flush anything EARM deems evictable with the final
            # guard (runs while the emitted file is still open).
            if self.maso is not None:
                self.maso.record_footprint(
                    self.processor.memory_footprint()
                )
                self.maso.record_cycle(
                    received, self.processor, force_scan=True
                )

            if self.earm is not None:
                self.earm.run_eviction_cycle(
                    received,
                    self.processor,
                    emit,
                )

        finally:
            consumer.close()
            rss.stop()
            if emitted_fh is not None:
                emitted_fh.close()

            if emitted_path is not None and os.path.exists(
                emitted_path
            ):
                self.emitted_rows = self._read_emitted(emitted_path)
            else:
                self.emitted_rows = []

        self.elapsed = time.time() - start

        self.stats = stats
        self.samples = samples

        self.report = self._make_report(
            expected=expected,
            received=received,
            inversions=inversions,
            stats=stats,
            samples=samples,
            rss=rss,
        )

        return self.report

    @staticmethod
    def _read_emitted(path):
        rows = []
        with open(path) as f:
            for line in f:
                if line.strip():
                    rows.append(json.loads(line))
        return rows

    # ------------------------------------------------------------------
    # Result assembly
    # ------------------------------------------------------------------

    def combined_results(self):
        """
        Union of emitted (frozen) rows and processor hot/archive state,
        sorted deterministically.
        """
        merged = {}
        for row in self.emitted_rows:
            merged[(row["start"], row["DeviceId"], row["Sensor"])] = row

        for row in self.processor.results():
            merged[(row["start"], row["DeviceId"], row["Sensor"])] = row

        return sorted(
            merged.values(),
            key=lambda x: (x["start"], x["DeviceId"], x["Sensor"]),
        )

    # ------------------------------------------------------------------
    # Metrics report
    # ------------------------------------------------------------------

    def _make_report(
        self,
        expected,
        received,
        inversions,
        stats,
        samples,
        rss,
    ):
        ooo_events = stats["ooo_events"]
        lateness_max = stats["lateness_max"]
        lateness_avg = (
            stats["lateness_sum"] / received if received else 0.0
        )

        over_budget = self.processor.over_budget_events

        within_budget = received - over_budget

        snapshot = self.processor.snapshot()

        state = {
            "peak_hot_entries": snapshot["peak_active_state_entries"],
            "final_hot_entries": snapshot["active_state_entries"],
            "final_archive_entries": snapshot[
                "finalized_state_entries"
            ],
            "peak_total_entries": (
                stats["peak_active"]
                + snapshot["finalized_state_entries"]
            ),
            "final_total_entries": (
                snapshot["active_state_entries"]
                + snapshot["finalized_state_entries"]
            ),
            "finalized_windows": snapshot["finalized_windows"],
            "evicted_state_entries": snapshot[
                "evicted_state_entries"
            ],
            "reconciled_events": snapshot["reconciled_events"],
            "late_after_finalization": snapshot[
                "late_after_finalization"
            ],
            "over_budget_events": snapshot["over_budget_events"],
            "late_after_eviction": snapshot["late_after_eviction"],
            "frozen_emissions": snapshot["frozen_emissions"],
        }

        if self.maso is not None:
            maso_stats = self.maso.snapshot()
            state["maso_peak_hot_entries"] = maso_stats[
                "peak_hot_entries"
            ]
            state["maso_peak_archive_entries"] = maso_stats[
                "peak_archive_entries"
            ]
            state["final_offloadable_entries"] = maso_stats[
                "final_offloadable_entries"
            ]

        report = {
            "mechanism": self.mechanism,
            "expected_events": expected,
            "events_received": received,
            "ordering": {
                "ooo_events": ooo_events,
                "ooo_ratio": (
                    ooo_events / received if received else 0.0
                ),
                "inversions": inversions,
            },
            "lateness": {
                "maximum_seconds": lateness_max,
                "average_seconds": round(lateness_avg, 4),
            },
            "budget": self._budget_report(stats),
            "budget_evaluation": {
                "within_budget_events": within_budget,
                "over_budget_events": over_budget,
                "within_budget_ratio": (
                    within_budget / received if received else 0.0
                ),
                "over_budget_ratio": (
                    over_budget / received if received else 0.0
                ),
            },
            "state": state,
            "state_reduction": self._reduction_report(state),
            "runtime": {
                "processing_seconds": round(self.elapsed, 6),
                "events_per_second": (
                    round(received / self.elapsed, 2)
                    if self.elapsed > 0
                    else 0.0
                ),
                "peak_rss_kb": rss.peak_rss_kb,
                "final_rss_kb": rss.last_rss_kb,
                "rss_samples": rss.samples,
            },
            "correctness_mode": {
                "all_events_processed_by_cmix": (
                    self.mechanism != "earm_agg"
                ),
                "over_budget_events_discarded": (
                    self.mechanism in ("earm", "full", "earm_agg")
                ),
                "late_after_eviction_dropped": (
                    self.mechanism in ("earm", "full", "earm_agg")
                ),
            },
            "processor_snapshot": snapshot,
            "emitted_rows": len(self.emitted_rows),
            "result_rows": len(self.combined_results()),
            "samples": samples,
        }

        if self.aloa is not None:
            report["aloa"] = self.aloa.snapshot()
            report["snapshot"] = self.aloa.snapshot()

        if self.maso is not None:
            report["maso"] = self.maso.snapshot()

        if self.earm is not None:
            report["earm"] = self.earm.snapshot()

        return report

    def _budget_report(self, stats):
        if self.mechanism == "fixed":
            return {
                "source": "fixed",
                "fixed_seconds": self.fixed_lateness,
                "configured_initial_seconds": self.fixed_lateness,
                "first_calculated_seconds": self.fixed_lateness,
                "final_seconds": self.fixed_lateness,
                "minimum_seconds": self.fixed_lateness,
                "maximum_seconds": self.fixed_lateness,
                "changes": 0,
                "mean_seconds": self.fixed_lateness,
            }

        if self.aloa is not None and stats["budget_count"]:
            return {
                "source": "aloa",
                "configured_initial_seconds": (
                    self.aloa.policy.initial_lateness_seconds
                ),
                "first_calculated_seconds": stats["budget_first"],
                "final_seconds": stats["budget_last"],
                "minimum_seconds": stats["budget_min"],
                "maximum_seconds": stats["budget_max"],
                "changes": stats["budget_changes"],
                "mean_seconds": round(
                    stats["budget_sum"] / stats["budget_count"], 3
                ),
            }

        return {"source": "none"}

    def _reduction_report(self, state):
        baseline = self.baseline_peak_hot

        if baseline is None or baseline <= 0:
            return {"available": False}

        peak = state["peak_hot_entries"]
        final = state["final_hot_entries"]

        return {
            "available": True,
            "baseline_peak_hot": baseline,
            "adaptive_peak_hot": peak,
            "peak_hot_reduction_percent": round(
                (baseline - peak) / baseline * 100, 4
            ),
            "final_hot_reduction_percent": round(
                (baseline - final) / baseline * 100, 4
            ),
        }


def main():
    import argparse

    parser = argparse.ArgumentParser()

    parser.add_argument("--expected", type=int, required=True)
    parser.add_argument("--output-rows", required=True)
    parser.add_argument("--metrics", required=True)
    parser.add_argument(
        "--emitted",
        default=None,
        help="JSON-lines sink for frozen/emitted rows (EARM).",
    )
    parser.add_argument(
        "--mechanism",
        required=True,
        choices=[
            "cmix",
            "fixed",
            "aloa",
            "maso",
            "earm",
            "full",
            "earm_agg",
        ],
    )
    parser.add_argument("--fixed-lateness", type=int, default=None)
    parser.add_argument("--baseline-peak-hot", type=int, default=None)
    parser.add_argument("--maso-cycle-events", type=int, default=20)
    parser.add_argument("--earm-cycle-events", type=int, default=20)
    parser.add_argument(
        "--earm-guard-policy",
        default="adaptive",
        choices=["fixed", "adaptive"],
    )
    parser.add_argument("--earm-guard-seconds", type=int, default=12)
    parser.add_argument("--scenario", default="")
    parser.add_argument("--event-count", type=int, default=0)
    parser.add_argument("--buffer-size", type=int, default=20)

    args = parser.parse_args()

    runner = StreamRunner(
        mechanism=args.mechanism,
        fixed_lateness=args.fixed_lateness,
        maso_enabled=(args.mechanism in ("maso", "earm", "full", "earm_agg")),
        earm_enabled=(args.mechanism in ("earm", "full", "earm_agg")),
        earm_guard_policy=args.earm_guard_policy,
        earm_guard_seconds=args.earm_guard_seconds,
        maso_cycle_events=args.maso_cycle_events,
        earm_cycle_events=args.earm_cycle_events,
        baseline_peak_hot=args.baseline_peak_hot,
    )

    report = runner.run(
        expected=args.expected,
        emitted_path=args.emitted,
    )

    report["scenario"] = args.scenario
    report["event_count"] = args.event_count or args.expected
    report["buffer_size"] = args.buffer_size

    rows = runner.combined_results()

    with open(args.output_rows, "w") as f:
        json.dump(rows, f, indent=2)

    with open(args.metrics, "w") as f:
        json.dump(report, f, indent=2)

    print("=" * 70)
    print(f"MECHANISM      : {args.mechanism}")
    print(f"Events received: {report['events_received']:,}")
    print(f"Result rows    : {report['result_rows']:,}")
    print(
        f"Peak hot state : "
        f"{report['state']['peak_hot_entries']:,}"
    )
    print(
        f"Final hot state: "
        f"{report['state']['final_hot_entries']:,}"
    )
    print(
        f"Archive state  : "
        f"{report['state']['final_archive_entries']:,}"
    )
    print(
        f"Evicted entries: "
        f"{report['state']['evicted_state_entries']:,}"
    )
    if report["state_reduction"].get("available"):
        print(
            f"Peak reduction : "
            f"{report['state_reduction']['peak_hot_reduction_percent']:.2f}%"
        )
    print(
        f"Processing     : "
        f"{report['runtime']['processing_seconds']:.3f}s "
        f"({report['runtime']['events_per_second']:.0f} ev/s)"
    )
    print(
        f"Peak RSS       : "
        f"{report['runtime']['peak_rss_kb']:,} KB"
    )
    print(f"Mismatch check : see correctness comparison")
    print("=" * 70)


if __name__ == "__main__":
    main()