"""
Unit tests for the ALOA+CMiX correctness fix (reconciliation),
MASO tier accounting, EARM guarded eviction, and the unified runner.
"""

import json
import random
import sys
import unittest
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.bda.aloa.controller import ALOAController  # noqa: E402
from src.bda.cmix.processor import CMiXProcessor  # noqa: E402
from src.bda.cmix.runner import StreamRunner  # noqa: E402
from src.bda.eval.correctness import (  # noqa: E402
    compare_rows,
    row_key,
)
from src.bda.earm.controller import EARMController  # noqa: E402
from src.bda.maso.controller import MASOController  # noqa: E402

WINDOW = 60
SLIDE = 10


def window_starts(event_time):
    last = (event_time // SLIDE) * SLIDE
    return [
        start
        for start in range(
            last - (WINDOW // SLIDE - 1) * SLIDE,
            last + 1,
            SLIDE,
        )
        if start <= event_time < start + WINDOW
    ]


def build_events(events):
    """events: list of (event_time, device, sensor, value)."""
    return [
        {"event_time": int(et), "device_id": d, "sensor": s, "value": v}
        for et, d, s, v in events
    ]


def reference_aggregate(events):
    """Exact reference aggregation (no watermark)."""
    state = {}

    for event in events:
        et = event["event_time"]
        for start in window_starts(et):
            key = (start, event["device_id"], event["sensor"])
            agg = state.setdefault(
                key,
                {
                    "count": 0,
                    "total": 0.0,
                    "min": float("inf"),
                    "max": float("-inf"),
                },
            )
            agg["count"] += 1
            agg["total"] += event["value"]
            agg["min"] = min(agg["min"], event["value"])
            agg["max"] = max(agg["max"], event["value"])

    return state


def to_rows(state):
    rows = []
    for (start, device, sensor), agg in state.items():
        rows.append(
            {
                "start": start,
                "DeviceId": device,
                "Sensor": sensor,
                "count": agg["count"],
                "sum": agg["total"],
                "avg": agg["total"] / agg["count"],
                "min": agg["min"],
                "max": agg["max"],
            }
        )
    return rows


def ordered_scenario(n):
    """Deterministic fully-shuffled out-of-order dataset."""
    rng = random.Random(42)
    events = []

    for i in range(n):
        et = (i // 5) * 2
        events.append((et, f"dev{i % 3}", f"s{i % 2}", float(i)))

    rng.shuffle(events)

    return build_events(events)


def bounded_disorder_scenario(n):
    """
    Deterministic dataset with BOUNDED disorder (like the Kafka
    injector's reorder buffer): times advance, but events within each
    local group are permuted, keeping maximum lateness small.
    """
    rng = random.Random(99)
    group = 30

    slots = [(i // 3) * 2 for i in range(n)]
    positions = list(range(n))

    for g in range(0, n, group):
        end = min(g + group, n)
        sub = positions[g:end]
        rng.shuffle(sub)
        positions[g:end] = sub

    events = []

    for i, pos in enumerate(positions):
        events.append(
            (slots[pos], f"dev{pos % 3}", f"s{pos % 2}", float(i))
        )

    return build_events(events)


def run_with_reconciliation(events, processor, controller):
    for event in events:
        observation = controller.observe_event(
            event_time=event["event_time"],
            state_size=len(processor.state),
        )
        processor.update_watermark(
            max_event_time=controller.max_event_time,
            allowed_lateness_seconds=(
                observation.allowed_lateness_seconds
            ),
        )
        processor.process_event(
            event_time=event["event_time"],
            device_id=event["device_id"],
            sensor=event["sensor"],
            value=event["value"],
            lateness_seconds=observation.lateness_seconds,
            allowed_lateness_seconds=(
                observation.allowed_lateness_seconds
            ),
        )
        processor.finalize_windows()


class ReconciliationTests(unittest.TestCase):

    def test_exact_with_reconciliation_no_earm(self):
        events = ordered_scenario(200)

        processor = CMiXProcessor(WINDOW, SLIDE)
        controller = ALOAController(window_size_seconds=WINDOW)

        run_with_reconciliation(events, processor, controller)

        compare = compare_rows(
            to_rows(reference_aggregate(events)),
            processor.results(),
        )

        self.assertTrue(
            compare["exact"],
            f"expected exact; {json.dumps(compare, indent=2)}",
        )

        self.assertEqual(compare["missing_in_result"], 0)
        self.assertEqual(compare["extra_in_result"], 0)
        self.assertEqual(compare["field_mismatches"], 0)

    def test_50k_style_bug_is_fixed(self):
        """
        The original bug: a late event to an already-finalized window
        re-opened a partial hot-tier aggregate that overrode the
        complete archived summary (count 1 instead of 6).  With
        reconciliation the late event merges into the retained archive,
        so the reference aggregate must match exactly.
        """
        events = []

        events.append((100, "dev0", "s0", 1.0))
        events.extend(
            (t, "dev0", "s0", 0.5)
            for t in range(200, 2000, 3)
        )
        events.append((100, "dev0", "s0", 6.0))

        events = build_events(events)

        processor = CMiXProcessor(WINDOW, SLIDE)
        controller = ALOAController(window_size_seconds=WINDOW)

        run_with_reconciliation(events, processor, controller)

        snapshot = processor.snapshot()

        self.assertGreater(snapshot["reconciled_events"], 0)
        self.assertGreater(snapshot["late_after_finalization"], 0)

        compare = compare_rows(
            to_rows(reference_aggregate(events)),
            processor.results(),
        )

        self.assertTrue(compare["exact"])

    def test_earm_safe_releases_memory_exactly(self):
        events = bounded_disorder_scenario(2000)

        processor = CMiXProcessor(WINDOW, SLIDE)
        controller = ALOAController(window_size_seconds=WINDOW)
        earm = EARMController(
            guard_policy="fixed",
            guard_seconds=20,
            cycle_events=10,
        )

        emitted = []

        processor.emit_callback = emitted.append

        max_lateness_seen = 0.0

        for index, event in enumerate(events):
            observation = controller.observe_event(
                event_time=event["event_time"],
                state_size=len(processor.state),
            )
            processor.update_watermark(
                max_event_time=controller.max_event_time,
                allowed_lateness_seconds=(
                    observation.allowed_lateness_seconds
                ),
            )
            processor.process_event(
                event_time=event["event_time"],
                device_id=event["device_id"],
                sensor=event["sensor"],
                value=event["value"],
                lateness_seconds=observation.lateness_seconds,
                allowed_lateness_seconds=(
                    observation.allowed_lateness_seconds
                ),
            )
            processor.finalize_windows()

            earm.observe_event(
                lateness_seconds=observation.lateness_seconds,
                allowed_lateness_seconds=(
                    observation.allowed_lateness_seconds
                ),
                over_budget=(
                    observation.lateness_seconds
                    > observation.allowed_lateness_seconds
                ),
            )

            max_lateness_seen = max(
                max_lateness_seen, observation.lateness_seconds
            )

            if (index + 1) % earm.cycle_events == 0:
                evictable = processor.evictable_keys(
                    max(int(max_lateness_seen) + 1, 1)
                )
                processor.freeze_and_emit(evictable)

        guard = int(max_lateness_seen) + 1

        # Final flush of anything that has become evictable.
        processor.freeze_and_emit(processor.evictable_keys(guard))

        # Streaming eviction must have released at least some memory.
        self.assertGreater(len(emitted), 0)

        self.assertEqual(processor.late_after_eviction, 0)

        merged = {}
        for row in emitted:
            merged[row_key(row)] = row
        for row in processor.results():
            merged[row_key(row)] = row

        compare = compare_rows(
            to_rows(reference_aggregate(events)),
            list(merged.values()),
        )

        self.assertTrue(
            compare["exact"],
            f"EARM dropped events: {json.dumps(compare, indent=2)}",
        )


class MASOTests(unittest.TestCase):

    def test_accounting_cycles(self):
        processor = CMiXProcessor(WINDOW, SLIDE)
        controller = ALOAController(window_size_seconds=WINDOW)
        maso = MASOController(cycle_events=25)

        events = bounded_disorder_scenario(300)

        for index, event in enumerate(events):
            observation = controller.observe_event(
                event_time=event["event_time"],
                state_size=len(processor.state),
                memory_utilization=maso.memory_utilization(),
            )
            processor.update_watermark(
                max_event_time=controller.max_event_time,
                allowed_lateness_seconds=(
                    observation.allowed_lateness_seconds
                ),
            )
            processor.process_event(
                event_time=event["event_time"],
                device_id=event["device_id"],
                sensor=event["sensor"],
                value=event["value"],
                lateness_seconds=observation.lateness_seconds,
                allowed_lateness_seconds=(
                    observation.allowed_lateness_seconds
                ),
            )
            processor.finalize_windows()

            maso.record_footprint(processor.memory_footprint())

            if (index + 1) % maso.cycle_events == 0:
                maso.record_cycle(len(events), processor)

        stats = maso.snapshot()

        self.assertGreaterEqual(stats["compaction_cycles"], 1)
        self.assertGreaterEqual(stats["peak_hot_entries"], 0)
        self.assertGreaterEqual(stats["final_archive_entries"], 0)

        accounting = maso.accounting()
        self.assertIn("offloadable_entries", accounting)
        self.assertIn("hot_entries", accounting)

    def test_guard_properties(self):
        earm = EARMController(guard_policy="fixed", guard_seconds=15)
        self.assertEqual(earm.effective_guard_seconds, 15)

        adaptive = EARMController(guard_policy="adaptive")
        adaptive.observe_event(
            8.0,
            allowed_lateness_seconds=5,
            over_budget=True,
        )
        self.assertEqual(adaptive.observed_max_overflow, 3.0)
        self.assertGreaterEqual(adaptive.effective_guard_seconds, 5)


class RunnerConstructionTests(unittest.TestCase):

    def test_configurations_construct(self):
        for mechanism in [
            "cmix",
            "fixed",
            "aloa",
            "maso",
            "earm",
            "full",
            "earm_agg",
        ]:
            runner = StreamRunner(mechanism=mechanism, fixed_lateness=5)
            runner._build()
            self.assertIsNotNone(runner.processor)
            self.assertEqual(runner.processor.window_size, WINDOW)


if __name__ == "__main__":
    unittest.main(verbosity=2)