import argparse
import json
import time
from statistics import mean

from kafka import KafkaConsumer

from src.bda.aloa.controller import ALOAController
from src.bda.cmix.processor import CMiXProcessor


KAFKA_BOOTSTRAP = "localhost:9092"
KAFKA_TOPIC = "bda-iot-events"


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument("--expected", type=int, default=10000)
    parser.add_argument("--output", required=True)
    parser.add_argument("--aloa-log", dest="aloa_log", required=True)

    args = parser.parse_args()

    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        group_id=None,
        value_deserializer=lambda value: json.loads(
            value.decode("utf-8")
        ),
    )

    processor = CMiXProcessor(
        window_size_seconds=60,
        slide_seconds=10,
    )

    aloa = ALOAController(
        window_size_seconds=60,
    )

    received = 0
    inversions = 0
    previous_event_time = None

    observations = []

    start = time.time()

    print("=" * 70)
    print("BDA ALOA + CMiX CONTROL MEASUREMENT")
    print("=" * 70)

    while received < args.expected:

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

                state_before = len(processor.state)

                observation = aloa.observe_event(
                    event_time=event_time,
                    state_size=state_before,
                    memory_utilization=0.0,
                )

                budget = observation.allowed_lateness_seconds
                lateness = observation.lateness_seconds

                within_budget = lateness <= budget

                # --------------------------------------------------
                # ALOA -> CMiX adaptive watermark.
                #
                # The watermark is derived from the latest event-time
                # position and ALOA's current lateness budget.
                # --------------------------------------------------
                watermark = processor.update_watermark(
                    max_event_time=aloa.max_event_time,
                    allowed_lateness_seconds=budget,
                )

                # --------------------------------------------------
                # Finalize windows that are now behind the adaptive
                # watermark. Finalized aggregates remain available
                # through processor.results(), while active state
                # can be released.
                # --------------------------------------------------
                finalized = processor.finalize_windows()

                # IMPORTANT:
                # V1 does NOT discard over-budget events.
                # Every event is still processed by CMiX so that
                # correctness remains directly comparable with
                # ground truth.
                processor.process_event(
                    event_time=event_time,
                    device_id=event["device_id"],
                    sensor=event["sensor"],
                    value=float(event["value"]),
                )

                state_after = len(processor.state)

                observations.append(
                    {
                        "event_number": received + 1,
                        "event_time": event_time,
                        "lateness_seconds": lateness,
                        "allowed_lateness_seconds": budget,
                        "within_budget": within_budget,
                        "watermark": watermark,
                        "finalized_windows": finalized,
                        "active_state_entries": len(processor.state),
                        "out_of_order": observation.out_of_order,
                        "state_size_before": state_before,
                        "state_size_after": state_after,
                        "state_growth": (
                            state_after - state_before
                        ),
                    }
                )

                received += 1

                if received >= args.expected:
                    break

            if received >= args.expected:
                break

    elapsed = time.time() - start

    results = processor.results()
    cmix_snapshot = processor.snapshot()

    lateness_values = [
        x["lateness_seconds"]
        for x in observations
    ]

    budgets = [
        x["allowed_lateness_seconds"]
        for x in observations
    ]

    state_sizes = [
        x["state_size_after"]
        for x in observations
    ]

    over_budget = sum(
        1
        for x in observations
        if not x["within_budget"]
    )

    within_budget = received - over_budget

    ooo_events = sum(
        1
        for x in observations
        if x["out_of_order"]
    )

    budget_changes = sum(
        1
        for i in range(1, len(budgets))
        if budgets[i] != budgets[i - 1]
    )

    configured_initial_budget = (
        aloa.policy.initial_lateness_seconds
    )

    aloa_output = {
        "events_observed": received,

        "ordering": {
            "ooo_events": ooo_events,
            "ooo_ratio": (
                ooo_events / received
                if received
                else 0.0
            ),
            "inversions": inversions,
        },

        "lateness": {
            "maximum_seconds": (
                max(lateness_values)
                if lateness_values
                else 0.0
            ),
            "average_seconds": (
                mean(lateness_values)
                if lateness_values
                else 0.0
            ),
        },

        "budget": {
            "configured_initial_seconds": (
                configured_initial_budget
            ),
            "first_calculated_seconds": (
                budgets[0]
                if budgets
                else 0
            ),
            "final_seconds": (
                budgets[-1]
                if budgets
                else 0
            ),
            "minimum_seconds": (
                min(budgets)
                if budgets
                else 0
            ),
            "maximum_seconds": (
                max(budgets)
                if budgets
                else 0
            ),
            "changes": budget_changes,
        },

        "budget_evaluation": {
            "within_budget_events": within_budget,
            "over_budget_events": over_budget,
            "within_budget_ratio": (
                within_budget / received
                if received
                else 0.0
            ),
            "over_budget_ratio": (
                over_budget / received
                if received
                else 0.0
            ),
        },

        "state": {
            "initial_entries": (
                state_sizes[0]
                if state_sizes
                else 0
            ),
            "maximum_entries": (
                max(state_sizes)
                if state_sizes
                else 0
            ),
            "final_entries": (
                state_sizes[-1]
                if state_sizes
                else 0
            ),
            "average_entries": (
                mean(state_sizes)
                if state_sizes
                else 0.0
            ),
            "peak_active_entries": cmix_snapshot.get(
                "peak_active_state_entries", 0
            ),
            "finalized_state_entries": cmix_snapshot.get(
                "finalized_state_entries", 0
            ),
            "evicted_state_entries": cmix_snapshot.get(
                "evicted_state_entries", 0
            ),
            "finalized_windows": cmix_snapshot.get(
                "finalized_windows", 0
            ),
        },

        "runtime": {
            "processing_seconds": elapsed,
            "events_per_second": (
                received / elapsed
                if elapsed > 0
                else 0.0
            ),
        },

        "correctness_mode": {
            "all_events_processed_by_cmix": True,
            "over_budget_events_discarded": False,
        },

        "limitations": {
            "memory_utilization_connected": False,
            "memory_utilization_value": 0.0,
        },

        "snapshot": aloa.snapshot(),
        "observations": observations,
    }

    with open(args.output, "w") as f:
        json.dump(results, f, indent=2)

    with open(args.aloa_log, "w") as f:
        json.dump(aloa_output, f, indent=2)

    consumer.close()

    print()
    print("=" * 70)
    print("ALOA + CMiX CONTROL MEASUREMENT COMPLETE")
    print("=" * 70)

    print(f"Events received          : {received:,}")
    print(f"OOO events               : {ooo_events:,}")
    print(f"OOO ratio                : {ooo_events / received:.4f}")

    print()
    print("LATENESS")
    print(
        f"Maximum                  : "
        f"{max(lateness_values):.2f}s"
    )
    print(
        f"Average                  : "
        f"{mean(lateness_values):.2f}s"
    )

    print()
    print("ALOA BUDGET")
    print(
        f"Configured initial       : "
        f"{configured_initial_budget}s"
    )
    print(
        f"First calculated         : "
        f"{budgets[0]}s"
    )
    print(
        f"Final                    : "
        f"{budgets[-1]}s"
    )
    print(
        f"Range                    : "
        f"{min(budgets)}-{max(budgets)}s"
    )
    print(
        f"Changes                  : "
        f"{budget_changes:,}"
    )

    print()
    print("BUDGET EVALUATION")
    print(
        f"Within budget            : "
        f"{within_budget:,}"
    )
    print(
        f"Over budget              : "
        f"{over_budget:,}"
    )
    print(
        f"Within-budget ratio      : "
        f"{within_budget / received:.4f}"
    )

    print()
    print("CMiX STATE")
    print(
        f"Maximum entries          : "
        f"{max(state_sizes):,}"
    )
    print(
        f"Final entries            : "
        f"{state_sizes[-1]:,}"
    )

    print()
    print(f"Result rows              : {len(results):,}")
    print(f"Elapsed                  : {elapsed:.2f}s")
    print("=" * 70)


if __name__ == "__main__":
    main()
