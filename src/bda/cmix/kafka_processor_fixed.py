import argparse
import json
import time

from kafka import KafkaConsumer

from src.bda.cmix.processor import CMiXProcessor


KAFKA_BOOTSTRAP = "localhost:9092"
KAFKA_TOPIC = "bda-iot-events"


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument("--expected", type=int, default=1000)
    parser.add_argument("--output", required=True)
    parser.add_argument("--fixed-lateness", type=int, required=True)

    args = parser.parse_args()

    if args.fixed_lateness < 0:
        raise ValueError("fixed-lateness must be >= 0")

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

    received = 0
    inversions = 0
    previous_event_time = None

    start = time.time()

    print("=" * 70)
    print("BDA CMiX FIXED-WATERMARK PROCESSOR")
    print("=" * 70)
    print(f"Fixed lateness     : {args.fixed_lateness}s")

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

                # Fixed-lateness watermark.
                processor.update_watermark(
                    max_event_time=event_time,
                    allowed_lateness_seconds=args.fixed_lateness,
                )

                # Process every event exactly as CMiX normally does.
                processor.process_event(
                    event_time=event_time,
                    device_id=event["device_id"],
                    sensor=event["sensor"],
                    value=float(event["value"]),
                )

                # Finalize windows made safe by the fixed watermark.
                processor.finalize_windows()

                received += 1

                if received >= args.expected:
                    break

            if received >= args.expected:
                break

    elapsed = time.time() - start

    results = processor.results()
    snapshot = processor.snapshot()

    with open(args.output, "w") as f:
        json.dump(
            results,
            f,
            indent=2,
        )

    consumer.close()

    print()
    print("=" * 70)
    print("CMiX FIXED-WATERMARK PROCESSING COMPLETE")
    print("=" * 70)
    print(f"Events received    : {received:,}")
    print(f"OOO inversions     : {inversions:,}")
    print(f"Result rows        : {len(results):,}")
    print(
        f"Peak state         : "
        f"{snapshot.get('peak_active_state_entries', len(processor.state)):,}"
    )
    print(
        f"Final active state : "
        f"{snapshot.get('active_state_entries', len(processor.state)):,}"
    )
    print(
        f"Finalized state    : "
        f"{snapshot.get('finalized_state_entries', 0):,}"
    )
    print(
        f"Evicted entries    : "
        f"{snapshot.get('evicted_state_entries', 0):,}"
    )
    print(
        f"Watermark          : "
        f"{snapshot.get('watermark')}"
    )
    print(f"Elapsed            : {elapsed:.2f} seconds")
    print(f"Output             : {args.output}")
    print("=" * 70)


if __name__ == "__main__":
    main()
