import argparse
import json
import time

from kafka import KafkaConsumer

from src.bda.cmix.processor import CMiXProcessor


KAFKA_BOOTSTRAP = "localhost:9092"
KAFKA_TOPIC = "bda-iot-events"


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument("--expected", type=int, required=True)
    parser.add_argument("--output", required=True)

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

    received = 0
    inversions = 0
    previous_event_time = None

    start = time.time()

    print("=" * 70)
    print("BDA CMiX 50K GROUND TRUTH")
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

                # Ground truth:
                # process every event without watermark
                # finalization or eviction.
                processor.process_event(
                    event_time=event_time,
                    device_id=event["device_id"],
                    sensor=event["sensor"],
                    value=float(event["value"]),
                )

                received += 1

                if received >= args.expected:
                    break

            if received >= args.expected:
                break

    elapsed = time.time() - start

    results = processor.results()

    consumer.close()

    with open(args.output, "w") as f:
        json.dump(
            results,
            f,
            indent=2,
        )

    print()
    print("=" * 70)
    print("GROUND TRUTH COMPLETE")
    print("=" * 70)
    print(f"Events received    : {received:,}")
    print(f"OOO inversions     : {inversions:,}")
    print(f"Result rows        : {len(results):,}")
    print(f"Elapsed            : {elapsed:.2f} seconds")
    print(f"Output             : {args.output}")
    print("=" * 70)


if __name__ == "__main__":
    main()
