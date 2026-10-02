"""
Dataset ingestion, schema validation, and profiling for IoT stream data.

Enforces:
1. Strict arrival-order preservation (NEVER sort or globally reorder before streaming).
2. Support for 'Timestamp' or 'Time', 'DeviceId', 'Sensor', 'Value' columns.
3. Out-of-order and lateness profiling based on arrival vs event time.
"""

import csv
import io
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union


REQUIRED_CANONICAL_COLUMNS = {"event_time", "device_id", "sensor", "value"}


def _parse_timestamp_to_seconds(ts_val: Any) -> int:
    """Convert timestamp string, float, or integer to epoch seconds."""
    if ts_val is None or str(ts_val).strip() == "":
        raise ValueError("Timestamp value is empty or None")

    if isinstance(ts_val, (int, float)):
        return int(ts_val)

    s = str(ts_val).strip()
    # Try integer/float epoch string first
    try:
        val = float(s)
        return int(val)
    except ValueError:
        pass

    # Try ISO/standard datetime formats
    formats = [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S.%f",
        "%Y-%m-%dT%H:%M:%S.%f",
        "%Y-%m-%d",
    ]
    for fmt in formats:
        try:
            dt = datetime.strptime(s, fmt)
            return int(dt.timestamp())
        except ValueError:
            continue

    # Fallback to datetime.fromisoformat
    try:
        dt = datetime.fromisoformat(s)
        return int(dt.timestamp())
    except Exception as e:
        raise ValueError(f"Unable to parse timestamp '{ts_val}': {e}")


def resolve_column_mapping(columns: List[str]) -> Dict[str, str]:
    """
    Map raw header columns to canonical keys:
    'event_time', 'device_id', 'sensor', 'value'.
    """
    mapping = {}
    col_lookup = {c.strip().lower(): c for c in columns}

    # Time / Timestamp
    if "timestamp" in col_lookup:
        mapping["event_time"] = col_lookup["timestamp"]
    elif "time" in col_lookup:
        mapping["event_time"] = col_lookup["time"]
    elif "event_time" in col_lookup:
        mapping["event_time"] = col_lookup["event_time"]
    else:
        raise ValueError(
            f"Missing required timestamp column ('Timestamp' or 'Time'). Found columns: {columns}"
        )

    # DeviceId
    if "deviceid" in col_lookup:
        mapping["device_id"] = col_lookup["deviceid"]
    elif "device_id" in col_lookup:
        mapping["device_id"] = col_lookup["device_id"]
    elif "device" in col_lookup:
        mapping["device_id"] = col_lookup["device"]
    else:
        raise ValueError(
            f"Missing required device column ('DeviceId' or 'device_id'). Found columns: {columns}"
        )

    # Sensor
    if "sensor" in col_lookup:
        mapping["sensor"] = col_lookup["sensor"]
    elif "sensor_type" in col_lookup:
        mapping["sensor"] = col_lookup["sensor_type"]
    else:
        raise ValueError(
            f"Missing required sensor column ('Sensor'). Found columns: {columns}"
        )

    # Value
    if "value" in col_lookup:
        mapping["value"] = col_lookup["value"]
    elif "val" in col_lookup:
        mapping["value"] = col_lookup["val"]
    elif "reading" in col_lookup:
        mapping["value"] = col_lookup["reading"]
    else:
        raise ValueError(
            f"Missing required value column ('Value'). Found columns: {columns}"
        )

    return mapping


def parse_csv_stream(
    source: Union[str, Path, io.StringIO, bytes],
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """
    Parse CSV data preserving arrival order (file order).

    Returns:
        (events, profile_data)
    """
    if isinstance(source, bytes):
        text = source.decode("utf-8", errors="replace")
        stream = io.StringIO(text)
    elif isinstance(source, str):
        if not source.strip():
            raise ValueError("Uploaded CSV is empty")
        if "\n" not in source and Path(source).is_file():
            with open(source, "r", encoding="utf-8", errors="replace") as f:
                text = f.read()
            stream = io.StringIO(text)
        else:
            stream = io.StringIO(source)
    elif isinstance(source, Path):
        if not source.is_file():
            raise ValueError(f"File not found: {source}")
        with open(source, "r", encoding="utf-8", errors="replace") as f:
            text = f.read()
        stream = io.StringIO(text)
    elif isinstance(source, io.StringIO):
        stream = source
    else:
        raise TypeError(f"Unsupported source type: {type(source)}")

    reader = csv.reader(stream)
    try:
        header = next(reader)
    except StopIteration:
        raise ValueError("Uploaded CSV is empty")

    header = [h.strip() for h in header]
    if not header or all(not h for h in header):
        raise ValueError("Uploaded CSV has empty header row")

    col_map = resolve_column_mapping(header)
    idx_time = header.index(col_map["event_time"])
    idx_dev = header.index(col_map["device_id"])
    idx_sensor = header.index(col_map["sensor"])
    idx_val = header.index(col_map["value"])

    events = []
    missing_count = 0
    seen_row_hashes = set()
    duplicate_count = 0

    arrival_sequence = 0
    for row_idx, row in enumerate(reader, start=2):
        if not row or all(not str(c).strip() for c in row):
            continue

        if len(row) <= max(idx_time, idx_dev, idx_sensor, idx_val):
            missing_count += 1
            continue

        raw_time = row[idx_time].strip()
        raw_dev = row[idx_dev].strip()
        raw_sensor = row[idx_sensor].strip()
        raw_val = row[idx_val].strip()

        if not raw_time or not raw_dev or not raw_sensor or not raw_val:
            missing_count += 1
            continue

        try:
            val_float = float(raw_val)
        except ValueError:
            raise ValueError(f"Line {row_idx}: Invalid numeric value '{raw_val}'")

        try:
            event_time = _parse_timestamp_to_seconds(raw_time)
        except ValueError as e:
            raise ValueError(f"Line {row_idx}: {e}")

        # Check duplicates based on (event_time, device, sensor, value)
        row_signature = (event_time, raw_dev, raw_sensor, round(val_float, 6))
        if row_signature in seen_row_hashes:
            duplicate_count += 1
        else:
            seen_row_hashes.add(row_signature)

        events.append(
            {
                "event_id": f"ev-{arrival_sequence}",
                "event_time": event_time,
                "device_id": raw_dev,
                "sensor": raw_sensor,
                "value": val_float,
                "raw_timestamp": raw_time,
                "arrival_sequence": arrival_sequence,
            }
        )
        arrival_sequence += 1

    if not events:
        raise ValueError("Uploaded CSV contains no valid data rows")

    profile = profile_dataset(
        events,
        missing_count=missing_count,
        duplicate_count=duplicate_count,
        original_header=header,
    )
    return events, profile


def profile_dataset(
    events: List[Dict[str, Any]],
    missing_count: int = 0,
    duplicate_count: int = 0,
    original_header: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """
    Profile dataset strictly in arrival order.
    Calculates OOO metrics, max lateness, time spans, and unique devices/sensors.
    """
    total = len(events)
    if total == 0:
        return {"total_events": 0}

    devices = set()
    sensors = set()

    max_seen_time = None
    ooo_count = 0
    max_lateness = 0.0
    lateness_sum = 0.0
    inversions = 0
    prev_time = None

    all_times = []

    for ev in events:
        et = ev["event_time"]
        all_times.append(et)
        devices.add(ev["device_id"])
        sensors.add(ev["sensor"])

        if prev_time is not None and et < prev_time:
            inversions += 1
        prev_time = et

        if max_seen_time is None:
            max_seen_time = et
        else:
            if et < max_seen_time:
                ooo_count += 1
                lateness = max_seen_time - et
                lateness_sum += lateness
                if lateness > max_lateness:
                    max_lateness = lateness
            else:
                max_seen_time = et

    min_ts = min(all_times)
    max_ts = max(all_times)
    timespan = max_ts - min_ts

    ooo_pct = round((ooo_count / total) * 100.0, 2)
    avg_lateness = round(lateness_sum / total, 3) if total else 0.0

    # Arrival order preview vs Event-time order preview for first 10 events
    arrival_preview = [
        {
            "arrival_seq": ev["arrival_sequence"],
            "event_time": ev["event_time"],
            "timestamp": ev.get("raw_timestamp", str(ev["event_time"])),
            "device_id": ev["device_id"],
            "sensor": ev["sensor"],
            "value": ev["value"],
        }
        for ev in events[:10]
    ]

    sorted_preview = sorted(events[:20], key=lambda x: x["event_time"])[:10]
    event_time_preview = [
        {
            "arrival_seq": ev["arrival_sequence"],
            "event_time": ev["event_time"],
            "timestamp": ev.get("raw_timestamp", str(ev["event_time"])),
            "device_id": ev["device_id"],
            "sensor": ev["sensor"],
            "value": ev["value"],
        }
        for ev in sorted_preview
    ]

    return {
        "total_events": total,
        "columns": original_header or ["Timestamp", "DeviceId", "Sensor", "Value"],
        "devices_count": len(devices),
        "devices": sorted(list(devices))[:20],
        "sensors_count": len(sensors),
        "sensors": sorted(list(sensors)),
        "min_timestamp": min_ts,
        "max_timestamp": max_ts,
        "timespan_seconds": timespan,
        "missing_values": missing_count,
        "duplicate_rows": duplicate_count,
        "ooo_events": ooo_count,
        "ooo_percentage": ooo_pct,
        "max_lateness_seconds": max_lateness,
        "avg_lateness_seconds": avg_lateness,
        "inversions": inversions,
        "arrival_preview": arrival_preview,
        "event_time_preview": event_time_preview,
    }
