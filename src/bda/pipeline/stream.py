"""
Stream pipeline execution engine.

Supports:
- Modes: 'cmix', 'aloa', 'maso', 'earm', 'full', 'fixed'
- Strict arrival-order processing (never sort before streaming)
- Kafka transport when available (localhost:9092)
- Clearly labeled in-process fallback when Kafka is unavailable
- Live progress callbacks
- Metrics collection and series generation for UI charts
- Real-time ground truth computation and correctness evaluation
"""

import os
import time
from typing import Any, Callable, Dict, List, Optional, Tuple

from src.bda.aloa.controller import ALOAController
from src.bda.cmix.processor import CMiXProcessor
from src.bda.earm.controller import EARMController
from src.bda.eval.correctness import compare_rows
from src.bda.maso.controller import MASOController

WINDOW_SIZE_SECONDS = 60
SLIDE_SECONDS = 10


def get_current_rss_mb() -> float:
    """Read current process Resident Set Size (RSS) in Megabytes cross-platform."""
    # Linux
    if os.path.exists("/proc/self/status"):
        try:
            with open("/proc/self/status") as f:
                for line in f:
                    if line.startswith("VmRSS:"):
                        parts = line.split()
                        return round(int(parts[1]) / 1024.0, 2)
        except Exception:
            pass

    # Windows
    try:
        import ctypes
        from ctypes import wintypes

        class PROCESS_MEMORY_COUNTERS(ctypes.Structure):
            _fields_ = [
                ("cb", wintypes.DWORD),
                ("PageFaultCount", wintypes.DWORD),
                ("PeakWorkingSetSize", ctypes.c_size_t),
                ("WorkingSetSize", ctypes.c_size_t),
                ("QuotaPeakPagedPoolUsage", ctypes.c_size_t),
                ("QuotaPagedPoolUsage", ctypes.c_size_t),
                ("QuotaPeakNonPagedPoolUsage", ctypes.c_size_t),
                ("QuotaNonPagedPoolUsage", ctypes.c_size_t),
                ("PagefileUsage", ctypes.c_size_t),
                ("PeakPagefileUsage", ctypes.c_size_t),
            ]

        func = ctypes.windll.psapi.GetProcessMemoryInfo
        func.argtypes = [
            wintypes.HANDLE,
            ctypes.POINTER(PROCESS_MEMORY_COUNTERS),
            wintypes.DWORD,
        ]
        func.restype = wintypes.BOOL

        pmc = PROCESS_MEMORY_COUNTERS()
        pmc.cb = ctypes.sizeof(PROCESS_MEMORY_COUNTERS)
        handle = ctypes.windll.kernel32.GetCurrentProcess()
        if func(handle, ctypes.byref(pmc), pmc.cb):
            return round(pmc.WorkingSetSize / (1024.0 * 1024.0), 2)
    except Exception:
        pass

    return 0.0


def check_kafka_availability(
    bootstrap_servers: str = "localhost:9092", timeout_seconds: float = 0.5
) -> bool:
    """Check if Kafka cluster is reachable."""
    try:
        from kafka import KafkaConsumer

        consumer = KafkaConsumer(
            bootstrap_servers=bootstrap_servers,
            request_timeout_ms=int(timeout_seconds * 1000),
            consumer_timeout_ms=int(timeout_seconds * 1000),
        )
        consumer.topics()
        consumer.close()
        return True
    except Exception:
        return False


def compute_ground_truth_rows(
    events: List[Dict[str, Any]],
    window_size: int = WINDOW_SIZE_SECONDS,
    slide: int = SLIDE_SECONDS,
) -> List[Dict[str, Any]]:
    """
    Compute reference event-time aggregates independently of watermarks or drops.
    Matches Spark ground-truth reference logic.
    """
    state: Dict[Tuple[int, str, str], Dict[str, Any]] = {}

    for ev in events:
        et = int(ev["event_time"])
        device = str(ev["device_id"])
        sensor = str(ev["sensor"])
        val = float(ev["value"])

        # Determine all sliding windows covering this event time
        last = (et // slide) * slide
        window_starts = [
            st
            for st in range(last - (window_size // slide - 1) * slide, last + 1, slide)
            if st <= et < st + window_size
        ]

        for st in window_starts:
            key = (st, device, sensor)
            if key not in state:
                state[key] = {
                    "count": 0,
                    "sum": 0.0,
                    "min": float("inf"),
                    "max": float("-inf"),
                }
            agg = state[key]
            agg["count"] += 1
            agg["sum"] += val
            agg["min"] = min(agg["min"], val)
            agg["max"] = max(agg["max"], val)

    rows = []
    for (start, dev, sens), agg in state.items():
        rows.append(
            {
                "start": start,
                "DeviceId": dev,
                "Sensor": sens,
                "count": agg["count"],
                "sum": round(agg["sum"], 6),
                "avg": round(agg["sum"] / agg["count"], 6),
                "min": round(agg["min"], 6),
                "max": round(agg["max"], 6),
            }
        )

    return sorted(rows, key=lambda r: (r["start"], r["DeviceId"], r["Sensor"]))


class StreamPipeline:
    """
    Unified stream processing pipeline for CMiX, ALOA, MASO, EARM, and Full pipelines.
    """

    def __init__(
        self,
        mode: str = "full",
        window_size_seconds: int = WINDOW_SIZE_SECONDS,
        slide_seconds: int = SLIDE_SECONDS,
        fixed_lateness: int = 10,
        transport: str = "auto",
        earm_guard_policy: str = "adaptive",
        earm_guard_seconds: int = 10,
        maso_cycle_events: int = 100,
        earm_cycle_events: int = 100,
    ):
        self.mode = mode.lower()
        self.window_size_seconds = window_size_seconds
        self.slide_seconds = slide_seconds
        self.fixed_lateness = fixed_lateness
        self.requested_transport = transport
        self.earm_guard_policy = earm_guard_policy
        self.earm_guard_seconds = earm_guard_seconds
        self.maso_cycle_events = maso_cycle_events
        self.earm_cycle_events = earm_cycle_events

        # Determine transport
        if self.requested_transport == "kafka":
            if check_kafka_availability():
                self.transport = "kafka"
                self.transport_label = "Kafka (localhost:9092)"
            else:
                self.transport = "in_process"
                self.transport_label = "In-process demo fallback (Kafka unavailable)"
        elif self.requested_transport == "in_process":
            self.transport = "in_process"
            self.transport_label = "In-process demo fallback"
        else:  # auto
            if check_kafka_availability():
                self.transport = "kafka"
                self.transport_label = "Kafka (localhost:9092)"
            else:
                self.transport = "in_process"
                self.transport_label = "In-process demo fallback"

    def _build_components(self) -> Tuple[CMiXProcessor, Any, Any, Any]:
        processor = CMiXProcessor(
            window_size_seconds=self.window_size_seconds,
            slide_seconds=self.slide_seconds,
        )

        aloa = None
        maso = None
        earm = None

        if self.mode in ("aloa", "maso", "earm", "full", "earm_agg"):
            aloa = ALOAController(window_size_seconds=self.window_size_seconds)

        if self.mode in ("maso", "earm", "full", "earm_agg"):
            maso = MASOController(
                cycle_events=self.maso_cycle_events,
                retention_horizon_seconds=self.earm_guard_seconds * 2,
            )

        if self.mode in ("earm", "full", "earm_agg"):
            earm = EARMController(
                guard_policy=self.earm_guard_policy,
                guard_seconds=self.earm_guard_seconds,
                cycle_events=self.earm_cycle_events,
            )

        return processor, aloa, maso, earm

    def run(
        self,
        events: List[Dict[str, Any]],
        progress_callback: Optional[Callable[[int, int, Dict[str, Any]], None]] = None,
    ) -> Dict[str, Any]:
        """
        Execute streaming run over the given events strictly in arrival order.
        """
        total_events = len(events)
        processor, aloa, maso, earm = self._build_components()

        processor.drop_late_after_eviction = self.mode in ("full", "earm", "earm_agg")

        emitted_rows: List[Dict[str, Any]] = []

        def emit(row: Dict[str, Any]):
            emitted_rows.append(row)

        processor.emit_callback = emit

        start_time = time.time()
        initial_rss = get_current_rss_mb()
        peak_rss = initial_rss

        received = 0
        inversions = 0
        previous_event_time = None
        current_max = None

        lateness_sum = 0.0
        lateness_max = 0.0
        ooo_events = 0
        peak_active = 0

        # Sample trajectories for UI visualization
        sample_interval = max(1, total_events // 50)
        throughput_series = []
        state_series = []
        lateness_series = []

        last_progress_time = start_time
        last_progress_count = 0

        for idx, ev in enumerate(events):
            event_time = int(ev["event_time"])
            device_id = str(ev["device_id"])
            sensor = str(ev["sensor"])
            value = float(ev["value"])

            if previous_event_time is not None and event_time < previous_event_time:
                inversions += 1
            previous_event_time = event_time

            budget_now = None
            lateness_now = 0.0

            if aloa is not None:
                mem_util = maso.memory_utilization() if maso is not None else 0.0
                obs = aloa.observe_event(
                    event_time=event_time,
                    state_size=len(processor.state),
                    memory_utilization=mem_util,
                )
                budget_now = obs.allowed_lateness_seconds
                lateness_now = obs.lateness_seconds

                processor.update_watermark(
                    max_event_time=aloa.max_event_time,
                    allowed_lateness_seconds=budget_now,
                )
            elif self.mode == "fixed":
                budget_now = self.fixed_lateness
                processor.update_watermark(
                    max_event_time=(current_max or event_time),
                    allowed_lateness_seconds=budget_now,
                )
                lateness_now = (
                    max(0, (current_max or event_time) - event_time)
                    if current_max is not None
                    else 0.0
                )
            else:  # cmix baseline
                lateness_now = (
                    max(0, (current_max or event_time) - event_time)
                    if current_max is not None
                    else 0.0
                )

            current_max = (
                max(current_max, event_time) if current_max is not None else event_time
            )

            processor.process_event(
                event_time=event_time,
                device_id=device_id,
                sensor=sensor,
                value=value,
                lateness_seconds=lateness_now,
                allowed_lateness_seconds=budget_now,
            )

            if self.mode != "cmix":
                processor.finalize_windows()

            if earm is not None:
                earm.observe_event(
                    lateness_seconds=lateness_now,
                    allowed_lateness_seconds=(budget_now or 0),
                    over_budget=(lateness_now > (budget_now or 0)),
                )

            if maso is not None and (received + 1) % maso.cycle_events == 0:
                maso.record_footprint(processor.memory_footprint())
                maso.record_cycle(received + 1, processor)

            if earm is not None and (received + 1) % earm.cycle_events == 0:
                earm.run_eviction_cycle(received + 1, processor, emit)

            received += 1
            lateness_sum += lateness_now
            if lateness_now > lateness_max:
                lateness_max = lateness_now
            if lateness_now > 0:
                ooo_events += 1

            active_cnt = len(processor.state)
            if active_cnt > peak_active:
                peak_active = active_cnt

            # Trajectory sampling
            if received % sample_interval == 0 or received == total_events:
                now = time.time()
                cur_rss = get_current_rss_mb()
                if cur_rss > peak_rss:
                    peak_rss = cur_rss

                step_elapsed = now - start_time
                step_ev_per_sec = (
                    round(received / step_elapsed, 1) if step_elapsed > 0 else 0.0
                )

                throughput_series.append(
                    {
                        "event_number": received,
                        "ev_per_sec": step_ev_per_sec,
                    }
                )
                state_series.append(
                    {
                        "event_number": received,
                        "active_hot": len(processor.state),
                        "archive": len(processor.finalized_state),
                        "frozen": processor.frozen_emissions,
                    }
                )
                lateness_series.append(round(lateness_now, 2))

                # Fire progress callback
                if progress_callback is not None:
                    # Throttle callbacks to at most once per 20ms
                    if now - last_progress_time >= 0.02 or received == total_events:
                        progress_callback(
                            received,
                            total_events,
                            {
                                "received": received,
                                "total": total_events,
                                "throughput": step_ev_per_sec,
                                "active_state": len(processor.state),
                                "finalized_state": len(processor.finalized_state),
                                "frozen_emissions": processor.frozen_emissions,
                                "reconciled_events": processor.reconciled_events,
                                "current_watermark": processor.watermark,
                                "current_event_time": event_time,
                                "peak_active": peak_active,
                            },
                        )
                        last_progress_time = now
                        last_progress_count = received

        # Final end-of-stream accounting cycles
        if maso is not None:
            maso.record_footprint(processor.memory_footprint())
            maso.record_cycle(received, processor, force_scan=True)

        if earm is not None:
            earm.run_eviction_cycle(received, processor, emit)

        elapsed_seconds = round(time.time() - start_time, 4)
        throughput = (
            round(received / elapsed_seconds, 2) if elapsed_seconds > 0 else 0.0
        )

        final_rss = get_current_rss_mb()
        peak_rss = max(peak_rss, final_rss)

        # Merge result rows: emitted (frozen) rows + hot/archive remaining rows
        merged_results = {}
        for r in emitted_rows:
            key = (r["start"], r["DeviceId"], r["Sensor"])
            merged_results[key] = r

        for r in processor.results():
            key = (r["start"], r["DeviceId"], r["Sensor"])
            merged_results[key] = r

        result_rows = sorted(
            merged_results.values(),
            key=lambda x: (x["start"], x["DeviceId"], x["Sensor"]),
        )

        snapshot = processor.snapshot()

        # Compute Ground Truth & Correctness
        gt_rows = compute_ground_truth_rows(
            events,
            window_size=self.window_size_seconds,
            slide=self.slide_seconds,
        )
        correctness = compare_rows(gt_rows, result_rows, tolerance=1e-6)

        avg_lateness = round(lateness_sum / received, 3) if received else 0.0

        metrics = {
            "mode": self.mode,
            "transport": self.transport,
            "transport_label": self.transport_label,
            "total_events": total_events,
            "received_events": received,
            "elapsed_seconds": elapsed_seconds,
            "throughput_ev_per_sec": throughput,
            "peak_rss_mb": peak_rss,
            "peak_hot": snapshot["peak_active_state_entries"],
            "final_hot": snapshot["active_state_entries"],
            "final_archive": snapshot["finalized_state_entries"],
            "frozen_emissions": snapshot["frozen_emissions"],
            "finalized_windows": snapshot["finalized_windows"],
            "reconciled_events": snapshot["reconciled_events"],
            "late_after_finalization": snapshot["late_after_finalization"],
            "late_after_eviction": snapshot["late_after_eviction"],
            "over_budget_events": snapshot["over_budget_events"],
            "ooo_events": ooo_events,
            "ooo_percentage": round((ooo_events / received) * 100.0, 2)
            if received
            else 0.0,
            "ooo_inversions": inversions,
            "max_lateness_seconds": lateness_max,
            "avg_lateness_seconds": avg_lateness,
        }

        return {
            "metrics": metrics,
            "correctness": correctness,
            "result_rows": result_rows,
            "ground_truth_rows_count": len(gt_rows),
            "series": {
                "throughput": throughput_series,
                "state": state_series,
                "lateness": lateness_series,
            },
        }
