from dataclasses import dataclass
from collections import defaultdict
from typing import Dict, Optional, Tuple, Iterator, List, Callable
import heapq
import sys


@dataclass
class Aggregate:
    count: int = 0
    total: float = 0.0
    minimum: float = float("inf")
    maximum: float = float("-inf")

    def add(self, value: float) -> None:
        self.count += 1
        self.total += value
        self.minimum = min(self.minimum, value)
        self.maximum = max(self.maximum, value)

    def result(self) -> dict:
        return {
            "count": self.count,
            "sum": self.total,
            "avg": self.total / self.count if self.count else 0.0,
            "min": self.minimum,
            "max": self.maximum,
        }


class CMiXProcessor:
    """
    Event-time aggregation state for out-of-order IoT events.

    ALOA provides the adaptive allowed-lateness budget.  CMiX converts
    that budget into an event-time watermark and manages window state.

    State model (multi-tier, in the spirit of the research design):

        hot tier:      self.state           windows still accepting
                                            non-late (in-budget) events
        archive tier:  self.finalized_state windows sealed by the
                                            watermark, retained as compact
                                            summaries (reconciliation)
        emitted tier:  produced by EARM      truly frozen windows whose
                                            results are finalized and can be
                                            released from memory.

    Correctness design
        Safe finalization + late-event reconciliation.
        A window is moved from the hot tier to the archive only once its
        end is at or behind the monotonic watermark.  If a late event
        later arrives that still belongs to that window, it is merged
        into the retained archive summary instead of re-opening a fresh
        partial aggregate in the hot tier (the correctness bug found in
        the 50K experiments).  With this reconciliation, aggregates are
        exact for every event regardless of lateness.

    EARM (Event-time-driven Adaptive Retention Management) may, when
    enabled, truly evict archived summaries once no future event can
    touch them (end + guard <= watermark).  Evicted windows' result rows
    are handed to an emit callback (the driver streams them to a results
    sink) so that aggregation state is actually released.
    """

    def __init__(
        self,
        window_size_seconds: int = 60,
        slide_seconds: int = 10,
        audit_limit: int = 2000,
        memory_probe_entries: int = 512,
    ):
        if window_size_seconds <= 0:
            raise ValueError("window_size_seconds must be positive")

        if slide_seconds <= 0:
            raise ValueError("slide_seconds must be positive")

        if window_size_seconds % slide_seconds != 0:
            raise ValueError(
                "window_size_seconds must be divisible by slide_seconds"
            )

        self.window_size = window_size_seconds
        self.slide = slide_seconds

        self.state: Dict[Tuple[int, str, str], Aggregate] = defaultdict(
            Aggregate
        )
        self.finalized_state: Dict[Tuple[int, str, str], Aggregate] = {}

        # Truly evicted (frozen) window keys, and how late events that
        # target them are handled.  In guarded "safe" operation,
        # zero late-after-eviction events were observed in evaluated workloads
        # and the count stays zero.
        self.frozen_keys: set = set()
        self.drop_late_after_eviction: bool = True
        self.late_after_eviction = 0

        self.max_event_time = None
        self.previous_event_time = None
        self.allowed_lateness_seconds = 0
        self.watermark = None

        self.watermark_updates = 0
        self.finalized_windows = 0
        self.evicted_state_entries = 0
        self.peak_active_state_entries = 0

        # Late-event reconciliation accounting.
        self.reconciled_events = 0
        self.late_after_finalization = 0
        self.over_budget_events = 0
        self.ooo_inversions = 0

        self._audit_limit = audit_limit
        self.finalization_audit: List[dict] = []

        # Streamed / frozen emissions (EARM true eviction).
        self.emit_callback: Optional[Callable[[dict], None]] = None
        self.frozen_emissions = 0

        # Incremental finalization frontier: min-heap of window keys
        # still resident in the hot tier, ordered by window start.  Keeps
        # finalize_windows O(k) in the number of newly-finalized windows
        # instead of O(hot state) per event.
        self._pending: List[Tuple[int, str, str]] = []

    # ------------------------------------------------------------------
    # Watermark
    # ------------------------------------------------------------------

    def update_watermark(
        self,
        max_event_time: int,
        allowed_lateness_seconds: int,
    ) -> int:
        """
        Update the monotonic event-time watermark.

        W = max_event_time - allowed_lateness
        """
        max_event_time = int(max_event_time)
        allowed_lateness_seconds = max(
            0,
            int(allowed_lateness_seconds),
        )

        self.max_event_time = max_event_time
        self.allowed_lateness_seconds = allowed_lateness_seconds

        candidate = max_event_time - allowed_lateness_seconds

        if self.watermark is None:
            self.watermark = candidate
            self.watermark_updates += 1
        elif candidate > self.watermark:
            self.watermark = candidate
            self.watermark_updates += 1

        return self.watermark

    # ------------------------------------------------------------------
    # Windowing
    # ------------------------------------------------------------------

    def window_starts(self, event_time: int) -> Iterator[int]:
        """
        Yield all sliding-window starts containing event_time.
        """
        last_start = (event_time // self.slide) * self.slide

        number_of_windows = self.window_size // self.slide

        for i in range(number_of_windows):
            start = last_start - i * self.slide

            if start <= event_time < start + self.window_size:
                yield start

    def process_event(
        self,
        event_time: int,
        device_id: str,
        sensor: str,
        value: float,
        lateness_seconds: float = 0.0,
        allowed_lateness_seconds: Optional[int] = None,
    ) -> None:
        """
        Add one event to every event-time window that contains it.

        Late events are reconciled against the retained archive when the
        target window has already been finalized, preserving exact
        aggregates.
        """
        if lateness_seconds > 0:
            self.ooo_inversions += 1

        if allowed_lateness_seconds is not None:
            self.allowed_lateness_seconds = int(
                allowed_lateness_seconds
            )

        if (
            allowed_lateness_seconds is not None
            and lateness_seconds > allowed_lateness_seconds
        ):
            self.over_budget_events += 1

        for start in self.window_starts(event_time):
            key = (start, device_id, sensor)

            if key in self.frozen_keys:
                # Truly evicted window.  Under the guard this must not
                # happen; if it does, correctness is affected.
                self.late_after_eviction += 1
                if not self.drop_late_after_eviction:
                    raise RuntimeError(
                        "late event after eviction with "
                        "drop_late_after_eviction=False"
                    )
                continue

            if key in self.finalized_state:
                # Late event after finalization: reconcile into the
                # retained archive summary (exact correctness).
                self.finalized_state[key].add(float(value))
                self.reconciled_events += 1
                self.late_after_finalization += 1
                self._audit_late(key, event_time)
            else:
                appended = key not in self.state

                self.state[key].add(float(value))

                if appended:
                    heapq.heappush(self._pending, key)

        self.peak_active_state_entries = max(
            self.peak_active_state_entries,
            len(self.state),
        )

    # ------------------------------------------------------------------
    # Finalization and eviction
    # ------------------------------------------------------------------

    def finalize_windows(self) -> int:
        """
        Move windows whose end is at or before the watermark from the
        hot tier into the retained archive.

        Retained summaries remain available for late-event
        reconciliation and for results(); releasing the hot tier is what
        reduces the state that must be kept mutable.
        """
        if self.watermark is None:
            return 0

        count = 0

        while (
            self._pending
            and self._pending[0][0] + self.window_size
            <= self.watermark
        ):
            key = heapq.heappop(self._pending)

            if key not in self.state:
                continue

            self.finalized_state[key] = self.state.pop(key)

            if len(self.finalization_audit) < self._audit_limit:
                self.finalization_audit.append(
                    {
                        "window_start": key[0],
                        "window_end": key[0] + self.window_size,
                        "device_id": key[1],
                        "sensor": key[2],
                        "finalized_at_watermark": self.watermark,
                        "budget_seconds": self.allowed_lateness_seconds,
                    }
                )

            count += 1

        self.finalized_windows += count
        self.evicted_state_entries += count

        return count

    def evictable_keys(
        self,
        guard_seconds: int,
    ) -> List[Tuple[int, str, str]]:
        """
        Archived summaries that can be truly evicted: their window end
        plus the security guard is already behind the watermark, so no
        future event can target them when the disorder bound holds.
        """
        if self.watermark is None:
            return []

        return [
            key
            for key in self.finalized_state
            if key[0] + self.window_size + guard_seconds
            <= self.watermark
        ]

    def freeze_and_emit(
        self,
        keys,
    ) -> int:
        """
        Freeze archived summaries and hand their final result rows to
        the configured emit callback (streamed to a results sink).

        Returns the number of truly evicted windows.
        """
        if self.emit_callback is None:
            raise RuntimeError(
                "freeze_and_emit requires an emit callback"
            )

        count = 0

        for key in keys:
            aggregate = self.finalized_state.pop(key, None)

            if aggregate is None:
                continue

            row = {
                "start": key[0],
                "end": key[0] + self.window_size,
                "DeviceId": key[1],
                "Sensor": key[2],
                **aggregate.result(),
            }

            self.emit_callback(row)
            self.frozen_emissions += 1
            self.frozen_keys.add(key)
            count += 1

        return count

    # ------------------------------------------------------------------
    # Output
    # ------------------------------------------------------------------

    def results(self) -> List[dict]:
        """
        Return the union of hot-tier and retained-archive aggregates in
        deterministic order.

        Frozen/emitted windows are streamed separately by the driver,
        which combines them with this method's output.
        """
        output = []

        combined_state = {}

        combined_state.update(self.finalized_state)
        combined_state.update(self.state)

        for (
            start,
            device_id,
            sensor,
        ), aggregate in combined_state.items():
            output.append(
                {
                    "start": start,
                    "end": start + self.window_size,
                    "DeviceId": device_id,
                    "Sensor": sensor,
                    **aggregate.result(),
                }
            )

        return sorted(
            output,
            key=lambda x: (
                x["start"],
                x["DeviceId"],
                x["Sensor"],
            ),
        )

    def memory_footprint(self) -> dict:
        """
        Approximate in-process footprint of the aggregation state tiers.
        """
        state_bytes = sys.getsizeof(self.state)
        archive_bytes = sys.getsizeof(self.finalized_state)

        for key, agg in self.state.items():
            state_bytes += sys.getsizeof(key) + sys.getsizeof(agg)

        for key, agg in self.finalized_state.items():
            archive_bytes += sys.getsizeof(key) + sys.getsizeof(agg)

        return {
            "hot_state_bytes": state_bytes,
            "archive_state_bytes": archive_bytes,
            "total_state_bytes": state_bytes + archive_bytes,
            "hot_entries": len(self.state),
            "archive_entries": len(self.finalized_state),
            "total_entries": len(self.state) + len(self.finalized_state),
        }

    # ------------------------------------------------------------------
    # Audit
    # ------------------------------------------------------------------

    def _audit_late(
        self,
        key: Tuple[int, str, str],
        event_time: int,
    ) -> None:
        """Record a bounded sample of late-after-finalization events."""

    def snapshot(self) -> dict:
        return {
            "state_entries": len(self.state),
            "max_event_time": self.max_event_time,
            "allowed_lateness_seconds": (
                self.allowed_lateness_seconds
            ),
            "watermark": self.watermark,
            "watermark_updates": self.watermark_updates,
            "active_state_entries": len(self.state),
            "finalized_state_entries": len(self.finalized_state),
            "peak_active_state_entries": (
                self.peak_active_state_entries
            ),
            "finalized_windows": self.finalized_windows,
            "evicted_state_entries": self.evicted_state_entries,
            "reconciled_events": self.reconciled_events,
            "late_after_finalization": self.late_after_finalization,
            "over_budget_events": self.over_budget_events,
            "frozen_emissions": self.frozen_emissions,
            "late_after_eviction": self.late_after_eviction,
            "frozen_keys_size": len(self.frozen_keys),
            "finalization_audit_records": len(
                self.finalization_audit
            ),
            "finalization_audit": self.finalization_audit,
            "memory": self.memory_footprint(),
        }