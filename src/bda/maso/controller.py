from collections import deque
from statistics import mean


class MASOController:
    """
    Memory-Aware State Organization (MASO).

    MASO is the state-organisation layer that manages the multi-tier
    state hierarchy of the integrated processor:

        hot tier       windows still accepting in-budget events
        archive tier   retained compact summaries of sealed windows
        frozen tier    truly evicted windows (EARM) streamed to sink

    Responsibilities captured here:

      1. tier accounting (hot vs archive vs total logical state),
      2. periodic compaction/retention cycles that inspect the
         finalization frontier and report how much state is truly
         offloadable under a retention horizon,
      3. translating the logical state footprint into a
         memory-utilisation signal fed back to ALOA (state pressure),
      4. reporting state-reduction statistics for the experiment.

    MASO does not by itself discard data; in the exact-correctness mode
    archived summaries are retained for late-event reconciliation.  True
    release of archived memory is EARM's guarded decision.
    """

    def __init__(
        self,
        cycle_events: int = 20,
        retention_horizon_seconds: int = 60,
        history_size: int = 512,
        scan_every_cycles: int = 10,
    ):
        self.cycle_events = int(cycle_events)
        self.retention_horizon_seconds = int(
            retention_horizon_seconds
        )
        self.history_size = int(history_size)
        self.scan_every_cycles = max(1, int(scan_every_cycles))

        self.hot_history = deque(maxlen=history_size)
        self.archive_history = deque(maxlen=history_size)
        self.offloadable_history = deque(maxlen=history_size)

        self.peak_hot = 0
        self.peak_archive = 0
        self.peak_total = 0
        self.compaction_cycles = 0
        self.last_cycle_events_seen = 0

        self._current_offloadable = 0

    # ------------------------------------------------------------------
    # Accounting (called periodically by the driver, not per event)
    # ------------------------------------------------------------------

    def record_cycle(
        self,
        events_seen: int,
        processor,
        force_scan: bool = False,
    ) -> dict:
        """
        Run one retention/accounting cycle against the processor's
        current tier state.
        """
        hot = len(processor.state)
        archive = len(processor.finalized_state)
        total = hot + archive

        # Full offloadable scans are O(archive); throttle them so the
        # accounting layer stays cheap at stream scale.
        if (
            force_scan
            or self.compaction_cycles % self.scan_every_cycles == 0
            or self.compaction_cycles == 0
        ):
            offloadable = self._compute_offloadable(processor)
        else:
            offloadable = self._current_offloadable

        self.hot_history.append(hot)
        self.archive_history.append(archive)
        self.offloadable_history.append(offloadable)

        self.peak_hot = max(self.peak_hot, hot)
        self.peak_archive = max(self.peak_archive, archive)
        self.peak_total = max(self.peak_total, total)

        self._current_offloadable = offloadable
        self.compaction_cycles += 1
        self.last_cycle_events_seen = events_seen

        return self.accounting()

    def accounting(self) -> dict:
        return {
            "events_seen": self.last_cycle_events_seen,
            "hot_entries": (
                self.hot_history[-1] if self.hot_history else 0
            ),
            "archive_entries": (
                self.archive_history[-1]
                if self.archive_history
                else 0
            ),
            "total_entries": (
                (self.hot_history[-1] if self.hot_history else 0)
                + (self.archive_history[-1]
                   if self.archive_history else 0)
            ),
            "offloadable_entries": self._current_offloadable,
            "compaction_cycles": self.compaction_cycles,
        }

    def _compute_offloadable(self, processor) -> int:
        """
        Archived windows whose window end plus the retention horizon is
        already behind the watermark are offloadable candidates.  MASO
        reports them; EARM decides actual eviction.
        """
        watermark = processor.watermark

        if watermark is None:
            return 0

        horizon = self.retention_horizon_seconds
        count = 0

        for key in processor.finalized_state:
            if key[0] + processor.window_size + horizon <= watermark:
                count += 1

        return count

    # ------------------------------------------------------------------
    # Feedback signal for ALOA
    # ------------------------------------------------------------------

    def memory_utilization(self) -> float:
        """
        Translate the current state footprint into a bounded [0, 1]
        memory-pressure signal consumed by ALOA's budget policy.
        """
        memo = getattr(self, "_footprint_peak", None)

        if memo is None or memo.get("total_entries", 0) <= 0:
            return 0.0

        total = memo["total_entries"]
        peak = self.peak_total or total

        return min(1.0, total / (peak * 1.5))

    def record_footprint(self, footprint: dict) -> None:
        self._footprint_peak = footprint

    # ------------------------------------------------------------------
    # Statistics
    # ------------------------------------------------------------------

    def state_reduction_stats(
        self,
        baseline_peak_hot: int,
    ) -> dict:
        """
        State-reduction percentage of the adaptive mechanism relative
        to a baseline configuration.
        """
        if baseline_peak_hot <= 0:
            return {}

        peak_reduction = (
            (baseline_peak_hot - self.peak_hot)
            / baseline_peak_hot
            * 100
        )

        return {
            "baseline_peak_hot": baseline_peak_hot,
            "adaptive_peak_hot": self.peak_hot,
            "peak_hot_reduction_percent": round(peak_reduction, 4),
        }

    def snapshot(self) -> dict:
        return {
            "cycle_events": self.cycle_events,
            "retention_horizon_seconds": self.retention_horizon_seconds,
            "compaction_cycles": self.compaction_cycles,
            "peak_hot_entries": self.peak_hot,
            "peak_archive_entries": self.peak_archive,
            "peak_total_entries": self.peak_total,
            "final_offloadable_entries": self._current_offloadable,
            "mean_hot_entries": (
                round(mean(self.hot_history), 2)
                if self.hot_history
                else 0
            ),
            "mean_archive_entries": (
                round(mean(self.archive_history), 2)
                if self.archive_history
                else 0
            ),
            "final_hot_entries": (
                self.hot_history[-1] if self.hot_history else 0
            ),
            "final_archive_entries": (
                self.archive_history[-1]
                if self.archive_history
                else 0
            ),
        }