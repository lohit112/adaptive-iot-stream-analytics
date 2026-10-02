from collections import deque
from statistics import mean


class EARMController:
    """
    Event-time-driven Adaptive Retention Management (EARM).

    EARM turns the finalization frontier produced by ALOA + CMiX into a
    guarded memory-release decision and audits its safety.

    A retained (archived) window summary may be truly evicted only when

        window_end + guard_seconds <= watermark

    where the guard represents the maximum lateness that can still
    arrive for a window that is already behind the watermark.  Two guard
    policies are supported:

      - "fixed":     guard is a constant configured by the experimenter.
      - "adaptive":  guard = max(budget, maximum_observed_lateness) with
                     rounded-up headroom, i.e. the empirically observed
                     disorder bound.  When observed lateness stays
                     within the guard, eviction is exact.

    EARM also tracks:

      - truly evicted (frozen) entries and the memory released,
      - late events that arrived after a window was finalized
        (reconciled into the archive) vs after it was truly evicted
        (correctness-affecting, counted and reported),
      - the eviction frontier and guard usage over time.
    """

    def __init__(
        self,
        guard_policy: str = "fixed",
        guard_seconds: int = 20,
        cycle_events: int = 20,
        history_size: int = 256,
    ):
        if guard_policy not in ("fixed", "adaptive"):
            raise ValueError(
                "guard_policy must be 'fixed' or 'adaptive'"
            )

        self.guard_policy = guard_policy
        self.guard_seconds = int(guard_seconds)
        self.cycle_events = int(cycle_events)
        self.history_size = int(history_size)

        self.observed_max_lateness = 0.0
        self.observed_max_overflow = 0.0
        self._budget = 0

        self.evicted_entries = 0
        self.eviction_cycles = 0
        self.reconciled_events = 0
        self.late_after_eviction = 0

        self.guard_history = deque(maxlen=history_size)
        self.evicted_history = deque(maxlen=history_size)

        self._last_events_seen = 0

    # ------------------------------------------------------------------
    # Runtime signals
    # ------------------------------------------------------------------

    def observe_event(
        self,
        lateness_seconds: float,
        allowed_lateness_seconds: int,
        over_budget: bool,
    ) -> None:
        lateness = max(0.0, float(lateness_seconds))
        budget = max(0, int(allowed_lateness_seconds))

        self._budget = budget

        self.observed_max_lateness = max(
            self.observed_max_lateness,
            lateness,
        )

        if over_budget and budget > 0:
            overflow = lateness - budget
            self.observed_max_overflow = max(
                self.observed_max_overflow,
                overflow,
            )

        self.reconciled_events += int(over_budget or lateness > budget)

    @property
    def effective_guard_seconds(self) -> int:
        if self.guard_policy == "fixed":
            return self.guard_seconds

        budget = max(self._budget, 1)
        bound = max(
            self._budget,
            self.observed_max_lateness + 1.0,
            self.observed_max_overflow + self._budget,
        )

        return int(max(budget, bound))

    def run_eviction_cycle(
        self,
        events_seen: int,
        processor,
        emit_callback,
    ) -> dict:
        """
        Compute the guarded eviction frontier and freeze the safe set.
        Returns a cycle record for accounting.
        """
        guard = self.effective_guard_seconds

        evictable = processor.evictable_keys(guard)

        evicted = 0

        if evictable:
            evicted = processor.freeze_and_emit(evictable)

        self.evicted_entries += evicted
        self.eviction_cycles += 1
        self._last_events_seen = events_seen

        self.guard_history.append(guard)
        self.evicted_history.append(evicted)

        return {
            "events_seen": events_seen,
            "guard_seconds": guard,
            "evictable_candidates": len(evictable),
            "evicted": evicted,
        }

    def snapshot(self) -> dict:
        return {
            "guard_policy": self.guard_policy,
            "configured_guard_seconds": self.guard_seconds,
            "effective_guard_seconds": self.effective_guard_seconds,
            "observed_max_lateness": round(
                self.observed_max_lateness, 3
            ),
            "observed_max_overflow": round(
                self.observed_max_overflow, 3
            ),
            "eviction_cycles": self.eviction_cycles,
            "evicted_entries": self.evicted_entries,
            "mean_guard_seconds": (
                round(mean(self.guard_history), 2)
                if self.guard_history
                else self.effective_guard_seconds
            ),
            "total_evicted_entries": self.evicted_entries,
            "late_after_eviction_events": self.late_after_eviction,
        }