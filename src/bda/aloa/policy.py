from collections import deque
from statistics import quantiles


class ALOAPolicy:
    """
    Adaptive lateness policy for the integrated BDA framework.

    ALOA observes recent event lateness together with runtime
    conditions and derives a bounded allowed-lateness budget.

    This is the experimental policy layer; coefficients are explicit
    so they can be evaluated and tuned rather than hidden.
    """

    def __init__(
        self,
        initial_lateness_seconds=10,
        min_lateness_seconds=2,
        max_lateness_seconds=20,
        history_size=100,
        percentile=0.95,
        min_observations=100,
        adaptation_interval=100,
    ):
        if history_size <= 0:
            raise ValueError("history_size must be positive")

        if not 0 <= min_lateness_seconds <= max_lateness_seconds:
            raise ValueError("invalid lateness bounds")

        if not 0 < percentile < 1:
            raise ValueError("percentile must be between 0 and 1")

        if min_observations <= 0:
            raise ValueError("min_observations must be positive")

        if adaptation_interval <= 0:
            raise ValueError("adaptation_interval must be positive")

        if not (
            min_lateness_seconds
            <= initial_lateness_seconds
            <= max_lateness_seconds
        ):
            raise ValueError("initial lateness outside configured bounds")

        self.initial_lateness_seconds = initial_lateness_seconds
        self.min_lateness_seconds = min_lateness_seconds
        self.max_lateness_seconds = max_lateness_seconds
        self.history_size = history_size
        self.percentile = percentile
        self.min_observations = min_observations
        self.adaptation_interval = adaptation_interval
        self._total_observations = 0

        self._lateness = deque(maxlen=history_size)
        self._ooo_history = deque(maxlen=history_size)

        self._allowed_lateness = initial_lateness_seconds
        self._last_observation = {}

    @property
    def allowed_lateness_seconds(self):
        return self._allowed_lateness

    @property
    def observations(self):
        return len(self._lateness)

    def observe(
        self,
        lateness_seconds,
        out_of_order=False,
        arrival_rate=0.0,
        watermark_lag_seconds=0.0,
        state_size=0,
        memory_utilization=0.0,
    ):
        """
        Observe one stream condition and update the lateness budget.

        Parameters:
            lateness_seconds:
                Event lateness in seconds.

            out_of_order:
                Whether this event arrived out of event-time order.

            arrival_rate:
                Recent events/second.

            watermark_lag_seconds:
                Distance between current event-time progress and
                the processing watermark.

            state_size:
                Current CMiX state-entry count.

            memory_utilization:
                Runtime memory utilization as a fraction [0, 1].
        """
        lateness = max(0.0, float(lateness_seconds))
        arrival_rate = max(0.0, float(arrival_rate))
        watermark_lag = max(0.0, float(watermark_lag_seconds))
        state_size = max(0, int(state_size))
        memory_utilization = min(
            1.0,
            max(0.0, float(memory_utilization)),
        )

        self._lateness.append(lateness)
        self._ooo_history.append(bool(out_of_order))

        self._total_observations += 1

        self._last_observation = {
            "lateness_seconds": lateness,
            "out_of_order": bool(out_of_order),
            "arrival_rate": arrival_rate,
            "watermark_lag_seconds": watermark_lag,
            "state_size": state_size,
            "memory_utilization": memory_utilization,
        }

        # Keep the configured initial budget until enough
        # observations exist to make a meaningful decision.
        if self._total_observations < self.min_observations:
            return self._allowed_lateness

        # Recalculate only periodically to avoid excessive
        # event-by-event budget changes.
        if (
            self._total_observations == self.min_observations
            or self._total_observations % self.adaptation_interval == 0
        ):
            self._allowed_lateness = self._calculate_budget(
                arrival_rate=arrival_rate,
                watermark_lag_seconds=watermark_lag,
                state_size=state_size,
                memory_utilization=memory_utilization,
            )

        return self._allowed_lateness

    def _lateness_percentile(self):
        values = sorted(self._lateness)

        if not values:
            return float(self.initial_lateness_seconds)

        if len(values) == 1:
            return values[0]

        index = int(self.percentile * 100) - 1

        return quantiles(
            values,
            n=100,
            method="inclusive",
        )[index]

    def _ooo_ratio(self):
        if not self._ooo_history:
            return 0.0

        return sum(self._ooo_history) / len(self._ooo_history)

    def _calculate_budget(
        self,
        arrival_rate,
        watermark_lag_seconds,
        state_size,
        memory_utilization,
    ):
        """
        Experimental multi-signal adjustment.

        The recent lateness percentile is the primary signal.
        OOO, watermark lag, arrival pressure, state pressure, and
        memory pressure provide bounded adjustments.

        All adjustments are deliberately small relative to the
        configured lateness range.
        """
        base = self._lateness_percentile()

        ooo_ratio = self._ooo_ratio()

        # OOO pressure: up to +20% of the configured range.
        ooo_adjustment = (
            ooo_ratio * 0.20 * self.max_lateness_seconds
        )

        # Watermark pressure: up to +10%.
        watermark_adjustment = min(
            0.10 * self.max_lateness_seconds,
            watermark_lag_seconds * 0.10,
        )

        # Arrival-rate pressure: intentionally conservative.
        arrival_adjustment = min(
            0.10 * self.max_lateness_seconds,
            arrival_rate / 1000.0,
        )

        # State pressure: conservative adjustment.
        state_adjustment = min(
            0.10 * self.max_lateness_seconds,
            state_size / 10000.0,
        )

        # Memory pressure reduces the lateness budget because retaining
        # more state becomes increasingly expensive.
        memory_reduction = max(
            0.0,
            memory_utilization - 0.70,
        ) * self.max_lateness_seconds

        raw_budget = (
            base
            + ooo_adjustment
            + watermark_adjustment
            + arrival_adjustment
            + state_adjustment
            - memory_reduction
        )

        budget = round(raw_budget)

        return int(
            max(
                self.min_lateness_seconds,
                min(self.max_lateness_seconds, budget),
            )
        )

    def snapshot(self):
        return {
            "observations": len(self._lateness),
            "total_observations": self._total_observations,
            "allowed_lateness_seconds": self._allowed_lateness,
            "min_observations": self.min_observations,
            "adaptation_interval": self.adaptation_interval,
            "lateness_percentile": self._lateness_percentile(),
            "ooo_ratio": self._ooo_ratio(),
            "min_lateness_seconds": self.min_lateness_seconds,
            "max_lateness_seconds": self.max_lateness_seconds,
            "history_size": self.history_size,
            "percentile": self.percentile,
            "last_observation": self._last_observation,
        }
