from dataclasses import dataclass

from src.bda.aloa.policy import ALOAPolicy


@dataclass(frozen=True)
class ALOAObservation:
    event_time: int
    lateness_seconds: float
    out_of_order: bool
    arrival_rate: float
    watermark_lag_seconds: float
    state_size: int
    memory_utilization: float
    allowed_lateness_seconds: int


class ALOAController:
    """
    Event-level controller for ALOA.

    The controller observes arrival-order disorder and forwards
    runtime signals to ALOAPolicy.

    CMiX is deliberately not modified here.
    """

    def __init__(
        self,
        policy=None,
        window_size_seconds=60,
    ):
        self.policy = policy or ALOAPolicy()

        self.window_size_seconds = window_size_seconds

        self._max_event_time = None
        self._events_seen = 0
        self._ooo_events = 0

        self._first_event_time = None
        self._last_event_time = None

    @property
    def max_event_time(self):
        return self._max_event_time

    @property
    def events_seen(self):
        return self._events_seen

    @property
    def ooo_events(self):
        return self._ooo_events

    @property
    def ooo_ratio(self):
        if self._events_seen == 0:
            return 0.0

        return self._ooo_events / self._events_seen

    def observe_event(
        self,
        event_time,
        state_size=0,
        memory_utilization=0.0,
    ):
        """
        Observe one arriving event.

        Lateness is measured relative to the maximum event-time
        observed so far.
        """

        event_time = int(event_time)

        if self._max_event_time is None:
            self._max_event_time = event_time
            self._first_event_time = event_time

        lateness = max(
            0,
            self._max_event_time - event_time,
        )

        out_of_order = lateness > 0

        if out_of_order:
            self._ooo_events += 1

        self._events_seen += 1

        self._last_event_time = event_time

        if event_time > self._max_event_time:
            self._max_event_time = event_time

        arrival_rate = self._arrival_rate()

        watermark_lag = self._watermark_lag()

        budget = self.policy.observe(
            lateness_seconds=lateness,
            out_of_order=out_of_order,
            arrival_rate=arrival_rate,
            watermark_lag_seconds=watermark_lag,
            state_size=state_size,
            memory_utilization=memory_utilization,
        )

        return ALOAObservation(
            event_time=event_time,
            lateness_seconds=float(lateness),
            out_of_order=out_of_order,
            arrival_rate=arrival_rate,
            watermark_lag_seconds=watermark_lag,
            state_size=int(state_size),
            memory_utilization=float(memory_utilization),
            allowed_lateness_seconds=int(budget),
        )

    def _arrival_rate(self):
        """
        Approximate event rate using event-time span.

        This is an experimental metric and is intentionally kept
        separate from Kafka's wall-clock throughput.
        """

        if self._events_seen <= 1:
            return 0.0

        span = self._max_event_time - self._first_event_time

        if span <= 0:
            return float(self._events_seen)

        return self._events_seen / float(span)

    def _watermark_lag(self):
        """
        Current event-time lag relative to the latest observed
        event-time position.
        """

        if (
            self._max_event_time is None
            or self._last_event_time is None
        ):
            return 0.0

        return max(
            0.0,
            float(self._max_event_time - self._last_event_time),
        )

    def snapshot(self):
        return {
            "events_seen": self._events_seen,
            "ooo_events": self._ooo_events,
            "ooo_ratio": self.ooo_ratio,
            "max_event_time": self._max_event_time,
            "allowed_lateness_seconds": (
                self.policy.allowed_lateness_seconds
            ),
            "policy": self.policy.snapshot(),
        }
