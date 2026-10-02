from dataclasses import dataclass


@dataclass(frozen=True)
class Event:
    event_id: str
    event_time: int
    device_id: str
    sensor: str
    value: float
    arrival_sequence: int
    scenario: str
