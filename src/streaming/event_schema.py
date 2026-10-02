from dataclasses import dataclass
import json


@dataclass(frozen=True)
class IoTEvent:
    event_id: str
    event_time: int
    device_id: str
    sensor: str
    value: float
    arrival_sequence: int
    scenario: str
    injected_delay_ms: int

    def to_dict(self) -> dict:
        return {
            "event_id": self.event_id,
            "event_time": self.event_time,
            "device_id": self.device_id,
            "sensor": self.sensor,
            "value": self.value,
            "arrival_sequence": self.arrival_sequence,
            "scenario": self.scenario,
            "injected_delay_ms": self.injected_delay_ms,
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), separators=(",", ":"))
