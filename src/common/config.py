from dataclasses import dataclass


@dataclass(frozen=True)
class WindowConfig:
    window_size_seconds: int
    slide_seconds: int
    allowed_lateness_seconds: int


DEFAULT_WINDOW_CONFIG = WindowConfig(
    window_size_seconds=60,
    slide_seconds=10,
    allowed_lateness_seconds=10,
)
