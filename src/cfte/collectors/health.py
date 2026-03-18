from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

CollectorState = Literal["idle", "running", "degraded"]


@dataclass(frozen=True, slots=True)
class CollectorErrorSurface:
    kind: str
    message: str
    exception_type: str


@dataclass(frozen=True, slots=True)
class CollectorHealthSnapshot:
    venue: str
    state: CollectorState
    connected: bool
    connect_attempts: int
    reconnect_count: int
    message_count: int
    last_disconnect_reason: CollectorErrorSurface | None
    last_error: CollectorErrorSurface | None

    def to_operator_summary(self) -> str:
        state_label = {
            "idle": "chưa kết nối",
            "running": "đang chạy",
            "degraded": "đang suy giảm",
        }[self.state]
        if self.last_error is not None:
            detail = f"Lỗi gần nhất: {self.last_error.message}."
        elif self.last_disconnect_reason is not None:
            detail = f"Lý do reconnect gần nhất: {self.last_disconnect_reason.message}."
        else:
            detail = "Chưa ghi nhận lỗi collector."
        return (
            f"Collector {self.venue.upper()} {state_label}; "
            f"kết nối={self.connected}; lần thử={self.connect_attempts}; "
            f"reconnect={self.reconnect_count}; message={self.message_count}. {detail}"
        )


def build_error_surface(exc: Exception) -> CollectorErrorSurface:
    message = str(exc).strip() or exc.__class__.__name__
    kind = exc.__class__.__name__.lower()
    return CollectorErrorSurface(kind=kind, message=message, exception_type=exc.__class__.__name__)
