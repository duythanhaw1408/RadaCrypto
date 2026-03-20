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
    latency_ms: int | None = None
    is_stale: bool = False
    last_message_ts: int | None = None
    idle_gap_seconds: float | None = None
    notes: tuple[str, ...] = ()

    def to_operator_summary(self) -> str:
        status_tag = "[OK]"
        if self.is_stale:
            status_tag = "[STALE]"
        elif not self.connected:
            status_tag = "[DISCONNECTED]"

        state_label = {
            "idle": "chưa kết nối",
            "running": "đang chạy",
            "degraded": "đang suy giảm",
        }[self.state]
        
        latency_str = f"latency={self.latency_ms}ms" if self.latency_ms is not None else "latency=N/A"
        freshness_str = (
            f"idle_gap={self.idle_gap_seconds:.1f}s"
            if self.idle_gap_seconds is not None
            else "idle_gap=N/A"
        )
        
        if self.last_error is not None:
            msg = getattr(self.last_error, 'message', str(self.last_error))
            detail = f"Lỗi gần nhất: {msg}."
        elif self.last_disconnect_reason is not None:
            detail = f"Lý do reconnect gần nhất: {self.last_disconnect_reason.message}."
        else:
            detail = "Chưa ghi nhận lỗi collector."

        notes_detail = ""
        if self.notes:
            notes_detail = f" Ghi chú: {'; '.join(self.notes)}."
            
        return (
            f"{status_tag} Collector {self.venue.upper()} {state_label}; {latency_str}; {freshness_str}; "
            f"kết nối={self.connected}; lần thử={self.connect_attempts}; "
            f"reconnect={self.reconnect_count}; message={self.message_count}. {detail}{notes_detail}"
        )


def build_error_surface(exc: Exception) -> CollectorErrorSurface:
    message = str(exc).strip() or exc.__class__.__name__
    kind = exc.__class__.__name__.lower()
    return CollectorErrorSurface(kind=kind, message=message, exception_type=exc.__class__.__name__)
