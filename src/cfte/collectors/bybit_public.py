from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass, field

from cfte.collectors.health import CollectorErrorSurface, CollectorHealthSnapshot, CollectorState, build_error_surface

BYBIT_WS_BASE = "wss://stream.bybit.com/v5/public/linear"
BYBIT_VENUE = "bybit"


def build_public_topics(symbols: list[str]) -> list[str]:
    topics: list[str] = []
    for symbol in symbols:
        upper = symbol.upper().replace("-", "")
        topics.append(f"publicTrade.{upper}")
        topics.append(f"orderbook.1.{upper}")
    return topics


@dataclass(slots=True)
class BybitPublicCollector:
    topics: list[str]
    ws_base: str = BYBIT_WS_BASE
    reconnect_sleep_seconds: float = 3.0
    _state: CollectorState = field(default="idle", init=False, repr=False)
    _connected: bool = field(default=False, init=False, repr=False)
    _connect_attempts: int = field(default=0, init=False, repr=False)
    _reconnect_count: int = field(default=0, init=False, repr=False)
    _message_count: int = field(default=0, init=False, repr=False)
    _last_disconnect_reason: CollectorErrorSurface | None = field(default=None, init=False, repr=False)
    _last_error: CollectorErrorSurface | None = field(default=None, init=False, repr=False)

    def subscription_message(self) -> dict[str, object]:
        return {"op": "subscribe", "args": self.topics}

    def health_snapshot(self) -> CollectorHealthSnapshot:
        return CollectorHealthSnapshot(
            venue=BYBIT_VENUE,
            state=self._state,
            connected=self._connected,
            connect_attempts=self._connect_attempts,
            reconnect_count=self._reconnect_count,
            message_count=self._message_count,
            last_disconnect_reason=self._last_disconnect_reason,
            last_error=self._last_error,
        )

    def _mark_connected(self) -> None:
        self._connected = True
        self._state = "running"
        self._last_error = None

    def _record_message(self) -> None:
        self._message_count += 1

    def _record_failure(self, exc: Exception) -> None:
        error = build_error_surface(exc)
        self._connected = False
        self._state = "degraded"
        self._reconnect_count += 1
        self._last_disconnect_reason = error
        self._last_error = error

    async def stream_forever(self):
        while True:
            try:
                import websockets

                self._connect_attempts += 1
                async with websockets.connect(self.ws_base, ping_interval=20, ping_timeout=20) as ws:
                    self._mark_connected()
                    await ws.send(json.dumps(self.subscription_message()))
                    async for raw in ws:
                        self._record_message()
                        yield json.loads(raw)
            except Exception as exc:
                self._record_failure(exc)
                await asyncio.sleep(self.reconnect_sleep_seconds)
