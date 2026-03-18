from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass, field
from typing import Any

import requests

from cfte.collectors.health import CollectorErrorSurface, CollectorHealthSnapshot, CollectorState, build_error_surface

BINANCE_WS_BASE = "wss://stream.binance.com:9443/stream"
BINANCE_REST_BASE = "https://api.binance.com"
DEFAULT_DEPTH_LEVEL = 1000
BINANCE_VENUE = "binance"


def build_public_streams(symbols: list[str], use_agg_trade: bool = True) -> list[str]:
    streams: list[str] = []
    trade_stream = "aggTrade" if use_agg_trade else "trade"
    kline_intervals = ["1m", "15m", "1h", "4h"]

    for symbol in symbols:
        symbol_l = symbol.lower()
        streams.append(f"{symbol_l}@{trade_stream}")
        streams.append(f"{symbol_l}@bookTicker")
        streams.append(f"{symbol_l}@depth@100ms")
        for interval in kline_intervals:
            streams.append(f"{symbol_l}@kline_{interval}")
    return streams


def fetch_depth_snapshot(symbol: str, limit: int = DEFAULT_DEPTH_LEVEL, rest_base: str = BINANCE_REST_BASE) -> dict[str, Any]:
    url = f"{rest_base}/api/v3/depth"
    response = requests.get(url, params={"symbol": symbol.upper(), "limit": limit}, timeout=10)
    response.raise_for_status()
    return response.json()


def try_fetch_depth_snapshot(
    symbol: str,
    limit: int = DEFAULT_DEPTH_LEVEL,
    rest_base: str = BINANCE_REST_BASE,
) -> tuple[dict[str, Any] | None, str | None]:
    try:
        snapshot = fetch_depth_snapshot(symbol=symbol, limit=limit, rest_base=rest_base)
    except requests.RequestException as exc:
        return None, f"Không lấy được snapshot depth Binance cho {symbol.upper()}: {exc}"
    except (KeyError, TypeError, ValueError) as exc:
        return None, f"Snapshot depth Binance không hợp lệ cho {symbol.upper()}: {exc}"
    return snapshot, None


@dataclass(slots=True)
class BinancePublicCollector:
    streams: list[str]
    ws_base: str = BINANCE_WS_BASE
    reconnect_sleep_seconds: float = 3.0
    _state: CollectorState = field(default="idle", init=False, repr=False)
    _connected: bool = field(default=False, init=False, repr=False)
    _connect_attempts: int = field(default=0, init=False, repr=False)
    _reconnect_count: int = field(default=0, init=False, repr=False)
    _message_count: int = field(default=0, init=False, repr=False)
    _last_disconnect_reason: CollectorErrorSurface | None = field(default=None, init=False, repr=False)
    _last_error: CollectorErrorSurface | None = field(default=None, init=False, repr=False)

    @property
    def url(self) -> str:
        joined = "/".join(self.streams)
        return f"{self.ws_base}?streams={joined}"

    def health_snapshot(self) -> CollectorHealthSnapshot:
        return CollectorHealthSnapshot(
            venue=BINANCE_VENUE,
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
                import ssl
                
                # Create unverified SSL context for macOS compatibility in personal use
                ssl_context = ssl.create_default_context()
                ssl_context.check_hostname = False
                ssl_context.verify_mode = ssl.CERT_NONE

                self._connect_attempts += 1
                async with websockets.connect(self.url, ping_interval=20, ping_timeout=20, ssl=ssl_context) as ws:
                    self._mark_connected()
                    print(f"WS Connected to {self.url}")
                    async for raw in ws:
                        self._record_message()
                        # print(f"DEBUG RAW: {raw[:100]}")
                        envelope = json.loads(raw)
                        yield envelope
            except Exception as exc:
                self._record_failure(exc)
                print(f"WS Error: {exc}")
                await asyncio.sleep(self.reconnect_sleep_seconds)
