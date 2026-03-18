from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass
from typing import Any

import requests


BINANCE_WS_BASE = "wss://stream.binance.com:9443/stream"
BINANCE_REST_BASE = "https://api.binance.com"
DEFAULT_DEPTH_LEVEL = 1000


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

    @property
    def url(self) -> str:
        joined = "/".join(self.streams)
        return f"{self.ws_base}?streams={joined}"

    async def stream_forever(self):
        while True:
            try:
                import websockets

                async with websockets.connect(self.url, ping_interval=20, ping_timeout=20) as ws:
                    async for raw in ws:
                        envelope = json.loads(raw)
                        yield envelope
            except Exception:
                await asyncio.sleep(self.reconnect_sleep_seconds)
