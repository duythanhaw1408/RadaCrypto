from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass

BYBIT_WS_BASE = "wss://stream.bybit.com/v5/public/linear"


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

    def subscription_message(self) -> dict[str, object]:
        return {"op": "subscribe", "args": self.topics}

    async def stream_forever(self):
        while True:
            try:
                import websockets

                async with websockets.connect(self.ws_base, ping_interval=20, ping_timeout=20) as ws:
                    await ws.send(json.dumps(self.subscription_message()))
                    async for raw in ws:
                        yield json.loads(raw)
            except Exception:
                await asyncio.sleep(self.reconnect_sleep_seconds)
