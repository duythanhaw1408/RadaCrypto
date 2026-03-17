from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass

import websockets

@dataclass(slots=True)
class BinancePublicCollector:
    ws_base: str
    streams: list[str]

    @property
    def url(self) -> str:
        joined = "/".join(self.streams)
        return f"{self.ws_base}?streams={joined}"

    async def stream_forever(self):
        while True:
            try:
                async with websockets.connect(self.url, ping_interval=20, ping_timeout=20) as ws:
                    async for raw in ws:
                        yield json.loads(raw)
            except Exception:
                await asyncio.sleep(3)
