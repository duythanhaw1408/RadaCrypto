from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass

OKX_WS_BASE = "wss://ws.okx.com:8443/ws/v5/public"


def build_public_args(inst_ids: list[str]) -> list[dict[str, str]]:
    args: list[dict[str, str]] = []
    for inst_id in inst_ids:
        upper = inst_id.upper()
        args.append({"channel": "trades", "instId": upper})
        args.append({"channel": "bbo-tbt", "instId": upper})
    return args


@dataclass(slots=True)
class OkxPublicCollector:
    args: list[dict[str, str]]
    ws_base: str = OKX_WS_BASE
    reconnect_sleep_seconds: float = 3.0

    def subscription_message(self) -> dict[str, object]:
        return {"op": "subscribe", "args": self.args}

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
