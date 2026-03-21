from __future__ import annotations

import asyncio
import json
import time
from dataclasses import dataclass, field

from cfte.collectors.health import CollectorErrorSurface, CollectorHealthSnapshot, CollectorState, build_error_surface

import ssl
import certifi

OKX_WS_BASE = "wss://ws.okx.com:8443/ws/v5/public"
OKX_VENUE = "okx"

OKX_REST_MIRRORS = [
    "https://www.okx.com",
    "https://aws.okex.com",
]
OKX_REST_BASE = OKX_REST_MIRRORS[0]

COMMON_USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36",
]

def build_public_args(inst_ids: list[str]) -> list[dict[str, str]]:
    args: list[dict[str, str]] = []
    for inst_id in inst_ids:
        upper = inst_id.upper()
        args.append({"channel": "trades", "instId": upper})
        args.append({"channel": "bbo-tbt", "instId": upper})
    return args


def fetch_depth_snapshot(symbol: str, limit: int = 50, rest_base: str = OKX_REST_BASE) -> dict[str, object]:
    """Fetch L2 orderbook from OKX V5."""
    import requests
    import random
    
    # OKX format is BTC-USDT
    if "-" not in symbol:
        symbol = f"{symbol[:3]}-{symbol[3:]}"
        
    url = f"{rest_base}/api/v5/market/books"
    params = {"instId": symbol.upper(), "sz": limit}
    headers = {"User-Agent": random.choice(COMMON_USER_AGENTS)}
    
    resp = requests.get(url, params=params, headers=headers, timeout=5)
    resp.raise_for_status()
    data = resp.json()
    
    if data.get("code") != "0":
        raise ValueError(f"OKX API error: {data.get('msg')}")
        
    result = data.get("data", [])
    if not result:
        return {}
    return result[0]


def try_fetch_depth_snapshot(
    symbol: str,
    limit: int = 50,
    rest_base: str = OKX_REST_BASE,
) -> tuple[dict[str, object] | None, str | None]:
    """Try multiple mirrors to fetch OKX depth snapshot."""
    import requests
    mirrors = [rest_base] + [m for m in OKX_REST_MIRRORS if m != rest_base]
    last_error = None
    
    for mirror in mirrors:
        try:
            snapshot = fetch_depth_snapshot(symbol=symbol, limit=limit, rest_base=mirror)
            return snapshot, None
        except requests.RequestException as exc:
            last_error = exc
            print(f"⚠️ OKX Mirror failed ({mirror}): {exc}")
            continue
            
    return None, f"Không lấy được snapshot depth OKX cho {symbol.upper()} qua các mirrors: {last_error}"

@dataclass(slots=True)
class OkxPublicCollector:
    args: list[dict[str, str]]
    ws_base: str = OKX_WS_BASE
    reconnect_sleep_seconds: float = 3.0
    _state: CollectorState = field(default="idle", init=False, repr=False)
    _connected: bool = field(default=False, init=False, repr=False)
    _connect_attempts: int = field(default=0, init=False, repr=False)
    _reconnect_count: int = field(default=0, init=False, repr=False)
    _message_count: int = field(default=0, init=False, repr=False)
    _last_message_ts: int | None = field(default=None, init=False, repr=False)
    _last_disconnect_reason: CollectorErrorSurface | None = field(default=None, init=False, repr=False)
    _last_error: CollectorErrorSurface | None = field(default=None, init=False, repr=False)

    def subscription_message(self) -> dict[str, object]:
        return {"op": "subscribe", "args": self.args}

    def health_snapshot(self) -> CollectorHealthSnapshot:
        idle_gap_seconds = None
        is_stale = False
        if self._last_message_ts is not None:
            idle_gap_seconds = max(0.0, time.time() - (self._last_message_ts / 1000.0))
            is_stale = idle_gap_seconds > 15.0
        return CollectorHealthSnapshot(
            venue=OKX_VENUE,
            state=self._state,
            connected=self._connected,
            connect_attempts=self._connect_attempts,
            reconnect_count=self._reconnect_count,
            message_count=self._message_count,
            last_disconnect_reason=self._last_disconnect_reason,
            last_error=self._last_error,
            is_stale=is_stale,
            last_message_ts=self._last_message_ts,
            idle_gap_seconds=idle_gap_seconds,
        )

    def _mark_connected(self) -> None:
        self._connected = True
        self._state = "running"
        self._last_error = None

    def _record_message(self) -> None:
        self._message_count += 1
        self._last_message_ts = int(time.time() * 1000)

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
                ssl_context = ssl.create_default_context(cafile=certifi.where())
                ssl_context.check_hostname = True
                ssl_context.verify_mode = ssl.CERT_REQUIRED

                self._connect_attempts += 1
                async with websockets.connect(
                    self.ws_base, 
                    ssl=ssl_context, 
                    ping_interval=None, # We handle OKX ping manually
                ) as ws:
                    self._mark_connected()
                    await ws.send(json.dumps(self.subscription_message()))
                    
                    # OKX requires manual 'ping' string
                    async def _heartbeat():
                        while self._connected:
                            try:
                                await ws.send("ping")
                                await asyncio.sleep(20)
                            except:
                                break
                    
                    hb_task = asyncio.create_task(_heartbeat())
                    try:
                        async for raw in ws:
                            if raw == "pong":
                                continue
                            self._record_message()
                            yield json.loads(raw)
                    finally:
                        hb_task.cancel()
            except Exception as exc:
                self._record_failure(exc)
                await asyncio.sleep(self.reconnect_sleep_seconds)

                self._connect_attempts += 1
                async with websockets.connect(self.ws_base, ssl=ssl_context, ping_interval=20, ping_timeout=20) as ws:
                    self._mark_connected()
                    await ws.send(json.dumps(self.subscription_message()))
                    async for raw in ws:
                        self._record_message()
                        yield json.loads(raw)
            except Exception as exc:
                self._record_failure(exc)
                await asyncio.sleep(self.reconnect_sleep_seconds)
