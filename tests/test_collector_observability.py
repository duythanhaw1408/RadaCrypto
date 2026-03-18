import asyncio
import json
import types

from cfte.collectors.bybit_public import BybitPublicCollector
from cfte.collectors.okx_public import OkxPublicCollector


class _FakeWebSocket:
    def __init__(self, messages):
        self._messages = iter(messages)
        self.sent = []

    async def send(self, payload):
        self.sent.append(json.loads(payload))

    def __aiter__(self):
        return self

    async def __anext__(self):
        try:
            return next(self._messages)
        except StopIteration:
            raise StopAsyncIteration


class _FakeConnect:
    def __init__(self, plan):
        self._plan = iter(plan)

    def __call__(self, *args, **kwargs):
        step = next(self._plan)
        return _FakeSession(step)


class _FakeSession:
    def __init__(self, step):
        self._step = step

    async def __aenter__(self):
        if isinstance(self._step, Exception):
            raise self._step
        return self._step

    async def __aexit__(self, exc_type, exc, tb):
        return False


async def _read_one(collector):
    async for event in collector.stream_forever():
        return event
    raise AssertionError('collector did not yield')


def test_bybit_health_snapshot_tracks_failure_and_reconnect(monkeypatch):
    collector = BybitPublicCollector(topics=['publicTrade.BTCUSDT'], reconnect_sleep_seconds=0)

    async def _fast_sleep(*args, **kwargs):
        return None

    monkeypatch.setattr(asyncio, 'sleep', _fast_sleep)
    fake_websockets = types.SimpleNamespace(
        connect=_FakeConnect([
            RuntimeError('dial tcp timeout'),
            _FakeWebSocket(['{"topic":"publicTrade.BTCUSDT"}']),
        ])
    )
    monkeypatch.setitem(__import__('sys').modules, 'websockets', fake_websockets)

    event = asyncio.run(_read_one(collector))
    snapshot = collector.health_snapshot()

    assert event['topic'] == 'publicTrade.BTCUSDT'
    assert snapshot.venue == 'bybit'
    assert snapshot.state == 'running'
    assert snapshot.connected is True
    assert snapshot.connect_attempts == 2
    assert snapshot.reconnect_count == 1
    assert snapshot.message_count == 1
    assert snapshot.last_disconnect_reason is not None
    assert snapshot.last_disconnect_reason.message == 'dial tcp timeout'
    assert snapshot.last_error is None
    assert 'Lý do reconnect gần nhất' in snapshot.to_operator_summary()


def test_okx_health_snapshot_reports_latest_failure_reason():
    collector = OkxPublicCollector(args=[{'channel': 'trades', 'instId': 'BTC-USDT-SWAP'}])

    collector._record_failure(ConnectionError('remote host closed connection'))
    snapshot = collector.health_snapshot()

    assert snapshot.venue == 'okx'
    assert snapshot.state == 'degraded'
    assert snapshot.connected is False
    assert snapshot.connect_attempts == 0
    assert snapshot.reconnect_count == 1
    assert snapshot.message_count == 0
    assert snapshot.last_disconnect_reason is not None
    assert snapshot.last_disconnect_reason.exception_type == 'ConnectionError'
    assert snapshot.last_error is not None
    assert snapshot.last_error.message == 'remote host closed connection'
    assert 'Lỗi gần nhất' in snapshot.to_operator_summary()
