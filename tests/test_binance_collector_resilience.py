import asyncio

import requests

from cfte.collectors.binance_public import try_fetch_depth_snapshot


class _BoomResponse:
    def raise_for_status(self):
        raise requests.HTTPError("503 Server Error")


def test_try_fetch_depth_snapshot_returns_vietnamese_error_on_request_failure(monkeypatch):
    def _fake_get(*args, **kwargs):
        return _BoomResponse()

    monkeypatch.setattr(requests, "get", _fake_get)

    snapshot, error = try_fetch_depth_snapshot("BTCUSDT")

    assert snapshot is None
    assert error is not None
    assert "Không lấy được snapshot depth Binance" in error
    assert "BTCUSDT" in error


def test_try_fetch_depth_snapshot_handles_timeout(monkeypatch):
    """Verify Vietnamese error message on network timeout."""

    def _timeout_get(*args, **kwargs):
        raise requests.exceptions.Timeout("Connection timed out")

    monkeypatch.setattr(requests, "get", _timeout_get)

    snapshot, error = try_fetch_depth_snapshot("ETHUSDT")

    assert snapshot is None
    assert error is not None
    assert "ETHUSDT" in error
