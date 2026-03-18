import asyncio

import requests

from cfte.cli.main import run_binance_public_ingest
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


def test_run_binance_public_ingest_fails_gracefully_when_snapshot_unavailable(monkeypatch, tmp_path, capsys):
    monkeypatch.setattr(
        "cfte.collectors.binance_public.try_fetch_depth_snapshot",
        lambda symbol, limit=1000, rest_base=None: (None, f"Không lấy được snapshot depth Binance cho {symbol}: timeout"),
    )

    exit_code = asyncio.run(
        run_binance_public_ingest(symbol="BTCUSDT", out_dir=tmp_path, max_events=1, use_agg_trade=True)
    )

    captured = capsys.readouterr()
    assert exit_code == 1
    assert "Không lấy được snapshot depth Binance cho BTCUSDT" in captured.out
