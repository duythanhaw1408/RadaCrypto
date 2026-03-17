import pytest
from cfte.normalizers.binance import (
    normalize_agg_trade,
    normalize_book_ticker,
    normalize_depth_diff,
    normalize_kline,
    normalize_trade,
)


def test_normalize_agg_trade():
    msg = {"e": "aggTrade", "p": "100.5", "q": "2.0", "m": True, "T": 1700000001000}
    event = normalize_agg_trade(msg, "BINANCE:BTCUSDT:SPOT")
    assert event.taker_side == "SELL"
    assert event.price == 100.5
    assert event.quote_qty == 201.0


def test_normalize_trade():
    msg = {"e": "trade", "p": "99.9", "q": "3.0", "m": False, "T": 1700000002000}
    event = normalize_trade(msg, "BINANCE:BTCUSDT:SPOT")
    assert event.taker_side == "BUY"
    assert event.qty == 3.0
    assert event.quote_qty == pytest.approx(299.7)


def test_normalize_book_ticker():
    msg = {"e": "bookTicker", "b": "100.0", "B": "1.5", "a": "100.2", "A": "2.1", "E": 1700000003000}
    event = normalize_book_ticker(msg, "BINANCE:BTCUSDT:SPOT")
    assert event.bid_px == 100.0
    assert event.ask_qty == 2.1


def test_normalize_depth_diff():
    msg = {
        "e": "depthUpdate",
        "U": 101,
        "u": 110,
        "b": [["100.0", "1.0"]],
        "a": [["100.2", "0.5"]],
        "E": 1700000004000,
    }
    event = normalize_depth_diff(msg, "BINANCE:BTCUSDT:SPOT")
    assert event.first_update_id == 101
    assert event.final_update_id == 110
    assert event.bid_updates == [(100.0, 1.0)]


def test_normalize_kline():
    msg = {
        "e": "kline",
        "E": 1700000005000,
        "k": {
            "i": "1m",
            "o": "100",
            "h": "101",
            "l": "99",
            "c": "100.2",
            "v": "120.5",
            "q": "12100",
            "t": 1700000000000,
            "T": 1700000059999,
            "x": True,
        },
    }
    event = normalize_kline(msg, "BINANCE:BTCUSDT:SPOT")
    assert event.interval == "1m"
    assert event.is_closed is True
    assert event.close_px == 100.2
