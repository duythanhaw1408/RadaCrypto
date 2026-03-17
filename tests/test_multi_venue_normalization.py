import pytest
from cfte.models.events import NormalizedTrade
from cfte.normalizers.bybit import normalize_orderbook_top, normalize_public_trade
from cfte.normalizers.instruments import build_cross_venue_map, map_okx_inst_id
from cfte.normalizers.okx import normalize_bbo_tbt, normalize_trade


def test_bybit_trade_and_book_normalization():
    instrument_key = "BYBIT:BTCUSDT:PERP"
    trade = {"T": 1700000001000, "S": "Buy", "p": "40000", "v": "0.25", "i": "abc"}
    book = {
        "topic": "orderbook.1.BTCUSDT",
        "type": "snapshot",
        "ts": 1700000001005,
        "data": {"s": "BTCUSDT", "b": [["39999.5", "12.5"]], "a": [["40000.0", "11.2"]], "u": 10},
    }

    normalized_trade = normalize_public_trade(trade, instrument_key)
    normalized_book = normalize_orderbook_top(book, instrument_key)

    assert normalized_trade.taker_side == "BUY"
    assert normalized_trade.quote_qty == 10000.0
    assert normalized_book.bid_px == 39999.5
    assert normalized_book.ask_qty == 11.2


def test_okx_trade_and_bbo_normalization():
    instrument_key = "OKX:BTCUSDT:PERP"
    trade = {"instId": "BTC-USDT-SWAP", "tradeId": "1", "px": "40001", "sz": "0.1", "side": "sell", "ts": "1700000002000"}
    bbo = {
        "instId": "BTC-USDT-SWAP",
        "bids": [["40000.5", "9.0", "0", "3"]],
        "asks": [["40001.0", "8.2", "0", "2"]],
        "ts": "1700000002100",
    }

    normalized_trade = normalize_trade(trade, instrument_key)
    normalized_book = normalize_bbo_tbt(bbo, instrument_key)

    assert normalized_trade.taker_side == "SELL"
    assert normalized_trade.quote_qty == pytest.approx(4000.1)
    assert normalized_book.ask_px == 40001.0


def test_cross_venue_mapping_supports_binance_bybit_okx_perp():
    mapping = build_cross_venue_map(binance_symbol="BTCUSDT", bybit_symbol="BTCUSDT", okx_inst_id="BTC-USDT-SWAP", market_type="PERP")

    assert mapping.canonical_key == "BTCUSDT:PERP"
    assert mapping.instruments["binance"].instrument_key == "BINANCE:BTCUSDT:PERP"
    assert mapping.instruments["bybit"].instrument_key == "BYBIT:BTCUSDT:PERP"
    assert mapping.instruments["okx"].instrument_key == "OKX:BTCUSDT:PERP"


def test_map_okx_inst_id_spot():
    mapped = map_okx_inst_id("ETH-USDT")
    assert mapped.instrument_key == "OKX:ETHUSDT:SPOT"


def test_cross_venue_mapping_validates_market_type_mismatch():
    with pytest.raises(ValueError):
        build_cross_venue_map(binance_symbol="BTCUSDT", bybit_symbol="BTCUSDT", okx_inst_id="BTC-USDT", market_type="PERP")
