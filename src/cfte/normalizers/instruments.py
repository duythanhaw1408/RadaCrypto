from __future__ import annotations

from dataclasses import dataclass


SUPPORTED_MARKET_TYPES = {"SPOT", "PERP"}


@dataclass(frozen=True, slots=True)
class VenueInstrument:
    venue: str
    raw_symbol: str
    unified_symbol: str
    market_type: str

    @property
    def instrument_key(self) -> str:
        return f"{self.venue.upper()}:{self.unified_symbol}:{self.market_type}"


@dataclass(frozen=True, slots=True)
class CrossVenueInstrumentMap:
    canonical_key: str
    instruments: dict[str, VenueInstrument]


def _normalize_compact_symbol(raw_symbol: str) -> str:
    return raw_symbol.replace("-", "").replace("_", "").replace("/", "").upper()


def map_binance_symbol(symbol: str, market_type: str = "SPOT") -> VenueInstrument:
    market = market_type.upper()
    if market not in SUPPORTED_MARKET_TYPES:
        raise ValueError(f"Unsupported market_type: {market_type}")
    unified = _normalize_compact_symbol(symbol)
    return VenueInstrument(venue="binance", raw_symbol=symbol, unified_symbol=unified, market_type=market)


def map_bybit_symbol(symbol: str, market_type: str = "PERP") -> VenueInstrument:
    market = market_type.upper()
    if market not in SUPPORTED_MARKET_TYPES:
        raise ValueError(f"Unsupported market_type: {market_type}")
    unified = _normalize_compact_symbol(symbol)
    return VenueInstrument(venue="bybit", raw_symbol=symbol, unified_symbol=unified, market_type=market)


def map_okx_inst_id(inst_id: str) -> VenueInstrument:
    parts = inst_id.upper().split("-")
    if len(parts) < 2:
        raise ValueError(f"Invalid OKX instId: {inst_id}")

    base, quote = parts[0], parts[1]
    unified = f"{base}{quote}"
    market_type = "PERP" if "SWAP" in parts else "SPOT"
    return VenueInstrument(venue="okx", raw_symbol=inst_id, unified_symbol=unified, market_type=market_type)


def build_cross_venue_map(binance_symbol: str, bybit_symbol: str, okx_inst_id: str, market_type: str = "PERP") -> CrossVenueInstrumentMap:
    market = market_type.upper()
    if market not in SUPPORTED_MARKET_TYPES:
        raise ValueError(f"Unsupported market_type: {market_type}")

    binance = map_binance_symbol(binance_symbol, market_type=market)
    bybit = map_bybit_symbol(bybit_symbol, market_type=market)
    okx = map_okx_inst_id(okx_inst_id)
    if okx.market_type != market:
        raise ValueError(f"OKX instId market_type mismatch: expected {market}, got {okx.market_type}")

    canonical_key = f"{binance.unified_symbol}:{market}"
    return CrossVenueInstrumentMap(
        canonical_key=canonical_key,
        instruments={
            "binance": binance,
            "bybit": bybit,
            "okx": okx,
        },
    )


def parse_instrument_key(instrument_key: str) -> tuple[str, str, str]:
    parts = instrument_key.split(":")
    if len(parts) != 3:
        raise ValueError(f"Invalid instrument_key: {instrument_key}")
    return parts[0].lower(), parts[1].upper(), parts[2].upper()
