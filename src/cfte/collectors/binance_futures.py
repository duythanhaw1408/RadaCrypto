import requests
from typing import Any, Dict, Optional
from datetime import datetime, timezone

class BinanceFuturesCollector:
    """Simple REST collector for Binance Futures public data (Mark Price, Funding, OI)"""
    
    BASE_URL = "https://fapi.binance.com"
    
    def __init__(self, symbol: str = "BTCUSDT"):
        self.symbol = symbol.upper()
        self._last_oi: Optional[float] = None
        self._last_ts: Optional[int] = None

    def fetch_mark_price_info(self) -> Dict[str, Any]:
        """Fetches mark price, index price, and funding rate"""
        url = f"{self.BASE_URL}/fapi/v1/premiumIndex"
        params = {"symbol": self.symbol}
        try:
            resp = requests.get(url, params=params, timeout=10)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            print(f"⚠️ Error fetching Binance Futures Premium Index: {e}")
            return {}

    def fetch_open_interest(self) -> Dict[str, Any]:
        """Fetches current open interest"""
        url = f"{self.BASE_URL}/fapi/v1/openInterest"
        params = {"symbol": self.symbol}
        try:
            resp = requests.get(url, params=params, timeout=10)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            print(f"⚠️ Error fetching Binance Futures OI: {e}")
            return {}

    def get_live_context(self) -> Dict[str, Any]:
        """Aggregates all context data for TPFM"""
        mark_info = self.fetch_mark_price_info()
        oi_info = self.fetch_open_interest()
        
        if not mark_info or not oi_info:
            return {"available": False}
        
        current_oi = float(oi_info.get("openInterest", 0.0))
        oi_delta = 0.0
        if self._last_oi is not None:
            oi_delta = current_oi - self._last_oi
            
        self._last_oi = current_oi
        self._last_ts = int(datetime.now(timezone.utc).timestamp() * 1000)
        
        # Calculate basis in bps: (Mark - Index) / Index * 10000
        mark_px = float(mark_info.get("markPrice", 0.0))
        index_px = float(mark_info.get("indexPrice", 0.0))
        basis_bps = 0.0
        if index_px > 0:
            basis_bps = ((mark_px - index_px) / index_px) * 10000.0
            
        return {
            "available": True,
            "fresh": True,
            "timestamp": self._last_ts,
            "mark_price": mark_px,
            "index_price": index_px,
            "basis_bps": basis_bps,
            "funding_rate": float(mark_info.get("lastFundingRate", 0.0)),
            "oi_value": current_oi,
            "oi_delta": oi_delta
        }
