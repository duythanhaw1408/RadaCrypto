from __future__ import annotations

import pyarrow as pa

RAW_EVENT_SCHEMA = pa.schema(
    [
        pa.field("event_id", pa.string()),
        pa.field("source", pa.string()),
        pa.field("stream_type", pa.string()),
        pa.field("venue", pa.string()),
        pa.field("account_id", pa.string()),
        pa.field("instrument_key", pa.string()),
        pa.field("seq_id", pa.int64()),
        pa.field("venue_ts", pa.int64()),
        pa.field("recv_ts", pa.int64()),
        pa.field("normalize_ts", pa.int64()),
        pa.field("payload_json", pa.string()),
        pa.field("checksum", pa.string()),
    ]
)

FEATURE_SNAPSHOT_SCHEMA = pa.schema(
    [
        pa.field("instrument_key", pa.string()),
        pa.field("window_start_ts", pa.int64()),
        pa.field("window_end_ts", pa.int64()),
        pa.field("spread_bps", pa.float64()),
        pa.field("microprice", pa.float64()),
        pa.field("imbalance_l1", pa.float64()),
        pa.field("delta_quote", pa.float64()),
        pa.field("cvd", pa.float64()),
        pa.field("trade_burst", pa.float64()),
        pa.field("absorption_proxy", pa.float64()),
    ]
)
