from __future__ import annotations

import json
import time
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from cfte.storage.parquet_schemas import RAW_EVENT_SCHEMA

class RawParquetWriter:
    def __init__(self, root: str | Path) -> None:
        self.root = Path(root)

    def write_event(
        self,
        source: str,
        stream_type: str,
        venue: str,
        instrument_key: str,
        payload: dict,
        event_id: str,
        venue_ts: int,
        seq_id: int | None = None,
        account_id: str | None = None,
        checksum: str | None = None,
    ) -> Path:
        now_ms = int(time.time() * 1000)
        day = time.strftime("%Y-%m-%d", time.gmtime(venue_ts / 1000.0))
        safe_symbol = instrument_key.replace(":", "_")
        out_dir = self.root / f"source={source}" / f"type={stream_type}" / f"date={day}" / f"instrument={safe_symbol}"
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / f"{event_id}.parquet"

        data = {
            "event_id": [event_id],
            "source": [source],
            "stream_type": [stream_type],
            "venue": [venue],
            "account_id": [account_id],
            "instrument_key": [instrument_key],
            "seq_id": [seq_id],
            "venue_ts": [venue_ts],
            "recv_ts": [now_ms],
            "normalize_ts": [now_ms],
            "payload_json": [json.dumps(payload, separators=(",", ":"))],
            "checksum": [checksum],
        }
        table = pa.Table.from_pydict(data, schema=RAW_EVENT_SCHEMA)
        pq.write_table(table, out_path)
        return out_path
