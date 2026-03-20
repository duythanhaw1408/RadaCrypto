#!/usr/bin/env python3
import json
import time
from pathlib import Path

FIXTURE = Path("fixtures/replay/btcusdt_normalized.jsonl")

def reprime():
    if not FIXTURE.exists():
        print(f"Không tìm xấy fixture tại {FIXTURE}")
        return

    lines = FIXTURE.read_text(encoding="utf-8").strip().splitlines()
    if not lines: return
    
    events = [json.loads(l) for l in lines]
    last_venue_ts = events[-1]["venue_ts"]
    now_ms = int(time.time() * 1000)
    offset = now_ms - last_venue_ts
    
    for ev in events:
        if "venue_ts" in ev: ev["venue_ts"] += offset
        if "timestamp" in ev: ev["timestamp"] += offset
        if "event_ts" in ev: ev["event_ts"] += offset
        
    with FIXTURE.open("w", encoding="utf-8") as f:
        for ev in events:
            f.write(json.dumps(ev) + "\n")
            
    print(f"✅ Đã dời {len(events)} sự kiện sang thời điểm hiện tại (offset: {offset}ms)")

if __name__ == "__main__":
    reprime()
