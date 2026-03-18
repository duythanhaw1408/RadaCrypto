from __future__ import annotations

import argparse
import asyncio
from pathlib import Path


def doctor() -> int:
    required = [
        Path("sql/sqlite/001_state.sql"),
        Path("sql/sqlite/002_indexes.sql"),
        Path("configs/profiles/swing_perp.yaml"),
        Path("src/cfte/books/local_book.py"),
        Path("src/cfte/features/tape.py"),
        Path("src/cfte/thesis/engines.py"),
    ]
    missing = [str(p) for p in required if not p.exists()]
    if missing:
        print("Thiếu các tệp bắt buộc:")
        for item in missing:
            print(f" - {item}")
        return 1
    print("Doctor OK: các tệp lõi đã sẵn sàng.")
    return 0


async def run_binance_public_ingest(symbol: str, out_dir: Path, max_events: int, use_agg_trade: bool) -> int:
    from cfte.books.binance_depth import BinanceDepthReconciler
    from cfte.collectors.binance_public import BinancePublicCollector, build_public_streams, try_fetch_depth_snapshot
    from cfte.normalizers.binance import (
        normalize_agg_trade,
        normalize_book_ticker,
        normalize_depth_diff,
        normalize_kline,
        normalize_trade,
    )
    from cfte.storage.raw_writer import RawParquetWriter

    instrument_key = f"BINANCE:{symbol.upper()}:SPOT"
    writer = RawParquetWriter(out_dir)
    depth = BinanceDepthReconciler(instrument_key=instrument_key)

    print(f"Khởi tạo snapshot sổ lệnh cho {symbol.upper()}...")
    snapshot, error = try_fetch_depth_snapshot(symbol=symbol.upper())
    if snapshot is None:
        print(error or f"Không lấy được snapshot sổ lệnh cho {symbol.upper()}.")
        return 1

    depth.apply_snapshot(
        bids=[(float(px), float(qty)) for px, qty in snapshot.get("bids", [])],
        asks=[(float(px), float(qty)) for px, qty in snapshot.get("asks", [])],
        last_update_id=int(snapshot["lastUpdateId"]),
    )

    streams = build_public_streams([symbol], use_agg_trade=use_agg_trade)
    collector = BinancePublicCollector(streams=streams)

    print(f"Bắt đầu ingest Binance public ({symbol.upper()}). Sẽ dừng sau {max_events} sự kiện.")
    processed = 0
    async for envelope in collector.stream_forever():
        stream = str(envelope.get("stream", ""))
        data = envelope.get("data", {})
        if not isinstance(data, dict):
            continue

        event_type = str(data.get("e", ""))
        if event_type == "aggTrade":
            normalized = normalize_agg_trade(data, instrument_key=instrument_key)
            writer.write_event("binance_public", "aggTrade", "binance", instrument_key, data, normalized.event_id, normalized.venue_ts)
        elif event_type == "trade":
            normalized = normalize_trade(data, instrument_key=instrument_key)
            writer.write_event("binance_public", "trade", "binance", instrument_key, data, normalized.event_id, normalized.venue_ts)
        elif event_type == "bookTicker":
            normalized = normalize_book_ticker(data, instrument_key=instrument_key)
            writer.write_event("binance_public", "bookTicker", "binance", instrument_key, data, normalized.event_id, normalized.venue_ts)
        elif event_type == "depthUpdate":
            normalized = normalize_depth_diff(data, instrument_key=instrument_key)
            writer.write_event(
                "binance_public",
                "depth",
                "binance",
                instrument_key,
                data,
                normalized.event_id,
                normalized.venue_ts,
                seq_id=normalized.final_update_id,
            )
            depth.ingest_diff(normalized)
        elif event_type == "kline":
            normalized = normalize_kline(data, instrument_key=instrument_key)
            writer.write_event("binance_public", f"kline_{normalized.interval}", "binance", instrument_key, data, normalized.event_id, normalized.venue_ts)
        else:
            continue

        processed += 1
        if processed % 10 == 0:
            print(f"Đã ghi {processed} sự kiện. Stream gần nhất: {stream}")
        if processed >= max_events:
            break

    print(f"Hoàn tất ingest. Tổng sự kiện đã ghi: {processed}.")
    return 0


def run_replay_research(events_path: Path, summary_out: Path) -> int:
    from cfte.replay.adapters import load_replay_events
    from cfte.replay.runner import persist_replay_summary, render_replay_summary_vi, run_replay

    events = load_replay_events(events_path)
    result = run_replay(events)
    persist_replay_summary(result, summary_out)
    print(render_replay_summary_vi(result))
    print(f"Đã lưu tóm tắt replay tại: {summary_out}")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(prog="cfte")
    sub = parser.add_subparsers(dest="cmd")
    sub.add_parser("doctor")

    ingest = sub.add_parser("binance-public-ingest")
    ingest.add_argument("--symbol", default="BTCUSDT")
    ingest.add_argument("--out", default="data/raw")
    ingest.add_argument("--max-events", type=int, default=25)
    ingest.add_argument("--use-trade", action="store_true", help="Dùng stream trade thay vì aggTrade")

    replay = sub.add_parser("replay-research")
    replay.add_argument("--events", default="fixtures/replay/btcusdt_normalized.jsonl")
    replay.add_argument("--summary-out", default="data/replay/summary_btcusdt.json")

    args = parser.parse_args()

    if args.cmd == "doctor":
        return doctor()
    if args.cmd == "binance-public-ingest":
        return asyncio.run(
            run_binance_public_ingest(
                symbol=args.symbol,
                out_dir=Path(args.out),
                max_events=args.max_events,
                use_agg_trade=not args.use_trade,
            )
        )
    if args.cmd == "replay-research":
        return run_replay_research(events_path=Path(args.events), summary_out=Path(args.summary_out))

    parser.print_help()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
