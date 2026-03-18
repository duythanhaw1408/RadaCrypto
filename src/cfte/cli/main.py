from __future__ import annotations

import argparse
import asyncio
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml


DEFAULT_PROFILE_PATH = Path("configs/profiles/personal_binance.yaml")
DEFAULT_REPLAY_EVENTS = Path("fixtures/replay/btcusdt_normalized.jsonl")
DEFAULT_REPLAY_SUMMARY = Path("data/replay/summary_btcusdt.json")
DEFAULT_RAW_DIR = Path("data/raw")
DEFAULT_STATE_DB = Path("data/state/state.db")
DEFAULT_THESIS_LOG = Path("data/thesis/thesis_log.jsonl")


@dataclass(frozen=True, slots=True)
class PersonalProfile:
    name: str
    locale: str
    trader: dict[str, Any]
    defaults: dict[str, Any]
    scan: dict[str, Any]
    live: dict[str, Any]
    review: dict[str, Any]
    outcomes: dict[str, Any]


@dataclass(frozen=True, slots=True)
class ShellContext:
    profile_path: Path
    profile: PersonalProfile


def _read_yaml(path: Path) -> dict[str, Any]:
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError(f"Nội dung YAML không hợp lệ: {path}")
    return data


def load_personal_profile(profile_path: Path) -> PersonalProfile:
    data = _read_yaml(profile_path)
    return PersonalProfile(
        name=str(data.get("profile", "personal-default")),
        locale=str(data.get("locale", "vi-VN")),
        trader=dict(data.get("trader", {})),
        defaults=dict(data.get("defaults", {})),
        scan=dict(data.get("scan", {})),
        live=dict(data.get("live", {})),
        review=dict(data.get("review", {})),
        outcomes=dict(data.get("outcomes", {"horizons": ["1h", "4h", "24h"]})),
    )


def build_context(profile_path: str | Path) -> ShellContext:
    path = Path(profile_path)
    return ShellContext(profile_path=path, profile=load_personal_profile(path))


def _resolve_path(value: str | Path | None, fallback: Path) -> Path:
    return Path(value) if value is not None else fallback


def _profile_path(profile: PersonalProfile, section: str, key: str, fallback: Path) -> Path:
    if section == "defaults":
        value = profile.defaults.get(key)
    else:
        section_map = getattr(profile, section)
        value = section_map.get(key)
    return Path(str(value)) if value is not None else fallback


def _format_header(title: str, profile: PersonalProfile) -> str:
    trader_name = profile.trader.get("display_name", "Trader")
    return f"=== {title} | hồ sơ: {profile.name} | người dùng: {trader_name} ==="


def doctor(context: ShellContext) -> int:
    required = [
        Path("sql/sqlite/001_state.sql"),
        Path("sql/sqlite/002_indexes.sql"),
        context.profile_path,
        Path("src/cfte/books/local_book.py"),
        Path("src/cfte/features/tape.py"),
        Path("src/cfte/thesis/engines.py"),
        DEFAULT_REPLAY_EVENTS,
        Path("configs/profiles/personal_binance.yaml"),
        Path("configs/profiles/personal_binance_onchain.yaml"),
        Path("configs/profiles/personal_replay.yaml"),
    ]
    missing = [str(p) for p in required if not p.exists()]
    print(_format_header("doctor", context.profile))
    if missing:
        print("Phát hiện thiếu tệp bắt buộc để chạy luồng cá nhân:")
        for item in missing:
            print(f" - {item}")
        print("Gợi ý: hoàn thiện tệp còn thiếu rồi chạy lại `cfte doctor`.")
        return 1

    print("Hệ thống lõi đã sẵn sàng cho luồng local-first, replay-first.")
    print(f"- Hồ sơ đang dùng: {context.profile_path}")
    print(f"- Cặp mặc định: {context.profile.defaults.get('symbol', 'BTCUSDT')}")
    print(f"- Replay mặc định: {context.profile.defaults.get('replay_events', str(DEFAULT_REPLAY_EVENTS))}")
    print("Luồng khuyến nghị: doctor -> run-scan -> run-live -> review-day.")
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


def _load_replay_result(events_path: Path):
    from cfte.replay.adapters import load_replay_events
    from cfte.replay.runner import run_replay

    events = load_replay_events(events_path)
    return run_replay(events)


def command_replay(context: ShellContext, events_path: Path, summary_out: Path) -> int:
    print(_format_header("replay", context.profile))
    print(f"Đang chạy replay từ: {events_path}")
    return run_replay_research(events_path=events_path, summary_out=summary_out)


def command_run_scan(context: ShellContext, events_path: Path, limit: int | None) -> int:
    from cfte.thesis.cards import render_trader_card

    print(_format_header("run-scan", context.profile))
    result = _load_replay_result(events_path)
    actionable_threshold = float(context.profile.scan.get("actionable_threshold", 75.0))
    candidates = [signal for signal in result.thesis_events if signal.score >= actionable_threshold]
    target_limit = limit or context.profile.scan.get("max_cards", 3)
    shown = candidates[:target_limit]

    print(f"Đã quét replay cho {result.instrument_key} với {result.feature_windows} cửa sổ đặc trưng.")
    print(f"Ngưỡng ưu tiên từ hồ sơ cá nhân: {actionable_threshold:.2f} điểm.")
    if not shown:
        print("Chưa có thiết lập nào vượt ngưỡng ưu tiên. Hãy tiếp tục theo dõi watchlist.")
        return 0

    print(f"Có {len(shown)} thiết lập đáng chú ý để trader xem nhanh:")
    for index, signal in enumerate(shown, start=1):
        print(f"\n--- Ứng viên #{index} ---")
        print(render_trader_card(signal))
    return 0


def command_run_live(context: ShellContext, symbol: str | None, max_events: int | None, use_trade: bool) -> int:
    from cfte.live.engine import LiveThesisLoop

    print(_format_header("run-live", context.profile))
    live_defaults = context.profile.live
    target_symbol = (symbol or live_defaults.get("symbol") or context.profile.defaults.get("symbol") or "BTCUSDT")
    target_max_events = int(max_events or live_defaults.get("max_events", 25))
    db_path = DEFAULT_STATE_DB

    print(f"Bắt đầu bộ máy live thesis cho {target_symbol}...")
    print(f"- Cơ sở dữ liệu trạng thái: {db_path}")
    print(f"- Giới hạn sự kiện: {target_max_events}")

    loop = LiveThesisLoop(
        symbol=str(target_symbol),
        db_path=db_path,
        use_agg_trade=not use_trade,
        horizons=context.profile.outcomes.get("horizons"),
    )

    try:
        asyncio.run(loop.run_forever(max_events=target_max_events))
    except KeyboardInterrupt:
        print("\nĐã nhận tín hiệu dừng từ người dùng.")
    except Exception as exc:
        print(f"Lỗi thực thi loop: {exc}")
        return 1

    print(f"Hoàn tất phiên live cho {target_symbol}.")
    return 0


def command_review_day(context: ShellContext, summary_path: Path) -> int:
    print(_format_header("review-day", context.profile))
    if not summary_path.exists():
        print(f"Chưa tìm thấy file review: {summary_path}")
        print("Gợi ý: chạy `cfte replay` hoặc `cfte run-scan` trước để tạo summary trong ngày.")
        return 1

    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    top_signals = summary.get("top_signals", [])
    print(f"Instrument: {summary.get('instrument_key', 'N/A')}")
    print(f"Số sự kiện replay: {summary.get('event_count', 0)}")
    print(f"Số cửa sổ đặc trưng: {summary.get('feature_windows', 0)}")
    print(f"Tổng tín hiệu thesis: {summary.get('thesis_count', 0)}")
    print(f"Fingerprint: {summary.get('fingerprint', 'N/A')}")
    if not top_signals:
        print("Hôm nay chưa có tín hiệu nổi bật trong file review.")
        return 0

    print("Top tín hiệu để xem lại cuối ngày:")
    for idx, signal in enumerate(top_signals, start=1):
        why_now = signal.get("why_now", [])
        print(
            f"- #{idx}: {signal.get('setup')} | {signal.get('stage')} | "
            f"điểm={signal.get('score')} | lý do chính={why_now[0] if why_now else 'chưa có'}"
        )
    return 0


def command_health(context: ShellContext) -> int:
    from cfte.collectors.health import CollectorHealthSnapshot

    print(_format_header("health", context.profile))
    print("Kiểm tra kết nối SQLite...")
    if DEFAULT_STATE_DB.exists():
        print(f"[OK] Tìm thấy database trạng thái tại: {DEFAULT_STATE_DB}")
    else:
        print(f"[WARN] Chưa có database tại {DEFAULT_STATE_DB}. Hãy chạy `init_sqlite_db.py`.")

    snapshot = CollectorHealthSnapshot(
        venue="binance",
        state="idle",
        connected=False,
        connect_attempts=0,
        reconnect_count=0,
        message_count=0,
        last_disconnect_reason=None,
        last_error=None,
    )
    print(snapshot.to_operator_summary())
    return 0


def command_review_thesis(context: ShellContext) -> int:
    import asyncio
    from cfte.storage.sqlite_writer import ThesisSQLiteStore
    from cfte.thesis.cards import render_trader_card
    from cfte.models.events import ThesisSignal

    print(_format_header("review-thesis", context.profile))
    store = ThesisSQLiteStore(DEFAULT_STATE_DB)
    
    async def _show():
        await store.migrate_schema()
        active = await store.get_active_thesis()
        recent = await store.get_recent_thesis(limit=10)
        
        if active:
            print(f"--- Đang hoạt động ({len(active)}) ---")
            for t in active:
                outcomes = await store.get_thesis_outcomes(t["thesis_id"])
                _render_t(t, outcomes)
        
        if recent:
            print(f"\n--- Lịch sử gần đây ({len(recent)}) ---")
            for t in recent:
                if any(a["thesis_id"] == t["thesis_id"] for a in active):
                    continue
                outcomes = await store.get_thesis_outcomes(t["thesis_id"])
                _render_t(t, outcomes)

        if not active and not recent:
            print("Không tìm thấy luận điểm nào trong database.")

    def _render_t(t, outcomes: list[dict[str, Any]] | None = None):
        entry_px = t.get("entry_px")
        entry_str = f" | Entry: {entry_px:.2f}" if entry_px else ""
        print(f"\nID: {t['thesis_id']} | {t['instrument_key']} | {t['setup']}{entry_str}")
        
        if outcomes:
            outcome_parts = []
            for o in outcomes:
                if o["status"] == "COMPLETED":
                    change = ((o["realized_px"] / entry_px) - 1) * 100 if entry_px else 0
                    color = "🟢" if change >= 0 else "🔴"
                    outcome_parts.append(f"{o['horizon']}: {color}{change:+.2f}%")
                else:
                    outcome_parts.append(f"{o['horizon']}: ⌛")
            print(f"Kết quả: {' | '.join(outcome_parts)}")

        signal = ThesisSignal(
            thesis_id=t["thesis_id"],
            instrument_key=t["instrument_key"],
            setup=t["setup"],
            direction=t["direction"],
            stage=t["stage"],
            score=t["score"],
            confidence=t["confidence"],
            coverage=t["coverage"],
            why_now=[],
            conflicts=[],
            invalidation="N/A",
            entry_style="N/A",
            targets=[],
        )
        print(render_trader_card(signal))

    asyncio.run(_show())
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="cfte",
        description="Shell cá nhân cho Crypto Flow Thesis Engine theo luồng doctor -> run-scan -> run-live -> review-day.",
    )
    parser.add_argument("--profile", default=str(DEFAULT_PROFILE_PATH), help="Đường dẫn hồ sơ YAML cá nhân.")
    sub = parser.add_subparsers(dest="cmd")

    sub.add_parser("doctor", help="Kiểm tra trạng thái hệ thống và tệp cấu hình (Health check).")
    sub.add_parser("health", help="Kiểm tra kết nối và tình trạng dữ liệu hiện tại.")
    sub.add_parser("review-thesis", help="Xem danh sách các luận điểm đang lưu trong SQLite.")

    replay = sub.add_parser("replay", help="Chạy replay deterministic và lưu summary.")
    replay.add_argument("--events", default=None)
    replay.add_argument("--summary-out", default=None)

    run_scan = sub.add_parser("run-scan", help="Quét replay theo hồ sơ cá nhân và in trader card tiếng Việt.")
    run_scan.add_argument("--events", default=None)
    run_scan.add_argument("--limit", type=int, default=None)

    run_live = sub.add_parser("run-live", help="Chạy ingest live Binance public bằng cấu hình cá nhân.")
    run_live.add_argument("--symbol", default=None)
    run_live.add_argument("--out", default=None)
    run_live.add_argument("--max-events", type=int, default=None)
    run_live.add_argument("--use-trade", action="store_true", help="Dùng stream trade thay vì aggTrade.")

    review = sub.add_parser("review-day", help="Xem báo cáo tổng kết ngày từ file summary.")
    review.add_argument("--summary", default=str(DEFAULT_REPLAY_SUMMARY), help="Đường dẫn file kết quả (summary.json).")

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    if not args.cmd:
        parser.print_help()
        return 0

    context = build_context(args.profile)

    if args.cmd == "doctor":
        return doctor(context)
    if args.cmd == "health":
        return command_health(context)
    if args.cmd == "replay":
        replay_events = _resolve_path(args.events, _profile_path(context.profile, "defaults", "replay_events", DEFAULT_REPLAY_EVENTS))
        replay_summary = _resolve_path(args.summary_out, _profile_path(context.profile, "defaults", "summary_out", DEFAULT_REPLAY_SUMMARY))
        return command_replay(context, events_path=replay_events, summary_out=replay_summary)
    if args.cmd == "run-scan":
        replay_events = _resolve_path(args.events, _profile_path(context.profile, "defaults", "replay_events", DEFAULT_REPLAY_EVENTS))
        return command_run_scan(context, events_path=replay_events, limit=args.limit)
    if args.cmd == "run-live":
        return command_run_live(context, symbol=args.symbol, max_events=args.max_events, use_trade=args.use_trade)
    if args.cmd == "review-day":
        summary_path = _resolve_path(args.summary, _profile_path(context.profile, "review", "summary_path", DEFAULT_REPLAY_SUMMARY))
        return command_review_day(context, summary_path=summary_path)
    if args.cmd == "review-thesis":
        return command_review_thesis(context)

    parser.print_help()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
