from __future__ import annotations

import argparse
import asyncio
import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import yaml


DEFAULT_PROFILE_PATH = Path("configs/profiles/personal_binance.yaml")
DEFAULT_REPLAY_EVENTS = Path("fixtures/replay/btcusdt_normalized.jsonl")
DEFAULT_REPLAY_SUMMARY = Path("data/replay/summary_btcusdt.json")
DEFAULT_RAW_DIR = Path("data/raw")
DEFAULT_STATE_DB = Path("data/state/state.db")
DEFAULT_THESIS_LOG = Path("data/thesis/thesis_log.jsonl")
DEFAULT_DAILY_SUMMARY = Path("data/review/daily_summary.json")
DEFAULT_WEEKLY_SUMMARY = Path("data/review/weekly_review.json")
DEFAULT_REVIEW_JOURNAL = Path("data/review/review_journal.jsonl")
DEFAULT_TUNING_REPORT = Path("data/review/tuning_report.json")


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
    ux: dict[str, Any]


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
        ux=dict(data.get("ux", {"alert_on_stage_change": True, "alert_on_score_delta": 10.0})),
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
    print("Luồng khuyến nghị: doctor -> run-scan -> run-live -> review-day -> review-week.")
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
    from cfte.replay.runner import persist_replay_summary
    from cfte.storage.thesis_log import ThesisLogWriter
    from cfte.thesis.cards import render_trader_card

    print(_format_header("run-scan", context.profile))
    result = _load_replay_result(events_path)
    actionable_threshold = float(context.profile.scan.get("actionable_threshold", 75.0))
    candidates = [signal for signal in result.thesis_events if signal.score >= actionable_threshold]
    target_limit = limit or context.profile.scan.get("max_cards", 3)
    shown = candidates[:target_limit]

    summary_out = _profile_path(context.profile, "defaults", "summary_out", DEFAULT_REPLAY_SUMMARY)
    persist_replay_summary(result, summary_out)
    print(f"Đã lưu summary scan tại: {summary_out}")

    thesis_log_path = _profile_path(context.profile, "scan", "thesis_log", DEFAULT_THESIS_LOG)
    ThesisLogWriter(thesis_log_path).append_scan_result(
        profile_name=context.profile.name,
        events_path=str(events_path),
        instrument_key=result.instrument_key,
        actionable_threshold=actionable_threshold,
        feature_windows=result.feature_windows,
        selected_signals=shown,
        total_signals=len(result.thesis_events),
    )
    print(f"Đã ghi log thesis scan tại: {thesis_log_path}")

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
    target_symbol = symbol or live_defaults.get("symbol") or context.profile.defaults.get("symbol") or "BTCUSDT"
    target_max_events = int(max_events or live_defaults.get("max_events", 25))
    db_path = DEFAULT_STATE_DB
    thesis_log_path = _profile_path(context.profile, "live", "thesis_log", DEFAULT_THESIS_LOG)

    print(f"Bắt đầu bộ máy live thesis cho {target_symbol}...")
    print(f"- Cơ sở dữ liệu trạng thái: {db_path}")
    print(f"- Giới hạn sự kiện: {target_max_events}")
    print(f"- Thesis log live: {thesis_log_path}")

    loop = LiveThesisLoop(
        symbol=str(target_symbol),
        db_path=db_path,
        use_agg_trade=not use_trade,
        horizons=context.profile.outcomes.get("horizons"),
        thesis_log_path=thesis_log_path,
    )
    loop.ux = context.profile.ux

    try:
        asyncio.run(loop.run_forever(max_events=target_max_events))
    except KeyboardInterrupt:
        print("\nĐã nhận tín hiệu dừng từ người dùng.")
    except Exception as exc:
        print(f"Lỗi thực thi loop: {exc}")
        return 1

    print(f"Hoàn tất phiên live cho {target_symbol}.")
    return 0


def _period_from_date(date_str: str) -> tuple[int, int]:
    start = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    end = start + timedelta(days=1)
    return int(start.timestamp() * 1000), int(end.timestamp() * 1000)




def _timestamp_ms(value: str | None) -> int:
    if value is None:
        return int(datetime.now(tz=timezone.utc).timestamp() * 1000)
    return int(datetime.strptime(value, "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc).timestamp() * 1000)

def _weekly_period(end_date_str: str | None) -> tuple[str, int, int]:
    if end_date_str is None:
        end = datetime.now(tz=timezone.utc)
    else:
        end = datetime.strptime(end_date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc) + timedelta(days=1)
    start = end - timedelta(days=7)
    label = f"{start.date().isoformat()}..{(end - timedelta(days=1)).date().isoformat()}"
    return label, int(start.timestamp() * 1000), int(end.timestamp() * 1000)


def command_review_day(context: ShellContext, date_str: str | None = None, summary_path: Path | None = None) -> int:
    from cfte.storage.measurement import SummaryDocument, persist_summary_document, render_daily_summary_vi
    from cfte.storage.review_journal import ReviewJournal, render_review_journal_vi, summarize_review_journal
    from cfte.storage.sqlite_writer import ThesisSQLiteStore

    print(_format_header("review-day", context.profile))
    store = ThesisSQLiteStore(DEFAULT_STATE_DB)

    async def _show() -> None:
        await store.migrate_schema()
        stats = await store.get_daily_summary_stats(date_str)
        journal_path = _profile_path(context.profile, "review", "review_journal_path", DEFAULT_REVIEW_JOURNAL)
        review_summary = summarize_review_journal(
            ReviewJournal(journal_path).read_records(),
            start_ts=stats["start_ts"],
            end_ts=stats["end_ts"],
        )
        text = render_daily_summary_vi(stats, review_summary)
        print(text)
        if review_summary.get("total_reviews", 0):
            print()
            print(render_review_journal_vi(review_summary))

        output_path = _profile_path(context.profile, "review", "daily_summary_path", DEFAULT_DAILY_SUMMARY)
        saved_path = persist_summary_document(
            output_path,
            SummaryDocument(label=stats["label"], summary_vi=text, payload={**stats, "review_summary": review_summary}),
        )
        print(f"Đã lưu daily summary tại: {saved_path}")

        summary_candidate = summary_path or _profile_path(context.profile, "review", "summary_path", DEFAULT_REPLAY_SUMMARY)
        if summary_candidate.exists():
            print(f"\nChi tiết từ file replay: {summary_candidate}")
            try:
                summary = json.loads(summary_candidate.read_text(encoding="utf-8"))
                top_signals = summary.get("top_signals", [])
                if top_signals:
                    print("Top tín hiệu từ replay:")
                    for idx, signal in enumerate(top_signals, start=1):
                        why_now = signal.get("why_now", [])
                        print(
                            f"  #{idx}: {signal.get('setup')} | {signal.get('stage')} | score={signal.get('score')} | lý do={why_now[0] if why_now else '...'}"
                        )
            except Exception as exc:
                print(f"Không thể đọc file summary: {exc}")

    asyncio.run(_show())
    return 0


def command_review_week(context: ShellContext, end_date_str: str | None = None) -> int:
    from cfte.storage.measurement import (
        SummaryDocument,
        persist_summary_document,
        render_setup_scorecard_vi,
        render_weekly_review_vi,
    )
    from cfte.storage.review_journal import (
        ReviewJournal,
        build_tuning_suggestions,
        render_review_journal_vi,
        render_tuning_report_vi,
        summarize_review_journal,
    )
    from cfte.storage.sqlite_writer import ThesisSQLiteStore

    print(_format_header("review-week", context.profile))
    store = ThesisSQLiteStore(DEFAULT_STATE_DB)

    async def _show() -> None:
        await store.migrate_schema()
        label, start_ts, end_ts = _weekly_period(end_date_str)
        stats = await store.get_period_summary(start_ts=start_ts, end_ts=end_ts, label=label)
        scorecard = await store.get_setup_scorecard()
        journal_path = _profile_path(context.profile, "review", "review_journal_path", DEFAULT_REVIEW_JOURNAL)
        review_summary = summarize_review_journal(ReviewJournal(journal_path).read_records(), start_ts=start_ts, end_ts=end_ts)
        threshold = float(context.profile.scan.get("actionable_threshold", 75.0))
        tuning_suggestions = build_tuning_suggestions(scorecard, review_summary, base_threshold=threshold)
        review_text = render_weekly_review_vi(stats, scorecard, review_summary, tuning_suggestions)
        print(review_text)
        print()
        print(render_setup_scorecard_vi(scorecard))
        if review_summary.get("total_reviews", 0):
            print()
            print(render_review_journal_vi(review_summary))
        print()
        tuning_text = render_tuning_report_vi(tuning_suggestions)
        print(tuning_text)

        output_path = _profile_path(context.profile, "review", "weekly_summary_path", DEFAULT_WEEKLY_SUMMARY)
        tuning_path = _profile_path(context.profile, "review", "tuning_report_path", DEFAULT_TUNING_REPORT)
        saved_path = persist_summary_document(
            output_path,
            SummaryDocument(
                label=label,
                summary_vi=review_text,
                payload={"stats": stats, "scorecard": scorecard, "review_summary": review_summary, "tuning_suggestions": tuning_suggestions},
            ),
        )
        tuning_saved_path = persist_summary_document(
            tuning_path,
            SummaryDocument(label=label, summary_vi=tuning_text, payload={"tuning_suggestions": tuning_suggestions}),
        )
        print(f"\nĐã lưu weekly review tại: {saved_path}")
        print(f"Đã lưu tuning report tại: {tuning_saved_path}")

    asyncio.run(_show())
    return 0


def command_log_review(
    context: ShellContext,
    thesis_id: str,
    decision: str,
    usefulness: str,
    note: str | None = None,
    tags: list[str] | None = None,
    review_ts: str | None = None,
) -> int:
    from cfte.storage.review_journal import ReviewDecision, ReviewJournal
    from cfte.storage.sqlite_writer import ThesisSQLiteStore

    print(_format_header("log-review", context.profile))
    store = ThesisSQLiteStore(DEFAULT_STATE_DB)
    journal_path = _profile_path(context.profile, "review", "review_journal_path", DEFAULT_REVIEW_JOURNAL)

    async def _run() -> int:
        await store.migrate_schema()
        thesis = await store.get_thesis_by_id(thesis_id)
        if thesis is None:
            print(f"Không tìm thấy thesis_id={thesis_id} trong SQLite.")
            return 1
        saved_path = ReviewJournal(journal_path).append(
            ReviewDecision(
                thesis_id=thesis_id,
                decision=decision,
                usefulness=usefulness,
                review_ts=_timestamp_ms(review_ts),
                setup=str(thesis.get("setup")),
                instrument_key=str(thesis.get("instrument_key")),
                note=note,
                tags=tuple(tags or ()),
                profile_name=context.profile.name,
            )
        )
        print(f"Đã ghi review cá nhân tại: {saved_path}")
        print(f"- thesis: {thesis_id} | setup={thesis.get('setup')} | quyết định={decision} | đánh giá={usefulness}")
        if note:
            print(f"- ghi chú: {note}")
        return 0

    return asyncio.run(_run())


def command_review_log(context: ShellContext, start_date: str | None = None, end_date: str | None = None) -> int:
    from cfte.storage.review_journal import ReviewJournal, render_review_journal_vi, summarize_review_journal

    print(_format_header("review-log", context.profile))
    start_ts = int(datetime.strptime(start_date, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp() * 1000) if start_date else None
    end_ts = int((datetime.strptime(end_date, "%Y-%m-%d").replace(tzinfo=timezone.utc) + timedelta(days=1)).timestamp() * 1000) if end_date else None
    journal_path = _profile_path(context.profile, "review", "review_journal_path", DEFAULT_REVIEW_JOURNAL)
    summary = summarize_review_journal(ReviewJournal(journal_path).read_records(), start_ts=start_ts, end_ts=end_ts)
    print(render_review_journal_vi(summary))
    return 0


def command_tune_profile(context: ShellContext) -> int:
    from cfte.storage.measurement import SummaryDocument, persist_summary_document
    from cfte.storage.review_journal import build_tuning_suggestions, render_tuning_report_vi, ReviewJournal, summarize_review_journal
    from cfte.storage.sqlite_writer import ThesisSQLiteStore

    print(_format_header("tune-profile", context.profile))
    store = ThesisSQLiteStore(DEFAULT_STATE_DB)

    async def _run() -> None:
        await store.migrate_schema()
        scorecard = await store.get_setup_scorecard()
        journal_path = _profile_path(context.profile, "review", "review_journal_path", DEFAULT_REVIEW_JOURNAL)
        review_summary = summarize_review_journal(ReviewJournal(journal_path).read_records())
        threshold = float(context.profile.scan.get("actionable_threshold", 75.0))
        tuning_suggestions = build_tuning_suggestions(scorecard, review_summary, base_threshold=threshold)
        text = render_tuning_report_vi(tuning_suggestions)
        print(text)
        tuning_path = _profile_path(context.profile, "review", "tuning_report_path", DEFAULT_TUNING_REPORT)
        saved_path = persist_summary_document(
            tuning_path,
            SummaryDocument(label=context.profile.name, summary_vi=text, payload={"tuning_suggestions": tuning_suggestions, "review_summary": review_summary}),
        )
        print(f"Đã lưu tuning report tại: {saved_path}")

    asyncio.run(_run())
    return 0


def command_scorecard(context: ShellContext) -> int:
    from cfte.storage.measurement import render_setup_scorecard_vi
    from cfte.storage.sqlite_writer import ThesisSQLiteStore

    print(_format_header("scorecard", context.profile))
    store = ThesisSQLiteStore(DEFAULT_STATE_DB)

    async def _show() -> None:
        await store.migrate_schema()
        rows = await store.get_setup_scorecard()
        print(render_setup_scorecard_vi(rows))

    asyncio.run(_show())
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
    from cfte.models.events import ThesisSignal
    from cfte.storage.sqlite_writer import ThesisSQLiteStore
    from cfte.thesis.cards import render_trader_card

    print(_format_header("review-thesis", context.profile))
    store = ThesisSQLiteStore(DEFAULT_STATE_DB)

    async def _show() -> None:
        await store.migrate_schema()
        active = await store.get_active_thesis()
        recent = await store.get_recent_thesis(limit=10)

        if active:
            print(f"--- Đang hoạt động ({len(active)}) ---")
            for thesis in active:
                outcomes = await store.get_thesis_outcomes(thesis["thesis_id"])
                _render_thesis(thesis, outcomes)

        if recent:
            print(f"\n--- Lịch sử gần đây ({len(recent)}) ---")
            for thesis in recent:
                if any(item["thesis_id"] == thesis["thesis_id"] for item in active):
                    continue
                outcomes = await store.get_thesis_outcomes(thesis["thesis_id"])
                _render_thesis(thesis, outcomes)

        if not active and not recent:
            print("Không tìm thấy luận điểm nào trong database.")

    def _render_thesis(thesis: dict[str, Any], outcomes: list[dict[str, Any]] | None = None) -> None:
        entry_px = thesis.get("entry_px")
        entry_str = f" | Entry: {entry_px:.2f}" if entry_px else ""
        print(f"\nID: {thesis['thesis_id']} | {thesis['instrument_key']} | {thesis['setup']}{entry_str}")

        if outcomes:
            outcome_parts: list[str] = []
            for outcome in outcomes:
                if outcome["status"] == "COMPLETED":
                    change = ((outcome["realized_px"] / entry_px) - 1) * 100 if entry_px else 0.0
                    color = "🟢" if change >= 0 else "🔴"
                    outcome_parts.append(f"{outcome['horizon']}: {color}{change:+.2f}%")
                else:
                    outcome_parts.append(f"{outcome['horizon']}: ⌛")
            print(f"Kết quả: {' | '.join(outcome_parts)}")

        signal = ThesisSignal(
            thesis_id=thesis["thesis_id"],
            instrument_key=thesis["instrument_key"],
            setup=thesis["setup"],
            direction=thesis["direction"],
            stage=thesis["stage"],
            score=thesis["score"],
            confidence=thesis["confidence"],
            coverage=thesis["coverage"],
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
        description="Shell cá nhân cho Crypto Flow Thesis Engine theo luồng doctor -> run-scan -> run-live -> review-day -> review-week.",
    )
    parser.add_argument("--profile", default=str(DEFAULT_PROFILE_PATH), help="Đường dẫn hồ sơ YAML cá nhân.")
    sub = parser.add_subparsers(dest="cmd")

    sub.add_parser("doctor", help="Kiểm tra trạng thái hệ thống và tệp cấu hình (Health check).")
    sub.add_parser("health", help="Kiểm tra kết nối và tình trạng dữ liệu hiện tại.")
    sub.add_parser("review-thesis", help="Xem danh sách các luận điểm đang lưu trong SQLite.")
    sub.add_parser("scorecard", help="Xem bảng điểm hiệu suất theo từng loại setup.")
    log_review = sub.add_parser("log-review", help="Ghi quyết định cá nhân: vào lệnh, bỏ qua hay phớt lờ một thesis.")
    log_review.add_argument("--thesis-id", required=True)
    log_review.add_argument("--decision", choices=["taken", "skipped", "ignored"], required=True)
    log_review.add_argument("--usefulness", choices=["useful", "neutral", "noise"], required=True)
    log_review.add_argument("--note", default=None)
    log_review.add_argument("--tags", nargs="*", default=None)
    log_review.add_argument("--review-ts", default=None, help="Thời gian review UTC dạng YYYY-MM-DDTHH:MM:SS.")

    review_log = sub.add_parser("review-log", help="Tổng hợp journal taken/skipped/ignored để review định kỳ.")
    review_log.add_argument("--start-date", default=None, help="Ngày bắt đầu (YYYY-MM-DD).")
    review_log.add_argument("--end-date", default=None, help="Ngày kết thúc (YYYY-MM-DD).")

    sub.add_parser("tune-profile", help="Sinh gợi ý tuning threshold cá nhân từ scorecard và review journal.")

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

    review_day = sub.add_parser("review-day", help="Sinh tổng kết ngày từ SQLite và lưu file summary tiếng Việt.")
    review_day.add_argument("--date", default=None, help="Ngày cần xem báo cáo (YYYY-MM-DD).")
    review_day.add_argument("--summary", default=None, help="Đường dẫn file kết quả replay (tùy chọn).")

    review_week = sub.add_parser("review-week", help="Sinh weekly review và setup scorecard từ outcome đã log.")
    review_week.add_argument("--end-date", default=None, help="Ngày kết thúc tuần review (YYYY-MM-DD).")

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
        summary_path = Path(args.summary) if args.summary else None
        return command_review_day(context, date_str=args.date, summary_path=summary_path)
    if args.cmd == "review-week":
        return command_review_week(context, end_date_str=args.end_date)
    if args.cmd == "review-thesis":
        return command_review_thesis(context)
    if args.cmd == "scorecard":
        return command_scorecard(context)
    if args.cmd == "log-review":
        return command_log_review(context, thesis_id=args.thesis_id, decision=args.decision, usefulness=args.usefulness, note=args.note, tags=args.tags, review_ts=args.review_ts)
    if args.cmd == "review-log":
        return command_review_log(context, start_date=args.start_date, end_date=args.end_date)
    if args.cmd == "tune-profile":
        return command_tune_profile(context)

    parser.print_help()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
