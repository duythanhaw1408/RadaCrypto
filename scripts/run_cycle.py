#!/usr/bin/env python3
"""
Worker script cho Railway/Render/cron deployment.

Chạy một chu kỳ đầy đủ trong 1 lần gọi:
  1. bootstrap
  2. health check
  3. run-scan (quét nhanh từ replay data)
  4. run-live (short burst, giới hạn events)
  5. review-day
  6. watchdog

Thiết kế cho scheduled execution (mỗi 1-4 giờ), không phải 24/7 loop.
Tối ưu cho free-tier có giới hạn thời gian chạy.

Sử dụng:
  python scripts/run_cycle.py                      # full cycle, chạy live tới khi có M5 đầu tiên
  python scripts/run_cycle.py --skip-live           # skip live, chỉ scan + review
  python scripts/run_cycle.py --max-events 100      # giới hạn live events
  python scripts/run_cycle.py --profile configs/profiles/personal_binance.yaml
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path

# Ensure src is in path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))


def run_command(label: str, args: list[str]) -> int:
    """Run a CLI command and return exit code."""
    from cfte.cli.main import main as cli_main

    print(f"\n{'='*60}")
    print(f"  [{label}] Bắt đầu...")
    print(f"{'='*60}")

    start = time.time()
    old_argv = sys.argv
    sys.argv = ["cfte"] + args
    try:
        code = cli_main()
    except SystemExit as e:
        code = e.code or 0
    except Exception as exc:
        print(f"  ⚠️  Lỗi: {exc}")
        code = 1
    finally:
        sys.argv = old_argv

    elapsed = time.time() - start
    status = "✅" if code == 0 else "❌"
    print(f"  {status} [{label}] Hoàn tất trong {elapsed:.1f}s (exit={code})")
    return code


def _runtime_artifact_path(profile_path: str) -> Path:
    from cfte.cli.main import DEFAULT_LIVE_RUNTIME_REPORT, build_context

    context = build_context(profile_path)
    review = context.profile.review
    return Path(str(review.get("live_runtime_path", DEFAULT_LIVE_RUNTIME_REPORT)))


def _runtime_is_ready(runtime_path: Path) -> bool:
    """Kiểm tra xem artifact live đã sẵn sàng (có M5 và grade) chưa."""
    if not runtime_path.exists():
        return False
    try:
        payload = json.loads(runtime_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return False
        
    # Check 1: Phải thấy M5
    has_m5 = bool(payload.get("first_m5_seen_at")) or bool(
        payload.get("latest_tpfm", {}).get("matrix_cell")
    )
    
    # Check 2: Phải có flow grade (Phase 6 requirement)
    has_grade = bool(payload.get("latest_flow_grade"))
    
    return has_m5 and has_grade


def _build_live_args(args: argparse.Namespace) -> list[str]:
    live_args = [
        "run-live",
        "--symbol", args.symbol,
        "--max-events", str(args.max_events),
    ]
    if args.min_runtime_seconds > 0:
        live_args.extend(["--min-runtime-seconds", str(args.min_runtime_seconds)])
    if not args.allow_missing_m5:
        live_args.append("--run-until-first-m5")
    return live_args


def main():
    parser = argparse.ArgumentParser(description="CFTE cycle runner cho free-tier deployment")
    parser.add_argument("--profile", default="configs/profiles/personal_binance.yaml",
                        help="Path tới profile YAML")
    parser.add_argument("--max-events", type=int, default=1500,
                        help="Số sự kiện tối đa cho live loop (mặc định: 1500)")
    parser.add_argument("--min-runtime-seconds", type=float, default=330.0,
                        help="Thời gian chạy tối thiểu (giây) trước khi được phép thoát (mặc định: 330)")
    parser.add_argument("--allow-missing-m5", action="store_true",
                        help="Cho phép chu kỳ kết thúc dù live session chưa sinh snapshot M5")
    parser.add_argument("--skip-live", action="store_true",
                        help="Bỏ qua live loop, chỉ chạy scan + review")
    parser.add_argument("--symbol", default="BTCUSDT",
                        help="Symbol để theo dõi (mặc định: BTCUSDT)")
    args = parser.parse_args()

    profile_args = ["--profile", args.profile]
    total_start = time.time()
    results = {}
    runtime_path = _runtime_artifact_path(args.profile)

    print("\n" + "=" * 60)
    print("  🚀 CFTE CYCLE — Crypto Flow Thesis Engine")
    print(f"  Profile: {args.profile}")
    print(f"  Symbol: {args.symbol}")
    if args.skip_live:
        live_mode = "skip"
    else:
        exit_mode = "M5 đầu tiên" if not args.allow_missing_m5 else "cho phép thiếu M5"
        live_mode = f"{args.max_events}+ events | min {args.min_runtime_seconds:.0f}s | {exit_mode}"
    print(f"  Live: {live_mode}")
    print("=" * 60)

    # 1. Bootstrap
    results["bootstrap"] = run_command("BOOTSTRAP", profile_args + ["bootstrap"])

    # 2. Health check
    results["health"] = run_command("HEALTH", profile_args + ["health"])

    # 3. Run-scan
    results["scan"] = run_command("RUN-SCAN", profile_args + ["run-scan", "--limit", "5"])

    # 4. Run-live (short burst)
    if not args.skip_live:
        results["live"] = run_command("RUN-LIVE", profile_args + _build_live_args(args))
        if results["live"] == 0 and not args.allow_missing_m5 and not _runtime_is_ready(runtime_path):
            print(
                "  ℹ️  [RUN-LIVE] Phiên live hoàn tất nhưng chưa đạt trạng thái SẴN SÀNG (thiếu M5 hoặc Grade). "
                "Đang ở chế độ STRICT nên đánh dấu chu kỳ là fail."
            )
            results["live"] = 1
        elif results["live"] == 0 and args.allow_missing_m5 and not _runtime_is_ready(runtime_path):
            print(
                "  ⚠️  [RUN-LIVE] Cảnh báo: Thiếu dữ liệu M5/Grade nhưng đang ở chế độ ALLOW-MISSING nên vẫn cho phép tiếp tục."
            )
    else:
        results["live"] = -1  # skipped
        print("\n  ⏭️  Bỏ qua live loop (--skip-live)")

    # 5. Review day
    results["review"] = run_command("REVIEW-DAY", profile_args + ["review-day"])

    # 6. Watchdog
    results["watchdog"] = run_command("WATCHDOG", profile_args + ["watchdog"])

    # Summary
    total_elapsed = time.time() - total_start
    print("\n" + "=" * 60)
    print("  📊 KẾT QUẢ CHU KỲ")
    print("=" * 60)
    for step, code in results.items():
        if code == -1:
            print(f"  ⏭️  {step}: bỏ qua")
        elif code == 0:
            print(f"  ✅ {step}: OK")
        else:
            print(f"  ❌ {step}: FAIL (exit={code})")
    print(f"\n  ⏱️  Tổng thời gian: {total_elapsed:.1f}s")
    print("=" * 60)

    # Return non-zero if any critical step failed
    critical = ["bootstrap", "health", "scan"]
    if not args.skip_live:
        critical.append("live")
    
    if any(results.get(s, 1) not in (0, -1) for s in critical):
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
