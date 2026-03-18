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
  python scripts/run_cycle.py                      # full cycle, 200 events
  python scripts/run_cycle.py --skip-live           # skip live, chỉ scan + review
  python scripts/run_cycle.py --max-events 100      # giới hạn live events
  python scripts/run_cycle.py --profile configs/profiles/personal_binance.yaml
"""
from __future__ import annotations

import argparse
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


def main():
    parser = argparse.ArgumentParser(description="CFTE cycle runner cho free-tier deployment")
    parser.add_argument("--profile", default="configs/profiles/personal_binance.yaml",
                        help="Path tới profile YAML")
    parser.add_argument("--max-events", type=int, default=200,
                        help="Số sự kiện tối đa cho live loop (mặc định: 200)")
    parser.add_argument("--skip-live", action="store_true",
                        help="Bỏ qua live loop, chỉ chạy scan + review")
    parser.add_argument("--symbol", default="BTCUSDT",
                        help="Symbol để theo dõi (mặc định: BTCUSDT)")
    args = parser.parse_args()

    profile_args = ["--profile", args.profile]
    total_start = time.time()
    results = {}

    print("\n" + "=" * 60)
    print("  🚀 CFTE CYCLE — Crypto Flow Thesis Engine")
    print(f"  Profile: {args.profile}")
    print(f"  Symbol: {args.symbol}")
    print(f"  Live: {'skip' if args.skip_live else f'{args.max_events} events'}")
    print("=" * 60)

    # 1. Bootstrap
    results["bootstrap"] = run_command("BOOTSTRAP", profile_args + ["bootstrap"])

    # 2. Health check
    results["health"] = run_command("HEALTH", profile_args + ["health"])

    # 3. Run-scan
    results["scan"] = run_command("RUN-SCAN", profile_args + ["run-scan", "--limit", "5"])

    # 4. Run-live (short burst)
    if not args.skip_live:
        results["live"] = run_command("RUN-LIVE", profile_args + [
            "run-live",
            "--symbol", args.symbol,
            "--max-events", str(args.max_events),
        ])
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
    if any(results.get(s, 1) not in (0, -1) for s in critical):
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
