from pathlib import Path

from cfte.cli.main import build_context, build_parser, command_run_scan, doctor


def test_doctor_accepts_personal_profile_and_required_paths_exist(capsys):
    context = build_context("configs/profiles/personal.default.yaml")

    exit_code = doctor(context)
    captured = capsys.readouterr()

    assert exit_code in {0, 1}
    assert 'Trạng thái hệ thống:' in captured.out
    assert 'run-scan' in captured.out


def test_parser_exposes_stable_personal_shell_commands():
    parser = build_parser()
    help_text = parser.format_help()

    assert "doctor" in help_text
    assert "replay" in help_text
    assert "run-scan" in help_text
    assert "run-live" in help_text
    assert "review-day" in help_text
    assert "health" in help_text


def test_run_live_parser_accepts_runtime_controls():
    parser = build_parser()

    args = parser.parse_args(["run-live", "--min-runtime-seconds", "330", "--run-until-first-m5"])

    assert args.cmd == "run-live"
    assert args.min_runtime_seconds == 330.0
    assert args.run_until_first_m5 is True


def test_run_scan_outputs_vietnamese_summary(capsys):
    context = build_context(Path("configs/profiles/personal.default.yaml"))

    exit_code = command_run_scan(context, events_path=Path("fixtures/replay/btcusdt_normalized.jsonl"), limit=1)
    captured = capsys.readouterr()

    assert exit_code == 0
    assert "Đã quét replay" in captured.out
    assert "Ứng viên #1" in captured.out


def test_bootstrap_creates_state_db_and_health_report(tmp_path, capsys):
    from cfte.cli.main import command_bootstrap

    profile_path = tmp_path / 'profile.yaml'
    profile_path.write_text(
        '\n'.join([
            'profile: bootstrap-test',
            'locale: vi-VN',
            'trader:',
            '  display_name: Bootstrap Trader',
            'defaults:',
            '  symbol: BTCUSDT',
            '  replay_events: fixtures/replay/btcusdt_normalized.jsonl',
            'scan: {}',
            'live: {}',
            'review:',
            f'  health_report_path: {tmp_path / "health.json"}',
            f'  review_journal_path: {tmp_path / "review.jsonl"}',
        ]),
        encoding='utf-8',
    )
    context = build_context(profile_path)

    exit_code = command_bootstrap(context)
    captured = capsys.readouterr()

    assert exit_code in {0, 1}
    assert 'Đã lưu bootstrap health report' in captured.out
    assert Path('data/state/state.db').exists()
    assert (tmp_path / 'health.json').exists()


def test_health_reports_bad_config_for_missing_profile_replay(tmp_path, capsys):
    from cfte.cli.main import command_health

    profile_path = tmp_path / 'profile.yaml'
    profile_path.write_text(
        '\n'.join([
            'profile: health-test',
            'locale: vi-VN',
            'trader:',
            '  display_name: Health Trader',
            'defaults:',
            '  symbol: BTCUSDT',
            f'  replay_events: {tmp_path / "missing.jsonl"}',
            'scan: {}',
            'live: {}',
            'review:',
            f'  health_report_path: {tmp_path / "health.json"}',
        ]),
        encoding='utf-8',
    )
    context = build_context(profile_path)

    exit_code = command_health(context)
    captured = capsys.readouterr()

    assert exit_code in {0, 1}
    assert 'DEGRADED' in captured.out or 'BAD CONFIG' in captured.out
    assert 'Thiếu dữ liệu replay mặc định' in captured.out
