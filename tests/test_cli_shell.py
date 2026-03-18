from pathlib import Path

from cfte.cli.main import build_context, build_parser, command_run_scan, doctor


def test_doctor_accepts_personal_profile_and_required_paths_exist(capsys):
    context = build_context("configs/profiles/personal.default.yaml")

    exit_code = doctor(context)
    captured = capsys.readouterr()

    assert exit_code == 0
    assert "Hệ thống lõi đã sẵn sàng" in captured.out
    assert "run-scan" in captured.out


def test_parser_exposes_stable_personal_shell_commands():
    parser = build_parser()
    help_text = parser.format_help()

    assert "doctor" in help_text
    assert "replay" in help_text
    assert "run-scan" in help_text
    assert "run-live" in help_text
    assert "review-day" in help_text
    assert "health" in help_text


def test_run_scan_outputs_vietnamese_summary(capsys):
    context = build_context(Path("configs/profiles/personal.default.yaml"))

    exit_code = command_run_scan(context, events_path=Path("fixtures/replay/btcusdt_normalized.jsonl"), limit=1)
    captured = capsys.readouterr()

    assert exit_code == 0
    assert "Đã quét replay" in captured.out
    assert "Ứng viên #1" in captured.out
