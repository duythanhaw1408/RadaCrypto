from __future__ import annotations

import importlib
import json
import sqlite3
import sys
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

REQUIRED_SQLITE_TABLES = ("thesis", "thesis_event")
REQUIRED_IMPORTS = {
    "yaml": "PyYAML",
    "requests": "requests",
    "duckdb": "duckdb",
    "pyarrow": "pyarrow",
    "websockets": "websockets",
    "aiosqlite": "aiosqlite",
}


@dataclass(frozen=True, slots=True)
class CheckResult:
    key: str
    level: str
    status: str
    summary_vi: str
    detail: str | None = None


@dataclass(frozen=True, slots=True)
class RuntimeReport:
    overall_status: str
    generated_at: str
    checks: list[CheckResult]
    artifacts: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "overall_status": self.overall_status,
            "generated_at": self.generated_at,
            "checks": [asdict(check) for check in self.checks],
            "artifacts": self.artifacts,
        }


@dataclass(frozen=True, slots=True)
class LiveRuntimeArtifact:
    symbol: str
    status: str
    started_at: str
    finished_at: str
    processed_events: int
    event_counts: dict[str, int]
    reconnect_count: int
    message_count: int
    idle_timeout_seconds: float
    heartbeat_interval: int
    stale_gap_seconds: float | None
    last_error: str | None
    last_trade_ts: int | None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


_CRITICAL_STATUSES = {"fail"}
_DEGRADED_STATUSES = {"warn"}


def build_runtime_report(*, profile_path: Path, profile: Any, state_db: Path, artifact_paths: dict[str, Path]) -> RuntimeReport:
    checks: list[CheckResult] = []
    checks.extend(_check_python_runtime())
    checks.extend(_check_dependencies())
    checks.extend(_check_profile(profile_path=profile_path, profile=profile))
    checks.extend(_check_state_db(state_db))
    checks.extend(_check_artifacts(artifact_paths))

    overall = "healthy"
    statuses = {check.status for check in checks}
    if statuses & _CRITICAL_STATUSES:
        overall = "bad_config"
    elif statuses & _DEGRADED_STATUSES:
        overall = "degraded"

    return RuntimeReport(
        overall_status=overall,
        generated_at=datetime.now(tz=timezone.utc).isoformat(),
        checks=checks,
        artifacts={name: str(path) for name, path in artifact_paths.items()},
    )


def render_runtime_report_vi(report: RuntimeReport) -> str:
    header = {
        "healthy": "HEALTHY",
        "degraded": "DEGRADED",
        "bad_config": "BAD CONFIG",
    }[report.overall_status]
    lines = [f"Trạng thái hệ thống: {header}"]
    for check in report.checks:
        icon = {"ok": "[OK]", "warn": "[DEGRADED]", "fail": "[FAIL]"}[check.status]
        detail = f" ({check.detail})" if check.detail else ""
        lines.append(f"{icon} {check.summary_vi}{detail}")
    return "\n".join(lines)


def persist_runtime_report(path: Path, report: RuntimeReport) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report.to_dict(), ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def persist_live_runtime_artifact(path: Path, artifact: LiveRuntimeArtifact) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(artifact.to_dict(), ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def _check_python_runtime() -> list[CheckResult]:
    version = sys.version_info
    ok = version >= (3, 11)
    summary = f"Python {version.major}.{version.minor}.{version.micro} {'đạt yêu cầu >=3.11' if ok else 'không đạt yêu cầu >=3.11'}"
    return [
        CheckResult(
            key="python_version",
            level="critical",
            status="ok" if ok else "fail",
            summary_vi=summary,
        )
    ]


def _check_dependencies() -> list[CheckResult]:
    missing: list[str] = []
    for module_name, label in REQUIRED_IMPORTS.items():
        try:
            importlib.import_module(module_name)
        except Exception:
            missing.append(label)
    if missing:
        return [
            CheckResult(
                key="dependencies",
                level="critical",
                status="fail",
                summary_vi="Thiếu dependency runtime quan trọng",
                detail=", ".join(sorted(missing)),
            )
        ]
    return [
        CheckResult(
            key="dependencies",
            level="critical",
            status="ok",
            summary_vi="Dependency runtime cốt lõi đã sẵn sàng",
        )
    ]


def _check_profile(*, profile_path: Path, profile: Any) -> list[CheckResult]:
    checks: list[CheckResult] = []
    if profile_path.exists():
        checks.append(CheckResult("profile_path", "critical", "ok", f"Tìm thấy hồ sơ cá nhân tại {profile_path}"))
    else:
        checks.append(CheckResult("profile_path", "critical", "fail", f"Thiếu hồ sơ cá nhân tại {profile_path}"))

    replay_events = Path(str(profile.defaults.get("replay_events", ""))) if getattr(profile, "defaults", None) else Path()
    if replay_events.exists():
        checks.append(CheckResult("replay_events", "warning", "ok", f"Dữ liệu replay mặc định sẵn sàng tại {replay_events}"))
    else:
        checks.append(CheckResult("replay_events", "warning", "warn", "Thiếu dữ liệu replay mặc định", str(replay_events)))

    symbol = str(profile.defaults.get("symbol", "")).strip()
    if symbol:
        checks.append(CheckResult("default_symbol", "critical", "ok", f"Symbol mặc định: {symbol}"))
    else:
        checks.append(CheckResult("default_symbol", "critical", "fail", "Hồ sơ chưa khai báo symbol mặc định"))
    return checks


def _check_state_db(state_db: Path) -> list[CheckResult]:
    if not state_db.exists():
        return [
            CheckResult(
                key="state_db",
                level="warning",
                status="warn",
                summary_vi="Chưa tìm thấy SQLite state DB",
                detail=str(state_db),
            )
        ]

    try:
        with sqlite3.connect(state_db) as conn:
            rows = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    except sqlite3.DatabaseError as exc:
        return [CheckResult("state_db", "critical", "fail", "SQLite state DB không đọc được", str(exc))]

    names = {row[0] for row in rows}
    missing = [name for name in REQUIRED_SQLITE_TABLES if name not in names]
    if missing:
        return [
            CheckResult(
                key="state_db",
                level="critical",
                status="fail",
                summary_vi="SQLite state DB thiếu bảng lõi",
                detail=", ".join(missing),
            )
        ]
    return [CheckResult("state_db", "critical", "ok", f"SQLite state DB sẵn sàng tại {state_db}")]


def _check_artifacts(artifact_paths: dict[str, Path]) -> list[CheckResult]:
    results: list[CheckResult] = []
    for name, path in artifact_paths.items():
        if path.exists():
            mtime = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc).isoformat()
            results.append(CheckResult(name, "warning", "ok", f"Artifact {name} sẵn sàng", mtime))
        else:
            results.append(CheckResult(name, "warning", "warn", f"Artifact {name} chưa được tạo", str(path)))
    return results
