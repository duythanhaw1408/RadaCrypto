from __future__ import annotations

import importlib
import json
import os
import sqlite3
import socket
import sys
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

REQUIRED_SQLITE_TABLES = ("thesis", "thesis_event")
REQUIRED_IMPORTS = {
    "yaml": "PyYAML",
    "requests": "requests",
    "websockets": "websockets",
    "aiosqlite": "aiosqlite",
}

OPTIONAL_IMPORTS = {
    "duckdb": "duckdb",
    "pyarrow": "pyarrow",
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
    pid: int | None = None
    run_id: str | None = None
    owner_host: str | None = None
    lock_path: str | None = None
    lock_acquired_at: str | None = None
    futures_ws_latency_ms: int | None = None
    futures_is_stale: bool = False
    collector_health: dict[str, Any] = field(default_factory=dict)
    context_health: dict[str, Any] = field(default_factory=dict)
    latest_tpfm: dict[str, Any] = field(default_factory=dict)
    first_m5_seen_at: str | None = None
    latest_transition: dict[str, Any] = field(default_factory=dict)
    latest_flow_grade: str | None = None
    last_transition_alias_vi: str | None = None
    degraded_flags: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class LiveRuntimeLease:
    path: Path
    lock_path: Path
    run_id: str
    pid: int
    host: str
    acquired_at: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_path": str(self.path),
            "lock_path": str(self.lock_path),
            "run_id": self.run_id,
            "pid": self.pid,
            "host": self.host,
            "acquired_at": self.acquired_at,
        }


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


def _live_runtime_lock_path(path: Path) -> Path:
    return path.with_name(f"{path.name}.lock")


def _pid_is_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def acquire_live_runtime_lease(path: Path, *, run_id: str) -> LiveRuntimeLease:
    path.parent.mkdir(parents=True, exist_ok=True)
    lock_path = _live_runtime_lock_path(path)
    lease = LiveRuntimeLease(
        path=path,
        lock_path=lock_path,
        run_id=run_id,
        pid=os.getpid(),
        host=socket.gethostname(),
        acquired_at=datetime.now(tz=timezone.utc).isoformat(),
    )

    while True:
        try:
            fd = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644)
        except FileExistsError:
            payload = load_json_artifact(lock_path)
            if isinstance(payload, dict):
                existing_pid = int(payload.get("pid", 0) or 0)
                existing_run_id = str(payload.get("run_id", "")).strip()
                existing_host = str(payload.get("host", "")).strip() or "unknown"
                if existing_pid == lease.pid and existing_run_id == lease.run_id:
                    return lease
                if _pid_is_alive(existing_pid):
                    raise RuntimeError(
                        "Runtime artifact đang được phiên khác sử dụng "
                        f"(pid={existing_pid}, host={existing_host}, run_id={existing_run_id or 'unknown'})."
                    )
            try:
                lock_path.unlink()
            except FileNotFoundError:
                continue
            except OSError as exc:
                raise RuntimeError(f"Không thể thu hồi runtime lock stale tại {lock_path}: {exc}") from exc
            continue

        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(json.dumps(lease.to_dict(), ensure_ascii=False, indent=2))
        return lease


def release_live_runtime_lease(lease: LiveRuntimeLease | None) -> None:
    if lease is None:
        return
    payload = load_json_artifact(lease.lock_path)
    if isinstance(payload, dict):
        current_pid = int(payload.get("pid", 0) or 0)
        current_run_id = str(payload.get("run_id", "")).strip()
        if current_pid != lease.pid or current_run_id != lease.run_id:
            return
    try:
        lease.lock_path.unlink()
    except FileNotFoundError:
        return


def persist_live_runtime_artifact(
    path: Path,
    artifact: LiveRuntimeArtifact,
    *,
    lease: LiveRuntimeLease | None = None,
) -> Path:
    import tempfile
    path.parent.mkdir(parents=True, exist_ok=True)

    if lease is not None:
        payload = load_json_artifact(lease.lock_path)
        if not isinstance(payload, dict):
            raise RuntimeError(f"Runtime lock bị mất trước khi ghi artifact: {lease.lock_path}")
        current_pid = int(payload.get("pid", 0) or 0)
        current_run_id = str(payload.get("run_id", "")).strip()
        if current_pid != lease.pid or current_run_id != lease.run_id:
            raise RuntimeError(
                "Runtime artifact lock không còn thuộc phiên hiện tại "
                f"(expected pid={lease.pid}, run_id={lease.run_id})."
            )

    fd, tmp_path = tempfile.mkstemp(dir=str(path.parent), prefix=f"{path.name}.tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(json.dumps(artifact.to_dict(), ensure_ascii=False, indent=2))
        os.replace(tmp_path, str(path))
    except Exception:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)
        raise
    return path


def load_json_artifact(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return payload if isinstance(payload, dict) else None


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
            
    optional_missing: list[str] = []
    for module_name, label in OPTIONAL_IMPORTS.items():
        try:
            importlib.import_module(module_name)
        except Exception:
            optional_missing.append(label)

    results = []
    if missing:
        results.append(
            CheckResult(
                key="dependencies",
                level="critical",
                status="fail",
                summary_vi="Thiếu dependency runtime quan trọng",
                detail=", ".join(sorted(missing)),
            )
        )
    else:
        results.append(
            CheckResult(
                key="dependencies",
                level="critical",
                status="ok",
                summary_vi="Dependency runtime cốt lõi đã sẵn sàng",
            )
        )
        
    if optional_missing:
        results.append(
            CheckResult(
                key="optional_dependencies",
                level="warning",
                status="warn",
                summary_vi="Thiếu một số thư viện phân tích nâng cao (không bắt buộc)",
                detail=", ".join(sorted(optional_missing)),
            )
        )
        
    return results


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

    live_symbol = str(profile.live.get("symbol", "")).strip() if getattr(profile, "live", None) else ""
    default_symbol = str(profile.defaults.get("symbol", "")).strip() if getattr(profile, "defaults", None) else ""
    symbol = live_symbol or default_symbol

    if symbol:
        checks.append(CheckResult("default_symbol", "critical", "ok", f"Symbol vận hành: {symbol}"))
    else:
        checks.append(CheckResult("default_symbol", "critical", "fail", "Hồ sơ chưa khai báo symbol (live hoặc default)"))
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
            if name == "live_runtime":
                results.extend(_check_live_runtime_artifact(path))
        else:
            results.append(CheckResult(name, "warning", "warn", f"Artifact {name} chưa được tạo", str(path)))
    return results


def _check_live_runtime_artifact(path: Path) -> list[CheckResult]:
    payload = load_json_artifact(path)
    if payload is None:
        return [
            CheckResult(
                key="live_runtime_payload",
                level="warning",
                status="warn",
                summary_vi="Artifact live_runtime không đọc được",
                detail=str(path),
            )
        ]

    checks: list[CheckResult] = []
    owner_pid = payload.get("pid")
    owner_run_id = str(payload.get("run_id", "")).strip()
    owner_host = str(payload.get("owner_host", "")).strip()
    if owner_pid is not None or owner_run_id:
        detail_parts = []
        if owner_run_id:
            detail_parts.append(f"run_id={owner_run_id}")
        if owner_pid is not None:
            detail_parts.append(f"pid={owner_pid}")
        if owner_host:
            detail_parts.append(f"host={owner_host}")
        checks.append(
            CheckResult(
                key="live_runtime_owner",
                level="warning",
                status="ok",
                summary_vi="Artifact live_runtime có owner rõ ràng",
                detail=" | ".join(detail_parts) if detail_parts else None,
            )
        )

    status = str(payload.get("status", "unknown"))
    if status in {"runtime_error", "bootstrap_failed"}:
        checks.append(
            CheckResult(
                key="live_runtime_status",
                level="critical",
                status="warn",
                summary_vi=f"Phiên live gần nhất kết thúc với trạng thái {status}",
                detail=str(payload.get("last_error") or "Không có chi tiết lỗi"),
            )
        )
    elif status == "watchdog_timeout":
        checks.append(
            CheckResult(
                key="live_runtime_status",
                level="warning",
                status="warn",
                summary_vi="Phiên live gần nhất bị watchdog timeout",
                detail=str(payload.get("last_error") or "Không có chi tiết lỗi"),
            )
        )
    else:
        checks.append(
            CheckResult(
                key="live_runtime_status",
                level="warning",
                status="ok",
                summary_vi=f"Phiên live gần nhất kết thúc với trạng thái {status}",
            )
        )

    degraded_flags = [str(flag) for flag in payload.get("degraded_flags", []) if str(flag).strip()]
    if degraded_flags:
        checks.append(
            CheckResult(
                key="live_runtime_degraded_flags",
                level="warning",
                status="warn",
                summary_vi="Phiên live gần nhất có dấu hiệu suy giảm feed/context",
                detail=", ".join(degraded_flags[:5]),
            )
        )

    context_health = payload.get("context_health", {})
    if isinstance(context_health, dict):
        futures_fresh = context_health.get("futures_context_fresh")
        if futures_fresh is False:
            checks.append(
                CheckResult(
                    key="live_runtime_futures_context",
                    level="warning",
                    status="warn",
                    summary_vi="Context futures của phiên live gần nhất đã stale",
                )
            )

        venue_state = str(context_health.get("venue_confirmation_state", "")).strip()
        active_venues = int(context_health.get("active_venues", 0) or 0)
        if venue_state == "UNCONFIRMED" and active_venues < 2:
            checks.append(
                CheckResult(
                    key="live_runtime_venue_context",
                    level="warning",
                    status="warn",
                    summary_vi="Phiên live gần nhất thiếu xác nhận đa sàn đủ mạnh",
                    detail=f"active_venues={active_venues}",
                )
            )

    latest_tpfm = payload.get("latest_tpfm", {})
    raw_first_m5_seen_at = payload.get("first_m5_seen_at")
    first_m5_seen_at = str(raw_first_m5_seen_at).strip() if raw_first_m5_seen_at is not None else ""
    if first_m5_seen_at:
        checks.append(
            CheckResult(
                key="live_runtime_first_m5",
                level="warning",
                status="ok",
                summary_vi="Phiên live gần nhất đã sinh M5 đầu tiên",
                detail=first_m5_seen_at,
            )
        )
    if isinstance(latest_tpfm, dict):
        matrix_alias = str(latest_tpfm.get("matrix_alias_vi", "")).strip()
        tradability_grade = str(latest_tpfm.get("tradability_grade", "")).strip()
        blind_spots = [str(flag) for flag in latest_tpfm.get("blind_spot_flags", []) if str(flag).strip()]
        detail_parts: list[str] = []
        if tradability_grade:
            detail_parts.append(f"grade={tradability_grade}")
        if blind_spots:
            detail_parts.append(f"blind_spots={', '.join(blind_spots[:4])}")
        if matrix_alias or detail_parts:
            checks.append(
                CheckResult(
                    key="live_runtime_latest_matrix",
                    level="warning",
                    status="ok",
                    summary_vi=f"Matrix gần nhất: {matrix_alias or 'N/A'}",
                    detail=" | ".join(detail_parts) if detail_parts else None,
                )
            )
            latest_transition = payload.get("latest_transition", {})
            last_trans = str(payload.get("last_transition_alias_vi", "")).strip()
            if not last_trans and isinstance(latest_transition, dict):
                last_trans = str(latest_transition.get("alias_vi", "")).strip()
            if last_trans:
                checks.append(
                    CheckResult(
                        key="live_runtime_last_transition",
                        level="warning",
                        status="ok",
                        summary_vi=f"Chuyển pha gần nhất: {last_trans}",
                    )
                )
        else:
            checks.append(
                CheckResult(
                    key="live_runtime_latest_matrix",
                    level="warning",
                    status="warn",
                    summary_vi="Phiên live gần nhất chưa sinh snapshot M5",
                )
            )
            if not first_m5_seen_at:
                checks.append(
                    CheckResult(
                        key="live_runtime_first_m5",
                        level="warning",
                        status="warn",
                        summary_vi="Phiên live gần nhất chưa sinh M5 đầu tiên",
                    )
                )

    return checks
