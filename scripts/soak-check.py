#!/usr/bin/env python3
"""Bounded two-hour Polybot soak monitor; never changes trading/runtime state."""

from __future__ import annotations

import argparse
import json
import math
import os
import shutil
import sqlite3
import subprocess
import time
import urllib.error
import urllib.request
from collections import Counter
from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from polybot.operational_report import build_operational_report

_ACTIVE_ORDER_STATES = ("OPEN", "AWAITING_RESULT", "RESOLVED")


def _now() -> datetime:
    return datetime.now(UTC)


def _parse(value: object) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed.replace(tzinfo=UTC) if parsed.tzinfo is None else parsed.astimezone(UTC)


def _percentile(values: Sequence[float], probability: float) -> float | None:
    ordered = sorted(values)
    if not ordered:
        return None
    position = (len(ordered) - 1) * probability
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return round(ordered[lower], 3)
    weight = position - lower
    return round(ordered[lower] * (1 - weight) + ordered[upper] * weight, 3)


def _distribution(values: Sequence[float]) -> dict[str, float | int | None]:
    return {
        "count": len(values),
        "p50": _percentile(values, 0.50),
        "p95": _percentile(values, 0.95),
        "max": None if not values else round(max(values), 3),
    }


def _command(*arguments: str) -> tuple[int, str]:
    result = subprocess.run(arguments, capture_output=True, text=True, check=False)
    return result.returncode, result.stdout.strip()


def _service_state(name: str) -> dict[str, object]:
    _, state = _command("systemctl", "is-active", name)
    _, details = _command(
        "systemctl",
        "show",
        name,
        "--property=NRestarts,MainPID,Result,ExecMainStatus",
        "--no-pager",
    )
    values: dict[str, object] = {"state": state or "unknown"}
    for line in details.splitlines():
        key, separator, value = line.partition("=")
        if not separator:
            continue
        values[key] = int(value) if value.isdigit() else value
    return values


def _observer_processes() -> dict[str, int]:
    observer = paper = 0
    proc = Path("/proc")
    for candidate in proc.iterdir():
        if not candidate.name.isdigit():
            continue
        try:
            arguments = [
                item.decode(errors="replace")
                for item in (candidate / "cmdline").read_bytes().split(b"\0")
                if item
            ]
        except OSError:
            continue
        lowered = [item.casefold() for item in arguments]
        if "run" not in lowered or not any(Path(item).name == "polybot" for item in arguments):
            continue
        observer += 1
        paper += int("--paper" in lowered)
    return {"observer": observer, "paper": paper, "non_paper": observer - paper}


def _dashboard_ok() -> bool:
    try:
        with urllib.request.urlopen("http://127.0.0.1:8787/health", timeout=5) as response:
            return response.status == 200
    except (OSError, urllib.error.URLError):
        return False


def _memory_available() -> int | None:
    try:
        lines = Path("/proc/meminfo").read_text().splitlines()
    except OSError:
        return None
    for line in lines:
        if line.startswith("MemAvailable:"):
            return int(line.split()[1]) * 1024
    return None


def _db_snapshot(database: Path, *, start_scan_id: int, started_at: datetime) -> dict[str, Any]:
    connection = sqlite3.connect(f"{database.as_uri()}?mode=ro", uri=True, timeout=30)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA query_only=ON")
    try:
        rows = connection.execute(
            "SELECT id, started_at, completed_at, mode, status FROM scan_runs "
            "WHERE id > ? ORDER BY id",
            (start_scan_id,),
        ).fetchall()
        completed_durations: list[float] = []
        running_ages: list[float] = []
        now = _now()
        statuses: Counter[str] = Counter()
        modes: Counter[str] = Counter()
        for row in rows:
            status = str(row["status"])
            statuses[status] += 1
            modes[str(row["mode"])] += 1
            start = _parse(row["started_at"])
            completed = _parse(row["completed_at"])
            if status == "completed" and start is not None and completed is not None:
                completed_durations.append(max(0.0, (completed - start).total_seconds()))
            if status == "running" and start is not None:
                running_ages.append(max(0.0, (now - start).total_seconds()))

        mark_rows = connection.execute(
            """
            WITH ordered AS (
                SELECT paper_order_id, captured_at,
                       LAG(captured_at) OVER (
                           PARTITION BY paper_order_id ORDER BY captured_at, id
                       ) AS previous_captured_at
                FROM paper_marks
            )
            SELECT captured_at, previous_captured_at FROM ordered
            WHERE captured_at >= ? ORDER BY captured_at
            """,
            (started_at.isoformat(),),
        ).fetchall()
        gaps: list[float] = []
        for row in mark_rows:
            captured = _parse(row["captured_at"])
            previous = _parse(row["previous_captured_at"])
            if captured is not None and previous is not None and previous <= captured:
                gaps.append((captured - previous).total_seconds())

        placeholders = ",".join("?" for _ in _ACTIVE_ORDER_STATES)
        mark_age_rows = connection.execute(
            f"""
            SELECT o.id, MAX(m.captured_at) AS latest_mark
            FROM paper_orders o
            LEFT JOIN paper_marks m ON m.paper_order_id=o.id
            WHERE o.status IN ({placeholders})
            GROUP BY o.id
            """,
            _ACTIVE_ORDER_STATES,
        ).fetchall()
        mark_ages = [
            max(0.0, (now - latest).total_seconds())
            for row in mark_age_rows
            if (latest := _parse(row["latest_mark"])) is not None
        ]

        tables = {
            str(row[0])
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
        shadow: dict[str, object] = {"available": False}
        if "shadow_research_runs" in tables:
            shadow_rows = connection.execute(
                "SELECT status, COUNT(*) AS count, "
                "COALESCE(SUM(outcome_requested),0), "
                "COALESCE(SUM(outcome_completed),0), "
                "COALESCE(SUM(outcome_pending),0), "
                "COALESCE(SUM(extra_registered),0) "
                "FROM shadow_research_runs WHERE parent_scan_run_id > ? GROUP BY status",
                (start_scan_id,),
            ).fetchall()
            shadow = {
                "available": True,
                "status_counts": {str(row[0]): int(row[1]) for row in shadow_rows},
                "outcome_requested": sum(int(row[2]) for row in shadow_rows),
                "outcome_completed": sum(int(row[3]) for row in shadow_rows),
                "outcome_pending": sum(int(row[4]) for row in shadow_rows),
                "extra_registered": sum(int(row[5]) for row in shadow_rows),
            }
        return {
            "scan_count": len(rows),
            "status_counts": dict(statuses),
            "mode_counts": dict(modes),
            "duration_seconds": _distribution(completed_durations),
            "running_max_age_seconds": round(max(running_ages, default=0), 3),
            "mark_gap_seconds": _distribution(gaps),
            "active_order_count": len(mark_age_rows),
            "oldest_active_mark_age_seconds": round(max(mark_ages, default=0), 3),
            "shadow": shadow,
        }
    finally:
        connection.close()


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str) + "\n")
    os.chmod(temporary, 0o600)
    temporary.replace(path)


def _improvement(before: float, after: object) -> float | None:
    if not isinstance(after, (int, float)) or before <= 0:
        return None
    return round((1 - float(after) / before) * 100, 2)


def _oom_seen(started_at: datetime) -> bool:
    code, output = _command(
        "journalctl",
        "--kernel",
        "--since",
        started_at.isoformat(),
        "--grep=Out of memory|Killed process|oom-kill",
        "--no-pager",
        "--quiet",
    )
    return code == 0 and bool(output.strip())


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", type=Path, required=True)
    parser.add_argument("--state-root", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--daily-output", type=Path, required=True)
    parser.add_argument("--hours", type=float, default=2.0)
    parser.add_argument("--interval", type=int, default=60)
    parser.add_argument("--start-scan-id", type=int, required=True)
    parser.add_argument("--baseline-cycle-p50", type=float, required=True)
    parser.add_argument("--baseline-cycle-p95", type=float, required=True)
    parser.add_argument("--baseline-mark-p50", type=float, required=True)
    parser.add_argument("--baseline-mark-p95", type=float, required=True)
    parser.add_argument("--weathernext-statistics-snapshot", type=Path, default=None)
    args = parser.parse_args()
    if not 0 < args.hours <= 24:
        raise SystemExit("hours must be in (0, 24]")
    interval = max(10, args.interval)
    started = _now()
    planned_end = started + timedelta(hours=args.hours)
    observer_start = _service_state("polybot-observer.service")
    restart_baseline = int(observer_start.get("NRestarts", 0))
    consecutive_health_failures = consecutive_low_memory = consecutive_high_load = 0
    failures: list[str] = []
    samples: list[dict[str, object]] = []
    static_services = (
        "polybot-observer.service",
        "polybot-dashboard.service",
        "polybot-astra-primary-tunnel.service",
        "polybot-astra-fallback-tunnel.service",
    )

    while _now() < planned_end and not failures:
        sampled_at = _now()
        services = {name: _service_state(name) for name in static_services}
        processes = _observer_processes()
        dashboard_ok = _dashboard_ok()
        memory_available = _memory_available()
        load1 = os.getloadavg()[0]
        disk = shutil.disk_usage(args.state_root)
        database = _db_snapshot(
            args.database.resolve(),
            start_scan_id=args.start_scan_id,
            started_at=started,
        )
        backup = _service_state("polybot-backup.service")

        if not dashboard_ok:
            consecutive_health_failures += 1
        else:
            consecutive_health_failures = 0
        if memory_available is not None and memory_available < 2 * 1024**3:
            consecutive_low_memory += 1
        else:
            consecutive_low_memory = 0
        if load1 > 4.0:
            consecutive_high_load += 1
        else:
            consecutive_high_load = 0

        sample = {
            "sampled_at_utc": sampled_at.isoformat(),
            "services": services,
            "observer_processes": processes,
            "dashboard_ok": dashboard_ok,
            "memory_available_bytes": memory_available,
            "load1": round(load1, 3),
            "disk_free_bytes": disk.free,
            "disk_free_fraction": round(disk.free / disk.total if disk.total else 0, 6),
            "database": database,
            "backup": backup,
        }
        samples.append(sample)

        for name, state in services.items():
            if state.get("state") != "active":
                failures.append(f"{name} is not active")
        if processes != {"observer": 1, "paper": 1, "non_paper": 0}:
            failures.append("observer process invariant failed")
        if int(services["polybot-observer.service"].get("NRestarts", 0)) > restart_baseline:
            failures.append("observer NRestarts increased")
        if consecutive_health_failures >= 3:
            failures.append("dashboard health failed three consecutive samples")
        if database["status_counts"].get("failed", 0):
            failures.append("a new failed scan was recorded")
        if database["mode_counts"] and set(database["mode_counts"]) != {"paper"}:
            failures.append("a non-paper scan was recorded")
        if float(database["running_max_age_seconds"]) > 900:
            failures.append("a running scan exceeded 900 seconds")
        if (
            sampled_at - started >= timedelta(minutes=15)
            and int(database["active_order_count"]) > 0
            and float(database["oldest_active_mark_age_seconds"]) > 600
        ):
            failures.append("an active paper position mark exceeded 600 seconds")
        if consecutive_low_memory >= 3:
            failures.append("available memory stayed below 2 GiB")
        if consecutive_high_load >= 5:
            failures.append("load1 stayed above 4.0")
        if disk.free < 50 * 1024**3 or disk.free / disk.total < 0.25:
            failures.append("disk free-space floor was crossed")
        if backup.get("Result") not in {"success", ""}:
            failures.append("latest backup result is not successful")

        checkpoint = {
            "status": "failed" if failures else "running",
            "started_at_utc": started.isoformat(),
            "planned_end_at_utc": planned_end.isoformat(),
            "sample_count": len(samples),
            "failures": failures,
            "latest_sample": sample,
        }
        _write_json(args.output, checkpoint)
        if not failures:
            time.sleep(min(interval, max(0.0, (planned_end - _now()).total_seconds())))

    ended = _now()
    database = _db_snapshot(
        args.database.resolve(),
        start_scan_id=args.start_scan_id,
        started_at=started,
    )
    if _oom_seen(started):
        failures.append("kernel OOM evidence appeared during the soak")
    completed = int(database["status_counts"].get("completed", 0))
    if not failures and completed < 2:
        failures.append("fewer than two completed primary cycles")
    cycle = database["duration_seconds"]
    marks = database["mark_gap_seconds"]
    result = {
        "status": "failed" if failures else "completed",
        "started_at_utc": started.isoformat(),
        "ended_at_utc": ended.isoformat(),
        "elapsed_seconds": round((ended - started).total_seconds(), 3),
        "sample_count": len(samples),
        "start_scan_id": args.start_scan_id,
        "failures": failures,
        "acceptance": {
            "at_least_two_completed_cycles": completed >= 2,
            "zero_new_failed_scans": database["status_counts"].get("failed", 0) == 0,
            "paper_only": set(database["mode_counts"]) in (set(), {"paper"}),
            "single_paper_observer": _observer_processes()
            == {"observer": 1, "paper": 1, "non_paper": 0},
        },
        "performance": {
            "baseline": {
                "cycle_p50_seconds": args.baseline_cycle_p50,
                "cycle_p95_seconds": args.baseline_cycle_p95,
                "mark_p50_seconds": args.baseline_mark_p50,
                "mark_p95_seconds": args.baseline_mark_p95,
            },
            "after": {"cycles": cycle, "paper_marks": marks},
            "improvement_percent": {
                "cycle_p50": _improvement(args.baseline_cycle_p50, cycle.get("p50")),
                "cycle_p95": _improvement(args.baseline_cycle_p95, cycle.get("p95")),
                "mark_p50": _improvement(args.baseline_mark_p50, marks.get("p50")),
                "mark_p95": _improvement(args.baseline_mark_p95, marks.get("p95")),
            },
        },
        "database": database,
        "resource_extremes": {
            "max_load1": max((float(item["load1"]) for item in samples), default=None),
            "min_memory_available_bytes": min(
                (
                    int(item["memory_available_bytes"])
                    for item in samples
                    if item["memory_available_bytes"] is not None
                ),
                default=None,
            ),
            "min_disk_free_bytes": min(
                (int(item["disk_free_bytes"]) for item in samples), default=None
            ),
        },
    }
    daily = build_operational_report(
        args.database,
        state_root=args.state_root,
        ecmwf_json_root=args.state_root / "forecasts" / "ecmwf-ifs025-json",
        ecmwf_raw_root=args.state_root / "forecasts" / "ecmwf-open-data",
        weathernext_statistics_snapshot_path=args.weathernext_statistics_snapshot,
        period_hours=24,
    )
    _write_json(args.daily_output, daily)
    result["daily_operational_report"] = str(args.daily_output.resolve())
    _write_json(args.output, result)
    raise SystemExit(1 if failures else 0)


if __name__ == "__main__":
    main()
