from __future__ import annotations

import argparse
import json
import math
import os
import shutil
import sqlite3
import statistics
from collections import Counter, defaultdict
from collections.abc import Iterable, Mapping, Sequence
from contextlib import suppress
from dataclasses import asdict
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any, cast

from polybot.ecmwf import EcmwfArchiveRetentionPolicy, EcmwfIfsEnsAdapter
from polybot.forecast_models import (
    OPEN_METEO_ALGORITHM_VERSION,
    WEATHERNEXT_ALGORITHM_VERSION,
)
from polybot.forecast_v2 import ECMWF_RAW_ALGORITHM_VERSION, FORECAST_V2_ALGORITHM_VERSION

_SAFE_SCAN_MODES = frozenset({"observe", "paper", "recovery"})
_ACTIVE_PAPER_STATUSES = frozenset({"OPEN", "AWAITING_RESULT", "RESOLVED"})
_BACKGROUND_TABLE_TOKENS = ("background", "research", "shadow")
_BACKGROUND_PAYLOAD_TOKENS = (
    "background",
    "outcome_refresh",
    "research_lane",
    "shadow",
    "extra_events",
)
_BACKGROUND_TIME_COLUMNS = (
    "completed_at_utc",
    "completed_at",
    "created_at_utc",
    "created_at",
    "started_at_utc",
    "started_at",
    "captured_at_utc",
    "captured_at",
)
_BACKGROUND_STATUS_COLUMNS = ("status", "state")
_SAFE_SOURCE_PAYLOAD_KEYS = frozenset(
    {
        "archive_id",
        "artifact_count",
        "decoded",
        "init_time_utc",
        "member_count",
        "message",
        "parameter",
        "product",
        "published_at_utc",
        "scope",
        "steps",
    }
)
_SNAPSHOT_READ_LIMIT_BYTES = 8 * 1024 * 1024
_FULL_STATUS_READ_LIMIT_BYTES = 1 * 1024 * 1024
_FULL_INDEX_READ_LIMIT_BYTES = 8 * 1024 * 1024
_FULL_MAX_TARGET_REPORT = 256


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _parse_datetime(value: object) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    normalized = value[:-1] + "+00:00" if value.endswith("Z") else value
    try:
        return _utc(datetime.fromisoformat(normalized))
    except ValueError:
        return None


def _iso(value: datetime | None) -> str | None:
    return None if value is None else _utc(value).isoformat()


def _round(value: float | int | None, digits: int = 3) -> float | None:
    if value is None or not math.isfinite(float(value)):
        return None
    return round(float(value), digits)


def _as_int(value: object, default: int = 0) -> int:
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, (int, float, str)):
        with suppress(TypeError, ValueError, OverflowError):
            return int(value)
    return default


def _as_mapping(value: object) -> Mapping[str, object]:
    return value if isinstance(value, Mapping) else {}


def _percentile(sorted_values: Sequence[float], probability: float) -> float | None:
    """Return a linearly interpolated percentile without optional dependencies."""

    if not sorted_values:
        return None
    if len(sorted_values) == 1:
        return float(sorted_values[0])
    position = (len(sorted_values) - 1) * probability
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return float(sorted_values[lower])
    weight = position - lower
    return float(sorted_values[lower] * (1 - weight) + sorted_values[upper] * weight)


def _distribution(values: Iterable[float]) -> dict[str, int | float | None | str]:
    finite = sorted(float(item) for item in values if math.isfinite(float(item)))
    return {
        "sample_count": len(finite),
        "p50": _round(_percentile(finite, 0.50)),
        "p95": _round(_percentile(finite, 0.95)),
        "max": _round(max(finite) if finite else None),
        "mean": _round(statistics.fmean(finite) if finite else None),
        "percentile_method": "linear_interpolation",
    }


def _quote_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _table_names(connection: sqlite3.Connection) -> set[str]:
    rows = connection.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
    ).fetchall()
    return {str(row[0]) for row in rows}


def _column_info(connection: sqlite3.Connection, table: str) -> dict[str, str]:
    rows = connection.execute(f"PRAGMA table_info({_quote_identifier(table)})").fetchall()
    return {str(row[1]): str(row[2] or "").upper() for row in rows}


def _cycle_report(
    connection: sqlite3.Connection,
    tables: set[str],
    *,
    cutoff: datetime,
    now: datetime,
    period_hours: float,
) -> dict[str, object]:
    empty = {
        "available": False,
        "started_count": 0,
        "completed_count": 0,
        "failed_count": 0,
        "running_count": 0,
        "geoblocked_count": 0,
        "throughput_completed_per_hour": 0.0,
        "duration_seconds": _distribution([]),
        "failed_duration_seconds": _distribution([]),
        "scan_modes": {},
        "latest": None,
    }
    if "scan_runs" not in tables:
        return empty
    columns = _column_info(connection, "scan_runs")
    required = {"id", "started_at", "completed_at", "mode", "status"}
    if not required.issubset(columns):
        return empty

    selected = ["id", "started_at", "completed_at", "mode", "status"]
    selected.extend(name for name in ("geoblocked", "window_id") if name in columns)
    rows = connection.execute(
        "SELECT " + ", ".join(_quote_identifier(item) for item in selected) + " FROM scan_runs"
    ).fetchall()
    mode_status: dict[str, Counter[str]] = defaultdict(Counter)
    durations: list[float] = []
    failed_durations: list[float] = []
    period_rows: list[dict[str, object]] = []
    for row in rows:
        started = _parse_datetime(row["started_at"])
        completed = _parse_datetime(row["completed_at"])
        status = str(row["status"]).lower()
        effective = completed or started
        if effective is None or effective < cutoff or effective > now:
            continue
        mode = str(row["mode"]).lower()
        mode_status[mode][status] += 1
        duration = None
        if started is not None and completed is not None:
            duration = max(0.0, (completed - started).total_seconds())
            if status == "completed":
                durations.append(duration)
            elif status == "failed":
                failed_durations.append(duration)
        period_rows.append(
            {
                "id": int(row["id"]),
                "started_at": started,
                "completed_at": completed,
                "mode": mode,
                "status": status,
                "duration_seconds": duration,
                "geoblocked": bool(row["geoblocked"]) if "geoblocked" in selected else False,
            }
        )

    statuses = Counter(str(row["status"]) for row in period_rows)
    latest_row = max(
        period_rows,
        key=lambda item: item["started_at"] if isinstance(item["started_at"], datetime) else cutoff,
        default=None,
    )
    latest: dict[str, object] | None = None
    if latest_row is not None:
        latest = {
            "run_id": latest_row["id"],
            "started_at_utc": _iso(
                latest_row["started_at"] if isinstance(latest_row["started_at"], datetime) else None
            ),
            "completed_at_utc": _iso(
                latest_row["completed_at"]
                if isinstance(latest_row["completed_at"], datetime)
                else None
            ),
            "mode": latest_row["mode"],
            "status": latest_row["status"],
            "duration_seconds": _round(
                latest_row["duration_seconds"]
                if isinstance(latest_row["duration_seconds"], (float, int))
                else None
            ),
        }

    return {
        "available": True,
        "started_count": len(period_rows),
        "completed_count": statuses["completed"],
        "failed_count": statuses["failed"],
        "running_count": statuses["running"],
        "geoblocked_count": sum(bool(row["geoblocked"]) for row in period_rows),
        "throughput_completed_per_hour": _round(statuses["completed"] / period_hours),
        "duration_seconds": _distribution(durations),
        "failed_duration_seconds": _distribution(failed_durations),
        "scan_modes": {
            mode: dict(sorted(status_counts.items()))
            for mode, status_counts in sorted(mode_status.items())
        },
        "latest": latest,
    }


def _mark_report(
    connection: sqlite3.Connection,
    tables: set[str],
    *,
    cutoff: datetime,
    now: datetime,
) -> dict[str, object]:
    empty = {
        "available": False,
        "mark_count": 0,
        "paper_order_count": 0,
        "latest_mark_at_utc": None,
        "latest_mark_age_seconds": None,
        "interval_seconds": _distribution([]),
    }
    if "paper_marks" not in tables:
        return empty
    columns = _column_info(connection, "paper_marks")
    required = {"id", "paper_order_id", "captured_at"}
    if not required.issubset(columns):
        return empty

    # LAG sees the mark immediately before the reporting window, while the
    # outer filter keeps the returned dataset bounded to the requested period.
    rows = connection.execute(
        """
        WITH ordered AS (
            SELECT id, paper_order_id, captured_at,
                   LAG(captured_at) OVER (
                       PARTITION BY paper_order_id ORDER BY captured_at, id
                   ) AS previous_captured_at
            FROM paper_marks
        )
        SELECT id, paper_order_id, captured_at, previous_captured_at
        FROM ordered
        WHERE captured_at >= ? AND captured_at <= ?
        ORDER BY captured_at, id
        """,
        (cutoff.isoformat(), now.isoformat()),
    ).fetchall()
    intervals: list[float] = []
    valid_marks: list[tuple[int, datetime]] = []
    for row in rows:
        captured = _parse_datetime(row["captured_at"])
        if captured is None or captured < cutoff or captured > now:
            continue
        valid_marks.append((int(row["paper_order_id"]), captured))
        previous = _parse_datetime(row["previous_captured_at"])
        if previous is not None and previous <= captured:
            intervals.append((captured - previous).total_seconds())
    latest = max((item[1] for item in valid_marks), default=None)
    return {
        "available": True,
        "mark_count": len(valid_marks),
        "paper_order_count": len({item[0] for item in valid_marks}),
        "latest_mark_at_utc": _iso(latest),
        "latest_mark_age_seconds": (
            None if latest is None else _round(max(0.0, (now - latest).total_seconds()))
        ),
        "interval_seconds": _distribution(intervals),
    }


def _timestamped_table_stats(
    connection: sqlite3.Connection,
    tables: set[str],
    table: str,
    time_column: str,
    *,
    cutoff: datetime,
    now: datetime,
) -> dict[str, object]:
    if table not in tables or time_column not in _column_info(connection, table):
        return {"available": False, "total_count": 0, "period_count": 0, "latest_at_utc": None}
    quoted_table = _quote_identifier(table)
    quoted_time = _quote_identifier(time_column)
    total = int(connection.execute(f"SELECT COUNT(*) FROM {quoted_table}").fetchone()[0])
    period = int(
        connection.execute(
            f"SELECT COUNT(*) FROM {quoted_table} WHERE {quoted_time} >= ? AND {quoted_time} <= ?",
            (cutoff.isoformat(), now.isoformat()),
        ).fetchone()[0]
    )
    latest = connection.execute(f"SELECT MAX({quoted_time}) FROM {quoted_table}").fetchone()[0]
    return {
        "available": True,
        "total_count": total,
        "period_count": period,
        "latest_at_utc": _iso(_parse_datetime(latest)),
    }


def _prediction_stats(
    connection: sqlite3.Connection,
    tables: set[str],
    *,
    cutoff: datetime,
    now: datetime,
) -> dict[str, dict[str, object]]:
    if "forecast_predictions_v2" not in tables:
        return {}
    columns = _column_info(connection, "forecast_predictions_v2")
    if not {"algorithm_version", "issued_at_utc"}.issubset(columns):
        return {}
    rows = connection.execute(
        "SELECT algorithm_version, issued_at_utc FROM forecast_predictions_v2"
    ).fetchall()
    totals: Counter[str] = Counter()
    periods: Counter[str] = Counter()
    latest: dict[str, datetime] = {}
    for row in rows:
        algorithm = str(row["algorithm_version"])
        issued = _parse_datetime(row["issued_at_utc"])
        totals[algorithm] += 1
        if issued is not None:
            previous = latest.get(algorithm)
            if previous is None or issued > previous:
                latest[algorithm] = issued
            if cutoff <= issued <= now:
                periods[algorithm] += 1
    return {
        algorithm: {
            "total_count": totals[algorithm],
            "period_count": periods[algorithm],
            "latest_issued_at_utc": _iso(latest.get(algorithm)),
        }
        for algorithm in sorted(totals)
    }


def _attempt_stats(
    connection: sqlite3.Connection,
    tables: set[str],
    *,
    cutoff: datetime,
    now: datetime,
) -> dict[str, dict[str, int]]:
    required = {"forecast_evaluation_events_v2", "forecast_evaluation_algorithms_v2"}
    if not required.issubset(tables):
        return {}
    event_columns = _column_info(connection, "forecast_evaluation_events_v2")
    algorithm_columns = _column_info(connection, "forecast_evaluation_algorithms_v2")
    if not {"id", "cohort_version", "considered_at_utc"}.issubset(event_columns) or not {
        "evaluation_event_id",
        "algorithm_version",
        "status",
    }.issubset(algorithm_columns):
        return {}
    rows = connection.execute(
        """
        SELECT a.algorithm_version, a.status, COUNT(*) AS count
        FROM forecast_evaluation_algorithms_v2 a
        JOIN forecast_evaluation_events_v2 e ON e.id = a.evaluation_event_id
        WHERE e.cohort_version = 'weather-evaluation-v1'
          AND e.considered_at_utc >= ? AND e.considered_at_utc <= ?
        GROUP BY a.algorithm_version, a.status
        """,
        (cutoff.isoformat(), now.isoformat()),
    ).fetchall()
    result: dict[str, dict[str, int]] = defaultdict(dict)
    for row in rows:
        result[str(row["algorithm_version"])][str(row["status"])] = int(row["count"])
    return dict(result)


def _model_source_stats(
    connection: sqlite3.Connection,
    tables: set[str],
) -> dict[str, dict[str, object]]:
    if "forecast_model_runs_v2" not in tables:
        return {}
    columns = _column_info(connection, "forecast_model_runs_v2")
    if not {"source", "model", "first_fetched_at_utc", "init_time_utc"}.issubset(columns):
        return {}
    rows = connection.execute(
        """
        SELECT source, model, COUNT(*) AS count,
               MAX(first_fetched_at_utc) AS latest_fetched_at_utc,
               MAX(init_time_utc) AS latest_init_time_utc
        FROM forecast_model_runs_v2
        GROUP BY source, model
        ORDER BY source, model
        """
    ).fetchall()
    result: dict[str, dict[str, object]] = {}
    for row in rows:
        key = f"{row['source']}::{row['model']}"
        result[key] = {
            "source": str(row["source"]),
            "model": str(row["model"]),
            "count": int(row["count"]),
            "latest_fetched_at_utc": _iso(_parse_datetime(row["latest_fetched_at_utc"])),
            "latest_init_time_utc": _iso(_parse_datetime(row["latest_init_time_utc"])),
        }
    return result


def _safe_source_payload(raw: object) -> dict[str, object]:
    if not isinstance(raw, str):
        return {}
    try:
        value = json.loads(raw)
    except (TypeError, ValueError):
        return {}
    if not isinstance(value, dict):
        return {}
    return {str(key): item for key, item in value.items() if str(key) in _SAFE_SOURCE_PAYLOAD_KEYS}


def _source_statuses(
    connection: sqlite3.Connection,
    tables: set[str],
) -> dict[str, dict[str, object]]:
    if "forecast_source_status_v2" not in tables:
        return {}
    columns = _column_info(connection, "forecast_source_status_v2")
    if not {"id", "source", "state", "checked_at_utc", "payload_json"}.issubset(columns):
        return {}
    rows = connection.execute(
        "SELECT source, state, checked_at_utc, payload_json "
        "FROM forecast_source_status_v2 ORDER BY checked_at_utc DESC, id DESC"
    ).fetchall()
    result: dict[str, dict[str, object]] = {}
    for row in rows:
        source = str(row["source"])
        if source in result:
            continue
        result[source] = {
            "state": str(row["state"]),
            "checked_at_utc": _iso(_parse_datetime(row["checked_at_utc"])),
            "details": _safe_source_payload(row["payload_json"]),
        }
    return result


def _local_snapshot(path: Path | None, *, summary_only: bool) -> dict[str, object]:
    mode = "SUMMARY_ONLY" if summary_only else "FULL_ENSEMBLE"
    base: dict[str, object] = {
        "configured": path is not None,
        "path": None if path is None else str(path.expanduser().resolve()),
        "access_state": "not_checked",
        "load_state": "not_configured" if path is None else "missing",
        "mode": mode,
        "network_access_performed": False,
    }
    if path is None:
        return base
    candidate = path.expanduser().resolve()
    if not candidate.is_file():
        return base
    try:
        size = candidate.stat().st_size
    except OSError:
        base["load_state"] = "error"
        return base
    base["size_bytes"] = size
    if size > _SNAPSHOT_READ_LIMIT_BYTES:
        base["load_state"] = "too_large_for_metadata_report"
        return base
    try:
        payload = json.loads(candidate.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, ValueError):
        base["load_state"] = "error"
        return base
    if not isinstance(payload, dict):
        base["load_state"] = "error"
        return base
    if summary_only and payload.get("mode") != "SUMMARY_ONLY":
        base["load_state"] = "invalid_mode"
        return base
    base.update(
        {
            "load_state": "available",
            "source": payload.get("source"),
            "surface": payload.get("surface"),
            "init_time_utc": _iso(_parse_datetime(payload.get("init_time_utc"))),
            "received_at_utc": _iso(_parse_datetime(payload.get("received_at_utc"))),
            "location": payload.get("location"),
            "station_id": payload.get("station_id"),
            "observation_date": payload.get("observation_date"),
        }
    )
    if summary_only:
        points = payload.get("points")
        base["point_count"] = len(points) if isinstance(points, list) else 0
        base["variable"] = payload.get("variable")
        base["mode"] = "SUMMARY_ONLY"
    else:
        scenarios = payload.get("scenario_max_c")
        base["member_count"] = len(scenarios) if isinstance(scenarios, list) else 0
    return base


def _read_bounded_json(path: Path, *, limit_bytes: int) -> tuple[str, object | None, int | None]:
    """Read one small local JSON artifact without following unbounded payloads.

    Operational reporting is deliberately a metadata/read-evidence operation.  A
    malformed or oversized artifact is represented by a stable state rather than
    exposing parser errors (which could contain arbitrary file contents).
    """

    candidate = path.expanduser().resolve()
    if not candidate.is_file():
        return "missing", None, None
    try:
        size = candidate.stat().st_size
    except OSError:
        return "error", None, None
    if size > limit_bytes:
        return "too_large", None, size
    try:
        return "available", json.loads(candidate.read_text(encoding="utf-8")), size
    except (OSError, UnicodeError, ValueError, TypeError):
        return "invalid", None, size


def _path_is_within(path: Path, root: Path) -> bool:
    try:
        path.expanduser().resolve().relative_to(root.expanduser().resolve())
    except ValueError:
        return False
    return True


def _full_refresh_base(root: Path | None) -> dict[str, object]:
    if root is None:
        return {
            "configured": False,
            "root": None,
            "manifest_path": None,
            "approval_path": None,
            "refresh_status_path": None,
            "index_path": None,
            "snapshot_root": None,
            "access_state": "not_configured",
            "load_state": "not_configured",
            "mode": "FULL_ENSEMBLE",
            "network_access_performed": False,
            "manifest_state": "not_checked",
            "approval_state": "not_checked",
            "approval_valid": False,
            "approval_expires_at_utc": None,
            "payload_read": False,
            "read_evidence": False,
            "refresh_status_state": "not_checked",
            "index_state": "not_checked",
            "index_manifest_binding": "not_bound",
            "coverage_state": "not_checked",
            "expected_target_count": 0,
            "actual_target_count": 0,
            "complete_target_count": 0,
            "partial_target_count": 0,
            "missing_target_count": 0,
            "complete_targets": [],
            "partial_targets": [],
            "missing_targets": [],
            "targets": [],
            "manifest_sha256": None,
            "release_id": None,
            "init_time_utc": None,
            "expected_network_bytes": None,
            "expected_object_count": None,
        }
    resolved = root.expanduser().resolve()
    return {
        **_full_refresh_base(None),
        "configured": True,
        "root": str(resolved),
        "manifest_path": str(resolved / "read-manifest.json"),
        "approval_path": str(resolved / "read-approval.json"),
        "refresh_status_path": str(resolved / "refresh-status.json"),
        "index_path": str(resolved / "latest-index.json"),
        "snapshot_root": str(resolved / "snapshots"),
        "access_state": "not_checked",
        "load_state": "missing",
    }


def _target_times(target: Mapping[str, object]) -> list[str]:
    raw = target.get("valid_times_utc")
    if not isinstance(raw, list):
        return []
    result: list[str] = []
    for value in raw:
        parsed = _parse_datetime(value)
        if parsed is None:
            return []
        result.append(parsed.isoformat())
    return result


def _full_target_snapshot_report(
    target: Mapping[str, object],
    *,
    snapshot_root: Path,
    release_id: str,
    init_time_utc: datetime,
) -> dict[str, object]:
    """Validate one immutable trajectory file using only bounded local reads."""

    target_id = str(target.get("target_id") or target.get("station_id") or "").strip()
    expected_times = _target_times(target)
    expected_members = [f"member-{index:03d}" for index in range(64)]
    raw_path = target.get("snapshot_path")
    record: dict[str, object] = {
        "target_id": target_id or None,
        "path": raw_path if isinstance(raw_path, str) else None,
        "status": "missing",
        "error_code": None,
        "expected_member_count": len(expected_members),
        "actual_member_count": 0,
        "expected_hour_count": len(expected_times),
        "actual_hour_count": 0,
        "member_coverage": "missing",
        "hour_coverage": "missing",
        "release_id": None,
        "init_time_utc": None,
        "observation_date": target.get("observation_date"),
        "station_id": target_id or None,
    }
    if not target_id:
        record.update({"status": "partial", "error_code": "missing_target_id"})
        return record
    coverage_complete = False
    if target.get("complete_station_local_day") is True and expected_times:
        try:
            from polybot.weathernext_manifest import assess_station_local_day_coverage

            coverage_complete, _ = assess_station_local_day_coverage(
                date.fromisoformat(str(target.get("observation_date", ""))),
                str(target.get("observation_timezone", "UTC")),
                expected_times,
            )
        except (TypeError, ValueError):
            coverage_complete = False
    if not coverage_complete:
        record.update({"status": "partial", "error_code": "incomplete_expected_hours"})
        return record
    if not isinstance(raw_path, str) or not raw_path:
        record.update({"status": "partial", "error_code": "missing_snapshot_path"})
        return record
    path = Path(raw_path).expanduser().resolve()
    record["path"] = str(path)
    if not _path_is_within(path, snapshot_root):
        record.update({"status": "partial", "error_code": "snapshot_path_outside_root"})
        return record
    state, payload, size = _read_bounded_json(path, limit_bytes=_SNAPSHOT_READ_LIMIT_BYTES)
    if size is not None:
        record["size_bytes"] = size
    if state == "missing":
        return record
    if state == "too_large":
        record.update({"status": "partial", "error_code": "snapshot_too_large"})
        return record
    if state != "available" or not isinstance(payload, Mapping):
        record.update({"status": "partial", "error_code": "snapshot_invalid_json"})
        return record

    # Pydantic performs the finite-value/provenance checks shared by the reader
    # and report.  Import lazily to keep the operational report import-light.
    try:
        from polybot.weathernext import WeatherNextSnapshot

        snapshot = WeatherNextSnapshot.model_validate(payload)
    except Exception:
        record.update({"status": "partial", "error_code": "snapshot_schema_invalid"})
        return record

    actual_members = list(snapshot.member_ids)
    if not actual_members and snapshot.trajectories:
        actual_members = [str(item.get("member_id", "")) for item in snapshot.trajectories]
    member_ids_field_complete = list(snapshot.member_ids) == expected_members
    actual_times = [value.isoformat() for value in snapshot.valid_times_utc]
    record.update(
        {
            "actual_member_count": len(actual_members),
            "actual_hour_count": len(actual_times),
            "member_coverage": (
                "complete" if actual_members == expected_members else "partial"
            ),
            "hour_coverage": "complete" if actual_times == expected_times else "partial",
            "release_id": snapshot.release_id,
            "init_time_utc": _iso(snapshot.init_time_utc),
            "units": snapshot.units,
            "observation_date": snapshot.observation_date.isoformat(),
            "station_id": snapshot.station_id,
        }
    )
    if (
        snapshot.release_id != release_id
        or snapshot.init_time_utc != init_time_utc
        or snapshot.station_id != target_id
        or snapshot.observation_date.isoformat() != str(target.get("observation_date"))
        or not member_ids_field_complete
        or actual_members != expected_members
        or actual_times != expected_times
        or len(snapshot.trajectories) != len(expected_members)
        or any(
            not isinstance(item.get("values_c"), list)
            or len(cast(list[object], item["values_c"])) != len(expected_times)
            for item in snapshot.trajectories
        )
    ):
        record.update({"status": "partial", "error_code": "coverage_or_provenance_mismatch"})
        return record
    record.update({"status": "complete", "error_code": None})
    return record


def _weathernext_full_refresh_report(
    root: Path | None,
    *,
    manifest_path: Path | None = None,
    approval_path: Path | None = None,
    refresh_status_path: Path | None = None,
    index_path: Path | None = None,
    snapshot_root: Path | None = None,
    now: datetime,
) -> dict[str, object]:
    """Inspect approval/read evidence and trajectory coverage without GCS I/O."""

    base = _full_refresh_base(root)
    if root is None and manifest_path is None:
        return base
    effective_root = (
        root.expanduser().resolve()
        if root is not None
        else manifest_path.expanduser().resolve().parent  # type: ignore[union-attr]
    )
    manifest = (manifest_path or effective_root / "read-manifest.json").expanduser().resolve()
    approval = (approval_path or effective_root / "read-approval.json").expanduser().resolve()
    status_file = (
        refresh_status_path or effective_root / "refresh-status.json"
    ).expanduser().resolve()
    index_file = (index_path or effective_root / "latest-index.json").expanduser().resolve()
    snapshot_dir = (snapshot_root or effective_root / "snapshots").expanduser().resolve()
    report = _full_refresh_base(effective_root)
    report.update(
        {
            "manifest_path": str(manifest),
            "approval_path": str(approval),
            "refresh_status_path": str(status_file),
            "index_path": str(index_file),
            "snapshot_root": str(snapshot_dir),
        }
    )

    manifest_state, raw_manifest, manifest_size = _read_bounded_json(
        manifest, limit_bytes=_SNAPSHOT_READ_LIMIT_BYTES
    )
    if manifest_size is not None:
        report["manifest_size_bytes"] = manifest_size
    manifest_obj: Any = None
    if manifest_state == "missing":
        report.update({"manifest_state": "missing", "access_state": "manifest_missing"})
        report["load_state"] = "missing"
        return report
    if manifest_state == "too_large":
        report.update(
            {
                "manifest_state": "too_large_for_metadata_report",
                "access_state": "manifest_too_large",
            }
        )
        report["load_state"] = "invalid"
        return report
    if manifest_state != "available" or not isinstance(raw_manifest, Mapping):
        report.update({"manifest_state": "invalid", "access_state": "manifest_invalid"})
        report["load_state"] = "invalid"
        return report
    try:
        from polybot.weathernext_manifest import WeatherNextFullReadManifest, verify_manifest_sha256

        manifest_obj = WeatherNextFullReadManifest.model_validate(raw_manifest)
    except Exception:
        report.update({"manifest_state": "invalid", "access_state": "manifest_invalid"})
        report["load_state"] = "invalid"
        return report
    report["manifest_sha256"] = manifest_obj.manifest_sha256
    if not verify_manifest_sha256(manifest_obj):
        report.update({"manifest_state": "digest_mismatch", "access_state": "manifest_invalid"})
        report["load_state"] = "invalid"
        return report
    report.update(
        {
            "manifest_state": "valid",
            "release_id": manifest_obj.release_id,
            "init_time_utc": _iso(manifest_obj.init_time_utc),
            "expected_target_count": len(manifest_obj.targets),
            "expected_network_bytes": manifest_obj.approval_gate.expected_network_bytes,
            "expected_object_count": manifest_obj.approval_gate.object_count,
            "provenance": {
                "source_uri": manifest_obj.source_uri,
                "release_id": manifest_obj.release_id,
                "init_time_utc": _iso(manifest_obj.init_time_utc),
                "variable": manifest_obj.variable,
                "units": manifest_obj.units,
                "metadata_only": manifest_obj.metadata_only,
                "payload_read": manifest_obj.payload_read,
                "expected_network_bytes": manifest_obj.approval_gate.expected_network_bytes,
                "object_count": manifest_obj.approval_gate.object_count,
                "max_network_bytes": manifest_obj.approval_gate.max_network_bytes,
                "max_objects": manifest_obj.approval_gate.max_objects,
            },
        }
    )

    # Approval is checked through the same fail-closed local verifier used by
    # the reader.  No GCS client is instantiated here.
    try:
        from polybot.weathernext_autonomy import verify_read_approval

        approval_result = verify_read_approval(
            manifest,
            approval,
            now_utc=now,
        )
    except Exception:
        approval_result = None
    approval_state = "invalid"
    if approval_result is not None:
        approval_state = str(approval_result.state)
        report.update(
            {
                "expected_network_bytes": approval_result.expected_network_bytes
                or report.get("expected_network_bytes"),
                "expected_object_count": approval_result.object_count
                or report.get("expected_object_count"),
                # Do not copy verifier/parser text into a report: malformed
                # sidecars can contain arbitrary values, including secrets.
                "approval_message": f"local approval state: {approval_state}",
            }
        )
    approval_file_state, raw_approval, approval_size = _read_bounded_json(
        approval, limit_bytes=_FULL_STATUS_READ_LIMIT_BYTES
    )
    if approval_size is not None:
        report["approval_size_bytes"] = approval_size
    if approval_file_state == "too_large":
        approval_state = "invalid"
    elif approval_file_state == "available" and isinstance(raw_approval, Mapping):
        expires = _parse_datetime(raw_approval.get("expires_at_utc"))
        approved_at = _parse_datetime(raw_approval.get("approved_at_utc"))
        report["approval_expires_at_utc"] = _iso(expires)
        report["approval_approved_at_utc"] = _iso(approved_at)
        if expires is not None and now >= expires:
            approval_state = "expired"
    report.update(
        {
            "approval_state": approval_state,
            "approval_valid": approval_state == "approved",
            "access_state": "approved" if approval_state == "approved" else approval_state,
        }
    )

    # Refresh status is evidence produced by the bounded sequential reader;
    # approval alone is never treated as a payload read.
    status_state, raw_status, status_size = _read_bounded_json(
        status_file, limit_bytes=_FULL_STATUS_READ_LIMIT_BYTES
    )
    if status_size is not None:
        report["refresh_status_size_bytes"] = status_size
    status_obj: Any = None
    if status_state == "available" and isinstance(raw_status, Mapping):
        try:
            from polybot.weathernext_autonomy import WeatherNextRefreshStatus

            status_obj = WeatherNextRefreshStatus.model_validate(raw_status)
        except Exception:
            status_state = "invalid"
    elif status_state == "too_large":
        status_state = "too_large_for_metadata_report"
    report["refresh_status_state"] = status_state
    status_path_matches = False
    status_approval_matches = False
    status_read_evidence = False
    if status_obj is not None:
        status_manifest_path = Path(status_obj.manifest_path).expanduser().resolve()
        status_path_matches = status_manifest_path == manifest
        status_approval_matches = (
            status_obj.approval.manifest_sha256 == manifest_obj.manifest_sha256
        )
        report.update(
            {
                "refresh_generated_at_utc": _iso(status_obj.generated_at_utc),
                "payload_read": bool(status_obj.payload_read),
                "status_target_count": status_obj.target_count,
                "snapshots_written": status_obj.snapshots_written,
                "status_manifest_state": status_obj.manifest_state,
                "status_path_matches_manifest": status_path_matches,
                "status_approval_matches_manifest": status_approval_matches,
            }
        )
        status_read_evidence = bool(
            status_obj.payload_read and status_path_matches and status_approval_matches
        )
    else:
        report.update(
            {
                "payload_read": False,
                "status_path_matches_manifest": False,
                "status_approval_matches_manifest": False,
            }
        )
    report["read_evidence"] = status_read_evidence

    # The index is useful for inventory, but the current writer does not bind
    # it to a manifest digest.  Therefore it is never accepted as the sole
    # provenance proof and is explicitly labelled not_bound.
    index_state, raw_index, index_size = _read_bounded_json(
        index_file, limit_bytes=_FULL_INDEX_READ_LIMIT_BYTES
    )
    if index_size is not None:
        report["index_size_bytes"] = index_size
    index_entries: list[Mapping[str, object]] = []
    if index_state == "available" and isinstance(raw_index, Mapping):
        raw_entries = raw_index.get("entries")
        schema_ok = raw_index.get("schema_version") == "weathernext-full-snapshot-index/v1"
        if not schema_ok or not isinstance(raw_entries, list):
            index_state = "invalid"
        else:
            index_entries = [item for item in raw_entries if isinstance(item, Mapping)]
            index_state = "valid"
    elif index_state == "too_large":
        index_state = "too_large_for_metadata_report"
    report["index_state"] = index_state
    digest_values = {
        str(item.get("manifest_sha256"))
        for item in index_entries
        if item.get("manifest_sha256")
    }
    if digest_values:
        report["index_manifest_binding"] = (
            "bound" if digest_values == {manifest_obj.manifest_sha256} else "mismatch"
        )
    else:
        report["index_manifest_binding"] = "not_bound"

    # Validate every target directly from the manifest paths.  This remains
    # bounded and local; no array, chunk, or shard is fetched.
    target_records: list[dict[str, object]] = []
    complete: list[str] = []
    partial: list[str] = []
    missing: list[str] = []
    seen_targets: set[str] = set()
    for raw_target in manifest_obj.targets:
        target = raw_target if isinstance(raw_target, Mapping) else {}
        target_id = str(target.get("target_id") or target.get("station_id") or "").strip()
        if target_id in seen_targets:
            record = {
                "target_id": target_id or None,
                "status": "partial",
                "error_code": "duplicate_target_id",
            }
        else:
            seen_targets.add(target_id)
            record = _full_target_snapshot_report(
                target,
                snapshot_root=snapshot_dir,
                release_id=manifest_obj.release_id,
                init_time_utc=manifest_obj.init_time_utc,
            )
        target_records.append(record)
        if record.get("status") == "complete":
            complete.append(str(record.get("target_id")))
        elif record.get("status") == "missing":
            missing.append(str(record.get("target_id")))
        else:
            partial.append(str(record.get("target_id")))

    expected_index_paths = {
        str(Path(str(target.get("snapshot_path"))).expanduser().resolve())
        for target in manifest_obj.targets
        if isinstance(target, Mapping) and target.get("snapshot_path")
    }
    indexed_paths = {
        str(Path(str(item.get("path"))).expanduser().resolve())
        for item in index_entries
        if item.get("path")
    }
    index_complete = index_state == "valid" and expected_index_paths.issubset(indexed_paths)
    if index_state == "valid" and not index_complete:
        index_state = "incomplete"
    report["index_state"] = index_state
    read_evidence = status_read_evidence and index_complete
    report["read_evidence"] = read_evidence

    # Keep the report bounded even if an operator accidentally places a huge
    # manifest in the directory. Counts remain exact; detail rows are capped.
    report["targets"] = target_records[:_FULL_MAX_TARGET_REPORT]
    report.update(
        {
            "actual_target_count": len(
                [item for item in target_records if item.get("status") != "missing"]
            ),
            "complete_target_count": len(complete),
            "partial_target_count": len(partial),
            "missing_target_count": len(missing),
            "complete_targets": complete[:_FULL_MAX_TARGET_REPORT],
            "partial_targets": partial[:_FULL_MAX_TARGET_REPORT],
            "missing_targets": missing[:_FULL_MAX_TARGET_REPORT],
            "member_hour_coverage": {
                "expected_member_count": 64,
                "complete_member_target_count": sum(
                    item.get("member_coverage") == "complete" for item in target_records
                ),
                "complete_hour_target_count": sum(
                    item.get("hour_coverage") == "complete" for item in target_records
                ),
            },
        }
    )
    if not target_records or len(missing) == len(target_records):
        coverage_state = "missing"
    elif missing or partial:
        coverage_state = "partial"
    else:
        coverage_state = "complete"
    report["coverage_state"] = coverage_state
    if coverage_state == "complete" and read_evidence and approval_state == "approved":
        report["load_state"] = "approved_and_complete"
    elif coverage_state == "complete" and not read_evidence:
        report["load_state"] = "complete_without_read_evidence"
    elif coverage_state == "partial":
        report["load_state"] = "partial"
    else:
        report["load_state"] = "missing"
    return report


def _source_report(
    connection: sqlite3.Connection,
    tables: set[str],
    *,
    cutoff: datetime,
    now: datetime,
    weathernext_full_root: Path | None,
    weathernext_manifest_path: Path | None,
    weathernext_approval_path: Path | None,
    weathernext_refresh_status_path: Path | None,
    weathernext_snapshot_index_path: Path | None,
    weathernext_snapshot_root: Path | None,
    weathernext_full_snapshot_path: Path | None,
    weathernext_statistics_snapshot_path: Path | None,
) -> dict[str, object]:
    predictions = _prediction_stats(connection, tables, cutoff=cutoff, now=now)
    attempts = _attempt_stats(connection, tables, cutoff=cutoff, now=now)
    model_sources = _model_source_stats(connection, tables)
    source_statuses = _source_statuses(connection, tables)
    weather_snapshots = _timestamped_table_stats(
        connection,
        tables,
        "weather_snapshots",
        "fetched_at",
        cutoff=cutoff,
        now=now,
    )
    weathernext_snapshots = _timestamped_table_stats(
        connection,
        tables,
        "weathernext_snapshots",
        "captured_at",
        cutoff=cutoff,
        now=now,
    )

    def algorithm(version: str) -> dict[str, object]:
        values = predictions.get(
            version,
            {"total_count": 0, "period_count": 0, "latest_issued_at_utc": None},
        )
        return {
            "algorithm_version": version,
            "predictions": values,
            "production_cohort_attempts": attempts.get(version, {}),
        }

    ecmwf_models = {
        key: value
        for key, value in model_sources.items()
        if "ecmwf" in str(value.get("source", "")).lower()
        or "ecmwf" in str(value.get("model", "")).lower()
    }
    ecmwf_statuses = {
        key: value for key, value in source_statuses.items() if "ecmwf" in key.lower()
    }
    # ``weathernext_full_snapshot_path`` is retained as a legacy/diagnostic
    # input.  Approval-gated trajectories are reported separately and can only
    # become available after local manifest, approval, status, index, and target
    # coverage checks all pass.
    legacy_full_snapshot = _local_snapshot(weathernext_full_snapshot_path, summary_only=False)
    full_snapshot = _weathernext_full_refresh_report(
        weathernext_full_root,
        manifest_path=weathernext_manifest_path,
        approval_path=weathernext_approval_path,
        refresh_status_path=weathernext_refresh_status_path,
        index_path=weathernext_snapshot_index_path,
        snapshot_root=weathernext_snapshot_root,
        now=now,
    )
    full_snapshot["legacy_snapshot"] = legacy_full_snapshot
    statistics_snapshot = _local_snapshot(weathernext_statistics_snapshot_path, summary_only=True)
    return {
        "v1": {
            **algorithm(OPEN_METEO_ALGORITHM_VERSION),
            "weather_snapshots": weather_snapshots,
            "state": _source_state(
                predictions.get(OPEN_METEO_ALGORITHM_VERSION), weather_snapshots
            ),
        },
        "ecmwf": {
            **algorithm(ECMWF_RAW_ALGORITHM_VERSION),
            "model_runs": ecmwf_models,
            "latest_source_statuses": ecmwf_statuses,
            "state": _source_state(predictions.get(ECMWF_RAW_ALGORITHM_VERSION)),
        },
        "v2": {
            **algorithm(FORECAST_V2_ALGORITHM_VERSION),
            "state": _source_state(predictions.get(FORECAST_V2_ALGORITHM_VERSION)),
        },
        "weathernext": {
            **algorithm(WEATHERNEXT_ALGORITHM_VERSION),
            "database_snapshots": weathernext_snapshots,
            "full_ensemble_snapshot": full_snapshot,
            "full_refresh": full_snapshot,
            "legacy_snapshot": legacy_full_snapshot,
            "statistics_snapshot": statistics_snapshot,
            "state": _source_state(
                predictions.get(WEATHERNEXT_ALGORITHM_VERSION),
                weathernext_snapshots,
                full_snapshot,
                statistics_snapshot,
            ),
            "comparison_only": True,
        },
        "all_model_runs": model_sources,
        "all_latest_source_statuses": source_statuses,
    }


def _source_state(*items: Mapping[str, object] | None) -> str:
    total = 0
    period = 0
    locally_available = False
    for item in items:
        if not item:
            continue
        total += _as_int(item.get("total_count"))
        period += _as_int(item.get("period_count"))
        locally_available = locally_available or item.get("load_state") in {
            "available",
            "approved_and_complete",
        }
    if period:
        return "active_in_period"
    if total or locally_available:
        return "available_historical_or_local"
    return "unavailable_or_not_configured"


def _sanitize_background(value: object) -> object:
    if isinstance(value, bool) or value is None:
        return value
    if isinstance(value, (int, float)):
        return value if math.isfinite(float(value)) else None
    if isinstance(value, str):
        return "[text omitted]"
    if isinstance(value, list):
        return [_sanitize_background(item) for item in value[:20]]
    if isinstance(value, dict):
        result: dict[str, object] = {}
        for key, item in value.items():
            normalized = str(key).lower()
            if isinstance(item, (bool, int, float)) or item is None:
                result[str(key)] = _sanitize_background(item)
            elif isinstance(item, str) and any(
                token in normalized for token in ("status", "state", "mode", "lane", "reason")
            ):
                result[str(key)] = item[:200]
            elif isinstance(item, (dict, list)):
                nested = _sanitize_background(item)
                if nested not in ({}, []):
                    result[str(key)] = nested
        return result
    return None


def _find_background_payloads(value: object, path: str = "$") -> list[tuple[str, object]]:
    found: list[tuple[str, object]] = []
    if isinstance(value, dict):
        for key, item in value.items():
            normalized = str(key).lower().replace("-", "_")
            child_path = f"{path}.{key}"
            if any(token in normalized for token in _BACKGROUND_PAYLOAD_TOKENS):
                found.append((child_path, _sanitize_background(item)))
            found.extend(_find_background_payloads(item, child_path))
    elif isinstance(value, list):
        for index, item in enumerate(value[:100]):
            found.extend(_find_background_payloads(item, f"{path}[{index}]"))
    return found


def _background_table_summary(
    connection: sqlite3.Connection,
    table: str,
    *,
    cutoff: datetime,
    now: datetime,
) -> dict[str, Any]:
    columns = _column_info(connection, table)
    quoted_table = _quote_identifier(table)
    time_column = next((item for item in _BACKGROUND_TIME_COLUMNS if item in columns), None)
    where = ""
    parameters: tuple[object, ...] = ()
    if time_column is not None:
        quoted_time = _quote_identifier(time_column)
        where = f" WHERE {quoted_time} >= ? AND {quoted_time} <= ?"
        parameters = (cutoff.isoformat(), now.isoformat())
    total_count = int(connection.execute(f"SELECT COUNT(*) FROM {quoted_table}").fetchone()[0])
    period_count = int(
        connection.execute(f"SELECT COUNT(*) FROM {quoted_table}{where}", parameters).fetchone()[0]
    )
    status_counts: dict[str, int] = {}
    status_column = next((item for item in _BACKGROUND_STATUS_COLUMNS if item in columns), None)
    if status_column is not None:
        quoted_status = _quote_identifier(status_column)
        rows = connection.execute(
            f"SELECT {quoted_status}, COUNT(*) FROM {quoted_table}{where} GROUP BY {quoted_status}",
            parameters,
        ).fetchall()
        status_counts = {str(row[0]): int(row[1]) for row in rows}
    latest = None
    if time_column is not None:
        raw = connection.execute(
            f"SELECT MAX({_quote_identifier(time_column)}) FROM {quoted_table}"
        ).fetchone()[0]
        latest = _iso(_parse_datetime(raw))

    numeric_sums: dict[str, float] = {}
    for name, declaration in columns.items():
        normalized = name.lower()
        if name == "id" or not any(
            marker in declaration for marker in ("INT", "REAL", "FLOA", "DOUB", "NUM")
        ):
            continue
        if not any(
            token in normalized
            for token in ("count", "event", "processed", "success", "fail", "skip", "duration")
        ):
            continue
        raw = connection.execute(
            f"SELECT SUM({_quote_identifier(name)}) FROM {quoted_table}{where}", parameters
        ).fetchone()[0]
        if raw is not None:
            numeric_sums[name] = float(raw)
    return {
        "table": table,
        "time_column": time_column,
        "total_count": total_count,
        "period_count": period_count,
        "latest_at_utc": latest,
        "status_counts": status_counts,
        "period_numeric_sums": numeric_sums,
    }


def _background_report(
    connection: sqlite3.Connection,
    tables: set[str],
    *,
    cutoff: datetime,
    now: datetime,
) -> dict[str, object]:
    candidate_tables = sorted(
        table
        for table in tables
        if any(token in table.lower() for token in _BACKGROUND_TABLE_TOKENS)
        or ("outcome" in table.lower() and "refresh" in table.lower())
    )
    table_summaries = [
        _background_table_summary(connection, table, cutoff=cutoff, now=now)
        for table in candidate_tables
    ]
    payload_samples: list[dict[str, object]] = []
    if "runtime_reports" in tables:
        columns = _column_info(connection, "runtime_reports")
        if {"created_at", "payload_json"}.issubset(columns):
            rows = connection.execute(
                "SELECT created_at, payload_json FROM runtime_reports "
                "WHERE created_at >= ? AND created_at <= ? ORDER BY created_at DESC LIMIT 500",
                (cutoff.isoformat(), now.isoformat()),
            ).fetchall()
            for row in rows:
                try:
                    payload = json.loads(row["payload_json"])
                except (TypeError, ValueError):
                    continue
                for path, stats in _find_background_payloads(payload):
                    payload_samples.append(
                        {
                            "reported_at_utc": _iso(_parse_datetime(row["created_at"])),
                            "path": path,
                            "stats": stats,
                        }
                    )
                    if len(payload_samples) >= 20:
                        break
                if len(payload_samples) >= 20:
                    break
    available = bool(table_summaries or payload_samples)
    return {
        "available": available,
        "tables": table_summaries,
        "runtime_report_sample_count": len(payload_samples),
        "runtime_report_samples": payload_samples,
        "note": (
            "No background/research telemetry was found in the current schema or runtime reports."
            if not available
            else "Background telemetry is observational and separate from the v1 decision lane."
        ),
    }


def _file_time(timestamp: float) -> str:
    return datetime.fromtimestamp(timestamp, UTC).isoformat()


def _directory_inventory(path: Path | None) -> dict[str, object]:
    if path is None:
        return {
            "configured": False,
            "exists": False,
            "path": None,
            "file_count": 0,
            "total_bytes": 0,
            "completed_release_count": 0,
            "partial_directory_count": 0,
        }
    root = path.expanduser().resolve()
    base: dict[str, object] = {
        "configured": True,
        "exists": root.is_dir(),
        "path": str(root),
        "file_count": 0,
        "total_bytes": 0,
        "completed_release_count": 0,
        "partial_directory_count": 0,
        "oldest_file_at_utc": None,
        "newest_file_at_utc": None,
        "stat_error_count": 0,
    }
    if not root.is_dir():
        return base
    file_count = total_bytes = stat_errors = 0
    mtimes: list[float] = []
    manifests: set[Path] = set()
    partials: set[Path] = set()
    for current, directories, files in os.walk(root, followlinks=False):
        current_path = Path(current)
        directories[:] = [name for name in directories if not (current_path / name).is_symlink()]
        for name in directories:
            if name.startswith((".tmp", ".staging", "tmp-", "staging-")):
                partials.add(current_path / name)
        for name in files:
            candidate = current_path / name
            if candidate.is_symlink():
                continue
            try:
                stat = candidate.stat()
            except OSError:
                stat_errors += 1
                continue
            file_count += 1
            total_bytes += stat.st_size
            mtimes.append(stat.st_mtime)
            if name == "manifest.json":
                manifests.add(current_path)
    base.update(
        {
            "file_count": file_count,
            "total_bytes": total_bytes,
            "completed_release_count": len(manifests),
            "partial_directory_count": len(partials),
            "oldest_file_at_utc": _file_time(min(mtimes)) if mtimes else None,
            "newest_file_at_utc": _file_time(max(mtimes)) if mtimes else None,
            "stat_error_count": stat_errors,
        }
    )
    return base


def _weathernext_full_inventory(path: Path | None) -> dict[str, object]:
    """Inventory approval-gated WeatherNext artifacts without deleting anything."""

    if path is None:
        return {
            "configured": False,
            "exists": False,
            "path": None,
            "file_count": 0,
            "total_bytes": 0,
            "snapshot_file_count": 0,
            "manifest_file_count": 0,
            "approval_file_count": 0,
            "status_file_count": 0,
            "index_file_count": 0,
            "partial_directory_count": 0,
            "stat_error_count": 0,
            "retention_policy": {
                "mode": "inventory_only",
                "deletion_performed": False,
                "within_limits": None,
            },
        }
    root = path.expanduser().resolve()
    result: dict[str, object] = {
        "configured": True,
        "exists": root.is_dir(),
        "path": str(root),
        "file_count": 0,
        "total_bytes": 0,
        "snapshot_file_count": 0,
        "manifest_file_count": 0,
        "approval_file_count": 0,
        "status_file_count": 0,
        "index_file_count": 0,
        "partial_directory_count": 0,
        "stat_error_count": 0,
        "oldest_file_at_utc": None,
        "newest_file_at_utc": None,
        "retention_policy": {
            "mode": "inventory_only",
            "deletion_performed": False,
            "within_limits": None,
            "note": "No WeatherNext full snapshot is deleted or pruned by the report.",
        },
    }
    if not root.is_dir():
        return result
    mtimes: list[float] = []
    file_count = total_bytes = stat_errors = 0
    snapshot_count = manifest_count = approval_count = status_count = index_count = 0
    partial_count = 0
    for current, directories, files in os.walk(root, followlinks=False):
        current_path = Path(current)
        directories[:] = [name for name in directories if not (current_path / name).is_symlink()]
        partial_count += sum(
            name.startswith((".tmp", ".staging", "tmp-", "staging-"))
            for name in directories
        )
        for name in files:
            candidate = current_path / name
            if candidate.is_symlink():
                continue
            try:
                stat = candidate.stat()
            except OSError:
                stat_errors += 1
                continue
            file_count += 1
            total_bytes += stat.st_size
            mtimes.append(stat.st_mtime)
            if current_path == root / "snapshots" or (root / "snapshots") in current_path.parents:
                snapshot_count += 1
            if name == "read-manifest.json":
                manifest_count += 1
            elif name == "read-approval.json":
                approval_count += 1
            elif name == "refresh-status.json":
                status_count += 1
            elif name == "latest-index.json":
                index_count += 1
    result.update(
        {
            "file_count": file_count,
            "total_bytes": total_bytes,
            "snapshot_file_count": snapshot_count,
            "manifest_file_count": manifest_count,
            "approval_file_count": approval_count,
            "status_file_count": status_count,
            "index_file_count": index_count,
            "partial_directory_count": partial_count,
            "stat_error_count": stat_errors,
            "oldest_file_at_utc": _file_time(min(mtimes)) if mtimes else None,
            "newest_file_at_utc": _file_time(max(mtimes)) if mtimes else None,
        }
    )
    return result


def _ecmwf_raw_inventory(
    path: Path,
    *,
    max_completed_releases: int,
    max_completed_bytes: int,
    min_free_bytes: int,
    min_free_fraction: float,
) -> dict[str, object]:
    """Combine physical size with the archive adapter's fail-closed inventory."""

    physical = _directory_inventory(path)
    physical["manifest_candidate_count"] = physical.pop("completed_release_count", 0)
    policy = EcmwfArchiveRetentionPolicy(
        max_completed_releases=max_completed_releases,
        max_completed_bytes=max_completed_bytes,
        min_free_bytes=min_free_bytes,
        min_free_fraction=min_free_fraction,
    )
    try:
        status = EcmwfIfsEnsAdapter(
            archive_root=path,
            retention_policy=policy,
        ).retention_status()
    except (OSError, ValueError):
        physical.update(
            {
                "completed_release_count": None,
                "completed_bytes": None,
                "protected_release_count": None,
                "protected_bytes": None,
                "preserved_diagnostic_count": None,
                "preserved_diagnostic_bytes": None,
                "retention_status": {
                    "available": False,
                    "error": "retention inventory could not be read",
                },
            }
        )
        return physical
    if status is None:
        raise RuntimeError("ECMWF retention adapter returned no status for an explicit policy")
    values = asdict(status)
    physical.update(
        {
            "completed_release_count": status.completed_releases,
            "completed_bytes": status.completed_bytes,
            "protected_release_count": status.protected_releases,
            "protected_bytes": status.protected_bytes,
            "preserved_diagnostic_count": status.preserved_diagnostics,
            "preserved_diagnostic_bytes": status.preserved_diagnostic_bytes,
            "retention_status": {"available": True, **values},
        }
    )
    return physical


def _backup_report(
    root: Path,
    *,
    now: datetime,
    keep_limit: int,
) -> dict[str, object]:
    path = root.expanduser().resolve()
    base: dict[str, object] = {
        "path": str(path),
        "exists": path.is_dir(),
        "backup_count": 0,
        "backup_bytes": 0,
        "checksum_count": 0,
        "checksum_bytes": 0,
        "configured_keep_limit": keep_limit,
        "within_count_limit": True,
        "oldest_backup_at_utc": None,
        "latest_backup_at_utc": None,
        "latest_backup_age_seconds": None,
        "observed_retention_span_hours": None,
        "observed_interval_seconds": _distribution([]),
    }
    if not path.is_dir():
        return base
    backups: list[tuple[Path, os.stat_result]] = []
    checksums: list[os.stat_result] = []
    for candidate in path.iterdir():
        if not candidate.is_file() or candidate.is_symlink():
            continue
        try:
            stat = candidate.stat()
        except OSError:
            continue
        if candidate.name.startswith("polybot-") and candidate.name.endswith(".sqlite3"):
            backups.append((candidate, stat))
        elif candidate.name.startswith("polybot-") and candidate.name.endswith(".sqlite3.sha256"):
            checksums.append(stat)
    backups.sort(key=lambda item: item[1].st_mtime)
    mtimes = [item[1].st_mtime for item in backups]
    intervals = [later - earlier for earlier, later in zip(mtimes, mtimes[1:], strict=False)]
    latest_time = datetime.fromtimestamp(mtimes[-1], UTC) if mtimes else None
    oldest_time = datetime.fromtimestamp(mtimes[0], UTC) if mtimes else None
    base.update(
        {
            "backup_count": len(backups),
            "backup_bytes": sum(item[1].st_size for item in backups),
            "checksum_count": len(checksums),
            "checksum_bytes": sum(item.st_size for item in checksums),
            "within_count_limit": len(backups) <= keep_limit,
            "oldest_backup_at_utc": _iso(oldest_time),
            "latest_backup_at_utc": _iso(latest_time),
            "latest_backup_age_seconds": (
                None
                if latest_time is None
                else _round(max(0.0, (now - latest_time).total_seconds()))
            ),
            "observed_retention_span_hours": (
                None
                if oldest_time is None or latest_time is None
                else _round((latest_time - oldest_time).total_seconds() / 3600)
            ),
            "observed_interval_seconds": _distribution(intervals),
        }
    )
    return base


def _database_files(path: Path) -> dict[str, object]:
    database = path.expanduser().resolve()
    files: dict[str, int] = {}
    for label, candidate in (
        ("database", database),
        ("wal", Path(f"{database}-wal")),
        ("shm", Path(f"{database}-shm")),
    ):
        with suppress(OSError):
            if candidate.is_file():
                files[label] = candidate.stat().st_size
    return {
        "path": str(database),
        "exists": database.is_file(),
        "files": files,
        "total_bytes": sum(files.values()),
    }


def _closest_existing_path(path: Path) -> Path:
    candidate = path.expanduser().resolve()
    while not candidate.exists() and candidate != candidate.parent:
        candidate = candidate.parent
    return candidate


def _disk_snapshot(path: Path) -> dict[str, object]:
    measured = _closest_existing_path(path)
    usage = shutil.disk_usage(measured)
    return {
        "requested_path": str(path.expanduser().resolve()),
        "measured_path": str(measured),
        "total_bytes": usage.total,
        "used_bytes": usage.used,
        "free_bytes": usage.free,
        "used_percent": _round(100 * usage.used / usage.total if usage.total else 0),
    }


def _read_proc_text(path: Path) -> str | None:
    try:
        return path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return None


def _process_inventory(proc_root: Path) -> dict[str, object]:
    if not proc_root.is_dir():
        return {
            "available": False,
            "roles": {},
            "total_rss_bytes": None,
            "paper_observer_count": None,
            "non_paper_observer_count": None,
        }
    roles: dict[str, dict[str, int]] = defaultdict(lambda: {"count": 0, "rss_bytes": 0})
    paper_observer_count = 0
    non_paper_observer_count = 0
    for candidate in proc_root.iterdir():
        if not candidate.name.isdigit() or not candidate.is_dir():
            continue
        try:
            raw = (candidate / "cmdline").read_bytes()
        except OSError:
            continue
        arguments = [item.decode("utf-8", errors="replace") for item in raw.split(b"\0") if item]
        normalized_arguments = [item.lower() for item in arguments]
        basenames = {Path(item).name.lower() for item in arguments}
        joined = " ".join(arguments).lower()
        if "polybot" not in basenames and not any(
            name.startswith("collect-ecmwf") for name in basenames
        ):
            continue
        if "dashboard" in normalized_arguments:
            role = "dashboard"
        elif "run" in normalized_arguments:
            role = "observer"
        elif "collect-ecmwf" in joined:
            role = "ecmwf_archiver"
        else:
            role = "other"
        if role == "observer":
            if "--paper" in normalized_arguments:
                paper_observer_count += 1
            else:
                non_paper_observer_count += 1
        rss_bytes = 0
        status = _read_proc_text(candidate / "status") or ""
        for line in status.splitlines():
            if line.startswith("VmRSS:"):
                parts = line.split()
                if len(parts) >= 2:
                    with suppress(ValueError):
                        rss_bytes = int(parts[1]) * 1024
                break
        roles[role]["count"] += 1
        roles[role]["rss_bytes"] += rss_bytes
    result = {name: dict(values) for name, values in sorted(roles.items())}
    return {
        "available": True,
        "roles": result,
        "total_rss_bytes": sum(values["rss_bytes"] for values in roles.values()),
        "paper_observer_count": paper_observer_count,
        "non_paper_observer_count": non_paper_observer_count,
    }


def _system_snapshot(
    *,
    proc_root: Path,
    disk_path: Path,
    cpu_count: int | None,
) -> dict[str, object]:
    logical_cpus = max(1, int(cpu_count or os.cpu_count() or 1))
    load_values: list[float] = []
    load_text = _read_proc_text(proc_root / "loadavg")
    if load_text:
        with suppress(ValueError):
            load_values = [float(item) for item in load_text.split()[:3]]
    if not load_values:
        with suppress(OSError):
            load_values = [float(item) for item in os.getloadavg()]
    cpu: dict[str, object] = {
        "logical_count": logical_cpus,
        "load_average_1m": _round(load_values[0] if len(load_values) > 0 else None),
        "load_average_5m": _round(load_values[1] if len(load_values) > 1 else None),
        "load_average_15m": _round(load_values[2] if len(load_values) > 2 else None),
        "load_per_cpu_1m": _round(load_values[0] / logical_cpus if load_values else None),
    }
    stat_text = _read_proc_text(proc_root / "stat")
    if stat_text:
        first = stat_text.splitlines()[0].split()
        if first and first[0] == "cpu":
            with suppress(ValueError):
                ticks = [int(item) for item in first[1:]]
                total = sum(ticks)
                idle = sum(ticks[index] for index in (3, 4) if index < len(ticks))
                cpu["busy_since_boot_percent"] = _round(
                    100 * (total - idle) / total if total else 0
                )

    memory: dict[str, object] = {"available": False}
    meminfo = _read_proc_text(proc_root / "meminfo")
    if meminfo:
        parsed: dict[str, int] = {}
        for line in meminfo.splitlines():
            name, separator, value = line.partition(":")
            if not separator:
                continue
            parts = value.split()
            if not parts:
                continue
            with suppress(ValueError):
                parsed[name] = int(parts[0]) * 1024
        total = parsed.get("MemTotal")
        available = parsed.get("MemAvailable", parsed.get("MemFree"))
        if total is not None and available is not None:
            used = max(0, total - available)
            memory = {
                "available": True,
                "total_bytes": total,
                "available_bytes": available,
                "used_bytes": used,
                "used_percent": _round(100 * used / total if total else 0),
                "swap_total_bytes": parsed.get("SwapTotal", 0),
                "swap_free_bytes": parsed.get("SwapFree", 0),
            }
    uptime = None
    uptime_text = _read_proc_text(proc_root / "uptime")
    if uptime_text:
        with suppress(ValueError):
            uptime = float(uptime_text.split()[0])
    return {
        "cpu": cpu,
        "memory": memory,
        "disk": _disk_snapshot(disk_path),
        "uptime_seconds": _round(uptime),
        "processes": _process_inventory(proc_root),
    }


def _paper_safety_report(
    connection: sqlite3.Connection,
    tables: set[str],
    *,
    cycles: Mapping[str, object],
    processes: Mapping[str, object],
) -> dict[str, object]:
    status_counts: dict[str, int] = {}
    strategy_counts: dict[str, int] = {}
    if "paper_orders" in tables:
        columns = _column_info(connection, "paper_orders")
        if "status" in columns:
            status_counts = {
                str(row[0]): int(row[1])
                for row in connection.execute(
                    "SELECT status, COUNT(*) FROM paper_orders GROUP BY status"
                ).fetchall()
            }
        if "strategy_version" in columns:
            strategy_counts = {
                str(row[0]): int(row[1])
                for row in connection.execute(
                    "SELECT strategy_version, COUNT(*) FROM paper_orders GROUP BY strategy_version"
                ).fetchall()
            }
    active_windows = 0
    active_paper_windows = 0
    if "runtime_windows" in tables:
        columns = _column_info(connection, "runtime_windows")
        if {"status", "paper"}.issubset(columns):
            row = connection.execute(
                "SELECT COUNT(*), COALESCE(SUM(CASE WHEN paper=1 THEN 1 ELSE 0 END), 0) "
                "FROM runtime_windows WHERE status='ACTIVE'"
            ).fetchone()
            active_windows = int(row[0])
            active_paper_windows = int(row[1])

    scan_modes = cycles.get("scan_modes", {})
    modes = set(scan_modes) if isinstance(scan_modes, dict) else set()
    unsafe_modes = sorted(mode for mode in modes if mode not in _SAFE_SCAN_MODES)
    live_rows = 0
    if isinstance(scan_modes, dict):
        for mode, statuses in scan_modes.items():
            if "live" in str(mode).lower() and isinstance(statuses, dict):
                live_rows += sum(int(value) for value in statuses.values())
    non_paper_order_tables = sorted(
        table for table in tables if "order" in table.lower() and not table.startswith("paper_")
    )
    process_roles = processes.get("roles", {})
    observer_process_count: int | None = None
    paper_observer_count: int | None = None
    non_paper_observer_count: int | None = None
    if bool(processes.get("available")) and isinstance(process_roles, dict):
        observer = process_roles.get("observer", {})
        observer_process_count = int(observer.get("count", 0)) if isinstance(observer, dict) else 0
        paper_observer_count = _as_int(processes.get("paper_observer_count"))
        non_paper_observer_count = _as_int(processes.get("non_paper_observer_count"))
    active_order_count = sum(
        count for status, count in status_counts.items() if status.upper() in _ACTIVE_PAPER_STATUSES
    )
    observer_paper_mode = (
        None
        if observer_process_count in (None, 0)
        else paper_observer_count == observer_process_count and non_paper_observer_count == 0
    )
    observer_mode_guard = (
        non_paper_observer_count == 0 if non_paper_observer_count is not None else False
    )
    live_disabled = (
        not unsafe_modes and live_rows == 0 and not non_paper_order_tables and observer_mode_guard
    )
    return {
        "live_executor_present_in_build": False,
        "live_scan_row_count": live_rows,
        "unexpected_scan_modes": unsafe_modes,
        "non_paper_order_tables": non_paper_order_tables,
        "live_disabled": live_disabled,
        "paper_order_status_counts": status_counts,
        "paper_strategy_version_counts": strategy_counts,
        "active_paper_order_count": active_order_count,
        "active_runtime_window_count": active_windows,
        "active_paper_runtime_window_count": active_paper_windows,
        "observer_process_count": observer_process_count,
        "paper_observer_count": paper_observer_count,
        "non_paper_observer_count": non_paper_observer_count,
        "observer_paper_mode": observer_paper_mode,
        "single_observer": None if observer_process_count is None else observer_process_count == 1,
        "v1_decision_lane_modified_by_report": False,
        "real_orders_sent_by_report": False,
        "weathernext_global_read_performed": False,
    }


def build_operational_report(
    database_path: Path,
    *,
    state_root: Path | None = None,
    backup_root: Path | None = None,
    ecmwf_json_root: Path | None = None,
    ecmwf_raw_root: Path | None = None,
    weathernext_full_root: Path | None = None,
    weathernext_manifest_path: Path | None = None,
    weathernext_approval_path: Path | None = None,
    weathernext_refresh_status_path: Path | None = None,
    weathernext_snapshot_index_path: Path | None = None,
    weathernext_snapshot_root: Path | None = None,
    weathernext_full_snapshot_path: Path | None = None,
    weathernext_statistics_snapshot_path: Path | None = None,
    period_hours: float = 24,
    now: datetime | None = None,
    proc_root: Path = Path("/proc"),
    disk_path: Path | None = None,
    cpu_count: int | None = None,
    backup_keep_limit: int = 12,
    ecmwf_raw_max_releases: int = 28,
    ecmwf_raw_max_bytes: int = 8 * 1024 * 1024 * 1024,
    ecmwf_raw_min_free_bytes: int = 50 * 1024 * 1024 * 1024,
    ecmwf_raw_min_free_fraction: float = 0.25,
) -> dict[str, Any]:
    """Build one bounded, read-only rolling operational report.

    The function never performs network I/O, never initializes/migrates SQLite,
    and never loads WeatherNext global arrays.  It reads only compact database
    rows, local snapshot metadata, file sizes, and procfs when available.
    """

    if period_hours <= 0 or period_hours > 24 * 31:
        raise ValueError("period_hours must be greater than 0 and no more than 744")
    if backup_keep_limit < 1:
        raise ValueError("backup_keep_limit must be at least 1")
    if ecmwf_raw_max_releases < 1 or ecmwf_raw_max_bytes < 1:
        raise ValueError("ECMWF raw retention limits must be positive")
    if ecmwf_raw_min_free_bytes < 0 or not 0 <= ecmwf_raw_min_free_fraction < 1:
        raise ValueError("ECMWF raw free-space limits are invalid")
    generated = _utc(now or datetime.now(UTC))
    cutoff = generated - timedelta(hours=period_hours)
    database = database_path.expanduser().resolve()
    if not database.is_file():
        raise FileNotFoundError(f"Polybot database does not exist: {database}")
    state = (state_root or database.parent).expanduser().resolve()
    backups = (backup_root or state / "backups").expanduser().resolve()
    raw_ecmwf = (ecmwf_raw_root or state / "forecasts" / "ecmwf-open-data").expanduser()
    json_ecmwf = None if ecmwf_json_root is None else ecmwf_json_root.expanduser()
    system = _system_snapshot(
        proc_root=proc_root,
        disk_path=disk_path or state,
        cpu_count=cpu_count,
    )

    connection = sqlite3.connect(f"{database.as_uri()}?mode=ro", uri=True, timeout=30)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA query_only = ON")
    try:
        connection.execute("BEGIN")
        tables = _table_names(connection)
        cycles = _cycle_report(
            connection,
            tables,
            cutoff=cutoff,
            now=generated,
            period_hours=period_hours,
        )
        marks = _mark_report(connection, tables, cutoff=cutoff, now=generated)
        sources = _source_report(
            connection,
            tables,
            cutoff=cutoff,
            now=generated,
            weathernext_full_root=weathernext_full_root,
            weathernext_manifest_path=weathernext_manifest_path,
            weathernext_approval_path=weathernext_approval_path,
            weathernext_refresh_status_path=weathernext_refresh_status_path,
            weathernext_snapshot_index_path=weathernext_snapshot_index_path,
            weathernext_snapshot_root=weathernext_snapshot_root,
            weathernext_full_snapshot_path=weathernext_full_snapshot_path,
            weathernext_statistics_snapshot_path=weathernext_statistics_snapshot_path,
        )
        background = _background_report(
            connection,
            tables,
            cutoff=cutoff,
            now=generated,
        )
        process_report = _as_mapping(system.get("processes"))
        safety = _paper_safety_report(
            connection,
            tables,
            cycles=cycles,
            processes=process_report,
        )
        connection.rollback()
    finally:
        connection.close()

    raw_inventory = _ecmwf_raw_inventory(
        raw_ecmwf,
        max_completed_releases=ecmwf_raw_max_releases,
        max_completed_bytes=ecmwf_raw_max_bytes,
        min_free_bytes=ecmwf_raw_min_free_bytes,
        min_free_fraction=ecmwf_raw_min_free_fraction,
    )
    return {
        "schema_version": 1,
        "generated_at_utc": generated.isoformat(),
        "period": {
            "hours": period_hours,
            "started_at_utc": cutoff.isoformat(),
            "ended_at_utc": generated.isoformat(),
        },
        "collection_guards": {
            "database_read_only": True,
            "network_access_performed": False,
            "weathernext_global_read_performed": False,
        },
        "system": system,
        "cycles": cycles,
        "paper_marks": marks,
        "background": background,
        "storage": {
            "database": _database_files(database),
            "backups": _backup_report(
                backups,
                now=generated,
                keep_limit=backup_keep_limit,
            ),
            "ecmwf_json_archive": _directory_inventory(json_ecmwf),
            "ecmwf_raw_archive": raw_inventory,
            "weathernext_full_archive": _weathernext_full_inventory(weathernext_full_root),
        },
        "scan_modes": cycles.get("scan_modes", {}),
        "paper_live_safety": safety,
        "sources": sources,
    }


def _path_from_env(name: str, fallback: str | None) -> Path | None:
    value = os.environ.get(name, fallback)
    return None if not value else Path(value)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Build a read-only rolling Polybot operational report."
    )
    parser.add_argument(
        "--database",
        type=Path,
        default=_path_from_env("POLYBOT_DATABASE_PATH", "data/polybot.sqlite3"),
    )
    parser.add_argument("--state-root", type=Path, default=None)
    parser.add_argument("--backup-root", type=Path, default=None)
    parser.add_argument(
        "--ecmwf-json-root",
        type=Path,
        default=_path_from_env(
            "POLYBOT_ECMWF_JSON_ARCHIVE_ROOT", "data/forecasts/ecmwf-ifs025-json"
        ),
    )
    parser.add_argument("--ecmwf-raw-root", type=Path, default=None)
    parser.add_argument(
        "--weathernext-full-root",
        type=Path,
        default=_path_from_env("POLYBOT_WEATHERNEXT_FULL_ROOT", None),
    )
    parser.add_argument("--weathernext-manifest", type=Path, default=None)
    parser.add_argument("--weathernext-approval", type=Path, default=None)
    parser.add_argument("--weathernext-refresh-status", type=Path, default=None)
    parser.add_argument("--weathernext-snapshot-index", type=Path, default=None)
    parser.add_argument("--weathernext-snapshot-root", type=Path, default=None)
    parser.add_argument(
        "--weathernext-full-snapshot",
        type=Path,
        default=_path_from_env("POLYBOT_WEATHERNEXT_SNAPSHOT_PATH", None),
    )
    parser.add_argument(
        "--weathernext-statistics-snapshot",
        type=Path,
        default=_path_from_env("POLYBOT_WEATHERNEXT_STATISTICS_SNAPSHOT_PATH", None),
    )
    parser.add_argument("--hours", type=float, default=24.0)
    parser.add_argument("--backup-keep", type=int, default=12)
    parser.add_argument("--ecmwf-raw-max-releases", type=int, default=28)
    parser.add_argument("--ecmwf-raw-max-bytes", type=int, default=8 * 1024 * 1024 * 1024)
    parser.add_argument("--ecmwf-raw-min-free-bytes", type=int, default=50 * 1024 * 1024 * 1024)
    parser.add_argument("--ecmwf-raw-min-free-fraction", type=float, default=0.25)
    parser.add_argument("--output", type=Path, default=None)
    parser.add_argument("--json", action="store_true", dest="as_json")
    return parser


def _human_bytes(value: object) -> str:
    if not isinstance(value, (int, float, str)):
        return "n/a"
    try:
        size = float(value)
    except (TypeError, ValueError):
        return "n/a"
    units = ("B", "KiB", "MiB", "GiB", "TiB")
    for unit in units:
        if abs(size) < 1024 or unit == units[-1]:
            return f"{size:.1f} {unit}"
        size /= 1024
    return "n/a"


def _human_report(report: Mapping[str, object]) -> str:
    system = _as_mapping(report.get("system"))
    cpu = _as_mapping(system.get("cpu"))
    memory = _as_mapping(system.get("memory"))
    disk = _as_mapping(system.get("disk"))
    cycles = _as_mapping(report.get("cycles"))
    duration = _as_mapping(cycles.get("duration_seconds"))
    marks = _as_mapping(report.get("paper_marks"))
    mark_interval = _as_mapping(marks.get("interval_seconds"))
    storage = _as_mapping(report.get("storage"))
    backups = _as_mapping(storage.get("backups"))
    raw = _as_mapping(storage.get("ecmwf_raw_archive"))
    safety = _as_mapping(report.get("paper_live_safety"))
    background = _as_mapping(report.get("background"))
    lines = [
        f"Polybot operational report: {report.get('generated_at_utc')}",
        (
            "CPU: "
            f"{cpu.get('logical_count', 'n/a')} logical, load1={cpu.get('load_average_1m')} | "
            f"RAM: {_human_bytes(memory.get('used_bytes'))} / "
            f"{_human_bytes(memory.get('total_bytes'))} | "
            f"disk: {_human_bytes(disk.get('used_bytes'))} / "
            f"{_human_bytes(disk.get('total_bytes'))}"
        ),
        (
            f"Cycles: completed={cycles.get('completed_count', 0)}, "
            f"failed={cycles.get('failed_count', 0)}, running={cycles.get('running_count', 0)}, "
            f"p50/p95/max={duration.get('p50')}/{duration.get('p95')}/{duration.get('max')} s"
        ),
        (
            f"Marks: count={marks.get('mark_count', 0)}, "
            f"p50/p95/max interval={mark_interval.get('p50')}/"
            f"{mark_interval.get('p95')}/{mark_interval.get('max')} s"
        ),
        (
            f"Backups: count={backups.get('backup_count', 0)}, "
            f"size={_human_bytes(backups.get('backup_bytes'))}, "
            f"retention={backups.get('observed_retention_span_hours')} h"
        ),
        (
            f"ECMWF raw: releases={raw.get('completed_release_count', 0)}, "
            f"size={_human_bytes(raw.get('total_bytes'))}"
        ),
        (
            f"Safety: live_disabled={safety.get('live_disabled')}, "
            f"observer_count={safety.get('observer_process_count')}, "
            f"background_available={background.get('available')}"
        ),
    ]
    return "\n".join(lines)


def main(argv: Sequence[str] | None = None) -> None:
    args = build_parser().parse_args(argv)
    report = build_operational_report(
        args.database,
        state_root=args.state_root,
        backup_root=args.backup_root,
        ecmwf_json_root=args.ecmwf_json_root,
        ecmwf_raw_root=args.ecmwf_raw_root,
        weathernext_full_root=args.weathernext_full_root,
        weathernext_manifest_path=args.weathernext_manifest,
        weathernext_approval_path=args.weathernext_approval,
        weathernext_refresh_status_path=args.weathernext_refresh_status,
        weathernext_snapshot_index_path=args.weathernext_snapshot_index,
        weathernext_snapshot_root=args.weathernext_snapshot_root,
        weathernext_full_snapshot_path=args.weathernext_full_snapshot,
        weathernext_statistics_snapshot_path=args.weathernext_statistics_snapshot,
        period_hours=args.hours,
        backup_keep_limit=args.backup_keep,
        ecmwf_raw_max_releases=args.ecmwf_raw_max_releases,
        ecmwf_raw_max_bytes=args.ecmwf_raw_max_bytes,
        ecmwf_raw_min_free_bytes=args.ecmwf_raw_min_free_bytes,
        ecmwf_raw_min_free_fraction=args.ecmwf_raw_min_free_fraction,
    )
    serialized = json.dumps(report, ensure_ascii=False, indent=2) + "\n"
    if args.output is not None:
        output = args.output.expanduser().resolve()
        output.parent.mkdir(parents=True, exist_ok=True)
        temporary = output.with_name(output.name + ".tmp")
        temporary.write_text(serialized, encoding="utf-8")
        temporary.replace(output)
    print(serialized if args.as_json else _human_report(report))


if __name__ == "__main__":
    main()
