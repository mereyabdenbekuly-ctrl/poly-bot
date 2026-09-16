"""Bounded, approval-gated WeatherNext refresh orchestration.

This module deliberately separates *planning* from payload reads.  The
observer can call :func:`derive_refresh_targets` and write a metadata-only
target inventory, but no GCS object body is touched until an operator-created
approval sidecar matches the exact manifest digest and limits.  The module
does not alter v1 decisions, paper orders, or the live executor.
"""

from __future__ import annotations

import hmac
import json
import math
import os
import resource
import shutil
import sqlite3
import sys
import tempfile
import time as monotonic_clock
from collections.abc import Callable, Iterable, Mapping, Sequence
from contextlib import suppress
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from typing import Any, Literal, cast
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from pydantic import Field, field_validator

from polybot.models import StrictModel, WeatherForecast
from polybot.weathernext_manifest import (
    WeatherNextCompressedObject,
    WeatherNextFullReadManifest,
    WeatherNextOneBlockProbeApproval,
    WeatherNextReadApproval,
    assess_station_local_day_coverage,
    expected_station_local_day_hours,
    validate_one_block_probe_approval,
    validate_read_approval,
    verify_manifest_sha256,
)

DEFAULT_ROOT = Path("/var/lib/polybot/weathernext/full")
DEFAULT_MANIFEST_PATH = DEFAULT_ROOT / "read-manifest.json"
DEFAULT_APPROVAL_PATH = DEFAULT_ROOT / "read-approval.json"
DEFAULT_PROBE_APPROVAL_PATH = DEFAULT_ROOT / "probe-approval.json"
DEFAULT_PROBE_ATTEMPT_PATH = DEFAULT_ROOT / "probe-attempt.json"
DEFAULT_TARGETS_PATH = DEFAULT_ROOT / "targets.json"
DEFAULT_STATUS_PATH = DEFAULT_ROOT / "refresh-status.json"


def _parse_utc(value: object, *, field: str) -> datetime:
    if not isinstance(value, str):
        raise ValueError(f"{field} must be an ISO timestamp")
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError(f"{field} must be timezone-aware")
    return parsed.astimezone(UTC)


def _aware(value: datetime, *, field: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field} must be timezone-aware")
    return value.astimezone(UTC)


def _process_peak_rss_bytes() -> int | None:
    """Return process peak RSS in bytes on the supported Unix hosts."""

    try:
        value = int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
    except (AttributeError, OSError, ValueError):
        return None
    # Linux reports KiB; macOS reports bytes.  The probe runs on Linux VPS,
    # but retaining the branch keeps local diagnostics truthful.
    return value if sys.platform == "darwin" else value * 1024


def _available_memory_bytes() -> int | None:
    """Return Linux MemAvailable when present; otherwise leave the gate unknown."""

    try:
        for line in Path("/proc/meminfo").read_text(encoding="utf-8").splitlines():
            if line.startswith("MemAvailable:"):
                return int(line.split()[1]) * 1024
    except (FileNotFoundError, IndexError, OSError, ValueError):
        return None
    return None


class WeatherNextRefreshTarget(StrictModel):
    """A normalized eligible event/station needing a full-ensemble snapshot."""

    event_id: str
    station_id: str
    location: str
    latitude: float
    longitude: float
    observation_date: date
    observation_timezone: str
    rule_day_end_utc: datetime
    source_fetched_at_utc: datetime

    @field_validator("latitude", "longitude")
    @classmethod
    def _finite_coordinate(cls, value: float) -> float:
        if not math.isfinite(value):
            raise ValueError("WeatherNext target coordinates must be finite")
        return float(value)

    @field_validator("longitude")
    @classmethod
    def _longitude_range(cls, value: float) -> float:
        if value < -180 or value > 360:
            raise ValueError("WeatherNext target longitude is outside [-180, 360]")
        return value

    @field_validator("observation_timezone")
    @classmethod
    def _timezone_exists(cls, value: str) -> str:
        cleaned = value.strip()
        try:
            ZoneInfo(cleaned)
        except ZoneInfoNotFoundError as error:
            raise ValueError(f"unknown WeatherNext target timezone: {cleaned}") from error
        return cleaned

    @field_validator("rule_day_end_utc", "source_fetched_at_utc")
    @classmethod
    def _timestamps_aware(cls, value: datetime) -> datetime:
        return _aware(value, field="WeatherNext target timestamp")


class WeatherNextTargetInventory(StrictModel):
    schema_version: Literal["weathernext-target-inventory/v1"] = (
        "weathernext-target-inventory/v1"
    )
    generated_at_utc: datetime
    source: Literal["forecast_evaluation_registry_and_weather_snapshots"] = (
        "forecast_evaluation_registry_and_weather_snapshots"
    )
    targets: list[WeatherNextRefreshTarget]
    skipped_count: int = Field(ge=0)

    @field_validator("generated_at_utc")
    @classmethod
    def _generated_aware(cls, value: datetime) -> datetime:
        return _aware(value, field="generated_at_utc")


class WeatherNextApprovalResult(StrictModel):
    state: Literal[
        "missing",
        "invalid",
        "manifest_mismatch",
        "limit_mismatch",
        "coverage_blocked",
        "approved",
    ]
    payload_read_permitted: bool = False
    manifest_sha256: str | None = None
    message: str
    expected_network_bytes: int = 0
    object_count: int = 0
    incomplete_target_ids: list[str] = Field(default_factory=list)


class WeatherNextRefreshStatus(StrictModel):
    schema_version: Literal["weathernext-refresh-status/v1"] = "weathernext-refresh-status/v1"
    generated_at_utc: datetime
    targets_path: str
    manifest_path: str
    approval_path: str
    target_count: int = Field(ge=0)
    manifest_state: str
    approval: WeatherNextApprovalResult
    payload_read: bool = False
    snapshots_written: int = Field(ge=0)
    message: str
    coverage_complete: bool = True
    incomplete_target_ids: list[str] = Field(default_factory=list)
    mixed_observation_dates: bool = False

    @field_validator("generated_at_utc")
    @classmethod
    def _status_time_aware(cls, value: datetime) -> datetime:
        return _aware(value, field="generated_at_utc")


class WeatherNextSequentialReadResult(StrictModel):
    """Result of one approved, one-object-at-a-time payload pass."""

    payload_read: bool
    manifest_sha256: str
    snapshots_written: int = Field(ge=0)
    snapshot_paths: list[str]
    bytes_read: int = Field(ge=0)
    object_count: int = Field(ge=0)
    message: str
    probe_only: bool = False
    elapsed_seconds: float | None = Field(default=None, ge=0)
    decoded_shape: list[int] = Field(default_factory=list)
    decoded_bytes: int = Field(default=0, ge=0)
    object_uri: str | None = None
    compressed_bytes: int = Field(default=0, ge=0)
    download_seconds: float | None = Field(default=None, ge=0)
    decode_seconds: float | None = Field(default=None, ge=0)
    peak_memory_bytes: int | None = Field(default=None, ge=0)
    temporary_path: str | None = None
    temporary_bytes: int = Field(default=0, ge=0)
    temporary_retained: bool = False
    extracted_values: list[dict[str, object]] = Field(default_factory=list)


class WeatherNextProbeApprovalResult(StrictModel):
    """Local-only result of validating the one-object probe sidecar."""

    state: Literal[
        "missing",
        "invalid",
        "manifest_mismatch",
        "object_mismatch",
        "limit_mismatch",
        "expired",
        "consumed",
        "approved",
    ]
    payload_read_permitted: bool = False
    manifest_sha256: str | None = None
    object_uri: str | None = None
    object_compressed_bytes: int = 0
    max_network_bytes: int = 0
    message: str


def _atomic_write(path: Path, payload: Mapping[str, object]) -> Path:
    path = path.expanduser()
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, default=str) + "\n",
        encoding="utf-8",
    )
    temporary.replace(path)
    return path


def _atomic_write_private(path: Path, payload: Mapping[str, object]) -> Path:
    """Atomically write an operator/audit sidecar with owner-only permissions."""

    path = path.expanduser()
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    descriptor = os.open(temporary, flags, 0o600)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False, indent=2, default=str)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        os.chmod(path, 0o600)
    except Exception:
        temporary.unlink(missing_ok=True)
        raise
    return path


def _claim_probe_attempt(
    path: Path,
    *,
    manifest_sha256: str,
    object_uri: str,
    object_compressed_bytes: int,
    max_network_bytes: int,
) -> datetime:
    """Consume the one-shot probe authorization before the payload request."""

    path = path.expanduser()
    path.parent.mkdir(parents=True, exist_ok=True)
    started_at = datetime.now(UTC)
    payload = {
        "schema_version": "weathernext-one-block-probe-attempt/v1",
        "state": "started",
        "started_at_utc": started_at.isoformat(),
        "manifest_sha256": manifest_sha256,
        "object_uri": object_uri,
        "object_compressed_bytes": object_compressed_bytes,
        "max_network_bytes": max_network_bytes,
        "actual_payload_bytes": 0,
    }
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        descriptor = os.open(path, flags, 0o600)
    except FileExistsError as error:
        raise RuntimeError(
            "one-block probe authorization was already consumed; replay is blocked"
        ) from error
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False, indent=2)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
    except Exception:
        path.unlink(missing_ok=True)
        raise
    return started_at


def _finish_probe_attempt(
    path: Path,
    *,
    state: Literal["completed", "failed"],
    actual_payload_bytes: int,
    error_type: str | None = None,
) -> None:
    """Finalize the persistent one-shot receipt without making it reusable."""

    path = path.expanduser()
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        raw = {
            "schema_version": "weathernext-one-block-probe-attempt/v1",
        }
    payload: dict[str, object] = dict(raw) if isinstance(raw, Mapping) else {}
    payload.update(
        {
            "state": state,
            "finished_at_utc": datetime.now(UTC).isoformat(),
            "actual_payload_bytes": actual_payload_bytes,
        }
    )
    if error_type is not None:
        payload["error_type"] = error_type
    _atomic_write_private(path, payload)


def _day_end_utc(observation_date: date, timezone_name: str) -> datetime:
    try:
        timezone = ZoneInfo(timezone_name)
    except ZoneInfoNotFoundError as error:
        raise ValueError(f"unknown target timezone: {timezone_name}") from error
    local_end = datetime.combine(observation_date + timedelta(days=1), time.min, tzinfo=timezone)
    return local_end.astimezone(UTC)


def _table_exists(connection: sqlite3.Connection, table: str) -> bool:
    row = connection.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)
    ).fetchone()
    return row is not None


def _latest_weather_rows(connection: sqlite3.Connection) -> list[sqlite3.Row]:
    """Read only the latest weather payload per event, never mutate SQLite."""

    if not _table_exists(connection, "weather_snapshots"):
        return []
    return list(
        connection.execute(
            """
            SELECT w.event_id, w.fetched_at, w.payload_json
            FROM weather_snapshots AS w
            JOIN (
                SELECT event_id, MAX(id) AS max_id
                FROM weather_snapshots
                GROUP BY event_id
            ) AS latest ON latest.max_id = w.id
            """
        ).fetchall()
    )


def _latest_weather_by_event(connection: sqlite3.Connection) -> dict[str, WeatherForecast]:
    forecasts: dict[str, WeatherForecast] = {}
    for row in _latest_weather_rows(connection):
        try:
            forecast = WeatherForecast.model_validate_json(row["payload_json"])
        except Exception:
            # A malformed historical row is not allowed to become a target.
            continue
        forecasts[str(row["event_id"])] = forecast
    return forecasts


def derive_refresh_targets(
    database_path: Path,
    *,
    now_utc: datetime | None = None,
    output_path: Path = DEFAULT_TARGETS_PATH,
    max_targets: int = 32,
) -> WeatherNextTargetInventory:
    """Derive bounded, eligible targets from read-only local evidence.

    Only production ``weather-evaluation-v1`` rows in Celsius whose station
    day has not ended are considered.  Coordinates come from the latest
    persisted weather snapshot, never from a hand-written target file.  No GCS
    access occurs here.
    """

    if max_targets < 1:
        raise ValueError("max_targets must be positive")
    now = _aware(now_utc or datetime.now(UTC), field="now_utc")
    path = database_path.expanduser().resolve()
    if not path.is_file():
        inventory = WeatherNextTargetInventory(
            generated_at_utc=now,
            targets=[],
            skipped_count=0,
        )
        _atomic_write(output_path, inventory.model_dump(mode="json"))
        return inventory

    connection = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=30)
    connection.row_factory = sqlite3.Row
    try:
        if not _table_exists(connection, "forecast_evaluation_events_v2"):
            rows: Iterable[sqlite3.Row] = ()
        else:
            rows = connection.execute(
                """
                SELECT event_id, station_id, observation_date, station_timezone,
                       rule_day_end_utc, display_unit, eligible, considered_at_utc
                FROM forecast_evaluation_events_v2
                WHERE cohort_version='weather-evaluation-v1'
                  AND eligible=1
                  AND display_unit='C'
                  AND station_id IS NOT NULL
                  AND observation_date IS NOT NULL
                  AND station_timezone IS NOT NULL
                ORDER BY considered_at_utc DESC, id DESC
                """
            ).fetchall()
        forecasts = _latest_weather_by_event(connection)
        selected: dict[tuple[str, date], WeatherNextRefreshTarget] = {}
        skipped = 0
        for row in rows:
            event_id = str(row["event_id"])
            forecast = forecasts.get(event_id)
            if forecast is None:
                skipped += 1
                continue
            try:
                observation_date = date.fromisoformat(str(row["observation_date"]))
                timezone_name = str(row["station_timezone"] or forecast.timezone)
                raw_end = row["rule_day_end_utc"]
                day_end = (
                    _parse_utc(raw_end, field="rule_day_end_utc")
                    if raw_end
                    else _day_end_utc(observation_date, timezone_name)
                )
                if day_end <= now:
                    skipped += 1
                    continue
                fetched = _parse_utc(row["considered_at_utc"], field="considered_at_utc")
                target = WeatherNextRefreshTarget(
                    event_id=event_id,
                    station_id=str(row["station_id"]).strip().upper(),
                    location=forecast.matched_location or forecast.requested_location,
                    latitude=forecast.latitude,
                    longitude=forecast.longitude,
                    observation_date=observation_date,
                    observation_timezone=timezone_name,
                    rule_day_end_utc=day_end,
                    source_fetched_at_utc=fetched,
                )
            except Exception:
                skipped += 1
                continue
            key = (target.station_id, target.observation_date)
            previous = selected.get(key)
            if previous is None or target.source_fetched_at_utc > previous.source_fetched_at_utc:
                selected[key] = target
        ordered = sorted(
            selected.values(),
            key=lambda item: (item.rule_day_end_utc, item.station_id, item.event_id),
        )[:max_targets]
        skipped += max(0, len(selected) - len(ordered))
    finally:
        connection.close()

    inventory = WeatherNextTargetInventory(
        generated_at_utc=now,
        targets=ordered,
        skipped_count=skipped,
    )
    _atomic_write(output_path, inventory.model_dump(mode="json"))
    return inventory


def verify_read_approval(
    manifest_path: Path = DEFAULT_MANIFEST_PATH,
    approval_path: Path = DEFAULT_APPROVAL_PATH,
    *,
    now_utc: datetime | None = None,
) -> WeatherNextApprovalResult:
    """Verify sidecar approval and all manifest-bound read limits.

    This function performs only local file reads.  It never instantiates a GCS
    client and therefore cannot accidentally trigger a payload request.
    """

    manifest_path = manifest_path.expanduser()
    approval_path = approval_path.expanduser()
    try:
        manifest_payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return WeatherNextApprovalResult(
            state="missing",
            message="metadata manifest is not present; payload read remains blocked",
        )
    except Exception as error:
        return WeatherNextApprovalResult(
            state="invalid", message=f"manifest is unreadable: {error}"
        )
    try:
        approval_payload = json.loads(approval_path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        manifest_mapping = cast(Mapping[str, object], manifest_payload)
        digest = str(manifest_mapping.get("manifest_sha256") or "")
        return WeatherNextApprovalResult(
            state="missing", manifest_sha256=digest or None,
            message="operator approval sidecar is absent; payload read remains blocked",
        )
    except Exception as error:
        return WeatherNextApprovalResult(
            state="invalid", message=f"approval sidecar is unreadable: {error}"
        )

    digest = str(manifest_payload.get("manifest_sha256") or "")
    try:
        manifest = WeatherNextFullReadManifest.model_validate(manifest_payload)
        approval = WeatherNextReadApproval.model_validate(approval_payload)
    except Exception as error:
        return WeatherNextApprovalResult(
            state="invalid", manifest_sha256=digest or None,
            message=f"approval validation failed: {error}"
        )

    if not verify_manifest_sha256(manifest):
        return WeatherNextApprovalResult(
            state="invalid", manifest_sha256=manifest.manifest_sha256,
            message="manifest digest does not match its contents; payload read remains blocked",
        )

    expected = int(manifest.approval_gate.expected_network_bytes)
    object_count = int(manifest.approval_gate.object_count)
    coverage_complete, incomplete_target_ids, _mixed_dates = _manifest_coverage_summary(manifest)
    if not coverage_complete or manifest.approval_gate.state == "blocked_incomplete_coverage":
        targets = ", ".join(incomplete_target_ids) or "unknown target"
        return WeatherNextApprovalResult(
            state="coverage_blocked",
            manifest_sha256=manifest.manifest_sha256,
            expected_network_bytes=expected,
            object_count=object_count,
            incomplete_target_ids=incomplete_target_ids,
            message=(
                "incomplete station-local-day coverage for "
                f"{targets}; payload read remains blocked and no hours are synthesized"
            ),
        )
    if approval.manifest_sha256 != manifest.manifest_sha256:
        return WeatherNextApprovalResult(
            state="manifest_mismatch", manifest_sha256=manifest.manifest_sha256,
            expected_network_bytes=expected, object_count=object_count,
            message="approval is bound to a different manifest digest",
        )
    try:
        validate_read_approval(manifest, approval, now_utc=now_utc)
    except Exception as error:
        return WeatherNextApprovalResult(
            state="limit_mismatch", manifest_sha256=manifest.manifest_sha256,
            expected_network_bytes=expected, object_count=object_count,
            message=f"manifest or approval limits are not approval-ready: {error}",
        )
    return WeatherNextApprovalResult(
        state="approved", payload_read_permitted=True,
        manifest_sha256=manifest.manifest_sha256,
        expected_network_bytes=expected, object_count=object_count,
        message="operator approval matches the immutable manifest and bounded limits",
    )


def verify_one_block_probe_approval(
    manifest_path: Path = DEFAULT_MANIFEST_PATH,
    approval_path: Path = DEFAULT_PROBE_APPROVAL_PATH,
    *,
    expected_manifest_sha256: str | None = None,
    expected_object_uri: str | None = None,
    expected_object_compressed_bytes: int | None = None,
    expected_max_network_bytes: int | None = None,
    expected_max_object_bytes: int | None = None,
    attempt_path: Path = DEFAULT_PROBE_ATTEMPT_PATH,
    now_utc: datetime | None = None,
) -> WeatherNextProbeApprovalResult:
    """Verify a probe sidecar without consulting or changing full-read approval.

    This function performs only local file reads.  In particular, no GCS
    client is constructed until the caller receives ``state=approved``.
    """

    manifest_path = manifest_path.expanduser()
    approval_path = approval_path.expanduser()
    try:
        manifest_payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return WeatherNextProbeApprovalResult(
            state="missing",
            message="metadata manifest is not present; one-block probe remains blocked",
        )
    except Exception as error:
        return WeatherNextProbeApprovalResult(
            state="invalid", message=f"manifest is unreadable: {error}"
        )
    if not isinstance(manifest_payload, Mapping):
        return WeatherNextProbeApprovalResult(
            state="invalid", message="manifest root is not an object"
        )
    manifest_digest = str(manifest_payload.get("manifest_sha256") or "")
    if expected_manifest_sha256 and not hmac.compare_digest(
        manifest_digest, expected_manifest_sha256
    ):
        return WeatherNextProbeApprovalResult(
            state="manifest_mismatch",
            manifest_sha256=manifest_digest or None,
            message="current manifest does not match the requested probe digest",
        )
    if attempt_path.expanduser().exists():
        return WeatherNextProbeApprovalResult(
            state="consumed",
            manifest_sha256=manifest_digest or None,
            message="one-block probe authorization was already consumed; replay is blocked",
        )
    try:
        approval_payload = json.loads(approval_path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return WeatherNextProbeApprovalResult(
            state="missing",
            manifest_sha256=manifest_digest or None,
            message="one-block probe approval sidecar is absent; probe remains blocked",
        )
    except Exception as error:
        return WeatherNextProbeApprovalResult(
            state="invalid",
            manifest_sha256=manifest_digest or None,
            message=f"probe approval is unreadable: {error}",
        )
    try:
        manifest = WeatherNextFullReadManifest.model_validate(manifest_payload)
        approval = WeatherNextOneBlockProbeApproval.model_validate(approval_payload)
        validate_one_block_probe_approval(
            manifest,
            approval,
            expected_manifest_sha256=expected_manifest_sha256,
            expected_object_uri=expected_object_uri,
            expected_object_compressed_bytes=expected_object_compressed_bytes,
            expected_max_network_bytes=expected_max_network_bytes,
            expected_max_object_bytes=expected_max_object_bytes,
            now_utc=now_utc,
        )
    except Exception as error:
        message = str(error)
        lowered = message.casefold()
        if "expired" in lowered:
            state: Literal[
                "missing",
                "invalid",
                "manifest_mismatch",
                "object_mismatch",
                "limit_mismatch",
                "expired",
                "consumed",
                "approved",
            ] = "expired"
        elif "object uri" in lowered or "compressed size" in lowered:
            state = "object_mismatch"
        elif "manifest" in lowered or "digest" in lowered:
            state = "manifest_mismatch"
        else:
            state = "limit_mismatch"
        raw_object_uri = (
            approval_payload.get("object_uri")
            if isinstance(approval_payload, Mapping)
            else None
        )
        raw_object_bytes = (
            approval_payload.get("object_compressed_bytes")
            if isinstance(approval_payload, Mapping)
            else None
        )
        raw_max_network = (
            approval_payload.get("max_network_bytes")
            if isinstance(approval_payload, Mapping)
            else None
        )
        return WeatherNextProbeApprovalResult(
            state=state,
            manifest_sha256=manifest_digest or None,
            object_uri=str(raw_object_uri) if raw_object_uri is not None else None,
            object_compressed_bytes=(
                int(raw_object_bytes)
                if isinstance(raw_object_bytes, (int, float, str))
                and str(raw_object_bytes).isdigit()
                else 0
            ),
            max_network_bytes=(
                int(raw_max_network)
                if isinstance(raw_max_network, (int, float, str)) and str(raw_max_network).isdigit()
                else 0
            ),
            message=f"one-block probe approval is not valid: {message}",
        )
    return WeatherNextProbeApprovalResult(
        state="approved",
        payload_read_permitted=True,
        manifest_sha256=manifest.manifest_sha256,
        object_uri=approval.object_uri,
        object_compressed_bytes=approval.object_compressed_bytes,
        max_network_bytes=approval.max_network_bytes,
        message="one-block probe approval matches the immutable manifest object and limits",
    )


def _load_approved_manifest(
    manifest_path: Path, approval_path: Path
) -> tuple[WeatherNextFullReadManifest, WeatherNextReadApproval, WeatherNextApprovalResult]:
    """Load the typed manifest only after the local approval check succeeds."""

    result = verify_read_approval(manifest_path, approval_path)
    if result.state != "approved":
        raise RuntimeError(result.message)
    payload = json.loads(manifest_path.expanduser().read_text(encoding="utf-8"))
    approval_payload = json.loads(approval_path.expanduser().read_text(encoding="utf-8"))
    manifest = WeatherNextFullReadManifest.model_validate(payload)
    approval = WeatherNextReadApproval.model_validate(approval_payload)
    return manifest, approval, result


def _load_probe_approved_manifest(
    manifest_path: Path,
    approval_path: Path,
    *,
    expected_manifest_sha256: str,
    expected_object_uri: str,
    expected_object_compressed_bytes: int,
    expected_max_network_bytes: int,
    expected_max_object_bytes: int,
    attempt_path: Path,
) -> tuple[
    WeatherNextFullReadManifest,
    WeatherNextOneBlockProbeApproval,
    WeatherNextProbeApprovalResult,
]:
    """Load the exact object only after the narrow local probe gate succeeds."""

    result = verify_one_block_probe_approval(
        manifest_path,
        approval_path,
        expected_manifest_sha256=expected_manifest_sha256,
        expected_object_uri=expected_object_uri,
        expected_object_compressed_bytes=expected_object_compressed_bytes,
        expected_max_network_bytes=expected_max_network_bytes,
        expected_max_object_bytes=expected_max_object_bytes,
        attempt_path=attempt_path,
    )
    if result.state != "approved":
        raise RuntimeError(result.message)
    manifest_payload = json.loads(manifest_path.expanduser().read_text(encoding="utf-8"))
    approval_payload = json.loads(approval_path.expanduser().read_text(encoding="utf-8"))
    manifest = WeatherNextFullReadManifest.model_validate(manifest_payload)
    approval = WeatherNextOneBlockProbeApproval.model_validate(approval_payload)
    return manifest, approval, result


def _manifest_coverage_summary(
    manifest: WeatherNextFullReadManifest,
) -> tuple[bool, list[str], bool]:
    """Re-check every target's exact station-local-day coverage locally."""

    incomplete: list[str] = []
    observation_dates: set[str] = set()
    for raw_target in manifest.targets:
        target_id = str(raw_target.get("target_id", "?"))
        observation_dates.add(str(raw_target.get("observation_date", "")))
        raw_times = raw_target.get("valid_times_utc", [])
        if not isinstance(raw_times, list):
            incomplete.append(target_id)
            continue
        try:
            observation_date = date.fromisoformat(str(raw_target.get("observation_date", "")))
            complete, _reason = assess_station_local_day_coverage(
                observation_date,
                str(raw_target.get("observation_timezone", "UTC")),
                raw_times,
            )
        except (TypeError, ValueError):
            complete = False
        if raw_target.get("complete_station_local_day") is not True or not complete:
            incomplete.append(target_id)
    return not incomplete, sorted(set(incomplete)), len(observation_dates) > 1


def _find_complete_release_manifest(
    client: object,
    *,
    targets: Sequence[Mapping[str, object]],
    initial_manifest: WeatherNextFullReadManifest,
    max_network_bytes: int,
    max_objects: int,
    max_object_bytes: int,
    snapshot_root: Path,
) -> WeatherNextFullReadManifest | None:
    """Find the newest prior release covering every target's full station day.

    A newly published run can be the newest metadata-visible object while its
    lead window no longer contains the beginning of an in-progress station day.
    Probe older releases using coordinates/chunk metadata only, then perform
    exact compressed-object HEADs once for the first complete candidate.  No
    payload body is touched by this search.
    """

    resolver = getattr(client, "_resolve_store_prefix", None)
    estimator = getattr(client, "estimate_point_day_read", None)
    if not callable(resolver) or not callable(estimator) or not targets:
        return None
    resolver = cast(Callable[..., tuple[str, datetime | None]], resolver)
    from polybot.weathernext_manifest import estimate_and_build_full_ensemble_read_manifest_batch

    first_target = targets[0]
    try:
        first_date = date.fromisoformat(str(first_target["observation_date"]))
        first_timezone = str(first_target["timezone"])
    except (KeyError, TypeError, ValueError):
        return None

    # Use the initial metadata manifest to jump directly to the newest run that
    # can span all requested station days.  The array has 48 hourly time slots
    # in the current WeatherNext release (8 lead × 6 sub-time); the exact
    # candidate is still verified by the normal coverage gate below.
    valid_offsets: list[timedelta] = []
    for target in initial_manifest.targets:
        raw_times = target.get("valid_times_utc", [])
        if not isinstance(raw_times, list):
            continue
        for raw_time in raw_times:
            try:
                valid_offsets.append(
                    _parse_utc(raw_time, field="valid_time_utc") - initial_manifest.init_time_utc
                )
            except ValueError:
                continue
    raw_shape = initial_manifest.array.get("shape", [])
    raw_dimensions = initial_manifest.array.get("dimensions", [])
    time_slots = 48
    if isinstance(raw_shape, list) and isinstance(raw_dimensions, list):
        try:
            time_slots = 1
            for dim, size in zip(raw_dimensions, raw_shape, strict=False):
                if str(dim) in {"lead_time", "lead_subtime"}:
                    time_slots *= max(1, int(size))
        except (TypeError, ValueError):
            time_slots = 48
    min_offset = min(valid_offsets, default=timedelta(hours=1))
    max_offset = min_offset + timedelta(hours=max(0, time_slots - 1))
    starts: list[datetime] = []
    ends: list[datetime] = []
    for target in targets:
        try:
            target_date = date.fromisoformat(str(target["observation_date"]))
            expected = expected_station_local_day_hours(
                target_date,
                str(target["timezone"]),
            )
        except (KeyError, TypeError, ValueError):
            return None
        starts.append(expected[0])
        ends.append(expected[-1] + timedelta(hours=1))
    latest_allowed_init = min(starts) - min_offset
    earliest_allowed_init = max(ends) - max_offset
    if earliest_allowed_init > latest_allowed_init:
        return None
    try:
        _prefix, candidate_init = resolver(
            init_time_utc=latest_allowed_init - timedelta(microseconds=1),
            observation_date=first_date,
            timezone_name=first_timezone,
        )
    except Exception:
        return None
    if candidate_init is None or candidate_init < earliest_allowed_init:
        return None

    # Verify the direct candidate with exact object HEADs first.  If the
    # publication is incomplete, walk only a small bounded number of prior
    # releases using metadata-only probes; never loop over the whole archive.
    for _attempt in range(6):
        try:
            candidate = estimate_and_build_full_ensemble_read_manifest_batch(
                client,  # type: ignore[arg-type]
                targets=targets,
                init_time_utc=candidate_init,
                include_chunk_sizes=True,
                max_network_bytes=max_network_bytes,
                max_objects=max_objects,
                max_object_bytes=max_object_bytes,
                snapshot_root=snapshot_root,
            )
            complete, _incomplete, _mixed = _manifest_coverage_summary(candidate)
            if complete:
                return candidate
        except Exception:
            pass
        try:
            _prefix, previous_init = resolver(
                init_time_utc=candidate_init - timedelta(microseconds=1),
                observation_date=first_date,
                timezone_name=first_timezone,
            )
        except Exception:
            return None
        if previous_init is None or previous_init >= candidate_init:
            return None
        candidate_init = previous_init
    return None


def _source_prefix(source_uri: str, bucket: str) -> str:
    prefix = f"gs://{bucket}/"
    if not source_uri.startswith(prefix):
        raise ValueError("WeatherNext manifest source is not in the configured bucket")
    return source_uri.removeprefix(prefix).rstrip("/")


def _object_key(object_uri: str, bucket: str) -> str:
    prefix = f"gs://{bucket}/"
    if not object_uri.startswith(prefix):
        raise ValueError("WeatherNext manifest object is not in the configured bucket")
    return object_uri.removeprefix(prefix)


def _manifest_target_selection(
    target: Mapping[str, object],
) -> tuple[list[str], list[dict[str, object]]]:
    raw_selection = target.get("selection")
    selection = (
        cast(Mapping[str, object], raw_selection)
        if isinstance(raw_selection, Mapping)
        else {}
    )
    raw_dims = selection.get("dimensions", [])
    dimensions = [str(item) for item in raw_dims] if isinstance(raw_dims, list) else []
    raw_tuples = selection.get("valid_index_tuples", [])
    records: list[dict[str, object]] = []
    if isinstance(raw_tuples, list):
        for item in raw_tuples:
            if isinstance(item, Mapping):
                records.append({str(key): value for key, value in item.items()})
    if not dimensions or not records:
        raise ValueError(
            f"WeatherNext target {target.get('target_id', '?')} has no valid index mapping"
        )
    return dimensions, records


def _target_member_accumulators(
    target: Mapping[str, object],
) -> tuple[list[str], list[str], dict[str, dict[str, float]]]:
    raw_times = target.get("valid_times_utc")
    if not isinstance(raw_times, list) or not raw_times:
        raise ValueError(f"WeatherNext target {target.get('target_id', '?')} has no valid times")
    valid_times = [
        _parse_utc(value, field="target.valid_times_utc").isoformat()
        for value in raw_times
    ]
    # A full station-local day is required for a usable paper snapshot.  A
    # short publication remains a metadata artifact and is never promoted to
    # the strategy as if missing hours had been filled.  Check the exact UTC
    # hour set here as well as the manifest flag so a hand-edited/tampered
    # manifest cannot bypass the coverage contract.
    try:
        observation_date = date.fromisoformat(str(target.get("observation_date", "")))
        timezone_name = str(target.get("observation_timezone", "UTC"))
        complete, reason = assess_station_local_day_coverage(
            observation_date,
            timezone_name,
            valid_times,
        )
    except (TypeError, ValueError) as error:
        raise ValueError(
            "WeatherNext target "
            f"{target.get('target_id', '?')} has invalid station-day coverage: {error}"
        ) from error
    if target.get("complete_station_local_day") is not True or not complete:
        raise ValueError(
            "WeatherNext target "
            f"{target.get('target_id', '?')} does not cover a complete station day: {reason}"
        )
    # Keep stable, zero-padded participant identities across snapshots and
    # ForecastStore records.  These are source member positions, not
    # fabricated probabilities or synthetic scenarios.
    member_ids = [f"member-{index:03d}" for index in range(64)]
    values = {member_id: {} for member_id in member_ids}
    return member_ids, valid_times, values


def _extract_probe_values(
    *,
    chunk: object,
    item: WeatherNextCompressedObject,
    target_map: Mapping[str, Mapping[str, object]],
    dimensions: Sequence[str],
    array_shape: Sequence[int],
    chunk_shape: Sequence[int],
    source_units: str,
) -> list[dict[str, object]]:
    """Extract only real station/member values that fall inside one probe block."""

    import numpy as np

    values = np.asarray(chunk)
    chunk_coordinates = list(item.chunk_coordinates)
    chunk_starts = [
        int(coordinate) * int(chunk_shape[position])
        for position, coordinate in enumerate(chunk_coordinates)
    ]
    target_ids = [str(value) for value in item.target_ids]
    output: list[dict[str, object]] = []
    for target_id in target_ids:
        target = target_map.get(target_id)
        if target is None:
            raise RuntimeError(f"Manifest object references unknown target {target_id}")
        target_dimensions, valid_records = _manifest_target_selection(target)
        if list(target_dimensions) != list(dimensions):
            raise RuntimeError(f"Target {target_id} dimensions do not match manifest array")
        raw_selection = target.get("selection")
        selection = (
            cast(Mapping[str, object], raw_selection)
            if isinstance(raw_selection, Mapping)
            else {}
        )
        lat_index = int(cast(int | str, selection["latitude_index"]))
        lon_index = int(cast(int | str, selection["longitude_index"]))
        for record in valid_records:
            global_indices: dict[str, int] = {
                "sample": 0,
                "lead_time": int(cast(int | str, record["lead_time_index"])),
                "lat_0p05": lat_index,
                "latitude": lat_index,
                "lat": lat_index,
                "lon_0p05": lon_index,
                "longitude": lon_index,
                "lon": lon_index,
            }
            if "lead_subtime_index" in record:
                global_indices["lead_subtime"] = int(
                    cast(int | str, record["lead_subtime_index"])
                )
            member_values: list[dict[str, object]] = []
            for member_index in range(64):
                global_indices["sample"] = member_index
                local_indices: list[int] = []
                for position, dimension in enumerate(dimensions):
                    if dimension not in global_indices:
                        if int(array_shape[position]) != 1:
                            raise RuntimeError(
                                f"Target {target_id} leaves dimension {dimension} unspecified"
                            )
                        global_index = 0
                    else:
                        global_index = global_indices[dimension]
                    local_index = global_index - chunk_starts[position]
                    if local_index < 0 or local_index >= values.shape[position]:
                        break
                    local_indices.append(local_index)
                else:
                    raw_value = float(values[tuple(local_indices)])
                    if not math.isfinite(raw_value):
                        raise RuntimeError(f"Target {target_id} has a missing probe value")
                    member_values.append(
                        {
                            "member_id": f"member-{member_index:03d}",
                            "value": (
                                raw_value - 273.15
                                if source_units.casefold() in {"k", "kelvin"}
                                else raw_value
                            ),
                        }
                    )
            if member_values:
                output.append(
                    {
                        "target_id": target_id,
                        "valid_time_utc": _parse_utc(
                            record.get("valid_time_utc", ""), field="valid_time_utc"
                        ).isoformat(),
                        "units": (
                            "C"
                            if source_units.casefold() in {"k", "kelvin"}
                            else source_units
                        ),
                        "members": member_values,
                    }
                )
    return output


def _reusable_snapshot_paths(
    manifest: WeatherNextFullReadManifest,
    *,
    snapshot_root: Path,
) -> list[str] | None:
    """Return already verified immutable snapshots for this exact manifest.

    Reuse is deliberately conservative: every target must have an immutable
    file whose release/init/date, 64 member identities, and complete hourly
    trajectory coverage match the manifest.  If any target is missing or
    mismatched, the caller performs the approved sequential pass instead.
    """

    from polybot.weathernext import WeatherNextSnapshot

    root = snapshot_root.expanduser().resolve()
    paths: list[str] = []
    for target in manifest.targets:
        target_id = str(target.get("target_id", ""))
        raw_times = target.get("valid_times_utc", [])
        try:
            observation_date = date.fromisoformat(str(target.get("observation_date", "")))
            coverage_complete, _reason = assess_station_local_day_coverage(
                observation_date,
                str(target.get("observation_timezone", "UTC")),
                raw_times if isinstance(raw_times, list) else [],
            )
        except (TypeError, ValueError):
            return None
        if target.get("complete_station_local_day") is not True or not coverage_complete:
            return None
        raw_path = target.get("snapshot_path")
        if not isinstance(raw_path, str) or not raw_path:
            return None
        path = Path(raw_path).expanduser().resolve()
        try:
            path.relative_to(root)
        except ValueError:
            return None
        try:
            snapshot = WeatherNextSnapshot.model_validate_json(path.read_text(encoding="utf-8"))
        except Exception:
            return None
        raw_expected_times = target.get("valid_times_utc", [])
        expected_times = (
            [
                _parse_utc(value, field="target.valid_times_utc").isoformat()
                for value in raw_expected_times
            ]
            if isinstance(raw_expected_times, list)
            else []
        )
        actual_times = [value.isoformat() for value in snapshot.valid_times_utc]
        if (
            snapshot.release_id != manifest.release_id
            or snapshot.init_time_utc != manifest.init_time_utc
            or snapshot.station_id != target_id
            or snapshot.observation_date.isoformat() != str(target.get("observation_date"))
            or snapshot.member_ids != [f"member-{index:03d}" for index in range(64)]
            or actual_times != expected_times
            or len(snapshot.trajectories) != 64
            or any(
                not isinstance(item.get("values_c"), list)
                or len(cast(list[object], item["values_c"])) != len(expected_times)
                for item in snapshot.trajectories
            )
        ):
            return None
        paths.append(str(path))
    return paths


def _write_immutable_snapshot(path: Path, snapshot: Mapping[str, object]) -> None:
    path = path.expanduser()
    payload = json.dumps(snapshot, ensure_ascii=False, indent=2, default=str) + "\n"
    if path.exists():
        existing = path.read_text(encoding="utf-8")
        if existing != payload:
            raise RuntimeError(
                "WeatherNext immutable snapshot already exists with different content: "
                f"{path}"
            )
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_text(payload, encoding="utf-8")
    try:
        # Avoid replacing a concurrently published immutable artifact.
        path.open("x").close()
    except FileExistsError:
        temporary.unlink(missing_ok=True)
        if path.read_text(encoding="utf-8") != payload:
            raise RuntimeError(
                "WeatherNext immutable snapshot changed concurrently: " f"{path}"
            ) from None
        return
    try:
        temporary.replace(path)
    except Exception:
        path.unlink(missing_ok=True)
        raise


def _update_snapshot_index(
    index_path: Path,
    entries: list[dict[str, object]],
) -> None:
    existing: list[dict[str, object]] = []
    if index_path.expanduser().is_file():
        try:
            payload = json.loads(index_path.expanduser().read_text(encoding="utf-8"))
            raw_entries = payload.get("entries", []) if isinstance(payload, Mapping) else []
            if isinstance(raw_entries, list):
                existing = [
                    {str(key): value for key, value in item.items()}
                    for item in raw_entries
                    if isinstance(item, Mapping)
                ]
        except Exception:
            # A corrupt index must not make immutable snapshots disappear; the
            # new index is rebuilt only from successfully verified artifacts.
            existing = []
    by_key: dict[tuple[str, str, str, str], dict[str, object]] = {}
    for item in existing + entries:
        key = (
            str(item.get("event_id", "")),
            str(item.get("station_id", item.get("target_id", ""))),
            str(item.get("observation_date", "")),
            str(item.get("init_time_utc", "")),
        )
        by_key[key] = item
    ordered = sorted(
        by_key.values(),
        key=lambda item: (
            str(item.get("observation_date", "")),
            str(item.get("station_id", item.get("target_id", ""))),
            str(item.get("init_time_utc", "")),
        ),
        reverse=True,
    )
    _atomic_write(
        index_path,
        {
            "schema_version": "weathernext-full-snapshot-index/v1",
            "generated_at_utc": datetime.now(UTC).isoformat(),
            "entries": ordered,
        },
    )


def _download_probe_object_once(
    client: object,
    *,
    item: WeatherNextCompressedObject,
    approval: WeatherNextOneBlockProbeApproval,
    temporary_root: Path,
    attempt_path: Path,
) -> tuple[Path, int, float]:
    """Download the exact approved object once, with retries disabled.

    The returned file must be deleted by the caller.  There is intentionally
    no loop or retry callback in this function: a failed/partial attempt ends
    the probe and requires a new explicit operator decision.
    """

    if item.object_uri != approval.object_uri:
        raise RuntimeError("probe reader selected an object not named in the approval")
    if item.compressed_bytes is None:
        raise RuntimeError("probe object has no compressed size")
    if item.compressed_bytes != approval.object_compressed_bytes:
        raise RuntimeError("probe object size differs from the approved manifest size")
    if item.compressed_bytes > approval.max_network_bytes:
        raise RuntimeError("probe object exceeds the cumulative payload budget")
    if approval.max_objects != 1 or approval.max_attempts != 1:
        raise RuntimeError("probe must remain limited to one object and one attempt")

    root = temporary_root.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True, mode=0o700)
    os.chmod(root, 0o700)
    free_bytes = shutil.disk_usage(root).free
    if free_bytes < item.compressed_bytes:
        raise RuntimeError(
            "insufficient temporary disk space for the approved WeatherNext object"
        )
    available_memory = _available_memory_bytes()
    uncompressed_bound = int(item.uncompressed_upper_bound_bytes or 0)
    conservative_memory_need = (
        item.compressed_bytes * 2 + uncompressed_bound + 256 * 1024 * 1024
    )
    if available_memory is not None and available_memory < conservative_memory_need:
        raise RuntimeError(
            "insufficient available memory for single-shot download and bounded decode"
        )

    bucket = getattr(client, "_bucket", None)
    bucket_name = str(getattr(client, "bucket_name", ""))
    if bucket is None or not bucket_name:
        raise RuntimeError("WeatherNext client does not expose its requester-pays bucket")
    blob = bucket.blob(_object_key(item.object_uri, bucket_name))
    blob.reload(retry=None)
    actual_size = getattr(blob, "size", None)
    if actual_size is None or int(actual_size) != item.compressed_bytes:
        raise RuntimeError(f"WeatherNext object size changed: {item.object_uri}")
    for field in ("generation", "etag", "md5_hash", "crc32c"):
        expected = getattr(item, field, None)
        actual = getattr(blob, field, None)
        if expected is not None and (actual is None or str(expected) != str(actual)):
            raise RuntimeError(f"WeatherNext object metadata changed: {item.object_uri}")

    temporary_descriptor, temporary_name = tempfile.mkstemp(
        prefix="weathernext-probe-",
        suffix=".zarr-chunk",
        dir=root,
    )
    temporary = os.fdopen(temporary_descriptor, "w+b")
    temporary_path = Path(temporary_name)
    os.chmod(temporary_path, 0o600)
    claimed = False
    try:
        _claim_probe_attempt(
            attempt_path,
            manifest_sha256=approval.manifest_sha256,
            object_uri=approval.object_uri,
            object_compressed_bytes=approval.object_compressed_bytes,
            max_network_bytes=approval.max_network_bytes,
        )
        claimed = True
        started = monotonic_clock.monotonic()
        generation = getattr(blob, "generation", None)
        download_kwargs: dict[str, object] = {
            "raw_download": True,
            "retry": None,
            "single_shot_download": True,
            "checksum": "auto",
        }
        if generation is not None:
            try:
                download_kwargs["if_generation_match"] = int(generation)
            except (TypeError, ValueError):
                raise RuntimeError("WeatherNext object generation is not an integer") from None
        blob.download_to_file(temporary, **download_kwargs)
        temporary.flush()
        os.fsync(temporary.fileno())
        temporary.close()
        elapsed = monotonic_clock.monotonic() - started
        downloaded_bytes = temporary_path.stat().st_size
        if downloaded_bytes != item.compressed_bytes:
            raise RuntimeError(
                "one-block probe downloaded a different byte count than the manifest"
            )
        if downloaded_bytes > approval.max_network_bytes:
            raise RuntimeError("one-block probe exceeded the cumulative payload limit")
        return temporary_path, downloaded_bytes, elapsed
    except Exception as error:
        if not temporary.closed:
            temporary.close()
        actual_bytes = temporary_path.stat().st_size if temporary_path.exists() else 0
        temporary_path.unlink(missing_ok=True)
        if claimed:
            with suppress(Exception):
                _finish_probe_attempt(
                    attempt_path,
                    state="failed",
                    actual_payload_bytes=actual_bytes,
                    error_type=type(error).__name__,
                )
        raise
    finally:
        if not temporary.closed:
            temporary.close()


def read_approved_manifest_sequentially(
    settings: object,
    *,
    manifest_path: Path = DEFAULT_MANIFEST_PATH,
    approval_path: Path = DEFAULT_APPROVAL_PATH,
    index_path: Path = DEFAULT_ROOT / "latest-index.json",
    snapshot_root: Path = DEFAULT_ROOT / "snapshots",
    probe_only: bool = False,
    probe_approval_path: Path = DEFAULT_PROBE_APPROVAL_PATH,
    probe_attempt_path: Path = DEFAULT_PROBE_ATTEMPT_PATH,
    expected_probe_manifest_sha256: str | None = None,
    expected_probe_object_uri: str | None = None,
    expected_probe_object_compressed_bytes: int | None = None,
    expected_probe_max_network_bytes: int | None = None,
    expected_probe_max_object_bytes: int | None = None,
    probe_temporary_root: Path | None = None,
) -> WeatherNextSequentialReadResult:
    """Read an approved manifest one compressed object at a time.

    The function intentionally has no code path that calls the existing
    ``read_point_day_ensemble`` bulk reader.  It opens the Zarr group without
    xarray/dask, requests an exact chunk region, extracts only the target
    point/hour values into tiny per-target accumulators, then releases the
    chunk before requesting the next object.  Any missing object, changed
    metadata, limit mismatch, unsupported sharding, incomplete member/hour
    coverage, or unexpected dimension stops the pass before publishing a
    snapshot.
    """

    from polybot.config import Settings
    from polybot.weathernext import WeatherNextGcsClient, WeatherNextSnapshot

    if not isinstance(settings, Settings):
        raise TypeError("settings must be a polybot.config.Settings instance")
    probe_approval: WeatherNextOneBlockProbeApproval | None = None
    if probe_only:
        required_probe_values = {
            "manifest SHA-256": expected_probe_manifest_sha256,
            "object URI": expected_probe_object_uri,
            "object compressed bytes": expected_probe_object_compressed_bytes,
            "cumulative payload limit": expected_probe_max_network_bytes,
            "object-size limit": expected_probe_max_object_bytes,
        }
        missing_probe_values = [
            name for name, value in required_probe_values.items() if value is None
        ]
        if missing_probe_values:
            raise RuntimeError(
                "one-block probe requires explicit expected "
                + ", ".join(missing_probe_values)
            )
        manifest, probe_approval, probe_approval_result = _load_probe_approved_manifest(
            manifest_path,
            probe_approval_path,
            expected_manifest_sha256=cast(str, expected_probe_manifest_sha256),
            expected_object_uri=cast(str, expected_probe_object_uri),
            expected_object_compressed_bytes=cast(
                int, expected_probe_object_compressed_bytes
            ),
            expected_max_network_bytes=cast(int, expected_probe_max_network_bytes),
            expected_max_object_bytes=cast(int, expected_probe_max_object_bytes),
            attempt_path=probe_attempt_path,
        )
        approval: WeatherNextReadApproval | WeatherNextOneBlockProbeApproval = probe_approval
        approval_result: WeatherNextApprovalResult | WeatherNextProbeApprovalResult = (
            probe_approval_result
        )
    else:
        manifest, full_approval, full_approval_result = _load_approved_manifest(
            manifest_path, approval_path
        )
        approval = full_approval
        approval_result = full_approval_result
        coverage_complete, incomplete_target_ids, _mixed_dates = _manifest_coverage_summary(
            manifest
        )
        if not coverage_complete:
            targets = ", ".join(incomplete_target_ids) or "unknown target"
            raise RuntimeError(
                "WeatherNext payload read blocked: incomplete station-local-day coverage for "
                f"{targets}; no hours are synthesized"
            )
    if manifest.payload_read or manifest.approval_gate.payload_read_permitted:
        raise RuntimeError("manifest must remain immutable and payload_read=false")
    if manifest.approval_gate.sharding_supported is not True:
        raise RuntimeError("sharded WeatherNext stores are not supported by the sequential reader")

    # A successful pass is immutable and reusable.  Do this local validation
    # before opening the GCS group so an hourly timer never re-downloads an
    # unchanged release merely because the approval sidecar is still valid.
    if not probe_only and index_path.expanduser().is_file():
        reused = _reusable_snapshot_paths(manifest, snapshot_root=snapshot_root)
        if reused is not None:
            return WeatherNextSequentialReadResult(
                payload_read=False,
                manifest_sha256=manifest.manifest_sha256,
                snapshots_written=0,
                snapshot_paths=reused,
                bytes_read=0,
                object_count=0,
                message=(
                    "reused immutable WeatherNext trajectory snapshots for the approved release; "
                    "no payload read was necessary"
                ),
            )

    import numpy as np

    started = monotonic_clock.monotonic()
    client = WeatherNextGcsClient(settings)
    store_prefix = _source_prefix(manifest.source_uri, client.bucket_name)
    probe_item: WeatherNextCompressedObject | None = None
    probe_path: Path | None = None
    probe_bytes = 0
    probe_download_seconds: float | None = None
    group: Any | None = None
    try:
        if probe_only:
            if probe_approval is None:
                raise RuntimeError("one-block probe approval was not loaded")
            matches = [
                item
                for item in manifest.compressed_objects
                if item.object_uri == probe_approval.object_uri
            ]
            if len(matches) != 1:
                raise RuntimeError("probe approval does not name exactly one manifest object")
            probe_item = matches[0]
            temporary_root = (
                probe_temporary_root
                if probe_temporary_root is not None
                else manifest_path.expanduser().parent / "probe-tmp"
            )
            probe_path, probe_bytes, probe_download_seconds = _download_probe_object_once(
                client,
                item=probe_item,
                approval=probe_approval,
                temporary_root=temporary_root,
                attempt_path=probe_attempt_path,
            )
            group = client.open_sequential_zarr_group_from_local_object(
                store_prefix,
                object_key=_object_key(probe_item.object_uri, client.bucket_name),
                object_path=probe_path,
                array_key=manifest.variable,
            )
        else:
            group = client.open_sequential_zarr_group(store_prefix)
        if group is None:
            raise RuntimeError("WeatherNext Zarr group did not open")
        array = group[manifest.variable]
        raw_dimensions = manifest.array.get("dimensions", [])
        dimensions = (
            [str(item) for item in cast(list[object], raw_dimensions)]
            if isinstance(raw_dimensions, list)
            else []
        )
        if not dimensions or len(dimensions) != len(array.shape):
            raise RuntimeError("WeatherNext manifest dimensions do not match the Zarr array")
        chunk_shape = tuple(int(value) for value in array.chunks)
        array_shape = tuple(int(value) for value in array.shape)
        raw_manifest_shape = manifest.array.get("shape", [])
        raw_manifest_chunks = manifest.array.get("chunk_shape", [])
        if not isinstance(raw_manifest_shape, list) or not isinstance(raw_manifest_chunks, list):
            raise RuntimeError("WeatherNext manifest is missing array shape/chunk metadata")
        manifest_shape = tuple(int(value) for value in raw_manifest_shape)
        manifest_chunks = tuple(int(value) for value in raw_manifest_chunks)
        if manifest_shape != array_shape or manifest_chunks != chunk_shape:
            raise RuntimeError(
                "WeatherNext array shape/chunk metadata changed since the manifest was built"
            )
        source_units = str(getattr(array, "attrs", {}).get("units", manifest.units)).strip()
        if source_units.casefold() not in {manifest.units.casefold(), "k", "kelvin", "c", "degc"}:
            raise RuntimeError(f"WeatherNext array units changed unexpectedly: {source_units!r}")
        target_map = {
            str(item["target_id"]): cast(Mapping[str, object], item)
            for item in manifest.targets
            if item.get("target_id") is not None
        }
        if not target_map:
            raise RuntimeError("WeatherNext manifest has no target payloads")
        accumulators: dict[str, tuple[list[str], list[str], dict[str, dict[str, float]]]] = {}
        if not probe_only:
            accumulators = {
                target_id: _target_member_accumulators(target)
                for target_id, target in target_map.items()
            }
        # The probe object was already downloaded exactly once to the local
        # file.  Start the loop counter at zero so its manifest size is
        # checked once rather than being double-counted against the budget.
        bytes_read = 0
        object_count = 0
        objects_to_read = (
            [probe_item]
            if probe_only and probe_item is not None
            else sorted(manifest.compressed_objects, key=lambda value: value.sequence)
        )
        for item in objects_to_read:
            if object_count >= approval.max_objects:
                raise RuntimeError("WeatherNext read stopped before approved object limit")
            if item.compressed_bytes is None:
                raise RuntimeError(f"WeatherNext object has no compressed size: {item.object_uri}")
            if item.compressed_bytes > approval.max_object_bytes:
                raise RuntimeError(
                    "WeatherNext object exceeds approved size limit: " f"{item.object_uri}"
                )
            if bytes_read + item.compressed_bytes > approval.max_network_bytes:
                raise RuntimeError("WeatherNext read stopped before approved network limit")
            if not probe_only:
                blob = client._bucket.blob(  # type: ignore[attr-defined]
                    _object_key(item.object_uri, client.bucket_name)
                )
                blob.reload()
                actual_size = getattr(blob, "size", None)
                if actual_size is None or int(actual_size) != item.compressed_bytes:
                    raise RuntimeError(f"WeatherNext object size changed: {item.object_uri}")
                for field in ("generation", "etag", "md5_hash", "crc32c"):
                    expected = getattr(item, field, None)
                    actual = getattr(blob, field, None)
                    if expected is not None and (actual is None or str(expected) != str(actual)):
                        raise RuntimeError(
                            f"WeatherNext object metadata changed: {item.object_uri}"
                        )

            # Exact chunk boundaries make this one payload GET.  No global
            # array selection is used, and ``chunk`` is released each loop.
            selection = tuple(
                slice(
                    coordinate * chunk_shape[position],
                    min((coordinate + 1) * chunk_shape[position], array_shape[position]),
                )
                for position, coordinate in enumerate(item.chunk_coordinates)
            )
            decode_started = monotonic_clock.monotonic()
            chunk = np.asarray(array.get_basic_selection(selection))
            decode_elapsed = monotonic_clock.monotonic() - decode_started
            expected_chunk_shape = tuple(value.stop - value.start for value in selection)
            if tuple(int(value) for value in chunk.shape) != expected_chunk_shape:
                raise RuntimeError(f"Unexpected decoded WeatherNext chunk shape: {chunk.shape}")
            if (
                item.uncompressed_upper_bound_bytes
                and int(chunk.nbytes) > item.uncompressed_upper_bound_bytes
            ):
                raise RuntimeError(
                    "Decoded WeatherNext chunk exceeded its manifest bound: "
                    f"{item.object_uri}"
                )
            if probe_only:
                decoded_shape = [int(value) for value in chunk.shape]
                decoded_bytes = int(chunk.nbytes)
                extracted_values = _extract_probe_values(
                    chunk=chunk,
                    item=item,
                    target_map=target_map,
                    dimensions=dimensions,
                    array_shape=array_shape,
                    chunk_shape=chunk_shape,
                    source_units=source_units,
                )
                peak_memory_bytes = _process_peak_rss_bytes()
                del chunk
                result = WeatherNextSequentialReadResult(
                    payload_read=True,
                    manifest_sha256=manifest.manifest_sha256,
                    snapshots_written=0,
                    snapshot_paths=[],
                    bytes_read=probe_bytes,
                    object_count=1,
                    message=(
                        "WeatherNext one-block probe completed; no snapshot was published. "
                        f"decode_seconds={decode_elapsed:.3f}"
                    ),
                    probe_only=True,
                    elapsed_seconds=monotonic_clock.monotonic() - started,
                    decoded_shape=decoded_shape,
                    decoded_bytes=decoded_bytes,
                    object_uri=item.object_uri,
                    compressed_bytes=probe_bytes,
                    download_seconds=probe_download_seconds,
                    decode_seconds=decode_elapsed,
                    peak_memory_bytes=peak_memory_bytes,
                    temporary_path=str(probe_path) if probe_path is not None else None,
                    temporary_bytes=probe_bytes,
                    temporary_retained=False,
                    extracted_values=extracted_values,
                )
                _finish_probe_attempt(
                    probe_attempt_path,
                    state="completed",
                    actual_payload_bytes=probe_bytes,
                )
                return result
            chunk_starts = [int(value.start) for value in selection]
            for target_id in item.target_ids:
                if target_id not in target_map:
                    raise RuntimeError(f"Manifest object references unknown target {target_id}")
                target = target_map[target_id]
                dimensions_for_target, valid_records = _manifest_target_selection(target)
                if dimensions_for_target != dimensions:
                    raise RuntimeError(f"Target {target_id} dimensions do not match manifest array")
                _, valid_times, member_values = accumulators[target_id]
                raw_selection = cast(Mapping[str, object], target["selection"])
                lat_index = int(cast(int | str, raw_selection["latitude_index"]))
                lon_index = int(cast(int | str, raw_selection["longitude_index"]))
                for record in valid_records:
                    valid_time = _parse_utc(
                        record.get("valid_time_utc", ""), field="valid_time_utc"
                    ).isoformat()
                    if valid_time not in valid_times:
                        raise RuntimeError(f"Target {target_id} has an unlisted valid time")
                    global_indices: dict[str, int] = {
                        "sample": 0,
                        "lead_time": int(cast(int | str, record["lead_time_index"])),
                        "lat_0p05": lat_index,
                        "latitude": lat_index,
                        "lat": lat_index,
                        "lon_0p05": lon_index,
                        "longitude": lon_index,
                        "lon": lon_index,
                    }
                    if "lead_subtime_index" in record:
                        global_indices["lead_subtime"] = int(
                            cast(int | str, record["lead_subtime_index"])
                        )
                    for member_index, member_id in enumerate(accumulators[target_id][0]):
                        global_indices["sample"] = member_index
                        local_indices: list[int] = []
                        for position, dimension in enumerate(dimensions):
                            if dimension not in global_indices:
                                if array_shape[position] != 1:
                                    raise RuntimeError(
                                        "Target "
                                        f"{target_id} leaves dimension {dimension} unspecified"
                                    )
                                index = 0
                            else:
                                index = global_indices[dimension]
                            local = index - chunk_starts[position]
                            if local < 0 or local >= chunk.shape[position]:
                                break
                            local_indices.append(local)
                        else:
                            raw_value = float(chunk[tuple(local_indices)])
                            if not math.isfinite(raw_value):
                                raise RuntimeError(
                                    f"Target {target_id} has a missing WeatherNext value"
                                )
                            value_c = (
                                raw_value - 273.15
                                if source_units.casefold() in {"k", "kelvin"}
                                else raw_value
                            )
                            member_values[member_id][valid_time] = value_c
            bytes_read += item.compressed_bytes
            object_count += 1
            del chunk
    except Exception as error:
        if probe_path is not None:
            with suppress(Exception):
                _finish_probe_attempt(
                    probe_attempt_path,
                    state="failed",
                    actual_payload_bytes=probe_bytes,
                    error_type=type(error).__name__,
                )
        raise
    finally:
        if group is not None:
            close = getattr(group, "close", None)
            if callable(close):
                close()
        if probe_path is not None:
            probe_path.unlink(missing_ok=True)

    now = datetime.now(UTC)
    snapshot_paths: list[str] = []
    index_entries: list[dict[str, object]] = []
    for target_id, target in target_map.items():
        member_ids, valid_times, member_values = accumulators[target_id]
        trajectories: list[dict[str, object]] = []
        for member_id in member_ids:
            values = member_values[member_id]
            if set(values) != set(valid_times):
                missing = sorted(set(valid_times) - set(values))
                raise RuntimeError(f"Target {target_id} missing WeatherNext hours: {missing}")
            trajectories.append(
                {
                    "member_id": member_id,
                    "valid_times_utc": valid_times,
                    "values_c": [values[valid_time] for valid_time in valid_times],
                }
            )
        scenario_max = [max(cast(list[float], item["values_c"])) for item in trajectories]
        raw_target_path = str(target.get("snapshot_path") or "")
        if not raw_target_path:
            candidates = [path for path in manifest.snapshot_target_paths if target_id in path]
            raw_target_path = candidates[0] if len(candidates) == 1 else ""
        if not raw_target_path:
            raise RuntimeError(f"Manifest has no snapshot path for target {target_id}")
        snapshot_path = Path(raw_target_path).expanduser()
        if not snapshot_path.is_relative_to(snapshot_root.expanduser().resolve()):
            raise RuntimeError(
                "Snapshot path escapes the configured WeatherNext root: "
                f"{snapshot_path}"
            )
        snapshot = WeatherNextSnapshot(
            init_time_utc=manifest.init_time_utc,
            received_at_utc=now,
            location=str(target.get("location", target_id)),
            observation_date=date.fromisoformat(str(target["observation_date"])),
            observation_timezone=str(target.get("observation_timezone", "UTC")),
            scenario_max_c=scenario_max,
            source_uri=str(target.get("source_uri", manifest.source_uri)),
            release_id=manifest.release_id,
            station_id=target_id,
            latitude=float(cast(float | int | str, target["requested_latitude"])),
            longitude=float(cast(float | int | str, target["requested_longitude"])),
            units="C",
            valid_times_utc=[_parse_utc(value, field="valid_time_utc") for value in valid_times],
            member_ids=member_ids,
            trajectories=trajectories,
        )
        _write_immutable_snapshot(snapshot_path, json.loads(snapshot.model_dump_json()))
        snapshot_paths.append(str(snapshot_path))
        index_entries.append(
            {
                "target_id": target_id,
                "event_id": target.get("event_id"),
                "station_id": target_id,
                "location": target.get("location"),
                "observation_date": target.get("observation_date"),
                "observation_timezone": target.get("observation_timezone"),
                "path": str(snapshot_path),
                "source_uri": str(target.get("source_uri", manifest.source_uri)),
                "release_id": manifest.release_id,
                "init_time_utc": manifest.init_time_utc.isoformat(),
                "received_at_utc": now.isoformat(),
                "member_count": 64,
                "valid_time_count": len(valid_times),
            }
        )
    _update_snapshot_index(index_path, index_entries)
    return WeatherNextSequentialReadResult(
        payload_read=True,
        manifest_sha256=cast(str, approval_result.manifest_sha256),
        snapshots_written=len(snapshot_paths),
        snapshot_paths=snapshot_paths,
        bytes_read=bytes_read,
        object_count=object_count,
        message=(
            "approved WeatherNext payload read completed sequentially with full "
            "trajectory coverage"
        ),
        probe_only=False,
        elapsed_seconds=monotonic_clock.monotonic() - started,
    )


def write_refresh_status(
    status: WeatherNextRefreshStatus, path: Path = DEFAULT_STATUS_PATH
) -> Path:
    return _atomic_write(path, status.model_dump(mode="json"))


def autonomous_refresh_preflight(
    database_path: Path,
    *,
    settings: object | None = None,
    targets_path: Path = DEFAULT_TARGETS_PATH,
    manifest_path: Path = DEFAULT_MANIFEST_PATH,
    approval_path: Path = DEFAULT_APPROVAL_PATH,
    status_path: Path = DEFAULT_STATUS_PATH,
    max_targets: int = 32,
    max_network_bytes: int | None = None,
    max_objects: int = 4096,
    max_object_bytes: int | None = None,
    refresh_manifest: bool = True,
) -> WeatherNextRefreshStatus:
    """One timer-safe run: inventory targets and gate payload reads.

    The function intentionally has no payload-reader call.  A separately
    approved reader may consume the manifest after this preflight; this
    boundary ensures that normal autonomous observer maintenance cannot issue
    an unapproved full-ensemble GET.
    """

    now = datetime.now(UTC)
    inventory = derive_refresh_targets(
        database_path, now_utc=now, output_path=targets_path, max_targets=max_targets
    )
    metadata_error: str | None = None
    if refresh_manifest and inventory.targets and settings is not None:
        from polybot.config import Settings
        from polybot.weathernext import WeatherNextGcsClient
        from polybot.weathernext_manifest import (
            estimate_and_build_full_ensemble_read_manifest_batch,
            write_full_ensemble_read_manifest,
        )

        if not isinstance(settings, Settings):
            raise TypeError("settings must be a polybot.config.Settings instance")
        if settings.weathernext_enabled and settings.weathernext_gcs_project:
            try:
                client = WeatherNextGcsClient(settings)
                targets = [
                    {
                        "event_id": target.event_id,
                        "station_id": target.station_id,
                        "latitude": target.latitude,
                        "longitude": target.longitude,
                        "location": target.location,
                        "observation_date": target.observation_date.isoformat(),
                        "timezone": target.observation_timezone,
                    }
                    for target in inventory.targets
                ]
                built = estimate_and_build_full_ensemble_read_manifest_batch(
                    client,
                    targets=targets,
                    max_network_bytes=(
                        settings.weathernext_full_max_network_bytes
                        if max_network_bytes is None
                        else max_network_bytes
                    ),
                    max_objects=max_objects,
                    max_object_bytes=(
                        settings.weathernext_full_max_object_bytes
                        if max_object_bytes is None
                        else max_object_bytes
                    ),
                    snapshot_root=Path(settings.weathernext_full_root) / "snapshots",
                )
                complete, _incomplete, _mixed = _manifest_coverage_summary(built)
                if not complete:
                    fallback = _find_complete_release_manifest(
                        client,
                        targets=targets,
                        initial_manifest=built,
                        max_network_bytes=(
                            settings.weathernext_full_max_network_bytes
                            if max_network_bytes is None
                            else max_network_bytes
                        ),
                        max_objects=max_objects,
                        max_object_bytes=(
                            settings.weathernext_full_max_object_bytes
                            if max_object_bytes is None
                            else max_object_bytes
                        ),
                        snapshot_root=Path(settings.weathernext_full_root) / "snapshots",
                    )
                    if fallback is not None:
                        built = fallback
                raw_dates = built.period.get("observation_dates", [])
                dates = cast(list[object], raw_dates) if isinstance(raw_dates, list) else []
                date_batch = "-".join(str(value) for value in dates)
                archive_path = (
                    manifest_path.parent
                    / "manifests"
                    / built.release_id
                    / (date_batch or "batch")
                    / "read-manifest.json"
                )
                write_full_ensemble_read_manifest(built, archive_path)
                write_full_ensemble_read_manifest(built, manifest_path)
            except Exception as error:
                metadata_error = str(error)
    approval = verify_read_approval(manifest_path, approval_path)
    if approval.state == "approved" and inventory.targets:
        try:
            raw_manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            raw_targets = raw_manifest.get("targets", [])
            manifest_target_ids = {
                str(item.get("target_id"))
                for item in raw_targets
                if isinstance(item, Mapping) and item.get("target_id") is not None
            }
            inventory_target_ids = {target.station_id for target in inventory.targets}
            if manifest_target_ids != inventory_target_ids:
                approval = WeatherNextApprovalResult(
                    state="manifest_mismatch",
                    manifest_sha256=approval.manifest_sha256,
                    expected_network_bytes=approval.expected_network_bytes,
                    object_count=approval.object_count,
                    message=(
                        "approved manifest target set differs from the current inventory; "
                        "payload read remains blocked"
                    ),
                )
        except Exception as error:
            approval = WeatherNextApprovalResult(
                state="invalid",
                manifest_sha256=approval.manifest_sha256,
                expected_network_bytes=approval.expected_network_bytes,
                object_count=approval.object_count,
                message=f"current manifest target set could not be checked: {error}",
            )
    try:
        manifest_payload = json.loads(manifest_path.read_text(encoding="utf-8"))
        manifest_mapping = cast(Mapping[str, object], manifest_payload)
        raw_gate = manifest_mapping.get("approval_gate")
        gate = cast(Mapping[str, object], raw_gate) if isinstance(raw_gate, Mapping) else {}
        manifest_state = str(gate.get("state", "missing"))
        raw_period = manifest_mapping.get("period")
        period = cast(Mapping[str, object], raw_period) if isinstance(raw_period, Mapping) else {}
        raw_incomplete = period.get("incomplete_target_ids", gate.get("incomplete_target_ids", []))
        incomplete_target_ids = (
            sorted({str(item) for item in raw_incomplete})
            if isinstance(raw_incomplete, list)
            else []
        )
        coverage_complete = bool(
            period.get(
                "all_targets_complete_station_local_day",
                gate.get("coverage_complete", not incomplete_target_ids),
            )
        ) and not incomplete_target_ids
        mixed_observation_dates = bool(period.get("mixed_observation_dates", False))
    except Exception:
        manifest_state = "missing"
        coverage_complete = False
        incomplete_target_ids = []
        mixed_observation_dates = False
    if metadata_error:
        approval = WeatherNextApprovalResult(
            state="invalid",
            manifest_sha256=approval.manifest_sha256,
            expected_network_bytes=approval.expected_network_bytes,
            object_count=approval.object_count,
            message=(
                "metadata manifest refresh failed; existing approval cannot be reused: "
                f"{metadata_error}"
            ),
        )
        message = (
            "metadata manifest refresh failed; payload read remains blocked: "
            f"{metadata_error}"
        )
    elif approval.state != "approved":
        if approval.state == "coverage_blocked" or not coverage_complete:
            targets = ", ".join(approval.incomplete_target_ids) or ", ".join(
                incomplete_target_ids
            ) or "unknown target"
            message = (
                "payload read remains blocked: incomplete station-local-day coverage for "
                f"{targets}; no hours are synthesized"
            )
        else:
            message = (
                "payload read remains blocked until an approved sequential reader "
                "consumes the manifest"
            )
        if mixed_observation_dates:
            message += (
                "; manifest contains multiple observation dates, each target is "
                "checked separately"
            )
    else:
        message = "approval verified; sequential payload reader may run as a separate bounded step"
    status = WeatherNextRefreshStatus(
        generated_at_utc=now,
        targets_path=str(targets_path),
        manifest_path=str(manifest_path),
        approval_path=str(approval_path),
        target_count=len(inventory.targets),
        manifest_state=manifest_state,
        approval=approval,
        payload_read=False,
        snapshots_written=0,
        message=message,
        coverage_complete=coverage_complete,
        incomplete_target_ids=incomplete_target_ids,
        mixed_observation_dates=mixed_observation_dates,
    )
    write_refresh_status(status, status_path)
    return status


__all__ = [
    "DEFAULT_APPROVAL_PATH",
    "DEFAULT_MANIFEST_PATH",
    "DEFAULT_PROBE_APPROVAL_PATH",
    "DEFAULT_PROBE_ATTEMPT_PATH",
    "DEFAULT_ROOT",
    "DEFAULT_STATUS_PATH",
    "DEFAULT_TARGETS_PATH",
    "WeatherNextApprovalResult",
    "WeatherNextReadApproval",
    "WeatherNextRefreshStatus",
    "WeatherNextRefreshTarget",
    "WeatherNextSequentialReadResult",
    "WeatherNextTargetInventory",
    "autonomous_refresh_preflight",
    "derive_refresh_targets",
    "read_approved_manifest_sequentially",
    "verify_one_block_probe_approval",
    "verify_read_approval",
    "write_refresh_status",
]
