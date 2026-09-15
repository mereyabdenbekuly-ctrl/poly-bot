"""Safe ECMWF IFS ENS temperature retrieval and immutable archival.

Only the physics-based IFS atmospheric ENS stream (``enfo``), perturbed
members (``pf``), and all 50 members are accepted.  ``oper/fc`` and ``enfo/cf``
are never used as an ensemble substitute; AIFS is not supported here.
"""

from __future__ import annotations

import fcntl
import hashlib
import importlib
import importlib.util
import json
import math
import os
import re
import shutil
import stat
import tempfile
from collections.abc import Callable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from email.utils import parsedate_to_datetime
from enum import StrEnum
from pathlib import Path
from typing import Protocol

import httpx

ECMWF_OPEN_DATA_URL = "https://data.ecmwf.int/forecasts"
IFS_ENS_MEMBER_NUMBERS = tuple(range(1, 51))
_RANGE_RE = re.compile(r"^bytes (\d+)-(\d+)/(\d+|\*)$")
_ARCHIVE_ID_RE = re.compile(r"^[0-9a-f]{64}$")
_GIB = 1024**3
_RETENTION_MANIFEST_MAX_BYTES = 4 * 1024 * 1024
_RETENTION_PROTECTED_MARKERS = (".retention-protected", "PROTECTED")


class EcmwfState(StrEnum):
    AVAILABLE = "available"
    PENDING = "pending"
    UNAVAILABLE = "unavailable"


class EcmwfProduct(StrEnum):
    """Supported surface temperature products."""

    INSTANTANEOUS_2T = "2t"
    DAILY_MAX_2T = "mx2t3"


@dataclass(frozen=True, slots=True)
class EcmwfStatus:
    state: EcmwfState
    product: EcmwfProduct
    parameter: str
    init_time_utc: datetime
    checked_at_utc: datetime
    steps: tuple[int, ...]
    published_at_utc: datetime | None
    member_count: int
    message: str
    index_urls: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class EcmwfTemperaturePoint:
    step_hours: int
    valid_time_utc: datetime
    temperature_c: float
    interval_start_utc: datetime | None = None
    interval_end_utc: datetime | None = None


@dataclass(frozen=True, slots=True)
class EcmwfMemberScenario:
    member_number: int
    points: tuple[EcmwfTemperaturePoint, ...]


@dataclass(frozen=True, slots=True)
class EcmwfArchiveArtifact:
    relative_path: str
    index_relative_path: str
    step_hours: int
    parameter: str
    interval_hours: int | None
    byte_size: int
    sha256: str
    index_byte_size: int
    index_sha256: str
    source_url: str
    index_source_url: str
    published_at_utc: datetime


@dataclass(frozen=True, slots=True)
class EcmwfRawArchive:
    archive_id: str
    archive_path: Path
    init_time_utc: datetime
    published_at_utc: datetime
    fetched_at_utc: datetime
    steps: tuple[int, ...]
    product: EcmwfProduct
    artifacts: tuple[EcmwfArchiveArtifact, ...]


@dataclass(frozen=True, slots=True)
class EcmwfArchiveResult:
    status: EcmwfStatus
    archive: EcmwfRawArchive | None
    retention: EcmwfArchiveRetentionReport | None = None


@dataclass(frozen=True, slots=True)
class EcmwfArchiveRetentionPolicy:
    """Bounds for the official raw archive collector.

    The policy is opt-in at adapter construction so read-only/library callers
    are not coupled to the capacity of the machine running their tests.  The
    checked-in collector enables these production-safe defaults explicitly.
    Unrecognised/partial directories are never candidates for deletion.  A
    completed release can be protected with ``<archive-id>.protected`` beside
    it, an in-directory marker, or ``protected_archive_ids``.
    """

    max_completed_releases: int = 28
    max_completed_bytes: int = 8 * _GIB
    min_free_bytes: int = 50 * _GIB
    min_free_fraction: float = 0.25

    def __post_init__(self) -> None:
        if self.max_completed_releases < 1:
            raise ValueError("ECMWF retention must keep at least one completed release")
        if self.max_completed_bytes < 1:
            raise ValueError("ECMWF retention byte limit must be positive")
        if self.min_free_bytes < 0:
            raise ValueError("ECMWF minimum free bytes cannot be negative")
        if not 0 <= self.min_free_fraction < 1:
            raise ValueError("ECMWF minimum free fraction must be in [0, 1)")


@dataclass(frozen=True, slots=True)
class EcmwfArchiveRetentionReport:
    max_completed_releases: int
    max_completed_bytes: int
    min_free_bytes: int
    min_free_fraction: float
    completed_releases: int
    completed_bytes: int
    protected_releases: int
    protected_bytes: int
    preserved_diagnostics: int
    preserved_diagnostic_bytes: int
    pruned_releases: int
    pruned_bytes: int
    disk_total_bytes: int
    disk_free_bytes: int
    disk_free_fraction: float
    within_limits: bool
    reason: str | None = None


@dataclass(frozen=True, slots=True)
class EcmwfPointForecast:
    point_id: str
    latitude: float
    longitude: float
    scenarios: tuple[EcmwfMemberScenario, ...]


@dataclass(frozen=True, slots=True)
class EcmwfDailyMaximum:
    member_number: int
    temperature_c: float


@dataclass(frozen=True, slots=True)
class EcmwfBatchSnapshot:
    archive: EcmwfRawArchive
    decoded_at_utc: datetime
    points: tuple[EcmwfPointForecast, ...]


@dataclass(frozen=True, slots=True)
class EcmwfBatchFetchResult:
    status: EcmwfStatus
    archive: EcmwfRawArchive | None
    snapshot: EcmwfBatchSnapshot | None


@dataclass(frozen=True, slots=True)
class EcmwfSnapshot:
    """Single-point compatibility shape; use ``fetch_points`` for scans."""

    archive_id: str
    archive_path: Path
    init_time_utc: datetime
    published_at_utc: datetime
    fetched_at_utc: datetime
    decoded_at_utc: datetime
    latitude: float
    longitude: float
    scenarios: tuple[EcmwfMemberScenario, ...]
    artifacts: tuple[EcmwfArchiveArtifact, ...]
    product: EcmwfProduct


@dataclass(frozen=True, slots=True)
class EcmwfFetchResult:
    status: EcmwfStatus
    snapshot: EcmwfSnapshot | None


@dataclass(frozen=True, slots=True)
class HttpResponse:
    status_code: int
    headers: Mapping[str, str]
    content: bytes


class EcmwfTransport(Protocol):
    def request(
        self,
        method: str,
        url: str,
        *,
        headers: Mapping[str, str] | None,
        max_bytes: int,
    ) -> HttpResponse: ...


class EcmwfPointDecoder(Protocol):
    def availability_error(self) -> str | None: ...

    def decode_points(
        self,
        path: Path,
        *,
        points: Mapping[str, tuple[float, float]],
        expected_init_time_utc: datetime,
        expected_step_hours: int,
        expected_parameter: str,
        expected_interval_hours: int | None,
        expected_members: Sequence[int],
    ) -> Mapping[str, Mapping[int, float]]: ...


class EcmwfError(RuntimeError):
    pass


class EcmwfPendingError(EcmwfError):
    pass


class EcmwfUnavailableError(EcmwfError):
    pass


class EcmwfValidationError(EcmwfUnavailableError):
    pass


class EcmwfRetentionError(EcmwfUnavailableError):
    def __init__(self, message: str, report: EcmwfArchiveRetentionReport) -> None:
        super().__init__(message)
        self.report = report


@dataclass(frozen=True, slots=True)
class _RetainedArchive:
    archive_id: str
    path: Path
    init_time_utc: datetime
    fetched_at_utc: datetime
    byte_size: int
    protected: bool


@dataclass(frozen=True, slots=True)
class _RetentionInventory:
    archives: tuple[_RetainedArchive, ...]
    diagnostic_count: int
    diagnostic_bytes: int


class HttpxEcmwfTransport:
    def __init__(self, *, timeout_seconds: float = 30.0) -> None:
        self.timeout_seconds = timeout_seconds

    def request(
        self,
        method: str,
        url: str,
        *,
        headers: Mapping[str, str] | None,
        max_bytes: int,
    ) -> HttpResponse:
        chunks: list[bytes] = []
        size = 0
        with httpx.stream(
            method, url, headers=headers, timeout=self.timeout_seconds, follow_redirects=False
        ) as response:
            for chunk in response.iter_bytes():
                size += len(chunk)
                if size > max_bytes:
                    raise EcmwfUnavailableError(f"response exceeded {max_bytes}-byte safety limit")
                chunks.append(chunk)
            return HttpResponse(
                response.status_code,
                {k.lower(): v for k, v in response.headers.items()},
                b"".join(chunks),
            )


class EcCodesPointDecoder:
    """Optional ecCodes decoder for multiple coordinates per GRIB message."""

    def availability_error(self) -> str | None:
        if importlib.util.find_spec("eccodes") is None:
            return "the optional 'eccodes' package is not installed"
        return None

    def decode_points(
        self,
        path: Path,
        *,
        points: Mapping[str, tuple[float, float]],
        expected_init_time_utc: datetime,
        expected_step_hours: int,
        expected_parameter: str,
        expected_interval_hours: int | None,
        expected_members: Sequence[int],
    ) -> Mapping[str, Mapping[int, float]]:
        error = self.availability_error()
        if error is not None:
            raise EcmwfUnavailableError(error)
        eccodes = importlib.import_module("eccodes")
        values: dict[str, dict[int, float]] = {key: {} for key in points}
        with path.open("rb") as handle:
            while True:
                message = eccodes.codes_grib_new_from_file(handle)
                if message is None:
                    break
                try:
                    short_name = str(eccodes.codes_get(message, "shortName"))
                    data_type = str(eccodes.codes_get(message, "dataType"))
                    member = int(eccodes.codes_get(message, "perturbationNumber"))
                    step = int(eccodes.codes_get(message, "endStep"))
                    data_date = int(eccodes.codes_get(message, "dataDate"))
                    data_time = int(eccodes.codes_get(message, "dataTime"))
                    units = str(eccodes.codes_get(message, "units"))
                    if short_name != expected_parameter or data_type != "pf":
                        raise EcmwfValidationError(
                            "decoded GRIB parameter/type does not match requested IFS ENS pf field"
                        )
                    if member not in expected_members or step != expected_step_hours:
                        raise EcmwfValidationError("decoded GRIB member/step mismatch")
                    if data_date != int(
                        expected_init_time_utc.strftime("%Y%m%d")
                    ) or data_time != int(expected_init_time_utc.strftime("%H%M")):
                        raise EcmwfValidationError("decoded GRIB initialization mismatch")
                    if expected_interval_hours is not None:
                        start_step = int(eccodes.codes_get(message, "startStep"))
                        step_type = str(eccodes.codes_get(message, "stepType"))
                        if (
                            step_type != "max"
                            or start_step != expected_step_hours - expected_interval_hours
                        ):
                            raise EcmwfValidationError(
                                "decoded maximum-temperature interval is not the "
                                "expected previous three-hour window"
                            )
                    for point_id, (latitude, longitude) in points.items():
                        point_values = values[point_id]
                        if member in point_values:
                            raise EcmwfValidationError(
                                f"duplicate member {member} for point {point_id!r}"
                            )
                        nearest = eccodes.codes_grib_find_nearest(message, latitude, longitude)
                        if not nearest:
                            raise EcmwfValidationError(f"no nearest grid point for {point_id!r}")
                        raw = float(nearest[0]["value"])
                        if units not in {"K", "kelvin", "Kelvin"}:
                            raise EcmwfValidationError(f"unsupported 2t unit {units!r}")
                        point_values[member] = raw - 273.15
                finally:
                    eccodes.codes_release(message)
        for member_values in values.values():
            _validate_member_values(member_values, expected_members)
        return values


@dataclass(frozen=True, slots=True)
class _IndexEntry:
    member_number: int
    offset: int
    length: int


@dataclass(frozen=True, slots=True)
class _StepIndex:
    step_hours: int
    parameter: str
    interval_hours: int | None
    index_url: str
    data_url: str
    published_at_utc: datetime
    payload: bytes
    entries: tuple[_IndexEntry, ...]


class EcmwfIfsEnsAdapter:
    """Archive IFS ENS once, then extract one or many station coordinates."""

    def __init__(
        self,
        *,
        archive_root: Path,
        source_base_url: str = ECMWF_OPEN_DATA_URL,
        transport: EcmwfTransport | None = None,
        decoder: EcmwfPointDecoder | None = None,
        clock: Callable[[], datetime] | None = None,
        publication_deadline_hours: int = 10,
        max_index_bytes: int = 8 * 1024 * 1024,
        max_grib_message_bytes: int = 4 * 1024 * 1024,
        retention_policy: EcmwfArchiveRetentionPolicy | None = None,
        protected_archive_ids: Sequence[str] = (),
        disk_usage: Callable[[Path], tuple[int, int, int]] | None = None,
    ) -> None:
        source = source_base_url.rstrip("/")
        if not source.startswith("https://"):
            raise ValueError("ECMWF Open Data source must use HTTPS")
        if publication_deadline_hours < 9:
            raise ValueError("publication deadline must be at least 9 hours")
        self.archive_root = archive_root.expanduser().resolve()
        self.source_base_url = source
        self.transport = transport or HttpxEcmwfTransport()
        self.decoder = decoder or EcCodesPointDecoder()
        self._clock = clock or (lambda: datetime.now(UTC))
        self.publication_deadline = timedelta(hours=publication_deadline_hours)
        self.max_index_bytes = max_index_bytes
        self.max_grib_message_bytes = max_grib_message_bytes
        self.retention_policy = retention_policy
        normalized_protected = tuple(
            str(value).strip().lower() for value in protected_archive_ids
        )
        if any(_ARCHIVE_ID_RE.fullmatch(archive_id) is None for archive_id in normalized_protected):
            raise ValueError("protected ECMWF archive IDs must be 64 lowercase hex characters")
        self.protected_archive_ids = frozenset(normalized_protected)
        self._disk_usage = disk_usage or (lambda path: tuple(shutil.disk_usage(path)))

    @staticmethod
    def latest_conservative_init(now: datetime | None = None) -> datetime:
        current = _as_utc(now or datetime.now(UTC), field="now")
        candidate = current - timedelta(hours=9)
        cycle_hour = candidate.hour - candidate.hour % 6
        return candidate.replace(hour=cycle_hour, minute=0, second=0, microsecond=0)

    def probe(
        self,
        *,
        init_time_utc: datetime,
        steps: Sequence[int],
        product: EcmwfProduct = EcmwfProduct.DAILY_MAX_2T,
    ) -> EcmwfStatus:
        init_time, normalized_steps = _validate_request(init_time_utc, steps, product)
        checked = _as_utc(self._clock(), field="clock")
        try:
            indexes = self._inspect_indexes(init_time, normalized_steps, product)
            return self._available_status(indexes, init_time, normalized_steps, product, checked)
        except (EcmwfPendingError, EcmwfUnavailableError, httpx.HTTPError, OSError) as error:
            return self._failure_status(
                init_time, normalized_steps, product, checked, error, "probe"
            )

    def fetch_archive(
        self,
        *,
        init_time_utc: datetime,
        steps: Sequence[int],
        product: EcmwfProduct = EcmwfProduct.DAILY_MAX_2T,
    ) -> EcmwfArchiveResult:
        init_time, normalized_steps = _validate_request(init_time_utc, steps, product)
        checked = _as_utc(self._clock(), field="clock")
        with self._retention_lock():
            return self._fetch_archive_locked(
                init_time=init_time,
                steps=normalized_steps,
                product=product,
                checked=checked,
            )

    def _fetch_archive_locked(
        self,
        *,
        init_time: datetime,
        steps: tuple[int, ...],
        product: EcmwfProduct,
        checked: datetime,
    ) -> EcmwfArchiveResult:
        try:
            indexes = self._inspect_indexes(init_time, steps, product)
            status = self._available_status(indexes, init_time, steps, product, checked)
            existing = self._find_existing_archive(init_time, indexes, product)
            if existing is not None:
                retention = self.retention_status()
                if retention is not None and not retention.within_limits:
                    status = self._retention_failure_status(status, retention)
                return EcmwfArchiveResult(status, existing, retention)
            if self.retention_policy is not None:
                projected_bytes = self._projected_archive_bytes(init_time, indexes, product)
                self._retention_preflight(projected_bytes)
            archive = self._download_archive(init_time, indexes, product)
            retention = self._prune_archives(keep_archive_ids={archive.archive_id})
            if retention is not None and not retention.within_limits:
                status = self._retention_failure_status(status, retention)
            return EcmwfArchiveResult(status, archive, retention)
        except EcmwfRetentionError as error:
            return EcmwfArchiveResult(
                self._failure_status(
                    init_time, steps, product, checked, error, "archive"
                ),
                None,
                error.report,
            )
        except (EcmwfPendingError, EcmwfUnavailableError, httpx.HTTPError, OSError) as error:
            return EcmwfArchiveResult(
                self._failure_status(
                    init_time, steps, product, checked, error, "archive"
                ),
                None,
            )

    @staticmethod
    def _retention_failure_status(
        status: EcmwfStatus, retention: EcmwfArchiveRetentionReport
    ) -> EcmwfStatus:
        return EcmwfStatus(
            EcmwfState.UNAVAILABLE,
            status.product,
            status.parameter,
            status.init_time_utc,
            status.checked_at_utc,
            status.steps,
            status.published_at_utc,
            status.member_count,
            "IFS ENS retention policy is not satisfied: "
            f"{retention.reason or 'unknown retention failure'}",
            status.index_urls,
        )

    def extract_points(
        self,
        archive: EcmwfRawArchive,
        *,
        points: Mapping[str, tuple[float, float]],
    ) -> EcmwfBatchFetchResult:
        normalized = _validate_points(points)
        checked = _as_utc(self._clock(), field="clock")
        index_urls = tuple(a.index_source_url for a in archive.artifacts)
        error = self.decoder.availability_error()
        if error is not None:
            return self._batch_failure(
                archive, checked, index_urls, f"decoder unavailable: {error}"
            )
        try:
            values: dict[str, dict[int, Mapping[int, float]]] = {
                point_id: {} for point_id in normalized
            }
            for artifact in sorted(archive.artifacts, key=lambda a: a.step_hours):
                grib = self._verified_artifact_path(archive, artifact)
                decoded = self.decoder.decode_points(
                    grib,
                    points=normalized,
                    expected_init_time_utc=archive.init_time_utc,
                    expected_step_hours=artifact.step_hours,
                    expected_parameter=artifact.parameter,
                    expected_interval_hours=artifact.interval_hours,
                    expected_members=IFS_ENS_MEMBER_NUMBERS,
                )
                if set(decoded) != set(normalized):
                    raise EcmwfValidationError("decoder returned an unexpected point set")
                for point_id, member_values in decoded.items():
                    _validate_member_values(member_values, IFS_ENS_MEMBER_NUMBERS)
                    values[point_id][artifact.step_hours] = member_values
            forecasts = tuple(
                EcmwfPointForecast(
                    point_id=point_id,
                    latitude=coords[0],
                    longitude=coords[1],
                    scenarios=_build_scenarios(
                        archive.init_time_utc, archive.steps, values[point_id], archive.product
                    ),
                )
                for point_id, coords in normalized.items()
            )
            decoded_at = _as_utc(self._clock(), field="clock")
            status = EcmwfStatus(
                EcmwfState.AVAILABLE,
                archive.product,
                _parameter_for_product(archive.product),
                archive.init_time_utc,
                decoded_at,
                archive.steps,
                archive.published_at_utc,
                len(IFS_ENS_MEMBER_NUMBERS),
                f"Decoded 50 IFS ENS scenarios for {len(forecasts)} point(s) from one archive.",
                index_urls,
            )
            return EcmwfBatchFetchResult(
                status,
                archive,
                EcmwfBatchSnapshot(archive, decoded_at, forecasts),
            )
        except (EcmwfError, OSError, ValueError) as error:
            return self._batch_failure(
                archive, _as_utc(self._clock(), field="clock"), index_urls, str(error)
            )

    def fetch_points(
        self,
        *,
        init_time_utc: datetime,
        steps: Sequence[int],
        points: Mapping[str, tuple[float, float]],
        product: EcmwfProduct = EcmwfProduct.DAILY_MAX_2T,
    ) -> EcmwfBatchFetchResult:
        init_time, normalized_steps = _validate_request(init_time_utc, steps, product)
        _validate_points(points)
        if self.decoder.availability_error() is not None:
            status = self._failure_status(
                init_time,
                normalized_steps,
                product,
                _as_utc(self._clock(), field="clock"),
                EcmwfUnavailableError(self.decoder.availability_error() or "decoder unavailable"),
                "fetch",
            )
            return EcmwfBatchFetchResult(status, None, None)
        archived = self.fetch_archive(
            init_time_utc=init_time, steps=normalized_steps, product=product
        )
        if archived.archive is None or archived.status.state != EcmwfState.AVAILABLE:
            return EcmwfBatchFetchResult(archived.status, None, None)
        return self.extract_points(archived.archive, points=points)

    def fetch_point(
        self,
        *,
        init_time_utc: datetime,
        steps: Sequence[int],
        latitude: float,
        longitude: float,
        product: EcmwfProduct = EcmwfProduct.DAILY_MAX_2T,
    ) -> EcmwfFetchResult:
        result = self.fetch_points(
            init_time_utc=init_time_utc,
            steps=steps,
            points={"point": (latitude, longitude)},
            product=product,
        )
        if result.snapshot is None or result.archive is None:
            return EcmwfFetchResult(result.status, None)
        point = result.snapshot.points[0]
        archive = result.archive
        return EcmwfFetchResult(
            result.status,
            EcmwfSnapshot(
                archive.archive_id,
                archive.archive_path,
                archive.init_time_utc,
                archive.published_at_utc,
                archive.fetched_at_utc,
                result.snapshot.decoded_at_utc,
                point.latitude,
                point.longitude,
                point.scenarios,
                archive.artifacts,
                archive.product,
            ),
        )

    def fetch_daily_max_points(
        self,
        *,
        init_time_utc: datetime,
        day_start_utc: datetime,
        day_end_utc: datetime,
        points: Mapping[str, tuple[float, float]],
    ) -> EcmwfBatchFetchResult:
        """Fetch exact aligned 3-hour maximum windows for one UTC day span.

        Local-day boundaries that are not aligned to the IFS three-hour grid
        are rejected rather than silently including temperatures outside the
        requested calendar day.
        """

        steps = daily_max_steps(
            init_time_utc=init_time_utc,
            day_start_utc=day_start_utc,
            day_end_utc=day_end_utc,
        )
        return self.fetch_points(
            init_time_utc=init_time_utc,
            steps=steps,
            points=points,
            product=EcmwfProduct.DAILY_MAX_2T,
        )

    def _available_status(
        self,
        indexes: tuple[_StepIndex, ...],
        init_time: datetime,
        steps: tuple[int, ...],
        product: EcmwfProduct,
        checked: datetime,
    ) -> EcmwfStatus:
        return EcmwfStatus(
            EcmwfState.AVAILABLE,
            product,
            _parameter_for_product(product),
            init_time,
            checked,
            steps,
            max(i.published_at_utc for i in indexes),
            len(IFS_ENS_MEMBER_NUMBERS),
            "All 50 IFS ENS perturbed-member fields are published.",
            tuple(i.index_url for i in indexes),
        )

    def _failure_status(
        self,
        init_time: datetime,
        steps: tuple[int, ...],
        product: EcmwfProduct,
        checked: datetime,
        error: Exception,
        context: str,
    ) -> EcmwfStatus:
        pending = isinstance(error, EcmwfPendingError)
        state = (
            EcmwfState.PENDING
            if pending and checked <= init_time + self.publication_deadline
            else EcmwfState.UNAVAILABLE
        )
        message = (
            f"IFS ENS publication pending: {error}"
            if state == EcmwfState.PENDING
            else (
                f"IFS ENS unavailable after publication deadline: {error}"
                if pending
                else f"IFS ENS {context} unavailable: {error}"
            )
        )
        return EcmwfStatus(
            state,
            product,
            _parameter_for_product(product),
            init_time,
            checked,
            steps,
            None,
            0,
            message,
            tuple(self._index_url(init_time, step, product) for step in steps),
        )

    def _inspect_indexes(
        self, init_time: datetime, steps: tuple[int, ...], product: EcmwfProduct
    ) -> tuple[_StepIndex, ...]:
        return tuple(self._load_index(init_time, step, product) for step in steps)

    def _load_index(self, init_time: datetime, step: int, product: EcmwfProduct) -> _StepIndex:
        index_url = self._index_url(init_time, step, product)
        response = self.transport.request(
            "GET", index_url, headers=None, max_bytes=self.max_index_bytes
        )
        if response.status_code == 404:
            raise EcmwfPendingError(f"step {step} index returned HTTP 404")
        if response.status_code != 200:
            raise EcmwfUnavailableError(f"step {step} index returned HTTP {response.status_code}")
        published = _required_last_modified(response.headers, context="index")
        parameter, interval, entries = _parse_index(
            response.content,
            init_time=init_time,
            step=step,
            product=product,
            max_message_bytes=self.max_grib_message_bytes,
        )
        return _StepIndex(
            step,
            parameter,
            interval,
            index_url,
            index_url.removesuffix(".index") + ".grib2",
            published,
            response.content,
            entries,
        )

    def _download_archive(
        self, init_time: datetime, indexes: tuple[_StepIndex, ...], product: EcmwfProduct
    ) -> EcmwfRawArchive:
        self.archive_root.mkdir(parents=True, exist_ok=True)
        staging = Path(tempfile.mkdtemp(prefix=".ifs-ens-", dir=self.archive_root))
        try:
            artifacts: list[EcmwfArchiveArtifact] = []
            for index in indexes:
                relative = f"step-{index.step_hours:03d}-{index.parameter}.grib2"
                index_relative = f"step-{index.step_hours:03d}-{index.parameter}.index"
                _write_new_file(staging / index_relative, index.payload)
                digest = hashlib.sha256()
                size = 0
                with (staging / relative).open("xb") as target:
                    for entry in index.entries:
                        body = self._fetch_range(index, entry)
                        target.write(body)
                        digest.update(body)
                        size += len(body)
                    target.flush()
                    os.fsync(target.fileno())
                artifacts.append(
                    EcmwfArchiveArtifact(
                        relative,
                        index_relative,
                        index.step_hours,
                        index.parameter,
                        index.interval_hours,
                        size,
                        digest.hexdigest(),
                        len(index.payload),
                        hashlib.sha256(index.payload).hexdigest(),
                        index.data_url,
                        index.index_url,
                        index.published_at_utc,
                    )
                )
            fetched = _as_utc(self._clock(), field="clock")
            stable = _raw_manifest_body(self.source_base_url, init_time, product, tuple(artifacts))
            archive_id = hashlib.sha256(_canonical_json(stable)).hexdigest()
            manifest = {
                "schema_version": 1,
                "archive_id": archive_id,
                "fetched_at_utc": fetched.isoformat(),
                **stable,
            }
            _write_new_file(staging / "manifest.json", _canonical_json(manifest) + b"\n")
            # The download can take several minutes.  Recheck the hard free-space
            # floor after all bytes are staged but before publishing the immutable
            # directory.  A failed check leaves only the temporary directory, which
            # the ``finally`` block removes.
            self._retention_free_space_check(additional_bytes=0, context="commit")
            final = self._commit_archive(staging, archive_id)
            return self._load_raw_archive(final)
        finally:
            shutil.rmtree(staging, ignore_errors=True)

    @contextmanager
    def _retention_lock(self) -> Iterator[None]:
        """Serialize retention-aware collectors across processes.

        The lock lives beside (not inside) the archive root so lock metadata is
        never mistaken for a partial release.  It is held across inventory,
        download, immutable commit, and pruning; callers without a retention
        policy retain the original lock-free library behaviour.
        """

        if self.retention_policy is None:
            yield
            return
        parent = self.archive_root.parent
        parent.mkdir(parents=True, exist_ok=True)
        lock_path = parent / f".{self.archive_root.name}.retention.lock"
        with lock_path.open("a+") as handle:
            os.chmod(lock_path, 0o600)
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)

    def retention_status(self) -> EcmwfArchiveRetentionReport | None:
        """Return a metadata/stat-only retention snapshot without deleting data."""

        if self.retention_policy is None:
            return None
        inventory = self._retention_inventory()
        reason = self._retention_limit_reason(inventory)
        return self._retention_report(inventory, reason=reason)

    def _retention_preflight(self, projected_archive_bytes: int) -> None:
        policy = self.retention_policy
        if policy is None:
            return
        inventory = self._retention_inventory()
        protected = tuple(item for item in inventory.archives if item.protected)
        protected_bytes = sum(item.byte_size for item in protected)
        reason: str | None = None
        if len(protected) + 1 > policy.max_completed_releases:
            reason = (
                "protected completed releases leave no retention slot for a new archive "
                f"({len(protected)} protected, max {policy.max_completed_releases})"
            )
        elif protected_bytes + projected_archive_bytes > policy.max_completed_bytes:
            reason = (
                "protected completed releases plus the projected archive exceed the "
                f"{policy.max_completed_bytes}-byte retention limit"
            )
        if reason is not None:
            report = self._retention_report(inventory, reason=reason, force_within_limits=False)
            raise EcmwfRetentionError(reason, report)
        self._retention_free_space_check(
            additional_bytes=projected_archive_bytes,
            context="download",
            inventory=inventory,
        )
        completed = len(inventory.archives) + 1
        completed_bytes = sum(item.byte_size for item in inventory.archives)
        completed_bytes += projected_archive_bytes
        if (
            completed <= policy.max_completed_releases
            and completed_bytes <= policy.max_completed_bytes
        ):
            return
        candidates = sorted(
            (
                item
                for item in inventory.archives
                if not item.protected
                and not self._archive_is_protected(item.path, item.archive_id, frozenset())
            ),
            key=lambda item: (item.init_time_utc, item.fetched_at_utc, item.archive_id),
        )
        for item in candidates:
            if (
                completed <= policy.max_completed_releases
                and completed_bytes <= policy.max_completed_bytes
            ):
                return
            try:
                # Validate every release that would have to be pruned before
                # any new network bytes are fetched.  This is fail-closed for
                # same-size digest corruption and forged/partial manifests.
                self._load_raw_archive(item.path)
            except (EcmwfError, OSError) as error:
                reason = (
                    f"cannot enforce ECMWF retention; candidate {item.archive_id} "
                    f"failed immutable validation: {error}"
                )
                report = self._retention_report(
                    inventory, reason=reason, force_within_limits=False
                )
                raise EcmwfRetentionError(reason, report) from error
            completed -= 1
            completed_bytes -= item.byte_size
        if (
            completed > policy.max_completed_releases
            or completed_bytes > policy.max_completed_bytes
        ):
            reason = (
                "ECMWF retention limits cannot be met without deleting protected or "
                "unverified diagnostics"
            )
            report = self._retention_report(inventory, reason=reason, force_within_limits=False)
            raise EcmwfRetentionError(reason, report)

    def _retention_free_space_check(
        self,
        *,
        additional_bytes: int,
        context: str,
        inventory: _RetentionInventory | None = None,
    ) -> None:
        policy = self.retention_policy
        if policy is None:
            return
        total, _, free = self._archive_disk_usage()
        projected_free = max(0, free - max(0, additional_bytes))
        projected_fraction = projected_free / total if total else 0.0
        reasons: list[str] = []
        if projected_free < policy.min_free_bytes:
            reasons.append(
                f"projected free space {projected_free} bytes is below "
                f"{policy.min_free_bytes} bytes"
            )
        if projected_fraction < policy.min_free_fraction:
            reasons.append(
                f"projected free space {projected_fraction:.2%} is below "
                f"{policy.min_free_fraction:.2%}"
            )
        if reasons:
            reason = f"refusing ECMWF archive {context}: " + "; ".join(reasons)
            current = inventory or self._retention_inventory()
            report = self._retention_report(
                current,
                reason=reason,
                force_within_limits=False,
            )
            raise EcmwfRetentionError(reason, report)

    def _projected_archive_bytes(
        self,
        init_time: datetime,
        indexes: tuple[_StepIndex, ...],
        product: EcmwfProduct,
    ) -> int:
        artifacts = tuple(
            EcmwfArchiveArtifact(
                relative_path=f"step-{index.step_hours:03d}-{index.parameter}.grib2",
                index_relative_path=f"step-{index.step_hours:03d}-{index.parameter}.index",
                step_hours=index.step_hours,
                parameter=index.parameter,
                interval_hours=index.interval_hours,
                byte_size=sum(entry.length for entry in index.entries),
                sha256="0" * 64,
                index_byte_size=len(index.payload),
                index_sha256=hashlib.sha256(index.payload).hexdigest(),
                source_url=index.data_url,
                index_source_url=index.index_url,
                published_at_utc=index.published_at_utc,
            )
            for index in indexes
        )
        stable = _raw_manifest_body(self.source_base_url, init_time, product, artifacts)
        manifest = {
            "schema_version": 1,
            "archive_id": "0" * 64,
            "fetched_at_utc": _as_utc(self._clock(), field="clock").isoformat(),
            **stable,
        }
        return (
            sum(item.byte_size + item.index_byte_size for item in artifacts)
            + len(_canonical_json(manifest))
            + 1
        )

    def _prune_archives(
        self, *, keep_archive_ids: set[str]
    ) -> EcmwfArchiveRetentionReport | None:
        policy = self.retention_policy
        if policy is None:
            return None
        inventory = self._retention_inventory()
        completed = len(inventory.archives)
        completed_bytes = sum(item.byte_size for item in inventory.archives)
        candidates = sorted(
            (
                item
                for item in inventory.archives
                if not item.protected and item.archive_id not in keep_archive_ids
            ),
            key=lambda item: (item.init_time_utc, item.fetched_at_utc, item.archive_id),
        )
        pruned_releases = 0
        pruned_bytes = 0
        errors: list[str] = []
        for item in candidates:
            if (
                completed <= policy.max_completed_releases
                and completed_bytes <= policy.max_completed_bytes
            ):
                break
            if self._archive_is_protected(item.path, item.archive_id, keep_archive_ids):
                continue
            try:
                # Inventory is intentionally metadata-only.  Immediately before
                # deletion, perform the full digest validation so a corrupt or
                # partially modified diagnostic is preserved fail-closed.
                self._load_raw_archive(item.path)
                if self._archive_is_protected(item.path, item.archive_id, keep_archive_ids):
                    continue
                _remove_archive_tree(item.path, archive_root=self.archive_root)
            except (EcmwfError, OSError) as error:
                errors.append(f"preserved unverified {item.archive_id}: {error}")
                continue
            completed -= 1
            completed_bytes -= item.byte_size
            pruned_releases += 1
            pruned_bytes += item.byte_size

        current = self._retention_inventory()
        reason = self._retention_limit_reason(current)
        if errors:
            detail = "; ".join(errors)
            reason = detail if reason is None else f"{reason}; {detail}"
        return self._retention_report(
            current,
            pruned_releases=pruned_releases,
            pruned_bytes=pruned_bytes,
            reason=reason,
        )

    def _retention_inventory(self) -> _RetentionInventory:
        if not self.archive_root.exists():
            return _RetentionInventory((), 0, 0)
        archives: list[_RetainedArchive] = []
        diagnostic_count = 0
        diagnostic_bytes = 0
        for path in self.archive_root.iterdir():
            entry = self._retained_archive(path)
            if entry is not None:
                archives.append(entry)
                continue
            diagnostic_count += 1
            diagnostic_bytes += _tree_size(path)
        return _RetentionInventory(tuple(archives), diagnostic_count, diagnostic_bytes)

    def _retained_archive(self, path: Path) -> _RetainedArchive | None:
        try:
            path_stat = path.lstat()
        except OSError:
            return None
        if (
            not stat.S_ISDIR(path_stat.st_mode)
            or stat.S_ISLNK(path_stat.st_mode)
            or _ARCHIVE_ID_RE.fullmatch(path.name) is None
            or path_stat.st_mode & 0o222
        ):
            return None
        manifest_path = path / "manifest.json"
        try:
            manifest_stat = manifest_path.lstat()
            if (
                not stat.S_ISREG(manifest_stat.st_mode)
                or stat.S_ISLNK(manifest_stat.st_mode)
                or manifest_stat.st_size > _RETENTION_MANIFEST_MAX_BYTES
                or manifest_stat.st_mode & 0o222
            ):
                return None
            manifest = json.loads(manifest_path.read_text())
            archive_id = str(manifest["archive_id"])
            if (
                manifest.get("schema_version") != 1
                or archive_id != path.name
                or _ARCHIVE_ID_RE.fullmatch(archive_id) is None
            ):
                return None
            stable = {
                key: value
                for key, value in manifest.items()
                if key not in {"schema_version", "archive_id", "fetched_at_utc"}
            }
            if hashlib.sha256(_canonical_json(stable)).hexdigest() != archive_id:
                return None
            artifacts = tuple(_artifact_from_json(item) for item in manifest["artifacts"])
            _validate_archive_shape(manifest, artifacts)
            expected = {"manifest.json"}
            for artifact in artifacts:
                for relative, expected_size in (
                    (artifact.relative_path, artifact.byte_size),
                    (artifact.index_relative_path, artifact.index_byte_size),
                ):
                    artifact_path = _safe_archive_child(path, relative)
                    artifact_stat = artifact_path.lstat()
                    if (
                        not stat.S_ISREG(artifact_stat.st_mode)
                        or stat.S_ISLNK(artifact_stat.st_mode)
                        or artifact_stat.st_size != expected_size
                        or artifact_stat.st_mode & 0o222
                    ):
                        return None
                    expected.add(relative)
                    relative_path = Path(relative)
                    expected.update(
                        str(parent)
                        for parent in relative_path.parents
                        if str(parent) != "."
                    )
            actual = _tree_relative_entries(path)
            has_extra_files = bool(actual.difference(expected))
            return _RetainedArchive(
                archive_id=archive_id,
                path=path,
                init_time_utc=_parse_iso(manifest["init_time_utc"]),
                fetched_at_utc=_parse_iso(manifest["fetched_at_utc"]),
                byte_size=_tree_size(path),
                protected=has_extra_files
                or self._archive_is_protected(path, archive_id, frozenset()),
            )
        except (
            EcmwfError,
            KeyError,
            OSError,
            TypeError,
            ValueError,
            json.JSONDecodeError,
        ):
            return None

    def _archive_is_protected(
        self, path: Path, archive_id: str, keep_archive_ids: set[str] | frozenset[str]
    ) -> bool:
        if archive_id in keep_archive_ids or archive_id in self.protected_archive_ids:
            return True
        if (self.archive_root / f"{archive_id}.protected").exists():
            return True
        return any((path / marker).exists() for marker in _RETENTION_PROTECTED_MARKERS)

    def _retention_limit_reason(self, inventory: _RetentionInventory) -> str | None:
        policy = self.retention_policy
        if policy is None:
            return None
        completed_bytes = sum(item.byte_size for item in inventory.archives)
        reasons: list[str] = []
        if len(inventory.archives) > policy.max_completed_releases:
            reasons.append(
                f"{len(inventory.archives)} completed releases exceed max "
                f"{policy.max_completed_releases}"
            )
        if completed_bytes > policy.max_completed_bytes:
            reasons.append(
                f"{completed_bytes} completed bytes exceed max {policy.max_completed_bytes}"
            )
        total, _, free = self._archive_disk_usage()
        free_fraction = free / total if total else 0.0
        if free < policy.min_free_bytes:
            reasons.append(f"free space {free} bytes is below {policy.min_free_bytes} bytes")
        if free_fraction < policy.min_free_fraction:
            reasons.append(
                f"free space {free_fraction:.2%} is below {policy.min_free_fraction:.2%}"
            )
        return "; ".join(reasons) or None

    def _retention_report(
        self,
        inventory: _RetentionInventory,
        *,
        pruned_releases: int = 0,
        pruned_bytes: int = 0,
        reason: str | None = None,
        force_within_limits: bool | None = None,
    ) -> EcmwfArchiveRetentionReport:
        policy = self.retention_policy
        if policy is None:
            raise RuntimeError("retention report requested without a policy")
        completed_bytes = sum(item.byte_size for item in inventory.archives)
        protected = tuple(item for item in inventory.archives if item.protected)
        total, _, free = self._archive_disk_usage()
        free_fraction = free / total if total else 0.0
        within_limits = (
            len(inventory.archives) <= policy.max_completed_releases
            and completed_bytes <= policy.max_completed_bytes
            and free >= policy.min_free_bytes
            and free_fraction >= policy.min_free_fraction
            and reason is None
        )
        if force_within_limits is not None:
            within_limits = force_within_limits
        return EcmwfArchiveRetentionReport(
            max_completed_releases=policy.max_completed_releases,
            max_completed_bytes=policy.max_completed_bytes,
            min_free_bytes=policy.min_free_bytes,
            min_free_fraction=policy.min_free_fraction,
            completed_releases=len(inventory.archives),
            completed_bytes=completed_bytes,
            protected_releases=len(protected),
            protected_bytes=sum(item.byte_size for item in protected),
            preserved_diagnostics=inventory.diagnostic_count,
            preserved_diagnostic_bytes=inventory.diagnostic_bytes,
            pruned_releases=pruned_releases,
            pruned_bytes=pruned_bytes,
            disk_total_bytes=total,
            disk_free_bytes=free,
            disk_free_fraction=free_fraction,
            within_limits=within_limits,
            reason=reason,
        )

    def _archive_disk_usage(self) -> tuple[int, int, int]:
        candidate = self.archive_root
        while not candidate.exists() and candidate != candidate.parent:
            candidate = candidate.parent
        total, used, free = self._disk_usage(candidate)
        return int(total), int(used), int(free)

    def _fetch_range(self, index: _StepIndex, entry: _IndexEntry) -> bytes:
        end = entry.offset + entry.length - 1
        response = self.transport.request(
            "GET",
            index.data_url,
            headers={"Range": f"bytes={entry.offset}-{end}"},
            max_bytes=self.max_grib_message_bytes,
        )
        if response.status_code != 206:
            raise EcmwfUnavailableError("range request was not honoured")
        if len(response.content) != entry.length:
            raise EcmwfValidationError("range length mismatch")
        match = _RANGE_RE.match(_header(response.headers, "content-range") or "")
        if match is None or int(match.group(1)) != entry.offset or int(match.group(2)) != end:
            raise EcmwfValidationError("range response does not match requested offsets")
        if _required_last_modified(response.headers, context="GRIB data") != index.published_at_utc:
            raise EcmwfValidationError("index and GRIB publication timestamps differ")
        return response.content

    def _find_existing_archive(
        self, init_time: datetime, indexes: tuple[_StepIndex, ...], product: EcmwfProduct
    ) -> EcmwfRawArchive | None:
        if not self.archive_root.exists():
            return None
        fingerprint = _request_fingerprint(self.source_base_url, init_time, product, indexes)
        for path in self.archive_root.iterdir():
            if not path.is_dir() or _ARCHIVE_ID_RE.fullmatch(path.name) is None:
                continue
            try:
                manifest = json.loads((path / "manifest.json").read_text())
                if manifest.get("request_fingerprint") == fingerprint:
                    return self._load_raw_archive(path)
            except (OSError, ValueError, EcmwfError):
                continue
        return None

    def _commit_archive(self, staging: Path, archive_id: str) -> Path:
        destination = self.archive_root / archive_id
        if destination.exists():
            self._load_raw_archive(destination)
            shutil.rmtree(staging)
            return destination
        _seal_archive_tree(staging)
        # Some filesystems require the staging directory itself to remain
        # writable while it is renamed.  Its children are already sealed; the
        # destination root is made read-only immediately after the atomic move.
        staging.chmod(0o700)
        try:
            staging.rename(destination)
        except FileExistsError:
            self._load_raw_archive(destination)
            shutil.rmtree(staging)
            return destination
        destination.chmod(0o555)
        # Validate the sealed tree after the atomic rename.  A chmod or rename
        # failure therefore cannot publish a writable/partial archive that a
        # later collector might mistake for immutable state.
        self._load_raw_archive(destination)
        return destination

    def _load_raw_archive(self, path: Path) -> EcmwfRawArchive:
        try:
            manifest = json.loads((path / "manifest.json").read_text())
            archive_id = str(manifest["archive_id"])
            if archive_id != path.name or _ARCHIVE_ID_RE.fullmatch(archive_id) is None:
                raise EcmwfValidationError("archive id/path mismatch")
            stable = {
                k: v
                for k, v in manifest.items()
                if k not in {"schema_version", "archive_id", "fetched_at_utc"}
            }
            if hashlib.sha256(_canonical_json(stable)).hexdigest() != archive_id:
                raise EcmwfValidationError("archive manifest digest mismatch")
            artifacts = tuple(_artifact_from_json(item) for item in manifest["artifacts"])
            _validate_archive_shape(manifest, artifacts)
            _assert_archive_immutable(
                path,
                tuple(
                    relative
                    for artifact in artifacts
                    for relative in (artifact.relative_path, artifact.index_relative_path)
                ),
            )
            for artifact in artifacts:
                _verify_file(path, artifact.relative_path, artifact.byte_size, artifact.sha256)
                _verify_file(
                    path,
                    artifact.index_relative_path,
                    artifact.index_byte_size,
                    artifact.index_sha256,
                )
            return EcmwfRawArchive(
                archive_id,
                path.resolve(),
                _parse_iso(manifest["init_time_utc"]),
                _parse_iso(manifest["published_at_utc"]),
                _parse_iso(manifest["fetched_at_utc"]),
                tuple(int(x) for x in manifest["steps"]),
                EcmwfProduct(manifest["product"]),
                artifacts,
            )
        except EcmwfError:
            raise
        except (KeyError, OSError, TypeError, ValueError, json.JSONDecodeError) as error:
            raise EcmwfValidationError("existing archive is invalid") from error

    def _verified_artifact_path(
        self, archive: EcmwfRawArchive, artifact: EcmwfArchiveArtifact
    ) -> Path:
        path = _verify_file(
            archive.archive_path, artifact.relative_path, artifact.byte_size, artifact.sha256
        )
        _verify_file(
            archive.archive_path,
            artifact.index_relative_path,
            artifact.index_byte_size,
            artifact.index_sha256,
        )
        return path

    def _batch_failure(
        self, archive: EcmwfRawArchive, checked: datetime, urls: tuple[str, ...], message: str
    ) -> EcmwfBatchFetchResult:
        status = EcmwfStatus(
            EcmwfState.UNAVAILABLE,
            archive.product,
            _parameter_for_product(archive.product),
            archive.init_time_utc,
            checked,
            archive.steps,
            archive.published_at_utc,
            0,
            f"IFS ENS extraction unavailable: {message}",
            urls,
        )
        return EcmwfBatchFetchResult(status, archive, None)

    def _index_url(self, init_time: datetime, step: int, product: EcmwfProduct) -> str:
        stamp = init_time.strftime("%Y%m%d%H%M%S")
        return (
            f"{self.source_base_url}/{init_time:%Y%m%d}/{init_time:%H}z/ifs/0p25/enfo/"
            f"{stamp}-{step}h-enfo-ef.index"
        )


def _parameter_for_product(product: EcmwfProduct) -> str:
    return product.value


def _interval_for_product(product: EcmwfProduct) -> int | None:
    return 3 if product == EcmwfProduct.DAILY_MAX_2T else None


def _raw_manifest_body(
    source_base_url: str,
    init_time: datetime,
    product: EcmwfProduct,
    artifacts: tuple[EcmwfArchiveArtifact, ...],
) -> dict[str, object]:
    return {
        "provider": "ecmwf-open-data",
        "model": "ifs",
        "resolution": "0p25",
        "class": "od",
        "stream": "enfo",
        "data_type": "pf",
        "product": product.value,
        "parameter": _parameter_for_product(product),
        "source_units": "K",
        "source_base_url": source_base_url,
        "ensemble": {
            "member_numbers": list(IFS_ENS_MEMBER_NUMBERS),
            "member_count": 50,
            "control_member_included": False,
            "single_run_substitution": False,
        },
        "init_time_utc": init_time.isoformat(),
        "published_at_utc": max(a.published_at_utc for a in artifacts).isoformat(),
        "steps": [a.step_hours for a in artifacts],
        "request_fingerprint": _request_fingerprint_from_artifacts(
            source_base_url, init_time, product, artifacts
        ),
        "artifacts": [
            {**asdict(a), "published_at_utc": a.published_at_utc.isoformat()} for a in artifacts
        ],
    }


def _request_fingerprint(
    source: str, init: datetime, product: EcmwfProduct, indexes: tuple[_StepIndex, ...]
) -> str:
    body = {
        "source": source,
        "init": init.isoformat(),
        "product": product.value,
        "indexes": [
            {
                "step": i.step_hours,
                "url": i.index_url,
                "published": i.published_at_utc.isoformat(),
                "sha256": hashlib.sha256(i.payload).hexdigest(),
            }
            for i in indexes
        ],
    }
    return hashlib.sha256(_canonical_json(body)).hexdigest()


def _request_fingerprint_from_artifacts(
    source: str, init: datetime, product: EcmwfProduct, artifacts: tuple[EcmwfArchiveArtifact, ...]
) -> str:
    body = {
        "source": source,
        "init": init.isoformat(),
        "product": product.value,
        "indexes": [
            {
                "step": a.step_hours,
                "url": a.index_source_url,
                "published": a.published_at_utc.isoformat(),
                "sha256": a.index_sha256,
            }
            for a in artifacts
        ],
    }
    return hashlib.sha256(_canonical_json(body)).hexdigest()


def _artifact_from_json(value: object) -> EcmwfArchiveArtifact:
    if not isinstance(value, dict):
        raise EcmwfValidationError("archive artifact is malformed")
    return EcmwfArchiveArtifact(
        relative_path=str(value["relative_path"]),
        index_relative_path=str(value["index_relative_path"]),
        step_hours=int(value["step_hours"]),
        parameter=str(value["parameter"]),
        interval_hours=(
            None if value.get("interval_hours") is None else int(value["interval_hours"])
        ),
        byte_size=int(value["byte_size"]),
        sha256=str(value["sha256"]),
        index_byte_size=int(value["index_byte_size"]),
        index_sha256=str(value["index_sha256"]),
        source_url=str(value["source_url"]),
        index_source_url=str(value["index_source_url"]),
        published_at_utc=_parse_iso(value["published_at_utc"]),
    )


def _parse_index(
    payload: bytes, *, init_time: datetime, step: int, product: EcmwfProduct, max_message_bytes: int
) -> tuple[str, int | None, tuple[_IndexEntry, ...]]:
    parameter = _parameter_for_product(product)
    interval = _interval_for_product(product)
    try:
        lines = payload.decode().splitlines()
    except UnicodeDecodeError as error:
        raise EcmwfValidationError("index is not UTF-8") from error
    entries: dict[int, _IndexEntry] = {}
    saw_single = False
    for number, line in enumerate(lines, 1):
        if not line.strip():
            continue
        try:
            item = json.loads(line)
        except json.JSONDecodeError as error:
            raise EcmwfValidationError(f"malformed index line {number}") from error
        if item.get("param") != parameter:
            continue
        if item.get("type") in {"fc", "cf"}:
            saw_single = True
            continue
        if item.get("type") != "pf":
            continue
        if (
            item.get("stream") != "enfo"
            or item.get("class") != "od"
            or item.get("levtype") != "sfc"
        ):
            raise EcmwfValidationError("index field is not class=od stream=enfo levtype=sfc pf")
        try:
            item_step = int(item["step"])
            member = int(item["number"])
            offset = int(item["_offset"])
            length = int(item["_length"])
        except (KeyError, TypeError, ValueError) as error:
            raise EcmwfValidationError("index entry lacks step/member/range") from error
        if (
            str(item.get("date")) != init_time.strftime("%Y%m%d")
            or str(item.get("time")).zfill(4) != init_time.strftime("%H%M")
            or item_step != step
        ):
            raise EcmwfValidationError("index metadata does not match requested run")
        if member not in IFS_ENS_MEMBER_NUMBERS:
            continue
        if member in entries:
            raise EcmwfValidationError(f"duplicate member {member}")
        if offset < 0 or length <= 0 or length > max_message_bytes:
            raise EcmwfValidationError(f"unsafe range for member {member}")
        entries[member] = _IndexEntry(member, offset, length)
    missing = sorted(set(IFS_ENS_MEMBER_NUMBERS).difference(entries))
    if missing:
        if saw_single and not entries:
            raise EcmwfValidationError("index contains single-run/control data, not ENS members")
        raise EcmwfPendingError(f"step {step} is missing {len(missing)} perturbed members")
    return parameter, interval, tuple(entries[m] for m in IFS_ENS_MEMBER_NUMBERS)


def _build_scenarios(
    init: datetime,
    steps: tuple[int, ...],
    values: Mapping[int, Mapping[int, float]],
    product: EcmwfProduct,
) -> tuple[EcmwfMemberScenario, ...]:
    if set(values) != set(steps):
        raise EcmwfValidationError("decoded step set is incomplete")
    interval = _interval_for_product(product)
    return tuple(
        EcmwfMemberScenario(
            m,
            tuple(
                EcmwfTemperaturePoint(
                    step,
                    init + timedelta(hours=step),
                    float(values[step][m]),
                    init + timedelta(hours=step - interval) if interval else None,
                    init + timedelta(hours=step) if interval else None,
                )
                for step in steps
            ),
        )
        for m in IFS_ENS_MEMBER_NUMBERS
    )


def daily_max_steps(
    *,
    init_time_utc: datetime,
    day_start_utc: datetime,
    day_end_utc: datetime,
) -> tuple[int, ...]:
    """Return mx2t3 end steps that exactly tile ``[start, end]``.

    ECMWF ``mx2t3`` at step N is the maximum over the previous three hours,
    i.e. ``(N-3h, N]``. The strict alignment check prevents an event-local
    calendar day from accidentally including an adjacent day's temperatures.
    """

    init_time = _as_utc(init_time_utc, "init_time_utc")
    start = _as_utc(day_start_utc, "day_start_utc")
    end = _as_utc(day_end_utc, "day_end_utc")
    if end <= start:
        raise ValueError("day_end_utc must be after day_start_utc")
    start_hours = (start - init_time).total_seconds() / 3600
    end_hours = (end - init_time).total_seconds() / 3600
    if not start_hours.is_integer() or not end_hours.is_integer():
        raise ValueError("daily maximum boundaries must fall on whole forecast hours")
    first_end = int(start_hours) + 3
    final_end = int(end_hours)
    if int(start_hours) % 3 or final_end % 3:
        raise ValueError("daily maximum boundaries do not align with ECMWF mx2t3 windows")
    steps = tuple(range(first_end, final_end + 1, 3))
    _validate_request(init_time, steps, EcmwfProduct.DAILY_MAX_2T)
    return steps


def daily_maxima(
    forecast: EcmwfPointForecast,
    *,
    day_start_utc: datetime,
    day_end_utc: datetime,
) -> tuple[EcmwfDailyMaximum, ...]:
    """Reduce exact contiguous mx2t3 windows to one maximum per member."""

    start = _as_utc(day_start_utc, "day_start_utc")
    end = _as_utc(day_end_utc, "day_end_utc")
    result: list[EcmwfDailyMaximum] = []
    for scenario in forecast.scenarios:
        intervals = sorted(
            (point for point in scenario.points if point.interval_start_utc is not None),
            key=lambda point: point.interval_start_utc or start,
        )
        if not intervals or intervals[0].interval_start_utc != start:
            raise EcmwfValidationError("mx2t3 scenarios do not start at day boundary")
        cursor = start
        for point in intervals:
            if point.interval_start_utc != cursor or point.interval_end_utc is None:
                raise EcmwfValidationError("mx2t3 scenario intervals are not contiguous")
            cursor = point.interval_end_utc
        if cursor != end:
            raise EcmwfValidationError("mx2t3 scenarios do not end at day boundary")
        result.append(
            EcmwfDailyMaximum(
                member_number=scenario.member_number,
                temperature_c=max(point.temperature_c for point in intervals),
            )
        )
    return tuple(result)


def _validate_request(
    init_time_utc: datetime, steps: Sequence[int], product: EcmwfProduct
) -> tuple[datetime, tuple[int, ...]]:
    init = _as_utc(init_time_utc, "init_time_utc")
    if init.hour not in {0, 6, 12, 18} or init.minute or init.second or init.microsecond:
        raise ValueError("IFS init must be a 00/06/12/18 UTC cycle")
    normalized = tuple(sorted(set(int(step) for step in steps)))
    if not normalized:
        raise ValueError("at least one step is required")
    if any(step < 0 or step > 360 for step in normalized):
        raise ValueError("steps must be between 0 and 360 hours")
    if product == EcmwfProduct.DAILY_MAX_2T:
        if init.hour not in {0, 6, 12, 18} or any(
            step < 3 or step > 144 or step % 3 for step in normalized
        ):
            raise ValueError("mx2t3 daily-max steps must be 3..144 by 3 hours")
    elif init.hour in {6, 18}:
        if any(step > 144 or step % 3 for step in normalized):
            raise ValueError("06/18 IFS ENS 2t steps must be 0..144 by 3 hours")
    elif any(
        (step <= 144 and step % 3) or (step > 144 and (step < 150 or step % 6))
        for step in normalized
    ):
        raise ValueError("00/12 IFS ENS 2t steps use 3-hour steps through 144, then 6-hour steps")
    return init, normalized


def _validate_points(points: Mapping[str, tuple[float, float]]) -> dict[str, tuple[float, float]]:
    if not points:
        raise ValueError("at least one point is required")
    result: dict[str, tuple[float, float]] = {}
    for raw_id, coordinates in points.items():
        point_id = str(raw_id).strip()
        if not point_id or len(coordinates) != 2:
            raise ValueError("point ids must be non-empty and have lat/lon")
        lat, lon = float(coordinates[0]), float(coordinates[1])
        _validate_coordinates(lat, lon)
        result[point_id] = (lat, lon)
    return result


def _validate_coordinates(latitude: float, longitude: float) -> None:
    if not math.isfinite(latitude) or not -90 <= latitude <= 90:
        raise ValueError("latitude must be finite and between -90 and 90")
    if not math.isfinite(longitude) or not -180 <= longitude <= 180:
        raise ValueError("longitude must be finite and between -180 and 180")


def _validate_member_values(values: Mapping[int, float], expected: Sequence[int]) -> None:
    if set(values) != set(expected):
        raise EcmwfValidationError("decoded member set is incomplete")
    for value in values.values():
        if not math.isfinite(float(value)) or not -150 <= float(value) <= 100:
            raise EcmwfValidationError("decoded 2t value is outside safety bounds")


def _required_last_modified(headers: Mapping[str, str], *, context: str) -> datetime:
    raw = _header(headers, "last-modified")
    if not raw:
        raise EcmwfValidationError(f"{context} lacks Last-Modified")
    try:
        return _as_utc(parsedate_to_datetime(raw), f"{context} Last-Modified")
    except (TypeError, ValueError) as error:
        raise EcmwfValidationError(f"{context} Last-Modified is invalid") from error


def _header(headers: Mapping[str, str], name: str) -> str | None:
    lower = name.casefold()
    return next((value for key, value in headers.items() if key.casefold() == lower), None)


def _as_utc(value: datetime, field: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field} must be timezone-aware")
    return value.astimezone(UTC)


def _parse_iso(value: object) -> datetime:
    return _as_utc(datetime.fromisoformat(str(value)), "archive timestamp")


def _canonical_json(value: object) -> bytes:
    return json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False
    ).encode()


def _write_new_file(path: Path, body: bytes) -> None:
    with path.open("xb") as target:
        target.write(body)
        target.flush()
        os.fsync(target.fileno())


def _verify_file(root: Path, relative: str, size: int, digest: str) -> Path:
    path = (root / relative).resolve()
    if not path.is_relative_to(root.resolve()):
        raise EcmwfValidationError("archive path escapes root")
    body = path.read_bytes()
    if len(body) != size or hashlib.sha256(body).hexdigest() != digest:
        raise EcmwfValidationError("archive digest/size check failed")
    return path


def _validate_archive_shape(
    manifest: Mapping[str, object], artifacts: Sequence[EcmwfArchiveArtifact]
) -> None:
    """Validate immutable archive structure before it can be reused or pruned."""

    try:
        product = EcmwfProduct(str(manifest["product"]))
        raw_steps = manifest["steps"]
        if not isinstance(raw_steps, (list, tuple)):
            raise TypeError("steps must be a list")
        steps = tuple(int(value) for value in raw_steps)
        init_time = _parse_iso(manifest["init_time_utc"])
        ensemble = manifest["ensemble"]
    except (KeyError, TypeError, ValueError) as error:
        raise EcmwfValidationError("archive manifest shape is malformed") from error
    if not steps or tuple(sorted(set(steps))) != steps:
        raise EcmwfValidationError("archive manifest steps are empty or not strictly ordered")
    try:
        _validate_request(init_time, steps, product)
    except (TypeError, ValueError) as error:
        raise EcmwfValidationError("archive manifest steps violate the product contract") from error
    if len(artifacts) != len(steps):
        raise EcmwfValidationError("archive manifest artifact/step coverage is incomplete")
    expected_parameter = _parameter_for_product(product)
    expected_metadata = {
        "provider": "ecmwf-open-data",
        "model": "ifs",
        "resolution": "0p25",
        "class": "od",
        "stream": "enfo",
        "data_type": "pf",
        "parameter": expected_parameter,
        "source_units": "K",
    }
    if any(manifest.get(key) != value for key, value in expected_metadata.items()):
        raise EcmwfValidationError("archive manifest provenance metadata is invalid")
    source_base = str(manifest.get("source_base_url", "")).rstrip("/")
    if not source_base.startswith("https://"):
        raise EcmwfValidationError("archive manifest source must use HTTPS")
    if not isinstance(ensemble, Mapping):
        raise EcmwfValidationError("archive manifest ensemble metadata is malformed")
    if (
        ensemble.get("member_count") != len(IFS_ENS_MEMBER_NUMBERS)
        or ensemble.get("member_numbers") != list(IFS_ENS_MEMBER_NUMBERS)
        or ensemble.get("control_member_included") is not False
        or ensemble.get("single_run_substitution") is not False
    ):
        raise EcmwfValidationError("archive manifest ensemble coverage is incomplete")
    seen_steps: set[int] = set()
    seen_paths: set[str] = set()
    expected_interval = _interval_for_product(product)
    for artifact in artifacts:
        if artifact.step_hours in seen_steps or artifact.step_hours not in steps:
            raise EcmwfValidationError("archive manifest contains duplicate/unexpected steps")
        seen_steps.add(artifact.step_hours)
        expected_relative = f"step-{artifact.step_hours:03d}-{artifact.parameter}.grib2"
        expected_index_relative = f"step-{artifact.step_hours:03d}-{artifact.parameter}.index"
        stamp = init_time.strftime("%Y%m%d%H%M%S")
        expected_index_url = (
            f"{source_base}/{init_time:%Y%m%d}/{init_time:%H}z/ifs/0p25/enfo/"
            f"{stamp}-{artifact.step_hours}h-enfo-ef.index"
        )
        expected_data_url = expected_index_url.removesuffix(".index") + ".grib2"
        if (
            artifact.parameter != expected_parameter
            or artifact.interval_hours != expected_interval
            or artifact.relative_path != expected_relative
            or artifact.index_relative_path != expected_index_relative
            or artifact.index_source_url != expected_index_url
            or artifact.source_url != expected_data_url
            or artifact.byte_size <= 0
            or artifact.index_byte_size <= 0
        ):
            raise EcmwfValidationError("archive artifact metadata does not match the product")
        for relative in (artifact.relative_path, artifact.index_relative_path):
            if relative in seen_paths:
                raise EcmwfValidationError("archive manifest contains duplicate artifact paths")
            seen_paths.add(relative)
    if seen_steps != set(steps):
        raise EcmwfValidationError("archive manifest does not cover every requested step")


def _assert_archive_immutable(path: Path, artifact_paths: Sequence[str]) -> None:
    try:
        root_stat = path.lstat()
    except OSError as error:
        raise EcmwfValidationError("archive root is not readable") from error
    if (
        not stat.S_ISDIR(root_stat.st_mode)
        or stat.S_ISLNK(root_stat.st_mode)
        or root_stat.st_mode & 0o222
    ):
        raise EcmwfValidationError("archive root is not immutable")
    for relative in ("manifest.json", *artifact_paths):
        candidate = _safe_archive_child(path, relative)
        try:
            item_stat = candidate.lstat()
        except OSError as error:
            raise EcmwfValidationError("archive immutable file is missing") from error
        if (
            not stat.S_ISREG(item_stat.st_mode)
            or stat.S_ISLNK(item_stat.st_mode)
            or item_stat.st_mode & 0o222
        ):
            raise EcmwfValidationError("archive contains a writable or linked artifact")


def _seal_archive_tree(path: Path) -> None:
    """Make every published archive file/dir read-only before rename."""

    for current, directories, files in os.walk(path, topdown=False, followlinks=False):
        current_path = Path(current)
        for name in files:
            candidate = current_path / name
            item_stat = candidate.lstat()
            if stat.S_ISLNK(item_stat.st_mode) or not stat.S_ISREG(item_stat.st_mode):
                raise EcmwfValidationError("archive staging tree contains a non-regular file")
            candidate.chmod(0o444)
        for name in directories:
            candidate = current_path / name
            item_stat = candidate.lstat()
            if stat.S_ISLNK(item_stat.st_mode) or not stat.S_ISDIR(item_stat.st_mode):
                raise EcmwfValidationError("archive staging tree contains a linked directory")
            candidate.chmod(0o555)
    path.chmod(0o555)


def _safe_archive_child(root: Path, relative: str) -> Path:
    candidate = Path(relative)
    if candidate.is_absolute() or not candidate.parts or ".." in candidate.parts:
        raise EcmwfValidationError("archive artifact path is unsafe")
    path = root.joinpath(*candidate.parts)
    if not path.resolve().is_relative_to(root.resolve()):
        raise EcmwfValidationError("archive artifact path escapes root")
    return path


def _tree_size(path: Path) -> int:
    """Return apparent bytes without following links or reading file bodies."""

    try:
        if path.is_symlink() or not path.is_dir():
            return path.lstat().st_size
        total = 0
        for root, directories, files in os.walk(path, followlinks=False):
            root_path = Path(root)
            for name in (*directories, *files):
                candidate = root_path / name
                try:
                    total += candidate.lstat().st_size
                except FileNotFoundError:
                    continue
        return total
    except FileNotFoundError:
        return 0


def _tree_relative_entries(path: Path) -> set[str]:
    entries: set[str] = set()
    for root, directories, files in os.walk(path, followlinks=False):
        root_path = Path(root)
        for name in (*directories, *files):
            entries.add(str((root_path / name).relative_to(path)))
    return entries


def _remove_archive_tree(path: Path, *, archive_root: Path) -> None:
    """Delete one verified archive without following links outside its root."""

    root = archive_root.resolve()
    candidate = path.resolve()
    if (
        candidate.parent != root
        or _ARCHIVE_ID_RE.fullmatch(candidate.name) is None
        or path.is_symlink()
    ):
        raise OSError("refusing to delete an unsafe archive path")
    for current, directories, _ in os.walk(candidate, topdown=False, followlinks=False):
        for directory in directories:
            child = Path(current) / directory
            if not child.is_symlink():
                child.chmod(0o700)
    candidate.chmod(0o700)
    shutil.rmtree(candidate)


__all__ = [
    "ECMWF_OPEN_DATA_URL",
    "IFS_ENS_MEMBER_NUMBERS",
    "EcCodesPointDecoder",
    "EcmwfArchiveResult",
    "EcmwfArchiveRetentionPolicy",
    "EcmwfArchiveRetentionReport",
    "EcmwfBatchFetchResult",
    "EcmwfBatchSnapshot",
    "EcmwfDailyMaximum",
    "EcmwfFetchResult",
    "EcmwfIfsEnsAdapter",
    "EcmwfMemberScenario",
    "EcmwfPointForecast",
    "EcmwfProduct",
    "EcmwfRawArchive",
    "EcmwfSnapshot",
    "EcmwfState",
    "EcmwfStatus",
    "EcmwfTemperaturePoint",
    "EcmwfTransport",
    "HttpResponse",
    "HttpxEcmwfTransport",
    "daily_max_steps",
    "daily_maxima",
]
