from __future__ import annotations

import hashlib
import hmac
import json
import re
from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from typing import Literal, cast
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from pydantic import Field, field_validator, model_validator

from polybot.models import StrictModel
from polybot.weathernext import WeatherNextGcsClient

_DEFAULT_MANIFEST_PATH = Path("/var/lib/polybot/weathernext/full/read-manifest.json")
_DEFAULT_APPROVAL_PATH = Path("/var/lib/polybot/weathernext/full/read-approval.json")
_DEFAULT_SNAPSHOT_ROOT = Path("/var/lib/polybot/weathernext/full/snapshots")
_READ_ONLY_SCOPE = "https://www.googleapis.com/auth/devstorage.read_only"
_OFFICIAL_SOURCE_PREFIX = "gs://weathernext3_spatial/"
_RELEASE_RE = re.compile(r"/(weathernext_[^/]+)/zarr/")
_SAFE_COMPONENT_RE = re.compile(r"[^A-Za-z0-9._-]+")


class WeatherNextCompressedObject(StrictModel):
    """One forecast chunk that a later, separately approved reader may fetch."""

    sequence: int = Field(ge=1)
    object_uri: str
    chunk_coordinates: list[int]
    compressed_bytes: int | None = Field(default=None, ge=1)
    uncompressed_upper_bound_bytes: int = Field(ge=0)
    target_ids: list[str] = Field(min_length=1)
    generation: str | None = None
    etag: str | None = None
    md5_hash: str | None = None
    crc32c: str | None = None

    @field_validator("object_uri")
    @classmethod
    def _official_object(cls, value: str) -> str:
        if not value.startswith(_OFFICIAL_SOURCE_PREFIX):
            raise ValueError("WeatherNext manifest objects must use the official bucket")
        return value


class WeatherNextApprovalGate(StrictModel):
    """Fail-closed limits bound to a metadata-only read manifest."""

    state: Literal[
        "awaiting_operator_approval",
        "blocked_incomplete_coverage",
        "blocked_incomplete_metadata",
        "blocked_sharding_unsupported",
        "blocked_network_limit",
        "blocked_object_limit",
        "blocked_object_size_limit",
    ]
    approval_required: Literal[True] = True
    approved: Literal[False] = False
    payload_read_permitted: Literal[False] = False
    payload_read: Literal[False] = False
    sequential_block_read: Literal[True] = True
    max_inflight_objects: Literal[1] = 1
    expected_network_bytes: int = Field(ge=0)
    max_network_bytes: int = Field(ge=1)
    object_count: int = Field(ge=0)
    max_objects: int = Field(ge=1)
    largest_compressed_object_bytes: int = Field(ge=0)
    max_object_bytes: int = Field(ge=1)
    compressed_sizes_complete: bool
    sharding_supported: bool
    within_network_limit: bool
    within_object_limit: bool
    within_object_size_limit: bool
    coverage_complete: bool
    incomplete_target_ids: list[str] = Field(default_factory=list)
    coverage_basis: Literal["exact_hourly_station_local_day"] = (
        "exact_hourly_station_local_day"
    )
    approval_binding: Literal["manifest_sha256"] = "manifest_sha256"

    @model_validator(mode="after")
    def _gate_stays_closed(self) -> WeatherNextApprovalGate:
        if self.state == "awaiting_operator_approval" and not (
            self.compressed_sizes_complete
            and self.sharding_supported
            and self.within_network_limit
            and self.within_object_limit
            and self.within_object_size_limit
            and self.coverage_complete
        ):
            raise ValueError("an approval-ready manifest must satisfy every metadata limit")
        if self.coverage_complete and self.incomplete_target_ids:
            raise ValueError("complete coverage cannot list incomplete targets")
        if not self.coverage_complete and not self.incomplete_target_ids:
            raise ValueError("incomplete coverage must identify at least one target")
        return self


class WeatherNextFullReadManifest(StrictModel):
    """Immutable metadata plan; this artifact can never authorize a payload read."""

    schema_version: Literal["weathernext-full-ensemble-read-manifest/v1"] = (
        "weathernext-full-ensemble-read-manifest/v1"
    )
    manifest_sha256: str
    generated_at_utc: datetime
    metadata_only: Literal[True] = True
    payload_read: Literal[False] = False
    source_uri: str
    release_id: str
    init_time_utc: datetime
    variable: Literal["station_head_temperature_2m"] = "station_head_temperature_2m"
    units: str
    station: dict[str, object]
    period: dict[str, object]
    array: dict[str, object]
    targets: list[dict[str, object]] = Field(min_length=1)
    compressed_objects: list[WeatherNextCompressedObject]
    approval_gate: WeatherNextApprovalGate
    authorization: dict[str, object]
    execution: dict[str, object]
    snapshot_target_path: str
    snapshot_target_paths: list[str] = Field(min_length=1)
    approval_sidecar_path: str

    @field_validator("manifest_sha256")
    @classmethod
    def _valid_hash(cls, value: str) -> str:
        if not re.fullmatch(r"[0-9a-f]{64}", value):
            raise ValueError("manifest_sha256 must be a lowercase SHA-256 digest")
        return value

    @field_validator("generated_at_utc", "init_time_utc")
    @classmethod
    def _aware_utc(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("manifest timestamps must be timezone-aware")
        return value.astimezone(UTC)

    @field_validator("source_uri")
    @classmethod
    def _official_source(cls, value: str) -> str:
        if not value.startswith(_OFFICIAL_SOURCE_PREFIX):
            raise ValueError("manifest source must use the official full-ensemble bucket")
        return value


class WeatherNextReadApproval(StrictModel):
    """Separate operator approval, bound exactly to one manifest digest."""

    schema_version: Literal["weathernext-full-ensemble-read-approval/v1"] = (
        "weathernext-full-ensemble-read-approval/v1"
    )
    manifest_sha256: str
    approved: Literal[True] = True
    approved_at_utc: datetime
    approved_by: str = Field(min_length=1, max_length=200)
    max_network_bytes: int = Field(ge=1)
    max_objects: int = Field(ge=1)
    max_object_bytes: int = Field(ge=1)
    expires_at_utc: datetime | None = None

    @field_validator("manifest_sha256")
    @classmethod
    def _approval_hash(cls, value: str) -> str:
        if not re.fullmatch(r"[0-9a-f]{64}", value):
            raise ValueError("approval manifest_sha256 must be a lowercase SHA-256 digest")
        return value

    @field_validator("approved_at_utc", "expires_at_utc")
    @classmethod
    def _approval_time(cls, value: datetime | None) -> datetime | None:
        if value is None:
            return None
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("approval timestamps must be timezone-aware")
        return value.astimezone(UTC)

    @model_validator(mode="after")
    def _expiration_after_approval(self) -> WeatherNextReadApproval:
        if self.expires_at_utc is not None and self.expires_at_utc <= self.approved_at_utc:
            raise ValueError("approval expiry must follow approval time")
        return self


def _mapping(value: object, *, field: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise ValueError(f"WeatherNext estimate field {field!r} must be an object")
    return cast(Mapping[str, object], value)


def _integer(value: object, *, field: str, minimum: int = 0) -> int:
    if isinstance(value, bool):
        raise ValueError(f"WeatherNext estimate field {field!r} must be an integer")
    try:
        parsed = int(cast(int | float | str, value))
    except (TypeError, ValueError, OverflowError) as error:
        raise ValueError(f"WeatherNext estimate field {field!r} must be an integer") from error
    if parsed < minimum:
        raise ValueError(f"WeatherNext estimate field {field!r} must be >= {minimum}")
    return parsed


def _integer_list(value: object, *, field: str) -> list[int]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        raise ValueError(f"WeatherNext estimate field {field!r} must be an array")
    return [_integer(item, field=field) for item in value]


def _parse_utc(value: object, *, field: str) -> datetime:
    if not isinstance(value, str):
        raise ValueError(f"WeatherNext estimate field {field!r} must be an ISO timestamp")
    normalized = value[:-1] + "+00:00" if value.endswith("Z") else value
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError(f"WeatherNext estimate field {field!r} must be timezone-aware")
    return parsed.astimezone(UTC)


def expected_station_local_day_hours(
    observation_date: date,
    timezone_name: str,
) -> list[datetime]:
    """Return the exact UTC hourly instants in one station-local calendar day.

    The UTC span is used instead of incrementing a timezone-aware local clock so
    DST transition days are handled correctly: a spring-forward day has 23
    values, a normal day 24, and a fall-back day 25.  This is a coverage
    contract, not a request to synthesize any missing values.
    """

    cleaned = timezone_name.strip()
    try:
        timezone = ZoneInfo(cleaned)
    except ZoneInfoNotFoundError as error:
        raise ValueError(f"unknown observation timezone: {cleaned}") from error
    local_start = datetime.combine(observation_date, time.min, tzinfo=timezone)
    local_end = datetime.combine(observation_date + timedelta(days=1), time.min, tzinfo=timezone)
    start_utc = local_start.astimezone(UTC)
    end_utc = local_end.astimezone(UTC)
    if end_utc <= start_utc:
        raise ValueError("station-local day has a non-positive UTC duration")
    expected: list[datetime] = []
    current = start_utc
    while current < end_utc:
        expected.append(current)
        current += timedelta(hours=1)
    if len(expected) not in {23, 24, 25}:
        raise ValueError(
            "station-local day must contain 23, 24, or 25 UTC hourly instants; "
            f"got {len(expected)} for {observation_date.isoformat()} {cleaned}"
        )
    return expected


def assess_station_local_day_coverage(
    observation_date: date,
    timezone_name: str,
    valid_times: Sequence[object],
) -> tuple[bool, str]:
    """Check exact hourly coverage without filling or inferring any values.

    The returned reason is intentionally suitable for an operator-facing
    manifest/status message.  A target is complete only when its UTC timestamps
    exactly equal the station-local day's expected hourly instants, including
    DST 23/24/25-hour days.
    """

    try:
        actual = [_parse_utc(value, field="valid_time_utc") for value in valid_times]
    except ValueError as error:
        return False, str(error)
    expected = expected_station_local_day_hours(observation_date, timezone_name)
    if actual != sorted(set(actual)):
        return False, "valid times must be sorted and unique"
    if actual == expected:
        return True, f"complete {len(expected)}-hour station-local day"
    expected_set = set(expected)
    actual_set = set(actual)
    missing = sorted(expected_set - actual_set)
    unexpected = sorted(actual_set - expected_set)
    missing_text = ", ".join(value.isoformat() for value in missing[:3])
    unexpected_text = ", ".join(value.isoformat() for value in unexpected[:3])
    details: list[str] = [
        f"expected {len(expected)} exact hourly UTC values, got {len(actual)}",
    ]
    if missing:
        details.append(f"missing={missing_text}{'…' if len(missing) > 3 else ''}")
    if unexpected:
        details.append(f"unexpected={unexpected_text}{'…' if len(unexpected) > 3 else ''}")
    return False, "; ".join(details)


def _safe_component(value: str) -> str:
    result = _SAFE_COMPONENT_RE.sub("-", value.strip()).strip("-.")
    if not result:
        raise ValueError("station_id must contain a filesystem-safe character")
    return result[:80]


def _release_id(source_uri: str) -> str:
    match = _RELEASE_RE.search(source_uri)
    if match is None:
        raise ValueError("WeatherNext source URI does not identify a release below /zarr/")
    return match.group(1)


def _chunk_object_uri(
    source_uri: str,
    variable: str,
    coordinates: Sequence[int],
    array: Mapping[str, object],
) -> str:
    separator = "/"
    encoding = array.get("chunk_key_encoding")
    if isinstance(encoding, Mapping):
        configuration = encoding.get("configuration")
        if isinstance(configuration, Mapping):
            configured = configuration.get("separator")
            if isinstance(configured, str) and configured:
                separator = configured
    encoded = separator.join(str(value) for value in coordinates)
    return f"{source_uri.rstrip('/')}/{variable}/c/{encoded}"


def _manifest_hash(payload: Mapping[str, object]) -> str:
    canonical = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ).encode("utf-8")
    return hashlib.sha256(canonical).hexdigest()


def recompute_manifest_sha256(
    manifest: WeatherNextFullReadManifest | Mapping[str, object],
) -> str:
    """Recompute the digest over a manifest with its digest field removed."""

    payload = (
        manifest.model_dump(mode="json")
        if isinstance(manifest, WeatherNextFullReadManifest)
        else dict(manifest)
    )
    payload.pop("manifest_sha256", None)
    return _manifest_hash(payload)


def verify_manifest_sha256(
    manifest: WeatherNextFullReadManifest | Mapping[str, object],
) -> bool:
    """Return true only when the manifest digest matches its canonical body."""

    if isinstance(manifest, WeatherNextFullReadManifest):
        supplied = manifest.manifest_sha256
    else:
        supplied = manifest.get("manifest_sha256")
    return isinstance(supplied, str) and hmac.compare_digest(
        supplied, recompute_manifest_sha256(manifest)
    )


def validate_read_approval(
    manifest: WeatherNextFullReadManifest,
    approval: WeatherNextReadApproval | Mapping[str, object],
    *,
    now_utc: datetime | None = None,
) -> WeatherNextReadApproval:
    """Validate, but never create, the separate payload-read authorization."""

    parsed = (
        approval
        if isinstance(approval, WeatherNextReadApproval)
        else WeatherNextReadApproval.model_validate(approval)
    )
    if manifest.approval_gate.state != "awaiting_operator_approval":
        raise ValueError("manifest is blocked and cannot be approved")
    if not verify_manifest_sha256(manifest):
        raise ValueError("manifest digest is invalid")
    if not hmac.compare_digest(parsed.manifest_sha256, manifest.manifest_sha256):
        raise ValueError("approval is bound to a different manifest")
    gate = manifest.approval_gate
    if (
        parsed.max_network_bytes != gate.max_network_bytes
        or parsed.max_objects != gate.max_objects
        or parsed.max_object_bytes != gate.max_object_bytes
    ):
        raise ValueError("approval limits must exactly match the manifest")
    now = now_utc or datetime.now(UTC)
    if now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("now_utc must be timezone-aware")
    if parsed.expires_at_utc is not None and now.astimezone(UTC) >= parsed.expires_at_utc:
        raise ValueError("WeatherNext read approval has expired")
    return parsed


def _normalise_estimates(
    estimates: Mapping[str, object] | Sequence[Mapping[str, object]],
    station_id: str | None,
) -> list[tuple[str, Mapping[str, object]]]:
    """Return deterministic target IDs and estimates without doing I/O."""

    if isinstance(estimates, Mapping):
        values: list[Mapping[str, object]] = [estimates]
    elif isinstance(estimates, Sequence) and not isinstance(estimates, (str, bytes, bytearray)):
        values = []
        for item in estimates:
            values.append(_mapping(item, field="estimates"))
    else:
        raise ValueError("estimates must be one object or an array of objects")
    if not values:
        raise ValueError("at least one WeatherNext target estimate is required")

    result: list[tuple[str, Mapping[str, object]]] = []
    seen: set[str] = set()
    for index, estimate in enumerate(values, start=1):
        candidate = (
            station_id
            if len(values) == 1 and station_id
            else estimate.get("target_id")
            or estimate.get("station_id")
            or estimate.get("location")
            or f"target-{index}"
        )
        target = _safe_component(str(candidate)).upper()
        if target in seen:
            raise ValueError(f"duplicate WeatherNext target ID: {target}")
        seen.add(target)
        result.append((target, estimate))
    return result


def _target_coordinates(estimate: Mapping[str, object], target_id: str) -> list[list[int]]:
    selection = _mapping(estimate.get("selection"), field="selection")
    raw = selection.get("selected_chunk_coordinates")
    if not isinstance(raw, Sequence) or isinstance(raw, (str, bytes, bytearray)):
        raise ValueError(f"target {target_id} has no selected chunk coordinates")
    return [_integer_list(item, field=f"target {target_id} chunk coordinates") for item in raw]


def _target_object_sizes(
    estimate: Mapping[str, object], target_id: str, count: int
) -> tuple[list[int], list[Mapping[str, object]]]:
    array = _mapping(estimate.get("array"), field=f"target {target_id} array")
    raw_sizes = array.get("selected_chunk_object_sizes", [])
    sizes = _integer_list(raw_sizes, field=f"target {target_id} compressed sizes")
    if sizes and len(sizes) != count:
        raise ValueError(f"target {target_id} compressed sizes do not align with chunks")
    raw_metadata = array.get("selected_chunk_object_metadata", [])
    records: list[Mapping[str, object]] = []
    if isinstance(raw_metadata, Sequence) and not isinstance(raw_metadata, (str, bytes, bytearray)):
        records = [
            _mapping(item, field=f"target {target_id} object metadata") for item in raw_metadata
        ]
        if records and len(records) != count:
            raise ValueError(f"target {target_id} object metadata do not align with chunks")
    return sizes, records


def _target_snapshot_path(
    *, snapshot_root: Path, init_time: datetime, station_id: str, observation_date: date
) -> str:
    return str(
        snapshot_root
        / init_time.strftime("%Y%m%dT%H%M%SZ")
        / _safe_component(station_id).upper()
        / f"{observation_date.isoformat()}.json"
    )


def build_full_ensemble_read_manifest(
    estimates: Mapping[str, object] | Sequence[Mapping[str, object]],
    *,
    station_id: str | None = None,
    billing_project: str,
    max_network_bytes: int,
    max_objects: int,
    max_object_bytes: int | None = None,
    generated_at_utc: datetime | None = None,
    snapshot_root: Path = _DEFAULT_SNAPSHOT_ROOT,
) -> WeatherNextFullReadManifest:
    """Build a multi-target, metadata-only WeatherNext read plan.

    ``estimates`` may contain one or many point/day estimates produced by the
    existing adapter.  All targets must resolve to the same release and
    ``init_time_utc``.  Chunk object URIs are deduplicated, and each object
    records every target that needs it; this is the plan for one sequential pass
    over common blocks.  No network operation occurs in this function.

    The approval gate is intentionally immutable and closed.  A complete,
    in-budget manifest is only *awaiting_operator_approval*; a separate
    approval sidecar bound to ``manifest_sha256`` is required before any
    payload GET.  Missing HEAD sizes, sharding, or limit violations leave the
    manifest blocked.
    """

    target_items = _normalise_estimates(estimates, station_id)
    if max_network_bytes < 1 or max_objects < 1:
        raise ValueError("manifest limits must be positive")
    if max_object_bytes is None:
        max_object_bytes = max_network_bytes
    if max_object_bytes < 1:
        raise ValueError("max_object_bytes must be positive")
    billing_project = billing_project.strip()
    if not billing_project:
        raise ValueError("Requester Pays billing project is required")

    _, first_estimate = target_items[0]
    if (
        first_estimate.get("metadata_only") is not True
        or first_estimate.get("payload_read") is not False
    ):
        raise ValueError("WeatherNext read manifests require metadata-only estimates")
    first_source = str(first_estimate.get("source_uri", ""))
    if not first_source.startswith(_OFFICIAL_SOURCE_PREFIX):
        raise ValueError("WeatherNext estimate must use the official full-ensemble bucket")
    release_id = _release_id(first_source)
    init_time = _parse_utc(first_estimate.get("init_time_utc"), field="init_time_utc")
    first_array = _mapping(first_estimate.get("array"), field="array")
    first_array_signature = {
        key: first_array.get(key)
        for key in (
            "dimensions",
            "shape",
            "chunk_shape",
            "dtype",
            "dtype_size_bytes",
            "codecs",
            "chunk_key_encoding",
            "sharding_detected",
            "transfer_unit",
        )
    }
    if not first_array_signature.get("dimensions"):
        first_selection = _mapping(first_estimate.get("selection"), field="selection")
        first_array_signature["dimensions"] = list(
            cast(Sequence[object], first_selection.get("dimensions", []))
        )

    object_by_uri: dict[str, dict[str, object]] = {}
    target_payloads: list[dict[str, object]] = []
    target_paths: list[str] = []
    all_sizes_complete = True
    all_sharding_supported = True
    per_target_expected: dict[str, int] = {}
    max_observed_object = 0
    observation_dates: list[date] = []
    selected_hours: list[int] = []
    coverage_complete_by_target: dict[str, bool] = {}
    coverage_reason_by_target: dict[str, str] = {}

    for target_id, estimate in target_items:
        if estimate.get("metadata_only") is not True or estimate.get("payload_read") is not False:
            raise ValueError(f"target {target_id} is not metadata-only")
        source_uri = str(estimate.get("source_uri", ""))
        if not source_uri.startswith(_OFFICIAL_SOURCE_PREFIX):
            raise ValueError(f"target {target_id} uses a non-official WeatherNext source")
        if source_uri != first_source or _release_id(source_uri) != release_id:
            raise ValueError("all WeatherNext targets must use one release")
        target_init = _parse_utc(
            estimate.get("init_time_utc"), field=f"target {target_id} init_time_utc"
        )
        if target_init != init_time:
            raise ValueError("all WeatherNext targets must use one init_time_utc/release")
        array = _mapping(estimate.get("array"), field=f"target {target_id} array")
        selection = _mapping(estimate.get("selection"), field=f"target {target_id} selection")
        signature = {key: array.get(key) for key in first_array_signature}
        if not signature.get("dimensions"):
            signature["dimensions"] = list(cast(Sequence[object], selection.get("dimensions", [])))
        if json.dumps(signature, sort_keys=True, default=str) != json.dumps(
            first_array_signature, sort_keys=True, default=str
        ):
            raise ValueError("all WeatherNext targets must share identical array metadata")
        sharding_detected = bool(array.get("sharding_detected"))
        all_sharding_supported = all_sharding_supported and not sharding_detected

        coordinates = _target_coordinates(estimate, target_id)
        sizes, records = _target_object_sizes(estimate, target_id, len(coordinates))
        sizes_complete = (
            bool(coordinates) and len(sizes) == len(coordinates) and all(size > 0 for size in sizes)
        )
        all_sizes_complete = all_sizes_complete and sizes_complete
        if sizes_complete:
            reported = _integer(
                estimate.get("expected_network_bytes", 0),
                field=f"target {target_id} expected_network_bytes",
            )
            if reported != sum(sizes):
                raise ValueError(
                    f"target {target_id} expected_network_bytes does not match compressed sizes"
                )
            per_target_expected[target_id] = reported
        else:
            # Do not let an incomplete HEAD inventory look cheap.  Retain the
            # estimator's conservative value for the blocked approval gate.
            per_target_expected[target_id] = _integer(
                estimate.get("expected_network_bytes", 0),
                field=f"target {target_id} expected_network_bytes",
            )

        selected_chunk_logical_total = _integer(
            array.get("selected_chunk_logical_bytes", 0),
            field=f"target {target_id} selected_chunk_logical_bytes",
        )
        per_object_logical = selected_chunk_logical_total // len(coordinates) if coordinates else 0
        object_uris: list[str] = []
        for index, chunk_coordinates in enumerate(coordinates):
            uri = _chunk_object_uri(
                source_uri,
                "station_head_temperature_2m",
                chunk_coordinates,
                array,
            )
            object_uris.append(uri)
            size = sizes[index] if sizes_complete else None
            record = records[index] if records else {}
            reported_uri = record.get("object_uri")
            if reported_uri is not None and str(reported_uri) != uri:
                raise ValueError(f"HEAD metadata URI mismatch for {uri}")
            current = object_by_uri.get(uri)
            if current is None:
                current = {
                    "object_uri": uri,
                    "chunk_coordinates": chunk_coordinates,
                    "compressed_bytes": size,
                    "uncompressed_upper_bound_bytes": per_object_logical,
                    "target_ids": [],
                    "generation": record.get("generation"),
                    "etag": record.get("etag"),
                    "md5_hash": record.get("md5_hash"),
                    "crc32c": record.get("crc32c"),
                }
                object_by_uri[uri] = current
            else:
                existing_size = current.get("compressed_bytes")
                if existing_size is not None and size is not None and existing_size != size:
                    raise ValueError(f"compressed size changed for shared object {uri}")
                if existing_size is None and size is not None:
                    current["compressed_bytes"] = size
                current["uncompressed_upper_bound_bytes"] = max(
                    _integer(
                        current.get("uncompressed_upper_bound_bytes", 0),
                        field="uncompressed_upper_bound_bytes",
                    ),
                    per_object_logical,
                )
                for key in ("generation", "etag", "md5_hash", "crc32c"):
                    if current.get(key) is None and record.get(key) is not None:
                        current[key] = record.get(key)
            target_ids = cast(list[str], current["target_ids"])
            if target_id not in target_ids:
                target_ids.append(target_id)
            if size is not None:
                max_observed_object = max(max_observed_object, size)

        observation_date = date.fromisoformat(str(estimate.get("observation_date", "")))
        observation_dates.append(observation_date)
        valid_times_raw = estimate.get("selected_valid_times_utc", [])
        if not isinstance(valid_times_raw, Sequence) or isinstance(
            valid_times_raw, (str, bytes, bytearray)
        ):
            raise ValueError(f"target {target_id} selected_valid_times_utc must be an array")
        valid_times = [
            _parse_utc(value, field=f"target {target_id} valid_time") for value in valid_times_raw
        ]
        if valid_times != sorted(set(valid_times)):
            raise ValueError(f"target {target_id} valid times must be sorted and unique")
        observation_timezone = str(estimate.get("observation_timezone", "UTC")).strip()
        complete_coverage, coverage_reason = assess_station_local_day_coverage(
            observation_date,
            observation_timezone,
            valid_times_raw,
        )
        coverage_complete_by_target[target_id] = complete_coverage
        coverage_reason_by_target[target_id] = coverage_reason
        selected_hours.append(len(valid_times))

        station = {
            "station_id": target_id,
            "location": str(estimate.get("location", "")).strip(),
            "requested_latitude": float(
                cast(
                    float | int | str,
                    estimate.get(
                        "latitude",
                        selection.get("requested_latitude", selection.get("nearest_latitude")),
                    ),
                )
            ),
            "requested_longitude": float(
                cast(
                    float | int | str,
                    estimate.get(
                        "longitude",
                        selection.get("requested_longitude", selection.get("nearest_longitude")),
                    ),
                )
            ),
            "nearest_latitude": float(cast(float | int | str, selection.get("nearest_latitude"))),
            "nearest_longitude": float(cast(float | int | str, selection.get("nearest_longitude"))),
            "latitude_index": _integer(
                selection.get("latitude_index"), field=f"target {target_id} latitude_index"
            ),
            "longitude_index": _integer(
                selection.get("longitude_index"), field=f"target {target_id} longitude_index"
            ),
        }
        snapshot_path = _target_snapshot_path(
            snapshot_root=snapshot_root,
            init_time=init_time,
            station_id=target_id,
            observation_date=observation_date,
        )
        target_paths.append(snapshot_path)
        target_payloads.append(
            {
                "target_id": target_id,
                **(
                    {"event_id": str(estimate["event_id"])}
                    if estimate.get("event_id") is not None
                    else {}
                ),
                **station,
                "observation_date": observation_date.isoformat(),
                "observation_timezone": observation_timezone,
                "valid_times_utc": [value.isoformat() for value in valid_times],
                "valid_hour_count": len(valid_times),
                "complete_station_local_day": complete_coverage,
                "coverage_status": "complete" if complete_coverage else "incomplete",
                "coverage_reason": coverage_reason,
                "source_uri": source_uri,
                "object_uris": object_uris,
                "expected_network_bytes": per_target_expected[target_id],
                "snapshot_path": snapshot_path,
                "selection": {
                    "dimensions": list(cast(Sequence[object], selection.get("dimensions", []))),
                    "sample_indices": list(
                        cast(Sequence[object], selection.get("sample_indices", [0, 63]))
                    ),
                    "lead_indices": list(cast(Sequence[object], selection.get("lead_indices", []))),
                    "lead_subtime_indices": list(
                        cast(
                            Sequence[object],
                            selection.get("lead_subtime_indices", []),
                        )
                    ),
                    "valid_index_tuples": [
                        (
                            dict(cast(Mapping[str, object], item))
                            if isinstance(item, Mapping)
                            else list(cast(Sequence[object], item))
                        )
                        for item in cast(
                            Sequence[object],
                            selection.get("valid_index_tuples", []),
                        )
                    ],
                    "latitude_index": station["latitude_index"],
                    "longitude_index": station["longitude_index"],
                    "selected_point_shape": list(
                        cast(
                            Sequence[object],
                            selection.get("selected_point_shape", []),
                        )
                    ),
                    "selected_valid_value_count": selection.get("selected_valid_value_count"),
                },
            }
        )

    sorted_objects: list[WeatherNextCompressedObject] = []
    for sequence, uri in enumerate(sorted(object_by_uri), start=1):
        value = object_by_uri[uri]
        sorted_objects.append(
            WeatherNextCompressedObject(
                sequence=sequence,
                object_uri=uri,
                chunk_coordinates=_integer_list(
                    value["chunk_coordinates"], field="chunk_coordinates"
                ),
                compressed_bytes=(
                    None
                    if value.get("compressed_bytes") is None
                    else _integer(
                        value.get("compressed_bytes"), field="compressed_bytes", minimum=1
                    )
                ),
                uncompressed_upper_bound_bytes=_integer(
                    value.get("uncompressed_upper_bound_bytes", 0),
                    field="uncompressed_upper_bound_bytes",
                ),
                target_ids=sorted(cast(list[str], value["target_ids"])),
                generation=(None if value.get("generation") is None else str(value["generation"])),
                etag=(None if value.get("etag") is None else str(value["etag"])),
                md5_hash=(None if value.get("md5_hash") is None else str(value["md5_hash"])),
                crc32c=(None if value.get("crc32c") is None else str(value["crc32c"])),
            )
        )

    complete_object_sizes = all_sizes_complete and all(
        item.compressed_bytes is not None for item in sorted_objects
    )
    unique_compressed_total = (
        sum(cast(int, item.compressed_bytes) for item in sorted_objects)
        if complete_object_sizes
        else 0
    )
    conservative_expected = max(
        unique_compressed_total,
        sum(per_target_expected.values()),
    )
    expected_network_bytes = (
        unique_compressed_total if complete_object_sizes else conservative_expected
    )
    object_count = len(sorted_objects)
    within_network_limit = expected_network_bytes <= max_network_bytes
    within_object_limit = object_count <= max_objects
    within_object_size_limit = max_observed_object <= max_object_bytes
    incomplete_target_ids = sorted(
        target_id for target_id, complete in coverage_complete_by_target.items() if not complete
    )
    all_coverage_complete = not incomplete_target_ids
    if not all_coverage_complete:
        state = "blocked_incomplete_coverage"
    elif not all_sharding_supported:
        state = "blocked_sharding_unsupported"
    elif not complete_object_sizes:
        state = "blocked_incomplete_metadata"
    elif not within_network_limit:
        state = "blocked_network_limit"
    elif not within_object_limit:
        state = "blocked_object_limit"
    elif not within_object_size_limit:
        state = "blocked_object_size_limit"
    else:
        state = "awaiting_operator_approval"

    generated_at = generated_at_utc or datetime.now(UTC)
    if generated_at.tzinfo is None or generated_at.utcoffset() is None:
        raise ValueError("generated_at_utc must be timezone-aware")
    generated_at = generated_at.astimezone(UTC)
    first_station = target_payloads[0]
    target_paths = sorted(target_paths)
    array_provenance = {
        **first_array_signature,
        "global_uncompressed_array_bytes": first_estimate.get("global_uncompressed_array_bytes"),
        "selected_logical_bytes": sum(
            _integer(
                _mapping(item, field="estimate").get("selected_logical_bytes", 0),
                field="selected_logical_bytes",
            )
            for _, item in target_items
        ),
        "selected_chunk_count": object_count,
        "selected_chunk_logical_bytes": sum(
            _integer(
                _mapping(_mapping(item, field="estimate").get("array"), field="array").get(
                    "selected_chunk_logical_bytes", 0
                ),
                field="selected_chunk_logical_bytes",
            )
            for _, item in target_items
        ),
        "unique_compressed_object_count": object_count,
        "compressed_object_count_with_size": sum(
            1 for item in sorted_objects if item.compressed_bytes is not None
        ),
        "expected_network_bytes": expected_network_bytes,
        "unique_compressed_bytes": unique_compressed_total,
        "per_target_expected_network_bytes": per_target_expected,
        "estimate_basis": "deduplicated_compressed_chunk_object_sizes"
        if complete_object_sizes
        else "conservative_incomplete_metadata_upper_bound",
        "global_array_is_not_mandatory_transfer": all(
            item.get("global_array_is_not_mandatory_transfer") is True for _, item in target_items
        ),
        "whole_shard_is_not_mandatory_transfer": all(
            item.get("whole_shard_is_not_mandatory_transfer") is True for _, item in target_items
        ),
    }
    gate = WeatherNextApprovalGate(
        state=cast(
            Literal[
                "awaiting_operator_approval",
                "blocked_incomplete_coverage",
                "blocked_incomplete_metadata",
                "blocked_sharding_unsupported",
                "blocked_network_limit",
                "blocked_object_limit",
                "blocked_object_size_limit",
            ],
            state,
        ),
        expected_network_bytes=expected_network_bytes,
        max_network_bytes=max_network_bytes,
        object_count=object_count,
        max_objects=max_objects,
        largest_compressed_object_bytes=max_observed_object,
        max_object_bytes=max_object_bytes,
        compressed_sizes_complete=complete_object_sizes,
        sharding_supported=all_sharding_supported,
        within_network_limit=within_network_limit,
        within_object_limit=within_object_limit,
        within_object_size_limit=within_object_size_limit,
        coverage_complete=all_coverage_complete,
        incomplete_target_ids=incomplete_target_ids,
    )
    unsigned: dict[str, object] = {
        "schema_version": "weathernext-full-ensemble-read-manifest/v1",
        "generated_at_utc": generated_at.isoformat(),
        "metadata_only": True,
        "payload_read": False,
        "source_uri": first_source,
        "release_id": release_id,
        "init_time_utc": init_time.isoformat(),
        "variable": "station_head_temperature_2m",
        "units": "K",
        "station": {
            **first_station,
            "station_count": len(target_payloads),
            "station_ids": [item["target_id"] for item in target_payloads],
        },
        "period": {
            "observation_dates": sorted({value.isoformat() for value in observation_dates}),
            "target_count": len(target_payloads),
            "valid_hour_count_min": min(selected_hours, default=0),
            "valid_hour_count_max": max(selected_hours, default=0),
            "all_targets_complete_station_local_day": all_coverage_complete,
            "coverage_basis": "exact_hourly_station_local_day",
            "incomplete_target_ids": incomplete_target_ids,
            "coverage_reasons": {
                target_id: coverage_reason_by_target[target_id]
                for target_id in incomplete_target_ids
            },
            "mixed_observation_dates": len(set(observation_dates)) > 1,
        },
        "array": array_provenance,
        "targets": target_payloads,
        "compressed_objects": [item.model_dump(mode="json") for item in sorted_objects],
        "approval_gate": gate.model_dump(mode="json"),
        "authorization": {
            "method": "application_default_credentials",
            "oauth_scope": _READ_ONLY_SCOPE,
            "requester_pays": True,
            "billing_project": billing_project,
            "secret_material_embedded": False,
        },
        "execution": {
            "strategy": "one_compressed_object_at_a_time",
            "sequential_block_read": True,
            "max_inflight_objects": 1,
            "accumulate_global_array": False,
            "global_payload_read": False,
            "deduplicate_shared_objects": True,
            "reuse_release": True,
            "verify_manifest_hash_before_read": True,
            "verify_object_size_before_decode": True,
            "stop_on_missing_object": True,
            "stop_on_size_change": True,
            "stop_before_network_limit": True,
            "require_complete_station_local_day": True,
            "coverage_basis": "exact_hourly_station_local_day",
        },
        "snapshot_target_path": target_paths[0],
        "snapshot_target_paths": target_paths,
        "approval_sidecar_path": str(_DEFAULT_APPROVAL_PATH),
    }
    # Hash the same JSON representation that Pydantic will persist (notably,
    # UTC datetimes serialize with a trailing ``Z``).  This keeps independent
    # consumers' digest verification stable across load/write cycles.
    canonical_model = WeatherNextFullReadManifest.model_validate(
        {"manifest_sha256": "0" * 64, **unsigned}
    )
    canonical_body = canonical_model.model_dump(mode="json")
    canonical_body.pop("manifest_sha256", None)
    digest = _manifest_hash(canonical_body)
    manifest = WeatherNextFullReadManifest.model_validate({"manifest_sha256": digest, **unsigned})
    return manifest


def estimate_and_build_full_ensemble_read_manifest(
    client: WeatherNextGcsClient,
    *,
    latitude: float,
    longitude: float,
    location: str,
    station_id: str,
    observation_date: date,
    timezone_name: str,
    init_time_utc: datetime | None = None,
    max_network_bytes: int | None = None,
    max_objects: int = 4096,
    max_object_bytes: int | None = None,
    generated_at_utc: datetime | None = None,
    snapshot_root: Path = _DEFAULT_SNAPSHOT_ROOT,
) -> WeatherNextFullReadManifest:
    """Run only the adapter's metadata estimate and emit a closed manifest."""

    estimate = client.estimate_point_day_read(
        latitude=latitude,
        longitude=longitude,
        location=location,
        observation_date=observation_date,
        init_time_utc=init_time_utc,
        timezone_name=timezone_name,
    )
    # Preserve the requested coordinates in addition to the nearest grid point.
    estimate = {
        **estimate,
        "latitude": latitude,
        "longitude": longitude,
    }
    configured_limit = _integer(
        estimate.get("read_limit_bytes", 0), field="read_limit_bytes", minimum=1
    )
    return build_full_ensemble_read_manifest(
        estimate,
        station_id=station_id,
        billing_project=client.billing_project,
        max_network_bytes=(configured_limit if max_network_bytes is None else max_network_bytes),
        max_objects=max_objects,
        max_object_bytes=max_object_bytes,
        generated_at_utc=generated_at_utc,
        snapshot_root=snapshot_root,
    )


def estimate_and_build_full_ensemble_read_manifest_batch(
    client: WeatherNextGcsClient,
    *,
    targets: Sequence[Mapping[str, object]],
    init_time_utc: datetime | None = None,
    include_chunk_sizes: bool = True,
    max_network_bytes: int | None = None,
    max_objects: int = 4096,
    max_object_bytes: int | None = None,
    generated_at_utc: datetime | None = None,
    snapshot_root: Path = _DEFAULT_SNAPSHOT_ROOT,
) -> WeatherNextFullReadManifest:
    """Estimate many stations/dates for one release without payload reads.

    Each target mapping must contain ``station_id``, ``latitude``,
    ``longitude``, ``location``, ``observation_date`` and ``timezone``.  The
    first target discovers a release; all following estimates are pinned to
    that exact ``init_time_utc`` so a clock tick cannot silently mix runs.
    """

    if not targets:
        raise ValueError("at least one WeatherNext target is required")
    estimates: list[dict[str, object]] = []
    if init_time_utc is not None and (
        init_time_utc.tzinfo is None or init_time_utc.utcoffset() is None
    ):
        raise ValueError("init_time_utc must be timezone-aware")
    pinned_init = None if init_time_utc is None else init_time_utc.astimezone(UTC)
    configured_limit: int | None = None
    for index, target in enumerate(targets, start=1):
        target_id = _safe_component(str(target.get("station_id", f"target-{index}"))).upper()
        try:
            latitude = float(cast(float | int | str, target["latitude"]))
            longitude = float(cast(float | int | str, target["longitude"]))
            location = str(target["location"])
            observation_date = date.fromisoformat(str(target["observation_date"]))
            timezone_name = str(target["timezone"])
        except (KeyError, TypeError, ValueError) as error:
            raise ValueError(f"invalid WeatherNext target {target_id}") from error
        estimate_kwargs = {
            "latitude": latitude,
            "longitude": longitude,
            "location": location,
            "observation_date": observation_date,
            "init_time_utc": pinned_init,
            "timezone_name": timezone_name,
        }
        if include_chunk_sizes:
            # Keep the default call shape compatible with small offline test
            # doubles and older adapters; the production client defaults to
            # exact metadata HEADs.
            estimate = client.estimate_point_day_read(**estimate_kwargs)
        else:
            try:
                estimate = client.estimate_point_day_read(
                    **estimate_kwargs,
                    include_chunk_sizes=False,
                )
            except TypeError as error:
                # A legacy adapter may not expose the optional probe flag.  It
                # is still safe to fall back to its bounded metadata estimate.
                if "include_chunk_sizes" not in str(error):
                    raise
                estimate = client.estimate_point_day_read(**estimate_kwargs)
        actual_init = _parse_utc(estimate.get("init_time_utc"), field="init_time_utc")
        if pinned_init is None:
            pinned_init = actual_init
        elif actual_init != pinned_init:
            raise ValueError("WeatherNext estimate did not resolve the pinned init_time_utc")
        if configured_limit is None:
            raw_limit = estimate.get("read_limit_bytes")
            if raw_limit is not None:
                configured_limit = _integer(raw_limit, field="read_limit_bytes", minimum=1)
        estimates.append(
            {
                **estimate,
                "target_id": target_id,
                "station_id": target_id,
                **(
                    {"event_id": str(target["event_id"])}
                    if target.get("event_id") is not None
                    else {}
                ),
                "latitude": latitude,
                "longitude": longitude,
            }
        )
    return build_full_ensemble_read_manifest(
        estimates,
        billing_project=client.billing_project,
        max_network_bytes=(configured_limit if max_network_bytes is None else max_network_bytes)
        or 0,
        max_objects=max_objects,
        max_object_bytes=max_object_bytes,
        generated_at_utc=generated_at_utc,
        snapshot_root=snapshot_root,
    )


def write_full_ensemble_read_manifest(
    manifest: WeatherNextFullReadManifest,
    path: Path = _DEFAULT_MANIFEST_PATH,
) -> Path:
    """Atomically persist a manifest without changing its approval state."""

    path = path.expanduser()
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_text(
        json.dumps(manifest.model_dump(mode="json"), ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    temporary.replace(path)
    return path
