from __future__ import annotations

import json
import math
import mmap
import re
from collections.abc import Callable, Mapping
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from typing import Any, Literal, cast
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from pydantic import Field, field_validator, model_validator

from polybot.config import Settings
from polybot.models import Bracket, RuleInterpretation, StrictModel

_WEATHERNEXT_BUCKET = "weathernext3_spatial"
_WEATHERNEXT_ROOT = "weathernext_3_0_0/zarr"
_WEATHERNEXT_STATISTICS_BUCKET = "weathernext3_statistics_spatial"
_WEATHERNEXT_STATISTICS_ROOT = "weathernext_3_0_0_statistics/zarr"
_WEATHERNEXT_STATISTICS_SURFACE = "gcs_statistics"
_WEATHERNEXT_STATISTICS_MODE = "SUMMARY_ONLY"
_STATISTICS_QUANTILES = ("mean", "p10", "p25", "p50", "p75", "p90")
_GCS_READ_SCOPE = "https://www.googleapis.com/auth/devstorage.read_only"
_RUN_RE = re.compile(r"(?P<date>\d{8})_(?P<hour>\d{2})hr_(?P<batch>\d{2})_preds(?:/|$)")
_ICAO_RE = re.compile(r"(?<![a-z])([a-z]{4})(?![a-z])", re.IGNORECASE)


def _weathernext_identity_matches(
    requested: str,
    indexed_location: str,
    indexed_station: str,
) -> bool:
    """Match a rule location to an indexed station without inventing identity.

    Market rule parsers often retain a short city label (for example,
    ``Milan``), while the frozen manifest records the canonical geocoded name
    (``Milan, Lombardy, Italy``).  Exact matching made a valid immutable
    station snapshot invisible to the observer.  We permit only normalized
    whole-phrase containment or an explicit four-letter ICAO token; a one-
    character/empty fragment can never match.
    """

    requested_key = _identity_text(requested)
    if not requested_key:
        return False
    location_key = _identity_text(indexed_location)
    station_key = _identity_text(indexed_station)
    if requested_key in (location_key, station_key):
        return True
    if len(requested_key) >= 4 and (
        requested_key in location_key or location_key in requested_key
    ):
        return True
    requested_codes = set(_ICAO_RE.findall(requested.casefold()))
    station_codes = set(_ICAO_RE.findall(indexed_station.casefold()))
    return bool(requested_codes & station_codes)


def _identity_text(value: str) -> str:
    return " ".join(re.findall(r"[a-z0-9]+", value.casefold()))


def _format_bytes(value: int) -> str:
    units = ("B", "KiB", "MiB", "GiB", "TiB")
    amount = float(max(0, value))
    for unit in units:
        if amount < 1024 or unit == units[-1]:
            return f"{amount:.1f} {unit}"
        amount /= 1024
    return f"{amount:.1f} TiB"


class WeatherNextSnapshot(StrictModel):
    """An explicitly supplied WeatherNext export; never fabricated by Polybot.

    ``trajectories`` is populated by the approval-gated full reader.  Keeping
    it optional preserves compatibility with the older compact daily-max
    snapshot while allowing the immutable archive to retain every member and
    valid hour instead of reducing the source to only ``scenario_max_c``.
    """

    source: Literal["weathernext3"] = "weathernext3"
    init_time_utc: datetime
    received_at_utc: datetime
    location: str
    observation_date: date
    observation_timezone: str = "UTC"
    scenario_max_c: list[float] = Field(min_length=64, max_length=64)
    source_uri: str
    release_id: str | None = None
    station_id: str | None = None
    latitude: float | None = None
    longitude: float | None = None
    units: Literal["C", "F"] = "C"
    valid_times_utc: list[datetime] = Field(default_factory=list)
    member_ids: list[str] = Field(default_factory=list)
    trajectories: list[dict[str, object]] = Field(default_factory=list)

    @field_validator("init_time_utc", "received_at_utc")
    @classmethod
    def _timezone_aware(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("WeatherNext timestamps must be timezone-aware")
        return value.astimezone(UTC)

    @field_validator("valid_times_utc")
    @classmethod
    def _valid_times_aware(cls, values: list[datetime]) -> list[datetime]:
        normalized = []
        for value in values:
            if value.tzinfo is None or value.utcoffset() is None:
                raise ValueError("WeatherNext valid_times_utc must be timezone-aware")
            normalized.append(value.astimezone(UTC))
        if normalized != sorted(set(normalized)):
            raise ValueError("WeatherNext valid_times_utc must be sorted and unique")
        return normalized

    @field_validator("scenario_max_c")
    @classmethod
    def _finite_scenarios(cls, values: list[float]) -> list[float]:
        if len(values) != 64:
            raise ValueError("WeatherNext snapshots require exactly 64 ensemble members")
        if not all(math.isfinite(value) for value in values):
            raise ValueError("WeatherNext scenarios must be finite numbers")
        return values

    @field_validator("observation_timezone")
    @classmethod
    def _valid_observation_timezone(cls, value: str) -> str:
        try:
            ZoneInfo(value)
        except ZoneInfoNotFoundError as error:
            raise ValueError(f"unknown observation timezone: {value}") from error
        return value

    @field_validator("latitude", "longitude")
    @classmethod
    def _finite_coordinates(cls, value: float | None) -> float | None:
        if value is not None and not math.isfinite(value):
            raise ValueError("WeatherNext coordinates must be finite")
        return value

    @model_validator(mode="after")
    def _validate_provenance(self) -> WeatherNextSnapshot:
        if self.received_at_utc < self.init_time_utc:
            raise ValueError("received_at_utc must not precede init_time_utc")
        if not self.source_uri.startswith(f"gs://{_WEATHERNEXT_BUCKET}/"):
            raise ValueError("source_uri must identify the official full-ensemble GCS bucket")
        if self.trajectories:
            if len(self.trajectories) != 64:
                raise ValueError("WeatherNext trajectory archives require exactly 64 members")
            ids: list[str] = []
            expected_hours = len(self.valid_times_utc)
            for trajectory in self.trajectories:
                member_id = str(trajectory.get("member_id", "")).strip()
                values = trajectory.get("values_c")
                if not member_id or member_id in ids:
                    raise ValueError("WeatherNext trajectory member IDs must be unique")
                if not isinstance(values, list) or len(values) != expected_hours:
                    raise ValueError(
                        "WeatherNext trajectories must contain one value for every valid hour"
                    )
                if not all(
                    isinstance(value, (int, float)) and math.isfinite(float(value))
                    for value in values
                ):
                    raise ValueError("WeatherNext trajectories must contain finite values")
                ids.append(member_id)
            if self.member_ids and self.member_ids != ids:
                raise ValueError("WeatherNext member_ids do not match trajectory member IDs")
            if self.valid_times_utc and len(set(self.valid_times_utc)) != len(self.valid_times_utc):
                raise ValueError("WeatherNext trajectory valid times must be unique")
        return self


class WeatherNextStatus(StrictModel):
    provider: Literal["weathernext3"] = "weathernext3"
    state: Literal["disabled", "access_pending", "snapshot_available", "error"]
    enabled: bool
    surface: str | None
    snapshot_path: str | None
    init_time_utc: datetime | None = None
    received_at_utc: datetime | None = None
    message: str


class WeatherNextStatisticsPoint(StrictModel):
    """One official WeatherNext statistics value at one valid hour.

    These are already-aggregated statistics published by Google.  They are
    intentionally represented as scalar fields rather than a member list so
    callers cannot mistake them for 64 synthetic scenarios or derive a daily
    probability from them.
    """

    valid_time_utc: datetime
    temperature_mean_c: float
    p10_c: float
    p25_c: float
    p50_c: float
    p75_c: float
    p90_c: float

    @field_validator("valid_time_utc")
    @classmethod
    def _point_time_aware(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("WeatherNext statistics valid_time_utc must be timezone-aware")
        return value.astimezone(UTC)

    @field_validator("temperature_mean_c", "p10_c", "p25_c", "p50_c", "p75_c", "p90_c")
    @classmethod
    def _point_finite(cls, value: float) -> float:
        if not math.isfinite(value):
            raise ValueError("WeatherNext statistics values must be finite")
        return float(value)


class WeatherNextStatisticsSnapshot(StrictModel):
    """A real Google WeatherNext 3 statistics-surface snapshot.

    ``mode=SUMMARY_ONLY`` is a hard provenance boundary: this model has no
    member/scenario field and is never accepted by the v1 probability engine.
    """

    source: Literal["weathernext3_statistics"] = "weathernext3_statistics"
    mode: Literal["SUMMARY_ONLY"] = "SUMMARY_ONLY"
    surface: Literal["gcs_statistics"] = _WEATHERNEXT_STATISTICS_SURFACE
    init_time_utc: datetime
    received_at_utc: datetime
    location: str
    station_id: str | None = None
    latitude: float
    longitude: float
    observation_date: date
    observation_timezone: str = "UTC"
    variable: Literal["temperature_2m", "station_head_temperature_2m"]
    points: list[WeatherNextStatisticsPoint] = Field(min_length=1, max_length=48)
    source_uri: str
    read_provenance: dict[str, object]

    @field_validator("init_time_utc", "received_at_utc")
    @classmethod
    def _statistics_time_aware(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("WeatherNext statistics timestamps must be timezone-aware")
        return value.astimezone(UTC)

    @field_validator("observation_timezone")
    @classmethod
    def _statistics_timezone(cls, value: str) -> str:
        try:
            ZoneInfo(value)
        except ZoneInfoNotFoundError as error:
            raise ValueError(f"unknown observation timezone: {value}") from error
        return value

    @field_validator("station_id")
    @classmethod
    def _statistics_station_id(cls, value: str | None) -> str | None:
        if value is None:
            return None
        cleaned = value.strip()
        if not cleaned:
            raise ValueError("station_id must not be empty when supplied")
        return cleaned.upper()

    @model_validator(mode="after")
    def _validate_statistics_snapshot(self) -> WeatherNextStatisticsSnapshot:
        if self.received_at_utc < self.init_time_utc:
            raise ValueError("received_at_utc must not precede init_time_utc")
        if not self.source_uri.startswith(f"gs://{_WEATHERNEXT_STATISTICS_BUCKET}/"):
            raise ValueError(
                "source_uri must identify the official WeatherNext statistics GCS bucket"
            )
        times = [point.valid_time_utc for point in self.points]
        if times != sorted(times) or len(set(times)) != len(times):
            raise ValueError("WeatherNext statistics points must be sorted and unique")
        return self


class WeatherNextStatisticsStatus(StrictModel):
    """Independent access/load status for the summary surface."""

    provider: Literal["weathernext3_statistics"] = "weathernext3_statistics"
    access_state: Literal["disabled", "pending", "granted", "error"]
    load_state: Literal["not_loaded", "available", "error"]
    enabled: bool
    surface: Literal["gcs_statistics"] = _WEATHERNEXT_STATISTICS_SURFACE
    snapshot_path: str | None
    init_time_utc: datetime | None = None
    received_at_utc: datetime | None = None
    message: str
    access_report: dict[str, object] | None = None


def _missing_modules(names: tuple[str, ...]) -> list[str]:
    from importlib.util import find_spec

    return [name for name in names if find_spec(name) is None]


def _require_gcs_stack() -> None:
    """Fail clearly when the optional Zarr-v3 reader is not installed."""

    missing = _missing_modules(("obstore", "zarr", "xarray", "numpy"))
    if missing:
        names = ", ".join(missing)
        raise RuntimeError(
            "WeatherNext GCS reading requires the optional dependency group "
            f"`uv sync --group weathernext`; missing: {names}."
        )


def _normalise_prefix(value: str | None) -> str:
    if not value:
        return ""
    prefix = value.strip().strip("/")
    if prefix.startswith("gs://"):
        prefix = prefix.removeprefix("gs://")
        prefix = prefix.split("/", 1)[1] if "/" in prefix else ""
    return prefix.strip("/")


def _parse_run_init(prefix: str) -> datetime | None:
    match = _RUN_RE.search(prefix.rstrip("/"))
    if match is None:
        return None
    try:
        return datetime.strptime(f"{match.group('date')}{match.group('hour')}", "%Y%m%d%H").replace(
            tzinfo=UTC
        )
    except ValueError:
        return None


def _as_utc_datetime(value: Any, *, field: str) -> datetime:
    """Convert xarray/numpy/Python datetime scalars without assuming local time."""

    if isinstance(value, datetime):
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError(f"{field} must be timezone-aware")
        return value.astimezone(UTC)

    # numpy.datetime64 is intentionally handled without pandas; pandas can add a
    # heavyweight optional dependency and nanosecond values otherwise become ints.
    try:
        import numpy as np

        if isinstance(value, np.datetime64):
            if np.isnat(value):
                raise ValueError(f"{field} is NaT")
            text = np.datetime_as_string(value, unit="us")
            return datetime.fromisoformat(str(text)).replace(tzinfo=UTC)
    except ImportError:
        pass

    if isinstance(value, str):
        text = value.strip()
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        try:
            parsed = datetime.fromisoformat(text)
        except ValueError as error:
            raise ValueError(f"could not parse {field}: {value!r}") from error
        if parsed.tzinfo is None or parsed.utcoffset() is None:
            raise ValueError(f"{field} must be timezone-aware")
        return parsed.astimezone(UTC)

    # xarray scalar coordinates expose ``item``; recurse once after extracting it.
    item = getattr(value, "item", None)
    if callable(item):
        extracted = item()
        if extracted is not value:
            return _as_utc_datetime(extracted, field=field)
    raise ValueError(f"could not parse {field}: {value!r}")


def _duration_seconds(value: Any, *, field: str) -> float:
    if isinstance(value, timedelta):
        return value.total_seconds()
    try:
        import numpy as np

        if isinstance(value, np.timedelta64):
            if np.isnat(value):
                raise ValueError(f"{field} contains NaT")
            return float(value / np.timedelta64(1, "s"))
    except ImportError:
        pass
    item = getattr(value, "item", None)
    if callable(item):
        extracted = item()
        if extracted is not value:
            return _duration_seconds(extracted, field=field)
    raise ValueError(f"could not parse {field} value: {value!r}")


def _observation_window_utc(
    observation_date: date, timezone_name: str | None
) -> tuple[datetime, datetime, str]:
    """Return one station-local calendar day as UTC boundaries."""

    canonical_name = timezone_name or "UTC"
    try:
        timezone = ZoneInfo(canonical_name)
    except ZoneInfoNotFoundError as error:
        raise ValueError(f"unknown observation timezone: {canonical_name}") from error
    local_start = datetime.combine(observation_date, time.min, tzinfo=timezone)
    local_end = local_start + timedelta(days=1)
    return local_start.astimezone(UTC), local_end.astimezone(UTC), canonical_name


def _coord_value(dataset: Any, name: str) -> Any:
    try:
        return dataset[name]
    except (KeyError, TypeError, AttributeError) as error:
        raise RuntimeError(f"WeatherNext dataset is missing coordinate {name!r}") from error


def _extract_point_day_ensemble(
    dataset: Any,
    *,
    latitude: float,
    longitude: float,
    observation_date: date,
    timezone_name: str | None = None,
    max_read_bytes: int | None = None,
    allow_large_read: bool = False,
) -> tuple[list[float], datetime]:
    """Extract 64 station-head daily maxima from one lazily opened Zarr dataset.

    WeatherNext 3 full-ensemble stores expose Kelvin values in
    ``station_head_temperature_2m`` with dimensions ``sample``, ``lead_time``,
    ``lead_subtime``, ``lat_0p05`` and ``lon_0p05``.  Only one nearest grid point
    is selected before values are loaded, keeping the request bounded.
    """

    if "station_head_temperature_2m" not in dataset:
        available = sorted(str(name) for name in getattr(dataset, "data_vars", {}))
        raise RuntimeError(
            "WeatherNext dataset is missing station_head_temperature_2m; "
            f"available variables: {available}"
        )
    variable = dataset["station_head_temperature_2m"]
    dims = tuple(str(name) for name in variable.dims)
    lat_name = next((name for name in ("lat_0p05", "latitude", "lat") if name in dims), None)
    lon_name = next((name for name in ("lon_0p05", "longitude", "lon") if name in dims), None)
    if lat_name is None or lon_name is None:
        raise RuntimeError(
            f"WeatherNext station-head variable has no supported lat/lon dims: {dims}"
        )

    # Raw WeatherNext longitudes are [0, 360); normalise negative user inputs.
    selected = variable.sel(
        {lat_name: latitude, lon_name: longitude % 360.0},
        method="nearest",
    )
    selected_dims = tuple(str(name) for name in selected.dims)
    if "sample" not in selected_dims:
        raise RuntimeError(
            f"WeatherNext variable has no ensemble sample dimension: {selected_dims}"
        )
    member_count = int(selected.sizes["sample"])
    if member_count < 64:
        raise RuntimeError(
            f"WeatherNext ensemble returned {member_count} members; at least 64 are required"
        )
    selected = selected.isel(sample=slice(0, 64))

    init_coord = _coord_value(dataset, "init_time")
    init_values = getattr(init_coord, "values", init_coord)
    actual_init = _as_utc_datetime(init_values, field="init_time")

    if "lead_time" not in selected.dims:
        raise RuntimeError(f"WeatherNext variable has no lead_time dimension: {selected.dims}")
    lead_values = getattr(selected["lead_time"], "values", selected["lead_time"])
    lead_seconds = [_duration_seconds(value, field="lead_time") for value in lead_values]
    sub_seconds: list[float] = []
    if "lead_subtime" in selected.dims:
        sub_values = getattr(selected["lead_subtime"], "values", selected["lead_subtime"])
        sub_seconds = [_duration_seconds(value, field="lead_subtime") for value in sub_values]
        ordered_dims = ("sample", "lead_time", "lead_subtime")
        valid_offsets = [lead + sub for lead in lead_seconds for sub in sub_seconds]
    else:
        ordered_dims = ("sample", "lead_time")
        valid_offsets = list(lead_seconds)

    extra_dims = [name for name in selected.dims if name not in ordered_dims]
    if extra_dims:
        raise RuntimeError(
            "WeatherNext station-head selection left unsupported dimensions: " + repr(extra_dims)
        )
    import numpy as np

    day_start, day_end, _ = _observation_window_utc(observation_date, timezone_name)
    valid_times = [actual_init + timedelta(seconds=offset) for offset in valid_offsets]
    mask = np.asarray([day_start <= value < day_end for value in valid_times], dtype=bool)
    if not bool(mask.any()):
        raise RuntimeError(
            f"WeatherNext run initialized at {actual_init.isoformat()} has no values "
            f"for observation date {observation_date.isoformat()}"
        )

    if len(ordered_dims) == 3:
        matrix_mask = mask.reshape(len(lead_seconds), len(sub_seconds))
        lead_indices = np.flatnonzero(matrix_mask.any(axis=1))
        sub_indices = np.flatnonzero(matrix_mask.any(axis=0))
        selected = selected.isel(lead_time=lead_indices, lead_subtime=sub_indices)
        selected_offsets = [
            lead_seconds[index] + sub_seconds[sub_index]
            for index in lead_indices
            for sub_index in sub_indices
        ]
        selected_mask = np.asarray(
            [
                day_start <= actual_init + timedelta(seconds=offset) < day_end
                for offset in selected_offsets
            ],
            dtype=bool,
        )
        selected_shape_0 = len(lead_indices)
        selected_shape_1: int | None = len(sub_indices)
    else:
        lead_indices = np.flatnonzero(mask)
        selected = selected.isel(lead_time=lead_indices)
        selected_mask = np.ones(len(lead_indices), dtype=bool)
        selected_shape_0 = len(lead_indices)
        selected_shape_1 = None

    # WeatherNext's raw station chunks include the complete global grid.  A
    # point selection therefore does not imply a small network transfer.  Use
    # the source array shape as a conservative upper bound before materializing
    # any values; callers must explicitly opt in to a larger read.
    dtype_size = int(np.dtype(getattr(variable, "dtype", np.dtype("float32"))).itemsize)
    spatial_size = 1
    for dimension in (lat_name, lon_name):
        spatial_size *= int(variable.sizes[dimension])
    estimated_bytes = (
        member_count
        * len(lead_indices)
        * (len(sub_seconds) if len(ordered_dims) == 3 else 1)
        * spatial_size
        * dtype_size
    )
    if max_read_bytes is not None and estimated_bytes > max_read_bytes and not allow_large_read:
        raise RuntimeError(
            "WeatherNext raw point read refused before downloading data: "
            f"estimated upper bound is {_format_bytes(estimated_bytes)}, limit is "
            f"{_format_bytes(max_read_bytes)}. The full-ensemble Zarr chunks include "
            "the global grid; use a precomputed statistics surface or explicitly "
            "pass --allow-large-read after approving the transfer cost."
        )

    values = selected.transpose(*ordered_dims).values
    array = np.asarray(values, dtype=float)
    if len(ordered_dims) == 2:
        expected_shape = (64, selected_shape_0)
    else:
        assert selected_shape_1 is not None
        expected_shape = (64, selected_shape_0, selected_shape_1)
    if array.shape != expected_shape:
        raise RuntimeError(
            f"Unexpected WeatherNext point shape {array.shape}; expected {expected_shape}"
        )

    flat_values = array.reshape(64, -1) if len(ordered_dims) == 3 else array

    units = str(getattr(variable, "attrs", {}).get("units", "")).strip().lower()
    if units in {"k", "kelvin"}:
        offset_c = -273.15
    elif units in {"c", "degc", "celsius", "degree_celsius", "degrees_celsius"}:
        offset_c = 0.0
    else:
        raise RuntimeError(
            "WeatherNext station_head_temperature_2m has unsupported/missing units "
            f"{units!r}; expected Kelvin (K)."
        )

    scenarios: list[float] = []
    for member_values in flat_values:
        day_values = member_values[selected_mask]
        finite = day_values[np.isfinite(day_values)]
        if finite.size == 0:
            raise RuntimeError(
                "WeatherNext ensemble member has no finite values in the requested day"
            )
        scenarios.append(float(np.max(finite) + offset_c))
    if len(scenarios) != 64 or not all(math.isfinite(value) for value in scenarios):
        raise RuntimeError("WeatherNext extraction did not produce 64 finite scenarios")
    return scenarios, actual_init


class WeatherNextGcsClient:
    """Direct WeatherNext 3 reader over ADC and the official Requester Pays bucket.

    The canonical full-ensemble path is a Zarr-v3 store.  Reads use ``obstore``
    and send ``x-goog-user-project`` with every object request.  No API key,
    proxy, or VPN is used.
    """

    def __init__(
        self,
        settings: Settings,
        *,
        storage_client: Any | None = None,
        credentials: Any | None = None,
        dataset_factory: Callable[[str], Any] | None = None,
    ) -> None:
        self.settings = settings
        self.bucket_name = settings.weathernext_gcs_bucket.strip()
        if self.bucket_name != _WEATHERNEXT_BUCKET:
            raise RuntimeError(
                f"WeatherNext full-ensemble access only permits {_WEATHERNEXT_BUCKET!r}; "
                f"got {self.bucket_name!r}."
            )
        self.billing_project = (settings.weathernext_gcs_project or "").strip()
        if not self.billing_project:
            raise RuntimeError(
                "Requester Pays requires POLYBOT_WEATHERNEXT_GCS_PROJECT "
                "(billing project ID, e.g. weather-508105)."
            )
        self.credentials = credentials
        self._dataset_factory = dataset_factory
        self._last_raw_chunk_object_metadata: list[dict[str, object]] = []
        if storage_client is None:
            missing = _missing_modules(("google.auth", "google.cloud.storage"))
            if missing:
                raise RuntimeError(
                    "WeatherNext access checks require google-cloud-storage; "
                    f"missing modules: {', '.join(missing)}"
                )
            from google.auth import default as adc_default
            from google.cloud import storage

            self.credentials, _ = adc_default(scopes=[_GCS_READ_SCOPE])
            storage_client = storage.Client(
                project=self.billing_project,
                credentials=self.credentials,
            )
        self._client = storage_client
        try:
            self._bucket = self._client.bucket(
                self.bucket_name,
                user_project=self.billing_project,
            )
        except TypeError:
            # Small fake clients used by offline tests may only accept a name.
            self._bucket = self._client.bucket(self.bucket_name)

    def check_access(self) -> dict[str, object]:
        """Check ADC/requester-pays access with bounded metadata-only requests."""

        result: dict[str, object] = {
            "billing_project": self.billing_project,
            "bucket": f"gs://{self.bucket_name}/",
            "prefix": _normalise_prefix(self.settings.weathernext_gcs_prefix) or _WEATHERNEXT_ROOT,
            "auth": "application_default_credentials",
            "access_granted": False,
            "requester_pays": None,
        }
        try:
            self._bucket.reload()
            requester_pays = getattr(self._bucket, "requester_pays", None)
            result["requester_pays"] = bool(requester_pays)
            if not requester_pays:
                result["error"] = "WeatherNext full-ensemble bucket is not marked Requester Pays"
                return result
            prefixes = self._list_common_prefixes(str(result["prefix"]))
            result["zarr_prefixes"] = list(prefixes[:100])
            try:
                latest_prefix, latest_init = self._resolve_store_prefix(
                    init_time_utc=None,
                    observation_date=datetime.now(UTC).date(),
                )
                result["latest_run_prefix"] = latest_prefix
                result["latest_run_init_time_utc"] = (
                    None if latest_init is None else latest_init.isoformat()
                )
                try:
                    dataset, latest_prefix, latest_init, fallback_errors = (
                        self._open_dataset_with_fallback(
                            store_prefix=latest_prefix,
                            discovered_init=latest_init,
                            observation_date=datetime.now(UTC).date(),
                            timezone_name=None,
                        )
                    )
                    result["latest_run_prefix"] = latest_prefix
                    result["latest_run_init_time_utc"] = (
                        None if latest_init is None else latest_init.isoformat()
                    )
                    if fallback_errors:
                        result["publication_fallbacks"] = fallback_errors
                    try:
                        variable = dataset["station_head_temperature_2m"]
                        result["schema_ok"] = True
                        result["zarr_format"] = 3
                        result["ensemble_member_count"] = int(variable.sizes.get("sample", 0))
                        result["station_variable_units"] = str(
                            getattr(variable, "attrs", {}).get("units", "")
                        )
                        result["station_variable_dimensions"] = [
                            str(item) for item in variable.dims
                        ]
                    finally:
                        close = getattr(dataset, "close", None)
                        if callable(close):
                            close()
                except Exception as error:
                    result["schema_ok"] = False
                    result["schema_error"] = str(error)
            except Exception as error:
                # Access itself is already proven by bucket reload/listing; a
                # temporarily incomplete publication should not turn that into
                # a false authentication failure.
                result["latest_run_error"] = str(error)
            result["access_granted"] = True
        except Exception as error:
            result["error"] = str(error)
        return result

    def _list_common_prefixes(self, prefix: str, *, append_slash: bool = True) -> tuple[str, ...]:
        clean = _normalise_prefix(prefix)
        request_prefix = clean + "/" if clean and append_slash else clean
        iterator = self._client.list_blobs(
            self._bucket,
            prefix=request_prefix or None,
            delimiter="/",
            max_results=1000,
        )
        names: set[str] = set()
        # Do not iterate the complete object listing.  A Zarr store has millions
        # of chunk objects; with a delimiter the first GCS page already exposes
        # the child prefixes we need, while consuming every page can hang for
        # minutes and create unnecessary Requester Pays reads.
        pages = getattr(iterator, "pages", None)
        if pages is not None:
            try:
                blobs = next(iter(pages))
            except StopIteration:
                blobs = ()
        else:
            from itertools import islice

            blobs = islice(iterator, 1000)
        for blob in blobs:
            name = str(getattr(blob, "name", blob))
            if request_prefix and name.startswith(request_prefix):
                relative = name[len(request_prefix) :]
                if "/" in relative:
                    names.add(request_prefix + relative.split("/", 1)[0] + "/")
            elif name:
                names.add(name.rstrip("/") + "/")
        for prefix_value in getattr(iterator, "prefixes", ()):
            names.add(str(prefix_value).rstrip("/") + "/")
        return tuple(sorted(names))

    def _resolve_store_prefix(
        self,
        *,
        init_time_utc: datetime | None,
        observation_date: date | None = None,
        timezone_name: str | None = None,
    ) -> tuple[str, datetime | None]:
        explicit = _normalise_prefix(self.settings.weathernext_gcs_store_prefix)
        if explicit:
            if explicit.endswith("/predictions.zarr"):
                return explicit, _parse_run_init(explicit)
            return explicit.rstrip("/") + "/predictions.zarr", _parse_run_init(explicit)

        root = _normalise_prefix(self.settings.weathernext_gcs_prefix) or _WEATHERNEXT_ROOT

        if init_time_utc:
            cutoff = init_time_utc.astimezone(UTC)
        elif observation_date is not None:
            _, day_end, _ = _observation_window_utc(observation_date, timezone_name)
            cutoff = min(datetime.now(UTC), day_end)
        else:
            cutoff = datetime.now(UTC)

        # Do not enumerate the root: GCS returns lexicographically oldest
        # prefixes first and the operational hierarchy contains a very large
        # number of hourly runs.  Construct the small set of year folders that
        # can contain the requested date, then query one date prefix at a time.
        root_leaf = root.rstrip("/").rsplit("/", 1)[-1]
        if root_leaf.endswith("_to_present") or root_leaf.isdigit():
            candidates = [root.rstrip("/")]
        else:
            year = cutoff.year
            candidates = [
                f"{root.rstrip('/')}/{year}_to_present",
                f"{root.rstrip('/')}/{year}",
            ]
            if year != 2026:
                candidates.append(f"{root.rstrip('/')}/2026_to_present")

        # GCS delimiter listings are lexicographically ordered, so consuming
        # only their first page would return the *oldest* run.  Query a narrow
        # date prefix instead (at most two dozen hourly runs) and walk backwards
        # a bounded number of days.  This avoids enumerating millions of chunks.
        for year_prefix in sorted(candidates, reverse=True):
            for days_back in range(15):
                probe_date = cutoff.date() - timedelta(days=days_back)
                run_query = f"{year_prefix.rstrip('/')}/{probe_date:%Y%m%d}_"
                run_prefixes = self._list_common_prefixes(run_query, append_slash=False)
                runs = [
                    (parsed, run_prefix.rstrip("/"))
                    for run_prefix in run_prefixes
                    if (parsed := _parse_run_init(run_prefix)) is not None
                ]
                eligible = [item for item in runs if item[0] <= cutoff]
                if eligible:
                    selected_init, selected_prefix = max(eligible, key=lambda item: item[0])
                    return selected_prefix + "/predictions.zarr", selected_init
        raise RuntimeError(
            "No WeatherNext prediction run at or before "
            f"{cutoff.isoformat()} was found below gs://{self.bucket_name}/{root}/; "
            "set POLYBOT_WEATHERNEXT_GCS_STORE_PREFIX to an explicit predictions.zarr path "
            "for older historical dates."
        )

    def _open_dataset(self, store_prefix: str) -> Any:
        if self._dataset_factory is not None:
            return self._dataset_factory(store_prefix)
        _require_gcs_stack()
        import importlib

        obstore = cast(Any, importlib.import_module("obstore"))
        xarray = cast(Any, importlib.import_module("xarray"))
        zarr = cast(Any, importlib.import_module("zarr"))

        gcs_store = obstore.store.GCSStore(
            bucket=self.bucket_name,
            prefix=store_prefix,
            client_options={
                "default_headers": {
                    "x-goog-user-project": self.billing_project,
                }
            },
        )
        zarr_store = zarr.storage.ObjectStore(gcs_store)
        # Dask is intentionally not required by the optional group.  The point
        # and time slices below are selected before ``.values`` is called, so
        # xarray's eager backend still performs a bounded read when Dask is absent.
        from importlib.util import find_spec

        chunks: dict[str, int] | None = {} if find_spec("dask") else None
        return xarray.open_zarr(zarr_store, chunks=chunks)

    def open_sequential_zarr_group(self, store_prefix: str) -> Any:
        """Open the raw Zarr-v3 group without xarray/dask materialization.

        The autonomous approved reader uses this narrow escape hatch to call
        ``Array.get_basic_selection`` for one exact chunk at a time.  Keeping
        it separate from ``_open_dataset`` avoids xarray's normal indexing
        machinery, which may gather several chunks concurrently.  Opening the
        group itself reads metadata only; no array payload is fetched here.
        """

        _require_gcs_stack()
        import importlib

        obstore = cast(Any, importlib.import_module("obstore"))
        zarr = cast(Any, importlib.import_module("zarr"))
        gcs_store = obstore.store.GCSStore(
            bucket=self.bucket_name,
            prefix=store_prefix,
            client_options={
                "default_headers": {
                    "x-goog-user-project": self.billing_project,
                }
            },
        )
        zarr_store = zarr.storage.ObjectStore(gcs_store)
        return zarr.open_group(zarr_store, mode="r")

    def open_sequential_zarr_group_from_local_object(
        self,
        store_prefix: str,
        *,
        object_key: str,
        object_path: Path,
        array_key: str,
    ) -> Any:
        """Open metadata from GCS but serve one approved chunk from disk.

        Used only by the one-block probe: the exact object is downloaded once
        with automatic retries disabled, then Zarr decoding reads the local
        mmap instead of issuing a hidden second payload request.
        """

        _require_gcs_stack()
        import importlib

        obstore = cast(Any, importlib.import_module("obstore"))
        zarr = cast(Any, importlib.import_module("zarr"))
        from zarr.abc.store import OffsetByteRequest, RangeByteRequest, SuffixByteRequest
        from zarr.core.buffer import BufferPrototype

        gcs_store = obstore.store.GCSStore(
            bucket=self.bucket_name,
            prefix=store_prefix,
            client_options={
                "default_headers": {
                    "x-goog-user-project": self.billing_project,
                }
            },
        )
        upstream = zarr.storage.ObjectStore(gcs_store)
        store_root = store_prefix.strip("/")
        approved_key = object_key.strip("/")
        if approved_key.startswith(store_root + "/"):
            approved_key = approved_key.removeprefix(store_root + "/")
        approved_path = object_path.expanduser().resolve()
        normalized_array_key = array_key.strip("/")
        allowed_metadata_keys = {
            "zarr.json",
            ".zgroup",
            ".zattrs",
            ".zmetadata",
            f"{normalized_array_key}/zarr.json",
            f"{normalized_array_key}/.zarray",
            f"{normalized_array_key}/.zattrs",
        }

        class _OneLocalObjectStore(zarr.storage.WrapperStore):
            async def get(
                self,
                key: str,
                prototype: BufferPrototype,
                byte_range: object | None = None,
            ) -> Any:
                normalized_key = key.strip("/")
                if normalized_key in allowed_metadata_keys:
                    return await self._store.get(key, prototype, byte_range)  # type: ignore[arg-type]
                if normalized_key != approved_key:
                    raise RuntimeError(
                        "one-block probe refused a non-approved Zarr key: "
                        f"{normalized_key}"
                    )
                with approved_path.open("rb") as handle:
                    mapped = mmap.mmap(handle.fileno(), 0, access=mmap.ACCESS_READ)
                    try:
                        if byte_range is None:
                            raw = mapped[:]
                        elif isinstance(byte_range, RangeByteRequest):
                            raw = mapped[byte_range.start : byte_range.end]
                        elif isinstance(byte_range, OffsetByteRequest):
                            raw = mapped[byte_range.offset :]
                        elif isinstance(byte_range, SuffixByteRequest):
                            raw = mapped[-byte_range.suffix :]
                        else:
                            # Partial-decode is not used by the current codec
                            # chain, so reject rather than silently read GCS.
                            raise RuntimeError(
                                "unsupported local WeatherNext probe byte range"
                            )
                    finally:
                        mapped.close()
                return prototype.buffer.from_bytes(raw)

        return zarr.open_group(_OneLocalObjectStore(upstream), mode="r")

    def _open_dataset_with_fallback(
        self,
        *,
        store_prefix: str,
        discovered_init: datetime | None,
        observation_date: date | None,
        timezone_name: str | None,
    ) -> tuple[Any, str, datetime | None, list[str]]:
        """Open the newest *complete* publication without reading payload data.

        Google can expose a run prefix before its root/group metadata is fully
        published.  In that case xarray raises ``No group found``.  Walk back
        through a few prior run prefixes rather than treating a transient
        publication gap as an authentication failure.  An explicit store
        prefix remains strict and is never silently changed.
        """

        current_prefix = store_prefix
        current_init = discovered_init
        errors: list[str] = []
        explicit = bool(_normalise_prefix(self.settings.weathernext_gcs_store_prefix))
        for _attempt in range(4):
            try:
                return self._open_dataset(current_prefix), current_prefix, current_init, errors
            except Exception as error:
                errors.append(f"{current_prefix}: {error}")
                if explicit or current_init is None:
                    raise
                previous_prefix, previous_init = self._resolve_store_prefix(
                    init_time_utc=current_init - timedelta(microseconds=1),
                    observation_date=observation_date,
                    timezone_name=timezone_name,
                )
                if previous_prefix == current_prefix:
                    raise RuntimeError(
                        "WeatherNext publication is incomplete and no prior run is available: "
                        + errors[-1]
                    ) from error
                current_prefix, current_init = previous_prefix, previous_init
        raise RuntimeError(
            "No complete WeatherNext prediction run could be opened after bounded fallback: "
            + " | ".join(errors)
        )

    def _zarr_metadata(self, store_prefix: str) -> dict[str, dict[str, object]]:
        """Read only the consolidated Zarr-v3 metadata for a store."""

        blob_method = getattr(self._bucket, "blob", None)
        if not callable(blob_method):
            return {}
        try:
            blob = cast(Any, blob_method(f"{store_prefix.rstrip('/')}/zarr.json"))
            raw = blob.download_as_bytes()
            payload = json.loads(raw)
            metadata = payload.get("consolidated_metadata", {}).get("metadata", {})
            if not isinstance(metadata, Mapping):
                return {}
            return {
                str(name): value
                for name, value in metadata.items()
                if isinstance(value, dict)
            }
        except Exception:
            # The estimate remains useful with xarray's encoding metadata when
            # consolidated metadata is unavailable.  Do not turn a metadata
            # diagnostic into a payload read or a hard dependency on GCS HEAD.
            return {}

    def _raw_chunk_object_size(
        self,
        store_prefix: str,
        name: str,
        chunk_coordinates: tuple[int, ...],
        meta: Mapping[str, object],
    ) -> int | None:
        """Return one compressed chunk object's size via metadata-only HEAD."""

        metadata = self._raw_chunk_object_metadata(
            store_prefix, name, chunk_coordinates, meta
        )
        return None if metadata is None else int(cast(int, metadata["size"]))

    def _raw_chunk_object_metadata(
        self,
        store_prefix: str,
        name: str,
        chunk_coordinates: tuple[int, ...],
        meta: Mapping[str, object],
    ) -> dict[str, object] | None:
        """Return one chunk's compressed-object metadata via HEAD only.

        ``reload()`` requests object metadata and never downloads the chunk
        body.  The returned generation/checksum fields let a later approved
        reader bind its payload GET to exactly the object described by the
        manifest.
        """

        blob_method = getattr(self._bucket, "blob", None)
        if not callable(blob_method):
            return None
        if bool(meta.get("sharding_detected")):
            # A sharded object is a different transfer unit; without parsing
            # the sharding index, guessing a byte range would under-report the
            # mandatory shard transfer.  Keep the logical upper bound instead.
            return None
        chunk_key_encoding = meta.get("chunk_key_encoding")
        separator = "/"
        if isinstance(chunk_key_encoding, Mapping):
            configuration = chunk_key_encoding.get("configuration")
            if isinstance(configuration, Mapping):
                value = configuration.get("separator")
                if isinstance(value, str) and value:
                    separator = value
        encoded_coordinates = separator.join(str(value) for value in chunk_coordinates)
        key = f"{store_prefix.rstrip('/')}/{name}/c/{encoded_coordinates}"
        try:
            blob = cast(Any, blob_method(key))
            blob.reload()
            size = getattr(blob, "size", None)
            if size is None:
                return None
            result: dict[str, object] = {
                "object_uri": f"gs://{self.bucket_name}/{key}",
                "size": int(size),
            }
            for field in ("generation", "etag", "md5_hash", "crc32c", "updated"):
                value = getattr(blob, field, None)
                if value is not None:
                    result[field] = str(value)
            return result
        except Exception:
            return None

    def _raw_chunk_object_metadata_list(
        self,
        store_prefix: str,
        name: str,
        chunk_coordinates: list[tuple[int, ...]],
        meta: Mapping[str, object],
    ) -> list[dict[str, object]]:
        """Resolve bounded metadata-only HEAD requests, preserving order."""

        if not chunk_coordinates or bool(meta.get("sharding_detected")):
            return []
        from concurrent.futures import ThreadPoolExecutor

        def get_metadata(coordinates: tuple[int, ...]) -> dict[str, object] | None:
            return self._raw_chunk_object_metadata(store_prefix, name, coordinates, meta)

        workers = min(16, len(chunk_coordinates))
        with ThreadPoolExecutor(max_workers=workers) as executor:
            records = list(executor.map(get_metadata, chunk_coordinates))
        if any(record is None for record in records):
            return []
        return [cast(dict[str, object], record) for record in records]

    def _raw_chunk_object_sizes(
        self,
        store_prefix: str,
        name: str,
        chunk_coordinates: list[tuple[int, ...]],
        meta: Mapping[str, object],
    ) -> list[int]:
        """Resolve bounded chunk HEAD requests concurrently, preserving order."""

        records = self._raw_chunk_object_metadata_list(
            store_prefix, name, chunk_coordinates, meta
        )
        self._last_raw_chunk_object_metadata = records
        if len(records) != len(chunk_coordinates):
            return []
        sizes = [record.get("size") for record in records]
        if any(not isinstance(size, int) or size <= 0 for size in sizes):
            return []
        return [int(cast(int, size)) for size in sizes]

    def estimate_point_day_read(
        self,
        *,
        latitude: float,
        longitude: float,
        location: str,
        observation_date: date,
        init_time_utc: datetime | None = None,
        timezone_name: str | None = None,
        include_chunk_sizes: bool = True,
    ) -> dict[str, object]:
        """Return a metadata-only estimate for a raw point/day extraction.

        This deliberately never accesses ``.values`` on the weather variable.
        It reports the logical global array, the selected point slice, the
        actual chunk coordinates, codec/sharding metadata, and (when available)
        compressed object sizes obtained through HEAD requests.  A global
        uncompressed array or a whole shard is *not* treated as mandatory
        transfer unless the metadata says that is the transfer unit.
        """

        if init_time_utc is not None and (
            init_time_utc.tzinfo is None or init_time_utc.utcoffset() is None
        ):
            raise ValueError("init_time_utc must be timezone-aware")
        store_prefix, discovered_init = self._resolve_store_prefix(
            init_time_utc=init_time_utc,
            observation_date=observation_date,
            timezone_name=timezone_name,
        )
        dataset, store_prefix, discovered_init, fallback_errors = self._open_dataset_with_fallback(
            store_prefix=store_prefix,
            discovered_init=discovered_init,
            observation_date=observation_date,
            timezone_name=timezone_name,
        )
        try:
            import numpy as np

            variable_name = "station_head_temperature_2m"
            if variable_name not in dataset:
                available = sorted(str(name) for name in getattr(dataset, "data_vars", {}))
                raise RuntimeError(
                    f"WeatherNext dataset is missing {variable_name}; "
                    f"available variables: {available}"
                )
            variable = dataset[variable_name]
            dims = tuple(str(name) for name in variable.dims)
            lat_name = next(
                (name for name in ("lat_0p05", "latitude", "lat") if name in dims), None
            )
            lon_name = next(
                (name for name in ("lon_0p05", "longitude", "lon") if name in dims), None
            )
            if lat_name is None or lon_name is None:
                raise RuntimeError(
                    f"WeatherNext station-head variable has no lat/lon dims: {dims}"
                )
            if "sample" not in dims or "lead_time" not in dims:
                raise RuntimeError(
                    f"WeatherNext station-head variable has unsupported dims: {dims}"
                )
            member_count = int(variable.sizes.get("sample", 0))
            if member_count < 64:
                raise RuntimeError(
                    f"WeatherNext ensemble returned {member_count} members; "
                    "at least 64 are required"
                )

            init_coord = _coord_value(dataset, "init_time")
            actual_init = _as_utc_datetime(
                getattr(init_coord, "values", init_coord), field="init_time"
            )
            lead_values = getattr(dataset["lead_time"], "values", dataset["lead_time"])
            lead_seconds = [_duration_seconds(value, field="lead_time") for value in lead_values]
            if "lead_subtime" in dims:
                sub_values = getattr(dataset["lead_subtime"], "values", dataset["lead_subtime"])
                sub_seconds = [
                    _duration_seconds(value, field="lead_subtime") for value in sub_values
                ]
                valid_offsets = [lead + sub for lead in lead_seconds for sub in sub_seconds]
            else:
                sub_seconds = []
                valid_offsets = list(lead_seconds)
            day_start, day_end, canonical_timezone = _observation_window_utc(
                observation_date, timezone_name
            )
            valid_times = [actual_init + timedelta(seconds=offset) for offset in valid_offsets]
            valid_mask = [day_start <= value < day_end for value in valid_times]
            if not any(valid_mask):
                raise RuntimeError(
                    f"WeatherNext run initialized at {actual_init.isoformat()} has no values "
                    f"for observation date {observation_date.isoformat()}"
                )
            selected_offsets: list[float]
            if "lead_subtime" in dims:
                matrix_mask = np.asarray(valid_mask, dtype=bool).reshape(
                    len(lead_seconds), len(sub_seconds)
                )
                lead_indices = [
                    int(index) for index in np.flatnonzero(matrix_mask.any(axis=1))
                ]
                sub_indices = [
                    int(index) for index in np.flatnonzero(matrix_mask.any(axis=0))
                ]
                selected_offsets = [
                    lead_seconds[index] + sub_seconds[sub_index]
                    for index in lead_indices
                    for sub_index in sub_indices
                ]
                selected_mask = [
                    day_start <= actual_init + timedelta(seconds=offset) < day_end
                    for offset in selected_offsets
                ]
                valid_index_tuples = [
                    {
                        "lead_time_index": int(index),
                        "lead_subtime_index": int(sub_index),
                        "valid_time_utc": (
                            actual_init
                            + timedelta(seconds=lead_seconds[index] + sub_seconds[sub_index])
                        ).isoformat(),
                    }
                    for index in lead_indices
                    for sub_index in sub_indices
                    if day_start
                    <= actual_init
                    + timedelta(seconds=lead_seconds[index] + sub_seconds[sub_index])
                    < day_end
                ]
            else:
                lead_indices = [int(index) for index, selected in enumerate(valid_mask) if selected]
                sub_indices = []
                selected_offsets = [lead_seconds[index] for index in lead_indices]
                selected_mask = [True] * len(lead_indices)
                valid_index_tuples = [
                    {
                        "lead_time_index": int(index),
                        "valid_time_utc": (
                            actual_init + timedelta(seconds=lead_seconds[index])
                        ).isoformat(),
                    }
                    for index in lead_indices
                ]

            lat_values = np.asarray(
                getattr(dataset[lat_name], "values", dataset[lat_name])
            ).reshape(-1)
            lon_values = np.asarray(
                getattr(dataset[lon_name], "values", dataset[lon_name])
            ).reshape(-1)
            if not len(lat_values) or not len(lon_values):
                raise RuntimeError("WeatherNext station-head dataset has empty lat/lon coordinates")
            target_lon = longitude % 360.0
            lat_index = int(np.abs(lat_values.astype(float) - latitude).argmin())
            lon_index = int(np.abs(lon_values.astype(float) - target_lon).argmin())

            raw_metadata = self._zarr_metadata(store_prefix)
            metadata = _statistics_array_meta(dataset, variable_name)
            if variable_name in raw_metadata:
                metadata = {
                    **metadata,
                    **_normalise_statistics_metadata(raw_metadata[variable_name]),
                }
            shape = _int_list(metadata.get("shape", []))
            chunk_shape = _int_list(metadata.get("chunk_shape", []))
            if len(shape) != len(dims):
                shape = [int(variable.sizes[dim]) for dim in dims]
            if len(chunk_shape) != len(dims) or any(value <= 0 for value in chunk_shape):
                chunk_shape = list(shape)
            dtype_size = _int_value(metadata.get("dtype_size_bytes"), default=4)
            selected_indices_by_dim: dict[str, list[int]] = {
                "sample": list(range(64)),
                "lead_time": lead_indices,
                "lat_0p05": [lat_index],
                "lat": [lat_index],
                "latitude": [lat_index],
                "lon_0p05": [lon_index],
                "lon": [lon_index],
                "longitude": [lon_index],
            }
            if "lead_subtime" in dims:
                selected_indices_by_dim["lead_subtime"] = sub_indices
            selected_indices_by_dim = {
                dim: selected_indices_by_dim.get(dim, list(range(int(variable.sizes[dim]))))
                for dim in dims
            }
            chunk_coordinates_by_dim: list[list[int]] = []
            for position, dim in enumerate(dims):
                selected = selected_indices_by_dim[dim]
                chunk_size = max(1, chunk_shape[position])
                chunk_coordinates_by_dim.append(sorted({index // chunk_size for index in selected}))
            from itertools import product

            chunk_coordinates = [
                tuple(int(value) for value in coordinates)
                for coordinates in product(*chunk_coordinates_by_dim)
            ]
            try:
                import math as _math

                global_bytes = dtype_size * _math.prod(shape)
                # Logical point bytes count only valid timestamps in the
                # requested station-local window.  Chunk bytes below may be
                # larger because a chunk can also contain neighbouring
                # sub-times that are outside the selected window.
                selected_time_count = sum(1 for selected in selected_mask if selected)
                selected_logical = dtype_size * 64 * selected_time_count
                selected_chunk_logical = dtype_size * _math.prod(chunk_shape)
            except (TypeError, ValueError):
                global_bytes = selected_logical = selected_chunk_logical = 0
                selected_time_count = 0
            self._last_raw_chunk_object_metadata = []
            if include_chunk_sizes:
                object_sizes = self._raw_chunk_object_sizes(
                    store_prefix, variable_name, chunk_coordinates, metadata
                )
                object_metadata = self._last_raw_chunk_object_metadata
            else:
                # Coverage probes intentionally inspect only the published
                # coordinates/chunk layout; exact compressed sizes are fetched
                # once, for the selected complete release, before approval.
                object_sizes = []
                object_metadata = []
            if len(object_metadata) != len(chunk_coordinates):
                object_metadata = []
            if len(object_sizes) == len(chunk_coordinates) and chunk_coordinates:
                expected_bytes = sum(object_sizes)
                estimate_basis = "compressed_chunk_object_sizes"
            else:
                expected_bytes = selected_chunk_logical * len(chunk_coordinates)
                estimate_basis = "uncompressed_selected_chunk_upper_bound"
            report: dict[str, object] = {
                "source_uri": f"gs://{self.bucket_name}/{store_prefix}",
                "location": location,
                "observation_date": observation_date.isoformat(),
                "observation_timezone": canonical_timezone,
                "init_time_utc": actual_init.isoformat(),
                "selected_valid_times_utc": [
                    (actual_init + timedelta(seconds=offset)).isoformat()
                    for offset, selected in zip(
                        (
                            selected_offsets
                            if "lead_subtime" in dims
                            else [lead_seconds[index] for index in lead_indices]
                        ),
                        selected_mask,
                        strict=False,
                    )
                    if selected
                ],
                "selection": {
                    "dimensions": list(dims),
                    "sample_indices": [0, 63],
                    "lead_indices": lead_indices,
                    "lead_subtime_indices": sub_indices,
                    "valid_index_tuples": valid_index_tuples,
                    "latitude_index": lat_index,
                    "longitude_index": lon_index,
                    "nearest_latitude": float(lat_values[lat_index]),
                    "nearest_longitude": float(lon_values[lon_index]),
                    "selected_point_shape": [
                        len(selected_indices_by_dim[dim]) for dim in dims
                    ],
                    "selected_valid_value_count": 64 * selected_time_count,
                    "selected_chunk_coordinates": [list(value) for value in chunk_coordinates],
                },
                "array": {
                    **metadata,
                    "global_uncompressed_array_bytes": global_bytes,
                    "selected_logical_bytes": selected_logical,
                    "selected_chunk_count": len(chunk_coordinates),
                    "selected_chunk_logical_bytes": selected_chunk_logical * len(chunk_coordinates),
                    "selected_chunk_object_sizes": object_sizes,
                    "selected_chunk_object_metadata": object_metadata,
                    "expected_network_bytes": expected_bytes,
                    "estimate_basis": estimate_basis,
                    "shard_shape": metadata.get("shard_shape"),
                    "shard_count": _int_value(metadata.get("shard_count"), default=0),
                    "transfer_unit": (
                        "shard" if bool(metadata.get("sharding_detected")) else "chunk"
                    ),
                    "sharding_detected": bool(metadata.get("sharding_detected")),
                },
                "global_uncompressed_array_bytes": global_bytes,
                "global_uncompressed_array_human": _format_bytes(global_bytes),
                "selected_logical_bytes": selected_logical,
                "selected_logical_human": _format_bytes(selected_logical),
                "selected_chunk_logical_bytes": selected_chunk_logical
                * len(chunk_coordinates),
                "selected_chunk_logical_human": _format_bytes(
                    selected_chunk_logical * len(chunk_coordinates)
                ),
                "expected_network_bytes": expected_bytes,
                "expected_network_human": _format_bytes(expected_bytes),
                "estimate_basis": estimate_basis,
                "legacy_guard_formula": (
                    "selected Zarr chunk count × chunk logical bytes; point selection "
                    "does not shrink the global spatial chunk"
                ),
                "shard_shape": metadata.get("shard_shape"),
                "shard_count": _int_value(metadata.get("shard_count"), default=0),
                "transfer_unit": "shard" if bool(metadata.get("sharding_detected")) else "chunk",
                "read_limit_bytes": self.settings.weathernext_raw_read_max_bytes,
                "limit_exceeded": expected_bytes > self.settings.weathernext_raw_read_max_bytes,
                "metadata_only": True,
                "payload_read": False,
                "global_array_is_not_mandatory_transfer": True,
                # This store currently has no sharding codec.  A whole-shard
                # transfer is therefore not applicable; if a future release
                # adds sharding, the field becomes false until its index/range
                # semantics are explicitly decoded.
                "whole_shard_is_not_mandatory_transfer": not bool(
                    metadata.get("sharding_detected")
                ),
                "whole_shard_transfer_applicable": bool(metadata.get("sharding_detected")),
                "publication_fallbacks": fallback_errors,
            }
            if discovered_init is not None and actual_init != discovered_init:
                raise RuntimeError(
                    "WeatherNext path/init_time mismatch: "
                    f"path={discovered_init.isoformat()} dataset={actual_init.isoformat()}"
                )
            return report
        finally:
            close = getattr(dataset, "close", None)
            if callable(close):
                close()

    def read_point_day_ensemble(
        self,
        *,
        latitude: float,
        longitude: float,
        location: str,
        observation_date: date,
        init_time_utc: datetime | None = None,
        timezone_name: str | None = None,
        allow_large_read: bool = False,
    ) -> WeatherNextSnapshot:
        """Read a 64-member station-head daily maximum from one Zarr run."""

        if init_time_utc is not None and (
            init_time_utc.tzinfo is None or init_time_utc.utcoffset() is None
        ):
            raise ValueError("init_time_utc must be timezone-aware")
        store_prefix, discovered_init = self._resolve_store_prefix(
            init_time_utc=init_time_utc,
            observation_date=observation_date,
            timezone_name=timezone_name,
        )
        dataset, store_prefix, discovered_init, _fallback_errors = self._open_dataset_with_fallback(
            store_prefix=store_prefix,
            discovered_init=discovered_init,
            observation_date=observation_date,
            timezone_name=timezone_name,
        )
        try:
            scenarios, dataset_init = _extract_point_day_ensemble(
                dataset,
                latitude=latitude,
                longitude=longitude,
                observation_date=observation_date,
                timezone_name=timezone_name,
                max_read_bytes=self.settings.weathernext_raw_read_max_bytes,
                allow_large_read=allow_large_read,
            )
        finally:
            close = getattr(dataset, "close", None)
            if callable(close):
                close()
        if discovered_init is not None and dataset_init != discovered_init:
            # Keep the dataset's scalar coordinate as the provenance source, but
            # surface a mismatch instead of silently labeling a different run.
            raise RuntimeError(
                "WeatherNext path/init_time mismatch: "
                f"path={discovered_init.isoformat()} dataset={dataset_init.isoformat()}"
            )
        return WeatherNextSnapshot(
            init_time_utc=dataset_init,
            received_at_utc=datetime.now(UTC),
            location=location,
            observation_date=observation_date,
            observation_timezone=timezone_name or "UTC",
            scenario_max_c=scenarios,
            source_uri=f"gs://{self.bucket_name}/{store_prefix}",
        )


def _statistics_variable_names(variable: str) -> tuple[str, ...]:
    if variable not in {"temperature_2m", "station_head_temperature_2m"}:
        raise ValueError(f"unsupported WeatherNext statistics variable: {variable!r}")
    return tuple(f"{variable}_{statistic}" for statistic in _STATISTICS_QUANTILES)


def _statistics_dim_name(variable: Any, candidates: tuple[str, ...]) -> str:
    dims = tuple(str(name) for name in getattr(variable, "dims", ()))
    name = next((candidate for candidate in candidates if candidate in dims), None)
    if name is None:
        raise RuntimeError(
            f"WeatherNext statistics variable has no supported coordinate in {dims!r}; "
            f"expected one of {candidates!r}"
        )
    return name


def _statistics_value_to_celsius(value: float, variable: Any) -> float:
    units = str(getattr(variable, "attrs", {}).get("units", "")).strip().lower()
    if units in {"k", "kelvin"}:
        value -= 273.15
    elif units not in {"c", "degc", "celsius", "degree_celsius", "degrees_celsius"}:
        raise RuntimeError(
            "WeatherNext statistics temperature variable has unsupported/missing units "
            f"{units!r}; expected Kelvin (K)."
        )
    if not math.isfinite(value):
        raise RuntimeError("WeatherNext statistics value is not finite")
    return float(value)


def _statistics_lead_times(
    dataset: Any,
    *,
    init_time: datetime,
    lead_count: int,
) -> list[datetime]:
    """Return valid UTC times without loading any weather data chunks."""

    import numpy as np

    datetime_coord = None
    try:
        datetime_coord = dataset["datetime"]
    except (KeyError, TypeError, AttributeError):
        datetime_coord = None
    if datetime_coord is not None:
        values = np.asarray(getattr(datetime_coord, "values", datetime_coord)).reshape(-1)
        if len(values) == lead_count:
            decoded = [_as_utc_datetime(value, field="datetime") for value in values]
            # Some operational statistics runs publish ``datetime`` metadata
            # before its chunk objects; xarray then decodes the Zarr fill value
            # as 1970-01-01 for every lead.  Do not accept that placeholder as
            # a real valid-time axis.  Fall back to the authoritative
            # init_time + lead_time coordinates unless the datetime values are
            # unique, ordered, and no earlier than the model initialization.
            if (
                decoded == sorted(decoded)
                and len(set(decoded)) == len(decoded)
                and all(value >= init_time for value in decoded)
            ):
                return decoded

    try:
        lead_coord = dataset["lead_time"]
    except (KeyError, TypeError, AttributeError) as error:
        raise RuntimeError("WeatherNext statistics dataset is missing lead_time") from error
    values = np.asarray(getattr(lead_coord, "values", lead_coord)).reshape(-1)
    if len(values) != lead_count:
        raise RuntimeError(
            f"WeatherNext statistics lead_time has {len(values)} values; expected {lead_count}"
        )
    return [
        init_time + timedelta(seconds=_duration_seconds(value, field="lead_time"))
        for value in values
    ]


def _statistics_array_meta(dataset: Any, name: str) -> dict[str, object]:
    """Best-effort metadata extraction used for an auditable read estimate."""

    variable = dataset[name]
    shape = tuple(int(value) for value in getattr(variable, "shape", ()))
    dtype = str(getattr(variable, "dtype", "float32"))
    chunks: tuple[int, ...] | None = None
    codecs: list[object] = []
    encoding = getattr(variable, "encoding", {})
    if isinstance(encoding, Mapping):
        encoded_chunks = encoding.get("chunks") or encoding.get("preferred_chunks")
        if isinstance(encoded_chunks, Mapping):
            chunks = tuple(int(encoded_chunks[dim]) for dim in variable.dims)
        elif isinstance(encoded_chunks, (tuple, list)):
            chunks = tuple(int(value) for value in encoded_chunks)
        encoded_codecs = encoding.get("compressors") or encoding.get("codecs")
        if isinstance(encoded_codecs, (tuple, list)):
            codecs = [
                {"name": type(codec).__name__, "configuration": {}}
                for codec in encoded_codecs
            ]
    # Do not touch ``variable.data`` here: on a lazily opened remote Zarr
    # array that property materializes the first chunk.  If xarray did not
    # expose chunk metadata, the caller will use the conservative logical
    # shape as an upper bound.
    if chunks is None:
        chunks = shape
    try:
        import numpy as np

        dtype_size = int(np.dtype(dtype).itemsize)
    except (TypeError, ValueError):
        dtype_size = 4
    return {
        "shape": list(shape),
        "dtype": dtype,
        "dtype_size_bytes": dtype_size,
        "chunk_shape": list(chunks),
        "codecs": codecs,
        "dimension_names": [str(dim) for dim in getattr(variable, "dims", ())],
        "shard_shape": None,
        "shard_count": 0,
    }


def _int_list(value: object) -> list[int]:
    if isinstance(value, (list, tuple)):
        return [int(item) for item in value]
    return []


def _int_value(value: object, default: int = 0) -> int:
    try:
        if isinstance(value, (int, float, str)):
            return int(value)
        return default
    except (TypeError, ValueError):
        return default


def _normalise_statistics_metadata(payload: Mapping[str, object]) -> dict[str, object]:
    """Convert raw Zarr-v3 array metadata to the estimator's compact shape.

    The public ``zarr.json`` schema uses ``data_type`` and nests the chunk
    dimensions under ``chunk_grid.configuration.chunk_shape``.  Accepting an
    already-normalized payload as well keeps this helper safe for unit tests
    and for callers that cache metadata between runs.
    """

    shape = _int_list(payload.get("shape", []))
    chunk_shape = _int_list(payload.get("chunk_shape", []))
    if not chunk_shape:
        chunk_grid = payload.get("chunk_grid")
        if isinstance(chunk_grid, Mapping):
            configuration = chunk_grid.get("configuration")
            if isinstance(configuration, Mapping):
                chunk_shape = _int_list(configuration.get("chunk_shape", []))
    data_type = payload.get("data_type", payload.get("dtype", "float32"))
    try:
        import numpy as np

        dtype_size = int(np.dtype(str(data_type)).itemsize)
        normalized_dtype = str(np.dtype(str(data_type)))
    except (TypeError, ValueError):
        dtype_size = 4
        normalized_dtype = str(data_type)
    codecs = payload.get("codecs", [])
    if not isinstance(codecs, (tuple, list)):
        codecs = []
    dimensions = payload.get("dimension_names", [])
    if not isinstance(dimensions, (tuple, list)):
        dimensions = []
    result: dict[str, object] = {
        "shape": shape,
        "dtype": normalized_dtype,
        "dtype_size_bytes": dtype_size,
        "chunk_shape": chunk_shape or shape,
        "codecs": list(codecs),
        "dimension_names": [str(value) for value in dimensions],
        "metadata_source": "zarr_root",
    }
    for key in ("chunk_key_encoding", "storage_transformers", "zarr_format", "node_type"):
        if key in payload:
            result[key] = payload[key]
    if "fill_value" in payload:
        result["fill_value"] = payload["fill_value"]
    result["sharding_detected"] = any(
        isinstance(codec, Mapping)
        and str(codec.get("name", "")).lower().startswith("sharding")
        for codec in codecs
    ) or bool(payload.get("storage_transformers"))
    # The published WeatherNext stores currently use regular chunks and no
    # sharding codec. Keep explicit null/zero fields in provenance so callers
    # can distinguish “no shard exists” from an omitted measurement.
    result["shard_shape"] = payload.get("shard_shape")
    result["shard_count"] = _int_value(payload.get("shard_count"), default=0)
    return result


def _statistics_extract_point_hours(
    dataset: Any,
    *,
    latitude: float,
    longitude: float,
    observation_date: date,
    timezone_name: str | None,
    variable_name: str,
    max_hours: int,
    now_utc: datetime | None,
    include_past_hours: bool,
    read_limit_bytes: int | None,
    chunk_size_lookup: Callable[[str, int, Mapping[str, object]], int | None] | None = None,
    metadata_by_name: Mapping[str, Mapping[str, object]] | None = None,
) -> tuple[list[WeatherNextStatisticsPoint], datetime, dict[str, object]]:
    """Extract official summary values for one point and bounded valid hours.

    Selection happens before ``.values``.  The returned provenance explicitly
    distinguishes logical array size, selected logical bytes, compressed chunk
    object sizes, and sharding so a global-array size is never presented as an
    inevitable transfer cost.
    """

    import numpy as np

    names = _statistics_variable_names(variable_name)
    missing = [name for name in names if name not in dataset]
    if missing:
        available = sorted(str(name) for name in getattr(dataset, "data_vars", {}))
        raise RuntimeError(
            "WeatherNext statistics dataset is missing variables "
            f"{missing!r}; available variables: {available[:40]!r}"
        )
    mean_variable = dataset[names[0]]
    lat_name = _statistics_dim_name(mean_variable, ("lat_0p05", "lat_0p1", "latitude", "lat"))
    lon_name = _statistics_dim_name(mean_variable, ("lon_0p05", "lon_0p1", "longitude", "lon"))

    init_coord = _coord_value(dataset, "init_time")
    init_values = getattr(init_coord, "values", init_coord)
    actual_init = _as_utc_datetime(init_values, field="init_time")
    lead_count = int(mean_variable.sizes.get("lead_time", 0))
    if lead_count <= 0:
        raise RuntimeError("WeatherNext statistics variable has no lead_time values")
    valid_times = _statistics_lead_times(dataset, init_time=actual_init, lead_count=lead_count)
    day_start, day_end, canonical_timezone = _observation_window_utc(
        observation_date, timezone_name
    )
    lower_bound = day_start
    if not include_past_hours:
        lower_bound = max(lower_bound, (now_utc or datetime.now(UTC)).astimezone(UTC))
    candidate_indices = [
        index
        for index, valid_time in enumerate(valid_times)
        if lower_bound <= valid_time < day_end
    ]
    if not candidate_indices:
        raise RuntimeError(
            "WeatherNext statistics run has no selected hours for "
            f"{observation_date.isoformat()} ({canonical_timezone}); "
            f"window={day_start.isoformat()}..{day_end.isoformat()}"
        )
    if max_hours < 1:
        raise ValueError("WeatherNext statistics max_hours must be at least 1")
    # ``--hours`` is an explicit bounded slice, not a permission to read the
    # whole station-local day.  Keep the earliest valid hours in the requested
    # window and record the truncation in provenance so the dashboard cannot
    # mistake this partial coverage for a daily maximum.
    selected_indices = candidate_indices[:max_hours]
    selection_truncated = len(candidate_indices) > len(selected_indices)

    # Resolve a single nearest grid point using coordinate arrays only.
    lat_values = np.asarray(getattr(dataset[lat_name], "values", dataset[lat_name])).reshape(-1)
    lon_values = np.asarray(getattr(dataset[lon_name], "values", dataset[lon_name])).reshape(-1)
    if not len(lat_values) or not len(lon_values):
        raise RuntimeError(
            "WeatherNext statistics dataset has empty latitude/longitude coordinates"
        )
    target_lon = longitude % 360.0
    lat_index = int(np.abs(lat_values.astype(float) - latitude).argmin())
    lon_index = int(np.abs(lon_values.astype(float) - target_lon).argmin())
    selected_lat = float(lat_values[lat_index])
    selected_lon = float(lon_values[lon_index])

    metadata: dict[str, dict[str, object]] = {}
    for name in names:
        inferred = _statistics_array_meta(dataset, name)
        override = metadata_by_name.get(name) if metadata_by_name else None
        if isinstance(override, Mapping):
            # Accept either the raw Zarr-v3 object metadata or the compact
            # normalized form used by the read-estimate cache.  Normalizing
            # before the calculation is important: otherwise
            # ``chunk_grid.configuration.chunk_shape`` is ignored and the
            # estimator silently falls back to the global array shape.
            normalized = _normalise_statistics_metadata(override)
            metadata[name] = {**inferred, **normalized}
        else:
            metadata[name] = inferred
    # Build a per-array estimate before materializing any values.
    total_expected = 0
    any_exact_chunk_sizes = False
    arrays_provenance: dict[str, object] = {}
    for name in names:
        meta = metadata[name]
        shape = _int_list(meta.get("shape", []))
        chunk_shape = _int_list(meta.get("chunk_shape", []))
        dtype_size = int(cast(int, meta["dtype_size_bytes"]))
        try:
            import math as _math

            global_bytes = dtype_size * _math.prod(shape)
            selected_logical = dtype_size * len(selected_indices)
            chunk_logical = dtype_size * _math.prod(chunk_shape)
        except (TypeError, ValueError):
            global_bytes = selected_logical = chunk_logical = 0
        codecs = cast(list[object], meta.get("codecs", []))
        sharding = any(
            isinstance(codec, Mapping) and str(codec.get("name", "")).startswith("sharding")
            for codec in codecs
        )
        selected_chunk_numbers = sorted(
            {
                index // max(1, chunk_shape[0])
                for index in selected_indices
            }
        )
        chunk_sizes: list[int] = []
        if chunk_size_lookup is not None:
            for chunk_number in selected_chunk_numbers:
                size = chunk_size_lookup(name, chunk_number, meta)
                if size is not None and size > 0:
                    chunk_sizes.append(int(size))
        if len(chunk_sizes) == len(selected_chunk_numbers):
            expected_bytes = sum(chunk_sizes)
            basis = "compressed_chunk_object_sizes"
            any_exact_chunk_sizes = True
        else:
            expected_bytes = chunk_logical * len(selected_chunk_numbers)
            basis = "uncompressed_selected_chunk_upper_bound"
        total_expected += expected_bytes
        arrays_provenance[name] = {
            **meta,
            "global_uncompressed_array_bytes": global_bytes,
            "selected_logical_bytes": selected_logical,
            "selected_chunk_count": len(selected_chunk_numbers),
            "selected_chunk_indices": selected_chunk_numbers,
            "selected_chunk_logical_bytes": chunk_logical * len(selected_chunk_numbers),
            "selected_chunk_object_sizes": chunk_sizes,
            "expected_network_bytes": expected_bytes,
            "estimate_basis": basis,
            "transfer_unit": "shard" if sharding else "chunk",
            "sharding_detected": sharding,
        }
    read_provenance: dict[str, object] = {
        "estimate_version": "weathernext-statistics-read-v1",
        "array_selection": names,
        "selection": {
            "observation_date": observation_date.isoformat(),
            "observation_timezone": canonical_timezone,
            "window_start_utc": day_start.isoformat(),
            "window_end_utc": day_end.isoformat(),
            "include_past_hours": include_past_hours,
            "candidate_lead_indices": candidate_indices,
            "lead_indices": selected_indices,
            "hour_limit": max_hours,
            "selection_truncated": selection_truncated,
            "valid_times_utc": [valid_times[index].isoformat() for index in selected_indices],
            "nearest_latitude": selected_lat,
            "nearest_longitude": selected_lon,
            "latitude_index": lat_index,
            "longitude_index": lon_index,
        },
        "arrays": arrays_provenance,
        "expected_network_bytes": total_expected,
        "estimate_basis": (
            "compressed_chunk_object_sizes"
            if any_exact_chunk_sizes
            else "uncompressed_selected_chunk_upper_bound"
        ),
        "global_uncompressed_array_bytes": sum(
            _int_value(cast(dict[str, object], value).get("global_uncompressed_array_bytes"))
            for value in arrays_provenance.values()
        ),
        "selected_logical_bytes": sum(
            _int_value(cast(dict[str, object], value).get("selected_logical_bytes"))
            for value in arrays_provenance.values()
        ),
        "read_limit_bytes": read_limit_bytes,
        "limit_exceeded": bool(read_limit_bytes is not None and total_expected > read_limit_bytes),
        "transfer_units": sorted(
            {
                str(cast(dict[str, object], value).get("transfer_unit", "chunk"))
                for value in arrays_provenance.values()
            }
        ),
    }
    if read_limit_bytes is not None and total_expected > read_limit_bytes:
        raise RuntimeError(
            "WeatherNext statistics read refused before downloading data: "
            f"expected {_format_bytes(total_expected)} ({read_provenance['estimate_basis']}), "
            f"limit {_format_bytes(read_limit_bytes)}. "
            "The estimate is based on selected chunks, not the global logical array; "
            "no raw/full-ensemble read was attempted."
        )

    values_by_stat: dict[str, np.ndarray] = {}
    for name in names:
        variable = dataset[name]
        selected = variable.isel(
            {
                "lead_time": selected_indices,
                lat_name: lat_index,
                lon_name: lon_index,
            }
        )
        values = np.asarray(getattr(selected, "values", selected), dtype=float).reshape(-1)
        if len(values) != len(selected_indices):
            raise RuntimeError(
                f"Unexpected WeatherNext statistics shape for {name}: {values.shape}; "
                f"expected {(len(selected_indices),)}"
            )
        values_by_stat[name.rsplit("_", 1)[-1]] = np.asarray(
            [_statistics_value_to_celsius(float(value), variable) for value in values], dtype=float
        )

    points = [
        WeatherNextStatisticsPoint(
            valid_time_utc=valid_times[index],
            temperature_mean_c=float(values_by_stat["mean"][position]),
            p10_c=float(values_by_stat["p10"][position]),
            p25_c=float(values_by_stat["p25"][position]),
            p50_c=float(values_by_stat["p50"][position]),
            p75_c=float(values_by_stat["p75"][position]),
            p90_c=float(values_by_stat["p90"][position]),
        )
        for position, index in enumerate(selected_indices)
    ]
    read_provenance["selected_hours"] = len(points)
    read_provenance["init_time_utc"] = actual_init.isoformat()
    return points, actual_init, read_provenance


class WeatherNextStatisticsGcsClient:
    """Reader for Google's official WeatherNext 3 statistics surface.

    This client never opens the raw 64-member bucket.  It reads only the
    published mean/percentile arrays, selecting one nearest station point and
    bounded valid hours before materialization.
    """

    def __init__(
        self,
        settings: Settings,
        *,
        storage_client: Any | None = None,
        credentials: Any | None = None,
        dataset_factory: Callable[[str], Any] | None = None,
    ) -> None:
        self.settings = settings
        self.bucket_name = settings.weathernext_statistics_bucket.strip()
        if self.bucket_name != _WEATHERNEXT_STATISTICS_BUCKET:
            raise RuntimeError(
                "WeatherNext statistics access only permits "
                f"{_WEATHERNEXT_STATISTICS_BUCKET!r}; got {self.bucket_name!r}."
            )
        self.billing_project = (settings.weathernext_gcs_project or "").strip() or None
        self.credentials = credentials
        self._dataset_factory = dataset_factory
        if storage_client is None:
            missing = _missing_modules(("google.auth", "google.cloud.storage"))
            if missing:
                raise RuntimeError(
                    "WeatherNext statistics access requires google-cloud-storage; "
                    f"missing modules: {', '.join(missing)}"
                )
            from google.auth import default as adc_default
            from google.cloud import storage

            self.credentials, _ = adc_default(scopes=[_GCS_READ_SCOPE])
            kwargs: dict[str, Any] = {"credentials": self.credentials}
            if self.billing_project:
                kwargs["project"] = self.billing_project
            storage_client = storage.Client(**kwargs)
        self._client = storage_client
        try:
            if self.billing_project:
                self._bucket = self._client.bucket(
                    self.bucket_name,
                    user_project=self.billing_project,
                )
            else:
                self._bucket = self._client.bucket(self.bucket_name)
        except TypeError:
            self._bucket = self._client.bucket(self.bucket_name)

    def _list_common_prefixes(self, prefix: str, *, append_slash: bool = True) -> tuple[str, ...]:
        clean = _normalise_prefix(prefix)
        request_prefix = clean + "/" if clean and append_slash else clean
        iterator = self._client.list_blobs(
            self._bucket,
            prefix=request_prefix or None,
            delimiter="/",
            max_results=1000,
        )
        names: set[str] = set()
        pages = getattr(iterator, "pages", None)
        if pages is not None:
            try:
                blobs = next(iter(pages))
            except StopIteration:
                blobs = ()
        else:
            from itertools import islice

            blobs = islice(iterator, 1000)
        for blob in blobs:
            name = str(getattr(blob, "name", blob))
            if request_prefix and name.startswith(request_prefix):
                relative = name[len(request_prefix) :]
                if "/" in relative:
                    names.add(request_prefix + relative.split("/", 1)[0] + "/")
            elif name:
                names.add(name.rstrip("/") + "/")
        for prefix_value in getattr(iterator, "prefixes", ()):
            names.add(str(prefix_value).rstrip("/") + "/")
        return tuple(sorted(names))

    def _resolve_store_prefix(
        self,
        *,
        init_time_utc: datetime | None,
        observation_date: date | None = None,
        timezone_name: str | None = None,
    ) -> tuple[str, datetime | None]:
        explicit = _normalise_prefix(self.settings.weathernext_statistics_store_prefix)
        if explicit:
            if explicit.endswith("/predictions.zarr"):
                return explicit, _parse_run_init(explicit)
            return explicit.rstrip("/") + "/predictions.zarr", _parse_run_init(explicit)
        root = (
            _normalise_prefix(self.settings.weathernext_statistics_prefix)
            or _WEATHERNEXT_STATISTICS_ROOT
        )
        if init_time_utc:
            cutoff = init_time_utc.astimezone(UTC)
        elif observation_date is not None:
            _, day_end, _ = _observation_window_utc(observation_date, timezone_name)
            cutoff = min(datetime.now(UTC), day_end)
        else:
            cutoff = datetime.now(UTC)
        root_leaf = root.rstrip("/").rsplit("/", 1)[-1]
        if root_leaf.endswith("_to_present") or root_leaf.isdigit():
            candidates = [root.rstrip("/")]
        else:
            year = cutoff.year
            candidates = [f"{root.rstrip('/')}/{year}_to_present", f"{root.rstrip('/')}/{year}"]
            if year != 2026:
                candidates.append(f"{root.rstrip('/')}/2026_to_present")
        for year_prefix in sorted(candidates, reverse=True):
            for days_back in range(15):
                probe_date = cutoff.date() - timedelta(days=days_back)
                run_query = f"{year_prefix.rstrip('/')}/{probe_date:%Y%m%d}_"
                run_prefixes = self._list_common_prefixes(run_query, append_slash=False)
                runs = [
                    (parsed, run_prefix.rstrip("/"))
                    for run_prefix in run_prefixes
                    if (parsed := _parse_run_init(run_prefix)) is not None
                ]
                eligible = [item for item in runs if item[0] <= cutoff]
                if eligible:
                    selected_init, selected_prefix = max(eligible, key=lambda item: item[0])
                    return selected_prefix + "/predictions.zarr", selected_init
        raise RuntimeError(
            "No WeatherNext statistics prediction run at or before "
            f"{cutoff.isoformat()} was found below gs://{self.bucket_name}/{root}/"
        )

    def _open_dataset(self, store_prefix: str) -> Any:
        if self._dataset_factory is not None:
            return self._dataset_factory(store_prefix)
        _require_gcs_stack()
        import importlib

        obstore = cast(Any, importlib.import_module("obstore"))
        xarray = cast(Any, importlib.import_module("xarray"))
        zarr = cast(Any, importlib.import_module("zarr"))
        client_options: dict[str, object] = {}
        if self.billing_project:
            client_options["default_headers"] = {"x-goog-user-project": self.billing_project}
        gcs_store = obstore.store.GCSStore(
            bucket=self.bucket_name,
            prefix=store_prefix,
            client_options=client_options,
        )
        zarr_store = zarr.storage.ObjectStore(gcs_store)
        from importlib.util import find_spec

        chunks: dict[str, int] | None = {} if find_spec("dask") else None
        return xarray.open_zarr(zarr_store, chunks=chunks)

    def _open_dataset_with_fallback(
        self,
        *,
        store_prefix: str,
        discovered_init: datetime | None,
        observation_date: date | None,
        timezone_name: str | None,
    ) -> tuple[Any, str, datetime | None, list[str]]:
        """Open the newest complete statistics publication.

        A run prefix can briefly appear before its Zarr group metadata is
        complete. Retry a bounded number of prior runs in that case. An
        explicit store prefix remains strict and is never silently replaced.
        """

        current_prefix = store_prefix
        current_init = discovered_init
        errors: list[str] = []
        explicit = bool(_normalise_prefix(self.settings.weathernext_statistics_store_prefix))
        for _attempt in range(4):
            try:
                return self._open_dataset(current_prefix), current_prefix, current_init, errors
            except Exception as error:
                errors.append(f"{current_prefix}: {error}")
                if explicit or current_init is None:
                    raise
                previous_prefix, previous_init = self._resolve_store_prefix(
                    init_time_utc=current_init - timedelta(microseconds=1),
                    observation_date=observation_date,
                    timezone_name=timezone_name,
                )
                if previous_prefix == current_prefix:
                    raise RuntimeError(
                        "WeatherNext statistics publication is incomplete and no prior run is "
                        f"available: {errors[-1]}"
                    ) from error
                current_prefix, current_init = previous_prefix, previous_init
        raise RuntimeError(
            "No complete WeatherNext statistics run could be opened after bounded fallback: "
            + " | ".join(errors)
        )

    def _zarr_metadata(self, store_prefix: str) -> dict[str, dict[str, object]]:
        blob_method = getattr(self._bucket, "blob", None)
        if not callable(blob_method):
            return {}
        try:
            blob = cast(Any, blob_method(f"{store_prefix.rstrip('/')}/zarr.json"))
            raw = blob.download_as_bytes()
            payload = json.loads(raw)
            metadata = payload.get("consolidated_metadata", {}).get("metadata", {})
            if not isinstance(metadata, Mapping):
                return {}
            return {
                str(name): value
                for name, value in metadata.items()
                if isinstance(value, dict)
            }
        except Exception:
            return {}

    def _chunk_object_size(
        self,
        store_prefix: str,
        name: str,
        chunk_number: int,
        meta: Mapping[str, object],
    ) -> int | None:
        blob_method = getattr(self._bucket, "blob", None)
        if not callable(blob_method):
            return None
        shape = _int_list(meta.get("shape", []))
        chunk_shape = _int_list(meta.get("chunk_shape", []))
        if len(shape) < 3 or len(chunk_shape) < 3:
            return None
        # The published statistics arrays use regular chunks with one complete
        # spatial tile per lead.  For a sharded store we deliberately return
        # None so the estimate remains an explicit upper bound rather than
        # pretending a chunk is independently transferable.
        if any(
            isinstance(codec, Mapping) and str(codec.get("name", "")).startswith("sharding")
            for codec in cast(list[object], meta.get("codecs", []))
        ):
            return None
        key = f"{store_prefix.rstrip('/')}/{name}/c/{chunk_number}/0/0"
        try:
            blob = cast(Any, blob_method(key))
            blob.reload()
            size = getattr(blob, "size", None)
            return None if size is None else int(size)
        except Exception:
            return None

    def check_access(self) -> dict[str, object]:
        """Perform metadata-only access/schema checks for the statistics bucket."""

        result: dict[str, object] = {
            "surface": _WEATHERNEXT_STATISTICS_SURFACE,
            "billing_project": self.billing_project,
            "bucket": f"gs://{self.bucket_name}/",
            "prefix": _normalise_prefix(self.settings.weathernext_statistics_prefix)
            or _WEATHERNEXT_STATISTICS_ROOT,
            "auth": "application_default_credentials",
            "access_granted": False,
            "requester_pays": None,
        }
        try:
            self._bucket.reload()
            result["requester_pays"] = bool(getattr(self._bucket, "requester_pays", False))
            prefixes = self._list_common_prefixes(str(result["prefix"]))
            result["zarr_prefixes"] = list(prefixes[:100])
            try:
                latest_prefix, latest_init = self._resolve_store_prefix(
                    init_time_utc=None,
                    observation_date=datetime.now(UTC).date(),
                )
                result["latest_run_prefix"] = latest_prefix
                result["latest_run_init_time_utc"] = (
                    None if latest_init is None else latest_init.isoformat()
                )
                dataset, latest_prefix, latest_init, fallback_errors = (
                    self._open_dataset_with_fallback(
                        store_prefix=latest_prefix,
                        discovered_init=latest_init,
                        observation_date=datetime.now(UTC).date(),
                        timezone_name=None,
                    )
                )
                result["latest_run_prefix"] = latest_prefix
                result["latest_run_init_time_utc"] = (
                    None if latest_init is None else latest_init.isoformat()
                )
                if fallback_errors:
                    result["publication_fallbacks"] = fallback_errors
                try:
                    names = _statistics_variable_names(
                        self.settings.weathernext_statistics_variable
                    )
                    missing = [name for name in names if name not in dataset]
                    result["schema_ok"] = not missing
                    result["missing_variables"] = missing
                    if not missing:
                        variable = dataset[names[0]]
                        result["variable"] = self.settings.weathernext_statistics_variable
                        result["statistics"] = list(_STATISTICS_QUANTILES)
                        result["dimensions"] = [str(item) for item in variable.dims]
                        result["units"] = str(getattr(variable, "attrs", {}).get("units", ""))
                finally:
                    close = getattr(dataset, "close", None)
                    if callable(close):
                        close()
            except Exception as error:
                result["schema_ok"] = False
                result["schema_error"] = str(error)
            result["access_granted"] = True
        except Exception as error:
            result["error"] = str(error)
        return result

    def read_point_hours(
        self,
        *,
        latitude: float,
        longitude: float,
        location: str,
        station_id: str | None = None,
        observation_date: date,
        init_time_utc: datetime | None = None,
        timezone_name: str | None = None,
        max_hours: int | None = None,
        now_utc: datetime | None = None,
        include_past_hours: bool = False,
    ) -> WeatherNextStatisticsSnapshot:
        """Read a summary slice, falling back from incomplete publications.

        Operational GCS listings can expose a run before all statistic chunks
        are present.  A non-finite value or an incomplete-chunk estimate is a
        publication problem, not an access denial; retry one prior run while
        keeping the selection bounded.  Explicit store pins remain strict.
        """

        explicit = bool(_normalise_prefix(self.settings.weathernext_statistics_store_prefix))
        latest_prefix: str | None = None
        latest_init: datetime | None = None
        try:
            latest_prefix, latest_init = self._resolve_store_prefix(
                init_time_utc=init_time_utc,
                observation_date=observation_date,
                timezone_name=timezone_name,
            )
            return self._read_point_hours_once(
                latitude=latitude,
                longitude=longitude,
                location=location,
                station_id=station_id,
                observation_date=observation_date,
                init_time_utc=init_time_utc,
                timezone_name=timezone_name,
                max_hours=max_hours,
                now_utc=now_utc,
                include_past_hours=include_past_hours,
            )
        except RuntimeError as error:
            message = str(error).lower()
            retryable = "not finite" in message or "read refused" in message
            if explicit or not retryable or latest_init is None:
                raise
            previous_prefix, previous_init = self._resolve_store_prefix(
                init_time_utc=latest_init - timedelta(microseconds=1),
                observation_date=observation_date,
                timezone_name=timezone_name,
            )
            if previous_prefix == latest_prefix or previous_init is None:
                raise
            snapshot = self._read_point_hours_once(
                latitude=latitude,
                longitude=longitude,
                location=location,
                station_id=station_id,
                observation_date=observation_date,
                init_time_utc=previous_init,
                timezone_name=timezone_name,
                max_hours=max_hours,
                now_utc=now_utc,
                include_past_hours=include_past_hours,
            )
            provenance = dict(snapshot.read_provenance)
            provenance["publication_read_fallback"] = {
                "failed_run_prefix": latest_prefix,
                "failed_run_init_time_utc": latest_init.isoformat(),
                "reason": str(error),
                "selected_fallback_prefix": previous_prefix,
                "selected_fallback_init_time_utc": previous_init.isoformat(),
            }
            return snapshot.model_copy(update={"read_provenance": provenance})

    def _read_point_hours_once(
        self,
        *,
        latitude: float,
        longitude: float,
        location: str,
        station_id: str | None = None,
        observation_date: date,
        init_time_utc: datetime | None = None,
        timezone_name: str | None = None,
        max_hours: int | None = None,
        now_utc: datetime | None = None,
        include_past_hours: bool = False,
    ) -> WeatherNextStatisticsSnapshot:
        if init_time_utc is not None and (
            init_time_utc.tzinfo is None or init_time_utc.utcoffset() is None
        ):
            raise ValueError("init_time_utc must be timezone-aware")
        if now_utc is not None and (now_utc.tzinfo is None or now_utc.utcoffset() is None):
            raise ValueError("now_utc must be timezone-aware")
        selected_variable = self.settings.weathernext_statistics_variable
        store_prefix, discovered_init = self._resolve_store_prefix(
            init_time_utc=init_time_utc,
            observation_date=observation_date,
            timezone_name=timezone_name,
        )
        dataset, store_prefix, discovered_init, fallback_errors = (
            self._open_dataset_with_fallback(
                store_prefix=store_prefix,
                discovered_init=discovered_init,
                observation_date=observation_date,
                timezone_name=timezone_name,
            )
        )
        try:
            names = _statistics_variable_names(selected_variable)
            # Consolidated metadata is used only for provenance/size estimates;
            # the dataset remains the source of truth for actual values.
            raw_metadata = self._zarr_metadata(store_prefix)
            root_meta = {
                name: _normalise_statistics_metadata(raw_metadata[name])
                for name in names
                if name in raw_metadata
            }

            def chunk_lookup(
                name: str, chunk_number: int, inferred: Mapping[str, object]
            ) -> int | None:
                # A metadata-only object HEAD records the compressed transfer
                # unit without downloading its payload.  If a provider refuses
                # the HEAD, the estimator falls back to the explicit logical
                # chunk upper bound and records that basis per array.
                meta = root_meta.get(name, inferred)
                return self._chunk_object_size(store_prefix, name, chunk_number, meta)

            # Merge exact Zarr metadata into the temporary dataset encoding used
            # by the estimator without mutating xarray objects.
            points, dataset_init, provenance = _statistics_extract_point_hours(
                dataset,
                latitude=latitude,
                longitude=longitude,
                observation_date=observation_date,
                timezone_name=timezone_name,
                variable_name=selected_variable,
                max_hours=(
                    self.settings.weathernext_statistics_max_hours
                    if max_hours is None
                    else max_hours
                ),
                now_utc=now_utc,
                include_past_hours=include_past_hours,
                read_limit_bytes=self.settings.weathernext_statistics_read_max_bytes,
                chunk_size_lookup=chunk_lookup,
                metadata_by_name=root_meta,
            )
            if discovered_init is not None and dataset_init != discovered_init:
                raise RuntimeError(
                    "WeatherNext statistics path/init_time mismatch: "
                    f"path={discovered_init.isoformat()} dataset={dataset_init.isoformat()}"
                )
            if fallback_errors:
                provenance["publication_fallbacks"] = fallback_errors
        finally:
            close = getattr(dataset, "close", None)
            if callable(close):
                close()
        return WeatherNextStatisticsSnapshot(
            init_time_utc=dataset_init,
            received_at_utc=datetime.now(UTC),
            location=location,
            station_id=station_id,
            latitude=latitude,
            longitude=longitude,
            observation_date=observation_date,
            observation_timezone=timezone_name or "UTC",
            variable=selected_variable,
            points=points,
            source_uri=f"gs://{self.bucket_name}/{store_prefix}",
            read_provenance=provenance,
        )


class WeatherNextProvider:
    """Optional comparison source; it never changes v1 decisions by itself."""

    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    def refresh_from_gcs(
        self,
        *,
        latitude: float,
        longitude: float,
        location: str,
        observation_date: date,
        init_time_utc: datetime | None = None,
        timezone_name: str | None = None,
        allow_large_read: bool = False,
        output_path: Path | None = None,
    ) -> WeatherNextSnapshot:
        """Pull and atomically save a fresh authorized WeatherNext snapshot."""

        client = WeatherNextGcsClient(self.settings)
        snapshot = client.read_point_day_ensemble(
            latitude=latitude,
            longitude=longitude,
            location=location,
            observation_date=observation_date,
            init_time_utc=init_time_utc,
            timezone_name=timezone_name,
            allow_large_read=allow_large_read,
        )
        target = output_path or self.settings.weathernext_snapshot_path
        if not target:
            raise RuntimeError("weathernext_snapshot_path is not configured; refusing to write.")
        path = Path(target).expanduser()
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_name(path.name + ".tmp")
        temporary.write_text(snapshot.model_dump_json(indent=2), encoding="utf-8")
        temporary.replace(path)
        return snapshot

    def refresh_statistics_from_gcs(
        self,
        *,
        latitude: float,
        longitude: float,
        location: str,
        station_id: str | None = None,
        observation_date: date,
        init_time_utc: datetime | None = None,
        timezone_name: str | None = None,
        max_hours: int | None = None,
        now_utc: datetime | None = None,
        include_past_hours: bool = False,
        output_path: Path | None = None,
    ) -> WeatherNextStatisticsSnapshot:
        """Pull and atomically save one official SUMMARY_ONLY snapshot."""

        client = WeatherNextStatisticsGcsClient(self.settings)
        snapshot = client.read_point_hours(
            latitude=latitude,
            longitude=longitude,
            location=location,
            station_id=station_id,
            observation_date=observation_date,
            init_time_utc=init_time_utc,
            timezone_name=timezone_name,
            max_hours=max_hours,
            now_utc=now_utc,
            include_past_hours=include_past_hours,
        )
        target = output_path or self.settings.weathernext_statistics_snapshot_path
        if not target:
            raise RuntimeError(
                "weathernext_statistics_snapshot_path is not configured; refusing to write."
            )
        path = Path(target).expanduser()
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_name(path.name + ".tmp")
        temporary.write_text(snapshot.model_dump_json(indent=2), encoding="utf-8")
        temporary.replace(path)
        return snapshot

    def statistics_status(self, *, check_access: bool = False) -> WeatherNextStatisticsStatus:
        """Return access and local snapshot state independently.

        Dashboard callers leave ``check_access`` false: rendering a local page
        must never trigger a GCS read.  The CLI/operator path can request a
        fresh metadata-only access check explicitly.
        """

        path = self.settings.weathernext_statistics_snapshot_path
        if not self.settings.weathernext_enabled:
            return WeatherNextStatisticsStatus(
                access_state="disabled",
                load_state="not_loaded",
                enabled=False,
                snapshot_path=path,
                message="WeatherNext statistics surface is disabled; v1 continues unchanged.",
            )

        # Load state is always derived independently from the optional access
        # probe.  A temporary GCS failure must not erase a valid local snapshot
        # from the dashboard, and a corrupt/missing file must not be reported as
        # an access failure.
        snapshot: WeatherNextStatisticsSnapshot | None = None
        load_error: str | None = None
        load_state: Literal["not_loaded", "available", "error"] = "not_loaded"
        if path:
            try:
                snapshot = self._load_statistics_snapshot(Path(path).expanduser())
            except Exception as error:
                load_state = "error"
                load_error = str(error)
            else:
                load_state = "available"

        access_report: dict[str, object] | None = None
        access_state: Literal["pending", "granted", "error"] = (
            "granted" if snapshot is not None else "pending"
        )
        access_message: str | None = None
        if check_access:
            try:
                client = WeatherNextStatisticsGcsClient(self.settings)
                access_report = client.check_access()
            except Exception as error:
                access_state = "error"
                access_message = f"WeatherNext statistics access check failed: {error}"
            else:
                if bool(access_report.get("access_granted")):
                    access_state = "granted"
                    access_message = "WeatherNext statistics access granted."
                else:
                    access_state = "error"
                    access_message = str(
                        access_report.get("error") or "WeatherNext statistics access denied"
                    )

        if load_error is not None:
            message = f"WeatherNext statistics snapshot could not be loaded: {load_error}"
        elif snapshot is not None:
            message = "Official WeatherNext statistics snapshot loaded as SUMMARY_ONLY."
        elif check_access and access_message:
            message = access_message
        elif check_access:
            message = "WeatherNext statistics access checked; no SUMMARY_ONLY snapshot saved."
        else:
            message = (
                "WeatherNext statistics access is not checked by the dashboard; "
                "run `polybot weathernext statistics-check` explicitly."
            )
        if access_message and snapshot is not None and access_state == "error":
            message = f"{message} Access probe: {access_message}"

        return WeatherNextStatisticsStatus(
            access_state=access_state,
            load_state=load_state,
            enabled=True,
            snapshot_path=path,
            init_time_utc=None if snapshot is None else snapshot.init_time_utc,
            received_at_utc=None if snapshot is None else snapshot.received_at_utc,
            message=message,
            access_report=access_report,
        )

    def statistics_dashboard_payload(self, *, probe_access: bool = False) -> dict[str, object]:
        """Return a read-only payload suitable for the local dashboard.

        Dashboard requests must not trigger GCS metadata/network reads.  The
        explicit ``statistics-check`` command (or an observer boundary) can
        probe access; by default this method reports access from local
        configuration/snapshot evidence only and loads the saved snapshot
        from disk.
        """

        if probe_access:
            status = self.statistics_status(check_access=True)
        else:
            path = self.settings.weathernext_statistics_snapshot_path
            if not self.settings.weathernext_enabled:
                status = WeatherNextStatisticsStatus(
                    access_state="disabled",
                    load_state="not_loaded",
                    enabled=False,
                    snapshot_path=path,
                    message="WeatherNext statistics surface is disabled; v1 continues unchanged.",
                )
            elif not path:
                status = WeatherNextStatisticsStatus(
                    access_state="pending",
                    load_state="not_loaded",
                    enabled=True,
                    snapshot_path=None,
                    message=(
                        "No local SUMMARY_ONLY snapshot is saved. Access is not probed by the "
                        "dashboard; run `polybot weathernext statistics-check` explicitly."
                    ),
                )
            else:
                try:
                    snapshot = self._load_statistics_snapshot(Path(path).expanduser())
                except Exception as error:
                    status = WeatherNextStatisticsStatus(
                        access_state="pending",
                        load_state="error",
                        enabled=True,
                        snapshot_path=path,
                        message=f"WeatherNext statistics snapshot could not be loaded: {error}",
                    )
                else:
                    status = WeatherNextStatisticsStatus(
                        access_state="granted",
                        load_state="available",
                        enabled=True,
                        snapshot_path=path,
                        init_time_utc=snapshot.init_time_utc,
                        received_at_utc=snapshot.received_at_utc,
                        message=(
                            "Local WeatherNext statistics snapshot loaded as SUMMARY_ONLY; "
                            "access was not re-probed by the dashboard."
                        ),
                    )
        payload: dict[str, object] = {"status": status.model_dump(mode="json"), "snapshot": None}
        path = status.snapshot_path
        if path and status.load_state == "available":
            try:
                payload["snapshot"] = self._load_statistics_snapshot(
                    Path(path).expanduser()
                ).model_dump(mode="json")
            except Exception as error:
                payload["status"] = WeatherNextStatisticsStatus(
                    access_state=status.access_state,
                    load_state="error",
                    enabled=status.enabled,
                    snapshot_path=path,
                    message=f"WeatherNext statistics snapshot could not be loaded: {error}",
                    access_report=status.access_report,
                ).model_dump(mode="json")
        return payload

    def status(self) -> WeatherNextStatus:
        path = self.settings.weathernext_snapshot_path
        if not self.settings.weathernext_enabled:
            return WeatherNextStatus(
                state="disabled",
                enabled=False,
                surface=self.settings.weathernext_surface,
                snapshot_path=path,
                message="WeatherNext 3 comparison is disabled; v1 continues with current sources.",
            )
        indexed = self._first_indexed_snapshot()
        if indexed is not None and (path is None or not Path(path).expanduser().is_file()):
            indexed_path, indexed_snapshot = indexed
            return WeatherNextStatus(
                state="snapshot_available",
                enabled=True,
                surface=self.settings.weathernext_surface,
                snapshot_path=str(indexed_path),
                init_time_utc=indexed_snapshot.init_time_utc,
                received_at_utc=indexed_snapshot.received_at_utc,
                message="Authorized WeatherNext 3 trajectory snapshot index loaded.",
            )
        if not self.settings.weathernext_gcs_project and indexed is None:
            return WeatherNextStatus(
                state="access_pending",
                enabled=True,
                surface=self.settings.weathernext_surface,
                snapshot_path=path,
                message=(
                    "WeatherNext 3 access is pending: set "
                    "POLYBOT_WEATHERNEXT_GCS_PROJECT to the Requester Pays billing project."
                ),
            )
        if not path and indexed is None:
            return WeatherNextStatus(
                state="access_pending",
                enabled=True,
                surface=self.settings.weathernext_surface,
                snapshot_path=None,
                message=(
                    "WeatherNext 3 billing project "
                    f"{self.settings.weathernext_gcs_project} is configured, but "
                    "no snapshot is saved; "
                    "run `polybot weathernext check` and then an explicit refresh."
                ),
            )
        if path is None:
            # ``indexed`` was handled above; this is only a defensive type
            # narrowing guard for a concurrently removed index file.
            return WeatherNextStatus(
                state="access_pending",
                enabled=True,
                surface=self.settings.weathernext_surface,
                snapshot_path=None,
                message="WeatherNext snapshot index no longer contains a usable artifact.",
            )
        try:
            snapshot = self._load_snapshot(Path(path).expanduser())
        except Exception as error:
            return WeatherNextStatus(
                state="error",
                enabled=True,
                surface=self.settings.weathernext_surface,
                snapshot_path=path,
                message=f"WeatherNext snapshot could not be loaded: {error}",
            )
        return WeatherNextStatus(
            state="snapshot_available",
            enabled=True,
            surface=self.settings.weathernext_surface,
            snapshot_path=path,
            init_time_utc=snapshot.init_time_utc,
            received_at_utc=snapshot.received_at_utc,
            message="Authorized WeatherNext 3 snapshot loaded for comparison only.",
        )

    def snapshot_for(self, rules: RuleInterpretation) -> WeatherNextSnapshot | None:
        if not self.settings.weathernext_enabled:
            return None
        if rules.location is None or rules.observation_date is None:
            return None
        indexed_candidates = self._indexed_snapshots_for_rules(rules)
        for snapshot in indexed_candidates:
            return snapshot
        path = self.settings.weathernext_snapshot_path
        if not path:
            return None
        try:
            snapshot = self._load_snapshot(Path(path).expanduser())
        except Exception:
            return None
        if snapshot.observation_date != rules.observation_date:
            return None
        if snapshot.location.casefold() != rules.location.casefold():
            return None
        return snapshot

    def paper_snapshot_for(
        self,
        rules: RuleInterpretation,
        *,
        now_utc: datetime | None = None,
    ) -> tuple[WeatherNextSnapshot | None, str | None]:
        """Resolve a snapshot for timely paper use, never historical backfill.

        Diagnostic archives may contain old station days.  They remain visible
        to comparison/reporting code, but the autonomous paper lane accepts a
        snapshot only while its station-local observation date is current or
        future.  This prevents a later extraction from creating retroactive
        paper results for a market day that already ended.
        """

        snapshot = self.snapshot_for(rules)
        if snapshot is None:
            return None, "SNAPSHOT_UNAVAILABLE"
        now = now_utc or datetime.now(UTC)
        if now.tzinfo is None or now.utcoffset() is None:
            raise ValueError("now_utc must be timezone-aware")
        now = now.astimezone(UTC)
        if snapshot.init_time_utc > now or snapshot.received_at_utc > now:
            return None, "SNAPSHOT_FROM_FUTURE"
        local_today = now.astimezone(ZoneInfo(snapshot.observation_timezone)).date()
        if snapshot.observation_date < local_today:
            return None, "SNAPSHOT_RETROACTIVE_BLOCKED"
        return snapshot, None

    @staticmethod
    def probabilities(
        snapshot: WeatherNextSnapshot, brackets: Mapping[str, Bracket]
    ) -> dict[str, float]:
        total = len(snapshot.scenario_max_c)
        result: dict[str, float] = {}
        for market_id, bracket in brackets.items():
            count = sum(
                1
                for value in snapshot.scenario_max_c
                if (
                    bracket.lower is None
                    or value > bracket.lower
                    or (bracket.lower_inclusive and value == bracket.lower)
                )
                and (
                    bracket.upper is None
                    or value < bracket.upper
                    or (bracket.upper_inclusive and value == bracket.upper)
                )
            )
            result[market_id] = count / total
        return result

    @staticmethod
    def _load_snapshot(path: Path) -> WeatherNextSnapshot:
        payload = json.loads(path.read_text(encoding="utf-8"))
        return WeatherNextSnapshot.model_validate(payload)

    def _snapshot_index_path(self) -> Path | None:
        value = getattr(self.settings, "weathernext_snapshot_index_path", None)
        if not value:
            return None
        return Path(value).expanduser()

    def _load_snapshot_index(self) -> list[dict[str, object]]:
        path = self._snapshot_index_path()
        if path is None or not path.is_file():
            return []
        payload = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(payload, Mapping):
            return []
        entries = payload.get("entries", [])
        if not isinstance(entries, list):
            return []
        return [
            {str(key): value for key, value in entry.items()}
            for entry in entries
            if isinstance(entry, Mapping)
        ]

    def _first_indexed_snapshot(self) -> tuple[Path, WeatherNextSnapshot] | None:
        for entry in self._load_snapshot_index():
            raw_path = entry.get("path")
            if not isinstance(raw_path, str) or not raw_path:
                continue
            try:
                path = Path(raw_path).expanduser()
                return path, self._load_snapshot(path)
            except Exception:
                continue
        return None

    def _indexed_snapshots_for_rules(
        self, rules: RuleInterpretation
    ) -> list[WeatherNextSnapshot]:
        if rules.location is None or rules.observation_date is None:
            return []
        location_key = rules.location.casefold().strip()
        authority_key = (rules.station_or_authority or "").casefold().strip()
        matches: list[WeatherNextSnapshot] = []
        for entry in self._load_snapshot_index():
            entry_date = entry.get("observation_date")
            if str(entry_date) != rules.observation_date.isoformat():
                continue
            entry_location = str(entry.get("location") or "").casefold().strip()
            entry_station = str(entry.get("station_id") or "").casefold().strip()
            if not (
                _weathernext_identity_matches(location_key, entry_location, entry_station)
                or (
                    authority_key
                    and _weathernext_identity_matches(
                        authority_key, entry_location, entry_station
                    )
                )
            ):
                continue
            raw_path = entry.get("path")
            if not isinstance(raw_path, str) or not raw_path:
                continue
            try:
                snapshot = self._load_snapshot(Path(raw_path).expanduser())
            except Exception:
                continue
            if snapshot.observation_date == rules.observation_date:
                matches.append(snapshot)
        return matches

    @staticmethod
    def _load_statistics_snapshot(path: Path) -> WeatherNextStatisticsSnapshot:
        payload = json.loads(path.read_text(encoding="utf-8"))
        return WeatherNextStatisticsSnapshot.model_validate(payload)
