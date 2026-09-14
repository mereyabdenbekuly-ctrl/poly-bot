from __future__ import annotations

import json
import math
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
_GCS_READ_SCOPE = "https://www.googleapis.com/auth/devstorage.read_only"
_RUN_RE = re.compile(r"(?P<date>\d{8})_(?P<hour>\d{2})hr_(?P<batch>\d{2})_preds(?:/|$)")


def _format_bytes(value: int) -> str:
    units = ("B", "KiB", "MiB", "GiB", "TiB")
    amount = float(max(0, value))
    for unit in units:
        if amount < 1024 or unit == units[-1]:
            return f"{amount:.1f} {unit}"
        amount /= 1024
    return f"{amount:.1f} TiB"


class WeatherNextSnapshot(StrictModel):
    """An explicitly supplied WeatherNext export; never fabricated by Polybot."""

    source: Literal["weathernext3"] = "weathernext3"
    init_time_utc: datetime
    received_at_utc: datetime
    location: str
    observation_date: date
    observation_timezone: str = "UTC"
    scenario_max_c: list[float] = Field(min_length=64, max_length=64)
    source_uri: str

    @field_validator("init_time_utc", "received_at_utc")
    @classmethod
    def _timezone_aware(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("WeatherNext timestamps must be timezone-aware")
        return value.astimezone(UTC)

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

    @model_validator(mode="after")
    def _validate_provenance(self) -> WeatherNextSnapshot:
        if self.received_at_utc < self.init_time_utc:
            raise ValueError("received_at_utc must not precede init_time_utc")
        if not self.source_uri.startswith(f"gs://{_WEATHERNEXT_BUCKET}/"):
            raise ValueError("source_uri must identify the official full-ensemble GCS bucket")
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
                    dataset = self._open_dataset(latest_prefix)
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
        dataset = self._open_dataset(store_prefix)
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
        if not self.settings.weathernext_gcs_project:
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
        if not path:
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
        if not self.settings.weathernext_enabled or not self.settings.weathernext_snapshot_path:
            return None
        snapshot = self._load_snapshot(Path(self.settings.weathernext_snapshot_path).expanduser())
        if rules.location is None or rules.observation_date is None:
            return None
        if snapshot.observation_date != rules.observation_date:
            return None
        if snapshot.location.casefold() != rules.location.casefold():
            return None
        return snapshot

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
