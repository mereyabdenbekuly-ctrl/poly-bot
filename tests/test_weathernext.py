from __future__ import annotations

from datetime import UTC, date, datetime
from types import SimpleNamespace

import numpy as np
import pytest
import xarray as xr

from polybot.config import Settings
from polybot.weathernext import (
    WeatherNextGcsClient,
    WeatherNextProvider,
    WeatherNextSnapshot,
    _extract_point_day_ensemble,
    _observation_window_utc,
)


class _BlobIterator(list):
    def __init__(self, values=(), *, prefixes=()):
        super().__init__(values)
        self.prefixes = set(prefixes)


class _FakeBucket:
    requester_pays = True

    def __init__(self) -> None:
        self.reload_calls = 0

    def reload(self) -> None:
        self.reload_calls += 1


class _FakeStorageClient:
    def __init__(self, prefixes=(), prefix_map=None) -> None:
        self.bucket_obj = _FakeBucket()
        self.prefixes = tuple(prefixes)
        self.prefix_map = dict(prefix_map or {})
        self.bucket_args: tuple[object, ...] | None = None
        self.list_calls: list[dict[str, object]] = []

    def bucket(self, *args, **kwargs):
        self.bucket_args = (*args, kwargs)
        return self.bucket_obj

    def list_blobs(self, bucket, **kwargs):
        self.list_calls.append(kwargs)
        prefix = kwargs.get("prefix")
        return _BlobIterator(prefixes=self.prefix_map.get(prefix, self.prefixes))


def _settings(tmp_path, **kwargs) -> Settings:
    values = {
        "database_path": tmp_path / "polybot.sqlite3",
        "weathernext_enabled": True,
        "weathernext_gcs_project": "weather-508105",
    }
    values.update(kwargs)
    return Settings(**values)


def test_snapshot_rejects_naive_or_nonfinite_provenance() -> None:
    base = {
        "location": "Munich",
        "observation_date": date(2026, 9, 14),
        "scenario_max_c": [20.0] * 64,
        "source_uri": "gs://weathernext3_spatial/test/predictions.zarr",
    }
    with pytest.raises(ValueError, match="timezone-aware"):
        WeatherNextSnapshot(
            **base,
            init_time_utc=datetime(2026, 9, 14),
            received_at_utc=datetime(2026, 9, 14, 1, tzinfo=UTC),
        )
    with pytest.raises(ValueError, match="finite"):
        nonfinite = {**base, "scenario_max_c": [float("nan")] + [20.0] * 63}
        WeatherNextSnapshot(
            **nonfinite,
            init_time_utc=datetime(2026, 9, 14, tzinfo=UTC),
            received_at_utc=datetime(2026, 9, 14, 1, tzinfo=UTC),
        )


def test_extracts_station_head_daily_maxima_in_celsius() -> None:
    samples = np.arange(64)
    lead_time = np.array([0, 6], dtype="timedelta64[h]")
    lead_subtime = np.array([0, 1], dtype="timedelta64[h]")
    # Kelvin values: each member's expected max is 10 + member/10 C.
    base_c = samples[:, None, None, None, None] / 10 + 10
    values_k = 273.15 + base_c + np.array([[[[[0.0]]], [[[1.0]]]]])
    values_k = np.broadcast_to(values_k, (64, 2, 2, 1, 1)).copy()
    dataset = xr.Dataset(
        {
            "station_head_temperature_2m": (
                ("sample", "lead_time", "lead_subtime", "lat_0p05", "lon_0p05"),
                values_k,
                {"units": "K"},
            )
        },
        coords={
            "sample": samples,
            "lead_time": lead_time,
            "lead_subtime": lead_subtime,
            "lat_0p05": [48.1],
            "lon_0p05": [360 - 11.7],
            "init_time": np.datetime64("2026-09-14T00:00:00"),
        },
    )

    scenarios, init = _extract_point_day_ensemble(
        dataset,
        latitude=48.1,
        longitude=-11.7,
        observation_date=date(2026, 9, 14),
    )

    assert init == datetime(2026, 9, 14, tzinfo=UTC)
    assert len(scenarios) == 64
    assert scenarios[0] == pytest.approx(11.0)
    assert scenarios[-1] == pytest.approx(17.3)


def test_raw_point_read_guard_runs_before_materializing_values() -> None:
    samples = np.arange(64)
    dataset = xr.Dataset(
        {
            "station_head_temperature_2m": (
                ("sample", "lead_time", "lead_subtime", "lat_0p05", "lon_0p05"),
                np.full((64, 1, 1, 1, 1), 273.15, dtype=float),
                {"units": "K"},
            )
        },
        coords={
            "sample": samples,
            "lead_time": np.array([0], dtype="timedelta64[h]"),
            "lead_subtime": np.array([0], dtype="timedelta64[h]"),
            "lat_0p05": [48.1],
            "lon_0p05": [11.7],
            "init_time": np.datetime64("2026-09-14T00:00:00"),
        },
    )

    with pytest.raises(RuntimeError, match="refused before downloading"):
        _extract_point_day_ensemble(
            dataset,
            latitude=48.1,
            longitude=11.7,
            observation_date=date(2026, 9, 14),
            max_read_bytes=1,
        )

    scenarios, _ = _extract_point_day_ensemble(
        dataset,
        latitude=48.1,
        longitude=11.7,
        observation_date=date(2026, 9, 14),
        max_read_bytes=1,
        allow_large_read=True,
    )
    assert scenarios == pytest.approx([0.0] * 64)


def test_gcs_check_uses_bounded_requester_pays_listing(tmp_path) -> None:
    fake = _FakeStorageClient(prefixes=("weathernext_3_0_0/zarr/2026_to_present/",))
    client = WeatherNextGcsClient(
        _settings(tmp_path), storage_client=fake, credentials=SimpleNamespace()
    )

    report = client.check_access()

    assert report["billing_project"] == "weather-508105"
    assert report["requester_pays"] is True
    assert report["access_granted"] is True
    assert fake.list_calls[0]["delimiter"] == "/"
    assert fake.list_calls[0]["max_results"] == 1000
    assert fake.bucket_args is not None
    assert fake.bucket_args[1] == {"user_project": "weather-508105"}


def test_explicit_store_prefix_is_normalized_without_listing(tmp_path) -> None:
    fake = _FakeStorageClient()
    client = WeatherNextGcsClient(
        _settings(
            tmp_path,
            weathernext_gcs_store_prefix=(
                "gs://weathernext3_spatial/weathernext_3_0_0/zarr/"
                "2026_to_present/20260914_00hr_01_preds/predictions.zarr"
            ),
        ),
        storage_client=fake,
        credentials=SimpleNamespace(),
    )

    prefix, parsed_init = client._resolve_store_prefix(init_time_utc=None)  # noqa: SLF001

    assert prefix.endswith("20260914_00hr_01_preds/predictions.zarr")
    assert parsed_init == datetime(2026, 9, 14, tzinfo=UTC)
    assert fake.list_calls == []


def test_dynamic_store_discovery_uses_latest_run_not_full_object_listing(tmp_path) -> None:
    fake = _FakeStorageClient(
        prefix_map={
            "weathernext_3_0_0/zarr/2026_to_present/20260914_": (
                "weathernext_3_0_0/zarr/2026_to_present/20260913_18hr_01_preds/",
                "weathernext_3_0_0/zarr/2026_to_present/20260914_00hr_01_preds/",
            ),
        }
    )
    client = WeatherNextGcsClient(
        _settings(tmp_path), storage_client=fake, credentials=SimpleNamespace()
    )

    prefix, parsed_init = client._resolve_store_prefix(  # noqa: SLF001
        init_time_utc=None,
        observation_date=date(2026, 9, 14),
    )

    assert prefix.endswith("20260914_00hr_01_preds/predictions.zarr")
    assert parsed_init == datetime(2026, 9, 14, tzinfo=UTC)
    assert len(fake.list_calls) == 1
    prefix_value = fake.list_calls[0].get("prefix")
    assert isinstance(prefix_value, str)
    assert prefix_value.endswith("20260914_")


def test_status_mentions_configured_billing_project_until_snapshot_exists(tmp_path) -> None:
    status = WeatherNextProvider(_settings(tmp_path)).status()

    assert status.state == "access_pending"
    assert "weather-508105" in status.message


def test_station_local_day_is_converted_to_utc() -> None:
    start, end, canonical = _observation_window_utc(date(2026, 9, 14), "America/New_York")

    assert canonical == "America/New_York"
    assert start == datetime(2026, 9, 14, 4, tzinfo=UTC)
    assert end == datetime(2026, 9, 15, 4, tzinfo=UTC)


def test_read_uses_dataset_init_and_writes_timezone_provenance(tmp_path) -> None:
    samples = np.arange(64)
    data = np.full((64, 1, 1, 1, 1), 293.15)
    dataset = xr.Dataset(
        {
            "station_head_temperature_2m": (
                ("sample", "lead_time", "lead_subtime", "lat_0p05", "lon_0p05"),
                data,
                {"units": "K"},
            )
        },
        coords={
            "sample": samples,
            "lead_time": np.array([0], dtype="timedelta64[h]"),
            "lead_subtime": np.array([0], dtype="timedelta64[h]"),
            "lat_0p05": [48.1],
            "lon_0p05": [348.3],
            "init_time": np.datetime64("2026-09-14T00:00:00"),
        },
    )
    fake = _FakeStorageClient()
    client = WeatherNextGcsClient(
        _settings(
            tmp_path,
            weathernext_gcs_store_prefix=(
                "weathernext_3_0_0/zarr/2026_to_present/20260914_00hr_01_preds/predictions.zarr"
            ),
        ),
        storage_client=fake,
        credentials=SimpleNamespace(),
        dataset_factory=lambda _: dataset,
    )

    snapshot = client.read_point_day_ensemble(
        latitude=48.1,
        longitude=-11.7,
        location="Munich",
        observation_date=date(2026, 9, 14),
        timezone_name="Europe/Berlin",
    )

    assert snapshot.init_time_utc == datetime(2026, 9, 14, tzinfo=UTC)
    assert snapshot.observation_timezone == "Europe/Berlin"
    assert snapshot.scenario_max_c == pytest.approx([20.0] * 64)
    assert snapshot.source_uri.endswith("20260914_00hr_01_preds/predictions.zarr")
