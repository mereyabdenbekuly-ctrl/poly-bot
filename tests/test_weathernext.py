from __future__ import annotations

from datetime import UTC, date, datetime
from types import SimpleNamespace
from typing import cast

import numpy as np
import pytest
import xarray as xr

from polybot.config import Settings
from polybot.weathernext import (
    WeatherNextGcsClient,
    WeatherNextProvider,
    WeatherNextSnapshot,
    WeatherNextStatisticsGcsClient,
    WeatherNextStatisticsPoint,
    WeatherNextStatisticsSnapshot,
    _extract_point_day_ensemble,
    _normalise_statistics_metadata,
    _observation_window_utc,
    _statistics_extract_point_hours,
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


def _statistics_dataset() -> xr.Dataset:
    lead_time = np.arange(6, dtype="timedelta64[h]")
    lat = np.array([48.3, 48.4], dtype=np.float32)
    lon = np.array([11.7, 11.8], dtype=np.float32)
    base = np.arange(24, dtype=np.float32).reshape(6, 2, 2) + 273.15
    variables = {
        f"temperature_2m_{name}": (("lead_time", "lat_0p1", "lon_0p1"), base, {"units": "K"})
        for name in ("mean", "p10", "p25", "p50", "p75", "p90")
    }
    return xr.Dataset(
        variables,
        coords={
            "lead_time": lead_time,
            "lat_0p1": lat,
            "lon_0p1": lon,
            "init_time": np.datetime64("2026-09-14T00:00:00"),
        },
    )


def test_statistics_metadata_normalization_preserves_chunks_and_codecs() -> None:
    normalized = _normalise_statistics_metadata(
        {
            "shape": [48, 1801, 3600],
            "data_type": "float32",
            "chunk_grid": {"configuration": {"chunk_shape": [1, 1801, 3600]}},
            "codecs": [
                {"name": "bytes", "configuration": {"endian": "little"}},
                {"name": "zstd", "configuration": {"level": 0}},
            ],
            "dimension_names": ["lead_time", "lat_0p1", "lon_0p1"],
        }
    )

    assert normalized["shape"] == [48, 1801, 3600]
    assert normalized["chunk_shape"] == [1, 1801, 3600]
    assert normalized["dtype_size_bytes"] == 4
    codecs = cast(list[dict[str, object]], normalized["codecs"])
    assert [codec["name"] for codec in codecs] == ["bytes", "zstd"]


def test_statistics_extracts_bounded_summary_without_scenarios() -> None:
    dataset = _statistics_dataset()
    metadata = {
        name: {
            "shape": [6, 2, 2],
            "data_type": "float32",
            "chunk_grid": {"configuration": {"chunk_shape": [1, 2, 2]}},
            "codecs": [{"name": "bytes"}, {"name": "zstd"}],
            "dimension_names": ["lead_time", "lat_0p1", "lon_0p1"],
        }
        for name in (
            "temperature_2m_mean",
            "temperature_2m_p10",
            "temperature_2m_p25",
            "temperature_2m_p50",
            "temperature_2m_p75",
            "temperature_2m_p90",
        )
    }
    points, init_time, provenance = _statistics_extract_point_hours(
        dataset,
        latitude=48.35,
        longitude=11.75,
        observation_date=date(2026, 9, 14),
        timezone_name="UTC",
        variable_name="temperature_2m",
        max_hours=2,
        now_utc=datetime(2026, 9, 14, tzinfo=UTC),
        include_past_hours=True,
        read_limit_bytes=10_000,
        metadata_by_name=metadata,
        chunk_size_lookup=lambda _name, _chunk, _meta: 100,
    )

    assert init_time == datetime(2026, 9, 14, tzinfo=UTC)
    assert len(points) == 2
    assert points[0].temperature_mean_c == pytest.approx(0.0, abs=1e-5)
    assert points[1].p90_c == pytest.approx(4.0, abs=1e-5)
    assert provenance["expected_network_bytes"] == 1_200
    assert provenance["global_uncompressed_array_bytes"] == 6 * 2 * 2 * 4 * 6
    selection = cast(dict[str, object], provenance["selection"])
    arrays = cast(dict[str, object], provenance["arrays"])
    mean_array = cast(dict[str, object], arrays["temperature_2m_mean"])
    assert selection["selection_truncated"] is True
    assert mean_array["selected_chunk_count"] == 2
    assert "codecs" in mean_array


def test_raw_estimate_is_metadata_only_and_does_not_use_global_array_as_transfer(
    tmp_path, monkeypatch
) -> None:
    lead_time = np.array([0, 6, 12, 18], dtype="timedelta64[h]")
    subtime = np.arange(6, dtype="timedelta64[h]")
    dataset = xr.Dataset(
        {
            "station_head_temperature_2m": (
                ("sample", "lead_time", "lead_subtime", "lat_0p05", "lon_0p05"),
                np.zeros((64, 4, 6, 2, 3), dtype=np.float32),
                {"units": "K"},
            )
        },
        coords={
            "sample": np.arange(64),
            "lead_time": lead_time,
            "lead_subtime": subtime,
            "lat_0p05": [48.3, 48.35],
            "lon_0p05": [11.7, 11.75, 11.8],
            "init_time": np.datetime64("2026-09-14T00:00:00"),
        },
    )
    fake = _FakeStorageClient()
    client = WeatherNextGcsClient(
        _settings(
            tmp_path,
            weathernext_gcs_store_prefix=(
                "weathernext_3_0_0/zarr/2026_to_present/"
                "20260914_00hr_01_preds/predictions.zarr"
            ),
        ),
        storage_client=fake,
        credentials=SimpleNamespace(),
        dataset_factory=lambda _: dataset,
    )
    monkeypatch.setattr(
        client,
        "_zarr_metadata",
        lambda _prefix: {
            "station_head_temperature_2m": {
                "shape": [64, 4, 6, 2, 3],
                "data_type": "float32",
                "chunk_grid": {"configuration": {"chunk_shape": [1, 1, 6, 2, 3]}},
                "chunk_key_encoding": {"configuration": {"separator": "/"}},
                "codecs": [{"name": "bytes"}, {"name": "zstd"}],
                "dimension_names": [
                    "sample",
                    "lead_time",
                    "lead_subtime",
                    "lat_0p05",
                    "lon_0p05",
                ],
            }
        },
    )
    monkeypatch.setattr(
        client,
        "_raw_chunk_object_sizes",
        lambda _prefix, _name, coordinates, _meta: [100] * len(coordinates),
    )

    report = client.estimate_point_day_read(
        latitude=48.35,
        longitude=11.75,
        location="Munich",
        observation_date=date(2026, 9, 14),
        timezone_name="UTC",
    )

    assert report["metadata_only"] is True
    assert report["payload_read"] is False
    assert report["global_array_is_not_mandatory_transfer"] is True
    assert report["whole_shard_transfer_applicable"] is False
    raw_array = cast(dict[str, object], report["array"])
    assert raw_array["sharding_detected"] is False
    assert raw_array["shape"] == [64, 4, 6, 2, 3]
    assert raw_array["chunk_shape"] == [1, 1, 6, 2, 3]
    assert raw_array["selected_chunk_count"] == 256
    expected_network = cast(int, report["expected_network_bytes"])
    global_bytes = cast(int, report["global_uncompressed_array_bytes"])
    assert expected_network == 25_600
    assert global_bytes > expected_network


def test_statistics_snapshot_has_summary_boundary_and_no_member_field() -> None:
    snapshot = WeatherNextStatisticsSnapshot(
        init_time_utc=datetime(2026, 9, 14, tzinfo=UTC),
        received_at_utc=datetime(2026, 9, 14, 1, tzinfo=UTC),
        location="Munich",
        station_id="eddm",
        latitude=48.35,
        longitude=11.75,
        observation_date=date(2026, 9, 14),
        observation_timezone="Europe/Berlin",
        variable="temperature_2m",
        points=[
            WeatherNextStatisticsPoint(
                valid_time_utc=datetime(2026, 9, 14, 1, tzinfo=UTC),
                temperature_mean_c=10,
                p10_c=9,
                p25_c=9.5,
                p50_c=10,
                p75_c=10.5,
                p90_c=11,
            )
        ],
        source_uri="gs://weathernext3_statistics_spatial/test/predictions.zarr",
        read_provenance={},
    )

    payload = snapshot.model_dump()
    assert payload["mode"] == "SUMMARY_ONLY"
    assert payload["station_id"] == "EDDM"
    assert "scenario_max_c" not in payload
    assert "members" not in payload


def test_statistics_reader_falls_back_from_nonfinite_publication(tmp_path, monkeypatch) -> None:
    client = WeatherNextStatisticsGcsClient(
        _settings(tmp_path),
        storage_client=_FakeStorageClient(),
        credentials=SimpleNamespace(),
    )
    first = datetime(2026, 9, 14, 13, tzinfo=UTC)
    previous = datetime(2026, 9, 14, 12, tzinfo=UTC)
    calls: list[datetime | None] = []

    def resolve(*, init_time_utc, observation_date=None, timezone_name=None):  # type: ignore[no-untyped-def]
        del observation_date, timezone_name
        if init_time_utc is None:
            return "run13/predictions.zarr", first
        return "run12/predictions.zarr", previous

    def read_once(*, init_time_utc, **kwargs):  # type: ignore[no-untyped-def]
        del kwargs
        calls.append(init_time_utc)
        if init_time_utc is None:
            raise RuntimeError("WeatherNext statistics value is not finite")
        return WeatherNextStatisticsSnapshot(
            init_time_utc=previous,
            received_at_utc=previous,
            location="EHAM",
            station_id="EHAM",
            latitude=52.3,
            longitude=4.8,
            observation_date=date(2026, 9, 15),
            observation_timezone="Europe/Amsterdam",
            variable="station_head_temperature_2m",
            points=[
                WeatherNextStatisticsPoint(
                    valid_time_utc=datetime(2026, 9, 14, 22, tzinfo=UTC),
                    temperature_mean_c=18,
                    p10_c=17,
                    p25_c=17.5,
                    p50_c=18,
                    p75_c=18.5,
                    p90_c=19,
                )
            ],
            source_uri="gs://weathernext3_statistics_spatial/run12/predictions.zarr",
            read_provenance={},
        )

    monkeypatch.setattr(client, "_resolve_store_prefix", resolve)
    monkeypatch.setattr(client, "_read_point_hours_once", read_once)

    snapshot = client.read_point_hours(
        latitude=52.3,
        longitude=4.8,
        location="EHAM",
        station_id="EHAM",
        observation_date=date(2026, 9, 15),
        timezone_name="Europe/Amsterdam",
        max_hours=1,
    )

    assert calls == [None, previous]
    assert snapshot.init_time_utc == previous
    fallback = cast(dict[str, object], snapshot.read_provenance["publication_read_fallback"])
    assert fallback["failed_run_init_time_utc"] == first.isoformat()


def test_statistics_access_and_load_states_are_independent(tmp_path, monkeypatch) -> None:
    snapshot_path = tmp_path / "summary.json"
    snapshot_path.write_text(
        WeatherNextStatisticsSnapshot(
            init_time_utc=datetime(2026, 9, 14, tzinfo=UTC),
            received_at_utc=datetime(2026, 9, 14, 1, tzinfo=UTC),
            location="EHAM",
            station_id="EHAM",
            latitude=52.3,
            longitude=4.8,
            observation_date=date(2026, 9, 15),
            observation_timezone="Europe/Amsterdam",
            variable="station_head_temperature_2m",
            points=[
                WeatherNextStatisticsPoint(
                    valid_time_utc=datetime(2026, 9, 14, 22, tzinfo=UTC),
                    temperature_mean_c=18,
                    p10_c=17,
                    p25_c=17.5,
                    p50_c=18,
                    p75_c=18.5,
                    p90_c=19,
                )
            ],
            source_uri="gs://weathernext3_statistics_spatial/run/predictions.zarr",
            read_provenance={},
        ).model_dump_json(),
        encoding="utf-8",
    )
    settings = _settings(
        tmp_path,
        weathernext_statistics_snapshot_path=str(snapshot_path),
        weathernext_statistics_variable="station_head_temperature_2m",
    )

    def denied(_self):  # type: ignore[no-untyped-def]
        raise RuntimeError("temporary access outage")

    monkeypatch.setattr(WeatherNextStatisticsGcsClient, "check_access", denied)
    status = WeatherNextProvider(settings).statistics_status(check_access=True)

    assert status.access_state == "error"
    assert status.load_state == "available"
    assert status.init_time_utc == datetime(2026, 9, 14, tzinfo=UTC)
