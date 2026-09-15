from __future__ import annotations

import json
from datetime import UTC, date, datetime
from pathlib import Path
from types import SimpleNamespace

import numpy as np

from polybot.config import Settings
from polybot.storage import Storage
from polybot.weathernext_autonomy import (
    autonomous_refresh_preflight,
    derive_refresh_targets,
    read_approved_manifest_sequentially,
    verify_read_approval,
)
from polybot.weathernext_manifest import (
    build_full_ensemble_read_manifest,
    expected_station_local_day_hours,
)


def test_target_inventory_is_read_only_and_requires_full_identity(tmp_path: Path) -> None:
    storage = Storage(tmp_path / "polybot.sqlite3")
    inventory = derive_refresh_targets(
        storage.path,
        now_utc=datetime(2026, 9, 15, 12, tzinfo=UTC),
        output_path=tmp_path / "targets.json",
    )

    assert inventory.targets == []
    assert json.loads((tmp_path / "targets.json").read_text()) ["targets"] == []


def _estimate(tmp_path: Path) -> tuple[Path, Path, Path, Path]:
    valid_times = [
        value.isoformat()
        for value in expected_station_local_day_hours(
            date(2026, 9, 15), "Europe/Amsterdam"
        )
    ]
    estimate: dict[str, object] = {
        "target_id": "EHAM",
        "event_id": "event-1",
        "metadata_only": True,
        "payload_read": False,
        "source_uri": (
            "gs://weathernext3_spatial/weathernext_3_0_0/zarr/"
            "2026_to_present/20260915_00hr_01_preds/predictions.zarr"
        ),
        "init_time_utc": datetime(2026, 9, 15, tzinfo=UTC).isoformat(),
        "location": "EHAM",
        "latitude": 52.3,
        "longitude": 4.8,
        "observation_date": "2026-09-15",
        "observation_timezone": "Europe/Amsterdam",
        "selected_valid_times_utc": valid_times,
        "expected_network_bytes": 1024,
        "global_uncompressed_array_bytes": 148 * 1024**3,
        "global_array_is_not_mandatory_transfer": True,
        "whole_shard_is_not_mandatory_transfer": True,
        "selection": {
            "dimensions": ["sample", "lead_time", "lat_0p05", "lon_0p05"],
            "selected_chunk_coordinates": [[0, 0, 0, 0]],
            "nearest_latitude": 52.3,
            "nearest_longitude": 4.8,
            "latitude_index": 0,
            "longitude_index": 0,
            "valid_index_tuples": [
                {
                    "lead_time_index": hour,
                    "valid_time_utc": valid_times[hour],
                }
                for hour in range(24)
            ],
        },
        "array": {
            "shape": [64, 24, 1, 1],
            "chunk_shape": [64, 24, 1, 1],
            "dtype": "float32",
            "dtype_size_bytes": 4,
            "codecs": [{"name": "bytes"}, {"name": "zstd"}],
            "chunk_key_encoding": {"configuration": {"separator": "/"}},
            "selected_chunk_logical_bytes": 64 * 24 * 4,
            "selected_chunk_object_sizes": [1024],
            "selected_chunk_object_metadata": [{"generation": "1"}],
            "sharding_detected": False,
            "transfer_unit": "chunk",
        },
    }
    manifest = build_full_ensemble_read_manifest(
        estimate,
        billing_project="weather-508105",
        max_network_bytes=10_000,
        max_objects=4,
        max_object_bytes=2_000,
        snapshot_root=tmp_path / "snapshots",
    )
    manifest_path = tmp_path / "read-manifest.json"
    manifest_path.write_text(manifest.model_dump_json(indent=2))
    approval_path = tmp_path / "read-approval.json"
    approval_path.write_text(
        json.dumps(
            {
                "schema_version": "weathernext-full-ensemble-read-approval/v1",
                "manifest_sha256": manifest.manifest_sha256,
                "approved": True,
                "approved_at_utc": "2026-09-15T01:00:00Z",
                "approved_by": "test",
                "max_network_bytes": 10_000,
                "max_objects": 4,
                "max_object_bytes": 2_000,
            }
        )
    )
    return manifest_path, approval_path, tmp_path / "snapshots", tmp_path / "latest-index.json"


class _Blob:
    size = 1024
    generation = "1"
    etag = None
    md5_hash = None
    crc32c = None

    def reload(self) -> None:
        return None


class _Bucket:
    def blob(self, _key: str) -> _Blob:
        return _Blob()


class _Array:
    shape = (64, 24, 1, 1)
    chunks = (64, 24, 1, 1)

    def get_basic_selection(self, _selection: object) -> np.ndarray:
        values = np.zeros(self.shape, dtype=np.float32)
        for member in range(64):
            values[member, :, 0, 0] = 273.15 + member + np.arange(24, dtype=np.float32)
        return values


class _Group:
    def __init__(self) -> None:
        self.array = _Array()

    def __getitem__(self, _name: str) -> _Array:
        return self.array

    def close(self) -> None:
        return None


class _Client:
    bucket_name = "weathernext3_spatial"

    def __init__(self, _settings: Settings) -> None:
        self._bucket = _Bucket()

    def open_sequential_zarr_group(self, _prefix: str) -> _Group:
        return _Group()


def test_approval_is_required_and_reader_streams_one_chunk(monkeypatch, tmp_path: Path) -> None:
    manifest_path, approval_path, snapshot_root, index_path = _estimate(tmp_path)
    assert verify_read_approval(manifest_path, tmp_path / "missing.json").state == "missing"
    # The reader imports the client from polybot.weathernext at call time.
    monkeypatch.setattr("polybot.weathernext.WeatherNextGcsClient", _Client)
    settings = Settings(
        database_path=tmp_path / "db.sqlite3",
        weathernext_enabled=True,
        weathernext_gcs_project="weather-508105",
        weathernext_full_refresh_enabled=True,
    )

    result = read_approved_manifest_sequentially(
        settings,
        manifest_path=manifest_path,
        approval_path=approval_path,
        snapshot_root=snapshot_root,
        index_path=index_path,
    )

    assert result.payload_read is True
    assert result.object_count == 1
    assert result.bytes_read == 1024
    snapshot = json.loads(Path(result.snapshot_paths[0]).read_text())
    assert len(snapshot["trajectories"]) == 64
    assert len(snapshot["trajectories"][0]["values_c"]) == 24
    assert json.loads(index_path.read_text())["entries"][0]["member_count"] == 64

    # The immutable artifact is reused on the next timer tick; no second
    # client/group construction (and therefore no second payload GET) occurs.
    class _FailingClient:
        def __init__(self, _settings: Settings) -> None:
            raise AssertionError("reused snapshots must not instantiate a GCS client")

    monkeypatch.setattr("polybot.weathernext.WeatherNextGcsClient", _FailingClient)
    reused = read_approved_manifest_sequentially(
        settings,
        manifest_path=manifest_path,
        approval_path=approval_path,
        snapshot_root=snapshot_root,
        index_path=index_path,
    )
    assert reused.payload_read is False
    assert reused.bytes_read == 0
    assert reused.snapshots_written == 0
    assert len(reused.snapshot_paths) == 1


def test_approved_probe_reads_one_block_without_publishing_snapshot(
    monkeypatch, tmp_path: Path
) -> None:
    manifest_path, approval_path, snapshot_root, index_path = _estimate(tmp_path)
    monkeypatch.setattr("polybot.weathernext.WeatherNextGcsClient", _Client)
    settings = Settings(
        database_path=tmp_path / "db.sqlite3",
        weathernext_enabled=True,
        weathernext_gcs_project="weather-508105",
        weathernext_full_refresh_enabled=True,
    )

    result = read_approved_manifest_sequentially(
        settings,
        manifest_path=manifest_path,
        approval_path=approval_path,
        snapshot_root=snapshot_root,
        index_path=index_path,
        probe_only=True,
    )

    assert result.payload_read is True
    assert result.probe_only is True
    assert result.object_count == 1
    assert result.bytes_read == 1024
    assert result.decoded_shape == [64, 24, 1, 1]
    assert result.decoded_bytes == 64 * 24 * 4
    assert result.elapsed_seconds is not None
    assert result.elapsed_seconds >= 0
    assert result.snapshots_written == 0
    assert result.snapshot_paths == []
    assert not list(snapshot_root.glob("*.json"))
    assert not index_path.exists()


def test_incomplete_coverage_blocks_approval_and_payload_client(
    monkeypatch, tmp_path: Path
) -> None:
    valid_times = [
        value.isoformat()
        for value in expected_station_local_day_hours(
            date(2026, 9, 15), "Europe/Amsterdam"
        )[:-1]
    ]
    estimate: dict[str, object] = {
        "target_id": "EHAM",
        "metadata_only": True,
        "payload_read": False,
        "source_uri": (
            "gs://weathernext3_spatial/weathernext_3_0_0/zarr/"
            "2026_to_present/20260915_00hr_01_preds/predictions.zarr"
        ),
        "init_time_utc": datetime(2026, 9, 15, tzinfo=UTC).isoformat(),
        "location": "EHAM",
        "latitude": 52.3,
        "longitude": 4.8,
        "observation_date": "2026-09-15",
        "observation_timezone": "Europe/Amsterdam",
        "selected_valid_times_utc": valid_times,
        "expected_network_bytes": 1024,
        "global_array_is_not_mandatory_transfer": True,
        "whole_shard_is_not_mandatory_transfer": True,
        "selection": {
            "dimensions": ["sample", "lead_time", "lat_0p05", "lon_0p05"],
            "selected_chunk_coordinates": [[0, 0, 0, 0]],
            "nearest_latitude": 52.3,
            "nearest_longitude": 4.8,
            "latitude_index": 0,
            "longitude_index": 0,
            "valid_index_tuples": [
                {"lead_time_index": index, "valid_time_utc": value}
                for index, value in enumerate(valid_times)
            ],
        },
        "array": {
            "shape": [64, 24, 1, 1],
            "chunk_shape": [64, 24, 1, 1],
            "dtype": "float32",
            "dtype_size_bytes": 4,
            "codecs": [{"name": "bytes"}, {"name": "zstd"}],
            "chunk_key_encoding": {"configuration": {"separator": "/"}},
            "selected_chunk_logical_bytes": 64 * 24 * 4,
            "selected_chunk_object_sizes": [1024],
            "selected_chunk_object_metadata": [{"generation": "1"}],
            "sharding_detected": False,
            "transfer_unit": "chunk",
        },
    }
    manifest = build_full_ensemble_read_manifest(
        estimate,
        billing_project="weather-508105",
        max_network_bytes=10_000,
        max_objects=4,
        max_object_bytes=2_000,
        snapshot_root=tmp_path / "snapshots",
    )
    assert manifest.approval_gate.state == "blocked_incomplete_coverage"
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(manifest.model_dump_json())
    approval_path = tmp_path / "approval.json"
    approval_path.write_text(
        json.dumps(
            {
                "schema_version": "weathernext-full-ensemble-read-approval/v1",
                "manifest_sha256": manifest.manifest_sha256,
                "approved": True,
                "approved_at_utc": "2026-09-15T01:00:00Z",
                "approved_by": "test",
                "max_network_bytes": 10_000,
                "max_objects": 4,
                "max_object_bytes": 2_000,
            }
        )
    )
    approval = verify_read_approval(manifest_path, approval_path)
    assert approval.state == "coverage_blocked"
    assert approval.incomplete_target_ids == ["EHAM"]

    class _NoPayloadClient:
        def __init__(self, _settings: Settings) -> None:
            raise AssertionError("coverage guard must run before GCS client construction")

    monkeypatch.setattr("polybot.weathernext.WeatherNextGcsClient", _NoPayloadClient)
    settings = Settings(
        database_path=tmp_path / "db.sqlite3",
        weathernext_enabled=True,
        weathernext_gcs_project="weather-508105",
        weathernext_full_refresh_enabled=True,
    )
    try:
        read_approved_manifest_sequentially(
            settings,
            manifest_path=manifest_path,
            approval_path=approval_path,
            snapshot_root=tmp_path / "snapshots",
            index_path=tmp_path / "latest-index.json",
        )
    except RuntimeError as error:
        assert "incomplete station-local-day coverage" in str(error)
    else:
        raise AssertionError("incomplete coverage must prevent payload reads")


def test_autonomous_preflight_never_reads_payload_without_approval(tmp_path: Path) -> None:
    storage = Storage(tmp_path / "db.sqlite3")
    status = autonomous_refresh_preflight(
        storage.path,
        settings=Settings(database_path=storage.path, weathernext_enabled=False),
        targets_path=tmp_path / "targets.json",
        manifest_path=tmp_path / "manifest.json",
        approval_path=tmp_path / "approval.json",
        status_path=tmp_path / "status.json",
    )
    assert status.payload_read is False
    assert status.approval.state == "missing"
    assert json.loads((tmp_path / "status.json").read_text())["payload_read"] is False


def test_provider_resolves_immutable_snapshot_from_index(tmp_path: Path) -> None:
    from polybot.weathernext import WeatherNextProvider, WeatherNextSnapshot

    snapshot_path = tmp_path / "snapshots" / "EHAM.json"
    snapshot = WeatherNextSnapshot(
        init_time_utc=datetime(2026, 9, 15, tzinfo=UTC),
        received_at_utc=datetime(2026, 9, 15, 1, tzinfo=UTC),
        location="EHAM",
        observation_date=date(2026, 9, 15),
        scenario_max_c=[20.0] * 64,
        source_uri=(
            "gs://weathernext3_spatial/weathernext_3_0_0/zarr/"
            "2026_to_present/20260915_00hr_01_preds/predictions.zarr"
        ),
    )
    snapshot_path.parent.mkdir(parents=True)
    snapshot_path.write_text(snapshot.model_dump_json())
    index_path = tmp_path / "latest-index.json"
    index_path.write_text(
        json.dumps(
            {
                "schema_version": "weathernext-full-snapshot-index/v1",
                "entries": [
                    {
                        "target_id": "EHAM",
                        "station_id": "EHAM",
                        "location": "EHAM",
                        "observation_date": "2026-09-15",
                        "path": str(snapshot_path),
                    }
                ],
            }
        )
    )
    settings = Settings(
        database_path=tmp_path / "db.sqlite3",
        weathernext_enabled=True,
        weathernext_snapshot_path=None,
        weathernext_snapshot_index_path=str(index_path),
        weathernext_gcs_project=None,
    )
    provider = WeatherNextProvider(settings)
    status = provider.status()
    assert status.state == "snapshot_available"
    rules = SimpleNamespace(
        location="EHAM",
        observation_date=date(2026, 9, 15),
        station_or_authority="EHAM",
    )
    resolved = provider.snapshot_for(rules)  # type: ignore[arg-type]
    assert resolved is not None
    assert resolved.scenario_max_c == [20.0] * 64
