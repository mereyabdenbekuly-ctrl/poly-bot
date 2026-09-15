from __future__ import annotations

import json
from datetime import UTC, date, datetime
from pathlib import Path
from typing import cast

import pytest

from polybot.config import Settings
from polybot.weathernext import WeatherNextGcsClient
from polybot.weathernext_manifest import (
    WeatherNextReadApproval,
    build_full_ensemble_read_manifest,
    estimate_and_build_full_ensemble_read_manifest_batch,
    expected_station_local_day_hours,
    recompute_manifest_sha256,
    validate_read_approval,
    verify_manifest_sha256,
    write_full_ensemble_read_manifest,
)

_INIT = datetime(2026, 9, 15, tzinfo=UTC)
_SOURCE = (
    "gs://weathernext3_spatial/weathernext_3_0_0/zarr/"
    "2026_to_present/20260915_00hr_01_preds/predictions.zarr"
)


def _estimate(
    target_id: str,
    coordinates: list[list[int]],
    sizes: list[int],
    *,
    source: str = _SOURCE,
    sharding: bool = False,
    observation_date: str = "2026-09-15",
    observation_timezone: str = "Europe/Amsterdam",
    selected_valid_times_utc: list[str] | None = None,
) -> dict[str, object]:
    if selected_valid_times_utc is None:
        selected_valid_times_utc = [
            value.isoformat()
            for value in expected_station_local_day_hours(
                date.fromisoformat(observation_date), observation_timezone
            )
        ]
    return {
        "target_id": target_id,
        "metadata_only": True,
        "payload_read": False,
        "source_uri": source,
        "init_time_utc": _INIT.isoformat(),
        "location": target_id,
        "latitude": 52.3,
        "longitude": 4.8,
        "observation_date": observation_date,
        "observation_timezone": observation_timezone,
        "selected_valid_times_utc": selected_valid_times_utc,
        "expected_network_bytes": sum(sizes) if sizes else 1000,
        "global_uncompressed_array_bytes": 148 * 1024**3,
        "global_array_is_not_mandatory_transfer": True,
        "whole_shard_is_not_mandatory_transfer": True,
        "selection": {
            "dimensions": ["sample", "lead_time", "lat_0p05", "lon_0p05"],
            "selected_chunk_coordinates": coordinates,
            "nearest_latitude": 52.3,
            "nearest_longitude": 4.8,
            "latitude_index": 10,
            "longitude_index": 20,
        },
        "array": {
            "shape": [64, 24, 721, 1440],
            "chunk_shape": [64, 1, 721, 1440],
            "dtype": "float32",
            "dtype_size_bytes": 4,
            "codecs": [
                {"name": "bytes", "configuration": {"endian": "little"}},
                {"name": "zstd", "configuration": {"level": 3}},
            ],
            "chunk_key_encoding": {"configuration": {"separator": "/"}},
            "selected_chunk_logical_bytes": len(coordinates) * 64 * 721 * 1440 * 4,
            "selected_chunk_object_sizes": sizes,
            "sharding_detected": sharding,
            "transfer_unit": "shard" if sharding else "chunk",
        },
    }


def test_batch_manifest_deduplicates_shared_chunks_and_stays_closed(tmp_path: Path) -> None:
    first = _estimate("EHAM", [[0, 0, 0, 0], [0, 1, 0, 0]], [100, 200])
    second = _estimate("EDDF", [[0, 1, 0, 0], [0, 2, 0, 0]], [200, 400])

    manifest = build_full_ensemble_read_manifest(
        [first, second],
        billing_project="weather-508105",
        max_network_bytes=1_000,
        max_objects=10,
        max_object_bytes=500,
        generated_at_utc=datetime(2026, 9, 15, 12, tzinfo=UTC),
        snapshot_root=tmp_path / "snapshots",
    )

    assert manifest.approval_gate.state == "awaiting_operator_approval"
    assert manifest.approval_gate.approved is False
    assert manifest.payload_read is False
    assert manifest.approval_gate.sequential_block_read is True
    assert manifest.approval_gate.max_inflight_objects == 1
    assert manifest.approval_gate.object_count == 3
    assert manifest.approval_gate.expected_network_bytes == 700
    assert manifest.array["global_array_is_not_mandatory_transfer"] is True
    assert (
        manifest.array["selected_chunk_logical_bytes_per_target_sum"]
        == manifest.array["selected_chunk_logical_bytes"]
    )
    assert cast(int, manifest.array["unique_selected_chunk_logical_bytes"]) > 0
    assert (
        manifest.array["selected_chunk_logical_bytes_basis"]
        == "per_target_sum_shared_objects_may_repeat"
    )
    assert len(manifest.targets) == 2
    shared = [item for item in manifest.compressed_objects if len(item.target_ids) == 2]
    assert len(shared) == 1
    assert shared[0].compressed_bytes == 200
    assert all(
        path.startswith(str(tmp_path / "snapshots")) for path in manifest.snapshot_target_paths
    )
    assert manifest.manifest_sha256 == manifest.manifest_sha256.lower()
    assert verify_manifest_sha256(manifest)
    assert recompute_manifest_sha256(manifest) == manifest.manifest_sha256


def test_manifest_blocks_missing_head_sizes_without_payload_read() -> None:
    estimate = _estimate("EHAM", [[0, 0, 0, 0]], [])
    manifest = build_full_ensemble_read_manifest(
        estimate,
        station_id="EHAM",
        billing_project="weather-508105",
        max_network_bytes=10_000,
        max_objects=10,
    )

    assert manifest.approval_gate.state == "blocked_incomplete_metadata"
    assert manifest.approval_gate.compressed_sizes_complete is False
    assert manifest.approval_gate.approved is False
    assert manifest.payload_read is False
    assert manifest.compressed_objects[0].compressed_bytes is None


def test_manifest_blocks_per_object_limit() -> None:
    estimate = _estimate("EHAM", [[0, 0, 0, 0]], [501])
    manifest = build_full_ensemble_read_manifest(
        estimate,
        station_id="EHAM",
        billing_project="weather-508105",
        max_network_bytes=10_000,
        max_objects=10,
        max_object_bytes=500,
    )
    assert manifest.approval_gate.state == "blocked_object_size_limit"
    assert manifest.approval_gate.largest_compressed_object_bytes == 501


def test_manifest_blocks_incomplete_station_day_even_when_hour_count_looks_valid() -> None:
    expected = [
        value.isoformat()
        for value in expected_station_local_day_hours(date(2026, 9, 15), "Europe/Amsterdam")
    ]
    # Keep 24 values but replace one expected instant with a duplicate-hour gap.
    shifted = expected[:12] + ["2026-09-15T10:30:00+00:00"] + expected[13:]
    manifest = build_full_ensemble_read_manifest(
        _estimate(
            "EHAM",
            [[0, 0, 0, 0]],
            [100],
            selected_valid_times_utc=shifted,
        ),
        billing_project="weather-508105",
        max_network_bytes=10_000,
        max_objects=10,
    )
    assert manifest.approval_gate.state == "blocked_incomplete_coverage"
    assert manifest.approval_gate.coverage_complete is False
    assert manifest.approval_gate.incomplete_target_ids == ["EHAM"]
    assert manifest.period["all_targets_complete_station_local_day"] is False
    assert manifest.period["coverage_basis"] == "exact_hourly_station_local_day"
    assert manifest.targets[0]["complete_station_local_day"] is False


def test_manifest_accepts_dst_23_and_25_hour_station_days() -> None:
    spring = build_full_ensemble_read_manifest(
        _estimate(
            "EHAM",
            [[0, 0, 0, 0]],
            [100],
            observation_date="2026-03-29",
        ),
        billing_project="weather-508105",
        max_network_bytes=10_000,
        max_objects=10,
    )
    assert spring.approval_gate.state == "awaiting_operator_approval"
    assert spring.targets[0]["valid_hour_count"] == 23

    autumn = build_full_ensemble_read_manifest(
        _estimate(
            "EHAM",
            [[0, 0, 0, 0]],
            [100],
            observation_date="2026-10-25",
        ),
        billing_project="weather-508105",
        max_network_bytes=10_000,
        max_objects=10,
    )
    assert autumn.approval_gate.state == "awaiting_operator_approval"
    assert autumn.targets[0]["valid_hour_count"] == 25


def test_manifest_reports_mixed_dates_but_checks_each_target_individually() -> None:
    manifest = build_full_ensemble_read_manifest(
        [
            _estimate("EHAM", [[0, 0, 0, 0]], [100]),
            _estimate(
                "ZUCK",
                [[0, 1, 0, 0]],
                [200],
                observation_date="2026-09-16",
            ),
        ],
        billing_project="weather-508105",
        max_network_bytes=10_000,
        max_objects=10,
    )
    assert manifest.approval_gate.state == "awaiting_operator_approval"
    assert manifest.period["mixed_observation_dates"] is True
    assert manifest.period["incomplete_target_ids"] == []


def test_manifest_rejects_mixed_release() -> None:
    other = _estimate(
        "EDDF",
        [[0, 0, 0, 0]],
        [100],
        source=_SOURCE.replace("weathernext_3_0_0", "weathernext_3_0_1"),
    )
    with pytest.raises(ValueError, match="one release"):
        build_full_ensemble_read_manifest(
            [_estimate("EHAM", [[0, 0, 0, 0]], [100]), other],
            billing_project="weather-508105",
            max_network_bytes=10_000,
            max_objects=10,
        )


def test_manifest_write_is_json_and_does_not_flip_gate(tmp_path: Path) -> None:
    manifest = build_full_ensemble_read_manifest(
        _estimate("EHAM", [[0, 0, 0, 0]], [100]),
        station_id="EHAM",
        billing_project="weather-508105",
        max_network_bytes=10_000,
        max_objects=10,
    )
    output = write_full_ensemble_read_manifest(manifest, tmp_path / "read-manifest.json")
    payload = json.loads(output.read_text(encoding="utf-8"))
    assert payload["manifest_sha256"] == manifest.manifest_sha256
    assert payload["approval_gate"]["approved"] is False
    assert payload["payload_read"] is False
    assert not (tmp_path / "read-manifest.json.tmp").exists()
    assert verify_manifest_sha256(payload)


def test_approval_is_separate_and_bound_to_exact_manifest_limits() -> None:
    manifest = build_full_ensemble_read_manifest(
        _estimate("EHAM", [[0, 0, 0, 0]], [100]),
        station_id="EHAM",
        billing_project="weather-508105",
        max_network_bytes=10_000,
        max_objects=10,
        max_object_bytes=500,
    )
    approval = WeatherNextReadApproval(
        manifest_sha256=manifest.manifest_sha256,
        approved_at_utc=datetime(2026, 9, 15, 12, tzinfo=UTC),
        approved_by="owner",
        max_network_bytes=10_000,
        max_objects=10,
        max_object_bytes=500,
    )
    assert (
        validate_read_approval(manifest, approval, now_utc=datetime(2026, 9, 15, 13, tzinfo=UTC))
        == approval
    )
    changed = approval.model_copy(update={"max_network_bytes": 20_000})
    with pytest.raises(ValueError, match="exactly match"):
        validate_read_approval(manifest, changed)


class _BatchClient:
    billing_project = "weather-508105"

    def __init__(self) -> None:
        self.calls: list[datetime | None] = []

    def estimate_point_day_read(self, **kwargs: object) -> dict[str, object]:
        init = kwargs.get("init_time_utc")
        self.calls.append(init if isinstance(init, datetime) else None)
        estimate = _estimate(
            str(kwargs["location"]),
            [[0, 0, 0, 0]],
            [100],
        )
        if isinstance(init, datetime):
            estimate["init_time_utc"] = init.isoformat()
        return estimate


def test_batch_estimator_pins_one_release_without_reading_payload(tmp_path: Path) -> None:
    client = _BatchClient()
    manifest = estimate_and_build_full_ensemble_read_manifest_batch(
        client,  # type: ignore[arg-type]
        targets=[
            {
                "station_id": "EHAM",
                "latitude": 52.3,
                "longitude": 4.8,
                "location": "EHAM",
                "observation_date": date(2026, 9, 15).isoformat(),
                "timezone": "Europe/Amsterdam",
            },
            {
                "station_id": "EDDF",
                "latitude": 50.0,
                "longitude": 8.6,
                "location": "EDDF",
                "observation_date": date(2026, 9, 15).isoformat(),
                "timezone": "Europe/Berlin",
            },
        ],
        max_network_bytes=10_000,
        snapshot_root=tmp_path / "snapshots",
    )
    assert client.calls[0] is None
    assert client.calls[1] == _INIT
    assert manifest.approval_gate.state == "awaiting_operator_approval"
    assert manifest.execution["sequential_block_read"] is True


def test_adapter_head_metadata_includes_identity_without_payload_get(tmp_path: Path) -> None:
    class Blob:
        size = 123
        generation = 7
        etag = "etag-7"
        md5_hash = "md5"
        crc32c = "crc"
        reload_calls = 0
        download_calls = 0

        def reload(self) -> None:
            self.reload_calls += 1

        def download_as_bytes(self) -> bytes:
            self.download_calls += 1
            raise AssertionError("forecast payload GET is forbidden")

    class Bucket:
        requester_pays = True

        def __init__(self) -> None:
            self.value = Blob()

        def blob(self, _key: str) -> Blob:
            return self.value

    class StorageClient:
        def __init__(self) -> None:
            self.value = Bucket()

        def bucket(self, *_args: object, **_kwargs: object) -> Bucket:
            return self.value

    storage = StorageClient()
    client = WeatherNextGcsClient(
        Settings(
            database_path=tmp_path / "polybot.sqlite3",
            weathernext_enabled=True,
            weathernext_gcs_project="weather-508105",
        ),
        storage_client=storage,
    )
    record = client._raw_chunk_object_metadata(  # noqa: SLF001
        "weathernext_3_0_0/zarr/2026_to_present/20260915_00hr_01_preds/predictions.zarr",
        "station_head_temperature_2m",
        (0, 1, 2, 3),
        {"chunk_key_encoding": {"configuration": {"separator": "/"}}},
    )
    assert record is not None
    assert record["size"] == 123
    assert record["generation"] == "7"
    assert record["etag"] == "etag-7"
    assert record["md5_hash"] == "md5"
    assert record["crc32c"] == "crc"
    assert storage.value.value.reload_calls == 1
    assert storage.value.value.download_calls == 0
