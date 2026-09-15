from __future__ import annotations

import hashlib
import json
import os
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from polybot.ecmwf import (
    IFS_ENS_MEMBER_NUMBERS,
    EcmwfArchiveRetentionPolicy,
    EcmwfIfsEnsAdapter,
    EcmwfProduct,
    EcmwfState,
    HttpResponse,
    daily_max_steps,
    daily_maxima,
)

INIT = datetime(2026, 9, 8, 18, tzinfo=UTC)
FETCHED = datetime(2026, 9, 9, 4, 30, tzinfo=UTC)
PUBLISHED_HEADER = "Wed, 09 Sep 2026 01:04:00 GMT"
PUBLISHED = datetime(2026, 9, 9, 1, 4, tzinfo=UTC)


@pytest.fixture(autouse=True)
def _restore_tmp_permissions(tmp_path: Path):
    """Let pytest remove immutable archive fixtures after each test."""

    yield
    for current, directories, files in os.walk(tmp_path, topdown=False, followlinks=False):
        current_path = Path(current)
        for name in files:
            candidate = current_path / name
            if not candidate.is_symlink():
                candidate.chmod(0o600)
        for name in directories:
            candidate = current_path / name
            if not candidate.is_symlink():
                candidate.chmod(0o700)
    tmp_path.chmod(0o700)


class FakeTransport:
    def __init__(self, responses: Mapping[tuple[str, str, str | None], HttpResponse]) -> None:
        self.responses = dict(responses)
        self.calls: list[tuple[str, str, str | None]] = []

    def request(
        self,
        method: str,
        url: str,
        *,
        headers: Mapping[str, str] | None,
        max_bytes: int,
    ) -> HttpResponse:
        byte_range = None if headers is None else headers.get("Range")
        key = (method, url, byte_range)
        self.calls.append(key)
        response = self.responses[key]
        assert len(response.content) <= max_bytes
        return response


class FakeDecoder:
    def __init__(self, unavailable: str | None = None) -> None:
        self.unavailable = unavailable
        self.calls: list[tuple[Path, tuple[str, ...], int, str, int | None]] = []

    def availability_error(self) -> str | None:
        return self.unavailable

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
        assert expected_init_time_utc == INIT
        assert path.read_bytes() == b"".join(
            member.to_bytes(2, "big") for member in expected_members
        )
        self.calls.append(
            (
                path,
                tuple(points),
                expected_step_hours,
                expected_parameter,
                expected_interval_hours,
            )
        )
        return {
            point_id: {
                member: member / 10 + expected_step_hours / 100 + index
                for member in expected_members
            }
            for index, point_id in enumerate(points)
        }


def _index_url(step: int, *, init: datetime = INIT) -> str:
    return (
        f"https://data.ecmwf.int/forecasts/{init:%Y%m%d}/{init:%H}z/ifs/0p25/enfo/"
        f"{init:%Y%m%d%H%M%S}-{step}h-enfo-ef.index"
    )


def _index_payload(
    step: int,
    *,
    parameter: str = "mx2t3",
    data_type: str = "pf",
    stream: str = "enfo",
    members: Sequence[int] = IFS_ENS_MEMBER_NUMBERS,
    init: datetime = INIT,
) -> bytes:
    rows = []
    for member in members:
        rows.append(
            json.dumps(
                {
                    "domain": "g",
                    "date": init.strftime("%Y%m%d"),
                    "time": init.strftime("%H%M"),
                    "class": "od",
                    "type": data_type,
                    "stream": stream,
                    "step": str(step),
                    "levtype": "sfc",
                    "number": str(member),
                    "param": parameter,
                    "_offset": (member - 1) * 2,
                    "_length": 2,
                }
            )
        )
    return ("\n".join(rows) + "\n").encode()


def _responses_for_step(
    step: int, *, parameter: str = "mx2t3", init: datetime = INIT
) -> dict[tuple[str, str, str | None], HttpResponse]:
    index_url = _index_url(step, init=init)
    data_url = index_url.removesuffix(".index") + ".grib2"
    result: dict[tuple[str, str, str | None], HttpResponse] = {
        ("GET", index_url, None): HttpResponse(
            200,
            {"Last-Modified": PUBLISHED_HEADER},
            _index_payload(step, parameter=parameter, init=init),
        )
    }
    for member in IFS_ENS_MEMBER_NUMBERS:
        start = (member - 1) * 2
        end = start + 1
        result[("GET", data_url, f"bytes={start}-{end}")] = HttpResponse(
            206,
            {
                "Content-Range": f"bytes {start}-{end}/100",
                "Last-Modified": PUBLISHED_HEADER,
            },
            member.to_bytes(2, "big"),
        )
    return result


def _seed_archive(root: Path, init: datetime):
    result = EcmwfIfsEnsAdapter(
        archive_root=root,
        transport=FakeTransport(_responses_for_step(3, init=init)),
        decoder=FakeDecoder(),
        clock=lambda: FETCHED,
    ).fetch_archive(init_time_utc=init, steps=[3])
    assert result.archive is not None
    return result.archive


def test_latest_conservative_init_is_at_least_nine_hours_old() -> None:
    assert (
        EcmwfIfsEnsAdapter.latest_conservative_init(datetime(2026, 9, 9, 4, 30, tzinfo=UTC)) == INIT
    )


def test_raw_archive_retention_defaults_are_bounded() -> None:
    policy = EcmwfArchiveRetentionPolicy()

    assert policy.max_completed_releases == 28
    assert policy.max_completed_bytes == 8 * 1024**3
    assert policy.min_free_bytes == 50 * 1024**3
    assert policy.min_free_fraction == 0.25


def test_probe_is_pending_before_deadline_and_unavailable_after(tmp_path: Path) -> None:
    response = HttpResponse(404, {}, b"")
    transport = FakeTransport({("GET", _index_url(3), None): response})
    pending = EcmwfIfsEnsAdapter(
        archive_root=tmp_path,
        transport=transport,
        decoder=FakeDecoder(),
        clock=lambda: INIT + timedelta(hours=8),
    ).probe(init_time_utc=INIT, steps=[3])
    unavailable = EcmwfIfsEnsAdapter(
        archive_root=tmp_path,
        transport=transport,
        decoder=FakeDecoder(),
        clock=lambda: INIT + timedelta(hours=11),
    ).probe(init_time_utc=INIT, steps=[3])

    assert pending.state == EcmwfState.PENDING
    assert unavailable.state == EcmwfState.UNAVAILABLE
    assert "publication deadline" in unavailable.message


def test_single_run_is_never_accepted_as_ensemble(tmp_path: Path) -> None:
    adapter = EcmwfIfsEnsAdapter(
        archive_root=tmp_path,
        transport=FakeTransport(
            {
                ("GET", _index_url(3), None): HttpResponse(
                    200,
                    {"Last-Modified": PUBLISHED_HEADER},
                    _index_payload(3, data_type="cf"),
                )
            }
        ),
        decoder=FakeDecoder(),
        clock=lambda: INIT + timedelta(hours=11),
    )

    status = adapter.probe(init_time_utc=INIT, steps=[3])

    assert status.state == EcmwfState.UNAVAILABLE
    assert "single-run/control" in status.message


def test_batch_downloads_once_and_decodes_many_points(tmp_path: Path) -> None:
    responses: dict[tuple[str, str, str | None], HttpResponse] = {}
    for step in (3, 6):
        responses.update(_responses_for_step(step))
    transport = FakeTransport(responses)
    decoder = FakeDecoder()
    adapter = EcmwfIfsEnsAdapter(
        archive_root=tmp_path,
        transport=transport,
        decoder=decoder,
        clock=lambda: FETCHED,
    )
    points = {
        "EDDM": (48.3538, 11.7861),
        "RJTT": (35.5494, 139.7798),
        "NZWN": (-41.3272, 174.8053),
    }

    result = adapter.fetch_points(init_time_utc=INIT, steps=[6, 3], points=points)

    assert result.status.state == EcmwfState.AVAILABLE
    assert result.archive is not None
    assert result.snapshot is not None
    assert len(result.snapshot.points) == 3
    assert all(len(point.scenarios) == 50 for point in result.snapshot.points)
    assert len([call for call in transport.calls if call[2] is not None]) == 100
    assert len(decoder.calls) == 2
    assert all(call[1] == tuple(points) for call in decoder.calls)
    assert all(call[3:] == ("mx2t3", 3) for call in decoder.calls)

    first = result.snapshot.points[0].scenarios[0]
    assert first.points[0].interval_start_utc == INIT
    assert first.points[0].interval_end_utc == INIT + timedelta(hours=3)
    assert first.points[1].interval_start_utc == INIT + timedelta(hours=3)
    assert result.archive.fetched_at_utc == FETCHED
    assert result.archive.published_at_utc == PUBLISHED

    range_count = len([call for call in transport.calls if call[2] is not None])
    again = adapter.fetch_points(init_time_utc=INIT, steps=[3, 6], points=points)
    assert again.archive is not None
    assert again.archive.archive_id == result.archive.archive_id
    assert len([call for call in transport.calls if call[2] is not None]) == range_count


def test_immutable_archive_contains_indexes_and_verified_grib(tmp_path: Path) -> None:
    responses = _responses_for_step(3)
    adapter = EcmwfIfsEnsAdapter(
        archive_root=tmp_path,
        transport=FakeTransport(responses),
        decoder=FakeDecoder(),
        clock=lambda: FETCHED,
    )

    result = adapter.fetch_archive(init_time_utc=INIT, steps=[3])

    assert result.archive is not None
    archive = result.archive
    assert archive.archive_path.stat().st_mode & 0o222 == 0
    artifact = archive.artifacts[0]
    grib = archive.archive_path / artifact.relative_path
    index = archive.archive_path / artifact.index_relative_path
    assert grib.stat().st_mode & 0o222 == 0
    assert index.stat().st_mode & 0o222 == 0
    assert hashlib.sha256(grib.read_bytes()).hexdigest() == artifact.sha256
    assert hashlib.sha256(index.read_bytes()).hexdigest() == artifact.index_sha256
    manifest = json.loads((archive.archive_path / "manifest.json").read_text())
    assert manifest["ensemble"]["member_count"] == 50
    assert manifest["ensemble"]["control_member_included"] is False
    assert manifest["ensemble"]["single_run_substitution"] is False
    assert manifest["parameter"] == "mx2t3"


def test_server_ignoring_range_is_rejected_without_archive(tmp_path: Path) -> None:
    responses = _responses_for_step(3)
    data_url = _index_url(3).removesuffix(".index") + ".grib2"
    responses[("GET", data_url, "bytes=0-1")] = HttpResponse(
        200, {"Last-Modified": PUBLISHED_HEADER}, b"\x00\x01"
    )
    result = EcmwfIfsEnsAdapter(
        archive_root=tmp_path,
        transport=FakeTransport(responses),
        decoder=FakeDecoder(),
        clock=lambda: FETCHED,
    ).fetch_archive(init_time_utc=INIT, steps=[3])

    assert result.status.state == EcmwfState.UNAVAILABLE
    assert result.archive is None
    assert list(tmp_path.iterdir()) == []


def test_missing_decoder_is_honestly_unavailable_without_download(tmp_path: Path) -> None:
    transport = FakeTransport({})
    result = EcmwfIfsEnsAdapter(
        archive_root=tmp_path,
        transport=transport,
        decoder=FakeDecoder("ecCodes missing"),
        clock=lambda: FETCHED,
    ).fetch_points(
        init_time_utc=INIT,
        steps=[3],
        points={"EDDM": (48.3538, 11.7861)},
    )

    assert result.status.state == EcmwfState.UNAVAILABLE
    assert result.snapshot is None
    assert transport.calls == []


def test_daily_max_helpers_require_exact_mx2t3_windows(tmp_path: Path) -> None:
    responses: dict[tuple[str, str, str | None], HttpResponse] = {}
    for step in (3, 6):
        responses.update(_responses_for_step(step))
    adapter = EcmwfIfsEnsAdapter(
        archive_root=tmp_path,
        transport=FakeTransport(responses),
        decoder=FakeDecoder(),
        clock=lambda: FETCHED,
    )
    start = INIT
    end = INIT + timedelta(hours=6)
    assert daily_max_steps(init_time_utc=INIT, day_start_utc=start, day_end_utc=end) == (3, 6)

    result = adapter.fetch_daily_max_points(
        init_time_utc=INIT,
        day_start_utc=start,
        day_end_utc=end,
        points={"EDDM": (48.3538, 11.7861)},
    )
    assert result.snapshot is not None
    maxima = daily_maxima(result.snapshot.points[0], day_start_utc=start, day_end_utc=end)
    assert len(maxima) == 50
    assert maxima[0].temperature_c == pytest.approx(0.16)

    with pytest.raises(ValueError, match="align"):
        daily_max_steps(
            init_time_utc=INIT,
            day_start_utc=INIT + timedelta(hours=1),
            day_end_utc=INIT + timedelta(hours=25),
        )


def test_instantaneous_2t_is_supported_but_not_default_daily_max(tmp_path: Path) -> None:
    responses = _responses_for_step(3, parameter="2t")
    result = EcmwfIfsEnsAdapter(
        archive_root=tmp_path,
        transport=FakeTransport(responses),
        decoder=FakeDecoder(),
        clock=lambda: FETCHED,
    ).fetch_points(
        init_time_utc=INIT,
        steps=[3],
        points={"EDDM": (48.3538, 11.7861)},
        product=EcmwfProduct.INSTANTANEOUS_2T,
    )

    assert result.snapshot is not None
    point = result.snapshot.points[0].scenarios[0].points[0]
    assert point.interval_start_utc is None
    assert point.interval_end_utc is None
    assert result.status.parameter == "2t"


@pytest.mark.parametrize(
    ("total_gib", "free_gib", "reason_fragment"),
    [
        (200, 49, "below 53687091200 bytes"),
        (300, 60, "below 25.00%"),
    ],
)
def test_retention_refuses_download_below_free_space_floors(
    tmp_path: Path,
    total_gib: int,
    free_gib: int,
    reason_fragment: str,
) -> None:
    gib = 1024**3
    transport = FakeTransport(_responses_for_step(3))
    adapter = EcmwfIfsEnsAdapter(
        archive_root=tmp_path,
        transport=transport,
        decoder=FakeDecoder(),
        clock=lambda: FETCHED,
        retention_policy=EcmwfArchiveRetentionPolicy(),
        disk_usage=lambda _: (total_gib * gib, (total_gib - free_gib) * gib, free_gib * gib),
    )

    result = adapter.fetch_archive(init_time_utc=INIT, steps=[3])

    assert result.status.state == EcmwfState.UNAVAILABLE
    assert result.archive is None
    assert result.retention is not None
    assert not result.retention.within_limits
    assert reason_fragment in (result.retention.reason or "")
    assert [call for call in transport.calls if call[2] is not None] == []
    assert list(tmp_path.iterdir()) == []


def test_retention_prunes_oldest_completed_releases_after_commit_only(
    tmp_path: Path,
) -> None:
    inits = tuple(INIT - timedelta(hours=offset) for offset in (18, 12, 6))
    old = tuple(_seed_archive(tmp_path, init) for init in inits)
    protected = old[0]
    protected_marker = tmp_path / f"{protected.archive_id}.protected"
    protected_marker.write_text("diagnostic release; retain\n")
    partial = tmp_path / ".ifs-ens-partial-diagnostic"
    partial.mkdir()
    (partial / "README.txt").write_text("incomplete diagnostic\n")
    corrupt_hash_directory = tmp_path / ("f" * 64)
    corrupt_hash_directory.mkdir()
    (corrupt_hash_directory / "manifest.json.partial").write_text("{}\n")
    new_init = INIT
    transport = FakeTransport(_responses_for_step(3, init=new_init))
    adapter = EcmwfIfsEnsAdapter(
        archive_root=tmp_path,
        transport=transport,
        decoder=FakeDecoder(),
        clock=lambda: FETCHED,
        retention_policy=EcmwfArchiveRetentionPolicy(
            max_completed_releases=2,
            max_completed_bytes=1024**4,
            min_free_bytes=0,
            min_free_fraction=0,
        ),
        disk_usage=lambda _: (1024**4, 0, 1024**4),
    )

    result = adapter.fetch_archive(init_time_utc=new_init, steps=[3])

    assert result.status.state == EcmwfState.AVAILABLE
    assert result.archive is not None
    assert result.archive.archive_path.exists()
    assert protected.archive_path.exists()
    assert not old[1].archive_path.exists()
    assert not old[2].archive_path.exists()
    assert protected_marker.exists()
    assert partial.is_dir()
    assert corrupt_hash_directory.is_dir()
    assert result.retention is not None
    assert result.retention.within_limits
    assert result.retention.completed_releases == 2
    assert result.retention.protected_releases == 1
    assert result.retention.pruned_releases == 2
    assert result.retention.preserved_diagnostics >= 3


def test_retention_enforces_completed_byte_limit(tmp_path: Path) -> None:
    first = _seed_archive(tmp_path, INIT - timedelta(hours=12))
    second = _seed_archive(tmp_path, INIT - timedelta(hours=6))
    one_archive_bytes = sum(path.stat().st_size for path in first.archive_path.iterdir())
    assert one_archive_bytes == sum(path.stat().st_size for path in second.archive_path.iterdir())
    transport = FakeTransport(_responses_for_step(3, init=INIT))
    adapter = EcmwfIfsEnsAdapter(
        archive_root=tmp_path,
        transport=transport,
        decoder=FakeDecoder(),
        clock=lambda: FETCHED,
        retention_policy=EcmwfArchiveRetentionPolicy(
            max_completed_releases=10,
            max_completed_bytes=one_archive_bytes * 2,
            min_free_bytes=0,
            min_free_fraction=0,
        ),
        disk_usage=lambda _: (1024**4, 0, 1024**4),
    )

    result = adapter.fetch_archive(init_time_utc=INIT, steps=[3])

    assert result.archive is not None
    assert result.retention is not None
    assert result.retention.within_limits
    assert result.retention.completed_releases == 2
    assert result.retention.completed_bytes <= one_archive_bytes * 2
    assert result.retention.pruned_releases == 1
    assert not first.archive_path.exists()
    assert second.archive_path.exists()


def test_failed_archive_does_not_trigger_retention_pruning(tmp_path: Path) -> None:
    first = _seed_archive(tmp_path, INIT - timedelta(hours=12))
    second = _seed_archive(tmp_path, INIT - timedelta(hours=6))
    responses = _responses_for_step(3, init=INIT)
    data_url = _index_url(3).removesuffix(".index") + ".grib2"
    responses[("GET", data_url, "bytes=0-1")] = HttpResponse(
        200, {"Last-Modified": PUBLISHED_HEADER}, b"\x00\x01"
    )
    adapter = EcmwfIfsEnsAdapter(
        archive_root=tmp_path,
        transport=FakeTransport(responses),
        decoder=FakeDecoder(),
        clock=lambda: FETCHED,
        retention_policy=EcmwfArchiveRetentionPolicy(
            max_completed_releases=1,
            max_completed_bytes=1024**4,
            min_free_bytes=0,
            min_free_fraction=0,
        ),
        disk_usage=lambda _: (1024**4, 0, 1024**4),
    )

    result = adapter.fetch_archive(init_time_utc=INIT, steps=[3])

    assert result.status.state == EcmwfState.UNAVAILABLE
    assert result.archive is None
    assert first.archive_path.exists()
    assert second.archive_path.exists()
    assert len([path for path in tmp_path.iterdir() if path.is_dir()]) == 2


def test_protected_releases_can_block_new_archive_without_range_reads(tmp_path: Path) -> None:
    protected = _seed_archive(tmp_path, INIT - timedelta(hours=6))
    (tmp_path / f"{protected.archive_id}.protected").touch()
    transport = FakeTransport(_responses_for_step(3, init=INIT))
    adapter = EcmwfIfsEnsAdapter(
        archive_root=tmp_path,
        transport=transport,
        decoder=FakeDecoder(),
        clock=lambda: FETCHED,
        retention_policy=EcmwfArchiveRetentionPolicy(
            max_completed_releases=1,
            max_completed_bytes=1024**4,
            min_free_bytes=0,
            min_free_fraction=0,
        ),
        disk_usage=lambda _: (1024**4, 0, 1024**4),
    )

    result = adapter.fetch_archive(init_time_utc=INIT, steps=[3])

    assert result.status.state == EcmwfState.UNAVAILABLE
    assert result.archive is None
    assert result.retention is not None
    assert result.retention.protected_releases == 1
    assert "leave no retention slot" in (result.retention.reason or "")
    assert [call for call in transport.calls if call[2] is not None] == []
    assert protected.archive_path.exists()


def test_retention_rechecks_free_space_before_immutable_commit(tmp_path: Path) -> None:
    gib = 1024**3
    samples = iter(
        (
            (200 * gib, 100 * gib, 100 * gib),
            (200 * gib, 151 * gib, 49 * gib),
            (200 * gib, 151 * gib, 49 * gib),
        )
    )
    adapter = EcmwfIfsEnsAdapter(
        archive_root=tmp_path,
        transport=FakeTransport(_responses_for_step(3)),
        decoder=FakeDecoder(),
        clock=lambda: FETCHED,
        retention_policy=EcmwfArchiveRetentionPolicy(),
        disk_usage=lambda _: next(samples),
    )

    result = adapter.fetch_archive(init_time_utc=INIT, steps=[3])

    assert result.status.state == EcmwfState.UNAVAILABLE
    assert result.archive is None
    assert result.retention is not None
    assert "archive commit" in (result.retention.reason or "")
    assert list(tmp_path.iterdir()) == []


def test_retention_blocks_before_download_for_digest_corruption(
    tmp_path: Path,
) -> None:
    corrupt = _seed_archive(tmp_path, INIT - timedelta(hours=6))
    artifact = corrupt.archive_path / corrupt.artifacts[0].relative_path
    corrupted_body = bytearray(artifact.read_bytes())
    corrupted_body[0] ^= 0xFF
    artifact.chmod(0o644)
    artifact.write_bytes(corrupted_body)
    artifact.chmod(0o444)
    transport = FakeTransport(_responses_for_step(3, init=INIT))
    adapter = EcmwfIfsEnsAdapter(
        archive_root=tmp_path,
        transport=transport,
        decoder=FakeDecoder(),
        clock=lambda: FETCHED,
        retention_policy=EcmwfArchiveRetentionPolicy(
            max_completed_releases=1,
            max_completed_bytes=1024**4,
            min_free_bytes=0,
            min_free_fraction=0,
        ),
        disk_usage=lambda _: (1024**4, 0, 1024**4),
    )

    result = adapter.fetch_archive(init_time_utc=INIT, steps=[3])

    assert result.archive is None
    assert corrupt.archive_path.exists()
    assert result.retention is not None
    assert not result.retention.within_limits
    assert "failed immutable validation" in (result.retention.reason or "")
    assert [call for call in transport.calls if call[2] is not None] == []


def test_retention_never_deletes_forged_empty_artifact_manifest(tmp_path: Path) -> None:
    valid = _seed_archive(tmp_path, INIT - timedelta(hours=6))
    source_manifest = json.loads((valid.archive_path / "manifest.json").read_text())
    source_manifest["artifacts"] = []
    stable = {
        key: value
        for key, value in source_manifest.items()
        if key not in {"schema_version", "archive_id", "fetched_at_utc"}
    }
    forged_id = hashlib.sha256(
        json.dumps(stable, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    forged = tmp_path / forged_id
    forged.mkdir()
    source_manifest["archive_id"] = forged_id
    (forged / "manifest.json").write_text(
        json.dumps(source_manifest, sort_keys=True, separators=(",", ":")) + "\n"
    )
    (forged / "manifest.json").chmod(0o444)
    forged.chmod(0o555)

    transport = FakeTransport(_responses_for_step(3, init=INIT))
    adapter = EcmwfIfsEnsAdapter(
        archive_root=tmp_path,
        transport=transport,
        decoder=FakeDecoder(),
        clock=lambda: FETCHED,
        retention_policy=EcmwfArchiveRetentionPolicy(
            max_completed_releases=1,
            max_completed_bytes=1024**4,
            min_free_bytes=0,
            min_free_fraction=0,
        ),
        disk_usage=lambda _: (1024**4, 0, 1024**4),
    )

    result = adapter.fetch_archive(init_time_utc=INIT, steps=[3])

    assert result.archive is not None
    assert result.retention is not None
    assert result.retention.within_limits
    assert forged.exists()
    assert not valid.archive_path.exists()
    assert result.retention.preserved_diagnostics >= 1


def test_existing_archive_retention_failure_blocks_point_extraction(tmp_path: Path) -> None:
    _seed_archive(tmp_path, INIT)
    policy = EcmwfArchiveRetentionPolicy(
        max_completed_releases=28,
        max_completed_bytes=1024**4,
        min_free_bytes=200,
        min_free_fraction=0,
    )
    adapter = EcmwfIfsEnsAdapter(
        archive_root=tmp_path,
        transport=FakeTransport(_responses_for_step(3)),
        decoder=FakeDecoder(),
        clock=lambda: FETCHED,
        retention_policy=policy,
        disk_usage=lambda _: (1000, 0, 100),
    )

    result = adapter.fetch_points(
        init_time_utc=INIT,
        steps=[3],
        points={"EDDM": (48.3538, 11.7861)},
    )

    assert result.status.state == EcmwfState.UNAVAILABLE
    assert result.snapshot is None
    assert result.archive is None
    assert "below 200 bytes" in result.status.message


def test_existing_archive_fast_path_enforces_retention(tmp_path: Path) -> None:
    oldest = _seed_archive(tmp_path, INIT - timedelta(hours=12))
    middle = _seed_archive(tmp_path, INIT - timedelta(hours=6))
    newest = _seed_archive(tmp_path, INIT)
    transport = FakeTransport(_responses_for_step(3))
    adapter = EcmwfIfsEnsAdapter(
        archive_root=tmp_path,
        transport=transport,
        decoder=FakeDecoder(),
        clock=lambda: FETCHED,
        retention_policy=EcmwfArchiveRetentionPolicy(
            max_completed_releases=2,
            max_completed_bytes=1024**4,
            min_free_bytes=0,
            min_free_fraction=0,
        ),
        disk_usage=lambda _: (1024**4, 0, 1024**4),
    )

    result = adapter.fetch_archive(init_time_utc=INIT, steps=[3])

    assert result.status.state == EcmwfState.AVAILABLE
    assert result.archive is not None
    assert result.archive.archive_id == newest.archive_id
    assert result.retention is not None
    assert result.retention.within_limits
    assert result.retention.completed_releases == 2
    assert result.retention.pruned_releases == 1
    assert not oldest.archive_path.exists()
    assert middle.archive_path.exists()
    assert newest.archive_path.exists()
    assert [call for call in transport.calls if call[2] is not None] == []
