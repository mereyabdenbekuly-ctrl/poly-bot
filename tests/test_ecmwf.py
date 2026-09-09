from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from polybot.ecmwf import (
    IFS_ENS_MEMBER_NUMBERS,
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


def _index_url(step: int) -> str:
    return (
        "https://data.ecmwf.int/forecasts/20260908/18z/ifs/0p25/enfo/"
        f"20260908180000-{step}h-enfo-ef.index"
    )


def _index_payload(
    step: int,
    *,
    parameter: str = "mx2t3",
    data_type: str = "pf",
    stream: str = "enfo",
    members: Sequence[int] = IFS_ENS_MEMBER_NUMBERS,
) -> bytes:
    rows = []
    for member in members:
        rows.append(
            json.dumps(
                {
                    "domain": "g",
                    "date": "20260908",
                    "time": "1800",
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
    step: int, *, parameter: str = "mx2t3"
) -> dict[tuple[str, str, str | None], HttpResponse]:
    index_url = _index_url(step)
    data_url = index_url.removesuffix(".index") + ".grib2"
    result: dict[tuple[str, str, str | None], HttpResponse] = {
        ("GET", index_url, None): HttpResponse(
            200,
            {"Last-Modified": PUBLISHED_HEADER},
            _index_payload(step, parameter=parameter),
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


def test_latest_conservative_init_is_at_least_nine_hours_old() -> None:
    assert (
        EcmwfIfsEnsAdapter.latest_conservative_init(datetime(2026, 9, 9, 4, 30, tzinfo=UTC)) == INIT
    )


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
