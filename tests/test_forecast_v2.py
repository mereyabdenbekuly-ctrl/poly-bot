from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path

import pytest

from polybot.config import Settings
from polybot.forecast_store import ForecastStore
from polybot.forecast_v2 import (
    EcmwfShadowSnapshot,
    ForecastV2Calibrator,
    OpenMeteoEcmwfIfsEns,
    StationCorrectionProfile,
    _empirical_probabilities,
    build_v2_forecast,
)
from polybot.models import Bracket
from polybot.observations import ObservationHistory


def _history() -> ObservationHistory:
    return ObservationHistory(
        station_id="TEST",
        station_name="Test Airport",
        station_timezone="UTC",
        source_url="https://www.weather.gov/wrh/timeseries?site=test",
        observation_date=date(2026, 9, 9),
        fetched_at_utc=datetime(2026, 9, 9, 12, tzinfo=UTC),
        day_started=True,
        day_finished=False,
        expected_cadence_minutes=60,
        observations=[],
        observed_max_c=Decimal("25"),
        displayed_max_c=Decimal("25"),
        latest_observed_at_utc=None,
        stale=False,
    )


def _snapshot(tmp_path: Path) -> EcmwfShadowSnapshot:
    return EcmwfShadowSnapshot(
        upstream_model="ecmwf_ifs025",
        requested_location="Test",
        matched_location="Test Airport",
        latitude=1,
        longitude=2,
        timezone="UTC",
        observation_date="2026-09-09",
        fetched_at_utc=datetime(2026, 9, 9, 12, tzinfo=UTC),
        source_uri="https://ensemble-api.open-meteo.com/v1/ensemble",
        payload_sha256="a" * 64,
        archive_path=str(tmp_path / "archive"),
        member_max_c=[Decimal("24"), Decimal("25"), Decimal("26")] * 10,
    )


def test_v2_applies_observed_floor_and_returns_normalized_distribution(
    tmp_path: Path,
) -> None:
    result = build_v2_forecast(
        snapshot=_snapshot(tmp_path),
        observations=_history(),
        brackets={
            "low": Bracket(market_id="low", label="24°C or below", lower=None, upper=25),
            "mid": Bracket(market_id="mid", label="25°C", lower=25, upper=26),
            "high": Bracket(market_id="high", label="26°C or higher", lower=26, upper=None),
        },
        profile=StationCorrectionProfile(
            state="insufficient_history",
            scope="station:TEST",
            sample_count=0,
            bias_c=Decimal(0),
            spread_scale=Decimal(1),
            residual_sigma_c=Decimal("1.5"),
        ),
        issued_at_utc=datetime(2026, 9, 9, 12, tzinfo=UTC),
    )

    assert min(result.corrected_member_max_c) == Decimal("25")
    assert result.v2_probabilities["low"] == 0
    assert sum(result.v2_probabilities.values(), Decimal(0)) == pytest.approx(Decimal(1))
    assert result.intraday_features["feature_adjustment_applied"] is False


def test_station_calibrator_is_explicitly_untrained_without_resolved_history(
    tmp_path: Path,
) -> None:
    from polybot.storage import Storage

    database = tmp_path / "forecast.sqlite3"
    Storage(database)
    store = ForecastStore(database)
    profile = ForecastV2Calibrator(
        Settings(_env_file=None), store  # type: ignore[call-arg]
    ).profile(station_id="TEST", as_of_utc=datetime(2026, 9, 9, 12, tzinfo=UTC))

    assert profile.state == "insufficient_history"
    assert profile.sample_count == 0
    assert profile.bias_c == 0


def test_ecmwf_json_archive_is_idempotent_and_read_only(tmp_path: Path) -> None:
    settings = Settings(
        _env_file=None,  # type: ignore[call-arg]
        ecmwf_json_archive_root=tmp_path,
    )
    provider = OpenMeteoEcmwfIfsEns(settings)
    payload = b'{"request":{},"response":{"daily":{}}}'
    import hashlib

    digest = hashlib.sha256(payload).hexdigest()
    first = provider._archive(  # noqa: SLF001
        digest=digest,
        payload=payload,
        fetched_at=datetime(2026, 9, 9, tzinfo=UTC),
    )
    second = provider._archive(  # noqa: SLF001
        digest=digest,
        payload=payload,
        fetched_at=datetime(2026, 9, 9, 1, tzinfo=UTC),
    )

    assert first == second
    assert (first / "snapshot.json").read_bytes() == payload
    assert (first / "snapshot.json").stat().st_mode & 0o222 == 0


def test_empirical_distribution_honors_bracket_inclusivity() -> None:
    probabilities = _empirical_probabilities(  # noqa: SLF001
        (Decimal("25"),),
        {
            "low": Bracket(
                market_id="low",
                label="up to 25",
                lower=None,
                upper=25,
                upper_inclusive=True,
            ),
            "high": Bracket(
                market_id="high",
                label="above 25",
                lower=25,
                upper=None,
                lower_inclusive=False,
            ),
        },
    )

    assert probabilities == {"low": Decimal(1), "high": Decimal(0)}
