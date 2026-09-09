from datetime import UTC, date, datetime
from decimal import Decimal
from types import SimpleNamespace
from typing import Any

import pytest

from polybot.config import Settings
from polybot.models import RuleInterpretation
from polybot.observations import (
    ObservationError,
    StationObservationCollector,
    apply_observed_max,
    bracket_is_impossible,
    station_id_from_source_url,
)


def test_station_id_comes_from_exact_wrh_source() -> None:
    assert station_id_from_source_url("https://www.weather.gov/wrh/timeseries?site=eddm") == "EDDM"


@pytest.mark.parametrize(
    "url",
    [
        "http://www.weather.gov/wrh/timeseries?site=eddm",
        "https://example.test/wrh/timeseries?site=eddm",
        "https://www.weather.gov/other?site=eddm",
        "https://www.weather.gov/wrh/timeseries",
    ],
)
def test_untrusted_station_source_is_rejected(url: str) -> None:
    with pytest.raises(ObservationError):
        station_id_from_source_url(url)


def test_observed_max_clamps_daily_max_forecast() -> None:
    assert apply_observed_max([23.4, 25.1], Decimal("24")) == [24.0, 25.1]


def test_bracket_below_observed_max_is_impossible() -> None:
    assert bracket_is_impossible(upper=25, observed_display_max_c=Decimal("27"))
    assert not bracket_is_impossible(upper=28, observed_display_max_c=Decimal("27"))
    assert not bracket_is_impossible(upper=None, observed_display_max_c=Decimal("27"))


def test_future_observation_day_returns_metadata_without_future_history_request(
    monkeypatch,
) -> None:
    collector = StationObservationCollector(Settings(_env_file=None))  # type: ignore[call-arg]
    calls: list[dict[str, Any]] = []

    def fake_fetch(**kwargs):  # noqa: ANN003, ANN202
        calls.append(kwargs)
        return {
            "STATION": [
                {"STID": "TEST", "TIMEZONE": "UTC", "NAME": "Test City Airport"}
            ]
        }

    monkeypatch.setattr(collector, "_fetch_synoptic", fake_fetch)
    monkeypatch.setattr("polybot.observations.datetime", SimpleNamespace(
        now=lambda timezone: datetime(2026, 9, 9, tzinfo=UTC),
        combine=datetime.combine,
        min=datetime.min,
    ))
    rules = RuleInterpretation(
        event_type="daily_max_temperature",
        tradeable=True,
        location="Test City",
        observation_date=date(2026, 9, 10),
        unit="C",
        precision_decimal_places=0,
        station_or_authority="Test City Airport",
        resolution_source_url="https://www.weather.gov/wrh/timeseries?site=test",
        source_local_date=True,
        bucket_semantics_clear=True,
        ambiguity_reasons=[],
        summary="test",
        confidence=1,
    )

    result = collector.fetch(rules)

    assert result.day_started is False
    assert result.warning_reasons == ["OBSERVATION_DAY_NOT_STARTED"]
    assert len(calls) == 1
