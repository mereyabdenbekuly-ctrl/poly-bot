from decimal import Decimal

import pytest

from polybot.observations import (
    ObservationError,
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
