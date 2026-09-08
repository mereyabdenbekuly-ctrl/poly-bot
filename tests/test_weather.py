from datetime import UTC, date, datetime

import pytest

from polybot.config import Settings
from polybot.models import Bracket, WeatherForecast
from polybot.weather import OpenMeteoEnsemble


def test_bracket_probabilities_cover_the_distribution() -> None:
    model = OpenMeteoEnsemble(Settings(weather_error_sigma_c=1.0))
    forecast = WeatherForecast(
        provider="test",
        requested_location="Test",
        matched_location="Test",
        latitude=0,
        longitude=0,
        timezone="UTC",
        observation_date=date(2026, 9, 8),
        unit="C",
        fetched_at=datetime.now(UTC),
        member_values=[27.5, 28.5, 29.5],
    )
    brackets = [
        Bracket(market_id="1", label="27 or below", lower=None, upper=28),
        Bracket(market_id="2", label="28", lower=28, upper=29),
        Bracket(market_id="3", label="29 or higher", lower=29, upper=None),
    ]

    total = sum(model.probability(forecast=forecast, bracket=item) for item in brackets)
    assert total == pytest.approx(1.0)
