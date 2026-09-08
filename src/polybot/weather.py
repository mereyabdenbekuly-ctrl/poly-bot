from __future__ import annotations

import math
import re
from dataclasses import dataclass
from datetime import UTC, date, datetime
from typing import Literal, cast

import httpx

from polybot.config import Settings
from polybot.models import Bracket, RuleInterpretation, WeatherForecast


class WeatherModelError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class GeocodedPlace:
    matched_location: str
    latitude: float
    longitude: float
    timezone: str


class OpenMeteoEnsemble:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    def forecast(self, rules: RuleInterpretation) -> WeatherForecast:
        if rules.location is None or rules.observation_date is None:
            raise WeatherModelError("location and observation date are required")
        if rules.unit not in {"C", "F"}:
            raise WeatherModelError("temperature unit must be C or F")
        unit = cast(Literal["C", "F"], rules.unit)

        today = datetime.now().astimezone().date()
        horizon = (rules.observation_date - today).days
        if horizon < 0:
            raise WeatherModelError("observation date is already in the past")
        if horizon > self.settings.max_forecast_horizon_days:
            raise WeatherModelError(
                f"forecast horizon {horizon}d exceeds "
                f"{self.settings.max_forecast_horizon_days}d limit"
            )

        place = self._geocode_rules(rules)
        response = httpx.get(
            self.settings.weather_ensemble_url,
            params={
                "latitude": place.latitude,
                "longitude": place.longitude,
                "daily": "temperature_2m_max",
                "timezone": place.timezone,
                "start_date": rules.observation_date.isoformat(),
                "end_date": rules.observation_date.isoformat(),
            },
            timeout=self.settings.http_timeout_seconds,
        )
        response.raise_for_status()
        data = response.json()
        daily = data.get("daily")
        if not isinstance(daily, dict):
            raise WeatherModelError("Open-Meteo response is missing daily data")
        dates = daily.get("time")
        if not isinstance(dates, list) or rules.observation_date.isoformat() not in dates:
            raise WeatherModelError("requested observation date is absent from forecast")
        index = dates.index(rules.observation_date.isoformat())
        values: list[float] = []
        for key, series in daily.items():
            if not key.startswith("temperature_2m_max_member") or not isinstance(series, list):
                continue
            if index >= len(series) or series[index] is None:
                continue
            values.append(float(series[index]))
        if len(values) < self.settings.min_ensemble_members:
            raise WeatherModelError(
                f"only {len(values)} ensemble members; need {self.settings.min_ensemble_members}"
            )
        if rules.unit == "F":
            values = [value * 9 / 5 + 32 for value in values]
        return WeatherForecast(
            provider="open-meteo-ensemble",
            requested_location=rules.location,
            matched_location=place.matched_location,
            latitude=place.latitude,
            longitude=place.longitude,
            timezone=place.timezone,
            observation_date=rules.observation_date,
            unit=unit,
            fetched_at=datetime.now(UTC),
            member_values=values,
        )

    def probability(self, *, forecast: WeatherForecast, bracket: Bracket) -> float:
        sigma = self.settings.weather_error_sigma_c
        if forecast.unit == "F":
            sigma *= 9 / 5
        probabilities = [
            _interval_probability(value, sigma, bracket.lower, bracket.upper)
            for value in forecast.member_values
        ]
        return sum(probabilities) / len(probabilities)

    def _geocode(self, location: str) -> GeocodedPlace:
        response = httpx.get(
            self.settings.weather_geocoding_url,
            params={"name": location, "count": 5, "language": "en", "format": "json"},
            timeout=self.settings.http_timeout_seconds,
        )
        response.raise_for_status()
        results = response.json().get("results", [])
        if not results:
            raise WeatherModelError(f"could not geocode {location!r}")
        normalized = location.casefold().strip()
        best = max(
            results,
            key=lambda item: (
                str(item.get("name", "")).casefold() == normalized,
                int(item.get("population") or 0),
            ),
        )
        timezone = best.get("timezone")
        if not isinstance(timezone, str) or not timezone:
            raise WeatherModelError(f"geocoder returned no timezone for {location!r}")
        parts = [best.get("name"), best.get("admin1"), best.get("country")]
        return GeocodedPlace(
            matched_location=", ".join(str(part) for part in parts if part),
            latitude=float(best["latitude"]),
            longitude=float(best["longitude"]),
            timezone=timezone,
        )

    def _geocode_rules(self, rules: RuleInterpretation) -> GeocodedPlace:
        assert rules.location is not None
        candidates: list[str] = []
        station = rules.station_or_authority or ""
        station = station.split(";", 1)[0]
        station = re.sub(r"^NOAA\s+at\s+the\s+", "", station, flags=re.I)
        station = re.sub(r"\s+Station$", "", station, flags=re.I).strip()
        if station and station.casefold() != rules.location.casefold():
            candidates.append(station)
            if station.lower().endswith(" airport") and "international" not in station.lower():
                candidates.append(station[: -len(" Airport")] + " International Airport")
        candidates.append(rules.location)

        failures: list[str] = []
        for candidate in dict.fromkeys(candidates):
            try:
                return self._geocode(candidate)
            except WeatherModelError as error:
                failures.append(str(error))
        raise WeatherModelError("; ".join(failures))


def _normal_cdf(value: float) -> float:
    return 0.5 * (1 + math.erf(value / math.sqrt(2)))


def _interval_probability(
    mean: float, sigma: float, lower: float | None, upper: float | None
) -> float:
    lower_cdf = 0.0 if lower is None else _normal_cdf((lower - mean) / sigma)
    upper_cdf = 1.0 if upper is None else _normal_cdf((upper - mean) / sigma)
    return max(0.0, min(1.0, upper_cdf - lower_cdf))


def is_forecast_date_eligible(observation_date: date, settings: Settings) -> bool:
    horizon = (observation_date - datetime.now().astimezone().date()).days
    return 0 <= horizon <= settings.max_forecast_horizon_days
