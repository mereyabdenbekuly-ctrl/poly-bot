from __future__ import annotations

import json
from datetime import date, datetime
from pathlib import Path
from typing import Literal

from pydantic import Field, model_validator

from polybot.config import Settings
from polybot.models import Bracket, RuleInterpretation, StrictModel


class WeatherNextSnapshot(StrictModel):
    """An explicitly supplied WeatherNext export; never fabricated by Polybot."""

    source: Literal["weathernext3"] = "weathernext3"
    init_time_utc: datetime
    received_at_utc: datetime
    location: str
    observation_date: date
    scenario_max_c: list[float] = Field(min_length=64, max_length=64)
    source_uri: str

    @model_validator(mode="after")
    def _validate_provenance(self) -> WeatherNextSnapshot:
        if self.received_at_utc < self.init_time_utc:
            raise ValueError("received_at_utc must not precede init_time_utc")
        if not self.source_uri.startswith("gs://weathernext3_spatial/"):
            raise ValueError("source_uri must identify the official full-ensemble GCS bucket")
        return self


class WeatherNextStatus(StrictModel):
    provider: Literal["weathernext3"] = "weathernext3"
    state: Literal["disabled", "access_pending", "snapshot_available", "error"]
    enabled: bool
    surface: str | None
    snapshot_path: str | None
    init_time_utc: datetime | None = None
    received_at_utc: datetime | None = None
    message: str


class WeatherNextProvider:
    """Optional comparison source; it never changes v1 decisions by itself.

    Google access is allow-listed. Until the user supplies an authorized export,
    this provider reports ``access_pending`` rather than returning fake data.
    """

    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    def status(self) -> WeatherNextStatus:
        path = self.settings.weathernext_snapshot_path
        if not self.settings.weathernext_enabled:
            return WeatherNextStatus(
                state="disabled",
                enabled=False,
                surface=self.settings.weathernext_surface,
                snapshot_path=path,
                message="WeatherNext 3 comparison is disabled; v1 continues with current sources.",
            )
        if not path:
            return WeatherNextStatus(
                state="access_pending",
                enabled=True,
                surface=self.settings.weathernext_surface,
                snapshot_path=None,
                message=(
                    "WeatherNext 3 access is not configured. Submit the Google allowlist request; "
                    "no forecast is being fabricated."
                ),
            )
        try:
            snapshot = self._load_snapshot(Path(path))
        except Exception as error:
            return WeatherNextStatus(
                state="error",
                enabled=True,
                surface=self.settings.weathernext_surface,
                snapshot_path=path,
                message=f"WeatherNext snapshot could not be loaded: {error}",
            )
        return WeatherNextStatus(
            state="snapshot_available",
            enabled=True,
            surface=self.settings.weathernext_surface,
            snapshot_path=path,
            init_time_utc=snapshot.init_time_utc,
            received_at_utc=snapshot.received_at_utc,
            message="Authorized WeatherNext 3 snapshot loaded for comparison only.",
        )

    def snapshot_for(self, rules: RuleInterpretation) -> WeatherNextSnapshot | None:
        if not self.settings.weathernext_enabled or not self.settings.weathernext_snapshot_path:
            return None
        snapshot = self._load_snapshot(Path(self.settings.weathernext_snapshot_path))
        if rules.location is None or rules.observation_date is None:
            return None
        if snapshot.observation_date != rules.observation_date:
            return None
        if snapshot.location.casefold() != rules.location.casefold():
            return None
        return snapshot

    @staticmethod
    def probabilities(
        snapshot: WeatherNextSnapshot, brackets: dict[str, Bracket]
    ) -> dict[str, float]:
        total = len(snapshot.scenario_max_c)
        result: dict[str, float] = {}
        for market_id, bracket in brackets.items():
            count = sum(
                1
                for value in snapshot.scenario_max_c
                if (bracket.lower is None or value >= bracket.lower)
                and (bracket.upper is None or value < bracket.upper)
            )
            result[market_id] = count / total
        return result

    @staticmethod
    def _load_snapshot(path: Path) -> WeatherNextSnapshot:
        payload = json.loads(path.read_text(encoding="utf-8"))
        return WeatherNextSnapshot.model_validate(payload)
