from __future__ import annotations

import hashlib
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Literal, cast
from zoneinfo import ZoneInfo

from polybot.forecast_models import (
    OPEN_METEO_ALGORITHM_VERSION,
    WEATHERNEXT_ALGORITHM_VERSION,
    ForecastEventEligibility,
    ForecastMetricsQuery,
    ForecastMetricsReport,
    ForecastModelRun,
    ForecastObservationEvidence,
    ForecastProbability,
    ForecastRuleDay,
    ForecastScenario,
    ForecastSubmission,
    RealizedForecastOutcome,
)
from polybot.forecast_store import ForecastStore
from polybot.forecast_v2 import EcmwfShadowSnapshot
from polybot.models import Bracket, RuleAudit, WeatherForecast
from polybot.observations import ObservationHistory
from polybot.weathernext import WeatherNextSnapshot


class ForecastEngineV2:
    """Shadow recorder/evaluator; it never participates in v1 trade decisions."""

    def __init__(self, database_path: Path, *, settings: object | None = None) -> None:
        self.store = ForecastStore(database_path)
        self._min_interval_seconds = int(
            getattr(settings, "forecast_snapshot_min_interval_seconds", 0)
        )
        self._probability_delta = Decimal(
            str(getattr(settings, "forecast_snapshot_probability_delta", 0))
        )

    def _record(self, submission: ForecastSubmission) -> int:
        return self.store.record(
            submission,
            min_interval_seconds=self._min_interval_seconds,
            probability_delta=self._probability_delta,
        )

    def register_evaluation_event(self, item: ForecastEventEligibility) -> int:
        return self.store.register_evaluation_event(item)

    def record_open_meteo(
        self,
        *,
        scan_run_id: int,
        event_id: str,
        audit: RuleAudit,
        forecast: WeatherForecast,
        brackets: dict[str, Bracket],
        probabilities: dict[str, Decimal],
        observations: ObservationHistory,
        source_uri: str,
        weather_error_sigma_c: Decimal,
        issued_at_utc: datetime | None = None,
        metadata: dict[str, object] | None = None,
        algorithm_version: str = OPEN_METEO_ALGORITHM_VERSION,
    ) -> int:
        raw_values = forecast.unadjusted_member_values or forecast.member_values
        if len(raw_values) != len(forecast.member_values):
            raise ValueError("raw and adjusted ensemble sizes differ")
        fetched_at = forecast.fetched_at.astimezone(UTC)
        observation_cutoff = observations.fetched_at_utc.astimezone(UTC)
        issued = (issued_at_utc or max(fetched_at, observation_cutoff)).astimezone(UTC)
        scenarios = [
            ForecastScenario(
                member_id=f"member-{index:03d}",
                raw_max_c=_to_celsius(raw, forecast.unit),
                adjusted_max_c=_to_celsius(adjusted, forecast.unit),
                weight=Decimal(1),
            )
            for index, (raw, adjusted) in enumerate(
                zip(raw_values, forecast.member_values, strict=True)
            )
        ]
        submission = ForecastSubmission(
            scan_run_id=scan_run_id,
            event_id=event_id,
            algorithm_version=algorithm_version,
            model_run=ForecastModelRun(
                source="open-meteo",
                model=forecast.provider,
                fetched_at_utc=fetched_at,
                source_uri=source_uri,
                metadata={
                    "requested_location": forecast.requested_location,
                    "matched_location": forecast.matched_location,
                    "latitude": forecast.latitude,
                    "longitude": forecast.longitude,
                    "forecast_timezone": forecast.timezone,
                    "weather_error_sigma_c": str(weather_error_sigma_c),
                    "conditioning_method": (
                        "clamp_full_day_members_then_truncated_normal_kernel"
                    ),
                    **(metadata or {}),
                },
            ),
            issued_at_utc=issued,
            observation_cutoff_at_utc=observation_cutoff,
            rule_day=_rule_day(audit=audit, observations=observations),
            scenarios=scenarios,
            observed_floor_c=forecast.observed_floor_c,
            probabilities=_probabilities(brackets, probabilities),
            observations=_observation_evidence(observations),
        )
        return self._record(submission)

    def record_weathernext(
        self,
        *,
        scan_run_id: int,
        event_id: str,
        audit: RuleAudit,
        snapshot: WeatherNextSnapshot,
        brackets: dict[str, Bracket],
        probabilities: dict[str, Decimal],
        observations: ObservationHistory,
        model_version: str,
        issued_at_utc: datetime | None = None,
        algorithm_version: str = WEATHERNEXT_ALGORITHM_VERSION,
    ) -> int:
        received_at = snapshot.received_at_utc.astimezone(UTC)
        observation_cutoff = observations.fetched_at_utc.astimezone(UTC)
        issued = (issued_at_utc or max(received_at, observation_cutoff)).astimezone(UTC)
        payload_hash = hashlib.sha256(snapshot.model_dump_json().encode()).hexdigest()
        submission = ForecastSubmission(
            scan_run_id=scan_run_id,
            event_id=event_id,
            algorithm_version=algorithm_version,
            model_run=ForecastModelRun(
                source=snapshot.source,
                model="WeatherNext 3 full ensemble",
                model_version=model_version,
                source_run_id=snapshot.init_time_utc.astimezone(UTC).isoformat(),
                init_time_utc=snapshot.init_time_utc.astimezone(UTC),
                fetched_at_utc=received_at,
                source_uri=snapshot.source_uri,
                source_payload_hash=payload_hash,
                metadata={"location": snapshot.location},
            ),
            issued_at_utc=issued,
            observation_cutoff_at_utc=observation_cutoff,
            rule_day=_rule_day(audit=audit, observations=observations),
            scenarios=[
                ForecastScenario(
                    member_id=f"member-{index:03d}",
                    raw_max_c=Decimal(str(value)),
                    adjusted_max_c=Decimal(str(value)),
                    weight=Decimal(1),
                )
                for index, value in enumerate(snapshot.scenario_max_c)
            ],
            probabilities=_probabilities(brackets, probabilities),
            observations=_observation_evidence(observations),
        )
        return self._record(submission)

    def record_ecmwf_shadow(
        self,
        *,
        scan_run_id: int,
        event_id: str,
        audit: RuleAudit,
        snapshot: EcmwfShadowSnapshot,
        brackets: dict[str, Bracket],
        probabilities: dict[str, Decimal],
        observations: ObservationHistory,
        algorithm_version: str,
        adjusted_member_max_c: tuple[Decimal, ...] | None = None,
        observed_floor_c: Decimal | None = None,
        point_forecast_c: Decimal | None = None,
        issued_at_utc: datetime | None = None,
        metadata: dict[str, object] | None = None,
        include_observations: bool = True,
    ) -> int:
        """Record raw or corrected IFS ENS scenarios without affecting v1."""

        raw = tuple(snapshot.member_max_c)
        adjusted = adjusted_member_max_c or raw
        if len(raw) != len(adjusted):
            raise ValueError("raw and adjusted ECMWF member counts differ")
        member_ids = tuple(snapshot.member_ids or ())
        if member_ids and len(member_ids) != len(raw):
            raise ValueError("ECMWF source member IDs and values differ in length")
        if not member_ids:
            member_ids = tuple(f"legacy-position-{index + 1:03d}" for index in range(len(raw)))
        observation_cutoff = observations.fetched_at_utc.astimezone(UTC)
        issued = (issued_at_utc or max(snapshot.fetched_at_utc, observation_cutoff)).astimezone(UTC)
        submission = ForecastSubmission(
            scan_run_id=scan_run_id,
            event_id=event_id,
            algorithm_version=algorithm_version,
            model_run=ForecastModelRun(
                source="open-meteo-ecmwf",
                model="ECMWF IFS ENS 0.25° daily max via Open-Meteo",
                model_version=snapshot.upstream_model,
                source_run_id=snapshot.source_run_id,
                init_time_utc=snapshot.init_time_utc,
                published_at_utc=snapshot.published_at_utc,
                fetched_at_utc=snapshot.fetched_at_utc.astimezone(UTC),
                source_uri=snapshot.source_uri,
                source_payload_hash=snapshot.payload_sha256,
                metadata={
                    "transport_provider": snapshot.transport_provider,
                    "archive_path": snapshot.archive_path,
                    "requested_location": snapshot.requested_location,
                    "matched_location": snapshot.matched_location,
                    "latitude": snapshot.latitude,
                    "longitude": snapshot.longitude,
                    "forecast_timezone": snapshot.timezone,
                    "source_member_ids": list(member_ids),
                    "member_identity": (
                        "provider_field_name"
                        if snapshot.member_ids is not None
                        else "legacy_positional_fallback"
                    ),
                    "control_field_present": snapshot.control_field_present,
                    "control_member_included": False,
                    "response_latitude": snapshot.response_latitude,
                    "response_longitude": snapshot.response_longitude,
                    "response_elevation_m": snapshot.response_elevation_m,
                    "response_timezone": snapshot.response_timezone,
                    "grid_distance_km": snapshot.grid_distance_km,
                    "daily_units": snapshot.daily_units,
                    "provider_processing_note": (
                        "Open-Meteo daily aggregation may include temporal interpolation "
                        "and terrain downscaling; not a raw station forecast."
                    ),
                    "provenance_complete": bool(
                        snapshot.source_run_id
                        and snapshot.init_time_utc
                        and snapshot.published_at_utc
                    ),
                },
            ),
            issued_at_utc=issued,
            observation_cutoff_at_utc=observation_cutoff,
            rule_day=_rule_day(audit=audit, observations=observations),
            scenarios=[
                ForecastScenario(
                    member_id=member_ids[index],
                    raw_max_c=raw_value,
                    adjusted_max_c=adjusted_value,
                    weight=Decimal(1),
                )
                for index, (raw_value, adjusted_value) in enumerate(zip(raw, adjusted, strict=True))
            ],
            point_forecast_c=point_forecast_c,
            observed_floor_c=observed_floor_c,
            probabilities=_probabilities(brackets, probabilities),
            observations=_observation_evidence(observations) if include_observations else [],
            metadata={
                **(metadata or {}),
                "observation_lineage": "included" if include_observations else "excluded",
            },
        )
        return self._record(submission)

    def record_outcome(self, outcome: RealizedForecastOutcome) -> int:
        return self.store.record_outcome(outcome)

    def metrics(self, query: ForecastMetricsQuery | None = None) -> ForecastMetricsReport:
        return self.store.metrics(query)


def _rule_day(*, audit: RuleAudit, observations: ObservationHistory) -> ForecastRuleDay:
    rules = audit.interpretation
    if rules.observation_date is None or rules.unit != "C":
        raise ValueError("forecast v2 currently requires Celsius and an exact observation date")
    if rules.precision_decimal_places != 0:
        raise ValueError("forecast v2 currently supports only whole-degree resolution rules")
    timezone = ZoneInfo(observations.station_timezone)
    local_start = datetime.combine(rules.observation_date, datetime.min.time(), tzinfo=timezone)
    return ForecastRuleDay(
        station_id=observations.station_id,
        observation_date=rules.observation_date,
        station_timezone=observations.station_timezone,
        day_start_utc=local_start.astimezone(UTC),
        day_end_utc=(local_start + timedelta(days=1)).astimezone(UTC),
        display_unit=cast(Literal["C", "F"], rules.unit),
        precision_decimal_places=rules.precision_decimal_places or 0,
        rounding_rule="displayed_temperature_c=floor(raw_temperature_c+0.5)",
        rules_hash=audit.rules_hash,
    )


def _probabilities(
    brackets: dict[str, Bracket], probabilities: dict[str, Decimal]
) -> list[ForecastProbability]:
    if brackets.keys() != probabilities.keys():
        missing = brackets.keys() - probabilities.keys()
        extra = probabilities.keys() - brackets.keys()
        raise ValueError(
            "forecast distribution/bracket mismatch: "
            f"missing={sorted(missing)}, extra={sorted(extra)}"
        )
    return [
        ForecastProbability(
            market_id=market_id,
            outcome_label=bracket.label,
            lower_bound=None if bracket.lower is None else Decimal(str(bracket.lower)),
            upper_bound=None if bracket.upper is None else Decimal(str(bracket.upper)),
            lower_inclusive=bracket.lower_inclusive,
            upper_inclusive=bracket.upper_inclusive,
            probability=probabilities[market_id],
        )
        for market_id, bracket in brackets.items()
    ]


def _observation_evidence(history: ObservationHistory) -> list[ForecastObservationEvidence]:
    return [
        ForecastObservationEvidence(
            station_id=item.station_id,
            observed_at_utc=item.observed_at_utc,
            revision_hash=item.revision_hash,
            first_seen_at_utc=item.first_seen_at_utc,
            source=item.source,
            temperature_c=item.temperature_c,
            displayed_temperature_c=item.displayed_temperature_c,
            corrected=item.corrected,
        )
        for item in history.observations
    ]


def _to_celsius(value: float, unit: str) -> Decimal:
    decimal = Decimal(str(value))
    if unit == "C":
        return decimal
    if unit == "F":
        return (decimal - Decimal(32)) * Decimal(5) / Decimal(9)
    raise ValueError(f"unsupported forecast unit {unit!r}")
