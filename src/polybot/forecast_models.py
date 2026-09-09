from __future__ import annotations

from datetime import date, datetime, timedelta
from decimal import Decimal
from enum import StrEnum
from typing import Literal
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from pydantic import Field, model_validator

from polybot.models import StrictModel

FORECAST_SCHEMA_VERSION = 2
OPEN_METEO_ALGORITHM_VERSION = "open-meteo-truncated-normal-v1"
WEATHERNEXT_ALGORITHM_VERSION = "weathernext-empirical-ensemble-v1"


class ForecastPhase(StrEnum):
    """Prediction issuance relative to the exact station-local rule day."""

    LEAD_TIME = "LEAD_TIME"
    INTRADAY = "INTRADAY"
    POST_EVENT = "POST_EVENT"


class ForecastEligibilityStage(StrEnum):
    """Event-level cohort stage, independent of whether a trade was selected."""

    DISCOVERED = "DISCOVERED"
    RULES_VALIDATED = "RULES_VALIDATED"
    OBSERVATIONS_VALIDATED = "OBSERVATIONS_VALIDATED"
    FORECAST_READY = "FORECAST_READY"
    FORECAST_FAILED = "FORECAST_FAILED"
    INELIGIBLE = "INELIGIBLE"
    BACKFILL_UNKNOWN = "BACKFILL_UNKNOWN"


class ForecastAlgorithmAttemptStatus(StrEnum):
    PREDICTED = "PREDICTED"
    NOT_EXPECTED = "NOT_EXPECTED"
    NOT_CONFIGURED = "NOT_CONFIGURED"
    SOURCE_UNAVAILABLE = "SOURCE_UNAVAILABLE"
    VALIDATION_FAILED = "VALIDATION_FAILED"
    PERSIST_FAILED = "PERSIST_FAILED"


class ForecastModelRun(StrictModel):
    """Immutable source/model artifact provenance.

    Source fields unavailable from a provider must stay ``None`` rather than be
    inferred. ``fetched_at_utc`` records when this copy reached the bot; a
    prediction has a separate issuance time because one archived model run may
    be reused with newer observations.
    """

    source: str
    model: str
    model_version: str | None = None
    source_run_id: str | None = None
    init_time_utc: datetime | None = None
    published_at_utc: datetime | None = None
    fetched_at_utc: datetime
    source_uri: str | None = None
    source_payload_hash: str | None = None
    metadata: dict[str, object] = Field(default_factory=dict)

    @model_validator(mode="after")
    def _validate_times(self) -> ForecastModelRun:
        for name in ("init_time_utc", "published_at_utc", "fetched_at_utc"):
            value = getattr(self, name)
            if value is not None and value.tzinfo is None:
                raise ValueError(f"{name} must be timezone-aware")
        if (
            self.init_time_utc is not None
            and self.published_at_utc is not None
            and self.published_at_utc < self.init_time_utc
        ):
            raise ValueError("published_at_utc must not precede init_time_utc")
        if self.published_at_utc is not None and self.fetched_at_utc < self.published_at_utc:
            raise ValueError("fetched_at_utc must not precede published_at_utc")
        return self


class ForecastRuleDay(StrictModel):
    """Exact station day and display transformation used by the market rules."""

    station_id: str
    observation_date: date
    station_timezone: str
    day_start_utc: datetime
    day_end_utc: datetime
    display_unit: Literal["C", "F"]
    precision_decimal_places: int = Field(ge=0, le=3)
    rounding_rule: str
    rules_hash: str

    @model_validator(mode="after")
    def _validate_day(self) -> ForecastRuleDay:
        if self.day_start_utc.tzinfo is None or self.day_end_utc.tzinfo is None:
            raise ValueError("rule-day boundaries must be timezone-aware")
        if self.day_end_utc <= self.day_start_utc:
            raise ValueError("day_end_utc must be later than day_start_utc")
        try:
            timezone = ZoneInfo(self.station_timezone)
        except ZoneInfoNotFoundError as error:
            raise ValueError(f"unknown station timezone: {self.station_timezone}") from error
        local_start = self.day_start_utc.astimezone(timezone)
        local_end = self.day_end_utc.astimezone(timezone)
        if local_start.date() != self.observation_date or local_start.time() != datetime.min.time():
            raise ValueError("day_start_utc is not midnight on the station observation date")
        if (
            local_end.date() != self.observation_date + timedelta(days=1)
            or local_end.time() != datetime.min.time()
        ):
            raise ValueError("day_end_utc is not the next station-local midnight")
        return self


class ForecastScenario(StrictModel):
    member_id: str
    raw_max_c: Decimal
    adjusted_max_c: Decimal
    weight: Decimal = Field(gt=0)


class ForecastProbability(StrictModel):
    market_id: str
    outcome_label: str
    lower_bound: Decimal | None
    upper_bound: Decimal | None
    lower_inclusive: bool = True
    upper_inclusive: bool = False
    probability: Decimal = Field(ge=0, le=1)


class ForecastObservationEvidence(StrictModel):
    station_id: str
    observed_at_utc: datetime
    revision_hash: str
    first_seen_at_utc: datetime
    source: str
    temperature_c: Decimal
    displayed_temperature_c: Decimal
    corrected: bool

    @model_validator(mode="after")
    def _validate_times(self) -> ForecastObservationEvidence:
        if self.observed_at_utc.tzinfo is None or self.first_seen_at_utc.tzinfo is None:
            raise ValueError("observation evidence timestamps must be timezone-aware")
        return self


class ForecastAlgorithmEligibility(StrictModel):
    source: str
    model: str
    algorithm_version: str
    expected: bool
    status: ForecastAlgorithmAttemptStatus
    prediction_id: int | None = None
    reason_codes: list[str] = Field(default_factory=list)


class ForecastEventEligibility(StrictModel):
    """One considered event in the frozen evaluation universe for a scan."""

    scan_run_id: int
    event_id: str
    cohort_version: str = "weather-evaluation-v1"
    considered_at_utc: datetime
    event_title: str
    event_slug: str | None = None
    market_count: int
    rules_hash: str
    rule_parser: str
    station_id: str | None = None
    observation_date: date | None = None
    station_timezone: str | None = None
    rule_day_start_utc: datetime | None = None
    rule_day_end_utc: datetime | None = None
    display_unit: str
    precision_decimal_places: int | None = None
    rounding_rule: str | None = None
    eligible: bool
    stage: ForecastEligibilityStage
    block_reasons: list[str] = Field(default_factory=list)
    warning_reasons: list[str] = Field(default_factory=list)
    algorithms: list[ForecastAlgorithmEligibility] = Field(default_factory=list)

    @model_validator(mode="after")
    def _validate_eligibility(self) -> ForecastEventEligibility:
        if self.considered_at_utc.tzinfo is None:
            raise ValueError("considered_at_utc must be timezone-aware")
        boundaries = (self.rule_day_start_utc, self.rule_day_end_utc)
        if any(value is not None and value.tzinfo is None for value in boundaries):
            raise ValueError("registry rule-day boundaries must be timezone-aware")
        if self.eligible and (
            self.station_id is None
            or self.observation_date is None
            or self.station_timezone is None
            or self.rule_day_start_utc is None
            or self.rule_day_end_utc is None
            or self.precision_decimal_places is None
            or not self.rounding_rule
        ):
            raise ValueError("eligible event requires complete station/rule-day identity")
        versions = [item.algorithm_version for item in self.algorithms]
        if len(versions) != len(set(versions)):
            raise ValueError("algorithm eligibility rows must have unique versions")
        return self

    @property
    def phase(self) -> ForecastPhase | None:
        if self.rule_day_start_utc is None or self.rule_day_end_utc is None:
            return None
        if self.considered_at_utc < self.rule_day_start_utc:
            return ForecastPhase.LEAD_TIME
        if self.considered_at_utc < self.rule_day_end_utc:
            return ForecastPhase.INTRADAY
        return ForecastPhase.POST_EVENT

    @property
    def lead_time_seconds(self) -> int | None:
        if self.rule_day_start_utc is None:
            return None
        return int((self.rule_day_start_utc - self.considered_at_utc).total_seconds())

    @property
    def intraday_elapsed_seconds(self) -> int | None:
        if self.phase != ForecastPhase.INTRADAY or self.rule_day_start_utc is None:
            return None
        return int((self.considered_at_utc - self.rule_day_start_utc).total_seconds())


class ForecastSubmission(StrictModel):
    """Append-only input for one event/model/algorithm prediction snapshot."""

    scan_run_id: int
    event_id: str
    algorithm_version: str
    model_run: ForecastModelRun
    issued_at_utc: datetime
    observation_cutoff_at_utc: datetime
    rule_day: ForecastRuleDay
    scenarios: list[ForecastScenario] = Field(min_length=1)
    point_forecast_c: Decimal | None = None
    observed_floor_c: Decimal | None = None
    probabilities: list[ForecastProbability] = Field(min_length=1)
    observations: list[ForecastObservationEvidence] = Field(default_factory=list)
    metadata: dict[str, object] = Field(default_factory=dict)

    @model_validator(mode="after")
    def _validate_submission(self) -> ForecastSubmission:
        if self.issued_at_utc.tzinfo is None or self.observation_cutoff_at_utc.tzinfo is None:
            raise ValueError("prediction issued/cutoff timestamps must be timezone-aware")
        if self.observation_cutoff_at_utc > self.issued_at_utc:
            raise ValueError("observation cutoff must not be later than prediction issuance")
        if self.model_run.fetched_at_utc > self.issued_at_utc:
            raise ValueError("model run must be fetched before prediction issuance")
        member_ids = [item.member_id for item in self.scenarios]
        if len(member_ids) != len(set(member_ids)):
            raise ValueError("scenario member_id values must be unique")
        market_ids = [item.market_id for item in self.probabilities]
        if len(market_ids) != len(set(market_ids)):
            raise ValueError("probability market_id values must be unique")
        if any(item.station_id != self.rule_day.station_id for item in self.observations):
            raise ValueError("observation evidence station does not match the rule day")
        if not Decimal("0.999999") <= self.distribution_mass <= Decimal("1.000001"):
            raise ValueError("probability distribution must sum to 1 within 1e-6")
        return self

    @property
    def phase(self) -> ForecastPhase:
        if self.issued_at_utc < self.rule_day.day_start_utc:
            return ForecastPhase.LEAD_TIME
        if self.issued_at_utc < self.rule_day.day_end_utc:
            return ForecastPhase.INTRADAY
        return ForecastPhase.POST_EVENT

    @property
    def lead_time_seconds(self) -> int:
        return int((self.rule_day.day_start_utc - self.issued_at_utc).total_seconds())

    @property
    def intraday_elapsed_seconds(self) -> int | None:
        if self.phase != ForecastPhase.INTRADAY:
            return None
        return int((self.issued_at_utc - self.rule_day.day_start_utc).total_seconds())

    @property
    def intraday_remaining_seconds(self) -> int | None:
        if self.phase != ForecastPhase.INTRADAY:
            return None
        return int((self.rule_day.day_end_utc - self.issued_at_utc).total_seconds())

    @property
    def distribution_mass(self) -> Decimal:
        return sum((item.probability for item in self.probabilities), Decimal(0))

    @property
    def weighted_point_forecast_c(self) -> Decimal:
        if self.point_forecast_c is not None:
            return self.point_forecast_c
        total_weight = sum((item.weight for item in self.scenarios), Decimal(0))
        return (
            sum((item.adjusted_max_c * item.weight for item in self.scenarios), Decimal(0))
            / total_weight
        )


class RealizedForecastOutcome(StrictModel):
    """One immutable revision of an observed and officially resolved outcome."""

    event_id: str
    station_id: str
    observation_date: date
    actual_max_c: Decimal
    displayed_max: Decimal
    winning_market_id: str
    winning_condition_id: str | None = None
    winning_label: str | None = None
    resolution_source: str
    source_revision: str
    source_published_at_utc: datetime | None = None
    resolved_at_utc: datetime
    recorded_at_utc: datetime
    evidence: dict[str, object] = Field(default_factory=dict)

    @model_validator(mode="after")
    def _validate_times(self) -> RealizedForecastOutcome:
        for name in ("source_published_at_utc", "resolved_at_utc", "recorded_at_utc"):
            value = getattr(self, name)
            if value is not None and value.tzinfo is None:
                raise ValueError(f"{name} must be timezone-aware")
        if self.recorded_at_utc < self.resolved_at_utc:
            raise ValueError("recorded_at_utc must not precede resolved_at_utc")
        if (
            self.source_published_at_utc is not None
            and self.recorded_at_utc < self.source_published_at_utc
        ):
            raise ValueError("recorded_at_utc must not precede source publication")
        return self


class ForecastMetricsQuery(StrictModel):
    source: str | None = None
    model: str | None = None
    algorithm_version: str | None = None
    station_id: str | None = None
    cohort_version: str = "weather-evaluation-v1"
    phases: tuple[ForecastPhase, ...] = (
        ForecastPhase.LEAD_TIME,
        ForecastPhase.INTRADAY,
    )
    lead_time_bins_hours: tuple[int, ...] = (0, 6, 12, 24, 48, 72, 168)
    calibration_bin_count: int = Field(default=10, ge=2, le=100)
    as_of_utc: datetime | None = None

    @model_validator(mode="after")
    def _validate_query(self) -> ForecastMetricsQuery:
        if not self.phases:
            raise ValueError("at least one phase is required")
        if any(value < 0 for value in self.lead_time_bins_hours):
            raise ValueError("lead-time bin edges must be non-negative")
        if tuple(sorted(set(self.lead_time_bins_hours))) != self.lead_time_bins_hours:
            raise ValueError("lead-time bin edges must be unique and sorted")
        if self.as_of_utc is not None and self.as_of_utc.tzinfo is None:
            raise ValueError("as_of_utc must be timezone-aware")
        return self


class CalibrationBin(StrictModel):
    lower: Decimal
    upper: Decimal
    upper_inclusive: bool
    count: int
    mean_confidence: Decimal | None
    empirical_accuracy: Decimal | None
    calibration_gap: Decimal | None


class ForecastMetricsSlice(StrictModel):
    segment: str
    forecast_count: int
    unique_forecast_event_count: int
    outcome_event_count: int
    event_count: int
    coverage: Decimal
    mae_event_count: int
    max_temperature_mae_c: Decimal | None
    exact_bracket_accuracy: Decimal | None
    multiclass_brier_score: Decimal | None
    expected_calibration_error: Decimal | None
    top_label_calibration: list[CalibrationBin]
    eligible_event_count: int = 0
    eligible_ended_event_count: int = 0
    forecast_coverage: Decimal = Decimal(0)
    outcome_coverage: Decimal = Decimal(0)
    calibration_bin_count: int = 10


class ForecastMetricsReport(StrictModel):
    schema_version: int = FORECAST_SCHEMA_VERSION
    generated_at_utc: datetime
    query: ForecastMetricsQuery
    overall: ForecastMetricsSlice
    by_phase: dict[str, ForecastMetricsSlice]
    by_lead_time: dict[str, ForecastMetricsSlice]


class ForecastEvaluationCase(StrictModel):
    """One unique event snapshot paired with its latest outcome revision."""

    prediction_id: int
    event_id: str
    phase: ForecastPhase
    issued_at_utc: datetime
    point_forecast_c: Decimal
    probabilities: dict[str, Decimal]
    distribution_mass: Decimal
    actual_max_c: Decimal
    winning_market_id: str


def compute_metrics_slice(
    *,
    segment: str,
    forecast_count: int,
    unique_forecast_event_count: int,
    outcome_event_count: int,
    cases: list[ForecastEvaluationCase],
    calibration_bin_count: int,
) -> ForecastMetricsSlice:
    """Calculate metrics once per unique event, never once per polling cycle."""

    complete = [
        item
        for item in cases
        if Decimal("0.999999") <= item.distribution_mass <= Decimal("1.000001")
        and item.winning_market_id in item.probabilities
    ]
    brier_values: list[Decimal] = []
    correct_values: list[Decimal] = []
    absolute_errors: list[Decimal] = []
    top_labels: list[tuple[Decimal, Decimal]] = []
    for item in complete:
        predicted_market, confidence = _select_top_probability(item.probabilities)
        correct = Decimal(1) if predicted_market == item.winning_market_id else Decimal(0)
        correct_values.append(correct)
        top_labels.append((confidence, correct))
        absolute_errors.append(abs(item.point_forecast_c - item.actual_max_c))
        brier_values.append(
            sum(
                (
                    (probability - (Decimal(1) if market_id == item.winning_market_id else 0)) ** 2
                    for market_id, probability in item.probabilities.items()
                ),
                Decimal(0),
            )
        )

    event_count = len(complete)
    coverage = (
        Decimal(event_count) / Decimal(outcome_event_count)
        if outcome_event_count > 0
        else Decimal(0)
    )
    calibration = _calibration_bins(top_labels, calibration_bin_count)
    nonempty_bins = [item for item in calibration if item.calibration_gap is not None]
    expected_calibration_error = (
        sum(
            (
                abs(item.calibration_gap or Decimal(0)) * Decimal(item.count) / Decimal(event_count)
                for item in nonempty_bins
            ),
            Decimal(0),
        )
        if event_count > 0
        else None
    )
    return ForecastMetricsSlice(
        segment=segment,
        forecast_count=forecast_count,
        unique_forecast_event_count=unique_forecast_event_count,
        outcome_event_count=outcome_event_count,
        event_count=event_count,
        coverage=coverage,
        mae_event_count=len(absolute_errors),
        max_temperature_mae_c=_mean(absolute_errors),
        exact_bracket_accuracy=_mean(correct_values),
        multiclass_brier_score=_mean(brier_values),
        expected_calibration_error=expected_calibration_error,
        top_label_calibration=calibration,
        calibration_bin_count=calibration_bin_count,
    )


def _select_top_probability(
    probabilities: dict[str, Decimal],
) -> tuple[str, Decimal]:
    """Select a top bracket deterministically, including ties."""

    ordered = sorted(probabilities.items(), key=lambda pair: pair[0])
    return max(ordered, key=lambda pair: pair[1])


def _mean(values: list[Decimal]) -> Decimal | None:
    if not values:
        return None
    return sum(values, Decimal(0)) / Decimal(len(values))


def _calibration_bins(
    values: list[tuple[Decimal, Decimal]], bin_count: int
) -> list[CalibrationBin]:
    bins: list[list[tuple[Decimal, Decimal]]] = [[] for _ in range(bin_count)]
    for confidence, correct in values:
        index = min(bin_count - 1, int(confidence * bin_count))
        bins[index].append((confidence, correct))
    result: list[CalibrationBin] = []
    for index, rows in enumerate(bins):
        lower = Decimal(index) / Decimal(bin_count)
        upper = Decimal(index + 1) / Decimal(bin_count)
        mean_confidence = _mean([row[0] for row in rows])
        accuracy = _mean([row[1] for row in rows])
        result.append(
            CalibrationBin(
                lower=lower,
                upper=upper,
                upper_inclusive=index == bin_count - 1,
                count=len(rows),
                mean_confidence=mean_confidence,
                empirical_accuracy=accuracy,
                calibration_gap=(
                    None
                    if mean_confidence is None or accuracy is None
                    else accuracy - mean_confidence
                ),
            )
        )
    return result
