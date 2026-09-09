from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any, cast

import pytest

from polybot.forecast_models import (
    ForecastEvaluationCase,
    ForecastMetricsSlice,
    ForecastPhase,
    compute_metrics_slice,
)
from polybot.forecast_uncertainty import (
    bootstrap_mean_interval,
    build_phase_quality,
    exact_binomial_interval,
)


def _case(
    event: int,
    *,
    point_c: str,
    actual_c: str,
    probability_a: str,
    winner: str,
) -> ForecastEvaluationCase:
    probability = Decimal(probability_a)
    return ForecastEvaluationCase(
        prediction_id=event,
        event_id=f"event-{event}",
        phase=ForecastPhase.INTRADAY,
        issued_at_utc=datetime(2026, 9, 9, tzinfo=UTC),
        point_forecast_c=Decimal(point_c),
        probabilities={"a": probability, "b": Decimal(1) - probability},
        distribution_mass=Decimal(1),
        actual_max_c=Decimal(actual_c),
        winning_market_id=winner,
    )


def _metrics(*, events: int, outcomes: int, eligible: int | None = None) -> ForecastMetricsSlice:
    eligible = outcomes if eligible is None else eligible
    return ForecastMetricsSlice(
        segment="INTRADAY",
        forecast_count=100,
        unique_forecast_event_count=events,
        outcome_event_count=outcomes,
        event_count=events,
        coverage=Decimal(events) / Decimal(outcomes) if outcomes else Decimal(0),
        mae_event_count=events,
        max_temperature_mae_c=Decimal("0.75") if events else None,
        exact_bracket_accuracy=Decimal("0.5") if events else None,
        multiclass_brier_score=Decimal("0.4") if events else None,
        expected_calibration_error=Decimal("0.2") if events else None,
        top_label_calibration=[],
        eligible_event_count=eligible,
        eligible_ended_event_count=eligible,
        forecast_coverage=(Decimal(events) / Decimal(eligible) if eligible else Decimal(0)),
        outcome_coverage=(Decimal(outcomes) / Decimal(eligible) if eligible else Decimal(0)),
    )


def test_exact_binomial_interval_is_wide_for_one_event() -> None:
    interval = exact_binomial_interval(1, 1)
    assert interval is not None
    assert interval[0] == pytest.approx(0.025)
    assert interval[1] == pytest.approx(1.0)
    assert exact_binomial_interval(0, 0) is None


def test_bootstrap_interval_is_deterministic_and_requires_two_events() -> None:
    first = bootstrap_mean_interval([0.25, 0.5, 1.5, 2.0], seed="model", samples=500)
    second = bootstrap_mean_interval([0.25, 0.5, 1.5, 2.0], seed="model", samples=500)
    assert first == second
    assert first is not None
    assert first[0] <= 1.0625 <= first[1]
    assert bootstrap_mean_interval([1.0], seed="model", samples=500) is None


def test_phase_quality_marks_small_n_descriptive_and_separates_intervals() -> None:
    cases = [
        _case(1, point_c="20", actual_c="21", probability_a="0.8", winner="a"),
        _case(2, point_c="22", actual_c="21.5", probability_a="0.7", winner="b"),
    ]
    quality = build_phase_quality(
        phase=ForecastPhase.INTRADAY,
        metrics=_metrics(events=2, outcomes=3),
        cases=cases,
        minimum_events=30,
        bootstrap_samples=500,
        seed_key="source|model|version",
    )

    assert quality["phase"] == "INTRADAY"
    assert quality["sample_state"] == "insufficient_sample"
    assert quality["minimum_reliable_events"] == 30
    assert "descriptive only" in str(quality["sample_message"])
    metrics = cast(dict[str, Any], quality["metrics"])
    accuracy = metrics["accuracy"]
    assert accuracy["method"] == "clopper_pearson_exact_95"
    assert accuracy["ci95"] is not None
    assert metrics["mae_c"]["method"] == "percentile_bootstrap_95"
    assert metrics["mae_c"]["ci95"] is not None
    assert metrics["forecast_coverage"]["n"] == 3
    assert metrics["outcome_coverage"]["n"] == 3
    assert metrics["evaluation_coverage"]["n"] == 3


def test_phase_quality_reports_unestimable_empty_phase() -> None:
    quality = build_phase_quality(
        phase=ForecastPhase.LEAD_TIME,
        metrics=_metrics(events=0, outcomes=1),
        cases=[],
        minimum_events=30,
        bootstrap_samples=500,
        seed_key="empty",
    )
    assert quality["sample_state"] == "insufficient_sample"
    metrics = cast(dict[str, Any], quality["metrics"])
    assert metrics["accuracy"]["value"] is None
    assert metrics["accuracy"]["ci95"] is None
    assert metrics["forecast_coverage"]["ci95"] is not None
    assert metrics["outcome_coverage"]["ci95"] is not None


def test_quality_tie_break_matches_point_metric_order() -> None:
    case = ForecastEvaluationCase(
        prediction_id=1,
        event_id="tie",
        phase=ForecastPhase.INTRADAY,
        issued_at_utc=datetime(2026, 9, 9, tzinfo=UTC),
        point_forecast_c=Decimal("20"),
        probabilities={"b": Decimal("0.5"), "a": Decimal("0.5")},
        distribution_mass=Decimal(1),
        actual_max_c=Decimal("20"),
        winning_market_id="a",
    )
    point = compute_metrics_slice(
        segment="INTRADAY",
        forecast_count=1,
        unique_forecast_event_count=1,
        outcome_event_count=1,
        cases=[case],
        calibration_bin_count=10,
    ).model_copy(
        update={
            "eligible_event_count": 1,
            "eligible_ended_event_count": 1,
            "forecast_coverage": Decimal(1),
            "outcome_coverage": Decimal(1),
        }
    )
    quality = build_phase_quality(
        phase=ForecastPhase.INTRADAY,
        metrics=point,
        cases=[case],
        minimum_events=30,
        bootstrap_samples=500,
        seed_key="tie",
    )
    metrics = cast(dict[str, Any], quality["metrics"])
    assert point.exact_bracket_accuracy == Decimal(1)
    assert metrics["accuracy"]["ci95"] == ["0.025000", "1.000000"]


def test_zero_denominator_coverages_are_not_applicable() -> None:
    quality = build_phase_quality(
        phase=ForecastPhase.LEAD_TIME,
        metrics=_metrics(events=0, outcomes=0, eligible=0),
        cases=[],
        minimum_events=30,
        bootstrap_samples=500,
        seed_key="none",
    )
    metrics = cast(dict[str, Any], quality["metrics"])
    assert metrics["forecast_coverage"]["value"] is None
    assert metrics["outcome_coverage"]["value"] is None
    assert metrics["evaluation_coverage"]["value"] is None
