"""Small-sample uncertainty summaries for the read-only forecast dashboard."""

from __future__ import annotations

import hashlib
import math
import random
from decimal import Decimal

from polybot.forecast_models import (
    ForecastEvaluationCase,
    ForecastMetricsSlice,
    ForecastPhase,
    _select_top_probability,
)


def build_phase_quality(
    *,
    phase: ForecastPhase,
    metrics: ForecastMetricsSlice,
    cases: list[ForecastEvaluationCase],
    minimum_events: int,
    bootstrap_samples: int,
    seed_key: str,
    calibration_bin_count: int | None = None,
) -> dict[str, object]:
    """Serialize one phase's metrics with intervals and a reliability threshold."""

    calibration_bin_count = (
        metrics.calibration_bin_count if calibration_bin_count is None else calibration_bin_count
    )

    complete = [
        item
        for item in cases
        if Decimal("0.999999") <= item.distribution_mass <= Decimal("1.000001")
        and item.winning_market_id in item.probabilities
    ]
    correct: list[int] = []
    absolute_errors: list[float] = []
    brier_values: list[float] = []
    confidence_correct: list[tuple[float, int]] = []
    for item in complete:
        predicted_market, confidence = _select_top_probability(item.probabilities)
        is_correct = int(predicted_market == item.winning_market_id)
        correct.append(is_correct)
        confidence_correct.append((float(confidence), is_correct))
        absolute_errors.append(float(abs(item.point_forecast_c - item.actual_max_c)))
        brier_values.append(
            float(
                sum(
                    (
                        (probability - (Decimal(1) if market_id == item.winning_market_id else 0))
                        ** 2
                        for market_id, probability in item.probabilities.items()
                    ),
                    Decimal(0),
                )
            )
        )

    event_count = len(complete)
    sample_state = (
        "monitoring_threshold_met" if event_count >= minimum_events else "insufficient_sample"
    )
    sample_message = (
        f"{event_count}/{minimum_events} resolved unique events; descriptive only—do not rank"
        if sample_state == "insufficient_sample"
        else (
            f"{event_count} resolved unique events; minimum monitoring threshold met—"
            "uncertainty still applies"
        )
    )
    accuracy_ci = exact_binomial_interval(sum(correct), event_count)
    evaluation_coverage_ci = exact_binomial_interval(
        metrics.event_count, metrics.outcome_event_count
    )
    forecast_coverage_ci = exact_binomial_interval(
        metrics.unique_forecast_event_count, metrics.eligible_event_count
    )
    outcome_coverage_ci = exact_binomial_interval(
        metrics.outcome_event_count, metrics.eligible_ended_event_count
    )
    mae_ci = bootstrap_mean_interval(
        absolute_errors,
        seed=f"{seed_key}|{phase.value}|mae",
        samples=bootstrap_samples,
    )
    brier_ci = bootstrap_mean_interval(
        brier_values,
        seed=f"{seed_key}|{phase.value}|brier",
        samples=bootstrap_samples,
    )
    ece_ci = bootstrap_ece_interval(
        confidence_correct,
        seed=f"{seed_key}|{phase.value}|ece",
        samples=bootstrap_samples,
        bin_count=calibration_bin_count,
    )

    def metric(
        value: Decimal | None,
        interval: tuple[float, float] | None,
        method: str,
        n: int,
        *,
        applicable: bool = True,
    ) -> dict[str, object]:
        return {
            "value": None if value is None else str(value),
            "ci95": None if interval is None else [f"{interval[0]:.6f}", f"{interval[1]:.6f}"],
            "method": method,
            "n": n,
            "interval_status": (
                "not_applicable_no_denominator"
                if not applicable
                else ("available" if interval is not None else "not_estimable_with_current_n")
            ),
        }

    return {
        "phase": phase.value,
        "label": "Lead-time" if phase == ForecastPhase.LEAD_TIME else "Intraday",
        "forecast_count": metrics.forecast_count,
        "unique_forecast_event_count": metrics.unique_forecast_event_count,
        "outcome_event_count": metrics.outcome_event_count,
        "event_count": metrics.event_count,
        "sample_state": sample_state,
        "sample_message": sample_message,
        "minimum_reliable_events": minimum_events,
        "eligible_event_count": metrics.eligible_event_count,
        "eligible_ended_event_count": metrics.eligible_ended_event_count,
        "calibration": [
            {
                "lower": str(item.lower),
                "upper": str(item.upper),
                "count": item.count,
                "mean_confidence": (
                    None if item.mean_confidence is None else str(item.mean_confidence)
                ),
                "empirical_accuracy": (
                    None if item.empirical_accuracy is None else str(item.empirical_accuracy)
                ),
            }
            for item in metrics.top_label_calibration
            if item.count > 0
        ],
        "metrics": {
            "mae_c": metric(
                metrics.max_temperature_mae_c,
                mae_ci,
                "percentile_bootstrap_95",
                event_count,
            ),
            "accuracy": metric(
                metrics.exact_bracket_accuracy,
                accuracy_ci,
                "clopper_pearson_exact_95",
                event_count,
            ),
            "brier": metric(
                metrics.multiclass_brier_score,
                brier_ci,
                "percentile_bootstrap_95",
                event_count,
            ),
            "ece": metric(
                metrics.expected_calibration_error,
                ece_ci,
                f"percentile_bootstrap_95_{calibration_bin_count}_bins",
                event_count,
            ),
            "forecast_coverage": metric(
                (metrics.forecast_coverage if metrics.eligible_event_count > 0 else None),
                forecast_coverage_ci,
                "clopper_pearson_exact_95",
                metrics.eligible_event_count,
                applicable=metrics.eligible_event_count > 0,
            ),
            "outcome_coverage": metric(
                (metrics.outcome_coverage if metrics.eligible_ended_event_count > 0 else None),
                outcome_coverage_ci,
                "clopper_pearson_exact_95",
                metrics.eligible_ended_event_count,
                applicable=metrics.eligible_ended_event_count > 0,
            ),
            "evaluation_coverage": metric(
                metrics.coverage if metrics.outcome_event_count > 0 else None,
                evaluation_coverage_ci,
                "clopper_pearson_exact_95",
                metrics.outcome_event_count,
                applicable=metrics.outcome_event_count > 0,
            ),
        },
    }


def bootstrap_mean_interval(
    values: list[float], *, seed: str, samples: int
) -> tuple[float, float] | None:
    if len(values) < 2 or samples < 100:
        return None
    rng = random.Random(_seed(seed))
    n = len(values)
    estimates = [sum(values[rng.randrange(n)] for _ in range(n)) / n for _ in range(samples)]
    return _percentile_interval(estimates)


def bootstrap_ece_interval(
    values: list[tuple[float, int]], *, seed: str, samples: int, bin_count: int
) -> tuple[float, float] | None:
    if bin_count <= 0:
        raise ValueError("bin_count must be positive")
    if len(values) < 2 or samples < 100:
        return None
    rng = random.Random(_seed(seed))
    n = len(values)
    estimates: list[float] = []
    for _ in range(samples):
        draw = [values[rng.randrange(n)] for _ in range(n)]
        estimates.append(_ece(draw, bin_count))
    return _percentile_interval(estimates)


def exact_binomial_interval(
    successes: int, trials: int, *, alpha: float = 0.05
) -> tuple[float, float] | None:
    """Clopper--Pearson exact two-sided interval via binomial inversion."""

    if trials <= 0 or successes < 0 or successes > trials:
        return None
    if not 0 < alpha < 1:
        raise ValueError("alpha must be between 0 and 1")
    lower = (
        0.0
        if successes == 0
        else _binomial_quantile(
            target=1 - alpha / 2,
            cutoff=successes - 1,
            trials=trials,
        )
    )
    upper = (
        1.0
        if successes == trials
        else _binomial_quantile(
            target=alpha / 2,
            cutoff=successes,
            trials=trials,
        )
    )
    return lower, upper


def _percentile_interval(values: list[float]) -> tuple[float, float] | None:
    if not values:
        return None
    ordered = sorted(values)

    def percentile(probability: float) -> float:
        position = (len(ordered) - 1) * probability
        lower = math.floor(position)
        upper = math.ceil(position)
        if lower == upper:
            return ordered[lower]
        fraction = position - lower
        return ordered[lower] + fraction * (ordered[upper] - ordered[lower])

    return percentile(0.025), percentile(0.975)


def _ece(values: list[tuple[float, int]], bin_count: int) -> float:
    if not values:
        return 0.0
    bins: list[list[tuple[float, int]]] = [[] for _ in range(bin_count)]
    for confidence, correct in values:
        index = min(bin_count - 1, max(0, int(confidence * bin_count)))
        bins[index].append((confidence, correct))
    total = len(values)
    return sum(
        abs(
            sum(confidence for confidence, _ in rows) / len(rows)
            - sum(correct for _, correct in rows) / len(rows)
        )
        * len(rows)
        / total
        for rows in bins
        if rows
    )


def _binomial_quantile(*, target: float, cutoff: int, trials: int) -> float:
    low, high = 0.0, 1.0
    for _ in range(80):
        mid = (low + high) / 2
        if _binomial_cdf(cutoff, trials, mid) > target:
            low = mid
        else:
            high = mid
    return (low + high) / 2


def _binomial_cdf(cutoff: int, trials: int, probability: float) -> float:
    if cutoff < 0:
        return 0.0
    if cutoff >= trials:
        return 1.0
    if probability <= 0:
        return 1.0
    if probability >= 1:
        return 0.0
    log_p = math.log(probability)
    log_q = math.log1p(-probability)
    terms = [
        math.lgamma(trials + 1)
        - math.lgamma(index + 1)
        - math.lgamma(trials - index + 1)
        + index * log_p
        + (trials - index) * log_q
        for index in range(cutoff + 1)
    ]
    maximum = max(terms)
    return min(1.0, math.exp(maximum) * sum(math.exp(term - maximum) for term in terms))


def _seed(value: str) -> int:
    return int(hashlib.sha256(value.encode("utf-8")).hexdigest()[:16], 16)
