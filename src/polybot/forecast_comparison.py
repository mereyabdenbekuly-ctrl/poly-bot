"""Read-only comparison of archived v1, raw-ECMWF and v2 forecasts.

The reader never constructs ``Storage`` or ``ForecastStore``.  Those runtime
classes can create/migrate tables; this report opens SQLite in ``mode=ro`` and
uses only immutable forecast/evaluation rows.  It compares all models at the
same selected evaluation-registry checkpoint, and is deliberately descriptive:
it never changes a paper decision or promotes v2.
"""

from __future__ import annotations

import hashlib
import json
import random
import sqlite3
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any
from urllib.parse import quote

from polybot.forecast_models import ForecastPhase
from polybot.forecast_sensitivity import (
    DEFAULT_V1_SIGMA_C,
    SigmaTradeThresholds,
    clamped_sigma_bracket_probabilities,
    empirical_bracket_probabilities,
    replay_paper_qualification,
    v1_sigma_bracket_probabilities,
)
from polybot.models import Bracket, MarketSnapshot

COMPARISON_VERSION = "forecast-comparison-v1"
MIN_RELIABLE_EVENTS = 30
BOOTSTRAP_SAMPLES = 1000
DEFAULT_LEAD_TIME_BINS_HOURS: tuple[int, ...] = (0, 6, 12, 24, 48, 72, 168)
V1_ALGORITHM = "open-meteo-truncated-normal-v1"
RAW_ECMWF_ALGORITHM = "ecmwf-ifs025-raw-ensemble-v1"
V2_ALGORITHM = "forecast-engine-v2-station-intraday@1"
COMPARISON_ALGORITHMS: tuple[dict[str, str], ...] = (
    {
        "name": "v1 fixed sigma",
        "source": "open-meteo",
        "model": "open-meteo-ensemble",
        "algorithm_version": V1_ALGORITHM,
    },
    {
        "name": "raw ECMWF IFS ENS",
        "source": "open-meteo-ecmwf",
        "model": "ECMWF IFS ENS 0.25° daily max via Open-Meteo",
        "algorithm_version": RAW_ECMWF_ALGORITHM,
    },
    {
        "name": "v2 conditioned",
        "source": "open-meteo-ecmwf",
        "model": "ECMWF IFS ENS 0.25° daily max via Open-Meteo",
        "algorithm_version": V2_ALGORITHM,
    },
)
_ALGORITHM_VERSIONS = frozenset(item["algorithm_version"] for item in COMPARISON_ALGORITHMS)


@dataclass(frozen=True, slots=True)
class _Attempt:
    expected: bool
    status: str
    prediction_id: int | None


@dataclass(frozen=True, slots=True)
class _Checkpoint:
    row_id: int
    event_id: str
    scan_run_id: int
    considered_at: datetime
    phase: str
    segment: str
    lead_time_seconds: int | None
    attempts: Mapping[str, _Attempt]


@dataclass(frozen=True, slots=True)
class _Prediction:
    prediction_id: int
    event_id: str
    algorithm_version: str
    phase: str
    issued_at: datetime
    point_forecast_c: Decimal | None
    distribution_mass: Decimal | None
    probabilities: Mapping[str, Decimal]
    scenarios: tuple[Decimal, ...]
    adjusted_scenarios: tuple[Decimal, ...]
    observed_floor_c: Decimal | None
    metadata: Mapping[str, Any]


@dataclass(frozen=True, slots=True)
class _Outcome:
    event_id: str
    actual_max_c: Decimal
    winning_market_id: str
    recorded_at: datetime


@dataclass(frozen=True, slots=True)
class _Case:
    checkpoint: _Checkpoint
    prediction: _Prediction
    outcome: _Outcome


def compare_forecasts(
    database: Path | str,
    *,
    as_of_utc: datetime | None = None,
    minimum_events: int = MIN_RELIABLE_EVENTS,
    bootstrap_samples: int = BOOTSTRAP_SAMPLES,
) -> dict[str, object]:
    """Build a JSON-friendly comparative report without mutating SQLite."""

    if minimum_events < 1:
        raise ValueError("minimum_events must be positive")
    if bootstrap_samples < 0:
        raise ValueError("bootstrap_samples must be non-negative")
    cutoff = _as_utc(as_of_utc)
    path = Path(database).expanduser().resolve()
    warnings: list[str] = []
    if not path.is_file():
        warnings.append("MISSING_DATABASE")
        return _empty_report(cutoff, minimum_events, warnings)

    try:
        with _open_read_only(path) as connection:
            tables = _tables(connection)
            required = {
                "forecast_evaluation_events_v2",
                "forecast_evaluation_algorithms_v2",
                "forecast_model_runs_v2",
                "forecast_predictions_v2",
                "forecast_probabilities_v2",
                "forecast_scenarios_v2",
                "forecast_outcome_versions_v2",
            }
            missing = sorted(required - tables)
            warnings.extend(f"MISSING_TABLE:{name}" for name in missing)
            if missing:
                checkpoints: list[_Checkpoint] = []
                predictions: dict[int, _Prediction] = {}
                outcomes: dict[str, _Outcome] = {}
            else:
                checkpoints = _load_checkpoints(connection, cutoff)
                predictions = _load_predictions(connection, checkpoints, cutoff)
                outcomes = _load_outcomes(connection, cutoff)
            model_reports = _build_model_reports(
                checkpoints,
                predictions,
                outcomes,
                minimum_events=minimum_events,
                bootstrap_samples=bootstrap_samples,
            )
            checkpoint_reports = _checkpoint_reports(checkpoints, predictions, outcomes)
            paired = _paired_comparison(
                checkpoints,
                predictions,
                outcomes,
                minimum_events=minimum_events,
                bootstrap_samples=bootstrap_samples,
            )
            sigma = _sigma_diagnostics(connection, predictions, outcomes, cutoff)
            pnl = _pnl_by_strategy(connection, cutoff)
    except sqlite3.Error as error:
        warnings.append(f"READ_ERROR:{type(error).__name__}")
        return _empty_report(cutoff, minimum_events, warnings)

    warnings.extend(
        (
            "Forecast quality and trading P&L are separate quantities.",
            "Missing entry forecasts are not reconstructed from later snapshots.",
            "v2 is shadow-only; this report never enables live execution.",
            "Selected checkpoints are descriptive and are not a frozen clock-time holdout.",
        )
    )
    return {
        "version": COMPARISON_VERSION,
        "status": "ready" if checkpoints else "no_data",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "as_of_utc": None if cutoff is None else cutoff.isoformat(),
        "checkpoint_policy": {
            "selection": "latest_registry_opportunity_per_event_per_phase_bin",
            "phase_aggregation": "latest_selected_checkpoint_per_event_per_phase",
            "pairing": "same_selected_evaluation_registry_row",
            "phases": [ForecastPhase.LEAD_TIME.value, ForecastPhase.INTRADAY.value],
            "lead_time_bins_hours": list(DEFAULT_LEAD_TIME_BINS_HOURS),
            "labels": [item["label"] for item in checkpoint_reports],
            "issuance_spread": "max minus min model issuance at the same registry checkpoint",
            "frozen_holdout": False,
        },
        "minimum_reliable_events": minimum_events,
        "models": model_reports,
        "checkpoints": checkpoint_reports,
        "paired_vs_v1": paired,
        "sigma_dependent_signals": sigma,
        "pnl_by_entry_strategy": pnl,
        "promotion": _promotion_status(minimum_events),
        "warnings": list(dict.fromkeys(warnings)),
    }


build_comparison_report = compare_forecasts
run_comparison = compare_forecasts


@contextmanager
def _open_read_only(path: Path) -> Iterator[sqlite3.Connection]:
    uri = f"file:{quote(str(path), safe='/')}?mode=ro"
    connection = sqlite3.connect(uri, uri=True, timeout=30)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA query_only = ON")
    try:
        yield connection
    finally:
        connection.close()


def _tables(connection: sqlite3.Connection) -> set[str]:
    return {
        str(row[0])
        for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")
    }


def _load_checkpoints(connection: sqlite3.Connection, cutoff: datetime | None) -> list[_Checkpoint]:
    rows = connection.execute(
        "SELECT * FROM forecast_evaluation_events_v2 "
        "WHERE cohort_version='weather-evaluation-v1' AND eligible=1 "
        "ORDER BY considered_at_utc, id"
    ).fetchall()
    grouped: dict[tuple[str, str], sqlite3.Row] = {}
    for row in rows:
        considered = _parse_datetime(row["considered_at_utc"])
        if considered is None or (cutoff is not None and considered > cutoff):
            continue
        segment = _segment(str(row["phase"] or ""), _int_or_none(row["lead_time_seconds"]))
        if segment is None:
            continue
        key = (str(row["event_id"]), segment)
        previous = grouped.get(key)
        previous_time = (
            _parse_datetime(previous["considered_at_utc"])
            if previous is not None
            else datetime.min.replace(tzinfo=UTC)
        )
        if previous is None or (considered, int(row["id"])) > (previous_time, int(previous["id"])):
            grouped[key] = row

    result: list[_Checkpoint] = []
    for row in grouped.values():
        segment = _segment(str(row["phase"] or ""), _int_or_none(row["lead_time_seconds"]))
        if segment is None:
            continue
        attempt_rows = connection.execute(
            "SELECT algorithm_version, expected, status, prediction_id "
            "FROM forecast_evaluation_algorithms_v2 WHERE evaluation_event_id=?",
            (row["id"],),
        ).fetchall()
        attempts = {
            str(item["algorithm_version"]): _Attempt(
                expected=bool(item["expected"]),
                status=str(item["status"]),
                prediction_id=_int_or_none(item["prediction_id"]),
            )
            for item in attempt_rows
            if str(item["algorithm_version"]) in _ALGORITHM_VERSIONS
        }
        considered = _parse_datetime(row["considered_at_utc"])
        if considered is None:
            continue
        result.append(
            _Checkpoint(
                row_id=int(row["id"]),
                event_id=str(row["event_id"]),
                scan_run_id=int(row["scan_run_id"]),
                considered_at=considered,
                phase=str(row["phase"]),
                segment=segment,
                lead_time_seconds=_int_or_none(row["lead_time_seconds"]),
                attempts=attempts,
            )
        )
    return sorted(result, key=lambda item: (item.considered_at, item.row_id))


def _load_predictions(
    connection: sqlite3.Connection,
    checkpoints: list[_Checkpoint],
    cutoff: datetime | None,
) -> dict[int, _Prediction]:
    ids = {
        attempt.prediction_id
        for checkpoint in checkpoints
        for attempt in checkpoint.attempts.values()
        if attempt.expected and attempt.prediction_id is not None
    }
    if not ids:
        return {}
    result: dict[int, _Prediction] = {}
    placeholders = ",".join("?" for _ in ids)
    rows = connection.execute(
        f"SELECT id FROM forecast_predictions_v2 WHERE id IN ({placeholders})",
        tuple(sorted(ids)),
    ).fetchall()
    for row in rows:
        prediction_id = int(row["id"])
        prediction = _read_prediction(connection, prediction_id, cutoff)
        if prediction is not None:
            result[prediction_id] = prediction
    return result


def _read_prediction(
    connection: sqlite3.Connection, prediction_id: int, cutoff: datetime | None
) -> _Prediction | None:
    row = connection.execute(
        "SELECT p.*, m.metadata_json AS model_metadata_json "
        "FROM forecast_predictions_v2 p "
        "LEFT JOIN forecast_model_runs_v2 m ON m.id=p.model_run_id WHERE p.id=?",
        (prediction_id,),
    ).fetchone()
    if row is None:
        return None
    issued = _parse_datetime(row["issued_at_utc"])
    if issued is None or (cutoff is not None and issued > cutoff):
        return None
    probability_rows = connection.execute(
        "SELECT market_id, probability FROM forecast_probabilities_v2 "
        "WHERE prediction_id=? ORDER BY ordinal",
        (prediction_id,),
    ).fetchall()
    probabilities: dict[str, Decimal] = {}
    for item in probability_rows:
        value = _decimal_or_none(item["probability"])
        if value is not None:
            probabilities[str(item["market_id"])] = value
    scenario_rows = connection.execute(
        "SELECT raw_max_c, adjusted_max_c FROM forecast_scenarios_v2 "
        "WHERE prediction_id=? ORDER BY rowid",
        (prediction_id,),
    ).fetchall()
    scenario_values: list[Decimal] = []
    adjusted_values: list[Decimal] = []
    for item in scenario_rows:
        raw = _decimal_or_none(item["raw_max_c"])
        adjusted = _decimal_or_none(item["adjusted_max_c"])
        if raw is not None and adjusted is not None:
            scenario_values.append(raw)
            adjusted_values.append(adjusted)
    return _Prediction(
        prediction_id=prediction_id,
        event_id=str(row["event_id"]),
        algorithm_version=str(row["algorithm_version"]),
        phase=str(row["phase"]),
        issued_at=issued,
        point_forecast_c=_decimal_or_none(row["point_forecast_c"]),
        distribution_mass=_decimal_or_none(row["distribution_mass"]),
        probabilities=probabilities,
        scenarios=tuple(scenario_values),
        adjusted_scenarios=tuple(adjusted_values),
        observed_floor_c=_decimal_or_none(row["observed_floor_c"]),
        metadata={
            **_json_object(row["model_metadata_json"]),
            **_json_object(row["metadata_json"]),
        },
    )


def _load_outcomes(connection: sqlite3.Connection, cutoff: datetime | None) -> dict[str, _Outcome]:
    rows = connection.execute(
        "SELECT event_id, actual_max_c, winning_market_id, recorded_at_utc "
        "FROM forecast_outcome_versions_v2 ORDER BY recorded_at_utc, id"
    ).fetchall()
    result: dict[str, _Outcome] = {}
    for row in rows:
        recorded = _parse_datetime(row["recorded_at_utc"])
        actual = _decimal_or_none(row["actual_max_c"])
        if recorded is None or actual is None or (cutoff is not None and recorded > cutoff):
            continue
        result[str(row["event_id"])] = _Outcome(
            event_id=str(row["event_id"]),
            actual_max_c=actual,
            winning_market_id=str(row["winning_market_id"]),
            recorded_at=recorded,
        )
    return result


def _build_model_reports(
    checkpoints: list[_Checkpoint],
    predictions: Mapping[int, _Prediction],
    outcomes: Mapping[str, _Outcome],
    *,
    minimum_events: int,
    bootstrap_samples: int,
) -> list[dict[str, object]]:
    reports: list[dict[str, object]] = []
    for spec in COMPARISON_ALGORITHMS:
        version = spec["algorithm_version"]
        expected = [
            checkpoint
            for checkpoint in checkpoints
            if checkpoint.attempts.get(version, _Attempt(False, "", None)).expected
        ]
        predicted = [
            checkpoint
            for checkpoint in expected
            if _prediction_for(version, checkpoint, predictions) is not None
        ]
        resolved = [checkpoint for checkpoint in expected if checkpoint.event_id in outcomes]
        raw_cases = _cases_for(version, checkpoints, predictions, outcomes)
        cases = _dedupe_cases(raw_cases)
        expected_events = _dedupe_checkpoints(expected)
        predicted_events = _dedupe_checkpoints(predicted)
        resolved_events = _dedupe_checkpoints(resolved)
        phase_summaries = []
        by_checkpoint = []
        for phase in (ForecastPhase.LEAD_TIME.value, ForecastPhase.INTRADAY.value):
            phase_expected = _dedupe_checkpoints([item for item in expected if item.phase == phase])
            phase_predicted = _dedupe_checkpoints(
                [item for item in predicted if item.phase == phase]
            )
            phase_resolved = _dedupe_checkpoints([item for item in resolved if item.phase == phase])
            phase_summaries.append(
                _summary(
                    label=phase,
                    cases=_dedupe_cases(
                        [item for item in raw_cases if item.checkpoint.phase == phase]
                    ),
                    expected=len(phase_expected),
                    predicted=len(phase_predicted),
                    resolved=len(phase_resolved),
                    minimum_events=minimum_events,
                    bootstrap_samples=bootstrap_samples,
                    seed=version + phase,
                )
            )
        for segment in sorted({checkpoint.segment for checkpoint in checkpoints}):
            segment_expected = [item for item in expected if item.segment == segment]
            segment_predicted = [item for item in predicted if item.segment == segment]
            segment_resolved = [item for item in resolved if item.segment == segment]
            by_checkpoint.append(
                _summary(
                    label=segment,
                    cases=[item for item in raw_cases if item.checkpoint.segment == segment],
                    expected=len(segment_expected),
                    predicted=len(segment_predicted),
                    resolved=len(segment_resolved),
                    minimum_events=minimum_events,
                    bootstrap_samples=bootstrap_samples,
                    seed=version + segment,
                )
            )
        reports.append(
            {
                **spec,
                "counts": {
                    "expected_checkpoints": len(expected),
                    "prediction_checkpoints": len(predicted),
                    "resolved_checkpoints": len(resolved),
                    "evaluated_checkpoints": len(raw_cases),
                    "unique_expected_events": len(expected_events),
                    "unique_prediction_events": len(predicted_events),
                    "unique_resolved_events": len(resolved_events),
                    "unique_evaluated_events": len(cases),
                },
                "coverage": {
                    "forecast": _rate(len(predicted_events), len(expected_events)),
                    "outcome": _rate(len(resolved_events), len(expected_events)),
                    "evaluation": _rate(len(cases), len(resolved_events)),
                    "checkpoint_forecast": _rate(len(predicted), len(expected)),
                    "checkpoint_outcome": _rate(len(resolved), len(expected)),
                    "checkpoint_evaluation": _rate(len(raw_cases), len(resolved)),
                },
                "metrics": _metrics(cases, seed=version, bootstrap_samples=bootstrap_samples),
                "model_state": _model_state(version, expected, predictions),
                "phase_summaries": phase_summaries,
                "by_checkpoint": by_checkpoint,
            }
        )
    return reports


def _model_state(
    algorithm: str,
    checkpoints: list[_Checkpoint],
    predictions: Mapping[int, _Prediction],
) -> dict[str, object]:
    selected = [
        prediction
        for checkpoint in checkpoints
        if (prediction := _prediction_for(algorithm, checkpoint, predictions)) is not None
    ]
    unique = {item.prediction_id: item for item in selected}
    if algorithm == V1_ALGORITHM:
        return {
            "role": "active_paper_decision_baseline",
            "decision_use": True,
            "numeric_probability_source": "deterministic_fixed_sigma_weather_model",
        }
    if algorithm == RAW_ECMWF_ALGORITHM:
        return {
            "role": "shadow_read_only_baseline",
            "decision_use": False,
            "numeric_probability_source": "raw_empirical_ifs_ens_members",
        }

    fitted = [
        item for item in unique.values() if item.metadata.get("uses_station_correction") is True
    ]
    raw_pairs = 0
    identical_pairs = 0
    for checkpoint in checkpoints:
        raw = _prediction_for(RAW_ECMWF_ALGORITHM, checkpoint, predictions)
        candidate = _prediction_for(V2_ALGORITHM, checkpoint, predictions)
        if raw is None or candidate is None:
            continue
        raw_pairs += 1
        if (
            raw.point_forecast_c == candidate.point_forecast_c
            and raw.probabilities == candidate.probabilities
        ):
            identical_pairs += 1
    return {
        "role": "shadow_candidate",
        "decision_use": False,
        "numeric_probability_source": "station_intraday_conditioned_weather_model",
        "unique_prediction_count": len(unique),
        "station_correction_fitted_prediction_count": len(fitted),
        "station_correction_unfitted_prediction_count": len(unique) - len(fitted),
        "paired_with_raw_checkpoint_count": raw_pairs,
        "identical_to_raw_checkpoint_count": identical_pairs,
        "training_evaluation_separation": "causal_online_but_not_frozen_holdout",
    }


def _cases_for(
    algorithm: str,
    checkpoints: list[_Checkpoint],
    predictions: Mapping[int, _Prediction],
    outcomes: Mapping[str, _Outcome],
) -> list[_Case]:
    result: list[_Case] = []
    for checkpoint in checkpoints:
        prediction = _prediction_for(algorithm, checkpoint, predictions)
        outcome = outcomes.get(checkpoint.event_id)
        if prediction is not None and outcome is not None:
            result.append(_Case(checkpoint, prediction, outcome))
    return result


def _dedupe_checkpoints(checkpoints: list[_Checkpoint]) -> list[_Checkpoint]:
    """Choose one deterministic checkpoint per event for aggregate metrics."""

    selected: dict[str, _Checkpoint] = {}
    for checkpoint in checkpoints:
        prior = selected.get(checkpoint.event_id)
        if prior is None or (checkpoint.considered_at, checkpoint.row_id) > (
            prior.considered_at,
            prior.row_id,
        ):
            selected[checkpoint.event_id] = checkpoint
    return sorted(selected.values(), key=lambda item: (item.considered_at, item.row_id))


def _dedupe_cases(cases: list[_Case]) -> list[_Case]:
    """Choose one deterministic case per event for aggregate model metrics."""

    selected: dict[str, _Case] = {}
    for case in cases:
        prior = selected.get(case.checkpoint.event_id)
        if prior is None or (
            case.checkpoint.considered_at,
            case.checkpoint.row_id,
        ) > (
            prior.checkpoint.considered_at,
            prior.checkpoint.row_id,
        ):
            selected[case.checkpoint.event_id] = case
    return sorted(
        selected.values(), key=lambda item: (item.checkpoint.considered_at, item.checkpoint.row_id)
    )


def _prediction_for(
    algorithm: str, checkpoint: _Checkpoint, predictions: Mapping[int, _Prediction]
) -> _Prediction | None:
    attempt = checkpoint.attempts.get(algorithm)
    if attempt is None or not attempt.expected or attempt.prediction_id is None:
        return None
    prediction = predictions.get(attempt.prediction_id)
    if (
        prediction is None
        or prediction.algorithm_version != algorithm
        or prediction.event_id != checkpoint.event_id
        or prediction.phase != checkpoint.phase
    ):
        return None
    return prediction


def _summary(
    *,
    label: str,
    cases: list[_Case],
    expected: int,
    predicted: int,
    resolved: int,
    minimum_events: int,
    bootstrap_samples: int,
    seed: str,
) -> dict[str, object]:
    metrics = _metrics(cases, seed=seed, bootstrap_samples=bootstrap_samples)
    return {
        "phase": label if label in {"LEAD_TIME", "INTRADAY"} else None,
        "label": label,
        "expected_checkpoints": expected,
        "prediction_checkpoints": predicted,
        "resolved_checkpoints": resolved,
        "evaluated_checkpoints": len(cases),
        "expected_event_count": expected,
        "prediction_event_count": predicted,
        "resolved_event_count": resolved,
        "evaluated_event_count": len(cases),
        "forecast_coverage": _rate(predicted, expected),
        "outcome_coverage": _rate(resolved, expected),
        "evaluation_coverage": _rate(len(cases), resolved),
        "sample_state": "monitoring_threshold_met"
        if len(cases) >= minimum_events
        else "insufficient_sample",
        "sample_message": (
            f"{len(cases)}/{minimum_events} resolved unique events; descriptive only"
            if len(cases) < minimum_events
            else f"{len(cases)} resolved unique events; threshold met but not a promotion decision"
        ),
        "metrics": metrics,
    }


def _metrics(cases: list[_Case], *, seed: str, bootstrap_samples: int) -> dict[str, object]:
    complete = [item for item in cases if _complete(item)]
    absolute_errors: list[float] = []
    brier: list[float] = []
    correctness: list[int] = []
    confidence_correct: list[tuple[float, int]] = []
    for item in complete:
        top_market, confidence = _top_probability(item.prediction.probabilities)
        correct = int(top_market == item.outcome.winning_market_id)
        correctness.append(correct)
        confidence_correct.append((float(confidence), correct))
        absolute_errors.append(_mae(item))
        brier.append(_brier(item))
    ece_value = _ece(confidence_correct)
    values: dict[str, object] = {
        "mae_c": _metric_interval(absolute_errors, seed + "|mae", bootstrap_samples),
        "exact_bracket_accuracy": _metric_interval(
            [float(value) for value in correctness], seed + "|accuracy", bootstrap_samples
        ),
        "multiclass_brier": _metric_interval(brier, seed + "|brier", bootstrap_samples),
        "ece": _ece_interval(confidence_correct, seed + "|ece", bootstrap_samples),
        "calibration": _calibration(confidence_correct),
        "n": len(complete),
    }
    # Compact scalar aliases are retained for the existing CLI/table.
    values["mae"] = (
        None if not absolute_errors else _string(sum(absolute_errors) / len(absolute_errors))
    )
    values["accuracy"] = None if not correctness else _string(sum(correctness) / len(correctness))
    values["brier"] = None if not brier else _string(sum(brier) / len(brier))
    values["ece_value"] = None if not confidence_correct else _string(ece_value)
    return values


def _metric_interval(values: list[float], seed: str, samples: int) -> dict[str, object]:
    if not values:
        return {"value": None, "ci95": None, "n": 0, "method": "not_estimable"}
    value = sum(values) / len(values)
    if len(values) < 2 or samples < 100:
        return {"value": _string(value), "ci95": None, "n": len(values), "method": "point_only"}
    rng = random.Random(int(hashlib.sha256(seed.encode()).hexdigest()[:16], 16))
    estimates = [
        sum(values[rng.randrange(len(values))] for _ in values) / len(values)
        for _ in range(samples)
    ]
    estimates.sort()
    low = estimates[int((len(estimates) - 1) * 0.025)]
    high = estimates[int((len(estimates) - 1) * 0.975)]
    return {
        "value": _string(value),
        "ci95": [_string(low), _string(high)],
        "n": len(values),
        "method": "deterministic_percentile_bootstrap_95",
    }


def _ece_interval(values: list[tuple[float, int]], seed: str, samples: int) -> dict[str, object]:
    if not values:
        return {"value": None, "ci95": None, "n": 0, "method": "not_estimable"}
    value = _ece(values)
    if len(values) < 2 or samples < 100:
        return {
            "value": _string(value),
            "ci95": None,
            "n": len(values),
            "method": "point_only",
        }
    rng = random.Random(int(hashlib.sha256(seed.encode()).hexdigest()[:16], 16))
    estimates: list[float] = []
    for _ in range(samples):
        draw = [values[rng.randrange(len(values))] for _ in values]
        estimates.append(_ece(draw))
    estimates.sort()
    return {
        "value": _string(value),
        "ci95": [
            _string(estimates[int((len(estimates) - 1) * 0.025)]),
            _string(estimates[int((len(estimates) - 1) * 0.975)]),
        ],
        "n": len(values),
        "method": "deterministic_percentile_bootstrap_95",
    }


def _calibration(values: list[tuple[float, int]], bins: int = 10) -> list[dict[str, object]]:
    grouped: list[list[tuple[float, int]]] = [[] for _ in range(bins)]
    for confidence, correct in values:
        grouped[min(bins - 1, max(0, int(confidence * bins)))].append((confidence, correct))
    result: list[dict[str, object]] = []
    for index, rows in enumerate(grouped):
        if not rows:
            continue
        mean_confidence = sum(item[0] for item in rows) / len(rows)
        accuracy = sum(item[1] for item in rows) / len(rows)
        result.append(
            {
                "lower": _string(index / bins),
                "upper": _string((index + 1) / bins),
                "count": len(rows),
                "mean_confidence": _string(mean_confidence),
                "empirical_accuracy": _string(accuracy),
                "calibration_gap": _string(accuracy - mean_confidence),
            }
        )
    return result


def _ece(values: list[tuple[float, int]], bins: int = 10) -> float:
    if not values:
        return 0.0
    grouped: list[list[tuple[float, int]]] = [[] for _ in range(bins)]
    for confidence, correct in values:
        grouped[min(bins - 1, max(0, int(confidence * bins)))].append((confidence, correct))
    total = len(values)
    return sum(
        abs(sum(c for c, _ in rows) / len(rows) - sum(y for _, y in rows) / len(rows))
        * len(rows)
        / total
        for rows in grouped
        if rows
    )


def _checkpoint_reports(
    checkpoints: list[_Checkpoint],
    predictions: Mapping[int, _Prediction],
    outcomes: Mapping[str, _Outcome],
) -> list[dict[str, object]]:
    reports: list[dict[str, object]] = []
    for segment in sorted({item.segment for item in checkpoints}):
        selected = [item for item in checkpoints if item.segment == segment]
        spreads: list[float] = []
        for checkpoint in selected:
            issued = [
                prediction.issued_at
                for version in _ALGORITHM_VERSIONS
                if (prediction := _prediction_for(version, checkpoint, predictions)) is not None
            ]
            if len(issued) >= 2:
                spreads.append((max(issued) - min(issued)).total_seconds())
        reports.append(
            {
                "label": segment,
                "phase": selected[0].phase if selected else None,
                "checkpoint_count": len(selected),
                "unique_event_count": len({item.event_id for item in selected}),
                "resolved_checkpoint_count": sum(item.event_id in outcomes for item in selected),
                "issuance_spread_seconds": {
                    "count": len(spreads),
                    "mean": None if not spreads else _string(sum(spreads) / len(spreads)),
                    "max": None if not spreads else _string(max(spreads)),
                },
            }
        )
    return reports


def _paired_comparison(
    checkpoints: list[_Checkpoint],
    predictions: Mapping[int, _Prediction],
    outcomes: Mapping[str, _Outcome],
    *,
    minimum_events: int,
    bootstrap_samples: int,
) -> dict[str, object]:
    """Compare candidates only at the same selected registry checkpoint."""

    result: dict[str, object] = {}
    for candidate in (RAW_ECMWF_ALGORITHM, V2_ALGORITHM):
        pairs: list[tuple[_Case, _Case, float]] = []
        for checkpoint in checkpoints:
            outcome = outcomes.get(checkpoint.event_id)
            if outcome is None:
                continue
            baseline = _prediction_for(V1_ALGORITHM, checkpoint, predictions)
            challenger = _prediction_for(candidate, checkpoint, predictions)
            if baseline is None or challenger is None:
                continue
            baseline_case = _Case(checkpoint, baseline, outcome)
            challenger_case = _Case(checkpoint, challenger, outcome)
            if not _complete(baseline_case) or not _complete(challenger_case):
                continue
            spread = abs((challenger.issued_at - baseline.issued_at).total_seconds())
            pairs.append((baseline_case, challenger_case, spread))
        phase_reports: list[dict[str, object]] = []
        for phase in (ForecastPhase.LEAD_TIME.value, ForecastPhase.INTRADAY.value):
            phase_pairs = _dedupe_pairs(
                [item for item in pairs if item[0].checkpoint.phase == phase]
            )
            phase_reports.append(
                _paired_summary(
                    candidate,
                    phase,
                    phase_pairs,
                    minimum_events=minimum_events,
                    bootstrap_samples=bootstrap_samples,
                )
            )
        result[candidate] = phase_reports
    return result


def _dedupe_pairs(
    pairs: list[tuple[_Case, _Case, float]],
) -> list[tuple[_Case, _Case, float]]:
    """Choose the latest paired checkpoint per event within a phase."""

    selected: dict[str, tuple[_Case, _Case, float]] = {}
    for pair in pairs:
        checkpoint = pair[0].checkpoint
        previous = selected.get(checkpoint.event_id)
        if previous is None or (checkpoint.considered_at, checkpoint.row_id) > (
            previous[0].checkpoint.considered_at,
            previous[0].checkpoint.row_id,
        ):
            selected[checkpoint.event_id] = pair
    return sorted(
        selected.values(),
        key=lambda item: (item[0].checkpoint.considered_at, item[0].checkpoint.row_id),
    )


def _paired_summary(
    candidate: str,
    phase: str,
    pairs: list[tuple[_Case, _Case, float]],
    *,
    minimum_events: int,
    bootstrap_samples: int,
) -> dict[str, object]:
    if not pairs:
        return {
            "phase": phase,
            "common_checkpoint_count": 0,
            "common_event_count": 0,
            "status": "no_common_resolved_cases",
            "delta_candidate_minus_v1": {},
        }
    mae_delta: list[float] = []
    brier_delta: list[float] = []
    accuracy_delta: list[float] = []
    ece_pairs: list[tuple[float, int, float, int]] = []
    for baseline, challenger, _ in pairs:
        mae_delta.append(_mae(challenger) - _mae(baseline))
        brier_delta.append(_brier(challenger) - _brier(baseline))
        accuracy_delta.append(float(_correct(challenger) - _correct(baseline)))
        b_conf, b_correct = _top_confidence_correct(baseline)
        c_conf, c_correct = _top_confidence_correct(challenger)
        ece_pairs.append((b_conf, b_correct, c_conf, c_correct))
    ece_delta = _ece([(item[2], item[3]) for item in ece_pairs]) - _ece(
        [(item[0], item[1]) for item in ece_pairs]
    )
    common_events = len({baseline.checkpoint.event_id for baseline, _, _ in pairs})
    spreads = [spread for _, _, spread in pairs]
    return {
        "phase": phase,
        "common_checkpoint_count": len(pairs),
        "common_event_count": common_events,
        "status": "descriptive_only"
        if len(pairs) < minimum_events
        else "threshold_met_but_not_promoted",
        "delta_candidate_minus_v1": {
            "mae_c": _metric_interval(mae_delta, candidate + phase + "|mae", bootstrap_samples),
            "brier": _metric_interval(brier_delta, candidate + phase + "|brier", bootstrap_samples),
            "accuracy": _metric_interval(
                accuracy_delta, candidate + phase + "|accuracy", bootstrap_samples
            ),
            "ece": {
                "value": _string(ece_delta),
                "ci95": _paired_ece_interval(ece_pairs, candidate, phase, bootstrap_samples),
                "n": len(pairs),
                "method": "deterministic_percentile_bootstrap_95"
                if len(pairs) >= 2 and bootstrap_samples >= 100
                else "point_only",
            },
        },
        "paired_counts": {
            "candidate_better_mae": sum(value < 0 for value in mae_delta),
            "candidate_better_brier": sum(value < 0 for value in brier_delta),
            "candidate_better_accuracy": sum(value > 0 for value in accuracy_delta),
            "ties_accuracy": sum(value == 0 for value in accuracy_delta),
        },
        "checkpoint_alignment": {
            "same_evaluation_checkpoint": True,
            "same_registry_scan_run_pair_count": len(pairs),
            "registry_scan_run_count": len(
                {baseline.checkpoint.scan_run_id for baseline, _, _ in pairs}
            ),
            "issuance_spread_seconds": {
                "mean": _string(sum(spreads) / len(spreads)),
                "max": _string(max(spreads)),
            },
        },
    }


def _paired_ece_interval(
    values: list[tuple[float, int, float, int]], candidate: str, phase: str, samples: int
) -> list[str] | None:
    if len(values) < 2 or samples < 100:
        return None
    rng = random.Random(
        int(hashlib.sha256((candidate + phase + "|ece").encode()).hexdigest()[:16], 16)
    )
    estimates: list[float] = []
    for _ in range(samples):
        draw = [values[rng.randrange(len(values))] for _ in values]
        estimates.append(
            _ece([(item[2], item[3]) for item in draw])
            - _ece([(item[0], item[1]) for item in draw])
        )
    estimates.sort()
    return [
        _string(estimates[int((len(estimates) - 1) * 0.025)]),
        _string(estimates[int((len(estimates) - 1) * 0.975)]),
    ]


def _complete(case: _Case) -> bool:
    return bool(
        case.prediction.point_forecast_c is not None
        and case.prediction.distribution_mass is not None
        and Decimal("0.999999") <= case.prediction.distribution_mass <= Decimal("1.000001")
        and case.outcome.winning_market_id in case.prediction.probabilities
    )


def _mae(case: _Case) -> float:
    assert case.prediction.point_forecast_c is not None
    return float(abs(case.prediction.point_forecast_c - case.outcome.actual_max_c))


def _brier(case: _Case) -> float:
    return float(
        sum(
            (probability - (Decimal(1) if market == case.outcome.winning_market_id else 0)) ** 2
            for market, probability in case.prediction.probabilities.items()
        )
    )


def _correct(case: _Case) -> int:
    top, _ = _top_probability(case.prediction.probabilities)
    return int(top == case.outcome.winning_market_id)


def _top_confidence_correct(case: _Case) -> tuple[float, int]:
    _, confidence = _top_probability(case.prediction.probabilities)
    return float(confidence), _correct(case)


def _top_probability(probabilities: Mapping[str, Decimal]) -> tuple[str, Decimal]:
    """Match forecast_models' deterministic low-market-ID tie break."""

    ordered = sorted(probabilities.items(), key=lambda pair: pair[0])
    return max(ordered, key=lambda pair: pair[1])


def _sigma_diagnostics(
    connection: sqlite3.Connection,
    predictions: Mapping[int, _Prediction],
    outcomes: Mapping[str, _Outcome],
    cutoff: datetime | None,
) -> dict[str, object]:
    tables = _tables(connection)
    required = {
        "decisions",
        "paper_orders",
        "market_snapshots",
        "forecast_model_runs_v2",
        "forecast_predictions_v2",
        "forecast_probabilities_v2",
        "forecast_scenarios_v2",
    }
    if not required.issubset(tables):
        return {"summary": {"status": "unavailable", "analyzed_count": 0}, "trades": []}
    rows = connection.execute(
        "SELECT d.*, p.strategy_version AS order_strategy, p.outcome AS order_outcome, "
        "p.api_cost_usd AS order_api_cost FROM decisions d LEFT JOIN paper_orders p "
        "ON p.id=d.paper_order_id WHERE d.action='PAPER_BUY' ORDER BY d.id"
    ).fetchall()
    trades: list[dict[str, object]] = []
    skipped = 0
    for row in rows:
        created = _parse_datetime(row["created_at"])
        if created is None or (cutoff is not None and created > cutoff):
            continue
        payload = _json_object(row["payload_json"])
        strategy = str(payload.get("strategy_version") or row["order_strategy"] or "")
        explicit_algorithm = str(payload.get("forecast_algorithm_version") or "")
        normalized_strategy = strategy.lower()
        if normalized_strategy and normalized_strategy not in {
            "v1",
            "recovered_v1",
            V1_ALGORITHM,
        }:
            continue
        if explicit_algorithm and explicit_algorithm != V1_ALGORITHM:
            continue
        if not normalized_strategy and explicit_algorithm != V1_ALGORITHM:
            continue
        prediction = _entry_prediction(connection, row, created, predictions)
        if prediction is None or not prediction.scenarios:
            skipped += 1
            continue
        probability_rows = connection.execute(
            "SELECT fp.*, ms.payload_json FROM forecast_probabilities_v2 fp "
            "JOIN market_snapshots ms ON ms.id=fp.market_snapshot_id "
            "WHERE fp.prediction_id=? ORDER BY fp.ordinal",
            (prediction.prediction_id,),
        ).fetchall()
        selected_market = str(row["market_id"])
        selected = next(
            (item for item in probability_rows if str(item["market_id"]) == selected_market), None
        )
        if selected is None:
            skipped += 1
            continue
        brackets: dict[str, Bracket] = {}
        for item in probability_rows:
            brackets[str(item["market_id"])] = Bracket(
                market_id=str(item["market_id"]),
                label=str(item["outcome_label"]),
                lower=_float_or_none(item["lower_bound"]),
                upper=_float_or_none(item["upper_bound"]),
                lower_inclusive=bool(item["lower_inclusive"]),
                upper_inclusive=bool(item["upper_inclusive"]),
            )
        try:
            raw_unconditioned = empirical_bracket_probabilities(prediction.scenarios, brackets)
            raw_conditioned = empirical_bracket_probabilities(
                prediction.scenarios, brackets, observed_floor_c=prediction.observed_floor_c
            )
            archived_adjusted = empirical_bracket_probabilities(
                prediction.adjusted_scenarios, brackets
            )
            expected_adjusted_scenarios = tuple(
                value
                if prediction.observed_floor_c is None
                else max(value, prediction.observed_floor_c)
                for value in prediction.scenarios
            )
            archived_conditioning_matches = (
                prediction.adjusted_scenarios == expected_adjusted_scenarios
            )
            sigma_c = (
                _decimal_or_none(prediction.metadata.get("weather_error_sigma_c"))
                or DEFAULT_V1_SIGMA_C
            )
            replayed_v1 = v1_sigma_bracket_probabilities(
                prediction.scenarios,
                brackets,
                sigma_c=sigma_c,
                observed_floor_c=prediction.observed_floor_c,
            )
            clamped_v1 = clamped_sigma_bracket_probabilities(
                prediction.scenarios,
                brackets,
                sigma_c=sigma_c,
                observed_floor_c=prediction.observed_floor_c,
            )
            snapshot = MarketSnapshot.model_validate(_json_object(selected["payload_json"]))
            api_cost = (
                _decimal_or_none(payload.get("api_cost_usd"))
                or _decimal_or_none(row["order_api_cost"])
                or Decimal(0)
            )
            raw_qualification = replay_paper_qualification(
                snapshot=snapshot,
                probability=raw_conditioned[selected_market],
                thresholds=SigmaTradeThresholds(),
                api_cost_usd=api_cost,
            )
            v1_probability = _decimal(selected["probability"])
            v1_qualification = replay_paper_qualification(
                snapshot=snapshot,
                probability=v1_probability,
                thresholds=SigmaTradeThresholds(),
                api_cost_usd=api_cost,
            )
            clamped_qualification = replay_paper_qualification(
                snapshot=snapshot,
                probability=clamped_v1[selected_market],
                thresholds=SigmaTradeThresholds(),
                api_cost_usd=api_cost,
            )
        except Exception:
            skipped += 1
            continue
        outcome = outcomes.get(str(row["event_id"]))
        side = str(payload.get("outcome") or row["order_outcome"] or "YES").upper()
        outcome_label = "UNRESOLVED"
        if outcome is not None:
            winner = str(outcome.winning_market_id) == selected_market
            outcome_label = (
                ("WIN" if winner else "LOSS") if side != "NO" else ("LOSS" if winner else "WIN")
            )
        trades.append(
            {
                "decision_id": int(row["id"]),
                "paper_order_id": _int_or_none(row["paper_order_id"]),
                "event_id": str(row["event_id"]),
                "market_id": selected_market,
                "entry_prediction_id": prediction.prediction_id,
                "sigma_c": str(sigma_c),
                "fixed_sigma_1_5": sigma_c == DEFAULT_V1_SIGMA_C,
                "scenario_count": len(prediction.scenarios),
                "observed_floor_c": (
                    None
                    if prediction.observed_floor_c is None
                    else str(prediction.observed_floor_c)
                ),
                "observed_floor_applied": prediction.observed_floor_c is not None,
                "v1_sigma_probability": str(v1_probability),
                "replayed_v1_sigma_probability": str(replayed_v1[selected_market]),
                "stored_probability_matches_replay": (
                    v1_probability == replayed_v1[selected_market]
                ),
                "raw_empirical_probability": str(raw_conditioned[selected_market]),
                "raw_unconditioned_probability": str(raw_unconditioned[selected_market]),
                "archived_adjusted_empirical_probability": str(archived_adjusted[selected_market]),
                "archived_member_conditioning_matches_replay": archived_conditioning_matches,
                "observed_floor_probability_delta": str(
                    raw_conditioned[selected_market] - raw_unconditioned[selected_market]
                ),
                "clamped_sigma_probability": str(clamped_v1[selected_market]),
                "v1_would_qualify": v1_qualification.qualifies,
                "would_qualify_without_sigma": raw_qualification.qualifies,
                "clamped_sigma_would_qualify": clamped_qualification.qualifies,
                "signal_depended_on_sigma": v1_qualification.qualifies
                and not raw_qualification.qualifies,
                "signal_depended_on_truncated_floor_treatment": (
                    v1_qualification.qualifies and not clamped_qualification.qualifies
                ),
                "raw_reason_codes": list(raw_qualification.reason_codes),
                "clamped_reason_codes": list(clamped_qualification.reason_codes),
                "outcome": outcome_label,
            }
        )
    summary = {
        "status": "available",
        "eligible_paper_buy_count": len(trades) + skipped,
        "analyzed_count": len(trades),
        "skipped_missing_entry_artifacts": skipped,
        "v1_qualifying_count": sum(bool(item["v1_would_qualify"]) for item in trades),
        "raw_qualifying_count": sum(bool(item["would_qualify_without_sigma"]) for item in trades),
        "sigma_dependent_count": sum(bool(item["signal_depended_on_sigma"]) for item in trades),
        "fixed_sigma_1_5_count": sum(bool(item["fixed_sigma_1_5"]) for item in trades),
        "observed_floor_applied_count": sum(
            bool(item["observed_floor_applied"]) for item in trades
        ),
        "stored_probability_replay_mismatch_count": sum(
            not bool(item["stored_probability_matches_replay"]) for item in trades
        ),
        "archived_member_conditioning_mismatch_count": sum(
            not bool(item["archived_member_conditioning_matches_replay"]) for item in trades
        ),
        "truncated_floor_treatment_dependent_count": sum(
            bool(item["signal_depended_on_truncated_floor_treatment"]) for item in trades
        ),
        "resolved_count": sum(item["outcome"] != "UNRESOLVED" for item in trades),
        "wins": sum(item["outcome"] == "WIN" for item in trades),
        "losses": sum(item["outcome"] == "LOSS" for item in trades),
    }
    return {
        "summary": summary,
        "method": {
            "fixed_sigma_c": str(DEFAULT_V1_SIGMA_C),
            "raw_conditioning": "max(raw_member_c, archived_observed_floor_c)",
            "kernel_counterfactual": (
                "clamp sub-floor kernel mass at the observed maximum; diagnostic only"
            ),
            "production_use": False,
        },
        "trades": trades,
    }


def _entry_prediction(
    connection: sqlite3.Connection,
    decision: sqlite3.Row,
    created: datetime,
    predictions: Mapping[int, _Prediction],
) -> _Prediction | None:
    rows = connection.execute(
        "SELECT id, scan_run_id, issued_at_utc FROM forecast_predictions_v2 "
        "WHERE event_id=? AND algorithm_version=? AND issued_at_utc<=? "
        "ORDER BY issued_at_utc DESC, id DESC",
        (decision["event_id"], V1_ALGORITHM, created.isoformat()),
    ).fetchall()
    same_run = [
        row for row in rows if _int_or_none(row["scan_run_id"]) == _int_or_none(decision["run_id"])
    ]
    # For sigma-entry reconstruction, a forecast from another scan may have
    # a different order-book snapshot.  Keep only same-run entry evidence.
    if not same_run:
        return None
    prediction_id = int(same_run[0]["id"])
    # Entry forecasts can predate the latest evaluation checkpoint selected
    # for model comparison, so they are loaded directly from the same
    # read-only connection rather than assumed to be in ``predictions``.
    return predictions.get(prediction_id) or _read_prediction(connection, prediction_id, created)


def _pnl_by_strategy(connection: sqlite3.Connection, cutoff: datetime | None) -> dict[str, object]:
    if "paper_orders" not in _tables(connection):
        return {}
    rows = connection.execute(
        "SELECT strategy_version, realized_pnl_usd, api_cost_usd, won, settled_at, closed_at "
        "FROM paper_orders WHERE status IN ('PAPER_SETTLED','SETTLED','CLOSED')"
    ).fetchall()
    result: dict[str, dict[str, object]] = {}
    for row in rows:
        settled = _parse_datetime(row["settled_at"] or row["closed_at"])
        if cutoff is not None and settled is not None and settled > cutoff:
            continue
        strategy = str(row["strategy_version"] or "unknown")
        item = result.setdefault(
            strategy,
            {
                "settled_count": 0,
                "wins": 0,
                "losses": 0,
                "realized_pnl_usd": "0",
                "allocated_order_api_cost_usd": "0",
                "gross_trade_pnl_usd": "0",
            },
        )
        settled_count = _int_or_none(item.get("settled_count")) or 0
        wins = _int_or_none(item.get("wins")) or 0
        losses = _int_or_none(item.get("losses")) or 0
        item["settled_count"] = settled_count + 1
        item["wins"] = wins + int(bool(row["won"]))
        item["losses"] = losses + int(not bool(row["won"]))
        realized = _decimal_or_none(row["realized_pnl_usd"]) or Decimal(0)
        api_cost = _decimal_or_none(row["api_cost_usd"]) or Decimal(0)
        item["realized_pnl_usd"] = str(Decimal(str(item["realized_pnl_usd"])) + realized)
        item["allocated_order_api_cost_usd"] = str(
            Decimal(str(item["allocated_order_api_cost_usd"])) + api_cost
        )
        item["gross_trade_pnl_usd"] = str(
            Decimal(str(item["gross_trade_pnl_usd"])) + realized + api_cost
        )
    return {
        key: {
            **value,
            "accounting_note": "realized includes allocated order API cost",
        }
        for key, value in sorted(result.items())
    }


def _promotion_status(minimum_events: int) -> dict[str, object]:
    return {
        "status": "not_evaluable",
        "v2_promoted": False,
        "candidate": V2_ALGORITHM,
        "baseline": V1_ALGORITHM,
        "holdout_status": "missing_frozen_holdout",
        "online_calibration_status": "online_calibration_not_frozen",
        "minimum_resolved_checkpoints": minimum_events,
        "reason": (
            "No frozen validation period/checkpoints; v2 calibration is "
            "online/descriptive. No improvement claim is made."
        ),
    }


def _empty_report(
    cutoff: datetime | None, minimum_events: int, warnings: list[str]
) -> dict[str, object]:
    return {
        "version": COMPARISON_VERSION,
        "status": "no_data",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "as_of_utc": None if cutoff is None else cutoff.isoformat(),
        "checkpoint_policy": {
            "selection": "latest_registry_opportunity_per_event_per_phase_bin",
            "phase_aggregation": "latest_selected_checkpoint_per_event_per_phase",
            "pairing": "same_selected_evaluation_registry_row",
            "frozen_holdout": False,
        },
        "minimum_reliable_events": minimum_events,
        "models": [],
        "checkpoints": [],
        "paired_vs_v1": {},
        "sigma_dependent_signals": {
            "summary": {"status": "unavailable", "analyzed_count": 0},
            "trades": [],
        },
        "pnl_by_entry_strategy": {},
        "promotion": _promotion_status(minimum_events),
        "warnings": warnings,
    }


def _segment(phase: str, lead_time_seconds: int | None) -> str | None:
    if phase == ForecastPhase.INTRADAY.value:
        return ForecastPhase.INTRADAY.value
    if phase != ForecastPhase.LEAD_TIME.value or lead_time_seconds is None:
        return None
    for lower, upper in zip(
        DEFAULT_LEAD_TIME_BINS_HOURS, DEFAULT_LEAD_TIME_BINS_HOURS[1:], strict=False
    ):
        if lower * 3600 <= lead_time_seconds < upper * 3600:
            return f"LEAD_{lower:03d}_{upper:03d}H"
    if lead_time_seconds >= DEFAULT_LEAD_TIME_BINS_HOURS[-1] * 3600:
        return f"LEAD_{DEFAULT_LEAD_TIME_BINS_HOURS[-1]:03d}H_PLUS"
    return None


def _parse_datetime(value: Any) -> datetime | None:
    if value is None:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    return parsed.replace(tzinfo=UTC) if parsed.tzinfo is None else parsed.astimezone(UTC)


def _as_utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        raise ValueError("as_of_utc must be timezone-aware")
    return value.astimezone(UTC)


def _decimal(value: Any) -> Decimal:
    parsed = _decimal_or_none(value)
    if parsed is None:
        raise ValueError(f"invalid decimal value: {value!r}")
    return parsed


def _decimal_or_none(value: Any) -> Decimal | None:
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return None


def _float_or_none(value: Any) -> float | None:
    parsed = _decimal_or_none(value)
    return None if parsed is None else float(parsed)


def _int_or_none(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _json_object(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    try:
        parsed = json.loads(str(value)) if value is not None else {}
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}
    return dict(parsed) if isinstance(parsed, dict) else {}


def _rate(numerator: int, denominator: int) -> str | None:
    return None if denominator <= 0 else str(Decimal(numerator) / Decimal(denominator))


def _string(value: float | Decimal) -> str:
    return str(value)


__all__ = [
    "COMPARISON_VERSION",
    "COMPARISON_ALGORITHMS",
    "build_comparison_report",
    "compare_forecasts",
    "run_comparison",
]
