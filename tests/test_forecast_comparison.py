from __future__ import annotations

import hashlib
import json
import sqlite3
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any, cast

from polybot.cli import build_parser
from polybot.forecast_comparison import (
    COMPARISON_ALGORITHMS,
    COMPARISON_VERSION,
    RAW_ECMWF_ALGORITHM,
    V1_ALGORITHM,
    compare_forecasts,
)
from polybot.forecast_sensitivity import v1_sigma_bracket_probabilities
from polybot.forecast_store import ForecastStore
from polybot.models import Bracket
from polybot.storage import Storage


def _algorithm_spec(version: str) -> dict[str, str]:
    return next(item for item in COMPARISON_ALGORITHMS if item["algorithm_version"] == version)


def _scan(connection: sqlite3.Connection, at: datetime) -> int:
    cursor = connection.execute(
        "INSERT INTO scan_runs(started_at, completed_at, query, mode, status) "
        "VALUES (?, ?, 'test', 'paper', 'complete')",
        (at.isoformat(), (at + timedelta(minutes=1)).isoformat()),
    )
    assert cursor.lastrowid is not None
    return int(cursor.lastrowid)


def _market_snapshot(
    connection: sqlite3.Connection,
    *,
    run_id: int,
    event_id: str,
    market_id: str,
    captured_at: datetime,
) -> int:
    payload = {
        "event_id": event_id,
        "event_slug": event_id,
        "event_title": event_id,
        "market_id": market_id,
        "market_slug": market_id,
        "market_question": market_id,
        "outcome_label": market_id,
        "asset_id": f"asset-{market_id}",
        "token_id": f"token-{market_id}",
        "condition_id": f"condition-{event_id}",
        "outcome": "YES",
        "end_date": None,
        "accepting_orders": True,
        "book_timestamp": captured_at.isoformat(),
        "book_hash": f"book-{run_id}-{market_id}",
        "bids": [],
        "asks": [],
        "min_order_size": "5",
        "tick_size": "0.01",
        "fee_rate": "0",
        "fee_exponent": "0",
        "fee_taker_only": False,
    }
    cursor = connection.execute(
        "INSERT INTO market_snapshots(run_id, event_id, market_id, captured_at, payload_json) "
        "VALUES (?, ?, ?, ?, ?)",
        (run_id, event_id, market_id, captured_at.isoformat(), json.dumps(payload)),
    )
    assert cursor.lastrowid is not None
    return int(cursor.lastrowid)


def _prediction(
    connection: sqlite3.Connection,
    *,
    run_id: int,
    event_id: str,
    algorithm: str,
    issued_at: datetime,
    point_forecast_c: str,
    probabilities: tuple[str, str],
) -> int:
    spec = _algorithm_spec(algorithm)
    provenance_key = f"provenance-{event_id}-{algorithm}-{issued_at.isoformat()}"
    model_cursor = connection.execute(
        "INSERT INTO forecast_model_runs_v2("
        "source, model, first_fetched_at_utc, metadata_json, provenance_key, created_at_utc"
        ") VALUES (?, ?, ?, '{}', ?, ?)",
        (
            spec["source"],
            spec["model"],
            (issued_at - timedelta(minutes=5)).isoformat(),
            provenance_key,
            issued_at.isoformat(),
        ),
    )
    assert model_cursor.lastrowid is not None
    market_ids = (f"{event_id}-market-a", f"{event_id}-market-b")
    snapshot_ids = tuple(
        _market_snapshot(
            connection,
            run_id=run_id,
            event_id=event_id,
            market_id=market_id,
            captured_at=issued_at,
        )
        for market_id in market_ids
    )
    day_start = datetime(2026, 9, 11, tzinfo=UTC)
    cursor = connection.execute(
        """
        INSERT INTO forecast_predictions_v2(
            scan_run_id, event_id, model_run_id, algorithm_version, phase,
            issued_at_utc, model_fetched_at_utc, lead_time_seconds,
            intraday_elapsed_seconds, intraday_remaining_seconds, station_id,
            observation_date, station_timezone, rule_day_start_utc,
            rule_day_end_utc, display_unit, precision_decimal_places,
            rounding_rule, rules_hash, point_forecast_c, observed_floor_c,
            scenario_count, observation_revision_count, observation_cutoff_at_utc,
            distribution_mass, metadata_json, submission_hash, created_at_utc
        ) VALUES (
            ?, ?, ?, ?, 'LEAD_TIME', ?, ?, 43200, NULL, NULL, 'TEST',
            '2026-09-11', 'UTC', ?, ?, 'C', 0, 'half-up', 'rules-v1',
            ?, NULL, 2, 0, ?, '1', '{}', ?, ?
        )
        """,
        (
            run_id,
            event_id,
            int(model_cursor.lastrowid),
            algorithm,
            issued_at.isoformat(),
            (issued_at - timedelta(minutes=5)).isoformat(),
            day_start.isoformat(),
            (day_start + timedelta(days=1)).isoformat(),
            point_forecast_c,
            issued_at.isoformat(),
            f"submission-{event_id}-{algorithm}-{issued_at.isoformat()}",
            issued_at.isoformat(),
        ),
    )
    assert cursor.lastrowid is not None
    prediction_id = int(cursor.lastrowid)
    connection.executemany(
        "INSERT INTO forecast_scenarios_v2("
        "prediction_id, member_id, raw_max_c, adjusted_max_c, weight"
        ") VALUES (?, ?, ?, ?, '1')",
        [
            (prediction_id, "member01", point_forecast_c, point_forecast_c),
            (prediction_id, "member02", point_forecast_c, point_forecast_c),
        ],
    )
    connection.executemany(
        """
        INSERT INTO forecast_probabilities_v2(
            prediction_id, market_id, market_snapshot_id, condition_id, token_id,
            outcome, book_hash, outcome_label, lower_bound, upper_bound,
            lower_inclusive, upper_inclusive, probability, ordinal
        ) VALUES (?, ?, ?, ?, ?, 'YES', ?, ?, NULL, NULL, 1, 0, ?, ?)
        """,
        [
            (
                prediction_id,
                market_id,
                snapshot_id,
                f"condition-{event_id}",
                f"token-{market_id}",
                f"book-{run_id}-{market_id}",
                market_id,
                probability,
                ordinal,
            )
            for ordinal, (market_id, snapshot_id, probability) in enumerate(
                zip(market_ids, snapshot_ids, probabilities, strict=True)
            )
        ],
    )
    return prediction_id


def _checkpoint(
    connection: sqlite3.Connection,
    *,
    run_id: int,
    event_id: str,
    considered_at: datetime,
    attempts: dict[str, int | None],
    lead_time_seconds: int = 43200,
) -> int:
    day_start = datetime(2026, 9, 11, tzinfo=UTC)
    cursor = connection.execute(
        """
        INSERT INTO forecast_evaluation_events_v2(
            cohort_version, scan_run_id, event_id, considered_at_utc, event_title,
            event_slug, market_count, rules_hash, rule_parser, station_id,
            observation_date, station_timezone, rule_day_start_utc, rule_day_end_utc,
            phase, lead_time_seconds, intraday_elapsed_seconds, display_unit,
            precision_decimal_places, rounding_rule, eligible, stage,
            block_reasons_json, warning_reasons_json, created_at_utc
        ) VALUES (
            'weather-evaluation-v1', ?, ?, ?, ?, ?, 2, 'rules-v1', 'test', 'TEST',
            '2026-09-11', 'UTC', ?, ?, 'LEAD_TIME', ?, NULL, 'C', 0,
            'half-up', 1, 'FORECAST_READY', '[]', '[]', ?
        )
        """,
        (
            run_id,
            event_id,
            considered_at.isoformat(),
            event_id,
            event_id,
            day_start.isoformat(),
            (day_start + timedelta(days=1)).isoformat(),
            lead_time_seconds,
            considered_at.isoformat(),
        ),
    )
    assert cursor.lastrowid is not None
    evaluation_id = int(cursor.lastrowid)
    for algorithm, prediction_id in attempts.items():
        spec = _algorithm_spec(algorithm)
        connection.execute(
            """
            INSERT INTO forecast_evaluation_algorithms_v2(
                evaluation_event_id, source, model, algorithm_version, expected,
                status, prediction_id, reason_codes_json
            ) VALUES (?, ?, ?, ?, 1, ?, ?, ?)
            """,
            (
                evaluation_id,
                spec["source"],
                spec["model"],
                algorithm,
                "PREDICTED" if prediction_id is not None else "SOURCE_UNAVAILABLE",
                prediction_id,
                "[]" if prediction_id is not None else '["SOURCE_UNAVAILABLE"]',
            ),
        )
    return evaluation_id


def _outcome(
    connection: sqlite3.Connection,
    *,
    event_id: str,
    actual_max_c: str,
    winning_market_id: str,
    recorded_at: datetime,
) -> None:
    connection.execute(
        """
        INSERT INTO forecast_outcome_versions_v2(
            event_id, station_id, observation_date, actual_max_c, displayed_max,
            winning_market_id, winning_condition_id, winning_label,
            resolution_source, source_revision, source_published_at_utc,
            resolved_at_utc, recorded_at_utc, evidence_json, outcome_hash
        ) VALUES (
            ?, 'TEST', '2026-09-11', ?, ?, ?, ?, ?, 'test', ?, ?, ?, ?, '{}', ?
        )
        """,
        (
            event_id,
            actual_max_c,
            actual_max_c,
            winning_market_id,
            f"condition-{event_id}",
            winning_market_id,
            f"revision-{event_id}",
            (recorded_at - timedelta(minutes=1)).isoformat(),
            recorded_at.isoformat(),
            recorded_at.isoformat(),
            f"outcome-{event_id}",
        ),
    )


def _model(report: dict[str, object], algorithm: str) -> dict[str, object]:
    models = cast(list[dict[str, object]], report["models"])
    return next(item for item in models if item["algorithm_version"] == algorithm)


def _phase(model: dict[str, object], phase: str) -> dict[str, object]:
    summaries = cast(list[dict[str, object]], model["phase_summaries"])
    return next(item for item in summaries if item["phase"] == phase)


def _paired_phase(report: dict[str, object], algorithm: str, phase: str) -> dict[str, object]:
    paired = cast(dict[str, list[dict[str, object]]], report["paired_vs_v1"])
    return next(item for item in paired[algorithm] if item["phase"] == phase)


def test_comparison_report_is_read_only_and_explicitly_not_promoted(tmp_path: Path) -> None:
    path = tmp_path / "comparison.sqlite3"
    # Simulate a valid bot database created before the forecast-v2 tables existed.
    Storage(path)
    before = hashlib.sha256(path.read_bytes()).hexdigest()

    report = compare_forecasts(path)

    assert hashlib.sha256(path.read_bytes()).hexdigest() == before
    assert report["version"] == COMPARISON_VERSION
    assert report["promotion"]["v2_promoted"] is False  # type: ignore[index]
    assert report["promotion"]["status"] == "not_evaluable"  # type: ignore[index]
    assert len(report["models"]) == 3  # type: ignore[arg-type]
    assert any(
        str(item).startswith("MISSING_TABLE:forecast_predictions_v2")
        for item in cast(list[object], report["warnings"])
    )


def test_paired_metrics_use_only_the_same_latest_registry_checkpoint(tmp_path: Path) -> None:
    path = tmp_path / "paired.sqlite3"
    Storage(path)
    ForecastStore(path)
    base = datetime(2026, 9, 9, 8, tzinfo=UTC)
    with ForecastStore(path).connect() as connection:
        # These two predictions exist for the same event but were never linked
        # from the same evaluation row. They must not become an artificial pair.
        cross_run_one = _scan(connection, base)
        cross_v1 = _prediction(
            connection,
            run_id=cross_run_one,
            event_id="cross-event",
            algorithm=V1_ALGORITHM,
            issued_at=base,
            point_forecast_c="20",
            probabilities=("0.8", "0.2"),
        )
        _checkpoint(
            connection,
            run_id=cross_run_one,
            event_id="cross-event",
            considered_at=base + timedelta(minutes=1),
            attempts={V1_ALGORITHM: cross_v1, RAW_ECMWF_ALGORITHM: None},
        )
        cross_run_two = _scan(connection, base + timedelta(hours=1))
        cross_raw = _prediction(
            connection,
            run_id=cross_run_two,
            event_id="cross-event",
            algorithm=RAW_ECMWF_ALGORITHM,
            issued_at=base + timedelta(hours=1),
            point_forecast_c="20",
            probabilities=("0.8", "0.2"),
        )
        _checkpoint(
            connection,
            run_id=cross_run_two,
            event_id="cross-event",
            considered_at=base + timedelta(hours=1, minutes=1),
            attempts={V1_ALGORITHM: None, RAW_ECMWF_ALGORITHM: cross_raw},
        )
        _outcome(
            connection,
            event_id="cross-event",
            actual_max_c="20",
            winning_market_id="cross-event-market-a",
            recorded_at=base + timedelta(days=2),
        )

        # A checkpoint may intentionally link a throttled/reused prediction
        # from the preceding scan. Pairing is by the selected registry row,
        # while the non-zero issuance spread remains visible in the report.
        old_run = _scan(connection, base + timedelta(hours=2))
        reused_v1 = _prediction(
            connection,
            run_id=old_run,
            event_id="paired-event",
            algorithm=V1_ALGORITHM,
            issued_at=base + timedelta(hours=2),
            point_forecast_c="22",
            probabilities=("0.5", "0.5"),
        )
        paired_run = _scan(connection, base + timedelta(hours=2, minutes=10))
        paired_raw = _prediction(
            connection,
            run_id=paired_run,
            event_id="paired-event",
            algorithm=RAW_ECMWF_ALGORITHM,
            issued_at=base + timedelta(hours=2, minutes=10),
            point_forecast_c="20.5",
            probabilities=("0.3", "0.7"),
        )
        _checkpoint(
            connection,
            run_id=paired_run,
            event_id="paired-event",
            considered_at=base + timedelta(hours=2, minutes=11),
            attempts={V1_ALGORITHM: reused_v1, RAW_ECMWF_ALGORITHM: paired_raw},
        )
        _outcome(
            connection,
            event_id="paired-event",
            actual_max_c="20",
            winning_market_id="paired-event-market-a",
            recorded_at=base + timedelta(days=2),
        )

    report = compare_forecasts(path, minimum_events=30, bootstrap_samples=0)
    paired = _paired_phase(report, RAW_ECMWF_ALGORITHM, "LEAD_TIME")

    # Only paired-event is linked from one selected checkpoint. cross-event is
    # deliberately excluded instead of cross-joining independent snapshots.
    assert paired["common_checkpoint_count"] == 1
    assert paired["common_event_count"] == 1
    deltas = cast(dict[str, dict[str, object]], paired["delta_candidate_minus_v1"])
    assert Decimal(str(deltas["mae_c"]["value"])) == Decimal("-1.5")
    assert Decimal(str(deltas["brier"]["value"])) == Decimal("0.48")
    assert Decimal(str(deltas["accuracy"]["value"])) == Decimal("-1")
    alignment = cast(dict[str, Any], paired["checkpoint_alignment"])
    spread = cast(dict[str, object], alignment["issuance_spread_seconds"])
    assert Decimal(str(spread["mean"])) == Decimal("600")
    assert Decimal(str(spread["max"])) == Decimal("600")

    # Equal 0.5/0.5 probabilities use the same deterministic low-market-id
    # tie break as the core forecast metrics implementation, so v1 was correct.
    v1_lead = _phase(_model(report, V1_ALGORITHM), "LEAD_TIME")
    metrics = cast(dict[str, object], v1_lead["metrics"])
    assert Decimal(str(metrics["accuracy"])) == Decimal("1")
    assert Decimal(str(metrics["mae"])) == Decimal("2")
    assert Decimal(str(metrics["brier"])) == Decimal("0.5")


def test_as_of_cutoff_excludes_future_outcomes_from_metrics_and_pairs(tmp_path: Path) -> None:
    path = tmp_path / "as-of.sqlite3"
    Storage(path)
    ForecastStore(path)
    issued = datetime(2026, 9, 9, 12, tzinfo=UTC)
    recorded = datetime(2026, 9, 10, 12, tzinfo=UTC)
    with ForecastStore(path).connect() as connection:
        run_id = _scan(connection, issued)
        baseline = _prediction(
            connection,
            run_id=run_id,
            event_id="event",
            algorithm=V1_ALGORITHM,
            issued_at=issued,
            point_forecast_c="20",
            probabilities=("0.7", "0.3"),
        )
        candidate = _prediction(
            connection,
            run_id=run_id,
            event_id="event",
            algorithm=RAW_ECMWF_ALGORITHM,
            issued_at=issued + timedelta(minutes=1),
            point_forecast_c="20",
            probabilities=("0.6", "0.4"),
        )
        _checkpoint(
            connection,
            run_id=run_id,
            event_id="event",
            considered_at=issued + timedelta(minutes=2),
            attempts={V1_ALGORITHM: baseline, RAW_ECMWF_ALGORITHM: candidate},
        )
        _outcome(
            connection,
            event_id="event",
            actual_max_c="20",
            winning_market_id="event-market-a",
            recorded_at=recorded,
        )

    before = compare_forecasts(
        path,
        as_of_utc=recorded - timedelta(seconds=1),
        bootstrap_samples=0,
    )
    after = compare_forecasts(path, as_of_utc=recorded, bootstrap_samples=0)

    assert _paired_phase(before, RAW_ECMWF_ALGORITHM, "LEAD_TIME")["common_event_count"] == 0
    assert _phase(_model(before, V1_ALGORITHM), "LEAD_TIME")["evaluated_checkpoints"] == 0
    assert _paired_phase(after, RAW_ECMWF_ALGORITHM, "LEAD_TIME")["common_event_count"] == 1
    assert _phase(_model(after, V1_ALGORITHM), "LEAD_TIME")["evaluated_checkpoints"] == 1


def test_phase_metrics_and_pairs_count_each_event_once_across_lead_bins(
    tmp_path: Path,
) -> None:
    path = tmp_path / "lead-bins.sqlite3"
    Storage(path)
    ForecastStore(path)
    base = datetime(2026, 9, 9, 6, tzinfo=UTC)
    with ForecastStore(path).connect() as connection:
        first_run = _scan(connection, base)
        first_v1 = _prediction(
            connection,
            run_id=first_run,
            event_id="event",
            algorithm=V1_ALGORITHM,
            issued_at=base,
            point_forecast_c="25",
            probabilities=("0.2", "0.8"),
        )
        first_raw = _prediction(
            connection,
            run_id=first_run,
            event_id="event",
            algorithm=RAW_ECMWF_ALGORITHM,
            issued_at=base + timedelta(seconds=1),
            point_forecast_c="20",
            probabilities=("0.8", "0.2"),
        )
        _checkpoint(
            connection,
            run_id=first_run,
            event_id="event",
            considered_at=base + timedelta(minutes=1),
            attempts={V1_ALGORITHM: first_v1, RAW_ECMWF_ALGORITHM: first_raw},
            lead_time_seconds=18 * 3600,
        )

        second_run = _scan(connection, base + timedelta(hours=10))
        second_v1 = _prediction(
            connection,
            run_id=second_run,
            event_id="event",
            algorithm=V1_ALGORITHM,
            issued_at=base + timedelta(hours=10),
            point_forecast_c="20",
            probabilities=("0.8", "0.2"),
        )
        second_raw = _prediction(
            connection,
            run_id=second_run,
            event_id="event",
            algorithm=RAW_ECMWF_ALGORITHM,
            issued_at=base + timedelta(hours=10, seconds=1),
            point_forecast_c="22",
            probabilities=("0.3", "0.7"),
        )
        _checkpoint(
            connection,
            run_id=second_run,
            event_id="event",
            considered_at=base + timedelta(hours=10, minutes=1),
            attempts={V1_ALGORITHM: second_v1, RAW_ECMWF_ALGORITHM: second_raw},
            lead_time_seconds=6 * 3600,
        )
        _outcome(
            connection,
            event_id="event",
            actual_max_c="20",
            winning_market_id="event-market-a",
            recorded_at=base + timedelta(days=2),
        )

    report = compare_forecasts(path, bootstrap_samples=0)
    v1_phase = _phase(_model(report, V1_ALGORITHM), "LEAD_TIME")
    paired = _paired_phase(report, RAW_ECMWF_ALGORITHM, "LEAD_TIME")

    assert v1_phase["evaluated_event_count"] == 1
    assert cast(dict[str, object], v1_phase["metrics"])["n"] == 1
    assert paired["common_checkpoint_count"] == 1
    assert paired["common_event_count"] == 1
    deltas = cast(dict[str, dict[str, object]], paired["delta_candidate_minus_v1"])
    # The later 6-hour checkpoint wins the deterministic per-phase selection.
    assert Decimal(str(deltas["mae_c"]["value"])) == Decimal("2")


def test_as_of_cutoff_excludes_prediction_issued_after_the_cutoff(tmp_path: Path) -> None:
    path = tmp_path / "future-prediction.sqlite3"
    Storage(path)
    ForecastStore(path)
    cutoff = datetime(2026, 9, 10, 12, tzinfo=UTC)
    with ForecastStore(path).connect() as connection:
        run_id = _scan(connection, cutoff - timedelta(hours=1))
        future_baseline = _prediction(
            connection,
            run_id=run_id,
            event_id="event",
            algorithm=V1_ALGORITHM,
            issued_at=cutoff + timedelta(minutes=1),
            point_forecast_c="20",
            probabilities=("0.7", "0.3"),
        )
        available_candidate = _prediction(
            connection,
            run_id=run_id,
            event_id="event",
            algorithm=RAW_ECMWF_ALGORITHM,
            issued_at=cutoff - timedelta(minutes=10),
            point_forecast_c="20",
            probabilities=("0.6", "0.4"),
        )
        # Protect the report from an inconsistent/corrupt registry link: the
        # checkpoint exists before cutoff, but one linked prediction does not.
        _checkpoint(
            connection,
            run_id=run_id,
            event_id="event",
            considered_at=cutoff - timedelta(minutes=5),
            attempts={
                V1_ALGORITHM: future_baseline,
                RAW_ECMWF_ALGORITHM: available_candidate,
            },
        )
        _outcome(
            connection,
            event_id="event",
            actual_max_c="20",
            winning_market_id="event-market-a",
            recorded_at=cutoff - timedelta(minutes=1),
        )

    before = compare_forecasts(path, as_of_utc=cutoff, bootstrap_samples=0)
    after = compare_forecasts(
        path,
        as_of_utc=cutoff + timedelta(minutes=1),
        bootstrap_samples=0,
    )

    before_v1 = _phase(_model(before, V1_ALGORITHM), "LEAD_TIME")
    assert before_v1["prediction_checkpoints"] == 0
    assert before_v1["evaluated_checkpoints"] == 0
    assert _paired_phase(before, RAW_ECMWF_ALGORITHM, "LEAD_TIME")["common_event_count"] == 0
    after_v1 = _phase(_model(after, V1_ALGORITHM), "LEAD_TIME")
    assert after_v1["prediction_checkpoints"] == 1
    assert after_v1["evaluated_checkpoints"] == 1
    assert _paired_phase(after, RAW_ECMWF_ALGORITHM, "LEAD_TIME")["common_event_count"] == 1


def test_sigma_diagnostics_exclude_v0_decisions(tmp_path: Path) -> None:
    path = tmp_path / "sigma-version.sqlite3"
    Storage(path)
    ForecastStore(path)
    created = datetime(2026, 9, 10, 10, tzinfo=UTC)
    with ForecastStore(path).connect() as connection:
        run_id = _scan(connection, created)
        connection.executemany(
            """
            INSERT INTO decisions(
                run_id, event_id, market_id, action, created_at, payload_json,
                paper_order_id
            ) VALUES (?, ?, ?, 'PAPER_BUY', ?, ?, NULL)
            """,
            [
                (
                    run_id,
                    "legacy-v0-event",
                    "legacy-market",
                    created.isoformat(),
                    json.dumps({"strategy_version": "v0"}),
                ),
                (
                    run_id,
                    "current-v1-event",
                    "current-market",
                    created.isoformat(),
                    json.dumps({"strategy_version": "v1"}),
                ),
            ],
        )

    report = compare_forecasts(path, bootstrap_samples=0)
    sigma = cast(dict[str, object], report["sigma_dependent_signals"])
    summary = cast(dict[str, object], sigma["summary"])

    # The v1 row is counted then skipped because this fixture intentionally
    # has no entry forecast. The v0 row is outside the sigma-v1 cohort entirely.
    assert summary["eligible_paper_buy_count"] == 1
    assert summary["analyzed_count"] == 0
    assert summary["skipped_missing_entry_artifacts"] == 1
    assert sigma["trades"] == []


def test_sigma_diagnostics_replay_fixed_sigma_and_observed_floor(tmp_path: Path) -> None:
    path = tmp_path / "sigma-replay.sqlite3"
    Storage(path)
    ForecastStore(path)
    issued = datetime(2026, 9, 10, 9, tzinfo=UTC)
    with ForecastStore(path).connect() as connection:
        run_id = _scan(connection, issued)
        prediction_id = _prediction(
            connection,
            run_id=run_id,
            event_id="event",
            algorithm=V1_ALGORITHM,
            issued_at=issued,
            point_forecast_c="20",
            probabilities=("0", "1"),
        )
        market_a = "event-market-a"
        market_b = "event-market-b"
        brackets = {
            market_a: Bracket(
                market_id=market_a,
                label="below 21",
                lower=None,
                upper=21,
            ),
            market_b: Bracket(
                market_id=market_b,
                label="21 or above",
                lower=21,
                upper=None,
            ),
        }
        replayed = v1_sigma_bracket_probabilities(
            [Decimal("20"), Decimal("20")],
            brackets,
            sigma_c=Decimal("1.5"),
            observed_floor_c=Decimal("21"),
        )
        connection.execute(
            "UPDATE forecast_predictions_v2 SET observed_floor_c='21' WHERE id=?",
            (prediction_id,),
        )
        connection.execute(
            "UPDATE forecast_scenarios_v2 SET adjusted_max_c='21' WHERE prediction_id=?",
            (prediction_id,),
        )
        connection.execute(
            "UPDATE forecast_probabilities_v2 SET upper_bound='21', upper_inclusive=0, "
            "probability=? WHERE prediction_id=? AND market_id=?",
            (str(replayed[market_a]), prediction_id, market_a),
        )
        connection.execute(
            "UPDATE forecast_probabilities_v2 SET lower_bound='21', lower_inclusive=1, "
            "probability=? WHERE prediction_id=? AND market_id=?",
            (str(replayed[market_b]), prediction_id, market_b),
        )
        snapshot_rows = connection.execute(
            "SELECT id, payload_json FROM market_snapshots WHERE run_id=? AND event_id='event'",
            (run_id,),
        ).fetchall()
        for snapshot_row in snapshot_rows:
            payload = json.loads(str(snapshot_row["payload_json"]))
            payload["asks"] = [{"price": "0.1", "size": "10"}]
            connection.execute(
                "UPDATE market_snapshots SET payload_json=? WHERE id=?",
                (json.dumps(payload), snapshot_row["id"]),
            )
        connection.execute(
            "INSERT INTO decisions(run_id,event_id,market_id,action,created_at,payload_json) "
            "VALUES (?, 'event', ?, 'PAPER_BUY', ?, ?)",
            (
                run_id,
                market_b,
                (issued + timedelta(minutes=1)).isoformat(),
                json.dumps(
                    {
                        "strategy_version": "v1",
                        "forecast_algorithm_version": V1_ALGORITHM,
                    }
                ),
            ),
        )

    report = compare_forecasts(path, bootstrap_samples=0)
    sigma = cast(dict[str, object], report["sigma_dependent_signals"])
    summary = cast(dict[str, object], sigma["summary"])
    trades = cast(list[dict[str, object]], sigma["trades"])

    assert summary["analyzed_count"] == 1
    assert summary["fixed_sigma_1_5_count"] == 1
    assert summary["observed_floor_applied_count"] == 1
    assert summary["stored_probability_replay_mismatch_count"] == 0
    assert summary["archived_member_conditioning_mismatch_count"] == 0
    assert trades[0]["observed_floor_c"] == "21"
    assert trades[0]["raw_unconditioned_probability"] == "0"
    assert trades[0]["raw_empirical_probability"] == "1"


def test_comparison_splits_trade_pnl_by_entry_strategy_without_double_charging_api(
    tmp_path: Path,
) -> None:
    path = tmp_path / "strategy-pnl.sqlite3"
    Storage(path)
    ForecastStore(path)
    settled_at = datetime(2026, 9, 10, 10, tzinfo=UTC).isoformat()
    with ForecastStore(path).connect() as connection:
        connection.executemany(
            """
            INSERT INTO paper_orders(
                idempotency_key, event_id, market_id, asset_id, status,
                strategy_version, execution_model, outcome, shares, entry_price,
                notional_usd, fee_usd, api_cost_usd, execution_buffer_usd,
                max_loss_usd, fee_rate, fee_exponent, identity_verified,
                opened_at, settled_at, closed_at, won, realized_pnl_usd
            ) VALUES (
                ?, ?, ?, ?, 'PAPER_SETTLED', ?, 'test', 'YES', '5', '0.1',
                '0.5', '0', ?, '0', '0.5', '0', '0', 1, ?, ?, ?, ?, ?
            )
            """,
            [
                (
                    "v0-order",
                    "v0-event",
                    "v0-market",
                    "v0-asset",
                    "v0",
                    "0",
                    settled_at,
                    settled_at,
                    settled_at,
                    1,
                    "4.5",
                ),
                (
                    "v1-order",
                    "v1-event",
                    "v1-market",
                    "v1-asset",
                    "v1",
                    "0.1",
                    settled_at,
                    settled_at,
                    settled_at,
                    0,
                    "-0.6",
                ),
            ],
        )

    report = compare_forecasts(path, bootstrap_samples=0)
    split = cast(dict[str, dict[str, object]], report["pnl_by_entry_strategy"])

    assert split["v0"]["wins"] == 1
    assert split["v0"]["losses"] == 0
    assert Decimal(str(split["v0"]["gross_trade_pnl_usd"])) == Decimal("4.5")
    assert split["v1"]["wins"] == 0
    assert split["v1"]["losses"] == 1
    assert Decimal(str(split["v1"]["realized_pnl_usd"])) == Decimal("-0.6")
    assert Decimal(str(split["v1"]["allocated_order_api_cost_usd"])) == Decimal("0.1")
    assert Decimal(str(split["v1"]["gross_trade_pnl_usd"])) == Decimal("-0.5")


def test_comparison_cli_and_report_expose_stable_read_only_fields(tmp_path: Path) -> None:
    args = build_parser().parse_args(["comparison", "--json"])
    assert args.command == "comparison"
    assert args.as_json is True

    path = tmp_path / "shape.sqlite3"
    Storage(path)
    report = compare_forecasts(path)

    assert {
        "version",
        "as_of_utc",
        "checkpoint_policy",
        "models",
        "checkpoints",
        "paired_vs_v1",
        "sigma_dependent_signals",
        "pnl_by_entry_strategy",
        "promotion",
        "warnings",
    }.issubset(report)
