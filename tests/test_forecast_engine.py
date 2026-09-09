from datetime import UTC, date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import pytest

from polybot.forecast_models import (
    OPEN_METEO_ALGORITHM_VERSION,
    ForecastAlgorithmAttemptStatus,
    ForecastAlgorithmEligibility,
    ForecastEligibilityStage,
    ForecastEvaluationCase,
    ForecastEventEligibility,
    ForecastMetricsQuery,
    ForecastModelRun,
    ForecastObservationEvidence,
    ForecastPhase,
    ForecastProbability,
    ForecastRuleDay,
    ForecastScenario,
    ForecastSubmission,
    RealizedForecastOutcome,
    compute_metrics_slice,
)
from polybot.forecast_store import ForecastStore
from polybot.models import MarketSnapshot
from polybot.observations import ObservationHistory, ObservationVersion
from polybot.storage import Storage


def _market_snapshot(*, event_id: str, market_id: str) -> MarketSnapshot:
    return MarketSnapshot(
        event_id=event_id,
        event_slug=event_id,
        event_title=f"Test {event_id}",
        market_id=market_id,
        market_slug=market_id,
        market_question=f"Will maximum be {market_id}?",
        outcome_label=market_id,
        asset_id=f"token-{market_id}",
        token_id=f"token-{market_id}",
        condition_id=f"condition-{event_id}",
        end_date=None,
        accepting_orders=True,
        book_timestamp=datetime(2026, 9, 8, 12, tzinfo=UTC),
        book_hash=f"book-{market_id}",
        bids=[],
        asks=[],
        min_order_size=Decimal("5"),
        tick_size=Decimal("0.01"),
        fee_rate=Decimal(0),
        fee_exponent=Decimal(0),
        fee_taker_only=False,
    )


def _prepare_database(tmp_path: Path) -> tuple[Path, int]:
    path = tmp_path / "forecast.sqlite3"
    storage = Storage(path)
    run_id = storage.start_scan(query="test", mode="paper")
    for market_id in ("market-a", "market-b"):
        storage.record_market_snapshot(
            run_id,
            _market_snapshot(event_id="event-1", market_id=market_id),
        )
    observed_at = datetime(2026, 9, 8, 10, tzinfo=UTC)
    first_seen = datetime(2026, 9, 8, 11, 59, tzinfo=UTC)
    version = ObservationVersion(
        station_id="TEST",
        station_timezone="UTC",
        observed_at_utc=observed_at,
        first_seen_at_utc=first_seen,
        source="test-observations",
        source_url="https://www.weather.gov/wrh/timeseries?site=test",
        temperature_c=Decimal("19"),
        displayed_temperature_c=Decimal("19"),
        raw_payload={"temperature": 19},
        revision_hash="obs-r1",
        corrected=False,
    )
    storage.record_observation_history(
        run_id,
        "event-1",
        ObservationHistory(
            station_id="TEST",
            station_name="Test station",
            station_timezone="UTC",
            source_url=version.source_url,
            observation_date=date(2026, 9, 9),
            fetched_at_utc=first_seen,
            day_started=False,
            day_finished=False,
            expected_cadence_minutes=60,
            observations=[version],
            observed_max_c=version.temperature_c,
            displayed_max_c=version.displayed_temperature_c,
            latest_observed_at_utc=version.observed_at_utc,
            stale=False,
        ),
    )
    return path, run_id


def _submission(
    *,
    run_id: int,
    issued_at: datetime,
    model_fetched_at: datetime | None = None,
    observation_cutoff: datetime | None = None,
    probabilities: tuple[str, str] = ("0.7", "0.3"),
    source_run_id: str = "source-run-1",
    source_payload_hash: str = "payload-hash-1",
    init_time: datetime | None = None,
    published_at: datetime | None = None,
) -> ForecastSubmission:
    model_fetched_at = model_fetched_at or issued_at - timedelta(minutes=10)
    observation_cutoff = observation_cutoff or issued_at - timedelta(minutes=1)
    init_time = init_time or model_fetched_at - timedelta(hours=1)
    published_at = published_at or model_fetched_at - timedelta(minutes=30)
    day_start = datetime(2026, 9, 9, tzinfo=UTC)
    return ForecastSubmission(
        scan_run_id=run_id,
        event_id="event-1",
        algorithm_version=OPEN_METEO_ALGORITHM_VERSION,
        model_run=ForecastModelRun(
            source="test-source",
            model="test-ensemble",
            model_version="2026.09",
            source_run_id=source_run_id,
            init_time_utc=init_time,
            published_at_utc=published_at,
            fetched_at_utc=model_fetched_at,
            source_uri="https://example.test/archive",
            source_payload_hash=source_payload_hash,
            metadata={"surface": "test"},
        ),
        issued_at_utc=issued_at,
        observation_cutoff_at_utc=observation_cutoff,
        rule_day=ForecastRuleDay(
            station_id="TEST",
            observation_date=date(2026, 9, 9),
            station_timezone="UTC",
            day_start_utc=day_start,
            day_end_utc=day_start + timedelta(days=1),
            display_unit="C",
            precision_decimal_places=0,
            rounding_rule="displayed_temperature_c=floor(raw_temperature_c+0.5)",
            rules_hash="rules-v1",
        ),
        scenarios=[
            ForecastScenario(
                member_id="member-000",
                raw_max_c=Decimal("20"),
                adjusted_max_c=Decimal("20"),
                weight=Decimal(1),
            ),
            ForecastScenario(
                member_id="member-001",
                raw_max_c=Decimal("21"),
                adjusted_max_c=Decimal("21"),
                weight=Decimal(1),
            ),
        ],
        probabilities=[
            ForecastProbability(
                market_id="market-a",
                outcome_label="20°C or below",
                lower_bound=None,
                upper_bound=Decimal("21"),
                probability=Decimal(probabilities[0]),
            ),
            ForecastProbability(
                market_id="market-b",
                outcome_label="21°C or above",
                lower_bound=Decimal("21"),
                upper_bound=None,
                probability=Decimal(probabilities[1]),
            ),
        ],
        observations=[
            ForecastObservationEvidence(
                station_id="TEST",
                observed_at_utc=datetime(2026, 9, 8, 10, tzinfo=UTC),
                revision_hash="obs-r1",
                first_seen_at_utc=datetime(2026, 9, 8, 11, 59, tzinfo=UTC),
                source="test-observations",
                temperature_c=Decimal("19"),
                displayed_temperature_c=Decimal("19"),
                corrected=False,
            )
        ],
    )


def _outcome(
    *,
    source_revision: str,
    actual_max_c: Decimal = Decimal("20"),
    winning_market_id: str = "market-a",
    recorded_at: datetime | None = None,
) -> RealizedForecastOutcome:
    recorded_at = recorded_at or datetime(2026, 9, 10, tzinfo=UTC)
    return RealizedForecastOutcome(
        event_id="event-1",
        station_id="TEST",
        observation_date=date(2026, 9, 9),
        actual_max_c=actual_max_c,
        displayed_max=actual_max_c,
        winning_market_id=winning_market_id,
        winning_condition_id="condition-event-1",
        resolution_source="test",
        source_revision=source_revision,
        source_published_at_utc=recorded_at - timedelta(minutes=5),
        resolved_at_utc=recorded_at,
        recorded_at_utc=recorded_at,
    )


def _register_event(
    store: ForecastStore,
    *,
    run_id: int,
    event_id: str = "event-1",
    station_id: str = "TEST",
    considered_at: datetime = datetime(2026, 9, 8, 12, tzinfo=UTC),
    predicted: bool = True,
    prediction_id: int | None = None,
) -> None:
    day_start = datetime(2026, 9, 9, tzinfo=UTC)
    store.register_evaluation_event(
        ForecastEventEligibility(
            scan_run_id=run_id,
            event_id=event_id,
            considered_at_utc=considered_at,
            event_title=f"Test {event_id}",
            event_slug=event_id,
            market_count=2,
            rules_hash="rules-v1",
            rule_parser="test",
            station_id=station_id,
            observation_date=date(2026, 9, 9),
            station_timezone="UTC",
            rule_day_start_utc=day_start,
            rule_day_end_utc=day_start + timedelta(days=1),
            display_unit="C",
            precision_decimal_places=0,
            rounding_rule="displayed_temperature_c=floor(raw_temperature_c+0.5)",
            eligible=True,
            stage=(
                ForecastEligibilityStage.FORECAST_READY
                if predicted
                else ForecastEligibilityStage.FORECAST_FAILED
            ),
            algorithms=[
                ForecastAlgorithmEligibility(
                    source="test-source",
                    model="test-ensemble",
                    algorithm_version=OPEN_METEO_ALGORITHM_VERSION,
                    expected=True,
                    status=(
                        ForecastAlgorithmAttemptStatus.PREDICTED
                        if predicted
                        else ForecastAlgorithmAttemptStatus.SOURCE_UNAVAILABLE
                    ),
                    prediction_id=prediction_id,
                )
            ],
        )
    )


def test_v2_persists_full_provenance_scenarios_and_snapshot_links(tmp_path: Path) -> None:
    path, run_id = _prepare_database(tmp_path)
    store = ForecastStore(path)
    submission = _submission(
        run_id=run_id,
        issued_at=datetime(2026, 9, 8, 12, tzinfo=UTC),
    )

    prediction_id = store.record(submission)
    assert store.record(submission) == prediction_id

    with store.connect() as connection:
        prediction = connection.execute(
            "SELECT * FROM forecast_predictions_v2 WHERE id = ?", (prediction_id,)
        ).fetchone()
        model_run = connection.execute(
            "SELECT * FROM forecast_model_runs_v2 WHERE id = ?",
            (prediction["model_run_id"],),
        ).fetchone()
        scenarios = connection.execute(
            "SELECT * FROM forecast_scenarios_v2 WHERE prediction_id = ? ORDER BY member_id",
            (prediction_id,),
        ).fetchall()
        probabilities = connection.execute(
            "SELECT * FROM forecast_probabilities_v2 WHERE prediction_id = ? ORDER BY ordinal",
            (prediction_id,),
        ).fetchall()
        evidence = connection.execute(
            "SELECT * FROM forecast_observation_evidence_v2 WHERE prediction_id = ?",
            (prediction_id,),
        ).fetchone()

    assert store.counts() == {"model_runs": 1, "predictions": 1, "outcome_versions": 0}
    assert prediction["algorithm_version"] == OPEN_METEO_ALGORITHM_VERSION
    assert prediction["phase"] == ForecastPhase.LEAD_TIME.value
    assert prediction["issued_at_utc"] == "2026-09-08T12:00:00+00:00"
    assert prediction["model_fetched_at_utc"] == "2026-09-08T11:50:00+00:00"
    assert prediction["observation_cutoff_at_utc"] == "2026-09-08T11:59:00+00:00"
    assert prediction["rounding_rule"].endswith("floor(raw_temperature_c+0.5)")
    assert model_run["source_run_id"] == "source-run-1"
    assert model_run["source_payload_hash"] == "payload-hash-1"
    assert len(scenarios) == 2
    assert [row["market_snapshot_id"] for row in probabilities] == [1, 2]
    assert [row["book_hash"] for row in probabilities] == [
        "book-market-a",
        "book-market-b",
    ]
    assert evidence["station_observation_version_id"] is not None
    assert evidence["canonical_first_seen_at_utc"] == evidence["first_seen_at_utc"]


def test_repeat_fetch_keeps_canonical_observation_first_seen_time(tmp_path: Path) -> None:
    path, run_id = _prepare_database(tmp_path)
    store = ForecastStore(path)
    submission = _submission(
        run_id=run_id,
        issued_at=datetime(2026, 9, 8, 13, tzinfo=UTC),
        observation_cutoff=datetime(2026, 9, 8, 12, 59, tzinfo=UTC),
    )
    repeated = submission.observations[0].model_copy(
        update={"first_seen_at_utc": datetime(2026, 9, 8, 12, 30, tzinfo=UTC)}
    )
    prediction_id = store.record(submission.model_copy(update={"observations": [repeated]}))

    with store.connect() as connection:
        evidence = connection.execute(
            "SELECT first_seen_at_utc, canonical_first_seen_at_utc "
            "FROM forecast_observation_evidence_v2 WHERE prediction_id=?",
            (prediction_id,),
        ).fetchone()
    assert evidence["first_seen_at_utc"] == "2026-09-08T11:59:00+00:00"
    assert evidence["canonical_first_seen_at_utc"] == "2026-09-08T11:59:00+00:00"


def test_archived_model_run_is_reused_with_new_issuance_and_observations(
    tmp_path: Path,
) -> None:
    path, run_id = _prepare_database(tmp_path)
    store = ForecastStore(path)
    first = _submission(
        run_id=run_id,
        issued_at=datetime(2026, 9, 8, 12, tzinfo=UTC),
    )
    second = _submission(
        run_id=run_id,
        issued_at=datetime(2026, 9, 8, 13, tzinfo=UTC),
        model_fetched_at=datetime(2026, 9, 8, 12, 30, tzinfo=UTC),
        observation_cutoff=datetime(2026, 9, 8, 12, 59, tzinfo=UTC),
        probabilities=("0.2", "0.8"),
        init_time=first.model_run.init_time_utc,
        published_at=first.model_run.published_at_utc,
    )
    store.record(first)
    store.record(second)

    assert store.counts()["model_runs"] == 1
    assert store.counts()["predictions"] == 2
    with store.connect() as connection:
        rows = connection.execute(
            "SELECT issued_at_utc, model_fetched_at_utc, observation_cutoff_at_utc, "
            "model_run_id FROM forecast_predictions_v2 ORDER BY id"
        ).fetchall()
    assert rows[0]["model_run_id"] == rows[1]["model_run_id"]
    assert rows[0]["issued_at_utc"] != rows[1]["issued_at_utc"]
    assert rows[0]["observation_cutoff_at_utc"] != rows[1]["observation_cutoff_at_utc"]


def test_prediction_archive_throttles_unchanged_polling_but_keeps_material_change(
    tmp_path: Path,
) -> None:
    path, run_id = _prepare_database(tmp_path)
    store = ForecastStore(path)
    first = _submission(
        run_id=run_id,
        issued_at=datetime(2026, 9, 8, 12, tzinfo=UTC),
    )
    unchanged = _submission(
        run_id=run_id,
        issued_at=datetime(2026, 9, 8, 12, 5, tzinfo=UTC),
        model_fetched_at=first.model_run.fetched_at_utc,
        observation_cutoff=datetime(2026, 9, 8, 11, 59, tzinfo=UTC),
        init_time=first.model_run.init_time_utc,
        published_at=first.model_run.published_at_utc,
    )
    changed = unchanged.model_copy(
        update={
            "issued_at_utc": datetime(2026, 9, 8, 12, 10, tzinfo=UTC),
            "probabilities": [
                unchanged.probabilities[0].model_copy(update={"probability": Decimal("0.67")}),
                unchanged.probabilities[1].model_copy(update={"probability": Decimal("0.33")}),
            ],
        }
    )

    first_id = store.record(first, min_interval_seconds=3600, probability_delta=Decimal("0.02"))
    assert (
        store.record(
            unchanged,
            min_interval_seconds=3600,
            probability_delta=Decimal("0.02"),
        )
        == first_id
    )
    assert store.counts()["predictions"] == 1
    assert (
        store.record(
            changed,
            min_interval_seconds=3600,
            probability_delta=Decimal("0.02"),
        )
        != first_id
    )
    assert store.counts()["predictions"] == 2


def test_phase_uses_prediction_issuance_not_archived_model_fetch(tmp_path: Path) -> None:
    path, run_id = _prepare_database(tmp_path)
    store = ForecastStore(path)
    prediction_id = store.record(
        _submission(
            run_id=run_id,
            model_fetched_at=datetime(2026, 9, 8, 12, tzinfo=UTC),
            issued_at=datetime(2026, 9, 9, 6, tzinfo=UTC),
            observation_cutoff=datetime(2026, 9, 9, 5, 59, tzinfo=UTC),
        )
    )
    with store.connect() as connection:
        row = connection.execute(
            "SELECT phase, lead_time_seconds, intraday_elapsed_seconds, "
            "intraday_remaining_seconds FROM forecast_predictions_v2 WHERE id = ?",
            (prediction_id,),
        ).fetchone()
    assert row["phase"] == ForecastPhase.INTRADAY.value
    assert row["lead_time_seconds"] == -6 * 3600
    assert row["intraday_elapsed_seconds"] == 6 * 3600
    assert row["intraday_remaining_seconds"] == 18 * 3600


def test_metrics_use_unique_events_and_latest_forecast_per_segment(tmp_path: Path) -> None:
    path, run_id = _prepare_database(tmp_path)
    store = ForecastStore(path)
    first_prediction_id = store.record(
        _submission(
            run_id=run_id,
            issued_at=datetime(2026, 9, 8, 12, tzinfo=UTC),
        )
    )
    _register_event(
        store,
        run_id=run_id,
        prediction_id=first_prediction_id,
    )
    storage = Storage(path)
    second_run_id = storage.start_scan(query="test", mode="paper")
    for market_id in ("market-a", "market-b"):
        storage.record_market_snapshot(
            second_run_id,
            _market_snapshot(event_id="event-1", market_id=market_id),
        )
    second_prediction_id = store.record(
        _submission(
            run_id=second_run_id,
            issued_at=datetime(2026, 9, 8, 13, tzinfo=UTC),
            probabilities=("0.2", "0.8"),
        )
    )
    _register_event(
        store,
        run_id=second_run_id,
        considered_at=datetime(2026, 9, 8, 13, tzinfo=UTC),
        prediction_id=second_prediction_id,
    )
    store.record_outcome(
        _outcome(
            source_revision="rev-1",
            actual_max_c=Decimal("21"),
            winning_market_id="market-b",
        )
    )
    # A resolved event without a forecast remains in the coverage denominator.
    event_two = _outcome(source_revision="rev-event-2").model_copy(
        update={"event_id": "event-2", "station_id": "TEST2"}
    )
    _register_event(
        store,
        run_id=run_id,
        event_id="event-2",
        station_id="TEST2",
        predicted=False,
    )
    for market_id in ("market-a", "market-b"):
        storage.record_market_snapshot(
            run_id,
            _market_snapshot(event_id="event-2", market_id=market_id),
        )
    store.record_outcome(event_two)

    report = store.metrics(ForecastMetricsQuery(calibration_bin_count=2))
    assert report.overall.forecast_count == 2
    assert report.overall.unique_forecast_event_count == 1
    assert report.overall.outcome_event_count == 2
    assert report.overall.event_count == 1
    assert report.overall.coverage == Decimal("0.5")
    assert report.overall.exact_bracket_accuracy == Decimal(1)
    assert report.overall.multiclass_brier_score == Decimal("0.08")
    assert report.overall.max_temperature_mae_c == Decimal("0.5")
    assert report.overall.expected_calibration_error == Decimal("0.2")
    assert sum(item.count for item in report.overall.top_label_calibration) == 1
    assert report.by_phase["LEAD_TIME"].unique_forecast_event_count == 1
    assert report.by_phase["INTRADAY"].forecast_count == 0
    assert report.by_lead_time["LEAD_006_012H"].forecast_count == 1
    assert report.by_lead_time["LEAD_012_024H"].forecast_count == 1

    station_report = store.metrics(ForecastMetricsQuery(station_id="TEST"))
    assert station_report.overall.outcome_event_count == 1
    assert station_report.overall.coverage == Decimal(1)


def test_multiclass_brier_uses_full_distribution_once_per_event() -> None:
    case = ForecastEvaluationCase(
        prediction_id=1,
        event_id="event",
        phase=ForecastPhase.LEAD_TIME,
        issued_at_utc=datetime.now(UTC),
        point_forecast_c=Decimal("20"),
        probabilities={"a": Decimal("0.1"), "b": Decimal("0.7"), "c": Decimal("0.2")},
        distribution_mass=Decimal(1),
        actual_max_c=Decimal("22"),
        winning_market_id="c",
    )
    result = compute_metrics_slice(
        segment="OVERALL",
        forecast_count=50,
        unique_forecast_event_count=1,
        outcome_event_count=1,
        cases=[case],
        calibration_bin_count=5,
    )
    assert result.event_count == 1
    assert result.multiclass_brier_score == Decimal("1.14")
    assert result.exact_bracket_accuracy == Decimal(0)


def test_outcome_revisions_are_append_only_and_as_of_is_reproducible(tmp_path: Path) -> None:
    path, run_id = _prepare_database(tmp_path)
    store = ForecastStore(path)
    prediction_id = store.record(
        _submission(
            run_id=run_id,
            issued_at=datetime(2026, 9, 8, 12, tzinfo=UTC),
        )
    )
    _register_event(store, run_id=run_id, prediction_id=prediction_id)
    first = _outcome(
        source_revision="rev-1",
        recorded_at=datetime(2026, 9, 10, tzinfo=UTC),
    )
    store.record_outcome(first)
    assert store.record_outcome(first) == 1
    assert (
        store.record_outcome(
            first.model_copy(
                update={
                    "recorded_at_utc": first.recorded_at_utc + timedelta(hours=1),
                    "evidence": {"observation_fetch_time": "2026-09-10T01:00:00+00:00"},
                }
            )
        )
        == 1
    )
    with pytest.raises(ValueError, match="immutable"):
        store.record_outcome(
            _outcome(
                source_revision="rev-1",
                actual_max_c=Decimal("21"),
                winning_market_id="market-b",
            )
        )
    store.record_outcome(
        _outcome(
            source_revision="rev-2",
            actual_max_c=Decimal("21"),
            winning_market_id="market-b",
            recorded_at=datetime(2026, 9, 11, tzinfo=UTC),
        )
    )
    historical = store.metrics(
        ForecastMetricsQuery(as_of_utc=datetime(2026, 9, 10, 12, tzinfo=UTC))
    )
    historical_non_utc = store.metrics(
        ForecastMetricsQuery(
            as_of_utc=datetime(2026, 9, 10, 17, tzinfo=timezone(timedelta(hours=5)))
        )
    )
    current = store.metrics()
    assert historical.overall.exact_bracket_accuracy == Decimal(1)
    assert historical_non_utc.overall == historical.overall
    assert current.overall.exact_bracket_accuracy == Decimal(0)
    assert store.counts()["outcome_versions"] == 2


def test_outcome_identity_is_checked_when_forecasts_exist(tmp_path: Path) -> None:
    path, run_id = _prepare_database(tmp_path)
    store = ForecastStore(path)
    prediction_id = store.record(
        _submission(run_id=run_id, issued_at=datetime(2026, 9, 8, 12, tzinfo=UTC))
    )
    _register_event(store, run_id=run_id, prediction_id=prediction_id)

    with pytest.raises(ValueError, match="rule-day identity mismatch"):
        store.record_outcome(
            _outcome(source_revision="wrong-station").model_copy(update={"station_id": "OTHER"})
        )


def test_outcome_identity_is_checked_for_eligible_unforecasted_event(tmp_path: Path) -> None:
    path, run_id = _prepare_database(tmp_path)
    store = ForecastStore(path)
    _register_event(store, run_id=run_id, predicted=False)

    with pytest.raises(ValueError, match="rule-day identity mismatch"):
        store.record_outcome(
            _outcome(source_revision="wrong-registry-station").model_copy(
                update={"station_id": "OTHER"}
            )
        )


def test_forecasted_ended_events_are_polled_and_refreshed_for_corrections(
    tmp_path: Path,
) -> None:
    path, run_id = _prepare_database(tmp_path)
    store = ForecastStore(path)
    prediction_id = store.record(
        _submission(run_id=run_id, issued_at=datetime(2026, 9, 8, 12, tzinfo=UTC))
    )
    _register_event(store, run_id=run_id, prediction_id=prediction_id)
    as_of = datetime(2026, 9, 10, 1, tzinfo=UTC)

    assert store.pending_outcome_event_ids(as_of_utc=as_of) == ["event-1"]
    store.record_outcome(_outcome(source_revision="first"))
    assert store.pending_outcome_event_ids(as_of_utc=as_of) == []
    assert store.outcome_refresh_event_ids(as_of_utc=as_of) == ["event-1"]


def test_outcome_timestamps_must_be_causal() -> None:
    payload = _outcome(source_revision="bad-time").model_dump()
    payload.update(
        {
            "resolved_at_utc": datetime(2026, 9, 10, 1, tzinfo=UTC),
            "recorded_at_utc": datetime(2026, 9, 10, tzinfo=UTC),
        }
    )
    with pytest.raises(ValueError, match="must not precede resolved"):
        RealizedForecastOutcome.model_validate(payload)


def test_future_observation_revision_cannot_leak_into_prediction(tmp_path: Path) -> None:
    path, run_id = _prepare_database(tmp_path)
    submission = _submission(
        run_id=run_id,
        issued_at=datetime(2026, 9, 8, 12, tzinfo=UTC),
        observation_cutoff=datetime(2026, 9, 8, 11, 30, tzinfo=UTC),
    )
    with pytest.raises(ValueError, match="later than the observation cutoff"):
        ForecastStore(path).record(submission)


def test_rule_day_supports_dst_and_uses_station_not_host_timezone() -> None:
    # Europe/Berlin's 2026 autumn transition makes this station-local day 25h.
    rule_day = ForecastRuleDay(
        station_id="EDDM",
        observation_date=date(2026, 10, 25),
        station_timezone="Europe/Berlin",
        day_start_utc=datetime(2026, 10, 24, 22, tzinfo=UTC),
        day_end_utc=datetime(2026, 10, 25, 23, tzinfo=UTC),
        display_unit="C",
        precision_decimal_places=0,
        rounding_rule="half-up",
        rules_hash="rules",
    )
    assert rule_day.day_end_utc - rule_day.day_start_utc == timedelta(hours=25)


def test_v1_payload_rows_remain_byte_identical_after_v2_insert(tmp_path: Path) -> None:
    path, run_id = _prepare_database(tmp_path)
    with Storage(path).connect() as connection:
        before = connection.execute(
            "SELECT id, payload_json FROM market_snapshots ORDER BY id"
        ).fetchall()
    ForecastStore(path).record(
        _submission(
            run_id=run_id,
            issued_at=datetime(2026, 9, 8, 12, tzinfo=UTC),
        )
    )
    with Storage(path).connect() as connection:
        after = connection.execute(
            "SELECT id, payload_json FROM market_snapshots ORDER BY id"
        ).fetchall()
    assert [(row["id"], row["payload_json"]) for row in before] == [
        (row["id"], row["payload_json"]) for row in after
    ]
