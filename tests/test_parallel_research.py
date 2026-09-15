from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace
from typing import Any, cast

from polybot.config import Settings, is_protected_model_endpoint
from polybot.forecast_models import (
    ForecastAlgorithmAttemptStatus,
    ForecastAlgorithmEligibility,
    ForecastEligibilityStage,
    ForecastEventEligibility,
)
from polybot.forecast_v2 import (
    ECMWF_RAW_ALGORITHM_VERSION,
    FORECAST_V2_ALGORITHM_VERSION,
)
from polybot.models import (
    BookLevel,
    EventDefinition,
    MarketDefinition,
    MarketSnapshot,
    WeatherForecast,
)
from polybot.observations import ObservationHistory
from polybot.scanner import Scanner, _EcmwfShadowJob
from polybot.storage import Storage


def _event(event_id: str) -> EventDefinition:
    return EventDefinition(
        id=event_id,
        slug=event_id,
        title=f"Temperature {event_id}",
        description="test",
        observation_date=None,
        markets=[
            MarketDefinition(
                id=f"{event_id}-market",
                slug=event_id,
                question="test",
                group_item_title="20°C",
                asset_id=f"{event_id}-asset",
                condition_id=f"{event_id}-condition",
                end_date=datetime.now(UTC) + timedelta(hours=1),
                accepting_orders=True,
                fee_rate=Decimal(0),
                fee_exponent=Decimal(0),
                fee_taker_only=False,
            )
        ],
    )


def test_model_transport_accepts_tls_or_numeric_loopback_only() -> None:
    assert is_protected_model_endpoint("https://example.test/v1")
    assert is_protected_model_endpoint("http://127.0.0.1:12455/v1")
    assert is_protected_model_endpoint("http://[::1]:12455/v1")
    assert not is_protected_model_endpoint("http://193.0.2.1:2455/v1")
    assert not is_protected_model_endpoint("http://localhost:2455/v1")
    assert not is_protected_model_endpoint("ftp://127.0.0.1/v1")

    cast(Any, Settings)(_env_file=None, openai_base_url="http://127.0.0.1:12455/v1")
    try:
        cast(Any, Settings)(_env_file=None, openai_base_url="http://193.0.2.1:2455/v1")
    except ValueError:
        pass
    else:
        raise AssertionError("public HTTP model endpoint must be rejected")


def test_shadow_registry_is_metadata_only_and_separate(tmp_path) -> None:
    storage = Storage(tmp_path / "state.sqlite3")
    parent = storage.start_scan(query="weather", mode="paper")
    research = storage.start_shadow_research(
        parent_scan_run_id=parent,
        outcome_requested=0,
        extra_requested=2,
    )
    inserted = storage.record_shadow_events(research, [_event("extra-1")])
    storage.finish_shadow_research(
        research,
        status="completed",
        outcome_completed=0,
        outcome_pending=0,
        extra_registered=inserted,
    )

    with storage.connect() as connection:
        row = connection.execute(
            "SELECT cohort_version, metadata_json FROM shadow_event_registry"
        ).fetchone()
        assert row is not None
        assert row[0] == "shadow-extra-v1"
        assert '"decision_eligible":false' in row[1]
        assert connection.execute("SELECT COUNT(*) FROM decisions").fetchone()[0] == 0
        tables = {
            row[0]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
        if "forecast_evaluation_events_v2" in tables:
            assert connection.execute(
                "SELECT COUNT(*) FROM forecast_evaluation_events_v2"
            ).fetchone()[0] == 0


def test_outcome_refresh_is_bounded_and_production_cohort_aware(tmp_path) -> None:
    storage = Storage(tmp_path / "state.sqlite3")
    # The forecast store owns the registry schema and validates the event shape.
    from polybot.forecast_store import ForecastStore

    store = ForecastStore(storage.path)
    now = datetime.now(UTC)
    for index in range(3):
        store.register_evaluation_event(
            ForecastEventEligibility(
                scan_run_id=storage.start_scan(query="weather", mode="paper"),
                event_id=f"event-{index}",
                considered_at_utc=now,
                event_title=f"Event {index}",
                market_count=1,
                rules_hash=f"hash-{index}",
                rule_parser="test",
                station_id="TEST",
                observation_date=now.date(),
                station_timezone="UTC",
                rule_day_start_utc=now - timedelta(hours=2),
                rule_day_end_utc=now - timedelta(hours=1),
                display_unit="C",
                precision_decimal_places=0,
                rounding_rule="round",
                eligible=True,
                stage=ForecastEligibilityStage.FORECAST_READY,
                algorithms=[
                    ForecastAlgorithmEligibility(
                        source="test",
                        model="test",
                        algorithm_version="test-v1",
                        expected=True,
                        status=ForecastAlgorithmAttemptStatus.PREDICTED,
                    )
                ],
            )
        )
    # A different cohort must never enter the production refresh batch.
    store.register_evaluation_event(
        ForecastEventEligibility(
            scan_run_id=storage.start_scan(query="weather", mode="paper"),
            event_id="shadow-only",
            cohort_version="shadow-extra-v1",
            considered_at_utc=now,
            event_title="Shadow",
            market_count=1,
            rules_hash="shadow-hash",
            rule_parser="test",
            station_id="TEST",
            observation_date=now.date(),
            station_timezone="UTC",
            rule_day_start_utc=now - timedelta(hours=2),
            rule_day_end_utc=now - timedelta(hours=1),
            display_unit="C",
            precision_decimal_places=0,
            rounding_rule="round",
            eligible=True,
            stage=ForecastEligibilityStage.FORECAST_READY,
            algorithms=[],
        )
    )

    assert store.outcome_refresh_event_ids(limit=2) == ["event-0", "event-1"]
    assert "shadow-only" not in store.outcome_refresh_event_ids()


def test_background_scheduler_skips_without_queuing(tmp_path) -> None:
    storage = Storage(tmp_path / "state.sqlite3")
    settings = cast(Any, Settings)(
        _env_file=None,
        database_path=storage.path,
        shadow_research_enabled=True,
        shadow_extra_max_events=1,
    )
    scanner = Scanner.__new__(Scanner)
    scanner_any = cast(Any, scanner)
    scanner_any.settings = settings
    scanner_any.storage = storage
    scanner_any.forecasts = SimpleNamespace(
        store=SimpleNamespace(outcome_refresh_event_ids=lambda **_: [])
    )
    scanner_any._background_research_enabled = True

    class Future:
        def done(self):
            return False

    class Executor:
        def submit(self, *args, **kwargs):  # noqa: ANN002, ANN003
            raise AssertionError("a second research batch must not be queued")

    scanner_any._research_future = cast(Any, Future())
    scanner_any._research_executor = cast(Any, Executor())
    parent = storage.start_scan(query="weather", mode="paper")
    scanner._schedule_shadow_research(  # noqa: SLF001
        parent_scan_run_id=parent,
        query="weather",
        excluded_event_ids=set(),
    )
    with storage.connect() as connection:
        status = connection.execute(
            "SELECT status, error FROM shadow_research_runs ORDER BY id DESC LIMIT 1"
        ).fetchone()
    assert status is not None
    assert status[0] == "skipped_busy"
    assert "no work queued" in status[1]


def _deferred_event_payload() -> tuple[
    EventDefinition,
    ObservationHistory,
    WeatherForecast,
    MarketSnapshot,
]:
    now = datetime.now(UTC)
    observation_date = date(2026, 9, 10)
    event = EventDefinition(
        id="deferred-event",
        slug="deferred-event",
        title="Highest temperature in Test City on September 10?",
        description=(
            "This market will resolve to the temperature range that contains the highest "
            "temperature recorded by NOAA at the Test City Airport Station in degrees "
            "Celsius on September 10. The resolution source is "
            "https://www.weather.gov/wrh/timeseries?site=test and measures temperatures "
            "to whole degrees Celsius."
        ),
        observation_date=observation_date,
        markets=[
            MarketDefinition(
                id="deferred-market",
                slug="deferred-market",
                question="Will the highest temperature be 20°C?",
                group_item_title="20°C",
                asset_id="deferred-asset",
                condition_id="deferred-condition",
                end_date=now + timedelta(hours=1),
                accepting_orders=True,
                fee_rate=Decimal(0),
                fee_exponent=Decimal(0),
                fee_taker_only=False,
            )
        ],
    )
    observation = ObservationHistory(
        station_id="TEST",
        station_name="Test City Airport",
        station_timezone="UTC",
        source_url="https://www.weather.gov/wrh/timeseries?site=test",
        observation_date=observation_date,
        fetched_at_utc=now,
        day_started=False,
        day_finished=False,
        expected_cadence_minutes=60,
        observations=[],
        observed_max_c=None,
        displayed_max_c=None,
        latest_observed_at_utc=None,
        stale=False,
    )
    forecast = WeatherForecast(
        provider="test-ensemble",
        requested_location="Test City",
        matched_location="Test City Airport",
        latitude=1,
        longitude=2,
        timezone="UTC",
        observation_date=observation_date,
        unit="C",
        fetched_at=now,
        member_values=[20.5] * 20,
    )
    snapshot = MarketSnapshot(
        event_id=event.id,
        event_slug=event.slug,
        event_title=event.title,
        market_id="deferred-market",
        market_slug="deferred-market",
        market_question=event.markets[0].question,
        outcome_label="20°C",
        asset_id="deferred-asset",
        token_id="deferred-token",
        condition_id="deferred-condition",
        end_date=event.markets[0].end_date,
        accepting_orders=True,
        book_timestamp=now,
        book_hash="deferred-book",
        bids=[],
        asks=[BookLevel(price=Decimal("0.10"), size=Decimal(100))],
        min_order_size=Decimal(5),
        tick_size=Decimal("0.01"),
        fee_rate=Decimal(0),
        fee_exponent=Decimal(0),
        fee_taker_only=False,
    )
    return event, observation, forecast, snapshot


def test_primary_lane_defers_ecmwf_shadow_until_after_market_snapshots() -> None:
    event, observation, forecast, market_snapshot = _deferred_event_payload()

    class FakeStorage:
        calls: list[str] = []

        def record_observation_history(self, run_id, event_id, value):  # noqa: ANN001
            self.calls.append("observation")

        def record_weather(self, run_id, event_id, value):  # noqa: ANN001
            self.calls.append("weather")

        def record_weathernext_snapshot(self, run_id, event_id, value):  # noqa: ANN001
            self.calls.append("weathernext")

        def record_market_snapshot(self, run_id, value):  # noqa: ANN001
            self.calls.append("market")

    class FakeForecasts:
        def __init__(self):
            self.registrations: list[Any] = []
            self.store = SimpleNamespace()

        def record_open_meteo(self, **kwargs):  # noqa: ANN003
            return 101

        def register_evaluation_event(self, item):  # noqa: ANN001
            self.registrations.append(item)
            return 202

    class FailingAdapter:
        calls = 0

        def forecast_from_baseline(self, baseline):  # noqa: ANN001
            self.calls += 1
            raise AssertionError("ECMWF must not run in the primary lane")

    scanner = Scanner.__new__(Scanner)
    scanner.settings = cast(
        Any,
        Settings(
            ecmwf_enabled=True,
            forecast_v2_enabled=True,
            shadow_research_enabled=True,
        ),
    )
    scanner.storage = cast(Any, FakeStorage())
    scanner.weather = cast(
        Any,
        SimpleNamespace(
            forecast=lambda rules: forecast,
            probability=lambda **kwargs: 0.60,
        ),
    )
    scanner.observations = cast(Any, SimpleNamespace(fetch=lambda rules: observation))
    scanner.weathernext = cast(
        Any,
        SimpleNamespace(
            snapshot_for=lambda rules: None,
            status=lambda: SimpleNamespace(state="access_pending"),
        ),
    )
    scanner.forecasts = cast(Any, FakeForecasts())
    scanner.ecmwf = cast(Any, FailingAdapter())
    scanner._background_research_enabled = True
    gateway = cast(Any, SimpleNamespace(get_snapshot=lambda **kwargs: market_snapshot))
    jobs: list[Any] = []

    decisions, errors = scanner._scan_event(  # noqa: SLF001
        run_id=77,
        gateway=gateway,
        event=event,
        use_astra=False,
        paper=True,
        shadow_jobs=jobs,
    )

    assert errors == []
    assert len(decisions) == 1
    assert len(jobs) == 1
    assert scanner.ecmwf.calls == 0
    assert scanner.storage.calls[-1] == "market"
    registration = scanner.forecasts.registrations[0]
    by_version = {item.algorithm_version: item for item in registration.algorithms}
    for version in (ECMWF_RAW_ALGORITHM_VERSION, FORECAST_V2_ALGORITHM_VERSION):
        assert by_version[version].status == ForecastAlgorithmAttemptStatus.SOURCE_UNAVAILABLE
        assert by_version[version].reason_codes == ["SHADOW_DEFERRED"]
    assert jobs[0].scan_run_id == 77
    assert jobs[0].event_id == event.id


def test_ecmwf_shadow_worker_preserves_scan_run_and_updates_attempts(monkeypatch) -> None:
    event, observation, forecast, _ = _deferred_event_payload()
    from polybot.rules import build_brackets, deterministic_rule_audit

    class FakeAdapter:
        def __init__(self, settings):  # noqa: ANN001
            pass

        def forecast_from_baseline(self, baseline):  # noqa: ANN001
            return SimpleNamespace(member_max_c=[Decimal("20")] * 20)

    class FakeProfile:
        state = "insufficient_history"

        def as_metadata(self):
            return {"state": self.state}

    class FakeCalibrator:
        def __init__(self, settings, store):  # noqa: ANN001
            pass

        def profile(self, **kwargs):  # noqa: ANN003
            return FakeProfile()

    result = SimpleNamespace(
        raw_probabilities={"deferred-market": Decimal("1")},
        v2_probabilities={"deferred-market": Decimal("1")},
        raw_point_c=Decimal("20"),
        v2_point_c=Decimal("20"),
        corrected_member_max_c=(Decimal("20"),) * 20,
        profile=FakeProfile(),
        intraday_features={},
    )
    monkeypatch.setattr("polybot.scanner.OpenMeteoEcmwfIfsEns", FakeAdapter)
    monkeypatch.setattr("polybot.scanner.ForecastV2Calibrator", FakeCalibrator)
    monkeypatch.setattr("polybot.scanner.build_v2_forecast", lambda **kwargs: result)

    class FakeStore:
        def __init__(self):
            self.attempts: list[dict[str, Any]] = []

        def update_algorithm_attempt(self, **kwargs):  # noqa: ANN003
            self.attempts.append(kwargs)

    class FakeEngine:
        def __init__(self):
            self.store = FakeStore()
            self.records: list[dict[str, Any]] = []

        def record_ecmwf_shadow(self, **kwargs):  # noqa: ANN003
            self.records.append(kwargs)
            return 501 + len(self.records)

    scanner = Scanner.__new__(Scanner)
    scanner.settings = cast(Any, Settings(ecmwf_enabled=True, forecast_v2_enabled=True))
    scanner.storage = cast(Any, SimpleNamespace())
    engine = FakeEngine()
    scanner_job = _EcmwfShadowJob(
        scan_run_id=88,
        event_id=event.id,
        audit=deterministic_rule_audit(event),
        brackets=build_brackets(event),
        observation_history=observation,
        baseline_forecast=forecast,
    )

    completed, failed, errors = scanner._run_ecmwf_shadow_jobs(  # noqa: SLF001
        (scanner_job,),
        forecast_engine=cast(Any, engine),
    )

    assert (completed, failed, errors) == (1, 0, [])
    assert [item["scan_run_id"] for item in engine.records] == [88, 88]
    assert {item["algorithm_version"] for item in engine.records} == {
        ECMWF_RAW_ALGORITHM_VERSION,
        FORECAST_V2_ALGORITHM_VERSION,
    }
    assert {(item["algorithm_version"], item["status"]) for item in engine.store.attempts} == {
        (ECMWF_RAW_ALGORITHM_VERSION, ForecastAlgorithmAttemptStatus.PREDICTED),
        (FORECAST_V2_ALGORITHM_VERSION, ForecastAlgorithmAttemptStatus.PREDICTED),
    }
