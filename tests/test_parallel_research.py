from __future__ import annotations

from datetime import UTC, datetime, timedelta
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
from polybot.models import EventDefinition, MarketDefinition
from polybot.scanner import Scanner
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
