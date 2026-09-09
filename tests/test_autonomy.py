from datetime import UTC, datetime, timedelta
from decimal import Decimal

from polybot.autonomy import AutonomousRunner
from polybot.config import Settings
from polybot.models import RuleInterpretation
from polybot.storage import Storage
from polybot.weathernext import WeatherNextProvider


def test_runtime_windows_and_reports_persist_across_reopen(tmp_path) -> None:
    path = tmp_path / "test.sqlite3"
    storage = Storage(path)
    window = storage.start_runtime_window(
        query="highest temperature", interval_seconds=300, paper=True, astra=True
    )
    storage.record_runtime_report(
        window.id,
        kind="STARTUP",
        elapsed_seconds=0,
        payload={"message": "started"},
    )

    reopened = Storage(path)
    active = reopened.get_active_runtime_window()
    assert active is not None
    assert active.id == window.id
    assert reopened.runtime_reports(window.id)[0].payload == {"message": "started"}


def test_cycle_report_is_updated_but_budget_state_is_not_reset(tmp_path) -> None:
    storage = Storage(tmp_path / "test.sqlite3")
    window = storage.start_runtime_window(
        query="weather", interval_seconds=300, paper=True, astra=True
    )
    reservation = storage.reserve_api_budget(
        model="gpt-6-astra", estimate=Decimal(1), budget=Decimal(5)
    )
    storage.settle_api_budget(reservation, actual_cost=Decimal(1), input_tokens=1, output_tokens=1)
    storage.record_runtime_report(window.id, kind="CYCLE", elapsed_seconds=1, payload={"cycle": 1})
    storage.record_runtime_report(window.id, kind="CYCLE", elapsed_seconds=2, payload={"cycle": 2})
    storage.finish_runtime_window(window.id)
    next_window = storage.start_runtime_window(
        query="weather", interval_seconds=300, paper=True, astra=True
    )

    assert next_window.id != window.id
    assert storage.runtime_reports(window.id)[0].payload == {"cycle": 2}
    assert storage.api_spend()[0] == Decimal(1)


def test_weathernext_reports_access_pending_without_fake_data(tmp_path) -> None:
    settings = Settings(
        weathernext_enabled=True,
        weathernext_snapshot_path=None,
        database_path=tmp_path / "test.sqlite3",
    )
    provider = WeatherNextProvider(settings)

    status = provider.status()
    assert status.state == "access_pending"
    assert (
        provider.snapshot_for(
            # Missing rule fields must not invent a forecast.
            RuleInterpretation(
                event_type="unsupported",
                tradeable=False,
                location=None,
                observation_date=None,
                unit="unknown",
                precision_decimal_places=None,
                station_or_authority=None,
                resolution_source_url=None,
                source_local_date=False,
                bucket_semantics_clear=False,
                ambiguity_reasons=[],
                summary="none",
                confidence=0,
            )
        )
        is None
    )


def test_60_and_120_minute_reports_roll_to_a_new_window(tmp_path) -> None:
    path = tmp_path / "test.sqlite3"
    storage = Storage(path)
    base = datetime(2026, 9, 9, tzinfo=UTC)
    window = storage.start_runtime_window(
        query="weather", interval_seconds=300, paper=True, astra=True
    ).model_copy(update={"started_at": base})
    runner = AutonomousRunner(
        settings=Settings(database_path=path),
        storage=storage,
        clock=lambda: base + timedelta(minutes=60),
    )
    same, rolled = runner.process_reporting_boundaries(
        window,
        query="weather",
        interval=300,
        paper=True,
        astra=True,
        scan_report=None,
    )
    assert not rolled
    assert same.id == window.id
    assert {item.kind for item in storage.runtime_reports(window.id)} == {"INTERIM_60M"}
    interim = storage.runtime_reports(window.id)[0]
    assert interim.payload["forecast_engine"]["counts"] == {  # type: ignore[index]
        "model_runs": 0,
        "predictions": 0,
        "outcome_versions": 0,
    }

    runner._clock = lambda: base + timedelta(minutes=120)  # noqa: SLF001
    next_window, rolled = runner.process_reporting_boundaries(
        window,
        query="weather",
        interval=300,
        paper=True,
        astra=True,
        scan_report=None,
    )
    assert rolled
    assert next_window.id != window.id
    assert {item.kind for item in storage.runtime_reports(window.id)} == {
        "INTERIM_60M",
        "COMPLETE_120M",
    }
    assert {item.kind for item in storage.runtime_reports(next_window.id)} == {"STARTUP"}
