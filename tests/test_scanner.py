from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace
from typing import Any, cast

from polybot.config import Settings
from polybot.models import (
    BookLevel,
    DecisionAction,
    EventDefinition,
    GeoblockStatus,
    MarketDefinition,
    MarketSnapshot,
    RuleInterpretation,
    WeatherForecast,
)
from polybot.observations import ObservationHistory
from polybot.scanner import Scanner, _runtime_rule_ambiguities


def history() -> ObservationHistory:
    return ObservationHistory(
        station_id="EDDM",
        station_name="Munich Airport",
        station_timezone="Europe/Berlin",
        source_url="https://www.weather.gov/wrh/timeseries?site=eddm",
        observation_date=date(2026, 9, 9),
        fetched_at_utc=datetime.now(UTC),
        day_started=True,
        day_finished=False,
        expected_cadence_minutes=30,
        observations=[],
        observed_max_c=None,
        displayed_max_c=None,
        latest_observed_at_utc=None,
        stale=False,
    )


def interpretation(*reasons: str) -> RuleInterpretation:
    return RuleInterpretation(
        event_type="daily_max_temperature",
        tradeable=False,
        location="Munich",
        observation_date=date(2026, 9, 9),
        unit="C",
        precision_decimal_places=0,
        station_or_authority="Munich Airport",
        resolution_source_url="https://www.weather.gov/wrh/timeseries?site=eddm",
        source_local_date=False,
        bucket_semantics_clear=True,
        ambiguity_reasons=list(reasons),
        summary="test",
        confidence=1,
    )


def test_station_timezone_evidence_resolves_that_specific_ambiguity() -> None:
    blockers, warnings = _runtime_rule_ambiguities(
        interpretation("The observation-day timezone is not specified."), history()
    )
    assert blockers == []
    assert warnings == ["RULE_TIMEZONE_VERIFIED_FROM_STATION_SOURCE"]


def test_unresolved_rule_ambiguity_blocks_entry() -> None:
    blockers, warnings = _runtime_rule_ambiguities(
        interpretation("The temperature unit is unclear."), history()
    )
    assert blockers == ["UNRESOLVED_RULE_AMBIGUITY"]
    assert warnings == []


def _monitoring_event(now: datetime) -> EventDefinition:
    return EventDefinition(
        id="active-event",
        slug="active-event",
        title="Highest temperature in Test City on September 10?",
        description=(
            "This market will resolve to the temperature range that contains the highest "
            "temperature recorded by NOAA at the Test City Airport Station in degrees "
            "Celsius on September 10. The resolution source is "
            "https://www.weather.gov/wrh/timeseries?site=test and measures temperatures "
            "to whole degrees Celsius."
        ),
        observation_date=date(2026, 9, 10),
        markets=[
            MarketDefinition(
                id="active-market",
                slug="active-market",
                question="Will the highest temperature be 20°C?",
                group_item_title="20°C",
                asset_id="active-token",
                condition_id="active-condition",
                end_date=now + timedelta(hours=1),
                accepting_orders=True,
                fee_rate=Decimal(0),
                fee_exponent=Decimal(0),
                fee_taker_only=False,
            )
        ],
    )


def test_active_event_runs_full_v1_pipeline_but_never_opens_second_position() -> None:
    now = datetime.now(UTC)
    event = _monitoring_event(now)
    observation = history().model_copy(
        update={
            "station_id": "TEST",
            "station_name": "Test City Airport",
            "station_timezone": "UTC",
            "observation_date": date(2026, 9, 10),
            "fetched_at_utc": now,
            "day_started": False,
        }
    )
    forecast = WeatherForecast(
        provider="test-ensemble",
        requested_location="Test City",
        matched_location="Test City Airport",
        latitude=1,
        longitude=2,
        timezone="UTC",
        observation_date=date(2026, 9, 10),
        unit="C",
        fetched_at=now,
        member_values=[20.5] * 20,
    )
    snapshot = MarketSnapshot(
        event_id=event.id,
        event_slug=event.slug,
        event_title=event.title,
        market_id="active-market",
        market_slug="active-market",
        market_question="Will the highest temperature be 20°C?",
        outcome_label="20°C",
        asset_id="active-token",
        token_id="active-token",
        condition_id="active-condition",
        end_date=now + timedelta(hours=1),
        accepting_orders=True,
        book_timestamp=now,
        book_hash="active-book",
        bids=[],
        asks=[BookLevel(price=Decimal("0.10"), size=Decimal(100))],
        min_order_size=Decimal(5),
        tick_size=Decimal("0.01"),
        fee_rate=Decimal(0),
        fee_exponent=Decimal(0),
        fee_taker_only=False,
    )

    class FakeStorage:
        calls: list[tuple[str, str]] = []

        def record_observation_history(self, run_id, event_id, value):  # noqa: ANN001
            self.calls.append(("observations", event_id))

        def record_weather(self, run_id, event_id, value):  # noqa: ANN001
            self.calls.append(("forecast", event_id))

        def record_market_snapshot(self, run_id, value):  # noqa: ANN001
            self.calls.append(("market_snapshot", value.event_id))

        def record_decision(self, run_id, value, *, paper_order_id=None):  # noqa: ANN001
            assert paper_order_id is None
            self.calls.append(("decision", value.event_id))

        def open_paper_order(self, *args, **kwargs):  # noqa: ANN002, ANN003
            raise AssertionError("active-event monitoring must never open an order")

    class FakeWeather:
        def forecast(self, rules):  # noqa: ANN001
            return forecast

        def probability(self, *, forecast, bracket):  # noqa: ANN001
            return 0.60

    storage = FakeStorage()
    scanner = Scanner.__new__(Scanner)
    scanner.settings = Settings(
        min_ensemble_members=20,
        max_event_risk_usd=Decimal(2),
        min_probability_edge=Decimal("0.08"),
        min_expected_profit_usd=Decimal("0.25"),
    )
    scanner.storage = cast(Any, storage)
    scanner.weather = cast(Any, FakeWeather())
    scanner.observations = cast(Any, SimpleNamespace(fetch=lambda rules: observation))
    scanner.weathernext = cast(Any, SimpleNamespace(snapshot_for=lambda rules: None))

    gateway = cast(Any, SimpleNamespace(get_snapshot=lambda **kwargs: snapshot))
    decisions, opened, errors = scanner._process_event(
        run_id=123,
        gateway=gateway,
        event=event,
        use_astra=False,
        paper=True,
        allow_paper_open=False,
    )

    assert errors == []
    assert opened == 0
    assert len(decisions) == 1
    assert decisions[0].action == DecisionAction.OBSERVE
    assert decisions[0].reason_codes == ["ACTIVE_PAPER_EVENT_MONITOR_ONLY"]
    assert decisions[0].probability == Decimal("0.6")
    assert storage.calls == [
        ("observations", "active-event"),
        ("forecast", "active-event"),
        ("market_snapshot", "active-event"),
        ("decision", "active-event"),
    ]


def test_active_monitoring_does_not_consume_new_candidate_quota(monkeypatch) -> None:
    calls: list[tuple[str, object]] = []
    active_event = SimpleNamespace(id="active-event")
    candidate_event = SimpleNamespace(id="candidate-event")

    class FakeGateway:
        def __enter__(self):
            return self

        def __exit__(self, *args):  # noqa: ANN002
            return None

        def get_weather_event(self, event_id):  # noqa: ANN001
            calls.append(("active_load", event_id))
            return active_event

        def discover_weather_events(self, *, query, max_events, excluded_event_ids):  # noqa: ANN001
            calls.append(("candidate_quota", max_events))
            calls.append(("excluded", excluded_event_ids))
            return [candidate_event]

    class FakeStorage:
        def start_scan(self, *, query, mode, window_id):  # noqa: ANN001
            return 321

        def active_paper_event_ids(self):
            return {"active-event"}

        def finish_scan(self, *args, **kwargs):  # noqa: ANN002, ANN003
            return None

    scanner = Scanner.__new__(Scanner)
    scanner.settings = cast(
        Any, SimpleNamespace(geoblock_url="https://example.test", http_timeout_seconds=1)
    )
    scanner.storage = cast(Any, FakeStorage())
    scanner.weathernext = cast(
        Any, SimpleNamespace(status=lambda: SimpleNamespace(model_dump=lambda mode: {}))
    )
    monkeypatch.setattr("polybot.scanner.PolymarketGateway", FakeGateway)
    monkeypatch.setattr(
        "polybot.scanner.fetch_geoblock_status",
        lambda **kwargs: GeoblockStatus(blocked=False),
    )
    monkeypatch.setattr(scanner, "_settle_resolved_paper_orders", lambda gateway: (0, []))

    def process_event(**kwargs):  # noqa: ANN003
        calls.append((str(kwargs["event"].id), kwargs["allow_paper_open"]))
        return [], 0, []

    monkeypatch.setattr(scanner, "_process_event", process_event)
    report = scanner.scan(
        query="highest temperature",
        max_events=8,
        use_astra=False,
        paper=True,
    )

    assert ("candidate_quota", 8) in calls
    assert ("excluded", {"active-event"}) in calls
    assert ("active-event", False) in calls
    assert ("candidate-event", True) in calls
    assert report.events_scanned == 2
    assert report.paper_orders_opened == 0
