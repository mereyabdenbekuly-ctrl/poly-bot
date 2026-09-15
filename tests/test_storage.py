import sqlite3
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

import pytest

from polybot.models import (
    DecisionAction,
    MarketDecision,
    OutcomeSide,
    ResolutionCheck,
    RuleAudit,
    RuleInterpretation,
)
from polybot.observations import ObservationHistory, ObservationVersion
from polybot.storage import BudgetExceededError, PaperRiskRejectedError, Storage


def decision(*, event_id: str = "event-1", market_id: str = "market-1") -> MarketDecision:
    return MarketDecision(
        action=DecisionAction.PAPER_BUY,
        reason_codes=[],
        event_id=event_id,
        market_id=market_id,
        asset_id=f"asset-{market_id}",
        token_id=f"asset-{market_id}",
        condition_id=f"condition-{market_id}",
        probability=Decimal("0.7"),
        shares=Decimal("5"),
        executable_price=Decimal("0.3"),
        probability_edge=Decimal("0.4"),
        notional_usd=Decimal("1.5"),
        fee_usd=Decimal("0.0525"),
        execution_buffer_usd=Decimal("0.02"),
        max_loss_usd=Decimal("1.5725"),
        expected_profit_usd=Decimal("1.9275"),
        book_hash="hash",
        created_at=datetime.now(UTC),
    )


def test_read_only_storage_skips_initialization_and_rejects_writes(tmp_path) -> None:
    path = tmp_path / "read-only.sqlite3"
    Storage(path)
    read_only = Storage(path, read_only=True)

    with read_only.connect() as connection:
        assert connection.execute("SELECT COUNT(*) FROM scan_runs").fetchone()[0] == 0
        with pytest.raises(sqlite3.OperationalError, match="readonly"):
            connection.execute("CREATE TABLE forbidden (id INTEGER)")

    with pytest.raises(RuntimeError, match="read-only storage"), read_only.transaction():
        pass


def test_read_only_storage_requires_existing_database(tmp_path) -> None:
    with pytest.raises(FileNotFoundError, match="does not exist"):
        Storage(tmp_path / "missing.sqlite3", read_only=True)


def test_api_budget_reserves_atomically(tmp_path) -> None:
    storage = Storage(tmp_path / "test.sqlite3")
    reservation = storage.reserve_api_budget(
        model="gpt-6-astra", estimate=Decimal("0.25"), budget=Decimal("0.30")
    )

    with pytest.raises(BudgetExceededError):
        storage.reserve_api_budget(
            model="gpt-6-astra", estimate=Decimal("0.10"), budget=Decimal("0.30")
        )

    storage.settle_api_budget(
        reservation,
        actual_cost=Decimal("0.07"),
        input_tokens=2000,
        output_tokens=1000,
    )
    settled, reserved = storage.api_spend()
    assert settled == Decimal("0.07")
    assert reserved == Decimal(0)


def test_only_one_open_paper_order_per_event(tmp_path) -> None:
    storage = Storage(tmp_path / "test.sqlite3")
    first = decision()
    storage.open_paper_order(
        first,
        idempotency_key="first",
        max_event_risk=Decimal("2"),
        max_total_risk=Decimal("6"),
    )

    with pytest.raises(PaperRiskRejectedError):
        storage.open_paper_order(
            decision(market_id="market-2"),
            idempotency_key="second",
            max_event_risk=Decimal("2"),
            max_total_risk=Decimal("6"),
        )

    assert storage.open_paper_market_ids() == ["market-1"]
    assert storage.active_paper_orders()[0].strategy_version == "v1"
    pnl = storage.settle_paper_order("market-1", won=False)
    assert pnl == -Decimal("1.5525")
    assert storage.open_paper_market_ids() == []


def test_decimal_exposure_is_not_aggregated_through_float(tmp_path) -> None:
    storage = Storage(tmp_path / "test.sqlite3")
    storage.open_paper_order(
        decision(event_id="event-1", market_id="market-1"),
        idempotency_key="first",
        max_event_risk=Decimal("2"),
        max_total_risk=Decimal("6"),
    )
    storage.open_paper_order(
        decision(event_id="event-2", market_id="market-2"),
        idempotency_key="second",
        max_event_risk=Decimal("2"),
        max_total_risk=Decimal("6"),
    )

    assert storage.portfolio_summary()["open_exposure_usd"] == Decimal("3.1450")
    assert storage.active_paper_event_ids() == {"event-1", "event-2"}


def test_observation_revisions_are_immutable_and_deduplicated(tmp_path) -> None:
    storage = Storage(tmp_path / "test.sqlite3")
    run_id = storage.start_scan(query="weather", mode="paper")
    first_seen = datetime.now(UTC)
    observation = ObservationVersion(
        station_id="EDDM",
        station_timezone="Europe/Berlin",
        observed_at_utc=datetime(2026, 9, 8, 22, 20, tzinfo=UTC),
        first_seen_at_utc=first_seen,
        source="weather.gov-wrh-synoptic",
        source_url="https://www.weather.gov/wrh/timeseries?site=eddm",
        temperature_c=Decimal("22"),
        displayed_temperature_c=Decimal("22"),
        raw_payload={"metar_set_1": "EDDM 082220Z 22/15"},
        revision_hash="revision-1",
        corrected=False,
    )
    history = ObservationHistory(
        station_id="EDDM",
        station_name="Munich Airport",
        station_timezone="Europe/Berlin",
        source_url=observation.source_url,
        observation_date=date(2026, 9, 9),
        fetched_at_utc=first_seen,
        day_started=True,
        day_finished=False,
        expected_cadence_minutes=30,
        observations=[observation],
        observed_max_c=Decimal("22"),
        displayed_max_c=Decimal("22"),
        latest_observed_at_utc=observation.observed_at_utc,
        stale=False,
    )
    storage.record_observation_history(run_id, "event", history)
    storage.record_observation_history(
        run_id,
        "event",
        history.model_copy(update={"fetched_at_utc": first_seen + timedelta(minutes=5)}),
    )

    with storage.connect() as connection:
        count = connection.execute("SELECT COUNT(*) FROM station_observation_versions").fetchone()[
            0
        ]
        fetches = connection.execute("SELECT COUNT(*) FROM observation_fetches").fetchone()[0]
    assert count == 1
    assert fetches == 2


def test_end_date_only_moves_order_to_awaiting_result(tmp_path) -> None:
    storage = Storage(tmp_path / "test.sqlite3")
    order_id = storage.open_paper_order(
        decision(),
        idempotency_key="first",
        max_event_risk=Decimal("2"),
        max_total_risk=Decimal("6"),
    )
    check = ResolutionCheck(
        market_id="market-1",
        condition_id="condition-market-1",
        token_id="asset-market-1",
        outcome=OutcomeSide.YES,
        checked_at=datetime.now(UTC),
        accepting_orders=False,
        closed=True,
        end_date=datetime.now(UTC),
        resolution_status=None,
        resolution_source="source",
        resolved_by=None,
        confirmed=False,
        won=None,
        yes_price=Decimal("0.9"),
        no_price=Decimal("0.1"),
    )

    storage.record_resolution_check(order_id, check)
    assert storage.mark_awaiting_result(order_id, check)
    target = storage.active_paper_orders()[0]
    assert target.status.value == "AWAITING_RESULT"
    assert storage.portfolio_summary()["realized_pnl_usd"] == Decimal(0)


def test_confirmed_identity_matched_result_uses_full_lifecycle(tmp_path) -> None:
    storage = Storage(tmp_path / "test.sqlite3")
    order_id = storage.open_paper_order(
        decision(),
        idempotency_key="first",
        max_event_risk=Decimal("2"),
        max_total_risk=Decimal("6"),
    )
    check = ResolutionCheck(
        market_id="market-1",
        condition_id="condition-market-1",
        token_id="asset-market-1",
        outcome=OutcomeSide.YES,
        checked_at=datetime.now(UTC),
        accepting_orders=False,
        closed=True,
        end_date=datetime.now(UTC),
        resolution_status="resolved",
        resolution_source="source",
        resolved_by="resolver",
        confirmed=True,
        won=True,
        yes_price=Decimal(1),
        no_price=Decimal(0),
    )
    storage.record_resolution_check(order_id, check)
    assert storage.resolve_paper_order(order_id, check)
    assert storage.active_paper_orders()[0].status.value == "RESOLVED"
    assert storage.settle_resolved_paper_order(order_id) == Decimal("3.4475")
    summary = storage.portfolio_summary()
    assert summary["orders_by_status"] == {"PAPER_SETTLED": 1}


def test_portfolio_net_after_api_does_not_double_charge_order_allocations(tmp_path) -> None:
    storage = Storage(tmp_path / "test.sqlite3")
    order_decision = decision().model_copy(update={"api_cost_usd": Decimal("0.10")})
    order_id = storage.open_paper_order(
        order_decision,
        idempotency_key="api-accounting",
        max_event_risk=Decimal("2"),
        max_total_risk=Decimal("6"),
    )
    reservation = storage.reserve_api_budget(
        model="gpt-6-astra", estimate=Decimal("0.10"), budget=Decimal("5")
    )
    storage.settle_api_budget(
        reservation, actual_cost=Decimal("0.10"), input_tokens=1, output_tokens=1
    )
    check = ResolutionCheck(
        market_id="market-1",
        condition_id="condition-market-1",
        token_id="asset-market-1",
        outcome=OutcomeSide.YES,
        checked_at=datetime.now(UTC),
        accepting_orders=False,
        closed=True,
        end_date=datetime.now(UTC),
        resolution_status="resolved",
        resolution_source="source",
        resolved_by="resolver",
        confirmed=True,
        won=False,
        yes_price=Decimal(0),
        no_price=Decimal(1),
    )
    storage.record_resolution_check(order_id, check)
    storage.resolve_paper_order(order_id, check)
    storage.settle_resolved_paper_order(order_id)

    summary = storage.portfolio_summary()

    # The order's realized value includes its $0.10 allocation. The global
    # ledger is subtracted once, so net remains gross trade P&L minus $0.10.
    assert summary["realized_pnl_usd"] == Decimal("-1.6525")
    assert summary["gross_trade_pnl_usd"] == Decimal("-1.5525")
    assert summary["net_project_pnl_after_api_usd"] == Decimal("-1.6525")


def test_cached_astra_audit_has_zero_marginal_cost(tmp_path) -> None:
    storage = Storage(tmp_path / "test.sqlite3")
    audit = RuleAudit(
        rules_hash="rules",
        parser="astra-v1",
        interpretation=RuleInterpretation(
            event_type="daily_max_temperature",
            tradeable=True,
            location="Test",
            observation_date=None,
            unit="C",
            precision_decimal_places=0,
            station_or_authority="Station",
            resolution_source_url="https://example.test",
            source_local_date=True,
            bucket_semantics_clear=True,
            ambiguity_reasons=[],
            summary="Test",
            confidence=1,
        ),
        astra_cost_usd=Decimal("0.07"),
        astra_input_tokens=2000,
        astra_output_tokens=1000,
    )
    storage.put_rule_cache(audit, model="gpt-6-astra")

    cached = storage.get_rule_cache("rules")
    assert cached is not None
    assert cached.cached
    assert cached.astra_cost_usd == Decimal(0)
    assert cached.astra_input_tokens == 0
    assert cached.astra_output_tokens == 0
