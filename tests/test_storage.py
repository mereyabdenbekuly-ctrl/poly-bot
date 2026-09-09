from datetime import UTC, datetime
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
    pnl = storage.settle_paper_order("market-1", won=False)
    assert pnl == -Decimal("1.5725")
    assert storage.open_paper_market_ids() == []


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
    assert storage.settle_resolved_paper_order(order_id) == Decimal("3.4275")
    summary = storage.portfolio_summary()
    assert summary["orders_by_status"] == {"PAPER_SETTLED": 1}


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
