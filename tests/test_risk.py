from datetime import UTC, datetime, timedelta
from decimal import Decimal

from polybot.config import Settings
from polybot.models import BookLevel, DecisionAction, MarketSnapshot
from polybot.risk import evaluate_market


def snapshot(*, timestamp: datetime | None = None) -> MarketSnapshot:
    return MarketSnapshot(
        event_id="event-1",
        event_slug="event",
        event_title="Event",
        market_id="market-1",
        market_slug="market",
        market_question="Question",
        outcome_label="Yes",
        asset_id="asset-1",
        condition_id=None,
        end_date=datetime.now(UTC) + timedelta(days=1),
        accepting_orders=True,
        book_timestamp=timestamp or datetime.now(UTC),
        book_hash="book-hash",
        bids=[],
        asks=[BookLevel(price=Decimal("0.30"), size=Decimal("10"))],
        min_order_size=Decimal("5"),
        tick_size=Decimal("0.01"),
        fee_rate=Decimal("0.05"),
        fee_exponent=Decimal(1),
        fee_taker_only=True,
    )


def test_qualified_candidate_is_paper_only() -> None:
    decision = evaluate_market(
        snapshot=snapshot(),
        probability=Decimal("0.70"),
        settings=Settings(),
    )

    assert decision.action == DecisionAction.PAPER_BUY
    assert decision.max_loss_usd is not None
    assert decision.max_loss_usd < Decimal("2")
    assert decision.expected_profit_usd is not None
    assert decision.expected_profit_usd > Decimal("0.25")


def test_stale_book_is_rejected() -> None:
    decision = evaluate_market(
        snapshot=snapshot(timestamp=datetime.now(UTC) - timedelta(hours=1)),
        probability=Decimal("0.70"),
        settings=Settings(),
    )

    assert decision.action == DecisionAction.SKIP
    assert "STALE_ORDER_BOOK" in decision.reason_codes
