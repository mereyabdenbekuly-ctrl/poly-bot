from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

from polybot.config import Settings
from polybot.fees import plan_buy_fill
from polybot.models import DecisionAction, MarketDecision, MarketSnapshot


def evaluate_market(
    *,
    snapshot: MarketSnapshot,
    probability: Decimal | None,
    settings: Settings,
    api_cost_usd: Decimal = Decimal(0),
    now: datetime | None = None,
) -> MarketDecision:
    now = now or datetime.now(UTC)
    reasons: list[str] = []

    if probability is None:
        reasons.append("NO_PROBABILITY_ESTIMATE")
    elif not Decimal(0) <= probability <= Decimal(1):
        reasons.append("INVALID_PROBABILITY")

    if not snapshot.accepting_orders:
        reasons.append("MARKET_NOT_ACCEPTING_ORDERS")
    if not snapshot.asks:
        reasons.append("NO_ASK_LIQUIDITY")
    if snapshot.end_date is not None and snapshot.end_date <= now:
        reasons.append("MARKET_ENDED")
    if snapshot.book_timestamp is None:
        reasons.append("BOOK_TIMESTAMP_UNKNOWN")
    else:
        age = (now - snapshot.book_timestamp.astimezone(UTC)).total_seconds()
        if age > settings.max_book_age_seconds:
            reasons.append("STALE_ORDER_BOOK")
        if age < -30:
            reasons.append("BOOK_TIMESTAMP_IN_FUTURE")

    plan = plan_buy_fill(
        asks=snapshot.asks,
        shares=snapshot.min_order_size,
        fee_rate=snapshot.fee_rate,
        fee_exponent=snapshot.fee_exponent,
    )
    if not plan.fully_fillable:
        reasons.append("MINIMUM_ORDER_NOT_FILLABLE")
    if plan.vwap is None:
        reasons.append("NO_EXECUTABLE_PRICE")

    max_loss = plan.total_notional + plan.total_fee + settings.execution_buffer_usd
    if max_loss > settings.max_event_risk_usd:
        reasons.append("MINIMUM_ORDER_EXCEEDS_EVENT_RISK")

    edge: Decimal | None = None
    expected: Decimal | None = None
    if (
        probability is not None
        and plan.vwap is not None
        and Decimal(0) <= probability <= Decimal(1)
    ):
        edge = probability - plan.vwap
        expected = (
            snapshot.min_order_size * edge
            - plan.total_fee
            - settings.execution_buffer_usd
            - api_cost_usd
        )
        if edge < settings.min_probability_edge:
            reasons.append("EDGE_BELOW_THRESHOLD")
        if expected < settings.min_expected_profit_usd:
            reasons.append("EXPECTED_PROFIT_BELOW_THRESHOLD")

    action = DecisionAction.PAPER_BUY if not reasons else DecisionAction.SKIP
    return MarketDecision(
        action=action,
        reason_codes=reasons,
        event_id=snapshot.event_id,
        market_id=snapshot.market_id,
        asset_id=snapshot.asset_id,
        probability=probability,
        shares=snapshot.min_order_size,
        executable_price=plan.vwap,
        probability_edge=edge,
        notional_usd=plan.total_notional,
        fee_usd=plan.total_fee,
        api_cost_usd=api_cost_usd,
        execution_buffer_usd=settings.execution_buffer_usd,
        max_loss_usd=max_loss,
        expected_profit_usd=expected,
        book_hash=snapshot.book_hash,
        created_at=now,
    )
