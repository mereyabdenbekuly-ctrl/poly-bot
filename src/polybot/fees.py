from __future__ import annotations

from decimal import Decimal

from polybot.models import BookLevel, Fill, FillPlan


def platform_fee(*, shares: Decimal, price: Decimal, rate: Decimal, exponent: Decimal) -> Decimal:
    """Return the platform fee for one fill using Polymarket's current fee curve."""

    if shares <= 0 or rate <= 0:
        return Decimal(0)
    if not Decimal(0) < price < Decimal(1):
        raise ValueError("price must be between 0 and 1")
    return shares * rate * ((price * (Decimal(1) - price)) ** exponent)


def plan_buy_fill(
    *,
    asks: list[BookLevel],
    shares: Decimal,
    fee_rate: Decimal,
    fee_exponent: Decimal,
) -> FillPlan:
    """Walk asks from best to worst and calculate a conservative taker fill."""

    remaining = shares
    fills: list[Fill] = []
    # The official SDK documents asks as descending, so the best ask is last.
    for level in reversed(asks):
        if remaining <= 0:
            break
        take = min(remaining, level.size)
        if take <= 0:
            continue
        notional = take * level.price
        fee = platform_fee(
            shares=take,
            price=level.price,
            rate=fee_rate,
            exponent=fee_exponent,
        )
        fills.append(Fill(price=level.price, shares=take, notional=notional, fee=fee))
        remaining -= take

    filled = shares - remaining
    total_notional = sum((fill.notional for fill in fills), Decimal(0))
    total_fee = sum((fill.fee for fill in fills), Decimal(0))
    vwap = total_notional / filled if filled > 0 else None
    return FillPlan(
        requested_shares=shares,
        filled_shares=filled,
        fills=fills,
        total_notional=total_notional,
        total_fee=total_fee,
        vwap=vwap,
        fully_fillable=remaining == 0,
    )


def plan_sell_fill(
    *,
    bids: list[BookLevel],
    shares: Decimal,
    fee_rate: Decimal,
    fee_exponent: Decimal,
) -> FillPlan:
    """Walk bids from best to worst and value an immediately executable sale."""

    remaining = shares
    fills: list[Fill] = []
    # The official SDK returns bids ascending, so the best bid is last.
    for level in reversed(bids):
        if remaining <= 0:
            break
        take = min(remaining, level.size)
        if take <= 0:
            continue
        notional = take * level.price
        fee = platform_fee(
            shares=take,
            price=level.price,
            rate=fee_rate,
            exponent=fee_exponent,
        )
        fills.append(Fill(price=level.price, shares=take, notional=notional, fee=fee))
        remaining -= take

    filled = shares - remaining
    total_notional = sum((fill.notional for fill in fills), Decimal(0))
    total_fee = sum((fill.fee for fill in fills), Decimal(0))
    vwap = total_notional / filled if filled > 0 else None
    return FillPlan(
        requested_shares=shares,
        filled_shares=filled,
        fills=fills,
        total_notional=total_notional,
        total_fee=total_fee,
        vwap=vwap,
        fully_fillable=remaining == 0,
    )
