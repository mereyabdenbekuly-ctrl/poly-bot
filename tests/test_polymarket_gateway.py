from datetime import UTC, datetime, timedelta
from decimal import Decimal

from polybot.models import EventDefinition, MarketDefinition
from polybot.polymarket_gateway import is_event_open_for_trading


def event(*, accepting_orders: bool, end_date: datetime | None) -> EventDefinition:
    return EventDefinition(
        id="event",
        slug="event",
        title="Event",
        description="",
        observation_date=None,
        markets=[
            MarketDefinition(
                id="market",
                slug="market",
                question="Question",
                group_item_title="20°C",
                asset_id="asset",
                condition_id=None,
                end_date=end_date,
                accepting_orders=accepting_orders,
                fee_rate=Decimal(0),
                fee_exponent=Decimal(0),
                fee_taker_only=False,
            )
        ],
    )


def test_ended_event_is_not_a_candidate() -> None:
    now = datetime.now(UTC)
    assert not is_event_open_for_trading(
        event(accepting_orders=True, end_date=now - timedelta(seconds=1)), now=now
    )


def test_future_accepting_event_is_a_candidate() -> None:
    now = datetime.now(UTC)
    assert is_event_open_for_trading(
        event(accepting_orders=True, end_date=now + timedelta(hours=1)), now=now
    )


def test_non_accepting_event_is_not_a_candidate() -> None:
    now = datetime.now(UTC)
    assert not is_event_open_for_trading(
        event(accepting_orders=False, end_date=now + timedelta(hours=1)), now=now
    )
