from datetime import UTC, datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace
from typing import Any, cast

from polybot.models import EventDefinition, MarketDefinition
from polybot.polymarket_gateway import PolymarketGateway, is_event_open_for_trading


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


def test_discovery_excludes_active_paper_events_without_consuming_limit(monkeypatch) -> None:
    now = datetime.now(UTC)
    events = {
        item_id: event(
            accepting_orders=True,
            end_date=now + timedelta(hours=1),
        ).model_copy(update={"id": item_id})
        for item_id in ("occupied", "new-1", "new-2")
    }

    class FakeClient:
        requested_page_size: int | None = None
        requested_ids: list[str] = []

        def search(self, **kwargs):  # noqa: ANN003
            self.requested_page_size = kwargs["page_size"]
            items = [SimpleNamespace(events=[SimpleNamespace(id=item_id)]) for item_id in events]
            return SimpleNamespace(first_page=lambda: SimpleNamespace(items=items))

        def get_event(self, *, id: str):
            self.requested_ids.append(id)
            return events[id]

    client = FakeClient()
    gateway = PolymarketGateway.__new__(PolymarketGateway)
    cast(Any, gateway)._client = client
    monkeypatch.setattr(
        PolymarketGateway,
        "_normalize_event",
        staticmethod(lambda item: item),
    )

    found = gateway.discover_weather_events(
        query="highest temperature",
        max_events=2,
        excluded_event_ids={"occupied"},
    )

    assert [item.id for item in found] == ["new-1", "new-2"]
    assert client.requested_ids == ["new-1", "new-2"]
    assert client.requested_page_size == 30


def test_exact_monitoring_load_deduplicates_and_keeps_ended_events(monkeypatch) -> None:
    ended = event(
        accepting_orders=False,
        end_date=datetime.now(UTC) - timedelta(hours=1),
    ).model_copy(update={"id": "occupied"})

    class FakeClient:
        requested_ids: list[str] = []

        def get_event(self, *, id: str):
            self.requested_ids.append(id)
            return ended

    client = FakeClient()
    gateway = PolymarketGateway.__new__(PolymarketGateway)
    cast(Any, gateway)._client = client
    monkeypatch.setattr(
        PolymarketGateway,
        "_normalize_event",
        staticmethod(lambda item: item),
    )

    found = gateway.get_weather_events_by_ids(["occupied", "occupied"])

    assert found == [ended]
    assert client.requested_ids == ["occupied"]
    assert not is_event_open_for_trading(found[0])


def test_resolved_winner_without_closed_time_uses_receipt_time() -> None:
    """The SDK Market model has no ``updated_at`` fallback attribute."""

    winner = SimpleNamespace(
        id="market-winner",
        condition_id="condition-winner",
        group_item_title="22°C",
        question="Will the maximum be 22°C?",
        state=SimpleNamespace(closed=True, closed_time=None),
        resolution=SimpleNamespace(
            uma_resolution_status=SimpleNamespace(value="resolved"),
            resolved_by="0xresolver",
            source="https://example.test/weather",
        ),
        outcomes=SimpleNamespace(
            yes=SimpleNamespace(price=Decimal(1)),
            no=SimpleNamespace(price=Decimal(0)),
        ),
    )
    gateway = PolymarketGateway.__new__(PolymarketGateway)
    cast(Any, gateway)._client = SimpleNamespace(
        get_event=lambda **kwargs: SimpleNamespace(id="event", markets=[winner])
    )

    resolved = gateway.get_resolved_weather_winner("event")

    assert resolved.market_id == "market-winner"
    assert resolved.resolved_at_utc.tzinfo is not None
