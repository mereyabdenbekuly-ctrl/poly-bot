from __future__ import annotations

from decimal import Decimal
from types import TracebackType
from typing import Self

from polymarket import Event, PublicClient

from polybot.models import (
    BookLevel,
    EventDefinition,
    MarketDefinition,
    MarketSnapshot,
)


class PolymarketGateway:
    """Thin normalization layer over the official ``polymarket-client`` SDK."""

    def __init__(self) -> None:
        self._client = PublicClient()

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.close()

    def close(self) -> None:
        self._client.close()

    def discover_weather_events(self, *, query: str, max_events: int) -> list[EventDefinition]:
        page = self._client.search(
            q=query,
            events_status="active",
            search_tags=False,
            search_profiles=False,
            page_size=max_events,
        ).first_page()
        if not page.items:
            return []

        events: list[EventDefinition] = []
        seen: set[str] = set()
        for result in page.items:
            for event_ref in result.events:
                event_id = str(event_ref.id)
                if event_id in seen:
                    continue
                seen.add(event_id)
                event = self._client.get_event(id=event_id)
                normalized = self._normalize_event(event)
                if normalized.markets:
                    events.append(normalized)
                if len(events) >= max_events:
                    return events
        return events

    def get_snapshot(self, *, event: EventDefinition, market: MarketDefinition) -> MarketSnapshot:
        book = self._client.get_order_book(asset_id=market.asset_id)
        return MarketSnapshot(
            event_id=event.id,
            event_slug=event.slug,
            event_title=event.title,
            market_id=market.id,
            market_slug=market.slug,
            market_question=market.question,
            outcome_label=market.group_item_title,
            asset_id=market.asset_id,
            condition_id=market.condition_id,
            end_date=market.end_date,
            accepting_orders=market.accepting_orders,
            book_timestamp=book.timestamp,
            book_hash=book.hash,
            bids=[BookLevel(price=level.price, size=level.size) for level in book.bids],
            asks=[BookLevel(price=level.price, size=level.size) for level in book.asks],
            min_order_size=book.min_order_size,
            tick_size=book.tick_size,
            fee_rate=market.fee_rate,
            fee_exponent=market.fee_exponent,
            fee_taker_only=market.fee_taker_only,
        )

    @staticmethod
    def _normalize_event(event: Event) -> EventDefinition:
        raw_markets = event.markets
        markets: list[MarketDefinition] = []
        for market in raw_markets:
            token_id = market.outcomes.yes.token_id
            if token_id is None:
                continue
            schedule = market.trading.fee_schedule
            markets.append(
                MarketDefinition(
                    id=str(market.id),
                    slug=market.slug,
                    question=market.question or market.group_item_title or str(market.id),
                    group_item_title=market.group_item_title or market.question or str(market.id),
                    asset_id=str(token_id),
                    condition_id=None if market.condition_id is None else str(market.condition_id),
                    end_date=market.state.end_date,
                    accepting_orders=bool(market.state.accepting_orders),
                    fee_rate=Decimal(0) if schedule is None else schedule.rate,
                    fee_exponent=Decimal(0)
                    if schedule is None
                    else Decimal(str(schedule.exponent)),
                    fee_taker_only=False if schedule is None else schedule.taker_only,
                )
            )
        return EventDefinition(
            id=str(event.id),
            slug=event.slug,
            title=event.title or str(event.id),
            description=event.description or "",
            observation_date=event.schedule.event_date,
            markets=markets,
        )
