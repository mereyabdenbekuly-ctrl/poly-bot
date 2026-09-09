from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from types import TracebackType
from typing import Self

from polymarket import Event, PublicClient

from polybot.models import (
    BookLevel,
    EventDefinition,
    MarketDefinition,
    MarketSnapshot,
    OutcomeSide,
    ResolutionCheck,
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
        # Search relevance can put already-ended markets before tomorrow's markets.
        # Fetch a wider candidate page and only count events that can still accept orders.
        candidate_page_size = min(100, max(20, max_events * 10))
        page = self._client.search(
            q=query,
            events_status="active",
            search_tags=False,
            search_profiles=False,
            page_size=candidate_page_size,
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
                if is_event_open_for_trading(normalized):
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
            token_id=market.asset_id,
            condition_id=market.condition_id,
            outcome=OutcomeSide.YES,
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

    def get_resolution(
        self,
        *,
        market_id: str,
        condition_id: str,
        token_id: str,
        outcome: OutcomeSide,
    ) -> ResolutionCheck:
        """Return resolution evidence after verifying exact condition/token identity."""

        market = self._client.get_market(id=market_id)
        actual_condition_id = None if market.condition_id is None else str(market.condition_id)
        if actual_condition_id != condition_id:
            raise ValueError(
                f"condition identity mismatch for market {market_id}: "
                f"stored={condition_id}, api={actual_condition_id}"
            )
        market_outcome = market.outcomes.yes if outcome == OutcomeSide.YES else market.outcomes.no
        actual_token_id = None if market_outcome.token_id is None else str(market_outcome.token_id)
        if actual_token_id != token_id:
            raise ValueError(
                f"token identity mismatch for market {market_id} {outcome}: "
                f"stored={token_id}, api={actual_token_id}"
            )

        yes = market.outcomes.yes.price
        no = market.outcomes.no.price
        resolution_status = market.resolution.uma_resolution_status
        status_value = None if resolution_status is None else str(resolution_status.value)
        # ``closed`` alone can mean merely closed. A binary payout plus an
        # explicit final UMA state is required before paper accounting resolves.
        binary_payout = (yes == Decimal(1) and no == Decimal(0)) or (
            yes == Decimal(0) and no == Decimal(1)
        )
        confirmed = bool(
            market.state.closed
            and status_value in {"resolved", "settled"}
            and market.resolution.resolved_by is not None
            and binary_payout
        )
        yes_won = bool(yes == Decimal(1) and no == Decimal(0))
        won = (yes_won if outcome == OutcomeSide.YES else not yes_won) if confirmed else None
        return ResolutionCheck(
            market_id=str(market.id),
            condition_id=condition_id,
            token_id=token_id,
            outcome=outcome,
            checked_at=datetime.now(UTC),
            accepting_orders=bool(market.state.accepting_orders),
            closed=bool(market.state.closed),
            end_date=market.state.end_date,
            resolution_status=status_value,
            resolution_source=market.resolution.source,
            resolved_by=(
                None
                if market.resolution.resolved_by is None
                else str(market.resolution.resolved_by)
            ),
            confirmed=confirmed,
            won=won,
            yes_price=yes,
            no_price=no,
        )

    def get_snapshot_for_token(
        self,
        *,
        event_id: str,
        market_id: str,
        condition_id: str,
        token_id: str,
        outcome: OutcomeSide,
    ) -> MarketSnapshot:
        """Fetch an order book only after the stored condition/token pair is verified."""

        market = self._client.get_market(id=market_id)
        actual_condition_id = None if market.condition_id is None else str(market.condition_id)
        if actual_condition_id != condition_id:
            raise ValueError(
                f"condition identity mismatch for market {market_id}: "
                f"stored={condition_id}, api={actual_condition_id}"
            )
        market_outcome = market.outcomes.yes if outcome == OutcomeSide.YES else market.outcomes.no
        actual_token_id = None if market_outcome.token_id is None else str(market_outcome.token_id)
        if actual_token_id != token_id:
            raise ValueError(
                f"token identity mismatch for market {market_id} {outcome}: "
                f"stored={token_id}, api={actual_token_id}"
            )
        book = self._client.get_order_book(asset_id=token_id)
        schedule = market.trading.fee_schedule
        event = market.events[0] if market.events else None
        return MarketSnapshot(
            event_id=event_id,
            event_slug=None if event is None else event.slug,
            event_title=(market.question or market.group_item_title or market_id),
            market_id=market_id,
            market_slug=market.slug,
            market_question=market.question or market.group_item_title or market_id,
            outcome_label=market_outcome.label,
            asset_id=token_id,
            token_id=token_id,
            condition_id=condition_id,
            outcome=outcome,
            end_date=market.state.end_date,
            accepting_orders=bool(market.state.accepting_orders),
            book_timestamp=book.timestamp,
            book_hash=book.hash,
            bids=[BookLevel(price=level.price, size=level.size) for level in book.bids],
            asks=[BookLevel(price=level.price, size=level.size) for level in book.asks],
            min_order_size=book.min_order_size,
            tick_size=book.tick_size,
            fee_rate=Decimal(0) if schedule is None else schedule.rate,
            fee_exponent=(Decimal(0) if schedule is None else Decimal(str(schedule.exponent))),
            fee_taker_only=False if schedule is None else schedule.taker_only,
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


def is_event_open_for_trading(event: EventDefinition, *, now: datetime | None = None) -> bool:
    now = now or datetime.now(UTC)
    return any(
        market.accepting_orders and (market.end_date is None or market.end_date > now)
        for market in event.markets
    )
