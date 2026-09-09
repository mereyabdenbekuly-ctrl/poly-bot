from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from enum import StrEnum
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class GeoblockStatus(StrictModel):
    blocked: bool
    ip: str | None = None
    country: str | None = None
    region: str | None = None


class RuleInterpretation(StrictModel):
    """Structured rule audit produced by code or GPT-6 Astra."""

    event_type: Literal["daily_max_temperature", "unsupported"]
    tradeable: bool
    location: str | None
    observation_date: date | None
    unit: Literal["C", "F", "unknown"]
    precision_decimal_places: int | None = Field(default=None, ge=0, le=3)
    station_or_authority: str | None
    resolution_source_url: str | None
    source_local_date: bool
    bucket_semantics_clear: bool
    ambiguity_reasons: list[str]
    summary: str
    confidence: float = Field(ge=0, le=1)


class RuleAudit(StrictModel):
    rules_hash: str
    parser: str
    interpretation: RuleInterpretation
    astra_cost_usd: Decimal = Decimal(0)
    astra_input_tokens: int = 0
    astra_output_tokens: int = 0
    cached: bool = False


class Bracket(StrictModel):
    market_id: str
    label: str
    lower: float | None
    upper: float | None
    lower_inclusive: bool = True
    upper_inclusive: bool = False


class WeatherForecast(StrictModel):
    provider: str
    requested_location: str
    matched_location: str
    latitude: float
    longitude: float
    timezone: str
    observation_date: date
    unit: Literal["C", "F"]
    fetched_at: datetime
    member_values: list[float]
    unadjusted_member_values: list[float] | None = None
    observed_floor_c: Decimal | None = None


class MarketDefinition(StrictModel):
    id: str
    slug: str | None
    question: str
    group_item_title: str
    asset_id: str
    condition_id: str | None
    end_date: datetime | None
    accepting_orders: bool
    fee_rate: Decimal
    fee_exponent: Decimal
    fee_taker_only: bool


class EventDefinition(StrictModel):
    id: str
    slug: str | None
    title: str
    description: str
    observation_date: date | None
    markets: list[MarketDefinition]


class BookLevel(StrictModel):
    price: Decimal
    size: Decimal


class OutcomeSide(StrEnum):
    """Binary outcome represented by a Polymarket token."""

    YES = "YES"
    NO = "NO"


class MarketSnapshot(StrictModel):
    event_id: str
    event_slug: str | None
    event_title: str
    market_id: str
    market_slug: str | None
    market_question: str
    outcome_label: str
    asset_id: str
    token_id: str | None = None
    condition_id: str | None
    outcome: OutcomeSide = OutcomeSide.YES
    end_date: datetime | None
    accepting_orders: bool
    book_timestamp: datetime | None
    book_hash: str
    bids: list[BookLevel]
    asks: list[BookLevel]
    min_order_size: Decimal
    tick_size: Decimal
    fee_rate: Decimal
    fee_exponent: Decimal
    fee_taker_only: bool


class Fill(StrictModel):
    price: Decimal
    shares: Decimal
    notional: Decimal
    fee: Decimal


class FillPlan(StrictModel):
    requested_shares: Decimal
    filled_shares: Decimal
    fills: list[Fill]
    total_notional: Decimal
    total_fee: Decimal
    vwap: Decimal | None
    fully_fillable: bool


class DecisionAction(StrEnum):
    OBSERVE = "OBSERVE"
    SKIP = "SKIP"
    PAPER_BUY = "PAPER_BUY"


class PaperOrderStatus(StrEnum):
    """Internal paper-position lifecycle; these are not Polymarket API values."""

    OPEN = "OPEN"
    AWAITING_RESULT = "AWAITING_RESULT"
    RESOLVED = "RESOLVED"
    PAPER_SETTLED = "PAPER_SETTLED"


class MarketDecision(StrictModel):
    action: DecisionAction
    reason_codes: list[str]
    warning_codes: list[str] = Field(default_factory=list)
    strategy_version: str = "v1"
    execution_model: str = "CROSSING_LIMIT_SHARES"
    event_id: str
    market_id: str
    asset_id: str
    token_id: str | None = None
    condition_id: str | None = None
    outcome: OutcomeSide = OutcomeSide.YES
    probability: Decimal | None = None
    shares: Decimal | None = None
    executable_price: Decimal | None = None
    probability_edge: Decimal | None = None
    notional_usd: Decimal | None = None
    fee_usd: Decimal | None = None
    api_cost_usd: Decimal = Decimal(0)
    execution_buffer_usd: Decimal = Decimal(0)
    max_loss_usd: Decimal | None = None
    expected_profit_usd: Decimal | None = None
    weathernext_probability: Decimal | None = None
    probability_delta_vs_weathernext: Decimal | None = None
    fee_rate: Decimal = Decimal(0)
    fee_exponent: Decimal = Decimal(0)
    end_date: datetime | None = None
    book_hash: str
    created_at: datetime


class PaperOrderTarget(StrictModel):
    """Identity and accounting fields needed to maintain one paper position."""

    id: int
    event_id: str
    market_id: str
    condition_id: str | None
    token_id: str
    outcome: OutcomeSide
    status: PaperOrderStatus
    strategy_version: str
    execution_model: str
    shares: Decimal
    entry_cost_usd: Decimal
    fee_rate: Decimal
    fee_exponent: Decimal
    end_date: datetime | None
    identity_verified: bool


class ResolutionCheck(StrictModel):
    """Resolution evidence read for one exact condition/outcome token."""

    market_id: str
    condition_id: str
    token_id: str
    outcome: OutcomeSide
    checked_at: datetime
    accepting_orders: bool
    closed: bool
    end_date: datetime | None
    resolution_status: str | None
    resolution_source: str | None
    resolved_by: str | None
    confirmed: bool
    won: bool | None
    yes_price: Decimal | None
    no_price: Decimal | None


class ResolvedWeatherWinner(StrictModel):
    """Official winning bracket for one fully resolved weather event."""

    event_id: str
    market_id: str
    condition_id: str
    outcome_label: str
    resolution_source: str
    resolved_by: str
    resolved_at_utc: datetime


class PaperMark(StrictModel):
    """Executable, same-token bid-side mark for a paper position."""

    order_id: int
    market_id: str
    condition_id: str
    token_id: str
    outcome: OutcomeSide
    captured_at: datetime
    book_timestamp: datetime | None
    book_hash: str
    requested_shares: Decimal
    current_bid_price: Decimal | None
    current_bid_size: Decimal
    immediately_sellable_shares: Decimal
    current_bid_mark_usd: Decimal | None
    full_exit_value_usd: Decimal | None
    full_exit_fee_usd: Decimal | None
    estimated_full_exit_pnl_usd: Decimal | None


class ScanReport(StrictModel):
    run_id: int
    geoblock: GeoblockStatus | None
    events_scanned: int
    markets_scanned: int
    paper_orders_opened: int
    paper_orders_settled: int = 0
    weather_next_status: dict[str, object] = Field(default_factory=dict)
    decisions: list[MarketDecision]
    errors: list[str]


class RuntimeWindow(StrictModel):
    id: int
    started_at: datetime
    ended_at: datetime | None
    status: Literal["ACTIVE", "COMPLETED"]
    query: str
    interval_seconds: int
    paper: bool
    astra: bool


class RuntimeReport(StrictModel):
    window_id: int
    kind: Literal["STARTUP", "INTERIM_60M", "COMPLETE_120M", "CYCLE"]
    created_at: datetime
    elapsed_seconds: int
    payload: dict[str, object]
