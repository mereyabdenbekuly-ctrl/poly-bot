from datetime import UTC, date, datetime
from decimal import Decimal

from polybot.config import Settings
from polybot.models import (
    BookLevel,
    DecisionAction,
    EventDefinition,
    MarketDefinition,
    MarketSnapshot,
    WeatherForecast,
)
from polybot.observations import apply_observed_max
from polybot.risk import evaluate_market
from polybot.rules import build_brackets
from polybot.weather import OpenMeteoEnsemble

_RUN_192_RAW_MEMBERS = [
    34.4,
    32.6,
    34.0,
    33.7,
    33.0,
    34.0,
    32.1,
    34.9,
    33.0,
    33.7,
    33.1,
    32.7,
    34.1,
    34.0,
    34.7,
    33.3,
    34.6,
    32.2,
    33.9,
    34.1,
    33.6,
    34.1,
    33.5,
    33.5,
    33.6,
    33.7,
    33.0,
    35.0,
    32.8,
    33.0,
]

_RUN_192_LABELS = [
    ("4321976", "27°C or below"),
    ("4321977", "28°C"),
    ("4321978", "29°C"),
    ("4321979", "30°C"),
    ("4321980", "31°C"),
    ("4321981", "32°C"),
    ("4321982", "33°C"),
    ("4321983", "34°C"),
    ("4321984", "35°C"),
    ("4321985", "36°C"),
    ("4321986", "37°C or higher"),
]

_RUN_192_PROBABILITIES = {
    "4321976": Decimal("0.0"),
    "4321977": Decimal("0.0"),
    "4321978": Decimal("0.0"),
    "4321979": Decimal("0.0"),
    "4321980": Decimal("0.0"),
    "4321981": Decimal("0.2443985621"),
    "4321982": Decimal("0.2874267403"),
    "4321983": Decimal("0.2410709517"),
    "4321984": Decimal("0.1439166602"),
    "4321985": Decimal("0.060824544"),
    "4321986": Decimal("0.0223625417"),
}


def _historical_event() -> EventDefinition:
    return EventDefinition(
        id="980859",
        slug="highest-temperature-in-shenzhen-on-september-9-2026",
        title="Highest temperature in Shenzhen on September 9?",
        description="Historical run 192 fixture; source NOAA ZGSZ, whole degrees Celsius.",
        observation_date=date(2026, 9, 9),
        markets=[
            MarketDefinition(
                id=market_id,
                slug=None,
                question=f"Historical Shenzhen bracket {label}",
                group_item_title=label,
                asset_id=f"asset-{market_id}",
                condition_id=f"condition-{market_id}",
                end_date=datetime(2026, 9, 9, 12, tzinfo=UTC),
                accepting_orders=True,
                fee_rate=Decimal("0.05"),
                fee_exponent=Decimal(1),
                fee_taker_only=True,
            )
            for market_id, label in _RUN_192_LABELS
        ],
    )


def test_historical_v1_shenzhen_run_192_is_reproducible_without_rewriting() -> None:
    settings = Settings(
        weather_error_sigma_c=1.5,
        min_probability_edge=Decimal("0.08"),
        min_expected_profit_usd=Decimal("0.25"),
        max_event_risk_usd=Decimal("2"),
        execution_buffer_usd=Decimal("0.02"),
        max_book_age_seconds=180,
    )
    historical_input = list(_RUN_192_RAW_MEMBERS)
    adjusted = apply_observed_max(historical_input, Decimal("32.0"))

    assert historical_input == _RUN_192_RAW_MEMBERS
    assert adjusted == _RUN_192_RAW_MEMBERS
    assert adjusted is not historical_input

    forecast = WeatherForecast(
        provider="open-meteo-ensemble",
        requested_location="Shenzhen",
        matched_location="Shenzhen Bao'an International Airport, Guangdong, China",
        latitude=22.63926,
        longitude=113.81066,
        timezone="Asia/Shanghai",
        observation_date=date(2026, 9, 9),
        unit="C",
        fetched_at=datetime(2026, 9, 9, 5, 3, 41, 398412, tzinfo=UTC),
        member_values=adjusted,
        unadjusted_member_values=historical_input,
        observed_floor_c=Decimal("32.0"),
    )
    brackets = build_brackets(_historical_event())
    model = OpenMeteoEnsemble(settings)
    probabilities = {
        market_id: Decimal(str(round(model.probability(forecast=forecast, bracket=bracket), 10)))
        for market_id, bracket in brackets.items()
    }

    assert probabilities == _RUN_192_PROBABILITIES
    assert sum(probabilities.values(), Decimal(0)) == Decimal("1.0000000000")

    snapshot = MarketSnapshot(
        event_id="980859",
        event_slug="highest-temperature-in-shenzhen-on-september-9-2026",
        event_title="Highest temperature in Shenzhen on September 9?",
        market_id="4321983",
        market_slug="highest-temperature-in-shenzhen-on-september-9-2026-34c",
        market_question="Will the highest temperature in Shenzhen be 34°C on September 9?",
        outcome_label="34°C",
        asset_id=(
            "101980983533229754999464419228686076156135035991340460388030949179894324134077"
        ),
        token_id=(
            "101980983533229754999464419228686076156135035991340460388030949179894324134077"
        ),
        condition_id=(
            "0x35630cc77c08486dba3e0d334a83d3090f4619fb42e667b05f56d29d6cbd6ae6"
        ),
        end_date=datetime(2026, 9, 9, 12, tzinfo=UTC),
        accepting_orders=True,
        book_timestamp=datetime(2026, 9, 9, 5, 3, 29, 141000, tzinfo=UTC),
        book_hash="eccf737c7360a923997c47e3e1e8cfcd6efd9da2",
        bids=[BookLevel(price=Decimal("0.01"), size=Decimal("134.45"))],
        asks=[BookLevel(price=Decimal("0.03"), size=Decimal("126.3"))],
        min_order_size=Decimal(5),
        tick_size=Decimal("0.01"),
        fee_rate=Decimal("0.05"),
        fee_exponent=Decimal(1),
        fee_taker_only=True,
    )
    decision = evaluate_market(
        snapshot=snapshot,
        probability=probabilities["4321983"],
        settings=settings,
        api_cost_usd=Decimal("0.02289"),
        now=datetime(2026, 9, 9, 5, 3, 45, 567407, tzinfo=UTC),
    )

    assert decision.strategy_version == "v1"
    assert decision.action == DecisionAction.PAPER_BUY
    assert decision.probability == Decimal("0.2410709517")
    assert decision.executable_price == Decimal("0.03")
    assert decision.probability_edge == Decimal("0.2110709517")
    assert decision.notional_usd == Decimal("0.15")
    assert decision.fee_usd == Decimal("0.007275")
    assert decision.max_loss_usd == Decimal("0.177275")
    assert decision.expected_profit_usd == Decimal("1.0051897585")
    assert decision.reason_codes == []
