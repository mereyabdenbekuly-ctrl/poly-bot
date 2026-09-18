from datetime import date
from decimal import Decimal

from polybot.models import EventDefinition, MarketDefinition
from polybot.rules import build_brackets, deterministic_rule_audit


def sample_event() -> EventDefinition:
    description = """This market will resolve to the temperature range that contains the
highest temperature recorded by the Hong Kong Observatory in degrees Celsius on 8 Sep '26.
The source is https://example.test/weather and measures temperatures to one decimal place.
"""
    labels = [("1", "27°C or below"), ("2", "28°C"), ("3", "29°C or higher")]
    return EventDefinition(
        id="event-1",
        slug="highest-temperature-hong-kong",
        title="Highest temperature in Hong Kong on September 8?",
        description=description,
        observation_date=date(2026, 9, 8),
        markets=[
            MarketDefinition(
                id=market_id,
                slug=None,
                question=label,
                group_item_title=label,
                asset_id=f"asset-{market_id}",
                condition_id=None,
                end_date=None,
                accepting_orders=True,
                fee_rate=Decimal("0.05"),
                fee_exponent=Decimal(1),
                fee_taker_only=True,
            )
            for market_id, label in labels
        ],
    )


def test_deterministic_weather_rules_are_normalized() -> None:
    audit = deterministic_rule_audit(sample_event())

    assert audit.interpretation.tradeable
    assert audit.interpretation.location == "Hong Kong"
    assert audit.interpretation.unit == "C"
    assert audit.interpretation.precision_decimal_places == 1
    assert audit.interpretation.station_or_authority == "the Hong Kong Observatory"


def test_temperature_labels_become_contiguous_brackets() -> None:
    brackets = build_brackets(sample_event())

    assert brackets["1"].lower is None
    assert brackets["1"].upper == 28.0
    assert brackets["2"].lower == 28.0
    assert brackets["2"].upper == 29.0
    assert brackets["3"].lower == 29.0
    assert brackets["3"].upper is None


def test_fahrenheit_range_labels_become_brackets() -> None:
    event = sample_event()
    event.markets = [
        MarketDefinition(
            id=f"r-{k}",
            slug=None,
            question=label,
            group_item_title=label,
            asset_id=f"asset-r-{k}",
            condition_id=None,
            end_date=None,
            accepting_orders=True,
            fee_rate=Decimal("0.05"),
            fee_exponent=Decimal(1),
            fee_taker_only=True,
        )
        for k, label in [("a", "98-99°F"), ("b", "100-101°F"), ("c", "102°F or higher")]
    ]
    brackets = build_brackets(event)

    assert brackets["r-a"].lower == 98.0
    assert brackets["r-a"].upper == 100.0
    assert brackets["r-b"].lower == 100.0
    assert brackets["r-b"].upper == 102.0
    assert brackets["r-c"].lower == 102.0
    assert brackets["r-c"].upper is None


def test_whole_degree_precision_is_supported() -> None:
    candidate = sample_event().model_copy(
        update={
            "description": sample_event().description.replace(
                "to one decimal place", "to whole degrees Celsius"
            )
        }
    )
    audit = deterministic_rule_audit(candidate)
    assert audit.interpretation.precision_decimal_places == 0
    assert audit.interpretation.tradeable
