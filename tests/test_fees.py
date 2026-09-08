from decimal import Decimal

from polybot.fees import plan_buy_fill, platform_fee
from polybot.models import BookLevel


def test_weather_fee_example() -> None:
    assert platform_fee(
        shares=Decimal("10"),
        price=Decimal("0.5"),
        rate=Decimal("0.05"),
        exponent=Decimal("1"),
    ) == Decimal("0.1250")


def test_fill_plan_walks_best_ask_first() -> None:
    plan = plan_buy_fill(
        asks=[
            BookLevel(price=Decimal("0.60"), size=Decimal("10")),
            BookLevel(price=Decimal("0.50"), size=Decimal("2")),
        ],
        shares=Decimal("5"),
        fee_rate=Decimal(0),
        fee_exponent=Decimal(0),
    )

    assert plan.fully_fillable
    assert plan.fills[0].price == Decimal("0.50")
    assert plan.fills[0].shares == Decimal("2")
    assert plan.fills[1].price == Decimal("0.60")
    assert plan.fills[1].shares == Decimal("3")
    assert plan.vwap == Decimal("0.56")
