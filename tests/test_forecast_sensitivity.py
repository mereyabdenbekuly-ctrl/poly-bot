from datetime import UTC, datetime
from decimal import Decimal

import pytest

from polybot.forecast_sensitivity import (
    SigmaTradeThresholds,
    calculate_sigma_sensitivity,
    clamped_sigma_bracket_probabilities,
    conditioned_intraday_maxima,
    empirical_bracket_probabilities,
    intraday_max_probabilities,
    replay_paper_qualification,
    v1_sigma_bracket_probabilities,
)
from polybot.models import BookLevel, Bracket, DecisionAction, MarketSnapshot

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

_RUN_192_BRACKETS = {
    "4321976": Bracket(market_id="4321976", label="27°C or below", lower=None, upper=28),
    "4321977": Bracket(market_id="4321977", label="28°C", lower=28, upper=29),
    "4321978": Bracket(market_id="4321978", label="29°C", lower=29, upper=30),
    "4321979": Bracket(market_id="4321979", label="30°C", lower=30, upper=31),
    "4321980": Bracket(market_id="4321980", label="31°C", lower=31, upper=32),
    "4321981": Bracket(market_id="4321981", label="32°C", lower=32, upper=33),
    "4321982": Bracket(market_id="4321982", label="33°C", lower=33, upper=34),
    "4321983": Bracket(market_id="4321983", label="34°C", lower=34, upper=35),
    "4321984": Bracket(market_id="4321984", label="35°C", lower=35, upper=36),
    "4321985": Bracket(market_id="4321985", label="36°C", lower=36, upper=37),
    "4321986": Bracket(market_id="4321986", label="37°C or higher", lower=37, upper=None),
}

_RUN_192_EXPECTED = {
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


def _snapshot(*, market_id: str = "target", ask: str = "0.01") -> MarketSnapshot:
    return MarketSnapshot(
        event_id="event-1",
        event_slug="event-1",
        event_title="Highest temperature test",
        market_id=market_id,
        market_slug=f"event-1-{market_id}",
        market_question="Will the maximum be in this range?",
        outcome_label="target",
        asset_id=f"asset-{market_id}",
        token_id=f"asset-{market_id}",
        condition_id=f"condition-{market_id}",
        end_date=datetime(2026, 9, 10, tzinfo=UTC),
        accepting_orders=True,
        book_timestamp=datetime(2026, 9, 9, 12, tzinfo=UTC),
        book_hash="archived-book-hash",
        bids=[],
        asks=[BookLevel(price=Decimal(ask), size=Decimal("100"))],
        min_order_size=Decimal(5),
        tick_size=Decimal("0.01"),
        fee_rate=Decimal("0.05"),
        fee_exponent=Decimal(1),
        fee_taker_only=True,
    )


def test_current_sigma_reproduces_the_immutable_v1_regression_distribution() -> None:
    probabilities = v1_sigma_bracket_probabilities(
        _RUN_192_RAW_MEMBERS,
        _RUN_192_BRACKETS,
        sigma_c=Decimal("1.5"),
        observed_floor_c=Decimal(32),
    )

    assert probabilities == _RUN_192_EXPECTED
    assert sum(probabilities.values(), Decimal(0)) == Decimal("1.0000000000")


def test_report_quantifies_each_probability_delta_without_rewriting_v1() -> None:
    report = calculate_sigma_sensitivity(
        member_values=_RUN_192_RAW_MEMBERS,
        brackets=_RUN_192_BRACKETS,
        current_sigma_c=Decimal("1.5"),
        alternative_sigma_c=Decimal("0.75"),
        observed_floor_c=Decimal(32),
    )

    assert report.current_sigma_probabilities == _RUN_192_EXPECTED
    assert report.raw_probabilities["4321983"] == Decimal(10) / Decimal(30)
    assert report.raw_probability_deltas["4321983"] == (
        report.raw_probabilities["4321983"] - Decimal("0.2410709517")
    )
    assert report.alternative_probability_deltas["4321983"] == (
        report.alternative_sigma_probabilities["4321983"]
        - report.current_sigma_probabilities["4321983"]
    )
    assert set(report.raw_probability_deltas) == set(_RUN_192_BRACKETS)
    assert sum(report.raw_probabilities.values(), Decimal(0)) == Decimal(1)


def test_kernel_created_paper_buy_ceases_under_raw_and_narrow_sigma() -> None:
    brackets = {
        "low": Bracket(market_id="low", label="below 29.5", lower=None, upper=29.5),
        "target": Bracket(market_id="target", label="29.5 to 30.5", lower=29.5, upper=30.5),
        "high": Bracket(market_id="high", label="30.5 or higher", lower=30.5, upper=None),
    }
    report = calculate_sigma_sensitivity(
        member_values=[Decimal(28)] * 20,
        brackets=brackets,
        current_sigma_c=Decimal("1.5"),
        alternative_sigma_c=Decimal("0.5"),
        snapshot=_snapshot(),
        thresholds=SigmaTradeThresholds(
            min_probability_edge=Decimal("0.08"),
            min_expected_profit_usd=Decimal("0.25"),
            max_event_risk_usd=Decimal(2),
            execution_buffer_usd=Decimal("0.02"),
        ),
    )

    assert report.raw_probabilities["target"] == Decimal(0)
    assert report.current_sigma_probabilities["target"] == Decimal("0.1108649017")
    assert report.alternative_sigma_probabilities["target"] == Decimal("0.0013496114")
    assert report.qualification is not None
    assert report.qualification.baseline.action == DecisionAction.PAPER_BUY
    assert report.qualification.baseline.expected_profit_usd == Decimal("0.4818495085")
    assert report.qualification.raw.action == DecisionAction.SKIP
    assert report.qualification.alternative.action == DecisionAction.SKIP
    assert report.qualification.raw_would_cease_to_qualify
    assert report.qualification.alternative_would_cease_to_qualify
    assert "EDGE_BELOW_THRESHOLD" in report.qualification.raw.reason_codes
    assert "EXPECTED_PROFIT_BELOW_THRESHOLD" in report.qualification.raw.reason_codes


def test_archived_replay_uses_configurable_edge_and_ev_thresholds() -> None:
    snapshot = _snapshot(ask="0.10")
    probability = Decimal("0.20")
    exact_edge = probability - Decimal("0.10")
    expected = Decimal(5) * exact_edge - Decimal("0.0225") - Decimal("0.02")
    thresholds = SigmaTradeThresholds(
        min_probability_edge=exact_edge,
        min_expected_profit_usd=expected,
    )

    qualified = replay_paper_qualification(
        snapshot=snapshot,
        probability=probability,
        thresholds=thresholds,
    )
    rejected = replay_paper_qualification(
        snapshot=snapshot,
        probability=probability,
        thresholds=SigmaTradeThresholds(
            min_probability_edge=exact_edge + Decimal("0.0001"),
            min_expected_profit_usd=expected + Decimal("0.0001"),
        ),
    )

    assert qualified.qualifies
    assert qualified.probability_edge == exact_edge
    assert qualified.expected_profit_usd == expected
    assert not rejected.qualifies
    assert rejected.reason_codes == (
        "EDGE_BELOW_THRESHOLD",
        "EXPECTED_PROFIT_BELOW_THRESHOLD",
    )


def test_intraday_max_preserves_scenario_mass_at_observed_maximum() -> None:
    brackets = {
        "below-27": Bracket(market_id="below-27", label="26°C or below", lower=None, upper=27),
        "27": Bracket(market_id="27", label="27°C", lower=27, upper=28),
        "28-plus": Bracket(market_id="28-plus", label="28°C or higher", lower=28, upper=None),
    }

    conditioned = conditioned_intraday_maxima(Decimal(27), [Decimal(26), Decimal(28)])
    probabilities = intraday_max_probabilities(
        observed_max_c=Decimal(27),
        future_maxima_c=[Decimal(26), Decimal(28)],
        brackets=brackets,
    )

    assert conditioned == (Decimal(27), Decimal(28))
    assert probabilities == {
        "below-27": Decimal(0),
        "27": Decimal("0.5"),
        "28-plus": Decimal("0.5"),
    }
    assert probabilities["28-plus"] != Decimal(1)


def test_clamped_kernel_keeps_lower_tail_as_mass_at_observed_maximum() -> None:
    brackets = {
        "below-27": Bracket(market_id="below-27", label="26°C or below", lower=None, upper=27),
        "27": Bracket(market_id="27", label="27°C", lower=27, upper=28),
        "28-plus": Bracket(market_id="28-plus", label="28°C or higher", lower=28, upper=None),
    }

    corrected = clamped_sigma_bracket_probabilities(
        [Decimal(26)],
        brackets,
        sigma_c=Decimal(1),
        observed_floor_c=Decimal(27),
    )
    immutable_v1 = v1_sigma_bracket_probabilities(
        [Decimal(26)],
        brackets,
        sigma_c=Decimal(1),
        observed_floor_c=Decimal(27),
    )

    assert corrected["27"] == pytest.approx(Decimal("0.9772498681"))
    assert corrected["28-plus"] == pytest.approx(Decimal("0.0227501319"))
    assert immutable_v1["28-plus"] > corrected["28-plus"]
    assert sum(corrected.values(), Decimal(0)) == Decimal("1.0000000000")


def test_empirical_distribution_rejects_gapped_brackets() -> None:
    brackets = {
        "low": Bracket(market_id="low", label="low", lower=None, upper=27),
        "high": Bracket(market_id="high", label="high", lower=28, upper=None),
    }

    with pytest.raises(ValueError, match="matched 0 market brackets"):
        empirical_bracket_probabilities([Decimal("27.5")], brackets)
