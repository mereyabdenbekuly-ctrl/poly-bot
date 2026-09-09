"""Deterministic diagnostics for the v1 ensemble error scale.

This module is deliberately independent from the scanner and from the paper
order writer.  It answers a narrow question: how much of a probability (and
therefore a historical paper decision) is created by the normal kernel around
an ensemble member?  The calculations use the same half-open bracket and
lower-floor semantics as the v1 weather model.

No value returned by this module is used to place an order.  A caller may pass
an archived :class:`~polybot.models.MarketSnapshot` to replay the economics of
one historical candidate, but the replay is read-only.
"""

from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, TypeAlias

from polybot.fees import plan_buy_fill
from polybot.models import Bracket, DecisionAction, MarketSnapshot

Number: TypeAlias = Decimal | int | float | str

DEFAULT_V1_SIGMA_C = Decimal("1.5")
DEFAULT_PROBABILITY_PLACES = 10


@dataclass(frozen=True, slots=True)
class SigmaTradeThresholds:
    """Probability/economics thresholds used by the historical replay.

    The defaults intentionally mirror ``Settings`` and ``risk.evaluate_market``
    but are copied here so this diagnostic remains a pure, versioned replay.
    Operational checks such as current book age are not part of the replay;
    the archived snapshot already represents the economics at decision time.
    """

    min_probability_edge: Decimal = Decimal("0.08")
    min_expected_profit_usd: Decimal = Decimal("0.25")
    max_event_risk_usd: Decimal = Decimal("2")
    execution_buffer_usd: Decimal = Decimal("0.02")

    def __post_init__(self) -> None:
        for field_name in (
            "min_probability_edge",
            "min_expected_profit_usd",
            "max_event_risk_usd",
            "execution_buffer_usd",
        ):
            value = _decimal(getattr(self, field_name), field_name=field_name)
            if value < 0:
                raise ValueError(f"{field_name} must be non-negative")
            object.__setattr__(self, field_name, value)

    @classmethod
    def from_settings(cls, settings: Any) -> SigmaTradeThresholds:
        """Build thresholds from a ``Settings``-like object.

        Keeping this duck-typed avoids importing the application configuration
        (and keeps this module usable from an offline diagnostics script).
        """

        defaults = cls()
        return cls(
            min_probability_edge=getattr(
                settings, "min_probability_edge", defaults.min_probability_edge
            ),
            min_expected_profit_usd=getattr(
                settings, "min_expected_profit_usd", defaults.min_expected_profit_usd
            ),
            max_event_risk_usd=getattr(settings, "max_event_risk_usd", defaults.max_event_risk_usd),
            execution_buffer_usd=getattr(
                settings, "execution_buffer_usd", defaults.execution_buffer_usd
            ),
        )


# A shorter name is convenient for callers and preserves room for a future
# distinction between probability and execution thresholds.
TradeQualificationThresholds = SigmaTradeThresholds


@dataclass(frozen=True, slots=True)
class PaperQualification:
    """Read-only replay of one archived market candidate."""

    market_id: str
    probability: Decimal
    qualifies: bool
    action: DecisionAction
    reason_codes: tuple[str, ...]
    executable_price: Decimal | None
    probability_edge: Decimal | None
    expected_profit_usd: Decimal | None
    notional_usd: Decimal
    fee_usd: Decimal
    max_loss_usd: Decimal
    api_cost_usd: Decimal
    shares: Decimal

    @property
    def would_paper_buy(self) -> bool:
        return self.qualifies


@dataclass(frozen=True, slots=True)
class QualificationComparison:
    """Qualification under current v1, raw empirical, and alternative sigma."""

    baseline: PaperQualification
    raw: PaperQualification
    alternative: PaperQualification

    @property
    def raw_would_cease_to_qualify(self) -> bool:
        return self.baseline.qualifies and not self.raw.qualifies

    @property
    def alternative_would_cease_to_qualify(self) -> bool:
        return self.baseline.qualifies and not self.alternative.qualifies

    # Explicit aliases make the result easy to consume from a dashboard/API.
    @property
    def raw_ceases_to_qualify(self) -> bool:
        return self.raw_would_cease_to_qualify

    @property
    def alternative_ceases_to_qualify(self) -> bool:
        return self.alternative_would_cease_to_qualify


@dataclass(frozen=True, slots=True)
class SigmaSensitivityReport:
    """Complete probability and historical-trade sensitivity report."""

    raw_probabilities: dict[str, Decimal]
    current_sigma_probabilities: dict[str, Decimal]
    alternative_sigma_probabilities: dict[str, Decimal]
    raw_probability_deltas: dict[str, Decimal]
    alternative_probability_deltas: dict[str, Decimal]
    current_sigma_c: Decimal
    alternative_sigma_c: Decimal
    observed_floor_c: Decimal | None
    conditioned_member_values_c: tuple[Decimal, ...]
    qualification: QualificationComparison | None = None

    # Common shorter aliases.  They are properties rather than duplicate
    # mutable fields, so the report stays immutable and deterministic.
    @property
    def raw_empirical(self) -> dict[str, Decimal]:
        return self.raw_probabilities

    @property
    def current_sigma(self) -> dict[str, Decimal]:
        return self.current_sigma_probabilities

    @property
    def alternative_sigma(self) -> dict[str, Decimal]:
        return self.alternative_sigma_probabilities

    @property
    def deltas_raw_vs_current(self) -> dict[str, Decimal]:
        return self.raw_probability_deltas

    @property
    def deltas_alternative_vs_current(self) -> dict[str, Decimal]:
        return self.alternative_probability_deltas

    def as_dict(self) -> dict[str, object]:
        """Return JSON-friendly strings without silently losing precision."""

        result: dict[str, object] = {
            "raw_probabilities": _string_map(self.raw_probabilities),
            "current_sigma_probabilities": _string_map(self.current_sigma_probabilities),
            "alternative_sigma_probabilities": _string_map(self.alternative_sigma_probabilities),
            "raw_probability_deltas": _string_map(self.raw_probability_deltas),
            "alternative_probability_deltas": _string_map(self.alternative_probability_deltas),
            "current_sigma_c": str(self.current_sigma_c),
            "alternative_sigma_c": str(self.alternative_sigma_c),
            "observed_floor_c": (
                None if self.observed_floor_c is None else str(self.observed_floor_c)
            ),
            "conditioned_member_values_c": [
                str(value) for value in self.conditioned_member_values_c
            ],
        }
        if self.qualification is not None:
            result["qualification"] = _qualification_dict(self.qualification)
        return result


# A descriptive alias for code that treats the report as a result object.
ForecastSensitivityReport = SigmaSensitivityReport


def empirical_bracket_probabilities(
    member_values: Sequence[Number],
    brackets: Mapping[str, Bracket],
    *,
    observed_floor_c: Number | None = None,
) -> dict[str, Decimal]:
    """Calculate the raw empirical full-bracket distribution.

    ``observed_floor_c`` conditions each full-day scenario with
    ``max(member, observed_floor)``.  This is intentionally *not* a filter of
    members followed by renormalization: a member forecasting 26°C after an
    observed 27°C becomes a 27°C final maximum and retains its ensemble mass.
    """

    values = _conditioned_values(member_values, observed_floor_c)
    _validate_brackets(brackets)
    counts = {market_id: 0 for market_id in brackets}
    for value in values:
        matched = _matching_markets(value, brackets)
        if len(matched) != 1:
            raise ValueError(
                f"ensemble member {value} matched {len(matched)} market brackets; "
                "full-bracket distribution is incomplete or overlapping"
            )
        counts[matched[0]] += 1
    total = Decimal(len(values))
    return {market_id: Decimal(count) / total for market_id, count in counts.items()}


def v1_sigma_bracket_probabilities(
    member_values: Sequence[Number],
    brackets: Mapping[str, Bracket],
    *,
    sigma_c: Number,
    observed_floor_c: Number | None = None,
    probability_places: int = DEFAULT_PROBABILITY_PLACES,
) -> dict[str, Decimal]:
    """Reproduce the v1 truncated-normal mixture for a supplied sigma.

    The implementation intentionally follows ``polybot.weather``:

    * each member is a Normal(mean, sigma) component;
    * an observed station maximum is both applied to member scenarios and used
      as a lower truncation floor;
    * interval endpoints use the existing ``Bracket`` inclusivity flags;
    * each market probability is rounded to ten decimal places by default,
      matching the immutable v1 audit convention.

    No post-hoc normalization is performed.  For a complete contiguous set of
    brackets the probabilities already sum to one (subject only to the same
    floating-point/rounding behavior as v1).
    """

    sigma = _positive_decimal(sigma_c, field_name="sigma_c")
    floor = _optional_decimal(observed_floor_c, field_name="observed_floor_c")
    values = _conditioned_values(member_values, floor)
    _validate_probability_places(probability_places)
    _validate_brackets(brackets)
    result: dict[str, Decimal] = {}
    for market_id, bracket in brackets.items():
        component_values = [
            _truncated_probability(
                mean=float(value),
                sigma=float(sigma),
                bracket=bracket,
                floor=None if floor is None else float(floor),
            )
            for value in values
        ]
        probability = sum(component_values) / len(component_values)
        result[market_id] = _round_probability(probability, probability_places)
    return result


def clamped_sigma_bracket_probabilities(
    member_values: Sequence[Number],
    brackets: Mapping[str, Bracket],
    *,
    sigma_c: Number,
    observed_floor_c: Number | None = None,
    probability_places: int = DEFAULT_PROBABILITY_PLACES,
) -> dict[str, Decimal]:
    """Kernel distribution for ``max(future maximum, observed maximum)``.

    Unlike the immutable v1 formula, this keeps the probability mass below the
    observed maximum as a point mass at that maximum. It is a diagnostic
    counterfactual only and never rewrites v1 history or trading decisions.
    """

    sigma = _positive_decimal(sigma_c, field_name="sigma_c")
    floor = _optional_decimal(observed_floor_c, field_name="observed_floor_c")
    values = tuple(_decimal(value, field_name="member_values") for value in member_values)
    if not values:
        raise ValueError("member_values must not be empty")
    _validate_probability_places(probability_places)
    _validate_brackets(brackets)
    result: dict[str, Decimal] = {}
    for market_id, bracket in brackets.items():
        component_values = [
            _clamped_probability(
                mean=float(value),
                sigma=float(sigma),
                bracket=bracket,
                floor=None if floor is None else float(floor),
            )
            for value in values
        ]
        result[market_id] = _round_probability(
            sum(component_values) / len(component_values), probability_places
        )
    return result


def calculate_sigma_sensitivity(
    *,
    member_values: Sequence[Number],
    brackets: Mapping[str, Bracket],
    current_sigma_c: Number = DEFAULT_V1_SIGMA_C,
    alternative_sigma_c: Number,
    observed_floor_c: Number | None = None,
    snapshot: MarketSnapshot | None = None,
    selected_market_id: str | None = None,
    thresholds: SigmaTradeThresholds | Any | None = None,
    api_cost_usd: Number = Decimal(0),
    probability_places: int = DEFAULT_PROBABILITY_PLACES,
) -> SigmaSensitivityReport:
    """Compare raw, current-v1, and alternative-sigma probabilities.

    If ``snapshot`` is supplied, ``selected_market_id`` defaults to its market
    ID and the archived order-book economics are replayed under all three
    probabilities.  This lets a caller answer whether a historical
    ``PAPER_BUY`` depended on the kernel, without mutating or re-evaluating the
    live scanner.
    """

    current = _positive_decimal(current_sigma_c, field_name="current_sigma_c")
    alternative = _positive_decimal(alternative_sigma_c, field_name="alternative_sigma_c")
    floor = _optional_decimal(observed_floor_c, field_name="observed_floor_c")
    values = _conditioned_values(member_values, floor)
    raw = empirical_bracket_probabilities(values, brackets)
    current_probabilities = v1_sigma_bracket_probabilities(
        values,
        brackets,
        sigma_c=current,
        observed_floor_c=floor,
        probability_places=probability_places,
    )
    alternative_probabilities = v1_sigma_bracket_probabilities(
        values,
        brackets,
        sigma_c=alternative,
        observed_floor_c=floor,
        probability_places=probability_places,
    )
    raw_delta = {
        market_id: raw[market_id] - current_probabilities[market_id] for market_id in brackets
    }
    alternative_delta = {
        market_id: alternative_probabilities[market_id] - current_probabilities[market_id]
        for market_id in brackets
    }

    comparison: QualificationComparison | None = None
    if snapshot is not None:
        market_id = selected_market_id or snapshot.market_id
        if market_id not in brackets:
            raise ValueError(f"selected market {market_id!r} is absent from brackets")
        if thresholds is None:
            threshold_values = SigmaTradeThresholds()
        elif isinstance(thresholds, SigmaTradeThresholds):
            threshold_values = thresholds
        else:
            threshold_values = SigmaTradeThresholds.from_settings(thresholds)
        cost = _decimal(api_cost_usd, field_name="api_cost_usd")
        if cost < 0:
            raise ValueError("api_cost_usd must be non-negative")
        comparison = QualificationComparison(
            baseline=replay_paper_qualification(
                snapshot=snapshot,
                probability=current_probabilities[market_id],
                thresholds=threshold_values,
                api_cost_usd=cost,
            ),
            raw=replay_paper_qualification(
                snapshot=snapshot,
                probability=raw[market_id],
                thresholds=threshold_values,
                api_cost_usd=cost,
            ),
            alternative=replay_paper_qualification(
                snapshot=snapshot,
                probability=alternative_probabilities[market_id],
                thresholds=threshold_values,
                api_cost_usd=cost,
            ),
        )

    return SigmaSensitivityReport(
        raw_probabilities=raw,
        current_sigma_probabilities=current_probabilities,
        alternative_sigma_probabilities=alternative_probabilities,
        raw_probability_deltas=raw_delta,
        alternative_probability_deltas=alternative_delta,
        current_sigma_c=current,
        alternative_sigma_c=alternative,
        observed_floor_c=floor,
        conditioned_member_values_c=values,
        qualification=comparison,
    )


# A few explicit aliases make the pure operation discoverable from scripts
# without duplicating any implementation.
analyze_sigma_sensitivity = calculate_sigma_sensitivity
run_sigma_sensitivity = calculate_sigma_sensitivity


def replay_paper_qualification(
    *,
    snapshot: MarketSnapshot,
    probability: Number,
    thresholds: SigmaTradeThresholds | Any | None = None,
    api_cost_usd: Number = Decimal(0),
) -> PaperQualification:
    """Replay the probability-sensitive portion of ``risk.evaluate_market``.

    The archived snapshot supplies asks, minimum shares, fees, and execution
    economics.  Current-time checks (book age, current accepting-orders state,
    and whether a market has since ended) are intentionally not inferred from
    today's clock.  ``accepting_orders`` and liquidity are retained because
    they are intrinsic to the archived economics.
    """

    if thresholds is None:
        threshold_values = SigmaTradeThresholds()
    elif isinstance(thresholds, SigmaTradeThresholds):
        threshold_values = thresholds
    else:
        threshold_values = SigmaTradeThresholds.from_settings(thresholds)
    probability_decimal = _decimal(probability, field_name="probability")
    api_cost = _decimal(api_cost_usd, field_name="api_cost_usd")
    if api_cost < 0:
        raise ValueError("api_cost_usd must be non-negative")

    reasons: list[str] = []
    if not Decimal(0) <= probability_decimal <= Decimal(1):
        reasons.append("INVALID_PROBABILITY")
    if not snapshot.accepting_orders:
        reasons.append("MARKET_NOT_ACCEPTING_ORDERS")
    if not snapshot.asks:
        reasons.append("NO_ASK_LIQUIDITY")

    plan = plan_buy_fill(
        asks=snapshot.asks,
        shares=snapshot.min_order_size,
        fee_rate=snapshot.fee_rate,
        fee_exponent=snapshot.fee_exponent,
    )
    if not plan.fully_fillable:
        reasons.append("MINIMUM_ORDER_NOT_FILLABLE")
    if plan.vwap is None:
        reasons.append("NO_EXECUTABLE_PRICE")

    max_loss = plan.total_notional + plan.total_fee + threshold_values.execution_buffer_usd
    if max_loss > threshold_values.max_event_risk_usd:
        reasons.append("MINIMUM_ORDER_EXCEEDS_EVENT_RISK")

    edge: Decimal | None = None
    expected: Decimal | None = None
    if Decimal(0) <= probability_decimal <= Decimal(1) and plan.vwap is not None:
        edge = probability_decimal - plan.vwap
        expected = (
            snapshot.min_order_size * edge
            - plan.total_fee
            - threshold_values.execution_buffer_usd
            - api_cost
        )
        if edge < threshold_values.min_probability_edge:
            reasons.append("EDGE_BELOW_THRESHOLD")
        if expected < threshold_values.min_expected_profit_usd:
            reasons.append("EXPECTED_PROFIT_BELOW_THRESHOLD")

    # Keep ordering stable and avoid duplicate reason codes when a malformed
    # snapshot triggers more than one path.
    unique_reasons = tuple(dict.fromkeys(reasons))
    qualifies = not unique_reasons
    return PaperQualification(
        market_id=snapshot.market_id,
        probability=probability_decimal,
        qualifies=qualifies,
        action=DecisionAction.PAPER_BUY if qualifies else DecisionAction.SKIP,
        reason_codes=unique_reasons,
        executable_price=plan.vwap,
        probability_edge=edge,
        expected_profit_usd=expected,
        notional_usd=plan.total_notional,
        fee_usd=plan.total_fee,
        max_loss_usd=max_loss,
        api_cost_usd=api_cost,
        shares=snapshot.min_order_size,
    )


# Alternate spelling for callers using the application's decision vocabulary.
evaluate_archived_paper_buy = replay_paper_qualification


def conditioned_intraday_maxima(
    observed_max_c: Number,
    future_maxima_c: Sequence[Number],
) -> tuple[Decimal, ...]:
    """Combine an observed station maximum with remaining-day scenarios.

    Every future scenario retains its original mass.  In particular,
    ``27`` observed with future scenarios ``26, 28`` returns ``(27, 28)``;
    callers must not discard the 26 scenario and renormalize 28 to 100%.
    """

    observed = _decimal(observed_max_c, field_name="observed_max_c")
    if not future_maxima_c:
        raise ValueError("future_maxima_c must not be empty")
    return tuple(
        max(observed, _decimal(value, field_name="future_maxima_c")) for value in future_maxima_c
    )


def intraday_max_probabilities(
    *,
    observed_max_c: Number,
    future_maxima_c: Sequence[Number],
    brackets: Mapping[str, Bracket],
) -> dict[str, Decimal]:
    """Return the empirical distribution of conditioned intraday maxima."""

    values = conditioned_intraday_maxima(observed_max_c, future_maxima_c)
    return empirical_bracket_probabilities(values, brackets)


# Readable aliases for test/diagnostic scripts.
combine_observed_and_future_maxima = conditioned_intraday_maxima
condition_intraday_maxima = conditioned_intraday_maxima
calculate_intraday_max_probabilities = intraday_max_probabilities


def _conditioned_values(
    member_values: Sequence[Number], observed_floor_c: Number | None
) -> tuple[Decimal, ...]:
    if not member_values:
        raise ValueError("member_values must not be empty")
    floor = _optional_decimal(observed_floor_c, field_name="observed_floor_c")
    values = tuple(_decimal(value, field_name="member_values") for value in member_values)
    if floor is None:
        return values
    return tuple(max(value, floor) for value in values)


def _validate_brackets(brackets: Mapping[str, Bracket]) -> None:
    if not brackets:
        raise ValueError("brackets must not be empty")
    if len(set(brackets)) != len(brackets):
        raise ValueError("bracket market IDs must be unique")


def _matching_markets(value: Decimal, brackets: Mapping[str, Bracket]) -> list[str]:
    return [
        market_id for market_id, bracket in brackets.items() if _value_in_bracket(value, bracket)
    ]


def _value_in_bracket(value: Decimal, bracket: Bracket) -> bool:
    lower = None if bracket.lower is None else Decimal(str(bracket.lower))
    upper = None if bracket.upper is None else Decimal(str(bracket.upper))
    lower_ok = lower is None or value > lower or (bracket.lower_inclusive and value == lower)
    upper_ok = upper is None or value < upper or (bracket.upper_inclusive and value == upper)
    return lower_ok and upper_ok


def _truncated_probability(
    *, mean: float, sigma: float, bracket: Bracket, floor: float | None
) -> float:
    lower = bracket.lower
    upper = bracket.upper
    if floor is None:
        return _interval_probability(mean, sigma, lower, upper)
    if upper is not None and upper <= floor:
        return 0.0
    effective_lower = floor if lower is None else max(lower, floor)
    numerator = _interval_probability(mean, sigma, effective_lower, upper)
    denominator = 1.0 - _normal_cdf((floor - mean) / sigma)
    return 0.0 if denominator <= 0 else max(0.0, min(1.0, numerator / denominator))


def _clamped_probability(
    *, mean: float, sigma: float, bracket: Bracket, floor: float | None
) -> float:
    if floor is None:
        return _interval_probability(mean, sigma, bracket.lower, bracket.upper)
    floor_decimal = Decimal(str(floor))
    if _value_in_bracket(floor_decimal, bracket):
        return 1.0 if bracket.upper is None else _normal_cdf((bracket.upper - mean) / sigma)
    if bracket.upper is not None and bracket.upper <= floor:
        return 0.0
    return _interval_probability(mean, sigma, bracket.lower, bracket.upper)


def _interval_probability(
    mean: float, sigma: float, lower: float | None, upper: float | None
) -> float:
    lower_cdf = 0.0 if lower is None else _normal_cdf((lower - mean) / sigma)
    upper_cdf = 1.0 if upper is None else _normal_cdf((upper - mean) / sigma)
    return max(0.0, min(1.0, upper_cdf - lower_cdf))


def _normal_cdf(value: float) -> float:
    return 0.5 * (1.0 + math.erf(value / math.sqrt(2.0)))


def _round_probability(value: float, places: int) -> Decimal:
    return Decimal(str(round(value, places)))


def _validate_probability_places(places: int) -> None:
    if not isinstance(places, int) or isinstance(places, bool) or places < 0 or places > 28:
        raise ValueError("probability_places must be an integer between 0 and 28")


def _decimal(value: Number | Decimal, *, field_name: str) -> Decimal:
    try:
        converted = value if isinstance(value, Decimal) else Decimal(str(value))
    except Exception as error:  # pragma: no cover - Decimal's exact exception varies
        raise ValueError(f"{field_name} must be a finite decimal") from error
    if not converted.is_finite():
        raise ValueError(f"{field_name} must be finite")
    return converted


def _optional_decimal(value: Number | None, *, field_name: str) -> Decimal | None:
    return None if value is None else _decimal(value, field_name=field_name)


def _positive_decimal(value: Number, *, field_name: str) -> Decimal:
    converted = _decimal(value, field_name=field_name)
    if converted <= 0:
        raise ValueError(f"{field_name} must be positive")
    return converted


def _string_map(values: Mapping[str, Decimal]) -> dict[str, str]:
    return {key: str(value) for key, value in values.items()}


def _qualification_dict(comparison: QualificationComparison) -> dict[str, object]:
    def one(item: PaperQualification) -> dict[str, object]:
        return {
            "market_id": item.market_id,
            "probability": str(item.probability),
            "qualifies": item.qualifies,
            "action": item.action.value,
            "reason_codes": list(item.reason_codes),
            "executable_price": (
                None if item.executable_price is None else str(item.executable_price)
            ),
            "probability_edge": (
                None if item.probability_edge is None else str(item.probability_edge)
            ),
            "expected_profit_usd": (
                None if item.expected_profit_usd is None else str(item.expected_profit_usd)
            ),
            "notional_usd": str(item.notional_usd),
            "fee_usd": str(item.fee_usd),
            "max_loss_usd": str(item.max_loss_usd),
            "api_cost_usd": str(item.api_cost_usd),
            "shares": str(item.shares),
        }

    return {
        "baseline": one(comparison.baseline),
        "raw": one(comparison.raw),
        "alternative": one(comparison.alternative),
        "raw_would_cease_to_qualify": comparison.raw_would_cease_to_qualify,
        "alternative_would_cease_to_qualify": comparison.alternative_would_cease_to_qualify,
    }


__all__ = [
    "DEFAULT_PROBABILITY_PLACES",
    "DEFAULT_V1_SIGMA_C",
    "ForecastSensitivityReport",
    "PaperQualification",
    "QualificationComparison",
    "SigmaSensitivityReport",
    "SigmaTradeThresholds",
    "TradeQualificationThresholds",
    "analyze_sigma_sensitivity",
    "calculate_intraday_max_probabilities",
    "calculate_sigma_sensitivity",
    "clamped_sigma_bracket_probabilities",
    "combine_observed_and_future_maxima",
    "condition_intraday_maxima",
    "conditioned_intraday_maxima",
    "empirical_bracket_probabilities",
    "evaluate_archived_paper_buy",
    "intraday_max_probabilities",
    "replay_paper_qualification",
    "run_sigma_sensitivity",
    "v1_sigma_bracket_probabilities",
]
