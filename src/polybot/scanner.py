from __future__ import annotations

import hashlib
from datetime import datetime
from decimal import Decimal

from polybot.astra import AstraRuleAuditor
from polybot.config import Settings
from polybot.geoblock import fetch_geoblock_status
from polybot.models import (
    DecisionAction,
    EventDefinition,
    MarketDecision,
    RuleAudit,
    RuleInterpretation,
    ScanReport,
)
from polybot.observations import (
    ObservationHistory,
    StationObservationCollector,
    apply_observed_max,
    bracket_is_impossible,
)
from polybot.polymarket_gateway import PolymarketGateway
from polybot.risk import evaluate_market
from polybot.rules import build_brackets, deterministic_rule_audit
from polybot.storage import PaperRiskRejectedError, Storage
from polybot.weather import OpenMeteoEnsemble
from polybot.weathernext import WeatherNextProvider


class Scanner:
    def __init__(self, *, settings: Settings, storage: Storage) -> None:
        self.settings = settings
        self.storage = storage
        self.weather = OpenMeteoEnsemble(settings)
        self.observations = StationObservationCollector(settings)
        self.weathernext = WeatherNextProvider(settings)

    def scan(
        self,
        *,
        query: str,
        max_events: int,
        use_astra: bool,
        paper: bool,
        window_id: int | None = None,
    ) -> ScanReport:
        mode = "paper" if paper else "observe"
        run_id = self.storage.start_scan(query=query, mode=mode, window_id=window_id)
        geoblock = None
        errors: list[str] = []
        decisions: list[MarketDecision] = []
        paper_orders_opened = 0
        paper_orders_settled = 0
        events_scanned = 0
        markets_scanned = 0
        weather_next_status = self.weathernext.status().model_dump(mode="json")

        try:
            try:
                geoblock = fetch_geoblock_status(
                    url=self.settings.geoblock_url,
                    timeout=self.settings.http_timeout_seconds,
                ).model_copy(update={"ip": None})
            except Exception as error:
                errors.append(f"geoblock check failed: {error}")

            with PolymarketGateway() as gateway:
                paper_orders_settled, settlement_errors = self._settle_resolved_paper_orders(
                    gateway
                )
                errors.extend(settlement_errors)
                events = gateway.discover_weather_events(query=query, max_events=max_events)
                for event in events:
                    events_scanned += 1
                    event_decisions, event_errors = self._scan_event(
                        run_id=run_id,
                        gateway=gateway,
                        event=event,
                        use_astra=use_astra,
                        paper=paper,
                    )
                    errors.extend(event_errors)
                    markets_scanned += len(event_decisions)

                    final_decisions, opened = self._finalize_event_decisions(
                        event_decisions, paper=paper
                    )
                    paper_orders_opened += opened
                    for decision, paper_order_id in final_decisions:
                        self.storage.record_decision(
                            run_id, decision, paper_order_id=paper_order_id
                        )
                        decisions.append(decision)

            self.storage.finish_scan(
                run_id,
                geoblocked=None if geoblock is None else geoblock.blocked,
            )
            return ScanReport(
                run_id=run_id,
                geoblock=geoblock,
                events_scanned=events_scanned,
                markets_scanned=markets_scanned,
                paper_orders_opened=paper_orders_opened,
                paper_orders_settled=paper_orders_settled,
                decisions=decisions,
                errors=errors,
                weather_next_status=weather_next_status,
            )
        except Exception as error:
            self.storage.finish_scan(
                run_id,
                geoblocked=None if geoblock is None else geoblock.blocked,
                status="failed",
                error=str(error),
            )
            raise

    def _settle_resolved_paper_orders(self, gateway: PolymarketGateway) -> tuple[int, list[str]]:
        settled = 0
        errors: list[str] = []
        for order in self.storage.active_paper_orders():
            try:
                if order.status.value == "RESOLVED":
                    self.storage.settle_resolved_paper_order(order.id)
                    settled += 1
                    continue
                if order.condition_id is None or not order.identity_verified:
                    raise ValueError(
                        f"paper order {order.id} has no verified condition/token identity"
                    )
                try:
                    snapshot = gateway.get_snapshot_for_token(
                        event_id=order.event_id,
                        market_id=order.market_id,
                        condition_id=order.condition_id,
                        token_id=order.token_id,
                        outcome=order.outcome,
                    )
                    self.storage.record_paper_mark(order, snapshot)
                except Exception as error:
                    errors.append(f"paper mark {order.market_id} failed: {error}")

                check = gateway.get_resolution(
                    market_id=order.market_id,
                    condition_id=order.condition_id,
                    token_id=order.token_id,
                    outcome=order.outcome,
                )
                self.storage.record_resolution_check(order.id, check)
                if check.confirmed:
                    self.storage.resolve_paper_order(order.id, check)
                    self.storage.settle_resolved_paper_order(order.id)
                    settled += 1
                else:
                    # endDate/closed/empty book only moves the position to an
                    # accounting wait state; it never books payout by itself.
                    self.storage.mark_awaiting_result(order.id, check)
            except Exception as error:
                errors.append(f"paper settlement {order.market_id} failed: {error}")
        return settled, errors

    def _scan_event(
        self,
        *,
        run_id: int,
        gateway: PolymarketGateway,
        event: EventDefinition,
        use_astra: bool,
        paper: bool,
    ) -> tuple[list[MarketDecision], list[str]]:
        errors: list[str] = []
        deterministic = deterministic_rule_audit(event)
        audit = deterministic
        if use_astra:
            try:
                astra = AstraRuleAuditor(settings=self.settings, storage=self.storage).audit(
                    event, run_id=run_id
                )
                audit = _combine_rule_audits(deterministic, astra)
            except Exception as error:
                errors.append(f"event {event.id}: Astra audit failed: {error}")
                audit = _failed_astra_audit(deterministic, str(error))

        try:
            brackets = build_brackets(event)
        except ValueError as error:
            brackets = {}
            errors.append(f"event {event.id}: {error}")

        observation_history: ObservationHistory | None = None
        observation_blockers: list[str] = []
        observation_warnings: list[str] = []
        if deterministic.interpretation.tradeable:
            try:
                observation_history = self.observations.fetch(deterministic.interpretation)
                self.storage.record_observation_history(run_id, event.id, observation_history)
                observation_blockers.extend(observation_history.blocking_reasons)
                observation_warnings.extend(observation_history.warning_reasons)
            except Exception as error:
                observation_blockers.append("OBSERVATION_SOURCE_UNAVAILABLE")
                errors.append(f"event {event.id}: observation source failed: {error}")

        rule_blockers, rule_warnings = _runtime_rule_ambiguities(
            audit.interpretation, observation_history
        )
        probabilities: dict[str, Decimal] = {}
        weathernext_probabilities: dict[str, Decimal] = {}
        analysis_rules = deterministic.interpretation
        if analysis_rules.tradeable and brackets and not observation_blockers and not rule_blockers:
            try:
                forecast = self.weather.forecast(analysis_rules)
                adjusted_members = apply_observed_max(
                    forecast.member_values,
                    None if observation_history is None else observation_history.observed_max_c,
                )
                forecast = forecast.model_copy(
                    update={
                        "unadjusted_member_values": forecast.member_values,
                        "member_values": adjusted_members,
                        "observed_floor_c": (
                            None
                            if observation_history is None
                            else observation_history.observed_max_c
                        ),
                    }
                )
                self.storage.record_weather(run_id, event.id, forecast)
                probabilities = {
                    market_id: Decimal(
                        str(round(self.weather.probability(forecast=forecast, bracket=bracket), 10))
                    )
                    for market_id, bracket in brackets.items()
                }
                try:
                    comparison = self.weathernext.snapshot_for(analysis_rules)
                    if comparison is not None:
                        self.storage.record_weathernext_snapshot(run_id, event.id, comparison)
                        weathernext_probabilities = {
                            market_id: Decimal(str(round(value, 10)))
                            for market_id, value in self.weathernext.probabilities(
                                comparison, brackets
                            ).items()
                        }
                except Exception as error:
                    errors.append(f"event {event.id}: WeatherNext comparison failed: {error}")
            except Exception as error:
                errors.append(f"event {event.id}: weather model failed: {error}")

        event_decisions: list[MarketDecision] = []
        for market in event.markets:
            try:
                snapshot = gateway.get_snapshot(event=event, market=market)
                self.storage.record_market_snapshot(run_id, snapshot)
                decision = evaluate_market(
                    snapshot=snapshot,
                    probability=probabilities.get(market.id),
                    settings=self.settings,
                    api_cost_usd=audit.astra_cost_usd,
                )
                wn_probability = weathernext_probabilities.get(market.id)
                if wn_probability is not None:
                    decision = decision.model_copy(
                        update={
                            "weathernext_probability": wn_probability,
                            "probability_delta_vs_weathernext": (
                                None
                                if decision.probability is None
                                else decision.probability - wn_probability
                            ),
                        }
                    )
                extra_reasons: list[str] = []
                warning_codes: list[str] = []
                if not deterministic.interpretation.tradeable:
                    extra_reasons.append("RULES_NOT_ANALYZABLE")
                extra_reasons.extend(rule_blockers)
                extra_reasons.extend(observation_blockers)
                warning_codes.extend(rule_warnings)
                warning_codes.extend(observation_warnings)
                if market.id not in brackets:
                    extra_reasons.append("BRACKET_NOT_PARSED")
                bracket = brackets.get(market.id)
                if (
                    bracket is not None
                    and observation_history is not None
                    and bracket_is_impossible(
                        upper=bracket.upper,
                        observed_display_max_c=observation_history.displayed_max_c,
                    )
                ):
                    extra_reasons.append("OBSERVED_MAX_EXCEEDS_BRACKET")
                if (
                    not probabilities
                    and analysis_rules.tradeable
                    and not observation_blockers
                    and not rule_blockers
                ):
                    extra_reasons.append("WEATHER_PROBABILITY_UNAVAILABLE")
                if extra_reasons or warning_codes:
                    decision = decision.model_copy(
                        update={
                            "action": DecisionAction.SKIP if extra_reasons else decision.action,
                            "reason_codes": list(
                                dict.fromkeys(decision.reason_codes + extra_reasons)
                            ),
                            "warning_codes": list(
                                dict.fromkeys(decision.warning_codes + warning_codes)
                            ),
                        }
                    )
                event_decisions.append(decision)
            except Exception as error:
                errors.append(f"event {event.id}, market {market.id}: snapshot failed: {error}")
        return event_decisions, errors

    def _finalize_event_decisions(
        self, decisions: list[MarketDecision], *, paper: bool
    ) -> tuple[list[tuple[MarketDecision, int | None]], int]:
        qualified = [
            decision for decision in decisions if decision.action == DecisionAction.PAPER_BUY
        ]
        if not qualified:
            return [(decision, None) for decision in decisions], 0

        best = max(
            qualified,
            key=lambda item: (
                item.expected_profit_usd
                if item.expected_profit_usd is not None
                else Decimal("-Infinity")
            ),
        )
        final: list[tuple[MarketDecision, int | None]] = []
        opened = 0
        for decision in decisions:
            if decision.action != DecisionAction.PAPER_BUY:
                final.append((decision, None))
                continue
            if decision.market_id != best.market_id:
                final.append(
                    (
                        decision.model_copy(
                            update={
                                "action": DecisionAction.SKIP,
                                "reason_codes": ["LOWER_RANKED_EVENT_CANDIDATE"],
                            }
                        ),
                        None,
                    )
                )
                continue
            if not paper:
                final.append(
                    (
                        decision.model_copy(
                            update={
                                "action": DecisionAction.OBSERVE,
                                "reason_codes": ["QUALIFIED_PAPER_SIGNAL"],
                            }
                        ),
                        None,
                    )
                )
                continue

            stop_reason = self._stop_reason()
            if stop_reason is not None:
                final.append(
                    (
                        decision.model_copy(
                            update={
                                "action": DecisionAction.SKIP,
                                "reason_codes": [stop_reason],
                            }
                        ),
                        None,
                    )
                )
                continue
            try:
                order_id = self.storage.open_paper_order(
                    decision,
                    idempotency_key=_paper_idempotency_key(decision),
                    max_event_risk=self.settings.max_event_risk_usd,
                    max_total_risk=self.settings.max_total_risk_usd,
                )
                final.append((decision, order_id))
                opened += 1
            except PaperRiskRejectedError as error:
                final.append(
                    (
                        decision.model_copy(
                            update={
                                "action": DecisionAction.SKIP,
                                "reason_codes": [f"PORTFOLIO_RISK_REJECTED: {error}"],
                            }
                        ),
                        None,
                    )
                )
        return final, opened

    def _stop_reason(self) -> str | None:
        local_day = datetime.now().astimezone().date()
        daily_pnl = self.storage.realized_pnl_for_day(local_day)
        if daily_pnl <= -self.settings.daily_stop_loss_usd:
            return "DAILY_STOP_LOSS_ACTIVE"
        total_pnl = Decimal(str(self.storage.portfolio_summary()["realized_pnl_usd"]))
        if total_pnl <= -self.settings.total_drawdown_stop_usd:
            return "TOTAL_DRAWDOWN_STOP_ACTIVE"
        return None


def _combine_rule_audits(deterministic: RuleAudit, astra: RuleAudit) -> RuleAudit:
    left = deterministic.interpretation
    right = astra.interpretation
    mismatches: list[str] = []
    for field in ("event_type", "observation_date", "unit"):
        if getattr(left, field) != getattr(right, field):
            mismatches.append(f"Astra disagrees with deterministic parser on {field}")
    if left.location and right.location and left.location.casefold() != right.location.casefold():
        mismatches.append("Astra disagrees with deterministic parser on location")
    ambiguity = list(dict.fromkeys(left.ambiguity_reasons + right.ambiguity_reasons + mismatches))
    interpretation = right.model_copy(
        update={
            "tradeable": left.tradeable and right.tradeable and not mismatches,
            "ambiguity_reasons": ambiguity,
            "confidence": min(left.confidence, right.confidence),
        }
    )
    return astra.model_copy(
        update={"parser": "deterministic+astra-v1", "interpretation": interpretation}
    )


def _failed_astra_audit(deterministic: RuleAudit, error: str) -> RuleAudit:
    interpretation: RuleInterpretation = deterministic.interpretation.model_copy(
        update={
            "tradeable": False,
            "ambiguity_reasons": deterministic.interpretation.ambiguity_reasons
            + [f"Astra audit unavailable: {error}"],
            "confidence": 0.0,
        }
    )
    return deterministic.model_copy(
        update={"parser": "astra-error", "interpretation": interpretation}
    )


def _paper_idempotency_key(decision: MarketDecision) -> str:
    payload = ":".join(
        (
            decision.event_id,
            decision.market_id,
            decision.asset_id,
            decision.book_hash,
            str(decision.probability),
            decision.strategy_version,
            decision.execution_model,
        )
    )
    return hashlib.sha256(payload.encode()).hexdigest()


def _runtime_rule_ambiguities(
    interpretation: RuleInterpretation,
    history: ObservationHistory | None,
) -> tuple[list[str], list[str]]:
    """Resolve only ambiguities proven by current primary-source evidence."""

    if interpretation.tradeable:
        return [], []
    blockers: list[str] = []
    warnings: list[str] = []
    for reason in interpretation.ambiguity_reasons:
        lowered = reason.casefold()
        if history is not None and ("timezone" in lowered or "source-local date" in lowered):
            warnings.append("RULE_TIMEZONE_VERIFIED_FROM_STATION_SOURCE")
            continue
        if (
            history is not None
            and history.observations
            and not history.stale
            and "weather underground" in lowered
        ):
            warnings.append("FALLBACK_SOURCE_AMBIGUOUS_PRIMARY_AVAILABLE")
            continue
        blockers.append("UNRESOLVED_RULE_AMBIGUITY")
    if not interpretation.ambiguity_reasons:
        blockers.append("ASTRA_RULES_NOT_TRADEABLE")
    return list(dict.fromkeys(blockers)), list(dict.fromkeys(warnings))
