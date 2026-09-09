from __future__ import annotations

import hashlib
from datetime import UTC, datetime
from decimal import Decimal

from polybot.astra import AstraRuleAuditor
from polybot.config import Settings
from polybot.forecast_engine import ForecastEngineV2
from polybot.forecast_models import RealizedForecastOutcome
from polybot.forecast_v2 import (
    ECMWF_RAW_ALGORITHM_VERSION,
    FORECAST_V2_ALGORITHM_VERSION,
    ForecastV2Calibrator,
    OpenMeteoEcmwfIfsEns,
    build_v2_forecast,
)
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
        self.forecasts = ForecastEngineV2(storage.path, settings=settings)
        self.ecmwf = OpenMeteoEcmwfIfsEns(settings)

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

                # Active paper events use a dedicated monitoring lane. They run
                # through the same rule/observation/forecast/snapshot pipeline,
                # never through the opening path, and do not consume the quota
                # reserved for new candidate events.
                active_event_ids = self.storage.active_paper_event_ids()
                for event_id in sorted(active_event_ids):
                    try:
                        event = gateway.get_weather_event(event_id)
                    except Exception as error:
                        errors.append(f"active paper event {event_id}: load failed: {error}")
                        continue
                    events_scanned += 1
                    event_decisions, opened, event_errors = self._process_event(
                        run_id=run_id,
                        gateway=gateway,
                        event=event,
                        use_astra=use_astra,
                        paper=paper,
                        allow_paper_open=False,
                    )
                    errors.extend(event_errors)
                    markets_scanned += len(event_decisions)
                    paper_orders_opened += opened
                    decisions.extend(event_decisions)

                candidate_events = gateway.discover_weather_events(
                    query=query,
                    max_events=max_events,
                    excluded_event_ids=active_event_ids,
                )
                for event in candidate_events:
                    events_scanned += 1
                    event_decisions, opened, event_errors = self._process_event(
                        run_id=run_id,
                        gateway=gateway,
                        event=event,
                        use_astra=use_astra,
                        paper=paper,
                        allow_paper_open=True,
                    )
                    errors.extend(event_errors)
                    markets_scanned += len(event_decisions)
                    paper_orders_opened += opened
                    decisions.extend(event_decisions)

                forecast_store = getattr(self, "forecasts", None)
                if forecast_store is not None:
                    for event_id in forecast_store.store.outcome_refresh_event_ids(
                        correction_window_hours=self.settings.forecast_outcome_monitor_hours
                    ):
                        try:
                            self._record_forecast_outcome(
                                gateway, event_id, datetime.now(UTC), run_id=run_id
                            )
                        except Exception as error:
                            errors.append(
                                f"event {event_id}: forecast outcome pending: {error}"
                            )

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

    def _process_event(
        self,
        *,
        run_id: int,
        gateway: PolymarketGateway,
        event: EventDefinition,
        use_astra: bool,
        paper: bool,
        allow_paper_open: bool,
    ) -> tuple[list[MarketDecision], int, list[str]]:
        event_decisions, errors = self._scan_event(
            run_id=run_id,
            gateway=gateway,
            event=event,
            use_astra=use_astra,
            paper=paper,
        )
        if allow_paper_open:
            final_decisions, opened = self._finalize_event_decisions(event_decisions, paper=paper)
        else:
            final_decisions = [
                (_monitor_only_decision(decision), None) for decision in event_decisions
            ]
            opened = 0

        recorded: list[MarketDecision] = []
        for decision, paper_order_id in final_decisions:
            self.storage.record_decision(run_id, decision, paper_order_id=paper_order_id)
            recorded.append(decision)
        return recorded, opened, errors

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

    def _record_forecast_outcome(
        self,
        gateway: PolymarketGateway,
        event_id: str,
        recorded_at: datetime,
        *,
        run_id: int | None = None,
    ) -> None:
        """Pair official resolution with the final saved station observations."""

        history = self.storage.latest_observation_history(event_id)
        if (
            history is None
            or history.observed_max_c is None
            or history.displayed_max_c is None
            or not history.day_finished
        ):
            event = gateway.get_weather_event(event_id)
            audit = deterministic_rule_audit(event)
            if not audit.interpretation.tradeable:
                raise ValueError(f"event {event_id} rules are not analyzable")
            refreshed = self.observations.fetch(audit.interpretation)
            if run_id is not None:
                self.storage.record_observation_history(run_id, event_id, refreshed)
            history = refreshed
            if (
                history.observed_max_c is None
                or history.displayed_max_c is None
                or not history.day_finished
            ):
                raise ValueError(
                    f"event {event_id} has no final station observation history for evaluation"
                )
        winner = gateway.get_resolved_weather_winner(event_id)
        recorded_at = max(recorded_at, winner.resolved_at_utc, datetime.now(UTC))
        revisions = sorted(item.revision_hash for item in history.observations)
        source_revision = hashlib.sha256(
            (winner.market_id + ":" + ":".join(revisions)).encode()
        ).hexdigest()
        self.forecasts.record_outcome(
            RealizedForecastOutcome(
                event_id=event_id,
                station_id=history.station_id,
                observation_date=history.observation_date,
                actual_max_c=history.observed_max_c,
                displayed_max=history.displayed_max_c,
                winning_market_id=winner.market_id,
                winning_condition_id=winner.condition_id,
                winning_label=winner.outcome_label,
                resolution_source=winner.resolution_source,
                source_revision=source_revision,
                resolved_at_utc=winner.resolved_at_utc,
                recorded_at_utc=recorded_at,
                evidence={
                    "resolved_by": winner.resolved_by,
                    "observation_revision_hashes": revisions,
                    "observation_fetch_time": history.fetched_at_utc.isoformat(),
                },
            )
        )

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
        forecast = None
        comparison = None
        ecmwf_snapshot = None
        v2_result = None
        post_event_reused_snapshot = False
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

                ecmwf_adapter = getattr(self, "ecmwf", None)
                if (
                    self.settings.ecmwf_enabled
                    and self.settings.forecast_v2_enabled
                    and ecmwf_adapter is not None
                    and observation_history is not None
                ):
                    try:
                        ecmwf_snapshot = ecmwf_adapter.forecast_from_baseline(forecast)
                        issued_at = datetime.now(UTC)
                        profile = ForecastV2Calibrator(
                            self.settings, self.forecasts.store
                        ).profile(
                            station_id=observation_history.station_id,
                            as_of_utc=issued_at,
                        )
                        v2_result = build_v2_forecast(
                            snapshot=ecmwf_snapshot,
                            observations=observation_history,
                            brackets=brackets,
                            profile=profile,
                            issued_at_utc=issued_at,
                        )
                    except Exception as error:
                        errors.append(f"event {event.id}: ECMWF/v2 shadow failed: {error}")
            except Exception as error:
                # Once a station-local day has passed, an ensemble endpoint
                # may no longer serve that date. Reuse the last immutable model
                # snapshot with fresh observations for monitoring/revaluation;
                # this is explicitly marked as post-event reanalysis and never
                # enables a new paper entry.
                previous = self.storage.latest_weather_forecast(event.id)
                if previous is None:
                    errors.append(f"event {event.id}: weather model failed: {error}")
                else:
                    errors.append(
                        f"event {event.id}: weather model unavailable; "
                        "reused last forecast snapshot for monitoring"
                    )
                    post_event_reused_snapshot = True
                    forecast = previous
                    raw_members = forecast.unadjusted_member_values or forecast.member_values
                    adjusted_members = apply_observed_max(
                        raw_members,
                        None
                        if observation_history is None
                        else observation_history.observed_max_c,
                    )
                    forecast = forecast.model_copy(
                        update={
                            "unadjusted_member_values": raw_members,
                            "member_values": adjusted_members,
                            "observed_floor_c": (
                                None
                                if observation_history is None
                                else observation_history.observed_max_c
                            ),
                        }
                    )
                    probabilities = {
                        market_id: Decimal(
                            str(
                                round(
                                    self.weather.probability(
                                        forecast=forecast, bracket=bracket
                                    ),
                                    10,
                                )
                            )
                        )
                        for market_id, bracket in brackets.items()
                    }

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

        # Shadow forecasts are persisted only after every same-run market
        # snapshot exists. Failures are diagnostic and never alter v1 actions.
        forecast_engine = getattr(self, "forecasts", None)
        if (
            forecast_engine is not None
            and forecast is not None
            and observation_history is not None
            and probabilities
        ):
            try:
                forecast_engine.record_open_meteo(
                    scan_run_id=run_id,
                    event_id=event.id,
                    audit=audit,
                    forecast=forecast,
                    brackets=brackets,
                    probabilities=probabilities,
                    observations=observation_history,
                    source_uri=self.settings.weather_ensemble_url,
                    weather_error_sigma_c=Decimal(str(self.settings.weather_error_sigma_c)),
                    metadata=(
                        {"post_event_reused_snapshot": True}
                        if post_event_reused_snapshot
                        else None
                    ),
                )
            except Exception as error:
                errors.append(f"event {event.id}: v1 forecast archive failed: {error}")
        if (
            forecast_engine is not None
            and comparison is not None
            and observation_history is not None
            and weathernext_probabilities
        ):
            try:
                forecast_engine.record_weathernext(
                    scan_run_id=run_id,
                    event_id=event.id,
                    audit=audit,
                    snapshot=comparison,
                    brackets=brackets,
                    probabilities=weathernext_probabilities,
                    observations=observation_history,
                    model_version="weathernext-3",
                )
            except Exception as error:
                errors.append(f"event {event.id}: WeatherNext forecast archive failed: {error}")
        if (
            forecast_engine is not None
            and ecmwf_snapshot is not None
            and v2_result is not None
            and observation_history is not None
        ):
            try:
                forecast_engine.record_ecmwf_shadow(
                    scan_run_id=run_id,
                    event_id=event.id,
                    audit=audit,
                    snapshot=ecmwf_snapshot,
                    brackets=brackets,
                    probabilities=v2_result.raw_probabilities,
                    observations=observation_history,
                    algorithm_version=ECMWF_RAW_ALGORITHM_VERSION,
                    point_forecast_c=v2_result.raw_point_c,
                    metadata={
                        "uses_station_correction": False,
                        "uses_observations": False,
                        "distribution": "empirical_50_member",
                    },
                )
                forecast_engine.record_ecmwf_shadow(
                    scan_run_id=run_id,
                    event_id=event.id,
                    audit=audit,
                    snapshot=ecmwf_snapshot,
                    brackets=brackets,
                    probabilities=v2_result.v2_probabilities,
                    observations=observation_history,
                    algorithm_version=FORECAST_V2_ALGORITHM_VERSION,
                    adjusted_member_max_c=v2_result.corrected_member_max_c,
                    observed_floor_c=observation_history.observed_max_c,
                    point_forecast_c=v2_result.v2_point_c,
                    metadata={
                        "uses_station_correction": v2_result.profile.state == "fitted",
                        "uses_observations": True,
                        "station_correction": v2_result.profile.as_metadata(),
                        "intraday_features": v2_result.intraday_features,
                        "distribution": "bias_spread_corrected_truncated_normal_mixture",
                    },
                )
            except Exception as error:
                errors.append(f"event {event.id}: ECMWF forecast archive failed: {error}")
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


def _monitor_only_decision(decision: MarketDecision) -> MarketDecision:
    """Prevent an active paper event from producing another opening action."""

    return decision.model_copy(
        update={
            "action": (
                DecisionAction.OBSERVE
                if decision.action == DecisionAction.PAPER_BUY
                else decision.action
            ),
            "reason_codes": list(
                dict.fromkeys(decision.reason_codes + ["ACTIVE_PAPER_EVENT_MONITOR_ONLY"])
            ),
        }
    )


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
            and "weather underground" in lowered
        ):
            warnings.append(
                "FALLBACK_SOURCE_AMBIGUOUS_PRIMARY_AVAILABLE"
                if history.observations and not history.stale
                else "FALLBACK_SOURCE_AMBIGUOUS_PRIMARY_CONFIGURED"
            )
            continue
        blockers.append("UNRESOLVED_RULE_AMBIGUITY")
    if not interpretation.ambiguity_reasons:
        blockers.append("ASTRA_RULES_NOT_TRADEABLE")
    return list(dict.fromkeys(blockers)), list(dict.fromkeys(warnings))
