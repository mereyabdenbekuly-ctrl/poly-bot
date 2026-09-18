from __future__ import annotations

import hashlib
from collections.abc import Callable, Sequence
from concurrent.futures import Future, ThreadPoolExecutor
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, datetime, time, timedelta
from decimal import Decimal
from typing import cast
from zoneinfo import ZoneInfo

from polybot.astra import AstraRuleAuditor
from polybot.config import Settings
from polybot.forecast_engine import ForecastEngineV2
from polybot.forecast_models import (
    OPEN_METEO_ALGORITHM_VERSION,
    WEATHERNEXT_ALGORITHM_VERSION,
    ForecastAlgorithmAttemptStatus,
    ForecastAlgorithmEligibility,
    ForecastEligibilityStage,
    ForecastEventEligibility,
    RealizedForecastOutcome,
)
from polybot.forecast_v2 import (
    ECMWF_RAW_ALGORITHM_VERSION,
    FORECAST_V2_ALGORITHM_VERSION,
    ForecastV2Calibrator,
    OpenMeteoEcmwfIfsEns,
    build_v2_forecast,
)
from polybot.geoblock import fetch_geoblock_status
from polybot.models import (
    Bracket,
    DecisionAction,
    EventDefinition,
    MarketDecision,
    MarketSnapshot,
    PaperOrderStatus,
    RuleAudit,
    RuleInterpretation,
    ScanReport,
    WeatherForecast,
    WeatherNextPaperOrderTarget,
)
from polybot.observations import (
    ObservationHistory,
    StationObservationCollector,
    apply_observed_max,
    bracket_is_impossible,
    station_id_from_source_url,
)
from polybot.polymarket_gateway import PolymarketGateway
from polybot.risk import evaluate_market
from polybot.rules import build_brackets, deterministic_rule_audit
from polybot.storage import PaperRiskRejectedError, Storage
from polybot.weather import OpenMeteoEnsemble
from polybot.weathernext import WeatherNextProvider, WeatherNextSnapshot
from polybot.weathernext_paper import (
    WEATHERNEXT_PAPER_STRATEGY_VERSION,
)
from polybot.weathernext_paper import (
    paper_idempotency_key as weathernext_paper_idempotency_key,
)


@dataclass(frozen=True, slots=True)
class _ShadowResearchResult:
    outcome_completed: int
    outcome_pending: int
    extra_registered: int
    status: str


@dataclass(frozen=True, slots=True)
class _EcmwfShadowJob:
    """Immutable input captured by the primary lane for deferred ECMWF work.

    The primary lane has already validated the event, fetched observations, and
    recorded every market snapshot by the time this job is submitted.  The
    worker therefore only produces shadow forecast artifacts; it never has
    enough information to create a decision or paper order.
    """

    scan_run_id: int
    event_id: str
    audit: RuleAudit
    brackets: dict[str, Bracket]
    observation_history: ObservationHistory
    baseline_forecast: WeatherForecast


class Scanner:
    def __init__(
        self,
        *,
        settings: Settings,
        storage: Storage,
        background_research: bool = False,
    ) -> None:
        self.settings = settings
        self.storage = storage
        self.weather = OpenMeteoEnsemble(settings)
        self.observations = StationObservationCollector(settings)
        self.weathernext = WeatherNextProvider(settings)
        self.forecasts = ForecastEngineV2(storage.path, settings=settings)
        self.ecmwf = OpenMeteoEcmwfIfsEns(settings)
        self._background_research_enabled = bool(
            background_research and settings.shadow_research_enabled
        )
        self._research_executor = (
            ThreadPoolExecutor(
                max_workers=settings.shadow_research_workers,
                thread_name_prefix="polybot-shadow-research",
            )
            if self._background_research_enabled
            else None
        )
        self._research_future: Future[_ShadowResearchResult] | None = None
        self._weathernext_paper_opened = 0
        self._weathernext_paper_settled = 0
        self._weathernext_paper_decisions = 0

    def close(self, *, wait: bool = True) -> None:
        executor = self._research_executor
        if executor is None:
            return
        executor.shutdown(wait=wait, cancel_futures=not wait)
        self._research_executor = None

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
        active_event_ids: set[str] = set()
        candidate_event_ids: set[str] = set()
        shadow_jobs: list[_EcmwfShadowJob] = []
        weather_next_status = self.weathernext.status().model_dump(mode="json")
        self._weathernext_paper_opened = 0
        self._weathernext_paper_settled = 0
        self._weathernext_paper_decisions = 0

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
                wn_settled, wn_settlement_errors = self._settle_resolved_weathernext_paper_orders(
                    gateway
                )
                self._weathernext_paper_settled += wn_settled
                errors.extend(wn_settlement_errors)

                # Active paper events use a dedicated monitoring lane. They run
                # through the same rule/observation/forecast/snapshot pipeline,
                # never through the opening path, and do not consume the quota
                # reserved for new candidate events.
                active_event_ids = self.storage.active_paper_event_ids()
                monitor_lookup = getattr(self.storage, "open_paper_event_ids", None)
                monitor_event_ids = (
                    cast(Callable[[], set[str]], monitor_lookup)()
                    if callable(monitor_lookup)
                    else active_event_ids
                )
                for event_id in sorted(monitor_event_ids):
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
                        shadow_jobs=shadow_jobs,
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
                candidate_event_ids = {event.id for event in candidate_events}
                for event in candidate_events:
                    events_scanned += 1
                    event_decisions, opened, event_errors = self._process_event(
                        run_id=run_id,
                        gateway=gateway,
                        event=event,
                        use_astra=use_astra,
                        paper=paper,
                        allow_paper_open=True,
                        shadow_jobs=shadow_jobs,
                    )
                    errors.extend(event_errors)
                    markets_scanned += len(event_decisions)
                    paper_orders_opened += opened
                    decisions.extend(event_decisions)

                if not getattr(self, "_background_research_enabled", False):
                    self._run_inline_outcome_refresh(
                        gateway=gateway,
                        run_id=run_id,
                        errors=errors,
                    )

            self._schedule_shadow_research(
                parent_scan_run_id=run_id,
                query=query,
                excluded_event_ids=active_event_ids | candidate_event_ids,
                shadow_jobs=tuple(shadow_jobs),
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
                weathernext_paper_decisions=self._weathernext_paper_decisions,
                weathernext_paper_orders_opened=self._weathernext_paper_opened,
                weathernext_paper_orders_settled=self._weathernext_paper_settled,
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

    def _run_inline_outcome_refresh(
        self,
        *,
        gateway: PolymarketGateway,
        run_id: int,
        errors: list[str],
    ) -> None:
        """Compatibility path when the optional background lane is disabled."""

        forecast_store = getattr(self, "forecasts", None)
        if forecast_store is None:
            return
        for event_id in forecast_store.store.outcome_refresh_event_ids(
            correction_window_hours=self.settings.forecast_outcome_monitor_hours
        ):
            try:
                self._record_forecast_outcome(
                    gateway,
                    event_id,
                    datetime.now(UTC),
                    run_id=run_id,
                )
            except Exception as error:
                errors.append(f"event {event_id}: forecast outcome pending: {error}")

    def _schedule_shadow_research(
        self,
        *,
        parent_scan_run_id: int,
        query: str,
        excluded_event_ids: set[str],
        shadow_jobs: Sequence[_EcmwfShadowJob] = (),
    ) -> None:
        """Launch at most one no-queue research batch beside the v1 lane."""

        if not getattr(self, "_background_research_enabled", False):
            return
        future = getattr(self, "_research_future", None)
        if future is not None:
            if not future.done():
                self.storage.record_shadow_research_skip(
                    parent_scan_run_id=parent_scan_run_id,
                    reason="previous bounded research batch is still running; no work queued",
                )
                return
            # The worker persists its own terminal state. Reaping only releases
            # the single scheduler slot; research failures never fail a v1 scan.
            with suppress(Exception):
                future.result()
            self._research_future = None

        forecast_engine = getattr(self, "forecasts", None)
        outcome_event_ids = (
            []
            if forecast_engine is None
            else forecast_engine.store.outcome_refresh_event_ids(
                correction_window_hours=self.settings.forecast_outcome_monitor_hours,
                cohort_version="weather-evaluation-v1",
                limit=self.settings.shadow_outcome_batch_size,
            )
        )
        extra_requested = self.settings.shadow_extra_max_events
        shadow_jobs = tuple(shadow_jobs)
        if not outcome_event_ids and extra_requested == 0 and not shadow_jobs:
            return
        research_run_id = self.storage.start_shadow_research(
            parent_scan_run_id=parent_scan_run_id,
            outcome_requested=len(outcome_event_ids),
            extra_requested=extra_requested,
        )
        executor = self._research_executor
        if executor is None:
            self.storage.finish_shadow_research(
                research_run_id,
                status="failed",
                outcome_completed=0,
                outcome_pending=len(outcome_event_ids),
                extra_registered=0,
                error="shadow research executor is unavailable",
            )
            return
        try:
            self._research_future = executor.submit(
                self._run_shadow_research,
                research_run_id=research_run_id,
                query=query,
                excluded_event_ids=frozenset(excluded_event_ids),
                outcome_event_ids=tuple(outcome_event_ids),
                extra_requested=extra_requested,
                shadow_jobs=shadow_jobs,
            )
        except Exception as error:
            self.storage.finish_shadow_research(
                research_run_id,
                status="failed",
                outcome_completed=0,
                outcome_pending=len(outcome_event_ids),
                extra_registered=0,
                error=str(error),
            )

    def _run_shadow_research(
        self,
        *,
        research_run_id: int,
        query: str,
        excluded_event_ids: frozenset[str],
        outcome_event_ids: tuple[str, ...],
        extra_requested: int,
        shadow_jobs: tuple[_EcmwfShadowJob, ...] = (),
    ) -> _ShadowResearchResult:
        """Run bounded research without sharing the primary decision client.

        Outcome refreshes and discovery retain their existing order.  Deferred
        ECMWF/v2 jobs run only after that position-supporting work and use a
        separate forecast store instance.  They never call the market decision
        or paper-order paths.
        """

        outcome_completed = 0
        outcome_pending = 0
        extra_registered = 0
        shadow_completed = 0
        shadow_failed = 0
        shadow_errors: list[str] = []
        status = "completed"
        terminal_error: str | None = None
        observations = StationObservationCollector(self.settings)
        forecasts = ForecastEngineV2(self.storage.path, settings=self.settings)
        try:
            with PolymarketGateway() as gateway:
                for event_id in outcome_event_ids:
                    attempted_at = datetime.now(UTC)
                    try:
                        self._record_forecast_outcome(
                            gateway,
                            event_id,
                            attempted_at,
                            observation_collector=observations,
                            forecast_engine=forecasts,
                        )
                    except Exception as error:
                        outcome_pending += 1
                        forecasts.store.record_outcome_refresh_attempt(
                            event_id,
                            status="pending",
                            error=str(error),
                            attempted_at_utc=attempted_at,
                        )
                    else:
                        outcome_completed += 1
                        forecasts.store.record_outcome_refresh_attempt(
                            event_id,
                            status="recorded",
                            attempted_at_utc=attempted_at,
                        )

                if extra_requested:
                    extra_events = gateway.discover_weather_events(
                        query=query,
                        max_events=extra_requested,
                        excluded_event_ids=set(excluded_event_ids),
                    )
                    extra_registered = self.storage.record_shadow_events(
                        research_run_id,
                        extra_events,
                    )
        except Exception as error:
            terminal_error = str(error)

        if shadow_jobs:
            shadow_completed, shadow_failed, shadow_errors = self._run_ecmwf_shadow_jobs(
                shadow_jobs,
                forecast_engine=forecasts,
            )

        if shadow_failed:
            status = (
                "partial"
                if (outcome_completed or outcome_pending or extra_registered or shadow_completed)
                else "failed"
            )
            shadow_summary = "; ".join(shadow_errors[:4])
            terminal_error = (
                "; ".join(item for item in (terminal_error, shadow_summary) if item)
                or "one or more ECMWF/v2 shadow jobs failed"
            )
        elif terminal_error:
            status = (
                "partial" if outcome_completed or outcome_pending or extra_registered else "failed"
            )

        self.storage.finish_shadow_research(
            research_run_id,
            status=status,
            outcome_completed=outcome_completed,
            outcome_pending=outcome_pending,
            extra_registered=extra_registered,
            error=terminal_error,
        )
        return _ShadowResearchResult(
            outcome_completed=outcome_completed,
            outcome_pending=outcome_pending,
            extra_registered=extra_registered,
            status=status,
        )

    def _run_ecmwf_shadow_jobs(
        self,
        jobs: tuple[_EcmwfShadowJob, ...],
        *,
        forecast_engine: ForecastEngineV2,
    ) -> tuple[int, int, list[str]]:
        """Fetch/build/archive only ECMWF/v2 artifacts for deferred jobs.

        Each job is isolated so a slow or malformed provider response cannot
        prevent later jobs from being attempted.  Registry attempt rows are
        updated through ``ForecastStore.update_algorithm_attempt`` using the
        original scan run id; no v1 decision or order is touched here.
        """

        adapter = OpenMeteoEcmwfIfsEns(self.settings)
        completed = 0
        failed = 0
        errors: list[str] = []
        for job in jobs:
            try:
                snapshot = adapter.forecast_from_baseline(job.baseline_forecast)
                issued_at = datetime.now(UTC)
                profile = ForecastV2Calibrator(
                    self.settings,
                    forecast_engine.store,
                ).profile(
                    station_id=job.observation_history.station_id,
                    as_of_utc=issued_at,
                )
                result = build_v2_forecast(
                    snapshot=snapshot,
                    observations=job.observation_history,
                    brackets=job.brackets,
                    profile=profile,
                    issued_at_utc=issued_at,
                )
            except Exception as error:
                failed += 1
                message = f"event {job.event_id}: ECMWF/v2 shadow unavailable: {error}"
                errors.append(message[:500])
                self._update_shadow_attempt(
                    forecast_engine,
                    scan_run_id=job.scan_run_id,
                    event_id=job.event_id,
                    algorithm_version=ECMWF_RAW_ALGORITHM_VERSION,
                    status=ForecastAlgorithmAttemptStatus.SOURCE_UNAVAILABLE,
                    reason_codes=["SHADOW_SOURCE_UNAVAILABLE"],
                )
                self._update_shadow_attempt(
                    forecast_engine,
                    scan_run_id=job.scan_run_id,
                    event_id=job.event_id,
                    algorithm_version=FORECAST_V2_ALGORITHM_VERSION,
                    status=ForecastAlgorithmAttemptStatus.SOURCE_UNAVAILABLE,
                    reason_codes=["SHADOW_SOURCE_UNAVAILABLE"],
                )
                continue

            raw_prediction_id: int | None = None
            try:
                raw_prediction_id = forecast_engine.record_ecmwf_shadow(
                    scan_run_id=job.scan_run_id,
                    event_id=job.event_id,
                    audit=job.audit,
                    snapshot=snapshot,
                    brackets=job.brackets,
                    probabilities=result.raw_probabilities,
                    observations=job.observation_history,
                    algorithm_version=ECMWF_RAW_ALGORITHM_VERSION,
                    point_forecast_c=result.raw_point_c,
                    include_observations=False,
                    metadata={
                        "uses_station_correction": False,
                        "uses_observations": False,
                        "distribution": "empirical_50_member",
                        "shadow_lane": "bounded_background",
                    },
                    issued_at_utc=issued_at,
                )
            except Exception as error:
                failed += 1
                errors.append(
                    f"event {job.event_id}: raw ECMWF forecast archive failed: {error}"[:500]
                )
                self._update_shadow_attempt(
                    forecast_engine,
                    scan_run_id=job.scan_run_id,
                    event_id=job.event_id,
                    algorithm_version=ECMWF_RAW_ALGORITHM_VERSION,
                    status=ForecastAlgorithmAttemptStatus.PERSIST_FAILED,
                    reason_codes=["SHADOW_ARCHIVE_FAILED"],
                )
            else:
                self._update_shadow_attempt(
                    forecast_engine,
                    scan_run_id=job.scan_run_id,
                    event_id=job.event_id,
                    algorithm_version=ECMWF_RAW_ALGORITHM_VERSION,
                    status=ForecastAlgorithmAttemptStatus.PREDICTED,
                    prediction_id=raw_prediction_id,
                )

            try:
                v2_prediction_id = forecast_engine.record_ecmwf_shadow(
                    scan_run_id=job.scan_run_id,
                    event_id=job.event_id,
                    audit=job.audit,
                    snapshot=snapshot,
                    brackets=job.brackets,
                    probabilities=result.v2_probabilities,
                    observations=job.observation_history,
                    algorithm_version=FORECAST_V2_ALGORITHM_VERSION,
                    adjusted_member_max_c=result.corrected_member_max_c,
                    observed_floor_c=job.observation_history.observed_max_c,
                    point_forecast_c=result.v2_point_c,
                    metadata={
                        "uses_station_correction": result.profile.state == "fitted",
                        "uses_observations": True,
                        "station_correction": result.profile.as_metadata(),
                        "intraday_features": result.intraday_features,
                        "distribution": "bias_spread_corrected_truncated_normal_mixture",
                        "shadow_lane": "bounded_background",
                    },
                    issued_at_utc=issued_at,
                )
            except Exception as error:
                failed += 1
                errors.append(
                    f"event {job.event_id}: v2 ECMWF forecast archive failed: {error}"[:500]
                )
                self._update_shadow_attempt(
                    forecast_engine,
                    scan_run_id=job.scan_run_id,
                    event_id=job.event_id,
                    algorithm_version=FORECAST_V2_ALGORITHM_VERSION,
                    status=ForecastAlgorithmAttemptStatus.PERSIST_FAILED,
                    reason_codes=["SHADOW_ARCHIVE_FAILED"],
                )
            else:
                completed += 1
                self._update_shadow_attempt(
                    forecast_engine,
                    scan_run_id=job.scan_run_id,
                    event_id=job.event_id,
                    algorithm_version=FORECAST_V2_ALGORITHM_VERSION,
                    status=ForecastAlgorithmAttemptStatus.PREDICTED,
                    prediction_id=v2_prediction_id,
                )

        return completed, failed, errors

    @staticmethod
    def _update_shadow_attempt(
        forecast_engine: ForecastEngineV2,
        *,
        scan_run_id: int,
        event_id: str,
        algorithm_version: str,
        status: ForecastAlgorithmAttemptStatus,
        prediction_id: int | None = None,
        reason_codes: list[str] | None = None,
    ) -> None:
        """Best-effort registry update; telemetry must not fail the worker."""

        try:
            forecast_engine.store.update_algorithm_attempt(
                scan_run_id=scan_run_id,
                event_id=event_id,
                algorithm_version=algorithm_version,
                status=status,
                prediction_id=prediction_id,
                reason_codes=reason_codes,
            )
        except Exception:
            # The immutable forecast artifact (or source failure) remains the
            # source of truth even if a late registry update cannot be written.
            return

    def _process_event(
        self,
        *,
        run_id: int,
        gateway: PolymarketGateway,
        event: EventDefinition,
        use_astra: bool,
        paper: bool,
        allow_paper_open: bool,
        shadow_jobs: list[_EcmwfShadowJob] | None = None,
    ) -> tuple[list[MarketDecision], int, list[str]]:
        active_lookup = getattr(self.storage, "active_weathernext_paper_event_ids", None)
        active_weathernext_events = (
            cast(Callable[[], set[str]], active_lookup)() if callable(active_lookup) else set()
        )
        event_decisions, errors = self._scan_event(
            run_id=run_id,
            gateway=gateway,
            event=event,
            use_astra=use_astra,
            paper=paper,
            allow_weathernext_paper_open=event.id not in active_weathernext_events,
            shadow_jobs=shadow_jobs,
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
                if (
                    order.status is PaperOrderStatus.AWAITING_RESULT
                    and order.resolution_checked_at is not None
                    and datetime.now(UTC) - order.resolution_checked_at.astimezone(UTC)
                    < timedelta(seconds=self.settings.paper_resolution_recheck_seconds)
                ):
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

    def _settle_resolved_weathernext_paper_orders(
        self, gateway: PolymarketGateway
    ) -> tuple[int, list[str]]:
        """Settle only the isolated WeatherNext paper ledger.

        Resolution evidence is checked against the exact condition/token
        identity.  No row in the legacy ``paper_orders`` table is read or
        changed by this lane.
        """

        active = getattr(self.storage, "active_weathernext_paper_orders", None)
        if not callable(active):
            return 0, []
        active_orders = cast(Callable[[], Sequence[WeatherNextPaperOrderTarget]], active)()
        settled = 0
        errors: list[str] = []
        for order in active_orders:
            try:
                if order.status.value == "RESOLVED":
                    self.storage.settle_resolved_weathernext_paper_order(order.id)
                    settled += 1
                    continue
                if order.condition_id is None or not order.identity_verified:
                    raise ValueError(
                        f"WeatherNext paper order {order.id} has no verified condition/token"
                    )
                try:
                    snapshot = gateway.get_snapshot_for_token(
                        event_id=order.event_id,
                        market_id=order.market_id,
                        condition_id=order.condition_id,
                        token_id=order.token_id,
                        outcome=order.outcome,
                    )
                    self.storage.record_weathernext_paper_mark(order, snapshot)
                except Exception as error:
                    errors.append(f"WeatherNext paper mark {order.market_id} failed: {error}")
                check = gateway.get_resolution(
                    market_id=order.market_id,
                    condition_id=order.condition_id,
                    token_id=order.token_id,
                    outcome=order.outcome,
                )
                self.storage.record_weathernext_paper_resolution_check(order.id, check)
                if check.confirmed:
                    self.storage.resolve_weathernext_paper_order(order.id, check)
                    self.storage.settle_resolved_weathernext_paper_order(order.id)
                    settled += 1
                else:
                    self.storage.mark_awaiting_weathernext_paper_result(order.id, check)
            except Exception as error:
                errors.append(f"WeatherNext paper settlement {order.market_id} failed: {error}")
        return settled, errors

    def _run_weathernext_paper_strategy(
        self,
        *,
        run_id: int,
        event: EventDefinition,
        comparison: object | None,
        snapshot_reason_code: str | None = None,
        brackets: dict[str, Bracket],
        probabilities: dict[str, Decimal],
        market_snapshots: dict[str, MarketSnapshot],
        event_blockers: Sequence[str],
        event_warnings: Sequence[str],
        paper: bool,
        allow_open: bool,
    ) -> tuple[int, int, list[str]]:
        """Evaluate and persist WeatherNext-only paper decisions.

        The method is deliberately called after the v1 market snapshots are
        immutable.  It never calls ``record_decision``/``open_paper_order``;
        the two ledgers remain independent even when they inspect the same
        market book.
        """

        record = getattr(self.storage, "record_weathernext_paper_decision", None)
        open_order = getattr(self.storage, "open_weathernext_paper_order", None)
        if not callable(record) or not callable(open_order):
            # Lightweight test doubles and pre-migration databases simply do
            # not expose the optional lane; v1 remains fully functional.
            return 0, 0, []
        record_decision = cast(Callable[..., int], record)
        open_weathernext_order = cast(Callable[..., int], open_order)
        if not bool(
            getattr(
                self.settings,
                "weathernext_paper_enabled",
                getattr(self.settings, "weathernext_enabled", False),
            )
        ):
            return 0, 0, []

        errors: list[str] = []
        try:
            wn_settings = self.settings.model_copy(
                update={
                    "min_probability_edge": getattr(
                        self.settings,
                        "weathernext_paper_min_probability_edge",
                        self.settings.min_probability_edge,
                    ),
                    "min_expected_profit_usd": getattr(
                        self.settings,
                        "weathernext_paper_min_expected_profit_usd",
                        self.settings.min_expected_profit_usd,
                    ),
                    "max_event_risk_usd": getattr(
                        self.settings,
                        "weathernext_paper_max_event_risk_usd",
                        self.settings.max_event_risk_usd,
                    ),
                }
            )
        except AttributeError:
            wn_settings = self.settings

        raw: list[tuple[MarketDecision, object | None]] = []
        for market in event.markets:
            market_snapshot = market_snapshots.get(market.id)
            if market_snapshot is None:
                # A missing book cannot produce a valid decision row; retain an
                # explicit event error instead of pretending a filter rejected
                # the opportunity.
                errors.append(
                    f"event {event.id}, market {market.id}: WeatherNext decision "
                    "not recorded because market snapshot is unavailable"
                )
                continue
            probability = probabilities.get(market.id)
            try:
                decision = evaluate_market(
                    snapshot=market_snapshot,
                    probability=probability,
                    settings=wn_settings,
                    api_cost_usd=Decimal(0),
                ).model_copy(
                    update={
                        "strategy_version": WEATHERNEXT_PAPER_STRATEGY_VERSION,
                        "execution_model": "WEATHERNEXT_CROSSING_LIMIT_SHARES",
                    }
                )
                reason_codes = list(decision.reason_codes)
                reason_codes.extend(event_blockers)
                if comparison is None:
                    reason_codes.append(snapshot_reason_code or "SNAPSHOT_UNAVAILABLE")
                elif probability is None:
                    reason_codes.append("WEATHERNEXT_PROBABILITY_UNAVAILABLE")
                if market.id not in brackets:
                    reason_codes.append("BRACKET_NOT_PARSED")
                decision = decision.model_copy(
                    update={
                        "action": DecisionAction.SKIP if reason_codes else decision.action,
                        "reason_codes": list(dict.fromkeys(reason_codes)),
                        "warning_codes": list(
                            dict.fromkeys(decision.warning_codes + list(event_warnings))
                        ),
                    }
                )
                raw.append((decision, comparison))
            except Exception as error:
                errors.append(
                    f"event {event.id}, market {market.id}: WeatherNext decision failed: {error}"
                )

        qualified = [decision for decision, _ in raw if decision.action == DecisionAction.PAPER_BUY]
        best_market_id = None
        if qualified:
            best_market_id = max(
                qualified,
                key=lambda item: (
                    item.expected_profit_usd
                    if item.expected_profit_usd is not None
                    else Decimal("-Infinity")
                ),
            ).market_id

        opened = 0
        for decision, snapshot in raw:
            final = decision
            order_id: int | None = None
            if decision.action == DecisionAction.PAPER_BUY:
                if decision.market_id != best_market_id:
                    final = decision.model_copy(
                        update={
                            "action": DecisionAction.SKIP,
                            "reason_codes": list(
                                dict.fromkeys(
                                    decision.reason_codes + ["LOWER_RANKED_WEATHERNEXT_CANDIDATE"]
                                )
                            ),
                        }
                    )
                elif not paper or not allow_open:
                    final = decision.model_copy(
                        update={
                            "action": DecisionAction.OBSERVE,
                            "reason_codes": list(
                                dict.fromkeys(
                                    decision.reason_codes
                                    + [
                                        "QUALIFIED_WEATHERNEXT_PAPER_SIGNAL"
                                        if not paper
                                        else "ACTIVE_WEATHERNEXT_PAPER_EVENT_MONITOR_ONLY"
                                    ]
                                )
                            ),
                        }
                    )
                else:
                    stop_reason = self._weathernext_paper_stop_reason()
                    if stop_reason is not None:
                        final = decision.model_copy(
                            update={
                                "action": DecisionAction.SKIP,
                                "reason_codes": list(
                                    dict.fromkeys(decision.reason_codes + [stop_reason])
                                ),
                            }
                        )
                    else:
                        try:
                            order_id = open_weathernext_order(
                                decision,
                                run_id=run_id,
                                idempotency_key=weathernext_paper_idempotency_key(decision),
                                max_event_risk=getattr(
                                    self.settings,
                                    "weathernext_paper_max_event_risk_usd",
                                    self.settings.max_event_risk_usd,
                                ),
                                max_total_risk=getattr(
                                    self.settings,
                                    "weathernext_paper_max_total_risk_usd",
                                    self.settings.max_total_risk_usd,
                                ),
                            )
                            opened += 1
                        except PaperRiskRejectedError as error:
                            final = decision.model_copy(
                                update={
                                    "action": DecisionAction.SKIP,
                                    "reason_codes": list(
                                        dict.fromkeys(
                                            decision.reason_codes
                                            + [f"WEATHERNEXT_PORTFOLIO_RISK_REJECTED: {error}"]
                                        )
                                    ),
                                }
                            )
                            order_id = None
                        except Exception as error:
                            errors.append(
                                f"event {event.id}, market {decision.market_id}: "
                                f"WeatherNext paper order failed: {error}"
                            )
                            final = decision.model_copy(
                                update={
                                    "action": DecisionAction.SKIP,
                                    "reason_codes": list(
                                        dict.fromkeys(
                                            decision.reason_codes + ["WEATHERNEXT_ORDER_FAILED"]
                                        )
                                    ),
                                }
                            )
                            order_id = None
            else:
                order_id = None
            try:
                record_decision(run_id, final, snapshot=snapshot, paper_order_id=order_id)
            except Exception as error:
                errors.append(
                    f"event {event.id}, market {final.market_id}: "
                    f"WeatherNext decision archive failed: {error}"
                )
        return opened, len(raw), errors

    def _weathernext_paper_stop_reason(self) -> str | None:
        today = datetime.now().astimezone().date()
        daily_limit = getattr(
            self.settings,
            "weathernext_paper_daily_stop_loss_usd",
            self.settings.daily_stop_loss_usd,
        )
        total_limit = getattr(
            self.settings,
            "weathernext_paper_total_drawdown_stop_usd",
            self.settings.total_drawdown_stop_usd,
        )
        daily = self.storage.weathernext_paper_realized_pnl_for_day(today)
        if daily <= -daily_limit:
            return "WEATHERNEXT_DAILY_STOP_LOSS_ACTIVE"
        total = Decimal(str(self.storage.weathernext_paper_summary()["realized_pnl_usd"]))
        if total <= -total_limit:
            return "WEATHERNEXT_TOTAL_DRAWDOWN_STOP_ACTIVE"
        return None

    def _record_forecast_outcome(
        self,
        gateway: PolymarketGateway,
        event_id: str,
        recorded_at: datetime,
        *,
        run_id: int | None = None,
        observation_collector: StationObservationCollector | None = None,
        forecast_engine: ForecastEngineV2 | None = None,
    ) -> None:
        """Pair official resolution with the final saved station observations."""

        # Check the official winning bracket first. Unresolved events can stay
        # in the persistent retry set without hammering the station source every
        # five minutes before a result exists.
        winner = gateway.get_resolved_weather_winner(event_id)
        # Re-fetch even when a prior history is complete. NOAA/Synoptic can
        # revise a station record after the rule day, and the 14-day refresh
        # window is meaningful only if new revisions are actually requested.
        event = gateway.get_weather_event(event_id)
        audit = deterministic_rule_audit(event)
        if not audit.interpretation.tradeable:
            raise ValueError(f"event {event_id} rules are not analyzable")
        collector = observation_collector or self.observations
        history = collector.fetch(audit.interpretation)
        if run_id is not None:
            self.storage.record_observation_history(run_id, event_id, history)
        if (
            history is None
            or history.observed_max_c is None
            or history.displayed_max_c is None
            or not history.day_finished
        ):
            raise ValueError(
                f"event {event_id} has no final station observation history for evaluation"
            )
        recorded_at = max(recorded_at, winner.resolved_at_utc, datetime.now(UTC))
        revisions = sorted(item.revision_hash for item in history.observations)
        source_revision = hashlib.sha256(
            (winner.market_id + ":" + ":".join(revisions)).encode()
        ).hexdigest()
        engine = forecast_engine or self.forecasts
        engine.record_outcome(
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
        allow_weathernext_paper_open: bool = True,
        shadow_jobs: list[_EcmwfShadowJob] | None = None,
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
                previous_history = getattr(
                    self.storage, "latest_observation_history", lambda _event_id: None
                )(event.id)
                expected_station_id = None
                if deterministic.interpretation.resolution_source_url:
                    try:
                        expected_station_id = station_id_from_source_url(
                            deterministic.interpretation.resolution_source_url
                        )
                    except Exception:
                        expected_station_id = None
                if (
                    previous_history is not None
                    and previous_history.observation_date
                    == deterministic.interpretation.observation_date
                    and (
                        expected_station_id is None
                        or previous_history.station_id == expected_station_id
                    )
                    and previous_history.source_url
                    == deterministic.interpretation.resolution_source_url
                ):
                    # Identity/timezone evidence is safe to reuse for cohort
                    # registration only. The source outage remains blocking, so
                    # no new forecast or paper entry is produced from stale data.
                    observation_history = previous_history

        rule_blockers, rule_warnings = _runtime_rule_ambiguities(
            audit.interpretation, observation_history
        )
        probabilities: dict[str, Decimal] = {}
        weathernext_probabilities: dict[str, Decimal] = {}
        forecast = None
        comparison = None
        weathernext_snapshot_reason: str | None = None
        ecmwf_snapshot = None
        v2_result = None
        post_event_reused_snapshot = False
        prediction_ids: dict[str, int] = {}
        persist_failures: set[str] = set()
        deferred_shadow_versions: set[str] = set()
        deferred_shadow_payload: (
            tuple[
                RuleAudit,
                dict[str, Bracket],
                ObservationHistory,
                WeatherForecast,
            ]
            | None
        ) = None
        market_snapshots: dict[str, MarketSnapshot] = {}
        analysis_rules = deterministic.interpretation
        if analysis_rules.tradeable and brackets and not observation_blockers and not rule_blockers:
            try:
                forecast = self.weather.forecast(analysis_rules)
                observed_floor_c = (
                    None if observation_history is None else observation_history.observed_max_c
                )
                adjusted_members = apply_observed_max(
                    forecast.member_values,
                    _floor_in_forecast_unit(observed_floor_c, forecast.unit),
                )
                forecast = forecast.model_copy(
                    update={
                        "unadjusted_member_values": forecast.member_values,
                        "member_values": adjusted_members,
                        "observed_floor_c": (observed_floor_c),
                    }
                )
                self.storage.record_weather(run_id, event.id, forecast)
                probabilities = {
                    market_id: Decimal(
                        str(round(self.weather.probability(forecast=forecast, bracket=bracket), 10))
                    )
                    for market_id, bracket in brackets.items()
                }
                ecmwf_enabled = bool(getattr(self.settings, "ecmwf_enabled", False))
                forecast_v2_enabled = bool(getattr(self.settings, "forecast_v2_enabled", False))
                if ecmwf_enabled and forecast_v2_enabled and observation_history is not None:
                    # In the autonomous runner this is the normal path: the
                    # expensive provider request/calibration is captured as an
                    # immutable job and executed after the primary lane returns.
                    # Keep the synchronous branch only for callers that do not
                    # provide the bounded worker (for example a one-shot CLI
                    # invocation), preserving that compatibility contract.
                    if shadow_jobs is not None and getattr(
                        self, "_background_research_enabled", False
                    ):
                        deferred_shadow_payload = (
                            audit,
                            brackets,
                            observation_history,
                            forecast,
                        )
                        deferred_shadow_versions.update(
                            (ECMWF_RAW_ALGORITHM_VERSION, FORECAST_V2_ALGORITHM_VERSION)
                        )
                    else:
                        ecmwf_adapter = getattr(self, "ecmwf", None)
                        if ecmwf_adapter is not None:
                            try:
                                ecmwf_snapshot = ecmwf_adapter.forecast_from_baseline(forecast)
                                issued_at = datetime.now(UTC)
                                profile = ForecastV2Calibrator(
                                    self.settings,
                                    self.forecasts.store,
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
                    observed_floor_c = (
                        None if observation_history is None else observation_history.observed_max_c
                    )
                    adjusted_members = apply_observed_max(
                        raw_members,
                        _floor_in_forecast_unit(observed_floor_c, forecast.unit),
                    )
                    forecast = forecast.model_copy(
                        update={
                            "unadjusted_member_values": raw_members,
                            "member_values": adjusted_members,
                            "observed_floor_c": (observed_floor_c),
                        }
                    )
                    probabilities = {
                        market_id: Decimal(
                            str(
                                round(
                                    self.weather.probability(forecast=forecast, bracket=bracket),
                                    10,
                                )
                            )
                        )
                        for market_id, bracket in brackets.items()
                    }

        # WeatherNext is loaded independently of Open-Meteo.  A temporary v1
        # source failure must not suppress a valid full-ensemble snapshot or
        # erase the separate paper strategy's decision record.
        if analysis_rules.tradeable and brackets:
            try:
                paper_snapshot_for = getattr(self.weathernext, "paper_snapshot_for", None)
                if callable(paper_snapshot_for):
                    resolve_paper_snapshot = cast(
                        Callable[
                            [RuleInterpretation],
                            tuple[WeatherNextSnapshot | None, str | None],
                        ],
                        paper_snapshot_for,
                    )
                    comparison, weathernext_snapshot_reason = resolve_paper_snapshot(analysis_rules)
                else:
                    # Compatibility for read-only adapters/tests predating the
                    # timely-paper gate.  The production provider implements
                    # paper_snapshot_for and enforces the retroactive block.
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

        event_decisions: list[MarketDecision] = []
        for market in event.markets:
            try:
                snapshot = gateway.get_snapshot(event=event, market=market)
                self.storage.record_market_snapshot(run_id, snapshot)
                market_snapshots[market.id] = snapshot
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

        # WeatherNext is a completely separate paper lane.  It receives the
        # same immutable market snapshots but never writes to ``decisions`` or
        # ``paper_orders`` and therefore cannot alter v1 selection/exposure.
        wn_opened, wn_decision_count, wn_errors = self._run_weathernext_paper_strategy(
            run_id=run_id,
            event=event,
            comparison=comparison,
            snapshot_reason_code=weathernext_snapshot_reason,
            brackets=brackets,
            probabilities=weathernext_probabilities,
            market_snapshots=market_snapshots,
            event_blockers=list(
                dict.fromkeys(
                    ([] if deterministic.interpretation.tradeable else ["RULES_NOT_ANALYZABLE"])
                    + rule_blockers
                    + observation_blockers
                )
            ),
            event_warnings=list(dict.fromkeys(rule_warnings + observation_warnings)),
            paper=paper,
            allow_open=allow_weathernext_paper_open,
        )
        self._weathernext_paper_opened = getattr(self, "_weathernext_paper_opened", 0) + wn_opened
        self._weathernext_paper_decisions = (
            getattr(self, "_weathernext_paper_decisions", 0) + wn_decision_count
        )
        errors.extend(wn_errors)

        # Queue ECMWF/v2 only after every market snapshot for this run exists;
        # ForecastStore.record() links each probability to that immutable
        # snapshot set.  A partial market read therefore remains diagnostic and
        # cannot produce a misleading shadow prediction.
        if (
            deferred_shadow_payload is not None
            and shadow_jobs is not None
            and len(event_decisions) == len(event.markets)
            and event.markets
        ):
            deferred_audit, deferred_brackets, deferred_observations, deferred_forecast = (
                deferred_shadow_payload
            )
            shadow_jobs.append(
                _EcmwfShadowJob(
                    scan_run_id=run_id,
                    event_id=event.id,
                    audit=deferred_audit.model_copy(deep=True),
                    brackets={
                        market_id: bracket.model_copy(deep=True)
                        for market_id, bracket in deferred_brackets.items()
                    },
                    observation_history=deferred_observations.model_copy(deep=True),
                    baseline_forecast=deferred_forecast.model_copy(deep=True),
                )
            )
        elif deferred_shadow_payload is not None:
            deferred_shadow_versions.clear()

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
                prediction_ids[OPEN_METEO_ALGORITHM_VERSION] = forecast_engine.record_open_meteo(
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
                        {"post_event_reused_snapshot": True} if post_event_reused_snapshot else None
                    ),
                )
            except Exception as error:
                persist_failures.add(OPEN_METEO_ALGORITHM_VERSION)
                errors.append(f"event {event.id}: v1 forecast archive failed: {error}")
        if (
            forecast_engine is not None
            and comparison is not None
            and observation_history is not None
            and weathernext_probabilities
        ):
            try:
                prediction_ids[WEATHERNEXT_ALGORITHM_VERSION] = forecast_engine.record_weathernext(
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
                persist_failures.add(WEATHERNEXT_ALGORITHM_VERSION)
                errors.append(f"event {event.id}: WeatherNext forecast archive failed: {error}")
        if (
            forecast_engine is not None
            and ecmwf_snapshot is not None
            and v2_result is not None
            and observation_history is not None
        ):
            try:
                prediction_ids[ECMWF_RAW_ALGORITHM_VERSION] = forecast_engine.record_ecmwf_shadow(
                    scan_run_id=run_id,
                    event_id=event.id,
                    audit=audit,
                    snapshot=ecmwf_snapshot,
                    brackets=brackets,
                    probabilities=v2_result.raw_probabilities,
                    observations=observation_history,
                    algorithm_version=ECMWF_RAW_ALGORITHM_VERSION,
                    point_forecast_c=v2_result.raw_point_c,
                    include_observations=False,
                    metadata={
                        "uses_station_correction": False,
                        "uses_observations": False,
                        "distribution": "empirical_50_member",
                    },
                )
            except Exception as error:
                persist_failures.add(ECMWF_RAW_ALGORITHM_VERSION)
                errors.append(f"event {event.id}: raw ECMWF forecast archive failed: {error}")
            try:
                prediction_ids[FORECAST_V2_ALGORITHM_VERSION] = forecast_engine.record_ecmwf_shadow(
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
                persist_failures.add(FORECAST_V2_ALGORITHM_VERSION)
                errors.append(f"event {event.id}: v2 ECMWF forecast archive failed: {error}")
        self._register_evaluation_event(
            run_id=run_id,
            event=event,
            audit=audit,
            deterministic=deterministic,
            brackets=brackets,
            observation_history=observation_history,
            observation_blockers=observation_blockers,
            rule_blockers=rule_blockers,
            rule_warnings=rule_warnings,
            observation_warnings=observation_warnings,
            baseline_model=("open-meteo-ensemble" if forecast is None else forecast.provider),
            prediction_ids=prediction_ids,
            persist_failures=persist_failures,
            deferred_shadow_versions=deferred_shadow_versions,
            errors=errors,
        )
        return event_decisions, errors

    def _register_evaluation_event(
        self,
        *,
        run_id: int,
        event: EventDefinition,
        audit: RuleAudit,
        deterministic: RuleAudit,
        brackets: dict[str, Bracket],
        observation_history: ObservationHistory | None,
        observation_blockers: list[str],
        rule_blockers: list[str],
        rule_warnings: list[str],
        observation_warnings: list[str],
        baseline_model: str,
        prediction_ids: dict[str, int],
        persist_failures: set[str],
        errors: list[str],
        deferred_shadow_versions: set[str] | None = None,
    ) -> None:
        forecast_engine = getattr(self, "forecasts", None)
        if forecast_engine is None:
            return
        rules = deterministic.interpretation
        readiness_blockers = list(dict.fromkeys(rule_blockers + observation_blockers))
        cohort_blockers: list[str] = []
        if not rules.tradeable:
            cohort_blockers.append("RULES_NOT_ANALYZABLE")
        if not brackets:
            cohort_blockers.append("BRACKET_NOT_PARSED")
        if observation_history is None:
            cohort_blockers.append("OBSERVATION_IDENTITY_UNAVAILABLE")
        if rules.unit != "C" or rules.precision_decimal_places != 0:
            cohort_blockers.append("FORECAST_RULE_FORMAT_UNSUPPORTED")
        blockers = list(dict.fromkeys(cohort_blockers + readiness_blockers))
        warnings = list(dict.fromkeys(rule_warnings + observation_warnings))
        deferred_shadow_versions = deferred_shadow_versions or set()
        eligible = not cohort_blockers
        if not eligible:
            stage = ForecastEligibilityStage.INELIGIBLE
        elif prediction_ids:
            stage = ForecastEligibilityStage.FORECAST_READY
        else:
            stage = ForecastEligibilityStage.FORECAST_FAILED

        # Cohort membership is a property of the opportunity and its rule/day
        # identity, not of whether a weather provider happened to answer or a
        # trade was selected.  Provider/observation failures stay visible in
        # block_reasons and algorithm attempt rows instead of disappearing from
        # the coverage denominator.
        def attempt_status(version: str) -> ForecastAlgorithmAttemptStatus:
            if not eligible:
                return ForecastAlgorithmAttemptStatus.VALIDATION_FAILED
            if version in prediction_ids:
                return ForecastAlgorithmAttemptStatus.PREDICTED
            if version in persist_failures:
                return ForecastAlgorithmAttemptStatus.PERSIST_FAILED
            if readiness_blockers:
                return ForecastAlgorithmAttemptStatus.VALIDATION_FAILED
            return ForecastAlgorithmAttemptStatus.SOURCE_UNAVAILABLE

        def attempt_reasons(version: str) -> list[str]:
            if not eligible:
                return blockers
            if version in persist_failures:
                return ["FORECAST_ARCHIVE_FAILED"]
            if version in deferred_shadow_versions:
                return ["SHADOW_DEFERRED"]
            if readiness_blockers:
                return readiness_blockers
            if version not in prediction_ids:
                return ["FORECAST_SOURCE_UNAVAILABLE"]
            return []

        expected: list[ForecastAlgorithmEligibility] = [
            ForecastAlgorithmEligibility(
                source="open-meteo",
                model=baseline_model,
                algorithm_version=OPEN_METEO_ALGORITHM_VERSION,
                expected=True,
                status=attempt_status(OPEN_METEO_ALGORITHM_VERSION),
                prediction_id=prediction_ids.get(OPEN_METEO_ALGORITHM_VERSION),
                reason_codes=attempt_reasons(OPEN_METEO_ALGORITHM_VERSION),
            )
        ]
        if self.settings.ecmwf_enabled and self.settings.forecast_v2_enabled:
            for version in (ECMWF_RAW_ALGORITHM_VERSION, FORECAST_V2_ALGORITHM_VERSION):
                expected.append(
                    ForecastAlgorithmEligibility(
                        source="open-meteo-ecmwf",
                        model="ECMWF IFS ENS 0.25° daily max via Open-Meteo",
                        algorithm_version=version,
                        expected=True,
                        status=attempt_status(version),
                        prediction_id=prediction_ids.get(version),
                        reason_codes=attempt_reasons(version),
                    )
                )
        weathernext_available = self.weathernext.status().state == "snapshot_available"
        expected.append(
            ForecastAlgorithmEligibility(
                source="weathernext3",
                model="WeatherNext 3 full ensemble",
                algorithm_version=WEATHERNEXT_ALGORITHM_VERSION,
                expected=weathernext_available,
                status=(
                    attempt_status(WEATHERNEXT_ALGORITHM_VERSION)
                    if weathernext_available
                    else ForecastAlgorithmAttemptStatus.NOT_CONFIGURED
                ),
                prediction_id=prediction_ids.get(WEATHERNEXT_ALGORITHM_VERSION),
                reason_codes=(
                    attempt_reasons(WEATHERNEXT_ALGORITHM_VERSION)
                    if weathernext_available
                    else ["WEATHERNEXT_ACCESS_PENDING"]
                ),
            )
        )
        station_id = None if observation_history is None else observation_history.station_id
        observation_date = rules.observation_date
        station_timezone = (
            None if observation_history is None else observation_history.station_timezone
        )
        day_start = day_end = None
        if observation_history is not None and observation_date is not None:
            local_start = datetime.combine(
                observation_date,
                time.min,
                tzinfo=ZoneInfo(observation_history.station_timezone),
            )
            day_start = local_start.astimezone(UTC)
            day_end = (local_start + timedelta(days=1)).astimezone(UTC)
        try:
            forecast_engine.register_evaluation_event(
                ForecastEventEligibility(
                    scan_run_id=run_id,
                    event_id=event.id,
                    considered_at_utc=datetime.now(UTC),
                    event_title=event.title,
                    event_slug=event.slug,
                    market_count=len(event.markets),
                    rules_hash=audit.rules_hash,
                    rule_parser=audit.parser,
                    station_id=station_id,
                    observation_date=observation_date,
                    station_timezone=station_timezone,
                    rule_day_start_utc=day_start,
                    rule_day_end_utc=day_end,
                    display_unit=rules.unit,
                    precision_decimal_places=rules.precision_decimal_places,
                    rounding_rule="displayed_temperature_c=floor(raw_temperature_c+0.5)",
                    eligible=eligible,
                    stage=stage,
                    block_reasons=blockers,
                    warning_reasons=warnings,
                    algorithms=expected,
                )
            )
        except Exception as error:
            errors.append(f"event {event.id}: evaluation registry failed: {error}")

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
    # Astra is an advisory audit layer. An unavailable or over-budget Astra call
    # must not veto a clean deterministic interpretation; the event stays
    # tradeable on the deterministic parser alone and the note is informational.
    interpretation: RuleInterpretation = deterministic.interpretation.model_copy(
        update={
            "tradeable": deterministic.interpretation.tradeable,
            "ambiguity_reasons": list(deterministic.interpretation.ambiguity_reasons),
            "confidence": min(deterministic.interpretation.confidence, Decimal("0.5")),
        }
    )
    return deterministic.model_copy(
        update={"parser": "astra-error", "interpretation": interpretation}
    )


def _floor_in_forecast_unit(observed_floor_c: Decimal | None, unit: str) -> Decimal | None:
    if observed_floor_c is None or unit == "C":
        return observed_floor_c
    if unit == "F":
        return observed_floor_c * Decimal(9) / Decimal(5) + Decimal(32)
    raise ValueError(f"unsupported forecast unit {unit!r}")


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
        if history is not None and "weather underground" in lowered:
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
