"""Deterministic cash-risk simulation with no wallet or network access.

This module models reservations and outcomes for paper research only.  It has no
imports from the live execution path and deliberately cannot submit an order.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime
from decimal import Decimal, InvalidOperation
from enum import StrEnum
from zoneinfo import ZoneInfo


class RiskSimulationError(ValueError):
    """Raised when an event would violate the simulated cash-risk ledger."""


class SimulatedPositionState(StrEnum):
    RESERVED = "RESERVED"
    PREFLIGHT_REJECTED = "PREFLIGHT_REJECTED"
    OPEN = "OPEN"
    AMBIGUOUS = "AMBIGUOUS"
    RECONCILED_NOT_SUBMITTED = "RECONCILED_NOT_SUBMITTED"
    SETTLED = "SETTLED"


_ACTIVE_STATES = {
    SimulatedPositionState.RESERVED,
    SimulatedPositionState.OPEN,
    SimulatedPositionState.AMBIGUOUS,
}


def _money(value: Decimal | str | int, field: str) -> Decimal:
    try:
        amount = Decimal(str(value))
    except (InvalidOperation, ValueError) as error:
        raise RiskSimulationError(f"invalid {field}") from error
    if not amount.is_finite():
        raise RiskSimulationError(f"non-finite {field}")
    return amount


def _aware(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise RiskSimulationError("event timestamp must be timezone-aware")
    return value


@dataclass(frozen=True, slots=True)
class CashRiskLimits:
    daily_loss_limit_usd: Decimal = Decimal("2")
    position_risk_limit_usd: Decimal = Decimal("2")
    capital_limit_usd: Decimal = Decimal("10")
    timezone_name: str = "Asia/Almaty"

    def __post_init__(self) -> None:
        for field in (
            "daily_loss_limit_usd",
            "position_risk_limit_usd",
            "capital_limit_usd",
        ):
            amount = _money(getattr(self, field), field)
            if amount <= 0:
                raise RiskSimulationError(f"{field} must be positive")
            object.__setattr__(self, field, amount)
        try:
            ZoneInfo(self.timezone_name)
        except (KeyError, ValueError) as error:
            raise RiskSimulationError("invalid risk-ledger timezone") from error


@dataclass(frozen=True, slots=True)
class SimulatedRiskRecord:
    decision_id: str
    max_loss_usd: Decimal
    state: SimulatedPositionState
    reserved_at: datetime
    updated_at: datetime
    transitions: tuple[SimulatedPositionState, ...]
    realized_pnl_usd: Decimal | None = None
    terminal_reason: str | None = None

    def as_dict(self) -> dict[str, object]:
        return {
            "decision_id": self.decision_id,
            "max_loss_usd": str(self.max_loss_usd),
            "state": self.state.value,
            "reserved_at": self.reserved_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
            "transitions": [state.value for state in self.transitions],
            "realized_pnl_usd": (
                None if self.realized_pnl_usd is None else str(self.realized_pnl_usd)
            ),
            "terminal_reason": self.terminal_reason,
        }


class OfflineCashRiskSimulator:
    """In-memory paper ledger for loss budget, exposure, and ambiguous states."""

    def __init__(self, limits: CashRiskLimits | None = None) -> None:
        self.limits = limits or CashRiskLimits()
        self._timezone = ZoneInfo(self.limits.timezone_name)
        self._records: dict[str, SimulatedRiskRecord] = {}
        self._realized_losses: dict[str, Decimal] = {}
        self._cumulative_realized_losses = Decimal(0)
        self._latest_event_at: datetime | None = None

    def _day(self, at: datetime) -> str:
        return _aware(at).astimezone(self._timezone).date().isoformat()

    def _active_records(self) -> list[SimulatedRiskRecord]:
        return [record for record in self._records.values() if record.state in _ACTIVE_STATES]

    def _outstanding_risk(self) -> Decimal:
        return sum(
            (record.max_loss_usd for record in self._active_records()), Decimal(0)
        )

    @staticmethod
    def _check_transition_time(record: SimulatedRiskRecord, at: datetime) -> None:
        _aware(at)
        if at < record.updated_at:
            raise RiskSimulationError("transition timestamp precedes prior state")

    def _check_new_event_time(self, at: datetime) -> None:
        _aware(at)
        if self._latest_event_at is not None and at < self._latest_event_at:
            raise RiskSimulationError("event timestamp precedes the latest ledger event")

    def _move(
        self,
        record: SimulatedRiskRecord,
        target: SimulatedPositionState,
        at: datetime,
        *,
        realized_pnl_usd: Decimal | None = None,
        terminal_reason: str | None = None,
    ) -> SimulatedRiskRecord:
        self._check_transition_time(record, at)
        self._check_new_event_time(at)
        updated = replace(
            record,
            state=target,
            updated_at=at,
            transitions=(*record.transitions, target),
            realized_pnl_usd=realized_pnl_usd,
            terminal_reason=terminal_reason,
        )
        self._records[record.decision_id] = updated
        self._latest_event_at = at
        return updated

    def reserve(
        self, decision_id: str, max_loss_usd: Decimal | str | int, at: datetime
    ) -> SimulatedRiskRecord:
        """Reserve worst-case loss once; repeated decision IDs never reserve twice."""

        _aware(at)
        if not decision_id or not decision_id.strip():
            raise RiskSimulationError("decision_id is required")
        risk = _money(max_loss_usd, "max loss")
        if risk <= 0:
            raise RiskSimulationError("max loss must be positive")
        prior = self._records.get(decision_id)
        if prior is not None:
            if prior.max_loss_usd != risk:
                raise RiskSimulationError("decision_id was already reserved with different risk")
            return prior
        self._check_new_event_time(at)
        if risk > self.limits.position_risk_limit_usd:
            raise RiskSimulationError("position risk limit exceeded")
        if self._active_records():
            raise RiskSimulationError("one-position limit is already occupied")
        capital_used = self._cumulative_realized_losses + self._outstanding_risk()
        if capital_used + risk > self.limits.capital_limit_usd:
            raise RiskSimulationError("fixed capital envelope exceeded")
        day = self._day(at)
        realized_loss = self._realized_losses.get(day, Decimal(0))
        if realized_loss + self._outstanding_risk() + risk > self.limits.daily_loss_limit_usd:
            raise RiskSimulationError("daily cash-risk limit exceeded")
        record = SimulatedRiskRecord(
            decision_id=decision_id,
            max_loss_usd=risk,
            state=SimulatedPositionState.RESERVED,
            reserved_at=at,
            updated_at=at,
            transitions=(SimulatedPositionState.RESERVED,),
        )
        self._records[decision_id] = record
        self._latest_event_at = at
        return record

    def preflight_reject(
        self, decision_id: str, at: datetime, *, reason: str
    ) -> SimulatedRiskRecord:
        """Release a reservation proven rejected before simulated submission."""

        record = self.get(decision_id)
        if SimulatedPositionState.PREFLIGHT_REJECTED in record.transitions:
            if record.terminal_reason != reason:
                raise RiskSimulationError("preflight rejection replay changed reason")
            return record
        if record.state is not SimulatedPositionState.RESERVED:
            raise RiskSimulationError("preflight rejection requires a reserved position")
        if not reason:
            raise RiskSimulationError("preflight rejection reason is required")
        return self._move(
            record,
            SimulatedPositionState.PREFLIGHT_REJECTED,
            at,
            terminal_reason=reason,
        )

    def mark_open(self, decision_id: str, at: datetime) -> SimulatedRiskRecord:
        """Mark the simulated position as opened without changing reserved risk."""

        record = self.get(decision_id)
        if SimulatedPositionState.OPEN in record.transitions:
            return record
        if record.state is not SimulatedPositionState.RESERVED:
            raise RiskSimulationError("open transition requires a reserved position")
        return self._move(record, SimulatedPositionState.OPEN, at)

    def mark_ambiguous(self, decision_id: str, at: datetime) -> SimulatedRiskRecord:
        """Quarantine uncertain submission state while retaining full exposure."""

        record = self.get(decision_id)
        if SimulatedPositionState.AMBIGUOUS in record.transitions:
            return record
        if record.state not in {
            SimulatedPositionState.RESERVED,
            SimulatedPositionState.OPEN,
        }:
            raise RiskSimulationError("ambiguous transition requires unresolved exposure")
        return self._move(record, SimulatedPositionState.AMBIGUOUS, at)

    def reconcile_not_submitted(self, decision_id: str, at: datetime) -> SimulatedRiskRecord:
        """Release ambiguous risk only after explicit not-submitted reconciliation."""

        record = self.get(decision_id)
        if SimulatedPositionState.RECONCILED_NOT_SUBMITTED in record.transitions:
            return record
        if record.state is not SimulatedPositionState.AMBIGUOUS:
            raise RiskSimulationError("not-submitted reconciliation requires ambiguous state")
        return self._move(record, SimulatedPositionState.RECONCILED_NOT_SUBMITTED, at)

    def settle(
        self,
        decision_id: str,
        realized_pnl_usd: Decimal | str | int,
        at: datetime,
    ) -> SimulatedRiskRecord:
        """Settle an open/ambiguous paper position and charge losses on settlement day."""

        record = self.get(decision_id)
        pnl = _money(realized_pnl_usd, "realized P&L")
        if SimulatedPositionState.SETTLED in record.transitions:
            if record.realized_pnl_usd != pnl:
                raise RiskSimulationError("settlement replay changed realized P&L")
            return record
        if record.state not in {
            SimulatedPositionState.OPEN,
            SimulatedPositionState.AMBIGUOUS,
        }:
            raise RiskSimulationError("settlement requires open or ambiguous state")
        if -pnl > record.max_loss_usd:
            raise RiskSimulationError("realized loss exceeds reserved worst-case loss")
        self._check_transition_time(record, at)
        self._check_new_event_time(at)
        loss = max(Decimal(0), -pnl)
        day = self._day(at)
        next_loss = self._realized_losses.get(day, Decimal(0)) + loss
        if next_loss > self.limits.daily_loss_limit_usd:
            raise RiskSimulationError("settlement would exceed daily cash-loss limit")
        self._realized_losses[day] = next_loss
        self._cumulative_realized_losses += loss
        return self._move(
            record,
            SimulatedPositionState.SETTLED,
            at,
            realized_pnl_usd=pnl,
        )

    def get(self, decision_id: str) -> SimulatedRiskRecord:
        try:
            return self._records[decision_id]
        except KeyError as error:
            raise RiskSimulationError("unknown decision_id") from error

    def status(self, at: datetime) -> dict[str, object]:
        """Return the local-day budget; carryover exposure remains fully charged."""

        _aware(at)
        if self._latest_event_at is not None and at < self._latest_event_at:
            raise RiskSimulationError("status timestamp precedes the latest ledger event")
        day = self._day(at)
        realized_loss = self._realized_losses.get(day, Decimal(0))
        outstanding = self._outstanding_risk()
        used = realized_loss + outstanding
        available = max(Decimal(0), self.limits.daily_loss_limit_usd - used)
        capital_used = self._cumulative_realized_losses + outstanding
        capital_available = max(Decimal(0), self.limits.capital_limit_usd - capital_used)
        active = sorted(record.decision_id for record in self._active_records())
        quarantined = sorted(
            record.decision_id
            for record in self._active_records()
            if record.state is SimulatedPositionState.AMBIGUOUS
        )
        return {
            "mode": "OFFLINE_PAPER_RISK_SIMULATION",
            "wallet_or_network_access": False,
            "local_day": day,
            "timezone": self.limits.timezone_name,
            "daily_loss_limit_usd": str(self.limits.daily_loss_limit_usd),
            "position_risk_limit_usd": str(self.limits.position_risk_limit_usd),
            "configured_capital_limit_usd": str(self.limits.capital_limit_usd),
            "cumulative_realized_losses_usd": str(self._cumulative_realized_losses),
            "capital_used_usd": str(capital_used),
            "capital_available_usd": str(capital_available),
            "realized_losses_today_usd": str(realized_loss),
            "outstanding_worst_case_risk_usd": str(outstanding),
            "cash_risk_used_today_usd": str(used),
            "cash_risk_available_today_usd": str(available),
            "active_decision_ids": active,
            "quarantined_decision_ids": quarantined,
            "profits_restore_loss_budget": False,
            "profits_restore_capital": False,
        }

    def snapshot(self, at: datetime) -> dict[str, object]:
        """Export a deterministic JSON-compatible research snapshot."""

        return {
            "version": "offline-cash-risk-simulator-v1",
            "status": self.status(at),
            "realized_losses_by_local_day_usd": {
                day: str(amount) for day, amount in sorted(self._realized_losses.items())
            },
            "cumulative_realized_losses_usd": str(self._cumulative_realized_losses),
            "records": [
                self._records[key].as_dict() for key in sorted(self._records)
            ],
        }
