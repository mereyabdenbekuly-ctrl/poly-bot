from __future__ import annotations

from datetime import datetime, timedelta
from decimal import Decimal
from zoneinfo import ZoneInfo

import pytest

from polybot.offline_risk_sim import (
    OfflineCashRiskSimulator,
    RiskSimulationError,
    SimulatedPositionState,
)

ALMATY = ZoneInfo("Asia/Almaty")


def at(hour: int, minute: int = 0, *, day: int = 17) -> datetime:
    return datetime(2026, 9, day, hour, minute, tzinfo=ALMATY)


def test_one_position_and_cash_risk_are_reserved_once() -> None:
    sim = OfflineCashRiskSimulator()
    first = sim.reserve("decision-1", "1.50", at(10))
    replay = sim.reserve("decision-1", Decimal("1.50"), at(10, 1))
    assert replay == first
    assert sim.status(at(10, 2))["outstanding_worst_case_risk_usd"] == "1.50"
    with pytest.raises(RiskSimulationError, match="one-position"):
        sim.reserve("decision-2", "0.25", at(10, 2))


def test_proven_preflight_rejection_releases_reserved_risk() -> None:
    sim = OfflineCashRiskSimulator()
    sim.reserve("rejected", "2", at(10))
    rejected = sim.preflight_reject("rejected", at(10, 1), reason="BOOK_CHANGED")
    replay = sim.preflight_reject("rejected", at(10, 2), reason="BOOK_CHANGED")
    assert rejected.state is SimulatedPositionState.PREFLIGHT_REJECTED
    assert replay == rejected
    assert sim.status(at(10, 3))["cash_risk_available_today_usd"] == "2"
    assert sim.status(at(10, 3))["capital_available_usd"] == "10"
    assert sim.reserve("replacement", "2", at(10, 4)).state is SimulatedPositionState.RESERVED


def test_realized_losses_plus_exposure_obey_daily_limit_and_profit_does_not_reset_it() -> None:
    sim = OfflineCashRiskSimulator()
    sim.reserve("loss", "1.25", at(9))
    sim.mark_open("loss", at(9, 1))
    sim.settle("loss", "-1.25", at(9, 2))

    sim.reserve("profit", "0.75", at(10))
    sim.mark_open("profit", at(10, 1))
    sim.settle("profit", "10", at(10, 2))

    status = sim.status(at(10, 3))
    assert status["realized_losses_today_usd"] == "1.25"
    assert status["cash_risk_available_today_usd"] == "0.75"
    assert status["profits_restore_loss_budget"] is False
    with pytest.raises(RiskSimulationError, match="daily cash-risk"):
        sim.reserve("too-large", "0.76", at(10, 4))
    sim.reserve("fits", "0.75", at(10, 5))


def test_local_day_rollover_resets_losses_but_carries_unresolved_exposure() -> None:
    sim = OfflineCashRiskSimulator()
    sim.reserve("overnight", "1.50", at(23, 59))
    sim.mark_open("overnight", at(23, 59) + timedelta(seconds=10))

    next_day = at(0, 1, day=18)
    before_settlement = sim.status(next_day)
    assert before_settlement["local_day"] == "2026-09-18"
    assert before_settlement["realized_losses_today_usd"] == "0"
    assert before_settlement["outstanding_worst_case_risk_usd"] == "1.50"
    assert before_settlement["cash_risk_available_today_usd"] == "0.50"

    sim.settle("overnight", "-1", at(0, 2, day=18))
    after_settlement = sim.status(at(0, 3, day=18))
    assert after_settlement["realized_losses_today_usd"] == "1"
    assert after_settlement["outstanding_worst_case_risk_usd"] == "0"
    assert after_settlement["cash_risk_available_today_usd"] == "1"


def test_ambiguous_submission_is_quarantined_until_explicit_reconciliation() -> None:
    sim = OfflineCashRiskSimulator()
    sim.reserve("unknown", "1", at(11))
    sim.mark_ambiguous("unknown", at(11, 1))
    status = sim.status(at(11, 2))
    assert status["quarantined_decision_ids"] == ["unknown"]
    assert status["outstanding_worst_case_risk_usd"] == "1"
    with pytest.raises(RiskSimulationError, match="one-position"):
        sim.reserve("unsafe-retry", "1", at(11, 3))

    reconciled = sim.reconcile_not_submitted("unknown", at(11, 4))
    assert reconciled.state is SimulatedPositionState.RECONCILED_NOT_SUBMITTED
    assert sim.status(at(11, 5))["outstanding_worst_case_risk_usd"] == "0"
    sim.reserve("safe-next", "1", at(11, 6))


def test_transitions_are_idempotent_and_never_regress_after_retries() -> None:
    sim = OfflineCashRiskSimulator()
    sim.reserve("idempotent", "1", at(12))
    opened = sim.mark_open("idempotent", at(12, 1))
    assert sim.mark_open("idempotent", at(12, 2)) == opened
    ambiguous = sim.mark_ambiguous("idempotent", at(12, 3))
    assert sim.mark_open("idempotent", at(12, 4)) == ambiguous
    assert sim.mark_ambiguous("idempotent", at(12, 5)) == ambiguous
    settled = sim.settle("idempotent", "-0.40", at(12, 6))
    assert sim.mark_ambiguous("idempotent", at(12, 7)) == settled
    assert sim.settle("idempotent", "-0.40", at(12, 8)) == settled
    with pytest.raises(RiskSimulationError, match="changed realized P&L"):
        sim.settle("idempotent", "-0.41", at(12, 9))


def test_fixed_capital_is_depleted_by_losses_and_not_refilled_by_profit() -> None:
    sim = OfflineCashRiskSimulator()
    for day in range(17, 22):
        sim.reserve(f"loss-{day}", "2", at(9, day=day))
        sim.mark_open(f"loss-{day}", at(9, 1, day=day))
        sim.settle(f"loss-{day}", "-2", at(9, 2, day=day))

    status = sim.status(at(9, 3, day=21))
    assert status["cumulative_realized_losses_usd"] == "10"
    assert status["capital_used_usd"] == "10"
    assert status["capital_available_usd"] == "0"
    assert status["profits_restore_capital"] is False
    with pytest.raises(RiskSimulationError, match="fixed capital envelope"):
        sim.reserve("not-funded-by-profit", "0.01", at(9, 4, day=21))


def test_profit_does_not_restore_capital_consumed_by_prior_losses() -> None:
    sim = OfflineCashRiskSimulator()
    for day in range(17, 21):
        sim.reserve(f"loss-{day}", "2", at(9, day=day))
        sim.mark_open(f"loss-{day}", at(9, 1, day=day))
        sim.settle(f"loss-{day}", "-2", at(9, 2, day=day))
    sim.reserve("partial-loss", "1", at(9, 3, day=21))
    sim.mark_open("partial-loss", at(9, 4, day=21))
    sim.settle("partial-loss", "-1", at(9, 5, day=21))
    sim.reserve("profit", "0.5", at(9, 3, day=22))
    sim.mark_open("profit", at(9, 4, day=22))
    sim.settle("profit", "100", at(9, 5, day=22))
    assert sim.status(at(9, 6, day=22))["capital_available_usd"] == "1"
    with pytest.raises(RiskSimulationError, match="fixed capital envelope"):
        sim.reserve("over-capital", "1.01", at(9, 7, day=22))


@pytest.mark.parametrize("risk", ["0", "-1", "NaN", "Infinity", "broken"])
def test_invalid_risk_is_rejected(risk: str) -> None:
    sim = OfflineCashRiskSimulator()
    with pytest.raises(RiskSimulationError):
        sim.reserve("bad", risk, at(14))


def test_naive_time_and_loss_beyond_reservation_are_rejected() -> None:
    sim = OfflineCashRiskSimulator()
    with pytest.raises(RiskSimulationError, match="timezone-aware"):
        sim.reserve("naive", "1", datetime(2026, 9, 17, 14))
    sim.reserve("bounded", "1", at(14))
    sim.mark_open("bounded", at(14, 1))
    with pytest.raises(RiskSimulationError, match="reserved worst-case"):
        sim.settle("bounded", "-1.01", at(14, 2))


def test_new_events_and_status_cannot_move_ledger_time_backwards() -> None:
    sim = OfflineCashRiskSimulator()
    sim.reserve("first", "1", at(16))
    sim.preflight_reject("first", at(16, 2), reason="NO_FILL")
    with pytest.raises(RiskSimulationError, match="latest ledger event"):
        sim.reserve("out-of-order", "1", at(16, 1))
    with pytest.raises(RiskSimulationError, match="status timestamp"):
        sim.status(at(16, 1))


def test_snapshot_is_json_compatible_and_explicitly_offline() -> None:
    sim = OfflineCashRiskSimulator()
    sim.reserve("paper-only", "0.50", at(15))
    exported = sim.snapshot(at(15, 1))
    assert exported["version"] == "offline-cash-risk-simulator-v1"
    status = exported["status"]
    records = exported["records"]
    assert isinstance(status, dict)
    assert isinstance(records, list)
    assert isinstance(records[0], dict)
    assert status["mode"] == "OFFLINE_PAPER_RISK_SIMULATION"
    assert status["wallet_or_network_access"] is False
    assert records[0]["max_loss_usd"] == "0.50"
