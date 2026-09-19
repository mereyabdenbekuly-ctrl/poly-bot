from __future__ import annotations

from copy import deepcopy
from typing import Any

import pytest

from polybot.strategy_audit import build_strategy_audit


def snapshot() -> dict[str, Any]:
    return {
        "generated_at": "2026-09-17T17:09:56+00:00",
        "portfolio": {
            "api_spend_usd": "0.50",
            "realized_pnl_usd": "1.60",
            "gross_trade_pnl_usd": "1.90",
            "allocated_settled_order_api_cost_usd": "0.30",
            "net_project_pnl_after_api_usd": "1.40",
            "closed_orders": 2,
        },
        "forecast_diagnostics": {
            "generated_at_utc": "2026-09-17T17:09:50+00:00",
            "warnings": [],
            "trades": [
                {
                    "paper_order_id": 1,
                    "strategy_version": "v0",
                    "realized_pnl_usd": "2.00",
                    "api_cost_usd": "0.10",
                    "trade_correct": True,
                },
                {
                    "paper_order_id": 2,
                    "strategy_version": "v1",
                    "realized_pnl_usd": "-0.40",
                    "api_cost_usd": "0.20",
                    "trade_correct": False,
                },
            ]
        },
    }


def comparison() -> dict[str, Any]:
    return {"generated_at_utc": "2026-09-17T17:09:54+00:00"}


def test_audit_separates_versions_and_does_not_double_subtract_api() -> None:
    data = snapshot()
    before = deepcopy(data)
    report = build_strategy_audit(data, comparison())
    assert data == before
    assert report["strategies"]["v0"]["realized_after_allocated_api_usd"] == "2.00"
    assert report["strategies"]["v1"]["realized_after_allocated_api_usd"] == "-0.40"
    assert report["strategies"]["v1"]["before_allocated_api_usd"] == "-0.20"
    assert report["project_accounting"]["project_net_after_recorded_api_usd"] == "1.40"
    assert (
        report["strategies"]["v0"]["excluding_largest_positive_result_usd"]
        == "0.00"
    )
    assert report["audit_capabilities"]["can_enable_live_execution"] is False
    assert report["production_execution_state"] == "NOT_CHECKED"
    assert report["comparison_gate"]["status"] == "unavailable"
    assert report["source_freshness"]["status"] == "OK"


def test_positive_history_and_external_gate_cannot_enable_execution() -> None:
    external = comparison()
    external["promotion"] = {"v2_promoted": True}
    report = build_strategy_audit(snapshot(), external)
    assert report["audit_capabilities"]["can_enable_live_execution"] is False
    assert report["decision"] == "INSUFFICIENT_EVIDENCE_FOR_AUTONOMOUS_PROFIT"


def test_live_dashboard_state_is_not_reported_as_checked_or_disabled() -> None:
    data = snapshot()
    data["live_v2"] = {"configured": True, "runtime": {"state": "WAITING"}}
    report = build_strategy_audit(data, comparison())
    assert "live_execution_enabled" not in report
    assert report["production_execution_state"] == "NOT_CHECKED"


@pytest.mark.parametrize("amount", [None, "NaN", "Infinity", "broken"])
def test_missing_or_nonfinite_costs_are_not_silently_zero(amount: object) -> None:
    data = snapshot()
    data["portfolio"]["api_spend_usd"] = amount
    with pytest.raises(ValueError):
        build_strategy_audit(data, comparison())


def test_duplicate_and_incomplete_trade_details_are_rejected() -> None:
    data = snapshot()
    data["forecast_diagnostics"]["trades"].append(
        deepcopy(data["forecast_diagnostics"]["trades"][0])
    )
    with pytest.raises(ValueError, match="duplicate"):
        build_strategy_audit(data, comparison())
    data = snapshot()
    data["portfolio"]["closed_orders"] = 3
    with pytest.raises(ValueError, match="counts do not reconcile"):
        build_strategy_audit(data, comparison())


def test_mismatched_pnl_is_rejected() -> None:
    data = snapshot()
    data["portfolio"]["realized_pnl_usd"] = "2"
    with pytest.raises(ValueError, match="P&L do not reconcile"):
        build_strategy_audit(data, comparison())


def test_allocated_api_and_available_portfolio_aggregates_are_reconciled() -> None:
    data = snapshot()
    data["forecast_diagnostics"]["trades"][1]["api_cost_usd"] = "0.30"
    with pytest.raises(ValueError, match="allocated API aggregate"):
        build_strategy_audit(data, comparison())

    data = snapshot()
    data["portfolio"]["gross_trade_pnl_usd"] = "99"
    with pytest.raises(ValueError, match="gross P&L aggregate"):
        build_strategy_audit(data, comparison())

    data = snapshot()
    data["portfolio"]["net_project_pnl_after_api_usd"] = "99"
    with pytest.raises(ValueError, match="project net P&L aggregate"):
        build_strategy_audit(data, comparison())


def test_allocated_api_cannot_exceed_recorded_project_ledger() -> None:
    data = snapshot()
    data["portfolio"]["api_spend_usd"] = "0.29"
    with pytest.raises(ValueError, match="exceeds recorded"):
        build_strategy_audit(data, comparison())


@pytest.mark.parametrize(
    ("target", "value", "match"),
    [
        ("dashboard", None, "dashboard timestamp"),
        ("dashboard", "2026-09-17T17:09:56", "timezone-naive dashboard"),
        ("diagnostics", "not-a-time", "forecast diagnostics timestamp"),
        ("comparison", None, "comparison timestamp"),
    ],
)
def test_missing_invalid_or_naive_source_timestamps_are_rejected(
    target: str, value: object, match: str
) -> None:
    data = snapshot()
    other = comparison()
    if target == "dashboard":
        data["generated_at"] = value
    elif target == "diagnostics":
        data["forecast_diagnostics"]["generated_at_utc"] = value
    else:
        other["generated_at_utc"] = value
    with pytest.raises(ValueError, match=match):
        build_strategy_audit(data, other)


def test_source_timestamp_skew_is_visible_in_report() -> None:
    data = snapshot()
    data["forecast_diagnostics"]["generated_at_utc"] = "2026-09-17T16:00:00+00:00"
    other = comparison()
    other["generated_at_utc"] = "2026-09-17T18:00:00+00:00"
    report = build_strategy_audit(data, other)
    freshness = report["source_freshness"]
    assert freshness["status"] == "WARNING"
    assert freshness["diagnostics_lag_vs_dashboard_seconds"] == 4196.0
    assert set(freshness["warnings"]) == {
        "DIAGNOSTICS_TIMESTAMP_SKEW_EXCEEDS_600_SECONDS",
        "COMPARISON_TIMESTAMP_SKEW_EXCEEDS_600_SECONDS",
    }


def test_largest_positive_sensitivity_is_named_accurately_for_all_losses() -> None:
    data = snapshot()
    trades = data["forecast_diagnostics"]["trades"]
    trades[0]["realized_pnl_usd"] = "-1"
    trades[0]["api_cost_usd"] = "0"
    trades[1]["realized_pnl_usd"] = "-2"
    trades[1]["api_cost_usd"] = "0"
    data["portfolio"].update(
        {
            "api_spend_usd": "0",
            "realized_pnl_usd": "-3",
            "gross_trade_pnl_usd": "-3",
            "allocated_settled_order_api_cost_usd": "0",
            "net_project_pnl_after_api_usd": "-3",
        }
    )
    report = build_strategy_audit(data, comparison())
    assert (
        report["strategies"]["v0"]["excluding_largest_positive_result_usd"] == "-1"
    )
    assert (
        report["strategies"]["v1"]["excluding_largest_positive_result_usd"] == "-2"
    )
