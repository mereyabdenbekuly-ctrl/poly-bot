"""Offline, descriptive audit of exported dashboard/comparison snapshots.

No client, credentials, database writes, configuration changes, or execution
actions. A good historical result can never activate a strategy here.
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import Any

_MAX_SOURCE_SKEW_SECONDS = 600


def _amount(value: object, field: str) -> Decimal:
    try:
        amount = Decimal(str(value))
    except (InvalidOperation, ValueError) as error:
        raise ValueError(f"missing or invalid {field}") from error
    if not amount.is_finite():
        raise ValueError(f"non-finite {field}")
    return amount


def _optional_amount(source: Mapping[str, Any], key: str, field: str) -> Decimal | None:
    if key not in source:
        return None
    return _amount(source.get(key), field)


def _timestamp(value: object, field: str) -> datetime:
    if not isinstance(value, str) or not value:
        raise ValueError(f"missing or invalid {field}")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise ValueError(f"missing or invalid {field}") from error
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError(f"timezone-naive {field}")
    return parsed.astimezone(UTC)


def build_strategy_audit(
    dashboard: Mapping[str, Any], comparison: Mapping[str, Any]
) -> dict[str, Any]:
    """Separate versioned paper results, costs, and forecast validation evidence."""

    diagnostics = dashboard.get("forecast_diagnostics")
    portfolio = dashboard.get("portfolio")
    if not isinstance(diagnostics, Mapping) or not isinstance(portfolio, Mapping):
        raise ValueError("snapshot lacks diagnostics or portfolio")
    trades = diagnostics.get("trades")
    if not isinstance(trades, list):
        raise ValueError("snapshot lacks settled paper trades")
    dashboard_at = _timestamp(dashboard.get("generated_at"), "dashboard timestamp")
    diagnostics_at = _timestamp(
        diagnostics.get("generated_at_utc"), "forecast diagnostics timestamp"
    )
    comparison_at = _timestamp(comparison.get("generated_at_utc"), "comparison timestamp")
    source_warnings = diagnostics.get("warnings", [])
    if not isinstance(source_warnings, list) or any(
        not isinstance(warning, str) for warning in source_warnings
    ):
        raise ValueError("invalid forecast diagnostics warnings")
    seen: set[int] = set()
    buckets: dict[str, list[dict[str, Any]]] = {}
    for trade in trades:
        if not isinstance(trade, Mapping):
            raise ValueError("invalid settled paper trade")
        raw_id = trade.get("paper_order_id")
        if type(raw_id) is not int or raw_id < 1:
            raise ValueError("invalid paper_order_id")
        if raw_id in seen:
            raise ValueError("duplicate paper_order_id")
        seen.add(raw_id)
        version = trade.get("strategy_version")
        if not isinstance(version, str) or not version:
            raise ValueError("trade lacks entry strategy version")
        allocated = _amount(trade.get("api_cost_usd"), "allocated API cost")
        if allocated < 0:
            raise ValueError("negative allocated API cost")
        buckets.setdefault(version, []).append(
            {
                "id": raw_id,
                "event_id": trade.get("event_id"),
                "won": trade.get("trade_correct"),
                "pnl": _amount(trade.get("realized_pnl_usd"), "realized paper P&L"),
                "api": allocated,
            }
        )

    strategies: dict[str, dict[str, Any]] = {}
    total_realized = Decimal(0)
    total_allocated = Decimal(0)
    for version, rows in sorted(buckets.items()):
        realized = sum((row["pnl"] for row in rows), Decimal(0))
        allocated = sum((row["api"] for row in rows), Decimal(0))
        positive = sorted((row["pnl"] for row in rows if row["pnl"] > 0), reverse=True)
        strategies[version] = {
            "settled_count": len(rows),
            "wins": sum(row["won"] is True for row in rows),
            "losses": sum(row["won"] is False for row in rows),
            "unknown_outcomes": sum(type(row["won"]) is not bool for row in rows),
            "realized_after_allocated_api_usd": str(realized),
            "allocated_api_usd": str(allocated),
            "before_allocated_api_usd": str(realized + allocated),
            "excluding_largest_positive_result_usd": str(
                realized - sum(positive[:1], Decimal(0))
            ),
            "excluding_two_largest_positive_results_usd": str(
                realized - sum(positive[:2], Decimal(0))
            ),
        }
        total_realized += realized
        total_allocated += allocated

    # Never allocate all shared project costs to v1 or subtract allocated costs
    # twice. The recorded project ledger includes research and unallocated calls.
    recorded_api = _amount(portfolio.get("api_spend_usd"), "recorded project API spend")
    reported_realized = _amount(portfolio.get("realized_pnl_usd"), "reported realized P&L")
    if recorded_api < 0:
        raise ValueError("negative project API spend")
    if total_allocated > recorded_api:
        raise ValueError("allocated API cost exceeds recorded project API spend")
    if reported_realized != total_realized:
        raise ValueError("trade details and portfolio P&L do not reconcile")
    reported_count = portfolio.get("closed_orders")
    if type(reported_count) is not int or reported_count != len(seen):
        raise ValueError("trade details and portfolio settled counts do not reconcile")

    reported_allocated = _optional_amount(
        portfolio,
        "allocated_settled_order_api_cost_usd",
        "reported allocated settled-order API cost",
    )
    if reported_allocated is not None and reported_allocated != total_allocated:
        raise ValueError("trade details and allocated API aggregate do not reconcile")
    gross_before_allocated = total_realized + total_allocated
    reported_gross = _optional_amount(
        portfolio, "gross_trade_pnl_usd", "reported gross trade P&L"
    )
    if reported_gross is not None and reported_gross != gross_before_allocated:
        raise ValueError("trade details and gross P&L aggregate do not reconcile")
    project_net = gross_before_allocated - recorded_api
    reported_project_net = _optional_amount(
        portfolio, "net_project_pnl_after_api_usd", "reported project net P&L"
    )
    if reported_project_net is not None and reported_project_net != project_net:
        raise ValueError("trade details and project net P&L aggregate do not reconcile")

    diagnostic_delta = (dashboard_at - diagnostics_at).total_seconds()
    comparison_skew = abs((dashboard_at - comparison_at).total_seconds())
    freshness_warnings: list[str] = []
    if diagnostic_delta < -1:
        freshness_warnings.append("DIAGNOSTICS_TIMESTAMP_AFTER_DASHBOARD")
    if abs(diagnostic_delta) > _MAX_SOURCE_SKEW_SECONDS:
        freshness_warnings.append("DIAGNOSTICS_TIMESTAMP_SKEW_EXCEEDS_600_SECONDS")
    if comparison_skew > _MAX_SOURCE_SKEW_SECONDS:
        freshness_warnings.append("COMPARISON_TIMESTAMP_SKEW_EXCEEDS_600_SECONDS")
    promotion = comparison.get("promotion")
    gate = dict(promotion) if isinstance(promotion, Mapping) else {"status": "unavailable"}
    return {
        "version": "offline-strategy-audit-v2",
        "dashboard_as_of_utc": dashboard_at.isoformat(),
        "comparison_as_of_utc": comparison_at.isoformat(),
        "scope": "SETTLED_PAPER_ONLY",
        "audit_capabilities": {"can_enable_live_execution": False},
        "production_execution_state": "NOT_CHECKED",
        "decision": "INSUFFICIENT_EVIDENCE_FOR_AUTONOMOUS_PROFIT",
        "research_control": "open-meteo-truncated-normal-v1",
        "research_comparator": "forecast-engine-v2-station-intraday@1",
        "strategies": strategies,
        "project_accounting": {
            "settled_paper_after_allocated_api_usd": str(total_realized),
            "allocated_api_added_back_usd": str(total_allocated),
            "settled_paper_before_allocated_api_usd": str(gross_before_allocated),
            "recorded_project_api_spend_usd": str(recorded_api),
            "unallocated_project_api_spend_usd": str(recorded_api - total_allocated),
            "project_net_after_recorded_api_usd": str(project_net),
            "reported_aggregates_checked": {
                "allocated_api": reported_allocated is not None,
                "gross_trade_pnl": reported_gross is not None,
                "project_net_pnl": reported_project_net is not None,
            },
            "open_positions_excluded": True,
            "per_strategy_share_of_unallocated_api": "unknown",
        },
        "source_freshness": {
            "forecast_diagnostics_as_of_utc": diagnostics_at.isoformat(),
            "diagnostics_lag_vs_dashboard_seconds": diagnostic_delta,
            "comparison_skew_vs_dashboard_seconds": comparison_skew,
            "status": "WARNING" if freshness_warnings else "OK",
            "warnings": freshness_warnings,
            "forecast_diagnostics_source_warnings": list(source_warnings),
        },
        "comparison_gate": gate,
        "limitations": [
            "Paper execution is simulated, not verified live cash profit.",
            "Historical point-forecast accuracy is not a trading return.",
            "Latest selected checkpoints are not a frozen holdout.",
            "Largest-positive-result exclusion is descriptive sensitivity, not a "
            "counterfactual backtest.",
            "A positive result never enables execution or proves future profit.",
            "Production execution state is not inspected or changed by this audit.",
        ],
    }
