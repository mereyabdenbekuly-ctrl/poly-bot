"""Compact dashboard/CLI view for ``forecast-diagnostics-v1``."""

from __future__ import annotations

import json
import math
import sqlite3
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any
from urllib.parse import quote

from polybot.forecast_diagnostics import reconstruct_settled_trades
from polybot.forecast_sensitivity import (
    DEFAULT_V1_SIGMA_C,
    SigmaTradeThresholds,
    calculate_sigma_sensitivity,
    clamped_sigma_bracket_probabilities,
    replay_paper_qualification,
)
from polybot.models import Bracket, MarketSnapshot

DIAGNOSTICS_VERSION = "forecast-diagnostics-v1"
MIN_SIGMA_HISTORY_EVENTS = 20


def build_forecast_diagnostics_view(
    database: Path | str,
    *,
    settings: object | None = None,
) -> dict[str, object]:
    """Reconstruct settled trades and quantify their dependence on v1 sigma.

    This function is strictly read-only. The history-derived sigma is a small-
    sample diagnostic proxy, never a production calibration or trading input.
    """

    path = Path(database).expanduser().resolve()
    report = reconstruct_settled_trades(path)
    thresholds = (
        SigmaTradeThresholds() if settings is None else SigmaTradeThresholds.from_settings(settings)
    )
    uri = f"file:{quote(str(path), safe='/')}?mode=ro"
    with sqlite3.connect(uri, uri=True, timeout=30) as connection:
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA query_only = ON")
        sigma_proxy = _estimate_sigma_proxy(connection, report.trades)
        rows = [
            _compact_trade(
                connection,
                trade,
                thresholds=thresholds,
                alternative_sigma=Decimal(str(sigma_proxy["value_c"])),
            )
            for trade in report.trades
        ]

    sensitivity_rows = [row for row in rows if row["sensitivity_status"] == "available"]
    return {
        "version": DIAGNOSTICS_VERSION,
        "generated_at_utc": report.generated_at_utc.isoformat(),
        "summary": {
            **report.summary,
            "sensitivity_available_count": len(sensitivity_rows),
            "sigma_created_signal_count": sum(
                bool(row.get("sigma_created_signal")) for row in sensitivity_rows
            ),
            "double_conditioning_created_signal_count": sum(
                bool(row.get("double_conditioning_created_signal")) for row in sensitivity_rows
            ),
            "history_sigma_ceases_to_qualify_count": sum(
                bool(row.get("history_sigma_ceases_to_qualify")) for row in sensitivity_rows
            ),
        },
        "history_sigma_proxy": sigma_proxy,
        "trades": rows,
        "warnings": list(report.warnings),
        "interpretation": (
            "Forecast correctness and trade selection are separate. Sigma sensitivity "
            "is descriptive and never changes immutable v1 or live decisions."
        ),
    }


def _estimate_sigma_proxy(
    connection: sqlite3.Connection, trades: tuple[Any, ...]
) -> dict[str, object]:
    errors: list[Decimal] = []
    for trade in trades:
        prediction_id = trade.provenance.prediction_id
        actual = trade.official_actual_max_c
        if prediction_id is None or actual is None:
            continue
        row = connection.execute(
            "SELECT point_forecast_c FROM forecast_predictions_v2 WHERE id=?",
            (prediction_id,),
        ).fetchone()
        if row is None:
            continue
        predicted = _decimal_or_none(row["point_forecast_c"])
        if predicted is not None:
            errors.append(actual - predicted)
    if errors:
        rms = Decimal(str(math.sqrt(sum(float(value) ** 2 for value in errors) / len(errors))))
        value = max(Decimal("0.35"), min(Decimal("4.0"), rms))
    else:
        value = DEFAULT_V1_SIGMA_C
    return {
        "value_c": str(value),
        "sample_count": len(errors),
        "minimum_sample_count": MIN_SIGMA_HISTORY_EVENTS,
        "state": (
            "experimental_estimate"
            if len(errors) >= MIN_SIGMA_HISTORY_EVENTS
            else "insufficient_history"
        ),
        "method": "rms_entry_point_forecast_error_clamped_0.35_4.0c",
        "production_use": False,
        "message": (
            f"Only {len(errors)}/{MIN_SIGMA_HISTORY_EVENTS} auditable outcomes; "
            "descriptive proxy only."
        ),
    }


def _compact_trade(
    connection: sqlite3.Connection,
    trade: Any,
    *,
    thresholds: SigmaTradeThresholds,
    alternative_sigma: Decimal,
) -> dict[str, object]:
    row: dict[str, object] = {
        "paper_order_id": trade.paper_order_id,
        "event_id": trade.event_id,
        "event_title": trade.event_title,
        "strategy_version": trade.strategy_version,
        "opened_at_utc": (None if trade.opened_at_utc is None else trade.opened_at_utc.isoformat()),
        "bought_market_id": trade.market_id,
        "bought_label": trade.bought_bracket_label,
        "bought_probability": _string(trade.bought_probability),
        "entry_price": _string(trade.entry_price),
        "expected_profit_usd": _string(trade.expected_profit_usd),
        "realized_pnl_usd": _string(trade.realized_pnl_usd),
        "top_market_id": trade.top_forecast_market_id,
        "top_label": trade.top_forecast_bracket_label,
        "top_probability": _string(trade.top_forecast_probability),
        "winning_market_id": trade.official_winning_market_id,
        "winning_label": trade.official_winning_label,
        "actual_max_c": _string(trade.official_actual_max_c),
        "forecast_correct": trade.forecast_correct,
        "trade_correct": trade.trade_correct,
        "forecast_vs_trade": trade.forecast_vs_trade,
        "prediction_id": trade.provenance.prediction_id,
        "provenance_status": trade.provenance.status,
        "sensitivity_status": "unavailable",
        "diagnostic_notes": list(trade.diagnostic_notes),
    }
    prediction_id = trade.provenance.prediction_id
    snapshot_id = trade.entry_market_snapshot_id
    if prediction_id is None or snapshot_id is None:
        row["sensitivity_reason"] = "missing archived entry forecast or snapshot"
        return row
    prediction = connection.execute(
        "SELECT observed_floor_c, point_forecast_c, metadata_json "
        "FROM forecast_predictions_v2 WHERE id=?",
        (prediction_id,),
    ).fetchone()
    snapshot_row = connection.execute(
        "SELECT payload_json FROM market_snapshots WHERE id=?", (snapshot_id,)
    ).fetchone()
    scenario_rows = connection.execute(
        "SELECT raw_max_c FROM forecast_scenarios_v2 WHERE prediction_id=? ORDER BY member_id",
        (prediction_id,),
    ).fetchall()
    probability_rows = connection.execute(
        "SELECT market_id, outcome_label, lower_bound, upper_bound, "
        "lower_inclusive, upper_inclusive, probability "
        "FROM forecast_probabilities_v2 WHERE prediction_id=? ORDER BY ordinal",
        (prediction_id,),
    ).fetchall()
    if prediction is None or snapshot_row is None or not scenario_rows or not probability_rows:
        row["sensitivity_reason"] = "incomplete forecast sensitivity archive"
        return row
    try:
        snapshot = MarketSnapshot.model_validate_json(snapshot_row["payload_json"])
        brackets = {
            str(item["market_id"]): Bracket(
                market_id=str(item["market_id"]),
                label=str(item["outcome_label"]),
                lower=(None if item["lower_bound"] is None else float(item["lower_bound"])),
                upper=(None if item["upper_bound"] is None else float(item["upper_bound"])),
                lower_inclusive=bool(item["lower_inclusive"]),
                upper_inclusive=bool(item["upper_inclusive"]),
            )
            for item in probability_rows
        }
        member_values = [Decimal(str(item["raw_max_c"])) for item in scenario_rows]
        floor = _decimal_or_none(prediction["observed_floor_c"])
        metadata = _json_object(prediction["metadata_json"])
        current_sigma = _decimal_or_none(metadata.get("weather_error_sigma_c"))
        current_sigma = current_sigma or DEFAULT_V1_SIGMA_C
        sensitivity = calculate_sigma_sensitivity(
            member_values=member_values,
            brackets=brackets,
            current_sigma_c=current_sigma,
            alternative_sigma_c=alternative_sigma,
            observed_floor_c=floor,
            snapshot=snapshot,
            selected_market_id=trade.market_id,
            thresholds=thresholds,
            api_cost_usd=trade.api_cost_usd or Decimal(0),
        )
        clamped = clamped_sigma_bracket_probabilities(
            member_values,
            brackets,
            sigma_c=current_sigma,
            observed_floor_c=floor,
        )
        clamped_qualification = replay_paper_qualification(
            snapshot=snapshot,
            probability=clamped[trade.market_id],
            thresholds=thresholds,
            api_cost_usd=trade.api_cost_usd or Decimal(0),
        )
        qualification = sensitivity.qualification
        if qualification is None:
            raise ValueError("qualification replay was not produced")
        stored_probability = next(
            (
                Decimal(str(item["probability"]))
                for item in probability_rows
                if str(item["market_id"]) == trade.market_id
            ),
            None,
        )
        row.update(
            {
                "sensitivity_status": "available",
                "current_sigma_c": str(current_sigma),
                "history_sigma_proxy_c": str(alternative_sigma),
                "raw_probability": str(sensitivity.raw_probabilities[trade.market_id]),
                "current_sigma_probability": str(
                    sensitivity.current_sigma_probabilities[trade.market_id]
                ),
                "history_sigma_probability": str(
                    sensitivity.alternative_sigma_probabilities[trade.market_id]
                ),
                "clamped_current_sigma_probability": str(clamped[trade.market_id]),
                "stored_probability": _string(stored_probability),
                "stored_probability_matches_replay": (
                    stored_probability == sensitivity.current_sigma_probabilities[trade.market_id]
                ),
                "raw_qualifies": qualification.raw.qualifies,
                "current_sigma_qualifies": qualification.baseline.qualifies,
                "history_sigma_qualifies": qualification.alternative.qualifies,
                "clamped_current_sigma_qualifies": clamped_qualification.qualifies,
                "sigma_created_signal": qualification.raw_would_cease_to_qualify,
                "history_sigma_ceases_to_qualify": (
                    qualification.alternative_would_cease_to_qualify
                ),
                "double_conditioning_created_signal": (
                    qualification.baseline.qualifies and not clamped_qualification.qualifies
                ),
                "raw_reason_codes": list(qualification.raw.reason_codes),
                "history_sigma_reason_codes": list(qualification.alternative.reason_codes),
                "clamped_reason_codes": list(clamped_qualification.reason_codes),
            }
        )
    except (KeyError, ValueError, TypeError, InvalidOperation) as error:
        row["sensitivity_reason"] = f"{type(error).__name__}: {error}"
    return row


def _decimal_or_none(value: object) -> Decimal | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        result = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None
    return result if result.is_finite() else None


def _json_object(value: object) -> dict[str, object]:
    try:
        parsed = json.loads(str(value or "{}"))
    except (TypeError, ValueError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _string(value: object) -> str | None:
    return None if value is None else str(value)
