#!/usr/bin/env python3
"""Replay a JSON cash-risk scenario locally; never access a wallet or network."""

from __future__ import annotations

import argparse
import json
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

from polybot.offline_risk_sim import CashRiskLimits, OfflineCashRiskSimulator


def _timestamp(value: object, field: str) -> datetime:
    if not isinstance(value, str):
        raise ValueError(f"{field} must be an ISO-8601 string")
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError(f"{field} must include a timezone")
    return parsed


def _limits(raw: object) -> CashRiskLimits:
    if raw is None:
        return CashRiskLimits()
    if not isinstance(raw, dict):
        raise ValueError("limits must be an object")
    return CashRiskLimits(
        daily_loss_limit_usd=Decimal(str(raw.get("daily_loss_limit_usd", "2"))),
        position_risk_limit_usd=Decimal(str(raw.get("position_risk_limit_usd", "2"))),
        capital_limit_usd=Decimal(str(raw.get("capital_limit_usd", "10"))),
        timezone_name=str(raw.get("timezone_name", "Asia/Almaty")),
    )


def _required_money(event: dict[str, Any], key: str) -> str | int:
    value = event.get(key)
    if isinstance(value, bool) or not isinstance(value, (str, int)):
        raise ValueError(f"{key} must be a decimal string or integer")
    return value


def replay_scenario(scenario: dict[str, Any]) -> dict[str, object]:
    simulator = OfflineCashRiskSimulator(_limits(scenario.get("limits")))
    events = scenario.get("events")
    if not isinstance(events, list):
        raise ValueError("events must be a list")
    latest: datetime | None = None
    for event in events:
        if not isinstance(event, dict):
            raise ValueError("each event must be an object")
        kind = event.get("type")
        decision_id = event.get("decision_id")
        if not isinstance(kind, str) or not isinstance(decision_id, str):
            raise ValueError("event type and decision_id are required")
        at = _timestamp(event.get("at"), "event.at")
        latest = at if latest is None or at > latest else latest
        if kind == "reserve":
            simulator.reserve(decision_id, _required_money(event, "max_loss_usd"), at)
        elif kind == "preflight_reject":
            reason = event.get("reason")
            if not isinstance(reason, str):
                raise ValueError("preflight_reject reason is required")
            simulator.preflight_reject(decision_id, at, reason=reason)
        elif kind == "mark_open":
            simulator.mark_open(decision_id, at)
        elif kind == "mark_ambiguous":
            simulator.mark_ambiguous(decision_id, at)
        elif kind == "reconcile_not_submitted":
            simulator.reconcile_not_submitted(decision_id, at)
        elif kind == "settle":
            simulator.settle(decision_id, _required_money(event, "realized_pnl_usd"), at)
        else:
            raise ValueError(f"unsupported event type: {kind}")
    raw_as_of = scenario.get("as_of")
    if raw_as_of is None:
        if latest is None:
            raise ValueError("as_of is required for an empty scenario")
        as_of = latest
    else:
        as_of = _timestamp(raw_as_of, "as_of")
    return simulator.snapshot(as_of)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("scenario", type=Path)
    args = parser.parse_args()
    raw = json.loads(args.scenario.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError("scenario root must be an object")
    print(json.dumps(replay_scenario(raw), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
