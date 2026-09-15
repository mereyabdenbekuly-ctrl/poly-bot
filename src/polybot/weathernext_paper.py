"""Isolated WeatherNext paper-strategy helpers.

This module deliberately contains no live-trading client and no reference to
the legacy v1 paper ledger.  The scanner supplies immutable market snapshots
and a real ``WeatherNextSnapshot``; this module only derives a deterministic
idempotency key for the separate research ledger.
"""

from __future__ import annotations

import hashlib

from polybot.models import MarketDecision

WEATHERNEXT_PAPER_STRATEGY_VERSION = "weathernext-paper-v1"


def paper_idempotency_key(decision: MarketDecision) -> str:
    """Return a strategy-scoped key, independent from v1 paper orders."""

    payload = ":".join(
        (
            WEATHERNEXT_PAPER_STRATEGY_VERSION,
            decision.event_id,
            decision.market_id,
            decision.asset_id,
            decision.book_hash,
            str(decision.probability),
            decision.execution_model,
        )
    )
    return hashlib.sha256(payload.encode()).hexdigest()

