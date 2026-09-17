from __future__ import annotations

import json
import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from polybot.live_pilot import LivePilotError
from polybot.live_pilot_auto import (
    AUTO_ONCE_AUTHORIZATION_KIND,
    eligible_auto_candidates,
    load_auto_once_authorization,
)

NOW = datetime(2026, 9, 17, 12, 0, tzinfo=UTC)
WALLET = "0x" + "11" * 20


def _authorization(path: Path, **updates: object) -> Path:
    payload: dict[str, object] = {
        "kind": AUTO_ONCE_AUTHORIZATION_KIND,
        "wallet": WALLET,
        "strategy": "open-meteo-truncated-normal-v1",
        "side": "BUY",
        "order_type": "FOK",
        "max_orders": 1,
        "max_wallet_balance_usd": "10.00",
        "max_buy_notional_usd": "1.90",
        "max_total_spend_usd": "2.00",
        "min_probability_edge": "0.08",
        "min_expected_profit_usd": "0.25",
        "jurisdiction_confirmed": True,
        "expires_at_utc": (NOW + timedelta(hours=1)).isoformat(),
    }
    payload.update(updates)
    path.write_text(json.dumps(payload), encoding="utf-8")
    path.chmod(0o600)
    return path


def _decision_database(path: Path) -> None:
    with sqlite3.connect(path) as connection:
        connection.execute(
            """
            CREATE TABLE decisions(
                id INTEGER PRIMARY KEY,
                market_id TEXT NOT NULL,
                action TEXT NOT NULL,
                created_at TEXT NOT NULL,
                payload_json TEXT NOT NULL
            )
            """
        )


def _insert_decision(
    path: Path,
    *,
    decision_id: int,
    market_id: str,
    action: str = "PAPER_BUY",
    reason_codes: list[str] | None = None,
    edge: str = "0.20",
    expected_profit: str = "0.50",
    created_at: datetime = NOW,
) -> None:
    payload = {
        "action": action,
        "reason_codes": reason_codes or [],
        "strategy_version": "v1",
        "created_at": created_at.isoformat(),
        "probability_edge": edge,
        "expected_profit_usd": expected_profit,
    }
    with sqlite3.connect(path) as connection:
        connection.execute(
            "INSERT INTO decisions VALUES (?, ?, ?, ?, ?)",
            (decision_id, market_id, action, created_at.isoformat(), json.dumps(payload)),
        )


def test_standing_authorization_is_private_fixed_and_expiring(tmp_path: Path) -> None:
    path = _authorization(tmp_path / "authorization.json")
    loaded = load_auto_once_authorization(path, now=NOW)
    assert loaded.wallet == WALLET
    assert loaded.max_orders == 1

    path.chmod(0o644)
    with pytest.raises(LivePilotError, match="0600"):
        load_auto_once_authorization(path, now=NOW)

    expired = _authorization(
        tmp_path / "expired.json",
        expires_at_utc=(NOW - timedelta(seconds=1)).isoformat(),
    )
    with pytest.raises(LivePilotError, match="expired"):
        load_auto_once_authorization(expired, now=NOW)


@pytest.mark.parametrize(
    ("updates", "message"),
    [
        ({"max_orders": 2}, "exactly one"),
        ({"order_type": "GTC"}, "FOK BUY"),
        ({"max_total_spend_usd": "2.01"}, r"\$2 BUY cap"),
        ({"max_buy_notional_usd": "1.91"}, r"\$1\.90 BUY cap"),
        ({"jurisdiction_confirmed": False}, "eligibility"),
    ],
)
def test_standing_authorization_cannot_raise_or_bypass_limits(
    tmp_path: Path,
    updates: dict[str, object],
    message: str,
) -> None:
    path = _authorization(tmp_path / "authorization.json", **updates)
    with pytest.raises(LivePilotError, match=message):
        load_auto_once_authorization(path, now=NOW)


def test_candidate_selection_requires_fresh_unsuperseded_v1_and_ranks_ev(
    tmp_path: Path,
) -> None:
    database = tmp_path / "polybot.sqlite3"
    _decision_database(database)
    _insert_decision(
        database,
        decision_id=1,
        market_id="market-low",
        expected_profit="0.40",
    )
    _insert_decision(
        database,
        decision_id=2,
        market_id="market-best",
        action="OBSERVE",
        reason_codes=["ACTIVE_PAPER_EVENT_MONITOR_ONLY"],
        expected_profit="0.80",
    )
    _insert_decision(
        database,
        decision_id=3,
        market_id="market-below-edge",
        edge="0.01",
        expected_profit="1.00",
    )
    _insert_decision(
        database,
        decision_id=4,
        market_id="market-low",
        action="SKIP",
        expected_profit="0.00",
    )
    authorization = load_auto_once_authorization(
        _authorization(tmp_path / "authorization.json"),
        now=NOW,
    )

    candidates = eligible_auto_candidates(
        database,
        after_decision_id=0,
        authorization=authorization,
        max_book_age_seconds=180,
        now=NOW + timedelta(seconds=30),
    )

    assert [candidate.decision_id for candidate in candidates] == [2]


def test_candidate_selection_rejects_stale_and_prebaseline_rows(tmp_path: Path) -> None:
    database = tmp_path / "polybot.sqlite3"
    _decision_database(database)
    _insert_decision(
        database,
        decision_id=1,
        market_id="old",
        created_at=NOW - timedelta(minutes=4),
    )
    _insert_decision(database, decision_id=2, market_id="baseline")
    authorization = load_auto_once_authorization(
        _authorization(tmp_path / "authorization.json"),
        now=NOW,
    )

    candidates = eligible_auto_candidates(
        database,
        after_decision_id=2,
        authorization=authorization,
        max_book_age_seconds=180,
        now=NOW,
    )

    assert candidates == ()
