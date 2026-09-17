from __future__ import annotations

import json
import sqlite3
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import pytest

from polybot.config import Settings
from polybot.live_pilot import LiveBuyIntent, LivePilotError, LivePilotJournal
from polybot.live_pilot_runtime import (
    CREDENTIAL_KIND,
    load_live_credentials,
    preview_intent_from_decision,
    write_prepared_intent,
)

NOW = datetime(2026, 9, 16, 20, 0, tzinfo=UTC)


class FakePublicClient:
    def get_market(self, *, id: str):  # noqa: ANN201
        return SimpleNamespace(
            id=id,
            condition_id="condition-1",
            state=SimpleNamespace(accepting_orders=True),
            outcomes=SimpleNamespace(
                yes=SimpleNamespace(token_id="token-1"),
                no=SimpleNamespace(token_id="token-no"),
            ),
            fee_rate=Decimal("0.05"),
            fee_exponent=Decimal("1"),
        )

    def get_order_book(self, *, token_id: str):  # noqa: ANN201
        return SimpleNamespace(
            asset_id=token_id,
            condition_id="condition-1",
            hash="book-1",
        )


def _decision_db(
    path: Path,
    *,
    created: datetime = NOW,
    action: str = "PAPER_BUY",
    reason_codes: list[str] | None = None,
) -> None:
    with sqlite3.connect(path) as connection:
        connection.execute(
            """
            CREATE TABLE decisions(
                id INTEGER PRIMARY KEY,
                event_id TEXT NOT NULL,
                market_id TEXT NOT NULL,
                action TEXT NOT NULL,
                created_at TEXT NOT NULL,
                payload_json TEXT NOT NULL
            )
            """
        )
        payload = {
            "action": action,
            "reason_codes": reason_codes or [],
            "strategy_version": "v1",
            "event_id": "event-1",
            "market_id": "market-1",
            "token_id": "token-1",
            "condition_id": "condition-1",
            "book_hash": "book-1",
            "notional_usd": "1.90",
            "executable_price": "0.50",
            "probability_edge": "0.10",
            "expected_profit_usd": "0.30",
            "created_at": created.isoformat(),
            "end_date": (created + timedelta(days=1)).isoformat(),
        }
        connection.execute(
            "INSERT INTO decisions VALUES(1, ?, ?, ?, ?, ?)",
            (
                "event-1",
                "market-1",
                action,
                created.isoformat(),
                json.dumps(payload),
            ),
        )


def _intent(**updates: object) -> LiveBuyIntent:
    values = {
        "strategy": "open-meteo-truncated-normal-v1",
        "event_id": "event-1",
        "market_id": "market-1",
        "condition_id": "condition-1",
        "token_id": "token-1",
        "amount_usd": Decimal("1.90"),
        "max_spend_usd": Decimal("1.995"),
        "max_price": Decimal("0.50"),
        "book_hash": "book-1",
        "decision_created_at_utc": NOW,
        "expires_at_utc": NOW + timedelta(minutes=3),
    }
    values.update(updates)
    return LiveBuyIntent(**values)  # type: ignore[arg-type]


def test_credentials_require_private_exact_schema(tmp_path: Path) -> None:
    path = tmp_path / "credentials.json"
    path.write_text(
        json.dumps(
            {
                "kind": CREDENTIAL_KIND,
                "auth_mode": "session_key",
                "wallet": "0x" + "11" * 20,
                "private_key": "0x" + "22" * 32,
                "clob_api_key": "key",
                "clob_api_secret": "secret",
                "clob_api_passphrase": "passphrase",
            }
        ),
        encoding="utf-8",
    )
    path.chmod(0o644)
    with pytest.raises(LivePilotError, match="0600"):
        load_live_credentials(path)
    path.chmod(0o600)
    loaded = load_live_credentials(path)
    assert loaded.auth_mode == "session_key"
    assert "0x" + "22" * 32 not in repr(loaded)
    assert "secret" not in repr(loaded)


def test_preview_is_fresh_v1_exact_book_and_unsigned(tmp_path: Path) -> None:
    database = tmp_path / "polybot.sqlite3"
    _decision_db(database)
    settings = Settings(database_path=database, max_book_age_seconds=180)
    intent = preview_intent_from_decision(
        settings=settings,
        decision_id=1,
        client=FakePublicClient(),
        now=NOW + timedelta(seconds=30),
    )
    assert intent.amount_usd == Decimal("1.90")
    assert intent.max_spend_usd == Decimal("1.995000")
    assert intent.expires_at_utc == NOW + timedelta(seconds=180)


def test_preview_rejects_stale_or_superseded_decision(tmp_path: Path) -> None:
    database = tmp_path / "polybot.sqlite3"
    _decision_db(database)
    settings = Settings(database_path=database, max_book_age_seconds=180)
    with pytest.raises(LivePilotError, match="stale"):
        preview_intent_from_decision(
            settings=settings,
            decision_id=1,
            client=FakePublicClient(),
            now=NOW + timedelta(seconds=181),
        )
    with sqlite3.connect(database) as connection:
        connection.execute(
            "INSERT INTO decisions VALUES(2, ?, ?, ?, ?, ?)",
            (
                "event-1",
                "market-1",
                "SKIP",
                (NOW + timedelta(seconds=10)).isoformat(),
                "{}",
            ),
        )
    with pytest.raises(LivePilotError, match="superseded"):
        preview_intent_from_decision(
            settings=settings,
            decision_id=1,
            client=FakePublicClient(),
            now=NOW + timedelta(seconds=20),
        )


def test_preview_accepts_candidate_suppressed_only_by_virtual_paper_duplicate(
    tmp_path: Path,
) -> None:
    database = tmp_path / "polybot.sqlite3"
    _decision_db(
        database,
        action="OBSERVE",
        reason_codes=["ACTIVE_PAPER_EVENT_MONITOR_ONLY"],
    )
    settings = Settings(database_path=database, max_book_age_seconds=180)

    intent = preview_intent_from_decision(
        settings=settings,
        decision_id=1,
        client=FakePublicClient(),
        now=NOW + timedelta(seconds=30),
    )

    assert intent.strategy == "open-meteo-truncated-normal-v1"
    assert intent.amount_usd == Decimal("1.90")


def test_preview_rejects_observe_with_any_non_virtual_reason(tmp_path: Path) -> None:
    database = tmp_path / "polybot.sqlite3"
    _decision_db(database, action="OBSERVE", reason_codes=["EDGE_TOO_SMALL"])
    settings = Settings(database_path=database, max_book_age_seconds=180)

    with pytest.raises(LivePilotError, match="requires PAPER_BUY"):
        preview_intent_from_decision(
            settings=settings,
            decision_id=1,
            client=FakePublicClient(),
            now=NOW + timedelta(seconds=30),
        )


def test_preparation_is_unsigned_and_execution_reserves_the_permanent_gate(
    tmp_path: Path,
) -> None:
    journal = LivePilotJournal(tmp_path / "polybot.sqlite3")
    output = tmp_path / "intent.json"
    report = write_prepared_intent(journal=journal, intent=_intent(), output_path=output)
    assert report["post_attempted"] is False
    assert report["signed"] is False
    assert report["one_shot_reserved"] is False
    assert output.stat().st_mode & 0o777 == 0o600
    journal.reserve(_intent())
    with pytest.raises(LivePilotError, match="one-shot live pilot slot"):
        journal.reserve(_intent(event_id="event-2"))
    with pytest.raises(LivePilotError, match="already consumed"):
        write_prepared_intent(
            journal=journal,
            intent=_intent(event_id="event-2"),
            output_path=tmp_path / "second.json",
        )
