from __future__ import annotations

import hashlib
import json
import sqlite3
from collections.abc import Callable, Iterator
from dataclasses import asdict, dataclass, replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from polybot.live_pilot import LiveBuyIntent, LivePilotError, LivePilotJournal
from polybot.live_v2 import (
    LIVE_V2_AUTHORIZATION_KIND,
    LIVE_V2_STRATEGY,
    LIVE_V2_TIMEZONE,
    LiveV2Executor,
    LiveV2Journal,
    LiveV2State,
    eligible_live_v2_candidates,
    load_live_v2_authorization,
    reconcile_live_v2,
)

NOW = datetime.now(UTC).replace(microsecond=0)
WALLET = "0x" + "22" * 20


class FakePaginator:
    def __init__(self, items: list[Any] | None = None) -> None:
        self.items = items or []

    def iter_items(self) -> Iterator[Any]:
        yield from self.items


@dataclass(frozen=True, slots=True, kw_only=True)
class FakeSignedOrder:
    builder: str = "0x" + "00" * 32
    expiration: int = 0
    maker: str = "0x1111111111111111111111111111111111111111"
    maker_amount: int = 1_900_000
    metadata: str = "0x" + "00" * 32
    order_type: str = "FOK"
    salt: int = 123
    side: str = "BUY"
    signature: str = "0xsignature"
    signature_type: int = 0
    signer: str = "0x1111111111111111111111111111111111111111"
    taker_amount: int = 3_800_000
    timestamp: int = 1
    token_id: str = "token-1"
    post_only: bool = False


@dataclass(frozen=True, slots=True)
class FakeTrade:
    id: str
    asset_id: str = "token-1"
    side: str = "BUY"
    taker_order_id: str = "order-1"


class FakeResponse:
    def __init__(
        self,
        *,
        ok: bool | None = True,
        order_id: str | None = "order-1",
        status: str = "matched",
        trade_ids: tuple[str, ...] = ("trade-1",),
        code: str | None = None,
        message: str | None = None,
    ) -> None:
        self.ok = ok
        self.order_id = order_id
        self.status = status
        self.trade_ids = trade_ids
        self.code = code
        self.message = message

    def model_dump(self, *, mode: str) -> dict[str, Any]:
        assert mode == "json"
        return {
            "ok": self.ok,
            "order_id": self.order_id,
            "status": self.status,
            "trade_ids": list(self.trade_ids),
            "code": self.code,
            "message": self.message,
        }


class FakeClient:
    wallet = WALLET

    def __init__(self) -> None:
        self.balance = 5_000_000
        self.allowances = {"exchange": 2_000_000}
        self.open_orders: list[Any] = []
        self.positions: list[Any] = []
        self.trades: list[Any] = []
        self.closed_positions: list[Any] = []
        self.book_hash = "book-1"
        self.asks = (SimpleNamespace(price=Decimal("0.40"), size=Decimal("100")),)
        self.market_closed = False
        self.closed_only = False
        self.post_calls = 0
        self.create_calls = 0
        self.post_error: BaseException | None = None
        self.response = FakeResponse()
        self.signed = FakeSignedOrder()

    def get_balance_allowance(self, *, asset_type: str) -> Any:
        assert asset_type == "COLLATERAL"
        return SimpleNamespace(balance=self.balance, allowances=self.allowances)

    def list_open_orders(self, **kwargs: Any) -> FakePaginator:
        return FakePaginator(self.open_orders)

    def list_positions(self, **kwargs: Any) -> FakePaginator:
        return FakePaginator(self.positions)

    def list_closed_positions(self, **kwargs: Any) -> FakePaginator:
        return FakePaginator(self.closed_positions)

    def list_account_trades(self, **kwargs: Any) -> FakePaginator:
        return FakePaginator(self.trades)

    def get_closed_only_mode(self) -> bool:
        return self.closed_only

    def get_market(self, *, id: str) -> Any:
        assert id == "market-1"
        outcomes = SimpleNamespace(
            yes=SimpleNamespace(token_id="token-1"),
            no=SimpleNamespace(token_id="token-no"),
        )
        return SimpleNamespace(
            id="market-1",
            condition_id="condition-1",
            state=SimpleNamespace(
                accepting_orders=True,
                closed=self.market_closed,
            ),
            outcomes=outcomes,
            fee_rate=Decimal("0.05"),
            fee_exponent=Decimal("1"),
        )

    def get_order_book(self, *, token_id: str) -> Any:
        assert token_id == "token-1"
        return SimpleNamespace(
            asset_id="token-1",
            condition_id="condition-1",
            hash=self.book_hash,
            asks=self.asks,
        )

    def create_market_order(self, **kwargs: Any) -> FakeSignedOrder:
        self.create_calls += 1
        assert kwargs == {
            "token_id": "token-1",
            "side": "BUY",
            "amount": Decimal("1.90"),
            "max_spend": Decimal("2.00"),
            "max_price": Decimal("0.55"),
            "order_type": "FOK",
        }
        return self.signed

    def post_order(self, signed_order: Any) -> FakeResponse:
        assert signed_order is self.signed
        self.post_calls += 1
        if self.post_error is not None:
            raise self.post_error
        return self.response


def _intent(**updates: Any) -> LiveBuyIntent:
    values: dict[str, Any] = {
        "strategy": LIVE_V2_STRATEGY,
        "event_id": "event-1",
        "market_id": "market-1",
        "condition_id": "condition-1",
        "token_id": "token-1",
        "amount_usd": Decimal("1.90"),
        "max_spend_usd": Decimal("2.00"),
        "max_price": Decimal("0.55"),
        "book_hash": "book-1",
        "decision_created_at_utc": NOW - timedelta(seconds=10),
        "expires_at_utc": NOW + timedelta(minutes=10),
    }
    values.update(updates)
    return LiveBuyIntent(**values)


def _authorization(path: Path, **updates: object) -> Path:
    payload: dict[str, object] = {
        "kind": LIVE_V2_AUTHORIZATION_KIND,
        "wallet": WALLET,
        "strategy": LIVE_V2_STRATEGY,
        "side": "BUY",
        "order_type": "FOK",
        "one_position_at_a_time": True,
        "max_orders_per_day": 50,
        "daily_stop_loss_usd": "6.00",
        "daily_timezone": LIVE_V2_TIMEZONE,
        "max_wallet_balance_usd": "10.00",
        "max_buy_notional_usd": "1.90",
        "max_total_spend_usd": "2.00",
        "min_probability_edge": "0.05",
        "min_expected_profit_usd": "0.15",
        "jurisdiction_confirmed": True,
        "authorized_at_utc": (NOW - timedelta(minutes=1)).isoformat(),
        "expires_at_utc": (NOW + timedelta(hours=1)).isoformat(),
    }
    payload.update(updates)
    path.write_text(json.dumps(payload), encoding="utf-8")
    path.chmod(0o600)
    return path


def _loaded_authorization(tmp_path: Path):  # noqa: ANN202
    return load_live_v2_authorization(
        _authorization(tmp_path / "authorization.json"),
        now=NOW,
    )


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
    payload_action: str | None = None,
    reason_codes: list[str] | None = None,
    edge: str = "0.20",
    expected_profit: str = "0.50",
    created_at: datetime = NOW,
    strategy_version: str = "v1",
) -> None:
    payload = {
        "action": action if payload_action is None else payload_action,
        "reason_codes": reason_codes or [],
        "strategy_version": strategy_version,
        "created_at": created_at.isoformat(),
        "probability_edge": edge,
        "expected_profit_usd": expected_profit,
    }
    with sqlite3.connect(path) as connection:
        connection.execute(
            "INSERT INTO decisions VALUES (?, ?, ?, ?, ?)",
            (decision_id, market_id, action, created_at.isoformat(), json.dumps(payload)),
        )


def test_authorization_is_private_fixed_and_time_bounded(tmp_path: Path) -> None:
    path = _authorization(tmp_path / "authorization.json")
    loaded = load_live_v2_authorization(path, now=NOW)

    assert loaded.wallet == WALLET
    assert loaded.daily_timezone == LIVE_V2_TIMEZONE
    assert loaded.max_orders_per_day == 50

    path.chmod(0o644)
    with pytest.raises(LivePilotError, match="mode 0600"):
        load_live_v2_authorization(path, now=NOW)

    expired = _authorization(
        tmp_path / "expired.json",
        expires_at_utc=(NOW - timedelta(seconds=1)).isoformat(),
    )
    with pytest.raises(LivePilotError, match="expired"):
        load_live_v2_authorization(expired, now=NOW)

    wrong_schema = _authorization(tmp_path / "wrong-schema.json", extra_gate=True)
    with pytest.raises(LivePilotError, match="fixed v1 schema"):
        load_live_v2_authorization(wrong_schema, now=NOW)


@pytest.mark.parametrize(
    ("updates", "message"),
    [
        ({"strategy": "different"}, "existing v1 strategy"),
        ({"side": "SELL"}, "only FOK BUY"),
        ({"order_type": "GTC"}, "only FOK BUY"),
        ({"one_position_at_a_time": False}, "one_position_at_a_time"),
        ({"max_orders_per_day": 51}, "at most fifty"),
        ({"daily_stop_loss_usd": "6.01"}, "daily stop"),
        ({"daily_timezone": "UTC"}, "Asia/Almaty"),
        ({"max_wallet_balance_usd": "10.01"}, r"\$10 wallet cap"),
        ({"max_buy_notional_usd": "1.91"}, r"\$1\.90 BUY cap"),
        ({"max_total_spend_usd": "2.01"}, r"\$2 all-in cap"),
        ({"min_probability_edge": "0.04"}, "probability edge gate"),
        ({"min_expected_profit_usd": "0.14"}, "expected-profit gate"),
        ({"jurisdiction_confirmed": False}, "eligibility"),
    ],
)
def test_authorization_caps_and_gates_are_immutable(
    tmp_path: Path,
    updates: dict[str, object],
    message: str,
) -> None:
    path = _authorization(tmp_path / "authorization.json", **updates)

    with pytest.raises(LivePilotError, match=message):
        load_live_v2_authorization(path, now=NOW)


def test_candidate_filtering_requires_fresh_unsuperseded_unattempted_v1(
    tmp_path: Path,
) -> None:
    database = tmp_path / "polybot.sqlite3"
    _decision_database(database)
    journal = LiveV2Journal(database)
    authorization = _loaded_authorization(tmp_path)

    _insert_decision(
        database,
        decision_id=1,
        market_id="market-superseded",
        expected_profit="0.90",
        created_at=NOW - timedelta(seconds=20),
    )
    _insert_decision(
        database,
        decision_id=2,
        market_id="market-best",
        action="OBSERVE",
        reason_codes=["ACTIVE_PAPER_EVENT_MONITOR_ONLY"],
        expected_profit="0.80",
        created_at=NOW - timedelta(seconds=15),
    )
    _insert_decision(
        database,
        decision_id=3,
        market_id="market-good",
        expected_profit="0.50",
        created_at=NOW - timedelta(seconds=10),
    )
    _insert_decision(database, decision_id=4, market_id="below-edge", edge="0.049")
    _insert_decision(
        database,
        decision_id=5,
        market_id="below-profit",
        expected_profit="0.149",
    )
    _insert_decision(
        database,
        decision_id=6,
        market_id="stale",
        created_at=NOW - timedelta(seconds=181),
    )
    _insert_decision(
        database,
        decision_id=7,
        market_id="before-authorization",
        created_at=NOW - timedelta(seconds=61),
    )
    _insert_decision(
        database,
        decision_id=8,
        market_id="wrong-strategy",
        strategy_version="v2",
    )
    _insert_decision(
        database,
        decision_id=9,
        market_id="future",
        created_at=NOW + timedelta(seconds=1),
    )
    _insert_decision(
        database,
        decision_id=10,
        market_id="market-superseded",
        action="SKIP",
        expected_profit="0.00",
    )
    _insert_decision(
        database,
        decision_id=11,
        market_id="already-attempted",
        expected_profit="1.00",
    )
    with sqlite3.connect(database) as connection:
        connection.execute(
            "INSERT INTO decisions VALUES (?, ?, ?, ?, ?)",
            (12, "malformed", "PAPER_BUY", NOW.isoformat(), "{not-json"),
        )

    attempted = _intent(event_id="attempted-event")
    journal.reserve(
        intent=attempted,
        decision_id=11,
        timezone=LIVE_V2_TIMEZONE,
        now=NOW,
    )
    journal.mark_pre_sign_rejected(attempted.digest, LivePilotError("test terminal"))

    candidates = eligible_live_v2_candidates(
        database,
        authorization=authorization,
        journal=journal,
        max_book_age_seconds=180,
        now=NOW,
    )

    assert [candidate.decision_id for candidate in candidates] == [2, 3]


def test_unsigned_rejection_does_not_consume_the_almaty_day(
    tmp_path: Path,
) -> None:
    journal = LiveV2Journal(tmp_path / "polybot.sqlite3")
    before_midnight = datetime(2026, 9, 17, 18, 58, tzinfo=UTC)
    same_almaty_day = datetime(2026, 9, 17, 18, 59, 59, tzinfo=UTC)
    next_almaty_day = datetime(2026, 9, 17, 19, 0, tzinfo=UTC)
    first = _intent(event_id="first")

    first_record = journal.reserve(
        intent=first,
        decision_id=1,
        timezone=LIVE_V2_TIMEZONE,
        now=before_midnight,
    )
    journal.mark_pre_sign_rejected(first.digest, LivePilotError("terminal"))

    assert first_record.local_day == "2026-09-17"
    assert journal.charged_count_for_day(first_record.local_day) == 0
    assert not journal.get(first.digest).consumes_daily_limit
    second = _intent(event_id="same-day")
    same_day_record = journal.reserve(
        intent=second,
        decision_id=2,
        timezone=LIVE_V2_TIMEZONE,
        now=same_almaty_day,
    )
    assert same_day_record.local_day == first_record.local_day
    assert same_day_record.consumes_daily_limit
    journal.mark_pre_sign_rejected(second.digest, LivePilotError("terminal"))

    next_record = journal.reserve(
        intent=_intent(event_id="next-day"),
        decision_id=3,
        timezone=LIVE_V2_TIMEZONE,
        now=next_almaty_day,
    )
    assert next_record.local_day == "2026-09-18"
    assert next_record.state is LiveV2State.PREPARED


def test_live_v2_does_not_mutate_legacy_live_pilot_gate(tmp_path: Path) -> None:
    database = tmp_path / "polybot.sqlite3"
    legacy = LivePilotJournal(database)
    legacy.reserve(_intent(event_id="legacy"))
    gate_before = legacy.gate()
    assert gate_before is not None

    v2 = LiveV2Journal(database)
    v2.reserve(
        intent=_intent(event_id="v2"),
        decision_id=101,
        timezone=LIVE_V2_TIMEZONE,
        now=NOW,
    )

    assert legacy.gate() == gate_before
    with sqlite3.connect(database) as connection:
        assert connection.execute("SELECT COUNT(*) FROM live_pilot_gate").fetchone()[0] == 1


def test_pre_sign_rejection_journals_without_signing_or_posting(tmp_path: Path) -> None:
    value = _intent()
    client = FakeClient()
    client.asks = (SimpleNamespace(price=Decimal("0.90"), size=Decimal("100")),)
    journal = LiveV2Journal(tmp_path / "polybot.sqlite3")

    record = LiveV2Executor(journal).execute(
        client=client,
        intent=value,
        decision_id=1,
        authorization=_loaded_authorization(tmp_path),
        geoblocked=False,
        now=NOW,
    )

    assert record.state is LiveV2State.PRE_SIGN_REJECTED
    assert record.signed_fingerprint is None
    assert record.signed_order_json is None
    assert not record.consumes_daily_limit
    assert "order book changed" in str(record.last_error)
    assert client.create_calls == 0
    assert client.post_calls == 0


@pytest.mark.parametrize(
    ("block", "message"),
    [
        (lambda client: client.open_orders.append(SimpleNamespace(id="open")), "open order"),
        (
            lambda client: [
                client.positions.append(SimpleNamespace(size=Decimal("1"))) for _ in range(3)
            ],
            "position cap",
        ),
    ],
)
def test_open_order_or_position_blocks_before_signing(
    tmp_path: Path,
    block: Callable[[FakeClient], None],
    message: str,
) -> None:
    client = FakeClient()
    block(client)

    record = LiveV2Executor(LiveV2Journal(tmp_path / "polybot.sqlite3")).execute(
        client=client,
        intent=_intent(),
        decision_id=1,
        authorization=_loaded_authorization(tmp_path),
        geoblocked=False,
        now=NOW,
    )

    assert record.state is LiveV2State.PRE_SIGN_REJECTED
    assert message in str(record.last_error)
    assert client.create_calls == 0
    assert client.post_calls == 0


def test_accepted_response_and_redacted_signature_are_journaled(tmp_path: Path) -> None:
    value = _intent()
    client = FakeClient()
    database = tmp_path / "polybot.sqlite3"

    record = LiveV2Executor(LiveV2Journal(database)).execute(
        client=client,
        intent=value,
        decision_id=1,
        authorization=_loaded_authorization(tmp_path),
        geoblocked=False,
        now=NOW,
    )

    assert record.state is LiveV2State.ACCEPTED
    assert record.remote_order_id == "order-1"
    assert record.signed_fingerprint is not None
    assert record.signed_order_json is not None
    stored_signed = json.loads(record.signed_order_json)
    assert "signature" not in stored_signed
    assert stored_signed["signature_sha256"] == hashlib.sha256(b"0xsignature").hexdigest()
    assert json.loads(record.response_json or "{}")["trade_ids"] == ["trade-1"]
    assert client.create_calls == 1
    assert client.post_calls == 1

    with sqlite3.connect(database) as connection:
        transitions = connection.execute(
            "SELECT from_state,to_state FROM live_v2_transitions ORDER BY id"
        ).fetchall()
    assert transitions == [
        (None, "PREPARED"),
        ("PREPARED", "SIGNING"),
        ("SIGNING", "SIGNED"),
        ("SIGNED", "SUBMITTING"),
        ("SUBMITTING", "ACCEPTED"),
    ]


@pytest.mark.parametrize(
    ("configure", "message", "last_error"),
    [
        (
            lambda client: setattr(client, "post_error", TimeoutError("response lost")),
            "submission is ambiguous",
            "TimeoutError",
        ),
        (
            lambda client: setattr(client, "response", FakeResponse(trade_ids=())),
            "indeterminate POST response",
            "lacks a matched order id and fill ids",
        ),
    ],
)
def test_timeout_or_malformed_response_becomes_ambiguous_without_retry(
    tmp_path: Path,
    configure: Callable[[FakeClient], None],
    message: str,
    last_error: str,
) -> None:
    value = _intent()
    client = FakeClient()
    configure(client)
    journal = LiveV2Journal(tmp_path / "polybot.sqlite3")
    executor = LiveV2Executor(journal)
    authorization = _loaded_authorization(tmp_path)

    with pytest.raises(LivePilotError, match=message):
        executor.execute(
            client=client,
            intent=value,
            decision_id=1,
            authorization=authorization,
            geoblocked=False,
            now=NOW,
        )

    record = journal.get(value.digest)
    assert record.state is LiveV2State.AMBIGUOUS
    assert last_error in str(record.last_error)
    assert client.create_calls == 1
    assert client.post_calls == 1

    client.post_error = None
    client.response = FakeResponse()
    with pytest.raises(LivePilotError, match="automatic retry is forbidden"):
        executor.execute(
            client=client,
            intent=value,
            decision_id=1,
            authorization=authorization,
            geoblocked=False,
            now=NOW,
        )
    assert client.create_calls == 1
    assert client.post_calls == 1


def test_reconciliation_treats_buy_fill_as_open_and_requires_confirmed_close(
    tmp_path: Path,
) -> None:
    value = _intent()
    client = FakeClient()
    journal = LiveV2Journal(tmp_path / "polybot.sqlite3")
    accepted = LiveV2Executor(journal).execute(
        client=client,
        intent=value,
        decision_id=1,
        authorization=_loaded_authorization(tmp_path),
        geoblocked=False,
        now=NOW,
    )
    assert accepted.submitted_at_utc is not None

    client.trades = [FakeTrade(id="trade-1")]
    opened = reconcile_live_v2(journal, client)
    assert opened is not None
    assert opened.state is LiveV2State.POSITION_OPEN

    valid_closed_position = SimpleNamespace(
        asset_id="token-1",
        condition_id="condition-1",
        timestamp=accepted.submitted_at_utc + timedelta(minutes=1),
        realized_pnl=Decimal("0.42"),
    )
    client.closed_positions = [valid_closed_position]
    still_open = reconcile_live_v2(journal, client)
    assert still_open is not None
    assert still_open.state is LiveV2State.POSITION_OPEN

    client.market_closed = True
    client.closed_positions = [
        SimpleNamespace(
            asset_id="token-1",
            condition_id="condition-1",
            timestamp=accepted.submitted_at_utc - timedelta(seconds=1),
            realized_pnl=Decimal("99"),
        )
    ]
    still_open = reconcile_live_v2(journal, client)
    assert still_open is not None
    assert still_open.state is LiveV2State.POSITION_OPEN

    client.closed_positions = [valid_closed_position]
    closed = reconcile_live_v2(journal, client)
    assert closed is not None
    assert closed.state is LiveV2State.CLOSED
    assert closed.realized_pnl_usd == Decimal("0.42")
    assert closed.closed_local_day is not None


def test_signed_validation_failure_never_releases_day(tmp_path: Path) -> None:
    value = _intent()
    client = FakeClient()
    client.signed = replace(client.signed, maker_amount=2_100_000)
    journal = LiveV2Journal(tmp_path / "polybot.sqlite3")

    with pytest.raises(LivePilotError, match="signing boundary requires manual review"):
        LiveV2Executor(journal).execute(
            client=client,
            intent=value,
            decision_id=1,
            authorization=_loaded_authorization(tmp_path),
            geoblocked=False,
            now=NOW,
        )

    record = journal.get(value.digest)
    assert record.state is LiveV2State.MANUAL_REVIEW
    assert record.consumes_daily_limit
    assert client.create_calls == 1
    assert client.post_calls == 0
    with pytest.raises(LivePilotError, match="invalid live-v2 transition"):
        journal.mark_pre_sign_rejected(value.digest, LivePilotError("not unsigned"))


def test_signing_boundary_is_single_owner_and_crash_safe(tmp_path: Path) -> None:
    journal = LiveV2Journal(tmp_path / "polybot.sqlite3")
    value = _intent()
    journal.reserve(intent=value, decision_id=1, timezone=LIVE_V2_TIMEZONE, now=NOW)
    journal.mark_signing(value.digest)

    with pytest.raises(LivePilotError, match="invalid live-v2 transition"):
        journal.mark_signing(value.digest)
    with pytest.raises(LivePilotError, match="invalid live-v2 transition"):
        journal.mark_pre_sign_rejected(value.digest, LivePilotError("not unsigned"))

    record = reconcile_live_v2(journal, FakeClient())
    assert record is not None
    assert record.state is LiveV2State.MANUAL_REVIEW
    assert record.consumes_daily_limit


def test_posted_rejection_still_consumes_day_and_allows_next_day(tmp_path: Path) -> None:
    journal = LiveV2Journal(tmp_path / "polybot.sqlite3")
    value = _intent()
    record = journal.reserve(intent=value, decision_id=1, timezone=LIVE_V2_TIMEZONE, now=NOW)
    journal.mark_signing(value.digest)
    journal.save_signed(value.digest, asdict(FakeSignedOrder()))
    journal.mark_submitting(value.digest)
    journal.mark_response(
        value.digest,
        FakeResponse(ok=False, order_id=None, code="fok_not_filled", message="not filled"),
    )

    assert journal.get(value.digest).consumes_daily_limit
    assert journal.charged_count_for_day(record.local_day) == 1
    same_day = journal.reserve(
        intent=_intent(event_id="same-day"),
        decision_id=2,
        timezone=LIVE_V2_TIMEZONE,
        now=NOW,
    )
    assert same_day.local_day == record.local_day
    assert journal.charged_count_for_day(record.local_day) == 2
    journal.mark_pre_sign_rejected(same_day.intent_sha256, LivePilotError("terminal"))
    next_record = journal.reserve(
        intent=_intent(event_id="next-day"),
        decision_id=3,
        timezone=LIVE_V2_TIMEZONE,
        now=NOW + timedelta(days=1),
    )
    assert next_record.local_day != record.local_day


def test_legacy_migration_preserves_history_and_does_not_release_old_rejections(
    tmp_path: Path,
) -> None:
    source = LiveV2Journal(tmp_path / "source.sqlite3")
    value = _intent()
    source.reserve(intent=value, decision_id=1, timezone=LIVE_V2_TIMEZONE, now=NOW)
    source.mark_pre_sign_rejected(value.digest, LivePilotError("old book changed"))
    database = tmp_path / "legacy.sqlite3"
    with sqlite3.connect(source.path) as old, sqlite3.connect(database) as legacy:
        schema = old.execute(
            "SELECT sql FROM sqlite_master WHERE name='live_v2_attempts'"
        ).fetchone()[0]
        # Recreate the real pre-fix implicit UNIQUE constraint, with no charge
        # column or durable signing-boundary evidence.
        schema = schema[: schema.index(",\n                    consumes_daily_limit")] + ")"
        schema = schema.replace("local_day TEXT NOT NULL,", "local_day TEXT NOT NULL UNIQUE,")
        legacy.execute(schema)
        columns = [row[1] for row in legacy.execute("PRAGMA table_info(live_v2_attempts)")]
        rows = old.execute(f"SELECT {','.join(columns)} FROM live_v2_attempts").fetchall()
        placeholders = ",".join("?" for _ in columns)
        legacy.executemany(f"INSERT INTO live_v2_attempts VALUES ({placeholders})", rows)
        transition_schema = old.execute(
            "SELECT sql FROM sqlite_master WHERE name='live_v2_transitions'"
        ).fetchone()[0]
        legacy.execute(transition_schema)
        transitions = old.execute("SELECT * FROM live_v2_transitions ORDER BY id").fetchall()
        legacy.executemany("INSERT INTO live_v2_transitions VALUES (?,?,?,?,?,?)", transitions)

    upgraded = LiveV2Journal(database)
    upgraded_again = LiveV2Journal(database)
    record = upgraded_again.get(value.digest)
    assert record.state is LiveV2State.PRE_SIGN_REJECTED
    assert record.consumes_daily_limit  # The old state alone was not proof of no signature.
    assert upgraded.charged_count_for_day(record.local_day) >= 1
    with sqlite3.connect(database) as check:
        assert (
            check.execute("SELECT * FROM live_v2_transitions ORDER BY id").fetchall() == transitions
        )
        assert check.execute(f"SELECT {','.join(columns)} FROM live_v2_attempts").fetchall() == rows
        assert check.execute("PRAGMA foreign_key_check").fetchall() == []


def test_unsigned_retry_preserves_both_attempts_and_consumes_no_cash(tmp_path: Path) -> None:
    journal = LiveV2Journal(tmp_path / "polybot.sqlite3")
    client = FakeClient()
    client.asks = (SimpleNamespace(price=Decimal("0.90"), size=Decimal("100")),)
    authorization = _loaded_authorization(tmp_path)
    for index in (1, 2):
        result = LiveV2Executor(journal).execute(
            client=client,
            intent=_intent(event_id=f"event-{index}"),
            decision_id=index,
            authorization=authorization,
            geoblocked=False,
            now=NOW,
        )
        assert result.state is LiveV2State.PRE_SIGN_REJECTED
        assert result.submitted_at_utc is None
        assert not result.consumes_daily_limit

    assert client.create_calls == client.post_calls == 0
    assert journal.summary()["states"] == {"PRE_SIGN_REJECTED": 2}
