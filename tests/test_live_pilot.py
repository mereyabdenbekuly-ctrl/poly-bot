from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from polybot.live_pilot import (
    APPROVAL_KIND,
    LiveBuyIntent,
    LiveIntentState,
    LivePilotError,
    LivePilotExecutor,
    LivePilotJournal,
)

NOW = datetime(2026, 9, 16, 16, 0, tzinfo=UTC)


class FakePaginator:
    def __init__(self, items: list[Any] | None = None) -> None:
        self.items = items or []

    def iter_items(self):  # noqa: ANN201
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
    asset_id: str
    side: str
    taker_order_id: str
    price: Decimal
    size: Decimal
    status: str
    matched_at: datetime
    maker_orders: tuple[Any, ...] = ()


class FakeResponse:
    def __init__(self, *, ok: bool = True, order_id: str = "order-1") -> None:
        self.ok = ok
        self.order_id = order_id if ok else None
        self.code = None if ok else "fok_not_filled"

    def model_dump(self, *, mode: str) -> dict[str, Any]:
        assert mode == "json"
        return {"ok": self.ok, "order_id": self.order_id, "code": self.code}


class FakeClient:
    wallet = "0x2222222222222222222222222222222222222222"

    def __init__(self) -> None:
        self.balance = 5_000_000
        self.allowances = {"exchange": 2_000_000}
        self.open_orders: list[Any] = []
        self.positions: list[Any] = []
        self.trades: list[Any] = []
        self.post_calls = 0
        self.create_calls = 0
        self.post_error: BaseException | None = None
        self.response = FakeResponse()
        self.signed = FakeSignedOrder()
        self.last_create_kwargs: dict[str, Any] | None = None

    def get_balance_allowance(self, *, asset_type: str) -> Any:
        assert asset_type == "COLLATERAL"
        return SimpleNamespace(balance=self.balance, allowances=self.allowances)

    def list_open_orders(self, **kwargs: Any) -> FakePaginator:
        return FakePaginator(self.open_orders)

    def list_positions(self, **kwargs: Any) -> FakePaginator:
        return FakePaginator(self.positions)

    def get_market(self, *, id: str) -> Any:
        assert id == "market-1"
        outcomes = SimpleNamespace(
            yes=SimpleNamespace(token_id="token-1"),
            no=SimpleNamespace(token_id="token-no"),
        )
        return SimpleNamespace(
            id="market-1",
            condition_id="condition-1",
            state=SimpleNamespace(accepting_orders=True),
            outcomes=outcomes,
        )

    def get_order_book(self, *, token_id: str) -> Any:
        assert token_id == "token-1"
        return SimpleNamespace(
            asset_id="token-1",
            condition_id="condition-1",
            hash="book-1",
        )

    def create_market_order(self, **kwargs: Any) -> FakeSignedOrder:
        self.create_calls += 1
        self.last_create_kwargs = kwargs
        assert kwargs["token_id"] == "token-1"
        assert kwargs["side"] == "BUY"
        assert kwargs["amount"] == Decimal("1.90")
        assert kwargs["order_type"] == "FOK"
        return self.signed

    def post_order(self, signed_order: Any) -> FakeResponse:
        assert signed_order is self.signed
        self.post_calls += 1
        if self.post_error:
            raise self.post_error
        return self.response

    def list_account_trades(self, **kwargs: Any) -> FakePaginator:
        return FakePaginator(self.trades)


def intent(**updates: Any) -> LiveBuyIntent:
    values: dict[str, Any] = {
        "strategy": "open-meteo-truncated-normal-v1",
        "event_id": "event-1",
        "market_id": "market-1",
        "condition_id": "condition-1",
        "token_id": "token-1",
        "amount_usd": Decimal("1.90"),
        "max_spend_usd": Decimal("2.00"),
        "max_price": Decimal("0.55"),
        "book_hash": "book-1",
        "decision_created_at_utc": NOW,
        "expires_at_utc": NOW + timedelta(minutes=10),
    }
    values.update(updates)
    return LiveBuyIntent(**values)


def approval(path: Path, value: LiveBuyIntent, **updates: Any) -> Path:
    payload: dict[str, Any] = {
        "kind": APPROVAL_KIND,
        "intent_sha256": value.digest,
        "expires_at_utc": (NOW + timedelta(minutes=5)).isoformat(),
        "jurisdiction_confirmed": True,
        "max_wallet_balance_usd": "10.00",
        "max_total_spend_usd": "2.00",
    }
    payload.update(updates)
    path.write_text(json.dumps(payload), encoding="utf-8")
    path.chmod(0o600)
    return path


def test_intent_hard_caps_cannot_be_raised() -> None:
    with pytest.raises(LivePilotError, match=r"\$2.00"):
        intent(max_spend_usd=Decimal("2.01"))
    with pytest.raises(LivePilotError, match="FOK"):
        intent(order_type="FAK")
    with pytest.raises(LivePilotError, match="BUY"):
        intent(side="SELL")


def test_success_is_one_shot_and_approval_is_burned(tmp_path: Path) -> None:
    value = intent()
    approval_path = approval(tmp_path / "approval.json", value)
    journal = LivePilotJournal(tmp_path / "polybot.sqlite3")
    executor = LivePilotExecutor(journal)
    client = FakeClient()

    result = executor.execute_once(
        client=client,
        intent=value,
        approval_path=approval_path,
        geoblocked=False,
        now=NOW + timedelta(minutes=1),
    )

    assert result.state is LiveIntentState.ACCEPTED
    assert result.remote_order_id == "order-1"
    assert result.signed_fingerprint is not None
    assert result.signed_order_json is not None
    stored_signed = json.loads(result.signed_order_json)
    assert "signature" not in stored_signed
    assert len(stored_signed["signature_sha256"]) == 64
    assert client.create_calls == 1
    assert client.post_calls == 1
    assert not approval_path.exists()
    assert list(tmp_path.glob("approval.json.used-*"))

    replacement = approval(tmp_path / "approval-2.json", value)
    with pytest.raises(LivePilotError, match="already reached"):
        executor.execute_once(
            client=client,
            intent=value,
            approval_path=replacement,
            geoblocked=False,
            now=NOW + timedelta(minutes=2),
        )
    assert client.post_calls == 1


def test_timeout_after_acceptance_is_never_retried_and_reconciles(tmp_path: Path) -> None:
    value = intent()
    approval_path = approval(tmp_path / "approval.json", value)
    journal = LivePilotJournal(tmp_path / "polybot.sqlite3")
    executor = LivePilotExecutor(journal)
    client = FakeClient()
    client.post_error = TimeoutError("response lost")

    with pytest.raises(LivePilotError, match="ambiguous"):
        executor.execute_once(
            client=client,
            intent=value,
            approval_path=approval_path,
            geoblocked=False,
            now=NOW + timedelta(minutes=1),
        )
    assert journal.get(value.digest).state is LiveIntentState.AMBIGUOUS
    assert client.post_calls == 1
    assert not approval_path.exists()

    client.post_error = None
    replacement = approval(tmp_path / "approval-2.json", value)
    with pytest.raises(LivePilotError, match="already reached"):
        executor.execute_once(
            client=client,
            intent=value,
            approval_path=replacement,
            geoblocked=False,
            now=NOW + timedelta(minutes=2),
        )
    assert client.post_calls == 1

    client.trades = [
        FakeTrade(
            id="trade-1",
            asset_id="token-1",
            side="BUY",
            taker_order_id="remote-after-timeout",
            price=Decimal("0.50"),
            size=Decimal("3.8"),
            status="CONFIRMED",
            matched_at=NOW + timedelta(minutes=1),
        )
    ]
    reconciled = executor.reconcile(client=client, intent=value)
    assert reconciled.state is LiveIntentState.RECONCILED_FILLED
    assert reconciled.remote_order_id == "remote-after-timeout"


@pytest.mark.parametrize(
    ("mutate", "message"),
    [
        (lambda client: setattr(client, "balance", 10_000_001), r"exceeds \$10\.00"),
        (lambda client: client.open_orders.append(SimpleNamespace()), "open order"),
        (
            lambda client: client.positions.append(SimpleNamespace(size=Decimal("1"))),
            "open position",
        ),
        (lambda client: setattr(client, "allowances", {"exchange": 1}), "allowance"),
    ],
)
def test_preflight_blocks_risk_before_post(
    tmp_path: Path,
    mutate: Any,
    message: str,
) -> None:
    value = intent()
    approval_path = approval(tmp_path / "approval.json", value)
    journal = LivePilotJournal(tmp_path / "polybot.sqlite3")
    client = FakeClient()
    mutate(client)

    with pytest.raises(LivePilotError, match=message):
        LivePilotExecutor(journal).execute_once(
            client=client,
            intent=value,
            approval_path=approval_path,
            geoblocked=False,
            now=NOW + timedelta(minutes=1),
        )
    assert client.post_calls == 0


def test_geoblock_and_jurisdiction_fail_closed(tmp_path: Path) -> None:
    value = intent()
    journal = LivePilotJournal(tmp_path / "polybot.sqlite3")
    client = FakeClient()
    blocked = approval(tmp_path / "blocked.json", value)
    with pytest.raises(LivePilotError, match="geoblocked"):
        LivePilotExecutor(journal).execute_once(
            client=client,
            intent=value,
            approval_path=blocked,
            geoblocked=True,
            now=NOW + timedelta(minutes=1),
        )

    not_eligible = approval(
        tmp_path / "not-eligible.json",
        value,
        jurisdiction_confirmed=False,
    )
    with pytest.raises(LivePilotError, match="eligibility"):
        LivePilotExecutor(journal).execute_once(
            client=client,
            intent=value,
            approval_path=not_eligible,
            geoblocked=False,
            now=NOW + timedelta(minutes=1),
        )
    assert client.post_calls == 0


def test_private_approval_file_is_required(tmp_path: Path) -> None:
    value = intent()
    path = approval(tmp_path / "approval.json", value)
    path.chmod(0o644)
    with pytest.raises(LivePilotError, match="group/others"):
        LivePilotExecutor(LivePilotJournal(tmp_path / "polybot.sqlite3")).execute_once(
            client=FakeClient(),
            intent=value,
            approval_path=path,
            geoblocked=False,
            now=NOW + timedelta(minutes=1),
        )


def test_changed_book_requires_a_new_decision_and_approval(tmp_path: Path) -> None:
    value = intent(book_hash="different")
    path = approval(tmp_path / "approval.json", value)
    client = FakeClient()
    with pytest.raises(LivePilotError, match="order book changed"):
        LivePilotExecutor(LivePilotJournal(tmp_path / "polybot.sqlite3")).execute_once(
            client=client,
            intent=value,
            approval_path=path,
            geoblocked=False,
            now=NOW + timedelta(minutes=1),
        )
    assert client.post_calls == 0


@pytest.mark.parametrize(
    ("value", "signed", "message"),
    [
        (
            intent(max_spend_usd=Decimal("1.95")),
            FakeSignedOrder(maker_amount=1_960_000),
            "approved intent maximum",
        ),
        (
            intent(max_price=Decimal("0.49")),
            FakeSignedOrder(maker_amount=1_900_000, taker_amount=3_800_000),
            "effective price",
        ),
    ],
)
def test_signed_payload_cannot_exceed_exact_intent(
    tmp_path: Path,
    value: LiveBuyIntent,
    signed: FakeSignedOrder,
    message: str,
) -> None:
    path = approval(tmp_path / "approval.json", value)
    client = FakeClient()
    client.signed = signed
    with pytest.raises(LivePilotError, match=message):
        LivePilotExecutor(LivePilotJournal(tmp_path / "polybot.sqlite3")).execute_once(
            client=client,
            intent=value,
            approval_path=path,
            geoblocked=False,
            now=NOW + timedelta(minutes=1),
        )
    assert client.post_calls == 0


def test_usd_values_must_match_collateral_precision() -> None:
    with pytest.raises(LivePilotError, match="six decimal places"):
        intent(amount_usd=Decimal("1.9000001"))
