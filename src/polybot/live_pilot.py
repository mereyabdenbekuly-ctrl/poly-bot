from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import stat
from collections.abc import Iterable, Mapping
from dataclasses import asdict, dataclass, fields
from datetime import UTC, datetime
from decimal import Decimal
from enum import StrEnum
from pathlib import Path
from typing import Any, Protocol, cast

PILOT_MAX_WALLET_USD = Decimal("10.00")
PILOT_MAX_BUY_USD = Decimal("2.00")
PILOT_MAX_BUY_NOTIONAL_USD = Decimal("1.90")
PILOT_MAX_FEE_RATE = Decimal("0.05")
COLLATERAL_BASE_UNITS = Decimal(1_000_000)
APPROVAL_KIND = "polybot-live-pilot-buy-v1"


class LivePilotError(RuntimeError):
    """Fail-closed live-pilot validation or lifecycle error."""


class LiveIntentState(StrEnum):
    PREPARED = "PREPARED"
    SIGNED = "SIGNED"
    SUBMITTING = "SUBMITTING"
    AMBIGUOUS = "AMBIGUOUS"
    ACCEPTED = "ACCEPTED"
    REJECTED = "REJECTED"
    RECONCILED_FILLED = "RECONCILED_FILLED"
    RECONCILED_OPEN = "RECONCILED_OPEN"
    MANUAL_REVIEW = "MANUAL_REVIEW"


@dataclass(frozen=True, slots=True)
class LiveBuyIntent:
    """Exact one-shot BUY authorized separately from the paper decision lane."""

    strategy: str
    event_id: str
    market_id: str
    condition_id: str
    token_id: str
    amount_usd: Decimal
    max_spend_usd: Decimal
    max_price: Decimal
    book_hash: str
    decision_created_at_utc: datetime
    expires_at_utc: datetime
    order_type: str = "FOK"
    side: str = "BUY"

    def __post_init__(self) -> None:
        if self.side != "BUY":
            raise LivePilotError("pilot permits BUY only")
        if self.order_type != "FOK":
            raise LivePilotError("pilot requires FOK so no resting order is left behind")
        if not self.strategy:
            raise LivePilotError("strategy is required")
        for name in ("event_id", "market_id", "condition_id", "token_id", "book_hash"):
            if not str(getattr(self, name)).strip():
                raise LivePilotError(f"{name} is required")
        if self.amount_usd <= 0:
            raise LivePilotError("amount_usd must be positive")
        _usd_base_units_exact(self.amount_usd, field="amount_usd")
        _usd_base_units_exact(self.max_spend_usd, field="max_spend_usd")
        if self.amount_usd > PILOT_MAX_BUY_NOTIONAL_USD:
            raise LivePilotError("pilot BUY notional cap is $1.90 before fees")
        if self.max_spend_usd < self.amount_usd:
            raise LivePilotError("max_spend_usd cannot be below amount_usd")
        if self.max_spend_usd > PILOT_MAX_BUY_USD:
            raise LivePilotError("pilot BUY cap is $2.00 including the signed spend")
        if not Decimal("0") < self.max_price < Decimal("1"):
            raise LivePilotError("max_price must be strictly between 0 and 1")
        if _as_utc(self.decision_created_at_utc) >= _as_utc(self.expires_at_utc):
            raise LivePilotError("intent must expire after the decision time")

    @property
    def digest(self) -> str:
        return _sha256_json(_intent_payload(self))

    @property
    def idempotency_key(self) -> str:
        return f"live-pilot-v1:{self.digest}"


@dataclass(frozen=True, slots=True)
class LiveApproval:
    kind: str
    intent_sha256: str
    expires_at_utc: datetime
    jurisdiction_confirmed: bool
    max_wallet_balance_usd: Decimal
    max_total_spend_usd: Decimal


@dataclass(frozen=True, slots=True)
class LiveIntentRecord:
    id: int
    idempotency_key: str
    intent_sha256: str
    state: LiveIntentState
    intent_json: str
    signed_fingerprint: str | None
    signed_order_json: str | None
    remote_order_id: str | None
    response_json: str | None
    last_error: str | None
    created_at_utc: datetime
    updated_at_utc: datetime
    submitted_at_utc: datetime | None
    reconciled_at_utc: datetime | None


class PaginatorLike(Protocol):
    def iter_items(self) -> Iterable[Any]: ...


class SecureTradingClient(Protocol):
    @property
    def wallet(self) -> object: ...

    def get_balance_allowance(self, *, asset_type: str) -> Any: ...

    def list_open_orders(self, **kwargs: Any) -> PaginatorLike: ...

    def list_positions(self, **kwargs: Any) -> PaginatorLike: ...

    def get_market(self, *, id: str) -> Any: ...

    def get_order_book(self, *, token_id: str) -> Any: ...

    def create_market_order(self, **kwargs: Any) -> Any: ...

    def post_order(self, signed_order: Any) -> Any: ...

    def list_account_trades(self, **kwargs: Any) -> PaginatorLike: ...


class LivePilotJournal:
    """Crash-safe intent journal in the existing SQLite database."""

    def __init__(self, database_path: Path) -> None:
        self.path = database_path.expanduser().resolve()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path, timeout=30)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA journal_mode = WAL")
        connection.execute("PRAGMA foreign_keys = ON")
        return connection

    def _initialize(self) -> None:
        with self._connect() as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS live_order_intents (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    idempotency_key TEXT NOT NULL UNIQUE,
                    intent_sha256 TEXT NOT NULL UNIQUE,
                    state TEXT NOT NULL,
                    intent_json TEXT NOT NULL,
                    signed_fingerprint TEXT UNIQUE,
                    signed_order_json TEXT,
                    remote_order_id TEXT UNIQUE,
                    response_json TEXT,
                    last_error TEXT,
                    created_at_utc TEXT NOT NULL,
                    updated_at_utc TEXT NOT NULL,
                    submitted_at_utc TEXT,
                    reconciled_at_utc TEXT
                );

                CREATE TABLE IF NOT EXISTS live_order_transitions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    intent_id INTEGER NOT NULL REFERENCES live_order_intents(id),
                    from_state TEXT,
                    to_state TEXT NOT NULL,
                    created_at_utc TEXT NOT NULL,
                    detail_json TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS live_order_fills (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    intent_id INTEGER NOT NULL REFERENCES live_order_intents(id),
                    trade_id TEXT NOT NULL,
                    order_id TEXT,
                    token_id TEXT NOT NULL,
                    price TEXT NOT NULL,
                    size TEXT NOT NULL,
                    status TEXT NOT NULL,
                    matched_at_utc TEXT,
                    payload_json TEXT NOT NULL,
                    UNIQUE(intent_id, trade_id)
                );
                """
            )

    def reserve(self, intent: LiveBuyIntent) -> LiveIntentRecord:
        now = _now().isoformat()
        payload = _canonical_json(_intent_payload(intent))
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            existing = connection.execute(
                "SELECT * FROM live_order_intents WHERE idempotency_key = ?",
                (intent.idempotency_key,),
            ).fetchone()
            if existing is not None:
                if existing["intent_json"] != payload:
                    raise LivePilotError("idempotency key collision with different intent")
                connection.commit()
                return _record(existing)
            cursor = connection.execute(
                """
                INSERT INTO live_order_intents(
                    idempotency_key,intent_sha256,state,intent_json,created_at_utc,updated_at_utc
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    intent.idempotency_key,
                    intent.digest,
                    LiveIntentState.PREPARED.value,
                    payload,
                    now,
                    now,
                ),
            )
            intent_id = int(cursor.lastrowid or 0)
            self._transition_row(
                connection,
                intent_id=intent_id,
                from_state=None,
                to_state=LiveIntentState.PREPARED,
                detail={"intent_sha256": intent.digest},
                now=now,
            )
            row = connection.execute(
                "SELECT * FROM live_order_intents WHERE id = ?", (intent_id,)
            ).fetchone()
            connection.commit()
            assert row is not None
            return _record(row)
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def get(self, intent_sha256: str) -> LiveIntentRecord:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM live_order_intents WHERE intent_sha256 = ?",
                (intent_sha256,),
            ).fetchone()
        if row is None:
            raise LivePilotError("live intent is not reserved")
        return _record(row)

    def unresolved(self) -> list[LiveIntentRecord]:
        terminal = (
            LiveIntentState.REJECTED.value,
            LiveIntentState.RECONCILED_FILLED.value,
        )
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT * FROM live_order_intents WHERE state NOT IN (?, ?) ORDER BY id",
                terminal,
            ).fetchall()
        return [_record(row) for row in rows]

    def save_signed(self, intent_sha256: str, signed_payload: Mapping[str, Any]) -> None:
        full_payload = _canonical_json(signed_payload)
        fingerprint = hashlib.sha256(full_payload.encode()).hexdigest()
        stored_payload = dict(signed_payload)
        signature = str(stored_payload.pop("signature", ""))
        if not signature:
            raise LivePilotError("signed order is missing its signature")
        stored_payload["signature_sha256"] = hashlib.sha256(signature.encode()).hexdigest()
        self._move(
            intent_sha256,
            expected={LiveIntentState.PREPARED},
            target=LiveIntentState.SIGNED,
            updates={
                "signed_fingerprint": fingerprint,
                "signed_order_json": _canonical_json(stored_payload),
            },
            detail={"signed_fingerprint": fingerprint},
        )

    def mark_submitting(self, intent_sha256: str) -> None:
        self._move(
            intent_sha256,
            expected={LiveIntentState.SIGNED},
            target=LiveIntentState.SUBMITTING,
            updates={"submitted_at_utc": _now().isoformat()},
            detail={},
        )

    def mark_ambiguous(self, intent_sha256: str, error: BaseException) -> None:
        self._move(
            intent_sha256,
            expected={LiveIntentState.SUBMITTING},
            target=LiveIntentState.AMBIGUOUS,
            updates={"last_error": _safe_error(error)},
            detail={"error": _safe_error(error)},
        )

    def mark_response(self, intent_sha256: str, response: Any) -> None:
        accepted = bool(getattr(response, "ok", False))
        state = LiveIntentState.ACCEPTED if accepted else LiveIntentState.REJECTED
        payload = _model_payload(response)
        remote_order_id = str(getattr(response, "order_id", "")) or None
        self._move(
            intent_sha256,
            expected={LiveIntentState.SUBMITTING},
            target=state,
            updates={
                "remote_order_id": remote_order_id,
                "response_json": _canonical_json(payload),
            },
            detail={"response": payload},
        )

    def mark_reconciled(
        self,
        intent_sha256: str,
        *,
        state: LiveIntentState,
        fills: Iterable[Any] = (),
        remote_order_id: str | None = None,
        detail: Mapping[str, Any] | None = None,
    ) -> None:
        if state not in {
            LiveIntentState.RECONCILED_FILLED,
            LiveIntentState.RECONCILED_OPEN,
            LiveIntentState.MANUAL_REVIEW,
        }:
            raise LivePilotError("invalid reconciliation state")
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT * FROM live_order_intents WHERE intent_sha256 = ?",
                (intent_sha256,),
            ).fetchone()
            if row is None:
                raise LivePilotError("live intent is not reserved")
            current = LiveIntentState(row["state"])
            if current not in {
                LiveIntentState.SUBMITTING,
                LiveIntentState.AMBIGUOUS,
                LiveIntentState.ACCEPTED,
                LiveIntentState.RECONCILED_OPEN,
                LiveIntentState.MANUAL_REVIEW,
            }:
                raise LivePilotError(f"cannot reconcile live intent in state {current}")
            now = _now().isoformat()
            for fill in fills:
                payload = _model_payload(fill)
                trade_id = str(getattr(fill, "id", payload.get("id", "")))
                if not trade_id:
                    raise LivePilotError("trade without id cannot be journaled")
                connection.execute(
                    """
                    INSERT OR IGNORE INTO live_order_fills(
                        intent_id,trade_id,order_id,token_id,price,size,status,
                        matched_at_utc,payload_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        int(row["id"]),
                        trade_id,
                        _trade_order_id(fill),
                        str(getattr(fill, "asset_id", getattr(fill, "token_id", ""))),
                        str(getattr(fill, "price", "")),
                        str(getattr(fill, "size", "")),
                        str(getattr(fill, "status", "")),
                        _optional_datetime(getattr(fill, "matched_at", None)),
                        _canonical_json(payload),
                    ),
                )
            connection.execute(
                """
                UPDATE live_order_intents
                SET state = ?, remote_order_id = COALESCE(remote_order_id, ?),
                    updated_at_utc = ?, reconciled_at_utc = ?
                WHERE id = ?
                """,
                (state.value, remote_order_id, now, now, int(row["id"])),
            )
            self._transition_row(
                connection,
                intent_id=int(row["id"]),
                from_state=current,
                to_state=state,
                detail=dict(detail or {}),
                now=now,
            )
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def _move(
        self,
        intent_sha256: str,
        *,
        expected: set[LiveIntentState],
        target: LiveIntentState,
        updates: Mapping[str, Any],
        detail: Mapping[str, Any],
    ) -> None:
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT * FROM live_order_intents WHERE intent_sha256 = ?",
                (intent_sha256,),
            ).fetchone()
            if row is None:
                raise LivePilotError("live intent is not reserved")
            current = LiveIntentState(row["state"])
            if current not in expected:
                raise LivePilotError(f"refusing duplicate transition from {current} to {target}")
            now = _now().isoformat()
            values = {**updates, "state": target.value, "updated_at_utc": now}
            allowed = {
                "state",
                "updated_at_utc",
                "signed_fingerprint",
                "signed_order_json",
                "remote_order_id",
                "response_json",
                "last_error",
                "submitted_at_utc",
            }
            if not set(values).issubset(allowed):
                raise LivePilotError("unsafe journal update")
            assignments = ", ".join(f"{key} = ?" for key in values)
            connection.execute(
                f"UPDATE live_order_intents SET {assignments} WHERE id = ?",  # noqa: S608
                (*values.values(), int(row["id"])),
            )
            self._transition_row(
                connection,
                intent_id=int(row["id"]),
                from_state=current,
                to_state=target,
                detail=dict(detail),
                now=now,
            )
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    @staticmethod
    def _transition_row(
        connection: sqlite3.Connection,
        *,
        intent_id: int,
        from_state: LiveIntentState | None,
        to_state: LiveIntentState,
        detail: Mapping[str, Any],
        now: str,
    ) -> None:
        connection.execute(
            """
            INSERT INTO live_order_transitions(
                intent_id,from_state,to_state,created_at_utc,detail_json
            ) VALUES (?, ?, ?, ?, ?)
            """,
            (
                intent_id,
                None if from_state is None else from_state.value,
                to_state.value,
                now,
                _canonical_json(detail),
            ),
        )


class LivePilotExecutor:
    """One-shot FOK executor; it never retries an ambiguous network submission."""

    def __init__(self, journal: LivePilotJournal) -> None:
        self.journal = journal

    def execute_once(
        self,
        *,
        client: SecureTradingClient,
        intent: LiveBuyIntent,
        approval_path: Path,
        geoblocked: bool,
        now: datetime | None = None,
    ) -> LiveIntentRecord:
        current_time = _as_utc(now or _now())
        approval = load_live_approval(approval_path, intent=intent, now=current_time)
        if geoblocked:
            raise LivePilotError("network is geoblocked; VPS location must not bypass eligibility")
        if not approval.jurisdiction_confirmed:
            raise LivePilotError("user/account jurisdiction eligibility is not confirmed")
        if current_time >= _as_utc(intent.expires_at_utc):
            raise LivePilotError("live intent expired before signing")

        record = self.journal.reserve(intent)
        if record.state is not LiveIntentState.PREPARED:
            raise LivePilotError(
                f"intent already reached {record.state}; reconcile it and never submit again"
            )
        other = [
            item
            for item in self.journal.unresolved()
            if item.intent_sha256 != intent.digest
        ]
        if other:
            raise LivePilotError("another live intent is unresolved")

        balance = client.get_balance_allowance(asset_type="COLLATERAL")
        balance_units = int(balance.balance)
        max_wallet_units = int(PILOT_MAX_WALLET_USD * COLLATERAL_BASE_UNITS)
        if balance_units > max_wallet_units:
            raise LivePilotError("pilot wallet collateral balance exceeds $10.00")

        if any(True for _ in client.list_open_orders().iter_items()):
            raise LivePilotError("pilot wallet already has an open order")
        positions = client.list_positions(user=str(client.wallet)).iter_items()
        if any(_position_is_open(value) for value in positions):
            raise LivePilotError("pilot wallet already has an open position")

        market = client.get_market(id=intent.market_id)
        fee_rate, fee_exponent = _verify_market_identity(market, intent)
        book = client.get_order_book(token_id=intent.token_id)
        _verify_book_identity(book, intent)

        signed = client.create_market_order(
            token_id=intent.token_id,
            side="BUY",
            amount=intent.amount_usd,
            max_spend=intent.max_spend_usd,
            max_price=intent.max_price,
            order_type="FOK",
        )
        _verify_signed_order(
            signed,
            intent,
            balance_units=balance_units,
            allowances=balance.allowances,
            fee_rate=fee_rate,
            fee_exponent=fee_exponent,
        )
        signed_payload = _signed_payload(signed)
        self.journal.save_signed(intent.digest, signed_payload)
        self.journal.mark_submitting(intent.digest)
        # Burn the exact one-shot authorization before crossing the network
        # boundary. A crash here is deliberately fail-closed and requires a
        # new explicit approval; it must never make the same payload retryable.
        _consume_approval(approval_path, intent.digest)

        try:
            response = client.post_order(signed)
        except BaseException as error:
            self.journal.mark_ambiguous(intent.digest, error)
            raise LivePilotError(
                "submission result is ambiguous; it was journaled and must never be retried"
            ) from error
        self.journal.mark_response(intent.digest, response)
        return self.journal.get(intent.digest)

    def reconcile(self, *, client: SecureTradingClient, intent: LiveBuyIntent) -> LiveIntentRecord:
        record = self.journal.get(intent.digest)
        if record.state in {LiveIntentState.REJECTED, LiveIntentState.RECONCILED_FILLED}:
            return record
        if record.state is LiveIntentState.PREPARED:
            return record
        if record.state is LiveIntentState.SIGNED:
            self.journal.mark_reconciled(
                intent.digest,
                state=LiveIntentState.MANUAL_REVIEW,
                detail={"reason": "signed_but_never_marked_submitting"},
            )
            return self.journal.get(intent.digest)

        open_orders = [
            item
            for item in client.list_open_orders(token_id=intent.token_id).iter_items()
            if _order_matches(item, intent, remote_order_id=record.remote_order_id)
        ]
        trades = [
            item
            for item in client.list_account_trades(
                token_id=intent.token_id,
                after=record.submitted_at_utc.isoformat() if record.submitted_at_utc else None,
            ).iter_items()
            if _trade_matches(item, intent, remote_order_id=record.remote_order_id)
        ]
        if trades:
            self.journal.mark_reconciled(
                intent.digest,
                state=LiveIntentState.RECONCILED_FILLED,
                fills=trades,
                remote_order_id=record.remote_order_id or _trade_order_id(trades[0]),
                detail={"trade_ids": [str(getattr(item, "id", "")) for item in trades]},
            )
        elif open_orders:
            self.journal.mark_reconciled(
                intent.digest,
                state=LiveIntentState.RECONCILED_OPEN,
                remote_order_id=record.remote_order_id or str(getattr(open_orders[0], "id", "")),
                detail={"open_order_ids": [str(getattr(item, "id", "")) for item in open_orders]},
            )
        else:
            self.journal.mark_reconciled(
                intent.digest,
                state=LiveIntentState.MANUAL_REVIEW,
                detail={"reason": "no_matching_open_order_or_trade; automatic retry forbidden"},
            )
        return self.journal.get(intent.digest)


def load_live_approval(path: Path, *, intent: LiveBuyIntent, now: datetime) -> LiveApproval:
    candidate = path.expanduser()
    if candidate.is_symlink():
        raise LivePilotError("approval must not be a symlink")
    resolved = candidate.resolve(strict=True)
    _require_private_regular_file(resolved)
    try:
        raw = json.loads(resolved.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise LivePilotError("invalid live approval sidecar") from error
    if not isinstance(raw, dict):
        raise LivePilotError("live approval must be a JSON object")
    expected = {
        "kind",
        "intent_sha256",
        "expires_at_utc",
        "jurisdiction_confirmed",
        "max_wallet_balance_usd",
        "max_total_spend_usd",
    }
    if set(raw) != expected:
        raise LivePilotError("live approval fields do not match the fixed v1 schema")
    try:
        approval = LiveApproval(
            kind=str(raw["kind"]),
            intent_sha256=str(raw["intent_sha256"]),
            expires_at_utc=_parse_datetime(raw["expires_at_utc"]),
            jurisdiction_confirmed=raw["jurisdiction_confirmed"] is True,
            max_wallet_balance_usd=Decimal(str(raw["max_wallet_balance_usd"])),
            max_total_spend_usd=Decimal(str(raw["max_total_spend_usd"])),
        )
    except (KeyError, ValueError, TypeError) as error:
        raise LivePilotError("invalid live approval values") from error
    if approval.kind != APPROVAL_KIND:
        raise LivePilotError("wrong live approval kind")
    if approval.intent_sha256 != intent.digest:
        raise LivePilotError("live approval is not bound to this exact intent")
    if _as_utc(now) >= _as_utc(approval.expires_at_utc):
        raise LivePilotError("live approval expired")
    if approval.max_wallet_balance_usd != PILOT_MAX_WALLET_USD:
        raise LivePilotError("live approval cannot raise or alter the $10 wallet cap")
    if approval.max_total_spend_usd != PILOT_MAX_BUY_USD:
        raise LivePilotError("live approval cannot raise or alter the $2 BUY cap")
    return approval


def _require_private_regular_file(path: Path) -> None:
    info = path.lstat()
    if stat.S_ISLNK(info.st_mode) or not stat.S_ISREG(info.st_mode):
        raise LivePilotError("approval must be a regular non-symlink file")
    if info.st_uid not in {0, os.geteuid()}:
        raise LivePilotError("approval owner is not trusted")
    if stat.S_IMODE(info.st_mode) & 0o077:
        raise LivePilotError("approval must not be readable or writable by group/others")


def _consume_approval(path: Path, intent_sha256: str) -> None:
    candidate = path.expanduser()
    if candidate.is_symlink():
        raise LivePilotError("approval must not be a symlink")
    resolved = candidate.resolve(strict=True)
    used = resolved.with_name(f"{resolved.name}.used-{intent_sha256[:12]}")
    if used.exists():
        raise LivePilotError("approval was already consumed")
    resolved.replace(used)


def _verify_market_identity(
    market: Any, intent: LiveBuyIntent
) -> tuple[Decimal, Decimal]:
    if str(getattr(market, "id", "")) != intent.market_id:
        raise LivePilotError("market id changed")
    condition = getattr(market, "condition_id", None)
    if str(condition or "") != intent.condition_id:
        raise LivePilotError("condition id changed")
    state = getattr(market, "state", None)
    if state is not None and not bool(getattr(state, "accepting_orders", False)):
        raise LivePilotError("market no longer accepts orders")
    outcomes = getattr(market, "outcomes", None)
    tokens = {
        str(getattr(getattr(outcomes, side, None), "token_id", ""))
        for side in ("yes", "no")
    }
    if intent.token_id not in tokens:
        raise LivePilotError("token no longer belongs to the selected market")
    try:
        fee_rate = Decimal(str(market.fee_rate))
        fee_exponent = Decimal(str(market.fee_exponent))
    except (AttributeError, ValueError) as error:
        raise LivePilotError("market fee metadata is unavailable") from error
    if fee_rate < 0 or fee_exponent < 0:
        raise LivePilotError("market fee metadata is invalid")
    if fee_rate > PILOT_MAX_FEE_RATE:
        raise LivePilotError("market fee rate exceeds the pilot's 5% safety bound")
    if fee_rate > 0 and fee_exponent < 1:
        raise LivePilotError("market fee exponent is outside the pilot safety model")
    return fee_rate, fee_exponent


def _verify_book_identity(book: Any, intent: LiveBuyIntent) -> None:
    asset_id = str(getattr(book, "asset_id", getattr(book, "token_id", "")))
    condition_id = str(getattr(book, "condition_id", ""))
    if asset_id != intent.token_id or condition_id != intent.condition_id:
        raise LivePilotError("order book identity mismatch")
    if str(getattr(book, "hash", "")) != intent.book_hash:
        raise LivePilotError("order book changed; a new decision and approval are required")


def _verify_signed_order(
    signed: Any,
    intent: LiveBuyIntent,
    *,
    balance_units: int,
    allowances: Mapping[str, int],
    fee_rate: Decimal,
    fee_exponent: Decimal,
) -> None:
    if str(getattr(signed, "side", "")) != "BUY":
        raise LivePilotError("SDK signed a non-BUY order")
    if str(getattr(signed, "order_type", "")) != "FOK":
        raise LivePilotError("SDK signed a non-FOK order")
    builder = str(getattr(signed, "builder", "")).lower()
    if builder not in {"0x" + "00" * 32, "0" * 64}:
        raise LivePilotError("builder fees are not permitted by the pilot")
    if str(getattr(signed, "token_id", "")) != intent.token_id:
        raise LivePilotError("SDK signed the wrong token")
    maker_amount = int(signed.maker_amount)
    if maker_amount <= 0:
        raise LivePilotError("signed maker amount is not positive")
    intent_max_units = _usd_base_units_exact(intent.max_spend_usd, field="max_spend_usd")
    if maker_amount > intent_max_units:
        raise LivePilotError("signed maker amount exceeds the approved intent maximum")
    intent_amount_units = _usd_base_units_exact(intent.amount_usd, field="amount_usd")
    if maker_amount > intent_amount_units:
        raise LivePilotError("signed maker amount exceeds the approved BUY notional")
    if maker_amount > int(PILOT_MAX_BUY_USD * COLLATERAL_BASE_UNITS):
        raise LivePilotError("signed maker amount exceeds the exact $2 cap")
    if maker_amount > balance_units:
        raise LivePilotError("pilot wallet balance is below signed spend")
    if not allowances or max(int(value) for value in allowances.values()) < maker_amount:
        raise LivePilotError(
            "collateral allowance is insufficient; executor refuses automatic unlimited approval"
        )
    taker_amount = int(getattr(signed, "taker_amount", 0))
    if taker_amount <= 0:
        raise LivePilotError("signed taker amount is not positive")
    signed_price = Decimal(maker_amount) / Decimal(taker_amount)
    if signed_price > intent.max_price:
        raise LivePilotError("signed effective price exceeds the approved intent maximum")
    # For the platform curve fee = shares * rate * (p * (1-p))**exponent.
    # When exponent >= 1, fee/notional is bounded by rate for every p in
    # (0, 1).  A $1.90 notional and rate <= 5% therefore remain below $2.00,
    # including the conservative worst-case platform fee. Builder code is not
    # accepted by this executor, so no builder fee is possible.
    maker_usd = Decimal(maker_amount) / COLLATERAL_BASE_UNITS
    worst_fee = Decimal(0) if fee_rate == 0 else maker_usd * fee_rate
    if maker_usd + worst_fee > intent.max_spend_usd:
        raise LivePilotError("signed notional plus worst-case fee exceeds the $2 pilot cap")


def _position_is_open(position: Any) -> bool:
    size = getattr(position, "size", None)
    return size is not None and Decimal(str(size)) > 0


def _order_matches(order: Any, intent: LiveBuyIntent, *, remote_order_id: str | None) -> bool:
    if remote_order_id and str(getattr(order, "id", "")) != remote_order_id:
        return False
    asset = str(getattr(order, "asset_id", getattr(order, "token_id", "")))
    return asset == intent.token_id and str(getattr(order, "side", "")) == "BUY"


def _trade_matches(trade: Any, intent: LiveBuyIntent, *, remote_order_id: str | None) -> bool:
    asset = str(getattr(trade, "asset_id", getattr(trade, "token_id", "")))
    if asset != intent.token_id or str(getattr(trade, "side", "")) != "BUY":
        return False
    if not remote_order_id:
        return True
    if str(getattr(trade, "taker_order_id", "")) == remote_order_id:
        return True
    return any(
        str(getattr(value, "order_id", "")) == remote_order_id
        for value in getattr(trade, "maker_orders", ())
    )


def _trade_order_id(trade: Any) -> str | None:
    value = str(getattr(trade, "taker_order_id", ""))
    if value:
        return value
    makers = tuple(getattr(trade, "maker_orders", ()))
    if not makers:
        return None
    return str(getattr(makers[0], "order_id", "")) or None


def _intent_payload(intent: LiveBuyIntent) -> dict[str, Any]:
    payload = asdict(intent)
    return {key: _json_value(value) for key, value in payload.items()}


def _signed_payload(signed: Any) -> dict[str, Any]:
    names = (
        [field.name for field in fields(signed)]
        if hasattr(signed, "__dataclass_fields__")
        else []
    )
    if not names:
        raise LivePilotError("SDK signed order does not expose a stable dataclass payload")
    return {name: _json_value(getattr(signed, name)) for name in names}


def _model_payload(value: Any) -> dict[str, Any]:
    if hasattr(value, "model_dump"):
        return cast(dict[str, Any], value.model_dump(mode="json"))
    if hasattr(value, "__dataclass_fields__"):
        return {field.name: _json_value(getattr(value, field.name)) for field in fields(value)}
    if isinstance(value, Mapping):
        return {str(key): _json_value(item) for key, item in value.items()}
    return {"value": str(value)}


def _record(row: sqlite3.Row) -> LiveIntentRecord:
    return LiveIntentRecord(
        id=int(row["id"]),
        idempotency_key=str(row["idempotency_key"]),
        intent_sha256=str(row["intent_sha256"]),
        state=LiveIntentState(row["state"]),
        intent_json=str(row["intent_json"]),
        signed_fingerprint=row["signed_fingerprint"],
        signed_order_json=row["signed_order_json"],
        remote_order_id=row["remote_order_id"],
        response_json=row["response_json"],
        last_error=row["last_error"],
        created_at_utc=_parse_datetime(row["created_at_utc"]),
        updated_at_utc=_parse_datetime(row["updated_at_utc"]),
        submitted_at_utc=_optional_parse_datetime(row["submitted_at_utc"]),
        reconciled_at_utc=_optional_parse_datetime(row["reconciled_at_utc"]),
    )


def _canonical_json(value: Mapping[str, Any]) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _sha256_json(value: Mapping[str, Any]) -> str:
    return hashlib.sha256(_canonical_json(value).encode()).hexdigest()


def _json_value(value: Any) -> Any:
    if isinstance(value, datetime):
        return _as_utc(value).isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, StrEnum):
        return value.value
    if isinstance(value, tuple | list):
        return [_json_value(item) for item in value]
    if isinstance(value, Mapping):
        return {str(key): _json_value(item) for key, item in value.items()}
    return value


def _parse_datetime(value: Any) -> datetime:
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    return _as_utc(parsed)


def _optional_parse_datetime(value: Any) -> datetime | None:
    return None if value is None else _parse_datetime(value)


def _optional_datetime(value: Any) -> str | None:
    return None if value is None else _as_utc(value).isoformat()


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise LivePilotError("timestamps must be timezone-aware")
    return value.astimezone(UTC)


def _now() -> datetime:
    return datetime.now(UTC)


def _usd_base_units_exact(value: Decimal, *, field: str) -> int:
    units = value * COLLATERAL_BASE_UNITS
    integral = units.to_integral_value()
    if units != integral:
        raise LivePilotError(f"{field} must use at most six decimal places")
    return int(integral)


def _safe_error(error: BaseException) -> str:
    return f"{type(error).__name__}: {str(error)[:500]}"


__all__ = [
    "APPROVAL_KIND",
    "LiveApproval",
    "LiveBuyIntent",
    "LiveIntentRecord",
    "LiveIntentState",
    "LivePilotError",
    "LivePilotExecutor",
    "LivePilotJournal",
    "PILOT_MAX_BUY_USD",
    "PILOT_MAX_BUY_NOTIONAL_USD",
    "PILOT_MAX_WALLET_USD",
    "load_live_approval",
]
