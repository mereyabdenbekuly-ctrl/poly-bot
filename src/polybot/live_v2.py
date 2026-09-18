from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import stat
import time
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from enum import StrEnum
from pathlib import Path
from typing import Any, cast
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from polybot.config import Settings
from polybot.geoblock import fetch_geoblock_status
from polybot.live_pilot import (
    COLLATERAL_BASE_UNITS,
    PILOT_MAX_BUY_NOTIONAL_USD,
    PILOT_MAX_BUY_USD,
    PILOT_MAX_WALLET_USD,
    LiveBuyIntent,
    LivePilotError,
    _canonical_json,
    _model_payload,
    _position_is_open,
    _safe_error,
    _signed_payload,
    _trade_matches,
    _verify_market_identity,
    _verify_signed_order,
)
from polybot.live_pilot_runtime import (
    LIVE_V2_PRICE_DRIFT_USD,
    _fok_buy_limit_from_book,
    load_live_credentials,
    open_live_client,
    preview_intent_from_decision,
)

LIVE_V2_AUTHORIZATION_KIND = "polybot-live-v2-authorization-v1"
LIVE_V2_STRATEGY = "open-meteo-truncated-normal-v1"
LIVE_V2_TIMEZONE = "Asia/Almaty"
LIVE_V2_DAILY_STOP_USD = Decimal("6.00")
LIVE_V2_MAX_ORDERS_PER_DAY = 50
LIVE_V2_MIN_PROBABILITY_EDGE = Decimal("0.05")
LIVE_V2_MIN_EXPECTED_PROFIT_USD = Decimal("0.15")
LIVE_V2_MIDNIGHT_GUARD_SECONDS = 120
LIVE_V2_RECONCILE_GRACE = timedelta(minutes=15)
LIVE_V2_BALANCE_POLL_SECONDS = float(300)


class LiveV2State(StrEnum):
    PREPARED = "PREPARED"
    PRE_SIGN_REJECTED = "PRE_SIGN_REJECTED"
    SIGNING = "SIGNING"
    SIGNED = "SIGNED"
    SUBMITTING = "SUBMITTING"
    AMBIGUOUS = "AMBIGUOUS"
    ACCEPTED = "ACCEPTED"
    REJECTED = "REJECTED"
    POSITION_OPEN = "POSITION_OPEN"
    CLOSED = "CLOSED"
    MANUAL_REVIEW = "MANUAL_REVIEW"


ACTIVE_STATES = frozenset(
    {
        LiveV2State.PREPARED,
        LiveV2State.SIGNING,
        LiveV2State.SIGNED,
        LiveV2State.SUBMITTING,
        LiveV2State.AMBIGUOUS,
        LiveV2State.ACCEPTED,
        LiveV2State.POSITION_OPEN,
        LiveV2State.MANUAL_REVIEW,
    }
)


@dataclass(frozen=True, slots=True)
class LiveV2Authorization:
    kind: str
    wallet: str
    strategy: str
    side: str
    order_type: str
    one_position_at_a_time: bool
    max_orders_per_day: int
    daily_stop_loss_usd: Decimal
    daily_timezone: str
    max_wallet_balance_usd: Decimal
    max_buy_notional_usd: Decimal
    max_total_spend_usd: Decimal
    min_probability_edge: Decimal
    min_expected_profit_usd: Decimal
    jurisdiction_confirmed: bool
    authorized_at_utc: datetime
    expires_at_utc: datetime


@dataclass(frozen=True, slots=True)
class LiveV2Candidate:
    decision_id: int
    expected_profit_usd: Decimal
    probability_edge: Decimal
    created_at_utc: datetime


@dataclass(frozen=True, slots=True)
class LiveV2Record:
    id: int
    local_day: str
    decision_id: int
    intent_sha256: str
    state: LiveV2State
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
    closed_at_utc: datetime | None
    closed_local_day: str | None
    realized_pnl_usd: Decimal | None
    consumes_daily_limit: bool

    @property
    def intent(self) -> LiveBuyIntent:
        return _intent_from_json(self.intent_json)


def load_live_v2_authorization(
    path: Path,
    *,
    now: datetime | None = None,
) -> LiveV2Authorization:
    resolved = _private_file(path, label="live-v2 authorization")
    try:
        raw = json.loads(resolved.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise LivePilotError("invalid live-v2 authorization sidecar") from error
    if not isinstance(raw, dict):
        raise LivePilotError("live-v2 authorization must be a JSON object")
    expected = {
        "kind",
        "wallet",
        "strategy",
        "side",
        "order_type",
        "one_position_at_a_time",
        "max_orders_per_day",
        "daily_stop_loss_usd",
        "daily_timezone",
        "max_wallet_balance_usd",
        "max_buy_notional_usd",
        "max_total_spend_usd",
        "min_probability_edge",
        "min_expected_profit_usd",
        "jurisdiction_confirmed",
        "authorized_at_utc",
        "expires_at_utc",
    }
    if set(raw) != expected:
        raise LivePilotError("live-v2 authorization fields do not match the fixed v1 schema")
    try:
        authorization = LiveV2Authorization(
            kind=str(raw["kind"]),
            wallet=str(raw["wallet"]),
            strategy=str(raw["strategy"]),
            side=str(raw["side"]),
            order_type=str(raw["order_type"]),
            one_position_at_a_time=raw["one_position_at_a_time"] is True,
            max_orders_per_day=int(raw["max_orders_per_day"]),
            daily_stop_loss_usd=Decimal(str(raw["daily_stop_loss_usd"])),
            daily_timezone=str(raw["daily_timezone"]),
            max_wallet_balance_usd=Decimal(str(raw["max_wallet_balance_usd"])),
            max_buy_notional_usd=Decimal(str(raw["max_buy_notional_usd"])),
            max_total_spend_usd=Decimal(str(raw["max_total_spend_usd"])),
            min_probability_edge=Decimal(str(raw["min_probability_edge"])),
            min_expected_profit_usd=Decimal(str(raw["min_expected_profit_usd"])),
            jurisdiction_confirmed=raw["jurisdiction_confirmed"] is True,
            authorized_at_utc=_parse_time(raw["authorized_at_utc"]),
            expires_at_utc=_parse_time(raw["expires_at_utc"]),
        )
    except (TypeError, ValueError) as error:
        raise LivePilotError("invalid live-v2 authorization values") from error
    if authorization.kind != LIVE_V2_AUTHORIZATION_KIND:
        raise LivePilotError("wrong live-v2 authorization kind")
    if not authorization.wallet.strip():
        raise LivePilotError("live-v2 authorization wallet is required")
    if authorization.strategy != LIVE_V2_STRATEGY:
        raise LivePilotError("live-v2 is fixed to the existing v1 strategy")
    if authorization.side != "BUY" or authorization.order_type != "FOK":
        raise LivePilotError("live-v2 permits only FOK BUY orders")
    if not authorization.one_position_at_a_time:
        raise LivePilotError("live-v2 requires one_position_at_a_time=true")
    if authorization.max_orders_per_day != LIVE_V2_MAX_ORDERS_PER_DAY:
        raise LivePilotError("live-v2 permits at most fifty order attempts per local day")
    if authorization.daily_stop_loss_usd != LIVE_V2_DAILY_STOP_USD:
        raise LivePilotError("live-v2 daily stop must remain $6.00")
    if authorization.daily_timezone != LIVE_V2_TIMEZONE:
        raise LivePilotError("live-v2 daily timezone must remain Asia/Almaty")
    if authorization.max_wallet_balance_usd != PILOT_MAX_WALLET_USD:
        raise LivePilotError("live-v2 cannot alter the $10 wallet cap")
    if authorization.max_buy_notional_usd != PILOT_MAX_BUY_NOTIONAL_USD:
        raise LivePilotError("live-v2 cannot alter the $1.90 BUY cap")
    if authorization.max_total_spend_usd != PILOT_MAX_BUY_USD:
        raise LivePilotError("live-v2 cannot alter the $2 all-in cap")
    if authorization.min_probability_edge != LIVE_V2_MIN_PROBABILITY_EDGE:
        raise LivePilotError("live-v2 probability edge gate must remain 0.08")
    if authorization.min_expected_profit_usd != LIVE_V2_MIN_EXPECTED_PROFIT_USD:
        raise LivePilotError("live-v2 expected-profit gate must remain $0.15")
    if not authorization.jurisdiction_confirmed:
        raise LivePilotError("user/account jurisdiction eligibility is not confirmed")
    if authorization.authorized_at_utc >= authorization.expires_at_utc:
        raise LivePilotError("live-v2 authorization expiry must follow authorization time")
    current = (now or datetime.now(UTC)).astimezone(UTC)
    if current < authorization.authorized_at_utc:
        raise LivePilotError("live-v2 authorization is not active yet")
    if current >= authorization.expires_at_utc:
        raise LivePilotError("live-v2 authorization expired")
    return authorization


class LiveV2Journal:
    def __init__(self, database: Path) -> None:
        self.path = database.expanduser().resolve()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._last_heartbeat_monotonic = 0.0
        self._last_heartbeat_state: str | None = None
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
                CREATE TABLE IF NOT EXISTS live_v2_attempts(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    local_day TEXT NOT NULL,
                    decision_id INTEGER NOT NULL UNIQUE,
                    intent_sha256 TEXT NOT NULL UNIQUE,
                    idempotency_key TEXT NOT NULL UNIQUE,
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
                    reconciled_at_utc TEXT,
                    closed_at_utc TEXT,
                    closed_local_day TEXT,
                    realized_pnl_usd TEXT,
                    consumes_daily_limit INTEGER NOT NULL DEFAULT 1
                        CHECK(consumes_daily_limit IN (0, 1))
                );

                CREATE TABLE IF NOT EXISTS live_v2_transitions(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    attempt_id INTEGER NOT NULL REFERENCES live_v2_attempts(id),
                    from_state TEXT,
                    to_state TEXT NOT NULL,
                    created_at_utc TEXT NOT NULL,
                    detail_json TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS live_v2_runtime(
                    singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
                    state TEXT NOT NULL,
                    detail_json TEXT NOT NULL,
                    updated_at_utc TEXT NOT NULL
                );

                """
            )
            columns = {
                str(row["name"])
                for row in connection.execute("PRAGMA table_info(live_v2_attempts)")
            }
            for name, declaration in (
                ("closed_at_utc", "TEXT"),
                ("closed_local_day", "TEXT"),
                ("realized_pnl_usd", "TEXT"),
            ):
                if name not in columns:
                    connection.execute(
                        f"ALTER TABLE live_v2_attempts ADD COLUMN {name} {declaration}"  # noqa: S608
                    )
            connection.execute(
                "CREATE INDEX IF NOT EXISTS live_v2_attempts_closed_day_idx "
                "ON live_v2_attempts(closed_local_day)"
            )

        self._migrate_daily_limit()

    def _migrate_daily_limit(self) -> None:
        """Keep old rows charged; release only provably unsigned new failures.

        Old PRE_SIGN_REJECTED rows may have signed before writing that state.
        Their history and daily charge must not be inferred away on upgrade.
        """

        connection = self._connect()
        try:
            # Rebuild the old day-UNIQUE parent without renaming it first;
            # dependent transition rows keep referring to live_v2_attempts.
            connection.execute("PRAGMA foreign_keys = OFF")
            connection.execute("BEGIN IMMEDIATE")
            columns = {
                str(row["name"])
                for row in connection.execute("PRAGMA table_info(live_v2_attempts)")
            }
            if "consumes_daily_limit" not in columns:
                connection.execute(
                    """
                    CREATE TABLE live_v2_attempts_migrating(
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        local_day TEXT NOT NULL,
                        decision_id INTEGER NOT NULL UNIQUE,
                        intent_sha256 TEXT NOT NULL UNIQUE,
                        idempotency_key TEXT NOT NULL UNIQUE,
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
                        reconciled_at_utc TEXT,
                        closed_at_utc TEXT,
                        closed_local_day TEXT,
                        realized_pnl_usd TEXT,
                        consumes_daily_limit INTEGER NOT NULL DEFAULT 1
                            CHECK(consumes_daily_limit IN (0, 1))
                    )
                    """
                )
                names = (
                    "id,local_day,decision_id,intent_sha256,idempotency_key,state,"
                    "intent_json,signed_fingerprint,signed_order_json,remote_order_id,"
                    "response_json,last_error,created_at_utc,updated_at_utc,"
                    "submitted_at_utc,reconciled_at_utc,closed_at_utc,closed_local_day,"
                    "realized_pnl_usd"
                )
                connection.execute(
                    f"INSERT INTO live_v2_attempts_migrating({names}) "  # noqa: S608
                    f"SELECT {names} FROM live_v2_attempts"
                )
                connection.execute("DROP TABLE live_v2_attempts")
                connection.execute(
                    "ALTER TABLE live_v2_attempts_migrating RENAME TO live_v2_attempts"
                )
            connection.execute("DROP INDEX IF EXISTS live_v2_charged_day_idx")
            connection.execute(
                "CREATE INDEX IF NOT EXISTS live_v2_charged_day_idx "
                "ON live_v2_attempts(local_day) WHERE consumes_daily_limit = 1"
            )
            connection.execute(
                "CREATE INDEX IF NOT EXISTS live_v2_attempts_closed_day_idx "
                "ON live_v2_attempts(closed_local_day)"
            )
            if connection.execute("PRAGMA foreign_key_check").fetchone() is not None:
                raise LivePilotError("daily-limit migration failed its foreign-key check")
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
        finally:
            connection.execute("PRAGMA foreign_keys = ON")
            connection.close()

    def heartbeat(self, state: str, *, detail: Mapping[str, Any] | None = None) -> None:
        now_monotonic = time.monotonic()
        if (
            state == self._last_heartbeat_state
            and now_monotonic - self._last_heartbeat_monotonic < 30
        ):
            return
        now = datetime.now(UTC).isoformat()
        with self._connect() as connection:
            connection.execute(
                """
                INSERT INTO live_v2_runtime(singleton,state,detail_json,updated_at_utc)
                VALUES (1, ?, ?, ?)
                ON CONFLICT(singleton) DO UPDATE SET
                    state=excluded.state,
                    detail_json=excluded.detail_json,
                    updated_at_utc=excluded.updated_at_utc
                """,
                (state, _canonical_json(detail or {}), now),
            )
        self._last_heartbeat_state = state
        self._last_heartbeat_monotonic = now_monotonic

    def reserve(
        self,
        *,
        intent: LiveBuyIntent,
        decision_id: int,
        timezone: str,
        now: datetime | None = None,
    ) -> LiveV2Record:
        current = (now or datetime.now(UTC)).astimezone(UTC)
        local_day = _local_day(current, timezone)
        now_text = current.isoformat()
        payload = _intent_json(intent)
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            existing = connection.execute(
                "SELECT * FROM live_v2_attempts WHERE intent_sha256 = ?",
                (intent.digest,),
            ).fetchone()
            if existing is not None:
                return _record(existing)
            active = connection.execute(
                "SELECT state FROM live_v2_attempts ORDER BY id DESC"
            ).fetchall()
            if any(LiveV2State(str(row["state"])) in ACTIVE_STATES for row in active):
                raise LivePilotError("an earlier live-v2 attempt still requires resolution")
            charged_today = int(
                connection.execute(
                    "SELECT COUNT(*) FROM live_v2_attempts "
                    "WHERE local_day = ? AND consumes_daily_limit = 1",
                    (local_day,),
                ).fetchone()[0]
            )
            if charged_today >= LIVE_V2_MAX_ORDERS_PER_DAY:
                raise LivePilotError("live-v2 daily order-attempt limit is already consumed")
            realized_today = connection.execute(
                "SELECT TOTAL(realized_pnl_usd) FROM live_v2_attempts "
                "WHERE closed_local_day = ? AND realized_pnl_usd IS NOT NULL",
                (local_day,),
            ).fetchone()[0]
            if Decimal(str(realized_today)) <= -LIVE_V2_DAILY_STOP_USD:
                raise LivePilotError("live-v2 daily stop is active after realized losses today")
            cursor = connection.execute(
                """
                INSERT INTO live_v2_attempts(
                    local_day,decision_id,intent_sha256,idempotency_key,state,intent_json,
                    created_at_utc,updated_at_utc
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    local_day,
                    decision_id,
                    intent.digest,
                    f"live-v2:{intent.digest}",
                    LiveV2State.PREPARED.value,
                    payload,
                    now_text,
                    now_text,
                ),
            )
            attempt_id = int(cursor.lastrowid or 0)
            self._transition(
                connection,
                attempt_id=attempt_id,
                old=None,
                new=LiveV2State.PREPARED,
                detail={"decision_id": decision_id, "intent_sha256": intent.digest},
                now=now_text,
            )
            row = connection.execute(
                "SELECT * FROM live_v2_attempts WHERE id = ?", (attempt_id,)
            ).fetchone()
            connection.commit()
            assert row is not None
            return _record(row)
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def charged_count_for_day(self, local_day: str) -> int:
        with self._connect() as connection:
            count = connection.execute(
                "SELECT COUNT(*) FROM live_v2_attempts "
                "WHERE local_day = ? AND consumes_daily_limit = 1",
                (local_day,),
            ).fetchone()[0]
        return int(count)

    def daily_loss_stop_hit(self, local_day: str) -> bool:
        with self._connect() as connection:
            realized = connection.execute(
                "SELECT TOTAL(realized_pnl_usd) FROM live_v2_attempts "
                "WHERE closed_local_day = ? AND realized_pnl_usd IS NOT NULL",
                (local_day,),
            ).fetchone()[0]
        return Decimal(str(realized)) <= -LIVE_V2_DAILY_STOP_USD

    def decision_attempted(self, decision_id: int) -> bool:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT 1 FROM live_v2_attempts WHERE decision_id = ?", (decision_id,)
            ).fetchone()
        return row is not None

    def latest_active(self) -> LiveV2Record | None:
        placeholders = ",".join("?" for _ in ACTIVE_STATES)
        with self._connect() as connection:
            row = connection.execute(
                f"SELECT * FROM live_v2_attempts WHERE state IN ({placeholders}) "  # noqa: S608
                "ORDER BY id DESC LIMIT 1",
                tuple(state.value for state in ACTIVE_STATES),
            ).fetchone()
        return None if row is None else _record(row)

    def get(self, intent_sha256: str) -> LiveV2Record:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM live_v2_attempts WHERE intent_sha256 = ?",
                (intent_sha256,),
            ).fetchone()
        if row is None:
            raise LivePilotError("live-v2 intent is not reserved")
        return _record(row)

    def mark_pre_sign_rejected(self, digest: str, error: BaseException) -> None:
        self._move(
            digest,
            expected={LiveV2State.PREPARED},
            target=LiveV2State.PRE_SIGN_REJECTED,
            updates={"last_error": _safe_error(error), "consumes_daily_limit": 0},
            detail={"error": _safe_error(error)},
        )

    def mark_signing(self, digest: str) -> None:
        self._move(
            digest,
            expected={LiveV2State.PREPARED},
            target=LiveV2State.SIGNING,
            updates={},
            detail={},
        )

    def save_signed(self, digest: str, signed: Mapping[str, Any]) -> None:
        full = _canonical_json(signed)
        fingerprint = hashlib.sha256(full.encode()).hexdigest()
        stored = dict(signed)
        signature = str(stored.pop("signature", ""))
        if not signature:
            raise LivePilotError("signed order is missing its signature")
        stored["signature_sha256"] = hashlib.sha256(signature.encode()).hexdigest()
        self._move(
            digest,
            expected={LiveV2State.SIGNING},
            target=LiveV2State.SIGNED,
            updates={
                "signed_fingerprint": fingerprint,
                "signed_order_json": _canonical_json(stored),
            },
            detail={"signed_fingerprint": fingerprint},
        )

    def mark_submitting(self, digest: str) -> None:
        self._move(
            digest,
            expected={LiveV2State.SIGNED},
            target=LiveV2State.SUBMITTING,
            updates={"submitted_at_utc": datetime.now(UTC).isoformat()},
            detail={},
        )

    def mark_exchange_rejected(self, digest: str, error: BaseException) -> None:
        self._move(
            digest,
            expected={LiveV2State.SUBMITTING},
            target=LiveV2State.REJECTED,
            updates={"last_error": _safe_error(error)},
            detail={"error": _safe_error(error)},
        )

    def mark_ambiguous(self, digest: str, error: BaseException) -> None:
        self._move(
            digest,
            expected={LiveV2State.SUBMITTING},
            target=LiveV2State.AMBIGUOUS,
            updates={"last_error": _safe_error(error)},
            detail={"error": _safe_error(error)},
        )

    def mark_response(self, digest: str, response: Any) -> None:
        ok = getattr(response, "ok", None)
        payload = _model_payload(response)
        if ok is True:
            remote = str(getattr(response, "order_id", "")) or None
            status = str(getattr(response, "status", ""))
            trade_ids = tuple(str(value) for value in getattr(response, "trade_ids", ()))
            if remote is not None and status == "matched" and trade_ids:
                state = LiveV2State.ACCEPTED
            elif status in {"live", ""} and remote is None and not trade_ids:
                remote = None
                state = LiveV2State.REJECTED
            else:
                raise LivePilotError("accepted FOK response lacks a matched order id and fill ids")
        elif ok is False:
            code = str(getattr(response, "code", ""))
            message = str(getattr(response, "message", ""))
            if not code or not message:
                raise LivePilotError("rejected order response lacks a definitive reason")
            remote = None
            state = LiveV2State.REJECTED
        else:
            raise LivePilotError("order response has no definitive acceptance result")
        self._move(
            digest,
            expected={LiveV2State.SUBMITTING},
            target=state,
            updates={
                "remote_order_id": remote,
                "response_json": _canonical_json(payload),
            },
            detail={"response": payload},
        )

    def mark_position_open(self, digest: str, *, detail: Mapping[str, Any]) -> None:
        self._move(
            digest,
            expected={
                LiveV2State.ACCEPTED,
                LiveV2State.SUBMITTING,
                LiveV2State.AMBIGUOUS,
                LiveV2State.POSITION_OPEN,
            },
            target=LiveV2State.POSITION_OPEN,
            updates={"reconciled_at_utc": datetime.now(UTC).isoformat()},
            detail=detail,
            allow_same=True,
        )

    def mark_closed(
        self,
        digest: str,
        *,
        timezone: str,
        realized_pnl_usd: Decimal | None,
        detail: Mapping[str, Any],
    ) -> None:
        now = datetime.now(UTC)
        self._move(
            digest,
            expected={LiveV2State.POSITION_OPEN},
            target=LiveV2State.CLOSED,
            updates={
                "reconciled_at_utc": now.isoformat(),
                "closed_at_utc": now.isoformat(),
                "closed_local_day": _local_day(now, timezone),
                "realized_pnl_usd": None if realized_pnl_usd is None else str(realized_pnl_usd),
            },
            detail=detail,
        )

    def mark_manual_review(self, digest: str, *, detail: Mapping[str, Any]) -> None:
        self._move(
            digest,
            expected=ACTIVE_STATES,
            target=LiveV2State.MANUAL_REVIEW,
            updates={"reconciled_at_utc": datetime.now(UTC).isoformat()},
            detail=detail,
            allow_same=True,
        )

    def summary(self) -> dict[str, Any]:
        with self._connect() as connection:
            states = {
                str(row["state"]): int(row["count"])
                for row in connection.execute(
                    "SELECT state,COUNT(*) AS count FROM live_v2_attempts GROUP BY state"
                )
            }
            latest = connection.execute(
                "SELECT * FROM live_v2_attempts ORDER BY id DESC LIMIT 1"
            ).fetchone()
            runtime = connection.execute(
                "SELECT state,detail_json,updated_at_utc FROM live_v2_runtime WHERE singleton=1"
            ).fetchone()
        return {
            "states": states,
            "runtime": None if runtime is None else dict(runtime),
            "latest": None
            if latest is None
            else {
                "local_day": str(latest["local_day"]),
                "decision_id": int(latest["decision_id"]),
                "intent_sha256": str(latest["intent_sha256"]),
                "state": str(latest["state"]),
                "remote_order_id": latest["remote_order_id"],
                "closed_at_utc": latest["closed_at_utc"],
                "closed_local_day": latest["closed_local_day"],
                "realized_pnl_usd": latest["realized_pnl_usd"],
                "consumes_daily_limit": bool(latest["consumes_daily_limit"]),
                "updated_at_utc": str(latest["updated_at_utc"]),
            },
        }

    def _move(
        self,
        digest: str,
        *,
        expected: set[LiveV2State] | frozenset[LiveV2State],
        target: LiveV2State,
        updates: Mapping[str, Any],
        detail: Mapping[str, Any],
        allow_same: bool = False,
    ) -> None:
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT * FROM live_v2_attempts WHERE intent_sha256 = ?", (digest,)
            ).fetchone()
            if row is None:
                raise LivePilotError("live-v2 intent is not reserved")
            current = LiveV2State(str(row["state"]))
            if current == target and allow_same:
                connection.commit()
                return
            if current not in expected:
                raise LivePilotError(f"invalid live-v2 transition from {current} to {target}")
            now = datetime.now(UTC).isoformat()
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
                "reconciled_at_utc",
                "closed_at_utc",
                "closed_local_day",
                "realized_pnl_usd",
                "consumes_daily_limit",
            }
            if not set(values).issubset(allowed):
                raise LivePilotError("unsafe live-v2 journal update")
            assignments = ", ".join(f"{key} = ?" for key in values)
            connection.execute(
                f"UPDATE live_v2_attempts SET {assignments} WHERE id = ?",  # noqa: S608
                (*values.values(), int(row["id"])),
            )
            self._transition(
                connection,
                attempt_id=int(row["id"]),
                old=current,
                new=target,
                detail=detail,
                now=now,
            )
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    @staticmethod
    def _transition(
        connection: sqlite3.Connection,
        *,
        attempt_id: int,
        old: LiveV2State | None,
        new: LiveV2State,
        detail: Mapping[str, Any],
        now: str,
    ) -> None:
        connection.execute(
            "INSERT INTO live_v2_transitions("
            "attempt_id,from_state,to_state,created_at_utc,detail_json"
            ") VALUES (?, ?, ?, ?, ?)",
            (
                attempt_id,
                None if old is None else old.value,
                new.value,
                now,
                _canonical_json(detail),
            ),
        )


class LiveV2Executor:
    def __init__(self, journal: LiveV2Journal) -> None:
        self.journal = journal

    def execute(
        self,
        *,
        client: Any,
        intent: LiveBuyIntent,
        decision_id: int,
        authorization: LiveV2Authorization,
        geoblocked: bool,
        now: datetime | None = None,
    ) -> LiveV2Record:
        current = (now or datetime.now(UTC)).astimezone(UTC)
        if geoblocked:
            raise LivePilotError("network is geoblocked; live-v2 is disabled")
        if current < authorization.authorized_at_utc:
            raise LivePilotError("live-v2 authorization is not active yet")
        if current >= authorization.expires_at_utc:
            raise LivePilotError("live-v2 authorization expired before reservation")
        if current >= intent.expires_at_utc:
            raise LivePilotError("live-v2 intent expired before reservation")
        if intent.strategy != authorization.strategy:
            raise LivePilotError("candidate strategy is not authorized for live-v2")
        if intent.side != authorization.side or intent.order_type != authorization.order_type:
            raise LivePilotError("candidate must remain a FOK BUY")
        if intent.amount_usd > authorization.max_buy_notional_usd:
            raise LivePilotError("candidate notional exceeds live-v2 authorization")
        if intent.max_spend_usd > authorization.max_total_spend_usd:
            raise LivePilotError("candidate spend exceeds live-v2 authorization")
        if (
            _seconds_until_next_local_day(current, authorization.daily_timezone)
            <= LIVE_V2_MIDNIGHT_GUARD_SECONDS
        ):
            raise LivePilotError("live-v2 waits across the local-day rollover boundary")
        record = self.journal.reserve(
            intent=intent,
            decision_id=decision_id,
            timezone=authorization.daily_timezone,
            now=current,
        )
        if record.state is not LiveV2State.PREPARED:
            raise LivePilotError(
                f"live-v2 intent already reached {record.state}; automatic retry is forbidden"
            )
        try:
            balance = client.get_balance_allowance(asset_type="COLLATERAL")
            balance_units = int(balance.balance)
            if balance_units <= 0:
                raise LivePilotError("live-v2 wallet collateral balance is zero")
            if balance_units > int(PILOT_MAX_WALLET_USD * COLLATERAL_BASE_UNITS):
                raise LivePilotError("live-v2 wallet collateral balance exceeds $10.00")
            if any(True for _ in client.list_open_orders().iter_items()):
                raise LivePilotError("live-v2 wallet already has an open order")
            positions = client.list_positions(user=str(client.wallet)).iter_items()
            if any(_position_is_open(value) for value in positions):
                raise LivePilotError("live-v2 wallet already has an open position")
            if bool(client.get_closed_only_mode()):
                raise LivePilotError("live-v2 account is in closed-only mode")
            day_start = _local_day_start_utc(current, authorization.daily_timezone)
            trades_today = sum(
                1 for _ in client.list_account_trades(after=day_start.isoformat()).iter_items()
            )
            if trades_today >= authorization.max_orders_per_day:
                raise LivePilotError(
                    "live-v2 daily order limit blocks a new order "
                    "after wallet trading activity today"
                )
            if self.journal.daily_loss_stop_hit(record.local_day):
                raise LivePilotError(
                    "live-v2 daily stop blocks a new order after realized losses today"
                )
            market = client.get_market(id=intent.market_id)
            fee_rate, fee_exponent = _verify_market_identity(client, market, intent)
            book = client.get_order_book(token_id=intent.token_id)
            _verify_book_price_limit(book, intent)
            signing_time = datetime.now(UTC)
            if signing_time >= authorization.expires_at_utc:
                raise LivePilotError("live-v2 authorization expired before signing")
            if signing_time >= intent.expires_at_utc:
                raise LivePilotError("live-v2 intent expired before signing")
            if _local_day(signing_time, authorization.daily_timezone) != record.local_day:
                raise LivePilotError("live-v2 local day changed before signing")
        except LivePilotError as error:
            self.journal.mark_pre_sign_rejected(intent.digest, error)
            return self.journal.get(intent.digest)

        # This durable CAS also prevents two workers that observed PREPARED
        # from both reaching the signing SDK. A crash after it is fail-closed.
        self.journal.mark_signing(intent.digest)
        try:
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
        except BaseException as error:
            self.journal.mark_manual_review(
                intent.digest,
                detail={
                    "reason": "signing or signed-order validation failed",
                    "error": _safe_error(error),
                },
            )
            raise LivePilotError("live-v2 signing boundary requires manual review") from error
        post_time = datetime.now(UTC)
        if (
            post_time >= authorization.expires_at_utc
            or post_time >= intent.expires_at_utc
            or _local_day(post_time, authorization.daily_timezone) != record.local_day
        ):
            self.journal.mark_manual_review(
                intent.digest,
                detail={"reason": "authorization or local day changed before POST"},
            )
            raise LivePilotError("live-v2 stopped before POST at an authorization boundary")
        self.journal.mark_submitting(intent.digest)
        try:
            response = client.post_order(signed)
        except BaseException as error:
            if _is_not_filled_rejection(error):
                self.journal.mark_exchange_rejected(intent.digest, error)
                raise LivePilotError("live-v2 order was not filled by the exchange") from error
            self.journal.mark_ambiguous(intent.digest, error)
            raise LivePilotError(
                "live-v2 submission is ambiguous; automatic retries are forbidden"
            ) from error
        try:
            self.journal.mark_response(intent.digest, response)
        except LivePilotError as error:
            self.journal.mark_ambiguous(intent.digest, error)
            raise LivePilotError(
                "live-v2 received an indeterminate POST response; automatic retries are forbidden"
            ) from error
        return self.journal.get(intent.digest)


def eligible_live_v2_candidates(
    database: Path,
    *,
    authorization: LiveV2Authorization,
    journal: LiveV2Journal,
    max_book_age_seconds: int,
    now: datetime | None = None,
) -> tuple[LiveV2Candidate, ...]:
    current = (now or datetime.now(UTC)).astimezone(UTC)
    candidates: list[LiveV2Candidate] = []
    with sqlite3.connect(f"file:{database.resolve()}?mode=ro", uri=True) as connection:
        connection.row_factory = sqlite3.Row
        rows = connection.execute(
            "SELECT id,market_id,action,created_at,payload_json FROM decisions "
            "WHERE created_at >= ? ORDER BY id DESC LIMIT 1000",
            (authorization.authorized_at_utc.isoformat(),),
        ).fetchall()
        for row in rows:
            decision_id = int(row["id"])
            if journal.decision_attempted(decision_id):
                continue
            try:
                payload = json.loads(str(row["payload_json"]))
            except json.JSONDecodeError:
                continue
            if not isinstance(payload, dict) or payload.get("strategy_version") != "v1":
                continue
            action = str(row["action"])
            payload_action = str(payload.get("action", ""))
            reason_codes = payload.get("reason_codes")
            paper_buy = action == "PAPER_BUY" and payload_action == "PAPER_BUY"
            monitor_only = (
                action == "OBSERVE"
                and payload_action == "OBSERVE"
                and reason_codes == ["ACTIVE_PAPER_EVENT_MONITOR_ONLY"]
            )
            if not paper_buy and not monitor_only:
                continue
            if (
                connection.execute(
                    "SELECT 1 FROM decisions WHERE market_id = ? AND id > ? LIMIT 1",
                    (str(row["market_id"]), decision_id),
                ).fetchone()
                is not None
            ):
                continue
            try:
                created = _parse_time(payload.get("created_at", row["created_at"]))
                edge = Decimal(str(payload["probability_edge"]))
                expected_profit = Decimal(str(payload["expected_profit_usd"]))
            except (KeyError, TypeError, ValueError, LivePilotError):
                continue
            if created < authorization.authorized_at_utc:
                continue
            if created > current:
                continue
            if current >= created + timedelta(seconds=max_book_age_seconds):
                continue
            if not edge.is_finite() or not expected_profit.is_finite():
                continue
            if edge < authorization.min_probability_edge:
                continue
            if expected_profit < authorization.min_expected_profit_usd:
                continue
            candidates.append(
                LiveV2Candidate(
                    decision_id=decision_id,
                    expected_profit_usd=expected_profit,
                    probability_edge=edge,
                    created_at_utc=created,
                )
            )
    candidates.sort(
        key=lambda item: (item.expected_profit_usd, item.probability_edge, item.decision_id),
        reverse=True,
    )
    return tuple(candidates)


def reconcile_live_v2(
    journal: LiveV2Journal,
    client: Any,
    *,
    timezone: str = LIVE_V2_TIMEZONE,
) -> LiveV2Record | None:
    record = journal.latest_active()
    if record is None:
        return None
    if record.state in {LiveV2State.PREPARED, LiveV2State.SIGNING, LiveV2State.SIGNED}:
        journal.mark_manual_review(
            record.intent_sha256,
            detail={"reason": "process stopped before submission boundary"},
        )
        return journal.get(record.intent_sha256)
    if record.state is LiveV2State.MANUAL_REVIEW:
        return record
    intent = record.intent
    open_orders = tuple(client.list_open_orders().iter_items())
    positions = tuple(client.list_positions(user=str(client.wallet)).iter_items())
    open_positions = tuple(item for item in positions if _position_is_open(item))
    matching_positions = tuple(item for item in open_positions if _position_matches(item, intent))
    expected_trade_ids = _response_trade_ids(record)
    all_candidate_trades = [
        item
        for item in client.list_account_trades(
            token_id=intent.token_id,
            after=None if record.submitted_at_utc is None else record.submitted_at_utc.isoformat(),
        ).iter_items()
        if _trade_matches(item, intent, remote_order_id=record.remote_order_id)
    ]
    trades = [
        item
        for item in all_candidate_trades
        if not expected_trade_ids or str(getattr(item, "id", "")) in expected_trade_ids
    ]
    if open_orders:
        journal.mark_manual_review(
            record.intent_sha256,
            detail={
                "reason": "FOK order unexpectedly remains open",
                "open_orders": len(open_orders),
            },
        )
    elif open_positions and not matching_positions:
        journal.mark_manual_review(
            record.intent_sha256,
            detail={"reason": "wallet has a different open position"},
        )
    elif matching_positions:
        journal.mark_position_open(
            record.intent_sha256,
            detail={"positions": len(matching_positions), "matching_trades": len(trades)},
        )
    elif (
        record.state is not LiveV2State.POSITION_OPEN
        and trades
        and (record.remote_order_id is not None or expected_trade_ids or len(trades) == 1)
    ):
        # A matching BUY fill proves exposure even if the portfolio endpoint is
        # temporarily lagging. It never proves that the position is closed.
        journal.mark_position_open(
            record.intent_sha256,
            detail={"positions": 0, "matching_trades": len(trades)},
        )
    elif record.state is LiveV2State.POSITION_OPEN:
        market = client.get_market(id=intent.market_id)
        market_state = getattr(market, "state", None)
        market_closed = bool(getattr(market_state, "closed", False))
        closed_positions = tuple(
            item
            for item in client.list_closed_positions(user=str(client.wallet)).iter_items()
            if _closed_position_matches(item, intent, record.submitted_at_utc)
        )
        if market_closed and closed_positions:
            pnl_values = [
                Decimal(str(value))
                for value in (getattr(item, "realized_pnl", None) for item in closed_positions)
                if value is not None
            ]
            realized_pnl = sum(pnl_values, Decimal(0)) if pnl_values else None
            journal.mark_closed(
                record.intent_sha256,
                timezone=timezone,
                realized_pnl_usd=realized_pnl,
                detail={
                    "matching_closed_positions": len(closed_positions),
                    "realized_pnl_usd": None if realized_pnl is None else str(realized_pnl),
                },
            )
        elif market_closed and datetime.now(UTC) - record.updated_at_utc > timedelta(days=1):
            journal.mark_manual_review(
                record.intent_sha256,
                detail={"reason": "closed market has no confirmed closed-position record"},
            )
    elif len(trades) > 1 and record.remote_order_id is None and not expected_trade_ids:
        journal.mark_manual_review(
            record.intent_sha256,
            detail={"reason": "multiple unbound trades match an ambiguous submission"},
        )
    elif (
        record.state in {LiveV2State.SUBMITTING, LiveV2State.AMBIGUOUS}
        and datetime.now(UTC) - record.updated_at_utc > LIVE_V2_RECONCILE_GRACE
    ):
        journal.mark_manual_review(
            record.intent_sha256,
            detail={"reason": "no matching trade/order after ambiguous submission"},
        )
    elif (
        record.state is LiveV2State.ACCEPTED
        and datetime.now(UTC) - record.updated_at_utc > LIVE_V2_RECONCILE_GRACE
    ):
        journal.mark_manual_review(
            record.intent_sha256,
            detail={"reason": "accepted response did not produce a visible trade or position"},
        )
    return journal.get(record.intent_sha256)


def _is_not_filled_rejection(error: BaseException) -> bool:
    text = str(error).lower()
    return (
        "couldn't be fully filled" in text or "not fully filled" in text or "fok_not_filled" in text
    )


def _verify_book_price_limit(book: Any, intent: LiveBuyIntent) -> None:
    asset_id = str(getattr(book, "asset_id", getattr(book, "token_id", "")))
    condition_id = str(getattr(book, "condition_id", ""))
    if asset_id != intent.token_id or condition_id != intent.condition_id:
        raise LivePilotError("order book identity mismatch")
    try:
        current_limit = _fok_buy_limit_from_book(book, amount_usd=intent.amount_usd)
    except LivePilotError as error:
        raise LivePilotError(
            "order book changed; a new decision and approval are required"
        ) from error
    if current_limit > intent.max_price + LIVE_V2_PRICE_DRIFT_USD:
        raise LivePilotError("order book changed; a new decision and approval are required")


def run_live_v2(
    *,
    settings: Settings,
    database: Path,
    credentials_path: Path,
    authorization_path: Path,
    poll_seconds: float,
    timeout_seconds: float,
) -> dict[str, Any]:
    journal = LiveV2Journal(database)
    journal.heartbeat("STARTING")
    started = time.monotonic()
    last_balance_poll = 0.0
    while timeout_seconds <= 0 or time.monotonic() - started < timeout_seconds:
        authorization = load_live_v2_authorization(authorization_path)
        credentials = load_live_credentials(credentials_path)
        if credentials.auth_mode not in {"session_key", "direct_signer"}:
            raise LivePilotError("live-v2 requires an authorized wallet signer")
        if credentials.wallet.casefold() != authorization.wallet.casefold():
            raise LivePilotError("live-v2 authorization wallet does not match credentials")
        active = journal.latest_active()
        if active is None:
            journal.heartbeat("RUNNING")
        if active is not None:
            with open_live_client(credentials) as client:
                if str(client.wallet_type) not in {"DEPOSIT_WALLET", "EOA"}:
                    raise LivePilotError("live-v2 requires a Deposit Wallet or EOA wallet")
                active = reconcile_live_v2(
                    journal,
                    client,
                    timezone=authorization.daily_timezone,
                )
                try:
                    recon_balance = client.get_balance_allowance(asset_type="COLLATERAL")
                    journal.heartbeat(
                        "RECONCILING",
                        detail={
                            "state": active.state.value if active is not None else None,
                            "balance_usd": str(
                                Decimal(int(recon_balance.balance)) / COLLATERAL_BASE_UNITS
                            ),
                        },
                    )
                except LivePilotError:
                    journal.heartbeat("RECONCILING", detail={"state": None})
                if active.state is LiveV2State.MANUAL_REVIEW:
                    raise LivePilotError("live-v2 requires manual review before any new order")
                time.sleep(max(1.0, poll_seconds))
                continue
        local_day = _local_day(datetime.now(UTC), authorization.daily_timezone)
        if journal.charged_count_for_day(
            local_day
        ) >= authorization.max_orders_per_day or journal.daily_loss_stop_hit(local_day):
            journal.heartbeat("DAILY_LIMIT", detail={"local_day": local_day})
            time.sleep(max(30.0, poll_seconds))
            continue
        candidates = eligible_live_v2_candidates(
            database,
            authorization=authorization,
            journal=journal,
            max_book_age_seconds=settings.max_book_age_seconds,
        )
        if not candidates:
            if time.monotonic() - last_balance_poll >= LIVE_V2_BALANCE_POLL_SECONDS:
                try:
                    with open_live_client(credentials) as client:
                        poll_balance = client.get_balance_allowance(asset_type="COLLATERAL")
                        journal.heartbeat(
                            "WAITING_FOR_SIGNAL",
                            detail={
                                "balance_usd": str(
                                    Decimal(int(poll_balance.balance)) / COLLATERAL_BASE_UNITS
                                )
                            },
                        )
                        last_balance_poll = time.monotonic()
                except LivePilotError:
                    journal.heartbeat("WAITING_FOR_SIGNAL")
            else:
                journal.heartbeat("WAITING_FOR_SIGNAL")
            time.sleep(max(1.0, poll_seconds))
            continue
        journal.heartbeat("CHECKING_CANDIDATE", detail={"count": len(candidates)})
        with open_live_client(credentials) as client:
            if str(client.wallet_type) not in {"DEPOSIT_WALLET", "EOA"}:
                raise LivePilotError("live-v2 requires a Deposit Wallet or EOA wallet")
            balance = client.get_balance_allowance(asset_type="COLLATERAL")
            balance_units = int(balance.balance)
            journal.heartbeat(
                "CHECKING_CANDIDATE",
                detail={
                    "count": len(candidates),
                    "balance_usd": str(Decimal(balance_units) / COLLATERAL_BASE_UNITS),
                },
            )
            if balance_units <= 0:
                raise LivePilotError("live-v2 wallet collateral balance is zero")
            if balance_units > int(PILOT_MAX_WALLET_USD * COLLATERAL_BASE_UNITS):
                raise LivePilotError("live-v2 wallet collateral balance exceeds $10.00")
            if bool(client.get_closed_only_mode()):
                raise LivePilotError("live-v2 account is in closed-only mode")
            if any(True for _ in client.list_open_orders().iter_items()):
                time.sleep(max(1.0, poll_seconds))
                continue
            positions = client.list_positions(user=str(client.wallet)).iter_items()
            if any(_position_is_open(item) for item in positions):
                time.sleep(max(1.0, poll_seconds))
                continue
            for candidate in candidates:
                try:
                    intent = preview_intent_from_decision(
                        settings=settings,
                        decision_id=candidate.decision_id,
                        client=client,
                    )
                except LivePilotError:
                    continue
                authorization = load_live_v2_authorization(authorization_path)
                if credentials.wallet.casefold() != authorization.wallet.casefold():
                    raise LivePilotError("live-v2 authorization wallet does not match credentials")
                geoblock = fetch_geoblock_status(
                    url=settings.geoblock_url,
                    timeout=min(settings.http_timeout_seconds, 20.0),
                )
                if geoblock.blocked:
                    raise LivePilotError("network is geoblocked; live-v2 is disabled")
                journal.heartbeat(
                    "EXECUTING",
                    detail={"decision_id": candidate.decision_id},
                )
                record = LiveV2Executor(journal).execute(
                    client=cast(Any, client),
                    intent=intent,
                    decision_id=candidate.decision_id,
                    authorization=authorization,
                    geoblocked=geoblock.blocked,
                )
                journal.heartbeat(
                    "ATTEMPT_RECORDED",
                    detail={"state": record.state.value},
                )
                return {
                    "mode": "LIVE_V2",
                    "local_day": record.local_day,
                    "decision_id": candidate.decision_id,
                    "intent_sha256": record.intent_sha256,
                    "state": record.state.value,
                    "remote_order_id": record.remote_order_id,
                    "submitted_at_utc": record.submitted_at_utc,
                    "intent": _intent_dict(intent),
                }
        time.sleep(max(1.0, poll_seconds))
    raise LivePilotError("live-v2 timed out before an eligible order attempt")


def live_v2_status(database: Path) -> dict[str, Any]:
    journal = LiveV2Journal(database)
    return {
        **journal.summary(),
        "max_wallet_usd": str(PILOT_MAX_WALLET_USD),
        "max_buy_notional_usd": str(PILOT_MAX_BUY_NOTIONAL_USD),
        "max_all_in_spend_usd": str(PILOT_MAX_BUY_USD),
        "daily_stop_loss_usd": str(LIVE_V2_DAILY_STOP_USD),
        "max_orders_per_day": LIVE_V2_MAX_ORDERS_PER_DAY,
        "daily_limit_scope": "signing_or_uncertain_attempts; unsigned_preflight_excluded",
        "daily_timezone": LIVE_V2_TIMEZONE,
        "min_probability_edge": str(LIVE_V2_MIN_PROBABILITY_EDGE),
        "min_expected_profit_usd": str(LIVE_V2_MIN_EXPECTED_PROFIT_USD),
    }


def _intent_json(intent: LiveBuyIntent) -> str:
    return json.dumps(_intent_dict(intent), sort_keys=True, separators=(",", ":"))


def _intent_dict(intent: LiveBuyIntent) -> dict[str, Any]:
    return {
        key: value.astimezone(UTC).isoformat()
        if isinstance(value, datetime)
        else str(value)
        if isinstance(value, Decimal)
        else value
        for key, value in asdict(intent).items()
    }


def _intent_from_json(payload: str) -> LiveBuyIntent:
    raw = json.loads(payload)
    return LiveBuyIntent(
        strategy=str(raw["strategy"]),
        event_id=str(raw["event_id"]),
        market_id=str(raw["market_id"]),
        condition_id=str(raw["condition_id"]),
        token_id=str(raw["token_id"]),
        amount_usd=Decimal(str(raw["amount_usd"])),
        max_spend_usd=Decimal(str(raw["max_spend_usd"])),
        max_price=Decimal(str(raw["max_price"])),
        book_hash=str(raw["book_hash"]),
        decision_created_at_utc=_parse_time(raw["decision_created_at_utc"]),
        expires_at_utc=_parse_time(raw["expires_at_utc"]),
        order_type=str(raw["order_type"]),
        side=str(raw["side"]),
    )


def _record(row: sqlite3.Row) -> LiveV2Record:
    return LiveV2Record(
        id=int(row["id"]),
        local_day=str(row["local_day"]),
        decision_id=int(row["decision_id"]),
        intent_sha256=str(row["intent_sha256"]),
        state=LiveV2State(str(row["state"])),
        intent_json=str(row["intent_json"]),
        signed_fingerprint=row["signed_fingerprint"],
        signed_order_json=row["signed_order_json"],
        remote_order_id=row["remote_order_id"],
        response_json=row["response_json"],
        last_error=row["last_error"],
        created_at_utc=_parse_time(row["created_at_utc"]),
        updated_at_utc=_parse_time(row["updated_at_utc"]),
        submitted_at_utc=None
        if row["submitted_at_utc"] is None
        else _parse_time(row["submitted_at_utc"]),
        reconciled_at_utc=None
        if row["reconciled_at_utc"] is None
        else _parse_time(row["reconciled_at_utc"]),
        closed_at_utc=None if row["closed_at_utc"] is None else _parse_time(row["closed_at_utc"]),
        closed_local_day=row["closed_local_day"],
        realized_pnl_usd=None
        if row["realized_pnl_usd"] is None
        else Decimal(str(row["realized_pnl_usd"])),
        consumes_daily_limit=bool(row["consumes_daily_limit"]),
    )


def _local_day(value: datetime, timezone: str) -> str:
    try:
        zone = ZoneInfo(timezone)
    except ZoneInfoNotFoundError as error:
        raise LivePilotError("live-v2 daily timezone is unavailable") from error
    return value.astimezone(zone).date().isoformat()


def _local_day_start_utc(value: datetime, timezone: str) -> datetime:
    try:
        zone = ZoneInfo(timezone)
    except ZoneInfoNotFoundError as error:
        raise LivePilotError("live-v2 daily timezone is unavailable") from error
    local = value.astimezone(zone)
    return local.replace(hour=0, minute=0, second=0, microsecond=0).astimezone(UTC)


def _seconds_until_next_local_day(value: datetime, timezone: str) -> float:
    try:
        zone = ZoneInfo(timezone)
    except ZoneInfoNotFoundError as error:
        raise LivePilotError("live-v2 daily timezone is unavailable") from error
    local = value.astimezone(zone)
    tomorrow = (local + timedelta(days=1)).replace(
        hour=0,
        minute=0,
        second=0,
        microsecond=0,
    )
    return (tomorrow - local).total_seconds()


def _position_matches(position: Any, intent: LiveBuyIntent) -> bool:
    asset = str(getattr(position, "asset_id", getattr(position, "token_id", "")) or "")
    condition = str(getattr(position, "condition_id", "") or "")
    return asset == intent.token_id and condition == intent.condition_id


def _closed_position_matches(
    position: Any,
    intent: LiveBuyIntent,
    submitted_at_utc: datetime | None,
) -> bool:
    if not _position_matches(position, intent):
        return False
    timestamp = getattr(position, "timestamp", None)
    if submitted_at_utc is None or timestamp is None:
        return True
    try:
        closed_at = timestamp if isinstance(timestamp, datetime) else _parse_time(timestamp)
    except LivePilotError:
        return False
    if closed_at.tzinfo is None:
        return False
    return closed_at.astimezone(UTC) >= submitted_at_utc


def _position_closed_since(position: Any, since_utc: datetime) -> bool:
    timestamp = getattr(position, "timestamp", None)
    if timestamp is None:
        return False
    try:
        closed_at = timestamp if isinstance(timestamp, datetime) else _parse_time(timestamp)
    except LivePilotError:
        return True
    if closed_at.tzinfo is None:
        return True
    return closed_at.astimezone(UTC) >= since_utc.astimezone(UTC)


def _response_trade_ids(record: LiveV2Record) -> frozenset[str]:
    if not record.response_json:
        return frozenset()
    try:
        raw = json.loads(record.response_json)
    except json.JSONDecodeError:
        return frozenset()
    if not isinstance(raw, dict):
        return frozenset()
    values = raw.get("trade_ids", raw.get("tradeIDs", ()))
    if not isinstance(values, list | tuple):
        return frozenset()
    return frozenset(str(value) for value in values if str(value))


def _parse_time(value: object) -> datetime:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError as error:
        raise LivePilotError("invalid live-v2 timestamp") from error
    if parsed.tzinfo is None:
        raise LivePilotError("live-v2 timestamps must include a timezone")
    return parsed.astimezone(UTC)


def _private_file(path: Path, *, label: str) -> Path:
    candidate = path.expanduser()
    if candidate.is_symlink():
        raise LivePilotError(f"{label} must not be a symlink")
    try:
        resolved = candidate.resolve(strict=True)
        info = resolved.lstat()
    except FileNotFoundError as error:
        raise LivePilotError(f"{label} file is not configured") from error
    if stat.S_ISLNK(info.st_mode) or not stat.S_ISREG(info.st_mode):
        raise LivePilotError(f"{label} must be a regular non-symlink file")
    if info.st_uid not in {0, os.geteuid()}:
        raise LivePilotError(f"{label} owner is not trusted")
    if stat.S_IMODE(info.st_mode) & 0o077:
        raise LivePilotError(f"{label} must have mode 0600 or stricter")
    return resolved


__all__ = [
    "LIVE_V2_AUTHORIZATION_KIND",
    "LIVE_V2_DAILY_STOP_USD",
    "LIVE_V2_MAX_ORDERS_PER_DAY",
    "LIVE_V2_MIN_EXPECTED_PROFIT_USD",
    "LIVE_V2_MIN_PROBABILITY_EDGE",
    "LIVE_V2_STRATEGY",
    "LIVE_V2_TIMEZONE",
    "LiveV2Authorization",
    "LiveV2Candidate",
    "LiveV2Executor",
    "LiveV2Journal",
    "LiveV2Record",
    "LiveV2State",
    "eligible_live_v2_candidates",
    "live_v2_status",
    "load_live_v2_authorization",
    "reconcile_live_v2",
    "run_live_v2",
]
