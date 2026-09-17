from __future__ import annotations

import json
import os
import sqlite3
import stat
import time
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any, cast

from polybot.config import Settings
from polybot.geoblock import fetch_geoblock_status
from polybot.live_pilot import (
    APPROVAL_KIND,
    PILOT_MAX_BUY_NOTIONAL_USD,
    PILOT_MAX_BUY_USD,
    PILOT_MAX_WALLET_USD,
    LivePilotError,
    LivePilotExecutor,
    LivePilotJournal,
)
from polybot.live_pilot_runtime import (
    account_check,
    load_live_credentials,
    open_live_client,
    preview_intent_from_decision,
    write_prepared_intent,
)

AUTO_ONCE_AUTHORIZATION_KIND = "polybot-live-auto-once-v1"
AUTO_ONCE_STRATEGY = "open-meteo-truncated-normal-v1"


@dataclass(frozen=True, slots=True)
class AutoOnceAuthorization:
    kind: str
    wallet: str
    strategy: str
    side: str
    order_type: str
    max_orders: int
    max_wallet_balance_usd: Decimal
    max_buy_notional_usd: Decimal
    max_total_spend_usd: Decimal
    min_probability_edge: Decimal
    min_expected_profit_usd: Decimal
    jurisdiction_confirmed: bool
    expires_at_utc: datetime


@dataclass(frozen=True, slots=True)
class AutoCandidate:
    decision_id: int
    expected_profit_usd: Decimal
    probability_edge: Decimal
    created_at_utc: datetime


def load_auto_once_authorization(
    path: Path,
    *,
    now: datetime | None = None,
) -> AutoOnceAuthorization:
    resolved = _private_file(path, label="live auto authorization")
    try:
        raw = json.loads(resolved.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise LivePilotError("invalid live auto authorization sidecar") from error
    if not isinstance(raw, dict):
        raise LivePilotError("live auto authorization must be a JSON object")
    expected = {
        "kind",
        "wallet",
        "strategy",
        "side",
        "order_type",
        "max_orders",
        "max_wallet_balance_usd",
        "max_buy_notional_usd",
        "max_total_spend_usd",
        "min_probability_edge",
        "min_expected_profit_usd",
        "jurisdiction_confirmed",
        "expires_at_utc",
    }
    if set(raw) != expected:
        raise LivePilotError("live auto authorization fields do not match the fixed v1 schema")
    try:
        authorization = AutoOnceAuthorization(
            kind=str(raw["kind"]),
            wallet=str(raw["wallet"]),
            strategy=str(raw["strategy"]),
            side=str(raw["side"]),
            order_type=str(raw["order_type"]),
            max_orders=int(raw["max_orders"]),
            max_wallet_balance_usd=Decimal(str(raw["max_wallet_balance_usd"])),
            max_buy_notional_usd=Decimal(str(raw["max_buy_notional_usd"])),
            max_total_spend_usd=Decimal(str(raw["max_total_spend_usd"])),
            min_probability_edge=Decimal(str(raw["min_probability_edge"])),
            min_expected_profit_usd=Decimal(str(raw["min_expected_profit_usd"])),
            jurisdiction_confirmed=raw["jurisdiction_confirmed"] is True,
            expires_at_utc=_parse_time(raw["expires_at_utc"]),
        )
    except (TypeError, ValueError) as error:
        raise LivePilotError("invalid live auto authorization values") from error
    if authorization.kind != AUTO_ONCE_AUTHORIZATION_KIND:
        raise LivePilotError("wrong live auto authorization kind")
    if not authorization.wallet.strip():
        raise LivePilotError("live auto authorization wallet is required")
    if authorization.strategy != AUTO_ONCE_STRATEGY:
        raise LivePilotError("live auto authorization is fixed to the existing v1 strategy")
    if authorization.side != "BUY" or authorization.order_type != "FOK":
        raise LivePilotError("live auto authorization permits only one FOK BUY")
    if authorization.max_orders != 1:
        raise LivePilotError("live auto authorization permits exactly one order")
    if authorization.max_wallet_balance_usd != PILOT_MAX_WALLET_USD:
        raise LivePilotError("live auto authorization cannot alter the $10 wallet cap")
    if authorization.max_buy_notional_usd != PILOT_MAX_BUY_NOTIONAL_USD:
        raise LivePilotError("live auto authorization cannot alter the $1.90 BUY cap")
    if authorization.max_total_spend_usd != PILOT_MAX_BUY_USD:
        raise LivePilotError("live auto authorization cannot alter the $2 BUY cap")
    if authorization.min_probability_edge < 0:
        raise LivePilotError("live auto minimum probability edge must be non-negative")
    if authorization.min_expected_profit_usd < 0:
        raise LivePilotError("live auto minimum expected profit must be non-negative")
    if not authorization.jurisdiction_confirmed:
        raise LivePilotError("user/account jurisdiction eligibility is not confirmed")
    current = (now or datetime.now(UTC)).astimezone(UTC)
    if current >= authorization.expires_at_utc:
        raise LivePilotError("live auto authorization expired")
    return authorization


def latest_decision_id(database: Path) -> int:
    with sqlite3.connect(f"file:{database.resolve()}?mode=ro", uri=True) as connection:
        row = connection.execute("SELECT COALESCE(MAX(id), 0) FROM decisions").fetchone()
    return int(row[0] if row is not None else 0)


def eligible_auto_candidates(
    database: Path,
    *,
    after_decision_id: int,
    authorization: AutoOnceAuthorization,
    max_book_age_seconds: int,
    now: datetime | None = None,
) -> tuple[AutoCandidate, ...]:
    current = (now or datetime.now(UTC)).astimezone(UTC)
    candidates: list[AutoCandidate] = []
    with sqlite3.connect(f"file:{database.resolve()}?mode=ro", uri=True) as connection:
        connection.row_factory = sqlite3.Row
        rows = connection.execute(
            "SELECT id,market_id,action,created_at,payload_json FROM decisions "
            "WHERE id > ? ORDER BY id DESC LIMIT 1000",
            (after_decision_id,),
        ).fetchall()
        for row in rows:
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
            newer = connection.execute(
                "SELECT 1 FROM decisions WHERE market_id = ? AND id > ? LIMIT 1",
                (str(row["market_id"]), int(row["id"])),
            ).fetchone()
            if newer is not None:
                continue
            try:
                created = _parse_time(payload.get("created_at", row["created_at"]))
                edge = Decimal(str(payload["probability_edge"]))
                expected_profit = Decimal(str(payload["expected_profit_usd"]))
            except (KeyError, TypeError, ValueError, LivePilotError):
                continue
            if current >= created + timedelta(seconds=max_book_age_seconds):
                continue
            if edge < authorization.min_probability_edge:
                continue
            if expected_profit < authorization.min_expected_profit_usd:
                continue
            candidates.append(
                AutoCandidate(
                    decision_id=int(row["id"]),
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


def run_auto_once(
    *,
    settings: Settings,
    database: Path,
    credentials_path: Path,
    authorization_path: Path,
    intent_output_path: Path,
    exact_approval_path: Path,
    poll_seconds: float,
    timeout_seconds: float,
    after_decision_id: int | None = None,
) -> dict[str, Any]:
    journal = LivePilotJournal(database)
    if journal.gate() is not None:
        raise LivePilotError("the one-shot live pilot slot is already permanently reserved")
    authorization = load_auto_once_authorization(authorization_path)
    credentials = load_live_credentials(credentials_path)
    if credentials.auth_mode != "session_key":
        raise LivePilotError("live auto-once requires an authorized Deposit Wallet session key")
    if credentials.wallet.casefold() != authorization.wallet.casefold():
        raise LivePilotError("live auto authorization wallet does not match credentials")

    check = account_check(
        settings=settings,
        credentials_path=credentials_path,
        journal=journal,
    )
    if not check.ready_for_prepare:
        raise LivePilotError("live account is not ready: " + ", ".join(check.blockers))
    if check.wallet_type != "DEPOSIT_WALLET":
        raise LivePilotError("live auto-once requires a Deposit Wallet")

    baseline = (
        latest_decision_id(database) if after_decision_id is None else int(after_decision_id)
    )
    started = time.monotonic()
    attempted: set[int] = set()
    while timeout_seconds <= 0 or time.monotonic() - started < timeout_seconds:
        authorization = load_auto_once_authorization(authorization_path)
        candidates = eligible_auto_candidates(
            database,
            after_decision_id=baseline,
            authorization=authorization,
            max_book_age_seconds=settings.max_book_age_seconds,
        )
        for candidate in candidates:
            if candidate.decision_id in attempted:
                continue
            attempted.add(candidate.decision_id)
            geoblock = fetch_geoblock_status(
                url=settings.geoblock_url,
                timeout=min(settings.http_timeout_seconds, 20.0),
            )
            if geoblock.blocked:
                raise LivePilotError("network is geoblocked; live auto-once is disabled")
            with open_live_client(credentials) as client:
                if str(client.wallet).casefold() != authorization.wallet.casefold():
                    raise LivePilotError("authenticated wallet changed")
                if str(client.wallet_type) != "DEPOSIT_WALLET":
                    raise LivePilotError("authenticated wallet is not a Deposit Wallet")
                if bool(client.get_closed_only_mode()):
                    raise LivePilotError("account is in closed-only mode")
                try:
                    intent = preview_intent_from_decision(
                        settings=settings,
                        decision_id=candidate.decision_id,
                        client=client,
                    )
                except LivePilotError:
                    continue
                if intent.strategy != authorization.strategy:
                    raise LivePilotError("candidate strategy is not authorized")
                if intent.amount_usd > authorization.max_buy_notional_usd:
                    raise LivePilotError("candidate notional exceeds standing authorization")
                if intent.max_spend_usd > authorization.max_total_spend_usd:
                    raise LivePilotError("candidate spend exceeds standing authorization")
                prepared = write_prepared_intent(
                    journal=journal,
                    intent=intent,
                    output_path=intent_output_path,
                )
                _write_exact_approval(
                    exact_approval_path,
                    intent_sha256=intent.digest,
                    expires_at_utc=min(intent.expires_at_utc, authorization.expires_at_utc),
                )
                try:
                    record = LivePilotExecutor(journal).execute_once(
                        client=cast(Any, client),
                        intent=intent,
                        approval_path=exact_approval_path,
                        geoblocked=False,
                    )
                except LivePilotError:
                    gate = journal.gate()
                    if gate is not None:
                        _consume_auto_authorization(
                            authorization_path,
                            gate["intent_sha256"],
                        )
                        raise
                    _retire_unused_exact_approval(exact_approval_path, candidate.decision_id)
                    continue
            _consume_auto_authorization(authorization_path, intent.digest)
            return {
                "mode": "AUTO_ONCE",
                "decision_id": candidate.decision_id,
                "intent_sha256": record.intent_sha256,
                "state": record.state.value,
                "remote_order_id": record.remote_order_id,
                "submitted_at_utc": record.submitted_at_utc,
                "intent": prepared["intent"],
                "standing_authorization_consumed": True,
            }
        time.sleep(max(1.0, poll_seconds))
    raise LivePilotError("live auto-once timed out before a fresh eligible v1 candidate")


def _write_exact_approval(
    path: Path,
    *,
    intent_sha256: str,
    expires_at_utc: datetime,
) -> None:
    payload = {
        "kind": APPROVAL_KIND,
        "intent_sha256": intent_sha256,
        "expires_at_utc": expires_at_utc.astimezone(UTC).isoformat(),
        "jurisdiction_confirmed": True,
        "max_wallet_balance_usd": str(PILOT_MAX_WALLET_USD),
        "max_total_spend_usd": str(PILOT_MAX_BUY_USD),
    }
    _atomic_private_json(path, payload)


def _consume_auto_authorization(path: Path, intent_sha256: str) -> None:
    candidate = path.expanduser()
    if not candidate.exists():
        return
    resolved = candidate.resolve(strict=True)
    used = resolved.with_name(f"{resolved.name}.used-{intent_sha256[:12]}")
    if used.exists():
        raise LivePilotError("live auto authorization was already consumed")
    resolved.replace(used)


def _retire_unused_exact_approval(path: Path, decision_id: int) -> None:
    candidate = path.expanduser()
    if not candidate.exists():
        return
    resolved = candidate.resolve(strict=True)
    retired = resolved.with_name(f"{resolved.name}.unused-{decision_id}")
    if retired.exists():
        retired.unlink()
    resolved.replace(retired)


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


def _atomic_private_json(path: Path, payload: dict[str, Any]) -> None:
    target = path.expanduser().resolve()
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_name(f".{target.name}.tmp-{os.getpid()}")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        temporary.replace(target)
        target.chmod(0o600)
    finally:
        temporary.unlink(missing_ok=True)


def _parse_time(value: object) -> datetime:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError as error:
        raise LivePilotError("invalid live auto timestamp") from error
    if parsed.tzinfo is None:
        raise LivePilotError("live auto timestamps must include a timezone")
    return parsed.astimezone(UTC)


def authorization_report(authorization: AutoOnceAuthorization) -> dict[str, Any]:
    payload = asdict(authorization)
    return {
        key: value.astimezone(UTC).isoformat()
        if isinstance(value, datetime)
        else str(value)
        if isinstance(value, Decimal)
        else value
        for key, value in payload.items()
    }


__all__ = [
    "AUTO_ONCE_AUTHORIZATION_KIND",
    "AUTO_ONCE_STRATEGY",
    "AutoCandidate",
    "AutoOnceAuthorization",
    "authorization_report",
    "eligible_auto_candidates",
    "latest_decision_id",
    "load_auto_once_authorization",
    "run_auto_once",
]
