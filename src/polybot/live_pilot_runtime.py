from __future__ import annotations

import json
import os
import sqlite3
import stat
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime, timedelta
from decimal import ROUND_UP, Decimal
from pathlib import Path
from typing import Any, Literal, cast

from polymarket import ApiKeyCreds, RelayerApiKey, SecureClient

from polybot.config import Settings
from polybot.geoblock import fetch_geoblock_status
from polybot.live_pilot import (
    COLLATERAL_BASE_UNITS,
    PILOT_MAX_BUY_NOTIONAL_USD,
    PILOT_MAX_BUY_USD,
    PILOT_MAX_WALLET_USD,
    LiveBuyIntent,
    LivePilotError,
    LivePilotJournal,
    resolve_platform_fee_info,
)

CREDENTIAL_KIND = "polybot-polymarket-account-v1"
INTENT_KIND = "polybot-live-buy-intent-v1"
AuthMode = Literal["session_key", "direct_signer"]


@dataclass(frozen=True, slots=True)
class LivePilotCredentials:
    auth_mode: AuthMode
    wallet: str
    private_key: str = field(repr=False)
    clob_api_key: str = field(repr=False)
    clob_api_secret: str = field(repr=False)
    clob_api_passphrase: str = field(repr=False)
    relayer_api_key: str | None = field(default=None, repr=False)
    relayer_api_address: str | None = None


@dataclass(frozen=True, slots=True)
class AccountCheck:
    checked_at_utc: str
    network_allowed: bool
    network_country: str | None
    network_region: str | None
    auth_mode: str
    wallet: str
    signer: str
    wallet_type: str
    collateral_balance_usd: str
    collateral_allowances: dict[str, int]
    open_orders: int
    open_positions: int
    account_trades: int
    closed_only_mode: bool
    one_shot_gate: dict[str, str] | None
    ready_for_prepare: bool
    blockers: tuple[str, ...]


def load_live_credentials(path: Path) -> LivePilotCredentials:
    resolved = _private_file(path, label="live credential")
    try:
        raw = json.loads(resolved.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise LivePilotError("invalid live credential file") from error
    if not isinstance(raw, dict):
        raise LivePilotError("live credential file must contain one JSON object")
    required = {
        "kind",
        "auth_mode",
        "wallet",
        "private_key",
        "clob_api_key",
        "clob_api_secret",
        "clob_api_passphrase",
    }
    optional = {"relayer_api_key", "relayer_api_address"}
    if not required.issubset(raw) or not set(raw).issubset(required | optional):
        raise LivePilotError("live credential fields do not match the fixed v1 schema")
    if raw.get("kind") != CREDENTIAL_KIND:
        raise LivePilotError("wrong live credential kind")
    mode = str(raw.get("auth_mode", ""))
    if mode not in {"session_key", "direct_signer"}:
        raise LivePilotError("unsupported live auth_mode")
    values = {
        key: str(raw.get(key, "")).strip()
        for key in (
            "wallet",
            "private_key",
            "clob_api_key",
            "clob_api_secret",
            "clob_api_passphrase",
        )
    }
    if any(not value for value in values.values()):
        raise LivePilotError("live credential file contains an empty required value")
    relayer_key = _optional_string(raw.get("relayer_api_key"))
    relayer_address = _optional_string(raw.get("relayer_api_address"))
    if bool(relayer_key) != bool(relayer_address):
        raise LivePilotError("relayer_api_key and relayer_api_address must be supplied together")
    return LivePilotCredentials(
        auth_mode=cast(AuthMode, mode),
        wallet=values["wallet"],
        private_key=values["private_key"],
        clob_api_key=values["clob_api_key"],
        clob_api_secret=values["clob_api_secret"],
        clob_api_passphrase=values["clob_api_passphrase"],
        relayer_api_key=relayer_key,
        relayer_api_address=relayer_address,
    )


def open_live_client(credentials: LivePilotCredentials) -> SecureClient:
    clob = ApiKeyCreds.model_validate(
        {
            "apiKey": credentials.clob_api_key,
            "secret": credentials.clob_api_secret,
            "passphrase": credentials.clob_api_passphrase,
        }
    )
    relayer = None
    if credentials.relayer_api_key and credentials.relayer_api_address:
        relayer = RelayerApiKey(
            key=credentials.relayer_api_key,
            address=credentials.relayer_api_address,
        )
    client = _secure_client_without_wallet_setup(
        private_key=credentials.private_key,
        wallet=credentials.wallet,
        credentials=clob,
        api_key=relayer,
    )
    if str(client.wallet).casefold() != credentials.wallet.casefold():
        client.close()
        raise LivePilotError("authenticated wallet does not match the credential file")
    if credentials.auth_mode == "session_key":
        if str(client.wallet_type) != "DEPOSIT_WALLET":
            client.close()
            raise LivePilotError("session-key mode requires a Deposit Wallet account")
        if str(client.signer).casefold() == str(client.wallet).casefold():
            client.close()
            raise LivePilotError("session-key mode unexpectedly authenticated the wallet owner")
    return client


def provision_live_credentials(
    *,
    output_path: Path,
    auth_mode: AuthMode,
    wallet: str,
    private_key: str,
    relayer_api_key: str | None = None,
    relayer_api_address: str | None = None,
) -> dict[str, str]:
    """Create/derive CLOB auth and persist it without printing any secret value.

    The wallet must already exist. This helper never calls a transfer, approval,
    wallet-setup, order-signing, or order-posting method.
    """

    target = output_path.expanduser().resolve()
    if target.exists() or target.is_symlink():
        raise LivePilotError("live credential file already exists; rotate it explicitly")
    if not wallet.strip() or not private_key.strip():
        raise LivePilotError("wallet and private key are required")
    if bool(relayer_api_key) != bool(relayer_api_address):
        raise LivePilotError("relayer key and address must be supplied together")
    api_key = None
    if relayer_api_key and relayer_api_address:
        api_key = RelayerApiKey(key=relayer_api_key, address=relayer_api_address)
    client = _secure_client_without_wallet_setup(
        private_key=private_key,
        wallet=wallet,
        api_key=api_key,
    )
    try:
        if str(client.wallet).casefold() != wallet.casefold():
            raise LivePilotError("authenticated wallet does not match the requested wallet")
        if auth_mode == "session_key":
            if str(client.wallet_type) != "DEPOSIT_WALLET":
                raise LivePilotError("session-key mode requires an existing Deposit Wallet")
            if str(client.signer).casefold() == str(client.wallet).casefold():
                raise LivePilotError("session-key mode unexpectedly used the wallet owner")
        credentials = client.credentials
        payload: dict[str, str] = {
            "kind": CREDENTIAL_KIND,
            "auth_mode": auth_mode,
            "wallet": str(client.wallet),
            "private_key": private_key,
            "clob_api_key": credentials.key,
            "clob_api_secret": credentials.secret,
            "clob_api_passphrase": credentials.passphrase,
        }
        if relayer_api_key and relayer_api_address:
            payload["relayer_api_key"] = relayer_api_key
            payload["relayer_api_address"] = relayer_api_address
        _atomic_private_json(target, payload)
        return {
            "credential_path": str(target),
            "auth_mode": auth_mode,
            "wallet": str(client.wallet),
            "signer": str(client.signer),
            "wallet_type": str(client.wallet_type),
            "order_signed": "false",
            "order_posted": "false",
        }
    finally:
        client.close()


def account_check(
    *,
    settings: Settings,
    credentials_path: Path,
    journal: LivePilotJournal,
) -> AccountCheck:
    credentials = load_live_credentials(credentials_path)
    geoblock = fetch_geoblock_status(
        url=settings.geoblock_url,
        timeout=min(settings.http_timeout_seconds, 20.0),
    )
    blockers: list[str] = []
    if geoblock.blocked:
        blockers.append("network_geoblocked")
    with open_live_client(credentials) as client:
        balance = client.get_balance_allowance(asset_type="COLLATERAL")
        balance_units = int(balance.balance)
        allowances = {str(key): int(value) for key, value in balance.allowances.items()}
        open_orders = tuple(client.list_open_orders().iter_items())
        positions = tuple(client.list_positions(user=str(client.wallet)).iter_items())
        open_positions = tuple(
            item
            for item in positions
            if getattr(item, "size", None) is not None and Decimal(str(item.size)) > 0
        )
        trades = tuple(client.list_account_trades().first_page().items)
        closed_only = bool(client.get_closed_only_mode())
        if balance_units > int(PILOT_MAX_WALLET_USD * COLLATERAL_BASE_UNITS):
            blockers.append("collateral_balance_exceeds_10_usd")
        if balance_units <= 0:
            blockers.append("collateral_balance_is_zero")
        if open_orders:
            blockers.append("existing_open_orders")
        if open_positions:
            blockers.append("existing_open_positions")
        if closed_only:
            blockers.append("account_closed_only")
        if not allowances or max(allowances.values()) < 1:
            blockers.append("collateral_allowance_is_zero")
        gate = journal.gate()
        if gate is not None:
            blockers.append("one_shot_slot_already_reserved")
        return AccountCheck(
            checked_at_utc=datetime.now(UTC).isoformat(),
            network_allowed=not geoblock.blocked,
            network_country=geoblock.country,
            network_region=geoblock.region,
            auth_mode=credentials.auth_mode,
            wallet=str(client.wallet),
            signer=str(client.signer),
            wallet_type=str(client.wallet_type),
            collateral_balance_usd=str(
                (Decimal(balance_units) / COLLATERAL_BASE_UNITS).quantize(Decimal("0.000001"))
            ),
            collateral_allowances=allowances,
            open_orders=len(open_orders),
            open_positions=len(open_positions),
            account_trades=len(trades),
            closed_only_mode=closed_only,
            one_shot_gate=gate,
            ready_for_prepare=not blockers,
            blockers=tuple(blockers),
        )


def live_pilot_status(journal: LivePilotJournal) -> dict[str, Any]:
    summary = journal.summary()
    return {
        "one_shot_gate": journal.gate(),
        **summary,
        "max_wallet_usd": str(PILOT_MAX_WALLET_USD),
        "max_buy_notional_usd": str(PILOT_MAX_BUY_NOTIONAL_USD),
        "max_all_in_spend_usd": str(PILOT_MAX_BUY_USD),
    }


def preview_intent_from_decision(
    *,
    settings: Settings,
    decision_id: int,
    client: Any,
    now: datetime | None = None,
) -> LiveBuyIntent:
    current = (now or datetime.now(UTC)).astimezone(UTC)
    with sqlite3.connect(
        f"file:{settings.database_path.resolve()}?mode=ro", uri=True
    ) as connection:
        connection.row_factory = sqlite3.Row
        row = connection.execute(
            "SELECT id,event_id,market_id,action,created_at,payload_json "
            "FROM decisions WHERE id = ?",
            (decision_id,),
        ).fetchone()
        if row is None:
            raise LivePilotError("paper decision does not exist")
        newer = connection.execute(
            "SELECT id,action FROM decisions WHERE market_id = ? AND id > ? "
            "ORDER BY id DESC LIMIT 1",
            (str(row["market_id"]), int(row["id"])),
        ).fetchone()
    try:
        payload = json.loads(str(row["payload_json"]))
    except json.JSONDecodeError as error:
        raise LivePilotError("paper decision payload is invalid") from error
    if not isinstance(payload, dict):
        raise LivePilotError("paper decision payload is invalid")
    row_action = str(row["action"])
    payload_action = str(payload.get("action", ""))
    reason_codes = payload.get("reason_codes")
    virtual_monitor_only = (
        row_action == "OBSERVE"
        and payload_action == "OBSERVE"
        and reason_codes == ["ACTIVE_PAPER_EVENT_MONITOR_ONLY"]
    )
    if (
        not (row_action == "PAPER_BUY" and payload_action == "PAPER_BUY")
        and not virtual_monitor_only
    ):
        raise LivePilotError(
            "live pilot requires PAPER_BUY or a v1 candidate suppressed solely by "
            "the virtual-paper duplicate guard"
        )
    if payload.get("strategy_version") != "v1":
        raise LivePilotError("live pilot is fixed to the existing v1 strategy")
    if virtual_monitor_only:
        try:
            edge = Decimal(str(payload["probability_edge"]))
            expected_profit = Decimal(str(payload["expected_profit_usd"]))
        except (KeyError, ValueError) as error:
            raise LivePilotError("monitor-only v1 candidate is missing risk metrics") from error
        if edge < settings.min_probability_edge:
            raise LivePilotError("monitor-only v1 candidate no longer meets the edge gate")
        if expected_profit < settings.min_expected_profit_usd:
            raise LivePilotError("monitor-only v1 candidate no longer meets the EV gate")
    if newer is not None:
        raise LivePilotError(
            f"paper decision is superseded by decision {int(newer['id'])} ({newer['action']})"
        )
    created = _parse_time(payload.get("created_at", row["created_at"]))
    expires = created + timedelta(seconds=settings.max_book_age_seconds)
    if current >= expires:
        raise LivePilotError("paper decision is stale; wait for a new v1 candidate")
    end_date = _parse_time(payload.get("end_date"))
    if current >= end_date:
        raise LivePilotError("market target has already ended")
    market_id = str(payload.get("market_id", ""))
    event_id = str(payload.get("event_id", ""))
    token_id = str(payload.get("token_id", ""))
    condition_id = str(payload.get("condition_id", ""))
    book_hash = str(payload.get("book_hash", ""))
    if not all((market_id, event_id, token_id, condition_id, book_hash)):
        raise LivePilotError("paper decision is missing immutable market identity")
    market = client.get_market(id=market_id)
    if str(getattr(market, "id", "")) != market_id:
        raise LivePilotError("market identity changed")
    if str(getattr(market, "condition_id", "")) != condition_id:
        raise LivePilotError("market condition identity changed")
    outcomes = getattr(market, "outcomes", None)
    market_tokens = {
        str(getattr(getattr(outcomes, side, None), "token_id", "")) for side in ("yes", "no")
    }
    if token_id not in market_tokens:
        raise LivePilotError("token no longer belongs to the selected market")
    state = getattr(market, "state", None)
    if state is not None and not bool(getattr(state, "accepting_orders", False)):
        raise LivePilotError("market no longer accepts orders")
    fee_rate, fee_exponent = resolve_platform_fee_info(
        client,
        market=market,
        condition_id=condition_id,
    )
    try:
        stored_fee_rate = Decimal(str(payload["fee_rate"]))
        stored_fee_exponent = Decimal(str(payload["fee_exponent"]))
    except (KeyError, ValueError) as error:
        raise LivePilotError("paper decision is missing fee provenance") from error
    if stored_fee_rate != fee_rate or stored_fee_exponent != fee_exponent:
        raise LivePilotError("platform fee metadata changed; wait for a new v1 candidate")
    amount = Decimal(str(payload.get("notional_usd", "0")))
    if amount <= 0 or amount > PILOT_MAX_BUY_NOTIONAL_USD:
        raise LivePilotError("paper notional is outside the live-pilot cap")
    max_price = Decimal(str(payload.get("executable_price", "0")))
    book = client.get_order_book(token_id=token_id)
    if str(getattr(book, "asset_id", "")) != token_id:
        raise LivePilotError("order book token identity changed")
    if str(getattr(book, "condition_id", "")) != condition_id:
        raise LivePilotError("order book condition identity changed")
    current_book_hash = str(getattr(book, "hash", ""))
    if not current_book_hash:
        raise LivePilotError("current order book has no immutable hash")
    if current_book_hash != book_hash:
        current_limit = _fok_buy_limit_from_book(book, amount_usd=amount)
        if current_limit > max_price:
            raise LivePilotError(
                "order book moved above the v1 candidate price; wait for a new candidate"
            )
    minimum_size = Decimal(str(getattr(book, "min_order_size", "0") or "0"))
    if minimum_size > 0 and amount / max_price < minimum_size:
        raise LivePilotError("live candidate is below the market minimum order size")
    worst_spend = (amount * (Decimal(1) + fee_rate)).quantize(
        Decimal("0.000001"), rounding=ROUND_UP
    )
    max_spend = min(worst_spend, PILOT_MAX_BUY_USD)
    return LiveBuyIntent(
        strategy="open-meteo-truncated-normal-v1",
        event_id=event_id,
        market_id=market_id,
        condition_id=condition_id,
        token_id=token_id,
        amount_usd=amount,
        max_spend_usd=max_spend,
        max_price=max_price,
        book_hash=current_book_hash,
        decision_created_at_utc=created,
        expires_at_utc=expires,
    )


def write_prepared_intent(
    *, journal: LivePilotJournal, intent: LiveBuyIntent, output_path: Path
) -> dict[str, Any]:
    if journal.gate() is not None:
        raise LivePilotError("the one-shot live pilot slot was already consumed")
    target = output_path.expanduser().resolve()
    payload = {
        "kind": INTENT_KIND,
        "intent_sha256": intent.digest,
        "intent": _intent_dict(intent),
    }
    _atomic_private_json(target, payload)
    return {
        "intent_path": str(target),
        "intent_sha256": intent.digest,
        "state": "PREPARED_UNSIGNED",
        "one_shot_reserved": False,
        "post_attempted": False,
        "signed": False,
        "intent": _intent_dict(intent),
    }


def load_live_intent(path: Path) -> LiveBuyIntent:
    resolved = _private_file(path, label="live intent")
    try:
        raw = json.loads(resolved.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise LivePilotError("invalid live intent file") from error
    if not isinstance(raw, dict) or set(raw) != {"kind", "intent_sha256", "intent"}:
        raise LivePilotError("live intent file does not match the fixed v1 schema")
    if raw.get("kind") != INTENT_KIND or not isinstance(raw.get("intent"), dict):
        raise LivePilotError("invalid live intent kind or payload")
    values = cast(dict[str, Any], raw["intent"])
    expected = {
        "strategy",
        "event_id",
        "market_id",
        "condition_id",
        "token_id",
        "amount_usd",
        "max_spend_usd",
        "max_price",
        "book_hash",
        "decision_created_at_utc",
        "expires_at_utc",
        "order_type",
        "side",
    }
    if set(values) != expected:
        raise LivePilotError("live intent fields do not match the fixed v1 schema")
    intent = LiveBuyIntent(
        strategy=str(values["strategy"]),
        event_id=str(values["event_id"]),
        market_id=str(values["market_id"]),
        condition_id=str(values["condition_id"]),
        token_id=str(values["token_id"]),
        amount_usd=Decimal(str(values["amount_usd"])),
        max_spend_usd=Decimal(str(values["max_spend_usd"])),
        max_price=Decimal(str(values["max_price"])),
        book_hash=str(values["book_hash"]),
        decision_created_at_utc=_parse_time(values["decision_created_at_utc"]),
        expires_at_utc=_parse_time(values["expires_at_utc"]),
        order_type=str(values["order_type"]),
        side=str(values["side"]),
    )
    if str(raw.get("intent_sha256")) != intent.digest:
        raise LivePilotError("live intent digest mismatch")
    return intent


def intent_report(intent: LiveBuyIntent) -> dict[str, Any]:
    return {
        "kind": INTENT_KIND,
        "intent_sha256": intent.digest,
        "intent": _intent_dict(intent),
        "signed": False,
        "post_attempted": False,
    }


def _intent_dict(intent: LiveBuyIntent) -> dict[str, Any]:
    payload = asdict(intent)
    return {
        key: (
            value.astimezone(UTC).isoformat()
            if isinstance(value, datetime)
            else str(value)
            if isinstance(value, Decimal)
            else value
        )
        for key, value in payload.items()
    }


def _fok_buy_limit_from_book(book: Any, *, amount_usd: Decimal) -> Decimal:
    asks = tuple(getattr(book, "asks", ()) or ())
    levels: list[tuple[Decimal, Decimal]] = []
    for level in asks:
        try:
            price = Decimal(str(level.price))
            size = Decimal(str(level.size))
        except (AttributeError, ValueError) as error:
            raise LivePilotError("current order book contains an invalid ask") from error
        if price <= 0 or size <= 0:
            continue
        levels.append((price, size))
    remaining = amount_usd
    limiting_price: Decimal | None = None
    for price, size in sorted(levels):
        available_cash = price * size
        limiting_price = price
        if remaining <= available_cash:
            return limiting_price
        remaining -= available_cash
    raise LivePilotError("current order book lacks FOK liquidity for the pilot amount")


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


def _optional_string(value: object) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip()
    return normalized or None


def _secure_client_without_wallet_setup(
    *,
    private_key: str,
    wallet: str,
    credentials: ApiKeyCreds | None = None,
    api_key: RelayerApiKey | None = None,
) -> SecureClient:
    """Authenticate without allowing the SDK to deploy a missing wallet.

    ``SecureClient.create`` calls a wallet-readiness helper that may deploy a
    default Deposit Wallet. This pilot accepts only an already existing account,
    so the pinned SDK's non-deploying constructor is deliberately used here.
    """

    factory = cast(Any, SecureClient._create)  # pyright: ignore[reportPrivateUsage]
    return cast(
        SecureClient,
        factory(
            private_key=private_key,
            wallet=wallet,
            credentials=credentials,
            api_key=api_key,
            nonce=0,
            validate_credentials=True,
        ),
    )


def _atomic_private_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp-{os.getpid()}")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        temporary.replace(path)
        path.chmod(0o600)
    finally:
        temporary.unlink(missing_ok=True)


def _parse_time(value: object) -> datetime:
    if value is None:
        raise LivePilotError("required timestamp is missing")
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError as error:
        raise LivePilotError("invalid timestamp in live-pilot input") from error
    if parsed.tzinfo is None:
        raise LivePilotError("live-pilot timestamps must include a timezone")
    return parsed.astimezone(UTC)


__all__ = [
    "AccountCheck",
    "CREDENTIAL_KIND",
    "INTENT_KIND",
    "LivePilotCredentials",
    "account_check",
    "intent_report",
    "live_pilot_status",
    "load_live_credentials",
    "load_live_intent",
    "open_live_client",
    "provision_live_credentials",
    "preview_intent_from_decision",
    "write_prepared_intent",
]
