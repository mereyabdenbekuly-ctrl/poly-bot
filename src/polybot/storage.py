from __future__ import annotations

import json
import sqlite3
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

from polybot.models import MarketDecision, MarketSnapshot, RuleAudit, WeatherForecast


def utc_now() -> datetime:
    return datetime.now(UTC)


class BudgetExceededError(RuntimeError):
    pass


class PaperRiskRejectedError(RuntimeError):
    pass


class Storage:
    def __init__(self, path: Path) -> None:
        self.path = path.expanduser().resolve()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._initialize()

    def connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path, timeout=30)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA journal_mode = WAL")
        return connection

    @contextmanager
    def transaction(self, *, immediate: bool = False) -> Iterator[sqlite3.Connection]:
        connection = self.connect()
        try:
            connection.execute("BEGIN IMMEDIATE" if immediate else "BEGIN")
            yield connection
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def _initialize(self) -> None:
        with self.connect() as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS scan_runs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    started_at TEXT NOT NULL,
                    completed_at TEXT,
                    query TEXT NOT NULL,
                    mode TEXT NOT NULL,
                    geoblocked INTEGER,
                    status TEXT NOT NULL,
                    error TEXT
                );

                CREATE TABLE IF NOT EXISTS rule_cache (
                    rules_hash TEXT PRIMARY KEY,
                    parser TEXT NOT NULL,
                    model TEXT,
                    interpretation_json TEXT NOT NULL,
                    input_tokens INTEGER NOT NULL DEFAULT 0,
                    output_tokens INTEGER NOT NULL DEFAULT 0,
                    cost_usd TEXT NOT NULL DEFAULT '0',
                    created_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS api_usage (
                    reservation_id TEXT PRIMARY KEY,
                    model TEXT NOT NULL,
                    status TEXT NOT NULL,
                    estimated_cost_usd TEXT NOT NULL,
                    actual_cost_usd TEXT,
                    input_tokens INTEGER,
                    output_tokens INTEGER,
                    error TEXT,
                    created_at TEXT NOT NULL,
                    settled_at TEXT
                );

                CREATE TABLE IF NOT EXISTS weather_snapshots (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id INTEGER NOT NULL REFERENCES scan_runs(id),
                    event_id TEXT NOT NULL,
                    fetched_at TEXT NOT NULL,
                    payload_json TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS market_snapshots (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id INTEGER NOT NULL REFERENCES scan_runs(id),
                    event_id TEXT NOT NULL,
                    market_id TEXT NOT NULL,
                    captured_at TEXT NOT NULL,
                    payload_json TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS decisions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id INTEGER NOT NULL REFERENCES scan_runs(id),
                    event_id TEXT NOT NULL,
                    market_id TEXT NOT NULL,
                    action TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    paper_order_id INTEGER
                );

                CREATE TABLE IF NOT EXISTS paper_orders (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    idempotency_key TEXT NOT NULL UNIQUE,
                    event_id TEXT NOT NULL,
                    market_id TEXT NOT NULL,
                    asset_id TEXT NOT NULL,
                    status TEXT NOT NULL,
                    shares TEXT NOT NULL,
                    entry_price TEXT NOT NULL,
                    notional_usd TEXT NOT NULL,
                    fee_usd TEXT NOT NULL,
                    api_cost_usd TEXT NOT NULL,
                    execution_buffer_usd TEXT NOT NULL,
                    max_loss_usd TEXT NOT NULL,
                    expected_profit_usd TEXT,
                    opened_at TEXT NOT NULL,
                    closed_at TEXT,
                    won INTEGER,
                    realized_pnl_usd TEXT
                );

                CREATE UNIQUE INDEX IF NOT EXISTS paper_one_open_order_per_event
                    ON paper_orders(event_id) WHERE status = 'open';
                CREATE INDEX IF NOT EXISTS decisions_run_idx ON decisions(run_id);
                CREATE INDEX IF NOT EXISTS snapshots_run_idx ON market_snapshots(run_id);
                """
            )

    def start_scan(self, *, query: str, mode: str) -> int:
        with self.connect() as connection:
            cursor = connection.execute(
                """
                INSERT INTO scan_runs(started_at, query, mode, status)
                VALUES (?, ?, ?, 'running')
                """,
                (utc_now().isoformat(), query, mode),
            )
            return _lastrowid(cursor)

    def finish_scan(
        self,
        run_id: int,
        *,
        geoblocked: bool | None,
        status: str = "completed",
        error: str | None = None,
    ) -> None:
        with self.connect() as connection:
            connection.execute(
                """
                UPDATE scan_runs
                SET completed_at = ?, geoblocked = ?, status = ?, error = ?
                WHERE id = ?
                """,
                (
                    utc_now().isoformat(),
                    None if geoblocked is None else int(geoblocked),
                    status,
                    error,
                    run_id,
                ),
            )

    def get_rule_cache(self, rules_hash: str) -> RuleAudit | None:
        with self.connect() as connection:
            row = connection.execute(
                "SELECT * FROM rule_cache WHERE rules_hash = ?", (rules_hash,)
            ).fetchone()
        if row is None:
            return None
        return RuleAudit.model_validate(
            {
                "rules_hash": row["rules_hash"],
                "parser": row["parser"],
                "interpretation": json.loads(row["interpretation_json"]),
                # The historical cost remains in rule_cache/api_usage. Reusing a
                # cached interpretation has zero marginal model cost this scan.
                "astra_cost_usd": "0",
                "astra_input_tokens": 0,
                "astra_output_tokens": 0,
                "cached": True,
            }
        )

    def put_rule_cache(self, audit: RuleAudit, *, model: str | None = None) -> None:
        with self.connect() as connection:
            connection.execute(
                """
                INSERT OR REPLACE INTO rule_cache(
                    rules_hash, parser, model, interpretation_json, input_tokens,
                    output_tokens, cost_usd, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    audit.rules_hash,
                    audit.parser,
                    model,
                    audit.interpretation.model_dump_json(),
                    audit.astra_input_tokens,
                    audit.astra_output_tokens,
                    str(audit.astra_cost_usd),
                    utc_now().isoformat(),
                ),
            )

    def reserve_api_budget(self, *, model: str, estimate: Decimal, budget: Decimal) -> str:
        reservation_id = uuid.uuid4().hex
        with self.transaction(immediate=True) as connection:
            rows = connection.execute(
                """
                SELECT status, estimated_cost_usd, actual_cost_usd
                FROM api_usage
                WHERE status IN ('reserved', 'settled')
                """
            ).fetchall()
            committed = sum(
                (
                    Decimal(row["actual_cost_usd"])
                    if row["status"] == "settled" and row["actual_cost_usd"] is not None
                    else Decimal(row["estimated_cost_usd"])
                )
                for row in rows
            )
            if committed + estimate > budget:
                raise BudgetExceededError(
                    f"Astra budget exhausted: committed ${committed}, "
                    f"requested reserve ${estimate}, limit ${budget}."
                )
            connection.execute(
                """
                INSERT INTO api_usage(
                    reservation_id, model, status, estimated_cost_usd, created_at
                ) VALUES (?, ?, 'reserved', ?, ?)
                """,
                (reservation_id, model, str(estimate), utc_now().isoformat()),
            )
        return reservation_id

    def settle_api_budget(
        self,
        reservation_id: str,
        *,
        actual_cost: Decimal,
        input_tokens: int,
        output_tokens: int,
    ) -> None:
        with self.connect() as connection:
            connection.execute(
                """
                UPDATE api_usage
                SET status = 'settled', actual_cost_usd = ?, input_tokens = ?,
                    output_tokens = ?, settled_at = ?
                WHERE reservation_id = ?
                """,
                (
                    str(actual_cost),
                    input_tokens,
                    output_tokens,
                    utc_now().isoformat(),
                    reservation_id,
                ),
            )

    def fail_api_budget(self, reservation_id: str, error: str) -> None:
        with self.connect() as connection:
            connection.execute(
                """
                UPDATE api_usage
                SET status = 'failed', error = ?, settled_at = ?
                WHERE reservation_id = ?
                """,
                (error[:2000], utc_now().isoformat(), reservation_id),
            )

    def api_spend(self) -> tuple[Decimal, Decimal]:
        with self.connect() as connection:
            rows = connection.execute(
                "SELECT status, estimated_cost_usd, actual_cost_usd FROM api_usage"
            ).fetchall()
        settled = sum(
            (Decimal(row["actual_cost_usd"] or "0") for row in rows if row["status"] == "settled"),
            Decimal(0),
        )
        reserved = sum(
            (Decimal(row["estimated_cost_usd"]) for row in rows if row["status"] == "reserved"),
            Decimal(0),
        )
        return settled, reserved

    def record_weather(self, run_id: int, event_id: str, forecast: WeatherForecast) -> None:
        with self.connect() as connection:
            connection.execute(
                """
                INSERT INTO weather_snapshots(run_id, event_id, fetched_at, payload_json)
                VALUES (?, ?, ?, ?)
                """,
                (run_id, event_id, forecast.fetched_at.isoformat(), forecast.model_dump_json()),
            )

    def record_market_snapshot(self, run_id: int, snapshot: MarketSnapshot) -> None:
        with self.connect() as connection:
            connection.execute(
                """
                INSERT INTO market_snapshots(run_id, event_id, market_id, captured_at, payload_json)
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    snapshot.event_id,
                    snapshot.market_id,
                    utc_now().isoformat(),
                    snapshot.model_dump_json(),
                ),
            )

    def record_decision(
        self, run_id: int, decision: MarketDecision, *, paper_order_id: int | None = None
    ) -> int:
        with self.connect() as connection:
            cursor = connection.execute(
                """
                INSERT INTO decisions(
                    run_id, event_id, market_id, action, created_at, payload_json, paper_order_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    decision.event_id,
                    decision.market_id,
                    decision.action.value,
                    decision.created_at.isoformat(),
                    decision.model_dump_json(),
                    paper_order_id,
                ),
            )
            return _lastrowid(cursor)

    def open_paper_order(
        self,
        decision: MarketDecision,
        *,
        idempotency_key: str,
        max_event_risk: Decimal,
        max_total_risk: Decimal,
    ) -> int:
        required = {
            "shares": decision.shares,
            "entry_price": decision.executable_price,
            "notional": decision.notional_usd,
            "fee": decision.fee_usd,
            "max_loss": decision.max_loss_usd,
        }
        missing = [key for key, value in required.items() if value is None]
        if missing:
            raise ValueError(f"Paper order is missing: {', '.join(missing)}")
        assert decision.max_loss_usd is not None

        with self.transaction(immediate=True) as connection:
            existing = connection.execute(
                "SELECT id FROM paper_orders WHERE idempotency_key = ?", (idempotency_key,)
            ).fetchone()
            if existing is not None:
                return int(existing["id"])

            event_row = connection.execute(
                """
                SELECT COALESCE(SUM(CAST(max_loss_usd AS REAL)), 0) AS exposure
                FROM paper_orders WHERE status = 'open' AND event_id = ?
                """,
                (decision.event_id,),
            ).fetchone()
            total_row = connection.execute(
                """
                SELECT COALESCE(SUM(CAST(max_loss_usd AS REAL)), 0) AS exposure
                FROM paper_orders WHERE status = 'open'
                """
            ).fetchone()
            event_exposure = Decimal(str(event_row["exposure"]))
            total_exposure = Decimal(str(total_row["exposure"]))
            if event_exposure + decision.max_loss_usd > max_event_risk:
                raise PaperRiskRejectedError(
                    f"event exposure ${event_exposure} + ${decision.max_loss_usd} "
                    f"exceeds ${max_event_risk}"
                )
            if total_exposure + decision.max_loss_usd > max_total_risk:
                raise PaperRiskRejectedError(
                    f"total exposure ${total_exposure} + ${decision.max_loss_usd} "
                    f"exceeds ${max_total_risk}"
                )

            try:
                cursor = connection.execute(
                    """
                    INSERT INTO paper_orders(
                        idempotency_key, event_id, market_id, asset_id, status, shares,
                        entry_price, notional_usd, fee_usd, api_cost_usd,
                        execution_buffer_usd, max_loss_usd, expected_profit_usd, opened_at
                    ) VALUES (?, ?, ?, ?, 'open', ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        idempotency_key,
                        decision.event_id,
                        decision.market_id,
                        decision.asset_id,
                        str(decision.shares),
                        str(decision.executable_price),
                        str(decision.notional_usd),
                        str(decision.fee_usd),
                        str(decision.api_cost_usd),
                        str(decision.execution_buffer_usd),
                        str(decision.max_loss_usd),
                        None
                        if decision.expected_profit_usd is None
                        else str(decision.expected_profit_usd),
                        decision.created_at.isoformat(),
                    ),
                )
            except sqlite3.IntegrityError as error:
                raise PaperRiskRejectedError(
                    "an open paper order already exists for this event"
                ) from error
            return _lastrowid(cursor)

    def settle_paper_order(self, market_id: str, *, won: bool) -> Decimal:
        with self.transaction(immediate=True) as connection:
            row = connection.execute(
                """
                SELECT * FROM paper_orders
                WHERE market_id = ? AND status = 'open'
                ORDER BY id DESC LIMIT 1
                """,
                (market_id,),
            ).fetchone()
            if row is None:
                raise ValueError(f"No open paper order for market {market_id}")
            payout = Decimal(row["shares"]) if won else Decimal(0)
            costs = sum(
                Decimal(row[name])
                for name in (
                    "notional_usd",
                    "fee_usd",
                    "api_cost_usd",
                    "execution_buffer_usd",
                )
            )
            pnl = payout - costs
            connection.execute(
                """
                UPDATE paper_orders
                SET status = 'closed', closed_at = ?, won = ?, realized_pnl_usd = ?
                WHERE id = ?
                """,
                (utc_now().isoformat(), int(won), str(pnl), row["id"]),
            )
        return pnl

    def open_paper_market_ids(self) -> list[str]:
        with self.connect() as connection:
            rows = connection.execute(
                "SELECT market_id FROM paper_orders WHERE status = 'open' ORDER BY id"
            ).fetchall()
        return [str(row["market_id"]) for row in rows]

    def portfolio_summary(self) -> dict[str, Any]:
        with self.connect() as connection:
            open_row = connection.execute(
                """
                SELECT COUNT(*) AS count,
                       COALESCE(SUM(CAST(max_loss_usd AS REAL)), 0) AS exposure
                FROM paper_orders WHERE status = 'open'
                """
            ).fetchone()
            closed_row = connection.execute(
                """
                SELECT COUNT(*) AS count,
                       COALESCE(SUM(CAST(realized_pnl_usd AS REAL)), 0) AS pnl
                FROM paper_orders WHERE status = 'closed'
                """
            ).fetchone()
            last_scan = connection.execute(
                "SELECT * FROM scan_runs ORDER BY id DESC LIMIT 1"
            ).fetchone()
            recent_orders = connection.execute(
                "SELECT * FROM paper_orders ORDER BY id DESC LIMIT 20"
            ).fetchall()
        settled, reserved = self.api_spend()
        return {
            "open_orders": int(open_row["count"]),
            "open_exposure_usd": Decimal(str(open_row["exposure"])),
            "closed_orders": int(closed_row["count"]),
            "realized_pnl_usd": Decimal(str(closed_row["pnl"])),
            "api_spend_usd": settled,
            "api_reserved_usd": reserved,
            "last_scan": None if last_scan is None else dict(last_scan),
            "recent_orders": [dict(row) for row in recent_orders],
        }

    def realized_pnl_for_day(self, day: date) -> Decimal:
        prefix = day.isoformat()
        with self.connect() as connection:
            row = connection.execute(
                """
                SELECT COALESCE(SUM(CAST(realized_pnl_usd AS REAL)), 0) AS pnl
                FROM paper_orders
                WHERE status = 'closed' AND substr(closed_at, 1, 10) = ?
                """,
                (prefix,),
            ).fetchone()
        return Decimal(str(row["pnl"]))


def _lastrowid(cursor: sqlite3.Cursor) -> int:
    if cursor.lastrowid is None:
        raise RuntimeError("SQLite did not return a row id")
    return int(cursor.lastrowid)
