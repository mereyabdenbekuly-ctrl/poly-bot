from __future__ import annotations

import json
import sqlite3
import uuid
from collections.abc import Iterator, Sequence
from contextlib import closing, contextmanager
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, cast

from polybot.models import (
    EventDefinition,
    MarketDecision,
    MarketSnapshot,
    OutcomeSide,
    PaperMark,
    PaperOrderStatus,
    PaperOrderTarget,
    ResolutionCheck,
    RuleAudit,
    RuntimeReport,
    RuntimeWindow,
    WeatherForecast,
    WeatherNextPaperOrderTarget,
)
from polybot.observations import ObservationHistory

if TYPE_CHECKING:
    from polybot.forecast_store import ForecastStore


def utc_now() -> datetime:
    return datetime.now(UTC)


class BudgetExceededError(RuntimeError):
    pass


class PaperRiskRejectedError(RuntimeError):
    pass


class Storage:
    def __init__(self, path: Path, *, read_only: bool = False) -> None:
        self.path = path.expanduser().resolve()
        self.read_only = read_only
        if self.read_only:
            if not self.path.is_file():
                raise FileNotFoundError(f"Polybot database does not exist: {self.path}")
        else:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            self._initialize()

    def connect(self) -> sqlite3.Connection:
        if self.read_only:
            connection = sqlite3.connect(f"{self.path.as_uri()}?mode=ro", uri=True, timeout=30)
        else:
            connection = sqlite3.connect(self.path, timeout=30)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        if self.read_only:
            connection.execute("PRAGMA query_only = ON")
        else:
            connection.execute("PRAGMA journal_mode = WAL")
        return connection

    @contextmanager
    def transaction(self, *, immediate: bool = False) -> Iterator[sqlite3.Connection]:
        if self.read_only:
            raise RuntimeError("read-only storage cannot start a transaction")
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
        with closing(self.connect()) as connection, connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS runtime_windows (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    started_at TEXT NOT NULL,
                    ended_at TEXT,
                    status TEXT NOT NULL,
                    query TEXT NOT NULL,
                    interval_seconds INTEGER NOT NULL,
                    paper INTEGER NOT NULL,
                    astra INTEGER NOT NULL
                );

                CREATE TABLE IF NOT EXISTS scan_runs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    started_at TEXT NOT NULL,
                    completed_at TEXT,
                    query TEXT NOT NULL,
                    mode TEXT NOT NULL,
                    geoblocked INTEGER,
                    status TEXT NOT NULL,
                    error TEXT,
                    window_id INTEGER REFERENCES runtime_windows(id)
                );

                CREATE TABLE IF NOT EXISTS runtime_reports (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    window_id INTEGER NOT NULL REFERENCES runtime_windows(id),
                    kind TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    elapsed_seconds INTEGER NOT NULL,
                    payload_json TEXT NOT NULL,
                    UNIQUE(window_id, kind)
                );

                CREATE TABLE IF NOT EXISTS shadow_research_runs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    parent_scan_run_id INTEGER NOT NULL REFERENCES scan_runs(id),
                    cohort_version TEXT NOT NULL,
                    started_at TEXT NOT NULL,
                    completed_at TEXT,
                    status TEXT NOT NULL,
                    outcome_requested INTEGER NOT NULL,
                    outcome_completed INTEGER NOT NULL DEFAULT 0,
                    outcome_pending INTEGER NOT NULL DEFAULT 0,
                    extra_requested INTEGER NOT NULL,
                    extra_registered INTEGER NOT NULL DEFAULT 0,
                    error TEXT
                );

                CREATE TABLE IF NOT EXISTS shadow_event_registry (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    research_run_id INTEGER NOT NULL REFERENCES shadow_research_runs(id),
                    cohort_version TEXT NOT NULL,
                    discovered_at TEXT NOT NULL,
                    event_id TEXT NOT NULL,
                    event_slug TEXT,
                    event_title TEXT NOT NULL,
                    observation_date TEXT,
                    market_count INTEGER NOT NULL,
                    accepting_market_count INTEGER NOT NULL,
                    earliest_end_at TEXT,
                    latest_end_at TEXT,
                    metadata_json TEXT NOT NULL,
                    UNIQUE(cohort_version, research_run_id, event_id)
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
                    run_id INTEGER REFERENCES scan_runs(id),
                    event_id TEXT,
                    rules_hash TEXT,
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

                CREATE TABLE IF NOT EXISTS weathernext_snapshots (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id INTEGER NOT NULL REFERENCES scan_runs(id),
                    event_id TEXT NOT NULL,
                    captured_at TEXT NOT NULL,
                    payload_json TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS observation_fetches (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id INTEGER NOT NULL REFERENCES scan_runs(id),
                    event_id TEXT NOT NULL,
                    station_id TEXT NOT NULL,
                    observation_date TEXT NOT NULL,
                    fetched_at TEXT NOT NULL,
                    payload_json TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS station_observation_versions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    station_id TEXT NOT NULL,
                    observed_at_utc TEXT NOT NULL,
                    revision_hash TEXT NOT NULL,
                    first_seen_at_utc TEXT NOT NULL,
                    last_seen_at_utc TEXT NOT NULL,
                    source TEXT NOT NULL,
                    temperature_c TEXT NOT NULL,
                    displayed_temperature_c TEXT NOT NULL,
                    corrected INTEGER NOT NULL,
                    payload_json TEXT NOT NULL,
                    UNIQUE(station_id, observed_at_utc, revision_hash)
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
                    strategy_version TEXT NOT NULL DEFAULT 'v0',
                    execution_model TEXT NOT NULL DEFAULT 'LEGACY_CROSSING_BOOK_SHARES',
                    validation_notes TEXT,
                    condition_id TEXT,
                    token_id TEXT,
                    outcome TEXT NOT NULL DEFAULT 'YES',
                    shares TEXT NOT NULL,
                    entry_price TEXT NOT NULL,
                    notional_usd TEXT NOT NULL,
                    fee_usd TEXT NOT NULL,
                    api_cost_usd TEXT NOT NULL,
                    execution_buffer_usd TEXT NOT NULL,
                    max_loss_usd TEXT NOT NULL,
                    expected_profit_usd TEXT,
                    fee_rate TEXT NOT NULL DEFAULT '0',
                    fee_exponent TEXT NOT NULL DEFAULT '0',
                    end_date TEXT,
                    identity_verified INTEGER NOT NULL DEFAULT 0,
                    opened_at TEXT NOT NULL,
                    awaiting_result_at TEXT,
                    resolved_at TEXT,
                    resolution_checked_at TEXT,
                    resolution_status TEXT,
                    resolution_source TEXT,
                    resolved_by TEXT,
                    resolution_json TEXT,
                    settled_at TEXT,
                    closed_at TEXT,
                    won INTEGER,
                    realized_pnl_usd TEXT
                );

                -- Isolated WeatherNext research ledger.  It intentionally has
                -- no foreign-key or index path into the v1 ``paper_orders``
                -- exposure/active-event queries.
                CREATE TABLE IF NOT EXISTS weathernext_paper_decisions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id INTEGER NOT NULL REFERENCES scan_runs(id),
                    event_id TEXT NOT NULL,
                    market_id TEXT NOT NULL,
                    action TEXT NOT NULL,
                    strategy_version TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    snapshot_init_time_utc TEXT,
                    snapshot_source_uri TEXT,
                    snapshot_member_count INTEGER,
                    probability TEXT,
                    executable_price TEXT,
                    probability_edge TEXT,
                    shares TEXT,
                    notional_usd TEXT,
                    fee_usd TEXT,
                    max_loss_usd TEXT,
                    expected_profit_usd TEXT,
                    reason_codes_json TEXT NOT NULL,
                    warning_codes_json TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    paper_order_id INTEGER
                );

                CREATE TABLE IF NOT EXISTS weathernext_paper_orders (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    idempotency_key TEXT NOT NULL UNIQUE,
                    run_id INTEGER NOT NULL REFERENCES scan_runs(id),
                    event_id TEXT NOT NULL,
                    market_id TEXT NOT NULL,
                    asset_id TEXT NOT NULL,
                    status TEXT NOT NULL,
                    strategy_version TEXT NOT NULL DEFAULT 'weathernext-paper-v1',
                    execution_model TEXT NOT NULL DEFAULT 'WEATHERNEXT_CROSSING_LIMIT_SHARES',
                    condition_id TEXT,
                    token_id TEXT,
                    outcome TEXT NOT NULL DEFAULT 'YES',
                    shares TEXT NOT NULL,
                    entry_price TEXT NOT NULL,
                    notional_usd TEXT NOT NULL,
                    fee_usd TEXT NOT NULL,
                    execution_buffer_usd TEXT NOT NULL DEFAULT '0',
                    max_loss_usd TEXT NOT NULL,
                    expected_profit_usd TEXT,
                    fee_rate TEXT NOT NULL DEFAULT '0',
                    fee_exponent TEXT NOT NULL DEFAULT '0',
                    end_date TEXT,
                    identity_verified INTEGER NOT NULL DEFAULT 0,
                    opened_at TEXT NOT NULL,
                    settled_at TEXT,
                    closed_at TEXT,
                    won INTEGER,
                    realized_pnl_usd TEXT,
                    resolution_json TEXT
                );

                CREATE TABLE IF NOT EXISTS weathernext_paper_marks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    paper_order_id INTEGER NOT NULL
                        REFERENCES weathernext_paper_orders(id),
                    captured_at TEXT NOT NULL,
                    payload_json TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS weathernext_paper_resolution_checks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    paper_order_id INTEGER NOT NULL
                        REFERENCES weathernext_paper_orders(id),
                    checked_at TEXT NOT NULL,
                    confirmed INTEGER NOT NULL,
                    won INTEGER,
                    payload_json TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS paper_order_transitions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    paper_order_id INTEGER NOT NULL REFERENCES paper_orders(id),
                    from_status TEXT,
                    to_status TEXT NOT NULL,
                    reason TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS paper_resolution_checks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    paper_order_id INTEGER NOT NULL REFERENCES paper_orders(id),
                    checked_at TEXT NOT NULL,
                    confirmed INTEGER NOT NULL,
                    won INTEGER,
                    payload_json TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS paper_marks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    paper_order_id INTEGER NOT NULL REFERENCES paper_orders(id),
                    captured_at TEXT NOT NULL,
                    payload_json TEXT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS decisions_run_idx ON decisions(run_id);
                CREATE INDEX IF NOT EXISTS runtime_reports_window_idx
                    ON runtime_reports(window_id, id);
                CREATE INDEX IF NOT EXISTS shadow_research_parent_idx
                    ON shadow_research_runs(parent_scan_run_id, id);
                CREATE INDEX IF NOT EXISTS shadow_event_registry_idx
                    ON shadow_event_registry(cohort_version, event_id, discovered_at, id);
                CREATE INDEX IF NOT EXISTS snapshots_run_idx ON market_snapshots(run_id);
                CREATE INDEX IF NOT EXISTS weathernext_snapshots_run_idx
                    ON weathernext_snapshots(run_id, event_id);
                CREATE INDEX IF NOT EXISTS observation_fetches_run_idx
                    ON observation_fetches(run_id, event_id);
                CREATE INDEX IF NOT EXISTS observation_versions_station_idx
                    ON station_observation_versions(station_id, observed_at_utc, id);
                CREATE INDEX IF NOT EXISTS paper_transitions_order_idx
                    ON paper_order_transitions(paper_order_id, id);
                CREATE INDEX IF NOT EXISTS paper_resolution_order_idx
                    ON paper_resolution_checks(paper_order_id, id);
                CREATE INDEX IF NOT EXISTS paper_marks_order_idx
                    ON paper_marks(paper_order_id, id);
                CREATE INDEX IF NOT EXISTS weathernext_paper_decisions_run_idx
                    ON weathernext_paper_decisions(run_id, id);
                CREATE INDEX IF NOT EXISTS weathernext_paper_decisions_event_idx
                    ON weathernext_paper_decisions(event_id, created_at, id);
                CREATE INDEX IF NOT EXISTS weathernext_paper_marks_order_idx
                    ON weathernext_paper_marks(paper_order_id, id);
                CREATE INDEX IF NOT EXISTS weathernext_paper_resolution_order_idx
                    ON weathernext_paper_resolution_checks(paper_order_id, id);
                CREATE UNIQUE INDEX IF NOT EXISTS weathernext_paper_one_active_event_idx
                    ON weathernext_paper_orders(event_id)
                    WHERE status IN ('OPEN', 'AWAITING_RESULT', 'RESOLVED');
                """
            )
            self._migrate_api_usage(connection)
            self._migrate_runtime_tables(connection)
            self._migrate_paper_orders(connection)

    @staticmethod
    def _migrate_runtime_tables(connection: sqlite3.Connection) -> None:
        scan_columns = {str(row[1]) for row in connection.execute("PRAGMA table_info(scan_runs)")}
        if "window_id" not in scan_columns:
            connection.execute(
                "ALTER TABLE scan_runs ADD COLUMN window_id INTEGER REFERENCES runtime_windows(id)"
            )
        connection.execute(
            "CREATE INDEX IF NOT EXISTS scan_runs_window_idx ON scan_runs(window_id, id)"
        )
        connection.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS runtime_active_window_idx "
            "ON runtime_windows(status) WHERE status = 'ACTIVE'"
        )

    @staticmethod
    def _migrate_api_usage(connection: sqlite3.Connection) -> None:
        columns = {str(row[1]) for row in connection.execute("PRAGMA table_info(api_usage)")}
        additions = {"run_id": "INTEGER", "event_id": "TEXT", "rules_hash": "TEXT"}
        for name, declaration in additions.items():
            if name not in columns:
                connection.execute(f"ALTER TABLE api_usage ADD COLUMN {name} {declaration}")

    def _migrate_paper_orders(self, connection: sqlite3.Connection) -> None:
        """Migrate databases created by pre-lifecycle releases in place.

        The old schema used lowercase ``open``/``closed`` and only stored an
        ``asset_id``.  Existing rows are retained as historical v0 records;
        token_id is backfilled from asset_id and status is mapped to the new
        lifecycle without inventing a resolution result.
        """

        columns = {
            str(row[1]) for row in connection.execute("PRAGMA table_info(paper_orders)").fetchall()
        }
        additions: dict[str, str] = {
            "condition_id": "TEXT",
            "strategy_version": "TEXT NOT NULL DEFAULT 'v0'",
            "execution_model": "TEXT NOT NULL DEFAULT 'LEGACY_CROSSING_BOOK_SHARES'",
            "validation_notes": "TEXT",
            "token_id": "TEXT",
            "outcome": "TEXT NOT NULL DEFAULT 'YES'",
            "fee_rate": "TEXT NOT NULL DEFAULT '0'",
            "fee_exponent": "TEXT NOT NULL DEFAULT '0'",
            "end_date": "TEXT",
            "identity_verified": "INTEGER NOT NULL DEFAULT 0",
            "awaiting_result_at": "TEXT",
            "resolved_at": "TEXT",
            "resolution_checked_at": "TEXT",
            "resolution_status": "TEXT",
            "resolution_source": "TEXT",
            "resolved_by": "TEXT",
            "resolution_json": "TEXT",
            "settled_at": "TEXT",
        }
        for name, declaration in additions.items():
            if name not in columns:
                connection.execute(f"ALTER TABLE paper_orders ADD COLUMN {name} {declaration}")

        # The historical index was bound to lowercase ``open``. Rebuild it
        # after normalizing statuses, preserving the one-position-per-event
        # invariant for all active lifecycle states.
        connection.execute("DROP INDEX IF EXISTS paper_one_open_order_per_event")
        connection.execute("UPDATE paper_orders SET status = 'OPEN' WHERE lower(status) = 'open'")
        connection.execute(
            "UPDATE paper_orders SET status = 'PAPER_SETTLED', "
            "settled_at = COALESCE(settled_at, closed_at) "
            "WHERE lower(status) IN ('closed', 'settled', 'paper_settled')"
        )
        connection.execute(
            "UPDATE paper_orders SET token_id = asset_id WHERE token_id IS NULL OR token_id = ''"
        )
        # Try to recover exact condition/token identity from the immutable
        # market snapshot captured at entry. Never guess a condition id.
        rows = connection.execute(
            "SELECT id, market_id, opened_at FROM paper_orders "
            "WHERE condition_id IS NULL OR identity_verified = 0"
        ).fetchall()
        for row in rows:
            snapshot = connection.execute(
                "SELECT payload_json FROM market_snapshots WHERE market_id = ? "
                "ORDER BY captured_at ASC, id ASC LIMIT 1",
                (row["market_id"],),
            ).fetchone()
            if snapshot is None:
                continue
            try:
                payload = json.loads(snapshot["payload_json"])
            except (TypeError, ValueError):
                continue
            condition_id = payload.get("condition_id")
            token_id = payload.get("token_id") or payload.get("asset_id")
            outcome = str(payload.get("outcome") or "YES").upper()
            end_date = payload.get("end_date")
            if outcome not in {"YES", "NO"}:
                outcome = "YES"
            connection.execute(
                "UPDATE paper_orders SET condition_id = COALESCE(condition_id, ?), "
                "token_id = COALESCE(token_id, ?), outcome = ?, end_date = COALESCE(end_date, ?), "
                "fee_rate = COALESCE(NULLIF(fee_rate, '0'), ?), "
                "fee_exponent = COALESCE(NULLIF(fee_exponent, '0'), ?), "
                "identity_verified = CASE WHEN ? IS NOT NULL AND ? IS NOT NULL "
                "THEN 1 ELSE identity_verified END "
                "WHERE id = ?",
                (
                    condition_id,
                    token_id,
                    outcome,
                    end_date,
                    str(payload.get("fee_rate", "0")),
                    str(payload.get("fee_exponent", "0")),
                    condition_id,
                    token_id,
                    row["id"],
                ),
            )
        connection.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS paper_one_active_order_per_event "
            "ON paper_orders(event_id) WHERE status IN ('OPEN', 'AWAITING_RESULT', 'RESOLVED')"
        )

    def start_runtime_window(
        self,
        *,
        query: str,
        interval_seconds: int,
        paper: bool,
        astra: bool,
    ) -> RuntimeWindow:
        with self.transaction(immediate=True) as connection:
            active = connection.execute(
                "SELECT * FROM runtime_windows WHERE status = 'ACTIVE' ORDER BY id DESC LIMIT 1"
            ).fetchone()
            if active is not None:
                return _runtime_window_from_row(active)
            now = utc_now()
            cursor = connection.execute(
                """
                INSERT INTO runtime_windows(
                    started_at, status, query, interval_seconds, paper, astra
                ) VALUES (?, 'ACTIVE', ?, ?, ?, ?)
                """,
                (now.isoformat(), query, interval_seconds, int(paper), int(astra)),
            )
            row = connection.execute(
                "SELECT * FROM runtime_windows WHERE id = ?", (_lastrowid(cursor),)
            ).fetchone()
            if row is None:
                raise RuntimeError("failed to read newly created runtime window")
            return _runtime_window_from_row(row)

    def get_active_runtime_window(self) -> RuntimeWindow | None:
        with self.connect() as connection:
            row = connection.execute(
                "SELECT * FROM runtime_windows WHERE status = 'ACTIVE' ORDER BY id DESC LIMIT 1"
            ).fetchone()
        return None if row is None else _runtime_window_from_row(row)

    def finish_runtime_window(self, window_id: int) -> None:
        with self.connect() as connection:
            connection.execute(
                "UPDATE runtime_windows SET status = 'COMPLETED', ended_at = ? "
                "WHERE id = ? AND status = 'ACTIVE'",
                (utc_now().isoformat(), window_id),
            )

    def runtime_report_exists(self, window_id: int, kind: str) -> bool:
        with self.connect() as connection:
            row = connection.execute(
                "SELECT 1 FROM runtime_reports WHERE window_id = ? AND kind = ?",
                (window_id, kind),
            ).fetchone()
        return row is not None

    def record_runtime_report(
        self,
        window_id: int,
        *,
        kind: str,
        elapsed_seconds: int,
        payload: dict[str, object],
    ) -> None:
        values = (
            window_id,
            kind,
            utc_now().isoformat(),
            max(0, int(elapsed_seconds)),
            json.dumps(payload, ensure_ascii=False, default=str),
        )
        with self.connect() as connection:
            if kind == "CYCLE":
                connection.execute(
                    """
                    INSERT INTO runtime_reports(
                        window_id, kind, created_at, elapsed_seconds, payload_json
                    ) VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT(window_id, kind) DO UPDATE SET
                        created_at=excluded.created_at,
                        elapsed_seconds=excluded.elapsed_seconds,
                        payload_json=excluded.payload_json
                    """,
                    values,
                )
            else:
                connection.execute(
                    """
                    INSERT OR IGNORE INTO runtime_reports(
                        window_id, kind, created_at, elapsed_seconds, payload_json
                    ) VALUES (?, ?, ?, ?, ?)
                    """,
                    values,
                )

    def runtime_reports(self, window_id: int) -> list[RuntimeReport]:
        with self.connect() as connection:
            rows = connection.execute(
                "SELECT * FROM runtime_reports WHERE window_id = ? ORDER BY id",
                (window_id,),
            ).fetchall()
        return [
            RuntimeReport(
                window_id=int(row["window_id"]),
                kind=row["kind"],
                created_at=datetime.fromisoformat(row["created_at"]),
                elapsed_seconds=int(row["elapsed_seconds"]),
                payload=json.loads(row["payload_json"]),
            )
            for row in rows
        ]

    def runtime_window_summary(self, window_id: int) -> dict[str, object]:
        with self.connect() as connection:
            scan_rows = connection.execute(
                "SELECT id, status, geoblocked, error FROM scan_runs "
                "WHERE window_id = ? ORDER BY id",
                (window_id,),
            ).fetchall()
            decision_rows = connection.execute(
                "SELECT d.action, COUNT(*) AS count FROM decisions d "
                "JOIN scan_runs s ON s.id = d.run_id WHERE s.window_id = ? "
                "GROUP BY d.action",
                (window_id,),
            ).fetchall()
        return {
            "scan_count": len(scan_rows),
            "completed_scans": sum(row["status"] == "completed" for row in scan_rows),
            "failed_scans": sum(row["status"] == "failed" for row in scan_rows),
            "geoblocked_scans": sum(row["geoblocked"] == 1 for row in scan_rows),
            "latest_scan_id": None if not scan_rows else int(scan_rows[-1]["id"]),
            "latest_error": next(
                (str(row["error"]) for row in reversed(scan_rows) if row["error"]), None
            ),
            "decisions_by_action": {str(row["action"]): int(row["count"]) for row in decision_rows},
        }

    def recent_runtime_windows(self, *, limit: int = 8) -> list[dict[str, object]]:
        with self.connect() as connection:
            rows = connection.execute(
                "SELECT * FROM runtime_windows ORDER BY id DESC LIMIT ?", (max(1, limit),)
            ).fetchall()
        result: list[dict[str, object]] = []
        for row in rows:
            window = _runtime_window_from_row(row)
            result.append(
                {
                    **window.model_dump(mode="json"),
                    "summary": self.runtime_window_summary(window.id),
                    "reports": [
                        report.model_dump(mode="json")
                        for report in self.runtime_reports(window.id)
                        if report.kind != "CYCLE"
                    ],
                }
            )
        return result

    def dashboard_payload(
        self, *, forecast_store: ForecastStore | None = None
    ) -> dict[str, object]:
        active = self.get_active_runtime_window()
        portfolio = self.portfolio_summary()
        raw_orders = portfolio.pop("recent_orders", [])
        raw_marks = portfolio.pop("latest_marks", [])
        marks_by_order = {int(mark["order_id"]): mark for mark in raw_marks}
        market_ids = [str(order["market_id"]) for order in raw_orders]
        snapshot_by_market: dict[str, dict[str, object]] = {}
        if market_ids:
            placeholders = ",".join("?" for _ in market_ids)
            with self.connect() as connection:
                snapshot_rows = connection.execute(
                    f"""
                    SELECT market_id, payload_json FROM market_snapshots
                    WHERE market_id IN ({placeholders})
                      AND id IN (
                        SELECT MAX(id) FROM market_snapshots
                        WHERE market_id IN ({placeholders}) GROUP BY market_id
                      )
                    """,
                    (*market_ids, *market_ids),
                ).fetchall()
            for row in snapshot_rows:
                try:
                    value = json.loads(row["payload_json"])
                except (TypeError, ValueError):
                    value = {}
                snapshot_by_market[str(row["market_id"])] = value if isinstance(value, dict) else {}
        positions: list[dict[str, object]] = []
        for order in raw_orders:
            if order.get("status") not in {"OPEN", "AWAITING_RESULT", "RESOLVED"}:
                continue
            mark = marks_by_order.get(int(str(order["id"])), {})
            snapshot_payload = snapshot_by_market.get(str(order["market_id"]), {})
            positions.append(
                {
                    "id": int(order["id"]),
                    "event_id": order["event_id"],
                    "event_title": snapshot_payload.get("event_title"),
                    "market_id": order["market_id"],
                    "market_question": snapshot_payload.get("market_question"),
                    "outcome_label": snapshot_payload.get("outcome_label"),
                    "status": order["status"],
                    "strategy_version": order.get("strategy_version", "v0"),
                    "outcome": order.get("outcome", "YES"),
                    "shares": order["shares"],
                    "entry_price": order["entry_price"],
                    "notional_usd": order["notional_usd"],
                    "fee_usd": order["fee_usd"],
                    "max_loss_usd": order["max_loss_usd"],
                    "opened_at": order["opened_at"],
                    "current_bid_price": mark.get("current_bid_price"),
                    "immediately_sellable_shares": mark.get("immediately_sellable_shares", "0"),
                    "estimated_full_exit_pnl_usd": mark.get("estimated_full_exit_pnl_usd"),
                    "full_exit_value_usd": mark.get("full_exit_value_usd"),
                }
            )
        positions.sort(
            key=lambda item: (str(item.get("opened_at", "")), int(str(item["id"]))),
            reverse=True,
        )
        latest_scan = portfolio.get("last_scan")
        with self.connect() as connection:
            latest_completed_scan_row = connection.execute(
                "SELECT * FROM scan_runs WHERE status != 'running' ORDER BY id DESC LIMIT 1"
            ).fetchone()
            recent_decision_rows = connection.execute(
                "SELECT payload_json FROM decisions ORDER BY id DESC LIMIT 40"
            ).fetchall()
        compact_decisions: list[dict[str, object]] = []
        for row in recent_decision_rows:
            try:
                decision = json.loads(str(row["payload_json"]))
            except (TypeError, ValueError):
                continue
            if not isinstance(decision, dict):
                continue
            compact_decisions.append(
                {
                    "action": decision.get("action"),
                    "event_id": decision.get("event_id"),
                    "market_id": decision.get("market_id"),
                    "probability": decision.get("probability"),
                    "executable_price": decision.get("executable_price"),
                    "probability_edge": decision.get("probability_edge"),
                    "expected_profit_usd": decision.get("expected_profit_usd"),
                    "reason_codes": decision.get("reason_codes", []),
                    "warning_codes": decision.get("warning_codes", []),
                    "strategy_version": decision.get("strategy_version", "v1"),
                    "created_at": decision.get("created_at"),
                }
            )
        forecast_comparison: dict[str, object]
        try:
            if forecast_store is None:
                from polybot.forecast_store import ForecastStore

                forecast_store = ForecastStore(self.path, read_only=True)
            forecast_comparison = forecast_store.dashboard_summary()
        except Exception as error:
            forecast_comparison = {
                "counts": {"model_runs": 0, "predictions": 0, "outcome_versions": 0},
                "events": [],
                "metrics": [],
                "station_metrics": [],
                "outcomes": [],
                "error": str(error),
            }
        try:
            from polybot.forecast_diagnostics_view import build_forecast_diagnostics_view

            forecast_diagnostics = build_forecast_diagnostics_view(self.path)
        except Exception as error:
            forecast_diagnostics = {
                "version": "forecast-diagnostics-v1",
                "summary": {},
                "history_sigma_proxy": {
                    "state": "unavailable",
                    "message": str(error),
                },
                "trades": [],
                "warnings": [str(error)],
            }
        dashboard_reports: list[dict[str, object]] = []
        if active is not None:
            for report in self.runtime_reports(active.id):
                payload = report.model_dump(mode="json")
                if report.kind == "CYCLE" and isinstance(report.payload.get("scan"), dict):
                    scan = cast(dict[str, object], report.payload["scan"])
                    payload["payload"] = {
                        "scan": {
                            key: scan.get(key)
                            for key in (
                                "run_id",
                                "geoblock",
                                "events_scanned",
                                "markets_scanned",
                                "paper_orders_opened",
                                "paper_orders_settled",
                                "weathernext_paper_decisions",
                                "weathernext_paper_orders_opened",
                                "weathernext_paper_orders_settled",
                                "errors",
                                "weather_next_status",
                            )
                        }
                    }
                dashboard_reports.append(payload)
        compact_windows = [
            {key: value for key, value in window.items() if key != "reports"}
            for window in self.recent_runtime_windows(limit=2)
        ]
        try:
            from polybot.weathernext import WeatherNextProvider

            weathernext_statistics = WeatherNextProvider(
                __import__("polybot.config", fromlist=["Settings"]).Settings()
            ).statistics_dashboard_payload()
        except Exception as error:
            # Dashboard remains read-only and local even when optional
            # WeatherNext statistics configuration is absent or malformed.
            weathernext_statistics = {
                "status": {
                    "access_state": "error",
                    "load_state": "error",
                    "enabled": False,
                    "surface": "gcs_statistics",
                    "snapshot_path": None,
                    "message": str(error),
                },
                "snapshot": None,
            }
        try:
            from polybot.config import Settings
            from polybot.operational_report import build_weathernext_full_dashboard_summary

            weathernext_full_refresh = build_weathernext_full_dashboard_summary(
                Settings().weathernext_full_root,
                now=utc_now(),
            )
        except Exception as error:
            # Full-ensemble status is diagnostic-only; a malformed local
            # artifact must not make the read-only dashboard unavailable.
            weathernext_full_refresh = {
                "configured": False,
                "access_state": "error",
                "load_state": "error",
                "manifest_state": "error",
                "approval_state": "error",
                "payload_read": False,
                "read_evidence": False,
                "coverage_state": "error",
                "message": str(error),
            }
        try:
            weathernext_paper = self.weathernext_paper_summary()
        except Exception as error:
            weathernext_paper = {
                "strategy_version": "weathernext-paper-v1",
                "open_orders": 0,
                "open_exposure_usd": Decimal(0),
                "settled_orders": 0,
                "realized_pnl_usd": Decimal(0),
                "orders_by_status": {},
                "decisions_by_action": {},
                "recent_orders": [],
                "recent_decisions": [],
                "error": str(error),
            }
        live_v2 = self.live_v2_dashboard_summary()
        return {
            "generated_at": utc_now().isoformat(),
            "portfolio": portfolio,
            "positions": positions,
            "latest_scan": latest_scan,
            "last_completed_scan": (
                None if latest_completed_scan_row is None else dict(latest_completed_scan_row)
            ),
            "decisions": compact_decisions,
            "active_window": None if active is None else active.model_dump(mode="json"),
            "reports": dashboard_reports,
            "recent_windows": compact_windows,
            "forecast_comparison": forecast_comparison,
            "forecast_diagnostics": forecast_diagnostics,
            "weathernext_statistics": weathernext_statistics,
            "weathernext_full_refresh": weathernext_full_refresh,
            "weathernext_paper": weathernext_paper,
            "live_v2": live_v2,
        }

    def live_v2_dashboard_summary(self) -> dict[str, object]:
        """Return a read-only summary of the isolated live-v2 journal."""

        with self.connect() as connection:
            exists = connection.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='live_v2_attempts'"
            ).fetchone()
            if exists is None:
                return {
                    "configured": False,
                    "state": "NOT_CONFIGURED",
                    "attempts": 0,
                    "states": {},
                    "latest": None,
                    "orders": [],
                    "totals": {},
                }
            state_rows = connection.execute(
                "SELECT state,COUNT(*) AS count FROM live_v2_attempts GROUP BY state"
            ).fetchall()
            latest = connection.execute(
                "SELECT local_day,decision_id,intent_sha256,state,remote_order_id,"
                "created_at_utc,updated_at_utc,submitted_at_utc,closed_at_utc,"
                "realized_pnl_usd FROM live_v2_attempts ORDER BY id DESC LIMIT 1"
            ).fetchone()
            order_rows = connection.execute(
                "SELECT id,decision_id,state,intent_json,remote_order_id,"
                "created_at_utc,submitted_at_utc,closed_at_utc,realized_pnl_usd "
                "FROM live_v2_attempts ORDER BY id DESC LIMIT 25"
            ).fetchall()
            totals_row = connection.execute(
                "SELECT "
                "SUM(CASE WHEN submitted_at_utc IS NOT NULL "
                "THEN 1 ELSE 0 END) AS submitted,"
                "SUM(CASE WHEN state='POSITION_OPEN' "
                "THEN 1 ELSE 0 END) AS open_positions,"
                "SUM(CASE WHEN state='POSITION_OPEN' "
                "THEN CAST(json_extract(intent_json,'$.amount_usd') AS REAL) "
                "ELSE 0 END) AS open_cost,"
                "TOTAL(realized_pnl_usd) AS realized_pnl,"
                "SUM(CASE WHEN realized_pnl_usd IS NOT NULL "
                "AND CAST(realized_pnl_usd AS REAL)>0 THEN 1 ELSE 0 END) AS wins,"
                "SUM(CASE WHEN realized_pnl_usd IS NOT NULL "
                "AND CAST(realized_pnl_usd AS REAL)<0 THEN 1 ELSE 0 END) AS losses "
                "FROM live_v2_attempts"
            ).fetchone()
            runtime_exists = connection.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='live_v2_runtime'"
            ).fetchone()
            runtime = (
                None
                if runtime_exists is None
                else connection.execute(
                    "SELECT state,detail_json,updated_at_utc FROM live_v2_runtime WHERE singleton=1"
                ).fetchone()
            )
        states = {str(row["state"]): int(row["count"]) for row in state_rows}
        orders: list[dict[str, object]] = []
        for row in order_rows:
            try:
                intent = json.loads(row["intent_json"])
            except (TypeError, ValueError):
                intent = {}
            orders.append(
                {
                    "id": int(row["id"]),
                    "decision_id": row["decision_id"],
                    "state": str(row["state"]),
                    "event_id": intent.get("event_id"),
                    "market_id": intent.get("market_id"),
                    "amount_usd": intent.get("amount_usd"),
                    "max_price": intent.get("max_price"),
                    "order_type": intent.get("order_type"),
                    "side": intent.get("side"),
                    "remote_order_id": row["remote_order_id"],
                    "created_at_utc": row["created_at_utc"],
                    "submitted_at_utc": row["submitted_at_utc"],
                    "closed_at_utc": row["closed_at_utc"],
                    "realized_pnl_usd": row["realized_pnl_usd"],
                }
            )
        totals = {
            "submitted": int(totals_row["submitted"] or 0),
            "open_positions": int(totals_row["open_positions"] or 0),
            "open_cost_usd": round(float(totals_row["open_cost"] or 0.0), 4),
            "realized_pnl_usd": round(float(totals_row["realized_pnl"] or 0.0), 4),
            "wins": int(totals_row["wins"] or 0),
            "losses": int(totals_row["losses"] or 0),
        }
        runtime_payload: dict[str, object] | None = None
        activity_rows = connection.execute(
            "SELECT t.id, t.attempt_id, t.from_state, t.to_state, t.created_at_utc, "
            "t.detail_json, a.decision_id FROM live_v2_transitions t "
            "LEFT JOIN live_v2_attempts a ON a.id = t.attempt_id "
            "ORDER BY t.id DESC LIMIT 12"
        ).fetchall()
        if runtime is not None:
            runtime_payload = dict(runtime)
            try:
                runtime_payload["detail"] = json.loads(runtime["detail_json"] or "{}")
            except (TypeError, ValueError):
                runtime_payload["detail"] = {}
        activity = []
        for row in activity_rows:
            try:
                detail = json.loads(row["detail_json"] or "{}")
            except (TypeError, ValueError):
                detail = {}
            activity.append(
                {
                    "id": int(row["id"]),
                    "attempt_id": int(row["attempt_id"]),
                    "decision_id": row["decision_id"],
                    "from_state": row["from_state"],
                    "to_state": row["to_state"],
                    "created_at_utc": row["created_at_utc"],
                    "detail": detail,
                }
            )

        return {
            "configured": True,
            "state": "WAITING_FOR_SIGNAL" if latest is None else str(latest["state"]),
            "attempts": sum(states.values()),
            "states": states,
            "latest": None if latest is None else dict(latest),
            "runtime": runtime_payload,
            "activity": activity,
            "orders": orders,
            "totals": totals,
            "limits": {
                "side": "BUY",
                "order_type": "FOK",
                "max_buy_notional_usd": "1.90",
                "max_all_in_spend_usd": "2.00",
                "max_wallet_balance_usd": "10.00",
                "max_orders_per_day": 12,
                "daily_timezone": "Asia/Almaty",
            },
        }

    def start_scan(self, *, query: str, mode: str, window_id: int | None = None) -> int:
        with self.connect() as connection:
            cursor = connection.execute(
                """
                INSERT INTO scan_runs(started_at, query, mode, status, window_id)
                VALUES (?, ?, ?, 'running', ?)
                """,
                (utc_now().isoformat(), query, mode, window_id),
            )
            return _lastrowid(cursor)

    def recover_stale_scans(self, *, older_than_seconds: int = 900) -> int:
        """Mark abandoned scans after a process/host restart.

        A scan is single-threaded, so an old ``running`` row cannot represent
        work still owned by the current observer. This keeps the dashboard and
        runtime reports honest after a crash or machine reboot.
        """

        cutoff = utc_now().timestamp() - max(0, older_than_seconds)
        with self.connect() as connection:
            rows = connection.execute(
                "SELECT id, started_at FROM scan_runs WHERE status = 'running'"
            ).fetchall()
            stale_ids: list[int] = []
            for row in rows:
                try:
                    started = datetime.fromisoformat(str(row["started_at"])).timestamp()
                except ValueError:
                    started = 0
                if started < cutoff:
                    stale_ids.append(int(row["id"]))
            if stale_ids:
                placeholders = ",".join("?" for _ in stale_ids)
                connection.execute(
                    f"UPDATE scan_runs SET status='failed', completed_at=?, "
                    f"error='stale scan recovered after observer restart' "
                    f"WHERE id IN ({placeholders})",
                    (utc_now().isoformat(), *stale_ids),
                )
        return len(stale_ids)

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

    def start_shadow_research(
        self,
        *,
        parent_scan_run_id: int,
        outcome_requested: int,
        extra_requested: int,
        cohort_version: str = "shadow-extra-v1",
    ) -> int:
        """Open a research-only batch that cannot contain trading records."""

        with self.connect() as connection:
            cursor = connection.execute(
                """
                INSERT INTO shadow_research_runs(
                    parent_scan_run_id, cohort_version, started_at, status,
                    outcome_requested, extra_requested
                ) VALUES (?, ?, ?, 'running', ?, ?)
                """,
                (
                    parent_scan_run_id,
                    cohort_version,
                    utc_now().isoformat(),
                    max(0, outcome_requested),
                    max(0, extra_requested),
                ),
            )
            return _lastrowid(cursor)

    def record_shadow_events(
        self,
        research_run_id: int,
        events: Sequence[EventDefinition],
        *,
        cohort_version: str = "shadow-extra-v1",
    ) -> int:
        """Persist discovery metadata only in the explicit shadow registry."""

        discovered_at = utc_now().isoformat()
        inserted = 0
        with self.connect() as connection:
            for event in events:
                end_dates = sorted(
                    market.end_date.astimezone(UTC)
                    for market in event.markets
                    if market.end_date is not None
                )
                cursor = connection.execute(
                    """
                    INSERT OR IGNORE INTO shadow_event_registry(
                        research_run_id, cohort_version, discovered_at, event_id,
                        event_slug, event_title, observation_date, market_count,
                        accepting_market_count, earliest_end_at, latest_end_at,
                        metadata_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        research_run_id,
                        cohort_version,
                        discovered_at,
                        event.id,
                        event.slug,
                        event.title,
                        None
                        if event.observation_date is None
                        else event.observation_date.isoformat(),
                        len(event.markets),
                        sum(1 for market in event.markets if market.accepting_orders),
                        None if not end_dates else end_dates[0].isoformat(),
                        None if not end_dates else end_dates[-1].isoformat(),
                        json.dumps(
                            {
                                "registry_role": "metadata_only",
                                "decision_eligible": False,
                                "paper_order_eligible": False,
                                "forecast_cohort_registered": False,
                            },
                            ensure_ascii=False,
                            sort_keys=True,
                            separators=(",", ":"),
                        ),
                    ),
                )
                inserted += int(cursor.rowcount > 0)
        return inserted

    def finish_shadow_research(
        self,
        research_run_id: int,
        *,
        status: str,
        outcome_completed: int,
        outcome_pending: int,
        extra_registered: int,
        error: str | None = None,
    ) -> None:
        if status not in {"completed", "partial", "failed", "skipped_busy"}:
            raise ValueError(f"unsupported shadow research status: {status}")
        with self.connect() as connection:
            connection.execute(
                """
                UPDATE shadow_research_runs
                SET completed_at=?, status=?, outcome_completed=?, outcome_pending=?,
                    extra_registered=?, error=?
                WHERE id=?
                """,
                (
                    utc_now().isoformat(),
                    status,
                    max(0, outcome_completed),
                    max(0, outcome_pending),
                    max(0, extra_registered),
                    None if error is None else error[:2000],
                    research_run_id,
                ),
            )

    def recover_stale_shadow_research(self) -> int:
        """Close batches abandoned by an observer process restart."""

        with self.connect() as connection:
            cursor = connection.execute(
                """
                UPDATE shadow_research_runs
                SET completed_at=?, status='failed',
                    error='shadow research abandoned after observer restart'
                WHERE status='running'
                """,
                (utc_now().isoformat(),),
            )
            return max(0, int(cursor.rowcount))

    def record_shadow_research_skip(
        self,
        *,
        parent_scan_run_id: int,
        reason: str,
        cohort_version: str = "shadow-extra-v1",
    ) -> int:
        run_id = self.start_shadow_research(
            parent_scan_run_id=parent_scan_run_id,
            outcome_requested=0,
            extra_requested=0,
            cohort_version=cohort_version,
        )
        self.finish_shadow_research(
            run_id,
            status="skipped_busy",
            outcome_completed=0,
            outcome_pending=0,
            extra_registered=0,
            error=reason,
        )
        return run_id

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

    def reserve_api_budget(
        self,
        *,
        model: str,
        estimate: Decimal,
        budget: Decimal,
        run_id: int | None = None,
        event_id: str | None = None,
        rules_hash: str | None = None,
    ) -> str:
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
                    reservation_id, model, run_id, event_id, rules_hash,
                    status, estimated_cost_usd, created_at
                ) VALUES (?, ?, ?, ?, ?, 'reserved', ?, ?)
                """,
                (
                    reservation_id,
                    model,
                    run_id,
                    event_id,
                    rules_hash,
                    str(estimate),
                    utc_now().isoformat(),
                ),
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

    def latest_weather_forecast(self, event_id: str) -> WeatherForecast | None:
        with self.connect() as connection:
            row = connection.execute(
                "SELECT payload_json FROM weather_snapshots WHERE event_id = ? "
                "ORDER BY id DESC LIMIT 1",
                (event_id,),
            ).fetchone()
        if row is None:
            return None
        return WeatherForecast.model_validate_json(row["payload_json"])

    def record_observation_history(
        self, run_id: int, event_id: str, history: ObservationHistory
    ) -> None:
        with self.transaction(immediate=True) as connection:
            connection.execute(
                """
                INSERT INTO observation_fetches(
                    run_id, event_id, station_id, observation_date, fetched_at, payload_json
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    event_id,
                    history.station_id,
                    history.observation_date.isoformat(),
                    history.fetched_at_utc.isoformat(),
                    history.model_dump_json(),
                ),
            )
            for observation in history.observations:
                existing = connection.execute(
                    """
                    SELECT id FROM station_observation_versions
                    WHERE station_id = ? AND observed_at_utc = ? AND revision_hash = ?
                    """,
                    (
                        observation.station_id,
                        observation.observed_at_utc.isoformat(),
                        observation.revision_hash,
                    ),
                ).fetchone()
                if existing is None:
                    connection.execute(
                        """
                        INSERT INTO station_observation_versions(
                            station_id, observed_at_utc, revision_hash, first_seen_at_utc,
                            last_seen_at_utc, source, temperature_c,
                            displayed_temperature_c, corrected, payload_json
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            observation.station_id,
                            observation.observed_at_utc.isoformat(),
                            observation.revision_hash,
                            observation.first_seen_at_utc.isoformat(),
                            history.fetched_at_utc.isoformat(),
                            observation.source,
                            str(observation.temperature_c),
                            str(observation.displayed_temperature_c),
                            int(observation.corrected),
                            observation.model_dump_json(),
                        ),
                    )
                else:
                    connection.execute(
                        "UPDATE station_observation_versions SET last_seen_at_utc = ? WHERE id = ?",
                        (history.fetched_at_utc.isoformat(), existing["id"]),
                    )

    def latest_observation_history(self, event_id: str) -> ObservationHistory | None:
        with self.connect() as connection:
            row = connection.execute(
                "SELECT payload_json FROM observation_fetches WHERE event_id = ? "
                "ORDER BY id DESC LIMIT 1",
                (event_id,),
            ).fetchone()
        if row is None:
            return None
        return ObservationHistory.model_validate_json(row["payload_json"])

    def record_weathernext_snapshot(self, run_id: int, event_id: str, snapshot: object) -> int:
        payload = snapshot.model_dump_json()  # type: ignore[union-attr]
        with self.connect() as connection:
            cursor = connection.execute(
                """
                INSERT INTO weathernext_snapshots(run_id, event_id, captured_at, payload_json)
                VALUES (?, ?, ?, ?)
                """,
                (run_id, event_id, utc_now().isoformat(), payload),
            )
            return _lastrowid(cursor)

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

    # ------------------------------------------------------------------
    # Isolated WeatherNext paper strategy ledger
    # ------------------------------------------------------------------
    def record_weathernext_paper_decision(
        self,
        run_id: int,
        decision: MarketDecision,
        *,
        snapshot: object | None = None,
        paper_order_id: int | None = None,
    ) -> int:
        """Persist one WeatherNext decision without touching the v1 ledger.

        ``snapshot`` is intentionally accepted as an opaque object so this
        table remains compatible with future full-trajectory snapshot schema
        revisions.  Only provenance scalars are copied into indexed columns;
        the immutable decision payload retains the complete source reference.
        """

        snapshot_init = getattr(snapshot, "init_time_utc", None)
        snapshot_uri = getattr(snapshot, "source_uri", None)
        scenarios = getattr(snapshot, "scenario_max_c", None)
        member_count = None if scenarios is None else len(scenarios)
        with self.connect() as connection:
            cursor = connection.execute(
                """
                INSERT INTO weathernext_paper_decisions(
                    run_id, event_id, market_id, action, strategy_version, created_at,
                    snapshot_init_time_utc, snapshot_source_uri, snapshot_member_count,
                    probability, executable_price, probability_edge, shares, notional_usd,
                    fee_usd, max_loss_usd, expected_profit_usd, reason_codes_json,
                    warning_codes_json, payload_json, paper_order_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    decision.event_id,
                    decision.market_id,
                    decision.action.value,
                    decision.strategy_version,
                    decision.created_at.isoformat(),
                    None if snapshot_init is None else snapshot_init.isoformat(),
                    None if snapshot_uri is None else str(snapshot_uri),
                    member_count,
                    _decimal_text(decision.probability),
                    _decimal_text(decision.executable_price),
                    _decimal_text(decision.probability_edge),
                    _decimal_text(decision.shares),
                    _decimal_text(decision.notional_usd),
                    _decimal_text(decision.fee_usd),
                    _decimal_text(decision.max_loss_usd),
                    _decimal_text(decision.expected_profit_usd),
                    json.dumps(decision.reason_codes, ensure_ascii=False),
                    json.dumps(decision.warning_codes, ensure_ascii=False),
                    decision.model_dump_json(),
                    paper_order_id,
                ),
            )
            return _lastrowid(cursor)

    def open_weathernext_paper_order(
        self,
        decision: MarketDecision,
        *,
        run_id: int,
        idempotency_key: str,
        max_event_risk: Decimal,
        max_total_risk: Decimal,
    ) -> int:
        """Open an isolated WeatherNext paper position.

        Exposure is calculated exclusively from ``weathernext_paper_orders``;
        v1 positions and stop-loss accounting are never consulted here.
        """

        required = {
            "shares": decision.shares,
            "entry_price": decision.executable_price,
            "notional": decision.notional_usd,
            "fee": decision.fee_usd,
            "max_loss": decision.max_loss_usd,
            "condition_id": decision.condition_id,
            "token_id": decision.token_id or decision.asset_id,
        }
        missing = [key for key, value in required.items() if value is None]
        if missing:
            raise ValueError(f"WeatherNext paper order is missing: {', '.join(missing)}")
        assert decision.shares is not None
        assert decision.executable_price is not None
        assert decision.notional_usd is not None
        assert decision.fee_usd is not None
        assert decision.max_loss_usd is not None

        with self.transaction(immediate=True) as connection:
            existing = connection.execute(
                "SELECT id FROM weathernext_paper_orders WHERE idempotency_key = ?",
                (idempotency_key,),
            ).fetchone()
            if existing is not None:
                return int(existing["id"])
            active_rows = connection.execute(
                "SELECT event_id, max_loss_usd FROM weathernext_paper_orders "
                "WHERE status IN ('OPEN', 'AWAITING_RESULT', 'RESOLVED')"
            ).fetchall()
            total_exposure = sum((Decimal(row["max_loss_usd"]) for row in active_rows), Decimal(0))
            event_exposure = sum(
                (
                    Decimal(row["max_loss_usd"])
                    for row in active_rows
                    if row["event_id"] == decision.event_id
                ),
                Decimal(0),
            )
            if event_exposure + decision.max_loss_usd > max_event_risk:
                raise PaperRiskRejectedError(
                    f"WeatherNext event exposure ${event_exposure} + "
                    f"${decision.max_loss_usd} exceeds ${max_event_risk}"
                )
            if total_exposure + decision.max_loss_usd > max_total_risk:
                raise PaperRiskRejectedError(
                    f"WeatherNext total exposure ${total_exposure} + "
                    f"${decision.max_loss_usd} exceeds ${max_total_risk}"
                )
            try:
                cursor = connection.execute(
                    """
                    INSERT INTO weathernext_paper_orders(
                        idempotency_key, run_id, event_id, market_id, asset_id, status,
                        strategy_version, execution_model, condition_id, token_id, outcome,
                        shares, entry_price, notional_usd, fee_usd, execution_buffer_usd,
                        max_loss_usd, expected_profit_usd, fee_rate, fee_exponent, end_date,
                        identity_verified, opened_at
                    ) VALUES (
                        ?, ?, ?, ?, ?, 'OPEN', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                    )
                    """,
                    (
                        idempotency_key,
                        run_id,
                        decision.event_id,
                        decision.market_id,
                        decision.asset_id,
                        decision.strategy_version,
                        decision.execution_model,
                        decision.condition_id,
                        decision.token_id or decision.asset_id,
                        decision.outcome.value,
                        str(decision.shares),
                        str(decision.executable_price),
                        str(decision.notional_usd),
                        str(decision.fee_usd),
                        str(decision.execution_buffer_usd),
                        str(decision.max_loss_usd),
                        _decimal_text(decision.expected_profit_usd),
                        str(decision.fee_rate),
                        str(decision.fee_exponent),
                        None if decision.end_date is None else decision.end_date.isoformat(),
                        int(
                            decision.condition_id is not None
                            and bool(decision.token_id or decision.asset_id)
                        ),
                        decision.created_at.isoformat(),
                    ),
                )
            except sqlite3.IntegrityError as error:
                raise PaperRiskRejectedError(
                    "a WeatherNext paper order already exists for this event"
                ) from error
            return _lastrowid(cursor)

    def active_weathernext_paper_orders(self) -> list[WeatherNextPaperOrderTarget]:
        with self.connect() as connection:
            rows = connection.execute(
                "SELECT * FROM weathernext_paper_orders "
                "WHERE status IN ('OPEN', 'AWAITING_RESULT', 'RESOLVED') ORDER BY id"
            ).fetchall()
        return [_weathernext_order_target_from_row(row) for row in rows]

    def active_weathernext_paper_event_ids(self) -> set[str]:
        with self.connect() as connection:
            rows = connection.execute(
                "SELECT DISTINCT event_id FROM weathernext_paper_orders "
                "WHERE status IN ('OPEN', 'AWAITING_RESULT', 'RESOLVED')"
            ).fetchall()
        return {str(row["event_id"]) for row in rows}

    def record_weathernext_paper_resolution_check(
        self, order_id: int, check: ResolutionCheck
    ) -> None:
        with self.transaction(immediate=True) as connection:
            row = _verified_weathernext_order_row(connection, order_id, check)
            connection.execute(
                "INSERT INTO weathernext_paper_resolution_checks("
                "paper_order_id, checked_at, confirmed, won, payload_json) "
                "VALUES (?, ?, ?, ?, ?)",
                (
                    order_id,
                    check.checked_at.isoformat(),
                    int(check.confirmed),
                    None if check.won is None else int(check.won),
                    check.model_dump_json(),
                ),
            )
            connection.execute(
                "UPDATE weathernext_paper_orders SET resolution_json = ? WHERE id = ?",
                (check.model_dump_json(), row["id"]),
            )

    def resolve_weathernext_paper_order(self, order_id: int, check: ResolutionCheck) -> bool:
        if not check.confirmed or check.won is None:
            raise ValueError("WeatherNext paper order cannot resolve without confirmed result")
        with self.transaction(immediate=True) as connection:
            row = _verified_weathernext_order_row(connection, order_id, check)
            if row["status"] == "RESOLVED":
                return False
            if row["status"] not in {"OPEN", "AWAITING_RESULT"}:
                raise ValueError(f"WeatherNext paper order {order_id} cannot resolve")
            connection.execute(
                "UPDATE weathernext_paper_orders SET status='RESOLVED', won=?, "
                "resolution_json=? WHERE id=?",
                (int(check.won), check.model_dump_json(), order_id),
            )
        return True

    def settle_resolved_weathernext_paper_order(self, order_id: int) -> Decimal:
        with self.transaction(immediate=True) as connection:
            row = connection.execute(
                "SELECT * FROM weathernext_paper_orders WHERE id = ? AND status = 'RESOLVED'",
                (order_id,),
            ).fetchone()
            if row is None:
                raise ValueError(f"No RESOLVED WeatherNext paper order {order_id}")
            if row["won"] is None or row["resolution_json"] is None:
                raise ValueError(f"WeatherNext paper order {order_id} lacks resolution evidence")
            payout = Decimal(row["shares"]) if bool(row["won"]) else Decimal(0)
            costs = sum(
                (
                    Decimal(row["notional_usd"]),
                    Decimal(row["fee_usd"]),
                    Decimal(row["execution_buffer_usd"]),
                ),
                Decimal(0),
            )
            pnl = payout - costs
            now = utc_now().isoformat()
            connection.execute(
                "UPDATE weathernext_paper_orders SET status='PAPER_SETTLED', "
                "settled_at=?, closed_at=?, realized_pnl_usd=? WHERE id=?",
                (now, now, str(pnl), order_id),
            )
        return pnl

    def mark_awaiting_weathernext_paper_result(self, order_id: int, check: ResolutionCheck) -> bool:
        if check.confirmed:
            raise ValueError("confirmed results must resolve before awaiting")
        ended = (
            check.closed
            or not check.accepting_orders
            or (check.end_date is not None and check.end_date <= check.checked_at)
        )
        if not ended:
            return False
        with self.transaction(immediate=True) as connection:
            row = _verified_weathernext_order_row(connection, order_id, check)
            if row["status"] != "OPEN":
                return False
            connection.execute(
                "UPDATE weathernext_paper_orders SET status='AWAITING_RESULT' WHERE id=?",
                (order_id,),
            )
        return True

    def record_weathernext_paper_mark(
        self, order: WeatherNextPaperOrderTarget, snapshot: MarketSnapshot
    ) -> PaperMark:
        token_id = snapshot.token_id or snapshot.asset_id
        if order.condition_id is None or not order.identity_verified:
            raise ValueError(f"WeatherNext paper order {order.id} has unverified identity")
        if (
            snapshot.market_id != order.market_id
            or snapshot.condition_id != order.condition_id
            or token_id != order.token_id
            or snapshot.outcome != order.outcome
        ):
            raise ValueError(f"WeatherNext paper mark identity mismatch for order {order.id}")
        from polybot.fees import plan_sell_fill

        plan = plan_sell_fill(
            bids=snapshot.bids,
            shares=order.shares,
            fee_rate=order.fee_rate,
            fee_exponent=order.fee_exponent,
        )
        best = max(snapshot.bids, key=lambda level: level.price) if snapshot.bids else None
        mark = PaperMark(
            order_id=order.id,
            market_id=order.market_id,
            condition_id=order.condition_id,
            token_id=order.token_id,
            outcome=order.outcome,
            captured_at=utc_now(),
            book_timestamp=snapshot.book_timestamp,
            book_hash=snapshot.book_hash,
            requested_shares=order.shares,
            current_bid_price=None if best is None else best.price,
            current_bid_size=Decimal(0) if best is None else best.size,
            immediately_sellable_shares=plan.filled_shares,
            current_bid_mark_usd=None if best is None else order.shares * best.price,
            full_exit_value_usd=(
                plan.total_notional - plan.total_fee if plan.fully_fillable else None
            ),
            full_exit_fee_usd=plan.total_fee if plan.fully_fillable else None,
            estimated_full_exit_pnl_usd=(
                plan.total_notional - plan.total_fee - order.entry_cost_usd
                if plan.fully_fillable
                else None
            ),
        )
        with self.connect() as connection:
            connection.execute(
                "INSERT INTO weathernext_paper_marks(paper_order_id, captured_at, payload_json) "
                "VALUES (?, ?, ?)",
                (order.id, mark.captured_at.isoformat(), mark.model_dump_json()),
            )
        return mark

    def weathernext_paper_summary(self) -> dict[str, Any]:
        """Return isolated WN paper P&L/decision telemetry for the dashboard."""

        with self.connect() as connection:
            open_rows = connection.execute(
                "SELECT * FROM weathernext_paper_orders "
                "WHERE status IN ('OPEN', 'AWAITING_RESULT', 'RESOLVED') ORDER BY id DESC"
            ).fetchall()
            settled_rows = connection.execute(
                "SELECT realized_pnl_usd FROM weathernext_paper_orders "
                "WHERE status = 'PAPER_SETTLED'"
            ).fetchall()
            status_rows = connection.execute(
                "SELECT status, COUNT(*) AS count FROM weathernext_paper_orders GROUP BY status"
            ).fetchall()
            decision_rows = connection.execute(
                "SELECT action, COUNT(*) AS count FROM weathernext_paper_decisions GROUP BY action"
            ).fetchall()
            recent_decisions = connection.execute(
                "SELECT id, run_id, event_id, market_id, action, strategy_version, created_at, "
                "snapshot_member_count, probability, executable_price, expected_profit_usd, "
                "reason_codes_json, warning_codes_json, paper_order_id "
                "FROM weathernext_paper_decisions ORDER BY id DESC LIMIT 40"
            ).fetchall()
            latest_marks = connection.execute(
                "SELECT m.paper_order_id, m.payload_json FROM weathernext_paper_marks m "
                "JOIN (SELECT paper_order_id, MAX(id) AS id FROM weathernext_paper_marks "
                "GROUP BY paper_order_id) latest ON latest.id = m.id"
            ).fetchall()
        realized = sum(
            (Decimal(row["realized_pnl_usd"]) for row in settled_rows if row["realized_pnl_usd"]),
            Decimal(0),
        )
        open_exposure = sum((Decimal(row["max_loss_usd"]) for row in open_rows), Decimal(0))
        marks = {
            int(row["paper_order_id"]): json.loads(row["payload_json"]) for row in latest_marks
        }
        orders = []
        for row in open_rows:
            order = dict(row)
            order["mark"] = marks.get(int(row["id"]))
            orders.append(order)
        decisions: list[dict[str, object]] = []
        for row in recent_decisions:
            item = dict(row)
            item["reason_codes"] = json.loads(item.pop("reason_codes_json") or "[]")
            item["warning_codes"] = json.loads(item.pop("warning_codes_json") or "[]")
            decisions.append(item)
        return {
            "strategy_version": "weathernext-paper-v1",
            "open_orders": len(open_rows),
            "open_exposure_usd": open_exposure,
            "settled_orders": len(settled_rows),
            "realized_pnl_usd": realized,
            "orders_by_status": {str(row["status"]): int(row["count"]) for row in status_rows},
            "decisions_by_action": {str(row["action"]): int(row["count"]) for row in decision_rows},
            "recent_orders": orders,
            "recent_decisions": decisions,
        }

    def weathernext_paper_realized_pnl_for_day(self, day: date) -> Decimal:
        with self.connect() as connection:
            rows = connection.execute(
                "SELECT realized_pnl_usd FROM weathernext_paper_orders "
                "WHERE status='PAPER_SETTLED' AND substr(settled_at, 1, 10)=?",
                (day.isoformat(),),
            ).fetchall()
        return sum(
            (Decimal(row["realized_pnl_usd"]) for row in rows if row["realized_pnl_usd"]),
            Decimal(0),
        )

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
            "condition_id": decision.condition_id,
            "token_id": decision.token_id or decision.asset_id,
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

            active_rows = connection.execute(
                "SELECT event_id, max_loss_usd FROM paper_orders "
                "WHERE status IN ('OPEN', 'AWAITING_RESULT', 'RESOLVED')"
            ).fetchall()
            total_exposure = sum((Decimal(row["max_loss_usd"]) for row in active_rows), Decimal(0))
            event_exposure = sum(
                (
                    Decimal(row["max_loss_usd"])
                    for row in active_rows
                    if row["event_id"] == decision.event_id
                ),
                Decimal(0),
            )
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
                        idempotency_key, event_id, market_id, asset_id, condition_id,
                        token_id, outcome, status, strategy_version, execution_model, shares,
                        entry_price, notional_usd, fee_usd, api_cost_usd,
                        execution_buffer_usd, max_loss_usd, expected_profit_usd,
                        fee_rate, fee_exponent, end_date, identity_verified, opened_at
                    ) VALUES (
                        ?, ?, ?, ?, ?, ?, ?, 'OPEN', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?
                    )
                    """,
                    (
                        idempotency_key,
                        decision.event_id,
                        decision.market_id,
                        decision.asset_id,
                        decision.condition_id,
                        decision.token_id or decision.asset_id,
                        decision.outcome.value,
                        decision.strategy_version,
                        decision.execution_model,
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
                        str(decision.fee_rate),
                        str(decision.fee_exponent),
                        None if decision.end_date is None else decision.end_date.isoformat(),
                        decision.created_at.isoformat(),
                    ),
                )
            except sqlite3.IntegrityError as error:
                raise PaperRiskRejectedError(
                    "an open paper order already exists for this event"
                ) from error
            return _lastrowid(cursor)

    def active_paper_orders(self) -> list[PaperOrderTarget]:
        with self.connect() as connection:
            rows = connection.execute(
                "SELECT * FROM paper_orders "
                "WHERE status IN ('OPEN', 'AWAITING_RESULT', 'RESOLVED') ORDER BY id"
            ).fetchall()
        return [
            PaperOrderTarget(
                id=int(row["id"]),
                event_id=str(row["event_id"]),
                market_id=str(row["market_id"]),
                condition_id=row["condition_id"],
                token_id=str(row["token_id"] or row["asset_id"]),
                outcome=OutcomeSide(str(row["outcome"] or "YES").upper()),
                status=PaperOrderStatus(str(row["status"]).upper()),
                strategy_version=str(row["strategy_version"] or "v0"),
                execution_model=str(row["execution_model"] or "LEGACY_CROSSING_BOOK_SHARES"),
                shares=Decimal(row["shares"]),
                entry_cost_usd=sum(
                    (
                        Decimal(row[name])
                        for name in (
                            "notional_usd",
                            "fee_usd",
                            "api_cost_usd",
                        )
                    ),
                    Decimal(0),
                ),
                fee_rate=Decimal(row["fee_rate"] or "0"),
                fee_exponent=Decimal(row["fee_exponent"] or "0"),
                end_date=(
                    None if row["end_date"] is None else datetime.fromisoformat(row["end_date"])
                ),
                identity_verified=bool(row["identity_verified"]),
                resolution_checked_at=(
                    None
                    if row["resolution_checked_at"] is None
                    else datetime.fromisoformat(str(row["resolution_checked_at"]))
                ),
            )
            for row in rows
        ]

    def active_paper_event_ids(self) -> set[str]:
        """Return events that cannot accept another paper position yet."""

        with self.connect() as connection:
            rows = connection.execute(
                "SELECT DISTINCT event_id FROM paper_orders "
                "WHERE status IN ('OPEN', 'AWAITING_RESULT', 'RESOLVED')"
            ).fetchall()
        return {str(row["event_id"]) for row in rows}

    def open_paper_event_ids(self) -> set[str]:
        """Return only positions that still need the full forecast monitor lane.

        ``AWAITING_RESULT`` rows remain active exposure and continue to block a
        duplicate entry, but their lightweight resolution check is enough; a
        complete weather/rules/forecast rescan cannot change their outcome.
        """

        with self.connect() as connection:
            rows = connection.execute(
                "SELECT DISTINCT event_id FROM paper_orders WHERE status = 'OPEN'"
            ).fetchall()
        return {str(row["event_id"]) for row in rows}

    def record_resolution_check(self, order_id: int, check: ResolutionCheck) -> None:
        with self.transaction(immediate=True) as connection:
            row = self._verified_order_row(connection, order_id, check)
            connection.execute(
                "INSERT INTO paper_resolution_checks("
                "paper_order_id, checked_at, confirmed, won, payload_json) "
                "VALUES (?, ?, ?, ?, ?)",
                (
                    order_id,
                    check.checked_at.isoformat(),
                    int(check.confirmed),
                    None if check.won is None else int(check.won),
                    check.model_dump_json(),
                ),
            )
            connection.execute(
                "UPDATE paper_orders SET resolution_checked_at = ?, resolution_status = ?, "
                "resolution_source = ?, resolved_by = ?, resolution_json = ? WHERE id = ?",
                (
                    check.checked_at.isoformat(),
                    check.resolution_status,
                    check.resolution_source,
                    check.resolved_by,
                    check.model_dump_json(),
                    row["id"],
                ),
            )

    def mark_awaiting_result(self, order_id: int, check: ResolutionCheck) -> bool:
        if check.confirmed:
            raise ValueError("confirmed results must transition through RESOLVED")
        ended = (
            check.closed
            or not check.accepting_orders
            or (check.end_date is not None and check.end_date <= check.checked_at)
        )
        if not ended:
            return False
        with self.transaction(immediate=True) as connection:
            row = self._verified_order_row(connection, order_id, check)
            if row["status"] != PaperOrderStatus.OPEN.value:
                return False
            self._transition(
                connection,
                order_id=order_id,
                from_status=PaperOrderStatus.OPEN,
                to_status=PaperOrderStatus.AWAITING_RESULT,
                reason="market ended or stopped accepting orders; official result not confirmed",
            )
            connection.execute(
                "UPDATE paper_orders SET awaiting_result_at = ? WHERE id = ?",
                (check.checked_at.isoformat(), order_id),
            )
        return True

    def resolve_paper_order(self, order_id: int, check: ResolutionCheck) -> bool:
        if not check.confirmed or check.won is None:
            raise ValueError("paper order cannot resolve without a confirmed binary result")
        with self.transaction(immediate=True) as connection:
            row = self._verified_order_row(connection, order_id, check)
            current = PaperOrderStatus(row["status"])
            if current == PaperOrderStatus.RESOLVED:
                return False
            if current not in {PaperOrderStatus.OPEN, PaperOrderStatus.AWAITING_RESULT}:
                raise ValueError(f"paper order {order_id} cannot resolve from {current}")
            self._transition(
                connection,
                order_id=order_id,
                from_status=current,
                to_status=PaperOrderStatus.RESOLVED,
                reason=f"confirmed {check.outcome} result for condition {check.condition_id}",
            )
            connection.execute(
                "UPDATE paper_orders SET resolved_at = ?, won = ?, resolution_checked_at = ?, "
                "resolution_status = ?, resolution_source = ?, resolved_by = ?, "
                "resolution_json = ? "
                "WHERE id = ?",
                (
                    check.checked_at.isoformat(),
                    int(check.won),
                    check.checked_at.isoformat(),
                    check.resolution_status,
                    check.resolution_source,
                    check.resolved_by,
                    check.model_dump_json(),
                    order_id,
                ),
            )
        return True

    def settle_resolved_paper_order(self, order_id: int) -> Decimal:
        with self.transaction(immediate=True) as connection:
            row = connection.execute(
                "SELECT * FROM paper_orders WHERE id = ? AND status = 'RESOLVED'",
                (order_id,),
            ).fetchone()
            if row is None:
                raise ValueError(f"No RESOLVED paper order {order_id}")
            if row["won"] is None or row["resolution_json"] is None:
                raise ValueError(f"Paper order {order_id} lacks confirmed resolution evidence")
            payout = Decimal(row["shares"]) if bool(row["won"]) else Decimal(0)
            costs = sum(
                (
                    Decimal(row[name])
                    for name in (
                        "notional_usd",
                        "fee_usd",
                        "api_cost_usd",
                    )
                ),
                Decimal(0),
            )
            pnl = payout - costs
            now = utc_now()
            self._transition(
                connection,
                order_id=order_id,
                from_status=PaperOrderStatus.RESOLVED,
                to_status=PaperOrderStatus.PAPER_SETTLED,
                reason="paper payout booked from confirmed resolution",
            )
            connection.execute(
                """
                UPDATE paper_orders
                SET settled_at = ?, closed_at = ?, realized_pnl_usd = ?
                WHERE id = ?
                """,
                (now.isoformat(), now.isoformat(), str(pnl), row["id"]),
            )
        return pnl

    def record_paper_mark(self, order: PaperOrderTarget, snapshot: MarketSnapshot) -> PaperMark:
        token_id = snapshot.token_id or snapshot.asset_id
        if order.condition_id is None or not order.identity_verified:
            raise ValueError(f"paper order {order.id} has unverified identity")
        if (
            snapshot.market_id != order.market_id
            or snapshot.condition_id != order.condition_id
            or token_id != order.token_id
            or snapshot.outcome != order.outcome
        ):
            raise ValueError(f"paper mark identity mismatch for order {order.id}")

        from polybot.fees import plan_sell_fill

        plan = plan_sell_fill(
            bids=snapshot.bids,
            shares=order.shares,
            fee_rate=order.fee_rate,
            fee_exponent=order.fee_exponent,
        )
        best = max(snapshot.bids, key=lambda level: level.price) if snapshot.bids else None
        mark = PaperMark(
            order_id=order.id,
            market_id=order.market_id,
            condition_id=order.condition_id,
            token_id=order.token_id,
            outcome=order.outcome,
            captured_at=utc_now(),
            book_timestamp=snapshot.book_timestamp,
            book_hash=snapshot.book_hash,
            requested_shares=order.shares,
            current_bid_price=None if best is None else best.price,
            current_bid_size=Decimal(0) if best is None else best.size,
            immediately_sellable_shares=plan.filled_shares,
            current_bid_mark_usd=(None if best is None else order.shares * best.price),
            full_exit_value_usd=(
                plan.total_notional - plan.total_fee if plan.fully_fillable else None
            ),
            full_exit_fee_usd=plan.total_fee if plan.fully_fillable else None,
            estimated_full_exit_pnl_usd=(
                plan.total_notional - plan.total_fee - order.entry_cost_usd
                if plan.fully_fillable
                else None
            ),
        )
        with self.connect() as connection:
            connection.execute(
                "INSERT INTO paper_marks(paper_order_id, captured_at, payload_json) "
                "VALUES (?, ?, ?)",
                (order.id, mark.captured_at.isoformat(), mark.model_dump_json()),
            )
        return mark

    def settle_paper_order(self, market_id: str, *, won: bool) -> Decimal:
        """Manually resolve and settle a paper order with explicit operator evidence."""

        orders = [order for order in self.active_paper_orders() if order.market_id == market_id]
        if not orders:
            raise ValueError(f"No active paper order for market {market_id}")
        order = orders[-1]
        if order.condition_id is None or not order.identity_verified:
            raise ValueError(f"Paper order {order.id} has unverified condition/token identity")
        check = ResolutionCheck(
            market_id=order.market_id,
            condition_id=order.condition_id,
            token_id=order.token_id,
            outcome=order.outcome,
            checked_at=utc_now(),
            accepting_orders=False,
            closed=True,
            end_date=order.end_date,
            resolution_status="manual",
            resolution_source="manual CLI assertion",
            resolved_by="operator",
            confirmed=True,
            won=won,
            yes_price=Decimal(1) if won == (order.outcome == OutcomeSide.YES) else Decimal(0),
            no_price=Decimal(0) if won == (order.outcome == OutcomeSide.YES) else Decimal(1),
        )
        self.record_resolution_check(order.id, check)
        self.resolve_paper_order(order.id, check)
        return self.settle_resolved_paper_order(order.id)

    def open_paper_market_ids(self) -> list[str]:
        return [order.market_id for order in self.active_paper_orders()]

    def portfolio_summary(self) -> dict[str, Any]:
        with self.connect() as connection:
            open_rows = connection.execute(
                "SELECT max_loss_usd FROM paper_orders "
                "WHERE status IN ('OPEN', 'AWAITING_RESULT', 'RESOLVED')"
            ).fetchall()
            closed_rows = connection.execute(
                "SELECT realized_pnl_usd, api_cost_usd "
                "FROM paper_orders WHERE status = 'PAPER_SETTLED'"
            ).fetchall()
            last_scan = connection.execute(
                "SELECT * FROM scan_runs ORDER BY id DESC LIMIT 1"
            ).fetchone()
            recent_orders = connection.execute(
                "SELECT * FROM paper_orders ORDER BY id DESC LIMIT 20"
            ).fetchall()
            status_rows = connection.execute(
                "SELECT status, COUNT(*) AS count FROM paper_orders GROUP BY status"
            ).fetchall()
            latest_marks = connection.execute(
                "SELECT p.paper_order_id, p.payload_json FROM paper_marks p "
                "JOIN (SELECT paper_order_id, MAX(id) AS id FROM paper_marks "
                "GROUP BY paper_order_id) latest "
                "ON latest.id = p.id ORDER BY p.paper_order_id"
            ).fetchall()
        settled, reserved = self.api_spend()
        open_exposure = sum((Decimal(row["max_loss_usd"]) for row in open_rows), Decimal(0))
        realized_pnl = sum(
            (
                Decimal(row["realized_pnl_usd"])
                for row in closed_rows
                if row["realized_pnl_usd"] is not None
            ),
            Decimal(0),
        )
        allocated_settled_api = sum(
            (
                Decimal(row["api_cost_usd"] or "0")
                for row in closed_rows
                if row["api_cost_usd"] is not None
            ),
            Decimal(0),
        )
        gross_trade_pnl = realized_pnl + allocated_settled_api
        return {
            "open_orders": len(open_rows),
            "open_exposure_usd": open_exposure,
            "closed_orders": len(closed_rows),
            "realized_pnl_usd": realized_pnl,
            "gross_trade_pnl_usd": gross_trade_pnl,
            "allocated_settled_order_api_cost_usd": allocated_settled_api,
            "api_spend_usd": settled,
            "api_reserved_usd": reserved,
            # Order-level realized P&L already includes each settled order's
            # allocated API cost. Add that allocation back before subtracting
            # the global API ledger, otherwise those costs are charged twice.
            "net_project_pnl_after_api_usd": gross_trade_pnl - settled,
            "last_scan": None if last_scan is None else dict(last_scan),
            "recent_orders": [dict(row) for row in recent_orders],
            "orders_by_status": {str(row["status"]): int(row["count"]) for row in status_rows},
            "latest_marks": [json.loads(row["payload_json"]) for row in latest_marks],
        }

    def realized_pnl_for_day(self, day: date) -> Decimal:
        prefix = day.isoformat()
        with self.connect() as connection:
            rows = connection.execute(
                "SELECT realized_pnl_usd FROM paper_orders "
                "WHERE status = 'PAPER_SETTLED' AND substr(settled_at, 1, 10) = ?",
                (prefix,),
            ).fetchall()
        return sum(
            (
                Decimal(row["realized_pnl_usd"])
                for row in rows
                if row["realized_pnl_usd"] is not None
            ),
            Decimal(0),
        )

    @staticmethod
    def _verified_order_row(
        connection: sqlite3.Connection, order_id: int, check: ResolutionCheck
    ) -> sqlite3.Row:
        row = connection.execute("SELECT * FROM paper_orders WHERE id = ?", (order_id,)).fetchone()
        if row is None:
            raise ValueError(f"Unknown paper order {order_id}")
        expected = (
            str(row["market_id"]),
            row["condition_id"],
            str(row["token_id"] or row["asset_id"]),
            str(row["outcome"] or "YES").upper(),
        )
        actual = (
            check.market_id,
            check.condition_id,
            check.token_id,
            check.outcome.value,
        )
        if not bool(row["identity_verified"]) or expected != actual:
            raise ValueError(
                f"paper order {order_id} condition/token identity mismatch: "
                f"stored={expected}, checked={actual}"
            )
        return row

    @staticmethod
    def _transition(
        connection: sqlite3.Connection,
        *,
        order_id: int,
        from_status: PaperOrderStatus,
        to_status: PaperOrderStatus,
        reason: str,
    ) -> None:
        cursor = connection.execute(
            "UPDATE paper_orders SET status = ? WHERE id = ? AND status = ?",
            (to_status.value, order_id, from_status.value),
        )
        if cursor.rowcount != 1:
            raise ValueError(
                f"paper order {order_id} did not transition from {from_status} to {to_status}"
            )
        connection.execute(
            "INSERT INTO paper_order_transitions("
            "paper_order_id, from_status, to_status, reason, created_at) "
            "VALUES (?, ?, ?, ?, ?)",
            (order_id, from_status.value, to_status.value, reason, utc_now().isoformat()),
        )


def _lastrowid(cursor: sqlite3.Cursor) -> int:
    if cursor.lastrowid is None:
        raise RuntimeError("SQLite did not return a row id")
    return int(cursor.lastrowid)


def _decimal_text(value: Decimal | None) -> str | None:
    return None if value is None else str(value)


def _weathernext_order_target_from_row(row: sqlite3.Row) -> WeatherNextPaperOrderTarget:
    return WeatherNextPaperOrderTarget(
        id=int(row["id"]),
        event_id=str(row["event_id"]),
        market_id=str(row["market_id"]),
        condition_id=row["condition_id"],
        token_id=str(row["token_id"] or row["asset_id"]),
        outcome=OutcomeSide(str(row["outcome"] or "YES").upper()),
        status=PaperOrderStatus(str(row["status"]).upper()),
        strategy_version=str(row["strategy_version"] or "weathernext-paper-v1"),
        execution_model=str(row["execution_model"] or "WEATHERNEXT_CROSSING_LIMIT_SHARES"),
        shares=Decimal(row["shares"]),
        entry_cost_usd=sum(
            (
                Decimal(row["notional_usd"]),
                Decimal(row["fee_usd"]),
                Decimal(row["execution_buffer_usd"] or "0"),
            ),
            Decimal(0),
        ),
        fee_rate=Decimal(row["fee_rate"] or "0"),
        fee_exponent=Decimal(row["fee_exponent"] or "0"),
        end_date=None if row["end_date"] is None else datetime.fromisoformat(row["end_date"]),
        identity_verified=bool(row["identity_verified"]),
    )


def _verified_weathernext_order_row(
    connection: sqlite3.Connection, order_id: int, check: ResolutionCheck
) -> sqlite3.Row:
    row = connection.execute(
        "SELECT * FROM weathernext_paper_orders WHERE id = ?", (order_id,)
    ).fetchone()
    if row is None:
        raise ValueError(f"Unknown WeatherNext paper order {order_id}")
    expected = (
        str(row["market_id"]),
        row["condition_id"],
        str(row["token_id"] or row["asset_id"]),
        str(row["outcome"] or "YES").upper(),
    )
    actual = (check.market_id, check.condition_id, check.token_id, check.outcome.value)
    if not bool(row["identity_verified"]) or expected != actual:
        raise ValueError(
            f"WeatherNext paper order {order_id} condition/token identity mismatch: "
            f"stored={expected}, checked={actual}"
        )
    return row


def _runtime_window_from_row(row: sqlite3.Row) -> RuntimeWindow:
    return RuntimeWindow(
        id=int(row["id"]),
        started_at=datetime.fromisoformat(row["started_at"]),
        ended_at=None if row["ended_at"] is None else datetime.fromisoformat(row["ended_at"]),
        status=cast(Literal["ACTIVE", "COMPLETED"], str(row["status"])),
        query=str(row["query"]),
        interval_seconds=int(row["interval_seconds"]),
        paper=bool(row["paper"]),
        astra=bool(row["astra"]),
    )
