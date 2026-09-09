"""Read-only reconstruction of settled paper trades and their forecasts.

The live scanner deliberately keeps execution and forecast persistence in
separate tables.  That is useful for the runtime, but it makes a post-hoc
question such as *"did the forecast miss, or did the trade select the wrong
bracket?"* surprisingly easy to answer incorrectly.  This module provides a
small, dependency-free diagnostic reader which joins the immutable rows that
were already written at entry and at resolution.

Important properties of the reader:

* it opens SQLite with ``mode=ro`` and enables ``query_only``;
* it never calls :class:`polybot.storage.Storage` or
  :class:`polybot.forecast_store.ForecastStore` (both can create/migrate
  tables);
* it never recalculates or rewrites historical decisions;
* a forecast is eligible for an entry only when it was issued no later than
  the entry timestamp, so later polling snapshots cannot leak into the
  reconstruction;
* missing data is represented explicitly rather than inferred as zero.

The public entry point is :func:`reconstruct_settled_trades`.  The
``ForecastDiagnostics`` class is provided for callers that want to reuse a
database path and request multiple reports.
"""

from __future__ import annotations

import json
import sqlite3
from collections import Counter
from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import asdict, dataclass, fields, is_dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any
from urllib.parse import quote

DIAGNOSTICS_SCHEMA_VERSION = 1

# These are intentionally duplicated as strings instead of importing
# ``forecast_models``.  Importing this module must stay side-effect free for a
# read-only report against an old database that predates forecast tables.
OPEN_METEO_ALGORITHM_VERSION = "open-meteo-truncated-normal-v1"
FORECAST_V2_ALGORITHM_VERSION = "forecast-engine-v2-station-intraday@1"
ECMWF_RAW_ALGORITHM_VERSION = "ecmwf-ifs025-raw-ensemble-v1"

_SETTLED_STATUSES = frozenset({"PAPER_SETTLED", "SETTLED", "CLOSED"})
_KNOWN_ALGORITHMS = frozenset(
    {
        OPEN_METEO_ALGORITHM_VERSION,
        FORECAST_V2_ALGORITHM_VERSION,
        ECMWF_RAW_ALGORITHM_VERSION,
    }
)
_SENSITIVE_METADATA_PARTS = (
    "api_key",
    "apikey",
    "access_token",
    "authorization",
    "client_secret",
    "private_key",
    "password",
    "secret",
)


@dataclass(frozen=True, slots=True)
class OfficialOutcome:
    """The latest official forecast outcome revision visible at the cutoff."""

    event_id: str
    station_id: str | None
    observation_date: str | None
    actual_max_c: Decimal | None
    displayed_max: str | None
    winning_market_id: str | None
    winning_label: str | None
    resolution_source: str | None
    source_revision: str | None
    source_published_at_utc: datetime | None
    resolved_at_utc: datetime | None
    recorded_at_utc: datetime | None
    revision_id: int | None
    evidence: dict[str, object]

    def to_dict(self) -> dict[str, object]:
        return _serialize(self)

    # ``model_dump`` makes this result convenient beside the project's
    # Pydantic models without adding Pydantic as a requirement here.
    def model_dump(self) -> dict[str, object]:
        return self.to_dict()


@dataclass(frozen=True, slots=True)
class ForecastProvenance:
    """Exact forecast/model/source rows used for the entry comparison."""

    status: str
    algorithm_version: str | None
    prediction_id: int | None
    prediction_scan_run_id: int | None
    prediction_issued_at_utc: datetime | None
    observation_cutoff_at_utc: datetime | None
    phase: str | None
    model_source: str | None
    model: str | None
    model_version: str | None
    source_run_id: str | None
    source_payload_hash: str | None
    source_uri: str | None
    model_init_time_utc: datetime | None
    model_published_at_utc: datetime | None
    model_fetched_at_utc: datetime | None
    entry_market_snapshot_id: int | None
    entry_market_book_hash: str | None
    forecast_market_snapshot_id: int | None
    forecast_market_book_hash: str | None
    entry_book_hash_matches_forecast: bool | None
    selection_method: str
    notes: tuple[str, ...]
    metadata: dict[str, object]

    def to_dict(self) -> dict[str, object]:
        return _serialize(self)

    def model_dump(self) -> dict[str, object]:
        return self.to_dict()


@dataclass(frozen=True, slots=True)
class TradeDiagnostic:
    """One settled paper position split into forecast and trade outcomes."""

    paper_order_id: int
    decision_id: int | None
    scan_run_id: int | None
    event_id: str
    event_title: str | None
    market_id: str
    strategy_version: str
    execution_model: str | None
    opened_at_utc: datetime | None
    settled_at_utc: datetime | None

    # What was bought and what the decision believed at entry.
    bought_bracket_label: str | None
    bought_outcome: str | None
    bought_probability: Decimal | None
    forecast_probability_at_entry: Decimal | None
    entry_price: Decimal | None
    expected_profit_usd: Decimal | None
    expected_value_usd: Decimal | None
    probability_edge: Decimal | None
    shares: Decimal | None
    notional_usd: Decimal | None
    fee_usd: Decimal | None
    api_cost_usd: Decimal | None
    max_loss_usd: Decimal | None

    # The forecast's top bracket at (or before) entry.
    forecast_algorithm_version: str | None
    top_forecast_market_id: str | None
    top_forecast_bracket_label: str | None
    top_forecast_probability: Decimal | None
    top_forecast_is_bought: bool | None

    # Official resolution and the two distinct correctness questions.
    official_outcome: OfficialOutcome | None
    official_actual_max_c: Decimal | None
    official_winning_market_id: str | None
    official_winning_label: str | None
    forecast_correct: bool | None
    trade_correct: bool | None
    bought_bracket_matches_winner: bool | None
    forecast_vs_trade: str
    won_recorded: bool | None
    realized_pnl_usd: Decimal | None

    # Entry identity and source lineage.
    entry_market_snapshot_id: int | None
    entry_book_hash: str | None
    entry_snapshot_captured_at_utc: datetime | None
    provenance: ForecastProvenance
    diagnostic_notes: tuple[str, ...]

    @property
    def ev_usd(self) -> Decimal | None:
        """Short alias used by reports and notebooks."""

        return self.expected_value_usd

    def to_dict(self) -> dict[str, object]:
        return _serialize(self)

    def model_dump(self) -> dict[str, object]:
        return self.to_dict()


@dataclass(frozen=True, slots=True)
class ForecastDiagnosticsReport:
    """Immutable report containing all reconstructed settled positions."""

    schema_version: int
    generated_at_utc: datetime
    as_of_utc: datetime | None
    trades: tuple[TradeDiagnostic, ...]
    summary: dict[str, object]
    warnings: tuple[str, ...]

    @property
    def settled_trade_count(self) -> int:
        return len(self.trades)

    def to_dict(self) -> dict[str, object]:
        return _serialize(self)

    def model_dump(self) -> dict[str, object]:
        return self.to_dict()

    def to_json(self, *, indent: int = 2) -> str:
        return json.dumps(self.to_dict(), indent=indent, sort_keys=True)

    # Common aliases for CLI/notebook callers.
    as_dict = to_dict
    as_json = to_json


class ForecastDiagnostics:
    """Read-only diagnostics reader for one SQLite database."""

    def __init__(self, database: Path | str) -> None:
        self.database = Path(database).expanduser().resolve()

    def report(
        self,
        *,
        as_of_utc: datetime | None = None,
        generated_at_utc: datetime | None = None,
    ) -> ForecastDiagnosticsReport:
        cutoff = _as_utc(as_of_utc)
        requested_generated = _as_utc(generated_at_utc)
        warnings: list[str] = []
        if not self.database.is_file():
            raise FileNotFoundError(f"diagnostics database does not exist: {self.database}")

        with _open_read_only(self.database) as connection:
            capabilities = _SchemaCapabilities(connection)
            if not capabilities.has("paper_orders"):
                warnings.append("MISSING_TABLE:paper_orders")
                return _empty_report(
                    requested_generated or cutoff or datetime(1970, 1, 1, tzinfo=UTC),
                    cutoff,
                    warnings,
                )
            rows = _settled_order_rows(connection, cutoff)
            trades = tuple(
                _diagnose_order(
                    connection,
                    row,
                    capabilities=capabilities,
                    as_of_utc=cutoff,
                )
                for row in rows
            )

        generated = requested_generated or cutoff or _latest_report_timestamp(trades)
        summary = _build_summary(trades)
        return ForecastDiagnosticsReport(
            schema_version=DIAGNOSTICS_SCHEMA_VERSION,
            generated_at_utc=generated,
            as_of_utc=cutoff,
            trades=trades,
            summary=summary,
            warnings=tuple(dict.fromkeys(warnings)),
        )

    # ``run`` and ``build_report`` keep the API discoverable for callers that
    # use other project readers under those names.
    run = report
    build_report = report


def reconstruct_settled_trades(
    database: Path | str,
    *,
    as_of_utc: datetime | None = None,
    generated_at_utc: datetime | None = None,
) -> ForecastDiagnosticsReport:
    """Reconstruct all settled paper positions without touching the database."""

    return ForecastDiagnostics(database).report(
        as_of_utc=as_of_utc,
        generated_at_utc=generated_at_utc,
    )


# Friendly aliases for scripts and notebooks.
build_report = reconstruct_settled_trades
diagnose_settled_trades = reconstruct_settled_trades


@contextmanager
def _open_read_only(path: Path) -> Iterator[sqlite3.Connection]:
    """Open a SQLite file without allowing writes or schema migrations."""

    # ``quote`` is required for spaces in the external Application Support
    # path.  The slash remains unescaped so SQLite recognises the absolute
    # path.  mode=ro also works with a live WAL database.
    uri = f"file:{quote(str(path), safe='/')}?mode=ro"
    connection = sqlite3.connect(uri, uri=True, timeout=30)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA query_only = ON")
    try:
        yield connection
    finally:
        connection.close()


class _SchemaCapabilities:
    def __init__(self, connection: sqlite3.Connection) -> None:
        self._tables = {
            str(row[0])
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
        self._columns: dict[str, set[str]] = {}
        for table in self._tables:
            self._columns[table] = {
                str(row[1])
                for row in connection.execute(f"PRAGMA table_info({_quote_identifier(table)})")
            }

    def has(self, table: str) -> bool:
        return table in self._tables

    def column(self, table: str, name: str) -> bool:
        return name in self._columns.get(table, set())


def _settled_order_rows(
    connection: sqlite3.Connection,
    as_of_utc: datetime | None,
) -> list[sqlite3.Row]:
    rows = connection.execute("SELECT * FROM paper_orders ORDER BY id").fetchall()
    result: list[sqlite3.Row] = []
    for row in rows:
        status = str(_row_value(row, "status", "")).upper()
        if status not in _SETTLED_STATUSES:
            continue
        if as_of_utc is not None:
            settled = _parse_datetime(
                _row_value(row, "settled_at")
                or _row_value(row, "closed_at")
                or _row_value(row, "resolved_at")
            )
            if settled is not None and settled > as_of_utc:
                continue
        result.append(row)
    return result


def _diagnose_order(
    connection: sqlite3.Connection,
    order: sqlite3.Row,
    *,
    capabilities: _SchemaCapabilities,
    as_of_utc: datetime | None,
) -> TradeDiagnostic:
    notes: list[str] = []
    order_id = _as_int(_row_value(order, "id")) or 0
    event_id = str(_row_value(order, "event_id", ""))
    market_id = str(_row_value(order, "market_id", ""))
    opened_at = _parse_datetime(_row_value(order, "opened_at"))
    settled_at = _parse_datetime(
        _row_value(order, "settled_at")
        or _row_value(order, "closed_at")
        or _row_value(order, "resolved_at")
    )

    decision_row = _entry_decision(connection, order, opened_at, capabilities)
    decision_payload: dict[str, Any] = {}
    decision_id: int | None = None
    scan_run_id: int | None = None
    if decision_row is not None:
        decision_id = _as_int(_row_value(decision_row, "id"))
        scan_run_id = _as_int(_row_value(decision_row, "run_id"))
        decision_payload = _json_object(_row_value(decision_row, "payload_json"))
        if not decision_payload and _row_value(decision_row, "payload_json") is not None:
            notes.append("DECISION_PAYLOAD_INVALID")
    else:
        notes.append("DECISION_ROW_MISSING")

    strategy_version = str(
        _row_value(order, "strategy_version") or decision_payload.get("strategy_version") or "v0"
    )
    execution_model = _optional_str(
        _row_value(order, "execution_model") or decision_payload.get("execution_model")
    )

    entry_snapshot = _entry_market_snapshot(
        connection,
        event_id=event_id,
        market_id=market_id,
        opened_at=opened_at,
        scan_run_id=scan_run_id,
        book_hash=_optional_str(decision_payload.get("book_hash")),
        capabilities=capabilities,
    )
    if entry_snapshot is None:
        notes.append("ENTRY_MARKET_SNAPSHOT_MISSING")
    elif entry_snapshot.future_relative_to_entry:
        notes.append("ENTRY_MARKET_SNAPSHOT_AFTER_ENTRY")
    if entry_snapshot is not None and entry_snapshot.book_hash_matches_requested is False:
        notes.append("ENTRY_BOOK_HASH_NOT_FOUND")

    bought_label = (
        entry_snapshot.outcome_label
        if entry_snapshot is not None
        else _optional_str(decision_payload.get("outcome_label"))
    )
    event_title = entry_snapshot.event_title if entry_snapshot is not None else None
    bought_outcome = _optional_str(_row_value(order, "outcome") or decision_payload.get("outcome"))

    algorithm = _select_algorithm(strategy_version, decision_payload, connection, event_id)
    prediction, probabilities, provenance = _entry_forecast(
        connection,
        event_id=event_id,
        market_id=market_id,
        opened_at=opened_at,
        scan_run_id=scan_run_id,
        strategy_version=strategy_version,
        algorithm_version=algorithm,
        entry_snapshot=entry_snapshot,
        capabilities=capabilities,
    )
    notes.extend(provenance.notes)

    bought_probability = _decimal_or_none(decision_payload.get("probability"))
    forecast_probability = (
        probabilities[market_id].probability if market_id in probabilities else None
    )
    if (
        bought_probability is not None
        and forecast_probability is not None
        and bought_probability != forecast_probability
    ):
        notes.append("BOUGHT_PROBABILITY_DIFFERS_FROM_FORECAST_ARCHIVE")
    if bought_probability is None and decision_payload.get("probability") is not None:
        notes.append("BOUGHT_PROBABILITY_INVALID")

    top = _top_probability(probabilities.values())
    official = _latest_outcome(
        connection,
        event_id=event_id,
        as_of_utc=as_of_utc,
        capabilities=capabilities,
    )
    won_recorded = _bool_or_none(_row_value(order, "won"))
    official_winner = None if official is None else official.winning_market_id
    bought_matches = None if official_winner is None else market_id == official_winner
    trade_correct = won_recorded
    trade_correct_source = "paper_orders.won" if won_recorded is not None else "derived"
    if trade_correct is None and official_winner is not None:
        if bought_outcome is None or bought_outcome.upper() == "YES":
            trade_correct = bought_matches
        elif bought_outcome.upper() == "NO":
            # Each weather event has one winning bracket.  A NO token wins
            # when its bracket is not that winner.
            trade_correct = not bought_matches
        else:
            notes.append("UNKNOWN_OUTCOME_SIDE")
        if trade_correct is not None:
            trade_correct_source = "derived_from_official_winner"
    if trade_correct_source == "derived" and official is None:
        notes.append("TRADE_RESULT_UNAVAILABLE")

    forecast_correct = (
        None if top is None or official_winner is None else top.market_id == official_winner
    )
    if top is None:
        notes.append("TOP_FORECAST_UNAVAILABLE")
    if official is None:
        notes.append("OFFICIAL_OUTCOME_MISSING")
    comparison = _comparison_label(forecast_correct, trade_correct)

    expected_profit = _first_decimal(
        decision_payload.get("expected_profit_usd"),
        _row_value(order, "expected_profit_usd"),
    )
    # ``expected_value_usd`` is deliberately a separate report field, while
    # retaining the database's established expected_profit name as the
    # source of truth.
    return TradeDiagnostic(
        paper_order_id=order_id,
        decision_id=decision_id,
        scan_run_id=scan_run_id,
        event_id=event_id,
        event_title=event_title,
        market_id=market_id,
        strategy_version=strategy_version,
        execution_model=execution_model,
        opened_at_utc=opened_at,
        settled_at_utc=settled_at,
        bought_bracket_label=bought_label,
        bought_outcome=bought_outcome,
        bought_probability=bought_probability,
        forecast_probability_at_entry=forecast_probability,
        entry_price=_first_decimal(
            decision_payload.get("executable_price"), _row_value(order, "entry_price")
        ),
        expected_profit_usd=expected_profit,
        expected_value_usd=expected_profit,
        probability_edge=_decimal_or_none(decision_payload.get("probability_edge")),
        shares=_first_decimal(decision_payload.get("shares"), _row_value(order, "shares")),
        notional_usd=_first_decimal(
            decision_payload.get("notional_usd"), _row_value(order, "notional_usd")
        ),
        fee_usd=_first_decimal(decision_payload.get("fee_usd"), _row_value(order, "fee_usd")),
        api_cost_usd=_first_decimal(
            decision_payload.get("api_cost_usd"), _row_value(order, "api_cost_usd")
        ),
        max_loss_usd=_first_decimal(
            decision_payload.get("max_loss_usd"), _row_value(order, "max_loss_usd")
        ),
        forecast_algorithm_version=algorithm,
        top_forecast_market_id=None if top is None else top.market_id,
        top_forecast_bracket_label=None if top is None else top.label,
        top_forecast_probability=None if top is None else top.probability,
        top_forecast_is_bought=(None if top is None else top.market_id == market_id),
        official_outcome=official,
        official_actual_max_c=None if official is None else official.actual_max_c,
        official_winning_market_id=official_winner,
        official_winning_label=None if official is None else official.winning_label,
        forecast_correct=forecast_correct,
        trade_correct=trade_correct,
        bought_bracket_matches_winner=bought_matches,
        forecast_vs_trade=comparison,
        won_recorded=won_recorded,
        realized_pnl_usd=_decimal_or_none(_row_value(order, "realized_pnl_usd")),
        entry_market_snapshot_id=None if entry_snapshot is None else entry_snapshot.snapshot_id,
        entry_book_hash=None if entry_snapshot is None else entry_snapshot.book_hash,
        entry_snapshot_captured_at_utc=(
            None if entry_snapshot is None else entry_snapshot.captured_at_utc
        ),
        provenance=provenance,
        diagnostic_notes=tuple(
            dict.fromkeys(notes + [f"trade_result_source:{trade_correct_source}"])
        ),
    )


@dataclass(frozen=True, slots=True)
class _EntrySnapshot:
    snapshot_id: int
    run_id: int | None
    captured_at_utc: datetime | None
    event_title: str | None
    outcome_label: str | None
    book_hash: str | None
    requested_book_hash: str | None
    book_hash_matches_requested: bool | None
    future_relative_to_entry: bool
    payload: dict[str, Any]


@dataclass(frozen=True, slots=True)
class _Probability:
    market_id: str
    label: str
    probability: Decimal
    market_snapshot_id: int | None
    book_hash: str | None
    ordinal: int


def _entry_decision(
    connection: sqlite3.Connection,
    order: sqlite3.Row,
    opened_at: datetime | None,
    capabilities: _SchemaCapabilities,
) -> sqlite3.Row | None:
    if not capabilities.has("decisions"):
        return None
    order_id = _as_int(_row_value(order, "id"))
    if order_id is not None and capabilities.column("decisions", "paper_order_id"):
        row = connection.execute(
            "SELECT * FROM decisions WHERE paper_order_id=? "
            "ORDER BY created_at DESC, id DESC LIMIT 1",
            (order_id,),
        ).fetchone()
        if row is not None:
            return row

    # Pre-lifecycle rows may not have carried paper_order_id.  Match only a
    # decision at or before entry and then choose the nearest immutable row.
    event_id = str(_row_value(order, "event_id", ""))
    market_id = str(_row_value(order, "market_id", ""))
    rows = connection.execute(
        "SELECT * FROM decisions WHERE event_id=? AND market_id=? "
        "ORDER BY created_at DESC, id DESC",
        (event_id, market_id),
    ).fetchall()
    if opened_at is None:
        return rows[0] if rows else None
    eligible = [
        row
        for row in rows
        if (_parse_datetime(_row_value(row, "created_at")) or datetime.min.replace(tzinfo=UTC))
        <= opened_at
    ]
    return eligible[0] if eligible else None


def _entry_market_snapshot(
    connection: sqlite3.Connection,
    *,
    event_id: str,
    market_id: str,
    opened_at: datetime | None,
    scan_run_id: int | None,
    book_hash: str | None,
    capabilities: _SchemaCapabilities,
) -> _EntrySnapshot | None:
    if not capabilities.has("market_snapshots"):
        return None
    rows = connection.execute(
        "SELECT id, run_id, captured_at, payload_json FROM market_snapshots "
        "WHERE event_id=? AND market_id=? ORDER BY id",
        (event_id, market_id),
    ).fetchall()
    candidates: list[_EntrySnapshot] = []
    for row in rows:
        payload = _json_object(_row_value(row, "payload_json"))
        captured = _parse_datetime(_row_value(row, "captured_at"))
        payload_hash = _optional_str(payload.get("book_hash"))
        candidates.append(
            _EntrySnapshot(
                snapshot_id=_as_int(_row_value(row, "id")) or 0,
                run_id=_as_int(_row_value(row, "run_id")),
                captured_at_utc=captured,
                event_title=_optional_str(payload.get("event_title")),
                outcome_label=_optional_str(payload.get("outcome_label")),
                book_hash=payload_hash,
                requested_book_hash=book_hash,
                book_hash_matches_requested=(
                    None if book_hash is None else payload_hash == book_hash
                ),
                future_relative_to_entry=(
                    opened_at is not None and captured is not None and captured > opened_at
                ),
                payload=payload,
            )
        )
    if not candidates:
        return None

    # An exact immutable book hash is stronger evidence than timestamp
    # proximity.  Within each class, prefer the same scan run, then a
    # non-future snapshot, then the closest timestamp.
    exact = [item for item in candidates if book_hash is not None and item.book_hash == book_hash]
    pool = exact or candidates
    before = [
        item
        for item in pool
        if opened_at is None or item.captured_at_utc is None or item.captured_at_utc <= opened_at
    ]
    if before:
        pool = before

    def score(item: _EntrySnapshot) -> tuple[int, int, int, float, int]:
        same_run = int(scan_run_id is not None and item.run_id == scan_run_id)
        exact_hash = int(book_hash is not None and item.book_hash == book_hash)
        not_future = int(not item.future_relative_to_entry)
        if opened_at is None or item.captured_at_utc is None:
            distance = 0.0
        else:
            distance = abs((item.captured_at_utc - opened_at).total_seconds())
        # Negative distance means closer is better; id is a deterministic
        # final tie-breaker (smaller id wins).
        return (exact_hash, same_run, not_future, -distance, -item.snapshot_id)

    return max(pool, key=score)


def _select_algorithm(
    strategy_version: str,
    decision_payload: Mapping[str, Any],
    connection: sqlite3.Connection,
    event_id: str,
) -> str | None:
    explicit = _optional_str(
        decision_payload.get("forecast_algorithm_version")
        or decision_payload.get("algorithm_version")
    )
    if explicit is not None:
        return explicit
    normalized = strategy_version.strip().lower()
    if normalized in {"v0", "legacy", "recovered_v0"}:
        return None
    if normalized in {"v1", "recovered_v1"}:
        return OPEN_METEO_ALGORITHM_VERSION
    if normalized in {"v2", "forecast-v2", "forecast_engine_v2"}:
        return FORECAST_V2_ALGORITHM_VERSION
    if strategy_version in _KNOWN_ALGORITHMS:
        return strategy_version

    # A custom strategy can still be reconstructed if exactly one known
    # algorithm was persisted for the event.  Do not guess when there are
    # several competing model versions.
    try:
        rows = connection.execute(
            "SELECT DISTINCT algorithm_version FROM forecast_predictions_v2 "
            "WHERE event_id=? ORDER BY algorithm_version",
            (event_id,),
        ).fetchall()
    except sqlite3.OperationalError:
        return None
    available = [str(row[0]) for row in rows if str(row[0]) in _KNOWN_ALGORITHMS]
    return available[0] if len(available) == 1 else None


def _entry_forecast(
    connection: sqlite3.Connection,
    *,
    event_id: str,
    market_id: str,
    opened_at: datetime | None,
    scan_run_id: int | None,
    strategy_version: str,
    algorithm_version: str | None,
    entry_snapshot: _EntrySnapshot | None,
    capabilities: _SchemaCapabilities,
) -> tuple[sqlite3.Row | None, dict[str, _Probability], ForecastProvenance]:
    entry_snapshot_id = None if entry_snapshot is None else entry_snapshot.snapshot_id
    entry_book_hash = None if entry_snapshot is None else entry_snapshot.book_hash
    unavailable = ForecastProvenance(
        status="UNAVAILABLE",
        algorithm_version=algorithm_version,
        prediction_id=None,
        prediction_scan_run_id=None,
        prediction_issued_at_utc=None,
        observation_cutoff_at_utc=None,
        phase=None,
        model_source=None,
        model=None,
        model_version=None,
        source_run_id=None,
        source_payload_hash=None,
        source_uri=None,
        model_init_time_utc=None,
        model_published_at_utc=None,
        model_fetched_at_utc=None,
        entry_market_snapshot_id=entry_snapshot_id,
        entry_market_book_hash=entry_book_hash,
        forecast_market_snapshot_id=None,
        forecast_market_book_hash=None,
        entry_book_hash_matches_forecast=None,
        selection_method=(
            "strategy_v0_no_forecast"
            if algorithm_version is None and strategy_version.lower() == "v0"
            else "no_prediction_at_or_before_entry"
        ),
        notes=(),
        metadata={},
    )
    if algorithm_version is None:
        return None, {}, unavailable
    if not capabilities.has("forecast_predictions_v2"):
        return None, {}, _with_provenance_note(unavailable, "MISSING_TABLE:forecast_predictions_v2")

    prediction_query = (
        "SELECT p.*, "
        "m.source AS model_source, m.model AS model_name, "
        "m.model_version AS model_version_name, m.source_run_id AS model_source_run_id, "
        "m.init_time_utc AS model_init_time, m.published_at_utc AS model_published_at, "
        "m.first_fetched_at_utc AS model_fetched_at, m.source_uri AS model_source_uri, "
        "m.source_payload_hash AS model_payload_hash, "
        "m.metadata_json AS model_metadata_json "
        "FROM forecast_predictions_v2 p "
        "LEFT JOIN forecast_model_runs_v2 m ON m.id=p.model_run_id "
        "WHERE p.event_id=? AND p.algorithm_version=? "
        "ORDER BY p.issued_at_utc DESC, p.id DESC"
    )
    try:
        rows = connection.execute(prediction_query, (event_id, algorithm_version)).fetchall()
    except sqlite3.OperationalError:
        # A partially migrated database may have predictions but not model
        # provenance.  Fall back to the prediction columns, preserving the
        # missing provenance explicitly.
        rows = connection.execute(
            "SELECT p.* FROM forecast_predictions_v2 p "
            "WHERE p.event_id=? AND p.algorithm_version=? "
            "ORDER BY p.issued_at_utc DESC, p.id DESC",
            (event_id, algorithm_version),
        ).fetchall()
    if opened_at is None:
        eligible = rows
    else:
        eligible = []
        for row in rows:
            issued_at = _parse_datetime(_row_value(row, "issued_at_utc"))
            if issued_at is not None and issued_at <= opened_at:
                eligible.append(row)
    same_run = [
        row
        for row in eligible
        if scan_run_id is not None and _as_int(_row_value(row, "scan_run_id")) == scan_run_id
    ]
    selected = same_run or eligible
    if not selected:
        return None, {}, _with_provenance_note(unavailable, "NO_PREDICTION_AT_OR_BEFORE_ENTRY")
    prediction = max(
        selected,
        key=lambda row: (
            _parse_datetime(_row_value(row, "issued_at_utc")) or datetime.min.replace(tzinfo=UTC),
            _as_int(_row_value(row, "id")) or 0,
        ),
    )

    probabilities = _prediction_probabilities(connection, prediction, capabilities)
    metadata = _json_object(_row_value(prediction, "metadata_json"))
    model_metadata = _json_object(_row_value(prediction, "model_metadata_json"))
    merged_metadata = _safe_metadata({**model_metadata, **metadata})
    forecast_snapshot_id: int | None = None
    forecast_book_hash: str | None = None
    bought_probability = probabilities.get(market_id)
    if bought_probability is not None:
        forecast_snapshot_id = bought_probability.market_snapshot_id
        forecast_book_hash = bought_probability.book_hash
    # The probability for the purchased market is not available through the
    # snapshot payload in all old databases; callers can still inspect the
    # full probability map from the trade's scalar fields/top bracket.
    matches = (
        None
        if entry_book_hash is None or forecast_book_hash is None
        else entry_book_hash == forecast_book_hash
    )
    selection_method = "same_scan_run_before_entry" if same_run else "event_before_entry"
    notes: list[str] = []
    if not probabilities:
        notes.append("FORECAST_PROBABILITIES_MISSING")
    if entry_book_hash is not None and forecast_book_hash is not None and matches is False:
        notes.append("FORECAST_ENTRY_BOOK_HASH_MISMATCH")
    provenance = ForecastProvenance(
        status="AVAILABLE" if probabilities else "PARTIAL",
        algorithm_version=algorithm_version,
        prediction_id=_as_int(_row_value(prediction, "id")),
        prediction_scan_run_id=_as_int(_row_value(prediction, "scan_run_id")),
        prediction_issued_at_utc=_parse_datetime(_row_value(prediction, "issued_at_utc")),
        observation_cutoff_at_utc=_parse_datetime(
            _row_value(prediction, "observation_cutoff_at_utc")
        ),
        phase=_optional_str(_row_value(prediction, "phase")),
        model_source=_optional_str(_row_value(prediction, "model_source")),
        model=_optional_str(_row_value(prediction, "model_name")),
        model_version=_optional_str(_row_value(prediction, "model_version_name")),
        source_run_id=_optional_str(_row_value(prediction, "model_source_run_id")),
        source_payload_hash=_optional_str(_row_value(prediction, "model_payload_hash")),
        source_uri=_optional_str(_row_value(prediction, "model_source_uri")),
        model_init_time_utc=_parse_datetime(_row_value(prediction, "model_init_time")),
        model_published_at_utc=_parse_datetime(_row_value(prediction, "model_published_at")),
        model_fetched_at_utc=_parse_datetime(_row_value(prediction, "model_fetched_at")),
        entry_market_snapshot_id=entry_snapshot_id,
        entry_market_book_hash=entry_book_hash,
        forecast_market_snapshot_id=forecast_snapshot_id,
        forecast_market_book_hash=forecast_book_hash,
        entry_book_hash_matches_forecast=matches,
        selection_method=selection_method,
        notes=tuple(notes),
        metadata=merged_metadata,
    )
    return prediction, probabilities, provenance


def _prediction_probabilities(
    connection: sqlite3.Connection,
    prediction: sqlite3.Row,
    capabilities: _SchemaCapabilities,
) -> dict[str, _Probability]:
    if not capabilities.has("forecast_probabilities_v2"):
        return {}
    prediction_id = _as_int(_row_value(prediction, "id"))
    if prediction_id is None:
        return {}
    rows = connection.execute(
        "SELECT market_id, market_snapshot_id, book_hash, outcome_label, probability, ordinal "
        "FROM forecast_probabilities_v2 WHERE prediction_id=? ORDER BY ordinal",
        (prediction_id,),
    ).fetchall()
    result: dict[str, _Probability] = {}
    for row in rows:
        market_id = str(_row_value(row, "market_id", ""))
        probability = _decimal_or_none(_row_value(row, "probability"))
        if not market_id or probability is None:
            continue
        result[market_id] = _Probability(
            market_id=market_id,
            label=str(_row_value(row, "outcome_label", "")),
            probability=probability,
            market_snapshot_id=_as_int(_row_value(row, "market_snapshot_id")),
            book_hash=_optional_str(_row_value(row, "book_hash")),
            ordinal=_as_int(_row_value(row, "ordinal")) or 0,
        )
    return result


def _latest_outcome(
    connection: sqlite3.Connection,
    *,
    event_id: str,
    as_of_utc: datetime | None,
    capabilities: _SchemaCapabilities,
) -> OfficialOutcome | None:
    if not capabilities.has("forecast_outcome_versions_v2"):
        return None
    rows = connection.execute(
        "SELECT * FROM forecast_outcome_versions_v2 WHERE event_id=? "
        "ORDER BY recorded_at_utc DESC, id DESC",
        (event_id,),
    ).fetchall()
    eligible = []
    for row in rows:
        recorded = _parse_datetime(_row_value(row, "recorded_at_utc"))
        if as_of_utc is not None and recorded is not None and recorded > as_of_utc:
            continue
        eligible.append(row)
    if not eligible:
        return None
    row = eligible[0]
    return OfficialOutcome(
        event_id=event_id,
        station_id=_optional_str(_row_value(row, "station_id")),
        observation_date=_optional_str(_row_value(row, "observation_date")),
        actual_max_c=_decimal_or_none(_row_value(row, "actual_max_c")),
        displayed_max=_optional_str(_row_value(row, "displayed_max")),
        winning_market_id=_optional_str(_row_value(row, "winning_market_id")),
        winning_label=_optional_str(_row_value(row, "winning_label")),
        resolution_source=_optional_str(_row_value(row, "resolution_source")),
        source_revision=_optional_str(_row_value(row, "source_revision")),
        source_published_at_utc=_parse_datetime(_row_value(row, "source_published_at_utc")),
        resolved_at_utc=_parse_datetime(_row_value(row, "resolved_at_utc")),
        recorded_at_utc=_parse_datetime(_row_value(row, "recorded_at_utc")),
        revision_id=_as_int(_row_value(row, "id")),
        evidence=_safe_metadata(_json_object(_row_value(row, "evidence_json"))),
    )


def _top_probability(values: Sequence[_Probability] | Any) -> _Probability | None:
    materialized = list(values)
    if not materialized:
        return None
    return max(materialized, key=lambda item: (item.probability, -item.ordinal, item.market_id))


def _comparison_label(forecast_correct: bool | None, trade_correct: bool | None) -> str:
    if forecast_correct is None or trade_correct is None:
        return "UNAVAILABLE"
    if forecast_correct and trade_correct:
        return "FORECAST_AND_TRADE_CORRECT"
    if forecast_correct and not trade_correct:
        return "FORECAST_CORRECT_TRADE_WRONG"
    if not forecast_correct and trade_correct:
        return "FORECAST_WRONG_TRADE_CORRECT"
    return "FORECAST_AND_TRADE_WRONG"


def _build_summary(trades: Sequence[TradeDiagnostic]) -> dict[str, object]:
    comparisons = Counter(item.forecast_vs_trade for item in trades)
    strategies = Counter(item.strategy_version for item in trades)
    forecast_values = [
        item.forecast_correct for item in trades if item.forecast_correct is not None
    ]
    trade_values = [item.trade_correct for item in trades if item.trade_correct is not None]
    return {
        "settled_trade_count": len(trades),
        "forecast_available_count": sum(item.provenance.status == "AVAILABLE" for item in trades),
        "forecast_partial_count": sum(item.provenance.status == "PARTIAL" for item in trades),
        "official_outcome_count": sum(item.official_outcome is not None for item in trades),
        "forecast_correct_count": sum(value is True for value in forecast_values),
        "forecast_incorrect_count": sum(value is False for value in forecast_values),
        "trade_correct_count": sum(value is True for value in trade_values),
        "trade_incorrect_count": sum(value is False for value in trade_values),
        "comparison_counts": dict(sorted(comparisons.items())),
        "strategy_counts": dict(sorted(strategies.items())),
        "missing_forecast_count": sum(item.top_forecast_market_id is None for item in trades),
    }


def _latest_report_timestamp(trades: Sequence[TradeDiagnostic]) -> datetime:
    """Choose a deterministic report timestamp from immutable input rows."""

    candidates = [
        value
        for item in trades
        for value in (
            item.settled_at_utc,
            None if item.official_outcome is None else item.official_outcome.recorded_at_utc,
            item.opened_at_utc,
        )
        if value is not None
    ]
    return max(candidates, default=datetime(1970, 1, 1, tzinfo=UTC))


def _empty_report(
    generated_at_utc: datetime,
    as_of_utc: datetime | None,
    warnings: Sequence[str],
) -> ForecastDiagnosticsReport:
    return ForecastDiagnosticsReport(
        schema_version=DIAGNOSTICS_SCHEMA_VERSION,
        generated_at_utc=generated_at_utc,
        as_of_utc=as_of_utc,
        trades=(),
        summary=_build_summary(()),
        warnings=tuple(warnings),
    )


def _with_provenance_note(provenance: ForecastProvenance, note: str) -> ForecastProvenance:
    return ForecastProvenance(
        **{
            **asdict(provenance),
            "notes": tuple(dict.fromkeys((*provenance.notes, note))),
        }
    )


def _row_value(row: sqlite3.Row | Mapping[str, Any], name: str, default: Any = None) -> Any:
    if isinstance(row, Mapping):
        return row.get(name, default)
    try:
        return row[name]
    except (IndexError, KeyError):
        return default


def _json_object(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    if not isinstance(value, str) or not value:
        return {}
    try:
        parsed = json.loads(value)
    except (TypeError, ValueError):
        return {}
    return dict(parsed) if isinstance(parsed, Mapping) else {}


def _safe_metadata(value: Mapping[str, Any]) -> dict[str, object]:
    """Copy provenance metadata while dropping credential-like fields."""

    def clean(item: Any, key: str | None = None) -> Any:
        if key is not None:
            lowered = key.lower()
            if any(part in lowered for part in _SENSITIVE_METADATA_PARTS):
                return "[REDACTED]"
        if isinstance(item, Mapping):
            return {str(k): clean(v, str(k)) for k, v in item.items()}
        if isinstance(item, list):
            return [clean(v) for v in item]
        if isinstance(item, tuple):
            return [clean(v) for v in item]
        return item

    cleaned = clean(value)
    return dict(cleaned) if isinstance(cleaned, Mapping) else {}


def _serialize(value: Any) -> Any:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, datetime):
        return value.astimezone(UTC).isoformat() if value.tzinfo is not None else value.isoformat()
    if is_dataclass(value):
        return {field.name: _serialize(getattr(value, field.name)) for field in fields(value)}
    if isinstance(value, Mapping):
        return {str(key): _serialize(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [_serialize(item) for item in value]
    return value


def _as_utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _parse_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return _as_utc(value)
    if not isinstance(value, str) or not value:
        return None
    candidate = value.strip()
    if candidate.endswith("Z"):
        candidate = candidate[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(candidate)
    except ValueError:
        return None
    return _as_utc(parsed)


def _decimal_or_none(value: Any) -> Decimal | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return None


def _first_decimal(*values: Any) -> Decimal | None:
    for value in values:
        parsed = _decimal_or_none(value)
        if parsed is not None:
            return parsed
    return None


def _as_int(value: Any) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _bool_or_none(value: Any) -> bool | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "y"}:
        return True
    if text in {"0", "false", "no", "n"}:
        return False
    return None


def _optional_str(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value)
    return text if text else None


def _quote_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'
