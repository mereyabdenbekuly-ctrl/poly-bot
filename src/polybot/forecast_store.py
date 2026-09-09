from __future__ import annotations

import hashlib
import json
import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

from polybot.forecast_models import (
    FORECAST_SCHEMA_VERSION,
    ForecastEvaluationCase,
    ForecastMetricsQuery,
    ForecastMetricsReport,
    ForecastMetricsSlice,
    ForecastPhase,
    ForecastSubmission,
    RealizedForecastOutcome,
    compute_metrics_slice,
)


class ForecastStore:
    """Append-only forecast-engine-v2 storage beside immutable v1 tables."""

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
    def transaction(self) -> Iterator[sqlite3.Connection]:
        connection = self.connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
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
                CREATE TABLE IF NOT EXISTS forecast_engine_schema_versions (
                    version INTEGER PRIMARY KEY,
                    applied_at_utc TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS forecast_model_runs_v2 (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    source TEXT NOT NULL,
                    model TEXT NOT NULL,
                    model_version TEXT,
                    source_run_id TEXT,
                    init_time_utc TEXT,
                    published_at_utc TEXT,
                    first_fetched_at_utc TEXT NOT NULL,
                    source_uri TEXT,
                    source_payload_hash TEXT,
                    metadata_json TEXT NOT NULL,
                    provenance_key TEXT NOT NULL UNIQUE,
                    created_at_utc TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS forecast_predictions_v2 (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    scan_run_id INTEGER NOT NULL REFERENCES scan_runs(id),
                    event_id TEXT NOT NULL,
                    model_run_id INTEGER NOT NULL REFERENCES forecast_model_runs_v2(id),
                    algorithm_version TEXT NOT NULL,
                    phase TEXT NOT NULL,
                    issued_at_utc TEXT NOT NULL,
                    model_fetched_at_utc TEXT NOT NULL,
                    lead_time_seconds INTEGER NOT NULL,
                    intraday_elapsed_seconds INTEGER,
                    intraday_remaining_seconds INTEGER,
                    station_id TEXT NOT NULL,
                    observation_date TEXT NOT NULL,
                    station_timezone TEXT NOT NULL,
                    rule_day_start_utc TEXT NOT NULL,
                    rule_day_end_utc TEXT NOT NULL,
                    display_unit TEXT NOT NULL,
                    precision_decimal_places INTEGER NOT NULL,
                    rounding_rule TEXT NOT NULL,
                    rules_hash TEXT NOT NULL,
                    point_forecast_c TEXT NOT NULL,
                    observed_floor_c TEXT,
                    scenario_count INTEGER NOT NULL,
                    observation_revision_count INTEGER NOT NULL,
                    observation_cutoff_at_utc TEXT NOT NULL,
                    distribution_mass TEXT NOT NULL,
                    submission_hash TEXT NOT NULL UNIQUE,
                    created_at_utc TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS forecast_scenarios_v2 (
                    prediction_id INTEGER NOT NULL REFERENCES forecast_predictions_v2(id),
                    member_id TEXT NOT NULL,
                    raw_max_c TEXT NOT NULL,
                    adjusted_max_c TEXT NOT NULL,
                    weight TEXT NOT NULL,
                    PRIMARY KEY(prediction_id, member_id)
                );

                CREATE TABLE IF NOT EXISTS forecast_probabilities_v2 (
                    prediction_id INTEGER NOT NULL REFERENCES forecast_predictions_v2(id),
                    market_id TEXT NOT NULL,
                    market_snapshot_id INTEGER NOT NULL REFERENCES market_snapshots(id),
                    condition_id TEXT,
                    token_id TEXT,
                    outcome TEXT,
                    book_hash TEXT NOT NULL,
                    outcome_label TEXT NOT NULL,
                    lower_bound TEXT,
                    upper_bound TEXT,
                    lower_inclusive INTEGER NOT NULL,
                    upper_inclusive INTEGER NOT NULL,
                    probability TEXT NOT NULL,
                    ordinal INTEGER NOT NULL,
                    PRIMARY KEY(prediction_id, market_id)
                );

                CREATE TABLE IF NOT EXISTS forecast_observation_evidence_v2 (
                    prediction_id INTEGER NOT NULL REFERENCES forecast_predictions_v2(id),
                    station_observation_version_id INTEGER
                        REFERENCES station_observation_versions(id),
                    station_id TEXT NOT NULL,
                    observed_at_utc TEXT NOT NULL,
                    revision_hash TEXT NOT NULL,
                    first_seen_at_utc TEXT NOT NULL,
                    source TEXT NOT NULL,
                    temperature_c TEXT NOT NULL,
                    displayed_temperature_c TEXT NOT NULL,
                    corrected INTEGER NOT NULL,
                    PRIMARY KEY(prediction_id, station_id, observed_at_utc, revision_hash)
                );

                CREATE TABLE IF NOT EXISTS forecast_outcome_versions_v2 (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_id TEXT NOT NULL,
                    station_id TEXT NOT NULL,
                    observation_date TEXT NOT NULL,
                    actual_max_c TEXT NOT NULL,
                    displayed_max TEXT NOT NULL,
                    winning_market_id TEXT NOT NULL,
                    winning_condition_id TEXT,
                    winning_label TEXT,
                    resolution_source TEXT NOT NULL,
                    source_revision TEXT NOT NULL,
                    source_published_at_utc TEXT,
                    resolved_at_utc TEXT NOT NULL,
                    recorded_at_utc TEXT NOT NULL,
                    evidence_json TEXT NOT NULL,
                    outcome_hash TEXT NOT NULL UNIQUE,
                    UNIQUE(event_id, source_revision)
                );

                CREATE INDEX IF NOT EXISTS forecast_predictions_event_idx
                    ON forecast_predictions_v2(event_id, phase, issued_at_utc);
                CREATE INDEX IF NOT EXISTS forecast_predictions_model_idx
                    ON forecast_predictions_v2(model_run_id, algorithm_version);
                CREATE INDEX IF NOT EXISTS forecast_probabilities_market_idx
                    ON forecast_probabilities_v2(market_id, market_snapshot_id);
                CREATE INDEX IF NOT EXISTS forecast_outcomes_event_idx
                    ON forecast_outcome_versions_v2(event_id, recorded_at_utc, id);
                """
            )
            connection.execute(
                "INSERT OR IGNORE INTO forecast_engine_schema_versions(version, applied_at_utc) "
                "VALUES (?, ?)",
                (FORECAST_SCHEMA_VERSION, datetime.now(UTC).isoformat()),
            )

    def record(self, submission: ForecastSubmission) -> int:
        """Persist a fully linked forecast; identical submissions are idempotent."""

        submission_hash = _digest(submission.model_dump(mode="json"))
        with self.transaction() as connection:
            existing = connection.execute(
                "SELECT id FROM forecast_predictions_v2 WHERE submission_hash = ?",
                (submission_hash,),
            ).fetchone()
            if existing is not None:
                return int(existing["id"])
            self._require_scan_run(connection, submission.scan_run_id)
            snapshots = self._market_snapshots(
                connection,
                run_id=submission.scan_run_id,
                event_id=submission.event_id,
                market_ids={item.market_id for item in submission.probabilities},
            )
            model_run_id = self._record_model_run(connection, submission)
            prediction_id = self._insert_prediction(
                connection,
                submission=submission,
                submission_hash=submission_hash,
                model_run_id=model_run_id,
            )
            self._insert_scenarios(connection, prediction_id, submission)
            self._insert_probabilities(connection, prediction_id, submission, snapshots)
            self._insert_observation_evidence(connection, prediction_id, submission)
            return prediction_id

    def record_outcome(self, outcome: RealizedForecastOutcome) -> int:
        """Append one outcome revision; never rewrite an earlier revision."""

        outcome_hash = _digest(outcome.model_dump(mode="json"))
        with self.transaction() as connection:
            existing = connection.execute(
                "SELECT id, outcome_hash FROM forecast_outcome_versions_v2 "
                "WHERE event_id = ? AND source_revision = ?",
                (outcome.event_id, outcome.source_revision),
            ).fetchone()
            if existing is not None:
                if existing["outcome_hash"] != outcome_hash:
                    raise ValueError(
                        "outcome revision is immutable; use a new source_revision for corrections"
                    )
                return int(existing["id"])
            cursor = connection.execute(
                """
                INSERT INTO forecast_outcome_versions_v2(
                    event_id, station_id, observation_date, actual_max_c, displayed_max,
                    winning_market_id, winning_condition_id, winning_label,
                    resolution_source, source_revision, source_published_at_utc,
                    resolved_at_utc, recorded_at_utc, evidence_json, outcome_hash
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    outcome.event_id,
                    outcome.station_id,
                    outcome.observation_date.isoformat(),
                    str(outcome.actual_max_c),
                    str(outcome.displayed_max),
                    outcome.winning_market_id,
                    outcome.winning_condition_id,
                    outcome.winning_label,
                    outcome.resolution_source,
                    outcome.source_revision,
                    None
                    if outcome.source_published_at_utc is None
                    else outcome.source_published_at_utc.isoformat(),
                    outcome.resolved_at_utc.isoformat(),
                    outcome.recorded_at_utc.isoformat(),
                    _canonical_json(outcome.evidence),
                    outcome_hash,
                ),
            )
            return _lastrowid(cursor)

    def metrics(self, query: ForecastMetricsQuery | None = None) -> ForecastMetricsReport:
        query = query or ForecastMetricsQuery()
        predictions = self._load_predictions(query)
        outcomes = self._load_latest_outcomes(as_of=query.as_of_utc)
        outcome_count = len(outcomes)

        def make_slice(segment: str, rows: list[sqlite3.Row]) -> ForecastMetricsSlice:
            latest = _latest_prediction_per_event(rows)
            return compute_metrics_slice(
                segment=segment,
                forecast_count=len(rows),
                unique_forecast_event_count=len(latest),
                outcome_event_count=outcome_count,
                cases=self._evaluation_cases(latest, outcomes),
                calibration_bin_count=query.calibration_bin_count,
            )

        by_phase = {
            phase.value: make_slice(
                phase.value, [row for row in predictions if row["phase"] == phase.value]
            )
            for phase in query.phases
        }
        by_lead_time = {
            segment: make_slice(segment, rows)
            for segment, rows in _lead_time_groups(predictions, query.lead_time_bins_hours).items()
        }
        return ForecastMetricsReport(
            generated_at_utc=datetime.now(UTC),
            query=query,
            overall=make_slice("OVERALL", predictions),
            by_phase=by_phase,
            by_lead_time=by_lead_time,
        )

    def counts(self) -> dict[str, int]:
        with self.connect() as connection:
            return {
                "model_runs": _count(connection, "forecast_model_runs_v2"),
                "predictions": _count(connection, "forecast_predictions_v2"),
                "outcome_versions": _count(connection, "forecast_outcome_versions_v2"),
            }

    @staticmethod
    def _require_scan_run(connection: sqlite3.Connection, run_id: int) -> None:
        if connection.execute("SELECT 1 FROM scan_runs WHERE id = ?", (run_id,)).fetchone() is None:
            raise ValueError(f"unknown scan run {run_id}")

    @staticmethod
    def _insert_prediction(
        connection: sqlite3.Connection,
        *,
        submission: ForecastSubmission,
        submission_hash: str,
        model_run_id: int,
    ) -> int:
        cursor = connection.execute(
            """
            INSERT INTO forecast_predictions_v2(
                scan_run_id, event_id, model_run_id, algorithm_version, phase,
                issued_at_utc, model_fetched_at_utc, lead_time_seconds,
                intraday_elapsed_seconds,
                intraday_remaining_seconds, station_id, observation_date,
                station_timezone, rule_day_start_utc, rule_day_end_utc,
                display_unit, precision_decimal_places, rounding_rule, rules_hash,
                point_forecast_c, observed_floor_c, scenario_count,
                observation_revision_count, observation_cutoff_at_utc,
                distribution_mass, submission_hash, created_at_utc
            ) VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                ?, ?, ?, ?, ?, ?, ?, ?, ?
            )
            """,
            (
                submission.scan_run_id,
                submission.event_id,
                model_run_id,
                submission.algorithm_version,
                submission.phase.value,
                submission.issued_at_utc.isoformat(),
                submission.model_run.fetched_at_utc.isoformat(),
                submission.lead_time_seconds,
                submission.intraday_elapsed_seconds,
                submission.intraday_remaining_seconds,
                submission.rule_day.station_id,
                submission.rule_day.observation_date.isoformat(),
                submission.rule_day.station_timezone,
                submission.rule_day.day_start_utc.isoformat(),
                submission.rule_day.day_end_utc.isoformat(),
                submission.rule_day.display_unit,
                submission.rule_day.precision_decimal_places,
                submission.rule_day.rounding_rule,
                submission.rule_day.rules_hash,
                str(submission.weighted_point_forecast_c),
                None if submission.observed_floor_c is None else str(submission.observed_floor_c),
                len(submission.scenarios),
                len(submission.observations),
                submission.observation_cutoff_at_utc.isoformat(),
                str(submission.distribution_mass),
                submission_hash,
                datetime.now(UTC).isoformat(),
            ),
        )
        return _lastrowid(cursor)

    @staticmethod
    def _insert_scenarios(
        connection: sqlite3.Connection,
        prediction_id: int,
        submission: ForecastSubmission,
    ) -> None:
        connection.executemany(
            "INSERT INTO forecast_scenarios_v2("
            "prediction_id, member_id, raw_max_c, adjusted_max_c, weight) "
            "VALUES (?, ?, ?, ?, ?)",
            [
                (
                    prediction_id,
                    item.member_id,
                    str(item.raw_max_c),
                    str(item.adjusted_max_c),
                    str(item.weight),
                )
                for item in submission.scenarios
            ],
        )

    @staticmethod
    def _insert_probabilities(
        connection: sqlite3.Connection,
        prediction_id: int,
        submission: ForecastSubmission,
        snapshots: dict[str, tuple[int, dict[str, Any]]],
    ) -> None:
        for ordinal, probability in enumerate(submission.probabilities):
            snapshot_id, snapshot = snapshots[probability.market_id]
            connection.execute(
                """
                INSERT INTO forecast_probabilities_v2(
                    prediction_id, market_id, market_snapshot_id, condition_id,
                    token_id, outcome, book_hash, outcome_label, lower_bound,
                    upper_bound, lower_inclusive, upper_inclusive, probability, ordinal
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    prediction_id,
                    probability.market_id,
                    snapshot_id,
                    snapshot.get("condition_id"),
                    snapshot.get("token_id") or snapshot.get("asset_id"),
                    snapshot.get("outcome", "YES"),
                    str(snapshot.get("book_hash") or ""),
                    probability.outcome_label,
                    None if probability.lower_bound is None else str(probability.lower_bound),
                    None if probability.upper_bound is None else str(probability.upper_bound),
                    int(probability.lower_inclusive),
                    int(probability.upper_inclusive),
                    str(probability.probability),
                    ordinal,
                ),
            )

    @staticmethod
    def _insert_observation_evidence(
        connection: sqlite3.Connection,
        prediction_id: int,
        submission: ForecastSubmission,
    ) -> None:
        for observation in submission.observations:
            if (
                observation.first_seen_at_utc > submission.observation_cutoff_at_utc
                or observation.observed_at_utc > submission.observation_cutoff_at_utc
            ):
                raise ValueError(
                    "forecast observation evidence is later than the observation cutoff"
                )
            linked = connection.execute(
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
            connection.execute(
                """
                INSERT INTO forecast_observation_evidence_v2(
                    prediction_id, station_observation_version_id, station_id,
                    observed_at_utc, revision_hash, first_seen_at_utc, source,
                    temperature_c, displayed_temperature_c, corrected
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    prediction_id,
                    None if linked is None else linked["id"],
                    observation.station_id,
                    observation.observed_at_utc.isoformat(),
                    observation.revision_hash,
                    observation.first_seen_at_utc.isoformat(),
                    observation.source,
                    str(observation.temperature_c),
                    str(observation.displayed_temperature_c),
                    int(observation.corrected),
                ),
            )

    @staticmethod
    def _record_model_run(connection: sqlite3.Connection, submission: ForecastSubmission) -> int:
        model_run = submission.model_run
        provenance: dict[str, object] = {
            "source": model_run.source,
            "model": model_run.model,
            "model_version": model_run.model_version,
            "source_run_id": model_run.source_run_id,
            "init_time_utc": model_run.init_time_utc,
            "published_at_utc": model_run.published_at_utc,
            "source_payload_hash": model_run.source_payload_hash,
        }
        # A hash or provider run ID identifies one immutable archive across many
        # five-minute prediction cycles. Only unidentifiable responses use fetch
        # time as part of the model-run key.
        if model_run.source_payload_hash is None and model_run.source_run_id is None:
            provenance["fetched_at_utc"] = model_run.fetched_at_utc
        key = _digest(provenance)
        connection.execute(
            """
            INSERT OR IGNORE INTO forecast_model_runs_v2(
                source, model, model_version, source_run_id, init_time_utc,
                published_at_utc, first_fetched_at_utc, source_uri,
                source_payload_hash, metadata_json, provenance_key, created_at_utc
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                model_run.source,
                model_run.model,
                model_run.model_version,
                model_run.source_run_id,
                None if model_run.init_time_utc is None else model_run.init_time_utc.isoformat(),
                None
                if model_run.published_at_utc is None
                else model_run.published_at_utc.isoformat(),
                model_run.fetched_at_utc.isoformat(),
                model_run.source_uri,
                model_run.source_payload_hash,
                _canonical_json(model_run.metadata),
                key,
                datetime.now(UTC).isoformat(),
            ),
        )
        row = connection.execute(
            "SELECT id FROM forecast_model_runs_v2 WHERE provenance_key = ?", (key,)
        ).fetchone()
        if row is None:
            raise RuntimeError("failed to persist forecast model run")
        return int(row["id"])

    @staticmethod
    def _market_snapshots(
        connection: sqlite3.Connection,
        *,
        run_id: int,
        event_id: str,
        market_ids: set[str],
    ) -> dict[str, tuple[int, dict[str, Any]]]:
        if not market_ids:
            raise ValueError("forecast has no market probability distribution")
        placeholders = ",".join("?" for _ in market_ids)
        rows = connection.execute(
            f"""
            SELECT id, market_id, payload_json FROM market_snapshots
            WHERE run_id = ? AND event_id = ? AND market_id IN ({placeholders})
            ORDER BY id
            """,
            (run_id, event_id, *sorted(market_ids)),
        ).fetchall()
        result: dict[str, tuple[int, dict[str, Any]]] = {}
        for row in rows:
            payload = json.loads(row["payload_json"])
            if not isinstance(payload, dict):
                raise ValueError(f"market snapshot {row['id']} payload is not an object")
            result[str(row["market_id"])] = (int(row["id"]), payload)
        missing = market_ids - result.keys()
        if missing:
            raise ValueError(
                f"forecast is missing same-run market snapshots: {', '.join(sorted(missing))}"
            )
        return result

    def _load_predictions(self, query: ForecastMetricsQuery) -> list[sqlite3.Row]:
        placeholders = ",".join("?" for _ in query.phases)
        clauses = [f"p.phase IN ({placeholders})"]
        params: list[object] = [phase.value for phase in query.phases]
        if query.source is not None:
            clauses.append("m.source = ?")
            params.append(query.source)
        if query.model is not None:
            clauses.append("m.model = ?")
            params.append(query.model)
        if query.algorithm_version is not None:
            clauses.append("p.algorithm_version = ?")
            params.append(query.algorithm_version)
        if query.as_of_utc is not None:
            clauses.append("p.issued_at_utc <= ?")
            params.append(query.as_of_utc.isoformat())
        with self.connect() as connection:
            return connection.execute(
                "SELECT p.*, m.source, m.model FROM forecast_predictions_v2 p "
                "JOIN forecast_model_runs_v2 m ON m.id = p.model_run_id WHERE "
                + " AND ".join(clauses)
                + " ORDER BY p.issued_at_utc, p.id",
                params,
            ).fetchall()

    def _load_latest_outcomes(self, *, as_of: datetime | None) -> dict[str, sqlite3.Row]:
        with self.connect() as connection:
            rows = connection.execute(
                "SELECT * FROM forecast_outcome_versions_v2 "
                + ("WHERE recorded_at_utc <= ? " if as_of is not None else "")
                + "ORDER BY recorded_at_utc, id",
                () if as_of is None else (as_of.isoformat(),),
            ).fetchall()
        latest: dict[str, sqlite3.Row] = {}
        for row in rows:
            latest[str(row["event_id"])] = row
        return latest

    def _evaluation_cases(
        self,
        predictions: dict[str, sqlite3.Row],
        outcomes: dict[str, sqlite3.Row],
    ) -> list[ForecastEvaluationCase]:
        cases: list[ForecastEvaluationCase] = []
        with self.connect() as connection:
            for event_id, prediction in predictions.items():
                outcome = outcomes.get(event_id)
                if outcome is None:
                    continue
                rows = connection.execute(
                    "SELECT market_id, probability FROM forecast_probabilities_v2 "
                    "WHERE prediction_id = ? ORDER BY ordinal",
                    (prediction["id"],),
                ).fetchall()
                cases.append(
                    ForecastEvaluationCase(
                        prediction_id=int(prediction["id"]),
                        event_id=event_id,
                        phase=ForecastPhase(prediction["phase"]),
                        issued_at_utc=datetime.fromisoformat(prediction["issued_at_utc"]),
                        point_forecast_c=Decimal(prediction["point_forecast_c"]),
                        probabilities={
                            str(row["market_id"]): Decimal(row["probability"]) for row in rows
                        },
                        distribution_mass=Decimal(prediction["distribution_mass"]),
                        actual_max_c=Decimal(outcome["actual_max_c"]),
                        winning_market_id=str(outcome["winning_market_id"]),
                    )
                )
        return cases


def _latest_prediction_per_event(rows: list[sqlite3.Row]) -> dict[str, sqlite3.Row]:
    latest: dict[str, sqlite3.Row] = {}
    for row in rows:
        latest[str(row["event_id"])] = row
    return latest


def _lead_time_groups(
    rows: list[sqlite3.Row], edges_hours: tuple[int, ...]
) -> dict[str, list[sqlite3.Row]]:
    result: dict[str, list[sqlite3.Row]] = {}
    if not edges_hours:
        return result
    for lower, upper in zip(edges_hours, edges_hours[1:], strict=False):
        result[f"LEAD_{lower:03d}_{upper:03d}H"] = [
            row
            for row in rows
            if row["phase"] == ForecastPhase.LEAD_TIME.value
            and lower * 3600 <= int(row["lead_time_seconds"]) < upper * 3600
        ]
    final = edges_hours[-1]
    result[f"LEAD_{final:03d}H_PLUS"] = [
        row
        for row in rows
        if row["phase"] == ForecastPhase.LEAD_TIME.value
        and int(row["lead_time_seconds"]) >= final * 3600
    ]
    result[ForecastPhase.INTRADAY.value] = [
        row for row in rows if row["phase"] == ForecastPhase.INTRADAY.value
    ]
    return result


def _canonical_json(value: object) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=_json_default,
    )


def _json_default(value: object) -> str:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    raise TypeError(f"not JSON serializable: {type(value).__name__}")


def _digest(value: object) -> str:
    return hashlib.sha256(_canonical_json(value).encode()).hexdigest()


def _count(connection: sqlite3.Connection, table: str) -> int:
    allowed = {
        "forecast_model_runs_v2",
        "forecast_predictions_v2",
        "forecast_outcome_versions_v2",
    }
    if table not in allowed:
        raise ValueError(f"unsupported count table {table}")
    return int(connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])


def _lastrowid(cursor: sqlite3.Cursor) -> int:
    if cursor.lastrowid is None:
        raise RuntimeError("SQLite did not return a row id")
    return int(cursor.lastrowid)
