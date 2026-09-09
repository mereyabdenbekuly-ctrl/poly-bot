from __future__ import annotations

import hashlib
import json
import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

from polybot.forecast_models import (
    FORECAST_SCHEMA_VERSION,
    ForecastAlgorithmAttemptStatus,
    ForecastEvaluationCase,
    ForecastEventEligibility,
    ForecastMetricsQuery,
    ForecastMetricsReport,
    ForecastMetricsSlice,
    ForecastPhase,
    ForecastSubmission,
    RealizedForecastOutcome,
    compute_metrics_slice,
)
from polybot.forecast_uncertainty import build_phase_quality

_EVALUATION_REGISTRY_MIGRATION_VERSION = 5


class ForecastStore:
    """Append-only forecast-engine-v2 storage beside immutable v1 tables."""

    def __init__(self, path: Path, *, read_only: bool = False) -> None:
        self.path = path.expanduser().resolve()
        self.read_only = read_only
        if not read_only:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            self._initialize()

    def connect(self) -> sqlite3.Connection:
        if self.read_only:
            connection = sqlite3.connect(f"file:{self.path}?mode=ro", uri=True, timeout=30)
        else:
            connection = sqlite3.connect(self.path, timeout=30)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        if not self.read_only:
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
                    metadata_json TEXT NOT NULL DEFAULT '{}',
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
                    canonical_first_seen_at_utc TEXT NOT NULL,
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

                CREATE TABLE IF NOT EXISTS forecast_source_status_v2 (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    source TEXT NOT NULL,
                    state TEXT NOT NULL,
                    checked_at_utc TEXT NOT NULL,
                    payload_json TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS forecast_evaluation_events_v2 (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    cohort_version TEXT NOT NULL,
                    scan_run_id INTEGER NOT NULL REFERENCES scan_runs(id),
                    event_id TEXT NOT NULL,
                    considered_at_utc TEXT NOT NULL,
                    event_title TEXT NOT NULL,
                    event_slug TEXT,
                    market_count INTEGER NOT NULL,
                    rules_hash TEXT NOT NULL,
                    rule_parser TEXT NOT NULL,
                    station_id TEXT,
                    observation_date TEXT,
                    station_timezone TEXT,
                    rule_day_start_utc TEXT,
                    rule_day_end_utc TEXT,
                    phase TEXT,
                    lead_time_seconds INTEGER,
                    intraday_elapsed_seconds INTEGER,
                    display_unit TEXT NOT NULL,
                    precision_decimal_places INTEGER,
                    rounding_rule TEXT,
                    eligible INTEGER NOT NULL,
                    stage TEXT NOT NULL,
                    block_reasons_json TEXT NOT NULL,
                    warning_reasons_json TEXT NOT NULL,
                    created_at_utc TEXT NOT NULL,
                    UNIQUE(cohort_version, scan_run_id, event_id)
                );

                CREATE TABLE IF NOT EXISTS forecast_evaluation_algorithms_v2 (
                    evaluation_event_id INTEGER NOT NULL
                        REFERENCES forecast_evaluation_events_v2(id),
                    source TEXT NOT NULL,
                    model TEXT NOT NULL,
                    algorithm_version TEXT NOT NULL,
                    expected INTEGER NOT NULL,
                    status TEXT NOT NULL,
                    prediction_id INTEGER REFERENCES forecast_predictions_v2(id),
                    reason_codes_json TEXT NOT NULL,
                    PRIMARY KEY(evaluation_event_id, algorithm_version)
                );

                CREATE INDEX IF NOT EXISTS forecast_predictions_event_idx
                    ON forecast_predictions_v2(event_id, phase, issued_at_utc);
                CREATE INDEX IF NOT EXISTS forecast_predictions_model_idx
                    ON forecast_predictions_v2(model_run_id, algorithm_version);
                CREATE INDEX IF NOT EXISTS forecast_probabilities_market_idx
                    ON forecast_probabilities_v2(market_id, market_snapshot_id);
                CREATE INDEX IF NOT EXISTS forecast_outcomes_event_idx
                    ON forecast_outcome_versions_v2(event_id, recorded_at_utc, id);
                CREATE INDEX IF NOT EXISTS forecast_source_status_idx
                    ON forecast_source_status_v2(source, checked_at_utc, id);
                CREATE INDEX IF NOT EXISTS forecast_evaluation_event_idx
                    ON forecast_evaluation_events_v2(
                        cohort_version, event_id, phase, considered_at_utc
                    );
                CREATE INDEX IF NOT EXISTS forecast_evaluation_algorithm_idx
                    ON forecast_evaluation_algorithms_v2(
                        algorithm_version, source, model, expected
                    );
                """
            )
            connection.execute(
                "INSERT OR IGNORE INTO forecast_engine_schema_versions(version, applied_at_utc) "
                "VALUES (?, ?)",
                (FORECAST_SCHEMA_VERSION, datetime.now(UTC).isoformat()),
            )
            columns = {
                str(row[1])
                for row in connection.execute(
                    "PRAGMA table_info(forecast_predictions_v2)"
                ).fetchall()
            }
            if "metadata_json" not in columns:
                connection.execute(
                    "ALTER TABLE forecast_predictions_v2 "
                    "ADD COLUMN metadata_json TEXT NOT NULL DEFAULT '{}'"
                )
            evidence_columns = {
                str(row[1])
                for row in connection.execute(
                    "PRAGMA table_info(forecast_observation_evidence_v2)"
                ).fetchall()
            }
            if "canonical_first_seen_at_utc" not in evidence_columns:
                connection.execute(
                    "ALTER TABLE forecast_observation_evidence_v2 "
                    "ADD COLUMN canonical_first_seen_at_utc TEXT"
                )
            connection.execute(
                "UPDATE forecast_model_runs_v2 SET "
                "source='open-meteo-ecmwf', "
                "model='ECMWF IFS ENS 0.25° daily max via Open-Meteo' "
                "WHERE source='ecmwf' "
                "AND json_extract(metadata_json, '$.transport_provider')='open-meteo'"
            )
            migration_done = connection.execute(
                "SELECT 1 FROM forecast_engine_schema_versions WHERE version = ?",
                (_EVALUATION_REGISTRY_MIGRATION_VERSION,),
            ).fetchone()
            if migration_done is None:
                # A short-lived pre-release migration wrote inferred legacy
                # rows into the production cohort. Move every explicit
                # BACKFILL_UNKNOWN row out before the idempotent repair below.
                connection.execute(
                    "UPDATE forecast_evaluation_events_v2 SET "
                    "cohort_version='legacy-backfill-v1', eligible=0, "
                    "block_reasons_json='[\"BACKFILL_ELIGIBILITY_UNKNOWN\"]' "
                    "WHERE stage='BACKFILL_UNKNOWN'"
                )
                connection.execute(
                    "UPDATE forecast_observation_evidence_v2 AS f SET "
                    "canonical_first_seen_at_utc=COALESCE(("
                    "SELECT s.first_seen_at_utc FROM station_observation_versions s "
                    "WHERE s.id=f.station_observation_version_id"
                    "), f.first_seen_at_utc) "
                    "WHERE canonical_first_seen_at_utc IS NULL"
                )
                self._backfill_evaluation_registry(connection)
                connection.execute(
                    "INSERT INTO forecast_engine_schema_versions(version, applied_at_utc) "
                    "VALUES (?, ?)",
                    (
                        _EVALUATION_REGISTRY_MIGRATION_VERSION,
                        datetime.now(UTC).isoformat(),
                    ),
                )

    @staticmethod
    def _backfill_evaluation_registry(connection: sqlite3.Connection) -> None:
        """Register legacy evidence without inventing missing eligibility facts."""

        prediction_rows = connection.execute(
            """
            SELECT p.*, m.source, m.model
            FROM forecast_predictions_v2 p
            JOIN forecast_model_runs_v2 m ON m.id = p.model_run_id
            ORDER BY p.scan_run_id, p.event_id, p.id
            """
        ).fetchall()
        evaluation_ids: dict[tuple[int, str], int] = {}
        production_keys: set[tuple[int, str]] = set()
        for row in prediction_rows:
            key = (int(row["scan_run_id"]), str(row["event_id"]))
            if key in production_keys:
                continue
            evaluation_id = evaluation_ids.get(key)
            existing_any: sqlite3.Row | None = None
            if evaluation_id is None:
                existing_any = connection.execute(
                    "SELECT id, cohort_version FROM forecast_evaluation_events_v2 "
                    "WHERE scan_run_id=? AND event_id=? "
                    "ORDER BY CASE WHEN cohort_version='weather-evaluation-v1' "
                    "THEN 0 ELSE 1 END, id LIMIT 1",
                    key,
                ).fetchone()
                if existing_any is not None:
                    # Never create a duplicate legacy cohort for a scan that
                    # already has a complete production registration. Existing
                    # legacy rows are repaired in place; production rows remain
                    # immutable and authoritative.
                    evaluation_id = int(existing_any["id"])
                    evaluation_ids[key] = evaluation_id
                    if str(existing_any["cohort_version"]) == "weather-evaluation-v1":
                        production_keys.add(key)
                        continue
                else:
                    evaluation_id = None
            if evaluation_id is None:
                connection.execute(
                    """
                    INSERT OR IGNORE INTO forecast_evaluation_events_v2(
                        cohort_version, scan_run_id, event_id, considered_at_utc,
                        event_title, event_slug, market_count, rules_hash, rule_parser,
                        station_id, observation_date, station_timezone,
                        rule_day_start_utc, rule_day_end_utc, phase, lead_time_seconds,
                        intraday_elapsed_seconds, display_unit, precision_decimal_places,
                        rounding_rule, eligible, stage, block_reasons_json,
                        warning_reasons_json, created_at_utc
                    ) VALUES (
                        'legacy-backfill-v1', ?, ?, ?, ?, NULL, ?, ?,
                        'backfill-from-prediction', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                        0, 'BACKFILL_UNKNOWN',
                        '["BACKFILL_ELIGIBILITY_UNKNOWN"]', '[]', ?
                    )
                    """,
                    (
                        key[0],
                        key[1],
                        row["issued_at_utc"],
                        f"Recovered forecast event {key[1]}",
                        int(
                            connection.execute(
                                "SELECT COUNT(*) FROM forecast_probabilities_v2 "
                                "WHERE prediction_id = ?",
                                (row["id"],),
                            ).fetchone()[0]
                        ),
                        row["rules_hash"],
                        row["station_id"],
                        row["observation_date"],
                        row["station_timezone"],
                        row["rule_day_start_utc"],
                        row["rule_day_end_utc"],
                        row["phase"],
                        row["lead_time_seconds"],
                        row["intraday_elapsed_seconds"],
                        row["display_unit"],
                        row["precision_decimal_places"],
                        row["rounding_rule"],
                        datetime.now(UTC).isoformat(),
                    ),
                )
                existing = connection.execute(
                    "SELECT id FROM forecast_evaluation_events_v2 "
                    "WHERE cohort_version='legacy-backfill-v1' "
                    "AND scan_run_id=? AND event_id=?",
                    key,
                ).fetchone()
                if existing is None:
                    raise RuntimeError("failed to backfill forecast evaluation event")
                evaluation_id = int(existing["id"])
                evaluation_ids[key] = evaluation_id
            connection.execute(
                """
                INSERT OR IGNORE INTO forecast_evaluation_algorithms_v2(
                    evaluation_event_id, source, model, algorithm_version,
                    expected, status, prediction_id, reason_codes_json
                ) VALUES (?, ?, ?, ?, 1, 'PREDICTED', ?, '[]')
                """,
                (
                    evaluation_id,
                    row["source"],
                    row["model"],
                    row["algorithm_version"],
                    row["id"],
                ),
            )

        decision_rows = connection.execute(
            """
            SELECT d.run_id, d.event_id, MIN(d.created_at) AS considered_at_utc,
                   COUNT(DISTINCT d.market_id) AS market_count
            FROM decisions d
            GROUP BY d.run_id, d.event_id
            """
        ).fetchall()
        for row in decision_rows:
            snapshot = connection.execute(
                "SELECT payload_json FROM market_snapshots "
                "WHERE run_id=? AND event_id=? ORDER BY id LIMIT 1",
                (row["run_id"], row["event_id"]),
            ).fetchone()
            payload = {} if snapshot is None else _json_object(snapshot["payload_json"])
            connection.execute(
                """
                INSERT OR IGNORE INTO forecast_evaluation_events_v2(
                    cohort_version, scan_run_id, event_id, considered_at_utc,
                    event_title, event_slug, market_count, rules_hash, rule_parser,
                    display_unit, eligible, stage, block_reasons_json,
                    warning_reasons_json, created_at_utc
                ) VALUES (
                    'legacy-backfill-v1', ?, ?, ?, ?, ?, ?, 'backfill-unknown',
                    'backfill-from-decisions', 'unknown', 0, 'BACKFILL_UNKNOWN',
                    '["BACKFILL_ELIGIBILITY_UNKNOWN"]', '[]', ?
                )
                """,
                (
                    row["run_id"],
                    row["event_id"],
                    row["considered_at_utc"],
                    str(payload.get("event_title") or f"Recovered event {row['event_id']}"),
                    payload.get("event_slug"),
                    row["market_count"],
                    datetime.now(UTC).isoformat(),
                ),
            )

    def register_evaluation_event(self, item: ForecastEventEligibility) -> int:
        """Append one immutable event opportunity and its expected algorithms."""

        with self.transaction() as connection:
            self._require_scan_run(connection, item.scan_run_id)
            existing = connection.execute(
                "SELECT id, rule_parser FROM forecast_evaluation_events_v2 "
                "WHERE cohort_version=? AND scan_run_id=? AND event_id=?",
                (item.cohort_version, item.scan_run_id, item.event_id),
            ).fetchone()
            if existing is not None:
                evaluation_id = int(existing["id"])
                if existing["rule_parser"] != "forecast-submission":
                    return evaluation_id
                # A direct ``record()`` call may have created a provisional
                # row so that metrics remain usable. Replace that provisional
                # metadata with the scanner's complete event verdict in the
                # same scan; subsequent calls remain immutable.
                connection.execute(
                    """
                    UPDATE forecast_evaluation_events_v2 SET
                        considered_at_utc=?, event_title=?, event_slug=?, market_count=?,
                        rules_hash=?, rule_parser=?, station_id=?, observation_date=?,
                        station_timezone=?, rule_day_start_utc=?, rule_day_end_utc=?,
                        phase=?, lead_time_seconds=?, intraday_elapsed_seconds=?,
                        display_unit=?, precision_decimal_places=?, rounding_rule=?,
                        eligible=?, stage=?, block_reasons_json=?, warning_reasons_json=?
                    WHERE id=?
                    """,
                    (
                        item.considered_at_utc.astimezone(UTC).isoformat(),
                        item.event_title,
                        item.event_slug,
                        item.market_count,
                        item.rules_hash,
                        item.rule_parser,
                        item.station_id,
                        None
                        if item.observation_date is None
                        else item.observation_date.isoformat(),
                        item.station_timezone,
                        None
                        if item.rule_day_start_utc is None
                        else item.rule_day_start_utc.astimezone(UTC).isoformat(),
                        None
                        if item.rule_day_end_utc is None
                        else item.rule_day_end_utc.astimezone(UTC).isoformat(),
                        None if item.phase is None else item.phase.value,
                        item.lead_time_seconds,
                        item.intraday_elapsed_seconds,
                        item.display_unit,
                        item.precision_decimal_places,
                        item.rounding_rule,
                        int(item.eligible),
                        item.stage.value,
                        _canonical_json(item.block_reasons),
                        _canonical_json(item.warning_reasons),
                        evaluation_id,
                    ),
                )
                connection.execute(
                    "DELETE FROM forecast_evaluation_algorithms_v2 WHERE evaluation_event_id=?",
                    (evaluation_id,),
                )
            else:
                cursor = connection.execute(
                    """
                INSERT INTO forecast_evaluation_events_v2(
                    cohort_version, scan_run_id, event_id, considered_at_utc,
                    event_title, event_slug, market_count, rules_hash, rule_parser,
                    station_id, observation_date, station_timezone,
                    rule_day_start_utc, rule_day_end_utc, phase, lead_time_seconds,
                    intraday_elapsed_seconds, display_unit, precision_decimal_places,
                    rounding_rule, eligible, stage, block_reasons_json,
                    warning_reasons_json, created_at_utc
                ) VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                    ?, ?, ?, ?, ?
                )
                """,
                    (
                        item.cohort_version,
                        item.scan_run_id,
                        item.event_id,
                        item.considered_at_utc.astimezone(UTC).isoformat(),
                        item.event_title,
                        item.event_slug,
                        item.market_count,
                        item.rules_hash,
                        item.rule_parser,
                        item.station_id,
                        None
                        if item.observation_date is None
                        else item.observation_date.isoformat(),
                        item.station_timezone,
                        None
                        if item.rule_day_start_utc is None
                        else item.rule_day_start_utc.astimezone(UTC).isoformat(),
                        None
                        if item.rule_day_end_utc is None
                        else item.rule_day_end_utc.astimezone(UTC).isoformat(),
                        None if item.phase is None else item.phase.value,
                        item.lead_time_seconds,
                        item.intraday_elapsed_seconds,
                        item.display_unit,
                        item.precision_decimal_places,
                        item.rounding_rule,
                        int(item.eligible),
                        item.stage.value,
                        _canonical_json(item.block_reasons),
                        _canonical_json(item.warning_reasons),
                        datetime.now(UTC).isoformat(),
                    ),
                )
                evaluation_id = _lastrowid(cursor)
            connection.executemany(
                """
                INSERT INTO forecast_evaluation_algorithms_v2(
                    evaluation_event_id, source, model, algorithm_version,
                    expected, status, prediction_id, reason_codes_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        evaluation_id,
                        algorithm.source,
                        algorithm.model,
                        algorithm.algorithm_version,
                        int(algorithm.expected),
                        algorithm.status.value,
                        algorithm.prediction_id,
                        _canonical_json(algorithm.reason_codes),
                    )
                    for algorithm in item.algorithms
                ],
            )
            return evaluation_id

    def update_algorithm_attempt(
        self,
        *,
        scan_run_id: int,
        event_id: str,
        algorithm_version: str,
        status: ForecastAlgorithmAttemptStatus,
        prediction_id: int | None = None,
        reason_codes: list[str] | None = None,
    ) -> None:
        """Record a prediction/failure for an expected algorithm in the cohort."""

        with self.transaction() as connection:
            row = connection.execute(
                "SELECT id FROM forecast_evaluation_events_v2 "
                "WHERE cohort_version='weather-evaluation-v1' AND scan_run_id=? AND event_id=?",
                (scan_run_id, event_id),
            ).fetchone()
            if row is None:
                return
            connection.execute(
                "UPDATE forecast_evaluation_algorithms_v2 SET status=?, prediction_id=?, "
                "reason_codes_json=? WHERE evaluation_event_id=? AND algorithm_version=?",
                (
                    status.value,
                    prediction_id,
                    _canonical_json(reason_codes or []),
                    row["id"],
                    algorithm_version,
                ),
            )

    def record(
        self,
        submission: ForecastSubmission,
        *,
        min_interval_seconds: int = 0,
        probability_delta: Decimal = Decimal(0),
    ) -> int:
        """Persist a fully linked forecast; identical submissions are idempotent."""

        submission_hash = _digest(submission.model_dump(mode="json"))
        with self.transaction() as connection:
            existing = connection.execute(
                "SELECT id FROM forecast_predictions_v2 WHERE submission_hash = ?",
                (submission_hash,),
            ).fetchone()
            if existing is not None:
                prediction_id = int(existing["id"])
                self._ensure_prediction_registry(connection, submission, prediction_id)
                return prediction_id
            self._require_scan_run(connection, submission.scan_run_id)
            previous = self._latest_prediction(
                connection,
                event_id=submission.event_id,
                algorithm_version=submission.algorithm_version,
            )
            if previous is not None and not self._materially_changed(
                connection,
                previous=previous,
                submission=submission,
                min_interval_seconds=max(0, min_interval_seconds),
                probability_delta=max(Decimal(0), probability_delta),
            ):
                prediction_id = int(previous["id"])
                self._ensure_prediction_registry(connection, submission, prediction_id)
                return prediction_id
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
            self._ensure_prediction_registry(connection, submission, prediction_id)
            return prediction_id

    @staticmethod
    def _ensure_prediction_registry(
        connection: sqlite3.Connection,
        submission: ForecastSubmission,
        prediction_id: int,
    ) -> None:
        """Keep direct ForecastStore users in the same explicit cohort.

        Scanner normally registers the full event verdict after all provider
        attempts. This fallback is intentionally conservative and derives only
        facts guaranteed by a validated submission, so tests/importers cannot
        silently create predictions that metrics can never see.
        """

        existing = connection.execute(
            "SELECT id FROM forecast_evaluation_events_v2 "
            "WHERE cohort_version='weather-evaluation-v1' AND scan_run_id=? AND event_id=?",
            (submission.scan_run_id, submission.event_id),
        ).fetchone()
        if existing is None:
            rule_day = submission.rule_day
            cursor = connection.execute(
                """
                INSERT INTO forecast_evaluation_events_v2(
                    cohort_version, scan_run_id, event_id, considered_at_utc,
                    event_title, event_slug, market_count, rules_hash, rule_parser,
                    station_id, observation_date, station_timezone,
                    rule_day_start_utc, rule_day_end_utc, phase, lead_time_seconds,
                    intraday_elapsed_seconds, display_unit, precision_decimal_places,
                    rounding_rule, eligible, stage, block_reasons_json,
                    warning_reasons_json, created_at_utc
                ) VALUES (
                    'weather-evaluation-v1', ?, ?, ?, ?, NULL, ?, ?,
                    'forecast-submission', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                    0, 'BACKFILL_UNKNOWN',
                    '["REGISTRY_PENDING"]', '[]', ?
                )
                """,
                (
                    submission.scan_run_id,
                    submission.event_id,
                    submission.issued_at_utc.isoformat(),
                    f"Forecast event {submission.event_id}",
                    len(submission.probabilities),
                    rule_day.rules_hash,
                    rule_day.station_id,
                    rule_day.observation_date.isoformat(),
                    rule_day.station_timezone,
                    rule_day.day_start_utc.isoformat(),
                    rule_day.day_end_utc.isoformat(),
                    submission.phase.value,
                    submission.lead_time_seconds,
                    submission.intraday_elapsed_seconds,
                    rule_day.display_unit,
                    rule_day.precision_decimal_places,
                    rule_day.rounding_rule,
                    datetime.now(UTC).isoformat(),
                ),
            )
            evaluation_id = _lastrowid(cursor)
        else:
            evaluation_id = int(existing["id"])
        connection.execute(
            """
            INSERT INTO forecast_evaluation_algorithms_v2(
                evaluation_event_id, source, model, algorithm_version,
                expected, status, prediction_id, reason_codes_json
            ) VALUES (?, ?, ?, ?, 1, ?, ?, '[]')
            ON CONFLICT(evaluation_event_id, algorithm_version) DO UPDATE SET
                source=excluded.source,
                model=excluded.model,
                expected=excluded.expected,
                status=excluded.status,
                prediction_id=excluded.prediction_id,
                reason_codes_json=excluded.reason_codes_json
            """,
            (
                evaluation_id,
                submission.model_run.source,
                submission.model_run.model,
                submission.algorithm_version,
                ForecastAlgorithmAttemptStatus.PREDICTED.value,
                prediction_id,
            ),
        )

    @staticmethod
    def _latest_prediction(
        connection: sqlite3.Connection,
        *,
        event_id: str,
        algorithm_version: str,
    ) -> sqlite3.Row | None:
        return connection.execute(
            "SELECT * FROM forecast_predictions_v2 WHERE event_id = ? "
            "AND algorithm_version = ? ORDER BY issued_at_utc DESC, id DESC LIMIT 1",
            (event_id, algorithm_version),
        ).fetchone()

    @staticmethod
    def _materially_changed(
        connection: sqlite3.Connection,
        *,
        previous: sqlite3.Row,
        submission: ForecastSubmission,
        min_interval_seconds: int,
        probability_delta: Decimal,
    ) -> bool:
        previous_issued = datetime.fromisoformat(str(previous["issued_at_utc"]))
        elapsed = (submission.issued_at_utc - previous_issued).total_seconds()
        if elapsed >= min_interval_seconds:
            return True
        if str(previous["phase"]) != submission.phase.value:
            return True
        prior_floor = (
            None
            if previous["observed_floor_c"] is None
            else Decimal(str(previous["observed_floor_c"]))
        )
        if prior_floor != submission.observed_floor_c:
            return True
        rows = connection.execute(
            "SELECT market_id, probability FROM forecast_probabilities_v2 WHERE prediction_id = ?",
            (previous["id"],),
        ).fetchall()
        prior = {str(row["market_id"]): Decimal(str(row["probability"])) for row in rows}
        current = {item.market_id: item.probability for item in submission.probabilities}
        if prior.keys() != current.keys():
            return True
        prior_top = max(prior, key=prior.__getitem__)
        current_top = max(current, key=current.__getitem__)
        if prior_top != current_top:
            return True
        return any(abs(current[key] - prior[key]) >= probability_delta for key in current)

    def record_outcome(self, outcome: RealizedForecastOutcome) -> int:
        """Append one outcome revision; never rewrite an earlier revision."""

        outcome_hash = _digest(_outcome_semantic_payload(outcome))
        with self.transaction() as connection:
            registry_rows = connection.execute(
                "SELECT DISTINCT station_id, observation_date "
                "FROM forecast_evaluation_events_v2 "
                "WHERE event_id=? AND eligible=1",
                (outcome.event_id,),
            ).fetchall()
            expected_identity = (
                outcome.station_id,
                outcome.observation_date.isoformat(),
            )
            if registry_rows:
                registry_identities = {
                    (str(row["station_id"]), str(row["observation_date"])) for row in registry_rows
                }
                if registry_identities != {expected_identity}:
                    raise ValueError(
                        "outcome rule-day identity mismatch: "
                        f"registry={sorted(registry_identities)}, "
                        f"outcome={expected_identity}"
                    )
            prediction_rows = connection.execute(
                "SELECT DISTINCT station_id, observation_date "
                "FROM forecast_predictions_v2 WHERE event_id = ?",
                (outcome.event_id,),
            ).fetchall()
            if prediction_rows:
                identities = {
                    (str(row["station_id"]), str(row["observation_date"]))
                    for row in prediction_rows
                }
                if identities != {expected_identity}:
                    raise ValueError(
                        f"outcome rule-day identity mismatch: forecasts={sorted(identities)}, "
                        f"outcome={expected_identity}"
                    )
            if registry_rows or prediction_rows:
                winner_known = connection.execute(
                    """
                    SELECT 1 FROM forecast_probabilities_v2 fp
                    JOIN forecast_predictions_v2 p ON p.id = fp.prediction_id
                    WHERE p.event_id = ? AND fp.market_id = ?
                    UNION ALL
                    SELECT 1 FROM market_snapshots
                    WHERE event_id = ? AND market_id = ?
                    LIMIT 1
                    """,
                    (
                        outcome.event_id,
                        outcome.winning_market_id,
                        outcome.event_id,
                        outcome.winning_market_id,
                    ),
                ).fetchone()
                if winner_known is None:
                    raise ValueError("outcome winner is absent from archived event markets")
            existing = connection.execute(
                "SELECT * FROM forecast_outcome_versions_v2 "
                "WHERE event_id = ? AND source_revision = ?",
                (outcome.event_id, outcome.source_revision),
            ).fetchone()
            if existing is not None:
                stored = _outcome_from_row(existing)
                if _outcome_semantic_payload(stored) != _outcome_semantic_payload(outcome):
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
        universe = self._load_evaluation_universe(query)
        outcomes = self._load_latest_outcomes(as_of=query.as_of_utc)
        as_of = query.as_of_utc or datetime.now(UTC)

        def make_slice(
            segment: str,
            prediction_rows: list[sqlite3.Row],
            universe_rows: list[sqlite3.Row],
        ) -> ForecastMetricsSlice:
            eligible_ids = {str(row["event_id"]) for row in universe_rows}
            ended_ids = {
                str(row["event_id"])
                for row in universe_rows
                if row["rule_day_end_utc"] is not None
                and datetime.fromisoformat(str(row["rule_day_end_utc"])) <= as_of
            }
            eligible_outcomes = {
                event_id: outcome
                for event_id, outcome in outcomes.items()
                if event_id in eligible_ids
            }
            eligible_predictions = [
                row for row in prediction_rows if str(row["event_id"]) in eligible_ids
            ]
            latest = _latest_prediction_per_event(eligible_predictions)
            metrics = compute_metrics_slice(
                segment=segment,
                forecast_count=len(eligible_predictions),
                unique_forecast_event_count=len(latest),
                outcome_event_count=len(eligible_outcomes),
                cases=self._evaluation_cases(latest, eligible_outcomes),
                calibration_bin_count=query.calibration_bin_count,
            )
            forecast_coverage = (
                Decimal(len(latest)) / Decimal(len(eligible_ids)) if eligible_ids else Decimal(0)
            )
            resolved_ended = ended_ids & outcomes.keys()
            outcome_coverage = (
                Decimal(len(resolved_ended)) / Decimal(len(ended_ids)) if ended_ids else Decimal(0)
            )
            return metrics.model_copy(
                update={
                    "eligible_event_count": len(eligible_ids),
                    "eligible_ended_event_count": len(ended_ids),
                    "forecast_coverage": forecast_coverage,
                    "outcome_coverage": outcome_coverage,
                }
            )

        by_phase = {
            phase.value: make_slice(
                phase.value,
                [row for row in predictions if row["phase"] == phase.value],
                [row for row in universe if row["phase"] == phase.value],
            )
            for phase in query.phases
        }
        prediction_groups = _lead_time_groups(predictions, query.lead_time_bins_hours)
        universe_groups = _lead_time_groups(universe, query.lead_time_bins_hours)
        by_lead_time = {
            segment: make_slice(
                segment,
                prediction_groups.get(segment, []),
                universe_groups.get(segment, []),
            )
            for segment in universe_groups.keys() | prediction_groups.keys()
        }
        return ForecastMetricsReport(
            generated_at_utc=datetime.now(UTC),
            query=query,
            overall=make_slice("OVERALL", predictions, universe),
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

    def record_source_status(
        self,
        *,
        source: str,
        state: str,
        checked_at_utc: datetime,
        payload: dict[str, object],
    ) -> int:
        if checked_at_utc.tzinfo is None:
            raise ValueError("source status timestamp must be timezone-aware")
        with self.connect() as connection:
            cursor = connection.execute(
                "INSERT INTO forecast_source_status_v2("
                "source, state, checked_at_utc, payload_json) VALUES (?, ?, ?, ?)",
                (
                    source,
                    state,
                    checked_at_utc.astimezone(UTC).isoformat(),
                    _canonical_json(payload),
                ),
            )
            return _lastrowid(cursor)

    def latest_source_statuses(self) -> list[dict[str, object]]:
        with self.connect() as connection:
            rows = connection.execute(
                """
                SELECT s.* FROM forecast_source_status_v2 s
                JOIN (
                    SELECT source, MAX(id) AS id FROM forecast_source_status_v2
                    GROUP BY source
                ) latest ON latest.id = s.id
                ORDER BY s.source
                """
            ).fetchall()
        return [
            {
                "source": str(row["source"]),
                "state": str(row["state"]),
                "checked_at_utc": str(row["checked_at_utc"]),
                "payload": _json_object(row["payload_json"]),
            }
            for row in rows
        ]

    def pending_outcome_event_ids(self, *, as_of_utc: datetime | None = None) -> list[str]:
        """Eligible ended events that still need an immutable weather outcome."""

        as_of = (as_of_utc or datetime.now(UTC)).isoformat()
        with self.connect() as connection:
            rows = connection.execute(
                """
                SELECT DISTINCT e.event_id
                FROM forecast_evaluation_events_v2 e
                LEFT JOIN forecast_outcome_versions_v2 o ON o.event_id = e.event_id
                WHERE e.eligible = 1 AND e.rule_day_end_utc <= ?
                  AND o.event_id IS NULL
                ORDER BY e.event_id
                """,
                (as_of,),
            ).fetchall()
        return [str(row["event_id"]) for row in rows]

    def outcome_refresh_event_ids(
        self, *, as_of_utc: datetime | None = None, correction_window_hours: int = 336
    ) -> list[str]:
        """Events whose final source can still publish a correction revision."""

        as_of = as_of_utc or datetime.now(UTC)
        lower = as_of.timestamp() - max(1, correction_window_hours) * 3600
        with self.connect() as connection:
            rows = connection.execute(
                "SELECT e.event_id, MAX(e.rule_day_end_utc) AS rule_day_end_utc, "
                "COUNT(o.id) AS outcome_count "
                "FROM forecast_evaluation_events_v2 e "
                "LEFT JOIN forecast_outcome_versions_v2 o ON o.event_id=e.event_id "
                "WHERE e.eligible=1 AND e.rule_day_end_utc <= ? "
                "GROUP BY e.event_id ORDER BY e.event_id",
                (as_of.isoformat(),),
            ).fetchall()
        return [
            str(row["event_id"])
            for row in rows
            if int(row["outcome_count"]) == 0
            or datetime.fromisoformat(str(row["rule_day_end_utc"])).timestamp() >= lower
        ]

    def dashboard_summary(self, *, event_limit: int = 20) -> dict[str, object]:
        """Return a compact, read-only comparison view for the local dashboard."""

        with self.connect() as connection:
            available_event_count = int(
                connection.execute(
                    "SELECT COUNT(DISTINCT event_id) FROM forecast_predictions_v2"
                ).fetchone()[0]
            )
            considered_event_count = int(
                connection.execute(
                    "SELECT COUNT(DISTINCT event_id) "
                    "FROM forecast_evaluation_events_v2 "
                    "WHERE cohort_version='weather-evaluation-v1'"
                ).fetchone()[0]
            )
            eligible_event_count = int(
                connection.execute(
                    "SELECT COUNT(DISTINCT event_id) "
                    "FROM forecast_evaluation_events_v2 "
                    "WHERE cohort_version='weather-evaluation-v1' AND eligible=1"
                ).fetchone()[0]
            )
            prediction_rows = connection.execute(
                """
                SELECT p.*, m.source, m.model, m.model_version, m.source_run_id,
                       m.init_time_utc, m.published_at_utc, m.first_fetched_at_utc,
                       m.source_uri, m.source_payload_hash, m.metadata_json AS model_metadata_json
                FROM forecast_predictions_v2 p
                JOIN forecast_model_runs_v2 m ON m.id = p.model_run_id
                ORDER BY p.issued_at_utc DESC, p.id DESC
                LIMIT 1000
                """
            ).fetchall()
            latest: dict[tuple[str, str], sqlite3.Row] = {}
            previous: dict[tuple[str, str], sqlite3.Row] = {}
            event_order: list[str] = []
            for row in prediction_rows:
                event_id = str(row["event_id"])
                key = (event_id, str(row["algorithm_version"]))
                if event_id not in event_order:
                    event_order.append(event_id)
                if key not in latest:
                    latest[key] = row
                elif key not in previous:
                    previous[key] = row

            selected_events = set(event_order[: max(1, event_limit)])
            grouped: dict[str, dict[str, object]] = {}
            for (event_id, algorithm), row in latest.items():
                if event_id not in selected_events:
                    continue
                probability_rows = connection.execute(
                    """
                    SELECT fp.*, ms.payload_json
                    FROM forecast_probabilities_v2 fp
                    JOIN market_snapshots ms ON ms.id = fp.market_snapshot_id
                    WHERE fp.prediction_id = ? ORDER BY fp.ordinal
                    """,
                    (row["id"],),
                ).fetchall()
                distribution: list[dict[str, object]] = []
                event_title = None
                for probability in probability_rows:
                    payload = json.loads(probability["payload_json"])
                    event_title = event_title or payload.get("event_title")
                    distribution.append(
                        {
                            "market_id": str(probability["market_id"]),
                            "market_question": payload.get("market_question"),
                            "label": probability["outcome_label"],
                            "probability": probability["probability"],
                            "executable_price": _best_ask_from_snapshot(payload),
                        }
                    )
                distribution.sort(key=lambda item: Decimal(str(item["probability"])), reverse=True)
                top = distribution[0] if distribution else None
                previous_top = self._top_probability(
                    connection, previous.get((event_id, algorithm))
                )
                metadata = _json_object(row["metadata_json"])
                model_metadata = _json_object(row["model_metadata_json"])
                observed_max_at = connection.execute(
                    "SELECT MAX(observed_at_utc) FROM forecast_observation_evidence_v2 "
                    "WHERE prediction_id=?",
                    (row["id"],),
                ).fetchone()[0]
                event = grouped.setdefault(
                    event_id,
                    {
                        "event_id": event_id,
                        "event_title": event_title,
                        "station_id": row["station_id"],
                        "observation_date": row["observation_date"],
                        "latest_issued_at_utc": row["issued_at_utc"],
                        "observed_max_c": row["observed_floor_c"],
                        "observed_max_at_utc": observed_max_at,
                        "versions": [],
                    },
                )
                event["event_title"] = event.get("event_title") or event_title
                if str(row["issued_at_utc"]) > str(event["latest_issued_at_utc"]):
                    event["latest_issued_at_utc"] = row["issued_at_utc"]
                if row["observed_floor_c"] is not None:
                    event["observed_max_c"] = row["observed_floor_c"]
                    event["observed_max_at_utc"] = observed_max_at
                versions = event["versions"]
                assert isinstance(versions, list)
                versions.append(
                    {
                        "prediction_id": int(row["id"]),
                        "algorithm_version": algorithm,
                        "source": row["source"],
                        "model": row["model"],
                        "model_version": row["model_version"],
                        "phase": row["phase"],
                        "issued_at_utc": row["issued_at_utc"],
                        "model_init_time_utc": row["init_time_utc"],
                        "model_published_at_utc": row["published_at_utc"],
                        "model_fetched_at_utc": row["model_fetched_at_utc"],
                        "scenario_count": int(row["scenario_count"]),
                        "point_forecast_c": row["point_forecast_c"],
                        "observed_floor_c": row["observed_floor_c"],
                        "observed_max_at_utc": observed_max_at,
                        "top": top,
                        "previous_top": previous_top,
                        "distribution": distribution,
                        "metadata": metadata,
                        "model_metadata": model_metadata,
                    }
                )

            outcome_rows = connection.execute(
                "SELECT * FROM forecast_outcome_versions_v2 "
                "ORDER BY recorded_at_utc DESC, id DESC LIMIT 50"
            ).fetchall()
            latest_outcome_by_event: dict[str, sqlite3.Row] = {}
            for row in outcome_rows:
                latest_outcome_by_event.setdefault(str(row["event_id"]), row)
            catalog_rows = connection.execute(
                """
                SELECT DISTINCT a.source, a.model, a.algorithm_version, e.station_id
                FROM forecast_evaluation_algorithms_v2 a
                JOIN forecast_evaluation_events_v2 e ON e.id=a.evaluation_event_id
                WHERE e.cohort_version='weather-evaluation-v1'
                  AND e.eligible=1 AND a.expected=1
                ORDER BY a.source, a.model, a.algorithm_version, e.station_id
                """
            ).fetchall()

        metrics: list[dict[str, object]] = []
        quality: list[dict[str, object]] = []
        seen_catalog: set[tuple[str, str, str]] = set()
        for row in catalog_rows:
            key = (str(row["source"]), str(row["model"]), str(row["algorithm_version"]))
            if key in seen_catalog:
                continue
            seen_catalog.add(key)
            report = self.metrics(
                ForecastMetricsQuery(source=key[0], model=key[1], algorithm_version=key[2])
            )
            metrics.append(_compact_metrics_report(report))
            quality.append(self._dashboard_quality(key=key, report=report))

        events = list(grouped.values())
        for event in events:
            outcome = latest_outcome_by_event.get(str(event["event_id"]))
            event["outcome"] = (
                None
                if outcome is None
                else {
                    "actual_max_c": outcome["actual_max_c"],
                    "displayed_max": outcome["displayed_max"],
                    "winning_market_id": outcome["winning_market_id"],
                    "winning_label": outcome["winning_label"],
                    "resolved_at_utc": outcome["resolved_at_utc"],
                    "source_revision": outcome["source_revision"],
                }
            )
        events.sort(key=lambda item: str(item["latest_issued_at_utc"]), reverse=True)
        return {
            "counts": self.counts(),
            "available_event_count": available_event_count,
            "considered_event_count": considered_event_count,
            "eligible_event_count": eligible_event_count,
            "shown_event_count": len(events),
            "events": events,
            "metrics": metrics,
            # Station-level reports are intentionally omitted from the compact
            # dashboard payload. They remain available through ``metrics()``.
            "station_metrics": [],
            "quality": quality,
            "outcomes": [dict(row) for row in outcome_rows],
            "source_statuses": self.latest_source_statuses(),
        }

    def _dashboard_quality(
        self,
        *,
        key: tuple[str, str, str],
        report: ForecastMetricsReport,
        minimum_events: int = 30,
        bootstrap_samples: int = 1000,
    ) -> dict[str, object]:
        """Build compact phase-separated metrics with uncertainty intervals.

        Polling snapshots are reduced to one latest prediction per event before
        intervals are calculated. Accuracy and coverage use exact
        Clopper--Pearson binomial intervals; MAE, multiclass Brier, and ECE use
        deterministic percentile bootstrap intervals. Values below the
        ``minimum_events`` threshold remain descriptive only.
        """

        query = ForecastMetricsQuery(source=key[0], model=key[1], algorithm_version=key[2])
        predictions = self._load_predictions(query)
        universe = self._load_evaluation_universe(query)
        outcomes = self._load_latest_outcomes(as_of=query.as_of_utc)
        phases: list[dict[str, object]] = []
        for phase in (ForecastPhase.LEAD_TIME, ForecastPhase.INTRADAY):
            phase_event_ids = {
                str(row["event_id"]) for row in universe if row["phase"] == phase.value
            }
            rows = [
                row
                for row in predictions
                if row["phase"] == phase.value and str(row["event_id"]) in phase_event_ids
            ]
            latest = _latest_prediction_per_event(rows)
            phase_outcomes = {
                event_id: outcome
                for event_id, outcome in outcomes.items()
                if event_id in phase_event_ids
            }
            cases = self._evaluation_cases(latest, phase_outcomes)
            phase_slice = report.by_phase.get(phase.value)
            if phase_slice is None:
                continue
            phases.append(
                build_phase_quality(
                    phase=phase,
                    metrics=phase_slice,
                    cases=cases,
                    minimum_events=minimum_events,
                    bootstrap_samples=bootstrap_samples,
                    calibration_bin_count=query.calibration_bin_count,
                    seed_key="|".join(key),
                )
            )
        return {
            "source": key[0],
            "model": key[1],
            "algorithm_version": key[2],
            "confidence_level": 0.95,
            "minimum_reliable_events": minimum_events,
            "bootstrap_samples": bootstrap_samples,
            "phases": phases,
        }

    @staticmethod
    def _top_probability(
        connection: sqlite3.Connection, row: sqlite3.Row | None
    ) -> dict[str, object] | None:
        if row is None:
            return None
        probability = connection.execute(
            "SELECT market_id, outcome_label, probability FROM forecast_probabilities_v2 "
            "WHERE prediction_id = ? ORDER BY CAST(probability AS REAL) DESC, ordinal LIMIT 1",
            (row["id"],),
        ).fetchone()
        if probability is None:
            return None
        return {
            "market_id": str(probability["market_id"]),
            "label": probability["outcome_label"],
            "probability": probability["probability"],
            "issued_at_utc": row["issued_at_utc"],
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
                distribution_mass, metadata_json, submission_hash, created_at_utc
            ) VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
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
                _canonical_json(submission.metadata),
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
            linked = connection.execute(
                """
                SELECT id, first_seen_at_utc, source, temperature_c,
                       displayed_temperature_c, corrected
                FROM station_observation_versions
                WHERE station_id = ? AND observed_at_utc = ? AND revision_hash = ?
                """,
                (
                    observation.station_id,
                    observation.observed_at_utc.isoformat(),
                    observation.revision_hash,
                ),
            ).fetchone()
            canonical_first_seen = observation.first_seen_at_utc.isoformat()
            if linked is not None:
                if (
                    str(linked["source"]) != observation.source
                    or Decimal(str(linked["temperature_c"])) != observation.temperature_c
                    or Decimal(str(linked["displayed_temperature_c"]))
                    != observation.displayed_temperature_c
                    or bool(linked["corrected"]) != observation.corrected
                ):
                    raise ValueError(
                        "linked station observation revision does not match forecast evidence"
                    )
                canonical_first_seen = str(linked["first_seen_at_utc"])
            canonical_first_seen_dt = datetime.fromisoformat(canonical_first_seen)
            if (
                canonical_first_seen_dt > submission.observation_cutoff_at_utc
                or observation.observed_at_utc > submission.observation_cutoff_at_utc
            ):
                raise ValueError(
                    "forecast observation evidence is later than the observation cutoff"
                )
            connection.execute(
                """
                INSERT INTO forecast_observation_evidence_v2(
                    prediction_id, station_observation_version_id, station_id,
                    observed_at_utc, revision_hash, first_seen_at_utc,
                    canonical_first_seen_at_utc, source, temperature_c,
                    displayed_temperature_c, corrected
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    prediction_id,
                    None if linked is None else linked["id"],
                    observation.station_id,
                    observation.observed_at_utc.isoformat(),
                    observation.revision_hash,
                    canonical_first_seen,
                    canonical_first_seen,
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
        clauses = [
            "e.cohort_version = ?",
            "e.eligible = 1",
            "a.expected = 1",
            "a.prediction_id = p.id",
            "a.algorithm_version = p.algorithm_version",
            f"p.phase IN ({placeholders})",
        ]
        params: list[object] = [
            query.cohort_version,
            *(phase.value for phase in query.phases),
        ]
        if query.source is not None:
            clauses.append("m.source = ?")
            params.append(query.source)
        if query.model is not None:
            clauses.append("m.model = ?")
            params.append(query.model)
        if query.algorithm_version is not None:
            clauses.append("p.algorithm_version = ?")
            params.append(query.algorithm_version)
        if query.station_id is not None:
            clauses.append("p.station_id = ?")
            params.append(query.station_id)
        if query.as_of_utc is not None:
            cutoff = query.as_of_utc.astimezone(UTC).isoformat()
            clauses.append("p.issued_at_utc <= ?")
            params.append(cutoff)
            clauses.append("e.considered_at_utc <= ?")
            params.append(cutoff)
        with self.connect() as connection:
            return connection.execute(
                "SELECT DISTINCT p.*, m.source, m.model FROM forecast_predictions_v2 p "
                "JOIN forecast_model_runs_v2 m ON m.id = p.model_run_id "
                "JOIN forecast_evaluation_algorithms_v2 a ON a.prediction_id=p.id "
                "JOIN forecast_evaluation_events_v2 e ON e.id=a.evaluation_event_id WHERE "
                + " AND ".join(clauses)
                + " ORDER BY p.issued_at_utc, p.id",
                params,
            ).fetchall()

    def _load_evaluation_universe(self, query: ForecastMetricsQuery) -> list[sqlite3.Row]:
        """Load expected eligible opportunities, including failed/no predictions."""

        placeholders = ",".join("?" for _ in query.phases)
        clauses = [
            "e.cohort_version = ?",
            "e.eligible = 1",
            "e.phase IN (" + placeholders + ")",
        ]
        params: list[object] = [query.cohort_version, *(phase.value for phase in query.phases)]
        join = ""
        if query.algorithm_version is not None:
            join = " JOIN forecast_evaluation_algorithms_v2 a ON a.evaluation_event_id=e.id "
            clauses.extend(["a.expected = 1", "a.algorithm_version = ?"])
            params.append(query.algorithm_version)
            if query.source is not None:
                clauses.append("a.source = ?")
                params.append(query.source)
            if query.model is not None:
                clauses.append("a.model = ?")
                params.append(query.model)
        elif query.source is not None or query.model is not None:
            # A source/model universe is meaningful only through the algorithms
            # explicitly expected for that event.
            join = " JOIN forecast_evaluation_algorithms_v2 a ON a.evaluation_event_id=e.id "
            clauses.append("a.expected = 1")
            if query.source is not None:
                clauses.append("a.source = ?")
                params.append(query.source)
            if query.model is not None:
                clauses.append("a.model = ?")
                params.append(query.model)
        if query.station_id is not None:
            clauses.append("e.station_id = ?")
            params.append(query.station_id)
        if query.as_of_utc is not None:
            clauses.append("e.considered_at_utc <= ?")
            params.append(query.as_of_utc.astimezone(UTC).isoformat())
        with self.connect() as connection:
            return connection.execute(
                "SELECT DISTINCT e.* FROM forecast_evaluation_events_v2 e"
                + join
                + " WHERE "
                + " AND ".join(clauses)
                + " ORDER BY e.considered_at_utc, e.id",
                params,
            ).fetchall()

    def _load_latest_outcomes(
        self, *, as_of: datetime | None, station_id: str | None = None
    ) -> dict[str, sqlite3.Row]:
        clauses: list[str] = []
        params: list[object] = []
        if as_of is not None:
            clauses.append("recorded_at_utc <= ?")
            params.append(as_of.astimezone(UTC).isoformat())
        if station_id is not None:
            clauses.append("station_id = ?")
            params.append(station_id)
        with self.connect() as connection:
            rows = connection.execute(
                "SELECT * FROM forecast_outcome_versions_v2 "
                + ("WHERE " + " AND ".join(clauses) + " " if clauses else "")
                + "ORDER BY recorded_at_utc, id",
                params,
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


def _json_object(value: object) -> dict[str, object]:
    try:
        parsed = json.loads(str(value or "{}"))
    except (TypeError, ValueError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _outcome_semantic_payload(outcome: RealizedForecastOutcome) -> dict[str, object]:
    """Revision identity excluding the local time at which it was re-polled."""

    payload = outcome.model_dump(mode="json", exclude={"recorded_at_utc", "resolved_at_utc"})
    evidence = payload.get("evidence")
    if isinstance(evidence, dict):
        # This is local collection metadata, not part of the source revision.
        # Re-polling identical NOAA content must remain idempotent.
        evidence = dict(evidence)
        evidence.pop("observation_fetch_time", None)
        payload["evidence"] = evidence
    return payload


def _outcome_from_row(row: sqlite3.Row) -> RealizedForecastOutcome:
    return RealizedForecastOutcome(
        event_id=str(row["event_id"]),
        station_id=str(row["station_id"]),
        observation_date=date.fromisoformat(str(row["observation_date"])),
        actual_max_c=Decimal(str(row["actual_max_c"])),
        displayed_max=Decimal(str(row["displayed_max"])),
        winning_market_id=str(row["winning_market_id"]),
        winning_condition_id=(
            None if row["winning_condition_id"] is None else str(row["winning_condition_id"])
        ),
        winning_label=(None if row["winning_label"] is None else str(row["winning_label"])),
        resolution_source=str(row["resolution_source"]),
        source_revision=str(row["source_revision"]),
        source_published_at_utc=(
            None
            if row["source_published_at_utc"] is None
            else datetime.fromisoformat(str(row["source_published_at_utc"]))
        ),
        resolved_at_utc=datetime.fromisoformat(str(row["resolved_at_utc"])),
        recorded_at_utc=datetime.fromisoformat(str(row["recorded_at_utc"])),
        evidence=_json_object(row["evidence_json"]),
    )


def _best_ask_from_snapshot(payload: dict[str, Any]) -> str | None:
    asks = payload.get("asks")
    if not isinstance(asks, list):
        return None
    prices: list[Decimal] = []
    for row in asks:
        if isinstance(row, dict) and row.get("price") is not None:
            try:
                prices.append(Decimal(str(row["price"])))
            except ValueError:
                continue
    return None if not prices else str(min(prices))


def _compact_metrics_report(report: ForecastMetricsReport) -> dict[str, object]:
    payload = report.model_dump(mode="json")
    for key in ("overall",):
        value = payload.get(key)
        if isinstance(value, dict):
            value.pop("top_label_calibration", None)
    for section in ("by_phase", "by_lead_time"):
        rows = payload.get(section)
        if isinstance(rows, dict):
            for value in rows.values():
                if isinstance(value, dict):
                    value.pop("top_label_calibration", None)
    return payload


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
