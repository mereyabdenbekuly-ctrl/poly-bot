from __future__ import annotations

import hashlib
import json
import sqlite3
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from polybot.forecast_diagnostics import (
    OPEN_METEO_ALGORITHM_VERSION,
    ForecastDiagnostics,
    reconstruct_settled_trades,
)
from polybot.forecast_store import ForecastStore
from polybot.storage import Storage

ENTRY = datetime(2026, 9, 9, 8, 55, tzinfo=UTC)


def _database(tmp_path: Path) -> Path:
    path = tmp_path / "diagnostics.sqlite3"
    Storage(path)
    ForecastStore(path)
    return path


def _snapshot_payload(market_id: str, label: str, book_hash: str) -> dict[str, object]:
    return {
        "event_id": "event-1",
        "event_title": "Highest temperature in Test City?",
        "market_id": market_id,
        "outcome_label": label,
        "outcome": "YES",
        "book_hash": book_hash,
        "condition_id": f"condition-{market_id}",
        "token_id": f"token-{market_id}",
        "asset_id": f"token-{market_id}",
        "asks": [{"price": "0.01", "size": "5"}],
        "bids": [],
    }


def _insert_complete_history(
    path: Path,
    *,
    strategy_version: str = "v1",
    decision_probability: str | None = "0.30",
    prediction_issued_at: datetime = ENTRY - timedelta(seconds=2),
    later_prediction: bool = False,
) -> None:
    with sqlite3.connect(path) as connection:
        cursor = connection.execute(
            "INSERT INTO scan_runs(started_at, query, mode, status) "
            "VALUES (?, 'test', 'paper', 'complete')",
            ((ENTRY - timedelta(minutes=1)).isoformat(),),
        )
        run_id = int(cursor.lastrowid or 0)
        snapshots: dict[str, int] = {}
        for ordinal, (market_id, label) in enumerate(
            (("market-top", "28°C"), ("market-bought", "29°C")),
            start=1,
        ):
            book_hash = f"book-{market_id}"
            payload = _snapshot_payload(market_id, label, book_hash)
            cursor = connection.execute(
                "INSERT INTO market_snapshots(run_id,event_id,market_id,captured_at,payload_json) "
                "VALUES (?, 'event-1', ?, ?, ?)",
                (
                    run_id,
                    market_id,
                    (ENTRY - timedelta(seconds=3 - ordinal)).isoformat(),
                    json.dumps(payload, sort_keys=True),
                ),
            )
            snapshots[market_id] = int(cursor.lastrowid or 0)

        decision = {
            "action": "PAPER_BUY",
            "strategy_version": strategy_version,
            "execution_model": "CROSSING_LIMIT_SHARES",
            "event_id": "event-1",
            "market_id": "market-bought",
            "outcome": "YES",
            "probability": decision_probability,
            "probability_edge": "0.29" if decision_probability is not None else None,
            "shares": "5",
            "executable_price": "0.01",
            "notional_usd": "0.05",
            "fee_usd": "0.002475",
            "api_cost_usd": "0.01",
            "max_loss_usd": "0.072475",
            "expected_profit_usd": "1.427525",
            "book_hash": "book-market-bought",
            "created_at": ENTRY.isoformat(),
        }
        cursor = connection.execute(
            """
            INSERT INTO paper_orders(
                idempotency_key,event_id,market_id,asset_id,status,strategy_version,
                execution_model,condition_id,token_id,outcome,shares,entry_price,
                notional_usd,fee_usd,api_cost_usd,execution_buffer_usd,max_loss_usd,
                expected_profit_usd,opened_at,won,realized_pnl_usd,settled_at,closed_at
            ) VALUES (
                'order-1','event-1','market-bought','token-market-bought','PAPER_SETTLED',
                ?, 'CROSSING_LIMIT_SHARES','condition-market-bought','token-market-bought',
                'YES','5','0.01','0.05','0.002475','0.01','0.02','0.072475',
                '1.427525',?,0,'-0.062475',?,?
            )
            """,
            (
                strategy_version,
                ENTRY.isoformat(),
                (ENTRY + timedelta(days=1)).isoformat(),
                (ENTRY + timedelta(days=1)).isoformat(),
            ),
        )
        order_id = int(cursor.lastrowid or 0)
        cursor = connection.execute(
            "INSERT INTO decisions(run_id,event_id,market_id,action,created_at,payload_json,"
            "paper_order_id) VALUES (?, 'event-1','market-bought','PAPER_BUY',?,?,?)",
            (run_id, ENTRY.isoformat(), json.dumps(decision), order_id),
        )
        assert cursor.lastrowid is not None

        cursor = connection.execute(
            """
            INSERT INTO forecast_model_runs_v2(
                source,model,model_version,source_run_id,init_time_utc,published_at_utc,
                first_fetched_at_utc,source_uri,source_payload_hash,metadata_json,
                provenance_key,created_at_utc
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                "open-meteo",
                "test-ensemble",
                "test-v1",
                "source-run-1",
                (ENTRY - timedelta(hours=3)).isoformat(),
                (ENTRY - timedelta(minutes=10)).isoformat(),
                (ENTRY - timedelta(minutes=5)).isoformat(),
                "https://example.test/forecast",
                "payload-hash",
                json.dumps({"api_key": "must-not-leak", "surface": "test"}),
                "provenance-1",
                (ENTRY - timedelta(minutes=5)).isoformat(),
            ),
        )
        model_run_id = int(cursor.lastrowid or 0)
        prediction_id = _insert_prediction(
            connection,
            run_id=run_id,
            model_run_id=model_run_id,
            issued_at=prediction_issued_at,
            suffix="entry",
        )
        for ordinal, (market_id, probability) in enumerate(
            (("market-top", "0.70"), ("market-bought", "0.30"))
        ):
            connection.execute(
                """
                INSERT INTO forecast_probabilities_v2(
                    prediction_id,market_id,market_snapshot_id,book_hash,outcome_label,
                    lower_inclusive,upper_inclusive,probability,ordinal
                ) VALUES (?,?,?,?,?,1,0,?,?)
                """,
                (
                    prediction_id,
                    market_id,
                    snapshots[market_id],
                    f"book-{market_id}",
                    "28°C" if market_id == "market-top" else "29°C",
                    probability,
                    ordinal,
                ),
            )
        if later_prediction:
            late_id = _insert_prediction(
                connection,
                run_id=run_id,
                model_run_id=model_run_id,
                issued_at=ENTRY + timedelta(hours=1),
                suffix="late",
            )
            for ordinal, (market_id, probability) in enumerate(
                (("market-top", "0.05"), ("market-bought", "0.95"))
            ):
                connection.execute(
                    """
                    INSERT INTO forecast_probabilities_v2(
                        prediction_id,market_id,market_snapshot_id,book_hash,outcome_label,
                        lower_inclusive,upper_inclusive,probability,ordinal
                    ) VALUES (?,?,?,?,?,1,0,?,?)
                    """,
                    (
                        late_id,
                        market_id,
                        snapshots[market_id],
                        f"book-{market_id}",
                        "28°C" if market_id == "market-top" else "29°C",
                        probability,
                        ordinal,
                    ),
                )

        connection.execute(
            """
            INSERT INTO forecast_outcome_versions_v2(
                event_id,station_id,observation_date,actual_max_c,displayed_max,
                winning_market_id,winning_condition_id,winning_label,resolution_source,
                source_revision,resolved_at_utc,recorded_at_utc,evidence_json,outcome_hash
            ) VALUES (
                'event-1','TEST','2026-09-09','28','28','market-top','condition-market-top',
                '28°C','station-source','revision-1',?,?,?,?
            )
            """,
            (
                (ENTRY + timedelta(hours=4)).isoformat(),
                (ENTRY + timedelta(hours=4, minutes=1)).isoformat(),
                json.dumps({"authorization": "must-not-leak", "report": "official"}),
                "outcome-hash-1",
            ),
        )


def _insert_prediction(
    connection: sqlite3.Connection,
    *,
    run_id: int,
    model_run_id: int,
    issued_at: datetime,
    suffix: str,
) -> int:
    cursor = connection.execute(
        """
        INSERT INTO forecast_predictions_v2(
            scan_run_id,event_id,model_run_id,algorithm_version,phase,issued_at_utc,
            model_fetched_at_utc,lead_time_seconds,intraday_elapsed_seconds,
            intraday_remaining_seconds,station_id,observation_date,station_timezone,
            rule_day_start_utc,rule_day_end_utc,display_unit,precision_decimal_places,
            rounding_rule,rules_hash,point_forecast_c,scenario_count,
            observation_revision_count,observation_cutoff_at_utc,distribution_mass,
            metadata_json,submission_hash,created_at_utc
        ) VALUES (
            ?,'event-1',?,?,'INTRADAY',?,?,-3600,1,1,'TEST','2026-09-09','UTC',
            ?,?,'C',0,'half-up','rules','28',2,0,?,'1',?,?,?
        )
        """,
        (
            run_id,
            model_run_id,
            OPEN_METEO_ALGORITHM_VERSION,
            issued_at.isoformat(),
            (ENTRY - timedelta(minutes=5)).isoformat(),
            (ENTRY - timedelta(hours=1)).isoformat(),
            (ENTRY + timedelta(hours=23)).isoformat(),
            (issued_at - timedelta(seconds=1)).isoformat(),
            json.dumps({"weather_error_sigma_c": "1.5", "secret": "must-not-leak"}),
            f"submission-{suffix}",
            issued_at.isoformat(),
        ),
    )
    return int(cursor.lastrowid or 0)


def test_reconstructs_forecast_correct_trade_wrong_with_exact_entry_provenance(
    tmp_path: Path,
) -> None:
    path = _database(tmp_path)
    _insert_complete_history(path, later_prediction=True)
    before = hashlib.sha256(path.read_bytes()).hexdigest()

    report = reconstruct_settled_trades(
        path,
        generated_at_utc=datetime(2026, 9, 10, tzinfo=UTC),
    )

    assert hashlib.sha256(path.read_bytes()).hexdigest() == before
    assert report.settled_trade_count == 1
    trade = report.trades[0]
    assert trade.strategy_version == "v1"
    assert trade.top_forecast_bracket_label == "28°C"
    assert trade.top_forecast_probability == Decimal("0.70")
    assert trade.bought_bracket_label == "29°C"
    assert trade.bought_probability == Decimal("0.30")
    assert trade.forecast_probability_at_entry == Decimal("0.30")
    assert trade.entry_price == Decimal("0.01")
    assert trade.expected_value_usd == Decimal("1.427525")
    assert trade.official_actual_max_c == Decimal("28")
    assert trade.official_winning_label == "28°C"
    assert trade.forecast_correct is True
    assert trade.trade_correct is False
    assert trade.forecast_vs_trade == "FORECAST_CORRECT_TRADE_WRONG"
    assert trade.provenance.prediction_issued_at_utc == ENTRY - timedelta(seconds=2)
    assert trade.provenance.selection_method == "same_scan_run_before_entry"
    assert trade.provenance.entry_book_hash_matches_forecast is True
    assert trade.provenance.metadata["api_key"] == "[REDACTED]"
    assert trade.provenance.metadata["secret"] == "[REDACTED]"
    assert trade.official_outcome is not None
    assert trade.official_outcome.evidence["authorization"] == "[REDACTED]"
    assert report.summary["forecast_correct_count"] == 1
    assert report.summary["trade_incorrect_count"] == 1


def test_missing_pre_entry_forecast_is_explicit_and_never_uses_future_prediction(
    tmp_path: Path,
) -> None:
    path = _database(tmp_path)
    _insert_complete_history(path, prediction_issued_at=ENTRY + timedelta(seconds=1))

    trade = ForecastDiagnostics(path).report().trades[0]

    assert trade.top_forecast_market_id is None
    assert trade.forecast_correct is None
    assert trade.trade_correct is False
    assert trade.forecast_vs_trade == "UNAVAILABLE"
    assert trade.provenance.status == "UNAVAILABLE"
    assert "NO_PREDICTION_AT_OR_BEFORE_ENTRY" in trade.diagnostic_notes


def test_v0_trade_does_not_invent_probability_or_forecast(tmp_path: Path) -> None:
    path = _database(tmp_path)
    _insert_complete_history(path, strategy_version="v0", decision_probability=None)

    trade = reconstruct_settled_trades(path).trades[0]

    assert trade.strategy_version == "v0"
    assert trade.bought_probability is None
    assert trade.forecast_algorithm_version is None
    assert trade.top_forecast_market_id is None
    assert trade.provenance.selection_method == "strategy_v0_no_forecast"
    assert trade.official_winning_market_id == "market-top"
    assert trade.trade_correct is False


def test_as_of_cutoff_excludes_trade_settled_later(tmp_path: Path) -> None:
    path = _database(tmp_path)
    _insert_complete_history(path)

    report = reconstruct_settled_trades(path, as_of_utc=ENTRY)

    assert report.trades == ()
    assert report.summary["settled_trade_count"] == 0


def test_missing_database_raises_without_creating_it(tmp_path: Path) -> None:
    path = tmp_path / "missing.sqlite3"

    with pytest.raises(FileNotFoundError):
        reconstruct_settled_trades(path)

    assert not path.exists()
