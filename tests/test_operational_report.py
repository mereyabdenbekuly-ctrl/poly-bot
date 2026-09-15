from __future__ import annotations

import json
import os
import sqlite3
import subprocess
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

from polybot.forecast_models import (
    OPEN_METEO_ALGORITHM_VERSION,
    WEATHERNEXT_ALGORITHM_VERSION,
)
from polybot.forecast_v2 import ECMWF_RAW_ALGORITHM_VERSION, FORECAST_V2_ALGORITHM_VERSION
from polybot.operational_report import build_operational_report

NOW = datetime(2026, 9, 15, 12, 0, tzinfo=UTC)


def _create_full_database(path: Path) -> None:
    with sqlite3.connect(path) as connection:
        connection.executescript(
            """
            CREATE TABLE scan_runs (
                id INTEGER PRIMARY KEY,
                started_at TEXT NOT NULL,
                completed_at TEXT,
                query TEXT NOT NULL,
                mode TEXT NOT NULL,
                geoblocked INTEGER,
                status TEXT NOT NULL,
                error TEXT,
                window_id INTEGER
            );
            CREATE TABLE paper_marks (
                id INTEGER PRIMARY KEY,
                paper_order_id INTEGER NOT NULL,
                captured_at TEXT NOT NULL,
                payload_json TEXT NOT NULL DEFAULT '{}'
            );
            CREATE TABLE paper_orders (
                id INTEGER PRIMARY KEY,
                status TEXT NOT NULL,
                strategy_version TEXT NOT NULL
            );
            CREATE TABLE runtime_windows (
                id INTEGER PRIMARY KEY,
                status TEXT NOT NULL,
                paper INTEGER NOT NULL
            );
            CREATE TABLE runtime_reports (
                id INTEGER PRIMARY KEY,
                created_at TEXT NOT NULL,
                payload_json TEXT NOT NULL
            );
            CREATE TABLE weather_snapshots (
                id INTEGER PRIMARY KEY,
                fetched_at TEXT NOT NULL
            );
            CREATE TABLE weathernext_snapshots (
                id INTEGER PRIMARY KEY,
                captured_at TEXT NOT NULL
            );
            CREATE TABLE forecast_predictions_v2 (
                id INTEGER PRIMARY KEY,
                algorithm_version TEXT NOT NULL,
                issued_at_utc TEXT NOT NULL
            );
            CREATE TABLE forecast_model_runs_v2 (
                id INTEGER PRIMARY KEY,
                source TEXT NOT NULL,
                model TEXT NOT NULL,
                first_fetched_at_utc TEXT NOT NULL,
                init_time_utc TEXT
            );
            CREATE TABLE forecast_source_status_v2 (
                id INTEGER PRIMARY KEY,
                source TEXT NOT NULL,
                state TEXT NOT NULL,
                checked_at_utc TEXT NOT NULL,
                payload_json TEXT NOT NULL
            );
            CREATE TABLE forecast_evaluation_events_v2 (
                id INTEGER PRIMARY KEY,
                cohort_version TEXT NOT NULL,
                considered_at_utc TEXT NOT NULL
            );
            CREATE TABLE forecast_evaluation_algorithms_v2 (
                evaluation_event_id INTEGER NOT NULL,
                algorithm_version TEXT NOT NULL,
                status TEXT NOT NULL
            );
            """
        )
        for run_id, duration in enumerate((60, 120, 180, 600), start=1):
            started = NOW - timedelta(hours=run_id, seconds=duration)
            connection.execute(
                "INSERT INTO scan_runs VALUES (?, ?, ?, 'weather', 'paper', 0, "
                "'completed', NULL, 1)",
                (run_id, started.isoformat(), (started + timedelta(seconds=duration)).isoformat()),
            )
        failed_start = NOW - timedelta(minutes=20)
        connection.execute(
            "INSERT INTO scan_runs VALUES (5, ?, ?, 'weather', 'paper', 1, 'failed', NULL, 1)",
            (failed_start.isoformat(), (failed_start + timedelta(seconds=30)).isoformat()),
        )
        connection.execute(
            "INSERT INTO scan_runs VALUES (6, ?, NULL, 'weather', 'paper', NULL, "
            "'running', NULL, 1)",
            ((NOW - timedelta(minutes=5)).isoformat(),),
        )
        outside = NOW - timedelta(hours=30)
        connection.execute(
            "INSERT INTO scan_runs VALUES (7, ?, ?, 'weather', 'paper', 0, 'completed', NULL, 1)",
            (outside.isoformat(), (outside + timedelta(seconds=10)).isoformat()),
        )

        cutoff = NOW - timedelta(hours=24)
        mark_times = (
            (1, 1, cutoff - timedelta(minutes=5)),
            (2, 1, cutoff + timedelta(minutes=5)),
            (3, 1, cutoff + timedelta(minutes=10)),
            (4, 2, cutoff + timedelta(minutes=2)),
            (5, 2, cutoff + timedelta(minutes=12)),
        )
        connection.executemany(
            "INSERT INTO paper_marks(id, paper_order_id, captured_at) VALUES (?, ?, ?)",
            [(row_id, order_id, captured.isoformat()) for row_id, order_id, captured in mark_times],
        )
        connection.executemany(
            "INSERT INTO paper_orders VALUES (?, ?, ?)",
            [(1, "OPEN", "v1"), (2, "PAPER_SETTLED", "v1")],
        )
        connection.execute("INSERT INTO runtime_windows VALUES (1, 'ACTIVE', 1)")
        connection.execute(
            "INSERT INTO runtime_reports VALUES (1, ?, ?)",
            ((NOW - timedelta(hours=1)).isoformat(), json.dumps({"scan": {"events": 8}})),
        )
        connection.execute(
            "INSERT INTO weather_snapshots VALUES (1, ?)",
            ((NOW - timedelta(hours=1)).isoformat(),),
        )
        connection.execute(
            "INSERT INTO weathernext_snapshots VALUES (1, ?)",
            ((NOW - timedelta(days=2)).isoformat(),),
        )
        algorithms = (
            OPEN_METEO_ALGORITHM_VERSION,
            ECMWF_RAW_ALGORITHM_VERSION,
            FORECAST_V2_ALGORITHM_VERSION,
            WEATHERNEXT_ALGORITHM_VERSION,
        )
        connection.executemany(
            "INSERT INTO forecast_predictions_v2 VALUES (?, ?, ?)",
            [
                (index, algorithm, (NOW - timedelta(hours=1)).isoformat())
                for index, algorithm in enumerate(algorithms, start=1)
            ],
        )
        connection.execute(
            "INSERT INTO forecast_model_runs_v2 VALUES (1, 'open-meteo-ecmwf', "
            "'ECMWF IFS ENS', ?, ?)",
            (
                (NOW - timedelta(hours=1)).isoformat(),
                (NOW - timedelta(hours=8)).isoformat(),
            ),
        )
        connection.execute(
            "INSERT INTO forecast_source_status_v2 VALUES (1, 'ecmwf-open-data-ifs-ens', "
            "'available', ?, ?)",
            (
                (NOW - timedelta(hours=2)).isoformat(),
                json.dumps({"member_count": 50, "message": "available", "source_url": "omit"}),
            ),
        )
        connection.execute(
            "INSERT INTO forecast_evaluation_events_v2 VALUES (1, 'weather-evaluation-v1', ?)",
            ((NOW - timedelta(hours=1)).isoformat(),),
        )
        connection.executemany(
            "INSERT INTO forecast_evaluation_algorithms_v2 VALUES (1, ?, 'PREDICTED')",
            [(algorithm,) for algorithm in algorithms],
        )


def _create_procfs(root: Path, *, observer_paper: bool = True) -> None:
    root.mkdir()
    (root / "loadavg").write_text("0.40 0.20 0.10 1/100 1\n", encoding="utf-8")
    (root / "stat").write_text("cpu  100 0 50 850 0 0 0 0 0 0\n", encoding="utf-8")
    (root / "uptime").write_text("7200.00 1000.00\n", encoding="utf-8")
    (root / "meminfo").write_text(
        "MemTotal:       16777216 kB\n"
        "MemAvailable:   12582912 kB\n"
        "SwapTotal:             0 kB\n"
        "SwapFree:              0 kB\n",
        encoding="utf-8",
    )
    observer = root / "101"
    observer.mkdir()
    observer_arguments = b"/opt/polybot/.venv/bin/polybot\0run\0"
    if observer_paper:
        observer_arguments += b"--paper\0"
    (observer / "cmdline").write_bytes(observer_arguments)
    (observer / "status").write_text("Name:\tpolybot\nVmRSS:\t153600 kB\n", encoding="utf-8")
    dashboard = root / "102"
    dashboard.mkdir()
    (dashboard / "cmdline").write_bytes(
        b"/opt/polybot/.venv/bin/polybot\0dashboard\0--host\0127.0.0.1\0"
    )
    (dashboard / "status").write_text("Name:\tpolybot\nVmRSS:\t180000 kB\n", encoding="utf-8")


def _write_snapshot(path: Path) -> None:
    path.write_text(
        json.dumps(
            {
                "source": "weathernext3_statistics",
                "mode": "SUMMARY_ONLY",
                "surface": "gcs_statistics",
                "init_time_utc": "2026-09-15T00:00:00Z",
                "received_at_utc": "2026-09-15T01:00:00Z",
                "location": "New York",
                "station_id": "KNYC",
                "observation_date": "2026-09-15",
                "variable": "temperature_2m",
                "points": [{"valid_time_utc": "2026-09-15T12:00:00Z"}],
            }
        ),
        encoding="utf-8",
    )


def test_operational_report_measures_period_resources_storage_and_sources(tmp_path: Path) -> None:
    database = tmp_path / "polybot.sqlite3"
    _create_full_database(database)
    proc = tmp_path / "proc"
    _create_procfs(proc)
    backups = tmp_path / "backups"
    backups.mkdir()
    for index in range(3):
        backup = backups / f"polybot-20260915T0{index}0000Z.sqlite3"
        backup.write_bytes(b"x" * (100 + index))
        backup.with_suffix(".sqlite3.sha256").write_text("digest", encoding="utf-8")
        timestamp = (NOW - timedelta(hours=2 - index)).timestamp()
        os.utime(backup, (timestamp, timestamp))
        os.utime(backup.with_suffix(".sqlite3.sha256"), (timestamp, timestamp))
    ecmwf_json = tmp_path / "ecmwf-json" / "release-a"
    ecmwf_json.mkdir(parents=True)
    (ecmwf_json / "manifest.json").write_text("{}", encoding="utf-8")
    ecmwf_raw = tmp_path / "ecmwf-raw" / "release-b"
    ecmwf_raw.mkdir(parents=True)
    (ecmwf_raw / "manifest.json").write_text("{}", encoding="utf-8")
    (tmp_path / "ecmwf-raw" / ".tmp-incomplete").mkdir()
    statistics_snapshot = tmp_path / "weathernext-statistics.json"
    _write_snapshot(statistics_snapshot)

    report = build_operational_report(
        database,
        state_root=tmp_path,
        backup_root=backups,
        ecmwf_json_root=tmp_path / "ecmwf-json",
        ecmwf_raw_root=tmp_path / "ecmwf-raw",
        weathernext_statistics_snapshot_path=statistics_snapshot,
        now=NOW,
        proc_root=proc,
        disk_path=tmp_path,
        cpu_count=4,
        ecmwf_raw_min_free_bytes=0,
        ecmwf_raw_min_free_fraction=0.0,
    )

    assert report["collection_guards"] == {
        "database_read_only": True,
        "network_access_performed": False,
        "weathernext_global_read_performed": False,
    }
    system = report["system"]
    assert system["cpu"]["logical_count"] == 4
    assert system["cpu"]["load_per_cpu_1m"] == 0.1
    assert system["memory"]["total_bytes"] == 16 * 1024**3
    assert system["processes"]["roles"]["observer"]["count"] == 1
    assert system["processes"]["roles"]["dashboard"]["count"] == 1
    assert system["processes"]["paper_observer_count"] == 1
    assert system["processes"]["non_paper_observer_count"] == 0

    cycles = report["cycles"]
    assert cycles["completed_count"] == 4
    assert cycles["failed_count"] == 1
    assert cycles["running_count"] == 1
    assert cycles["geoblocked_count"] == 1
    assert cycles["duration_seconds"]["p50"] == 150.0
    assert cycles["duration_seconds"]["p95"] == 537.0
    assert cycles["duration_seconds"]["max"] == 600.0
    assert report["scan_modes"] == {"paper": {"completed": 4, "failed": 1, "running": 1}}

    marks = report["paper_marks"]
    assert marks["mark_count"] == 4
    assert marks["paper_order_count"] == 2
    assert marks["interval_seconds"]["sample_count"] == 3
    assert marks["interval_seconds"]["p50"] == 600.0
    assert marks["interval_seconds"]["max"] == 600.0

    storage = report["storage"]
    assert storage["backups"]["backup_count"] == 3
    assert storage["backups"]["backup_bytes"] == 303
    assert storage["backups"]["observed_retention_span_hours"] == 2.0
    assert storage["backups"]["observed_interval_seconds"]["p50"] == 3600.0
    assert storage["ecmwf_json_archive"]["completed_release_count"] == 1
    assert storage["ecmwf_raw_archive"]["manifest_candidate_count"] == 1
    assert storage["ecmwf_raw_archive"]["completed_release_count"] == 0
    assert storage["ecmwf_raw_archive"]["completed_bytes"] == 0
    assert storage["ecmwf_raw_archive"]["partial_directory_count"] == 1
    assert storage["ecmwf_raw_archive"]["preserved_diagnostic_count"] == 2
    assert storage["ecmwf_raw_archive"]["retention_status"]["within_limits"] is True

    sources = report["sources"]
    assert sources["v1"]["state"] == "active_in_period"
    assert sources["ecmwf"]["state"] == "active_in_period"
    assert sources["v2"]["state"] == "active_in_period"
    assert sources["weathernext"]["statistics_snapshot"]["load_state"] == "available"
    assert sources["weathernext"]["statistics_snapshot"]["mode"] == "SUMMARY_ONLY"
    assert sources["weathernext"]["statistics_snapshot"]["point_count"] == 1
    assert sources["weathernext"]["statistics_snapshot"]["access_state"] == "not_checked"
    assert sources["ecmwf"]["latest_source_statuses"]["ecmwf-open-data-ifs-ens"]["details"] == {
        "member_count": 50,
        "message": "available",
    }
    assert report["background"]["available"] is False
    assert report["paper_live_safety"]["live_disabled"] is True
    assert report["paper_live_safety"]["single_observer"] is True
    assert report["paper_live_safety"]["observer_paper_mode"] is True


def test_operational_report_is_schema_tolerant_and_does_not_migrate_database(
    tmp_path: Path,
) -> None:
    database = tmp_path / "minimal.sqlite3"
    with sqlite3.connect(database) as connection:
        connection.execute(
            "CREATE TABLE scan_runs (id INTEGER PRIMARY KEY, started_at TEXT NOT NULL, "
            "completed_at TEXT, mode TEXT NOT NULL, status TEXT NOT NULL)"
        )
    with sqlite3.connect(database) as connection:
        before = connection.execute(
            "SELECT name, sql FROM sqlite_master WHERE type='table' ORDER BY name"
        ).fetchall()

    report = build_operational_report(
        database,
        state_root=tmp_path,
        ecmwf_json_root=None,
        now=NOW,
        proc_root=tmp_path / "missing-proc",
        disk_path=tmp_path,
    )

    assert report["cycles"]["available"] is True
    assert report["paper_marks"]["available"] is False
    assert report["background"]["available"] is False
    assert report["sources"]["v1"]["state"] == "unavailable_or_not_configured"
    with sqlite3.connect(database) as connection:
        after = connection.execute(
            "SELECT name, sql FROM sqlite_master WHERE type='table' ORDER BY name"
        ).fetchall()
    assert after == before


def test_operational_report_discovers_generic_background_telemetry_without_leaking_text(
    tmp_path: Path,
) -> None:
    database = tmp_path / "background.sqlite3"
    with sqlite3.connect(database) as connection:
        connection.executescript(
            """
            CREATE TABLE scan_runs (
                id INTEGER PRIMARY KEY, started_at TEXT NOT NULL, completed_at TEXT,
                mode TEXT NOT NULL, status TEXT NOT NULL
            );
            CREATE TABLE runtime_reports (
                id INTEGER PRIMARY KEY, created_at TEXT NOT NULL, payload_json TEXT NOT NULL
            );
            CREATE TABLE research_batches (
                id INTEGER PRIMARY KEY, started_at TEXT NOT NULL, status TEXT NOT NULL,
                events_processed INTEGER NOT NULL, failure_count INTEGER NOT NULL
            );
            """
        )
        connection.execute(
            "INSERT INTO research_batches VALUES (1, ?, 'completed', 8, 0)",
            ((NOW - timedelta(minutes=10)).isoformat(),),
        )
        connection.execute(
            "INSERT INTO runtime_reports VALUES (1, ?, ?)",
            (
                (NOW - timedelta(minutes=5)).isoformat(),
                json.dumps(
                    {
                        "forecast_engine": {
                            "background_outcome_refresh": {
                                "status": "completed",
                                "processed": 8,
                                "endpoint": "must-not-appear",
                            }
                        }
                    }
                ),
            ),
        )

    report = build_operational_report(
        database,
        state_root=tmp_path,
        now=NOW,
        proc_root=tmp_path / "missing-proc",
        disk_path=tmp_path,
    )

    background = report["background"]
    assert background["available"] is True
    assert background["tables"][0]["table"] == "research_batches"
    assert background["tables"][0]["period_numeric_sums"] == {
        "events_processed": 8.0,
        "failure_count": 0.0,
    }
    serialized = json.dumps(background)
    assert "must-not-appear" not in serialized
    assert background["runtime_report_samples"][0]["stats"] == {
        "status": "completed",
        "processed": 8,
    }


def test_non_paper_observer_fails_paper_mode_safety_guard(tmp_path: Path) -> None:
    database = tmp_path / "non-paper-observer.sqlite3"
    with sqlite3.connect(database) as connection:
        connection.execute(
            "CREATE TABLE scan_runs (id INTEGER PRIMARY KEY, started_at TEXT NOT NULL, "
            "completed_at TEXT, mode TEXT NOT NULL, status TEXT NOT NULL)"
        )
    proc = tmp_path / "proc"
    _create_procfs(proc, observer_paper=False)

    report = build_operational_report(
        database,
        state_root=tmp_path,
        now=NOW,
        proc_root=proc,
        disk_path=tmp_path,
        ecmwf_raw_min_free_bytes=0,
        ecmwf_raw_min_free_fraction=0.0,
    )

    processes = report["system"]["processes"]
    assert processes["paper_observer_count"] == 0
    assert processes["non_paper_observer_count"] == 1
    safety = report["paper_live_safety"]
    assert safety["observer_process_count"] == 1
    assert safety["observer_paper_mode"] is False
    assert safety["live_disabled"] is False


def test_operational_report_script_emits_json(tmp_path: Path) -> None:
    database = tmp_path / "cli.sqlite3"
    with sqlite3.connect(database) as connection:
        connection.execute(
            "CREATE TABLE scan_runs (id INTEGER PRIMARY KEY, started_at TEXT NOT NULL, "
            "completed_at TEXT, mode TEXT NOT NULL, status TEXT NOT NULL)"
        )

    result = subprocess.run(
        [
            sys.executable,
            "scripts/operational-report.py",
            "--database",
            str(database),
            "--state-root",
            str(tmp_path),
            "--ecmwf-json-root",
            str(tmp_path / "missing-json"),
            "--ecmwf-raw-root",
            str(tmp_path / "missing-raw"),
            "--json",
        ],
        check=True,
        capture_output=True,
        text=True,
    )

    payload = json.loads(result.stdout)
    assert payload["schema_version"] == 1
    assert payload["collection_guards"]["network_access_performed"] is False
