from datetime import UTC, datetime, timedelta
from pathlib import Path

from polybot.storage import Storage


def test_recover_stale_scans_marks_only_old_running_rows(tmp_path: Path) -> None:
    storage = Storage(tmp_path / "state.sqlite3")
    old = (datetime.now(UTC) - timedelta(hours=2)).isoformat()
    recent = datetime.now(UTC).isoformat()
    with storage.connect() as connection:
        connection.execute(
            "INSERT INTO scan_runs(started_at, query, mode, status) VALUES (?, ?, ?, 'running')",
            (old, "old", "paper"),
        )
        connection.execute(
            "INSERT INTO scan_runs(started_at, query, mode, status) VALUES (?, ?, ?, 'running')",
            (recent, "recent", "paper"),
        )

    assert storage.recover_stale_scans(older_than_seconds=900) == 1
    with storage.connect() as connection:
        rows = connection.execute(
            "SELECT query, status, error FROM scan_runs ORDER BY id"
        ).fetchall()
    assert rows[0][0] == "old"
    assert rows[0][1] == "failed"
    assert "stale" in rows[0][2]
    assert rows[1][0] == "recent"
    assert rows[1][1] == "running"


def test_zero_threshold_recovers_every_prior_running_row(tmp_path: Path) -> None:
    storage = Storage(tmp_path / "state.sqlite3")
    storage.start_scan(query="abandoned", mode="paper")

    assert storage.recover_stale_scans(older_than_seconds=0) == 1
