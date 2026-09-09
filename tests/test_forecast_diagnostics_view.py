from __future__ import annotations

import hashlib
from pathlib import Path

from polybot.forecast_diagnostics_view import build_forecast_diagnostics_view
from polybot.storage import Storage


def test_diagnostics_view_is_read_only_on_empty_database(tmp_path: Path) -> None:
    path = tmp_path / "diagnostics.sqlite3"
    Storage(path)
    before = hashlib.sha256(path.read_bytes()).hexdigest()

    report = build_forecast_diagnostics_view(path)

    after = hashlib.sha256(path.read_bytes()).hexdigest()
    assert before == after
    assert report["version"] == "forecast-diagnostics-v1"
    assert report["summary"]["settled_trade_count"] == 0  # type: ignore[index]
