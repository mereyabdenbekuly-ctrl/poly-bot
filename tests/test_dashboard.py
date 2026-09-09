from typing import cast

from polybot.forecast_store import ForecastStore
from polybot.storage import Storage


def test_dashboard_is_read_only_snapshot(tmp_path) -> None:
    storage = Storage(tmp_path / "test.sqlite3")
    forecast_store = ForecastStore(storage.path)
    with forecast_store.connect() as connection:
        schema_rows_before = connection.execute(
            "SELECT COUNT(*) FROM forecast_engine_schema_versions"
        ).fetchone()[0]
    before = storage.portfolio_summary()
    payload = storage.dashboard_payload(forecast_store=forecast_store)
    after = storage.portfolio_summary()
    with forecast_store.connect() as connection:
        schema_rows_after = connection.execute(
            "SELECT COUNT(*) FROM forecast_engine_schema_versions"
        ).fetchone()[0]

    dashboard_portfolio = cast(dict[str, object], payload["portfolio"])
    for key in (
        "open_orders",
        "open_exposure_usd",
        "closed_orders",
        "realized_pnl_usd",
        "api_spend_usd",
        "api_reserved_usd",
        "net_project_pnl_after_api_usd",
    ):
        assert dashboard_portfolio[key] == before[key]
    assert before == after
    assert schema_rows_after == schema_rows_before
    assert payload["active_window"] is None
    comparison = cast(dict[str, object], payload["forecast_comparison"])
    assert comparison["events"] == []
    assert comparison["metrics"] == []
