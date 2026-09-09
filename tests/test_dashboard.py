from typing import cast

from polybot.storage import Storage


def test_dashboard_is_read_only_snapshot(tmp_path) -> None:
    storage = Storage(tmp_path / "test.sqlite3")
    before = storage.portfolio_summary()
    payload = storage.dashboard_payload()
    after = storage.portfolio_summary()

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
    assert payload["active_window"] is None
