from polybot.storage import Storage


def test_dashboard_is_read_only_snapshot(tmp_path) -> None:
    storage = Storage(tmp_path / "test.sqlite3")
    before = storage.portfolio_summary()
    payload = storage.dashboard_payload()
    after = storage.portfolio_summary()

    assert payload["portfolio"] == before
    assert before == after
    assert payload["active_window"] is None
