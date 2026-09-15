from __future__ import annotations

import hashlib
import threading
import time
from typing import cast

import polybot.dashboard as dashboard_module
from polybot.dashboard import COMPARISON_VERSION, ComparisonCache
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


def test_comparison_cache_returns_read_only_no_data_for_fresh_database(tmp_path) -> None:
    storage = Storage(tmp_path / "fresh.sqlite3")
    before = hashlib.sha256(storage.path.read_bytes()).hexdigest()

    report = ComparisonCache(storage.path).get()

    assert hashlib.sha256(storage.path.read_bytes()).hexdigest() == before
    assert report["version"] == COMPARISON_VERSION
    models = cast(list[dict[str, object]], report["models"])
    assert len(models) == 3
    assert all(
        cast(dict[str, object], model["counts"])["evaluated_checkpoints"] == 0 for model in models
    )
    warnings = cast(list[str], report["warnings"])
    assert any(item.startswith("MISSING_TABLE:") for item in warnings)
    promotion = cast(dict[str, object], report["promotion"])
    assert promotion["status"] == "not_evaluable"
    assert promotion["v2_promoted"] is False


def test_comparison_cache_reuses_report_until_ttl_expires(tmp_path, monkeypatch) -> None:
    storage = Storage(tmp_path / "cache.sqlite3")
    ForecastStore(storage.path)
    now = [100.0]
    calls = 0

    def build(_database):  # type: ignore[no-untyped-def]
        nonlocal calls
        calls += 1
        return {"version": COMPARISON_VERSION, "build": calls}

    monkeypatch.setattr(dashboard_module, "compare_forecasts", build)
    cache = ComparisonCache(storage.path, ttl_seconds=60, clock=lambda: now[0])

    first = cache.get()
    now[0] = 159.99
    second = cache.get()
    now[0] = 160.0
    third = cache.get()

    assert calls == 2
    assert first is second
    assert third is not first
    assert third["build"] == 2


def test_comparison_cache_is_single_flight_for_concurrent_requests(tmp_path, monkeypatch) -> None:
    storage = Storage(tmp_path / "single-flight.sqlite3")
    ForecastStore(storage.path)
    start = threading.Barrier(6)
    call_lock = threading.Lock()
    calls = 0

    def build(_database):  # type: ignore[no-untyped-def]
        nonlocal calls
        with call_lock:
            calls += 1
            build_number = calls
        time.sleep(0.05)
        return {"version": COMPARISON_VERSION, "build": build_number}

    monkeypatch.setattr(dashboard_module, "compare_forecasts", build)
    cache = ComparisonCache(storage.path, ttl_seconds=60)
    results: list[dict[str, object]] = []

    def request() -> None:
        start.wait()
        results.append(cache.get())

    threads = [threading.Thread(target=request) for _ in range(6)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=2)

    assert all(not thread.is_alive() for thread in threads)
    assert calls == 1
    assert len(results) == 6
    assert {id(result) for result in results} == {id(results[0])}


def test_comparison_endpoint_is_separate_from_dashboard_payload(tmp_path, monkeypatch) -> None:
    storage = Storage(tmp_path / "routes.sqlite3")
    comparison_payload: dict[str, object] = {"version": COMPARISON_VERSION, "status": "ready"}
    comparison_calls = 0
    captured: dict[str, object] = {}

    class FakeCache:
        def get(self) -> dict[str, object]:
            nonlocal comparison_calls
            comparison_calls += 1
            return comparison_payload

    class FakeServer:
        def __init__(self, address, handler):  # type: ignore[no-untyped-def]
            captured["address"] = address
            captured["handler"] = handler

        def serve_forever(self) -> None:
            return

    monkeypatch.setattr(dashboard_module, "ComparisonCache", lambda _path: FakeCache())
    monkeypatch.setattr(dashboard_module, "ThreadingHTTPServer", FakeServer)
    dashboard_module.serve_dashboard(storage, host="127.0.0.1", port=0)
    handler = captured["handler"]

    class Request:
        def __init__(self, path: str) -> None:
            self.path = path
            self.payload: dict[str, object] | None = None
            self.status: int | None = None

        def _send_json(self, payload: dict[str, object], *, status: int = 200) -> None:
            self.payload = payload
            self.status = status

        def send_error(self, status: int) -> None:
            self.status = status

    comparison_request = Request("/api/comparison?ignored=1")
    handler.do_GET(comparison_request)  # type: ignore[union-attr]

    assert comparison_request.status == 200
    assert comparison_request.payload == comparison_payload
    assert comparison_calls == 1

    dashboard_request = Request("/api/dashboard")
    handler.do_GET(dashboard_request)  # type: ignore[union-attr]

    assert dashboard_request.status == 200
    assert dashboard_request.payload is not None
    assert "forecast_comparison" in dashboard_request.payload
    assert comparison_calls == 1


def test_dashboard_renders_cached_comparison_gate() -> None:
    assert 'id="comparison-gate"' in dashboard_module._HTML  # noqa: SLF001
    assert "fetch('/api/comparison'" in dashboard_module._HTML  # noqa: SLF001
    assert "v1 remains active" in dashboard_module._HTML  # noqa: SLF001


def test_dashboard_renders_weathernext_statistics_as_summary_only() -> None:
    html = dashboard_module._HTML  # noqa: SLF001
    assert 'id="weathernext-summary"' in html
    assert "SUMMARY_ONLY" in html
    assert "weathernext_statistics" in html
    assert "access_state" in html
    assert "load_state" in html
    assert "temperature_mean_c" in html
    assert "p10_c" in html and "p90_c" in html
    # The statistics surface must not be presented as a synthetic member set
    # or fed into daily-maximum probability rendering.
    assert "not a 64-member scenario set" in html
    assert "daily-maximum probabilities" in html
    assert "Read provenance and transfer estimate" in html
    assert "Global logical size (reference)" in html
    assert "Selected logical bytes" in html
    assert "Expected network bytes" in html
    assert "Codecs" in html
    assert "Shard shape (count)" in html
    assert "Transfer unit" in html


def test_dashboard_renders_isolated_weathernext_paper_strategy() -> None:
    html = dashboard_module._HTML  # noqa: SLF001
    assert 'id="weathernext-paper"' in html
    assert "WeatherNext paper strategy" in html
    assert "isolated ledger" in html
    assert "SNAPSHOT_UNAVAILABLE" in html
    assert "excluded from v1 exposure" in html
