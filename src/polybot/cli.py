from __future__ import annotations

import argparse
import importlib.metadata
import json
from decimal import Decimal
from typing import Any, cast

from rich.console import Console
from rich.table import Table

from polybot.autonomy import AutonomousRunner
from polybot.config import Settings
from polybot.dashboard import serve_dashboard
from polybot.geoblock import fetch_geoblock_status
from polybot.polymarket_gateway import PolymarketGateway
from polybot.scanner import Scanner
from polybot.storage import Storage
from polybot.weathernext import WeatherNextProvider

console = Console()
error_console = Console(stderr=True)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="polybot",
        description="Safe-by-default Polymarket weather research bot (no live executor).",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    doctor = subparsers.add_parser("doctor", help="Check network, SDK, database, and config")
    doctor.add_argument("--json", action="store_true", dest="as_json")

    scan = subparsers.add_parser("scan", help="Run one market scan")
    _add_scan_options(scan)

    run = subparsers.add_parser("run", help="Run the scanner on an interval")
    _add_scan_options(run)
    run.add_argument("--interval", type=int, default=300, help="Seconds between scans")

    dashboard = subparsers.add_parser("dashboard", help="Run the read-only local dashboard")
    dashboard.add_argument("--host", default="127.0.0.1")
    dashboard.add_argument("--port", type=int, default=8787)

    status = subparsers.add_parser("status", help="Show paper portfolio and API spend")
    status.add_argument("--json", action="store_true", dest="as_json")

    diagnostics = subparsers.add_parser(
        "diagnostics", help="Read-only forecast/trade and sigma-sensitivity diagnostics"
    )
    diagnostics.add_argument("--json", action="store_true", dest="as_json")

    comparison = subparsers.add_parser(
        "comparison", help="Read-only v1/ECMWF/v2 forecast comparison report"
    )
    comparison.add_argument("--json", action="store_true", dest="as_json")

    settle = subparsers.add_parser("settle", help="Settle an open paper order manually")
    settle.add_argument("market_id")
    outcome = settle.add_mutually_exclusive_group(required=True)
    outcome.add_argument("--won", action="store_true")
    outcome.add_argument("--lost", action="store_true")
    return parser


def _add_scan_options(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--query", help="Polymarket search query")
    parser.add_argument("--max-events", type=int, help="Maximum events per scan")
    parser.add_argument("--astra", action="store_true", help="Audit new rule text with GPT-6 Astra")
    parser.add_argument(
        "--paper",
        action="store_true",
        help="Open simulated positions for qualifying signals; never sends live orders",
    )
    parser.add_argument("--json", action="store_true", dest="as_json")


def main(argv: list[str] | None = None) -> None:
    args = build_parser().parse_args(argv)
    settings = Settings()
    if args.command == "diagnostics":
        _diagnostics(settings, as_json=args.as_json)
        return
    if args.command == "comparison":
        _comparison(settings, as_json=args.as_json)
        return
    settings.ensure_runtime_directories()
    storage = Storage(settings.database_path)

    try:
        if args.command == "doctor":
            _doctor(settings, storage, as_json=args.as_json)
        elif args.command == "scan":
            _scan_once(settings, storage, args)
        elif args.command == "run":
            _run(settings, storage, args)
        elif args.command == "dashboard":
            serve_dashboard(storage, host=args.host, port=args.port)
        elif args.command == "status":
            _status(storage, as_json=args.as_json)
        elif args.command == "settle":
            pnl = storage.settle_paper_order(args.market_id, won=bool(args.won))
            console.print(f"Paper order settled. Net P&L: [bold]${pnl:.4f}[/bold]")
    except KeyboardInterrupt:
        console.print("\nStopped.")
    except Exception as error:
        error_console.print(f"[bold red]Error:[/bold red] {error}")
        raise SystemExit(1) from error


def _doctor(settings: Settings, storage: Storage, *, as_json: bool) -> None:
    checks: dict[str, Any] = {
        "database": str(storage.path),
        "polymarket_client_version": importlib.metadata.version("polymarket-client"),
        "openai_version": importlib.metadata.version("openai"),
        "astra_model": settings.astra_model,
        "astra_enabled": settings.astra_enabled,
        "observe_max_events": settings.max_events,
        "paper_max_events": settings.paper_max_events,
        "openai_base_url": settings.openai_base_url,
        "openai_transport_secure": settings.openai_base_url.lower().startswith("https://"),
        "openai_fallback_configured": bool(
            settings.openai_fallback_base_url and settings.openai_fallback_api_key
        ),
        "openai_fallback_transport_secure": bool(
            settings.openai_fallback_base_url
            and settings.openai_fallback_base_url.lower().startswith("https://")
        ),
        "openai_key_present": settings.openai_api_key is not None,
        "live_executor_present": False,
        "weathernext": WeatherNextProvider(settings).status().model_dump(mode="json"),
    }
    try:
        geoblock = fetch_geoblock_status(
            url=settings.geoblock_url, timeout=settings.http_timeout_seconds
        )
        checks["geoblock"] = geoblock.model_dump(mode="json", exclude={"ip"})
    except Exception as error:
        checks["geoblock_error"] = str(error)

    try:
        with PolymarketGateway() as gateway:
            events = gateway.discover_weather_events(
                query=settings.market_search_query, max_events=1
            )
        checks["public_market_access"] = True
        checks["sample_events_found"] = len(events)
    except Exception as error:
        checks["public_market_access"] = False
        checks["public_market_error"] = str(error)

    if as_json:
        print(json.dumps(checks, ensure_ascii=False, indent=2, default=str))
        return

    table = Table(title="Polybot doctor")
    table.add_column("Check")
    table.add_column("Value")
    for key, value in checks.items():
        table.add_row(
            key, json.dumps(value, ensure_ascii=False) if isinstance(value, dict) else str(value)
        )
    console.print(table)
    if checks.get("geoblock", {}).get("blocked"):
        console.print("[bold red]Trading is geoblocked from this network.[/bold red]")
    else:
        console.print("[green]Public access works. Live trading is not implemented.[/green]")
    if not checks["openai_transport_secure"]:
        console.print(
            "[bold red]Warning:[/bold red] primary model endpoint uses plain HTTP; "
            "the API key and request text are not protected by TLS."
        )
    if checks["openai_fallback_configured"] and not checks["openai_fallback_transport_secure"]:
        console.print("[bold red]Warning:[/bold red] fallback model endpoint also uses plain HTTP.")


def _scan_once(settings: Settings, storage: Storage, args: argparse.Namespace) -> None:
    paper = bool(args.paper)
    max_events = _resolve_event_limit(settings, requested=args.max_events, paper=paper)
    report = Scanner(settings=settings, storage=storage).scan(
        query=args.query or settings.market_search_query,
        max_events=max_events,
        use_astra=bool(args.astra or settings.astra_enabled),
        paper=paper,
    )
    if args.as_json:
        print(report.model_dump_json(indent=2))
        return
    _print_scan_report(report.model_dump(mode="python"))


def _run(settings: Settings, storage: Storage, args: argparse.Namespace) -> None:
    interval = max(30, int(args.interval))
    paper = bool(args.paper)
    max_events = _resolve_event_limit(settings, requested=args.max_events, paper=paper)
    console.print(
        f"Starting {'paper' if paper else 'observe'} loop every {interval}s "
        f"with up to {max_events} candidate events. "
        "Press Ctrl-C to stop."
    )
    AutonomousRunner(settings=settings, storage=storage).run(
        query=args.query or settings.market_search_query,
        max_events=max_events,
        use_astra=bool(args.astra or settings.astra_enabled),
        paper=paper,
        interval=interval,
    )


def _resolve_event_limit(settings: Settings, *, requested: int | None, paper: bool) -> int:
    limit = (
        requested
        if requested is not None
        else (settings.paper_max_events if paper else settings.max_events)
    )
    if not 1 <= limit <= 20:
        raise ValueError("--max-events must be between 1 and 20")
    return limit


def _status(storage: Storage, *, as_json: bool) -> None:
    summary = storage.portfolio_summary()
    if as_json:
        print(json.dumps(summary, ensure_ascii=False, indent=2, default=str))
        return
    table = Table(title="Paper portfolio")
    table.add_column("Metric")
    table.add_column("Value", justify="right")
    for key in (
        "open_orders",
        "open_exposure_usd",
        "closed_orders",
        "realized_pnl_usd",
        "api_spend_usd",
        "api_reserved_usd",
        "net_project_pnl_after_api_usd",
    ):
        table.add_row(key, str(summary[key]))
        console.print(table)
    if summary["recent_orders"]:
        orders = Table(title="Recent paper orders")
        for column in ("id", "event_id", "market_id", "status", "entry_price", "max_loss_usd"):
            orders.add_column(column)
        for order in summary["recent_orders"]:
            orders.add_row(
                *(
                    str(order[column])
                    for column in (
                        "id",
                        "event_id",
                        "market_id",
                        "status",
                        "entry_price",
                        "max_loss_usd",
                    )
                )
            )
            console.print(orders)


def _diagnostics(settings: Settings, *, as_json: bool) -> None:
    from polybot.forecast_diagnostics_view import build_forecast_diagnostics_view

    report = build_forecast_diagnostics_view(settings.database_path, settings=settings)
    if as_json:
        print(json.dumps(report, ensure_ascii=False, indent=2, default=str))
        return
    summary = cast(dict[str, object], report.get("summary", {}))
    table = Table(title="Forecast diagnostics v1 (read-only)")
    table.add_column("Metric")
    table.add_column("Value", justify="right")
    for key in (
        "settled_trade_count",
        "forecast_available_count",
        "forecast_correct_count",
        "trade_incorrect_count",
        "sigma_created_signal_count",
        "double_conditioning_created_signal_count",
    ):
        table.add_row(key, str(summary.get(key, "—")))
    console.print(table)
    proxy = cast(dict[str, object], report.get("history_sigma_proxy", {}))
    console.print(
        f"Diagnostic sigma proxy: {proxy.get('value_c', '—')}°C · "
        f"state={proxy.get('state', 'unknown')} · {proxy.get('message', '')}"
    )


def _comparison(settings: Settings, *, as_json: bool) -> None:
    from polybot.forecast_comparison import compare_forecasts

    report = compare_forecasts(settings.database_path)
    if as_json:
        print(json.dumps(report, ensure_ascii=False, indent=2, default=str))
        return
    table = Table(title="Forecast comparison (read-only, descriptive)")
    table.add_column("Model")
    table.add_column("Phase")
    table.add_column("Resolved", justify="right")
    table.add_column("MAE", justify="right")
    table.add_column("Exact", justify="right")
    table.add_column("Brier", justify="right")
    table.add_column("State")
    model_rows = report.get("models")
    for model in model_rows if isinstance(model_rows, list) else []:
        if not isinstance(model, dict):
            continue
        phase_rows = model.get("phase_summaries")
        for phase in phase_rows if isinstance(phase_rows, list) else []:
            if not isinstance(phase, dict):
                continue
            metrics = phase.get("metrics", {})
            metrics = metrics if isinstance(metrics, dict) else {}
            table.add_row(
                str(model.get("name", model.get("algorithm_version", "unknown"))),
                str(phase.get("phase", "unknown")),
                str(phase.get("evaluated_event_count", 0)),
                _display_metric(metrics.get("mae")),
                _display_metric(metrics.get("accuracy")),
                _display_metric(metrics.get("brier")),
                str(phase.get("sample_state", "unknown")),
            )
    console.print(table)
    promotion = report.get("promotion", {})
    if isinstance(promotion, dict):
        console.print(
            f"Promotion: {promotion.get('status', 'unknown')} — {promotion.get('reason', '')}"
        )


def _display_metric(value: object) -> str:
    return "—" if value is None else str(value)


def _print_scan_report(report: dict[str, Any]) -> None:
    geo = report.get("geoblock")
    if geo:
        console.print(
            f"Run #{report['run_id']} · geoblocked={geo['blocked']} · "
            f"country={geo.get('country')} · region={geo.get('region')}"
        )
    table = Table(title="Market decisions")
    table.add_column("Action")
    table.add_column("Market")
    table.add_column("p")
    table.add_column("Ask/VWAP")
    table.add_column("Edge")
    table.add_column("EV $")
    table.add_column("Why", overflow="fold")
    for decision in report["decisions"]:
        table.add_row(
            str(decision["action"]),
            str(decision["market_id"]),
            _fmt(decision.get("probability")),
            _fmt(decision.get("executable_price")),
            _fmt(decision.get("probability_edge")),
            _fmt(decision.get("expected_profit_usd")),
            ", ".join(decision["reason_codes"])
            or ", ".join(f"WARN:{code}" for code in decision.get("warning_codes", []))
            or "qualified",
        )
    console.print(table)
    console.print(
        f"Events: {report['events_scanned']}; markets: {report['markets_scanned']}; "
        f"paper orders opened: {report['paper_orders_opened']}; "
        f"settled: {report.get('paper_orders_settled', 0)}"
    )
    for error in report["errors"]:
        console.print(f"[yellow]Warning:[/yellow] {error}")


def _fmt(value: object) -> str:
    if value is None:
        return "—"
    if isinstance(value, (int, float, Decimal, str)):
        try:
            return f"{float(value):.4f}"
        except ValueError:
            return str(value)
    else:
        return str(value)


if __name__ == "__main__":
    main()
