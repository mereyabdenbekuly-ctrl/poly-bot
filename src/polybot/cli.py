from __future__ import annotations

import argparse
import importlib.metadata
import json
import time
from decimal import Decimal
from typing import Any

from rich.console import Console
from rich.table import Table

from polybot.config import Settings
from polybot.geoblock import fetch_geoblock_status
from polybot.polymarket_gateway import PolymarketGateway
from polybot.scanner import Scanner
from polybot.storage import Storage

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

    status = subparsers.add_parser("status", help="Show paper portfolio and API spend")
    status.add_argument("--json", action="store_true", dest="as_json")

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
    settings.ensure_runtime_directories()
    storage = Storage(settings.database_path)

    try:
        if args.command == "doctor":
            _doctor(settings, storage, as_json=args.as_json)
        elif args.command == "scan":
            _scan_once(settings, storage, args)
        elif args.command == "run":
            _run(settings, storage, args)
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
    max_events = args.max_events or settings.max_events
    if not 1 <= max_events <= 20:
        raise ValueError("--max-events must be between 1 and 20")
    report = Scanner(settings=settings, storage=storage).scan(
        query=args.query or settings.market_search_query,
        max_events=max_events,
        use_astra=bool(args.astra or settings.astra_enabled),
        paper=bool(args.paper),
    )
    if args.as_json:
        print(report.model_dump_json(indent=2))
        return
    _print_scan_report(report.model_dump(mode="python"))


def _run(settings: Settings, storage: Storage, args: argparse.Namespace) -> None:
    interval = max(30, int(args.interval))
    console.print(
        f"Starting {'paper' if args.paper else 'observe'} loop every {interval}s. "
        "Press Ctrl-C to stop."
    )
    while True:
        started = time.monotonic()
        try:
            _scan_once(settings, storage, args)
        except SystemExit:
            raise
        except Exception as error:
            console.print(f"[red]Scan failed:[/red] {error}")
        elapsed = time.monotonic() - started
        time.sleep(max(1, interval - elapsed))


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
        f"paper orders opened: {report['paper_orders_opened']}"
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
