from __future__ import annotations

import argparse
import importlib.metadata
import json
from decimal import Decimal
from typing import Any, cast

from rich.console import Console
from rich.table import Table

from polybot.autonomy import AutonomousRunner
from polybot.config import Settings, is_protected_model_endpoint
from polybot.dashboard import serve_dashboard
from polybot.geoblock import fetch_geoblock_status
from polybot.operational_report import build_operational_report
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

    operational = subparsers.add_parser(
        "operational-report", help="Read-only rolling CPU/RAM/disk and cycle report"
    )
    operational.add_argument("--hours", type=float, default=24.0)
    operational.add_argument("--output", default=None)
    operational.add_argument("--json", action="store_true", dest="as_json")

    wn = subparsers.add_parser("weathernext", help="WeatherNext3 GCS comparison source")
    wn_sub = wn.add_subparsers(dest="weathernext_command", required=True)
    wn_check = wn_sub.add_parser("check", help="Verify ADC and Requester Pays access")
    wn_check.add_argument("--json", action="store_true", dest="as_json")
    wn_refresh = wn_sub.add_parser("refresh", help="Pull a fresh snapshot from GCS")
    wn_refresh.add_argument("--latitude", type=float, required=True)
    wn_refresh.add_argument("--longitude", type=float, required=True)
    wn_refresh.add_argument("--location", required=True)
    wn_refresh.add_argument("--date", required=True, dest="observation_date")
    wn_refresh.add_argument(
        "--timezone",
        default="UTC",
        help="IANA timezone for the station-local observation date (default: UTC)",
    )
    wn_refresh.add_argument(
        "--allow-large-read",
        action="store_true",
        help="Override the raw full-ensemble transfer safety ceiling",
    )
    wn_refresh.add_argument("--output", default=None)
    wn_raw_estimate = wn_sub.add_parser(
        "raw-estimate",
        aliases=["estimate"],
        help="Estimate a raw full-ensemble point/day transfer using metadata only",
    )
    wn_raw_estimate.add_argument("--latitude", type=float, required=True)
    wn_raw_estimate.add_argument("--longitude", type=float, required=True)
    wn_raw_estimate.add_argument("--location", required=True)
    wn_raw_estimate.add_argument("--date", required=True, dest="observation_date")
    wn_raw_estimate.add_argument(
        "--timezone",
        default="UTC",
        help="IANA timezone for the station-local observation date (default: UTC)",
    )
    wn_raw_estimate.add_argument("--json", action="store_true", dest="as_json")
    wn_raw_estimate.add_argument(
        "--output",
        default=None,
        help="Optional local JSON path for the metadata-only provenance report",
    )
    wn_stats_check = wn_sub.add_parser(
        "statistics-check",
        aliases=["stats-check"],
        help="Verify official statistics-surface access",
    )
    wn_stats_check.add_argument("--json", action="store_true", dest="as_json")
    wn_stats_refresh = wn_sub.add_parser(
        "statistics-refresh",
        aliases=["stats-refresh", "summary-refresh"],
        help="Pull one bounded SUMMARY_ONLY statistics snapshot",
    )
    wn_stats_refresh.add_argument("--latitude", type=float, required=True)
    wn_stats_refresh.add_argument("--longitude", type=float, required=True)
    wn_stats_refresh.add_argument("--location", required=True)
    wn_stats_refresh.add_argument(
        "--station-id",
        default=None,
        help="Optional ICAO/station identifier kept with the summary provenance",
    )
    wn_stats_refresh.add_argument("--date", required=True, dest="observation_date")
    wn_stats_refresh.add_argument(
        "--timezone",
        default="UTC",
        help="IANA timezone for the station-local observation date (default: UTC)",
    )
    wn_stats_refresh.add_argument(
        "--hours",
        type=int,
        default=None,
        help=(
            "Number of earliest valid hours to retain from the station-local window "
            "(bounded by configured read limit)"
        ),
    )
    wn_stats_refresh.add_argument(
        "--include-past",
        action="store_true",
        help="Include already elapsed hours in the station-local day",
    )
    wn_stats_refresh.add_argument("--output", default=None)

    wn_first_trial = wn_sub.add_parser(
        "first-full-trial-plan",
        aliases=["first-trial-plan"],
        help=(
            "Select real strictly-future market targets with common UTC coverage and "
            "build a frozen metadata-only manifest"
        ),
    )
    wn_first_trial.add_argument("--database", default=None)
    wn_first_trial.add_argument("--root", default=None)
    wn_first_trial.add_argument("--max-targets", type=int, default=None)
    wn_first_trial.add_argument("--probe-result", default=None)
    wn_first_trial.add_argument("--json", action="store_true", dest="as_json")

    wn_auto = wn_sub.add_parser(
        "autonomous-refresh",
        aliases=["refresh-auto", "preflight"],
        help=(
            "Derive bounded targets, refresh a metadata-only manifest, and—only "
            "when explicitly enabled plus sidecar-approved—read it sequentially"
        ),
    )
    wn_auto.add_argument("--database", default=None)
    wn_auto.add_argument("--root", default=None)
    wn_auto.add_argument("--targets", default=None)
    wn_auto.add_argument("--manifest", default=None)
    wn_auto.add_argument("--approval", default=None)
    wn_auto.add_argument("--probe-approval", default=None)
    wn_auto.add_argument("--status", default=None)
    wn_auto.add_argument("--index", default=None)
    wn_auto.add_argument("--snapshot-root", default=None)
    wn_auto.add_argument("--max-targets", type=int, default=None)
    wn_auto.add_argument(
        "--read-approved",
        action="store_true",
        help=(
            "Read the existing manifest only after local sidecar verification; do not "
            "refresh or replace its digest first"
        ),
    )
    wn_auto.add_argument(
        "--require-strictly-future-targets",
        action="store_true",
        help="Require station-local midnight to remain in the future for the first full trial",
    )
    wn_auto.add_argument(
        "--probe-one-block",
        action="store_true",
        help=(
            "Use the separate one-shot probe sidecar to read and decode exactly one "
            "compressed block; do not refresh the manifest or publish snapshots"
        ),
    )
    wn_auto.add_argument("--probe-manifest-sha256", default=None)
    wn_auto.add_argument("--probe-object-uri", default=None)
    wn_auto.add_argument("--probe-object-bytes", type=int, default=None)
    wn_auto.add_argument("--probe-max-network-bytes", type=int, default=None)
    wn_auto.add_argument("--probe-max-object-bytes", type=int, default=None)
    wn_auto.add_argument("--probe-temp-root", default=None)
    wn_auto.add_argument("--json", action="store_true", dest="as_json")

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
    if args.command == "weathernext":
        try:
            from datetime import date as _date

            from polybot.weathernext import (
                WeatherNextGcsClient,
                WeatherNextProvider,
                WeatherNextStatisticsGcsClient,
            )

            provider = WeatherNextProvider(settings)
            if args.weathernext_command in {
                "first-full-trial-plan",
                "first-trial-plan",
            }:
                from pathlib import Path

                from polybot.weathernext_autonomy import plan_first_full_trial

                root = Path(args.root).expanduser() if args.root else settings.weathernext_full_root
                database = (
                    Path(args.database).expanduser()
                    if args.database
                    else settings.database_path
                )
                status = plan_first_full_trial(
                    database,
                    settings=settings,
                    root=root,
                    max_targets=args.max_targets,
                    probe_result_path=(
                        Path(args.probe_result).expanduser() if args.probe_result else None
                    ),
                )
                print(
                    json.dumps(
                        {"status": status.model_dump(mode="json")},
                        ensure_ascii=False,
                        indent=2,
                        default=str,
                    )
                )
                return
            if args.weathernext_command in {
                "autonomous-refresh",
                "refresh-auto",
                "preflight",
            }:
                from pathlib import Path

                from polybot.weathernext_autonomy import (
                    DEFAULT_APPROVAL_PATH,
                    DEFAULT_MANIFEST_PATH,
                    DEFAULT_PROBE_APPROVAL_PATH,
                    DEFAULT_PROBE_ATTEMPT_PATH,
                    DEFAULT_ROOT,
                    DEFAULT_STATUS_PATH,
                    DEFAULT_TARGETS_PATH,
                    autonomous_refresh_preflight,
                    read_approved_manifest_sequentially,
                )

                root = Path(args.root).expanduser() if args.root else settings.weathernext_full_root
                database = (
                    Path(args.database).expanduser()
                    if args.database
                    else settings.database_path
                )
                targets = (
                    Path(args.targets).expanduser()
                    if args.targets
                    else root / DEFAULT_TARGETS_PATH.relative_to(DEFAULT_ROOT)
                )
                manifest = (
                    Path(args.manifest).expanduser()
                    if args.manifest
                    else root / DEFAULT_MANIFEST_PATH.relative_to(DEFAULT_ROOT)
                )
                approval = (
                    Path(args.approval).expanduser()
                    if args.approval
                    else root / DEFAULT_APPROVAL_PATH.relative_to(DEFAULT_ROOT)
                )
                probe_approval = (
                    Path(args.probe_approval).expanduser()
                    if args.probe_approval
                    else root / DEFAULT_PROBE_APPROVAL_PATH.relative_to(DEFAULT_ROOT)
                )
                probe_attempt = root / DEFAULT_PROBE_ATTEMPT_PATH.relative_to(DEFAULT_ROOT)
                status_path = (
                    Path(args.status).expanduser()
                    if args.status
                    else root / DEFAULT_STATUS_PATH.relative_to(DEFAULT_ROOT)
                )
                index = (
                    Path(args.index).expanduser()
                    if args.index
                    else Path(
                        settings.weathernext_snapshot_index_path
                        or root / "latest-index.json"
                    ).expanduser()
                )
                snapshot_root = (
                    Path(args.snapshot_root).expanduser()
                    if args.snapshot_root
                    else root / "snapshots"
                )
                if args.probe_one_block:
                    if not args.read_approved:
                        raise ValueError("--probe-one-block requires --read-approved")
                    required_probe_args = {
                        "--probe-manifest-sha256": args.probe_manifest_sha256,
                        "--probe-object-uri": args.probe_object_uri,
                        "--probe-object-bytes": args.probe_object_bytes,
                        "--probe-max-network-bytes": args.probe_max_network_bytes,
                        "--probe-max-object-bytes": args.probe_max_object_bytes,
                    }
                    missing_probe_args = [
                        name for name, value in required_probe_args.items() if value is None
                    ]
                    if missing_probe_args:
                        raise ValueError(
                            "--probe-one-block requires " + ", ".join(missing_probe_args)
                        )
                    result = read_approved_manifest_sequentially(
                        settings,
                        manifest_path=manifest,
                        approval_path=approval,
                        index_path=index,
                        snapshot_root=snapshot_root,
                        probe_only=True,
                        probe_approval_path=probe_approval,
                        probe_attempt_path=probe_attempt,
                        expected_probe_manifest_sha256=args.probe_manifest_sha256,
                        expected_probe_object_uri=args.probe_object_uri,
                        expected_probe_object_compressed_bytes=args.probe_object_bytes,
                        expected_probe_max_network_bytes=args.probe_max_network_bytes,
                        expected_probe_max_object_bytes=args.probe_max_object_bytes,
                        probe_temporary_root=(
                            Path(args.probe_temp_root).expanduser()
                            if args.probe_temp_root
                            else None
                        ),
                    )
                    response = {"read": result.model_dump(mode="json")}
                    print(json.dumps(response, ensure_ascii=False, indent=2, default=str))
                    return
                if args.read_approved:
                    if not settings.weathernext_full_refresh_enabled:
                        raise RuntimeError(
                            "approved full read remains disabled by "
                            "POLYBOT_WEATHERNEXT_FULL_REFRESH_ENABLED"
                        )
                    result = read_approved_manifest_sequentially(
                        settings,
                        manifest_path=manifest,
                        approval_path=approval,
                        index_path=index,
                        snapshot_root=snapshot_root,
                        probe_only=False,
                        require_strictly_future_targets=(
                            args.require_strictly_future_targets
                        ),
                    )
                    response = {"read": result.model_dump(mode="json")}
                    print(json.dumps(response, ensure_ascii=False, indent=2, default=str))
                    return
                status = autonomous_refresh_preflight(
                    database,
                    settings=settings,
                    targets_path=targets,
                    manifest_path=manifest,
                    approval_path=approval,
                    status_path=status_path,
                    max_targets=args.max_targets or settings.weathernext_full_max_targets,
                    max_network_bytes=settings.weathernext_full_max_network_bytes,
                    max_objects=settings.weathernext_full_max_objects,
                    max_object_bytes=settings.weathernext_full_max_object_bytes,
                )
                response: dict[str, object] = {"status": status.model_dump(mode="json")}
                if args.as_json:
                    print(json.dumps(response, ensure_ascii=False, indent=2, default=str))
                else:
                    print(json.dumps(response, ensure_ascii=False, indent=2, default=str))
                return
            if args.weathernext_command in {"raw-estimate", "estimate"}:
                from pathlib import Path

                report = WeatherNextGcsClient(settings).estimate_point_day_read(
                    latitude=args.latitude,
                    longitude=args.longitude,
                    location=args.location,
                    observation_date=_date.fromisoformat(args.observation_date),
                    timezone_name=args.timezone,
                )
                if args.output:
                    output = Path(args.output).expanduser()
                    output.parent.mkdir(parents=True, exist_ok=True)
                    temporary = output.with_name(output.name + ".tmp")
                    temporary.write_text(
                        json.dumps(report, ensure_ascii=False, indent=2, default=str),
                        encoding="utf-8",
                    )
                    temporary.replace(output)
                if args.as_json:
                    print(json.dumps(report, indent=2, default=str))
                else:
                    for key, value in report.items():
                        print(f"{key}: {value}")
                return
            if args.weathernext_command in {"statistics-check", "stats-check"}:
                client = WeatherNextStatisticsGcsClient(settings)
                report = client.check_access()
                if args.as_json:
                    print(json.dumps(report, indent=2, default=str))
                else:
                    for key, value in report.items():
                        print(f"{key}: {value}")
                return
            if args.weathernext_command == "check":
                client = WeatherNextGcsClient(settings)
                report = client.check_access()
                if args.as_json:
                    print(json.dumps(report, indent=2, default=str))
                else:
                    for key, value in report.items():
                        print(f"{key}: {value}")
                return
            if args.weathernext_command in {
                "statistics-refresh",
                "stats-refresh",
                "summary-refresh",
            }:
                from pathlib import Path

                snapshot = provider.refresh_statistics_from_gcs(
                    latitude=args.latitude,
                    longitude=args.longitude,
                    location=args.location,
                    station_id=args.station_id,
                    observation_date=_date.fromisoformat(args.observation_date),
                    timezone_name=args.timezone,
                    max_hours=args.hours,
                    include_past_hours=args.include_past,
                    output_path=None if args.output is None else Path(args.output),
                )
                print(snapshot.model_dump_json(indent=2))
                return
            snapshot = provider.refresh_from_gcs(
                latitude=args.latitude,
                longitude=args.longitude,
                location=args.location,
                observation_date=_date.fromisoformat(args.observation_date),
                timezone_name=args.timezone,
                allow_large_read=args.allow_large_read,
                output_path=args.output,
            )
            print(snapshot.model_dump_json(indent=2))
        except Exception as error:
            error_console.print(f"[bold red]Error:[/bold red] {error}")
            raise SystemExit(1) from error
        return

    if args.command == "diagnostics":
        _diagnostics(settings, as_json=args.as_json)
        return
    if args.command == "comparison":
        _comparison(settings, as_json=args.as_json)
        return
    if args.command == "operational-report":
        from pathlib import Path

        report = build_operational_report(
            settings.database_path,
            state_root=settings.database_path.parent,
            ecmwf_json_root=settings.ecmwf_json_archive_root,
            weathernext_full_root=settings.weathernext_full_root,
            weathernext_snapshot_index_path=(
                None
                if settings.weathernext_snapshot_index_path is None
                else Path(settings.weathernext_snapshot_index_path)
            ),
            weathernext_full_snapshot_path=(
                None
                if settings.weathernext_snapshot_path is None
                else Path(settings.weathernext_snapshot_path)
            ),
            weathernext_statistics_snapshot_path=(
                None
                if settings.weathernext_statistics_snapshot_path is None
                else Path(settings.weathernext_statistics_snapshot_path)
            ),
            period_hours=args.hours,
        )
        if args.output:
            output = Path(args.output).expanduser()
            output.parent.mkdir(parents=True, exist_ok=True)
            temporary = output.with_name(output.name + ".tmp")
            temporary.write_text(
                json.dumps(report, ensure_ascii=False, indent=2, default=str),
                encoding="utf-8",
            )
            temporary.replace(output)
        if args.as_json:
            print(json.dumps(report, ensure_ascii=False, indent=2, default=str))
        else:
            from polybot.operational_report import _human_report

            print(_human_report(report))
        return
    if args.command == "dashboard":
        storage = Storage(settings.database_path, read_only=True)
    else:
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
        "openai_transport_secure": is_protected_model_endpoint(settings.openai_base_url),
        "openai_fallback_configured": bool(
            settings.openai_fallback_base_url and settings.openai_fallback_api_key
        ),
        "openai_fallback_transport_secure": bool(
            is_protected_model_endpoint(settings.openai_fallback_base_url)
        ),
        "openai_key_present": settings.openai_api_key is not None,
        "live_executor_present": True,
        "live_executor_enabled": False,
        "live_executor_mode": "library_only_one_shot_fok",
        "live_executor_cli_present": False,
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
