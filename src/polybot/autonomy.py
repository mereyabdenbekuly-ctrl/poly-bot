from __future__ import annotations

import time
from collections.abc import Callable
from datetime import UTC, datetime

from polybot.config import Settings
from polybot.forecast_store import ForecastStore
from polybot.models import RuntimeWindow, ScanReport
from polybot.scanner import Scanner
from polybot.storage import Storage
from polybot.weathernext import WeatherNextProvider


class AutonomousRunner:
    """Run paper/observe cycles forever and persist window reports.

    A runtime window is a reporting boundary only. API budget, database state,
    and paper positions are never reset when a 120-minute window rolls over.
    """

    def __init__(
        self,
        *,
        settings: Settings,
        storage: Storage,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self.settings = settings
        self.storage = storage
        self.weathernext = WeatherNextProvider(settings)
        self.forecast_store = ForecastStore(storage.path)
        self._clock = clock or (lambda: datetime.now(UTC))

    def run(
        self,
        *,
        query: str,
        max_events: int,
        use_astra: bool,
        paper: bool,
        interval: int,
    ) -> None:
        interval = max(30, int(interval))
        # No scan can belong to this brand-new runner yet. Any persisted
        # ``running`` row was abandoned by a previous process.
        self.storage.recover_stale_scans(older_than_seconds=0)
        self.storage.recover_stale_shadow_research()
        window = self.storage.start_runtime_window(
            query=query,
            interval_seconds=interval,
            paper=paper,
            astra=use_astra,
        )
        self._ensure_startup_report(window.id, paper=paper, astra=use_astra)

        scanner = Scanner(
            settings=self.settings,
            storage=self.storage,
            background_research=True,
        )
        try:
            while True:
                window, rolled = self.process_reporting_boundaries(
                    window,
                    query=query,
                    interval=interval,
                    paper=paper,
                    astra=use_astra,
                    scan_report=None,
                )
                if rolled:
                    continue

                cycle_started = time.monotonic()
                scan_report = None
                try:
                    scan_report = scanner.scan(
                        query=query,
                        max_events=max_events,
                        use_astra=use_astra,
                        paper=paper,
                        window_id=window.id,
                    )
                    elapsed = self._elapsed(window)
                    self._record_report(window.id, "CYCLE", elapsed, scan_report, None)
                except Exception as error:
                    elapsed = self._elapsed(window)
                    self._record_report(window.id, "CYCLE", elapsed, None, str(error))

                window, _ = self.process_reporting_boundaries(
                    window,
                    query=query,
                    interval=interval,
                    paper=paper,
                    astra=use_astra,
                    scan_report=scan_report,
                )

                time.sleep(max(1, interval - (time.monotonic() - cycle_started)))
        finally:
            scanner.close(wait=False)

    def process_reporting_boundaries(
        self,
        window: RuntimeWindow,
        *,
        query: str,
        interval: int,
        paper: bool,
        astra: bool,
        scan_report: ScanReport | None,
    ) -> tuple[RuntimeWindow, bool]:
        """Persist due reports and roll a completed two-hour window."""

        elapsed = self._elapsed(window)
        if elapsed >= 3600:
            self._record_report(window.id, "INTERIM_60M", elapsed, scan_report, None)
        if elapsed < 7200:
            return window, False
        self._record_report(window.id, "COMPLETE_120M", elapsed, scan_report, None)
        self.storage.finish_runtime_window(window.id)
        next_window = self.storage.start_runtime_window(
            query=query,
            interval_seconds=interval,
            paper=paper,
            astra=astra,
        )
        self._ensure_startup_report(next_window.id, paper=paper, astra=astra)
        return next_window, True

    def _elapsed(self, window: RuntimeWindow) -> int:
        return max(
            0,
            int(
                (self._clock().astimezone(UTC) - window.started_at.astimezone(UTC)).total_seconds()
            ),
        )

    def _ensure_startup_report(self, window_id: int, *, paper: bool, astra: bool) -> None:
        if self.storage.runtime_report_exists(window_id, "STARTUP"):
            return
        self.storage.record_runtime_report(
            window_id,
            kind="STARTUP",
            elapsed_seconds=0,
            payload={
                "mode": "paper" if paper else "observe",
                "astra_enabled": astra,
                "weathernext": self.weathernext.status().model_dump(mode="json"),
                "message": "Autonomous loop started; browser and terminal are not required.",
            },
        )

    def _record_report(
        self,
        window_id: int,
        kind: str,
        elapsed: int,
        scan_report: ScanReport | None,
        error: str | None,
    ) -> None:
        if kind != "CYCLE" and self.storage.runtime_report_exists(window_id, kind):
            return
        payload: dict[str, object] = {
            "weathernext": self.weathernext.status().model_dump(mode="json"),
            "weathernext_paper": self.storage.weathernext_paper_summary(),
            "portfolio": self.storage.portfolio_summary(),
            "window": self.storage.runtime_window_summary(window_id),
            "forecast_engine": self._forecast_report(kind),
        }
        if scan_report is not None:
            payload["scan"] = scan_report.model_dump(mode="json")
        if error is not None:
            payload["error"] = error
        self.storage.record_runtime_report(
            window_id,
            kind=kind,
            elapsed_seconds=elapsed,
            payload=payload,
        )

    def _forecast_report(self, kind: str) -> dict[str, object]:
        """Persist a compact technical snapshot at every reporting boundary."""

        report: dict[str, object] = {
            "counts": self.forecast_store.counts(),
            "source_statuses": self.forecast_store.latest_source_statuses(),
        }
        if kind in {"INTERIM_60M", "COMPLETE_120M"}:
            comparison = self.forecast_store.dashboard_summary(event_limit=1)
            outcomes = comparison.get("outcomes", [])
            report.update(
                {
                    "metrics": comparison.get("metrics", []),
                    "station_metrics": comparison.get("station_metrics", []),
                    "outcome_count": len(outcomes) if isinstance(outcomes, list) else 0,
                    "eligible_event_count": comparison.get(
                        "eligible_event_count",
                        comparison.get("available_event_count", 0),
                    ),
                }
            )
        return report
