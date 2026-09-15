#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from dataclasses import asdict
from datetime import UTC, datetime, timedelta
from pathlib import Path

from polybot.ecmwf import EcmwfArchiveRetentionPolicy, EcmwfIfsEnsAdapter, EcmwfProduct
from polybot.forecast_store import ForecastStore


def _latest_main_cycle(now: datetime) -> datetime:
    candidate = now.astimezone(UTC) - timedelta(hours=9)
    hour = 12 if candidate.hour >= 12 else 0
    return candidate.replace(hour=hour, minute=0, second=0, microsecond=0)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Archive official ECMWF IFS ENS mx2t3 byte ranges."
    )
    parser.add_argument("--database", required=True)
    parser.add_argument("--archive-root", required=True)
    parser.add_argument("--hours", type=int, default=24)
    args = parser.parse_args()
    hours = max(3, min(144, args.hours))
    hours -= hours % 3
    steps = tuple(range(3, hours + 1, 3))
    now = datetime.now(UTC)
    init = _latest_main_cycle(now)
    store = ForecastStore(Path(args.database))
    adapter = EcmwfIfsEnsAdapter(
        archive_root=Path(args.archive_root),
        retention_policy=EcmwfArchiveRetentionPolicy(),
    )
    result = adapter.fetch_archive(
        init_time_utc=init,
        steps=steps,
        product=EcmwfProduct.DAILY_MAX_2T,
    )
    payload: dict[str, object] = {
        "product": result.status.product.value,
        "parameter": result.status.parameter,
        "init_time_utc": result.status.init_time_utc.isoformat(),
        "published_at_utc": (
            None
            if result.status.published_at_utc is None
            else result.status.published_at_utc.isoformat()
        ),
        "steps": list(result.status.steps),
        "member_count": result.status.member_count,
        "message": result.status.message,
        "archive_id": None if result.archive is None else result.archive.archive_id,
        "archive_path": (
            None if result.archive is None else str(result.archive.archive_path)
        ),
        "artifact_count": 0 if result.archive is None else len(result.archive.artifacts),
        "decoded": False,
        "scope": "raw official index/GRIB ranges; station decoding is separate",
        "retention": None if result.retention is None else asdict(result.retention),
    }
    store.record_source_status(
        source="ecmwf-open-data-ifs-ens",
        state=result.status.state.value,
        checked_at_utc=result.status.checked_at_utc,
        payload=payload,
    )
    print(json.dumps({"state": result.status.state.value, **payload}, indent=2))


if __name__ == "__main__":
    main()
