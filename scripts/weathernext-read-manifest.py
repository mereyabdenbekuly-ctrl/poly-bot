#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from datetime import date, datetime
from pathlib import Path

from polybot.config import Settings
from polybot.weathernext import WeatherNextGcsClient
from polybot.weathernext_manifest import (
    estimate_and_build_full_ensemble_read_manifest,
    estimate_and_build_full_ensemble_read_manifest_batch,
    write_full_ensemble_read_manifest,
)


def _aware_datetime(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise argparse.ArgumentTypeError("--init-time must include a UTC offset")
    return parsed


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Create a metadata-only, fail-closed WeatherNext full-ensemble read manifest. "
            "This command never downloads forecast chunk bodies."
        )
    )
    parser.add_argument("--target-file", type=Path, help="JSON array of station/day targets")
    parser.add_argument("--station-id")
    parser.add_argument("--latitude", type=float)
    parser.add_argument("--longitude", type=float)
    parser.add_argument("--location")
    parser.add_argument("--date", type=date.fromisoformat, dest="observation_date")
    parser.add_argument("--timezone")
    parser.add_argument("--init-time", type=_aware_datetime)
    parser.add_argument("--max-network-bytes", type=int)
    parser.add_argument("--max-objects", type=int, default=4096)
    parser.add_argument("--max-object-bytes", type=int)
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("/var/lib/polybot/weathernext/full/read-manifest.json"),
    )
    args = parser.parse_args()

    client = WeatherNextGcsClient(Settings())
    if args.target_file is not None:
        payload = json.loads(args.target_file.read_text(encoding="utf-8"))
        if not isinstance(payload, list):
            raise SystemExit("--target-file must contain a JSON array")
        targets = [item for item in payload if isinstance(item, dict)]
        if len(targets) != len(payload):
            raise SystemExit("every target-file item must be a JSON object")
        manifest = estimate_and_build_full_ensemble_read_manifest_batch(
            client,
            targets=targets,
            init_time_utc=args.init_time,
            max_network_bytes=args.max_network_bytes,
            max_objects=args.max_objects,
            max_object_bytes=args.max_object_bytes,
        )
    else:
        required = {
            "--station-id": args.station_id,
            "--latitude": args.latitude,
            "--longitude": args.longitude,
            "--location": args.location,
            "--date": args.observation_date,
            "--timezone": args.timezone,
        }
        missing = [name for name, value in required.items() if value is None]
        if missing:
            parser.error("single-target mode requires " + ", ".join(missing))
        manifest = estimate_and_build_full_ensemble_read_manifest(
            client,
            latitude=args.latitude,
            longitude=args.longitude,
            location=args.location,
            station_id=args.station_id,
            observation_date=args.observation_date,
            timezone_name=args.timezone,
            init_time_utc=args.init_time,
            max_network_bytes=args.max_network_bytes,
            max_objects=args.max_objects,
            max_object_bytes=args.max_object_bytes,
        )
    path = write_full_ensemble_read_manifest(manifest, args.output)
    print(
        json.dumps(
            {
                "path": str(path),
                "manifest_sha256": manifest.manifest_sha256,
                "state": manifest.approval_gate.state,
                "payload_read": manifest.payload_read,
                "expected_network_bytes": manifest.approval_gate.expected_network_bytes,
                "max_network_bytes": manifest.approval_gate.max_network_bytes,
                "object_count": manifest.approval_gate.object_count,
                "max_objects": manifest.approval_gate.max_objects,
                "target_count": len(manifest.targets),
                "largest_compressed_object_bytes": (
                    manifest.approval_gate.largest_compressed_object_bytes
                ),
                "snapshot_target_path": manifest.snapshot_target_path,
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
