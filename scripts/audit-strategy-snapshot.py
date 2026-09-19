#!/usr/bin/env python3
"""Audit JSON exports locally; never connect to a wallet or trading API."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from polybot.strategy_audit import build_strategy_audit


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dashboard", type=Path, required=True)
    parser.add_argument("--comparison", type=Path, required=True)
    args = parser.parse_args()
    dashboard = json.loads(args.dashboard.read_text(encoding="utf-8"))
    comparison = json.loads(args.comparison.read_text(encoding="utf-8"))
    print(json.dumps(build_strategy_audit(dashboard, comparison), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
