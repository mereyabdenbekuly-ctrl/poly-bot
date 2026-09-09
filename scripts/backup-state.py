#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import os
import sqlite3
import tempfile
from datetime import UTC, datetime
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser(description="Create an atomic SQLite state backup.")
    parser.add_argument("--database", required=True)
    parser.add_argument("--root", required=True)
    parser.add_argument("--keep", type=int, default=96)
    args = parser.parse_args()

    source = Path(args.database).expanduser().resolve()
    root = Path(args.root).expanduser().resolve()
    if not source.is_file():
        raise SystemExit(f"database does not exist: {source}")
    root.mkdir(parents=True, exist_ok=True, mode=0o700)
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    destination = root / f"polybot-{stamp}.sqlite3"
    if destination.exists():
        return

    fd, temporary_name = tempfile.mkstemp(prefix=".polybot-backup-", suffix=".sqlite3", dir=root)
    os.close(fd)
    temporary = Path(temporary_name)
    try:
        with (
            sqlite3.connect(source) as source_connection,
            sqlite3.connect(temporary) as target_connection,
        ):
            source_connection.backup(target_connection)
            result = target_connection.execute("PRAGMA integrity_check").fetchone()
            if result is None or result[0] != "ok":
                raise RuntimeError(f"backup integrity check failed: {result}")
        body = temporary.read_bytes()
        digest = hashlib.sha256(body).hexdigest()
        temporary.chmod(0o400)
        temporary.rename(destination)
        checksum = destination.with_suffix(destination.suffix + ".sha256")
        checksum.write_text(f"{digest}  {destination.name}\n", encoding="utf-8")
        checksum.chmod(0o400)
        backups = sorted(root.glob("polybot-*.sqlite3"), reverse=True)
        for expired in backups[max(1, args.keep) :]:
            checksum_path = expired.with_suffix(expired.suffix + ".sha256")
            expired.chmod(0o600)
            expired.unlink()
            if checksum_path.exists():
                checksum_path.chmod(0o600)
                checksum_path.unlink()
    finally:
        if temporary.exists():
            temporary.unlink()


if __name__ == "__main__":
    main()
