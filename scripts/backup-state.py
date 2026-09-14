#!/usr/bin/env python3
from __future__ import annotations

import argparse
import fcntl
import hashlib
import os
import shutil
import sqlite3
import tempfile
import time
from collections.abc import Iterator
from contextlib import contextmanager, suppress
from datetime import UTC, datetime
from pathlib import Path

_TEMP_PREFIX = ".polybot-backup-"
_TEMP_SUFFIXES = ("", "-wal", "-shm", "-journal")
_MIN_FREE_MARGIN_BYTES = 256 * 1024 * 1024
_ORPHAN_AGE_SECONDS = 60 * 60


def _backup_files(root: Path) -> list[Path]:
    """Return only complete, named backups (never temporary sidecars)."""

    return sorted(root.glob("polybot-*.sqlite3"), key=lambda path: path.name, reverse=True)


def _remove_backup(path: Path) -> None:
    """Remove a named backup and its checksum, if present."""

    checksum = path.with_suffix(path.suffix + ".sha256")
    for candidate in (path, checksum):
        with suppress(FileNotFoundError):
            candidate.chmod(0o600)
        with suppress(FileNotFoundError):
            candidate.unlink()


def _prune_backups(root: Path, *, keep: int) -> None:
    """Keep at most ``keep`` named backups; unrelated files are untouched."""

    for expired in _backup_files(root)[max(1, keep) :]:
        _remove_backup(expired)


def _temporary_paths(base: Path) -> tuple[Path, ...]:
    return tuple(Path(f"{base}{suffix}") for suffix in _TEMP_SUFFIXES)


def _cleanup_temporary(base: Path) -> None:
    """Remove a temporary backup and SQLite sidecars after its connection closes."""

    for candidate in _temporary_paths(base):
        with suppress(FileNotFoundError):
            candidate.chmod(0o600)
        with suppress(FileNotFoundError):
            candidate.unlink()


def _cleanup_orphaned_temporary(
    root: Path, *, older_than_seconds: int = _ORPHAN_AGE_SECONDS
) -> None:
    """Remove abandoned temporary artifacts, never a named/current database backup.

    A live backup keeps the temporary base file and holds the process lock below.  We
    only clean artifacts older than the grace period, so a crash cannot leave an
    ever-growing set of SQLite ``-wal``/``-shm``/``-journal`` sidecars.
    """

    now = time.time()
    for candidate in root.iterdir():
        if not candidate.name.startswith(_TEMP_PREFIX):
            continue
        try:
            age = now - candidate.stat().st_mtime
        except FileNotFoundError:
            continue
        if age >= older_than_seconds:
            # Derive the temporary base from every supported sidecar suffix.  The
            # base itself is also safe to remove once it is older than the grace
            # period: it is never renamed to a published backup until complete.
            base = candidate
            for suffix in ("-wal", "-shm", "-journal"):
                if candidate.name.endswith(suffix):
                    base = candidate.with_name(candidate.name[: -len(suffix)])
                    break
            _cleanup_temporary(base)


def _estimate_backup_bytes(source: Path) -> int:
    """Conservatively estimate bytes needed for a copy of a WAL database."""

    estimate = source.stat().st_size
    wal = Path(f"{source}-wal")
    if wal.is_file():
        estimate += wal.stat().st_size
    return estimate


def _ensure_free_space(root: Path, source: Path, *, keep: int) -> None:
    """Prune old backups until one new copy plus a safety margin can fit.

    ``keep`` is an upper bound, not a promise: a multi-gigabyte database can make
    the nominal retention count unsafe on a finite disk.  At least one existing
    backup is retained while preparing a new one.
    """

    # Leave a slot for the backup we are about to create.  Keep one known-good
    # backup even when ``keep`` is 1 or a backup subsequently fails.
    _prune_backups(root, keep=max(1, keep - 1))
    estimate = _estimate_backup_bytes(source)
    required = estimate + max(_MIN_FREE_MARGIN_BYTES, estimate // 20)

    backups = _backup_files(root)
    while shutil.disk_usage(root).free < required and len(backups) > 1:
        _remove_backup(backups[-1])
        backups = _backup_files(root)
    available = shutil.disk_usage(root).free
    if available < required:
        raise RuntimeError(
            "insufficient free space for an atomic SQLite backup: "
            f"need at least {required} bytes, have {available}"
        )


@contextmanager
def _backup_lock(root: Path) -> Iterator[None]:
    """Serialize launchd/manual invocations of the backup job."""

    lock_path = root / ".polybot-backup.lock"
    with lock_path.open("a+") as lock:
        try:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise RuntimeError("another Polybot backup is already running") from error
        try:
            yield
        finally:
            fcntl.flock(lock.fileno(), fcntl.LOCK_UN)


def _sha256_file(path: Path) -> str:
    """Hash a large backup without loading it into memory."""

    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


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
    keep = max(1, args.keep)
    with _backup_lock(root):
        _cleanup_orphaned_temporary(root)
        stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
        destination = root / f"polybot-{stamp}.sqlite3"
        if destination.exists():
            return

        _ensure_free_space(root, source, keep=keep)
        fd, temporary_name = tempfile.mkstemp(prefix=_TEMP_PREFIX, suffix=".sqlite3", dir=root)
        os.close(fd)
        temporary = Path(temporary_name)
        try:
            with (
                sqlite3.connect(source) as source_connection,
                sqlite3.connect(temporary) as target_connection,
            ):
                source_connection.backup(target_connection)
                # ``backup()`` copies the source's WAL mode to the target.  Switch
                # back to DELETE before closing so no data-bearing WAL is stranded
                # beside the published backup; the remaining empty sidecar is
                # removed by ``_cleanup_temporary`` below.
                target_connection.commit()
                journal_mode = target_connection.execute("PRAGMA journal_mode=DELETE").fetchone()
                if journal_mode is None or str(journal_mode[0]).lower() != "delete":
                    raise RuntimeError(f"could not finalize backup journal mode: {journal_mode}")
                result = target_connection.execute("PRAGMA integrity_check").fetchone()
                if result is None or result[0] != "ok":
                    raise RuntimeError(f"backup integrity check failed: {result}")
            digest = _sha256_file(temporary)
            temporary.chmod(0o400)
            temporary.rename(destination)
            checksum = destination.with_suffix(destination.suffix + ".sha256")
            checksum.write_text(f"{digest}  {destination.name}\n", encoding="utf-8")
            checksum.chmod(0o400)
            _prune_backups(root, keep=keep)
        finally:
            # This also removes ``-wal``, ``-shm`` and ``-journal`` artifacts left
            # behind when sqlite3.backup() fails halfway through.
            _cleanup_temporary(temporary)


if __name__ == "__main__":
    main()
