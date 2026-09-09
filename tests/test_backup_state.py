import sqlite3
import subprocess
import sys
from pathlib import Path


def test_backup_script_creates_integrity_checked_copy(tmp_path: Path) -> None:
    source = tmp_path / "source.sqlite3"
    with sqlite3.connect(source) as connection:
        connection.execute("CREATE TABLE sample(value TEXT NOT NULL)")
        connection.execute("INSERT INTO sample VALUES ('preserved')")
    root = tmp_path / "backups"

    subprocess.run(
        [
            sys.executable,
            "scripts/backup-state.py",
            "--database",
            str(source),
            "--root",
            str(root),
        ],
        check=True,
    )

    backups = list(root.glob("*.sqlite3"))
    assert len(backups) == 1
    with sqlite3.connect(backups[0]) as connection:
        assert connection.execute("SELECT value FROM sample").fetchone() == ("preserved",)
        assert connection.execute("PRAGMA integrity_check").fetchone() == ("ok",)
    assert backups[0].with_suffix(".sqlite3.sha256").is_file()


def test_backup_script_prunes_only_its_oldest_named_backups(tmp_path: Path) -> None:
    source = tmp_path / "source.sqlite3"
    with sqlite3.connect(source) as connection:
        connection.execute("CREATE TABLE sample(value TEXT NOT NULL)")
    root = tmp_path / "backups"
    root.mkdir()
    for stamp in ("20260101T000000Z", "20260102T000000Z"):
        backup = root / f"polybot-{stamp}.sqlite3"
        backup.write_bytes(b"old")
        backup.with_suffix(".sqlite3.sha256").write_text("old", encoding="utf-8")
    unrelated = root / "keep-me.sqlite3"
    unrelated.write_bytes(b"unrelated")

    subprocess.run(
        [
            sys.executable,
            "scripts/backup-state.py",
            "--database",
            str(source),
            "--root",
            str(root),
            "--keep",
            "2",
        ],
        check=True,
    )

    assert len(list(root.glob("polybot-*.sqlite3"))) == 2
    assert unrelated.read_bytes() == b"unrelated"
