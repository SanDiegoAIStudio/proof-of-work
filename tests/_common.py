"""Shared helpers for the test files. Every test works in a fresh temp folder."""

from __future__ import annotations

import contextlib
import io
import json
import os
import shutil
import sys
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve().parent.parent
if str(HERE) not in sys.path:
    sys.path.insert(0, str(HERE))

import pow as P  # noqa: E402


def ts(hours_ago: float) -> str:
    """An ISO timestamp this many hours in the past, in the ledger's format."""
    when = datetime.now(timezone.utc) - timedelta(hours=hours_ago)
    return when.isoformat(timespec="seconds").replace("+00:00", "Z")


def run_main(argv: list[str]) -> tuple[int, str, str]:
    """Call pow.main in this process and return (exit code, stdout, stderr)."""
    out, err = io.StringIO(), io.StringIO()
    with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
        code = P.main(argv)
    return code, out.getvalue(), err.getvalue()


def child_env(spine: Path, **extra: str) -> dict[str, str]:
    """An environment for a child process that cannot touch the repo's own spine.

    The rollup file is redirected to a path beside the temporary spine, so a
    child `rollup` can never write into the repo. The registry is left unset
    on purpose: the default (jobs.json beside pow.py) is part of what is tested.
    """
    env = dict(os.environ)
    for name in ("POW_SPINE_DIR", "POW_REGISTRY", "POW_ROLLUP", "POW_HEARTBEAT_DIR"):
        env.pop(name, None)
    env["POW_SPINE_DIR"] = str(spine)
    env["POW_ROLLUP"] = str(Path(spine).parent / "child-rollup.json")
    env["PYTHONDONTWRITEBYTECODE"] = "1"
    env.update(extra)
    return env


class SpineTempDir(unittest.TestCase):
    """A temp spine folder, with the module's paths pointed at temp for the test."""

    def setUp(self) -> None:
        self.dir = Path(tempfile.mkdtemp(prefix="pow-test-"))
        self.addCleanup(shutil.rmtree, self.dir, True)
        self.spine = self.dir / "spine"
        self.spine.mkdir()
        self._saved = (P.SPINE_DIR, P.REGISTRY_PATH, P.ROLLUP_PATH, P.HEARTBEAT_DIR)
        P.SPINE_DIR = self.spine
        P.REGISTRY_PATH = self.dir / "no-registry.json"
        P.ROLLUP_PATH = self.dir / "rollup.json"
        P.HEARTBEAT_DIR = None
        self.addCleanup(self._restore)

    def _restore(self) -> None:
        P.SPINE_DIR, P.REGISTRY_PATH, P.ROLLUP_PATH, P.HEARTBEAT_DIR = self._saved

    def write(self, job: str, records: list[Any]) -> Path:
        """Write rows as JSON lines (strings are written as given) and return the file."""
        path = self.spine / f"{job}.jsonl"
        lines = [r if isinstance(r, str) else json.dumps(r) for r in records]
        path.write_text("\n".join(lines) + "\n", encoding="utf-8")
        return path

    def registry(self, data: dict[str, Any]) -> Path:
        path = self.dir / "jobs.json"
        path.write_text(json.dumps(data), encoding="utf-8")
        P.REGISTRY_PATH = path
        return path

    def lines(self, job: str) -> list[str]:
        path = self.spine / f"{job}.jsonl"
        return [x for x in path.read_text(encoding="utf-8").splitlines() if x]

    def rows(self, job: str) -> list[dict[str, Any]]:
        return [json.loads(x) for x in self.lines(job)]
