"""examples/demo.sh runs end to end, shows a clean chain, then a broken one, and leaves the repo alone."""

from __future__ import annotations

import hashlib
import os
import re
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from _common import HERE, child_env

DEMO = HERE / "examples" / "demo.sh"


def snapshot() -> dict[str, str]:
    """Map every path in the repo to a fingerprint, so a new file, a removed file or an edit shows up.

    A file maps to its size and the sha256 of its bytes, a folder to "dir". Any .git or __pycache__
    folder is skipped, with everything inside it.
    """
    found: dict[str, str] = {}
    for root, dirs, files in os.walk(HERE):
        dirs[:] = [d for d in dirs if d not in (".git", "__pycache__")]
        for name in dirs:
            found[os.path.relpath(os.path.join(root, name), HERE)] = "dir"
        for name in files:
            path = os.path.join(root, name)
            with open(path, "rb") as fh:
                data = fh.read()
            found[os.path.relpath(path, HERE)] = f"{len(data)}:{hashlib.sha256(data).hexdigest()}"
    return found


class TestDemo(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.before = snapshot()
        with tempfile.TemporaryDirectory() as tmp:
            env = child_env(Path(tmp) / "unused", TMPDIR=tmp, PYTHON=sys.executable)
            for name in ("POW_SPINE_DIR",):
                env.pop(name, None)
            cls.proc = subprocess.run(["/bin/bash", str(DEMO)], capture_output=True, text=True, env=env,
                                      timeout=120, check=False, cwd=tmp)
            cls.leftovers = sorted(os.listdir(tmp))
        cls.after = snapshot()
        cls.out = cls.proc.stdout

    def test_it_exits_0(self) -> None:
        self.assertEqual(self.proc.returncode, 0, self.proc.stderr[-800:])

    def test_the_first_verify_is_clean(self) -> None:
        part = self.out.split("== 7.")[1].split("== 8.")[0]
        self.assertIn("verify exit code: 0", part)
        self.assertIn("2 rows, 0 unhashed, ok", part)
        self.assertNotIn("BAD", part)

    def test_the_second_verify_fails_and_names_a_line_number(self) -> None:
        part = self.out.split("== 9.")[1]
        self.assertRegex(part, r"BAD at line \d+: hash mismatch")
        self.assertIn("verify exit code: 1", part)
        line = int(re.search(r"BAD at line (\d+)", part).group(1))
        self.assertEqual(line, 1)

    def test_all_three_states_and_a_check_failure_are_shown(self) -> None:
        self.assertIn("did_work", self.out)
        self.assertIn("could_not", self.out)
        self.assertIn("VIOLATED inbox-sync", self.out)
        self.assertIn("check exit code: 1", self.out)

    def test_the_wrapped_runs_were_recorded_by_the_wrapper(self) -> None:
        self.assertIn("nightly-backup.jsonl: 2 rows", self.out)

    def test_nothing_was_written_inside_the_repo(self) -> None:
        added = sorted(set(self.after) - set(self.before))
        removed = sorted(set(self.before) - set(self.after))
        changed = sorted(p for p in set(self.before) & set(self.after) if self.before[p] != self.after[p])
        self.assertEqual(self.after, self.before,
                         f"the demo changed the repo folder: added {added}, removed {removed}, changed {changed}")

    def test_the_demo_cleans_up_its_temp_folder(self) -> None:
        self.assertEqual(self.leftovers, [])

    def test_output_does_not_show_the_temp_path(self) -> None:
        self.assertNotIn("pow-demo.", self.out)

    def test_the_script_is_plain_bash_3_2_compatible(self) -> None:
        text = DEMO.read_text(encoding="utf-8")
        self.assertTrue(text.startswith("#!/usr/bin/env bash\n"))
        for banned in ("declare -A", "mapfile", "readarray", "${!", "&>>", ";&"):
            self.assertNotIn(banned, text)


if __name__ == "__main__":
    unittest.main(verbosity=2)
