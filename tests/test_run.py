"""The wrapper, pow-run.py: exit codes, the four patterns, timeouts, detail files, output and failures.

Wrapped commands are small /bin/sh one-liners. Every test that starts a child process makes sure
the child is gone afterwards, including when the test fails.
"""

from __future__ import annotations

import importlib.util
import json
import os
import shlex
import shutil
import signal
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path
from typing import Any

from _common import HERE, SpineTempDir, child_env

RUNNER = HERE / "pow-run.py"


def load_runner() -> Any:
    spec = importlib.util.spec_from_file_location("pow_run", RUNNER)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class WrapperCase(SpineTempDir):
    """Runs pow-run.py as a subprocess against the temp spine."""

    JOB = "wrapped-job"

    def wrap(self, script: str, *flags: str, job: str | None = None, env: dict[str, str] | None = None,
             timeout: int = 60, text: bool = True) -> subprocess.CompletedProcess:
        cmd = [sys.executable, str(RUNNER), "--job", job or self.JOB, *flags, "--", "/bin/sh", "-c", script]
        return subprocess.run(cmd, capture_output=True, text=text, timeout=timeout, check=False,
                              env=env or child_env(self.spine))

    def last_row(self, job: str | None = None) -> dict[str, Any]:
        return self.rows(job or self.JOB)[-1]


class TestExitCodes(WrapperCase):
    def test_exit_0_passes_through(self) -> None:
        proc = self.wrap("exit 0")
        self.assertEqual(proc.returncode, 0)
        self.assertEqual(len(self.rows(self.JOB)), 1)

    def test_a_non_zero_exit_passes_through_and_is_could_not(self) -> None:
        proc = self.wrap("echo disk went away >&2; exit 7")
        self.assertEqual(proc.returncode, 7)
        row = self.last_row()
        self.assertEqual(row["status"], "could_not")
        self.assertEqual(row["counts"], {"did_work": 0, "no_work": 0, "could_not": 1})
        self.assertIn("exit 7", row["reason"])
        self.assertIn("disk went away", row["reason"])
        self.assertEqual(row["evidence"]["exit_code"], 7)

    def test_a_command_that_cannot_launch_is_127_and_could_not(self) -> None:
        cmd = [sys.executable, str(RUNNER), "--job", self.JOB, "--", "/nonexistent/program-xyz"]
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=60, env=child_env(self.spine),
                              check=False)
        self.assertEqual(proc.returncode, 127)
        row = self.last_row()
        self.assertEqual(row["status"], "could_not")
        self.assertIn("could not launch", row["reason"])

    def test_no_command_is_a_usage_error_and_records_nothing(self) -> None:
        proc = subprocess.run([sys.executable, str(RUNNER), "--job", self.JOB], capture_output=True,
                              text=True, timeout=60, env=child_env(self.spine), check=False)
        self.assertEqual(proc.returncode, 2)
        self.assertFalse((self.spine / f"{self.JOB}.jsonl").exists())

    def test_the_command_line_is_kept_in_the_evidence(self) -> None:
        self.wrap("echo hello")
        self.assertIn("echo hello", self.last_row()["evidence"]["cmd"])


class TestPatterns(WrapperCase):
    def test_no_pattern_never_claims_work(self) -> None:
        proc = self.wrap("echo synced 9 files")
        self.assertEqual(proc.returncode, 0)
        row = self.last_row()
        self.assertEqual(row["status"], "no_work")
        self.assertEqual(row["evidence"]["classification"], "unclassified")
        self.assertIn("liveness-only", row["reason"])
        self.assertIn("synced 9 files", row["evidence"]["tail"])

    def test_work_pattern_records_did_work_with_the_captured_count(self) -> None:
        self.wrap("echo synced 5 files", "--work-pattern", r"synced (\d+) files")
        row = self.last_row()
        self.assertEqual(row["status"], "did_work")
        self.assertEqual(row["counts"]["did_work"], 5)
        self.assertEqual(row["evidence"]["classification"], "pattern")

    def test_work_pattern_without_a_group_counts_one(self) -> None:
        self.wrap("echo backup complete", "--work-pattern", "backup complete")
        self.assertEqual(self.last_row()["counts"]["did_work"], 1)

    def test_a_work_pattern_that_does_not_match_is_not_work(self) -> None:
        self.wrap("echo something else", "--work-pattern", "backup complete")
        self.assertEqual(self.last_row()["status"], "no_work")

    def test_none_pattern_records_a_classified_no_work(self) -> None:
        self.wrap("echo nothing to do", "--none-pattern", "nothing to do")
        row = self.last_row()
        self.assertEqual(row["status"], "no_work")
        self.assertEqual(row["reason"], "matched --none-pattern")
        self.assertEqual(row["evidence"]["classification"], "pattern")
        self.assertNotIn("tail", row["evidence"])

    def test_fail_pattern_makes_an_exit_0_run_could_not(self) -> None:
        proc = self.wrap("echo partial failure on 2 of 9", "--fail-pattern", "partial failure")
        self.assertEqual(proc.returncode, 0)
        row = self.last_row()
        self.assertEqual(row["status"], "could_not")
        self.assertIn("matched --fail-pattern: partial failure", row["reason"])

    def test_fail_pattern_outranks_the_work_pattern(self) -> None:
        self.wrap("echo synced 4 files; echo partial failure", "--work-pattern", r"synced (\d+)",
                  "--fail-pattern", "partial failure")
        self.assertEqual(self.last_row()["status"], "could_not")

    def test_a_non_zero_exit_outranks_the_work_pattern(self) -> None:
        self.wrap("echo synced 4 files; exit 3", "--work-pattern", r"synced (\d+)")
        self.assertEqual(self.last_row()["status"], "could_not")

    def test_work_pattern_outranks_the_none_pattern(self) -> None:
        self.wrap("echo nothing to do; echo synced 2 files", "--work-pattern", r"synced (\d+)",
                  "--none-pattern", "nothing to do")
        self.assertEqual(self.last_row()["status"], "did_work")

    def test_queue_pattern_records_the_backlog_and_its_unit(self) -> None:
        self.wrap("echo remaining: 12", "--queue-pattern", r"remaining: (\d+)", "--queue-unit", "mails")
        self.assertEqual(self.last_row()["queue"], {"depth": 12, "unit": "mails"})

    def test_patterns_also_read_stderr(self) -> None:
        self.wrap("echo synced 3 files >&2", "--work-pattern", r"synced (\d+)")
        self.assertEqual(self.last_row()["status"], "did_work")

    def test_long_output_is_read_at_both_ends_and_not_in_the_middle(self) -> None:
        pr = load_runner()
        self.assertEqual(pr.MATCH_CHARS, 200_000)
        script = "head -c 250000 /dev/zero | tr '\\0' x; echo; echo LATE-WORK-MARKER"
        self.wrap(script, "--work-pattern", "LATE-WORK-MARKER")
        self.assertEqual(self.last_row()["status"], "did_work")
        script = "echo EARLY-WORK-MARKER; head -c 250000 /dev/zero | tr '\\0' x"
        self.wrap(script, "--work-pattern", "EARLY-WORK-MARKER", job="early-job")
        self.assertEqual(self.last_row("early-job")["status"], "did_work")
        script = ("head -c 120000 /dev/zero | tr '\\0' x; echo; echo MIDDLE-WORK-MARKER; "
                  "head -c 130000 /dev/zero | tr '\\0' x")
        self.wrap(script, "--work-pattern", "MIDDLE-WORK-MARKER", job="middle-job")
        self.assertEqual(self.last_row("middle-job")["status"], "no_work")


LONG = "head -c 250000 /dev/zero | tr '\\0' x; echo; "


class TestOutputThatIsNotUtf8(WrapperCase):
    JOB = "odd-bytes"

    def test_a_stray_byte_on_stdout_does_not_hide_the_work(self) -> None:
        # text=False: the wrapper now echoes the raw byte, which text=True cannot decode.
        proc = self.wrap("printf '\\377copied 3 files\\n'", "--work-pattern", r"copied (\d+) files",
                         text=False)
        self.assertEqual(proc.returncode, 0)
        row = self.last_row()
        self.assertEqual(row["status"], "did_work")
        self.assertEqual(row["counts"]["did_work"], 3)

    def test_a_stray_byte_on_stderr_keeps_the_real_exit_code(self) -> None:
        # text=False: the wrapper now echoes the raw byte, which text=True cannot decode.
        proc = self.wrap("printf 'bad \\377 byte' >&2; exit 3", text=False)
        self.assertEqual(proc.returncode, 3)
        row = self.last_row()
        self.assertEqual(row["status"], "could_not")
        self.assertNotIn("could not launch", row["reason"])
        self.assertIn("exit 3", row["reason"])
        self.assertEqual(row["evidence"]["exit_code"], 3)

    def test_a_stray_byte_before_a_timeout_is_still_a_timeout(self) -> None:
        # text=False: the wrapper now echoes the raw byte, which text=True cannot decode.
        proc = self.wrap("printf '\\377'; sleep 30", "--timeout", "1", text=False)
        self.assertEqual(proc.returncode, 124)
        self.assertEqual(self.last_row()["reason"], "timed out after 1s")


class TestACountOfZeroIsNotWork(WrapperCase):
    JOB = "zero-count"

    def test_a_matched_count_of_zero_is_no_work_and_classified(self) -> None:
        proc = self.wrap("echo copied 0 files", "--work-pattern", r"copied (\d+) files")
        self.assertEqual(proc.returncode, 0)
        row = self.last_row()
        self.assertEqual(row["status"], "no_work")
        self.assertEqual(row["reason"], "matched --work-pattern with a count of 0")
        self.assertEqual(row["counts"], {"did_work": 0, "no_work": 1, "could_not": 0})
        self.assertEqual(row["evidence"]["classification"], "pattern")
        self.assertNotIn("tail", row["evidence"])

    def test_a_group_that_is_not_a_number_still_counts_one(self) -> None:
        self.wrap("echo all done", "--work-pattern", r"(done)")
        row = self.last_row()
        self.assertEqual(row["status"], "did_work")
        self.assertEqual(row["counts"]["did_work"], 1)

    def test_an_optional_group_that_took_no_part_still_counts_one(self) -> None:
        """source: pre-check 5, an empty optional group crashed the wrapper after the job ran."""
        proc = self.wrap("echo copied files", "--work-pattern", r"copied ([0-9]+)? *files")
        self.assertEqual(proc.returncode, 0)
        self.assertNotIn("Traceback", proc.stderr)
        row = self.last_row()
        self.assertEqual(row["status"], "did_work")
        self.assertEqual(row["counts"]["did_work"], 1)

    def test_an_optional_group_that_took_no_part_keeps_the_exit_code(self) -> None:
        """source: pre-check 5, the crash replaced the job's own exit code with 1 and wrote no row."""
        proc = self.wrap("echo copied files; exit 3", "--work-pattern", r"copied ([0-9]+)? *files")
        self.assertEqual(proc.returncode, 3)
        self.assertEqual(self.last_row()["status"], "could_not")

    def test_a_non_zero_count_is_still_work(self) -> None:
        self.wrap("echo copied 10 files", "--work-pattern", r"copied (\d+) files")
        self.assertEqual(self.last_row()["counts"]["did_work"], 10)


class TestTheEndOfLongOutput(WrapperCase):
    JOB = "long-output"

    def test_a_count_at_the_very_end_is_matched(self) -> None:
        self.wrap(LONG + "echo copied 6 files", "--work-pattern", r"copied (\d+) files", "--quiet")
        row = self.last_row()
        self.assertEqual(row["status"], "did_work")
        self.assertEqual(row["counts"]["did_work"], 6)

    def test_a_fail_pattern_at_the_end_of_an_exit_0_run_is_could_not(self) -> None:
        self.wrap(LONG + "echo partial failure on 2 of 9", "--fail-pattern", "partial failure", "--quiet")
        self.assertEqual(self.last_row()["status"], "could_not")

    def test_a_none_pattern_and_a_queue_line_at_the_end_are_matched(self) -> None:
        self.wrap(LONG + "echo nothing due; echo remaining: 7", "--none-pattern", "nothing due",
                  "--queue-pattern", r"remaining: (\d+)", "--quiet")
        row = self.last_row()
        self.assertEqual(row["reason"], "matched --none-pattern")
        self.assertEqual(row["queue"]["depth"], 7)

    def test_a_match_does_not_span_the_cut(self) -> None:
        # 100000 is half of MATCH_CHARS. ABCD starts two characters before the cut, so AB ends the
        # first part and CD lies in the unsearched middle.
        script = ("head -c 99998 /dev/zero | tr '\\0' x; printf 'ABCD'; "
                  "head -c 150000 /dev/zero | tr '\\0' x")
        self.wrap(script, "--work-pattern", "AB", "--quiet")
        self.assertEqual(self.last_row()["status"], "did_work", "the first part is searched")
        self.wrap(script, "--work-pattern", "ABCD", "--quiet", job="span-job")
        self.assertEqual(self.last_row("span-job")["status"], "no_work", "a match never spans the cut")

    def test_the_stored_tail_is_the_real_end_of_the_output(self) -> None:
        self.wrap(LONG + "echo THE-REAL-END", "--quiet")
        row = self.last_row()
        self.assertEqual(row["evidence"]["classification"], "unclassified")
        self.assertTrue(row["evidence"]["tail"].endswith("THE-REAL-END"), row["evidence"]["tail"][-40:])

    def test_a_marker_at_the_end_of_what_a_detail_file_gained_is_matched(self) -> None:
        log = self.dir / "big.log"
        log.write_text("old line\n", encoding="utf-8")
        script = f"({LONG}echo copied 4 files) >> {shlex.quote(str(log))}"
        self.wrap(script, "--quiet", "--work-pattern", r"copied (\d+) files", "--detail-file", str(log))
        row = self.last_row()
        self.assertEqual(row["status"], "did_work")
        self.assertEqual(row["counts"]["did_work"], 4)

    def test_the_tail_of_a_detail_file_run_is_its_real_end(self) -> None:
        log = self.dir / "big.log"
        log.write_text("", encoding="utf-8")
        script = f"({LONG}echo FILE-END) >> {shlex.quote(str(log))}"
        self.wrap(script, "--quiet", "--detail-file", str(log))
        tail = self.last_row()["evidence"]["tail"]
        self.assertTrue(tail.endswith("FILE-END"), tail[-40:])


class TestAppendedPartsOfALargeFile(unittest.TestCase):
    def setUp(self) -> None:
        self.pr = load_runner()

    def test_a_large_appended_region_comes_back_as_its_first_and_last_halves(self) -> None:
        half = self.pr.MATCH_CHARS // 2
        with tempfile.TemporaryDirectory() as d:
            log = Path(d) / "job.log"
            log.write_text("EARLIER RUN\n", encoding="utf-8")
            mark = self.pr._mark(str(log))
            with log.open("a", encoding="utf-8") as fh:
                fh.write("START-OF-THIS-RUN\n" + "x" * 300_000 + "\nEND-OF-THIS-RUN\n")
            parts, end = self.pr._appended_parts(str(log), mark)
        self.assertEqual(len(parts), 2)
        self.assertTrue(all(len(p) <= half for p in parts))
        self.assertTrue(parts[0].startswith("START-OF-THIS-RUN"))
        self.assertTrue(parts[1].endswith("END-OF-THIS-RUN\n"))
        self.assertEqual(end, parts[1])
        self.assertNotIn("EARLIER RUN", parts[0])

    def test_a_small_appended_region_comes_back_whole(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            log = Path(d) / "job.log"
            log.write_text("", encoding="utf-8")
            mark = self.pr._mark(str(log))
            log.write_text("one\ntwo\n", encoding="utf-8")
            self.assertEqual(self.pr._appended_parts(str(log), mark), (["one\ntwo\n"], "one\ntwo\n"))


class TestAKillingSignal(WrapperCase):
    JOB = "signalled"

    def test_a_command_killed_by_a_signal_returns_128_plus_the_number(self) -> None:
        proc = self.wrap("kill -9 $$")
        self.assertEqual(proc.returncode, 137)
        row = self.last_row()
        self.assertEqual(row["status"], "could_not")
        self.assertIn("signal 9", row["reason"])
        self.assertIn("SIGKILL", row["reason"])
        self.assertEqual(row["evidence"]["exit_code"], 137)
        self.assertEqual(row["evidence"]["signal"], 9)

    def test_work_exit_codes_compare_against_the_returned_number(self) -> None:
        proc = self.wrap("kill -15 $$", "--work-exit-codes", "143")
        self.assertEqual(proc.returncode, 143)
        self.assertEqual(self.last_row()["status"], "did_work")

    def test_an_ordinary_exit_has_no_signal_in_its_evidence(self) -> None:
        self.wrap("exit 5")
        self.assertNotIn("signal", self.last_row()["evidence"])


class TestWorkExitCodes(WrapperCase):
    def test_a_listed_non_zero_exit_is_did_work_and_keeps_the_findings(self) -> None:
        proc = self.wrap("echo box-a drift; exit 1", "--work-exit-codes", "1", "--none-pattern", "ok")
        self.assertEqual(proc.returncode, 1, "the exit code still passes through")
        row = self.last_row()
        self.assertEqual(row["status"], "did_work")
        self.assertEqual(row["evidence"]["classification"], "exit-code")
        self.assertEqual(row["evidence"]["findings_exit_code"], 1)
        self.assertIn("box-a drift", row["evidence"]["tail"])

    def test_a_clean_run_keeps_no_tail_and_no_findings(self) -> None:
        self.wrap("echo box-a ok; exit 0", "--work-exit-codes", "1", "--none-pattern", "ok")
        row = self.last_row()
        self.assertEqual(row["status"], "no_work")
        self.assertEqual(row["reason"], "matched --none-pattern")
        self.assertNotIn("tail", row["evidence"])
        self.assertNotIn("findings_exit_code", row["evidence"])

    def test_an_exit_code_that_is_not_listed_is_still_could_not(self) -> None:
        proc = self.wrap("exit 3", "--work-exit-codes", "1,2")
        self.assertEqual(proc.returncode, 3)
        self.assertEqual(self.last_row()["status"], "could_not")

    def test_several_codes_can_be_listed(self) -> None:
        self.wrap("exit 2", "--work-exit-codes", "1, 2")
        self.assertEqual(self.last_row()["status"], "did_work")


class TestOutput(WrapperCase):
    def test_output_is_echoed_by_default(self) -> None:
        proc = self.wrap("echo to-stdout; echo to-stderr >&2")
        self.assertIn("to-stdout", proc.stdout)
        self.assertIn("to-stderr", proc.stderr)

    def test_quiet_does_not_echo_but_still_records(self) -> None:
        proc = self.wrap("echo to-stdout; echo to-stderr >&2", "--quiet", "--work-pattern", "to-stdout")
        self.assertEqual(proc.stdout, "")
        self.assertEqual(proc.stderr, "")
        self.assertEqual(self.last_row()["status"], "did_work")


class TestRecordFailureNeverChangesTheExitCode(WrapperCase):
    def broken_spine_env(self) -> dict[str, str]:
        blocker = self.dir / "not-a-folder"
        blocker.write_text("a file where the spine folder should be", encoding="utf-8")
        return child_env(blocker)

    def test_exit_0_stays_0_and_a_warning_goes_to_stderr(self) -> None:
        proc = self.wrap("echo hi", env=self.broken_spine_env())
        self.assertEqual(proc.returncode, 0)
        self.assertIn("WARN could not record", proc.stderr)
        self.assertIn("hi", proc.stdout)

    def test_a_non_zero_exit_stays_the_same(self) -> None:
        proc = self.wrap("exit 5", env=self.broken_spine_env())
        self.assertEqual(proc.returncode, 5)
        self.assertIn("WARN could not record", proc.stderr)

    def test_a_bad_job_name_is_also_only_a_warning(self) -> None:
        proc = self.wrap("exit 0", job="../escape")
        self.assertEqual(proc.returncode, 0)
        self.assertIn("WARN could not record", proc.stderr)
        self.assertEqual(list(self.spine.iterdir()), [])


class TestHeartbeatThroughTheWrapper(WrapperCase):
    def test_the_heartbeat_folder_is_touched_when_set(self) -> None:
        hb = self.dir / "hb"
        hb.mkdir()
        self.wrap("exit 0", env=child_env(self.spine, POW_HEARTBEAT_DIR=str(hb)))
        self.assertTrue((hb / f"{self.JOB}.last").is_file())

    def test_nothing_is_touched_when_unset(self) -> None:
        hb = self.dir / "hb"
        hb.mkdir()
        self.wrap("exit 0")
        self.assertEqual(list(hb.iterdir()), [])


class TestTimeout(WrapperCase):
    def test_a_timeout_is_124_could_not_and_leaves_no_child_running(self) -> None:
        pidfile = self.dir / "sleeper.pid"
        script = f"sleep 300 & echo $! > {shlex.quote(str(pidfile))}; wait"

        def reap() -> None:
            # Only a pid that is still a sleep we started may be killed.
            if not pidfile.exists() or not pidfile.read_text().strip():
                return
            pid = int(pidfile.read_text().split()[0])
            out = subprocess.run(["/bin/ps", "-o", "command=", "-p", str(pid)], capture_output=True,
                                 text=True, check=False).stdout
            if "sleep 300" in out:
                os.kill(pid, signal.SIGKILL)

        self.addCleanup(reap)
        started = time.monotonic()
        proc = self.wrap(script, "--timeout", "2")
        self.assertLess(time.monotonic() - started, 30)
        self.assertEqual(proc.returncode, 124)
        row = self.last_row()
        self.assertEqual(row["status"], "could_not")
        self.assertEqual(row["reason"], "timed out after 2s")
        self.assertEqual(row["evidence"]["exit_code"], 124)
        pid = int(pidfile.read_text().split()[0])
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            try:
                os.kill(pid, 0)
            except ProcessLookupError:
                break
            time.sleep(0.05)
        with self.assertRaises(ProcessLookupError):
            os.kill(pid, 0)

    def test_a_run_inside_the_limit_is_not_a_timeout(self) -> None:
        proc = self.wrap("exit 0", "--timeout", "20")
        self.assertEqual(proc.returncode, 0)
        self.assertNotEqual(self.last_row().get("reason"), "timed out after 20s")


GRANDCHILD = r"""
import os, signal, sys, time
if sys.argv[2] == "ignore-term":
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
with open(sys.argv[1] + ".tmp", "w") as fh:
    fh.write(f"{os.getpid()} {os.getpgrp()}")
os.rename(sys.argv[1] + ".tmp", sys.argv[1])
time.sleep(60)
"""


class TestATimeoutStopsTheWholeTree(unittest.TestCase):
    """subprocess.run(timeout=) kills only the direct child, so a job's own children would outlive it.

    One wrapper under a 3 s timeout runs a shell that starts two sleeping grandchildren, one of them
    deaf to SIGTERM, and the wrapper shares its process group with an unrelated sibling. After the
    timeout both grandchildren must be gone and the sibling untouched.
    """

    @classmethod
    def setUpClass(cls) -> None:
        cls.tmp = Path(tempfile.mkdtemp(prefix="pow-tree-"))
        cls.marker = f"pow-tree-test-{os.getpid()}"
        cls.files = {"plain": cls.tmp / "plain.pid", "deaf": cls.tmp / "deaf.pid",
                     "sibling": cls.tmp / "sibling.pid"}
        # A class cleanup also runs when setUpClass itself fails, so nothing is left sleeping.
        cls.addClassCleanup(cls._cleanup)
        spine = cls.tmp / "spine"
        spine.mkdir()
        gc = cls.tmp / "gc.py"
        gc.write_text(GRANDCHILD)
        q = shlex.quote
        plain, deaf = q(str(cls.files["plain"])), q(str(cls.files["deaf"]))
        child = (f'"$0" {q(str(gc))} {plain} plain {cls.marker} & '
                 f'"$0" {q(str(gc))} {deaf} ignore-term {cls.marker} & '
                 f'until [ -s {plain} ] && [ -s {deaf} ]; do sleep 0.05; done; '
                 'echo flushed-before-timeout; wait')
        # sh starts the sibling and then becomes the wrapper (exec keeps the pid), in a fresh session,
        # so the sibling sits in the wrapper's own process group.
        launcher = (f'"$0" -c "import time; time.sleep(60)" {cls.marker}-sibling >/dev/null 2>&1 & '
                    f'echo $! $$ > {q(str(cls.files["sibling"]))}; exec "$0" "$@"')
        cls.proc = subprocess.run(
            ["/bin/sh", "-c", launcher, sys.executable, str(RUNNER),
             "--job", "treetest", "--timeout", "3", "--", "/bin/sh", "-c", child, sys.executable],
            capture_output=True, text=True, env=child_env(spine), timeout=90,
            start_new_session=True, check=False)
        cls.row = json.loads((spine / "treetest.jsonl").read_text().splitlines()[-1])
        cls.pids: dict[str, list[int]] = {}
        for name, path in cls.files.items():
            if path.exists() and path.read_text().split():
                cls.pids[name] = [int(x) for x in path.read_text().split()]

    @classmethod
    def _cleanup(cls) -> None:
        for path in cls.files.values():
            words = path.read_text().split() if path.exists() else []
            if words and cls._running(int(words[0]), cls.marker):
                try:
                    os.kill(int(words[0]), signal.SIGKILL)
                except OSError:
                    pass
        shutil.rmtree(cls.tmp, ignore_errors=True)

    @staticmethod
    def _running(pid: int, marker: str) -> bool:
        """True while pid is a live (not zombie) process whose command line carries the marker."""
        out = subprocess.run(["/bin/ps", "-o", "stat=,command=", "-p", str(pid)],
                             capture_output=True, text=True, check=False).stdout.strip()
        return bool(out) and not out.startswith("Z") and marker in out

    def _gone_within(self, pid: int, seconds: float = 3.0) -> bool:
        deadline = time.monotonic() + seconds
        while self._running(pid, self.marker):
            if time.monotonic() > deadline:
                return False
            time.sleep(0.05)
        return True

    def test_the_exit_code_row_and_flushed_output_read_as_before(self) -> None:
        self.assertEqual(self.proc.returncode, 124, self.proc.stderr[-500:])
        self.assertEqual(self.row["status"], "could_not")
        self.assertEqual(self.row["reason"], "timed out after 3s")
        self.assertEqual(self.row["evidence"]["exit_code"], 124)
        self.assertIn("flushed-before-timeout", self.proc.stdout)
        self.assertIn("flushed-before-timeout", self.row["evidence"]["tail"])

    def test_both_grandchildren_are_gone_including_the_one_deaf_to_sigterm(self) -> None:
        self.assertTrue(set(self.pids) >= {"plain", "deaf"}, f"the grandchildren never started: {self.pids}")
        survivors = [n for n in ("plain", "deaf") if not self._gone_within(self.pids[n][0])]
        self.assertEqual(survivors, [], "these grandchildren outlived the timeout")

    def test_the_tree_stays_in_the_wrappers_process_group(self) -> None:
        wrapper_group = self.pids["sibling"][1]
        self.assertEqual(self.pids["plain"][1], wrapper_group, "the job's tree left the wrapper's process group")

    def test_a_sibling_in_the_wrappers_own_group_is_never_signalled(self) -> None:
        self.assertTrue(self._running(self.pids["sibling"][0], self.marker),
                        "the timeout killed a process the wrapper did not start")


class TestFailureEvidence(unittest.TestCase):
    """A failure nobody can act on is the same as a failure nobody saw."""

    def setUp(self) -> None:
        self.pr = load_runner()

    def test_stderr_wins_when_the_job_speaks(self) -> None:
        self.assertEqual(self.pr._why("the real reason", "noise on stdout", None), "the real reason")

    def test_stdout_is_used_when_stderr_is_empty(self) -> None:
        self.assertEqual(self.pr._why("", "only stdout said something", None), "only stdout said something")

    def test_a_detail_file_is_read_when_both_streams_are_silent(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            f = Path(td) / "health.json"
            f.write_text('{"alerts": ["widget check code 1", "free space 4 units"]}', encoding="utf-8")
            why = self.pr._why("", "", str(f))
        self.assertIn("widget check code 1", why)
        self.assertIn("silent", why)

    def test_a_wholly_silent_failure_says_so(self) -> None:
        why = self.pr._why("", "", None)
        self.assertIn("silent", why)
        self.assertIn("no evidence", why)
        self.assertNotEqual(why.strip(), "")

    def test_an_unreadable_detail_file_is_not_mistaken_for_silence(self) -> None:
        self.assertIn("unreadable", self.pr._why("", "", "/nope/does/not/exist.json"))

    def test_a_detail_file_quotes_the_failing_line_not_the_status_line(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            f = Path(td) / "backup.log"
            f.write_text("Copy(<sample/a>) ok\nFatal: unable to copy sample: link dropped: "
                         "exit status 255\nwidget run code=1 done\n", encoding="utf-8")
            why = self.pr._why("", "", str(f))
        self.assertIn("exit status 255", why)
        self.assertNotIn("widget run code=1 done", why)

    def test_a_huge_detail_file_is_read_from_the_tail(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            f = Path(td) / "big.log"
            f.write_text("filler line\n" * 200_000 + "Fatal: the actual problem\n", encoding="utf-8")
            why = self.pr._why("", "", str(f))
        self.assertIn("the actual problem", why)


class TestAppendedDetailOnly(unittest.TestCase):
    """The wrapper only ever reads what the run itself appended to a detail file."""

    def setUp(self) -> None:
        self.pr = load_runner()

    def test_a_prior_runs_success_line_is_not_read_as_this_runs_work(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            log = Path(d) / "job.log"
            log.write_text("2026-01-01 02:30:00 SUCCESS: snapshot complete\n")
            mark = self.pr._mark(str(log))
            self.assertEqual(self.pr._appended(str(log), mark), "")

    def test_what_this_run_appended_is_what_gets_matched(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            log = Path(d) / "job.log"
            log.write_text("2026-01-01 02:30:00 SUCCESS: snapshot complete\n")
            mark = self.pr._mark(str(log))
            with log.open("a") as fh:
                fh.write("2026-01-02 02:30:01 SUCCESS: snapshot complete\n")
            got = self.pr._appended(str(log), mark)
            self.assertIn("2026-01-02", got)
            self.assertNotIn("2026-01-01", got)

    def test_a_log_rotated_mid_run_falls_back_to_the_tail(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            log = Path(d) / "job.log"
            log.write_text("x" * 5000)
            mark = self.pr._mark(str(log))
            log.write_text("run: nothing due\n")
            self.assertIn("nothing due", self.pr._appended(str(log), mark))

    def test_a_missing_detail_file_reads_as_empty_rather_than_raising(self) -> None:
        self.assertEqual(self.pr._appended("/nope/does/not/exist.log", (0, b"")), "")

    def test_a_first_ever_run_reads_from_the_start(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            log = Path(d) / "new.log"
            mark = self.pr._mark(str(log))
            log.write_text("Done: 3 files, 9 chunks ingested\n")
            self.assertIn("Done: 3 files", self.pr._appended(str(log), mark))

    def test_a_rewritten_state_file_is_read_whole_not_sliced(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            state = Path(d) / "state.json"
            state.write_text('{"status": "ok", "checked": 40, "stale": []}')
            mark = self.pr._mark(str(state))
            state.write_text('{"status": "degraded", "checked": 40, "stale": ["mail"]}')
            got = self.pr._appended(str(state), mark)
            self.assertTrue(got.startswith("{"), f"read a fragment, not the document: {got!r}")
            self.assertIn("degraded", got)

    def test_a_rewrite_that_lands_at_the_same_size_is_still_seen_as_a_rewrite(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            state = Path(d) / "s.json"
            state.write_text('{"result": "AAAA"}')
            mark = self.pr._mark(str(state))
            state.write_text('{"result": "BBBB"}')
            self.assertEqual(self.pr._appended(str(state), mark), '{"result": "BBBB"}')

    def test_a_rewrite_with_an_identical_tail_is_not_silent(self) -> None:
        """Only the changed mtime separates this rewrite from "appended nothing"."""
        with tempfile.TemporaryDirectory() as d:
            log = Path(d) / "detail.json"
            tail = '"pad": "' + ("x" * 400) + '", "alerts": []}\n'
            log.write_text('{"checked_at": "2026-01-01T01:00:00", ' + tail)
            mark = self.pr._mark(str(log))
            log.write_text('{"checked_at": "2026-01-01T01:15:00", ' + tail)
            now_ns = time.time_ns() + 5_000_000
            os.utime(log, ns=(now_ns, now_ns))
            self.assertEqual(log.stat().st_size, mark[0])
            self.assertIn("01:15:00", self.pr._appended(str(log), mark))
            self.pr._MARK_MTIMES.pop(mark, None)
            self.assertEqual(self.pr._appended(str(log), mark), "")

    def test_why_quotes_this_runs_slice_rather_than_an_older_error(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            log = Path(d) / "j.log"
            log.write_text("ERROR: the failure from last week\n")
            why = self.pr._why("", "", str(log), "quiet line from this run\n")
            self.assertNotIn("last week", why)
            self.assertIn("quiet line from this run", why)

    def test_appending_nothing_says_so_rather_than_claiming_no_detail_file(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            log = Path(d) / "j.log"
            log.write_text("ERROR: the failure from last week\n")
            why = self.pr._why("", "", str(log), "")
            self.assertIn("appended", why)
            self.assertIn("j.log", why)
            self.assertNotIn("last week", why)


class TestOffsetIsCapturedBeforeTheRun(WrapperCase):
    """Only an end-to-end run exercises the point where the wrapper notes the file's size."""

    JOB = "offsettest"

    def test_a_run_that_appends_nothing_does_not_inherit_yesterdays_success(self) -> None:
        log = self.dir / "snap.log"
        log.write_text("2026-01-01 02:30:00 SUCCESS: snapshot complete\n")
        self.wrap("true", "--quiet", "--work-pattern", "SUCCESS: snapshot complete", "--detail-file", str(log))
        row = self.last_row()
        self.assertNotEqual(row["status"], "did_work")
        self.assertEqual(row["evidence"]["classification"], "unclassified")

    def test_a_run_that_appends_the_marker_is_credited(self) -> None:
        log = self.dir / "snap.log"
        log.write_text("2026-01-01 02:30:00 SUCCESS: snapshot complete\n")
        self.wrap(f"echo 'SUCCESS: snapshot complete' >> {shlex.quote(str(log))}", "--quiet",
                  "--work-pattern", "SUCCESS: snapshot complete", "--detail-file", str(log))
        row = self.last_row()
        self.assertEqual(row["status"], "did_work")
        self.assertEqual(row["evidence"]["classification"], "pattern")

    def test_the_detail_file_is_a_fallback_and_never_overrides_the_streams(self) -> None:
        log = self.dir / "j.log"
        log.write_text("")
        script = f"echo 'nothing due' >> {shlex.quote(str(log))}; echo REAL WORK"
        self.wrap(script, "--quiet", "--work-pattern", "REAL WORK", "--none-pattern", "nothing due",
                  "--detail-file", str(log))
        self.assertEqual(self.last_row()["status"], "did_work")

    def test_a_failure_with_silent_streams_quotes_what_the_run_appended(self) -> None:
        log = self.dir / "j.log"
        log.write_text("ERROR: an old failure\n")
        script = f"echo 'Fatal: disk is full' >> {shlex.quote(str(log))}; exit 4"
        self.wrap(script, "--quiet", "--detail-file", str(log))
        row = self.last_row()
        self.assertEqual(row["status"], "could_not")
        self.assertIn("disk is full", row["reason"])
        self.assertNotIn("old failure", row["reason"])


class TestFailPatternCarriesTheDetail(WrapperCase):
    JOB = "failpattern-detail"

    def test_a_fail_pattern_hit_also_quotes_what_the_run_appended(self) -> None:
        log = self.dir / "j.log"
        log.write_text("")
        script = f"echo 'Fatal: snapshot refused: exit status 255' >> {shlex.quote(str(log))}; echo partial failure"
        proc = self.wrap(script, "--quiet", "--fail-pattern", "partial failure", "--detail-file", str(log))
        self.assertEqual(proc.returncode, 0)
        row = self.last_row()
        self.assertEqual(row["status"], "could_not")
        self.assertIn("matched --fail-pattern: partial failure", row["reason"])
        self.assertIn("exit status 255", row["reason"])


class TestJsonDetailFiles(unittest.TestCase):
    """A JSON verdict file is read by structure, because line scanning misses it."""

    def setUp(self) -> None:
        self.pr = load_runner()

    def test_the_alert_list_is_what_gets_quoted(self) -> None:
        body = json.dumps({"overall": "fail", "counts": {"fail": 2},
                           "drift_alerts": ["disk_check: rc_1", "free_disk: 13GB free"]})
        got = self.pr._from_json(body)
        self.assertIn("disk_check: rc_1", got)
        self.assertIn("free_disk: 13GB free", got)

    def test_without_an_alert_list_the_failing_leaves_are_named_with_their_path(self) -> None:
        body = json.dumps({"checks": {"mail_capture": {"status": "fail"}, "calendar": {"status": "ok"}}})
        got = self.pr._from_json(body)
        self.assertIn("checks.mail_capture.status=fail", got)
        self.assertNotIn("calendar", got)

    def test_an_empty_alert_list_does_not_shadow_the_failing_leaves(self) -> None:
        body = json.dumps({"alerts": [], "svc": {"state": "critical"}})
        self.assertIn("svc.state=critical", self.pr._from_json(body))

    def test_a_healthy_document_yields_nothing_rather_than_a_false_alarm(self) -> None:
        self.assertEqual(self.pr._from_json(json.dumps({"overall": "ok", "counts": {"fail": 0}})), "")

    def test_a_non_json_body_falls_through_to_the_line_scan(self) -> None:
        self.assertEqual(self.pr._from_json("backup: Fatal: unable to open repo\n"), "")

    def test_a_json_array_or_scalar_does_not_crash(self) -> None:
        self.assertEqual(self.pr._from_json("[1, 2, 3]"), "")
        self.assertEqual(self.pr._from_json("null"), "")

    def test_why_prefers_the_structure_over_the_closing_braces(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            f = Path(d) / "state.json"
            body = json.dumps({"overall": "fail", "drift_alerts": ["mail_capture: format drift"]}, indent=2)
            f.write_text(body)
            why = self.pr._why("", "", str(f), body)
            self.assertIn("format drift", why)
            self.assertNotIn("}", why)


class TestAClosedPipeStillRecords(WrapperCase):
    """Piping the wrapper into head closes its stdout after a line.

    The echo happens before the row is written. A BrokenPipeError there used to
    kill the wrapper with a traceback and leave no ledger row.
    """

    def closed_stdout(self, child: list[str], *flags: str) -> tuple[int, str]:
        """Run the wrapper and close its stdout after one line, the way head does."""
        cmd = [sys.executable, str(RUNNER), "--job", self.JOB, *flags, "--", *child]
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
                                env=child_env(self.spine))

        def stop() -> None:
            if proc.poll() is None:
                proc.kill()
                proc.wait(timeout=60)

        self.addCleanup(stop)
        try:
            proc.stdout.readline()
            proc.stdout.close()
            code = proc.wait(timeout=60)
            err = proc.stderr.read()
            return code, err
        finally:
            proc.stdout.close()
            proc.stderr.close()

    def test_a_reader_that_stops_after_one_line_records_no_work(self) -> None:
        """source: a closed stdout pipe must still record no_work and exit 0."""
        code, err = self.closed_stdout([sys.executable, "-c", "print('x\\n' * 200000)"])
        self.assertEqual(code, 0)
        self.assertNotIn("Traceback", err)
        self.assertNotIn("BrokenPipeError", err)
        rows = self.rows(self.JOB)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["status"], "no_work")

    def test_a_reader_that_stops_early_keeps_a_failing_exit_code(self) -> None:
        """source: a closed stdout pipe must still return the command's own exit code."""
        child = [sys.executable, "-c", "print('x\\n' * 200000); raise SystemExit(3)"]
        code, err = self.closed_stdout(child)
        self.assertEqual(code, 3)
        self.assertNotIn("Traceback", err)
        self.assertNotIn("BrokenPipeError", err)
        rows = self.rows(self.JOB)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["status"], "could_not")

    def test_a_reader_that_stops_early_still_counts_work_at_the_start(self) -> None:
        """source: a closed stdout pipe must not drop a did_work count already printed."""
        child = [sys.executable, "-c", "print('copied 4 files'); print('x\\n' * 200000)"]
        code, err = self.closed_stdout(child, "--work-pattern", r"copied ([0-9]+) files")
        self.assertEqual(code, 0)
        self.assertNotIn("Traceback", err)
        self.assertNotIn("BrokenPipeError", err)
        rows = self.rows(self.JOB)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["status"], "did_work")
        self.assertEqual(rows[0]["counts"]["did_work"], 4)


class TestBytesPassThroughUnchanged(WrapperCase):
    """The echo is the command's own bytes. Patterns and the ledger still read text."""

    JOB = "binary-echo"

    def run_bytes(self, child: list[str], *flags: str) -> subprocess.CompletedProcess:
        """Run the wrapper and keep stdout and stderr as bytes. No text mode, no encoding."""
        cmd = [sys.executable, str(RUNNER), "--job", self.JOB, *flags, "--", *child]
        return subprocess.run(cmd, capture_output=True, timeout=60, check=False, env=child_env(self.spine))

    def test_stdout_bytes_pass_through_unchanged(self) -> None:
        """source: a byte that is not UTF-8 must reach the wrapper's stdout unchanged, with one row."""
        blob = b"\xff\xfe\x00\x1babc\n"
        proc = self.run_bytes([sys.executable, "-c",
                               "import sys; sys.stdout.buffer.write(b'\\xff\\xfe\\x00\\x1babc\\n')"])
        self.assertEqual(proc.returncode, 0)
        self.assertEqual(proc.stdout, blob)
        self.assertEqual(len(self.rows(self.JOB)), 1)

    def test_stderr_bytes_pass_through_and_exit_3_is_could_not(self) -> None:
        """source: stderr bytes must pass through unchanged, and exit 3 stays could_not."""
        blob = b"\xff\xfe\x00\x1babc\n"
        proc = self.run_bytes([sys.executable, "-c",
                               "import sys; sys.stderr.buffer.write(b'\\xff\\xfe\\x00\\x1babc\\n');"
                               " raise SystemExit(3)"])
        self.assertEqual(proc.returncode, 3)
        self.assertTrue(proc.stderr.endswith(blob), proc.stderr)
        row = self.last_row()
        self.assertEqual(row["status"], "could_not")
        self.assertIn("exit 3", row["reason"])

    def test_a_work_pattern_counts_bytes_that_are_not_utf8(self) -> None:
        """source: raw stdout passes through, and the work pattern still reads the replaced text."""
        blob = b"\xffcopied 3 files\n"
        proc = self.run_bytes(
            [sys.executable, "-c", "import sys; sys.stdout.buffer.write(b'\\xffcopied 3 files\\n')"],
            "--work-pattern", "copied ([0-9]+) files",
        )
        self.assertEqual(proc.stdout, blob)
        row = self.last_row()
        self.assertEqual(row["status"], "did_work")
        self.assertEqual(row["counts"]["did_work"], 3)


class TestMainRestoresStderr(WrapperCase):
    def test_two_in_process_calls_leave_stderr_as_it_was(self) -> None:
        """source: main() must restore sys.stderr, or a second in-process call nests wrappers."""
        pr = load_runner()
        before = sys.stderr
        argv = ["--job", self.JOB, "--quiet", "--", "/bin/echo", "ok"]
        try:
            self.assertEqual(pr.main(argv), 0)
            self.assertEqual(pr.main(argv), 0)
            self.assertIs(sys.stderr, before)
        finally:
            sys.stderr = before


if __name__ == "__main__":
    unittest.main(verbosity=2)
