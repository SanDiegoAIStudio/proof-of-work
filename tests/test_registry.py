"""The shipped jobs.json, and how the registry loader treats good and bad files."""

from __future__ import annotations

import contextlib
import io
import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from typing import Any
from unittest import mock

from _common import HERE, SpineTempDir, child_env, run_main, ts
import _common
from _common import P

SHIPPED = HERE / "jobs.json"

ALL_KINDS = {
    "seen_within_hours", "work_within_hours", "max_miss_rate", "max_could_not_share",
    "max_consecutive_not_work", "queue_non_increasing", "min_items",
}


def shipped() -> dict[str, Any]:
    return json.loads(SHIPPED.read_text(encoding="utf-8"))


def jobs_only(raw: dict[str, Any]) -> dict[str, Any]:
    return {k: v for k, v in raw.items() if not k.startswith("_")}


class TestShippedRegistry(SpineTempDir):
    def setUp(self) -> None:
        super().setUp()
        P.REGISTRY_PATH = SHIPPED

    def test_it_loads_and_has_jobs(self) -> None:
        reg = P.load_registry()
        self.assertGreaterEqual(len(reg), 3)

    def test_every_job_has_a_description_and_at_least_one_invariant(self) -> None:
        for name, cfg in P.load_registry().items():
            with self.subTest(job=name):
                self.assertTrue(str(cfg.get("description", "")).strip())
                self.assertGreaterEqual(len(cfg.get("invariants", [])), 1)
                self.assertGreater(float(cfg.get("window_hours", 0)), 0)

    def test_every_job_tier_is_native_or_wrapped_when_given(self) -> None:
        for name, cfg in P.load_registry().items():
            with self.subTest(job=name):
                self.assertIn(cfg.get("tier", "native"), ("native", "wrapped"))

    def test_every_invariant_says_why_it_goes_red(self) -> None:
        for name, cfg in P.load_registry().items():
            for inv in cfg["invariants"]:
                with self.subTest(job=name, kind=inv.get("kind")):
                    self.assertTrue(str(inv.get("why", "")).strip())

    def test_every_invariant_evaluates_against_a_blank_summary_without_raising(self) -> None:
        empty = Path(tempfile.mkdtemp(prefix="pow-blank-"))
        self.addCleanup(lambda: empty.rmdir())
        blank = P.summarize("never-ran", 24.0, spine_dir=empty)
        for name, cfg in P.load_registry().items():
            for inv in cfg["invariants"]:
                with self.subTest(job=name, kind=inv["kind"]):
                    verdict = P.eval_invariant(inv, blank)
                    self.assertEqual(verdict["kind"], inv["kind"])
                    self.assertNotIn("unknown invariant", verdict["detail"])
                    if inv.get("quiet_ok"):
                        # quiet_ok says a quiet window is acceptable, so it may pass.
                        self.assertIn(verdict["ok"], (True, None))
                    else:
                        # A job that never ran is never reported healthy.
                        self.assertIsNot(verdict["ok"], True)

    def test_all_seven_kinds_appear(self) -> None:
        kinds = {inv["kind"] for cfg in P.load_registry().values() for inv in cfg["invariants"]}
        self.assertEqual(kinds, ALL_KINDS)

    def test_underscore_keys_are_notes_and_not_jobs(self) -> None:
        raw = shipped()
        self.assertTrue(any(k.startswith("_") for k in raw), "the shipped file should carry a note key")
        loaded = P.load_registry()
        self.assertFalse([k for k in loaded if k.startswith("_")])
        self.assertEqual(set(loaded), set(jobs_only(raw)))

    def test_no_phantom_job_comes_from_a_note_key(self) -> None:
        self.assertFalse([j for j in P.known_jobs() if j.startswith("_")])

    def test_the_jobs_command_lists_every_registered_job(self) -> None:
        code, out, _ = run_main(["jobs"])
        self.assertEqual(code, 0)
        for name in P.load_registry():
            self.assertIn(name, out)

    def test_a_fresh_install_reports_every_job_as_awaiting_and_check_passes(self) -> None:
        code, out, _ = run_main(["check"])
        self.assertEqual(code, 0)
        self.assertIn("AWAITING", out)


class TestLoader(SpineTempDir):
    def test_an_unreadable_registry_warns_on_stderr_and_returns_no_jobs(self) -> None:
        bad = self.dir / "bad.json"
        bad.write_text("{ this is not json", encoding="utf-8")
        P.REGISTRY_PATH = bad
        buf = io.StringIO()
        with contextlib.redirect_stderr(buf):
            reg = P.load_registry()
        self.assertEqual(reg, {})
        self.assertIn("WARN registry unreadable", buf.getvalue())
        self.assertIn("NO invariants will be checked", buf.getvalue())

    def test_a_registry_that_is_not_an_object_warns_too(self) -> None:
        bad = self.dir / "list.json"
        bad.write_text("[1, 2]", encoding="utf-8")
        P.REGISTRY_PATH = bad
        buf = io.StringIO()
        with contextlib.redirect_stderr(buf):
            reg = P.load_registry()
        self.assertEqual(reg, {})
        self.assertIn("not a JSON object", buf.getvalue())

    def test_a_missing_registry_is_just_empty(self) -> None:
        P.REGISTRY_PATH = self.dir / "absent.json"
        self.assertEqual(P.load_registry(), {})

    def test_the_check_command_surfaces_the_warning_for_a_broken_registry(self) -> None:
        bad = self.dir / "bad.json"
        bad.write_text("not json at all", encoding="utf-8")
        P.REGISTRY_PATH = bad
        code, _, err = run_main(["check"])
        self.assertIn("WARN registry unreadable", err)
        # Added in follow-up 2 (item H): a gate that checked nothing must not exit 0.
        self.assertEqual(code, 2)
        self.assertIn("pow: ERROR check was incomplete", err)


class TestEnvironmentSelectsTheRegistry(unittest.TestCase):
    """POW_REGISTRY is read at import, so the proof needs a fresh interpreter."""

    def run_cli(self, registry: Path, spine: Path, *args: str) -> subprocess.CompletedProcess:
        env = child_env(spine, POW_REGISTRY=str(registry), POW_ROLLUP=str(spine.parent / "roll.json"))
        return subprocess.run([sys.executable, str(HERE / "pow.py"), *args], capture_output=True, text=True,
                              env=env, timeout=60, check=False)

    def test_pow_registry_points_the_loader_at_another_file(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            spine = root / "spine"
            spine.mkdir()
            other = root / "other.json"
            other.write_text(json.dumps({
                "_note": "ignored",
                "orchard-picker": {
                    "description": "Counts the apples.",
                    "window_hours": 24,
                    "invariants": [{"kind": "seen_within_hours", "value": 5, "why": "no report"}],
                },
            }), encoding="utf-8")
            proc = self.run_cli(other, spine, "jobs")
            self.assertEqual(proc.returncode, 0, proc.stderr)
            self.assertIn("orchard-picker", proc.stdout)
            self.assertNotIn("nightly-backup", proc.stdout)
            self.assertNotIn("_note", proc.stdout)

    def test_without_the_variable_the_registry_beside_pow_py_is_used(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            env = child_env(Path(td) / "spine")
            env.pop("POW_REGISTRY", None)
            proc = subprocess.run([sys.executable, str(HERE / "pow.py"), "jobs"], capture_output=True,
                                  text=True, env=env, timeout=60, check=False)
            self.assertEqual(proc.returncode, 0, proc.stderr)
            self.assertIn("nightly-backup", proc.stdout)

    def test_a_pointer_at_a_broken_file_is_loud_in_a_real_process(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / "spine").mkdir()
            broken = root / "broken.json"
            broken.write_text("{nope", encoding="utf-8")
            proc = self.run_cli(broken, root / "spine", "check")
            self.assertIn("WARN registry unreadable", proc.stderr)
            # Added in follow-up 2 (item H).
            self.assertEqual(proc.returncode, 2)


def problems_for(raw: Any) -> tuple[dict[str, Any], list[str]]:
    """Write `raw` as the registry and load it the checked way."""
    path = P.REGISTRY_PATH
    path.write_text(raw if isinstance(raw, str) else json.dumps(raw), encoding="utf-8")
    return P.load_registry_checked()


class TestRegistryProblems(SpineTempDir):
    def setUp(self) -> None:
        super().setUp()
        P.REGISTRY_PATH = self.dir / "reg.json"

    def test_a_missing_file_has_no_problem(self) -> None:
        self.assertEqual(P.load_registry_checked(), ({}, []))

    def test_a_good_registry_has_no_problem(self) -> None:
        reg, problems = problems_for({"_n": "note", "a-job": {"window_hours": 48, "invariants": [
            {"kind": "seen_within_hours", "value": 3}]}})
        self.assertEqual(problems, [])
        self.assertEqual(list(reg), ["a-job"])
        self.assertEqual(reg["a-job"]["window_hours"], 48)

    def test_not_json(self) -> None:
        reg, problems = problems_for("{ nope")
        self.assertEqual(reg, {})
        self.assertEqual(len(problems), 1)
        self.assertIn("unreadable", problems[0])
        self.assertIn("NO invariants will be checked", problems[0])

    def test_bytes_that_are_not_text(self) -> None:
        P.REGISTRY_PATH.write_bytes(b"\xff\xfe{")
        reg, problems = P.load_registry_checked()
        self.assertEqual(reg, {})
        self.assertIn("NO invariants will be checked", problems[0])

    def test_nested_too_deep(self) -> None:
        reg, problems = problems_for("[" * 100000)
        self.assertEqual(reg, {})
        self.assertEqual(len(problems), 1)

    def test_not_an_object(self) -> None:
        reg, problems = problems_for([1, 2])
        self.assertEqual(reg, {})
        self.assertEqual(len(problems), 1)
        self.assertIn("not a JSON object", problems[0])
        self.assertIn("NO invariants will be checked", problems[0])

    def test_a_job_that_is_not_an_object_is_kept_with_no_settings(self) -> None:
        reg, problems = problems_for({"a-job": "text", "b-job": {"invariants": []}})
        self.assertEqual(reg["a-job"], {})
        self.assertIn("b-job", reg)
        self.assertEqual(len(problems), 1)
        self.assertIn("a-job", problems[0])

    def test_a_bad_window_hours_falls_back_to_the_default(self) -> None:
        for bad in ("12", 0, -5, True, False, None, [1], float("inf"), float("nan"), 1e400):
            with self.subTest(repr(bad)):
                text = json.dumps({"a-job": {"window_hours": bad, "invariants": []}})
                reg, problems = problems_for(text.replace("1e+400", "1e400"))
                self.assertNotIn("window_hours", reg["a-job"])
                self.assertEqual(len(problems), 1)
                self.assertIn("window_hours", problems[0])
                self.assertIn("24", problems[0])

    def test_a_good_window_hours_is_kept(self) -> None:
        reg, problems = problems_for({"a-job": {"window_hours": 0.5, "invariants": []}})
        self.assertEqual((reg["a-job"]["window_hours"], problems), (0.5, []))

    def test_invariants_that_is_not_a_list_is_an_empty_list(self) -> None:
        for bad in ({"kind": "seen_within_hours"}, "seen_within_hours", 5, None):
            with self.subTest(repr(bad)):
                reg, problems = problems_for({"a-job": {"invariants": bad}})
                self.assertEqual(reg["a-job"]["invariants"], [])
                self.assertEqual(len(problems), 1)
                self.assertIn("invariants", problems[0])

    def test_an_invariant_that_is_not_an_object_is_dropped(self) -> None:
        good = {"kind": "seen_within_hours", "value": 3}
        reg, problems = problems_for({"a-job": {"invariants": ["x", 5, None, good, [1]]}})
        self.assertEqual(reg["a-job"]["invariants"], [good])
        self.assertEqual(len(problems), 1)
        self.assertIn("a-job", problems[0])

    def test_load_registry_prints_each_problem_once_with_the_warn_prefix(self) -> None:
        problems_for({"a-job": {"window_hours": "x", "invariants": 5}})
        buf = io.StringIO()
        with contextlib.redirect_stderr(buf):
            P.load_registry()
        lines = buf.getvalue().splitlines()
        self.assertEqual(len(lines), 2)
        for line in lines:
            self.assertTrue(line.startswith("pow: WARN registry: "), line)


class TestBadParametersAndExitCodes(SpineTempDir):
    def two_jobs(self, bad: dict[str, Any]) -> None:
        self.registry({
            "alpha-sync": {"invariants": [bad]},
            "beta-scan": {"invariants": [{"kind": "seen_within_hours", "value": 24}]},
        })
        self.write("alpha-sync", [{"ts": ts(1), "job": "alpha-sync", "status": "did_work",
                                   "counts": {"did_work": 1}}])
        self.write("beta-scan", [{"ts": ts(1), "job": "beta-scan", "status": "did_work",
                                  "counts": {"did_work": 1}}])

    def test_a_bad_value_makes_that_verdict_unknown_and_the_check_incomplete(self) -> None:
        self.two_jobs({"kind": "min_items", "value": "three"})
        results = {c["job"]: c for c in P.check_all()}
        (u,) = results["alpha-sync"]["unknown"]
        self.assertTrue(u["detail"].startswith("bad parameter:"))
        self.assertIs(u["config_error"], True)
        self.assertIsNone(u["ok"])
        # The other job still gets its verdict.
        self.assertEqual([v["ok"] for v in results["beta-scan"]["verdicts"]], [True])
        code, out, err = run_main(["check"])
        self.assertEqual(code, 2)
        self.assertIn("UNKNOWN alpha-sync: min_items - bad parameter:", out)
        self.assertEqual(len([ln for ln in err.splitlines() if ln.startswith("pow: ERROR")]), 1)
        self.assertIn("incomplete", err)

    def test_every_kind_survives_a_bad_parameter(self) -> None:
        cases = [
            {"kind": "seen_within_hours", "value": "x"},
            {"kind": "work_within_hours", "value": None},
            {"kind": "max_miss_rate", "value": [1]},
            {"kind": "max_miss_rate", "value": True},
            {"kind": "max_could_not_share", "value": {}},
            {"kind": "max_consecutive_not_work", "value": "many"},
            {"kind": "max_consecutive_not_work", "value": float("inf")},
            {"kind": "queue_non_increasing", "tolerance": "a lot"},
            {"kind": "min_items", "value": 1, "field": 5},
        ]
        for inv in cases:
            with self.subTest(inv):
                self.two_jobs(inv)
                code, out, err = run_main(["check"])
                self.assertEqual(code, 2, (out, err))
                results = {c["job"]: c for c in P.check_all()}
                self.assertEqual([v["ok"] for v in results["beta-scan"]["verdicts"]], [True])

    def test_a_bad_parameter_beside_a_real_violation_exits_1(self) -> None:
        self.registry({
            "alpha-sync": {"invariants": [{"kind": "min_items", "value": "three"}]},
            "beta-scan": {"invariants": [{"kind": "seen_within_hours", "value": 1}]},
        })
        self.write("alpha-sync", [{"ts": ts(1), "job": "alpha-sync", "status": "did_work"}])
        self.write("beta-scan", [{"ts": ts(50), "job": "beta-scan", "status": "did_work"}])
        code, out, err = run_main(["check"])
        self.assertEqual(code, 1)
        self.assertIn("VIOLATED beta-scan", out)
        self.assertIn("UNKNOWN alpha-sync: min_items - bad parameter:", out)

    def test_an_unknown_kind_is_a_config_error_and_exit_2(self) -> None:
        self.two_jobs({"kind": "no_such_kind"})
        (u,) = [c for c in P.check_all() if c["job"] == "alpha-sync"][0]["unknown"]
        self.assertIs(u["config_error"], True)
        code, _, err = run_main(["check"])
        self.assertEqual(code, 2)
        self.assertIn("pow: ERROR check was incomplete", err)

    def test_a_good_parameter_is_not_a_config_error(self) -> None:
        self.two_jobs({"kind": "min_items", "value": 1})
        code, _, err = run_main(["check"])
        self.assertEqual(code, 0)
        self.assertEqual(err, "")

    def test_a_broken_registry_exits_2_and_names_it(self) -> None:
        bad = self.dir / "bad.json"
        bad.write_text("{nope", encoding="utf-8")
        P.REGISTRY_PATH = bad
        self.write("alpha-sync", [{"ts": ts(1), "job": "alpha-sync", "status": "did_work"}])
        code, out, err = run_main(["check"])
        self.assertEqual(code, 2)
        self.assertIn("pow: ERROR check was incomplete", err)
        self.assertIn("registry", err)

    def test_a_problem_in_one_job_exits_2_but_the_other_job_is_checked(self) -> None:
        self.registry({
            "alpha-sync": {"window_hours": "soon", "invariants": [{"kind": "seen_within_hours", "value": 5}]},
            "beta-scan": {"invariants": [{"kind": "seen_within_hours", "value": 24}]},
        })
        self.write("alpha-sync", [{"ts": ts(1), "job": "alpha-sync", "status": "did_work"}])
        self.write("beta-scan", [{"ts": ts(1), "job": "beta-scan", "status": "did_work"}])
        code, out, err = run_main(["check"])
        self.assertEqual(code, 2)
        self.assertIn("window_hours", err)
        self.assertEqual(len(P.check_all()), 2)

    def test_pow_registry_naming_a_missing_file_exits_2(self) -> None:
        P.REGISTRY_PATH = self.dir / "absent.json"
        self.write("alpha-sync", [{"ts": ts(1), "job": "alpha-sync", "status": "did_work"}])
        with mock.patch.dict(os.environ, {"POW_REGISTRY": str(P.REGISTRY_PATH)}):
            code, _, err = run_main(["check"])
        self.assertEqual(code, 2)
        self.assertIn("POW_REGISTRY", err)

    def test_a_missing_default_registry_stays_exit_0(self) -> None:
        P.REGISTRY_PATH = self.dir / "absent.json"
        with mock.patch.dict(os.environ):
            os.environ.pop("POW_REGISTRY", None)
            code, out, err = run_main(["check"])
        self.assertEqual(code, 0)
        self.assertEqual(err, "")

    def test_pow_registry_naming_a_missing_file_exits_2_in_a_real_process(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / "spine").mkdir()
            env = child_env(root / "spine", POW_REGISTRY=str(root / "absent.json"))
            proc = subprocess.run([sys.executable, str(HERE / "pow.py"), "check"], capture_output=True,
                                  text=True, env=env, timeout=60, check=False)
            self.assertEqual(proc.returncode, 2, proc.stdout + proc.stderr)
            self.assertIn("POW_REGISTRY", proc.stderr)


ODD_SHAPES: dict[str, Any] = {
    "a job that is a string": {"a-job": "text"},
    "a job that is null": {"a-job": None},
    "a job that is a list": {"a-job": [1]},
    "invariants that is an object": {"a-job": {"invariants": {"kind": "seen_within_hours"}}},
    "invariants holding non-objects": {"a-job": {"invariants": [1, "x", None, []]}},
    "a huge window": {"a-job": {"window_hours": 1e300, "invariants": [{"kind": "seen_within_hours"}]}},
    "a bool window": {"a-job": {"window_hours": True, "invariants": [{"kind": "seen_within_hours"}]}},
    "a string window": {"a-job": {"window_hours": "24", "invariants": []}},
    "a bool value": {"a-job": {"invariants": [{"kind": "seen_within_hours", "value": True}]}},
    "a bool tolerance": {"a-job": {"invariants": [{"kind": "queue_non_increasing", "tolerance": True}]}},
    "a list value": {"a-job": {"invariants": [{"kind": "min_items", "value": [3]}]}},
    "a non-string kind": {"a-job": {"invariants": [{"kind": 5}, {"kind": None}, {}]}},
    "a non-string field": {"a-job": {"invariants": [{"kind": "min_items", "field": ["x"]}]}},
    "a description that is a number": {"a-job": {"description": 5, "invariants": []}},
    "only notes": {"_a": 1, "_b": [2]},
    "an empty object": {},
}


class TestOddRegistriesInRealProcesses(unittest.TestCase):
    def test_no_command_raises_on_a_registry_of_any_shape(self) -> None:
        for name, raw in ODD_SHAPES.items():
            with tempfile.TemporaryDirectory() as td:
                root = Path(td)
                spine = root / "spine"
                spine.mkdir()
                row = {"ts": ts(1), "job": "a-job", "status": "did_work", "counts": {"did_work": 1}}
                (spine / "a-job.jsonl").write_text(json.dumps(row) + "\n", encoding="utf-8")
                reg = root / "reg.json"
                reg.write_text(json.dumps(raw), encoding="utf-8")
                env = child_env(spine, POW_REGISTRY=str(reg))
                for argv in (["report"], ["report", "--json"], ["jobs"], ["rollup"], ["check"],
                             ["check", "--json"]):
                    with self.subTest(shape=name, cmd=" ".join(argv)):
                        proc = subprocess.run([sys.executable, str(HERE / "pow.py"), *argv],
                                              capture_output=True, text=True, env=env, timeout=60,
                                              check=False)
                        self.assertNotIn("Traceback", proc.stderr)
                        self.assertIn(proc.returncode, (0, 1, 2), proc.stderr)
                        if argv == ["report", "--json"]:
                            self.assertIn("a-job", proc.stdout)

    def test_an_unreadable_registry_leaves_report_and_rollup_working(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            spine = root / "spine"
            spine.mkdir()
            row = {"ts": ts(1), "job": "a-job", "status": "did_work", "counts": {"did_work": 1}}
            (spine / "a-job.jsonl").write_text(json.dumps(row) + "\n", encoding="utf-8")
            reg = root / "reg.json"
            reg.write_bytes(b"\xff\xfe[[[")
            env = child_env(spine, POW_REGISTRY=str(reg))
            for argv in (["report"], ["rollup"], ["jobs"]):
                proc = subprocess.run([sys.executable, str(HERE / "pow.py"), *argv], capture_output=True,
                                      text=True, env=env, timeout=60, check=False)
                self.assertNotIn("Traceback", proc.stderr)
                self.assertEqual(proc.returncode, 0, proc.stderr)


if __name__ == "__main__":
    unittest.main(verbosity=2)
