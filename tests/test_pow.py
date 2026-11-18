"""Library behaviour: record, read, summarize, invariants, check, report, chip, rollup, CLI."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import time
import unittest
from datetime import datetime, timedelta, timezone
from typing import Any
from unittest import mock

from _common import HERE, SpineTempDir, child_env, run_main, ts

import chain  # noqa: E402
import pow as P  # noqa: E402


def summary_with(**over: Any) -> dict[str, Any]:
    """A blank summary with the given fields replaced."""
    base: dict[str, Any] = {
        "queue_delta": None, "queue_first": None, "queue_last": None,
        "miss_rate": None, "last_did_work": None, "last_seen": None,
        "items": {"did_work": 0, "no_work": 0, "could_not": 0},
        "runs_by_status": {"did_work": 0, "no_work": 0, "could_not": 0},
        "trailing_not_work": 0, "total_records": 0, "unclassified_quiet": 0,
    }
    base.update(over)
    return base


def row(hours_ago: float, status: str, job: str = "alpha-sync", **extra: Any) -> dict[str, Any]:
    counts = {"did_work": 0, "no_work": 0, "could_not": 0}
    counts[status] = 1
    rec: dict[str, Any] = {"v": 1, "ts": ts(hours_ago), "job": job, "status": status, "counts": counts}
    rec.update(extra)
    return rec


class TestRecord(SpineTempDir):
    def test_writes_one_line_per_call(self) -> None:
        P.record("alpha-sync", "did_work", did_work=3, spine_dir=self.spine)
        P.record("alpha-sync", "no_work", no_work=1, spine_dir=self.spine)
        rows = self.rows("alpha-sync")
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["status"], "did_work")
        self.assertEqual(rows[0]["counts"], {"did_work": 3, "no_work": 0, "could_not": 0})
        self.assertEqual(rows[0]["job"], "alpha-sync")
        self.assertEqual(rows[0]["v"], 1)

    def test_default_spine_is_the_module_setting(self) -> None:
        path = P.record("alpha-sync", "did_work", did_work=1)
        self.assertEqual(path, self.spine / "alpha-sync.jsonl")

    def test_two_records_form_a_verifying_chain(self) -> None:
        P.record("alpha-sync", "did_work", did_work=1, spine_dir=self.spine)
        P.record("alpha-sync", "could_not", could_not=1, reason="boom", spine_dir=self.spine)
        res = chain.verify_file(str(self.spine / "alpha-sync.jsonl"))
        self.assertTrue(res["ok"])
        self.assertEqual(res["rows"], 2)
        self.assertEqual(res["unhashed"], 0)
        rows = self.rows("alpha-sync")
        self.assertEqual(rows[0]["prev_row_hash"], "genesis")
        self.assertEqual(rows[1]["prev_row_hash"], rows[0]["row_hash"])

    def test_rejects_unknown_status(self) -> None:
        with self.assertRaises(ValueError):
            P.record("alpha-sync", "success", spine_dir=self.spine)
        self.assertFalse((self.spine / "alpha-sync.jsonl").exists())

    def test_rejects_job_names_that_leave_the_spine_folder(self) -> None:
        for bad in ("../evil", "a/b", ".hidden", ""):
            with self.subTest(job=bad):
                with self.assertRaises(ValueError):
                    P.record(bad, "did_work", spine_dir=self.spine)
        self.assertEqual(list(self.spine.iterdir()), [])
        self.assertEqual(list(self.dir.glob("evil*")), [])

    def test_queue_reason_duration_and_evidence_round_trip(self) -> None:
        P.record("alpha-sync", "could_not", could_not=1, queue_depth=42, queue_unit="messages",
                 duration_ms=1500, reason="upstream refused", evidence={"exit_code": 3},
                 spine_dir=self.spine)
        rec = self.rows("alpha-sync")[0]
        self.assertEqual(rec["queue"], {"depth": 42, "unit": "messages"})
        self.assertEqual(rec["duration_ms"], 1500)
        self.assertEqual(rec["reason"], "upstream refused")
        self.assertEqual(rec["evidence"], {"exit_code": 3})

    def test_queue_depth_zero_is_recorded(self) -> None:
        P.record("alpha-sync", "no_work", no_work=1, queue_depth=0, spine_dir=self.spine)
        self.assertEqual(self.rows("alpha-sync")[0]["queue"]["depth"], 0)

    def test_queue_unit_defaults_to_items(self) -> None:
        P.record("alpha-sync", "no_work", no_work=1, queue_depth=4, spine_dir=self.spine)
        self.assertEqual(self.rows("alpha-sync")[0]["queue"]["unit"], "items")

    def test_timestamp_is_utc_and_current(self) -> None:
        P.record("alpha-sync", "did_work", did_work=1, spine_dir=self.spine)
        stamp = self.rows("alpha-sync")[0]["ts"]
        self.assertTrue(stamp.endswith("Z"))
        parsed = datetime.fromisoformat(stamp.replace("Z", "+00:00"))
        self.assertLess(abs((datetime.now(timezone.utc) - parsed).total_seconds()), 60)


class TestHeartbeat(SpineTempDir):
    def test_no_heartbeat_file_when_the_variable_is_unset(self) -> None:
        hb = self.dir / "hb"
        hb.mkdir()
        P.record("alpha-sync", "did_work", did_work=1, spine_dir=self.spine)
        self.assertEqual(list(hb.iterdir()), [])

    def test_heartbeat_file_is_touched_when_the_folder_is_set(self) -> None:
        hb = self.dir / "hb"
        hb.mkdir()
        P.HEARTBEAT_DIR = hb
        P.record("alpha-sync", "did_work", did_work=1, spine_dir=self.spine)
        self.assertTrue((hb / "alpha-sync.last").is_file())

    def test_heartbeat_false_skips_the_touch(self) -> None:
        hb = self.dir / "hb"
        hb.mkdir()
        P.HEARTBEAT_DIR = hb
        P.record("alpha-sync", "did_work", did_work=1, spine_dir=self.spine, heartbeat=False)
        self.assertEqual(list(hb.iterdir()), [])

    def test_no_heartbeat_flag_skips_the_touch(self) -> None:
        hb = self.dir / "hb"
        hb.mkdir()
        P.HEARTBEAT_DIR = hb
        code, _, _ = run_main(["record", "--job", "alpha-sync", "--status", "no_work",
                               "--no-heartbeat"])
        self.assertEqual(code, 0)
        self.assertEqual(list(hb.iterdir()), [])
        code, _, _ = run_main(["record", "--job", "alpha-sync", "--status", "no_work"])
        self.assertEqual(code, 0)
        self.assertTrue((hb / "alpha-sync.last").is_file())

    def test_a_missing_heartbeat_folder_is_not_created_and_not_an_error(self) -> None:
        P.HEARTBEAT_DIR = self.dir / "does-not-exist"
        P.record("alpha-sync", "did_work", did_work=1, spine_dir=self.spine)
        self.assertFalse(P.HEARTBEAT_DIR.exists())
        self.assertEqual(len(self.rows("alpha-sync")), 1)


class TestReadRecords(SpineTempDir):
    def test_missing_file_reads_as_empty(self) -> None:
        self.assertEqual(P.read_records("nobody", spine_dir=self.spine), [])

    def test_a_malformed_line_comes_back_flagged_not_dropped(self) -> None:
        self.write("alpha-sync", [row(1, "did_work"), "{not json", row(0.5, "no_work")])
        recs = P.read_records("alpha-sync", spine_dir=self.spine)
        self.assertEqual(len(recs), 3)
        self.assertTrue(recs[1].get("_malformed"))

    def test_since_drops_old_rows_and_rows_with_no_readable_time(self) -> None:
        self.write("alpha-sync", [row(100, "did_work"), row(1, "did_work"),
                                  {"status": "did_work", "ts": "garbage"}])
        since = datetime.now(timezone.utc) - timedelta(hours=24)
        recs = P.read_records("alpha-sync", since=since, spine_dir=self.spine)
        self.assertEqual(len(recs), 1)

    def test_known_jobs_is_registry_plus_ledger_files_sorted(self) -> None:
        self.registry({"_note": "x", "zeta-job": {"description": "d", "invariants": []}})
        self.write("alpha-sync", [row(1, "did_work")])
        self.assertEqual(P.known_jobs(spine_dir=self.spine), ["alpha-sync", "zeta-job"])


class TestSummarize(SpineTempDir):
    def summarize(self, window: float = 24.0) -> dict[str, Any]:
        return P.summarize("alpha-sync", window_hours=window, spine_dir=self.spine)

    def test_miss_rate_excludes_no_ops(self) -> None:
        self.write("alpha-sync", [row(9, "did_work")] + [row(8 - i * 0.1, "no_work") for i in range(8)]
                   + [row(1, "could_not")])
        s = self.summarize()
        self.assertEqual(s["miss_rate"], 0.5)
        self.assertEqual(s["runs"], 10)
        self.assertEqual(s["runs_by_status"], {"did_work": 1, "no_work": 8, "could_not": 1})

    def test_miss_rate_is_none_when_nothing_was_attempted(self) -> None:
        self.write("alpha-sync", [row(3, "no_work"), row(2, "no_work")])
        self.assertIsNone(self.summarize()["miss_rate"])

    def test_queue_delta_uses_the_window_endpoints(self) -> None:
        self.write("alpha-sync", [
            row(10, "no_work", queue={"depth": 100, "unit": "items"}),
            row(5, "no_work", queue={"depth": 140, "unit": "items"}),
            row(1, "no_work", queue={"depth": 180, "unit": "items"}),
        ])
        s = self.summarize()
        self.assertEqual(s["queue_delta"], 80)
        self.assertEqual((s["queue_first"], s["queue_last"]), (100, 180))

    def test_a_single_queue_observation_gives_no_delta(self) -> None:
        self.write("alpha-sync", [row(1, "no_work", queue={"depth": 7, "unit": "items"})])
        s = self.summarize()
        self.assertIsNone(s["queue_delta"])
        verdict = P.eval_invariant({"kind": "queue_non_increasing"}, s)
        self.assertIsNone(verdict["ok"])

    def test_rows_outside_the_window_are_excluded_but_last_work_spans_history(self) -> None:
        old_work = row(200, "did_work")
        self.write("alpha-sync", [old_work, row(1, "no_work")])
        s = self.summarize(24)
        self.assertEqual(s["runs"], 1)
        self.assertEqual(s["last_did_work"], old_work["ts"])
        self.assertEqual(s["total_records"], 2)

    def test_malformed_lines_are_counted(self) -> None:
        self.write("alpha-sync", ["{broken", row(1, "did_work")])
        s = self.summarize()
        self.assertEqual(s["malformed_lines"], 1)
        self.assertEqual(s["runs"], 1)

    def test_unproven_until_the_first_did_work(self) -> None:
        self.write("alpha-sync", [row(2, "no_work"), row(1, "could_not")])
        self.assertTrue(self.summarize()["unproven"])
        self.write("alpha-sync", [row(2, "no_work"), row(1, "did_work")])
        self.assertFalse(self.summarize()["unproven"])

    def test_trailing_not_work_counts_back_to_the_last_did_work(self) -> None:
        self.write("alpha-sync", [row(4, "did_work"), row(3, "no_work"), row(2, "could_not")])
        self.assertEqual(self.summarize()["trailing_not_work"], 2)

    def test_trailing_not_work_is_zero_after_recovery(self) -> None:
        self.write("alpha-sync", [row(4, "no_work"), row(3, "could_not"), row(1, "did_work")])
        self.assertEqual(self.summarize()["trailing_not_work"], 0)

    def test_trailing_not_work_ignores_the_window(self) -> None:
        self.write("alpha-sync", [row(200, "no_work"), row(100, "no_work"), row(1, "no_work")])
        self.assertEqual(self.summarize(24)["trailing_not_work"], 3)

    def test_unclassified_quiet_counts_only_unclassified_no_work_rows(self) -> None:
        self.write("alpha-sync", [
            row(3, "no_work", evidence={"classification": "unclassified"}),
            row(2, "no_work", evidence={"classification": "pattern"}),
            row(1, "no_work"),
        ])
        self.assertEqual(self.summarize()["unclassified_quiet"], 1)

    def test_items_sum_the_per_row_counts(self) -> None:
        a = row(3, "did_work")
        a["counts"]["did_work"] = 4
        b = row(2, "could_not")
        self.write("alpha-sync", [a, b])
        self.assertEqual(self.summarize()["items"], {"did_work": 4, "no_work": 0, "could_not": 1})


class TestInvariants(unittest.TestCase):
    def test_queue_growth_violates(self) -> None:
        v = P.eval_invariant({"kind": "queue_non_increasing", "tolerance": 5},
                             summary_with(queue_delta=60, queue_first=10, queue_last=70))
        self.assertIs(v["ok"], False)
        self.assertIn("+60", v["detail"])

    def test_queue_drain_holds(self) -> None:
        v = P.eval_invariant({"kind": "queue_non_increasing"},
                             summary_with(queue_delta=-30, queue_first=50, queue_last=20))
        self.assertIs(v["ok"], True)

    def test_queue_growth_inside_the_tolerance_holds_and_outside_does_not(self) -> None:
        s = summary_with(queue_delta=5, queue_first=10, queue_last=15)
        self.assertIs(P.eval_invariant({"kind": "queue_non_increasing", "tolerance": 5}, s)["ok"], True)
        self.assertIs(P.eval_invariant({"kind": "queue_non_increasing", "tolerance": 4}, s)["ok"], False)
        self.assertIs(P.eval_invariant({"kind": "queue_non_increasing"}, s)["ok"], False)

    def test_missing_queue_data_is_unknown(self) -> None:
        v = P.eval_invariant({"kind": "queue_non_increasing"}, summary_with())
        self.assertIsNone(v["ok"])

    def test_miss_rate_over_and_under_the_limit(self) -> None:
        inv = {"kind": "max_miss_rate", "value": 0.1}
        self.assertIs(P.eval_invariant(inv, summary_with(miss_rate=0.5))["ok"], False)
        self.assertIs(P.eval_invariant(inv, summary_with(miss_rate=0.05))["ok"], True)
        self.assertIs(P.eval_invariant(inv, summary_with(miss_rate=0.1))["ok"], True)

    def test_miss_rate_with_no_attempts_is_unknown_by_default(self) -> None:
        v = P.eval_invariant({"kind": "max_miss_rate", "value": 0.1}, summary_with())
        self.assertIsNone(v["ok"])
        self.assertIn("no attempted runs", v["detail"])

    def test_quiet_ok_turns_an_all_quiet_window_into_ok(self) -> None:
        # Changed on purpose (follow-up 2, item F): quiet_ok now needs a run
        # recorded no_work in the window, so the summary supplies some. The old
        # version passed a blank summary, which is a job that did not run.
        quiet = summary_with(runs=3, runs_by_status={"did_work": 0, "no_work": 3, "could_not": 0})
        v = P.eval_invariant({"kind": "max_miss_rate", "value": 0.1, "quiet_ok": True}, quiet)
        self.assertIs(v["ok"], True)
        self.assertIn("quiet", v["detail"])

    def test_quiet_ok_with_no_runs_at_all_is_unknown(self) -> None:
        v = P.eval_invariant({"kind": "max_miss_rate", "value": 0.1, "quiet_ok": True}, summary_with())
        self.assertIsNone(v["ok"])
        self.assertEqual(v["detail"], "no runs in window")

    def test_quiet_ok_is_not_earned_by_unclassified_quiet_rows(self) -> None:
        v = P.eval_invariant({"kind": "max_miss_rate", "value": 0.1, "quiet_ok": True},
                             summary_with(unclassified_quiet=2))
        self.assertIsNone(v["ok"])
        self.assertIn("unclassified", v["detail"])

    def test_quiet_ok_never_hides_a_real_miss_rate(self) -> None:
        v = P.eval_invariant({"kind": "max_miss_rate", "value": 0.1, "quiet_ok": True},
                             summary_with(miss_rate=0.4))
        self.assertIs(v["ok"], False)

    def test_never_worked_fails_work_within_hours(self) -> None:
        v = P.eval_invariant({"kind": "work_within_hours", "value": 24}, summary_with())
        self.assertIs(v["ok"], False)
        self.assertIn(P.UNPROVEN, v["detail"])

    def test_work_recency_fresh_and_stale(self) -> None:
        inv = {"kind": "work_within_hours", "value": 24}
        self.assertIs(P.eval_invariant(inv, summary_with(last_did_work=ts(2)))["ok"], True)
        self.assertIs(P.eval_invariant(inv, summary_with(last_did_work=ts(30)))["ok"], False)

    def test_seen_within_hours_stale_fresh_and_never(self) -> None:
        inv = {"kind": "seen_within_hours", "value": 12}
        self.assertIs(P.eval_invariant(inv, summary_with(last_seen=ts(20)))["ok"], False)
        self.assertIs(P.eval_invariant(inv, summary_with(last_seen=ts(1)))["ok"], True)
        self.assertIs(P.eval_invariant(inv, summary_with())["ok"], False)

    def test_an_unparseable_timestamp_fails_seen_within_hours(self) -> None:
        v = P.eval_invariant({"kind": "seen_within_hours", "value": 12},
                             summary_with(last_seen="not a time"))
        self.assertIs(v["ok"], False)

    def test_min_items_reads_the_named_field(self) -> None:
        s = summary_with(items={"did_work": 2, "no_work": 0, "could_not": 9})
        self.assertIs(P.eval_invariant({"kind": "min_items", "value": 5}, s)["ok"], False)
        self.assertIs(P.eval_invariant({"kind": "min_items", "value": 2}, s)["ok"], True)
        self.assertIs(P.eval_invariant({"kind": "min_items", "value": 5, "field": "could_not"}, s)["ok"], True)

    def test_could_not_share_divides_by_every_run(self) -> None:
        s = summary_with(runs_by_status={"did_work": 2, "no_work": 81, "could_not": 1}, miss_rate=1 / 3)
        v = P.eval_invariant({"kind": "max_could_not_share", "value": 0.05}, s)
        self.assertIs(v["ok"], True)
        self.assertIn("1% of 84 runs", v["detail"])
        self.assertIs(P.eval_invariant({"kind": "max_miss_rate", "value": 0.05}, s)["ok"], False)

    def test_could_not_share_over_the_limit(self) -> None:
        s = summary_with(runs_by_status={"did_work": 1, "no_work": 3, "could_not": 4})
        v = P.eval_invariant({"kind": "max_could_not_share", "value": 0.25}, s)
        self.assertIs(v["ok"], False)
        self.assertIn("50% of 8 runs", v["detail"])

    def test_could_not_share_with_no_runs_is_unknown_even_when_quiet_ok_is_set(self) -> None:
        # No runs at all is silence, and silence is never health. quiet_ok belongs to max_miss_rate.
        for inv in ({"kind": "max_could_not_share", "value": 0.25},
                    {"kind": "max_could_not_share", "value": 0.25, "quiet_ok": True}):
            with self.subTest(inv=inv):
                v = P.eval_invariant(inv, summary_with())
                self.assertIsNone(v["ok"])
                self.assertEqual(v["detail"], "no runs in window")

    def test_consecutive_not_work(self) -> None:
        inv = {"kind": "max_consecutive_not_work", "value": 3}
        at_limit = summary_with(trailing_not_work=3, total_records=10)
        below = summary_with(trailing_not_work=2, total_records=10)
        thin = summary_with(trailing_not_work=2, total_records=2)
        self.assertIs(P.eval_invariant(inv, at_limit)["ok"], False)
        self.assertIs(P.eval_invariant(inv, below)["ok"], True)
        self.assertIsNone(P.eval_invariant(inv, thin)["ok"])
        self.assertIn("too thin", P.eval_invariant(inv, thin)["detail"])

    def test_unknown_kind_is_unknown_not_ok(self) -> None:
        v = P.eval_invariant({"kind": "made_up"}, summary_with())
        self.assertIsNone(v["ok"])
        self.assertIn("unknown invariant kind", v["detail"])


class TestAQuietBrokenJobIsNotHealthy(SpineTempDir):
    """A queue that grows under runs that all say no_work looks calm in a naive miss rate."""

    def test_growing_queue_with_only_no_work_rows(self) -> None:
        self.write("beta-export", [
            row(10 - i, "no_work", job="beta-export", queue={"depth": 100 + 10 * i, "unit": "items"})
            for i in range(6)
        ])
        s = P.summarize("beta-export", window_hours=24, spine_dir=self.spine)
        self.assertIsNone(s["miss_rate"])
        self.assertTrue(s["unproven"])
        self.assertIs(P.eval_invariant({"kind": "queue_non_increasing", "tolerance": 5}, s)["ok"], False)
        self.assertIs(P.eval_invariant({"kind": "work_within_hours", "value": 24}, s)["ok"], False)
        self.assertIsNone(P.eval_invariant({"kind": "max_miss_rate", "value": 0.1}, s)["ok"])


class TestRegistryLoading(SpineTempDir):
    def test_underscore_keys_are_notes_not_jobs(self) -> None:
        self.registry({"_comment": "a note", "gamma-scan": {"description": "d", "invariants": []}})
        self.assertEqual(list(P.load_registry()), ["gamma-scan"])

    def test_a_missing_registry_file_is_no_jobs(self) -> None:
        self.assertEqual(P.load_registry(), {})

    def test_an_unreadable_registry_warns_on_stderr_and_returns_no_jobs(self) -> None:
        import contextlib
        import io
        path = self.dir / "jobs.json"
        path.write_text("{ this is not json", encoding="utf-8")
        P.REGISTRY_PATH = path
        err = io.StringIO()
        with contextlib.redirect_stderr(err):
            self.assertEqual(P.load_registry(), {})
        self.assertIn("WARN", err.getvalue())
        self.assertIn("NO invariants", err.getvalue())

    def test_a_registry_that_is_not_an_object_warns_too(self) -> None:
        import contextlib
        import io
        path = self.dir / "jobs.json"
        path.write_text("[1, 2]", encoding="utf-8")
        P.REGISTRY_PATH = path
        err = io.StringIO()
        with contextlib.redirect_stderr(err):
            self.assertEqual(P.load_registry(), {})
        self.assertIn("WARN", err.getvalue())


class TestChipSummary(SpineTempDir):
    def test_no_jobs_is_a_warning(self) -> None:
        chip = P.chip_summary(spine_dir=self.spine)
        self.assertEqual(chip["level"], "warn")
        self.assertIn("no jobs", chip["text"])

    def test_an_invariant_that_cannot_be_evaluated_is_not_clean(self) -> None:
        self.registry({"alpha-sync": {"description": "d", "window_hours": 24,
                                      "invariants": [{"kind": "max_miss_rate", "value": 0.1}]}})
        self.write("alpha-sync", [row(1, "no_work")])
        chip = P.chip_summary(spine_dir=self.spine)
        self.assertEqual(chip["level"], "warn")
        self.assertEqual(len(chip["unknown"]), 1)
        self.assertNotIn("clean", chip["text"])

    def test_a_job_with_no_invariants_is_uncovered(self) -> None:
        self.registry({"alpha-sync": {"description": "d", "invariants": []}})
        self.write("alpha-sync", [row(1, "did_work")])
        chip = P.chip_summary(spine_dir=self.spine)
        self.assertEqual(chip["level"], "warn")
        self.assertEqual(chip["uncovered"], ["alpha-sync"])

    def test_a_job_on_disk_but_not_in_the_registry_is_uncovered(self) -> None:
        self.registry({"alpha-sync": {"description": "d", "invariants": [{"kind": "seen_within_hours", "value": 24}]}})
        self.write("alpha-sync", [row(1, "did_work")])
        self.write("stray-job", [row(1, "did_work", job="stray-job")])
        chip = P.chip_summary(spine_dir=self.spine)
        self.assertEqual(chip["uncovered"], ["stray-job"])
        self.assertEqual(chip["level"], "warn")

    def test_a_registered_job_that_never_reported_is_awaiting(self) -> None:
        self.registry({"alpha-sync": {"description": "d", "invariants": [{"kind": "seen_within_hours", "value": 24}]}})
        chip = P.chip_summary(spine_dir=self.spine)
        self.assertEqual(chip["violated"], [])
        self.assertEqual(chip["awaiting"], ["alpha-sync"])
        self.assertEqual(chip["level"], "warn")
        self.assertIn("awaiting", chip["text"])

    def test_a_real_violation_outranks_the_warnings(self) -> None:
        self.registry({
            "alpha-sync": {"description": "d", "window_hours": 24,
                           "invariants": [{"kind": "seen_within_hours", "value": 1}]},
            "beta-export": {"description": "d", "invariants": [{"kind": "seen_within_hours", "value": 1}]},
        })
        self.write("alpha-sync", [row(30, "did_work")])
        chip = P.chip_summary(spine_dir=self.spine)
        self.assertEqual(chip["level"], "err")
        self.assertIn("alpha-sync", chip["text"])
        self.assertEqual(chip["awaiting"], ["beta-export"])

    def test_all_good_is_ok_and_says_clean(self) -> None:
        self.registry({"alpha-sync": {"description": "d", "window_hours": 24,
                                      "invariants": [{"kind": "seen_within_hours", "value": 24},
                                                     {"kind": "work_within_hours", "value": 24}]}})
        self.write("alpha-sync", [row(1, "did_work")])
        chip = P.chip_summary(spine_dir=self.spine)
        self.assertEqual(chip["level"], "ok")
        self.assertIn("clean", chip["text"])
        self.assertEqual(chip["n_jobs"], 1)


class TestReportAndCheckAgree(SpineTempDir):
    """report and check must reach the same verdict about the same job."""

    def setUp(self) -> None:
        super().setUp()
        self.registry({
            "_comment": "notes are not jobs",
            "slowjob": {"description": "runs every few days", "window_hours": 192,
                        "invariants": [{"kind": "max_miss_rate", "value": 0.1}]},
        })
        # Two runs about 100h old: inside the declared 192h window, outside 24h.
        self.write("slowjob", [row(100, "did_work", job="slowjob"),
                               row(99, "could_not", job="slowjob", reason="boom")])

    def test_check_honors_the_declared_window(self) -> None:
        results = P.check_all(spine_dir=self.spine)
        violated = [v for c in results for v in c["violated"]]
        self.assertEqual(len(violated), 1)
        self.assertEqual(violated[0]["kind"], "max_miss_rate")

    def test_report_honors_the_declared_window_too(self) -> None:
        out = P.render_report(spine_dir=self.spine)
        self.assertIn("INVARIANTS VIOLATED", out)
        self.assertIn("slowjob", out)

    def test_report_and_check_agree_by_default(self) -> None:
        checked = {c["job"] for c in P.check_all(spine_dir=self.spine) if c["violated"]}
        reported = P.render_report(spine_dir=self.spine)
        self.assertTrue(checked)
        for job in checked:
            self.assertIn(job, reported.split("INVARIANTS VIOLATED", 1)[-1])

    def test_an_explicit_window_override_still_applies(self) -> None:
        results = P.check_all(window_hours=24, spine_dir=self.spine)
        self.assertEqual([v for c in results for v in c["violated"]], [])
        unknown = [v for c in results for v in c["unknown"]]
        self.assertEqual(len(unknown), 1)
        self.assertIn("no attempted runs", unknown[0]["detail"])

    def test_report_json_honors_the_declared_window_without_a_flag(self) -> None:
        code, out, _ = run_main(["report", "--json"])
        self.assertEqual(code, 0)
        rows = {r["job"]: r for r in json.loads(out)}
        self.assertEqual(rows["slowjob"]["window_hours"], 192.0)

    def test_report_json_flag_overrides_the_window(self) -> None:
        code, out, _ = run_main(["report", "--json", "--window-hours", "24"])
        rows = {r["job"]: r for r in json.loads(out)}
        self.assertEqual(rows["slowjob"]["window_hours"], 24.0)

    def test_report_header_names_which_window_it_used(self) -> None:
        self.assertIn("declared window", P.render_report(spine_dir=self.spine))
        self.assertIn("last 24h", P.render_report(window_hours=24, spine_dir=self.spine))

    def test_chip_summary_agrees_with_check(self) -> None:
        chip = P.chip_summary(spine_dir=self.spine)
        self.assertEqual(chip["level"], "err")
        self.assertIn(("slowjob", "max_miss_rate"), chip["violated"])


class TestReportText(SpineTempDir):
    def test_an_empty_spine_says_no_job_has_reported(self) -> None:
        out = P.render_report(spine_dir=self.spine)
        self.assertIn("no jobs have reported yet", out)

    def test_a_job_outside_the_registry_is_listed_as_a_coverage_gap(self) -> None:
        self.registry({"alpha-sync": {"description": "d", "invariants": []}})
        self.write("stray-job", [row(1, "did_work", job="stray-job")])
        out = P.render_report(spine_dir=self.spine)
        self.assertIn("coverage gaps", out)
        self.assertIn("stray-job: not in the registry", out)

    def test_never_worked_shows_as_unproven_not_as_a_zero_percent_miss(self) -> None:
        self.write("alpha-sync", [row(2, "no_work"), row(1, "no_work")])
        out = P.render_report(spine_dir=self.spine)
        self.assertIn(P.UNPROVEN, out)

    def test_awaiting_jobs_are_listed_apart_from_violations(self) -> None:
        self.registry({"alpha-sync": {"description": "d",
                                      "invariants": [{"kind": "seen_within_hours", "value": 1}]}})
        out = P.render_report(spine_dir=self.spine)
        self.assertIn("awaiting first report", out)
        self.assertNotIn("INVARIANTS VIOLATED", out)

    def test_output_is_ascii(self) -> None:
        self.registry({"alpha-sync": {"description": "d",
                                      "invariants": [{"kind": "seen_within_hours", "value": 1}]}})
        self.write("alpha-sync", [row(30, "did_work")])
        P.render_report(spine_dir=self.spine).encode("ascii")


class TestCli(SpineTempDir):
    def test_record_writes_a_row_and_reports_where(self) -> None:
        code, out, _ = run_main(["record", "--job", "alpha-sync", "--status", "did_work",
                                 "--did-work", "5", "--queue-depth", "9", "--queue-unit", "mails",
                                 "--duration-ms", "12", "--reason", "ok",
                                 "--evidence", '{"k": 1}'])
        self.assertEqual(code, 0)
        self.assertIn("recorded alpha-sync", out)
        rec = self.rows("alpha-sync")[0]
        self.assertEqual(rec["counts"]["did_work"], 5)
        self.assertEqual(rec["queue"], {"depth": 9, "unit": "mails"})
        self.assertEqual(rec["evidence"], {"k": 1})

    def test_record_with_a_bad_job_name_exits_2(self) -> None:
        code, _, err = run_main(["record", "--job", "../x", "--status", "did_work"])
        self.assertEqual(code, 2)
        self.assertIn("invalid job name", err)
        self.assertEqual(list(self.spine.iterdir()), [])

    def test_record_with_bad_evidence_exits_2(self) -> None:
        code, _, err = run_main(["record", "--job", "alpha-sync", "--status", "did_work",
                                 "--evidence", "{nope"])
        self.assertEqual(code, 2)
        self.assertIn("not valid JSON", err)
        code, _, err = run_main(["record", "--job", "alpha-sync", "--status", "did_work",
                                 "--evidence", "[1]"])
        self.assertEqual(code, 2)
        self.assertEqual(list(self.spine.iterdir()), [])

    def test_an_unknown_status_is_refused_by_the_parser(self) -> None:
        import contextlib
        import io
        with contextlib.redirect_stderr(io.StringIO()):
            with self.assertRaises(SystemExit) as ctx:
                P.main(["record", "--job", "alpha-sync", "--status", "success"])
        self.assertEqual(ctx.exception.code, 2)

    def test_check_exits_1_on_a_violation_and_names_it(self) -> None:
        self.registry({"alpha-sync": {"description": "d", "invariants": [{"kind": "seen_within_hours", "value": 1}]}})
        self.write("alpha-sync", [row(30, "did_work")])
        code, out, _ = run_main(["check"])
        self.assertEqual(code, 1)
        self.assertIn("VIOLATED alpha-sync: seen_within_hours", out)

    def test_check_exits_0_when_clean(self) -> None:
        self.registry({"alpha-sync": {"description": "d", "invariants": [{"kind": "seen_within_hours", "value": 5}]}})
        self.write("alpha-sync", [row(1, "did_work")])
        code, out, _ = run_main(["check"])
        self.assertEqual(code, 0)
        self.assertIn("no invariant violations", out)

    def test_a_job_that_never_reported_is_awaiting_not_a_failure(self) -> None:
        self.registry({"alpha-sync": {"description": "d", "invariants": [{"kind": "seen_within_hours", "value": 5}]}})
        code, out, _ = run_main(["check"])
        self.assertEqual(code, 0)
        self.assertIn("AWAITING alpha-sync", out)

    def test_check_json_lists_every_job(self) -> None:
        self.registry({"alpha-sync": {"description": "d", "invariants": [{"kind": "seen_within_hours", "value": 5}]}})
        self.write("alpha-sync", [row(1, "did_work")])
        code, out, _ = run_main(["check", "--json"])
        data = json.loads(out)
        self.assertEqual(code, 0)
        self.assertEqual(data[0]["job"], "alpha-sync")
        self.assertEqual(data[0]["violated"], [])

    def test_jobs_lists_each_invariant(self) -> None:
        self.registry({"alpha-sync": {"description": "pulls mail",
                                      "invariants": [{"kind": "min_items", "value": 2, "field": "did_work"}]}})
        code, out, _ = run_main(["jobs"])
        self.assertEqual(code, 0)
        self.assertIn("alpha-sync: pulls mail", out)
        self.assertIn("min_items", out)

    def test_jobs_with_no_registry_says_so(self) -> None:
        code, out, _ = run_main(["jobs"])
        self.assertEqual(code, 0)
        self.assertIn("no jobs registered", out)

    def test_report_text_runs(self) -> None:
        self.write("alpha-sync", [row(1, "did_work")])
        code, out, _ = run_main(["report"])
        self.assertEqual(code, 0)
        self.assertIn("alpha-sync", out)


class TestRollup(SpineTempDir):
    def test_rollup_counts_per_day_and_job(self) -> None:
        self.write("alpha-sync", [
            {"ts": "2026-03-01T01:00:00Z", "status": "did_work", "counts": {"did_work": 3},
             "queue": {"depth": 10, "unit": "items"}},
            {"ts": "2026-03-01T02:00:00Z", "status": "could_not", "counts": {"could_not": 1},
             "queue": {"depth": 12, "unit": "items"}},
            {"ts": "2026-03-02T02:00:00Z", "status": "no_work", "counts": {"no_work": 1}},
        ])
        data = P.build_rollup(spine_dir=self.spine)
        day1 = data["days"]["2026-03-01"]["alpha-sync"]
        self.assertEqual((day1["runs"], day1["did_work"], day1["could_not"]), (2, 1, 1))
        self.assertEqual(day1["items_did_work"], 3)
        self.assertEqual((day1["queue_first"], day1["queue_last"]), (10, 12))
        self.assertEqual(data["days"]["2026-03-02"]["alpha-sync"]["no_work"], 1)

    def test_rollup_keeps_days_that_are_no_longer_in_the_ledger(self) -> None:
        P.ROLLUP_PATH.write_text(json.dumps({"v": 1, "days": {
            "2025-12-31": {"alpha-sync": {"runs": 7, "did_work": 7, "no_work": 0, "could_not": 0,
                                          "items_did_work": 7, "queue_first": None, "queue_last": None}}}}))
        self.write("alpha-sync", [{"ts": "2026-03-01T01:00:00Z", "status": "did_work", "counts": {"did_work": 1}}])
        data = P.build_rollup(spine_dir=self.spine)
        self.assertEqual(data["days"]["2025-12-31"]["alpha-sync"]["runs"], 7)
        self.assertIn("2026-03-01", data["days"])

    def test_rollup_keeps_other_jobs_on_a_shared_day(self) -> None:
        P.ROLLUP_PATH.write_text(json.dumps({"v": 1, "days": {
            "2026-03-01": {"gone-job": {"runs": 4}, "alpha-sync": {"runs": 99}}}}))
        self.write("alpha-sync", [{"ts": "2026-03-01T01:00:00Z", "status": "did_work", "counts": {"did_work": 1}}])
        data = P.build_rollup(spine_dir=self.spine)
        self.assertEqual(data["days"]["2026-03-01"]["gone-job"], {"runs": 4})
        self.assertEqual(data["days"]["2026-03-01"]["alpha-sync"]["runs"], 1)

    def test_a_corrupt_old_rollup_is_ignored(self) -> None:
        P.ROLLUP_PATH.write_text("{nope")
        self.write("alpha-sync", [{"ts": "2026-03-01T01:00:00Z", "status": "did_work", "counts": {"did_work": 1}}])
        self.assertIn("2026-03-01", P.build_rollup(spine_dir=self.spine)["days"])

    def test_rollup_command_writes_the_file(self) -> None:
        self.write("alpha-sync", [{"ts": "2026-03-01T01:00:00Z", "status": "did_work", "counts": {"did_work": 1}}])
        code, out, _ = run_main(["rollup"])
        self.assertEqual(code, 0)
        self.assertIn("rolled up 1 day(s)", out)
        self.assertEqual(json.loads(P.ROLLUP_PATH.read_text())["v"], 1)


# ---------------------------------------------------------------------------
# One odd byte, line, row or value never stops the tool for the other jobs.
# ---------------------------------------------------------------------------


def jline(**over: Any) -> bytes:
    """One recent did_work row as a JSON line (bytes), with the given fields replaced."""
    rec: dict[str, Any] = {"v": 1, "ts": ts(0.5), "job": "odd", "status": "did_work",
                           "counts": {"did_work": 1, "no_work": 0, "could_not": 0}}
    rec.update(over)
    return json.dumps(rec).encode("utf-8")


def odd_ledgers() -> dict[str, bytes]:
    """One hand-written ledger per shape of damage, as raw bytes."""
    def build(*lines: bytes) -> bytes:
        return b"\n".join((jline(),) + lines) + b"\n"

    stamp = ts(0.5).encode("ascii")
    return {
        "a tail cut inside a multibyte character":
            jline() + b"\n" + b'{"v": 1, "ts": "' + stamp + b'", "status": "did_work", "reason": "caf\xc3',
        "a stray non-UTF-8 byte inside a complete line":
            build(b'{"ts": "' + stamp + b'", "status": "did_work", "reason": "caf\xc3"}'),
        "lines that are JSON but not objects": build(b"[1]", b"5", b'"text"', b"null", b"true"),
        "a line nested deeper than the parser allows": build(b"[" * 100000),
        "counts that are null": build(jline(counts=None)),
        "counts that are not an object": build(jline(counts="many"), jline(counts=[1, 2])),
        "counts that hold words": build(jline(counts={"did_work": "many", "no_work": None, "could_not": [1]})),
        "counts that hold booleans": build(jline(counts={"did_work": True})),
        "counts that hold infinity": build(b'{"ts": "' + stamp + b'", "status": "did_work", '
                                           b'"counts": {"did_work": Infinity}}'),
        "a queue that is a string": build(jline(queue="deep")),
        "a queue without depth": build(jline(queue={"unit": "items"})),
        "a queue whose depth is text": build(jline(queue={"depth": "5", "unit": "items"})),
        "a queue whose depth is a fraction": build(jline(queue={"depth": 2.5})),
        "evidence that is a string": build(jline(status="no_work", evidence="text")),
        "evidence that is a list": build(jline(status="no_work", evidence=["a"])),
        "a timestamp that is missing": build(b'{"status": "did_work", "counts": {"did_work": 1}}'),
        "a timestamp that is a number": build(jline(ts=5)),
        "a timestamp that is null": build(jline(ts=None)),
        "a timestamp that is text": build(jline(ts="yesterday around noon")),
        "a timestamp with no zone": build(jline(ts=ts(1).rstrip("Z"))),
        "a status that is not one of the three": build(jline(status="weird"), jline(status=["x"]), jline(status=None)),
        "nothing but damaged lines": b"not json\n[1]\n\xff\xfe\n",
    }


ODD_REGISTRY = {
    "odd": {"description": "d", "invariants": [
        {"kind": "seen_within_hours", "value": 24},
        {"kind": "work_within_hours", "value": 24},
        {"kind": "max_miss_rate", "value": 0.5, "quiet_ok": True},
        {"kind": "max_could_not_share", "value": 0.5},
        {"kind": "max_consecutive_not_work", "value": 2},
        {"kind": "queue_non_increasing"},
        {"kind": "min_items", "value": 1}]},
    "good": {"invariants": [{"kind": "seen_within_hours", "value": 24}]},
}


class TestOddLedgers(SpineTempDir):
    """Every command still completes, and the other job is still reported."""

    def setUp(self) -> None:
        super().setUp()
        self.registry(ODD_REGISTRY)
        self.write("good", [row(1, "did_work", job="good")])

    def put(self, data: bytes) -> None:
        (self.spine / "odd.jsonl").write_bytes(data)

    def test_the_functions_survive_every_shape(self) -> None:
        for name, data in odd_ledgers().items():
            with self.subTest(name):
                self.put(data)
                since = datetime.now(timezone.utc) - timedelta(hours=24)
                P.read_records("odd")
                P.read_records("odd", since=since)
                P.summarize("odd")
                P.build_rollup(spine_dir=self.spine)
                P.render_report()
                P.chip_summary()
                jobs = [c["job"] for c in P.check_all()]
                self.assertIn("good", jobs)
                self.assertIn("odd", jobs)

    def test_the_commands_survive_every_shape_and_still_report_the_other_job(self) -> None:
        for name, data in odd_ledgers().items():
            with self.subTest(name):
                self.put(data)
                for argv, allowed in (
                    (["report"], (0,)), (["report", "--json"], (0,)),
                    (["check"], (0, 1, 2)), (["check", "--json"], (0, 1, 2)),
                    (["rollup"], (0,)), (["jobs"], (0,)), (["verify"], (0, 1)),
                    (["verify", "--json"], (0, 1)),
                ):
                    code, out, _ = run_main(argv)
                    self.assertIn(code, allowed, argv)
                    if argv == ["report"] or argv == ["check", "--json"]:
                        self.assertIn("good", out, argv)

    def test_a_tail_cut_inside_a_character_reads_instead_of_raising(self) -> None:
        data = odd_ledgers()["a stray non-UTF-8 byte inside a complete line"]
        self.put(data)
        recs = P.read_records("odd")
        self.assertEqual(len(recs), 2)
        self.assertFalse(any(r.get("_malformed") for r in recs))
        self.assertEqual(recs[1]["status"], "did_work")
        self.assertIn("caf", recs[1]["reason"])

    def test_a_cut_tail_is_one_malformed_line(self) -> None:
        self.put(odd_ledgers()["a tail cut inside a multibyte character"])
        s = P.summarize("odd")
        self.assertEqual((s["total_records"], s["malformed_lines"], s["ledger_lines"]), (1, 1, 2))

    def test_lines_that_are_not_objects_count_as_malformed_and_are_skipped(self) -> None:
        self.put(odd_ledgers()["lines that are JSON but not objects"])
        since = datetime.now(timezone.utc) - timedelta(hours=24)
        for window in (None, since):
            recs = P.read_records("odd", since=window)
            self.assertEqual(sum(1 for r in recs if r.get("_malformed")), 5)
            self.assertEqual(sum(1 for r in recs if not r.get("_malformed")), 1)
        s = P.summarize("odd")
        self.assertEqual((s["runs"], s["malformed_lines"], s["total_records"]), (1, 5, 1))

    def test_a_line_nested_too_deep_is_malformed(self) -> None:
        self.put(odd_ledgers()["a line nested deeper than the parser allows"])
        s = P.summarize("odd")
        self.assertEqual((s["malformed_lines"], s["total_records"]), (1, 1))

    def test_verify_reports_a_ledger_it_cannot_parse_and_still_checks_the_others(self) -> None:
        self.put(odd_ledgers()["a line nested deeper than the parser allows"])
        code, out, err = run_main(["verify"])
        self.assertEqual(code, 1)
        self.assertIn("odd.jsonl: 0 rows, 0 unhashed, BAD: could not be read", out)
        self.assertIn("good.jsonl: 1 rows, 1 unhashed, ok", out)
        self.assertEqual(err, "")
        code, out, _ = run_main(["verify", "--json"])
        self.assertEqual(code, 1)
        flags = {r["file"]: r["ok"] for r in json.loads(out)}
        self.assertEqual(flags, {"good.jsonl": True, "odd.jsonl": False})

    def test_odd_counts_count_as_zero_for_that_value(self) -> None:
        cases = {
            "counts that are null": (0, 0),
            "counts that are not an object": (0, 0),
            "counts that hold words": (0, 0),
            "counts that hold booleans": (0, 0),
            "counts that hold infinity": (0, 0),
        }
        for name, (did, _) in cases.items():
            with self.subTest(name):
                self.put(odd_ledgers()[name])
                s = P.summarize("odd")
                # The first line of every ledger is a good row: counts did_work 1.
                self.assertEqual(s["items"], {"did_work": 1 + did, "no_work": 0, "could_not": 0})
                self.assertGreaterEqual(s["runs"], 2)

    def test_a_good_count_beside_a_bad_one_is_kept(self) -> None:
        self.put(jline(counts={"did_work": 3, "no_work": "x"}) + b"\n")
        s = P.summarize("odd")
        self.assertEqual(s["items"], {"did_work": 3, "no_work": 0, "could_not": 0})

    def test_a_queue_without_usable_data_is_ignored_and_does_not_hide_the_good_ones(self) -> None:
        for name in ("a queue that is a string", "a queue without depth",
                     "a queue whose depth is text", "a queue whose depth is a fraction"):
            with self.subTest(name):
                self.put(jline(queue={"depth": 5, "unit": "items"}) + b"\n"
                         + odd_ledgers()[name].split(b"\n", 1)[1]
                         + jline(queue={"depth": 8, "unit": "items"}) + b"\n")
                s = P.summarize("odd")
                self.assertEqual((s["queue_first"], s["queue_last"], s["queue_delta"]), (5, 8, 3))

    def test_a_queue_that_is_only_odd_gives_no_delta(self) -> None:
        self.put(odd_ledgers()["a queue whose depth is text"])
        s = P.summarize("odd")
        self.assertEqual((s["queue_first"], s["queue_last"], s["queue_delta"]), (None, None, None))
        # The rollup treats the same row the same way.
        day = ts(0.5)[:10]
        bucket = P.build_rollup(spine_dir=self.spine)["days"][day]["odd"]
        self.assertEqual((bucket["queue_first"], bucket["queue_last"]), (None, None))

    def test_evidence_that_is_not_an_object_reads_as_empty(self) -> None:
        for name in ("evidence that is a string", "evidence that is a list"):
            with self.subTest(name):
                self.put(odd_ledgers()[name])
                s = P.summarize("odd")
                self.assertEqual((s["runs_by_status"]["no_work"], s["unclassified_quiet"]), (1, 0))

    def test_odd_counts_do_not_change_the_rollup(self) -> None:
        self.put(odd_ledgers()["counts that hold words"])
        bucket = P.build_rollup(spine_dir=self.spine)["days"][ts(0.5)[:10]]["odd"]
        self.assertEqual((bucket["runs"], bucket["items_did_work"]), (2, 1))

    def test_a_status_outside_the_three_keeps_todays_behaviour(self) -> None:
        self.put(odd_ledgers()["a status that is not one of the three"])
        s = P.summarize("odd")
        self.assertEqual(s["runs"], 4)
        self.assertEqual(s["runs_by_status"], {"did_work": 1, "no_work": 0, "could_not": 0})
        bucket = P.build_rollup(spine_dir=self.spine)["days"][ts(0.5)[:10]]["odd"]
        self.assertEqual((bucket["runs"], bucket["did_work"]), (4, 1))

    def test_a_row_with_no_readable_timestamp_is_left_out_of_a_windowed_read_only(self) -> None:
        for name in ("a timestamp that is missing", "a timestamp that is a number",
                     "a timestamp that is null", "a timestamp that is text"):
            with self.subTest(name):
                self.put(odd_ledgers()[name])
                self.assertEqual(len(P.read_records("odd")), 2)
                since = datetime.now(timezone.utc) - timedelta(hours=24)
                self.assertEqual(len(P.read_records("odd", since=since)), 1)
                s = P.summarize("odd")
                self.assertEqual((s["runs"], s["total_records"], s["ledger_lines"]), (1, 2, 2))

    def test_a_timestamp_with_no_zone_is_read_as_utc(self) -> None:
        # Pin the machine's zone away from UTC, so reading it as local time shows.
        old = os.environ.get("TZ")

        def restore() -> None:
            if old is None:
                os.environ.pop("TZ", None)
            else:
                os.environ["TZ"] = old
            time.tzset()

        self.addCleanup(restore)
        os.environ["TZ"] = "PST8"
        time.tzset()
        naive = (datetime.now(timezone.utc) - timedelta(hours=2)).replace(tzinfo=None).isoformat(timespec="seconds")
        parsed = P._parse_ts(naive)
        assert parsed is not None
        self.assertEqual(parsed.utcoffset(), timedelta(0))
        age = P._hours_since(naive)
        assert age is not None
        self.assertAlmostEqual(age, 2.0, delta=0.1)
        self.put(jline(ts=naive) + b"\n")
        s = P.summarize("odd", window_hours=24)
        self.assertEqual(s["runs"], 1)
        self.assertAlmostEqual(P._hours_since(s["last_seen"]) or 0.0, 2.0, delta=0.1)

    def test_unreadable_timestamps_give_none(self) -> None:
        for value in (None, 5, 1.5, ["2026-01-01"], {}, "", "garbage", "2026-13-45T00:00:00Z",
                      "0001-01-01T00:00:00+05:00"):
            with self.subTest(repr(value)):
                self.assertIsNone(P._parse_ts(value))
        self.assertIsNotNone(P._parse_ts("2026-01-01T00:00:00Z"))

    def test_a_window_wider_than_the_calendar_reads_every_row(self) -> None:
        self.put(jline() + b"\n")
        self.assertEqual(P.summarize("odd", window_hours=1e30)["runs"], 1)


class TestOddLedgersInRealProcesses(SpineTempDir):
    """The same damage through the real commands: no traceback, the other job reported."""

    def cli(self, *args: str) -> subprocess.CompletedProcess:
        registry = self.dir / "odd-jobs.json"
        registry.write_text(json.dumps(ODD_REGISTRY), encoding="utf-8")
        return subprocess.run([sys.executable, str(HERE / "pow.py"), *args], capture_output=True,
                              text=True, timeout=60, check=False,
                              env=child_env(self.spine, POW_REGISTRY=str(registry)))

    def test_commands_survive_the_damage(self) -> None:
        self.write("good", [row(1, "did_work", job="good")])
        shapes = ("a tail cut inside a multibyte character",
                  "a stray non-UTF-8 byte inside a complete line",
                  "lines that are JSON but not objects",
                  "counts that hold words", "counts that are null",
                  "nothing but damaged lines")
        for name in shapes:
            (self.spine / "odd.jsonl").write_bytes(odd_ledgers()[name])
            for argv in (["report"], ["report", "--json"], ["check"], ["check", "--json"],
                         ["rollup"], ["jobs"], ["verify"]):
                with self.subTest(name=name, cmd=" ".join(argv)):
                    proc = self.cli(*argv)
                    self.assertNotIn("Traceback", proc.stderr)
                    self.assertIn(proc.returncode, (0, 1))
                    if argv in (["report"], ["check", "--json"]):
                        self.assertIn("good", proc.stdout)

    def test_the_rollup_is_written_beside_the_temporary_spine_and_not_into_the_repo(self) -> None:
        self.write("good", [row(1, "did_work", job="good")])
        before = (HERE / "spine-rollup.json").exists()
        proc = self.cli("rollup")
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertTrue((self.dir / "child-rollup.json").is_file())
        self.assertEqual((HERE / "spine-rollup.json").exists(), before)
        self.assertEqual(child_env(self.spine)["POW_ROLLUP"], str(self.dir / "child-rollup.json"))


class TestAwaitingMeansNoLineAtAll(SpineTempDir):
    """A ledger of only damaged lines has reported; only an empty or missing one is awaiting."""

    def setUp(self) -> None:
        super().setUp()
        self.registry({"odd": {"invariants": [{"kind": "seen_within_hours", "value": 24}]}})
        (self.spine / "odd.jsonl").write_bytes(b"not json\n[1]\n")

    def test_the_summary_counts_every_non_blank_line(self) -> None:
        s = P.summarize("odd")
        self.assertEqual((s["total_records"], s["ledger_lines"]), (0, 2))

    def test_check_all_does_not_call_it_awaiting_and_its_invariant_fails(self) -> None:
        (c,) = P.check_all()
        self.assertIs(c["awaiting"], False)
        self.assertEqual([v["kind"] for v in c["violated"]], ["seen_within_hours"])

    def test_check_fails_on_it(self) -> None:
        code, out, _ = run_main(["check"])
        self.assertEqual(code, 1)
        self.assertIn("VIOLATED odd: seen_within_hours", out)
        self.assertNotIn("AWAITING odd", out)

    def test_the_report_judges_it_like_any_other_job(self) -> None:
        out = P.render_report()
        self.assertIn("INVARIANTS VIOLATED", out)
        self.assertNotIn("awaiting first report", out)

    def test_the_chip_summary_does_not_bucket_it_as_awaiting(self) -> None:
        chip = P.chip_summary()
        self.assertEqual(chip["awaiting"], [])
        self.assertEqual(chip["level"], "err")

    def test_an_empty_file_and_a_missing_file_are_still_awaiting(self) -> None:
        (self.spine / "odd.jsonl").write_bytes(b"")
        (c,) = P.check_all()
        self.assertIs(c["awaiting"], True)
        self.assertEqual(P.chip_summary()["awaiting"], ["odd"])
        code, out, _ = run_main(["check"])
        self.assertEqual(code, 0)
        self.assertIn("AWAITING odd", out)
        self.assertNotIn("awaiting first report", "")
        self.assertIn("awaiting first report", P.render_report())


class TestCheckSaysWhatItCouldNotJudge(SpineTempDir):
    def test_an_unknown_verdict_prints_an_unknown_line_and_the_exit_code_stays_0(self) -> None:
        self.registry({"alpha-sync": {"invariants": [{"kind": "queue_non_increasing"}]}})
        self.write("alpha-sync", [row(1, "did_work")])
        code, out, _ = run_main(["check"])
        self.assertEqual(code, 0)
        self.assertIn("UNKNOWN alpha-sync: queue_non_increasing - no queue depth reported in window", out)

    def test_an_awaiting_job_gets_no_unknown_line(self) -> None:
        self.registry({"beta-scan": {"invariants": [{"kind": "queue_non_increasing"}]}})
        code, out, _ = run_main(["check"])
        self.assertEqual(code, 0)
        self.assertIn("AWAITING beta-scan", out)
        self.assertNotIn("UNKNOWN", out)

    def test_the_unknown_lines_come_after_the_violated_lines(self) -> None:
        self.registry({
            "alpha-sync": {"invariants": [{"kind": "queue_non_increasing"}]},
            "beta-scan": {"invariants": [{"kind": "seen_within_hours", "value": 1}]},
        })
        self.write("alpha-sync", [row(1, "did_work")])
        self.write("beta-scan", [row(50, "did_work", job="beta-scan")])
        code, out, _ = run_main(["check"])
        self.assertEqual(code, 1)
        self.assertLess(out.index("VIOLATED beta-scan"), out.index("UNKNOWN alpha-sync"))

    def test_json_results_carry_the_awaiting_flag_in_both_states(self) -> None:
        self.registry({
            "alpha-sync": {"invariants": [{"kind": "queue_non_increasing"}]},
            "beta-scan": {"invariants": [{"kind": "queue_non_increasing"}]},
        })
        self.write("alpha-sync", [row(1, "did_work")])
        code, out, _ = run_main(["check", "--json"])
        self.assertEqual(code, 0)
        flags = {c["job"]: c["awaiting"] for c in json.loads(out)}
        self.assertEqual(flags, {"alpha-sync": False, "beta-scan": True})

    def test_with_no_invariant_registered_it_says_nothing_was_checked(self) -> None:
        self.write("alpha-sync", [row(1, "did_work")])
        code, out, _ = run_main(["check"])
        self.assertEqual(code, 0)
        self.assertIn("pow: no invariants are registered; nothing was checked", out)
        self.assertNotIn("no invariant violations", out)

    def test_with_invariants_evaluated_and_none_violated_it_says_so(self) -> None:
        self.registry({"alpha-sync": {"invariants": [{"kind": "seen_within_hours", "value": 24}]}})
        self.write("alpha-sync", [row(1, "did_work")])
        code, out, _ = run_main(["check"])
        self.assertEqual(code, 0)
        self.assertIn("pow: no invariant violations", out)
        self.assertNotIn("nothing was checked", out)


class TestQuietOkNeedsAQuietRun(SpineTempDir):
    def test_quiet_ok_with_no_run_in_the_window_is_unknown(self) -> None:
        # The only row is far older than the window: the job did not run.
        self.write("alpha-sync", [row(500, "no_work")])
        inv = {"kind": "max_miss_rate", "value": 0.1, "quiet_ok": True}
        summary = P.summarize("alpha-sync", window_hours=24, spine_dir=self.spine)
        self.assertEqual(summary["runs"], 0)
        v = P.eval_invariant(inv, summary)
        self.assertIsNone(v["ok"])
        self.assertEqual(v["detail"], "no runs in window")

    def test_quiet_ok_with_a_quiet_run_in_the_window_is_ok(self) -> None:
        self.write("alpha-sync", [row(500, "no_work"), row(2, "no_work", evidence={"classification": "none_pattern"})])
        inv = {"kind": "max_miss_rate", "value": 0.1, "quiet_ok": True}
        v = P.eval_invariant(inv, P.summarize("alpha-sync", window_hours=24, spine_dir=self.spine))
        self.assertIs(v["ok"], True)


class TestRollupIsWrittenSafely(SpineTempDir):
    def setUp(self) -> None:
        super().setUp()
        self.write("alpha-sync", [{"ts": "2026-03-01T01:00:00Z", "status": "did_work", "counts": {"did_work": 1}}])
        old = os.umask(0o022)
        self.addCleanup(os.umask, old)

    def leftovers(self) -> list[str]:
        return sorted(p.name for p in self.dir.iterdir() if p.name not in ("spine", "rollup.json"))

    def test_garbage_in_the_rollup_file_is_refused_and_left_alone(self) -> None:
        for garbage in (b"{nope", b"[1]", b'{"days": []}', b'{"days": {"2026-01-01": 5}}', b"\xff\xfe"):
            with self.subTest(garbage):
                P.ROLLUP_PATH.write_bytes(garbage)
                code, out, err = run_main(["rollup"])
                self.assertEqual(code, 2)
                self.assertEqual(out, "")
                self.assertEqual(len(err.strip().splitlines()), 1)
                self.assertTrue(err.startswith("pow: ERROR"))
                self.assertIn(str(P.ROLLUP_PATH), err)
                self.assertEqual(P.ROLLUP_PATH.read_bytes(), garbage)
                self.assertEqual(self.leftovers(), [])

    def test_a_failed_replace_leaves_the_old_file_and_removes_the_temporary_file(self) -> None:
        old = json.dumps({"v": 1, "days": {"2025-12-31": {"alpha-sync": {"runs": 7}}}}).encode()
        P.ROLLUP_PATH.write_bytes(old)
        with mock.patch.object(P.os, "replace", side_effect=OSError("disk said no")):
            code, out, err = run_main(["rollup"])
        self.assertEqual(code, 2)
        self.assertTrue(err.startswith("pow: ERROR"))
        self.assertEqual(len(err.strip().splitlines()), 1)
        self.assertIn("disk said no", err)
        self.assertEqual(P.ROLLUP_PATH.read_bytes(), old)
        self.assertEqual(self.leftovers(), [])

    def test_a_failed_fsync_leaves_the_old_file_and_removes_the_temporary_file(self) -> None:
        old = b'{"v": 1, "days": {}}\n'
        P.ROLLUP_PATH.write_bytes(old)
        with mock.patch.object(P.os, "fsync", side_effect=OSError("no sync")):
            code, _, err = run_main(["rollup"])
        self.assertEqual(code, 2)
        self.assertIn("no sync", err)
        self.assertEqual(P.ROLLUP_PATH.read_bytes(), old)
        self.assertEqual(self.leftovers(), [])

    def test_the_file_is_put_in_place_by_a_replace_and_not_written_in_place(self) -> None:
        P.ROLLUP_PATH.write_text('{"v": 1, "days": {}}\n')
        calls = []
        real = os.replace

        def spy(src: Any, dst: Any) -> None:
            calls.append((os.path.dirname(src), str(dst)))
            real(src, dst)

        with mock.patch.object(P.os, "replace", side_effect=spy):
            code, _, _ = run_main(["rollup"])
        self.assertEqual(code, 0)
        self.assertEqual(calls, [(str(P.ROLLUP_PATH.parent), str(P.ROLLUP_PATH))])
        self.assertIn("2026-03-01", json.loads(P.ROLLUP_PATH.read_text())["days"])
        self.assertEqual(self.leftovers(), [])

    def test_an_existing_rollup_keeps_its_permission_bits(self) -> None:
        P.ROLLUP_PATH.write_text('{"v": 1, "days": {}}\n')
        os.chmod(P.ROLLUP_PATH, 0o640)
        code, _, _ = run_main(["rollup"])
        self.assertEqual(code, 0)
        self.assertEqual(P.ROLLUP_PATH.stat().st_mode & 0o777, 0o640)

    def test_a_new_rollup_gets_the_default_mode_and_not_0600(self) -> None:
        self.assertFalse(P.ROLLUP_PATH.exists())
        code, _, _ = run_main(["rollup"])
        self.assertEqual(code, 0)
        self.assertEqual(P.ROLLUP_PATH.stat().st_mode & 0o777, 0o644)

    def test_a_missing_folder_is_an_error_line_and_exit_2(self) -> None:
        P.ROLLUP_PATH = self.dir / "no-such-folder" / "rollup.json"
        code, _, err = run_main(["rollup"])
        self.assertEqual(code, 2)
        self.assertTrue(err.startswith("pow: ERROR"))


if __name__ == "__main__":
    unittest.main(verbosity=2)
