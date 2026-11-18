"""The hash chain: stamping, verifying, tamper cases, concurrent writers and the verify command."""

from __future__ import annotations

import copy
import json
import shutil
import subprocess
import sys
import unittest
from pathlib import Path

from _common import HERE, SpineTempDir, child_env, run_main

import chain  # noqa: E402
import pow as P  # noqa: E402


class ChainCase(SpineTempDir):
    def make_file(self, n: int = 5, job: str = "alpha-sync") -> Path:
        for i in range(n):
            P.record(job, "did_work", did_work=i + 1, reason=f"run {i + 1}", spine_dir=self.spine)
        return self.spine / f"{job}.jsonl"

    def rewrite(self, path: Path, lines: list[str]) -> None:
        path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    def raw_lines(self, path: Path) -> list[str]:
        return path.read_text(encoding="utf-8").splitlines()


class TestStamping(ChainCase):
    def test_the_first_row_of_a_new_file_links_to_genesis(self) -> None:
        stamped = chain.chain_fields(str(self.spine / "new.jsonl"), {"a": 1})
        self.assertEqual(stamped["prev_row_hash"], "genesis")
        self.assertTrue(stamped["row_hash"].startswith("sha256:"))

    def test_an_empty_or_blank_file_is_also_genesis(self) -> None:
        path = self.spine / "blank.jsonl"
        path.write_text("", encoding="utf-8")
        self.assertEqual(chain.chain_fields(str(path), {"a": 1})["prev_row_hash"], "genesis")
        path.write_text("\n\n  \n", encoding="utf-8")
        self.assertEqual(chain.chain_fields(str(path), {"a": 1})["prev_row_hash"], "genesis")

    def test_each_row_links_to_the_one_before(self) -> None:
        path = self.make_file(4)
        rows = [json.loads(x) for x in self.raw_lines(path)]
        self.assertEqual(rows[0]["prev_row_hash"], "genesis")
        for before, after in zip(rows, rows[1:]):
            self.assertEqual(after["prev_row_hash"], before["row_hash"])
        self.assertEqual(len({r["row_hash"] for r in rows}), 4)

    def test_a_row_after_a_line_with_no_hash_links_to_legacy(self) -> None:
        path = self.spine / "old.jsonl"
        path.write_text(json.dumps({"ts": "2026-01-01T00:00:00Z", "status": "did_work"}) + "\n", encoding="utf-8")
        stamped = chain.chain_fields(str(path), {"a": 1})
        self.assertEqual(stamped["prev_row_hash"], "legacy")

    def test_a_row_after_a_line_that_is_not_json_links_to_legacy(self) -> None:
        path = self.spine / "old.jsonl"
        path.write_text("{ torn", encoding="utf-8")
        self.assertEqual(chain.chain_fields(str(path), {"a": 1})["prev_row_hash"], "legacy")

    def test_chain_fields_does_not_change_its_input(self) -> None:
        original = {"b": [1, 2, {"c": 3}], "a": "x"}
        snapshot = copy.deepcopy(original)
        out = chain.chain_fields(str(self.spine / "new.jsonl"), original)
        self.assertEqual(original, snapshot)
        self.assertNotIn("row_hash", original)
        self.assertIsNot(out, original)
        out["b"].append(99)
        self.assertEqual(original, snapshot)

    def test_the_hash_does_not_depend_on_key_order(self) -> None:
        a = {"x": 1, "y": 2, "prev_row_hash": "genesis"}
        b = {"y": 2, "prev_row_hash": "genesis", "x": 1}
        self.assertEqual(chain.canonical_hash(a), chain.canonical_hash(b))

    def test_the_hash_depends_on_the_content(self) -> None:
        a = {"x": 1, "prev_row_hash": "genesis"}
        b = {"x": 2, "prev_row_hash": "genesis"}
        self.assertNotEqual(chain.canonical_hash(a), chain.canonical_hash(b))

    def test_the_hash_depends_on_the_previous_hash(self) -> None:
        a = {"x": 1, "prev_row_hash": "genesis"}
        b = {"x": 1, "prev_row_hash": "sha256:" + "0" * 64}
        self.assertNotEqual(chain.canonical_hash(a), chain.canonical_hash(b))

    def test_the_hash_ignores_the_stored_hash_fields(self) -> None:
        a = {"x": 1, "prev_row_hash": "genesis"}
        b = dict(a, row_hash="sha256:whatever")
        self.assertEqual(chain.canonical_hash(a), chain.canonical_hash(b))

    def test_non_ascii_content_survives_the_round_trip(self) -> None:
        P.record("alpha-sync", "could_not", could_not=1, reason="caf\u00e9 \u2713 failed", spine_dir=self.spine)
        P.record("alpha-sync", "did_work", did_work=1, spine_dir=self.spine)
        self.assertTrue(chain.verify_file(str(self.spine / "alpha-sync.jsonl"))["ok"])

    def test_a_torn_tail_gets_a_newline_and_the_chain_restarts_as_legacy(self) -> None:
        path = self.make_file(2)
        with open(path, "ab") as fh:
            fh.write(b'{"ts": "2026-01-01T00:00:00Z", "sta')
        P.record("alpha-sync", "did_work", did_work=1, spine_dir=self.spine)
        lines = self.raw_lines(path)
        self.assertEqual(len(lines), 4)
        self.assertEqual(json.loads(lines[3])["prev_row_hash"], "legacy")
        res = chain.verify_file(str(path))
        self.assertTrue(res["ok"], res)
        self.assertEqual(res["unhashed"], 1)


class TestVerifyFile(ChainCase):
    def test_an_untouched_file_verifies(self) -> None:
        path = self.make_file(5)
        res = chain.verify_file(str(path))
        self.assertEqual(res, {"rows": 5, "unhashed": 0, "ok": True, "first_bad_line": None, "problem": None})

    def test_an_edited_row_is_a_hash_mismatch_on_its_own_line(self) -> None:
        path = self.make_file(5)
        lines = self.raw_lines(path)
        self.assertIn('"run 3"', lines[2])
        lines[2] = lines[2].replace('"run 3"', '"run X"')
        self.rewrite(path, lines)
        res = chain.verify_file(str(path))
        self.assertFalse(res["ok"])
        self.assertEqual(res["first_bad_line"], 3)
        self.assertEqual(res["problem"], "hash mismatch")

    def test_a_one_character_edit_is_caught(self) -> None:
        path = self.make_file(3)
        lines = self.raw_lines(path)
        lines[1] = lines[1].replace('"did_work": 2', '"did_work": 3', 1)
        self.rewrite(path, lines)
        res = chain.verify_file(str(path))
        self.assertEqual((res["first_bad_line"], res["problem"]), (2, "hash mismatch"))

    def test_a_removed_row_is_a_link_mismatch_on_the_next_line(self) -> None:
        path = self.make_file(5)
        lines = self.raw_lines(path)
        del lines[2]
        self.rewrite(path, lines)
        res = chain.verify_file(str(path))
        self.assertFalse(res["ok"])
        self.assertEqual(res["first_bad_line"], 3)
        self.assertEqual(res["problem"], "link mismatch")

    def test_the_first_row_removed_is_caught_at_line_one(self) -> None:
        path = self.make_file(3)
        lines = self.raw_lines(path)
        del lines[0]
        self.rewrite(path, lines)
        res = chain.verify_file(str(path))
        self.assertEqual((res["first_bad_line"], res["problem"]), (1, "link mismatch"))

    def test_two_swapped_rows_are_a_link_mismatch_at_the_first_of_them(self) -> None:
        path = self.make_file(5)
        lines = self.raw_lines(path)
        lines[1], lines[2] = lines[2], lines[1]
        self.rewrite(path, lines)
        res = chain.verify_file(str(path))
        self.assertFalse(res["ok"])
        self.assertEqual(res["first_bad_line"], 2)
        self.assertEqual(res["problem"], "link mismatch")

    def test_a_row_with_its_hash_removed_is_caught_on_that_row(self) -> None:
        path = self.make_file(5)
        lines = self.raw_lines(path)
        rec = json.loads(lines[2])
        del rec["row_hash"]
        lines[2] = json.dumps(rec)
        self.rewrite(path, lines)
        res = chain.verify_file(str(path))
        self.assertFalse(res["ok"])
        self.assertEqual(res["first_bad_line"], 3)
        self.assertEqual(res["problem"], "hash mismatch")
        self.assertEqual(res["unhashed"], 1)

    def test_a_row_with_both_chain_fields_removed_breaks_the_next_link(self) -> None:
        path = self.make_file(5)
        lines = self.raw_lines(path)
        rec = json.loads(lines[2])
        del rec["row_hash"]
        del rec["prev_row_hash"]
        lines[2] = json.dumps(rec)
        self.rewrite(path, lines)
        res = chain.verify_file(str(path))
        self.assertEqual((res["first_bad_line"], res["problem"]), (4, "link mismatch"))
        self.assertEqual(res["unhashed"], 1)

    def test_a_forged_link_value_is_caught(self) -> None:
        path = self.make_file(3)
        lines = self.raw_lines(path)
        rec = json.loads(lines[1])
        rec["prev_row_hash"] = "sha256:" + "f" * 64
        lines[1] = json.dumps(rec)
        self.rewrite(path, lines)
        res = chain.verify_file(str(path))
        self.assertEqual(res["first_bad_line"], 2)

    def test_the_first_bad_line_is_reported_when_there_are_several(self) -> None:
        path = self.make_file(6)
        lines = self.raw_lines(path)
        lines[4] = lines[4].replace('"run 5"', '"run Y"')
        lines[1] = lines[1].replace('"run 2"', '"run X"')
        self.rewrite(path, lines)
        self.assertEqual(chain.verify_file(str(path))["first_bad_line"], 2)

    def test_blank_lines_do_not_count_as_rows_but_keep_physical_line_numbers(self) -> None:
        path = self.make_file(3)
        lines = self.raw_lines(path)
        lines.insert(1, "")
        lines[2] = lines[2].replace('"run 2"', '"run X"')
        self.rewrite(path, lines)
        res = chain.verify_file(str(path))
        self.assertEqual(res["rows"], 3)
        self.assertEqual(res["first_bad_line"], 3)

    def test_old_rows_without_a_hash_are_counted_not_failed(self) -> None:
        path = self.spine / "mixed.jsonl"
        old = [json.dumps({"ts": "2026-01-01T00:00:00Z", "n": i}) for i in range(2)]
        path.write_text("\n".join(old) + "\n", encoding="utf-8")
        P.record("mixed", "did_work", did_work=1, spine_dir=self.spine)
        P.record("mixed", "did_work", did_work=1, spine_dir=self.spine)
        res = chain.verify_file(str(path))
        self.assertTrue(res["ok"], res)
        self.assertEqual((res["rows"], res["unhashed"]), (4, 2))

    def test_a_line_that_is_not_json_is_unhashed_and_the_next_row_must_say_legacy(self) -> None:
        path = self.make_file(2)
        lines = self.raw_lines(path)
        lines.insert(1, "garbage")
        self.rewrite(path, lines)
        res = chain.verify_file(str(path))
        self.assertEqual(res["unhashed"], 1)
        self.assertEqual((res["first_bad_line"], res["problem"]), (3, "link mismatch"))

    def test_a_last_row_replaced_by_a_line_with_no_hash_leaves_no_bad_line(self) -> None:
        path = self.make_file(3)
        lines = self.raw_lines(path)
        rec = json.loads(lines[2])
        del rec["row_hash"]
        del rec["prev_row_hash"]
        rec["counts"] = {"did_work": 999, "no_work": 0, "could_not": 0}
        lines[2] = json.dumps(rec)
        self.rewrite(path, lines)
        res = chain.verify_file(str(path))
        self.assertTrue(res["ok"], res)
        self.assertIsNone(res["first_bad_line"])
        self.assertEqual((res["rows"], res["unhashed"]), (3, 1))

    def test_rows_cut_from_the_end_are_not_detectable(self) -> None:
        path = self.make_file(5)
        lines = self.raw_lines(path)
        self.rewrite(path, lines[:3])
        self.assertTrue(chain.verify_file(str(path))["ok"])

    def test_an_empty_file_verifies_with_zero_rows(self) -> None:
        path = self.spine / "empty.jsonl"
        path.write_text("", encoding="utf-8")
        self.assertEqual(chain.verify_file(str(path))["rows"], 0)
        self.assertTrue(chain.verify_file(str(path))["ok"])


WRITER = (
    "import sys\n"
    "sys.path.insert(0, sys.argv[1])\n"
    "import pow\n"
    "for i in range(int(sys.argv[3])):\n"
    "    pow.record(sys.argv[2], 'did_work', did_work=1, reason='w' + str(i))\n"
)


class TestRowsLongerThanOneReadBlock(ChainCase):
    """The tail reader walks back in blocks; a row bigger than a block must still be read whole."""

    def long_evidence(self) -> dict:
        return {"blob": "x" * (chain._READ_BLOCK * 2 + 905)}

    def test_a_long_first_row_is_linked_to_by_the_rows_after_it(self) -> None:
        P.record("alpha-sync", "did_work", did_work=1, evidence=self.long_evidence(), spine_dir=self.spine)
        path = self.spine / "alpha-sync.jsonl"
        self.assertGreater(len(path.read_bytes()), chain._READ_BLOCK * 2)
        P.record("alpha-sync", "did_work", did_work=2, spine_dir=self.spine)
        P.record("alpha-sync", "did_work", did_work=3, spine_dir=self.spine)
        res = chain.verify_file(str(path))
        self.assertEqual(res, {"rows": 3, "unhashed": 0, "ok": True, "first_bad_line": None, "problem": None})
        rows = [json.loads(line) for line in self.raw_lines(path)]
        self.assertEqual(rows[1]["prev_row_hash"], rows[0]["row_hash"])
        self.assertEqual(rows[2]["prev_row_hash"], rows[1]["row_hash"])

    def test_a_long_newest_row_followed_by_a_blank_line_is_still_found(self) -> None:
        P.record("alpha-sync", "did_work", did_work=1, spine_dir=self.spine)
        P.record("alpha-sync", "did_work", did_work=2, evidence=self.long_evidence(), spine_dir=self.spine)
        path = self.spine / "alpha-sync.jsonl"
        with open(path, "ab") as fh:
            fh.write(b"\n\n")
        P.record("alpha-sync", "did_work", did_work=3, spine_dir=self.spine)
        res = chain.verify_file(str(path))
        self.assertEqual((res["rows"], res["unhashed"], res["ok"]), (3, 0, True), res)
        rows = [json.loads(line) for line in self.raw_lines(path) if line.strip()]
        self.assertEqual(rows[2]["prev_row_hash"], rows[1]["row_hash"])


class TestConcurrentWriters(ChainCase):
    def test_four_processes_writing_25_rows_each_leave_100_rows_and_a_good_chain(self) -> None:
        procs = []

        def reap() -> None:
            for p in procs:
                if p.poll() is None:
                    p.kill()
                p.wait()

        self.addCleanup(reap)
        env = child_env(self.spine)
        for _ in range(4):
            procs.append(subprocess.Popen(
                [sys.executable, "-c", WRITER, str(HERE), "shared-job", "25"],
                env=env, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE))
        for p in procs:
            _, err = p.communicate(timeout=120)
            self.assertEqual(p.returncode, 0, err.decode()[-500:])
        res = chain.verify_file(str(self.spine / "shared-job.jsonl"))
        self.assertEqual(res["rows"], 100)
        self.assertEqual(res["unhashed"], 0)
        self.assertTrue(res["ok"], res)
        reasons = [json.loads(x)["reason"] for x in self.raw_lines(self.spine / "shared-job.jsonl")]
        self.assertEqual(len(set(reasons)), 25)
        self.assertEqual(len(reasons), 100)


class TestVerifyCommand(ChainCase):
    def test_a_clean_spine_exits_0_and_prints_one_line_per_file(self) -> None:
        self.make_file(3, "alpha-sync")
        self.make_file(2, "beta-export")
        code, out, _ = run_main(["verify"])
        self.assertEqual(code, 0)
        self.assertIn("alpha-sync.jsonl: 3 rows, 0 unhashed, ok", out)
        self.assertIn("beta-export.jsonl: 2 rows, 0 unhashed, ok", out)

    def test_a_tampered_file_exits_1_and_names_the_line_and_reason(self) -> None:
        path = self.make_file(4)
        lines = self.raw_lines(path)
        lines[2] = lines[2].replace('"run 3"', '"run X"')
        self.rewrite(path, lines)
        code, out, _ = run_main(["verify"])
        self.assertEqual(code, 1)
        self.assertIn("BAD at line 3: hash mismatch", out)

    def test_one_bad_file_makes_the_exit_1_even_when_others_are_fine(self) -> None:
        self.make_file(3, "alpha-sync")
        bad = self.make_file(3, "beta-export")
        lines = self.raw_lines(bad)
        del lines[1]
        self.rewrite(bad, lines)
        code, out, _ = run_main(["verify"])
        self.assertEqual(code, 1)
        self.assertIn("alpha-sync.jsonl: 3 rows, 0 unhashed, ok", out)
        self.assertIn("beta-export.jsonl: 2 rows, 0 unhashed, BAD at line 2: link mismatch", out)

    def test_an_empty_spine_says_so_and_exits_0(self) -> None:
        code, out, _ = run_main(["verify"])
        self.assertEqual(code, 0)
        self.assertIn("nothing to verify", out)

    def test_a_spine_folder_that_does_not_exist_is_also_nothing_to_verify(self) -> None:
        P.SPINE_DIR = self.dir / "never-made"
        code, out, _ = run_main(["verify"])
        self.assertEqual(code, 0)
        self.assertIn("nothing to verify", out)

    def test_job_limits_the_check_to_one_file(self) -> None:
        self.make_file(2, "alpha-sync")
        bad = self.make_file(2, "beta-export")
        self.rewrite(bad, [self.raw_lines(bad)[0].replace("run 1", "run X"), self.raw_lines(bad)[1]])
        code, out, _ = run_main(["verify", "--job", "alpha-sync"])
        self.assertEqual(code, 0)
        self.assertNotIn("beta-export", out)
        code, out, _ = run_main(["verify", "--job", "beta-export"])
        self.assertEqual(code, 1)

    def test_a_missing_job_or_a_bad_job_name_exits_2(self) -> None:
        code, _, err = run_main(["verify", "--job", "nobody"])
        self.assertEqual(code, 2)
        self.assertIn("no ledger file", err)
        code, _, err = run_main(["verify", "--job", "../x"])
        self.assertEqual(code, 2)
        self.assertIn("invalid job name", err)

    def test_a_job_name_with_a_slash_or_a_leading_dot_is_refused_not_followed(self) -> None:
        sub = self.spine / "sub"
        sub.mkdir()
        P.record("x", "did_work", did_work=1, spine_dir=sub)
        self.assertTrue((sub / "x.jsonl").is_file())
        shutil.copyfile(sub / "x.jsonl", self.spine / ".x.jsonl")
        for name in ("sub/x", ".x"):
            with self.subTest(job=name):
                code, out, err = run_main(["verify", "--job", name])
                self.assertEqual(code, 2)
                self.assertIn("invalid job name", err)
                self.assertNotIn("x.jsonl", out)

    def test_json_shape_for_a_clean_file(self) -> None:
        self.make_file(2)
        code, out, _ = run_main(["verify", "--json"])
        self.assertEqual(code, 0)
        data = json.loads(out)
        self.assertEqual(len(data), 1)
        self.assertEqual(data[0], {"rows": 2, "unhashed": 0, "ok": True, "first_bad_line": None,
                                   "problem": None, "file": "alpha-sync.jsonl"})

    def test_json_shape_for_a_bad_file(self) -> None:
        path = self.make_file(3)
        lines = self.raw_lines(path)
        lines[1] = lines[1].replace("run 2", "run X")
        self.rewrite(path, lines)
        code, out, _ = run_main(["verify", "--json"])
        self.assertEqual(code, 1)
        item = json.loads(out)[0]
        self.assertFalse(item["ok"])
        self.assertEqual(item["first_bad_line"], 2)
        self.assertEqual(item["problem"], "hash mismatch")
        self.assertEqual(set(item), {"rows", "unhashed", "ok", "first_bad_line", "problem", "file"})

    def test_json_for_an_empty_spine_is_an_empty_list(self) -> None:
        code, out, _ = run_main(["verify", "--json"])
        self.assertEqual(code, 0)
        self.assertEqual(json.loads(out), [])


if __name__ == "__main__":
    unittest.main(verbosity=2)
