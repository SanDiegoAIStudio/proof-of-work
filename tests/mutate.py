"""Mutation run: break the code on purpose and confirm the test suite notices.

For each mutation this script copies the repository into a fresh temp folder, replaces one piece
of code with a broken version, checks that the copy now differs from the original, runs the whole
suite inside the copy and prints `killed` (the suite failed, as it should) or `survived` (the
suite still passed, which means a test is missing). The script exits 1 when any mutation survived.

It first runs the suite on an unmodified copy, because a suite that already fails would make every
mutation look killed.

    python3 tests/mutate.py

The original files are never touched. Standard library only.
"""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import NamedTuple

HERE = Path(__file__).resolve().parent.parent
SUITE_TIMEOUT_S = 600


class Mutation(NamedTuple):
    name: str
    path: str
    old: str
    new: str


MUTATIONS = [
    # chain.py
    Mutation(
        "previous hash is always genesis",
        "chain.py",
        "prev = LEGACY if last is None else _prev_from_row(last)",
        "prev = GENESIS",
    ),
    Mutation(
        "canonical hash ignores the previous hash",
        "chain.py",
        'if "prev_row_hash" in obj:\n        prev = obj["prev_row_hash"]',
        'if False:\n        prev = obj["prev_row_hash"]',
    ),
    Mutation(
        "verify_file accepts any hash",
        "chain.py",
        'if row["row_hash"] != canonical_hash(row):',
        "if False:",
    ),
    Mutation(
        "verify_file skips the link check",
        "chain.py",
        'if bad is None and "prev_row_hash" in row and row["prev_row_hash"] != expected:',
        "if False:",
    ),
    Mutation(
        "chain_fields changes its input",
        "chain.py",
        "stamped = json.loads(json.dumps(obj, ensure_ascii=False))",
        "stamped = obj",
    ),
    Mutation(
        "a row with a link but no hash passes",
        "chain.py",
        '                if "prev_row_hash" in row:\n'
        "                    # This writer always sets both fields, so a row with a\n"
        "                    # link and no hash had its hash removed.\n"
        '                    bad = "hash mismatch"',
        "                pass",
    ),
    Mutation(
        "the tail reader takes the cut start of a long row for a whole row",
        "chain.py",
        "complete = parts[1:] if pos > 0 else parts",
        "complete = parts",
    ),
    # jsonl_io.py
    Mutation(
        "append does not repair a torn tail",
        "jsonl_io.py",
        'if handle.read(1) != b"\\n":\n                    handle.write(b"\\n")',
        "pass",
    ),
    # pow.py: writing
    Mutation(
        "record appends without the lock",
        "pow.py",
        "    with flocked(target):\n"
        "        stamped = chain_fields(target, rec)\n"
        "        append_row(target, stamped, lock=False)",
        "    stamped = chain_fields(target, rec)\n"
        "    append_row(target, stamped, lock=False)",
    ),
    Mutation(
        "heartbeat touched even when switched off",
        "pow.py",
        "if heartbeat and HEARTBEAT_DIR is not None and HEARTBEAT_DIR.is_dir():",
        "if HEARTBEAT_DIR is not None and HEARTBEAT_DIR.is_dir():",
    ),
    # pow.py: summary and invariants
    Mutation(
        "miss rate counts no_work as attempted",
        "pow.py",
        'attempted = by_status["did_work"] + by_status["could_not"]',
        'attempted = by_status["did_work"] + by_status["could_not"] + by_status["no_work"]',
    ),
    Mutation(
        "queue delta from a single observation",
        "pow.py",
        "if len(queued) >= 2 else None",
        "if len(queued) >= 1 else None",
    ),
    Mutation(
        "max_miss_rate compares the wrong way",
        "pow.py",
        'verdict["ok"] = rate <= limit',
        'verdict["ok"] = rate >= limit',
    ),
    Mutation(
        "quiet_ok ignored",
        "pow.py",
        'if inv.get("quiet_ok") and not unclassified:',
        "if False:",
    ),
    Mutation(
        "work_within_hours passes a never-worked job",
        "pow.py",
        'verdict["ok"] = False\n            verdict["detail"] = f"{UNPROVEN} (limit {limit:g}h)"',
        'verdict["ok"] = True\n            verdict["detail"] = f"{UNPROVEN} (limit {limit:g}h)"',
    ),
    Mutation(
        "seen_within_hours passes a job with no rows",
        "pow.py",
        'verdict["ok"] = False\n            verdict["detail"] = f"never reported (limit {limit:g}h)"',
        'verdict["ok"] = True\n            verdict["detail"] = f"never reported (limit {limit:g}h)"',
    ),
    Mutation(
        "queue_non_increasing ignores tolerance",
        "pow.py",
        'tolerance = _param(inv, "tolerance", 0, as_int=True)',
        "tolerance = 0",
    ),
    Mutation(
        "max_consecutive_not_work counts runs that worked",
        "pow.py",
        '        if r.get("status") == "did_work":\n            break\n        trailing_not_work += 1',
        '        if False:\n            break\n        trailing_not_work += 1',
    ),
    Mutation(
        "min_items reads the wrong field",
        "pow.py",
        'field = inv.get("field", "did_work")',
        'field = "no_work"',
    ),
    Mutation(
        "max_could_not_share is green with no runs",
        "pow.py",
        '        if total == 0:\n            verdict["detail"] = "no runs in window"\n',
        '        if total == 0:\n            verdict["ok"] = True\n            verdict["detail"] = "no runs in window"\n',
    ),
    # pow.py: registry
    Mutation(
        "underscore keys become jobs",
        "pow.py",
        '        if job.startswith("_"):\n            continue',
        "        if False:\n            continue",
    ),
    Mutation(
        "an unreadable registry is silent",
        "pow.py",
        '        print(f"pow: WARN {problem}", file=sys.stderr)',
        "        pass",
    ),
    # pow.py: reading and commands
    Mutation(
        "verify --job follows a slash in the name",
        "pow.py",
        'return bool(job) and "/" not in job and not job.startswith(".")',
        'return bool(job) and not job.startswith(".")',
    ),
    # examples/demo.sh
    Mutation(
        "the demo writes its ledger inside the repo folder",
        "examples/demo.sh",
        'export POW_SPINE_DIR="$WORK/spine"',
        'export POW_SPINE_DIR="$HERE/spine"',
    ),
    Mutation(
        "check exits 0 with a violation",
        "pow.py",
        "        if violations:\n            return 1\n",
        "        if False:\n            return 1\n",
    ),
    Mutation(
        "verify exits 0 with a bad line",
        "pow.py",
        'return 0 if all(r["ok"] for r in results) else 1',
        "return 0",
    ),
    Mutation(
        "chip summary counts an unknown verdict as clean",
        "pow.py",
        'unknown += [(job, v["kind"]) for v in c["unknown"]]',
        "pass",
    ),
    Mutation(
        "chip summary counts an uncovered job as clean",
        "pow.py",
        "            uncovered.append(job)\n            continue",
        "            continue",
    ),
    Mutation(
        "chip summary counts a job that never reported as clean",
        "pow.py",
        "            awaiting.append(job)\n            continue",
        "            continue",
    ),
    Mutation(
        "rollup replaces old days instead of merging",
        "pow.py",
        "days: dict[str, Any] = {k: dict(v) for k, v in existing.items()}",
        "days: dict[str, Any] = {}",
    ),
    # pow-run.py
    Mutation(
        "an empty optional group crashes the wrapper",
        "pow-run.py",
        "return None if value is None else int(value)",
        "return int(value)",
    ),
    Mutation(
        "wrapper returns 0 for a failing command",
        "pow-run.py",
        'proof-of-work: {exc}", file=sys.stderr)\n\n    return code',
        'proof-of-work: {exc}", file=sys.stderr)\n\n    return 0',
    ),
    Mutation(
        "wrapper records did_work with no pattern",
        "pow-run.py",
        '        status, reason = "no_work", (\n'
        '            "exit 0, no work/no-work pattern configured -- liveness-only coverage"\n'
        "        )",
        '        status, reason = "did_work", None',
    ),
    Mutation(
        "fail pattern ignored",
        "pow-run.py",
        "    elif fail_m:",
        "    elif False:",
    ),
    Mutation(
        "work exit codes ignored",
        "pow-run.py",
        "    elif code in work_codes:",
        "    elif False:",
    ),
    Mutation(
        "a non-zero exit is not a failure",
        "pow-run.py",
        "    elif code != 0:",
        "    elif False:",
    ),
    Mutation(
        "timeout not recorded",
        "pow-run.py",
        '    if timed_out:\n        status, reason = "could_not", f"timed out after {args.timeout}s"',
        '    if False:\n        status, reason = "could_not", f"timed out after {args.timeout}s"',
    ),
    # follow-up 2
    Mutation(
        'pow-run: output that is not UTF-8 costs the exit code',
        'pow-run.py',
        '    out = raw_out.decode("utf-8", errors="replace")\n'
        '    err = raw_err.decode("utf-8", errors="replace")',
        '    out = raw_out.decode("utf-8")\n'
        '    err = raw_err.decode("utf-8")',
    ),
    Mutation(
        'pow-run: a matched count of 0 is work',
        'pow-run.py',
        '        if count == 0:\n            # "copied 0 files"',
        '        if False:\n            # "copied 0 files"',
    ),
    Mutation(
        'pow-run: the end of long output is not searched',
        'pow-run.py',
        '    return [text[:half], text[-half:]]',
        '    return [text[:half]]',
    ),
    Mutation(
        'pow-run: the stored tail comes from the head of long output',
        'pow-run.py',
        '    if status == "could_not" or not classified:\n        evidence["tail"] = combined.strip()[-TAIL_CHARS:]',
        '    if status == "could_not" or not classified:\n        evidence["tail"] = parts[0].strip()[-TAIL_CHARS:]',
    ),
    Mutation(
        'pow-run: a killing signal is not 128 plus its number',
        'pow-run.py',
        '            code = 128 + signum',
        '            code = signum',
    ),
    Mutation(
        'pow-run: a signal is not named in the evidence',
        'pow-run.py',
        '        evidence["signal"] = signum',
        '        pass',
    ),
    Mutation(
        'pow: a ledger with a cut multibyte character raises',
        'pow.py',
        'with path.open("r", encoding="utf-8", errors="replace") as fh:',
        'with path.open("r", encoding="utf-8") as fh:',
    ),
    Mutation(
        'pow: a line that is not an object is kept as a row',
        'pow.py',
        '            if not isinstance(rec, dict):\n                out.append({"_malformed": True, "ts": None})',
        '            if False:\n                out.append({"_malformed": True, "ts": None})',
    ),
    Mutation(
        'pow: a line nested too deep raises',
        'pow.py',
        '            except (ValueError, RecursionError):\n                rec = None',
        '            except ValueError:\n                rec = None',
    ),
    Mutation(
        'pow: a timestamp with no zone is read in local time',
        'pow.py',
        '        if parsed.tzinfo is None:\n            parsed = parsed.replace(tzinfo=timezone.utc)',
        '        if False:\n            parsed = parsed.replace(tzinfo=timezone.utc)',
    ),
    Mutation(
        'pow: a timestamp that is not text raises',
        'pow.py',
        '    if not isinstance(value, str):\n        return None\n    try:\n        parsed = datetime.fromisoformat',
        '    if False:\n        return None\n    try:\n        parsed = datetime.fromisoformat',
    ),
    Mutation(
        'pow: counts that are not an object raise',
        'pow.py',
        '    value = counts.get(status, 0) if isinstance(counts, dict) else 0',
        '    value = counts.get(status, 0)',
    ),
    Mutation(
        'pow: a count that is not a number raises',
        'pow.py',
        '    if isinstance(value, bool) or not isinstance(value, (int, float)):\n        return 0\n    try:',
        '    if False:\n        return 0\n    try:',
    ),
    Mutation(
        'pow: a queue that is not an object raises',
        'pow.py',
        '    if not isinstance(queue, dict):\n        return None\n    depth',
        '    if False:\n        return None\n    depth',
    ),
    Mutation(
        'pow: a queue depth that is text is used',
        'pow.py',
        '    if isinstance(depth, bool) or not isinstance(depth, int):\n        return None',
        '    if False:\n        return None',
    ),
    Mutation(
        'pow: evidence that is not an object raises',
        'pow.py',
        '    return value if isinstance(value, dict) else {}',
        '    return value',
    ),
    Mutation(
        'pow: a window wider than the calendar raises',
        'pow.py',
        '    except OverflowError:\n        # A window wider',
        '    except KeyError:\n        # A window wider',
    ),
    Mutation(
        'pow: verify stops at a ledger it cannot parse',
        'pow.py',
        '        except (RecursionError, OSError) as exc:',
        '        except OSError as exc:',
    ),
    Mutation(
        'pow: quiet_ok is green with no run in the window',
        'pow.py',
        '                if quiet_runs >= 1:',
        '                if True:',
    ),
    Mutation(
        'pow: a garbage rollup file is overwritten',
        'pow.py',
        '        if damaged is not None:\n            print(',
        '        if False:\n            print(',
    ),
    Mutation(
        'pow: the rollup is written in place',
        'pow.py',
        '            write_rollup_file(ROLLUP_PATH, text)',
        '            ROLLUP_PATH.write_text(text, encoding="utf-8")',
    ),
    Mutation(
        'pow: a failed rollup write leaves a temporary file',
        'pow.py',
        '        try:\n            os.unlink(tmp)\n        except OSError:\n            pass\n        raise',
        '        raise',
    ),
    Mutation(
        'pow: a rewritten rollup loses its permission bits',
        'pow.py',
        '        if mode is not None:\n            os.chmod(tmp, mode)',
        '        if False:\n            os.chmod(tmp, mode)',
    ),
    Mutation(
        'pow: a broken registry still exits 0',
        'pow.py',
        '        if problems:\n            why.append(',
        '        if False:\n            why.append(',
    ),
    Mutation(
        'pow: a bad parameter is not reported',
        'pow.py',
        '        if broken:\n            why.append(',
        '        if False:\n            why.append(',
    ),
    Mutation(
        'pow: a missing POW_REGISTRY file still exits 0',
        'pow.py',
        '        if os.environ.get("POW_REGISTRY") and not REGISTRY_PATH.is_file():',
        '        if False:',
    ),
    Mutation(
        'pow: min_items reads its value unguarded',
        'pow.py',
        '        limit = _param(inv, "value", 1, as_int=True)\n        field',
        '        limit = int(inv.get("value", 1))\n        field',
    ),
    Mutation(
        'pow: a boolean is a number for a parameter',
        'pow.py',
        '    if isinstance(raw, bool):\n        raise _BadParameter',
        '    if False:\n        raise _BadParameter',
    ),
    Mutation(
        'pow: a bad window_hours is kept',
        'pow.py',
        '            if not _is_number(hours) or hours <= 0:',
        '            if False:',
    ),
    Mutation(
        'pow: invariants that are not objects are kept',
        'pow.py',
        '                kept = [i for i in invs if isinstance(i, dict)]',
        '                kept = list(invs)',
    ),
    Mutation(
        'pow: check does not name what it could not judge',
        'pow.py',
        '                print(f"UNKNOWN {job}: {v[\'kind\']} - {v[\'detail\']}")',
        '                pass',
    ),
    Mutation(
        'pow: check says no violations when nothing was checked',
        'pow.py',
        '                    print("pow: no invariants are registered; nothing was checked")',
        '                    print("pow: no invariant violations")',
    ),
    Mutation(
        'pow: check --json awaiting flag counts parsed rows',
        'pow.py',
        '"awaiting": not summary["ledger_lines"],',
        '"awaiting": not summary["total_records"],',
    ),
    Mutation(
        'pow: check treats a damaged ledger as awaiting',
        'pow.py',
        '        never = {c["job"] for c in results if not c["summary"]["ledger_lines"]}',
        '        never = {c["job"] for c in results if not c["summary"]["total_records"]}',
    ),
    Mutation(
        'pow: the report treats a damaged ledger as awaiting',
        'pow.py',
        '    never = {c["job"] for c in checks if not c["summary"]["ledger_lines"]}',
        '    never = {c["job"] for c in checks if not c["summary"]["total_records"]}',
    ),
    Mutation(
        'pow: the chip summary treats a damaged ledger as awaiting',
        'pow.py',
        '        if not c["summary"]["ledger_lines"]:',
        '        if not c["summary"]["total_records"]:',
    ),
    Mutation(
        'tests: a child process writes the rollup into the repo',
        'tests/_common.py',
        '    env["POW_ROLLUP"] = str(Path(spine).parent / "child-rollup.json")',
        '    pass',
    ),
    Mutation(
        "a closed stdout skips the ledger row",
        "pow-run.py",
        "        try:\n"
        "            stream.buffer.write(raw)\n"
        "            stream.buffer.flush()\n"
        "        except OSError:\n"
        "            _point_at_devnull(stream)",
        "        stream.buffer.write(raw)",
    ),
    # follow-up 4
    Mutation(
        "pow-run: the child is read as text, so a raw byte is replaced before the echo",
        "pow-run.py",
        "        with subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE) as proc:",
        "        with subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,\n"
        "                              encoding=\"utf-8\", errors=\"replace\") as proc:",
    ),
    Mutation(
        "pow-run: main does not restore sys.stderr",
        "pow-run.py",
        "    finally:\n        sys.stderr = err_stream",
        "    finally:\n        pass",
    ),
]


def copy_repo(dest: Path) -> None:
    ignore = shutil.ignore_patterns("__pycache__", ".git", "spine", "spine-rollup.json", "*.pyc")
    shutil.copytree(HERE, dest, ignore=ignore)


def run_suite(repo: Path) -> subprocess.CompletedProcess:
    env = dict(os.environ)
    for name in ("POW_SPINE_DIR", "POW_REGISTRY", "POW_ROLLUP", "POW_HEARTBEAT_DIR"):
        env.pop(name, None)
    env["PYTHONDONTWRITEBYTECODE"] = "1"
    return subprocess.run([sys.executable, "-m", "unittest", "discover", "-s", "tests", "-f"], cwd=str(repo),
                          capture_output=True, text=True, env=env, timeout=SUITE_TIMEOUT_S, check=False)


def tail(text: str, lines: int = 6) -> str:
    return "\n".join(text.strip().splitlines()[-lines:])


def main() -> int:
    survived = []
    killed = 0
    scratch = Path(tempfile.mkdtemp(prefix="pow-mutate-"))
    try:
        base = scratch / "baseline"
        copy_repo(base)
        try:
            result = run_suite(base)
        except subprocess.TimeoutExpired:
            print(f"baseline FAILED: the suite did not finish in {SUITE_TIMEOUT_S}s on an unmodified copy")
            return 2
        if result.returncode != 0:
            print("baseline FAILED: the suite does not pass on an unmodified copy")
            print(tail(result.stderr + result.stdout, 12))
            return 2
        print("baseline passed")
        shutil.rmtree(base, ignore_errors=True)

        for index, mutation in enumerate(MUTATIONS):
            repo = scratch / f"m{index}"
            copy_repo(repo)
            target = repo / mutation.path
            original = (HERE / mutation.path).read_text(encoding="utf-8")
            count = original.count(mutation.old)
            if count != 1:
                print(f"error {mutation.name}: the text to replace appears {count} times in {mutation.path}")
                return 2
            mutated = original.replace(mutation.old, mutation.new, 1)
            if mutated == original:
                print(f"error {mutation.name}: the mutation did not change the file")
                return 2
            target.write_text(mutated, encoding="utf-8")
            if target.read_text(encoding="utf-8") == (HERE / mutation.path).read_text(encoding="utf-8"):
                print(f"error {mutation.name}: the copy is identical to the original")
                return 2

            try:
                result = run_suite(repo)
                failed = result.returncode != 0
                note = ""
            except subprocess.TimeoutExpired:
                failed = True
                note = f" (suite timed out after {SUITE_TIMEOUT_S}s)"
            if failed:
                killed += 1
                print(f"killed {mutation.name}{note}")
            else:
                survived.append(mutation.name)
                print(f"survived {mutation.name}")
            shutil.rmtree(repo, ignore_errors=True)
    finally:
        shutil.rmtree(scratch, ignore_errors=True)

    print(f"mutations: {len(MUTATIONS)}, killed: {killed}, survived: {len(survived)}")
    return 1 if survived else 0


if __name__ == "__main__":
    sys.exit(main())
