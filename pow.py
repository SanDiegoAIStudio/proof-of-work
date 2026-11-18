#!/usr/bin/env python3
"""proof-of-work: a typed ledger that shows an unattended job did its work.

THE PROBLEM
-----------
A job that runs on a schedule has two ways to do nothing, and both usually
exit 0 and print something cheerful:

    "nothing to do"   a healthy no-op
    "I am broken"     silent degradation

A log or a heartbeat file cannot tell them apart. This module can.

THE LEDGER
----------
Every run appends one row to `<spine>/<job>.jsonl` with one of THREE states:

    did_work    real work happened; the row carries counts and evidence
    no_work     there was legitimately nothing to do
    could_not   the run failed or degraded

`could_not` is the state most monitoring lacks. With it a miss rate, a queue
delta and loud invariants become possible. The ledger is the truth; reports
are derived from it and can be thrown away.

Each row is hash-chained to the one before it (see chain.py), so an edited or
removed row inside a file can be found with `python3 pow.py verify`.

USAGE
-----
    python3 pow.py record --job nightly-backup --status did_work \\
        --did-work 5 --no-work 2 --could-not 1 \\
        --queue-depth 40 --queue-unit files --duration-ms 41230

    from pow import record
    record("nightly-backup", "did_work", did_work=5, queue_depth=40)

    python3 pow.py report        per-job runs, miss rate, queue delta
    python3 pow.py check         evaluate the registry's invariants; exit 1 on a violation,
                                 2 when the check could not be completed
    python3 pow.py jobs          list registered jobs and their invariants
    python3 pow.py rollup        write the small daily aggregate
    python3 pow.py verify        check every ledger file's hash chain

    python3 pow-run.py --job nightly-backup -- /path/to/script.sh
        wraps a command you cannot edit and records it for you
"""

from __future__ import annotations

import argparse
import json
import math
import os
import stat
import sys
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable

_HERE = Path(__file__).resolve().parent
if str(_HERE) not in sys.path:
    sys.path.insert(0, str(_HERE))
from chain import chain_fields, verify_file  # noqa: E402
from jsonl_io import append_row, flocked  # noqa: E402

SPINE_VERSION = 1
ROOT = _HERE
SPINE_DIR = Path(os.environ.get("POW_SPINE_DIR") or ROOT / "spine")
REGISTRY_PATH = Path(os.environ.get("POW_REGISTRY") or ROOT / "jobs.json")
ROLLUP_PATH = Path(os.environ.get("POW_ROLLUP") or ROOT / "spine-rollup.json")
_HEARTBEAT_ENV = os.environ.get("POW_HEARTBEAT_DIR")
HEARTBEAT_DIR = Path(_HEARTBEAT_ENV) if _HEARTBEAT_ENV else None

STATUSES = ("did_work", "no_work", "could_not")

# A job that has never once reported did_work is not "quiet", it is unproven.
# Reports say so instead of showing a reassuring 0% miss rate.
UNPROVEN = "never reported did_work"


# ------------------------------- WRITE -------------------------------


def _append_chained(path: Path, rec: dict[str, Any]) -> dict[str, Any]:
    """Stamp row_hash and prev_row_hash, then append, under one lock.

    The chain link is read from the file's last row, so reading it and
    appending must be one critical section or two writers could link to the
    same predecessor.
    """
    target = str(path)
    with flocked(target):
        stamped = chain_fields(target, rec)
        append_row(target, stamped, lock=False)
    return stamped


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def record(
    job: str,
    status: str,
    *,
    did_work: int = 0,
    no_work: int = 0,
    could_not: int = 0,
    queue_depth: int | None = None,
    queue_unit: str | None = None,
    duration_ms: int | None = None,
    evidence: dict[str, Any] | None = None,
    reason: str | None = None,
    spine_dir: Path | None = None,
    heartbeat: bool = True,
) -> Path:
    """Append one proof-of-work row. Returns the ledger file written.

    `status` is the run-level verdict. The per-item counts make a partly
    degraded run legible: a run can be did_work=4 could_not=1, and the run
    status should then be `could_not`, because degradation is never rounded
    down to success.

    When POW_HEARTBEAT_DIR is set and exists, `<dir>/<job>.last` is touched as
    well, for tools that only watch file times. It proves only that the job
    ran, which is weaker than the row.
    """
    if status not in STATUSES:
        raise ValueError(f"status must be one of {STATUSES}, got {status!r}")
    if not job or "/" in job or job.startswith("."):
        raise ValueError(f"invalid job name: {job!r}")

    rec: dict[str, Any] = {
        "v": SPINE_VERSION,
        "ts": _utcnow(),
        "job": job,
        "status": status,
        "counts": {"did_work": did_work, "no_work": no_work, "could_not": could_not},
    }
    if queue_depth is not None:
        rec["queue"] = {"depth": queue_depth, "unit": queue_unit or "items"}
    if duration_ms is not None:
        rec["duration_ms"] = duration_ms
    if reason:
        rec["reason"] = reason
    if evidence:
        rec["evidence"] = evidence

    target_dir = Path(spine_dir) if spine_dir else SPINE_DIR
    target_dir.mkdir(parents=True, exist_ok=True)
    path = target_dir / f"{job}.jsonl"
    _append_chained(path, rec)

    if heartbeat and HEARTBEAT_DIR is not None and HEARTBEAT_DIR.is_dir():
        try:
            (HEARTBEAT_DIR / f"{job}.last").touch()
        except OSError:
            pass

    return path


# -------------------------------- READ --------------------------------


def _parse_ts(value: Any) -> datetime | None:
    """A timestamp as an aware UTC datetime, or None when it cannot be read.

    A value without a zone is read as UTC, the zone every writer here uses. A
    value that is missing, is not text, does not parse, or is too far from
    today to convert gives None. It never raises.
    """
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    except (ValueError, OverflowError):
        return None


def read_records(job: str, since: datetime | None = None, spine_dir: Path | None = None) -> list[dict[str, Any]]:
    """Read a job's rows, newest last. Malformed lines are counted, not dropped.

    A line that is not valid JSON, or is valid JSON but not an object, comes
    back as {"_malformed": True, "ts": None} so callers can report it. A byte
    sequence that is not valid UTF-8 (a tail cut inside a multibyte character)
    is read as a replacement character instead of raising. With `since`, rows
    older than it, and rows whose timestamp is missing or cannot be read, are
    left out; a malformed line is always kept so it still gets counted. A
    timestamp without a zone is read as UTC.
    """
    target_dir = Path(spine_dir) if spine_dir else SPINE_DIR
    path = target_dir / f"{job}.jsonl"
    if not path.is_file():
        return []
    out: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8", errors="replace") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except (ValueError, RecursionError):
                rec = None
            if not isinstance(rec, dict):
                out.append({"_malformed": True, "ts": None})
                continue
            if since is not None:
                ts = _parse_ts(rec.get("ts"))
                if ts is None or ts < since:
                    continue
            out.append(rec)
    return out


def _item_count(rec: dict[str, Any], status: str) -> int:
    """One per-status item count from a row. Anything that is not a number is 0."""
    counts = rec.get("counts")
    value = counts.get(status, 0) if isinstance(counts, dict) else 0
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return 0
    try:
        return int(value)
    except (ValueError, OverflowError):
        return 0


def _queue_depth(rec: dict[str, Any]) -> int | None:
    """The queue depth a row reports, or None when it carries no usable queue data."""
    queue = rec.get("queue")
    if not isinstance(queue, dict):
        return None
    depth = queue.get("depth")
    if isinstance(depth, bool) or not isinstance(depth, int):
        return None
    return depth


def _evidence(rec: dict[str, Any]) -> dict[str, Any]:
    """A row's evidence as an object; anything else reads as an empty one."""
    value = rec.get("evidence")
    return value if isinstance(value, dict) else {}


def known_jobs(spine_dir: Path | None = None, registry: dict[str, Any] | None = None) -> list[str]:
    """Registered jobs plus every job that has a ledger file, sorted."""
    target_dir = Path(spine_dir) if spine_dir else SPINE_DIR
    registered = set((load_registry() if registry is None else registry).keys())
    on_disk = {p.stem for p in target_dir.glob("*.jsonl")} if target_dir.is_dir() else set()
    return sorted(registered | on_disk)


def _is_number(value: Any) -> bool:
    return not isinstance(value, bool) and isinstance(value, (int, float)) and math.isfinite(value)


def load_registry_checked() -> tuple[dict[str, Any], list[str]]:
    """The usable job registry plus a list of problems found in the file.

    Keys starting with '_' are notes, not jobs. Without that filter a
    `_comment` key would become a phantom job that never reports and shows up
    permanently red.

    Each problem is one short sentence. Whatever can be used is kept: a job
    whose entry is not an object keeps empty settings, a bad `window_hours`
    falls back to the default 24, `invariants` that is not a list is treated as
    empty, and an invariant that is not an object is dropped. A file that
    exists but cannot be read, is not JSON, or is not an object gives an empty
    registry and a problem that says NO invariants will be checked. A file that
    does not exist gives an empty registry and no problem.
    """
    if not REGISTRY_PATH.is_file():
        return {}, []
    try:
        raw = json.loads(REGISTRY_PATH.read_text(encoding="utf-8"))
    except (OSError, ValueError, RecursionError) as exc:
        # A malformed registry means NO invariants are being checked. That must
        # be loud: returning {} silently is how a monitor stops monitoring
        # without anyone noticing.
        return {}, [f"registry unreadable ({exc}); NO invariants will be checked"]
    if not isinstance(raw, dict):
        return {}, ["registry is not a JSON object; NO invariants will be checked"]

    problems: list[str] = []
    registry: dict[str, Any] = {}
    for job, entry in raw.items():
        if job.startswith("_"):
            continue
        if not isinstance(entry, dict):
            problems.append(f"registry: job {job!r} is not an object, so it has no settings")
            registry[job] = {}
            continue
        cfg = dict(entry)
        if "window_hours" in cfg:
            hours = cfg["window_hours"]
            if not _is_number(hours) or hours <= 0:
                problems.append(f"registry: job {job!r} has a window_hours that is not a number above 0; "
                                f"24 is used")
                del cfg["window_hours"]
        if "invariants" in cfg:
            invs = cfg["invariants"]
            if not isinstance(invs, list):
                problems.append(f"registry: job {job!r} has invariants that is not a list, so it has none")
                cfg["invariants"] = []
            else:
                kept = [i for i in invs if isinstance(i, dict)]
                if len(kept) != len(invs):
                    problems.append(f"registry: job {job!r} has {len(invs) - len(kept)} "
                                    f"invariant(s) that are not objects; they are skipped")
                cfg["invariants"] = kept
        registry[job] = cfg
    return registry, problems


def load_registry() -> dict[str, Any]:
    """The job registry. Each problem found in the file is printed once on stderr."""
    registry, problems = load_registry_checked()
    _warn_registry(problems)
    return registry


def _warn_registry(problems: list[str]) -> None:
    for problem in problems:
        print(f"pow: WARN {problem}", file=sys.stderr)


def summarize(job: str, window_hours: float = 24.0, spine_dir: Path | None = None) -> dict[str, Any]:
    """Health of one job over a window. This is the miss-rate aggregator."""
    try:
        since = datetime.now(timezone.utc) - timedelta(hours=window_hours)
    except OverflowError:
        # A window wider than the calendar reaches every row.
        since = datetime.min.replace(tzinfo=timezone.utc)
    recent = read_records(job, since=since, spine_dir=spine_dir)
    allrecs = read_records(job, spine_dir=spine_dir)

    malformed = sum(1 for r in recent if r.get("_malformed"))
    clean = [r for r in recent if not r.get("_malformed")]

    runs = len(clean)
    by_status = {s: sum(1 for r in clean if r.get("status") == s) for s in STATUSES}
    items = {s: sum(_item_count(r, s) for r in clean) for s in STATUSES}

    # Miss rate is the share of runs that COULD NOT work. A run that
    # legitimately had nothing to do is not a miss; that is the distinction a
    # two-state ledger cannot express.
    attempted = by_status["did_work"] + by_status["could_not"]
    miss_rate = (by_status["could_not"] / attempted) if attempted else None

    # A no_work row is evidence of "nothing to do" only when something
    # classified it: a --none-pattern match, or the job calling record() itself.
    # pow-run marks every other exit-0 run "unclassified", which means nobody
    # said what work looks like. The two read the same in a count and mean
    # opposite things, so quiet_ok refuses to green a job whose quiet has never
    # been classified.
    unclassified_quiet = sum(
        1 for r in clean
        if r.get("status") == "no_work"
        and _evidence(r).get("classification") == "unclassified")

    # Queue delta over the window. A queue that grows while the job reports
    # success is the signature of silent degradation. A delta needs TWO
    # observations: with one, first == last and subtraction reports "+0",
    # which reads as flat and healthy when the honest answer is unknown.
    queued = [r for r in clean if _queue_depth(r) is not None]
    queue_first = _queue_depth(queued[0]) if queued else None
    queue_last = _queue_depth(queued[-1]) if queued else None
    queue_delta = (queue_last - queue_first) if len(queued) >= 2 else None

    last_work = None
    for r in reversed(allrecs):
        if r.get("status") == "did_work":
            last_work = r.get("ts")
            break

    last_seen = next((r.get("ts") for r in reversed(allrecs) if r.get("ts")), None)

    # Consecutive most-recent runs without real work, over ALL rows and not the
    # window: a long run of "skipping" rows on a slow schedule can be wider
    # than any one window.
    trailing_not_work = 0
    for r in reversed(allrecs):
        if r.get("_malformed"):
            continue
        if r.get("status") == "did_work":
            break
        trailing_not_work += 1

    return {
        "job": job,
        "window_hours": window_hours,
        "runs": runs,
        "runs_by_status": by_status,
        "items": items,
        "miss_rate": miss_rate,
        "queue_first": queue_first,
        "queue_last": queue_last,
        "queue_unit": (queued[-1]["queue"].get("unit") if queued else None),
        "queue_delta": queue_delta,
        "unclassified_quiet": unclassified_quiet,
        "last_did_work": last_work,
        "last_seen": last_seen,
        "trailing_not_work": trailing_not_work,
        "malformed_lines": malformed,
        "total_records": len([r for r in allrecs if not r.get("_malformed")]),
        # Every non-blank line, readable or not. A job is "awaiting" only when
        # this is 0: a ledger that holds nothing but damaged lines has reported.
        "ledger_lines": len(allrecs),
        "unproven": last_work is None,
    }


# ----------------------------- INVARIANTS -----------------------------
#
# An invariant is a testable assertion that fails loudly. Each one is a pure
# function of a summary, so it can be tested without touching disk.


def _hours_since(ts: str | None) -> float | None:
    parsed = _parse_ts(ts) if ts else None
    if parsed is None:
        return None
    return (datetime.now(timezone.utc) - parsed).total_seconds() / 3600.0


class _BadParameter(ValueError):
    """An invariant setting that cannot be read as the number it must be."""


def _param(inv: dict[str, Any], key: str, default: float, as_int: bool = False) -> Any:
    """A numeric setting of an invariant. A boolean or non-finite value is not a number."""
    raw = inv.get(key, default)
    if isinstance(raw, bool):
        raise _BadParameter(f"{key} must be a number, got {raw!r}")
    try:
        number = int(raw) if as_int else float(raw)
    except (TypeError, ValueError, OverflowError):
        raise _BadParameter(f"{key} must be a number, got {str(raw)[:40]!r}")
    if not as_int and not math.isfinite(number):
        raise _BadParameter(f"{key} must be a finite number, got {raw!r}")
    return number


def eval_invariant(inv: dict[str, Any], summary: dict[str, Any]) -> dict[str, Any]:
    """Evaluate one invariant against a job summary. Returns a verdict dict.

    The verdict is {"kind", "ok", "detail"}. An invariant that cannot be
    evaluated returns ok=None (unknown) and never ok=True: absence of evidence
    must not read as evidence of health.

    A setting that cannot be read as a number, or a kind this tool does not
    know, is a mistake in the registry and not a fact about the job. That
    verdict is unknown and also carries "config_error": True, so `check` can
    tell "could not judge" from "the check itself is broken".
    """
    kind = inv.get("kind")
    try:
        return _eval_kind(kind, inv, summary)
    except _BadParameter as exc:
        return {"kind": kind, "ok": None, "detail": f"bad parameter: {exc}", "config_error": True}


def _eval_kind(kind: Any, inv: dict[str, Any], summary: dict[str, Any]) -> dict[str, Any]:
    verdict: dict[str, Any] = {"kind": kind, "ok": None, "detail": ""}

    if kind == "queue_non_increasing":
        delta = summary["queue_delta"]
        tolerance = _param(inv, "tolerance", 0, as_int=True)
        if delta is None:
            verdict["detail"] = "no queue depth reported in window"
        else:
            verdict["ok"] = delta <= tolerance
            verdict["detail"] = (
                f"queue {summary['queue_first']} -> {summary['queue_last']} "
                f"(delta {delta:+d}, tolerance {tolerance})"
            )

    elif kind == "max_miss_rate":
        limit = _param(inv, "value", 0.1)
        rate = summary["miss_rate"]
        if rate is None:
            # No attempted runs: every run in the window reported no_work. For
            # most jobs that is genuinely unknown and stays unknown. For a job
            # whose steady state IS quiet (a queue drainer with an empty queue,
            # a scan that finds nothing) it is the healthy answer, and leaving
            # it unknown would keep that job permanently in the warning list.
            #
            # The flag is opt-in per job, never a default, and liveness still
            # has to be proven separately by seen_within_hours: alive and quiet
            # is fine, silent is not.
            unclassified = summary.get("unclassified_quiet") or 0
            quiet_runs = (summary.get("runs_by_status") or {}).get("no_work", 0)
            if inv.get("quiet_ok") and not unclassified:
                # Quiet has to be seen. A window with no run at all is a job
                # that did not run, which is not the same as a job with
                # nothing to do.
                if quiet_runs >= 1:
                    verdict["ok"] = True
                    verdict["detail"] = "quiet: nothing to do in window (expected for this job)"
                else:
                    verdict["detail"] = "no runs in window"
            elif inv.get("quiet_ok"):
                # The flag is set but unearned. Greening a job whose quiet was
                # never classified would turn "I could not tell" into
                # "healthy", the filtered-zero mistake this module exists to stop.
                verdict["detail"] = (f"quiet_ok is set but {unclassified} no_work row(s) are "
                                     f"unclassified: give the job a --none-pattern or have it "
                                     f"call record(), then the flag counts")
            else:
                verdict["detail"] = "no attempted runs in window"
        else:
            verdict["ok"] = rate <= limit
            verdict["detail"] = f"miss rate {rate:.0%} (limit {limit:.0%})"

    elif kind == "work_within_hours":
        limit = _param(inv, "value", 24)
        age = _hours_since(summary["last_did_work"])
        if age is None:
            verdict["ok"] = False
            verdict["detail"] = f"{UNPROVEN} (limit {limit:g}h)"
        else:
            verdict["ok"] = age <= limit
            verdict["detail"] = f"last real work {age:.1f}h ago (limit {limit:g}h)"

    elif kind == "seen_within_hours":
        limit = _param(inv, "value", 24)
        age = _hours_since(summary["last_seen"])
        if age is None:
            verdict["ok"] = False
            verdict["detail"] = f"never reported (limit {limit:g}h)"
        else:
            verdict["ok"] = age <= limit
            verdict["detail"] = f"last ran {age:.1f}h ago (limit {limit:g}h)"

    elif kind == "max_could_not_share":
        # For jobs whose normal answer is no_work, max_miss_rate is misleading:
        # its denominator excludes no_work, so one failed run beside two real
        # ones on a quiet day reads 33%. This kind divides could_not by EVERY
        # run in the window, which answers the question such a job needs
        # answered: what share of its runs could not do the job at all.
        limit = _param(inv, "value", 0.1)
        by = summary.get("runs_by_status") or {}
        total = int(by.get("did_work", 0)) + int(by.get("no_work", 0)) + int(by.get("could_not", 0))
        if total == 0:
            verdict["detail"] = "no runs in window"
        else:
            share = int(by.get("could_not", 0)) / total
            verdict["ok"] = share <= limit
            verdict["detail"] = f"could_not share {share:.0%} of {total} runs (limit {limit:.0%})"

    elif kind == "max_consecutive_not_work":
        limit = _param(inv, "value", 3, as_int=True)
        streak = summary.get("trailing_not_work")
        total = int(summary.get("total_records", 0) or 0)
        if streak is None:
            verdict["detail"] = "summary carries no trailing_not_work"
        elif total < limit:
            verdict["detail"] = (
                f"only {total} records ever (streak {streak}, limit {limit}); too thin to judge"
            )
        else:
            verdict["ok"] = streak < limit
            verdict["detail"] = (
                f"{streak} consecutive runs without real work (limit {limit})"
            )

    elif kind == "min_items":
        limit = _param(inv, "value", 1, as_int=True)
        field = inv.get("field", "did_work")
        if not isinstance(field, str):
            raise _BadParameter(f"field must be text, got {str(field)[:40]!r}")
        got = int(summary["items"].get(field, 0))
        verdict["ok"] = got >= limit
        verdict["detail"] = f"{field}={got} in window (min {limit})"

    else:
        verdict["detail"] = f"unknown invariant kind: {kind!r}"
        verdict["config_error"] = True

    return verdict


def check_all(
    window_hours: float | None = None,
    spine_dir: Path | None = None,
    registry: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    """Evaluate every known job. Each job uses its declared window_hours unless overridden.

    `registry` is an already loaded registry; without it the file is read here.
    """
    if registry is None:
        registry = load_registry()
    results: list[dict[str, Any]] = []
    for job in known_jobs(spine_dir=spine_dir, registry=registry):
        cfg = registry.get(job, {})
        win = window_hours if window_hours is not None else float(cfg.get("window_hours", 24))
        summary = summarize(job, window_hours=win, spine_dir=spine_dir)
        invs = cfg.get("invariants", [])
        verdicts = [eval_invariant(i, summary) for i in invs]
        results.append(
            {
                "job": job,
                "registered": job in registry,
                # True only when the ledger holds no line at all (see summarize).
                "awaiting": not summary["ledger_lines"],
                "summary": summary,
                "verdicts": verdicts,
                "violated": [v for v in verdicts if v["ok"] is False],
                "unknown": [v for v in verdicts if v["ok"] is None],
            }
        )
    return results


# ------------------------------- RENDER -------------------------------


def _fmt_rate(rate: float | None) -> str:
    return "  n/a" if rate is None else f"{rate:>4.0%}"


def chip_summary(window_hours: float | None = None, spine_dir: Path | None = None) -> dict[str, Any]:
    """One-glance state of every job, small enough for a single line.

    Four disjoint buckets. `level` is the worst thing present, and only a
    clean state is "ok":
        violated   a declared invariant is breached           (err)
        unknown    an invariant could not be evaluated        (warn)
        uncovered  the job has no invariants at all           (warn)
        awaiting   the job is registered but never reported   (warn)

    This lives in the library, not in a caller, so it can be tested. A summary
    that counted only `violated` would show unknown and uncovered jobs as
    clean, and a job that had stopped recording entirely would show green.
    """
    results = check_all(window_hours=window_hours, spine_dir=spine_dir)
    violated: list[tuple[str, str]] = []
    unknown: list[tuple[str, str]] = []
    uncovered: list[str] = []
    awaiting: list[str] = []

    for c in results:
        job = c["job"]
        if not c["summary"]["ledger_lines"]:
            # Never reported at all. Distinct from "reported before, now
            # silent": one is a coverage gap, the other is a regression.
            awaiting.append(job)
            continue
        if not c["verdicts"]:
            uncovered.append(job)
            continue
        violated += [(job, v["kind"]) for v in c["violated"]]
        unknown += [(job, v["kind"]) for v in c["unknown"]]

    if not results:
        level, text = "warn", "pow: no jobs reporting"
    elif violated:
        first = f"{violated[0][0]} {violated[0][1]}"
        level, text = "err", f"pow: {first}{f' +{len(violated) - 1}' if len(violated) > 1 else ''}"
    elif unknown or uncovered or awaiting:
        bits = []
        if unknown:
            bits.append(f"{len(unknown)} unproven")
        if uncovered:
            bits.append(f"{len(uncovered)} uncovered")
        if awaiting:
            bits.append(f"{len(awaiting)} awaiting 1st")
        level, text = "warn", f"pow: {', '.join(bits)}"
    else:
        level, text = "ok", f"pow: {len(results)} jobs clean"

    return {
        "level": level,
        "text": text,
        "violated": violated,
        "unknown": unknown,
        "uncovered": uncovered,
        "awaiting": awaiting,
        "n_jobs": len(results),
    }


def render_report(window_hours: float | None = None, spine_dir: Path | None = None) -> str:
    registry = load_registry()
    jobs = known_jobs(spine_dir=spine_dir, registry=registry)
    if not jobs:
        return (
            "proof-of-work: no jobs have reported yet.\n"
            f"Spine dir: {spine_dir or SPINE_DIR}\n"
            "Wire a job with:  pow.py record --job <name> --status did_work --did-work N\n"
        )
    span = f"last {window_hours:g}h" if window_hours is not None else "each job's declared window"
    lines = [
        f"=== proof-of-work: {span} ===",
        "",
        f"{'JOB':<26} {'RUNS':>5} {'WORK':>5} {'NONE':>5} {'FAIL':>5} {'MISS':>5}  {'QUEUE D':>9}  LAST REAL WORK",
        "-" * 100,
    ]
    problems: list[str] = []
    for job in jobs:
        cfg = registry.get(job, {})
        # Honor the job's declared window, exactly as check_all does. If the
        # two used different windows, `report` and `check` could disagree about
        # the same job, and two surfaces disagreeing about health is a bug.
        win = window_hours if window_hours is not None else float(cfg.get("window_hours", 24))
        s = summarize(job, window_hours=win, spine_dir=spine_dir)
        by = s["runs_by_status"]
        delta = "     n/a" if s["queue_delta"] is None else f"{s['queue_delta']:+d}"
        if s["unproven"]:
            last = UNPROVEN
        else:
            age = _hours_since(s["last_did_work"])
            last = f"{age:.1f}h ago" if age is not None else str(s["last_did_work"])
        lines.append(
            f"{job:<26} {s['runs']:>5} {by['did_work']:>5} {by['no_work']:>5} "
            f"{by['could_not']:>5} {_fmt_rate(s['miss_rate'])}  {delta:>9}  {last}"
        )
        if s["malformed_lines"]:
            problems.append(f"  {job}: {s['malformed_lines']} malformed ledger line(s)")
        if not cfg:
            problems.append(f"  {job}: not in the registry, so no invariants are being checked")

    lines.append("")
    checks = check_all(window_hours=window_hours, spine_dir=spine_dir, registry=registry)
    # Split day-zero from regressions, matching chip_summary exactly. A job
    # registered five minutes ago is a coverage gap, not a break; lumping the
    # two together buries real failures under first-run noise.
    never = {c["job"] for c in checks if not c["summary"]["ledger_lines"]}
    violated = [(c["job"], v) for c in checks if c["job"] not in never for v in c["violated"]]
    unknown = [(c["job"], v) for c in checks if c["job"] not in never for v in c["unknown"]]

    if violated:
        lines.append("!! INVARIANTS VIOLATED")
        for job, v in violated:
            lines.append(f"  x {job}: {v['kind']} - {v['detail']}")
    else:
        lines.append("ok: no invariant violations")

    if never:
        lines.append("")
        lines.append("awaiting first report (registered, has never run yet):")
        for job in sorted(never):
            lines.append(f"  - {job}")

    if unknown:
        lines.append("")
        lines.append("?  not evaluable (absence of evidence is NOT health):")
        for job, v in unknown:
            lines.append(f"  ? {job}: {v['kind']} - {v['detail']}")

    if problems:
        lines.append("")
        lines.append("coverage gaps:")
        lines.extend(problems)

    return "\n".join(lines) + "\n"


# -------------------------------- ROLLUP --------------------------------


def build_rollup(spine_dir: Path | None = None) -> dict[str, Any]:
    """Daily per-job aggregate, small enough to keep under version control.

    Counts per day per job: runs, the three statuses, items of real work, and
    the first and last queue depth. Merges into the existing rollup file
    instead of replacing it, so days whose raw rows were pruned or moved away
    are preserved. Each day a job still has rows for is recomputed from those
    rows, so a day with some of its rows removed gets smaller counts.
    """
    # A damaged file adds nothing here; the `rollup` command refuses to write
    # over one (see rollup_problem), and a direct caller just gets fresh days.
    existing: dict[str, Any] = {}
    if ROLLUP_PATH.is_file() and rollup_problem() is None:
        existing = json.loads(ROLLUP_PATH.read_text(encoding="utf-8")).get("days", {})

    days: dict[str, Any] = {k: dict(v) for k, v in existing.items()}
    for job in known_jobs(spine_dir=spine_dir):
        # Rebuild this job's days from its raw rows. Days that exist only in
        # the old rollup stay as they were.
        fresh: dict[str, Any] = {}
        for rec in read_records(job, spine_dir=spine_dir):
            if rec.get("_malformed") or not rec.get("ts"):
                continue
            day = str(rec["ts"])[:10]
            bucket = fresh.setdefault(
                day,
                {"runs": 0, "did_work": 0, "no_work": 0, "could_not": 0,
                 "items_did_work": 0, "queue_first": None, "queue_last": None},
            )
            bucket["runs"] += 1
            status = rec.get("status")
            if status in STATUSES:
                bucket[status] += 1
            bucket["items_did_work"] += _item_count(rec, "did_work")
            depth = _queue_depth(rec)
            if depth is not None:
                if bucket["queue_first"] is None:
                    bucket["queue_first"] = depth
                bucket["queue_last"] = depth
        for day, bucket in fresh.items():
            days.setdefault(day, {})[job] = bucket

    return {"v": SPINE_VERSION, "days": days}


def rollup_problem() -> str | None:
    """Why the existing rollup file cannot be merged into, or None when it can.

    A missing file is fine. A file that cannot be read, is not JSON, is not an
    object, or whose "days" is not a map of days is damaged: merging into it
    would drop whatever it holds, so the `rollup` command refuses it.
    """
    if not ROLLUP_PATH.is_file():
        return None
    try:
        data = json.loads(ROLLUP_PATH.read_text(encoding="utf-8"))
    except (OSError, ValueError, RecursionError) as exc:
        return f"is not readable JSON ({str(exc)[:80]})"
    if not isinstance(data, dict):
        return "is not a JSON object"
    days = data.get("days", {})
    if not isinstance(days, dict) or not all(isinstance(v, dict) for v in days.values()):
        return "has a days entry that is not a map of days"
    return None


def write_rollup_file(path: Path, text: str) -> None:
    """Replace `path` with `text`: a temp file in the same folder, fsynced, then os.replace.

    An existing file keeps its permission bits. A new file gets the usual
    default (0666 masked by the umask). On any failure the old file is left
    exactly as it was and the temp file is removed.
    """
    try:
        mode: int | None = stat.S_IMODE(path.stat().st_mode)
    except FileNotFoundError:
        mode = None
    tmp = path.with_name(f".{path.name}.{uuid.uuid4().hex[:10]}.tmp")
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o666)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            fh.write(text)
            fh.flush()
            os.fsync(fh.fileno())
        if mode is not None:
            os.chmod(tmp, mode)
        os.replace(tmp, path)
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


# --------------------------------- CLI ---------------------------------


def _valid_job_name(job: str) -> bool:
    return bool(job) and "/" not in job and not job.startswith(".")


def _cmd_verify(job: str | None, as_json: bool) -> int:
    if job is not None:
        if not _valid_job_name(job):
            print(f"pow: invalid job name: {job!r}", file=sys.stderr)
            return 2
        path = SPINE_DIR / f"{job}.jsonl"
        if not path.is_file():
            print(f"pow: no ledger file for job {job!r} in {SPINE_DIR}", file=sys.stderr)
            return 2
        files = [path]
    else:
        files = sorted(SPINE_DIR.glob("*.jsonl")) if SPINE_DIR.is_dir() else []

    if not files:
        if as_json:
            print(json.dumps([], indent=2))
        else:
            print(f"pow: nothing to verify in {SPINE_DIR}")
        return 0

    results = []
    for path in files:
        try:
            res = verify_file(str(path))
        except (RecursionError, OSError) as exc:
            # One unreadable ledger is a failed ledger, not a stop for the others.
            res = {"ok": False, "rows": 0, "unhashed": 0, "first_bad_line": None,
                   "problem": f"could not be read ({type(exc).__name__})"}
        res["file"] = path.name
        results.append(res)

    if as_json:
        print(json.dumps(results, indent=2))
    else:
        for res in results:
            if res["ok"]:
                tail = "ok"
            elif res["first_bad_line"] is None:
                tail = f"BAD: {res['problem']}"
            else:
                tail = f"BAD at line {res['first_bad_line']}: {res['problem']}"
            print(f"{res['file']}: {res['rows']} rows, {res['unhashed']} unhashed, {tail}")
    return 0 if all(r["ok"] for r in results) else 1


def main(argv: Iterable[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="pow.py", description="proof-of-work: a typed ledger for unattended jobs")
    sub = ap.add_subparsers(dest="cmd", required=True)

    r = sub.add_parser("record", help="append one proof-of-work row")
    r.add_argument("--job", required=True)
    r.add_argument("--status", required=True, choices=STATUSES)
    r.add_argument("--did-work", type=int, default=0)
    r.add_argument("--no-work", type=int, default=0)
    r.add_argument("--could-not", type=int, default=0)
    r.add_argument("--queue-depth", type=int)
    r.add_argument("--queue-unit")
    r.add_argument("--duration-ms", type=int)
    r.add_argument("--reason")
    r.add_argument("--evidence", help="JSON object with run-specific proof")
    r.add_argument("--no-heartbeat", action="store_true")

    rep = sub.add_parser("report", help="per-job health: miss rate and queue delta")
    rep.add_argument("--window-hours", type=float,
                     help="override every job's declared window (default: honor each job's own)")
    rep.add_argument("--json", action="store_true")

    chk = sub.add_parser("check", help="evaluate invariants; exit 1 if any is violated")
    chk.add_argument("--window-hours", type=float)
    chk.add_argument("--json", action="store_true")

    sub.add_parser("jobs", help="list registered jobs and their invariants")
    sub.add_parser("rollup", help="write the daily aggregate")

    ver = sub.add_parser("verify", help="check each ledger file's hash chain; exit 1 on a bad line")
    ver.add_argument("--job", help="verify only this job's ledger file")
    ver.add_argument("--json", action="store_true")

    args = ap.parse_args(list(argv) if argv is not None else None)

    if args.cmd == "record":
        evidence = None
        if args.evidence:
            try:
                evidence = json.loads(args.evidence)
            except json.JSONDecodeError as exc:
                print(f"pow: --evidence is not valid JSON: {exc}", file=sys.stderr)
                return 2
            if not isinstance(evidence, dict):
                print("pow: --evidence must be a JSON object", file=sys.stderr)
                return 2
        try:
            path = record(
                args.job,
                args.status,
                did_work=args.did_work,
                no_work=args.no_work,
                could_not=args.could_not,
                queue_depth=args.queue_depth,
                queue_unit=args.queue_unit,
                duration_ms=args.duration_ms,
                reason=args.reason,
                evidence=evidence,
                heartbeat=not args.no_heartbeat,
            )
        except ValueError as exc:
            print(f"pow: {exc}", file=sys.stderr)
            return 2
        print(f"pow: recorded {args.job} status={args.status} -> {path}")
        return 0

    if args.cmd == "report":
        if args.json:
            # Same window rule as the text report and check_all: the flag
            # overrides, otherwise each job's own window.
            registry = load_registry()
            payload = [
                summarize(
                    j,
                    window_hours=args.window_hours
                    if args.window_hours is not None
                    else float(registry.get(j, {}).get("window_hours", 24)),
                )
                for j in known_jobs(registry=registry)
            ]
            print(json.dumps(payload, indent=2))
        else:
            print(render_report(args.window_hours), end="")
        return 0

    if args.cmd == "check":
        registry, problems = load_registry_checked()
        _warn_registry(problems)
        results = check_all(window_hours=args.window_hours, registry=registry)
        # Same bucketing as chip_summary and render_report: a job that has never
        # reported is a coverage gap, not a violation. `check` exits 1 only on
        # a real regression, so it is safe as a scheduled gate on day zero.
        never = {c["job"] for c in results if not c["summary"]["ledger_lines"]}
        violations = [(c["job"], v) for c in results if c["job"] not in never for v in c["violated"]]
        unjudged = [(c["job"], v) for c in results if c["job"] not in never for v in c["unknown"]]
        evaluated = sum(len(c["verdicts"]) for c in results)
        if args.json:
            print(json.dumps(results, indent=2))
        else:
            for job, v in violations:
                print(f"VIOLATED {job}: {v['kind']} - {v['detail']}")
            for job, v in unjudged:
                print(f"UNKNOWN {job}: {v['kind']} - {v['detail']}")
            for job in sorted(never):
                print(f"AWAITING {job}: registered but has never reported")
            if not violations:
                if evaluated:
                    print("pow: no invariant violations")
                else:
                    print("pow: no invariants are registered; nothing was checked")
        if violations:
            return 1
        # Exit 2: the check ran but could not be trusted to be complete. A
        # gate that reads exit 0 as "all clear" must not get 0 here.
        why: list[str] = []
        if problems:
            why.append(f"the registry has {len(problems)} problem(s)")
        broken = [f"{c['job']}: {v['kind']}" for c in results for v in c["verdicts"] if v.get("config_error")]
        if broken:
            why.append(f"{len(broken)} invariant(s) could not be read ({broken[0]}"
                       f"{' and more' if len(broken) > 1 else ''})")
        if os.environ.get("POW_REGISTRY") and not REGISTRY_PATH.is_file():
            why.append(f"POW_REGISTRY names {REGISTRY_PATH}, which is not a file")
        if why:
            print(f"pow: ERROR check was incomplete: {'; '.join(why)}", file=sys.stderr)
            return 2
        return 0

    if args.cmd == "rollup":
        damaged = rollup_problem()
        if damaged is not None:
            print(f"pow: ERROR rollup file {ROLLUP_PATH} {damaged}; it was left as it was",
                  file=sys.stderr)
            return 2
        data = build_rollup()
        text = json.dumps(data, indent=2, sort_keys=True) + "\n"
        try:
            write_rollup_file(ROLLUP_PATH, text)
        except OSError as exc:
            print(f"pow: ERROR could not write {ROLLUP_PATH}: {exc}; the old file was left as it was",
                  file=sys.stderr)
            return 2
        n_days = len(data["days"])
        print(f"pow: rolled up {n_days} day(s) -> {ROLLUP_PATH}")
        return 0

    if args.cmd == "jobs":
        registry = load_registry()
        if not registry:
            print(f"pow: no jobs registered in {REGISTRY_PATH}")
            return 0
        for job, cfg in sorted(registry.items()):
            print(f"{job}: {cfg.get('description', '')}")
            for inv in cfg.get("invariants", []):
                print(f"    - {inv.get('kind')}: {json.dumps({k: v for k, v in inv.items() if k != 'kind'})}")
        return 0

    if args.cmd == "verify":
        return _cmd_verify(args.job, args.json)

    return 2


if __name__ == "__main__":
    sys.exit(main())
