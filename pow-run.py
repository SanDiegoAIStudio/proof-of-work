#!/usr/bin/env python3
"""pow-run: wrap any command so it records proof-of-work, without editing it.

    python3 pow-run.py --job nightly-backup -- /path/to/backup.sh

Rewriting every job to call `record()` itself is the right end state, but it
means editing every job. This wrapper gives each one a baseline today: it runs
the command, passes stdout, stderr and the exit code straight through, and
records what it can honestly observe.

WHAT IT CAN HONESTLY KNOW
-------------------------
    exit != 0   could_not. A proven failure. Many scheduled commands end in
                `>> some.log 2>&1`, so this signal is otherwise invisible.
    exit == 0   it does NOT know whether real work happened.

That second line is why this wrapper is a floor and not a substitute. With no
patterns configured, an exit-0 run is recorded as `no_work`, which is literally
true ("I did not observe work"). Because the job then never reports
`did_work`, `python3 pow.py report` shows `never reported did_work` and a
`work_within_hours` invariant fails. Coverage that only proves the job ran says
so, and never passes for proof of work.

To get the full three-state signal, teach the wrapper the job's output
vocabulary:

    python3 pow-run.py --job photo-ingest \\
        --work-pattern 'imported (\\d+) photos' \\
        --none-pattern 'nothing new to import' \\
        --queue-pattern 'backlog: (\\d+)' \\
        -- python3 photo-ingest.py

or have the job call `pow.record()` itself. A job that reports its own outcome
cannot be misread by a regular expression over its logs.

EXIT CODE
---------
Always the wrapped command's own exit code, so cron semantics are unchanged.
124 when the wrapper stopped the command at --timeout, 127 when it could not
start it. A failure to record is a warning on stderr and never a failure of the
job.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import signal
import subprocess
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from pow import record  # noqa: E402

# Cap what is kept from a chatty job's output: enough to match patterns and to
# quote a failure, far short of storing a large log in the ledger.
TAIL_CHARS = 4000
MATCH_CHARS = 200_000


def _ends(text: str, limit: int = MATCH_CHARS) -> list[str]:
    """The parts of `text` that patterns are searched in.

    Output of at most `limit` characters is searched whole. Longer output is
    searched as its first and its last `limit // 2` characters, each on its
    own, so a match never spans the cut and the middle is not searched. A
    summary or backlog line sits at the end of a long run, so the end must be
    read as well as the start.
    """
    if len(text) <= limit:
        return [text]
    half = limit // 2
    return [text[:half], text[-half:]]


def _search(pattern: str | None, parts: list[str]) -> re.Match[str] | None:
    """The first match of `pattern` in `parts`, trying them in order."""
    if not pattern:
        return None
    try:
        compiled = re.compile(pattern, re.MULTILINE)
    except re.error as exc:
        print(f"pow-run: bad pattern {pattern!r}: {exc}", file=sys.stderr)
        return None
    for part in parts:
        found = compiled.search(part)
        if found:
            return found
    return None


def _int_group(m: re.Match[str] | None, group: int = 1) -> int | None:
    if not m or m.re.groups < group:
        return None
    try:
        value = m.group(group)
        # An optional group that took no part in the match is None: count it like a word.
        return None if value is None else int(value)
    except (ValueError, IndexError):
        return None


def _tail(detail_file: str, limit: int = 8192) -> str:
    """The last `limit` bytes of a file the job writes instead of printing.

    Seeked, not read through, so a log with many lines costs the same as a short one.
    """
    try:
        path = Path(detail_file).expanduser()
        with path.open("rb") as fh:
            if path.stat().st_size > limit:
                fh.seek(-limit, 2)
            return fh.read().decode("utf-8", errors="replace")
    except OSError:
        return ""


ANCHOR_BYTES = 256


_MARK_MTIMES: dict = {}


def _mark(detail_file: str | None) -> tuple[int, bytes]:
    """A (size, anchor) reading of the detail file, taken BEFORE the job runs.

    The anchor is the last ANCHOR_BYTES of the file. After the run, finding
    those same bytes still sitting at the same place proves the job APPENDED;
    finding anything else proves it REWROTE the file. Both shapes exist (a log
    that gains a line per run, and a state file overwritten on every run) and
    they need opposite handling, so guessing is not an option.
    """
    if not detail_file:
        return 0, b""
    try:
        path = Path(detail_file).expanduser()
        st = path.stat()
        size = st.st_size
        with path.open("rb") as fh:
            fh.seek(max(0, size - ANCHOR_BYTES))
            anchor = fh.read(ANCHOR_BYTES)
        # The mtime is kept in a side table keyed by the same (size, anchor)
        # pair, so the pair every caller and test relies on stays a 2-tuple.
        _MARK_MTIMES[(size, anchor)] = st.st_mtime_ns
        return size, anchor
    except OSError:
        return 0, b""


def _appended_parts(detail_file: str, mark: tuple[int, bytes],
                    limit: int = MATCH_CHARS) -> tuple[list[str], str]:
    """What the job wrote to its detail file during THIS run: (parts to search, its end).

    Appending job: only the bytes past the pre-run size. Reading the tail
    instead would be wrong, because the tail of an append-only log is mostly
    earlier runs. A --work-pattern matched against it would report did_work on
    a run that did nothing, which makes the wrapper a green-washer.

    Rewriting job: the pre-run offset points into unrelated content, so the
    whole file counts. A rewrite is detected by the anchor no longer being
    where it was, which also covers a log rotated mid-run.

    Up to `limit` bytes come back whole. More comes back as the first and the
    last `limit // 2` bytes, read by seeking, so a large log is never loaded.
    The second item is the text to quote: the whole text, or its last part.
    """
    offset, anchor = mark
    half = limit // 2
    try:
        path = Path(detail_file).expanduser()
        size = path.stat().st_size
        with path.open("rb") as fh:
            # The anchor is the whole test. A separate size comparison would
            # look like a second guard without being one: every case it would
            # reject (empty file, rotation, truncation) already fails the
            # anchor read. An empty anchor means there was no file to append
            # to, so read from the top.
            appended = False
            if anchor:
                fh.seek(max(0, offset - len(anchor)))
                appended = fh.read(len(anchor)) == anchor
            # A file rewritten with identical last bytes and an unchanged size
            # passes the anchor test and would read as "appended nothing". A
            # rewrite moves the mtime while anchor and size stay put, and an
            # append never leaves the size unchanged. So: same size and a
            # newer mtime means rewritten, and the whole file is the evidence.
            if appended and size == offset:
                marked = _MARK_MTIMES.get((offset, anchor))
                if marked is not None and path.stat().st_mtime_ns != marked:
                    appended = False
            begin = offset if appended else 0
            if size - begin <= limit:
                fh.seek(begin)
                body = fh.read(limit).decode("utf-8", errors="replace")
                return [body], body
            fh.seek(begin)
            first = fh.read(half).decode("utf-8", errors="replace")
            fh.seek(size - half)
            last = fh.read(half).decode("utf-8", errors="replace")
            return [first, last], last
    except OSError:
        return [], ""


def _appended(detail_file: str, mark: tuple[int, bytes], limit: int = MATCH_CHARS) -> str:
    """The text of what this run appended that is worth quoting: all of it, or its last part."""
    return _appended_parts(detail_file, mark, limit)[1]


FAILING_WORDS = ("error", "fatal", "fail", "denied", "unable", "refused", "timeout")
FAILING_STATES = ("fail", "failed", "error", "critical", "down", "degraded", "abort", "aborted")
ALERT_KEYS = ("drift_alerts", "alerts", "errors", "failures", "problems", "warnings")


def _from_json(body: str) -> str:
    """A one-line summary of a JSON detail file.

    Some jobs write their verdict to a JSON state file and leave both output
    streams empty. The findings are in the file, but the line-scanning
    heuristic in `_why` cannot see them: the last lines of a JSON document are
    mostly closing braces, and an entry like "disk: below threshold" contains
    none of the failure words. Reading the structure instead of the text makes
    such a file usable: first a non-empty list under one of ALERT_KEYS, then
    any string value that is a failing state.
    """
    try:
        doc = json.loads(body)
    except (ValueError, TypeError):
        return ""
    if not isinstance(doc, dict):
        return ""
    for key in ALERT_KEYS:
        listed = doc.get(key)
        if isinstance(listed, list) and listed:
            return f"{key}: " + "; ".join(str(x) for x in listed)
    failing: list[str] = []

    def walk(node: object, path: str) -> None:
        if isinstance(node, dict):
            for k, v in node.items():
                walk(v, f"{path}.{k}" if path else str(k))
        elif isinstance(node, str) and node.lower() in FAILING_STATES:
            failing.append(f"{path}={node}")

    walk(doc, "")
    return ("failing: " + ", ".join(failing)) if failing else ""


def _why(err: str, out: str, detail_file: str | None, detail_text: str | None = None) -> str:
    """The evidence behind a non-zero exit.

    A failure nobody can act on is the same as a failure nobody saw, so a
    reason is never left as a bare "exit 2:". Prefers stderr, then stdout, then
    whatever the job says it wrote elsewhere. When all three are silent it says
    so, because "this command failed and told nobody why" is itself the finding
    and must not read like a truncation bug.
    """
    for stream in (err, out):
        text = (stream or "").strip()
        if text:
            return text[-300:]
    if detail_file:
        try:
            path = Path(detail_file).expanduser()
            # Touch the file directly so an unreadable one is distinguishable
            # from an empty one. _tail swallows OSError on purpose, since a
            # missing log must never break pattern matching, and folding the
            # two cases together would lose the "unreadable" message that tells
            # a reader the pointer itself is wrong.
            path.stat()
            # detail_text is this run's appended slice when the caller has one.
            # Falling back to the whole tail would quote an EARLIER run's error
            # at a run that failed silently.
            body = _tail(detail_file) if detail_text is None else detail_text
            structured = _from_json(body)
            if structured:
                return f"(silent; from {path.name}) {structured[-260:]}"
            # The last line is usually a bare status; the line before it is
            # often where the tool says what actually went wrong.
            lines = [x.strip() for x in body.splitlines() if x.strip()]
            interesting = [x for x in lines[-12:]
                           if any(w in x.lower() for w in FAILING_WORDS)]
            picked = " | ".join(interesting[-2:]) or " ".join(lines[-2:])
            if picked:
                return f"(silent; from {path.name}) {picked[-260:]}"
            return (f"(silent: the command wrote nothing to stdout or stderr, and appended "
                    f"nothing to {path.name} either, so this failure carries no evidence)")
        except OSError as error:
            return f"(silent, and its detail file is unreadable: {type(error).__name__})"
    return ("(silent: the command wrote nothing to stdout or stderr and named no --detail-file, "
            "so this failure carries no evidence)")


# After a timeout the command's whole tree gets SIGTERM, this many seconds to
# exit, then SIGKILL.
KILL_GRACE_S = 5.0


def _ps() -> tuple[dict[int, str], dict[int, list[int]]]:
    """One snapshot of every process: pid to start time, and ppid to child pids.

    No command lines, which can carry secrets. Empty when ps cannot run, which
    leaves only the direct child to stop.
    """
    try:
        snap = subprocess.run(["/bin/ps", "-axo", "pid=,ppid=,lstart="], capture_output=True,
                              text=True, timeout=10, check=False).stdout
    except (OSError, subprocess.SubprocessError):
        snap = ""
    started: dict[int, str] = {}
    kids: dict[int, list[int]] = {}
    for line in snap.splitlines():
        parts = line.split(None, 2)
        if len(parts) == 3 and parts[0].isdigit() and parts[1].isdigit():
            started[int(parts[0])] = parts[2].strip()
            kids.setdefault(int(parts[1]), []).append(int(parts[0]))
    return started, kids


def _descendants(roots: list[int], started: dict[int, str],
                 kids: dict[int, list[int]]) -> dict[int, str]:
    found: dict[int, str] = {}
    pending = list(roots)
    while pending:
        pid = pending.pop()
        if pid in started and pid not in found:
            found[pid] = started[pid]
            pending.extend(kids.get(pid, ()))
    return found


def _signal(pids: list[int], sig: int) -> None:
    for pid in pids:
        try:
            os.kill(pid, sig)
        except OSError:  # already gone, or no longer ours to signal
            pass


def _alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False
    except OSError:
        return True


def _stop_tree(proc: subprocess.Popen) -> None:
    """Stop a timed-out child and everything it started: SIGTERM, KILL_GRACE_S, then SIGKILL.

    subprocess.run(timeout=) sends SIGKILL to the direct child only, so the
    child's own children outlive it. This walks the tree first.

    The child stays in the wrapper's own process group, so whatever supervises
    the wrapper keeps covering every run that does not time out; a child in its
    own session would lose that. Only pids found below the child are signalled,
    never a process group. SIGKILL goes only to pids a fresh snapshot shows
    with the start time they had at the timeout, and to their children in that
    snapshot, so a pid reused in the meantime is left alone while an orphan
    that the TERM reparented is still found.
    """
    started, kids = _ps()
    tree = _descendants([proc.pid], started, kids)
    proc.send_signal(signal.SIGTERM)
    _signal([p for p in tree if p != proc.pid], signal.SIGTERM)
    deadline = time.monotonic() + KILL_GRACE_S
    while time.monotonic() < deadline:
        proc.poll()  # reap the child, so a dead one stops counting as alive
        if proc.returncode is not None and not any(_alive(p) for p in tree if p != proc.pid):
            break
        time.sleep(0.05)
    started, kids = _ps()
    survivors = [p for p, t in tree.items() if p != proc.pid and started.get(p) == t]
    if proc.returncode is None:
        survivors.append(proc.pid)
    _signal([p for p in _descendants(survivors, started, kids) if p != proc.pid], signal.SIGKILL)
    if proc.poll() is None:
        proc.kill()
    proc.wait()


def _point_at_devnull(stream) -> None:
    """Point a stream at os.devnull.

    Python flushes the standard streams as it exits. A flush that still
    points at a closed pipe prints "Exception ignored" and can turn the
    exit code into 120, which would replace the command's own code.

    The descriptor opened for os.devnull is closed after dup2, including
    when dup2 itself fails. The stream keeps the duplicated descriptor.
    """
    fd = None
    try:
        fd = os.open(os.devnull, os.O_WRONLY)
        os.dup2(fd, stream.fileno())
    except OSError:
        pass
    finally:
        if fd is not None:
            os.close(fd)


class _PipeTolerant:
    """Forwards a stream, and drops a write the reader will not take.

    Warnings and usage text go through this so a closed pipe is skipped
    and never changes the exit code. The echo does not use it: main()
    guards that write on each stream itself.
    """

    def __init__(self, raw):
        self._raw = raw

    def write(self, text: str) -> int:
        try:
            written = self._raw.write(text)
            self._raw.flush()
        except (OSError, UnicodeEncodeError):
            _point_at_devnull(self._raw)
            return 0
        return written if isinstance(written, int) else 0

    def flush(self) -> None:
        try:
            self._raw.flush()
        except (OSError, UnicodeEncodeError):
            _point_at_devnull(self._raw)

    def fileno(self) -> int:
        return self._raw.fileno()

    def __getattr__(self, name: str):
        return getattr(self._raw, name)


def _echo(stream, raw: bytes, text: str) -> None:
    """Write one captured stream: the raw bytes, or the decoded text if there is no buffer.

    An in-process caller may have swapped in a StringIO, which has no buffer.
    A reader that closed its pipe raises OSError; point that stream at
    os.devnull and keep going. UnicodeEncodeError can only happen on the text path.
    """
    if hasattr(stream, "buffer"):
        if not raw:
            return
        try:
            stream.buffer.write(raw)
            stream.buffer.flush()
        except OSError:
            _point_at_devnull(stream)
        return
    if not text:
        return
    try:
        stream.write(text)
        stream.flush()
    except (OSError, UnicodeEncodeError):
        _point_at_devnull(stream)


def main(argv: list[str] | None = None) -> int:
    # Kept for the echo. sys.stderr itself becomes the wrapper below, so a
    # warning printed after the echo cannot raise on a closed pipe.
    # Restored on the way out, including when main() raises, so a second
    # in-process call does not nest wrappers.
    err_stream = sys.stderr
    sys.stderr = _PipeTolerant(err_stream)
    try:
        return _run(argv, err_stream)
    finally:
        sys.stderr = err_stream


def _run(argv, err_stream) -> int:
    ap = argparse.ArgumentParser(
        prog="pow-run.py",
        description="Run a command and record its proof-of-work outcome.",
    )
    ap.add_argument("--job", required=True, help="job name (ledger file and registry key)")
    ap.add_argument("--work-pattern", help="regex whose match means real work happened")
    ap.add_argument("--none-pattern", help="regex whose match means a legitimate no-op")
    ap.add_argument("--fail-pattern", help="regex whose match means degraded, even on exit 0")
    ap.add_argument("--queue-pattern", help="regex with a capture group holding the remaining backlog")
    ap.add_argument("--queue-unit", default="items")
    ap.add_argument(
        "--work-exit-codes",
        help="comma-separated exit codes that mean the job RAN AND DID WORK. For a "
             "health checker, exit 2 can mean 'I evaluated everything and found "
             "failures': the checker worked. Without this, a non-zero exit is "
             "assumed to be breakage, which would mislabel every finding as a "
             "broken job and pollute the miss rate.",
    )
    ap.add_argument("--timeout", type=int, help="seconds; a timeout is recorded as could_not")
    ap.add_argument("--detail-file", help="a file the job writes its own findings to; what the "
                                          "run appended to it is used as the failure reason "
                                          "when the command itself prints nothing")
    ap.add_argument("--quiet", action="store_true", help="do not echo the wrapped output")
    ap.add_argument("cmd", nargs=argparse.REMAINDER, help="-- then the command to run")
    args = ap.parse_args(argv)

    cmd = args.cmd[1:] if args.cmd and args.cmd[0] == "--" else args.cmd
    if not cmd:
        ap.error("no command given (use: python3 pow-run.py --job X -- your command)")

    # Captured BEFORE the child starts: see _appended.
    detail_mark = _mark(args.detail_file)
    started = time.monotonic()
    timed_out = False
    signum = None
    # Pipes stay bytes so the echo can be what the command wrote. Patterns and
    # the ledger decode below, with any byte that is not UTF-8 replaced, so a
    # stray byte still cannot cost the command's real exit code.
    raw_out = b""
    raw_err = b""
    try:
        # subprocess.run's own body, except that a timeout stops the child's whole tree.
        with subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE) as proc:
            try:
                stdout, stderr = proc.communicate(timeout=args.timeout)
            except subprocess.TimeoutExpired:
                _stop_tree(proc)
                raise
            except BaseException:
                proc.kill()
                raise
        raw_out = stdout or b""
        raw_err = stderr or b""
        code = proc.returncode
        if code < 0:
            # Killed by a signal. A shell reports that as 128 plus the number,
            # and sys.exit(-9) would show as 247.
            signum = -code
            code = 128 + signum
    except subprocess.TimeoutExpired as exc:
        timed_out = True
        raw_out = exc.stdout or b""
        raw_err = exc.stderr or b""
        code = 124
    except (OSError, ValueError) as exc:
        # No child output. stderr is the launch message encoded as UTF-8.
        raw_out = b""
        raw_err = f"pow-run: could not launch: {exc}".encode("utf-8")
        code = 127

    out = raw_out.decode("utf-8", errors="replace")
    err = raw_err.decode("utf-8", errors="replace")

    duration_ms = int((time.monotonic() - started) * 1000)

    # A reader that stops early (a pipe into head, for example) closes this
    # process's pipe, and the next write raises. That must not skip the row
    # or replace the command's exit code. Each stream is guarded on its own,
    # so a failure on stdout still echoes stderr, and the reverse. Nothing is
    # written until the command has ended: communicate() has already returned.
    # stderr goes to the saved stream, not the _PipeTolerant wrapper.
    if not args.quiet:
        _echo(sys.stdout, raw_out, out)
        _echo(err_stream, raw_err, err)

    combined = out + "\n" + err
    # A job that redirects its own output to a log file hands pow-run nothing
    # to match against, so every run would land "unclassified" however clearly
    # the script announced what it did. When the streams are silent and the job
    # named a --detail-file, what the run appended to that file IS the output.
    detail_parts, detail_text = (_appended_parts(args.detail_file, detail_mark)
                                 if args.detail_file else ([], ""))
    if not combined.strip() and detail_text:
        combined = detail_text
        parts = detail_parts
    else:
        parts = _ends(combined)

    work_m = _search(args.work_pattern, parts)
    none_m = _search(args.none_pattern, parts)
    fail_m = _search(args.fail_pattern, parts)
    queue_m = _search(args.queue_pattern, parts)

    # Precedence is deliberate: a proven failure outranks any success pattern.
    # A job that handled 4 items and then crashed is `could_not`, not
    # `did_work`, because degradation is never rounded down to success.
    work_codes = set()
    if args.work_exit_codes:
        work_codes = {int(c) for c in args.work_exit_codes.split(",") if c.strip().isdigit()}

    count = _int_group(work_m) if work_m else None

    if timed_out:
        status, reason = "could_not", f"timed out after {args.timeout}s"
    elif code in work_codes:
        status, reason = "did_work", None
    elif code != 0:
        what = f"exit {code}"
        if signum is not None:
            try:
                name = f" ({signal.Signals(signum).name})"
            except ValueError:
                name = ""
            what = f"killed by signal {signum}{name}"
        status, reason = "could_not", f"{what}: {_why(err, out, args.detail_file, detail_text)}"
    elif fail_m:
        # The matched pattern says WHICH failure; the detail file says why.
        extra = ""
        if args.detail_file:
            found = _why("", "", args.detail_file, detail_text)
            if "silent;" in found:
                extra = " -- " + found.split(")", 1)[-1].strip()
        status, reason = "could_not", f"matched --fail-pattern: {fail_m.group(0)[:160]}{extra}"
    elif work_m:
        if count == 0:
            # "copied 0 files" matched the work pattern and says nothing was
            # done. Recording work for it would be a false green.
            status, reason = "no_work", "matched --work-pattern with a count of 0"
        else:
            status, reason = "did_work", None
    elif none_m:
        status, reason = "no_work", "matched --none-pattern"
    else:
        # Exit 0, nothing recognized. Work was not OBSERVED, so none is
        # claimed. The job shows as `never reported did_work` until it is
        # given patterns or taught to call record() itself.
        status, reason = "no_work", (
            "exit 0, no work/no-work pattern configured -- liveness-only coverage"
        )

    classified = bool(work_m or none_m or fail_m or code in work_codes)

    evidence = {
        "cmd": " ".join(cmd)[:500],
        "exit_code": code,
        "classification": "exit-code" if code in work_codes else ("pattern" if classified else "unclassified"),
    }
    if signum is not None:
        evidence["signal"] = signum
    if code in work_codes and code != 0:
        # A checker that exited non-zero because it FOUND things. Keep the
        # tail: the findings are the point.
        evidence["findings_exit_code"] = code
        evidence["tail"] = combined.strip()[-TAIL_CHARS:]
    if status == "could_not" or not classified:
        evidence["tail"] = combined.strip()[-TAIL_CHARS:]

    try:
        record(
            args.job,
            status,
            did_work=(count if count is not None else 1) if status == "did_work" else 0,
            no_work=1 if status == "no_work" else 0,
            could_not=1 if status == "could_not" else 0,
            queue_depth=_int_group(queue_m),
            queue_unit=args.queue_unit,
            duration_ms=duration_ms,
            reason=reason,
            evidence=evidence,
        )
    except Exception as exc:  # noqa: BLE001  telemetry must never break the job
        print(f"pow-run: WARN could not record proof-of-work: {exc}", file=sys.stderr)

    return code


if __name__ == "__main__":
    sys.exit(main())
