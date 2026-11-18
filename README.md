# proof-of-work

A small ledger that lets an unattended job prove it did its work. Every run appends one row saying `did_work`, `no_work` or `could_not`, a registry says what "working" means for each job, and a check goes red when a job goes quiet, fails too often or lets its queue grow. The name is about jobs proving they worked and has nothing to do with blockchains or mining.

It is a few Python files with no dependencies beyond the standard library. Rows are appended to plain JSON Lines files, each row is linked to the one before it by a hash, and a `verify` command shows where a file was edited.

## What you get

This is the real output of `bash examples/demo.sh`. The script records a few runs for two of the three example jobs in `jobs.json` (the third, `weekly-report`, is left awaiting its first report), prints a report and a check, verifies the ledger, changes one character in one row and verifies again. It works in a temp folder and removes it at the end.

```
== 1. A job you cannot edit, wrapped: it printed a count, so it did work ==
copied 42 files

== 2. The same job on a quiet night: it ran and found nothing to copy ==
nothing to copy

== 3. A job that calls record itself: it synced 7 messages, 3 still waiting ==
pow: recorded inbox-sync status=did_work -> <tmp>/spine/inbox-sync.jsonl

== 4. The same job later: it could not run, and says why ==
pow: recorded inbox-sync status=could_not -> <tmp>/spine/inbox-sync.jsonl

== 5. report ==
=== proof-of-work: each job's declared window ===

JOB                         RUNS  WORK  NONE  FAIL  MISS    QUEUE D  LAST REAL WORK
----------------------------------------------------------------------------------------------------
inbox-sync                     2     1     0     1  50%        n/a  0.0h ago
nightly-backup                 2     1     1     0   0%        n/a  0.0h ago
weekly-report                  0     0     0     0   n/a        n/a  never reported did_work

!! INVARIANTS VIOLATED
  x inbox-sync: max_miss_rate - miss rate 50% (limit 10%)

awaiting first report (registered, has never run yet):
  - weekly-report

?  not evaluable (absence of evidence is NOT health):
  ? inbox-sync: queue_non_increasing - no queue depth reported in window
  ? nightly-backup: max_consecutive_not_work - only 2 records ever (streak 1, limit 3); too thin to judge

== 6. check (exit code printed on the next line) ==
VIOLATED inbox-sync: max_miss_rate - miss rate 50% (limit 10%)
UNKNOWN inbox-sync: queue_non_increasing - no queue depth reported in window
UNKNOWN nightly-backup: max_consecutive_not_work - only 2 records ever (streak 1, limit 3); too thin to judge
AWAITING weekly-report: registered but has never reported
check exit code: 1

== 7. verify: the hash chain of every ledger file ==
inbox-sync.jsonl: 2 rows, 0 unhashed, ok
nightly-backup.jsonl: 2 rows, 0 unhashed, ok
verify exit code: 0

== 8. Edit one character inside one row of a ledger file ==
changed 7 to 8 in line 1 of inbox-sync.jsonl

== 9. verify again ==
inbox-sync.jsonl: 2 rows, 0 unhashed, BAD at line 1: hash mismatch
nightly-backup.jsonl: 2 rows, 0 unhashed, ok
verify exit code: 1
```

## Quick start

You need Python 3.9 or newer and a POSIX system. There is nothing to install: copy the folder, or clone it.

```
bash examples/demo.sh                       # the tour above, in a temp folder
python3 pow.py record --job my-job --status did_work --did-work 3
python3 pow.py report
python3 pow.py check                        # exit 1 on a violation, 2 if incomplete
python3 pow.py verify
```

Rows go to `spine/<job>.jsonl` beside `pow.py` unless `POW_SPINE_DIR` says otherwise. A job that has no entry in `jobs.json` is still recorded and shows up in the report with a note that no invariants are being checked for it.

## The three states

A job that "ran and exited 0" has told you almost nothing. A backup can exit 0 after copying nothing, a sync can exit 0 because it could not log in and swallowed the error, and a cron entry can quietly stop firing. Each row therefore carries one of three states:

- `did_work`: the job did something real, and the row says how many items.
- `no_work`: the job ran and legitimately had nothing to do. An empty queue is a good reason for this.
- `could_not`: the job tried and could not do it. A run that handled four items and then crashed is `could_not`, because degradation is never rounded down to success.

Absence is a fourth thing that no row can record, so the check handles it: a job that has not reported inside its window is red, and a job that has never reported is listed as awaiting its first report rather than as healthy. Where an invariant cannot be evaluated (a queue limit with no queue depth reported, for example) the answer is "unknown", never "ok".

## Record from your own job

From Python, in a job you can edit:

```python
import pow

pow.record("inbox-sync", "did_work", did_work=7, queue_depth=3, queue_unit="messages")
pow.record("inbox-sync", "no_work")
pow.record("inbox-sync", "could_not", could_not=1, reason="mail server refused the login",
           evidence={"error": "auth failed"})
```

`record(job, status, *, did_work, no_work, could_not, queue_depth, queue_unit, duration_ms, evidence, reason, spine_dir, heartbeat)` appends one row and returns the ledger path. It raises `ValueError` for a status other than the three above or a job name that is empty, contains `/` or starts with a dot.

From a shell script or any other language:

```
python3 pow.py record --job inbox-sync --status did_work --did-work 7 --queue-depth 3 --queue-unit messages
python3 pow.py record --job inbox-sync --status could_not --could-not 1 \
  --reason "mail server refused the login" --evidence '{"error": "auth failed"}'
```

The CLI exits 2 for a bad job name or evidence that is not a JSON object. The other flags are `--no-work N`, `--duration-ms N` and `--no-heartbeat`.

## Wrap a job you cannot edit

`pow-run.py` runs a command, passes its output and exit code through, and records one row from what it saw. The command goes after `--`:

```
python3 pow-run.py --job nightly-backup [flags] -- /path/to/backup.sh --some-arg
```

The wrapper exits with the command's own exit code, with 124 when `--timeout` stopped it and 127 when the command could not be launched. A command that a signal killed gives 128 plus the signal number, as a shell reports it (137 for SIGKILL). Output that is not valid UTF-8 is decoded with replacement characters and never changes the exit code. The wrapper echoes the command's stdout and stderr byte for byte once the command ends, so binary output such as an archive written to stdout passes through unchanged; patterns and the ledger read it as UTF-8 with any other byte replaced. If recording the row fails, it prints a warning to stderr and still exits with the command's code. When the reader of the wrapper's output closes early (piping into `head`, for example), the run is still recorded and the wrapper still returns the command's exit code; the rest of the echo is dropped.

| Flag | Meaning | Worked example |
|---|---|---|
| `--work-pattern RE` | a match means real work happened; a capture group holding a number becomes the item count, and a count of 0 is recorded as `no_work` | `--work-pattern 'copied ([0-9]+) files'` records `did_work` with 42 items when the output says `copied 42 files`, and `no_work` when it says `copied 0 files` |
| `--none-pattern RE` | a match means a legitimate no-op | `--none-pattern 'nothing to copy'` records `no_work` with reason `matched --none-pattern` |
| `--fail-pattern RE` | a match means degraded, even on exit 0 | `--fail-pattern 'partial failure'` records `could_not` when the output says `partial failure on 2 of 9` |
| `--queue-pattern RE` | the capture group is the remaining backlog | `--queue-pattern 'remaining: ([0-9]+)' --queue-unit mails` stores a queue depth of 12 from `remaining: 12` |
| `--work-exit-codes 1,2` | these non-zero exit codes mean the job ran and found things, such as a checker that exits 1 when it finds drift | `--work-exit-codes 1` records `did_work`, keeps the output tail as evidence, and still returns exit code 1 |
| `--timeout SECONDS` | stop the command after this long | `--timeout 600` records `could_not` with reason `timed out after 600s` and exit code 124 |
| `--detail-file PATH` | a file the job writes its findings to; only what the run itself appended is read. It stands in for the output when the command printed nothing, and it is quoted in the reason when a failed command printed nothing or when `--fail-pattern` matched | `--detail-file /var/log/backup.log` quotes the new log lines as the reason for a failure |
| `--quiet` | do not echo the command's output | |

Patterns are regular expressions searched in stdout and stderr together. The order of precedence is fixed so that a proven failure always beats a success claim: a timeout, then `--work-exit-codes`, then any other non-zero exit, then `--fail-pattern`, then `--work-pattern`, then `--none-pattern`.

With no pattern at all, a run that exits 0 is recorded as `no_work` with `evidence.classification` set to `unclassified` and the reason `exit 0, no work/no-work pattern configured -- liveness-only coverage`. The wrapper never claims work it did not observe: the row proves the job ran, and `work_within_hours` stays red until you give the wrapper a pattern or teach the job to call `record` itself. When a timeout stops a command, the wrapper also stops the processes the command started, sending SIGTERM first and SIGKILL to any that ignore it.

## Say what working means

`jobs.json` is an object keyed by job name. Keys that start with `_` are notes, not jobs. Each job has a `description`, a `window_hours` (the span that rates, queue changes and item counts are computed over), an optional `tier` of `native` (the job calls `record`) or `wrapped` (`pow-run.py` records it), and a list of invariants. An invariant is its `kind`, that kind's parameters written flat beside it, and a `why` sentence saying what red means. `tier` and `why` change no result: `pow.py` and `pow-run.py` never read `tier`, and `why` is only printed by `pow.py jobs`:

```json
{
  "inbox-sync": {
    "description": "Pulls new messages into the local mailbox every 15 minutes.",
    "tier": "native",
    "window_hours": 24,
    "invariants": [
      {"kind": "max_miss_rate", "value": 0.1, "quiet_ok": true,
       "why": "Red means more than one in ten attempted runs failed."}
    ]
  }
}
```

| Kind | Parameters (default) | Turns red when |
|---|---|---|
| `seen_within_hours` | `value` (24) | the job has never reported, or its newest row is older than `value` hours |
| `work_within_hours` | `value` (24) | the job has never reported `did_work`, or its newest `did_work` row is older than `value` hours |
| `max_miss_rate` | `value` (0.1), `quiet_ok` | `could_not` divided by (`did_work` plus `could_not`) in the window is above `value`. With no attempted runs the answer is unknown, unless `quiet_ok` is set, the window holds at least one quiet run and no quiet run is unclassified |
| `max_could_not_share` | `value` (0.1) | `could_not` divided by all runs in the window is above `value`. With no runs in the window the answer is unknown. |
| `max_consecutive_not_work` | `value` (3) | the newest `value` rows, taken over the whole ledger, include no `did_work`. Unknown while the ledger has fewer than `value` rows |
| `queue_non_increasing` | `tolerance` (0) | the last queue depth in the window is more than `tolerance` above the first. Needs two observations; with fewer the answer is unknown |
| `min_items` | `value` (1), `field` (`did_work`) | the summed per-item count of `field` (`did_work`, `no_work` or `could_not`) in the window is below `value`. Any other `field` name counts as 0, so it turns red |

`quiet_ok` belongs to `max_miss_rate` only and is opt-in per job. It lets a job whose normal day is quiet stay green, but only when its quiet runs were classified (a `--none-pattern` match, or the job itself calling `record`). A no-pattern wrapper run does not count, so `quiet_ok` cannot turn "nobody checked" into "healthy". A window with no runs at all stays unknown too. Pair it with `seen_within_hours` so that a silent job is still caught.

## Read the ledger

```
python3 pow.py report [--window-hours H] [--json]   # one table, violations, awaiting, coverage gaps
python3 pow.py check  [--window-hours H] [--json]   # VIOLATED / UNKNOWN / AWAITING lines; exit 1 or 2, see below
python3 pow.py jobs                                 # registered jobs and their invariants
python3 pow.py rollup                               # write per-day counts to POW_ROLLUP
python3 pow.py verify [--job NAME] [--json]         # check each file's hash chain
```

`report` and `check` use each job's own `window_hours` unless `--window-hours` is given. A registered job with no rows is reported as awaiting its first report and does not make `check` fail, so `check` is safe to schedule on day one. A job that has rows but no registry entry is listed under coverage gaps. A job is awaiting only when its ledger holds no line at all; a ledger that holds only damaged lines is judged like any other, so its `seen_within_hours` fails.

`check` prints one `VIOLATED` line per breach, then one `UNKNOWN job: kind - detail` line for every invariant it could not judge (too few rows, no queue depth reported), then the `AWAITING` lines. When nothing is violated, one more line ends the output: `pow: no invariant violations`, or `pow: no invariants are registered; nothing was checked` when no invariant was evaluated. With `--json` it prints the results as JSON in place of all these lines. Exit codes:

| Exit | Meaning |
|---|---|
| 0 | nothing is violated and every invariant could be read. UNKNOWN and AWAITING lines do not change this |
| 1 | at least one invariant is violated |
| 2 | no violation was found but the check was incomplete: the registry file has a problem, an invariant has a parameter that cannot be used (for example `"value": "soon"`) or a kind that is not known, or `POW_REGISTRY` names a file that is not there. The reason is printed to stderr as `pow: ERROR check was incomplete` |

When both apply, 1 wins over 2. One odd line in one ledger, or one bad invariant, never stops the check for the other jobs. With `--json`, each result also carries `awaiting` (true or false), so you can tell why a job listed under `violated` did not fail the check.

`verify` prints one line per ledger file: the file name, the number of rows, how many have no hash, and either `ok` or the first bad line with its reason (`hash mismatch` or `link mismatch`). A file that cannot be read shows `BAD: could not be read (<error name>)` and counts as bad. It exits 0 when every file is ok, 1 when any file has a bad line and 2 for a bad `--job` name or a job with no file. An empty spine folder prints `pow: nothing to verify in <folder>` and exits 0. With `--json` it prints a list with one object per file:

```json
[
  {
    "rows": 1,
    "unhashed": 0,
    "ok": true,
    "first_bad_line": null,
    "problem": null,
    "file": "nightly-backup.jsonl"
  }
]
```

`rollup` exits 2 and leaves the file as it was when the existing rollup file is damaged (not JSON, or not the shape below), and when it cannot write. It writes through a temporary file in the same folder and keeps the old file's permission bits. `rollup` writes `{"v": 1, "days": {"<date>": {"<job>": {...}}}}` with runs, the three status counts, the items of real work, and the first and last queue depth for each job and day. It merges into the file that is already there, so a day that exists only in the old file is kept.

Environment variables, each read once when `pow.py` is imported:

| Variable | Meaning | Default |
|---|---|---|
| `POW_SPINE_DIR` | folder holding the `<job>.jsonl` ledger files | `spine/` beside `pow.py` |
| `POW_REGISTRY` | the registry file | `jobs.json` beside `pow.py` |
| `POW_ROLLUP` | where `rollup` writes | `spine-rollup.json` beside `pow.py` |
| `POW_HEARTBEAT_DIR` | when set and the folder exists, each `record` also touches `<dir>/<job>.last`, for tools that only watch file times (skip it with `heartbeat=False` or `--no-heartbeat`) | unset: no heartbeat files |

A ledger file is one JSON object per line. This is a real row written by the wrapper:

```
{"v": 1, "ts": "2026-11-17T11:34:21Z", "job": "nightly-backup", "status": "did_work", "counts": {"did_work": 42, "no_work": 0, "could_not": 0}, "duration_ms": 4, "evidence": {"cmd": "/bin/sh -c echo copied 42 files", "exit_code": 0, "classification": "pattern"}, "prev_row_hash": "genesis", "row_hash": "sha256:08a08cc279aaef379e0a7167939aa380f79901d31e883d67e04fa38a978f0f06"}
```

| Field | Meaning |
|---|---|
| `v` | ledger format version, 1 |
| `ts` | UTC time of the write |
| `job` | job name |
| `status` | `did_work`, `no_work` or `could_not` |
| `counts` | items done, skipped and failed in this run |
| `queue` | optional: `depth` and `unit` of the backlog |
| `duration_ms` | optional: how long the run took (set by the wrapper) |
| `reason` | optional: why, for a `could_not` or a classified `no_work` |
| `evidence` | optional: run-specific proof. The wrapper writes `cmd`, `exit_code`, `classification` (`exit-code`, `pattern` or `unclassified`), `findings_exit_code` when a non-zero exit code listed in `--work-exit-codes` counted as work, `signal` (its number) when a signal stopped the command, and a `tail` for failures, unclassified runs and those non-zero work runs: the last 4000 characters of the output it matched against (see Limits) |
| `prev_row_hash` | `genesis` for the first row, the previous row's `row_hash`, or `legacy` after a line with no usable hash |
| `row_hash` | `sha256:` and the hex digest of the row's sorted-key compact JSON (without the two chain fields), a newline, and `prev_row_hash` |

Rows are appended under an exclusive file lock (a `<job>.jsonl.lock` file beside the ledger) held for the read of the previous hash and the write, so concurrent writers on one machine produce one straight chain.

## Run the check on a schedule

One cron line that runs the check at the top of every hour and appends what it says to a log:

```
0 * * * * cd /path/to/proof-of-work && /usr/bin/python3 pow.py check >> check.log 2>&1
```

The check prints one `VIOLATED` line per breach and exits 1, so the same command also works as a gate in any scheduler that looks at exit codes. The log is only as good as the habit of reading it: for a check that must reach you, send the exit code or the log line to whatever you already watch.

## Tests

```
python3 -m unittest discover -s tests     # 314 tests, about 25 seconds
python3 tests/mutate.py                   # 77 mutations, about 15 minutes
```

All tests use the standard library only and work in temp folders; a run writes no ledger into the repo folder, and a `spine/` folder that is already there is left as it was. They cover the library, the hash chain (including four processes writing 25 rows each), the wrapper (exit codes, every pattern, timeouts that leave no child running, detail files), the shipped `jobs.json` and `examples/demo.sh`.

`tests/mutate.py` shows that the tests can fail. For each of 77 deliberate breakages (a lock removed, a comparison turned around, a check that no longer checks, a wrapper that returns 0 for a failing command) it copies the repo to a temp folder, applies the one change, confirms the copy differs, runs the whole suite there and prints `killed` or `survived`. It first runs the suite on an unmodified copy. It exits 1 if any mutation survived, and 2 when the unmodified suite fails or does not finish in time, or when a mutation's text does not appear exactly once in its file. A surviving mutation would mean a missing test.

## Limits

- POSIX only. Locking uses `fcntl.flock` and the wrapper's process cleanup uses `ps`.
- One machine. The lock is a local file lock; locks on a network drive were not tested, so do not point several machines at one spine folder.
- The tests were run on macOS (Darwin 27.2.0) with Python 3.9.6, and on nothing else. Other systems and Python versions are untested.
- The wrapper holds a command's whole output in memory, and nothing is echoed until the command ends. For output longer than `MATCH_CHARS` = 200000 characters it matches patterns against the first 100000 and the last 100000 characters of stdout and stderr joined; a marker in the middle is not seen. The stored `tail` is the last 4000 characters of the output.
- The ledger keeps what a run showed, in plain text. The wrapper stores the command line (cut to 500 characters), up to 300 characters of the command's stderr or stdout in the reason of a failure, up to 260 characters quoted from a `--detail-file` in the reason, the text a `--fail-pattern` matched (cut to 160 characters), and the `tail` described above. A password or key passed as an argument, or printed by a job, ends up in a ledger file. Hand such values to a job through its environment or a file, and give the spine folder the same care as the job's own logs.
- The chain shows an edit or a removal inside a file: a changed row, a deleted row or reordered rows all leave a bad line. It cannot show rows cut from the end of a file, or a file rewritten from scratch with new hashes; both leave a chain that is consistent with itself. Keep a copy of the newest `row_hash` somewhere else if that matters to you.
- A line that is not valid JSON, or a row with neither chain field, is counted as `unhashed` and is not a failure. This is on purpose: a crash in the middle of a write can leave such a line, and the ledger has to keep working. In a file that only this tool wrote and that never lost a write, the unhashed count is 0. An unhashed line that takes the place of a hashed row, or is put between two hashed rows, makes the next row a bad line (link mismatch). A row the tool itself appends after an unhashed line carries `legacy` as its link and verifies. A last row replaced by a line with no hash leaves no bad line, so an unhashed count you cannot explain deserves a look.
- Nothing is ever pruned. Ledger files only grow. `rollup` keeps daily counts, not rows, so it is a summary and not a replacement for the files. When you trim a ledger by hand, remove whole days: `rollup` recomputes each day it still sees from the rows that are left, so a day with some of its rows removed gets smaller counts.
- The wrapper waits until the command's output pipes close, not only until the command exits. A job that starts a background process which keeps stdout or stderr open holds the wrapper until that process ends, and with `--timeout` the run is recorded `could_not` with exit 124 even though the command itself exited 0. Send a background process's output elsewhere inside the job, for example `sleep 60 >/dev/null 2>&1 &`.
- A writer waits for a ledger's lock with no time limit. A process that hangs while it holds the lock blocks the other writers of that job, including a wrapper whose command has already ended.
- `check` cannot tell a job that never reported from one whose ledger file was deleted or emptied: both read as awaiting and do not fail the check.
- When a ledger is read for a time window, a row whose `ts` is missing or cannot be read is left out of the window. A full read keeps it.
- A row whose `ts` is later than the clock counts as fresh until that time comes, so a writer with a clock set ahead keeps `seen_within_hours` and `work_within_hours` green.
- A `--detail-file` whose size and last bytes are unchanged after the run but whose modification time moved is read as rewritten, and the whole file becomes the evidence. A job that only touches an append-only log on a run with no work therefore has the whole log searched, earlier runs' lines included.

## License

MIT. See `LICENSE`.

## Built at San Diego AI Studio

This ledger comes out of San Diego AI Studio, the builders' community in San Diego where builders show the systems they run. More projects from the community: https://sandiegoaistudio.com/projects
