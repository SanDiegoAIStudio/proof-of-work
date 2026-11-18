#!/usr/bin/env bash
# A tour of proof-of-work. Needs bash and python3, plus mktemp and sed. Everything happens in a temp folder
# that is removed at the end; nothing is written inside this repository.
#
#   bash examples/demo.sh
#
# Set PYTHON to use a specific interpreter (default: python3).

set -u

HERE="$(cd "$(dirname "$0")/.." && pwd)"
PY="${PYTHON:-python3}"

TMP_ROOT="${TMPDIR:-/tmp}"
WORK="$(mktemp -d "${TMP_ROOT%/}/pow-demo.XXXXXX")" || exit 1
trap 'rm -rf "$WORK"' EXIT

export PYTHONDONTWRITEBYTECODE=1
export POW_SPINE_DIR="$WORK/spine"
export POW_REGISTRY="$HERE/jobs.json"
export POW_ROLLUP="$WORK/spine-rollup.json"
unset POW_HEARTBEAT_DIR

# Output is shown with the temp folder's name replaced, so it reads the same on every run.
pow() { "$PY" "$HERE/pow.py" "$@" 2>&1 | sed "s|$WORK|<tmp>|g"; return "${PIPESTATUS[0]}"; }
say() { printf '\n== %s ==\n' "$1"; }

say "1. A job you cannot edit, wrapped: it printed a count, so it did work"
"$PY" "$HERE/pow-run.py" --job nightly-backup --work-pattern 'copied ([0-9]+) files' \
  -- /bin/sh -c 'echo copied 42 files'

say "2. The same job on a quiet night: it ran and found nothing to copy"
"$PY" "$HERE/pow-run.py" --job nightly-backup --none-pattern 'nothing to copy' \
  -- /bin/sh -c 'echo nothing to copy'

say "3. A job that calls record itself: it synced 7 messages, 3 still waiting"
pow record --job inbox-sync --status did_work --did-work 7 --queue-depth 3 --queue-unit messages

say "4. The same job later: it could not run, and says why"
pow record --job inbox-sync --status could_not --could-not 1 \
  --reason "mail server refused the login" --evidence '{"error": "auth failed"}'

say "5. report"
pow report

say "6. check (exit code printed on the next line)"
pow check
echo "check exit code: $?"

say "7. verify: the hash chain of every ledger file"
pow verify
echo "verify exit code: $?"

say "8. Edit one character inside one row of a ledger file"
LEDGER="$WORK/spine/inbox-sync.jsonl"
"$PY" - "$LEDGER" <<'PYEOF'
import sys

path = sys.argv[1]
with open(path, encoding="utf-8") as fh:
    lines = fh.read().split("\n")
# Change the first row's did_work count from 7 to 8.
edited = lines[0].replace('"did_work": 7', '"did_work": 8', 1)
if edited == lines[0]:
    sys.exit("demo: the row did not contain the expected text")
lines[0] = edited
with open(path, "w", encoding="utf-8") as fh:
    fh.write("\n".join(lines))
PYEOF
echo "changed 7 to 8 in line 1 of inbox-sync.jsonl"

say "9. verify again"
pow verify
echo "verify exit code: $?"

exit 0
