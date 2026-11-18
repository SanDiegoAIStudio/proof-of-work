"""A hash chain for append-only JSON Lines files.

Every row carries two extra fields:

    row_hash       sha256 of the row's content plus its own prev_row_hash
    prev_row_hash  the row_hash of the row before it

so that editing a row, removing a row or reordering rows inside a file leaves
a visible break. The first row of a file links to the word "genesis". A row
written after a line that has no usable hash (a row from an older writer, or a
line that is not valid JSON) links to the word "legacy", which marks the
chain as restarting there instead of pretending it continued.

What a chain cannot show: rows cut from the end of a file, or a whole file
rewritten from scratch with fresh hashes. Both leave a chain that is
consistent with itself.

Standard library only.
"""

from __future__ import annotations

import hashlib
import json
import os
from typing import Any

GENESIS = "genesis"
LEGACY = "legacy"

_CHAIN_FIELDS = frozenset(("row_hash", "prev_row_hash"))
_READ_BLOCK = 4096


def _canon_json(body: Any) -> str:
    return json.dumps(body, sort_keys=True, ensure_ascii=False, separators=(",", ":"))


def canonical_hash(obj: dict) -> str:
    """Return "sha256:" plus the hex digest of the row's canonical form.

    The canonical form is the row without row_hash and prev_row_hash, as JSON
    with sorted keys, compact separators and non-ASCII characters kept. When
    the row has a prev_row_hash, a newline and that literal value follow, so a
    row whose link was changed no longer matches its own hash.
    """
    body = {k: v for k, v in obj.items() if k not in _CHAIN_FIELDS}
    blob = _canon_json(body)
    if "prev_row_hash" in obj:
        prev = obj["prev_row_hash"]
        blob = blob + "\n" + (prev if isinstance(prev, str) else _canon_json(prev))
    return "sha256:" + hashlib.sha256(blob.encode("utf-8")).hexdigest()


def _last_line_bytes(path: str):
    """The last non-blank line of `path` as bytes, or None.

    The line is returned whether or not it ends in a newline: a torn tail is a
    line like any other, because the next append puts a newline after it.
    Reads backwards in blocks, so the cost does not grow with the file.
    """
    try:
        fh = open(path, "rb")
    except FileNotFoundError:
        return None
    with fh:
        fh.seek(0, os.SEEK_END)
        pos = fh.tell()
        buf = b""
        while pos > 0:
            step = min(_READ_BLOCK, pos)
            pos -= step
            fh.seek(pos)
            buf = fh.read(step) + buf
            parts = buf.split(b"\n")
            # While more of the file is unread, parts[0] may be a partial line.
            complete = parts[1:] if pos > 0 else parts
            for line in reversed(complete):
                if line.strip():
                    return line
        return None


def _parse_row(raw: bytes):
    """The row as a dict, or None when the bytes are not a JSON object."""
    try:
        obj = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, ValueError):
        return None
    return obj if isinstance(obj, dict) else None


def _usable_hash(row: dict):
    digest = row.get("row_hash")
    if isinstance(digest, str) and digest:
        return digest
    return None


def _prev_from_row(last: dict) -> str:
    """The prev_row_hash a new row gets after `last`."""
    return _usable_hash(last) or LEGACY


def chain_fields(path: str, obj: dict) -> dict:
    """Return a new dict with prev_row_hash and row_hash set. `obj` is not changed.

    prev_row_hash is "genesis" when the file is missing, empty or only blank
    lines, the last row's row_hash when that row has one, and "legacy" when
    the last line has no hash or is not valid JSON.

    The row is passed through JSON first, so the hash is computed over exactly
    what a reader will parse back (tuples become lists, keys become strings).
    """
    raw = _last_line_bytes(path)
    if raw is None:
        prev = GENESIS
    else:
        last = _parse_row(raw)
        prev = LEGACY if last is None else _prev_from_row(last)
    stamped = json.loads(json.dumps(obj, ensure_ascii=False))
    stamped["prev_row_hash"] = prev
    stamped["row_hash"] = canonical_hash(stamped)
    return stamped


def verify_file(path: str) -> dict:
    """Check every line of a ledger file against the chain rules.

    Returns a dict with:
        rows            non-blank lines in the file
        unhashed        lines with no row_hash (counted, not a failure alone)
        ok              True when no line failed
        first_bad_line  1-based physical line number of the first failure, or None
        problem         None, "hash mismatch" or "link mismatch"

    A line fails with "hash mismatch" when its row_hash is not the canonical
    hash of the row, or when it has a prev_row_hash but no row_hash. It fails
    with "link mismatch" when its prev_row_hash is not what the line before it
    implies: "genesis" for the first line, the previous line's row_hash, or
    "legacy" after a line with no hash or a line that is not a JSON object. A
    line with no prev_row_hash is not link-checked.
    """
    rows = 0
    unhashed = 0
    first_bad_line = None
    problem = None
    expected = GENESIS

    with open(path, "rb") as fh:
        for number, raw in enumerate(fh, start=1):
            if not raw.strip():
                continue
            rows += 1
            row = _parse_row(raw)
            if row is None:
                unhashed += 1
                expected = LEGACY
                continue

            bad = None
            if "row_hash" in row:
                if row["row_hash"] != canonical_hash(row):
                    bad = "hash mismatch"
            else:
                unhashed += 1
                if "prev_row_hash" in row:
                    # This writer always sets both fields, so a row with a
                    # link and no hash had its hash removed.
                    bad = "hash mismatch"
            if bad is None and "prev_row_hash" in row and row["prev_row_hash"] != expected:
                bad = "link mismatch"
            if bad is not None and first_bad_line is None:
                first_bad_line = number
                problem = bad

            expected = _usable_hash(row) or LEGACY

    return {
        "rows": rows,
        "unhashed": unhashed,
        "ok": first_bad_line is None,
        "first_bad_line": first_bad_line,
        "problem": problem,
    }
