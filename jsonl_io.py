"""Append one JSON line to a file under a file lock.

`append_row` writes a single JSONL line and repairs a torn tail first.
`flocked` is the lock it uses (flock on `<file>.lock`), exposed so a caller can
hold the lock across a read and an append.

Standard library only. POSIX only, because the lock is fcntl.flock.
"""

from __future__ import annotations

import contextlib
import errno
import fcntl
import json
import os


class LockHeld(Exception):
    """A non-blocking flock found the lock already held."""


@contextlib.contextmanager
def flocked(path, *, blocking=True):
    """Exclusive flock on `path + ".lock"`.

    Non-blocking mode raises LockHeld instead of waiting.
    """
    lock_path = path + ".lock"
    handle = open(lock_path, "ab")
    acquired = False
    try:
        flags = fcntl.LOCK_EX
        if not blocking:
            flags |= fcntl.LOCK_NB
        try:
            fcntl.flock(handle, flags)
        except BlockingIOError as exc:
            raise LockHeld(path) from exc
        except OSError as exc:
            if not blocking and exc.errno in (errno.EAGAIN, errno.EWOULDBLOCK):
                raise LockHeld(path) from exc
            raise
        acquired = True
        yield handle
    finally:
        if acquired:
            try:
                fcntl.flock(handle, fcntl.LOCK_UN)
            except OSError:
                pass
        handle.close()


def append_row(path, obj, *, lock=True, fsync=True):
    """Append one JSON object as a single JSONL line. Returns `obj`.

    When the file is non-empty and its last byte is not a newline, a newline
    is written first so a torn tail stays on its own line. flush and, when
    requested, fsync finish before the lock is released.
    """
    parent = os.path.dirname(path)
    if parent:
        os.makedirs(parent, exist_ok=True)
    line = (json.dumps(obj, ensure_ascii=False) + "\n").encode("utf-8")

    def _write():
        with open(path, "ab+") as handle:
            handle.seek(0, os.SEEK_END)
            if handle.tell() > 0:
                handle.seek(-1, os.SEEK_END)
                if handle.read(1) != b"\n":
                    handle.write(b"\n")
            handle.write(line)
            handle.flush()
            if fsync:
                os.fsync(handle.fileno())

    if lock:
        with flocked(path):
            _write()
    else:
        _write()
    return obj
