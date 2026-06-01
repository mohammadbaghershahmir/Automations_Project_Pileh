"""File-based lock per (job_id, pair_index) for unit repair operations."""

from __future__ import annotations

import os
import time
from contextlib import contextmanager
from typing import Iterator

from webapp.job_files import pair_dir

LOCK_TTL_SECONDS = 3600


def _lock_path(job_id: str, pair_index: int) -> str:
    return os.path.join(pair_dir(job_id, pair_index), ".unit_repair.lock")


def _pid_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False
    except OSError:
        return False


def _read_lock(path: str) -> tuple[float | None, int | None]:
    try:
        raw = open(path, encoding="utf-8").read().strip()
    except OSError:
        return None, None
    if not raw:
        return None, None
    if ":" in raw:
        ts_s, pid_s = raw.split(":", 1)
        try:
            return float(ts_s), int(pid_s)
        except ValueError:
            return None, None
    try:
        return float(raw), None
    except ValueError:
        return None, None


def _clear_lock(path: str) -> None:
    try:
        if os.path.isfile(path):
            os.remove(path)
    except OSError:
        pass


def is_locked(job_id: str, pair_index: int) -> bool:
    path = _lock_path(job_id, pair_index)
    if not os.path.isfile(path):
        return False
    ts, pid = _read_lock(path)
    if pid is not None and not _pid_alive(pid):
        _clear_lock(path)
        return False
    if ts is not None and time.time() - ts > LOCK_TTL_SECONDS:
        _clear_lock(path)
        return False
    if ts is None:
        try:
            mtime = os.path.getmtime(path)
            if time.time() - mtime > LOCK_TTL_SECONDS:
                _clear_lock(path)
                return False
        except OSError:
            _clear_lock(path)
            return False
    return True


@contextmanager
def pair_repair_lock(job_id: str, pair_index: int) -> Iterator[None]:
    path = _lock_path(job_id, pair_index)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    if is_locked(job_id, pair_index):
        raise RuntimeError("Unit repair already in progress for this pair")
    try:
        with open(path, "w", encoding="utf-8") as f:
            f.write(f"{time.time()}:{os.getpid()}")
        yield
    finally:
        _clear_lock(path)
