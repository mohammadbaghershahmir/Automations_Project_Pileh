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


def is_locked(job_id: str, pair_index: int) -> bool:
    path = _lock_path(job_id, pair_index)
    if not os.path.isfile(path):
        return False
    try:
        mtime = os.path.getmtime(path)
        if time.time() - mtime > LOCK_TTL_SECONDS:
            os.remove(path)
            return False
    except OSError:
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
            f.write(str(time.time()))
        yield
    finally:
        try:
            if os.path.isfile(path):
                os.remove(path)
        except OSError:
            pass
