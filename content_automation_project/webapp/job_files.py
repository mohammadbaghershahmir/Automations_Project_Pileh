"""Filesystem layout under JOBS_ROOT / job_id."""

from __future__ import annotations

import hashlib
import os
from typing import Iterable, List, Optional

from sqlalchemy import func
from sqlalchemy.orm import Session

from webapp.config import JOBS_ROOT
from webapp.models import Artifact, JobLogLine


def job_root(job_id: str) -> str:
    return os.path.join(JOBS_ROOT, job_id)


def pair_dir(job_id: str, pair_index: int) -> str:
    return os.path.join(job_root(job_id), f"pair_{pair_index}")


def pair_inputs(job_id: str, pair_index: int) -> str:
    return os.path.join(pair_dir(job_id, pair_index), "inputs")


def pair_output(job_id: str, pair_index: int) -> str:
    return os.path.join(pair_dir(job_id, pair_index), "output")


def ensure_dirs(job_id: str, pair_index: int) -> None:
    os.makedirs(pair_inputs(job_id, pair_index), exist_ok=True)
    os.makedirs(pair_output(job_id, pair_index), exist_ok=True)


def next_log_seq(db: Session, job_id: str) -> int:
    m = db.query(func.max(JobLogLine.seq)).filter(JobLogLine.job_id == job_id).scalar()
    return (m or 0) + 1


def append_log(db: Session, job_id: str, line: str, pair_index: Optional[int] = None) -> None:
    seq = next_log_seq(db, job_id)
    db.add(JobLogLine(job_id=job_id, seq=seq, line=line, pair_index=pair_index))
    db.commit()


def append_logs_bulk(db: Session, job_id: str, lines: Iterable[str], pair_index: Optional[int] = None) -> None:
    seq_start = next_log_seq(db, job_id)
    for i, line in enumerate(lines):
        db.add(JobLogLine(job_id=job_id, seq=seq_start + i, line=line, pair_index=pair_index))
    db.commit()


def _sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def register_artifacts_under(
    db: Session,
    job_id: str,
    pair_index: Optional[int],
    base_job_root: str,
    relative_dir: str,
    role_guess: str = "output",
) -> None:
    """Register every file under base_job_root/relative_dir as artifacts (skip duplicates by rel_path)."""
    full_dir = os.path.join(base_job_root, relative_dir)
    if not os.path.isdir(full_dir):
        return

    existing = {a.rel_path for a in db.query(Artifact).filter(Artifact.job_id == job_id).all()}

    for root, _, files in os.walk(full_dir):
        for fn in files:
            abs_path = os.path.join(root, fn)
            rel_path = os.path.relpath(abs_path, base_job_root).replace("\\", "/")
            if rel_path in existing:
                continue
            try:
                size = os.path.getsize(abs_path)
            except OSError:
                continue
            sha = _sha256_file(abs_path)
            role = role_guess
            low = rel_path.lower()
            fn_low = fn.lower()
            if "step1_combined" in low:
                role = "step1_combined"
            elif "stage_v_step2" in low or "_stage_v_step2_" in low:
                role = "step2_topic"
            elif "step2_failed_topics" in low and low.endswith(".json"):
                role = "step2_failed_topics"
            elif fn_low.startswith("b") and fn_low.endswith(".json") and "+" in fn:
                role = "final_b_json"
            elif low.endswith(".txt"):
                role = "txt_dump"

            db.add(
                Artifact(
                    job_id=job_id,
                    pair_index=pair_index,
                    rel_path=rel_path.replace("\\", "/"),
                    role=role,
                    byte_size=size,
                    sha256=sha,
                )
            )
            existing.add(rel_path)
    db.commit()


def register_input_artifact(
    db: Session,
    job_id: str,
    pair_index: int,
    base_job_root: str,
    rel_path: str,
    role: str,
) -> None:
    rel_norm = rel_path.replace("\\", "/")
    exists = (
        db.query(Artifact.id)
        .filter(Artifact.job_id == job_id, Artifact.rel_path == rel_norm)
        .first()
    )
    if exists:
        return
    abs_path = os.path.join(base_job_root, rel_path)
    if not os.path.isfile(abs_path):
        return
    size = os.path.getsize(abs_path)
    sha = _sha256_file(abs_path)
    db.add(
        Artifact(
            job_id=job_id,
            pair_index=pair_index,
            rel_path=rel_norm,
            role=role,
            byte_size=size,
            sha256=sha,
        )
    )
    db.commit()


def list_word_basenames_for_job(job_id: str) -> List[str]:
    """All .doc/.docx basenames under any pair's inputs/ (for pairing repair UI)."""
    base = job_root(job_id)
    names: set[str] = set()
    if not os.path.isdir(base):
        return []
    for root, _, files in os.walk(base):
        norm = root.replace("\\", "/")
        if "/inputs" not in norm and not norm.endswith("inputs"):
            continue
        for fn in files:
            low = fn.lower()
            if low.endswith(".docx") or low.endswith(".doc"):
                names.add(fn)
    return sorted(names, key=lambda s: s.lower())


def find_word_file_abs_for_basename(job_id: str, basename: str) -> Optional[str]:
    """Absolute path to a Word file under job with this basename (first match under */inputs/)."""
    base = job_root(job_id)
    target = os.path.basename(basename.replace("\\", "/"))
    if not os.path.isdir(base):
        return None
    for root, _, files in os.walk(base):
        norm = root.replace("\\", "/")
        if "/inputs" not in norm and not norm.endswith("inputs"):
            continue
        for fn in files:
            if fn == target:
                return os.path.join(root, fn)
    return None
