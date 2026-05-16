"""Shared cancel/stop helpers for Test Bank and single-stage web job runners."""

from __future__ import annotations

from datetime import datetime
from typing import List, Optional

from sqlalchemy.orm import Session

from webapp.database import SessionLocal
from webapp.job_files import append_log
from webapp.models import Job, JobPair


class JobCancelled(Exception):
    """Raised when the user requested stop (`Job.cancel_requested`)."""


# Jobs whose pipeline ends after one worker phase in this app, or use a single Run button:
# no legacy combined Step1+Step2 on one job (except test_bank uses full_pipeline separately).
# test_bank_1: Step 1 only — job must show/list as succeeded when all pairs finish Step 1 (step2 stays pending).
SINGLE_STAGE_JOB_TYPES = frozenset(
    {
        "pre_ocr_topic",
        "ocr_extraction",
        "document_processing",
        "image_notes",
        "table_notes",
        "image_file_catalog",
        "importance_type",
        "flashcard",
        "chapter_summary",
        "test_bank_1",
        "test_bank_2",
    }
)


def _scalar_cancel_requested(db: Session, job_id: str) -> bool:
    v = db.query(Job.cancel_requested).filter(Job.id == job_id).scalar()
    return bool(v)


def _cancel_check_session(job_id: str):
    """Fresh SQLite read each call (safe with API commits and with Step 2 thread pool workers)."""

    def check() -> bool:
        s = SessionLocal()
        try:
            return _scalar_cancel_requested(s, job_id)
        finally:
            s.close()

    return check


def _finalize_step1_cancelled(db: Session, job_id: str, pairs: List[JobPair]) -> None:
    append_log(db, job_id, "--- Step 1 stopped by user ---", None)
    for p in pairs:
        if p.step1_status == "running":
            p.step1_status = "failed"
            p.step1_error = "Cancelled by user"
    job = db.query(Job).filter(Job.id == job_id).one()
    job.status = "cancelled"
    job.error_summary = "Stopped by user"
    job.cancel_requested = False
    job.finished_at = datetime.utcnow()
    db.commit()


def _finalize_step2_cancelled(db: Session, job_id: str, pairs: List[JobPair]) -> None:
    append_log(db, job_id, "--- Step 2 stopped by user ---", None)
    for p in pairs:
        if p.step2_status == "running":
            p.step2_status = "failed"
            p.step2_error = "Cancelled by user"
    job = db.query(Job).filter(Job.id == job_id).one()
    job.status = "cancelled"
    job.error_summary = "Stopped by user"
    job.cancel_requested = False
    job.finished_at = datetime.utcnow()
    db.commit()
