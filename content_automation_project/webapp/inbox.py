"""Inbox notifications for job owners (written by Celery workers, read by web UI)."""

from __future__ import annotations

import json
from datetime import datetime
from typing import List, Optional

from sqlalchemy.orm import Session

from webapp.job_runner_common import SINGLE_STAGE_JOB_TYPES
from webapp.models import InboxNotification, Job, JobPair


def job_display_name(job: Job) -> str:
    cfg = json.loads(job.config_json or "{}")
    return (cfg.get("display_name") or "Test Bank job").strip() or "Job"


def _add(db: Session, user_id: int, job_id: str, kind: str, title: str, body: Optional[str] = None) -> None:
    db.add(
        InboxNotification(
            user_id=user_id,
            job_id=job_id,
            kind=kind,
            title=title[:500],
            body=body,
        )
    )
    db.commit()


def notify_step1_finished(db: Session, job: Job, pairs: List[JobPair]) -> None:
    name = job_display_name(job)
    if any(p.step1_status == "failed" for p in pairs):
        body = (job.error_summary or "").strip() or None
        _add(
            db,
            job.created_by_id,
            job.id,
            "step1_fail",
            f"{name}: Step 1 finished with errors",
            body,
        )
    else:
        body_ok = (
            "Outputs are available on the job page."
            if (job.type or "").strip() in SINGLE_STAGE_JOB_TYPES
            else "You can review outputs and run Step 2 when ready."
        )
        _add(
            db,
            job.created_by_id,
            job.id,
            "step1_ok",
            f"{name}: Step 1 succeeded",
            body_ok,
        )


def notify_step2_finished(db: Session, job: Job) -> None:
    name = job_display_name(job)
    if job.status == "succeeded":
        _add(
            db,
            job.created_by_id,
            job.id,
            "step2_ok",
            f"{name}: Step 2 succeeded",
            "Outputs are available on the job page.",
        )
    else:
        body = (job.error_summary or "").strip() or None
        _add(
            db,
            job.created_by_id,
            job.id,
            "step2_fail",
            f"{name}: Step 2 job failed or incomplete",
            body,
        )


def notify_job_crash(db: Session, job: Job, phase: str, message: str) -> None:
    name = job_display_name(job)
    _add(
        db,
        job.created_by_id,
        job.id,
        "job_crash",
        f"{name}: {phase} crashed",
        message[:4000] if message else None,
    )
