"""Background runners for unit regenerate / renumber."""

from __future__ import annotations

import logging

from webapp.database import SessionLocal
from webapp.job_files import append_log
from webapp.models import Job
from webapp.unit_repair.lock import is_locked, pair_repair_lock
from webapp.unit_repair.service import run_regenerate_unit, run_renumber_pair

logger = logging.getLogger(__name__)


def run_regenerate_unit_task(job_id: str, pair_index: int, unit_index: int) -> None:
    db = SessionLocal()
    try:
        job = db.query(Job).filter(Job.id == job_id).one_or_none()
        if not job:
            return
        if job.status == "running":
            append_log(
                db,
                job_id,
                f"Cannot regenerate unit {unit_index}: job is still running.",
                pair_index,
            )
            return
        run_regenerate_unit(db, job_id, pair_index, unit_index)
    except Exception as e:
        logger.exception("run_regenerate_unit_task")
        try:
            append_log(db, job_id, f"Regenerate unit {unit_index} failed: {e}", pair_index)
        except Exception:
            pass
    finally:
        db.close()


def run_renumber_pair_task(job_id: str, pair_index: int) -> None:
    db = SessionLocal()
    try:
        job = db.query(Job).filter(Job.id == job_id).one_or_none()
        if not job:
            return
        if job.status == "running":
            append_log(db, job_id, "Cannot renumber: job is still running.", pair_index)
            return
        run_renumber_pair(db, job_id, pair_index)
    except Exception as e:
        logger.exception("run_renumber_pair_task")
        try:
            append_log(db, job_id, f"Renumber failed: {e}", pair_index)
        except Exception:
            pass
    finally:
        db.close()


def pair_repair_busy(job_id: str, pair_index: int) -> bool:
    return is_locked(job_id, pair_index)
