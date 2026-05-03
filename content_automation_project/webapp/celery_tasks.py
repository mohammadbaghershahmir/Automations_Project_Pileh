"""Celery task entrypoints for Stage V (thin wrappers around webapp.tasks_stage_v)."""

from __future__ import annotations

from typing import List, Optional

from webapp.celery_app import celery_app
from webapp.tasks_stage_v import run_full_pipeline_job, run_step1_job, run_step2_job


@celery_app.task(name="webapp.run_step1_job")
def run_step1_task(job_id: str, pair_indices: Optional[List[int]] = None) -> None:
    run_step1_job(job_id, pair_indices)


@celery_app.task(name="webapp.run_step2_job")
def run_step2_task(job_id: str, pair_indices: Optional[List[int]] = None) -> None:
    run_step2_job(job_id, pair_indices)


@celery_app.task(name="webapp.run_full_pipeline_job")
def run_full_pipeline_task(job_id: str, pair_indices: Optional[List[int]] = None) -> None:
    run_full_pipeline_job(job_id, pair_indices)
