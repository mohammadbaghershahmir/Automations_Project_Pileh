"""Celery task entrypoints for Stage V (thin wrappers around webapp.tasks_stage_v)."""

from __future__ import annotations

from typing import List, Optional

from webapp.celery_app import celery_app
from webapp.tasks_stage_v import run_full_pipeline_job, run_step1_job, run_step2_job
from webapp.unit_repair.tasks import run_regenerate_unit_task, run_renumber_pair_task


@celery_app.task(name="webapp.run_step1_job")
def run_step1_task(job_id: str, pair_indices: Optional[List[int]] = None) -> None:
    run_step1_job(job_id, pair_indices)


@celery_app.task(name="webapp.run_step2_job")
def run_step2_task(job_id: str, pair_indices: Optional[List[int]] = None) -> None:
    run_step2_job(job_id, pair_indices)


@celery_app.task(name="webapp.run_full_pipeline_job")
def run_full_pipeline_task(job_id: str, pair_indices: Optional[List[int]] = None) -> None:
    run_full_pipeline_job(job_id, pair_indices)


@celery_app.task(name="webapp.run_regenerate_unit")
def regenerate_unit_task(job_id: str, pair_index: int, unit_index: int) -> None:
    run_regenerate_unit_task(job_id, pair_index, unit_index)


@celery_app.task(name="webapp.run_renumber_pair")
def renumber_pair_task(job_id: str, pair_index: int) -> None:
    run_renumber_pair_task(job_id, pair_index)


@celery_app.task(name="webapp.run_voice_class_merge_only")
def run_voice_class_merge_only_task(job_id: str, pair_index: int) -> None:
    from webapp.tasks_voice_class import run_voice_class_merge_only

    run_voice_class_merge_only(job_id, pair_index, source="celery_worker")
