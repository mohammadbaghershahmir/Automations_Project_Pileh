"""
Stage V Step 1 / Step 2 for Test Bank jobs (invoked by Celery workers or in-process threads).

Worker: `celery -A webapp.celery_app worker --loglevel=info` (see webapp/run_worker.py).
"""

from __future__ import annotations

import json
import logging
import os
import queue
import sys
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import List, Optional

from dotenv import load_dotenv

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

load_dotenv(_ROOT / ".env")

from sqlalchemy.orm import Session

from webapp.config import DEFAULT_TEST_BANK_MODEL, DEFAULT_TEST_BANK_PROVIDER
from webapp.database import SessionLocal
from webapp.job_files import (
    append_log,
    append_logs_bulk,
    job_root,
    pair_output,
    register_artifacts_under,
)
from webapp.default_prompts import get_default_step1_prompt, get_default_step2_prompt
from webapp.inbox import notify_job_crash, notify_step1_finished, notify_step2_finished
from webapp.models import Job, JobPair
from webapp.job_runner_common import (
    JobCancelled,
    SINGLE_STAGE_JOB_TYPES,
    _cancel_check_session,
    _finalize_step1_cancelled,
    _finalize_step2_cancelled,
    _scalar_cancel_requested,
)
from webapp.processor_context import build_stage_v_processor
from webapp.prompt_capture import wrap_prompt_capture

from openrouter_api_client import OpenRouterRequestAborted

logger = logging.getLogger(__name__)


def _flush_log_buf(db: Session, job_id: str, buf: List[str], pair_index: Optional[int]) -> None:
    if not buf:
        return
    append_logs_bulk(db, job_id, buf, pair_index=pair_index)
    buf.clear()


def run_step1_job(job_id: str, pair_indices: Optional[List[int]] = None) -> None:
    db = SessionLocal()
    try:
        job = db.query(Job).filter(Job.id == job_id).one_or_none()
        if not job:
            logger.error("Job not found: %s", job_id)
            return

        jt = (job.type or "test_bank").strip()
        if jt == "pre_ocr_topic":
            db.close()
            from webapp.tasks_single_stage import run_pre_ocr_topic_step1_job

            run_pre_ocr_topic_step1_job(job_id, pair_indices)
            return
        if jt == "ocr_extraction":
            db.close()
            from webapp.tasks_single_stage import run_ocr_extraction_step1_job

            run_ocr_extraction_step1_job(job_id, pair_indices)
            return
        if jt == "document_processing":
            db.close()
            from webapp.tasks_single_stage import run_document_processing_step1_job

            run_document_processing_step1_job(job_id, pair_indices)
            return
        if jt == "image_notes":
            db.close()
            from webapp.tasks_single_stage import run_image_notes_step1_job

            run_image_notes_step1_job(job_id, pair_indices)
            return
        if jt == "table_notes":
            db.close()
            from webapp.tasks_single_stage import run_table_notes_step1_job

            run_table_notes_step1_job(job_id, pair_indices)
            return
        if jt == "image_file_catalog":
            db.close()
            from webapp.tasks_single_stage import run_image_file_catalog_step1_job

            run_image_file_catalog_step1_job(job_id, pair_indices)
            return

        cfg = json.loads(job.config_json or "{}")
        prompt_1 = (cfg.get("prompt_1") or "").strip() or get_default_step1_prompt()
        model_1 = cfg.get("model_1", DEFAULT_TEST_BANK_MODEL)
        provider_1 = cfg.get("provider_1", DEFAULT_TEST_BANK_PROVIDER)
        delay_seconds = float(cfg.get("delay_seconds", 5))

        job.status = "running"
        if not job.started_at:
            job.started_at = datetime.utcnow()
        db.commit()
        append_log(
            db,
            job_id,
            "Step 1 runner started (log lines below confirm the worker/thread is running; OpenRouter is called after Word + Stage J load).",
            None,
        )

        pairs = (
            db.query(JobPair)
            .filter(JobPair.job_id == job_id)
            .order_by(JobPair.pair_index)
            .all()
        )
        if pair_indices is not None:
            wanted = set(pair_indices)
            pairs = [p for p in pairs if p.pair_index in wanted]

        db.refresh(job)
        if job.cancel_requested:
            _finalize_step1_cancelled(db, job_id, pairs)
            return

        client, ssm, processor = build_stage_v_processor()
        jt = (job.type or "test_bank").strip()
        base = job_root(job_id)
        cancel_check = _cancel_check_session(job_id)

        for i, pair in enumerate(pairs):
            if _scalar_cancel_requested(db, job_id):
                _finalize_step1_cancelled(db, job_id, pairs)
                return
            if not pair.word_relpath:
                pair.step1_status = "failed"
                pair.step1_error = "No Word file paired"
                db.commit()
                append_log(db, job_id, f"pair {pair.pair_index}: skipped (no Word file)", pair.pair_index)
                continue

            pair.step1_status = "running"
            pair.step1_error = None
            db.commit()

            abs_j = os.path.join(base, pair.stage_j_relpath.replace("/", os.sep))
            abs_w = os.path.join(base, pair.word_relpath.replace("/", os.sep))
            out_dir = pair_output(job_id, pair.pair_index)

            def progress(msg: str) -> None:
                append_log(db, job_id, msg, pair.pair_index)
                if _scalar_cancel_requested(db, job_id):
                    raise JobCancelled()

            try:
                append_log(db, job_id, f"--- Step 1 start pair {pair.pair_index} ---", pair.pair_index)
                processor.api_client = wrap_prompt_capture(
                    client, db, job_id, pair.pair_index, jt, "step1"
                )
                try:
                    result = processor.process_stage_v_step1(
                        stage_j_path=abs_j,
                        word_file_path=abs_w,
                        prompt_1=prompt_1,
                        model_name_1=model_1,
                        provider_1=provider_1,
                        stage_settings_manager=ssm,
                        output_dir=out_dir,
                        progress_callback=progress,
                        cancel_check=cancel_check,
                    )
                except (JobCancelled, OpenRouterRequestAborted):
                    _finalize_step1_cancelled(db, job_id, pairs)
                    return
                if result:
                    pair.step1_status = "succeeded"
                    register_artifacts_under(db, job_id, pair.pair_index, base, os.path.relpath(out_dir, base))
                else:
                    pair.step1_status = "failed"
                    pair.step1_error = "Step 1 returned no output"
                    append_log(db, job_id, f"pair {pair.pair_index}: Step 1 failed (no output)", pair.pair_index)
            except Exception as e:
                logger.exception("Step 1 error")
                pair.step1_status = "failed"
                pair.step1_error = str(e)
                append_log(db, job_id, f"pair {pair.pair_index}: ERROR {e}", pair.pair_index)

            db.commit()

            if _scalar_cancel_requested(db, job_id):
                _finalize_step1_cancelled(db, job_id, pairs)
                return

            if i < len(pairs) - 1 and delay_seconds > 0:
                time.sleep(delay_seconds)

        # Job-level status must match pair outcomes (do not mark succeeded if any pair failed).
        any_s1_failed = any(p.step1_status == "failed" for p in pairs)
        if any_s1_failed:
            job.status = "failed"
            job.error_summary = "One or more pairs failed Step 1"
        else:
            job.status = "succeeded"
            job.error_summary = None
        job.finished_at = datetime.utcnow()
        db.commit()
        append_log(db, job_id, "--- Step 1 job finished ---", None)
        job = db.query(Job).filter(Job.id == job_id).one_or_none()
        if job:
            notify_step1_finished(db, job, pairs)
    except Exception as e:
        logger.exception("run_step1_job")
        try:
            job = db.query(Job).filter(Job.id == job_id).one_or_none()
            if job:
                job.status = "failed"
                job.error_summary = str(e)
                job.finished_at = datetime.utcnow()
                db.commit()
                notify_job_crash(db, job, "Step 1", str(e))
        except Exception:
            db.rollback()
    finally:
        db.close()


def run_step2_job(job_id: str, pair_indices: Optional[List[int]] = None) -> None:
    db = SessionLocal()
    try:
        job = db.query(Job).filter(Job.id == job_id).one_or_none()
        if not job:
            logger.error("Job not found: %s", job_id)
            return

        if (job.type or "test_bank").strip() in SINGLE_STAGE_JOB_TYPES:
            return

        cfg = json.loads(job.config_json or "{}")
        prompt_2 = (cfg.get("prompt_2") or "").strip() or get_default_step2_prompt()
        model_2 = cfg.get("model_2", DEFAULT_TEST_BANK_MODEL)
        provider_2 = cfg.get("provider_2", DEFAULT_TEST_BANK_PROVIDER)
        model_1 = cfg.get("model_1", DEFAULT_TEST_BANK_MODEL)
        delay_seconds = float(cfg.get("delay_seconds", 5))

        if job.status == "cancelled":
            return

        job.status = "running"
        if not job.started_at:
            job.started_at = datetime.utcnow()
        db.commit()

        pairs = (
            db.query(JobPair)
            .filter(JobPair.job_id == job_id)
            .order_by(JobPair.pair_index)
            .all()
        )
        if pair_indices is not None:
            wanted = set(pair_indices)
            pairs = [p for p in pairs if p.pair_index in wanted]

        db.refresh(job)
        if job.cancel_requested:
            _finalize_step2_cancelled(db, job_id, pairs)
            return

        client, ssm, processor = build_stage_v_processor()
        jt = (job.type or "test_bank").strip()
        base = job_root(job_id)
        cancel_check = _cancel_check_session(job_id)

        for i, pair in enumerate(pairs):
            if _scalar_cancel_requested(db, job_id):
                _finalize_step2_cancelled(db, job_id, pairs)
                return
            if pair.step1_status != "succeeded":
                append_log(
                    db,
                    job_id,
                    f"pair {pair.pair_index}: Step 2 skipped (Step 1 not succeeded)",
                    pair.pair_index,
                )
                continue
            if not pair.word_relpath:
                pair.step2_status = "failed"
                pair.step2_error = "No Word file"
                db.commit()
                continue

            pair.step2_status = "running"
            pair.step2_error = None
            db.commit()

            abs_j = os.path.join(base, pair.stage_j_relpath.replace("/", os.sep))
            abs_w = os.path.join(base, pair.word_relpath.replace("/", os.sep))
            out_dir = pair_output(job_id, pair.pair_index)
            pair_index = pair.pair_index
            progress_queue: "queue.Queue[str]" = queue.Queue()
            stop_flush = threading.Event()
            flush_errors: List[Exception] = []

            def _flush_worker() -> None:
                """Flush Step 2 progress logs using an isolated DB session (thread-safe)."""
                s = SessionLocal()
                buf: List[str] = []
                try:
                    while True:
                        try:
                            msg = progress_queue.get(timeout=0.2)
                            buf.append(msg)
                        except queue.Empty:
                            pass
                        if len(buf) >= 25:
                            append_logs_bulk(s, job_id, buf, pair_index=pair_index)
                            buf.clear()
                        if stop_flush.is_set() and progress_queue.empty():
                            if buf:
                                append_logs_bulk(s, job_id, buf, pair_index=pair_index)
                                buf.clear()
                            break
                except Exception as e:
                    try:
                        s.rollback()
                    except Exception:
                        pass
                    logger.exception("Step 2 log flush worker failed")
                    flush_errors.append(e)
                finally:
                    s.close()

            flush_thread = threading.Thread(target=_flush_worker, daemon=True)
            flush_thread.start()

            def progress(msg: str) -> None:
                progress_queue.put(msg)
                # Use fresh-session cancel checker; never use shared worker session from pool threads.
                if cancel_check and cancel_check():
                    raise JobCancelled()

            try:
                append_log(db, job_id, f"--- Step 2 start pair {pair_index} ---", pair_index)
                processor.api_client = wrap_prompt_capture(
                    client, db, job_id, pair.pair_index, jt, "step2"
                )
                try:
                    result = processor.process_stage_v_step2(
                        stage_j_path=abs_j,
                        word_file_path=abs_w,
                        prompt_2=prompt_2,
                        model_name_2=model_2,
                        provider_2=provider_2,
                        step1_combined_path=None,
                        model_name_1=model_1,
                        stage_settings_manager=ssm,
                        output_dir=out_dir,
                        progress_callback=progress,
                        delete_step1_combined_after_success=False,
                        cancel_check=cancel_check,
                    )
                except (JobCancelled, OpenRouterRequestAborted):
                    stop_flush.set()
                    flush_thread.join(timeout=10)
                    if flush_errors:
                        raise flush_errors[0]
                    _finalize_step2_cancelled(db, job_id, pairs)
                    return
                stop_flush.set()
                flush_thread.join(timeout=10)
                if flush_errors:
                    raise flush_errors[0]
                if result:
                    pair.step2_status = "succeeded"
                    register_artifacts_under(db, job_id, pair.pair_index, base, os.path.relpath(out_dir, base))
                else:
                    pair.step2_status = "failed"
                    pair.step2_error = "Step 2 returned no output"
                    append_log(db, job_id, f"pair {pair.pair_index}: Step 2 failed", pair.pair_index)
            except Exception as e:
                stop_flush.set()
                flush_thread.join(timeout=10)
                logger.exception("Step 2 error")
                pair.step2_status = "failed"
                pair.step2_error = str(e)
                append_log(db, job_id, f"pair {pair.pair_index}: ERROR {e}", pair.pair_index)

            db.commit()

            if _scalar_cancel_requested(db, job_id):
                _finalize_step2_cancelled(db, job_id, pairs)
                return

            if i < len(pairs) - 1 and delay_seconds > 0:
                time.sleep(delay_seconds)

        # Aggregate failures in this run's pair scope (pair_indices filter already applied to `pairs`).
        msgs: List[str] = []
        if any(p.step1_status == "failed" for p in pairs):
            msgs.append("One or more pairs failed Step 1")
        eligible_s2 = [p for p in pairs if p.step1_status == "succeeded"]
        if eligible_s2 and any(p.step2_status == "failed" for p in eligible_s2):
            msgs.append("One or more pairs failed Step 2")
        if msgs:
            job.status = "failed"
            job.error_summary = "; ".join(msgs)
        else:
            job.status = "succeeded"
            job.error_summary = None
        job.finished_at = datetime.utcnow()
        db.commit()
        append_log(db, job_id, "--- Step 2 job finished ---", None)
        job = db.query(Job).filter(Job.id == job_id).one_or_none()
        if job:
            notify_step2_finished(db, job)
    except Exception as e:
        logger.exception("run_step2_job")
        try:
            job = db.query(Job).filter(Job.id == job_id).one_or_none()
            if job:
                job.status = "failed"
                job.error_summary = str(e)
                job.finished_at = datetime.utcnow()
                db.commit()
                notify_job_crash(db, job, "Step 2", str(e))
        except Exception:
            db.rollback()
    finally:
        db.close()


def run_full_pipeline_job(job_id: str, pair_indices: Optional[List[int]] = None) -> None:
    """Run Step 1 then Step 2 in-process (same as desktop Process All).

    Step 2 always runs after Step 1 returns: pairs without Step 1 success are skipped inside
    run_step2_job, so partial Step 1 failure still allows Step 2 on pairs that succeeded Step 1.
    """
    db = SessionLocal()
    try:
        j = db.query(Job).filter(Job.id == job_id).one_or_none()
        jt = (j.type or "test_bank").strip() if j else ""
    finally:
        db.close()
    if jt in SINGLE_STAGE_JOB_TYPES:
        run_step1_job(job_id, pair_indices)
        return
    run_step1_job(job_id, pair_indices)
    db = SessionLocal()
    try:
        j = db.query(Job).filter(Job.id == job_id).one_or_none()
        if not j or j.status == "cancelled":
            return
    finally:
        db.close()
    run_step2_job(job_id, pair_indices)
