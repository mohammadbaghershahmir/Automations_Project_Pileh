"""
Pre-OCR, OCR Extraction, and Document Processing job runners (web worker / Celery).
"""

from __future__ import annotations

import json
import logging
import os
import tempfile
import time
from datetime import datetime
from typing import List, Optional

from sqlalchemy.orm import Session

from webapp.database import SessionLocal
from webapp.inbox import notify_job_crash, notify_step1_finished
from webapp.job_files import append_log, job_root, pair_output, register_artifacts_under
from webapp.models import Job, JobPair
from webapp.job_runner_common import (
    JobCancelled,
    _cancel_check_session,
    _finalize_step1_cancelled,
    _scalar_cancel_requested,
)
from webapp.processor_context import build_unified_api_client
from webapp.prompt_capture import wrap_prompt_capture

logger = logging.getLogger(__name__)


def _load_pairs(db: Session, job_id: str, pair_indices: Optional[List[int]]) -> List[JobPair]:
    pairs = (
        db.query(JobPair)
        .filter(JobPair.job_id == job_id)
        .order_by(JobPair.pair_index)
        .all()
    )
    if pair_indices is not None:
        wanted = set(pair_indices)
        pairs = [p for p in pairs if p.pair_index in wanted]
    return pairs


def run_pre_ocr_topic_step1_job(job_id: str, pair_indices: Optional[List[int]] = None) -> None:
    from pre_ocr_topic_processor import PreOCRTopicProcessor

    db = SessionLocal()
    try:
        job = db.query(Job).filter(Job.id == job_id).one_or_none()
        if not job:
            logger.error("Job not found: %s", job_id)
            return

        cfg = json.loads(job.config_json or "{}")
        prompt = (cfg.get("prompt") or "").strip()
        if not prompt:
            from webapp.default_prompts import get_default_pre_ocr_prompt

            prompt = get_default_pre_ocr_prompt()
        model_name = (cfg.get("model") or "z-ai/glm-5").strip()
        delay_seconds = float(cfg.get("delay_seconds", 5))

        job.status = "running"
        if not job.started_at:
            job.started_at = datetime.utcnow()
        db.commit()
        append_log(
            db,
            job_id,
            "Pre-OCR runner started (PDF → topic JSON per pair).",
            None,
        )

        pairs = _load_pairs(db, job_id, pair_indices)
        if job.cancel_requested:
            _finalize_step1_cancelled(db, job_id, pairs)
            return

        client, _ssm = build_unified_api_client()
        jt = (job.type or "pre_ocr_topic").strip()
        base = job_root(job_id)
        cancel_check = _cancel_check_session(job_id)

        for i, pair in enumerate(pairs):
            if _scalar_cancel_requested(db, job_id):
                _finalize_step1_cancelled(db, job_id, pairs)
                return

            abs_pdf = os.path.join(base, pair.stage_j_relpath.replace("/", os.sep))
            if not pair.stage_j_relpath or not os.path.isfile(abs_pdf):
                pair.step1_status = "failed"
                pair.step1_error = "No PDF input"
                db.commit()
                append_log(db, job_id, f"pair {pair.pair_index}: skipped (no PDF)", pair.pair_index)
                continue

            pair.step1_status = "running"
            pair.step1_error = None
            db.commit()

            out_dir = pair_output(job_id, pair.pair_index)
            processor = PreOCRTopicProcessor(
                wrap_prompt_capture(client, db, job_id, pair.pair_index, jt, "step1")
            )

            def progress(msg: str) -> None:
                append_log(db, job_id, msg, pair.pair_index)
                if _scalar_cancel_requested(db, job_id):
                    raise JobCancelled()

            try:
                append_log(db, job_id, f"--- Pre-OCR start pair {pair.pair_index} ---", pair.pair_index)
                try:
                    result = processor.process_pre_ocr_topic(
                        pdf_path=abs_pdf,
                        prompt=prompt,
                        model_name=model_name,
                        output_dir=out_dir,
                        progress_callback=progress,
                    )
                except JobCancelled:
                    _finalize_step1_cancelled(db, job_id, pairs)
                    return
                if result and os.path.isfile(result):
                    pair.step1_status = "succeeded"
                    register_artifacts_under(
                        db, job_id, pair.pair_index, base, os.path.relpath(out_dir, base)
                    )
                else:
                    pair.step1_status = "failed"
                    pair.step1_error = "Pre-OCR returned no output"
                    append_log(db, job_id, f"pair {pair.pair_index}: Pre-OCR failed", pair.pair_index)
            except Exception as e:
                logger.exception("Pre-OCR error")
                pair.step1_status = "failed"
                pair.step1_error = str(e)
                append_log(db, job_id, f"pair {pair.pair_index}: ERROR {e}", pair.pair_index)

            db.commit()

            if _scalar_cancel_requested(db, job_id):
                _finalize_step1_cancelled(db, job_id, pairs)
                return

            if i < len(pairs) - 1 and delay_seconds > 0:
                time.sleep(delay_seconds)

        any_failed = any(p.step1_status == "failed" for p in pairs)
        if any_failed:
            job.status = "failed"
            job.error_summary = "One or more pairs failed"
        else:
            job.status = "succeeded"
            job.error_summary = None
        job.finished_at = datetime.utcnow()
        db.commit()
        append_log(db, job_id, "--- Pre-OCR job finished ---", None)
        job = db.query(Job).filter(Job.id == job_id).one_or_none()
        if job:
            notify_step1_finished(db, job, pairs)
    except Exception as e:
        logger.exception("run_pre_ocr_topic_step1_job")
        try:
            job = db.query(Job).filter(Job.id == job_id).one_or_none()
            if job:
                job.status = "failed"
                job.error_summary = str(e)
                job.finished_at = datetime.utcnow()
                db.commit()
                notify_job_crash(db, job, "Pre-OCR", str(e))
        except Exception:
            db.rollback()
    finally:
        db.close()


def run_ocr_extraction_step1_job(job_id: str, pair_indices: Optional[List[int]] = None) -> None:
    from multi_part_processor import MultiPartProcessor

    db = SessionLocal()
    try:
        job = db.query(Job).filter(Job.id == job_id).one_or_none()
        if not job:
            logger.error("Job not found: %s", job_id)
            return

        cfg = json.loads(job.config_json or "{}")
        prompt = (cfg.get("prompt") or "").strip()
        if not prompt:
            from webapp.default_prompts import get_default_ocr_extraction_prompt

            prompt = get_default_ocr_extraction_prompt()
        model_name = (cfg.get("model") or "z-ai/glm-5").strip()
        delay_seconds = float(cfg.get("delay_seconds", 5))

        job.status = "running"
        if not job.started_at:
            job.started_at = datetime.utcnow()
        db.commit()
        append_log(db, job_id, "OCR Extraction runner started (PDF + topic JSON per pair).", None)

        pairs = _load_pairs(db, job_id, pair_indices)
        if job.cancel_requested:
            _finalize_step1_cancelled(db, job_id, pairs)
            return

        client, _ssm = build_unified_api_client()
        jt = (job.type or "ocr_extraction").strip()
        base = job_root(job_id)
        cancel_check = _cancel_check_session(job_id)

        for i, pair in enumerate(pairs):
            if _scalar_cancel_requested(db, job_id):
                _finalize_step1_cancelled(db, job_id, pairs)
                return

            abs_pdf = os.path.join(base, pair.stage_j_relpath.replace("/", os.sep))
            if not pair.word_relpath:
                pair.step1_status = "failed"
                pair.step1_error = "No topic JSON paired"
                db.commit()
                append_log(db, job_id, f"pair {pair.pair_index}: skipped (no topic JSON)", pair.pair_index)
                continue
            abs_topic = os.path.join(base, pair.word_relpath.replace("/", os.sep))
            if not os.path.isfile(abs_pdf) or not os.path.isfile(abs_topic):
                pair.step1_status = "failed"
                pair.step1_error = "Missing PDF or topic JSON on disk"
                db.commit()
                continue

            pair.step1_status = "running"
            pair.step1_error = None
            db.commit()

            out_dir = pair_output(job_id, pair.pair_index)
            os.makedirs(out_dir, exist_ok=True)
            mproc = MultiPartProcessor(
                wrap_prompt_capture(client, db, job_id, pair.pair_index, jt, "step1"),
                output_dir=None,
            )

            def progress(msg: str) -> None:
                append_log(db, job_id, msg, pair.pair_index)
                if _scalar_cancel_requested(db, job_id):
                    raise JobCancelled()

            try:
                append_log(db, job_id, f"--- OCR Extraction start pair {pair.pair_index} ---", pair.pair_index)
                try:
                    result = mproc.process_ocr_extraction_with_topics(
                        pdf_path=abs_pdf,
                        topic_file_path=abs_topic,
                        base_prompt=prompt,
                        model_name=model_name,
                        progress_callback=progress,
                        output_dir=out_dir,
                    )
                except JobCancelled:
                    _finalize_step1_cancelled(db, job_id, pairs)
                    return
                if result and os.path.isfile(result):
                    pair.step1_status = "succeeded"
                    rel_out = os.path.relpath(out_dir, base).replace("\\", "/")
                    register_artifacts_under(db, job_id, pair.pair_index, base, rel_out)
                else:
                    pair.step1_status = "failed"
                    pair.step1_error = "OCR Extraction returned no output"
                    append_log(db, job_id, f"pair {pair.pair_index}: OCR Extraction failed", pair.pair_index)
            except Exception as e:
                logger.exception("OCR Extraction error")
                pair.step1_status = "failed"
                pair.step1_error = str(e)
                append_log(db, job_id, f"pair {pair.pair_index}: ERROR {e}", pair.pair_index)

            db.commit()

            if _scalar_cancel_requested(db, job_id):
                _finalize_step1_cancelled(db, job_id, pairs)
                return

            if i < len(pairs) - 1 and delay_seconds > 0:
                time.sleep(delay_seconds)

        any_failed = any(p.step1_status == "failed" for p in pairs)
        if any_failed:
            job.status = "failed"
            job.error_summary = "One or more pairs failed"
        else:
            job.status = "succeeded"
            job.error_summary = None
        job.finished_at = datetime.utcnow()
        db.commit()
        append_log(db, job_id, "--- OCR Extraction job finished ---", None)
        job = db.query(Job).filter(Job.id == job_id).one_or_none()
        if job:
            notify_step1_finished(db, job, pairs)
    except Exception as e:
        logger.exception("run_ocr_extraction_step1_job")
        try:
            job = db.query(Job).filter(Job.id == job_id).one_or_none()
            if job:
                job.status = "failed"
                job.error_summary = str(e)
                job.finished_at = datetime.utcnow()
                db.commit()
                notify_job_crash(db, job, "OCR Extraction", str(e))
        except Exception:
            db.rollback()
    finally:
        db.close()


def run_document_processing_step1_job(job_id: str, pair_indices: Optional[List[int]] = None) -> None:
    from multi_part_post_processor import MultiPartPostProcessor

    db = SessionLocal()
    try:
        job = db.query(Job).filter(Job.id == job_id).one_or_none()
        if not job:
            logger.error("Job not found: %s", job_id)
            return

        cfg = json.loads(job.config_json or "{}")
        user_prompt = (cfg.get("prompt") or "").strip()
        if not user_prompt:
            from webapp.default_prompts import get_default_document_processing_prompt

            user_prompt = get_default_document_processing_prompt()
        model_name = (cfg.get("model") or "z-ai/glm-5").strip()
        delay_seconds = float(cfg.get("delay_seconds", 5))
        book_id = cfg.get("book_id")
        chapter_id = cfg.get("chapter_id")
        start_point_index = int(cfg.get("start_point_index") or 1)
        if book_id is not None:
            book_id = int(book_id)
        if chapter_id is not None:
            chapter_id = int(chapter_id)

        job.status = "running"
        if not job.started_at:
            job.started_at = datetime.utcnow()
        db.commit()
        append_log(db, job_id, "Document Processing runner started (OCR JSON → lesson JSON per pair).", None)

        pairs = _load_pairs(db, job_id, pair_indices)
        if job.cancel_requested:
            _finalize_step1_cancelled(db, job_id, pairs)
            return

        client, _ssm = build_unified_api_client()
        jt = (job.type or "document_processing").strip()
        base = job_root(job_id)

        pointid_rel = (cfg.get("pointid_mapping_relpath") or "").strip()
        pointid_full = (
            os.path.join(base, pointid_rel.replace("/", os.sep)) if pointid_rel else ""
        )
        all_pointids: List[str] = []
        if pointid_full and os.path.isfile(pointid_full):
            all_pointids = post.load_chapter_pointid_mapping(pointid_full)

        for i, pair in enumerate(pairs):
            if _scalar_cancel_requested(db, job_id):
                _finalize_step1_cancelled(db, job_id, pairs)
                return

            abs_json = os.path.join(base, pair.stage_j_relpath.replace("/", os.sep))
            if not pair.stage_j_relpath or not os.path.isfile(abs_json):
                pair.step1_status = "failed"
                pair.step1_error = "No OCR JSON input"
                db.commit()
                append_log(db, job_id, f"pair {pair.pair_index}: skipped (no JSON)", pair.pair_index)
                continue

            pair.step1_status = "running"
            pair.step1_error = None
            db.commit()

            post = MultiPartPostProcessor(
                wrap_prompt_capture(client, db, job_id, pair.pair_index, jt, "step1")
            )

            file_pointid_txt: Optional[str] = None
            if all_pointids and i < len(all_pointids):
                try:
                    fd, tmp_path = tempfile.mkstemp(prefix="pid_", suffix=".txt", text=True)
                    with os.fdopen(fd, "w", encoding="utf-8") as tf:
                        tf.write(all_pointids[i] + "\n")
                    file_pointid_txt = tmp_path
                except OSError as e:
                    append_log(
                        db,
                        job_id,
                        f"pair {pair.pair_index}: could not write PointId temp file: {e}",
                        pair.pair_index,
                    )

            def progress(msg: str) -> None:
                append_log(db, job_id, msg, pair.pair_index)
                if _scalar_cancel_requested(db, job_id):
                    raise JobCancelled()

            final_output_path: Optional[str] = None
            try:
                append_log(db, job_id, f"--- Document Processing start pair {pair.pair_index} ---", pair.pair_index)
                final_output_path = post.process_document_processing_from_ocr_json(
                    ocr_json_path=abs_json,
                    user_prompt=user_prompt,
                    model_name=model_name,
                    book_id=book_id,
                    chapter_id=chapter_id,
                    start_point_index=start_point_index,
                    pointid_mapping_txt=file_pointid_txt,
                    progress_callback=progress,
                )
            except JobCancelled:
                if file_pointid_txt and os.path.isfile(file_pointid_txt):
                    try:
                        os.remove(file_pointid_txt)
                    except OSError:
                        pass
                _finalize_step1_cancelled(db, job_id, pairs)
                return
            except Exception as e:
                logger.exception("Document Processing error")
                pair.step1_status = "failed"
                pair.step1_error = str(e)
                append_log(db, job_id, f"pair {pair.pair_index}: ERROR {e}", pair.pair_index)
            else:
                if final_output_path and os.path.isfile(final_output_path):
                    pair.step1_status = "succeeded"
                    rel_out = os.path.relpath(
                        os.path.dirname(os.path.abspath(final_output_path)),
                        base,
                    ).replace("\\", "/")
                    register_artifacts_under(db, job_id, pair.pair_index, base, rel_out)
                else:
                    pair.step1_status = "failed"
                    pair.step1_error = "Document Processing returned no output"
                    append_log(db, job_id, f"pair {pair.pair_index}: failed", pair.pair_index)
            finally:
                if file_pointid_txt and os.path.isfile(file_pointid_txt):
                    try:
                        os.remove(file_pointid_txt)
                    except OSError:
                        pass

            db.commit()

            if _scalar_cancel_requested(db, job_id):
                _finalize_step1_cancelled(db, job_id, pairs)
                return

            if i < len(pairs) - 1 and delay_seconds > 0:
                time.sleep(delay_seconds)

        any_failed = any(p.step1_status == "failed" for p in pairs)
        if any_failed:
            job.status = "failed"
            job.error_summary = "One or more pairs failed"
        else:
            job.status = "succeeded"
            job.error_summary = None
        job.finished_at = datetime.utcnow()
        db.commit()
        append_log(db, job_id, "--- Document Processing job finished ---", None)
        job = db.query(Job).filter(Job.id == job_id).one_or_none()
        if job:
            notify_step1_finished(db, job, pairs)
    except Exception as e:
        logger.exception("run_document_processing_step1_job")
        try:
            job = db.query(Job).filter(Job.id == job_id).one_or_none()
            if job:
                job.status = "failed"
                job.error_summary = str(e)
                job.finished_at = datetime.utcnow()
                db.commit()
                notify_job_crash(db, job, "Document Processing", str(e))
        except Exception:
            db.rollback()
    finally:
        db.close()


def run_image_notes_step1_job(job_id: str, pair_indices: Optional[List[int]] = None) -> None:
    """Stage E: document processing (Stage 4) JSON + OCR extraction JSON → image notes JSON (writes beside inputs)."""
    from stage_e_processor import StageEProcessor

    db = SessionLocal()
    try:
        job = db.query(Job).filter(Job.id == job_id).one_or_none()
        if not job:
            logger.error("Job not found: %s", job_id)
            return

        cfg = json.loads(job.config_json or "{}")
        prompt = (cfg.get("prompt") or "").strip()
        if not prompt:
            from webapp.default_prompts import get_default_image_notes_prompt

            prompt = get_default_image_notes_prompt()
        model_name = (cfg.get("model") or "z-ai/glm-5").strip()
        delay_seconds = float(cfg.get("delay_seconds", 5))

        job.status = "running"
        if not job.started_at:
            job.started_at = datetime.utcnow()
        db.commit()
        append_log(db, job_id, "Image Notes runner started (document processing / Stage 4 JSON + OCR extraction JSON per pair).", None)

        pairs = _load_pairs(db, job_id, pair_indices)
        if job.cancel_requested:
            _finalize_step1_cancelled(db, job_id, pairs)
            return

        client, _ssm = build_unified_api_client()
        jt = (job.type or "image_notes").strip()
        base = job_root(job_id)

        for i, pair in enumerate(pairs):
            if _scalar_cancel_requested(db, job_id):
                _finalize_step1_cancelled(db, job_id, pairs)
                return

            if not pair.stage_j_relpath or not pair.word_relpath:
                pair.step1_status = "failed"
                pair.step1_error = "Missing document processing (Stage 4) or OCR extraction JSON path"
                db.commit()
                append_log(db, job_id, f"pair {pair.pair_index}: skipped (incomplete pair)", pair.pair_index)
                continue

            abs_stage4 = os.path.join(base, pair.stage_j_relpath.replace("/", os.sep))
            abs_ocr = os.path.join(base, pair.word_relpath.replace("/", os.sep))
            if not os.path.isfile(abs_stage4) or not os.path.isfile(abs_ocr):
                pair.step1_status = "failed"
                pair.step1_error = "Missing document processing (Stage 4) or OCR JSON on disk"
                db.commit()
                continue

            pair.step1_status = "running"
            pair.step1_error = None
            db.commit()

            processor = StageEProcessor(
                wrap_prompt_capture(client, db, job_id, pair.pair_index, jt, "step1")
            )

            def progress(msg: str) -> None:
                append_log(db, job_id, msg, pair.pair_index)
                if _scalar_cancel_requested(db, job_id):
                    raise JobCancelled()

            try:
                append_log(db, job_id, f"--- Image Notes (Stage E) start pair {pair.pair_index} ---", pair.pair_index)
                try:
                    result = processor.process_stage_e(
                        stage4_path=abs_stage4,
                        ocr_extraction_json_path=abs_ocr,
                        prompt=prompt,
                        model_name=model_name,
                        output_dir=None,
                        progress_callback=progress,
                    )
                except JobCancelled:
                    _finalize_step1_cancelled(db, job_id, pairs)
                    return
                if result and os.path.isfile(result):
                    pair.step1_status = "succeeded"
                    rel_inputs = f"pair_{pair.pair_index}/inputs"
                    register_artifacts_under(db, job_id, pair.pair_index, base, rel_inputs)
                else:
                    pair.step1_status = "failed"
                    pair.step1_error = "Image Notes returned no output"
                    append_log(db, job_id, f"pair {pair.pair_index}: Image Notes failed", pair.pair_index)
            except Exception as e:
                logger.exception("Image Notes error")
                pair.step1_status = "failed"
                pair.step1_error = str(e)
                append_log(db, job_id, f"pair {pair.pair_index}: ERROR {e}", pair.pair_index)

            db.commit()

            if _scalar_cancel_requested(db, job_id):
                _finalize_step1_cancelled(db, job_id, pairs)
                return

            if i < len(pairs) - 1 and delay_seconds > 0:
                time.sleep(delay_seconds)

        any_failed = any(p.step1_status == "failed" for p in pairs)
        if any_failed:
            job.status = "failed"
            job.error_summary = "One or more pairs failed"
        else:
            job.status = "succeeded"
            job.error_summary = None
        job.finished_at = datetime.utcnow()
        db.commit()
        append_log(db, job_id, "--- Image Notes job finished ---", None)
        job = db.query(Job).filter(Job.id == job_id).one_or_none()
        if job:
            notify_step1_finished(db, job, pairs)
    except Exception as e:
        logger.exception("run_image_notes_step1_job")
        try:
            job = db.query(Job).filter(Job.id == job_id).one_or_none()
            if job:
                job.status = "failed"
                job.error_summary = str(e)
                job.finished_at = datetime.utcnow()
                db.commit()
                notify_job_crash(db, job, "Image Notes", str(e))
        except Exception:
            db.rollback()
    finally:
        db.close()


def run_table_notes_step1_job(job_id: str, pair_indices: Optional[List[int]] = None) -> None:
    """Stage TA: image notes (Stage E) JSON + OCR extraction JSON → table notes JSON (writes beside inputs)."""
    from stage_ta_processor import StageTAProcessor

    db = SessionLocal()
    try:
        job = db.query(Job).filter(Job.id == job_id).one_or_none()
        if not job:
            logger.error("Job not found: %s", job_id)
            return

        cfg = json.loads(job.config_json or "{}")
        prompt = (cfg.get("prompt") or "").strip()
        if not prompt:
            from webapp.default_prompts import get_default_table_notes_prompt

            prompt = get_default_table_notes_prompt()
        model_name = (cfg.get("model") or "z-ai/glm-5").strip()
        delay_seconds = float(cfg.get("delay_seconds", 5))

        job.status = "running"
        if not job.started_at:
            job.started_at = datetime.utcnow()
        db.commit()
        append_log(db, job_id, "Table Notes runner started (image notes / Stage E JSON + OCR extraction JSON per pair).", None)

        pairs = _load_pairs(db, job_id, pair_indices)
        if job.cancel_requested:
            _finalize_step1_cancelled(db, job_id, pairs)
            return

        client, _ssm = build_unified_api_client()
        jt = (job.type or "table_notes").strip()
        base = job_root(job_id)

        for i, pair in enumerate(pairs):
            if _scalar_cancel_requested(db, job_id):
                _finalize_step1_cancelled(db, job_id, pairs)
                return

            if not pair.stage_j_relpath or not pair.word_relpath:
                pair.step1_status = "failed"
                pair.step1_error = "Missing image notes (Stage E) or OCR extraction JSON path"
                db.commit()
                append_log(db, job_id, f"pair {pair.pair_index}: skipped (incomplete pair)", pair.pair_index)
                continue

            abs_stage_e = os.path.join(base, pair.stage_j_relpath.replace("/", os.sep))
            abs_ocr = os.path.join(base, pair.word_relpath.replace("/", os.sep))
            if not os.path.isfile(abs_stage_e) or not os.path.isfile(abs_ocr):
                pair.step1_status = "failed"
                pair.step1_error = "Missing image notes (Stage E) or OCR JSON on disk"
                db.commit()
                continue

            pair.step1_status = "running"
            pair.step1_error = None
            db.commit()

            processor = StageTAProcessor(
                wrap_prompt_capture(client, db, job_id, pair.pair_index, jt, "step1")
            )

            def progress(msg: str) -> None:
                append_log(db, job_id, msg, pair.pair_index)
                if _scalar_cancel_requested(db, job_id):
                    raise JobCancelled()

            try:
                append_log(db, job_id, f"--- Table Notes (Stage TA) start pair {pair.pair_index} ---", pair.pair_index)
                try:
                    result = processor.process_stage_ta(
                        stage_e_path=abs_stage_e,
                        ocr_extraction_json_path=abs_ocr,
                        prompt=prompt,
                        model_name=model_name,
                        output_dir=None,
                        progress_callback=progress,
                    )
                except JobCancelled:
                    _finalize_step1_cancelled(db, job_id, pairs)
                    return
                if result and os.path.isfile(result):
                    pair.step1_status = "succeeded"
                    rel_inputs = f"pair_{pair.pair_index}/inputs"
                    register_artifacts_under(db, job_id, pair.pair_index, base, rel_inputs)
                else:
                    pair.step1_status = "failed"
                    pair.step1_error = "Table Notes returned no output"
                    append_log(db, job_id, f"pair {pair.pair_index}: Table Notes failed", pair.pair_index)
            except Exception as e:
                logger.exception("Table Notes error")
                pair.step1_status = "failed"
                pair.step1_error = str(e)
                append_log(db, job_id, f"pair {pair.pair_index}: ERROR {e}", pair.pair_index)

            db.commit()

            if _scalar_cancel_requested(db, job_id):
                _finalize_step1_cancelled(db, job_id, pairs)
                return

            if i < len(pairs) - 1 and delay_seconds > 0:
                time.sleep(delay_seconds)

        any_failed = any(p.step1_status == "failed" for p in pairs)
        if any_failed:
            job.status = "failed"
            job.error_summary = "One or more pairs failed"
        else:
            job.status = "succeeded"
            job.error_summary = None
        job.finished_at = datetime.utcnow()
        db.commit()
        append_log(db, job_id, "--- Table Notes job finished ---", None)
        job = db.query(Job).filter(Job.id == job_id).one_or_none()
        if job:
            notify_step1_finished(db, job, pairs)
    except Exception as e:
        logger.exception("run_table_notes_step1_job")
        try:
            job = db.query(Job).filter(Job.id == job_id).one_or_none()
            if job:
                job.status = "failed"
                job.error_summary = str(e)
                job.finished_at = datetime.utcnow()
                db.commit()
                notify_job_crash(db, job, "Table Notes", str(e))
        except Exception:
            db.rollback()
    finally:
        db.close()


def run_image_file_catalog_step1_job(job_id: str, pair_indices: Optional[List[int]] = None) -> None:
    """Stage F: image notes (Stage E) JSON → catalog JSON f_*.json under pair output (optional filepic JSON beside Stage E in inputs)."""
    from stage_f_processor import StageFProcessor

    db = SessionLocal()
    try:
        job = db.query(Job).filter(Job.id == job_id).one_or_none()
        if not job:
            logger.error("Job not found: %s", job_id)
            return

        cfg = json.loads(job.config_json or "{}")
        delay_seconds = float(cfg.get("delay_seconds", 5))

        job.status = "running"
        if not job.started_at:
            job.started_at = datetime.utcnow()
        db.commit()
        append_log(db, job_id, "Image File Catalog runner started (image notes / Stage E JSON → f_*.json per pair).", None)

        pairs = _load_pairs(db, job_id, pair_indices)
        if job.cancel_requested:
            _finalize_step1_cancelled(db, job_id, pairs)
            return

        client, _ssm = build_unified_api_client()
        jt = (job.type or "image_file_catalog").strip()
        base = job_root(job_id)

        for i, pair in enumerate(pairs):
            if _scalar_cancel_requested(db, job_id):
                _finalize_step1_cancelled(db, job_id, pairs)
                return

            if not pair.stage_j_relpath:
                pair.step1_status = "failed"
                pair.step1_error = "No image notes (Stage E) JSON input"
                db.commit()
                append_log(db, job_id, f"pair {pair.pair_index}: skipped (no image notes / Stage E JSON)", pair.pair_index)
                continue

            abs_stage_e = os.path.join(base, pair.stage_j_relpath.replace("/", os.sep))
            if not os.path.isfile(abs_stage_e):
                pair.step1_status = "failed"
                pair.step1_error = "Image notes (Stage E) JSON missing on disk"
                db.commit()
                continue

            pair.step1_status = "running"
            pair.step1_error = None
            db.commit()

            out_dir = pair_output(job_id, pair.pair_index)
            os.makedirs(out_dir, exist_ok=True)

            processor = StageFProcessor(
                wrap_prompt_capture(client, db, job_id, pair.pair_index, jt, "step1")
            )

            def progress(msg: str) -> None:
                append_log(db, job_id, msg, pair.pair_index)
                if _scalar_cancel_requested(db, job_id):
                    raise JobCancelled()

            try:
                append_log(db, job_id, f"--- Image File Catalog (Stage F) start pair {pair.pair_index} ---", pair.pair_index)
                try:
                    result = processor.process_stage_f(
                        stage_e_path=abs_stage_e,
                        output_dir=out_dir,
                        progress_callback=progress,
                    )
                except JobCancelled:
                    _finalize_step1_cancelled(db, job_id, pairs)
                    return
                if result and os.path.isfile(result):
                    pair.step1_status = "succeeded"
                    rel_out = os.path.relpath(out_dir, base).replace("\\", "/")
                    register_artifacts_under(db, job_id, pair.pair_index, base, rel_out)
                else:
                    pair.step1_status = "failed"
                    pair.step1_error = "Image File Catalog returned no output"
                    append_log(db, job_id, f"pair {pair.pair_index}: Stage F failed", pair.pair_index)
            except Exception as e:
                logger.exception("Image File Catalog error")
                pair.step1_status = "failed"
                pair.step1_error = str(e)
                append_log(db, job_id, f"pair {pair.pair_index}: ERROR {e}", pair.pair_index)

            db.commit()

            if _scalar_cancel_requested(db, job_id):
                _finalize_step1_cancelled(db, job_id, pairs)
                return

            if i < len(pairs) - 1 and delay_seconds > 0:
                time.sleep(delay_seconds)

        any_failed = any(p.step1_status == "failed" for p in pairs)
        if any_failed:
            job.status = "failed"
            job.error_summary = "One or more pairs failed"
        else:
            job.status = "succeeded"
            job.error_summary = None
        job.finished_at = datetime.utcnow()
        db.commit()
        append_log(db, job_id, "--- Image File Catalog job finished ---", None)
        job = db.query(Job).filter(Job.id == job_id).one_or_none()
        if job:
            notify_step1_finished(db, job, pairs)
    except Exception as e:
        logger.exception("run_image_file_catalog_step1_job")
        try:
            job = db.query(Job).filter(Job.id == job_id).one_or_none()
            if job:
                job.status = "failed"
                job.error_summary = str(e)
                job.finished_at = datetime.utcnow()
                db.commit()
                notify_job_crash(db, job, "Image File Catalog", str(e))
        except Exception:
            db.rollback()
    finally:
        db.close()
