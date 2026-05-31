"""Voice Class job runners (Step 1 script, Step 2 TTS + merge)."""

from __future__ import annotations

import json
import logging
import os
import time
from datetime import datetime
from typing import List, Optional

from sqlalchemy.orm import Session

from webapp.config import (
    DEFAULT_TEST_BANK_MODEL,
    DEFAULT_VOICE_CLASS_CHARS_PER_SECOND,
    DEFAULT_VOICE_CLASS_MAX_SEGMENT_SECONDS,
    DEFAULT_VOICE_CLASS_TTS_MODEL,
    DEFAULT_VOICE_CLASS_TTS_VOICE,
    PROJECT_ROOT,
    SONGS_DIR,
    normalize_test_bank_model,
)
from webapp.database import SessionLocal
from webapp.gemini_tts_key_manager import GeminiTtsKeyManager
from webapp.inbox import notify_job_crash, notify_step1_finished, notify_step2_finished
from webapp.job_files import append_log, job_root, pair_output, register_artifacts_under
from webapp.job_runner_common import (
    JobCancelled,
    _cancel_check_session,
    _finalize_step1_cancelled,
    _finalize_step2_cancelled,
    _scalar_cancel_requested,
)
from webapp.models import Job, JobPair
from webapp.processor_context import build_unified_api_client
from webapp.prompt_capture import wrap_prompt_capture
from webapp.system_prompt_defaults import resolve_prompt_for_job
from webapp.tasks_single_stage import _load_pairs

logger = logging.getLogger(__name__)


# Remove unused helper
def _find_voice_script(base: str, pair_index: int) -> Optional[str]:
    abs_dir = os.path.join(base, f"pair_{pair_index}", "output")
    if not os.path.isdir(abs_dir):
        return None
    for name in sorted(os.listdir(abs_dir)):
        if name.startswith("voice_script_") and name.endswith(".json"):
            return os.path.join(abs_dir, name)
    return None


def _register_voice_artifacts(db: Session, job_id: str, pair_index: int, base: str, out_dir: str) -> None:
    rel_out = os.path.relpath(out_dir, base).replace("\\", "/")
    register_artifacts_under(db, job_id, pair_index, base, rel_out)


def run_voice_class_step1_job(job_id: str, pair_indices: Optional[List[int]] = None) -> None:
    from stage_voice_processor import StageVoiceProcessor

    db = SessionLocal()
    try:
        job = db.query(Job).filter(Job.id == job_id).one_or_none()
        if not job:
            logger.error("Job not found: %s", job_id)
            return

        cfg = json.loads(job.config_json or "{}")
        jt = (job.type or "voice_class").strip()
        prompt = resolve_prompt_for_job(db, jt, cfg, "prompt_1")
        model_name = normalize_test_bank_model(cfg.get("model_1"), DEFAULT_TEST_BANK_MODEL)
        max_seg = float(cfg.get("max_segment_seconds", DEFAULT_VOICE_CLASS_MAX_SEGMENT_SECONDS))
        cps = float(cfg.get("chars_per_second", DEFAULT_VOICE_CLASS_CHARS_PER_SECOND))
        delay_seconds = float(cfg.get("delay_seconds", 5))

        job.status = "running"
        if not job.started_at:
            job.started_at = datetime.utcnow()
        db.commit()
        append_log(db, job_id, "Voice Class Step 1 started (OpenRouter script generation).", None)

        pairs = _load_pairs(db, job_id, pair_indices)
        if job.cancel_requested:
            _finalize_step1_cancelled(db, job_id, pairs)
            return

        client, _ssm = build_unified_api_client()
        base = job_root(job_id)
        cancel_check = _cancel_check_session(job_id)

        for i, pair in enumerate(pairs):
            if _scalar_cancel_requested(db, job_id):
                _finalize_step1_cancelled(db, job_id, pairs)
                return

            if not pair.stage_j_relpath:
                pair.step1_status = "failed"
                pair.step1_error = "Missing tagged JSON (Importance & Type output)"
                db.commit()
                continue

            abs_tagged = os.path.join(base, pair.stage_j_relpath.replace("/", os.sep))
            if not os.path.isfile(abs_tagged):
                pair.step1_status = "failed"
                pair.step1_error = "Tagged JSON file missing on disk"
                db.commit()
                continue

            pair.step1_status = "running"
            pair.step1_error = None
            pair.step2_status = "pending"
            pair.step2_error = None
            db.commit()

            out_dir = pair_output(job_id, pair.pair_index)
            os.makedirs(out_dir, exist_ok=True)
            processor = StageVoiceProcessor(
                wrap_prompt_capture(client, db, job_id, pair.pair_index, jt, "step1")
            )

            def progress(msg: str) -> None:
                append_log(db, job_id, msg, pair.pair_index)
                if _scalar_cancel_requested(db, job_id):
                    raise JobCancelled()

            try:
                append_log(db, job_id, f"--- Voice Class Step 1 pair {pair.pair_index} ---", pair.pair_index)
                result = processor.process_voice_class_step1(
                    tagged_json_path=abs_tagged,
                    prompt=prompt,
                    model_name=model_name,
                    output_dir=out_dir,
                    max_segment_seconds=max_seg,
                    chars_per_second=cps,
                    progress_callback=progress,
                    cancel_check=cancel_check,
                )
                if result and os.path.isfile(result):
                    pair.step1_status = "succeeded"
                    _register_voice_artifacts(db, job_id, pair.pair_index, base, out_dir)
                else:
                    pair.step1_status = "failed"
                    pair.step1_error = "Script generation returned no output"
            except JobCancelled:
                _finalize_step1_cancelled(db, job_id, pairs)
                return
            except Exception as e:
                logger.exception("Voice Class Step 1 error")
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
        job.status = "failed" if any_failed else "succeeded"
        job.error_summary = "One or more pairs failed" if any_failed else None
        job.finished_at = datetime.utcnow()
        db.commit()
        append_log(db, job_id, "--- Voice Class Step 1 finished ---", None)
        job = db.query(Job).filter(Job.id == job_id).one_or_none()
        if job:
            notify_step1_finished(db, job, pairs)
    except Exception as e:
        logger.exception("run_voice_class_step1_job")
        try:
            job = db.query(Job).filter(Job.id == job_id).one_or_none()
            if job:
                job.status = "failed"
                job.error_summary = str(e)
                job.finished_at = datetime.utcnow()
                db.commit()
                notify_job_crash(db, job, "Voice Class Step 1", str(e))
        except Exception:
            db.rollback()
    finally:
        db.close()


def _run_voice_class_step2_pairs(
    db: Session,
    job_id: str,
    pairs: List[JobPair],
    *,
    segment_indices_by_pair: Optional[dict] = None,
    skip_merge: bool = False,
) -> None:
    from stage_voice_processor import StageVoiceProcessor

    cfg = json.loads(db.query(Job).filter(Job.id == job_id).one().config_json or "{}")
    jt = "voice_class"
    tts_instruction = resolve_prompt_for_job(db, jt, cfg, "tts_instruction")
    tts_model = (cfg.get("tts_model") or DEFAULT_VOICE_CLASS_TTS_MODEL).strip()
    tts_voice = (cfg.get("tts_voice") or DEFAULT_VOICE_CLASS_TTS_VOICE).strip()
    delay_seconds = float(cfg.get("delay_seconds", 5))

    intro = os.path.join(SONGS_DIR, "a_int.mp3")
    outro = os.path.join(SONGS_DIR, "a_out.mp3")
    base = job_root(job_id)
    cancel_check = _cancel_check_session(job_id)
    key_mgr = GeminiTtsKeyManager(db)
    processor = StageVoiceProcessor(None, gemini_tts_key_manager=key_mgr)

    for i, pair in enumerate(pairs):
        if _scalar_cancel_requested(db, job_id):
            _finalize_step2_cancelled(db, job_id, pairs)
            return
        if pair.step1_status != "succeeded":
            append_log(db, job_id, f"pair {pair.pair_index}: Step 2 skipped (Step 1 not succeeded)", pair.pair_index)
            continue

        script_path = _find_voice_script(base, pair.pair_index)
        if not script_path:
            pair.step2_status = "failed"
            pair.step2_error = "Voice script JSON not found — run Step 1 first"
            db.commit()
            continue

        pair.step2_status = "running"
        pair.step2_error = None
        db.commit()

        out_dir = pair_output(job_id, pair.pair_index)
        seg_indices = None
        if segment_indices_by_pair is not None:
            seg_indices = segment_indices_by_pair.get(pair.pair_index)

        def progress(msg: str) -> None:
            append_log(db, job_id, msg, pair.pair_index)
            if _scalar_cancel_requested(db, job_id):
                raise JobCancelled()

        try:
            append_log(db, job_id, f"--- Voice Class Step 2 pair {pair.pair_index} ---", pair.pair_index)
            result = processor.process_voice_class_step2(
                script_json_path=script_path,
                output_dir=out_dir,
                intro_mp3=intro,
                outro_mp3=outro,
                tts_model=tts_model,
                tts_voice=tts_voice,
                tts_instruction=tts_instruction,
                segment_indices=seg_indices,
                skip_merge=skip_merge,
                progress_callback=progress,
                cancel_check=cancel_check,
            )
            if result and os.path.isfile(result):
                pair.step2_status = "succeeded"
                _register_voice_artifacts(db, job_id, pair.pair_index, base, out_dir)
            elif skip_merge and result:
                pair.step2_status = "succeeded"
                _register_voice_artifacts(db, job_id, pair.pair_index, base, out_dir)
            else:
                pair.step2_status = "failed"
                pair.step2_error = "TTS or merge failed"
        except JobCancelled:
            _finalize_step2_cancelled(db, job_id, pairs)
            return
        except Exception as e:
            logger.exception("Voice Class Step 2 error")
            pair.step2_status = "failed"
            pair.step2_error = str(e)
            append_log(db, job_id, f"pair {pair.pair_index}: ERROR {e}", pair.pair_index)

        db.commit()
        if _scalar_cancel_requested(db, job_id):
            _finalize_step2_cancelled(db, job_id, pairs)
            return
        if i < len(pairs) - 1 and delay_seconds > 0:
            time.sleep(delay_seconds)


def run_voice_class_step2_job(job_id: str, pair_indices: Optional[List[int]] = None) -> None:
    db = SessionLocal()
    try:
        job = db.query(Job).filter(Job.id == job_id).one_or_none()
        if not job:
            return

        job.status = "running"
        if not job.started_at:
            job.started_at = datetime.utcnow()
        db.commit()
        append_log(db, job_id, "Voice Class Step 2 started (Gemini TTS + merge).", None)

        pairs = _load_pairs(db, job_id, pair_indices)
        if job.cancel_requested:
            _finalize_step2_cancelled(db, job_id, pairs)
            return

        _run_voice_class_step2_pairs(db, job_id, pairs)

        pairs = _load_pairs(db, job_id, pair_indices)
        any_failed = any(p.step2_status == "failed" for p in pairs)
        job = db.query(Job).filter(Job.id == job_id).one()
        job.status = "failed" if any_failed else "succeeded"
        job.error_summary = "One or more pairs failed Step 2" if any_failed else None
        job.finished_at = datetime.utcnow()
        db.commit()
        append_log(db, job_id, "--- Voice Class Step 2 finished ---", None)
        notify_step2_finished(db, job, pairs)
    except Exception as e:
        logger.exception("run_voice_class_step2_job")
        try:
            job = db.query(Job).filter(Job.id == job_id).one_or_none()
            if job:
                job.status = "failed"
                job.error_summary = str(e)
                job.finished_at = datetime.utcnow()
                db.commit()
                notify_job_crash(db, job, "Voice Class Step 2", str(e))
        except Exception:
            db.rollback()
    finally:
        db.close()


def run_voice_class_regenerate_segment(
    job_id: str,
    pair_index: int,
    segment_id: int,
) -> None:
    db = SessionLocal()
    try:
        job = db.query(Job).filter(Job.id == job_id).one_or_none()
        if not job:
            return
        job.status = "running"
        job.cancel_requested = False
        db.commit()

        pair = (
            db.query(JobPair)
            .filter(JobPair.job_id == job_id, JobPair.pair_index == pair_index)
            .one_or_none()
        )
        if not pair:
            return

        _run_voice_class_step2_pairs(
            db,
            job_id,
            [pair],
            segment_indices_by_pair={pair_index: [segment_id]},
            skip_merge=False,
        )

        pair = (
            db.query(JobPair)
            .filter(JobPair.job_id == job_id, JobPair.pair_index == pair_index)
            .one_or_none()
        )
        job = db.query(Job).filter(Job.id == job_id).one()
        job.status = "succeeded" if pair and pair.step2_status == "succeeded" else "failed"
        job.finished_at = datetime.utcnow()
        db.commit()
    finally:
        db.close()


def run_voice_class_merge_only(job_id: str, pair_index: int) -> None:
    from stage_voice_processor import StageVoiceProcessor

    db = SessionLocal()
    try:
        base = job_root(job_id)
        script_path = _find_voice_script(base, pair_index)
        if not script_path:
            return
        out_dir = pair_output(job_id, pair_index)
        intro = os.path.join(SONGS_DIR, "a_int.mp3")
        outro = os.path.join(SONGS_DIR, "a_out.mp3")
        processor = StageVoiceProcessor(None)
        result = processor.merge_existing_segments(script_path, out_dir, intro, outro)
        if result:
            _register_voice_artifacts(db, job_id, pair_index, base, out_dir)
            db.commit()
    finally:
        db.close()
