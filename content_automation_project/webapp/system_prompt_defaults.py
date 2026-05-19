"""System-wide default prompts per job type (DB). prompts.json is seed-only."""

from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Any, Dict, Optional

from sqlalchemy.orm import Session

from webapp.database import SessionLocal
from webapp.default_prompts import (
    get_default_chapter_summary_prompt,
    get_default_document_processing_prompt,
    get_default_flashcard_prompt,
    get_default_importance_type_prompt,
    get_default_image_notes_prompt,
    get_default_ocr_extraction_prompt,
    get_default_pre_ocr_prompt,
    get_default_step1_prompt,
    get_default_step2_prompt,
    get_default_table_notes_prompt,
)
from webapp.prompt_keys import PROMPT_KEYS_BY_JOB_TYPE
from webapp.models import Job, SystemPromptDefault

logger = logging.getLogger(__name__)


class PromptNotConfiguredError(RuntimeError):
    """No row in system_prompt_defaults for this job type (seed may have failed)."""


# Where to send the browser after admin saves a global default.
NEW_JOB_PAGE_BY_TYPE: Dict[str, str] = {
    "pre_ocr_topic": "/pre-ocr/new",
    "ocr_extraction": "/ocr-extraction/new",
    "document_processing": "/document-processing/new",
    "image_notes": "/image-notes/new",
    "table_notes": "/table-notes/new",
    "test_bank": "/test-bank-1/new",
    "test_bank_1": "/test-bank-1/new",
    "test_bank_2": "/test-bank-2/new",
    "importance_type": "/importance-type/new",
    "flashcard": "/flashcard/new",
    "chapter_summary": "/chapter-summary/new",
}


def _seed_text_for_key(job_type: str, config_key: str) -> str:
    if config_key == "prompt_1":
        return get_default_step1_prompt()
    if config_key == "prompt_2":
        return get_default_step2_prompt()
    if config_key != "prompt":
        return ""
    if job_type == "pre_ocr_topic":
        return get_default_pre_ocr_prompt()
    if job_type == "ocr_extraction":
        return get_default_ocr_extraction_prompt()
    if job_type == "document_processing":
        return get_default_document_processing_prompt()
    if job_type == "image_notes":
        return get_default_image_notes_prompt()
    if job_type == "table_notes":
        return get_default_table_notes_prompt()
    if job_type == "importance_type":
        return get_default_importance_type_prompt()
    if job_type == "flashcard":
        return get_default_flashcard_prompt()
    if job_type == "chapter_summary":
        return get_default_chapter_summary_prompt()
    return ""


def seed_system_prompt_defaults(db: Session) -> int:
    """Insert missing rows from prompts.json (via default_prompts). Runs once per key at startup."""
    inserted = 0
    for job_type, fields in PROMPT_KEYS_BY_JOB_TYPE.items():
        for config_key, _label in fields:
            exists = (
                db.query(SystemPromptDefault)
                .filter(
                    SystemPromptDefault.job_type == job_type,
                    SystemPromptDefault.config_key == config_key,
                )
                .first()
            )
            if exists is not None:
                continue
            text = _seed_text_for_key(job_type, config_key)
            if not text.strip():
                logger.warning(
                    "Skipping seed for %s/%s: empty text in prompts.json",
                    job_type,
                    config_key,
                )
                continue
            db.add(
                SystemPromptDefault(
                    job_type=job_type,
                    config_key=config_key,
                    prompt_text=text,
                    updated_at=datetime.utcnow(),
                    updated_by_id=None,
                )
            )
            inserted += 1
    if inserted:
        db.commit()
        logger.info("Seeded %s system_prompt_defaults row(s) from prompts.json", inserted)
    return inserted


def get_system_prompt_default(db: Session, job_type: str, config_key: str) -> str:
    jt = (job_type or "").strip()
    key = (config_key or "").strip()
    row = (
        db.query(SystemPromptDefault)
        .filter(
            SystemPromptDefault.job_type == jt,
            SystemPromptDefault.config_key == key,
        )
        .one_or_none()
    )
    if row is None or not (row.prompt_text or "").strip():
        raise PromptNotConfiguredError(
            f"No system prompt configured for job_type={jt!r} config_key={key!r}"
        )
    return row.prompt_text


def set_system_prompt_default(
    db: Session,
    job_type: str,
    config_key: str,
    text: str,
    admin_user_id: int,
) -> None:
    stripped = (text or "").strip()
    if not stripped:
        raise ValueError("Prompt text cannot be empty")
    jt = (job_type or "").strip()
    key = (config_key or "").strip()
    row = (
        db.query(SystemPromptDefault)
        .filter(
            SystemPromptDefault.job_type == jt,
            SystemPromptDefault.config_key == key,
        )
        .one_or_none()
    )
    if row is None:
        row = SystemPromptDefault(
            job_type=jt,
            config_key=key,
            prompt_text=stripped,
            updated_at=datetime.utcnow(),
            updated_by_id=admin_user_id,
        )
        db.add(row)
    else:
        row.prompt_text = stripped
        row.updated_at = datetime.utcnow()
        row.updated_by_id = admin_user_id
    db.commit()


def apply_submitted_system_prompts(
    db: Session,
    job_type: str,
    form: Any,
    admin_user_id: int,
) -> None:
    jt = (job_type or "").strip()
    for config_key, _label in PROMPT_KEYS_BY_JOB_TYPE.get(jt, []):
        raw = form.get(config_key)
        if raw is None:
            continue
        set_system_prompt_default(db, jt, config_key, str(raw), admin_user_id)


def resolve_prompt_for_job(
    db: Session,
    job_type: str,
    cfg: Dict[str, Any],
    config_key: str,
) -> str:
    raw = cfg.get(config_key)
    if isinstance(raw, str) and raw.strip():
        return raw.strip()
    return get_system_prompt_default(db, job_type, config_key)


def resolve_prompt_for_job_id(job_id: str, config_key: str) -> str:
    db = SessionLocal()
    try:
        job = db.query(Job).filter(Job.id == job_id).one_or_none()
        if job is None:
            raise PromptNotConfiguredError(f"Job not found: {job_id}")
        cfg = json.loads(job.config_json or "{}")
        jt = (job.type or "").strip()
        return resolve_prompt_for_job(db, jt, cfg, config_key)
    finally:
        db.close()
