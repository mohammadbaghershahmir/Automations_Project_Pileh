"""Job-scoped LLM prompts: keys in Job.config_json; system defaults from DB."""

from __future__ import annotations

from typing import Any, Dict, List, TypedDict

from sqlalchemy.orm import Session

from webapp.prompt_keys import PROMPT_KEYS_BY_JOB_TYPE
from webapp.system_prompt_defaults import resolve_prompt_for_job


class PromptFieldRow(TypedDict):
    key: str
    label: str
    value: str


def job_type_has_editable_prompts(job_type: str) -> bool:
    jt = (job_type or "").strip()
    return bool(PROMPT_KEYS_BY_JOB_TYPE.get(jt))


def build_prompt_editor_rows(
    db: Session,
    job_type: str,
    cfg: Dict[str, Any],
) -> List[PromptFieldRow]:
    """Values shown in textareas: job config if set, else system default from DB."""
    jt = (job_type or "").strip()
    rows: List[PromptFieldRow] = []
    for key, label in PROMPT_KEYS_BY_JOB_TYPE.get(jt, []):
        value = resolve_prompt_for_job(db, jt, cfg, key)
        rows.append({"key": key, "label": label, "value": value})
    return rows


def apply_submitted_prompts_to_cfg(job_type: str, cfg: Dict[str, Any], form: Any) -> None:
    """Mutate cfg in place from multipart form. Empty → key cleared (worker uses system default from DB)."""
    jt = (job_type or "").strip()
    for key, _label in PROMPT_KEYS_BY_JOB_TYPE.get(jt, []):
        raw = form.get(key)
        if raw is None:
            continue
        cfg[key] = str(raw).strip()
