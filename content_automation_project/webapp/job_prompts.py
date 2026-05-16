"""Job-scoped LLM prompts: keys in Job.config_json, defaults from prompts.json."""

from __future__ import annotations

from typing import Any, Dict, List, TypedDict

from webapp.default_prompts import (
    get_default_document_processing_prompt,
    get_default_image_notes_prompt,
    get_default_flashcard_prompt,
    get_default_importance_type_prompt,
    get_default_ocr_extraction_prompt,
    get_default_pre_ocr_prompt,
    get_default_step1_prompt,
    get_default_step2_prompt,
    get_default_table_notes_prompt,
)


class PromptFieldRow(TypedDict):
    key: str
    label: str
    value: str


# job.type -> list of (config_json key, short label for the UI)
_PROMPT_KEYS: Dict[str, List[tuple[str, str]]] = {
    "test_bank": [
        ("prompt_1", "Step 1 — combined output prompt"),
        ("prompt_2", "Step 2 — per-topic prompt"),
    ],
    "test_bank_1": [
        ("prompt_1", "Step 1 — combined output prompt"),
    ],
    "test_bank_2": [
        ("prompt_2", "Step 2 — per-topic prompt"),
    ],
    "pre_ocr_topic": [("prompt", "Pre-OCR (topic extraction) prompt")],
    "ocr_extraction": [("prompt", "OCR extraction prompt")],
    "document_processing": [("prompt", "Document processing prompt")],
    "image_notes": [("prompt", "Image notes (Stage E) prompt")],
    "table_notes": [("prompt", "Table notes (Stage TA) prompt")],
    "importance_type": [("prompt", "Importance & Type (Stage J) prompt")],
    "flashcard": [("prompt", "Flashcard Generation (Stage H) prompt")],
}


def job_type_has_editable_prompts(job_type: str) -> bool:
    jt = (job_type or "").strip()
    return bool(_PROMPT_KEYS.get(jt))


def build_prompt_editor_rows(job_type: str, cfg: Dict[str, Any]) -> List[PromptFieldRow]:
    """Values shown in textareas: stored string if non-empty after strip, else canonical default."""
    jt = (job_type or "").strip()
    rows: List[PromptFieldRow] = []
    for key, label in _PROMPT_KEYS.get(jt, []):
        raw = cfg.get(key)
        stored = raw.strip() if isinstance(raw, str) else ""
        if stored:
            value = raw if isinstance(raw, str) else stored
        else:
            value = _default_for_config_key(jt, key)
        rows.append({"key": key, "label": label, "value": value})
    return rows


def _default_for_config_key(job_type: str, key: str) -> str:
    if job_type == "test_bank":
        if key == "prompt_1":
            return get_default_step1_prompt()
        if key == "prompt_2":
            return get_default_step2_prompt()
    if job_type == "test_bank_1":
        if key == "prompt_1":
            return get_default_step1_prompt()
    if job_type == "test_bank_2":
        if key == "prompt_2":
            return get_default_step2_prompt()
    if key != "prompt":
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
    return ""


def apply_submitted_prompts_to_cfg(job_type: str, cfg: Dict[str, Any], form: Any) -> None:
    """Mutate cfg in place from multipart form (Starlette FormData). Empty → key set to '' (runner uses file default)."""
    jt = (job_type or "").strip()
    for key, _label in _PROMPT_KEYS.get(jt, []):
        raw = form.get(key)
        if raw is None:
            continue
        cfg[key] = str(raw).strip()
