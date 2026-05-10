"""Default Test Bank (Stage V) Step 1 / Step 2 prompts — canonical copy lives in `prompts.json`."""

from __future__ import annotations

import json
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parents[1]

_STEP1_KEY = "Test Bank Generation - Step 1 Prompt"
_STEP2_KEY = "Test Bank Generation - Step 2 Prompt"
_PRE_OCR_KEY = "Pre OCR Topic"
_OCR_EXTRACTION_KEY = "OCR Extraction Prompt"
_DOCUMENT_PROCESSING_KEY = "Document Processing Prompt"
_IMAGE_NOTES_KEY = "Image Notes Prompt"
_TABLE_NOTES_KEY = "Table Notes Prompt"


def _read_prompts() -> dict:
    p = _PROJECT_ROOT / "prompts.json"
    return json.loads(p.read_text(encoding="utf-8"))


def get_default_step1_prompt() -> str:
    return _read_prompts()[_STEP1_KEY].strip()


def get_default_step2_prompt() -> str:
    return _read_prompts()[_STEP2_KEY].strip()


def get_default_pre_ocr_prompt() -> str:
    return _read_prompts()[_PRE_OCR_KEY].strip()


def get_default_ocr_extraction_prompt() -> str:
    return _read_prompts()[_OCR_EXTRACTION_KEY].strip()


def get_default_document_processing_prompt() -> str:
    return _read_prompts()[_DOCUMENT_PROCESSING_KEY].strip()


def get_default_image_notes_prompt() -> str:
    return _read_prompts()[_IMAGE_NOTES_KEY].strip()


def get_default_table_notes_prompt() -> str:
    """Prefer prompts.json 'Table Notes Prompt'; add that key for a dedicated TA prompt."""
    data = _read_prompts()
    raw = data.get(_TABLE_NOTES_KEY)
    if raw and str(raw).strip():
        return str(raw).strip()
    return data[_IMAGE_NOTES_KEY].strip()


DEFAULT_TEST_BANK_STEP1_PROMPT = get_default_step1_prompt()
DEFAULT_TEST_BANK_STEP2_PROMPT = get_default_step2_prompt()
