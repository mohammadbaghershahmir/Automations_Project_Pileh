"""Config keys for LLM prompts per job.type (shared by job_prompts and system_prompt_defaults)."""

from __future__ import annotations

from typing import Dict, List

PROMPT_KEYS_BY_JOB_TYPE: Dict[str, List[tuple[str, str]]] = {
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
    "chapter_summary": [("prompt", "Chapter Summary (Stage L) prompt")],
    "voice_class": [
        ("prompt_1", "Step 1 — voice script prompt"),
        ("tts_instruction", "Step 2 — Gemini TTS delivery instruction"),
    ],
}
