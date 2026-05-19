"""Web job types that convert uploaded stage JSON to CSV (no LLM)."""

from __future__ import annotations

from typing import Any, Dict, Optional

JSON_TO_CSV_JOB_TYPES = frozenset(
    {
        "chapter_summary_json_to_csv",
        "image_catalog_json_to_csv",
        "test_bank_2_json_to_csv",
        "flashcard_json_to_csv",
    }
)

JSON_TO_CSV_STAGE_SPECS: Dict[str, Dict[str, Any]] = {
    "chapter_summary": {
        "job_type": "chapter_summary_json_to_csv",
        "title": "Chapter Summary → CSV",
        "description": (
            "Upload <strong>Chapter Summary</strong> output (<code>o*.json</code>). "
            "Each file becomes one pair; row data is written to CSV with the <code>;;;</code> delimiter."
        ),
        "form_action": "/jobs/chapter-summary/json-to-csv",
        "new_path": "/chapter-summary/json-to-csv/new",
        "file_hint": "Chapter Summary JSON (o*.json)",
        "placeholder": "e.g. Ch.3 chapter summary CSV",
    },
    "image_catalog": {
        "job_type": "image_catalog_json_to_csv",
        "title": "Image Catalog → CSV",
        "description": (
            "Upload <strong>Image File Catalog</strong> output (<code>f_*.json</code> or <code>f*.json</code>). "
            "Each file becomes one pair; row data is written to CSV with the <code>;;;</code> delimiter."
        ),
        "form_action": "/jobs/image-catalog/json-to-csv",
        "new_path": "/image-catalog/json-to-csv/new",
        "file_hint": "Image catalog JSON (f_*.json)",
        "placeholder": "e.g. Ch.3 catalog CSV",
    },
    "test_bank_2": {
        "job_type": "test_bank_2_json_to_csv",
        "title": "Test Bank 2 → CSV",
        "description": (
            "Upload <strong>Test Bank 2</strong> output (<code>b*.json</code>). "
            "Each file becomes one pair; row data is written to CSV with the <code>;;;</code> delimiter."
        ),
        "form_action": "/jobs/test-bank-2/json-to-csv",
        "new_path": "/test-bank-2/json-to-csv/new",
        "file_hint": "Test Bank 2 JSON (b*.json)",
        "placeholder": "e.g. Ch.3 test bank CSV",
    },
    "flashcard": {
        "job_type": "flashcard_json_to_csv",
        "title": "Flashcard → CSV",
        "description": (
            "Upload <strong>Flashcard Generation</strong> output (<code>ac*.json</code>). "
            "Row data is written to CSV; five extra columns are appended at the end "
            "(<strong>Card ID</strong>, <strong>Deck</strong>, <strong>Tags</strong>, "
            "<strong>Flag</strong>, <strong>Card State</strong>) — left empty for website import."
        ),
        "form_action": "/jobs/flashcard/json-to-csv",
        "new_path": "/flashcard/json-to-csv/new",
        "file_hint": "Flashcard JSON (ac*.json)",
        "placeholder": "e.g. Ch.3 flashcards CSV",
        "flashcard_import_columns": True,
    },
}

JSON_TO_CSV_JOB_LABELS: Dict[str, str] = {
    spec["job_type"]: spec["title"] for spec in JSON_TO_CSV_STAGE_SPECS.values()
}

# Order shown on the unified JSON → CSV page.
JSON_TO_CSV_CONVERSION_ORDER = (
    "chapter_summary",
    "image_catalog",
    "test_bank_2",
    "flashcard",
)


def job_type_for_conversion_key(conversion_key: str) -> Optional[str]:
    spec = JSON_TO_CSV_STAGE_SPECS.get((conversion_key or "").strip())
    return spec["job_type"] if spec else None


def json_to_csv_page_context() -> Dict[str, Any]:
    """Template context for /json-to-csv/new."""
    options = []
    hints: Dict[str, Dict[str, str]] = {}
    for key in JSON_TO_CSV_CONVERSION_ORDER:
        spec = JSON_TO_CSV_STAGE_SPECS[key]
        label = spec["title"].replace(" → CSV", "")
        options.append({"key": key, "label": label})
        hints[key] = {
            "description": spec["description"],
            "file_hint": spec["file_hint"],
            "placeholder": spec.get("placeholder", "e.g. Ch.3 export CSV"),
        }
    return {
        "conversion_options": options,
        "default_conversion": "chapter_summary",
        "conversion_hints": hints,
    }


def json_to_csv_uses_flashcard_trailing_columns(job_type: str) -> bool:
    return (job_type or "").strip() == "flashcard_json_to_csv"
