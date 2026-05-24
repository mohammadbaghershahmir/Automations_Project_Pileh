"""Web job types that convert Table Notes JSON to Word (.docx)."""

from __future__ import annotations

JSON_TO_WORD_JOB_TYPES = frozenset({"table_notes_json_to_word"})

# Legacy job type from earlier releases (same runner).
JSON_TO_WORD_JOB_TYPES_LEGACY = frozenset({"document_processing_json_to_word"})

JSON_TO_WORD_JOB_LABELS = {
    "table_notes_json_to_word": "Table Notes → Word",
    "document_processing_json_to_word": "Table Notes → Word",
}
