"""
JSON → Word conversion for Table Notes Generation output (ta*.json).

Headings only (no chapter, no italic): subchapter H1, topic H2, subtopic H3, subsubtopic H4.
Body from each row's ``points`` field.
"""

from __future__ import annotations

import json
import logging
import os
import re
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

# field_name → Word heading level (1–4)
HIERARCHY_HEADINGS: List[Tuple[str, int]] = [
    ("subchapter", 1),
    ("topic", 2),
    ("subtopic", 3),
    ("subsubtopic", 4),
]

HIERARCHY_FIELD_NAMES = [name for name, _ in HIERARCHY_HEADINGS]

_TA_FILENAME_RE = re.compile(r"^ta\d{6}_.+\.json$", re.IGNORECASE)


class TableNotesJsonError(ValueError):
    """Invalid or unsupported JSON for Table Notes → Word export."""


class DocumentProcessingJsonError(TableNotesJsonError):
    """Backward-compatible alias for runners that still import this name."""


def _normalize_key_map(row: Dict[str, Any]) -> Dict[str, Any]:
    return {str(k).lower(): v for k, v in row.items()}


def _row_field(row: Dict[str, Any], *names: str) -> str:
    lower = _normalize_key_map(row)
    for name in names:
        val = lower.get(name.lower())
        if val is not None and str(val).strip():
            return str(val).strip()
    return ""


def _looks_like_table_notes_filename(path_or_name: str) -> bool:
    base = os.path.basename(path_or_name or "")
    return bool(_TA_FILENAME_RE.match(base))


def validate_table_notes_json(
    data: Any,
    *,
    source_filename: str = "",
) -> Tuple[List[Dict[str, Any]], Optional[Dict[str, Any]]]:
    """
    Validate Table Notes (Stage TA) JSON and return the ``data`` row list.

    Requires top-level ``data`` (non-empty). Rejects bare Document Processing ``points`` files.
    """
    if not isinstance(data, dict):
        raise TableNotesJsonError("JSON root must be an object with a 'data' array.")

    if data.get("points") is not None and data.get("data") is None:
        raise TableNotesJsonError(
            "This file has top-level 'points' (Document Processing). "
            "Upload Table Notes output instead: ta{book}{chapter}_*.json with a 'data' array."
        )

    raw_rows = data.get("data")
    if raw_rows is None:
        stage = ""
        meta = data.get("metadata")
        if isinstance(meta, dict):
            stage = str(meta.get("stage") or "").strip()
        hint = f" (metadata.stage={stage!r})" if stage else ""
        raise TableNotesJsonError(
            "Missing top-level 'data' array. Upload Table Notes Generation output "
            f"(e.g. ta105003_Lesson_file_....json){hint}."
        )
    if not isinstance(raw_rows, list):
        raise TableNotesJsonError("'data' must be a JSON array.")
    if not raw_rows:
        raise TableNotesJsonError("'data' array is empty.")

    meta = data.get("metadata")
    metadata = meta if isinstance(meta, dict) else None
    if metadata is not None:
        stage = str(metadata.get("stage") or "").strip().upper()
        if stage and stage != "TA":
            raise TableNotesJsonError(
                f"Not Table Notes output (metadata.stage={metadata.get('stage')!r}). "
                "Upload ta{book}{chapter}_*.json from Table Notes Generation."
            )
        status = str(metadata.get("processing_status") or "").strip()
        if status and status != "completed":
            logger.warning(
                "Table Notes metadata.processing_status=%r (expected 'completed')",
                status,
            )
    elif source_filename and not _looks_like_table_notes_filename(source_filename):
        raise TableNotesJsonError(
            f"Filename {os.path.basename(source_filename)!r} does not match "
            "Table Notes pattern ta{book}{chapter}_*.json."
        )

    normalized: List[Dict[str, Any]] = []
    for i, item in enumerate(raw_rows):
        if not isinstance(item, dict):
            raise TableNotesJsonError(f"data[{i}] must be an object.")
        normalized.append(item)

    return normalized, metadata


def validate_document_processing_json(
    data: Any,
    *,
    source_filename: str = "",
) -> Tuple[List[Dict[str, Any]], Optional[Dict[str, Any]]]:
    """Alias: Table Notes validation (legacy import name)."""
    return validate_table_notes_json(data, source_filename=source_filename)


def _row_body(row: Dict[str, Any]) -> str:
    return _row_field(row, "points", "Points")


def convert_points_to_docx(points: List[Dict[str, Any]], output_path: str) -> bool:
    """Build a .docx from Table Notes ``data`` rows."""
    try:
        from docx import Document
    except ImportError as e:
        logger.error("python-docx is required for JSON to Word: %s", e)
        return False

    doc = Document()
    last: Dict[str, str] = {name: "" for name in HIERARCHY_FIELD_NAMES}
    paragraphs_added = 0

    for row in points:
        for field, level in HIERARCHY_HEADINGS:
            value = _row_field(row, field)
            if not value or value == last[field]:
                continue
            doc.add_heading(value, level=level)
            last[field] = value
            idx = HIERARCHY_FIELD_NAMES.index(field)
            for reset_field in HIERARCHY_FIELD_NAMES[idx + 1 :]:
                last[reset_field] = ""

        body = _row_body(row)
        if not body:
            continue
        doc.add_paragraph(body)
        paragraphs_added += 1

    if paragraphs_added == 0:
        logger.error("No point body text found in data rows")
        return False

    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
    doc.save(output_path)
    logger.info("Wrote Word document: %s (%d body paragraphs)", output_path, paragraphs_added)
    return True


def convert_json_file_to_docx(json_path: str, docx_path: str) -> bool:
    """Read a Table Notes JSON file and write a .docx."""
    try:
        with open(json_path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except json.JSONDecodeError as e:
        logger.error("Invalid JSON in %s: %s", json_path, e)
        return False
    except OSError as e:
        logger.error("Cannot read %s: %s", json_path, e)
        return False

    try:
        rows, _meta = validate_table_notes_json(data, source_filename=json_path)
    except TableNotesJsonError as e:
        logger.error("%s", e)
        return False

    return convert_points_to_docx(rows, docx_path)
