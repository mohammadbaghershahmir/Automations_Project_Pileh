"""
JSON → Word conversion for Document Processing output (Lesson_file_*.json).

Maps chapter → Heading 1, subchapter → H2, topic → H3, subtopic → H4, subsubtopic → H5.
Point body text is written as normal paragraphs.
"""

from __future__ import annotations

import json
import logging
import os
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

HIERARCHY_FIELDS = (
    ("chapter", 1),
    ("subchapter", 2),
    ("topic", 3),
    ("subtopic", 4),
    ("subsubtopic", 5),
)


class DocumentProcessingJsonError(ValueError):
    """Invalid or unsupported JSON for Document Processing → Word export."""


def _normalize_key_map(row: Dict[str, Any]) -> Dict[str, Any]:
    """Case-insensitive key lookup map for a single row."""
    return {str(k).lower(): v for k, v in row.items()}


def _row_field(row: Dict[str, Any], *names: str) -> str:
    lower = _normalize_key_map(row)
    for name in names:
        val = lower.get(name.lower())
        if val is not None and str(val).strip():
            return str(val).strip()
    return ""


def validate_document_processing_json(data: Any) -> Tuple[List[Dict[str, Any]], Optional[Dict[str, Any]]]:
    """
    Validate Document Processing final JSON and return the points list.

    Requires top-level ``points`` (non-empty list of dicts).
    Returns (points, metadata) where metadata may be None.
    """
    if not isinstance(data, dict):
        raise DocumentProcessingJsonError("JSON root must be an object with a 'points' array.")

    points = data.get("points")
    if points is None:
        raise DocumentProcessingJsonError(
            "Missing top-level 'points' array. Upload Lesson_file_*.json from Document Processing."
        )
    if not isinstance(points, list):
        raise DocumentProcessingJsonError("'points' must be a JSON array.")
    if not points:
        raise DocumentProcessingJsonError("'points' array is empty.")

    normalized: List[Dict[str, Any]] = []
    for i, item in enumerate(points):
        if not isinstance(item, dict):
            raise DocumentProcessingJsonError(f"points[{i}] must be an object.")
        normalized.append(item)

    meta = data.get("metadata")
    metadata = meta if isinstance(meta, dict) else None
    if metadata is not None:
        status = str(metadata.get("processing_status") or "").strip()
        if status and status != "completed":
            logger.warning(
                "Document Processing metadata.processing_status=%r (expected 'completed')",
                status,
            )

    return normalized, metadata


def convert_points_to_docx(points: List[Dict[str, Any]], output_path: str) -> bool:
    """Build a .docx from flat points rows; emit headings only when hierarchy values change."""
    try:
        from docx import Document
    except ImportError as e:
        logger.error("python-docx is required for JSON to Word: %s", e)
        return False

    doc = Document()
    last: Dict[str, str] = {field: "" for field, _ in HIERARCHY_FIELDS}
    paragraphs_added = 0

    field_names = [f for f, _ in HIERARCHY_FIELDS]

    for row in points:
        for field, level in HIERARCHY_FIELDS:
            value = _row_field(row, field)
            if value and value != last[field]:
                doc.add_heading(value, level=level)
                last[field] = value
                idx = field_names.index(field)
                for reset_field in field_names[idx + 1 :]:
                    last[reset_field] = ""

        body = _row_field(row, "points", "Points")
        if not body:
            continue
        doc.add_paragraph(body)
        paragraphs_added += 1

    if paragraphs_added == 0:
        logger.error("No point body text found in points rows")
        return False

    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
    doc.save(output_path)
    logger.info("Wrote Word document: %s (%d paragraphs)", output_path, paragraphs_added)
    return True


def convert_json_file_to_docx(json_path: str, docx_path: str) -> bool:
    """Read a Document Processing JSON file and write a .docx."""
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
        points, _meta = validate_document_processing_json(data)
    except DocumentProcessingJsonError as e:
        logger.error("%s", e)
        return False

    return convert_points_to_docx(points, docx_path)
