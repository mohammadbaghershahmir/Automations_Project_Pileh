"""
JSON → Word conversion for lesson-style JSON (Document Processing and related outputs).

Accepts top-level ``points`` (Document Processing) or ``data`` (e.g. filepic, table notes).
Maps chapter → Heading 1, subchapter → H2, topic → H3, subtopic → H4, subsubtopic → H5.
Body text from ``points``/``Points``, or ``caption`` (+ optional ``point_text`` prefix).
"""

from __future__ import annotations

import json
import logging
import os
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

# Must support Arabic/Persian; Heading 4 default theme fonts often render as □□□.
DOCX_FONT_NAME = "Arial"

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


def _extract_rows_array(data: Dict[str, Any]) -> Tuple[List[Any], str]:
    """Return (rows, source_key) from top-level points or data."""
    points = data.get("points")
    if points is not None:
        return points, "points"
    rows = data.get("data")
    if rows is not None:
        return rows, "data"
    return None, ""  # type: ignore[return-value]


def validate_document_processing_json(data: Any) -> Tuple[List[Dict[str, Any]], Optional[Dict[str, Any]]]:
    """
    Validate lesson JSON and return the row list.

    Requires top-level ``points`` (Document Processing) or ``data`` (filepic, table notes, etc.).
    Returns (rows, metadata) where metadata may be None.
    """
    if not isinstance(data, dict):
        raise DocumentProcessingJsonError(
            "JSON root must be an object with a 'points' or 'data' array."
        )

    raw_rows, source_key = _extract_rows_array(data)
    if raw_rows is None:
        stage = ""
        meta = data.get("metadata")
        if isinstance(meta, dict):
            stage = str(meta.get("stage") or "").strip()
        hint = f" (metadata.stage={stage!r})" if stage else ""
        raise DocumentProcessingJsonError(
            "Missing top-level 'points' or 'data' array. "
            "Use Document Processing output (points) or lesson JSON with chapter/subchapter/topic rows "
            f"(data){hint}."
        )
    if not isinstance(raw_rows, list):
        raise DocumentProcessingJsonError(f"'{source_key}' must be a JSON array.")
    if not raw_rows:
        raise DocumentProcessingJsonError(f"'{source_key}' array is empty.")

    normalized: List[Dict[str, Any]] = []
    for i, item in enumerate(raw_rows):
        if not isinstance(item, dict):
            raise DocumentProcessingJsonError(f"{source_key}[{i}] must be an object.")
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


def _has_arabic_script(text: str) -> bool:
    for ch in text:
        o = ord(ch)
        if (
            0x0600 <= o <= 0x06FF
            or 0x0750 <= o <= 0x077F
            or 0x08A0 <= o <= 0x08FF
            or 0xFB50 <= o <= 0xFDFF
            or 0xFE70 <= o <= 0xFEFF
        ):
            return True
    return False


def _apply_run_font(run: Any, font_name: str = DOCX_FONT_NAME) -> None:
    from docx.oxml import OxmlElement
    from docx.oxml.ns import qn

    run.font.name = font_name
    r_pr = run._element.get_or_add_rPr()
    existing = r_pr.find(qn("w:rFonts"))
    if existing is not None:
        r_pr.remove(existing)
    r_fonts = OxmlElement("w:rFonts")
    r_fonts.set(qn("w:ascii"), font_name)
    r_fonts.set(qn("w:hAnsi"), font_name)
    r_fonts.set(qn("w:cs"), font_name)
    r_fonts.set(qn("w:eastAsia"), font_name)
    r_pr.insert(0, r_fonts)


def _apply_paragraph_bidi(paragraph: Any, rtl: bool) -> None:
    if not rtl:
        return
    from docx.oxml import OxmlElement
    from docx.oxml.ns import qn

    p_pr = paragraph._element.get_or_add_pPr()
    if p_pr.find(qn("w:bidi")) is None:
        p_pr.append(OxmlElement("w:bidi"))
    jc = p_pr.find(qn("w:jc"))
    if jc is None:
        jc = OxmlElement("w:jc")
        p_pr.append(jc)
    jc.set(qn("w:val"), "right")


def _style_paragraph(paragraph: Any, font_name: str = DOCX_FONT_NAME) -> None:
    text = paragraph.text or ""
    rtl = _has_arabic_script(text)
    _apply_paragraph_bidi(paragraph, rtl)
    for run in paragraph.runs:
        _apply_run_font(run, font_name)


def _configure_document_fonts(doc: Any, font_name: str = DOCX_FONT_NAME) -> None:
    """Heading 4 (and some themes) use Latin-only fonts — set Arial on Normal + Heading 1–5."""
    from docx.oxml import OxmlElement
    from docx.oxml.ns import qn

    def _style_font(style: Any) -> None:
        style.font.name = font_name
        r_pr = style.element.rPr
        if r_pr is None:
            return
        existing = r_pr.find(qn("w:rFonts"))
        if existing is not None:
            r_pr.remove(existing)
        r_fonts = OxmlElement("w:rFonts")
        r_fonts.set(qn("w:ascii"), font_name)
        r_fonts.set(qn("w:hAnsi"), font_name)
        r_fonts.set(qn("w:cs"), font_name)
        r_fonts.set(qn("w:eastAsia"), font_name)
        r_pr.insert(0, r_fonts)

    _style_font(doc.styles["Normal"])
    for level in range(1, 10):
        name = f"Heading {level}"
        try:
            _style_font(doc.styles[name])
        except KeyError:
            break


def _add_heading_styled(doc: Any, text: str, level: int) -> None:
    paragraph = doc.add_heading(text, level=level)
    _style_paragraph(paragraph)


def _add_body_paragraph(doc: Any, text: str) -> None:
    paragraph = doc.add_paragraph(text)
    _style_paragraph(paragraph)


def _row_body(row: Dict[str, Any]) -> str:
    """Lesson point text, or image/table note caption (optional point_text label)."""
    body = _row_field(row, "points", "Points")
    if body:
        return body
    caption = _row_field(row, "caption")
    point_text = _row_field(row, "point_text")
    if caption and point_text:
        return f"{point_text}\n{caption}"
    return caption or point_text


def convert_points_to_docx(points: List[Dict[str, Any]], output_path: str) -> bool:
    """Build a .docx from flat points rows; emit headings only when hierarchy values change."""
    try:
        from docx import Document
    except ImportError as e:
        logger.error("python-docx is required for JSON to Word: %s", e)
        return False

    doc = Document()
    _configure_document_fonts(doc)
    last: Dict[str, str] = {field: "" for field, _ in HIERARCHY_FIELDS}
    paragraphs_added = 0

    field_names = [f for f, _ in HIERARCHY_FIELDS]

    for row in points:
        for field, level in HIERARCHY_FIELDS:
            value = _row_field(row, field)
            if value and value != last[field]:
                _add_heading_styled(doc, value, level)
                last[field] = value
                idx = field_names.index(field)
                for reset_field in field_names[idx + 1 :]:
                    last[reset_field] = ""

        body = _row_body(row)
        if not body:
            continue
        _add_body_paragraph(doc, body)
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
