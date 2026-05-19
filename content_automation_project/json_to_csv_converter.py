"""
JSON → CSV conversion (;;; delimiter by default).

Flashcard exports (ac*.json): append five empty import columns for the target website;
all other columns are filled from JSON row data.
"""

from __future__ import annotations

import json
import logging
import os
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

FLASHCARD_CSV_TRAILING_COLUMNS = (
    "Card ID",
    "Deck",
    "Tags",
    "Flag",
    "Card State",
)


def is_flashcard_json_basename(name: str) -> bool:
    """True for Stage H flashcard output filenames (ac{book}{chapter}...json)."""
    base = os.path.basename(name or "").lower()
    return base.endswith(".json") and base.startswith("ac")


def resolve_flashcard_trailing_columns(mode: str, basename: str) -> bool:
    """Map config flashcard_columns: auto | always | never."""
    m = (mode or "auto").strip().lower()
    if m == "always":
        return True
    if m == "never":
        return False
    return is_flashcard_json_basename(basename)


def _flatten_nested_structure(data: Any) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []

    if isinstance(data, list):
        for item in data:
            if isinstance(item, dict):
                if "chapters" in item:
                    for chapter in item.get("chapters", []):
                        chapter_name = chapter.get("chapter", "")
                        for subchapter in chapter.get("subchapters", []):
                            subchapter_name = subchapter.get("subchapter", "")
                            for topic in subchapter.get("topics", []):
                                topic_name = topic.get("topic", "")
                                for extraction in topic.get("extractions", []):
                                    rows.append(
                                        {
                                            "chapter": chapter_name,
                                            "subchapter": subchapter_name,
                                            "topic": topic_name,
                                            **extraction,
                                        }
                                    )
                                if not topic.get("extractions") and topic:
                                    rows.append(
                                        {
                                            "chapter": chapter_name,
                                            "subchapter": subchapter_name,
                                            "topic": topic_name,
                                            **{k: v for k, v in topic.items() if k != "extractions"},
                                        }
                                    )
                            if not subchapter.get("topics") and subchapter:
                                rows.append(
                                    {
                                        "chapter": chapter_name,
                                        "subchapter": subchapter_name,
                                        **{k: v for k, v in subchapter.items() if k != "topics"},
                                    }
                                )
                        if not chapter.get("subchapters") and chapter:
                            rows.append(
                                {
                                    "chapter": chapter_name,
                                    **{k: v for k, v in chapter.items() if k != "subchapters"},
                                }
                            )
                else:
                    rows.append(item)
    elif isinstance(data, dict):
        return [data]

    return rows


def extract_rows_from_json(json_data: Any) -> List[Dict[str, Any]]:
    if isinstance(json_data, list):
        if json_data and isinstance(json_data[0], dict) and "chapters" in json_data[0]:
            return _flatten_nested_structure(json_data)
        return json_data

    if isinstance(json_data, dict):
        if "chapters" in json_data:
            chapters_data = json_data["chapters"]
            if isinstance(chapters_data, list):
                if (
                    chapters_data
                    and isinstance(chapters_data[0], dict)
                    and "subchapters" in chapters_data[0]
                ):
                    return _flatten_nested_structure(chapters_data)
                return chapters_data
            if isinstance(chapters_data, dict):
                if "rows" in chapters_data:
                    return chapters_data["rows"]
                if "data" in chapters_data:
                    return chapters_data["data"]
                return [chapters_data]

        if "data" in json_data:
            data = json_data["data"]
            if isinstance(data, list) and data and isinstance(data[0], dict) and "chapters" in data[0]:
                return _flatten_nested_structure(data)
            return data if isinstance(data, list) else [data]

        if "points" in json_data:
            pts = json_data["points"]
            return pts if isinstance(pts, list) else [pts]

        if "rows" in json_data:
            rws = json_data["rows"]
            return rws if isinstance(rws, list) else [rws]

    return []


def _normalize_rows(rows: List[Dict[str, Any]]) -> tuple[List[str], List[Dict[str, Any]]]:
    all_headers_dict: Dict[str, str] = {}
    header_counts: Dict[str, Dict[str, int]] = {}

    for row in rows:
        for key in row.keys():
            key_lower = key.lower()
            if key_lower not in header_counts:
                header_counts[key_lower] = {}
            if key not in header_counts[key_lower]:
                header_counts[key_lower][key] = 0
            header_counts[key_lower][key] += 1

    for key_lower, variants in header_counts.items():
        most_common = max(variants.items(), key=lambda x: (x[1], x[0]))
        all_headers_dict[key_lower] = most_common[0]

    headers = sorted(all_headers_dict.values())
    if not headers:
        return [], []

    key_mapping: Dict[str, str] = {}
    for key_lower, normalized_key in all_headers_dict.items():
        for original_key in header_counts[key_lower].keys():
            if original_key != normalized_key:
                key_mapping[original_key] = normalized_key

    normalized_rows: List[Dict[str, Any]] = []
    for row in rows:
        normalized_row: Dict[str, Any] = {}
        for key, value in row.items():
            normalized_key = key_mapping.get(key, key)
            if normalized_key in normalized_row:
                if not normalized_row[normalized_key] and value:
                    normalized_row[normalized_key] = value
            else:
                normalized_row[normalized_key] = value
        normalized_rows.append(normalized_row)

    return headers, normalized_rows


def rows_to_csv_text(
    rows: List[Dict[str, Any]],
    *,
    delimiter: str = ";;;",
    flashcard_trailing_columns: bool = False,
) -> Optional[str]:
    rows = [row for row in rows if isinstance(row, dict)]
    if not rows:
        return None

    headers, normalized_rows = _normalize_rows(rows)
    if not headers:
        return None

    if flashcard_trailing_columns:
        for col in FLASHCARD_CSV_TRAILING_COLUMNS:
            if col not in headers:
                headers.append(col)

    csv_lines = [delimiter.join(headers)]
    for row in normalized_rows:
        values = [str(row.get(h, "")) for h in headers]
        if flashcard_trailing_columns:
            for col in FLASHCARD_CSV_TRAILING_COLUMNS:
                if col in headers:
                    idx = headers.index(col)
                    values[idx] = ""
        csv_lines.append(delimiter.join(values))

    return "\n".join(csv_lines)


def convert_json_file_to_csv(
    json_path: str,
    csv_path: str,
    *,
    delimiter: str = ";;;",
    flashcard_trailing_columns: bool = False,
) -> bool:
    try:
        with open(json_path, "r", encoding="utf-8") as f:
            json_data = json.load(f)

        rows = extract_rows_from_json(json_data)
        if not rows:
            logger.warning("No data rows found in %s", json_path)
            return False

        csv_text = rows_to_csv_text(
            rows,
            delimiter=delimiter,
            flashcard_trailing_columns=flashcard_trailing_columns,
        )
        if not csv_text:
            logger.error("No valid dictionary rows in %s", json_path)
            return False

        out_dir = os.path.dirname(csv_path)
        if out_dir:
            os.makedirs(out_dir, exist_ok=True)
        with open(csv_path, "w", encoding="utf-8") as f:
            f.write(csv_text)

        row_count = csv_text.count("\n")
        logger.info(
            "Converted %s → %s (%s data rows, flashcard trailing cols=%s)",
            json_path,
            csv_path,
            max(0, row_count - 1),
            flashcard_trailing_columns,
        )
        return True
    except json.JSONDecodeError as e:
        logger.error("Failed to parse JSON %s: %s", json_path, e)
        return False
    except Exception as e:
        logger.exception("Error converting %s to CSV: %s", json_path, e)
        return False


def convert_json_file_to_csv_text(
    json_path: str,
    *,
    delimiter: str = ";;;",
    flashcard_trailing_columns: bool = False,
) -> Optional[str]:
    try:
        with open(json_path, "r", encoding="utf-8") as f:
            json_data = json.load(f)
        rows = extract_rows_from_json(json_data)
        if not rows:
            return None
        return rows_to_csv_text(
            rows,
            delimiter=delimiter,
            flashcard_trailing_columns=flashcard_trailing_columns,
        )
    except (json.JSONDecodeError, OSError) as e:
        logger.error("convert_json_file_to_csv_text failed for %s: %s", json_path, e)
        return None
