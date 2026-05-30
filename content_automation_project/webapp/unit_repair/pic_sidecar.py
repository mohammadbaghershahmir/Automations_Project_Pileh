"""Helpers to keep filepic/tablepic sidecars in sync when a unit is regenerated."""

from __future__ import annotations

import json
import os
from typing import Any, Dict, List, Tuple


def replace_topic_rows(
    rows: List[Dict[str, Any]],
    topic_name: str,
    new_rows: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Replace all rows for one topic in place (keeps order of other topics)."""
    topic = (topic_name or "").strip()
    result: List[Dict[str, Any]] = []
    inserted = False
    for row in rows:
        if not isinstance(row, dict):
            result.append(row)
            continue
        if (row.get("topic") or "").strip() == topic:
            if not inserted:
                result.extend(new_rows)
                inserted = True
            continue
        result.append(row)
    if not inserted:
        result.extend(new_rows)
    return result


def process_pic_records_to_notes(
    records: List[Dict[str, Any]],
    book_id: int,
    chapter_id: int,
    start_index: int,
) -> Tuple[List[Dict[str, Any]], int]:
    """Same shape as Stage E / Stage TA post-processing (caption removed, Points + PointId)."""
    processed: List[Dict[str, Any]] = []
    idx = start_index
    for record in records:
        if not isinstance(record, dict):
            continue
        out: Dict[str, Any] = {}
        for key, val in record.items():
            if key not in ("caption", "point_text"):
                out[key] = val
        out["Points"] = record.get("point_text", "")
        out["PointId"] = f"{book_id:03d}{chapter_id:03d}{idx:04d}"
        processed.append(out)
        idx += 1
    return processed, idx


def load_pic_sidecar(path: str) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    if not os.path.isfile(path):
        return {}, []
    with open(path, "r", encoding="utf-8") as f:
        doc = json.load(f)
    if isinstance(doc, list):
        return {}, doc
    rows = doc.get("data") if isinstance(doc, dict) else None
    if rows is None and isinstance(doc, dict):
        rows = doc.get("points")
    if not isinstance(rows, list):
        rows = []
    meta = doc.get("metadata") if isinstance(doc, dict) else {}
    return meta if isinstance(meta, dict) else {}, rows


def save_pic_sidecar(
    processor: Any,
    path: str,
    rows: List[Dict[str, Any]],
    meta_base: Dict[str, Any],
    stage_label: str,
) -> None:
    meta = dict(meta_base)
    meta["total_records"] = len(rows)
    meta["records_with_caption"] = sum(
        1 for r in rows if isinstance(r, dict) and r.get("caption")
    )
    processor.save_json_file(rows, path, meta, stage_label)


def notes_start_index(source_points: List[Dict[str, Any]]) -> int:
    """First PointId index for image/table notes (after last source point)."""
    last_id = ""
    for p in source_points:
        if isinstance(p, dict):
            last_id = (p.get("PointId") or "").strip() or last_id
    if last_id and len(last_id) >= 10 and last_id.isdigit():
        return int(last_id[6:10]) + 1
    return 1


def apply_regenerate_to_merged_files(
    processor: Any,
    *,
    source_points: List[Dict[str, Any]],
    pic_path: str,
    main_path: str,
    topic_name: str,
    new_pic_rows: List[Dict[str, Any]],
    pic_stage_label: str,
    notes_count_key: str,
    source_count_key: str,
    first_note_id_key: str,
    pic_meta_prefix: str,
) -> None:
    """
    Update sidecar (filepic/tablepic), then rebuild merged main JSON (e*/ta*).
    """
    book_id, chapter_id = processor.extract_book_chapter_from_pointid(
        (source_points[0].get("PointId") or "").strip()
    )
    start_idx = notes_start_index(source_points)

    pic_meta, pic_rows = load_pic_sidecar(pic_path)
    pic_rows = replace_topic_rows(pic_rows, topic_name, new_pic_rows)
    save_pic_sidecar(processor, pic_path, pic_rows, pic_meta, pic_stage_label)

    processed, last_idx = process_pic_records_to_notes(
        pic_rows, book_id, chapter_id, start_idx
    )
    merged = list(source_points) + processed

    with open(main_path, "r", encoding="utf-8") as f:
        main_doc = json.load(f)
    if isinstance(main_doc, dict):
        main_doc["data"] = merged
        meta = main_doc.setdefault("metadata", {})
        if isinstance(meta, dict):
            meta[notes_count_key] = len(processed)
            meta[source_count_key] = len(source_points)
            if processed:
                meta[first_note_id_key] = processed[0].get("PointId")
            meta["last_point_id"] = f"{book_id:03d}{chapter_id:03d}{last_idx - 1:04d}"
            meta[f"{pic_meta_prefix}_json_file"] = os.path.basename(pic_path)
            meta[f"{pic_meta_prefix}_json_path"] = pic_path
    with open(main_path, "w", encoding="utf-8") as f:
        json.dump(main_doc, f, ensure_ascii=False, indent=2)
