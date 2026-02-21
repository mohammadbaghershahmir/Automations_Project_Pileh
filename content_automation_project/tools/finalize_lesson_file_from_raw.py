#!/usr/bin/env python3
"""
Finalize a partially-written Lesson_file_*.json that still contains raw_responses.

This converts available model responses (raw_responses[*].response_text / .response)
into the final flat `points` structure using ThirdStageConverter._flatten_to_points,
assigns PointId, updates metadata, and removes raw_responses.

Supports partial runs (topics_processed < total_topics).
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime
from typing import Any, Dict, List, Tuple


def _point_id(book_id: int, chapter_id: int, index: int) -> str:
    return f"{book_id:03d}{chapter_id:03d}{index:04d}"


def _load_json(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _write_json(path: str, obj: Dict[str, Any]) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)


def _extract_blocks(mpp, raw_entry: Dict[str, Any]) -> List[Dict[str, Any]]:
    # Prefer already-parsed response object if present.
    parsed = raw_entry.get("response")
    if isinstance(parsed, dict):
        return [parsed]
    if isinstance(parsed, list):
        return [x for x in parsed if isinstance(x, dict)]

    text = raw_entry.get("response_text") or ""
    return mpp._extract_json_blocks_from_text(text)  # noqa: SLF001 (intentional internal reuse)


def finalize(in_path: str, out_path: str) -> Tuple[int, int]:
    # Local imports (keep sys.path concerns outside this file).
    repo_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    if repo_dir not in sys.path:
        sys.path.insert(0, repo_dir)

    from third_stage_converter import ThirdStageConverter
    from multi_part_post_processor import MultiPartPostProcessor

    data = _load_json(in_path)
    meta = data.get("metadata") or {}
    raw_list = data.get("raw_responses") or []

    if not isinstance(raw_list, list) or not raw_list:
        raise SystemExit(f"No raw_responses found in: {in_path}")

    book_id = int(meta.get("book_id") or 1)
    chapter_id = int(meta.get("chapter_id") or 1)
    start_point_index = int(meta.get("start_point_index") or 1)
    chapter_from_meta = meta.get("chapter") or ""

    # Reuse robust extraction logic.
    mpp = MultiPartPostProcessor(api_client=None)
    converter = ThirdStageConverter()

    flat_rows: List[Dict[str, Any]] = []
    segments: List[Tuple[Dict[str, Any], int]] = []

    for r in raw_list:
        if not isinstance(r, dict):
            continue
        subchapter = r.get("subchapter") or ""
        topic = r.get("paragraph") or r.get("topic") or ""
        chapter = r.get("chapter") or chapter_from_meta

        blocks = _extract_blocks(mpp, r)
        before = len(flat_rows)
        for b in blocks:
            try:
                rows = converter._flatten_to_points(b)  # noqa: SLF001
            except Exception:
                rows = []
            if rows:
                flat_rows.extend(rows)
        produced = len(flat_rows) - before
        segments.append(({"chapter": chapter, "subchapter": subchapter, "topic": topic}, produced))

    if not flat_rows:
        raise SystemExit("No points could be extracted from raw_responses (flatten_to_points returned 0 rows).")

    # Assign chapter/subchapter/topic based on the raw_responses ordering.
    idx = 0
    for seg, count in segments:
        for _ in range(count):
            if idx >= len(flat_rows):
                break
            if seg.get("chapter"):
                flat_rows[idx]["chapter"] = seg["chapter"]
            if seg.get("subchapter"):
                flat_rows[idx]["subchapter"] = seg["subchapter"]
            if seg.get("topic"):
                flat_rows[idx]["topic"] = seg["topic"]
            idx += 1

    # Assign PointId sequentially.
    next_index = start_point_index
    for row in flat_rows:
        row["PointId"] = _point_id(book_id, chapter_id, next_index)
        next_index += 1

    total_points = len(flat_rows)
    next_free_index = start_point_index + total_points - 1 if total_points > 0 else start_point_index

    # Metadata: match the "completed" Lesson_file schema (topics_* keys).
    topics_processed = int(meta.get("paragraphs_processed") or meta.get("topics_processed") or len(raw_list))
    total_topics = int(meta.get("total_paragraphs") or meta.get("total_topics") or topics_processed)

    new_meta = dict(meta)
    new_meta["total_points"] = total_points
    new_meta["next_free_index"] = next_free_index
    new_meta["processed_at"] = datetime.now().isoformat()
    new_meta["processing_status"] = "completed"
    new_meta["topics_processed"] = topics_processed
    new_meta["total_topics"] = total_topics
    new_meta.pop("paragraphs_processed", None)
    new_meta.pop("total_paragraphs", None)

    out_obj = {
        "metadata": new_meta,
        "points": flat_rows,
    }

    _write_json(out_path, out_obj)
    return topics_processed, total_points


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="in_path", required=True, help="Path to Lesson_file_*.json with raw_responses")
    ap.add_argument("--out", dest="out_path", required=True, help="Output path for finalized Lesson_file JSON")
    args = ap.parse_args()

    topics_processed, total_points = finalize(args.in_path, args.out_path)
    print(f"Finalized. topics_processed={topics_processed}, total_points={total_points}")


if __name__ == "__main__":
    main()

