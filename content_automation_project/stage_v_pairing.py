"""
Pure helpers for Test Bank (Stage V): auto-pair Stage J JSON files with Word files by book/chapter.

Shared by the desktop GUI and the server web app — keep logic identical.
"""

from __future__ import annotations

import json
import os
import re
from typing import Any, Dict, List, Optional, Tuple


def _records_from_stage_j_data(data: Any) -> List[Dict[str, Any]]:
    """Same shapes as BaseStageProcessor.get_data_from_json (list root, data, points, rows, chapters)."""
    if isinstance(data, list):
        return [x for x in data if isinstance(x, dict)]
    if isinstance(data, dict):
        if "data" in data and isinstance(data["data"], list):
            return [x for x in data["data"] if isinstance(x, dict)]
        if "points" in data and isinstance(data["points"], list):
            return [x for x in data["points"] if isinstance(x, dict)]
        if "rows" in data and isinstance(data["rows"], list):
            return [x for x in data["rows"] if isinstance(x, dict)]
        if "chapters" in data:
            ch = data["chapters"]
            if isinstance(ch, list):
                return [x for x in ch if isinstance(x, dict)]
            if isinstance(ch, dict):
                if "rows" in ch and isinstance(ch["rows"], list):
                    return [x for x in ch["rows"] if isinstance(x, dict)]
                if "data" in ch and isinstance(ch["data"], list):
                    return [x for x in ch["data"] if isinstance(x, dict)]
    return []


def _point_id_to_str(point_id: Any) -> Optional[str]:
    if point_id is None:
        return None
    if isinstance(point_id, int):
        s = str(point_id)
        if len(s) >= 6:
            return s.zfill(max(10, len(s)))
        return s.zfill(10)
    s = str(point_id).strip()
    if not s:
        return None
    if s.isdigit() and len(s) < 10:
        return s.zfill(10)
    return s


def extract_book_chapter_from_stage_j_for_v(stage_j_path: str) -> Tuple[Optional[int], Optional[int]]:
    """Extract book and chapter from Stage J file (PointId in JSON or a{book}{chapter}.json filename)."""
    try:
        with open(stage_j_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        records = _records_from_stage_j_data(data)
        if records:
            pid = _point_id_to_str(records[0].get("PointId"))
            if pid and len(pid) >= 6 and pid[:6].isdigit():
                book_id = int(pid[0:3])
                chapter_id = int(pid[3:6])
                return book_id, chapter_id
    except Exception:
        pass

    try:
        basename = os.path.basename(stage_j_path)
        name_without_ext = os.path.splitext(basename)[0]
        if name_without_ext.startswith("a") and len(name_without_ext) >= 7:
            book_chapter = name_without_ext[1:]
            book_id = int(book_chapter[0:3])
            chapter_id = int(book_chapter[3:6])
            return book_id, chapter_id
    except Exception:
        pass

    return None, None


def extract_book_chapter_from_word_filename_for_v(word_path: str) -> Tuple[Optional[int], Optional[int]]:
    """Extract book and chapter from Word filename (various patterns)."""
    basename = os.path.basename(word_path)
    name_without_ext = os.path.splitext(basename)[0]

    patterns = [
        r"ch(\d{3})_(\d{3})",
        r"chapter_(\d{3})_(\d{3})",
        r"(\d{3})_(\d{3})",
        r"book(\d{3})_chapter(\d{3})",
        r"e(\d{3})(\d{3})",
        r"a(\d{3})(\d{3})\b",
        r"b(\d{3})(\d{3})\b",
        r"(?<![0-9])(\d{3})(\d{3})(?![0-9])",
        r"ch(\d{1,3})[_\-](\d{1,3})",
        r"(\d{1,3})[_\-](\d{1,3})(?![0-9])",
    ]

    for pattern in patterns:
        match = re.search(pattern, name_without_ext, re.IGNORECASE)
        if match:
            book_id = int(match.group(1))
            chapter_id = int(match.group(2))
            if book_id <= 999 and chapter_id <= 999:
                return book_id, chapter_id

    return None, None


def auto_pair_stage_v_files(
    stage_j_paths: List[str],
    word_paths: List[str],
) -> List[Dict[str, Any]]:
    """
    For each Stage J file in order, match the first unused Word file with the same (book_id, chapter_id).

    Returns a list of dicts: stage_j_path, word_path (or None), status, output_path, error
    (same shape as the Tkinter `stage_v_pairs` entries).
    """
    pairs: List[Dict[str, Any]] = []
    paired_word_files: set = set()

    for stage_j_path in stage_j_paths:
        book_id, chapter_id = extract_book_chapter_from_stage_j_for_v(stage_j_path)

        if book_id is None or chapter_id is None:
            continue

        matched_word: Optional[str] = None
        for word_path in word_paths:
            if word_path in paired_word_files:
                continue
            word_book, word_chapter = extract_book_chapter_from_word_filename_for_v(word_path)
            if word_book == book_id and word_chapter == chapter_id:
                matched_word = word_path
                paired_word_files.add(word_path)
                break

        pairs.append(
            {
                "stage_j_path": stage_j_path,
                "word_path": matched_word,
                "status": "pending",
                "output_path": None,
                "error": None,
            }
        )

    # Single-file fallback: one Stage J + one Word → pair regardless of names
    if len(pairs) == 1 and len(word_paths) == 1 and pairs[0]["word_path"] is None:
        pairs[0]["word_path"] = word_paths[0]

    # Fill remaining unmatched pairs with leftover words in upload order (1:1 by position)
    remaining_words = [w for w in word_paths if w not in paired_word_files]
    for pair in pairs:
        if pair["word_path"] is None and remaining_words:
            w = remaining_words.pop(0)
            pair["word_path"] = w
            paired_word_files.add(w)

    return pairs


def extract_book_chapter_from_stage_f_filename_for_h(stage_f_path: str) -> Tuple[Optional[int], Optional[int]]:
    """Extract book and chapter from Image File Catalog JSON (f_e{book}{chapter}.json, f_{book}{chapter}.json)."""
    try:
        basename = os.path.basename(stage_f_path)
        name_without_ext = os.path.splitext(basename)[0]
        if name_without_ext.startswith("f_e") and len(name_without_ext) >= 9:
            book_chapter = name_without_ext[3:]
            return int(book_chapter[0:3]), int(book_chapter[3:6])
        if name_without_ext.startswith("f_") and len(name_without_ext) >= 8:
            book_chapter = name_without_ext[2:]
            if len(book_chapter) >= 6:
                return int(book_chapter[0:3]), int(book_chapter[3:6])
    except (ValueError, IndexError):
        pass
    return None, None


def auto_pair_flashcard_files(
    tagged_paths: List[str],
    catalog_paths: List[str],
) -> List[Dict[str, Any]]:
    """
    Pair tagged JSON (a*) with image catalog JSON (f*) by book/chapter.

    Returns dicts: stage_j_path (tagged), word_path (catalog — reuses JobPair column), status, ...
    """
    pairs: List[Dict[str, Any]] = []
    paired_catalog: set = set()

    for tagged_path in tagged_paths:
        book_id, chapter_id = extract_book_chapter_from_stage_j_for_v(tagged_path)
        matched_catalog: Optional[str] = None

        if book_id is not None and chapter_id is not None:
            for catalog_path in catalog_paths:
                if catalog_path in paired_catalog:
                    continue
                f_book, f_chapter = extract_book_chapter_from_stage_f_filename_for_h(catalog_path)
                if f_book == book_id and f_chapter == chapter_id:
                    matched_catalog = catalog_path
                    paired_catalog.add(catalog_path)
                    break

        pairs.append(
            {
                "stage_j_path": tagged_path,
                "word_path": matched_catalog,
                "status": "pending",
                "output_path": None,
                "error": None,
            }
        )

    if len(pairs) == 1 and len(catalog_paths) == 1 and pairs[0]["word_path"] is None:
        pairs[0]["word_path"] = catalog_paths[0]

    remaining = [c for c in catalog_paths if c not in paired_catalog]
    for pair in pairs:
        if pair["word_path"] is None and remaining:
            c = remaining.pop(0)
            pair["word_path"] = c
            paired_catalog.add(c)

    return pairs


def extract_book_chapter_from_stage_v_filename_for_l(stage_v_path: str) -> Tuple[Optional[int], Optional[int]]:
    """Extract book and chapter from Test Bank JSON (b{book}{chapter}+*.json or PointId)."""
    try:
        with open(stage_v_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        records = _records_from_stage_j_data(data)
        if records:
            pid = _point_id_to_str(records[0].get("PointId") or records[0].get("point_id"))
            if pid and len(pid) >= 6 and pid[:6].isdigit():
                return int(pid[0:3]), int(pid[3:6])
    except Exception:
        pass

    try:
        basename = os.path.basename(stage_v_path)
        name_without_ext = os.path.splitext(basename)[0]
        match = re.match(r"^b(\d{3})(\d{3})\+", name_without_ext)
        if match:
            return int(match.group(1)), int(match.group(2))
        if name_without_ext.startswith("b") and len(name_without_ext) >= 7:
            book_chapter = name_without_ext[1:]
            return int(book_chapter[0:3]), int(book_chapter[3:6])
    except Exception:
        pass

    return None, None


def auto_pair_chapter_summary_files(
    tagged_paths: List[str],
    test_bank_paths: List[str],
) -> List[Dict[str, Any]]:
    """
    Pair tagged lesson JSON (a*) with Test Bank JSON (b*) by book/chapter.

    Returns dicts: stage_j_path (tagged), word_path (test bank — reuses JobPair column), ...
    """
    pairs: List[Dict[str, Any]] = []
    paired_test_bank: set = set()

    for tagged_path in tagged_paths:
        book_id, chapter_id = extract_book_chapter_from_stage_j_for_v(tagged_path)
        matched_tb: Optional[str] = None

        if book_id is not None and chapter_id is not None:
            for tb_path in test_bank_paths:
                if tb_path in paired_test_bank:
                    continue
                tb_book, tb_chapter = extract_book_chapter_from_stage_v_filename_for_l(tb_path)
                if tb_book == book_id and tb_chapter == chapter_id:
                    matched_tb = tb_path
                    paired_test_bank.add(tb_path)
                    break

        pairs.append(
            {
                "stage_j_path": tagged_path,
                "word_path": matched_tb,
                "status": "pending",
                "output_path": None,
                "error": None,
            }
        )

    if len(pairs) == 1 and len(test_bank_paths) == 1 and pairs[0]["word_path"] is None:
        pairs[0]["word_path"] = test_bank_paths[0]

    remaining = [c for c in test_bank_paths if c not in paired_test_bank]
    for pair in pairs:
        if pair["word_path"] is None and remaining:
            c = remaining.pop(0)
            pair["word_path"] = c
            paired_test_bank.add(c)

    return pairs


def extract_book_chapter_from_step1_combined_filename(path: str) -> Tuple[Optional[int], Optional[int]]:
    """
    Parse book/chapter from a Test Bank Step 1 combined JSON basename.

    Expected shape under the processor is ``step1_combined_{book}{chapter:03d}.json``
    (e.g. book 105, chapter 3 → ``...105003...``). Also tolerates extra suffixes like `` (1)``.
    """
    basename = os.path.basename(path)
    name_without_ext = os.path.splitext(basename)[0]
    name_without_ext = re.sub(r"\s*\(\d+\)\s*$", "", name_without_ext)

    m = re.search(r"step1_combined_(\d+)$", name_without_ext, re.IGNORECASE)
    digits = m.group(1) if m else None
    if not digits and re.fullmatch(r"\d{4,}", name_without_ext):
        digits = name_without_ext
    if not digits or len(digits) < 4:
        return None, None
    try:
        chapter_id = int(digits[-3:])
        book_id = int(digits[:-3])
        return book_id, chapter_id
    except ValueError:
        return None, None


def attach_step1_combined_uploads_to_pairs(
    pairs_spec: List[Dict[str, Any]],
    step1_paths: List[str],
) -> None:
    """
    Mutates each dict in ``pairs_spec`` to set ``step1_combined_upload`` (absolute path while
    staging) to the best matching Step 1 combined JSON for that pair's Stage J book/chapter.
    """
    used: set = set()
    for p in pairs_spec:
        sj = p.get("stage_j_path")
        if not sj:
            p["step1_combined_upload"] = None
            continue
        book_id, chapter_id = extract_book_chapter_from_stage_j_for_v(sj)
        matched: Optional[str] = None
        if book_id is not None and chapter_id is not None:
            for s1_path in step1_paths:
                if s1_path in used:
                    continue
                b2, c2 = extract_book_chapter_from_step1_combined_filename(s1_path)
                if b2 == book_id and c2 == chapter_id:
                    matched = s1_path
                    used.add(s1_path)
                    break
        if matched is None:
            for s1_path in step1_paths:
                if s1_path not in used:
                    matched = s1_path
                    used.add(s1_path)
                    break
        p["step1_combined_upload"] = matched
