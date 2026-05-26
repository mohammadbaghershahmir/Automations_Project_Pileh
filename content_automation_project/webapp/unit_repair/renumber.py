"""Sequential PointId / QId reassignment from manifest unit order."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple


def parse_pointid(point_id: str) -> Tuple[int, int, int]:
    if not point_id or len(point_id) != 10 or not point_id.isdigit():
        raise ValueError(f"Invalid PointId: {point_id!r}")
    return int(point_id[0:3]), int(point_id[3:6]), int(point_id[6:10])


def format_pointid(book_id: int, chapter_id: int, index: int) -> str:
    return f"{book_id:03d}{chapter_id:03d}{index:04d}"


def format_qid(book_id: int, chapter_id: int, index: int) -> str:
    return format_pointid(book_id, chapter_id, index)


def _unit_topic_key(unit: Dict[str, Any]) -> Tuple[str, str, str]:
    return (
        (unit.get("chapter") or "").strip(),
        (unit.get("subchapter") or "").strip(),
        (unit.get("topic") or unit.get("paragraph") or "").strip(),
    )


def _row_topic_key(row: Dict[str, Any]) -> Tuple[str, str, str]:
    return (
        (row.get("chapter") or row.get("Chapter") or "").strip(),
        (row.get("subchapter") or row.get("Subchapter") or "").strip(),
        (row.get("topic") or row.get("Topic") or row.get("paragraph") or "").strip(),
    )


def renumber_points_in_rows(
    rows: List[Dict[str, Any]],
    manifest: Dict[str, Any],
    start_pointid: str,
) -> int:
    """Reassign PointId on flat point rows ordered by manifest units."""
    book_id, chapter_id, start_index = parse_pointid(start_pointid)
    units = sorted(manifest.get("units") or [], key=lambda u: int(u.get("unit_index", 0)))
    order_keys: List[Tuple[str, str, str]] = [_unit_topic_key(u) for u in units]

    def sort_key(row: Dict[str, Any]) -> Tuple[int, int]:
        key = _row_topic_key(row)
        try:
            return (order_keys.index(key), 0)
        except ValueError:
            return (len(order_keys), 0)

    ordered = sorted(rows, key=sort_key)
    idx = start_index
    for row in ordered:
        row["PointId"] = format_pointid(book_id, chapter_id, idx)
        idx += 1
    rows[:] = ordered
    return len(ordered)


def renumber_qids_in_rows(
    rows: List[Dict[str, Any]],
    manifest: Dict[str, Any],
    book_id: int,
    chapter_id: int,
) -> int:
    """Reassign QId (and TestID per topic group) on Stage V question rows."""
    units = sorted(manifest.get("units") or [], key=lambda u: int(u.get("unit_index", 0)))
    order_keys: List[Tuple[str, str, str]] = []
    for u in units:
        order_keys.append(
            (
                (u.get("chapter") or u.get("chapter_name") or "").strip(),
                (u.get("subchapter") or u.get("subchapter_name") or "").strip(),
                (u.get("topic") or u.get("topic_name") or "").strip(),
            )
        )

    def sort_key(row: Dict[str, Any]) -> Tuple[int, int]:
        key = (
            (row.get("Chapter") or row.get("chapter") or "").strip(),
            (row.get("Subchapter") or row.get("subchapter") or "").strip(),
            (row.get("Topic") or row.get("topic") or "").strip(),
        )
        try:
            return (order_keys.index(key), 0)
        except ValueError:
            return (len(order_keys), 0)

    ordered = sorted(rows, key=sort_key)
    global_q = 1
    local_test_by_topic: Dict[Tuple[str, str, str], int] = {}
    for row in ordered:
        row["QId"] = format_qid(book_id, chapter_id, global_q)
        global_q += 1
        tk = (
            (row.get("Chapter") or row.get("chapter") or "").strip(),
            (row.get("Subchapter") or row.get("subchapter") or "").strip(),
            (row.get("Topic") or row.get("topic") or "").strip(),
        )
        local_test_by_topic[tk] = local_test_by_topic.get(tk, 0) + 1
        row["TestID"] = local_test_by_topic[tk]
    rows[:] = ordered
    return len(ordered)


def mark_renumber_applied(manifest: Dict[str, Any]) -> None:
    r = manifest.setdefault("renumber", {})
    r["last_applied_at"] = datetime.now(timezone.utc).isoformat()
    r["ids_provisional"] = False
