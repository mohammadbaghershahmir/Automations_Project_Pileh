"""Document processing unit repair: manifest, regenerate, renumber."""

from __future__ import annotations

import json
import os
import re
from typing import Any, Callable, Dict, List, Optional, Tuple

from sqlalchemy.orm import Session

from webapp.job_files import job_root, register_artifacts_under
from webapp.unit_repair.manifest import (
    abs_from_relpath,
    get_unit,
    load_manifest,
    manifest_path,
    save_manifest,
    unit_artifact_relpath,
    upsert_unit,
)
from webapp.unit_repair.renumber import mark_renumber_applied, renumber_points_in_rows


class DocumentProcessingUnitHooks:
    """Callbacks from MultiPartPostProcessor during a normal job run."""

    def __init__(
        self,
        job_id: str,
        pair_index: int,
        job_type: str,
        start_pointid: str,
        output_relpath: Optional[str] = None,
        prompt_client: Any = None,
    ):
        self.job_id = job_id
        self.pair_index = pair_index
        self.job_type = job_type
        self.start_pointid = start_pointid
        self.output_relpath = output_relpath
        self.prompt_client = prompt_client
        self._manifest: Optional[Dict[str, Any]] = None

    def _ensure_manifest(self) -> Dict[str, Any]:
        if self._manifest is None:
            existing = load_manifest(self.job_id, self.pair_index)
            if existing:
                self._manifest = existing
            else:
                self._manifest = {
                    "job_type": self.job_type,
                    "units": [],
                    "renumber": {
                        "scheme": "pointid",
                        "start_id": self.start_pointid,
                        "last_applied_at": None,
                        "ids_provisional": False,
                    },
                    "output_relpath": self.output_relpath,
                }
        return self._manifest

    def before_unit(
        self,
        unit_index: int,
        chapter: str,
        subchapter: str,
        topic: str,
        prompt_seq: int,
    ) -> None:
        m = self._ensure_manifest()
        label = f"{chapter} > {subchapter} > {topic}".strip(" >")
        upsert_unit(
            m,
            {
                "unit_index": unit_index,
                "label": label,
                "chapter": chapter,
                "subchapter": subchapter,
                "topic": topic,
                "prompt_seq": prompt_seq,
                "status": "running",
            },
        )
        save_manifest(self.job_id, self.pair_index, m)
        if self.prompt_client and hasattr(self.prompt_client, "set_current_unit"):
            self.prompt_client.set_current_unit(unit_index, topic or subchapter)

    def after_unit(
        self,
        unit_index: int,
        chapter: str,
        subchapter: str,
        topic: str,
        points: List[Dict[str, Any]],
        prompt_seq: int,
        status: str = "succeeded",
    ) -> None:
        m = self._ensure_manifest()
        safe = re.sub(r"[^a-zA-Z0-9._-]+", "_", (topic or "unit")[:60])
        rel = unit_artifact_relpath(self.job_id, self.pair_index, unit_index, f"{safe}.json")
        abs_path = abs_from_relpath(self.job_id, rel)
        os.makedirs(os.path.dirname(abs_path), exist_ok=True)
        with open(abs_path, "w", encoding="utf-8") as f:
            json.dump({"points": points}, f, ensure_ascii=False, indent=2)
        label = f"{chapter} > {subchapter} > {topic}".strip(" >")
        upsert_unit(
            m,
            {
                "unit_index": unit_index,
                "label": label,
                "chapter": chapter,
                "subchapter": subchapter,
                "topic": topic,
                "prompt_seq": prompt_seq,
                "status": status,
                "artifact_relpath": rel,
            },
        )
        if status == "succeeded" and m.get("renumber", {}).get("last_applied_at"):
            m.setdefault("renumber", {})["ids_provisional"] = True
        save_manifest(self.job_id, self.pair_index, m)

    def set_output_relpath(self, relpath: str) -> None:
        m = self._ensure_manifest()
        m["output_relpath"] = relpath.replace("\\", "/")
        self.output_relpath = m["output_relpath"]
        save_manifest(self.job_id, self.pair_index, m)

    def seed_units(self, units: List[Dict[str, Any]]) -> None:
        """Pre-register expected units (e.g. from Stage E topics) before LLM calls run."""
        m = self._ensure_manifest()
        for u in units:
            upsert_unit(
                m,
                {
                    "unit_index": u["unit_index"],
                    "label": u.get("label") or "",
                    "chapter": u.get("chapter") or "",
                    "subchapter": u.get("subchapter") or "",
                    "topic": u.get("topic") or "",
                    "status": u.get("status") or "pending",
                },
            )
        save_manifest(self.job_id, self.pair_index, m)

    def finalize_stale_units(self, final_status: str = "skipped") -> int:
        """
        Mark units still pending/running after a job ends (crash, cancel, or partial run).
        Returns how many units were updated.
        """
        m = self._ensure_manifest()
        updated = 0
        for u in m.get("units") or []:
            st = (u.get("status") or "").strip().lower()
            if st in ("pending", "running"):
                u["status"] = final_status
                updated += 1
        if updated:
            save_manifest(self.job_id, self.pair_index, m)
        return updated


def hooks_for_pair(
    db: Session,
    job_id: str,
    pair_index: int,
    job_type: str,
    cfg: Dict[str, Any],
    prompt_client: Any = None,
) -> DocumentProcessingUnitHooks:
    start = (cfg.get("start_pointid") or "0010010001").strip()
    return DocumentProcessingUnitHooks(
        job_id, pair_index, job_type, start, prompt_client=prompt_client
    )


def _lesson_path(job_id: str, manifest: Dict[str, Any]) -> str:
    rel = manifest.get("output_relpath")
    if not rel:
        raise FileNotFoundError("Manifest has no output_relpath")
    path = abs_from_relpath(job_id, rel)
    if not os.path.isfile(path):
        raise FileNotFoundError(f"Lesson output not found: {path}")
    return path


def build_manifest_from_lesson(job_id: str, pair_index: int, lesson_path: str, start_pointid: str) -> Dict[str, Any]:
    with open(lesson_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    points = data.get("points") or []
    seen: Dict[Tuple[str, str, str], int] = {}
    units: List[Dict[str, Any]] = []
    for row in points:
        if not isinstance(row, dict):
            continue
        key = (
            (row.get("chapter") or "").strip(),
            (row.get("subchapter") or "").strip(),
            (row.get("topic") or "").strip(),
        )
        if key in seen:
            continue
        seen[key] = len(seen) + 1
        ch, sub, top = key
        units.append(
            {
                "unit_index": seen[key],
                "label": f"{ch} > {sub} > {top}".strip(" >"),
                "chapter": ch,
                "subchapter": sub,
                "topic": top,
                "status": "succeeded",
            }
        )
    rel = os.path.relpath(lesson_path, job_root(job_id)).replace("\\", "/")
    return {
        "job_type": "document_processing",
        "units": units,
        "renumber": {
            "scheme": "pointid",
            "start_id": start_pointid,
            "last_applied_at": None,
            "ids_provisional": False,
        },
        "output_relpath": rel,
    }


def ensure_manifest(job_id: str, pair_index: int, cfg: Dict[str, Any]) -> Dict[str, Any]:
    m = load_manifest(job_id, pair_index)
    if m and m.get("units"):
        return m
    m2 = load_manifest(job_id, pair_index)
    if m2 and m2.get("output_relpath"):
        try:
            path = _lesson_path(job_id, m2)
            start = (m2.get("renumber") or {}).get("start_id") or cfg.get("start_pointid") or "0010010001"
            built = build_manifest_from_lesson(job_id, pair_index, path, str(start))
            save_manifest(job_id, pair_index, built)
            return built
        except FileNotFoundError:
            pass
    raise FileNotFoundError(
        "No unit manifest for this pair. Run document processing once to build it."
    )


def _remove_unit_points(all_points: List[Dict[str, Any]], unit: Dict[str, Any]) -> List[Dict[str, Any]]:
    ch = (unit.get("chapter") or "").strip()
    sub = (unit.get("subchapter") or "").strip()
    top = (unit.get("topic") or "").strip()
    return [
        p
        for p in all_points
        if not (
            isinstance(p, dict)
            and (p.get("chapter") or "").strip() == ch
            and (p.get("subchapter") or "").strip() == sub
            and (p.get("topic") or "").strip() == top
        )
    ]


def _units_to_clear(manifest_unit: Dict[str, Any], new_points: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Manifest and process_list keys can differ on legacy jobs — remove both."""
    units = [manifest_unit]
    if new_points and isinstance(new_points[0], dict):
        sample = new_points[0]
        process_unit = {
            "chapter": sample.get("chapter") or "",
            "subchapter": sample.get("subchapter") or "",
            "topic": sample.get("topic") or "",
        }
        manifest_key = (
            (manifest_unit.get("chapter") or "").strip(),
            (manifest_unit.get("subchapter") or "").strip(),
            (manifest_unit.get("topic") or "").strip(),
        )
        process_key = (
            (process_unit.get("chapter") or "").strip(),
            (process_unit.get("subchapter") or "").strip(),
            (process_unit.get("topic") or "").strip(),
        )
        if process_key != manifest_key:
            units.append(process_unit)
    return units


def regenerate_unit(
    db: Session,
    job_id: str,
    pair_index: int,
    unit_index: int,
    cfg: Dict[str, Any],
    ocr_json_path: str,
    user_prompt: str,
    model_name: str,
    progress_callback: Optional[Callable[[str], None]] = None,
    prompt_client: Any = None,
) -> None:
    from multi_part_post_processor import MultiPartPostProcessor

    manifest = ensure_manifest(job_id, pair_index, cfg)
    unit = get_unit(manifest, unit_index)
    if not unit:
        raise ValueError(f"Unknown unit_index {unit_index}")

    from webapp.processor_context import build_unified_api_client

    if prompt_client is None:
        prompt_client, _ = build_unified_api_client()
    post = MultiPartPostProcessor(prompt_client)
    book_id = cfg.get("book_id")
    chapter_id = cfg.get("chapter_id")
    if book_id is not None:
        book_id = int(book_id)
    if chapter_id is not None:
        chapter_id = int(chapter_id)
    start_point_index = int(cfg.get("start_point_index") or 1)

    pointid_txt = None
    pointid_rel = (cfg.get("pointid_mapping_relpath") or "").strip()
    if pointid_rel:
        pi = abs_from_relpath(job_id, pointid_rel)
        if os.path.isfile(pi):
            all_pids = post.load_chapter_pointid_mapping(pi)
            if pair_index < len(all_pids):
                import tempfile

                fd, pointid_txt = tempfile.mkstemp(prefix="pid_", suffix=".txt", text=True)
                with os.fdopen(fd, "w", encoding="utf-8") as tf:
                    tf.write(all_pids[pair_index] + "\n")

    hooks = DocumentProcessingUnitHooks(
        job_id,
        pair_index,
        "document_processing",
        (manifest.get("renumber") or {}).get("start_id") or cfg.get("start_pointid") or "0010010001",
        manifest.get("output_relpath"),
        prompt_client=prompt_client,
    )
    if prompt_client and hasattr(prompt_client, "set_current_unit"):
        prompt_client.set_current_unit(unit_index, unit.get("topic") or unit.get("label"))

    new_points, prompt_seq = post.regenerate_document_processing_unit(
        ocr_json_path=ocr_json_path,
        user_prompt=user_prompt,
        model_name=model_name,
        unit_index=unit_index,
        chapter=unit.get("chapter") or "",
        subchapter=unit.get("subchapter") or "",
        topic=unit.get("topic") or "",
        book_id=book_id,
        chapter_id=chapter_id,
        start_point_index=start_point_index,
        pointid_mapping_txt=pointid_txt,
        progress_callback=progress_callback,
        assign_pointids=False,
    )
    if pointid_txt and os.path.isfile(pointid_txt):
        try:
            os.remove(pointid_txt)
        except OSError:
            pass

    if not new_points:
        raise RuntimeError(
            f"Regenerate unit {unit_index} produced no points — LLM response empty or JSON parse failed"
        )

    lesson_path = _lesson_path(job_id, manifest)
    with open(lesson_path, "r", encoding="utf-8") as f:
        lesson = json.load(f)
    points = lesson.get("points") or []
    for clear_unit in _units_to_clear(unit, new_points):
        points = _remove_unit_points(points, clear_unit)
    points.extend(new_points)
    lesson["points"] = points
    meta = lesson.setdefault("metadata", {})
    meta["total_points"] = len(points)
    meta["ids_provisional"] = True
    with open(lesson_path, "w", encoding="utf-8") as f:
        json.dump(lesson, f, ensure_ascii=False, indent=2)

    hooks.after_unit(
        unit_index,
        (new_points[0].get("chapter") if new_points else None) or unit.get("chapter") or "",
        (new_points[0].get("subchapter") if new_points else None) or unit.get("subchapter") or "",
        (new_points[0].get("topic") if new_points else None) or unit.get("topic") or "",
        new_points,
        prompt_seq,
        status="succeeded",
    )
    m = load_manifest(job_id, pair_index) or manifest
    m.setdefault("renumber", {})["ids_provisional"] = True
    save_manifest(job_id, pair_index, m)
    base = job_root(job_id)
    register_artifacts_under(db, job_id, pair_index, base, os.path.dirname(manifest["output_relpath"]))


def renumber_pair(
    db: Session,
    job_id: str,
    pair_index: int,
    cfg: Dict[str, Any],
) -> int:
    manifest = ensure_manifest(job_id, pair_index, cfg)
    lesson_path = _lesson_path(job_id, manifest)
    start_id = (manifest.get("renumber") or {}).get("start_id") or cfg.get("start_pointid") or "0010010001"
    with open(lesson_path, "r", encoding="utf-8") as f:
        lesson = json.load(f)
    points = lesson.get("points") or []
    n = renumber_points_in_rows(points, manifest, str(start_id))
    lesson["points"] = points
    meta = lesson.setdefault("metadata", {})
    meta["total_points"] = len(points)
    meta.pop("ids_provisional", None)
    try:
        book_id, chapter_id, next_idx = __import__(
            "webapp.unit_repair.renumber", fromlist=["parse_pointid"]
        ).parse_pointid(str(start_id))
        meta["book_id"] = book_id
        meta["chapter_id"] = chapter_id
        meta["next_free_index"] = next_idx + n
    except ValueError:
        pass
    with open(lesson_path, "w", encoding="utf-8") as f:
        json.dump(lesson, f, ensure_ascii=False, indent=2)
    mark_renumber_applied(manifest)
    save_manifest(job_id, pair_index, manifest)
    base = job_root(job_id)
    register_artifacts_under(db, job_id, pair_index, base, os.path.dirname(manifest["output_relpath"]))
    return n
