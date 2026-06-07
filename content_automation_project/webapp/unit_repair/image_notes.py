"""Image notes (Stage E) unit repair — per-topic regenerate and PointId renumber."""

from __future__ import annotations

import json
import os
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy.orm import Session

from webapp.job_files import job_root, register_artifacts_under
from webapp.unit_repair.docproc import DocumentProcessingUnitHooks, hooks_for_pair
from webapp.unit_repair.manifest import get_unit, load_manifest, save_manifest
from webapp.unit_repair.renumber import mark_renumber_applied, renumber_points_in_rows
from webapp.unit_repair.table_notes import (
    _sync_status_from_prompts,
    filter_points_for_unit,
    topic_unit_map,
    topic_units_from_points,
)


def _stage4_path(job_id: str, stage4_relpath: str) -> str:
    path = os.path.join(job_root(job_id), stage4_relpath.replace("/", os.sep))
    if not os.path.isfile(path):
        raise FileNotFoundError(f"Stage 4 JSON not found: {path}")
    return path


def _e_output_path(job_id: str, stage4_relpath: str) -> str:
    """Find e{book}{chapter}_*.json written beside Stage 4 (Stage E output)."""
    out_dir = os.path.dirname(_stage4_path(job_id, stage4_relpath))
    e_files = sorted(
        fn
        for fn in os.listdir(out_dir)
        if fn.startswith("e") and len(fn) > 7 and fn[1:7].isdigit() and fn.endswith(".json")
    )
    if e_files:
        return os.path.join(out_dir, e_files[-1])
    raise FileNotFoundError("Stage E output (e*.json) not found")


def _load_e_file(path: str) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    with open(path, "r", encoding="utf-8") as f:
        doc = json.load(f)
    rows = doc.get("data") if isinstance(doc, dict) else None
    if rows is None and isinstance(doc, dict):
        rows = doc.get("points")
    if not isinstance(rows, list):
        rows = []
    return doc, rows


def _save_e_file(path: str, doc: Dict[str, Any], rows: List[Dict[str, Any]]) -> None:
    if "data" in doc:
        doc["data"] = rows
    else:
        doc["points"] = rows
    with open(path, "w", encoding="utf-8") as f:
        json.dump(doc, f, ensure_ascii=False, indent=2)


def _points_from_json_path(path: str) -> List[Dict[str, Any]]:
    with open(path, "r", encoding="utf-8") as f:
        raw = json.load(f)
    rows = raw.get("data") if isinstance(raw, dict) else None
    if rows is None and isinstance(raw, dict):
        rows = raw.get("points")
    if not isinstance(rows, list):
        rows = raw if isinstance(raw, list) else []
    return [r for r in rows if isinstance(r, dict)]


def build_manifest_from_stage4(
    job_id: str,
    pair_index: int,
    stage4_path: str,
    output_relpath: Optional[str] = None,
    start_pointid: str = "0010010001",
) -> Dict[str, Any]:
    units = topic_units_from_points(_points_from_json_path(stage4_path))
    rel = output_relpath
    if not rel:
        try:
            stage4_rel = os.path.relpath(stage4_path, job_root(job_id)).replace("\\", "/")
            out_abs = _e_output_path(job_id, stage4_rel)
            rel = os.path.relpath(out_abs, job_root(job_id)).replace("\\", "/")
        except FileNotFoundError:
            rel = None

    manifest = {
        "job_type": "image_notes",
        "units": units,
        "renumber": {
            "scheme": "pointid",
            "start_id": start_pointid,
            "last_applied_at": None,
            "ids_provisional": False,
        },
        "output_relpath": rel,
    }
    _sync_status_from_prompts(job_id, pair_index, manifest)
    return manifest


def ensure_manifest(db: Session, job_id: str, pair_index: int, cfg: Dict[str, Any]) -> Dict[str, Any]:
    existing = load_manifest(job_id, pair_index)
    if existing and existing.get("units"):
        return existing

    from webapp.models import JobPair

    pair = (
        db.query(JobPair)
        .filter(JobPair.job_id == job_id, JobPair.pair_index == pair_index)
        .one_or_none()
    )
    if not pair or not pair.stage_j_relpath:
        raise FileNotFoundError("No Stage 4 input for this pair")

    stage4 = _stage4_path(job_id, pair.stage_j_relpath)
    output_rel: Optional[str] = None
    try:
        out_abs = _e_output_path(job_id, pair.stage_j_relpath)
        output_rel = os.path.relpath(out_abs, job_root(job_id)).replace("\\", "/")
    except FileNotFoundError:
        pass

    start = (cfg.get("start_pointid") or "0010010001").strip()
    built = build_manifest_from_stage4(job_id, pair_index, stage4, output_rel, start)
    save_manifest(job_id, pair_index, built)
    return built


def _filepic_path(stage4_path: str) -> str:
    base_dir = os.path.dirname(stage4_path)
    base_name = os.path.splitext(os.path.basename(stage4_path))[0]
    return os.path.join(base_dir, f"{base_name}_filepic.json")


def regenerate_unit(
    db: Session,
    job_id: str,
    pair_index: int,
    unit_index: int,
    cfg: Dict[str, Any],
    prompt_client: Any,
) -> None:
    from stage_e_processor import StageEProcessor
    from webapp.models import JobPair
    from webapp.system_prompt_defaults import resolve_prompt_for_job
    from webapp.unit_repair.pic_sidecar import apply_regenerate_to_merged_files

    pair = db.query(JobPair).filter(JobPair.job_id == job_id, JobPair.pair_index == pair_index).one()
    base = job_root(job_id)
    abs_stage4 = _stage4_path(job_id, pair.stage_j_relpath)
    abs_ocr = os.path.join(base, (pair.word_relpath or "").replace("/", os.sep))
    out_path = _e_output_path(job_id, pair.stage_j_relpath)

    processor = StageEProcessor(prompt_client)
    stage4_data = processor.load_json_file(abs_stage4)
    ocr_data = processor.load_json_file(abs_ocr)
    if not stage4_data or not ocr_data:
        raise FileNotFoundError("Missing Stage 4 or OCR JSON")

    stage4_points = processor.get_data_from_json(stage4_data)
    manifest = ensure_manifest(db, job_id, pair_index, cfg)
    unit = get_unit(manifest, unit_index)
    if not unit:
        raise ValueError(f"Unknown unit_index {unit_index}")
    topic_name = (unit.get("topic") or "").strip()
    pts = filter_points_for_unit(stage4_points or [], unit)
    sub = (unit.get("subchapter") or "").strip() or (
        (pts[0].get("subchapter") or "").strip() if pts else ""
    )
    ch = (unit.get("chapter") or "").strip() or (
        (pts[0].get("chapter") or "").strip() if pts else ""
    )

    # #region agent log
    from base_stage_processor import _agent_debug_log

    _agent_debug_log(
        "webapp/unit_repair/image_notes.py:regenerate_unit",
        "Regenerate unit context",
        {
            "unit_index": unit_index,
            "topic_name": topic_name,
            "manifest_chapter": ch,
            "manifest_subchapter": sub,
            "stage4_pts_count": len(pts),
        },
        "C",
    )
    # #endregion

    if not pts:
        raise ValueError(
            f"No Stage 4 points for unit {unit_index} "
            f"({ch} > {sub} > {topic_name})"
        )

    prompt = resolve_prompt_for_job(db, "image_notes", cfg, "prompt")
    model = (cfg.get("model") or "z-ai/glm-5").strip()
    prompt_sub = prompt.replace("{SUBCHAPTER_NAME}", sub).replace("{Subchapter_Name}", sub)

    if hasattr(prompt_client, "set_current_unit"):
        prompt_client.set_current_unit(unit_index, topic_name)

    stage4_topics_in_subchapter = {
        (p.get("topic") or "").strip()
        for p in stage4_points
        if isinstance(p, dict)
        and (p.get("subchapter") or "").strip() == sub
        and (p.get("topic") or "").strip()
    }

    _ti, tn, rows, err = processor._run_stage_e_single_topic(
        unit_index - 1,
        topic_name,
        pts,
        prompt_with_subchapter=prompt_sub,
        persian_subchapter_name=sub,
        ocr_extraction_data=ocr_data,
        model_name=model,
        output_path=out_path,
        part_num=1,
        _progress=lambda _m: None,
        raw_response_kind="topic_parallel",
        stage4_topics_in_subchapter=stage4_topics_in_subchapter,
    )
    if err:
        raise RuntimeError(err)
    if not rows:
        raise RuntimeError(
            f"Regenerate unit {unit_index} produced no image rows for "
            f"«{topic_name}» in subchapter «{sub}» — OCR slice likely has no figures "
            f"for this topic (check worker log line 'Stage E OCR slice')."
        )

    filepic_path = _filepic_path(abs_stage4)
    apply_regenerate_to_merged_files(
        processor,
        source_points=stage4_points,
        pic_path=filepic_path,
        main_path=out_path,
        topic_name=topic_name,
        new_pic_rows=rows,
        pic_stage_label="filepic",
        notes_count_key="image_notes_count",
        source_count_key="stage4_total_points",
        first_note_id_key="first_image_point_id",
        pic_meta_prefix="filepic",
        chapter=ch,
        subchapter=sub,
    )

    hooks = hooks_for_pair(db, job_id, pair_index, "image_notes", cfg, prompt_client)
    hooks.after_unit(
        unit_index,
        ch,
        sub,
        topic_name,
        rows,
        int(unit.get("prompt_seq") or unit_index),
        status="succeeded",
    )

    # #region agent log
    _agent_debug_log(
        "webapp/unit_repair/image_notes.py:regenerate_unit",
        "after_unit wrote unit artifact",
        {"unit_index": unit_index, "row_count": len(rows), "topic_name": topic_name},
        "D",
    )
    # #endregion

    manifest = ensure_manifest(db, job_id, pair_index, cfg)
    rel = os.path.relpath(out_path, base).replace("\\", "/")
    manifest["output_relpath"] = rel
    manifest.setdefault("renumber", {})["ids_provisional"] = True
    save_manifest(job_id, pair_index, manifest)
    register_artifacts_under(db, job_id, pair_index, base, os.path.dirname(rel))


def renumber_pair(db: Session, job_id: str, pair_index: int, cfg: Dict[str, Any]) -> int:
    from webapp.models import JobPair

    pair = db.query(JobPair).filter(JobPair.job_id == job_id, JobPair.pair_index == pair_index).one()
    out_path = _e_output_path(job_id, pair.stage_j_relpath)
    manifest = ensure_manifest(db, job_id, pair_index, cfg)
    doc, points = _load_e_file(out_path)
    start_id = (manifest.get("renumber") or {}).get("start_id") or cfg.get("start_pointid") or "0010010001"
    n = renumber_points_in_rows(points, manifest, str(start_id))
    _save_e_file(out_path, doc, points)
    mark_renumber_applied(manifest)
    save_manifest(job_id, pair_index, manifest)
    register_artifacts_under(
        db, job_id, pair_index, job_root(job_id), os.path.dirname(manifest["output_relpath"] or "")
    )
    return n


__all__ = [
    "DocumentProcessingUnitHooks",
    "ensure_manifest",
    "hooks_for_pair",
    "regenerate_unit",
    "renumber_pair",
    "topic_unit_map",
    "topic_units_from_points",
]
