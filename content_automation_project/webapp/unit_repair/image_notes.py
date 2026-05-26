"""Image notes (Stage E) unit repair — per-topic regenerate and PointId renumber."""

from __future__ import annotations

import json
import os
from typing import Any, Dict, List, Optional

from sqlalchemy.orm import Session

from webapp.job_files import job_root, register_artifacts_under
from webapp.unit_repair.docproc import build_manifest_from_lesson, ensure_manifest as _ensure_from_lesson
from webapp.unit_repair.manifest import abs_from_relpath, get_unit, load_manifest, save_manifest
from webapp.unit_repair.renumber import mark_renumber_applied, renumber_points_in_rows


def _output_path(job_id: str, pair_index: int, stage4_relpath: str) -> str:
    base = job_root(job_id)
    stage4 = os.path.join(base, stage4_relpath.replace("/", os.sep))
    out_dir = os.path.dirname(stage4) or base
    for fn in os.listdir(out_dir):
        if fn.startswith("e") and fn.endswith(".json"):
            return os.path.join(out_dir, fn)
    raise FileNotFoundError("Stage E output (e*.json) not found")


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

    pair = db.query(JobPair).filter(JobPair.job_id == job_id, JobPair.pair_index == pair_index).one()
    base = job_root(job_id)
    abs_stage4 = os.path.join(base, pair.stage_j_relpath.replace("/", os.sep))
    abs_ocr = os.path.join(base, (pair.word_relpath or "").replace("/", os.sep))
    out_path = _output_path(job_id, pair_index, pair.stage_j_relpath)

    processor = StageEProcessor(prompt_client)
    stage4_data = processor.load_json_file(abs_stage4)
    ocr_data = processor.load_json_file(abs_ocr)
    if not stage4_data or not ocr_data:
        raise FileNotFoundError("Missing Stage 4 or OCR JSON")

    points = processor.get_data_from_json(stage4_data)
    topic_groups: Dict[str, List[Dict[str, Any]]] = {}
    for p in points:
        if not isinstance(p, dict):
            continue
        tn = (p.get("topic") or "").strip()
        topic_groups.setdefault(tn, []).append(p)

    topics_ordered = list(topic_groups.keys())
    if unit_index < 1 or unit_index > len(topics_ordered):
        raise ValueError(f"unit_index {unit_index} out of range")
    topic_name = topics_ordered[unit_index - 1]
    pts = topic_groups[topic_name]

    if hasattr(prompt_client, "set_current_unit"):
        prompt_client.set_current_unit(unit_index, topic_name)

    from webapp.system_prompt_defaults import resolve_prompt_for_job
    from webapp.models import Job

    job = db.query(Job).filter(Job.id == job_id).one()
    prompt = resolve_prompt_for_job(db, "image_notes", cfg, "prompt")
    model = (cfg.get("model") or "z-ai/glm-5").strip()

  # Find subchapter from first point
    sub = (pts[0].get("subchapter") or "").strip() if pts else ""
    prompt_sub = prompt.replace("{Subchapter_Name}", sub)
    ti, tn, rows, err = processor._run_stage_e_single_topic(
        unit_index - 1,
        topic_name,
        pts,
        prompt_with_subchapter=prompt_sub,
        persian_subchapter_name=sub,
        ocr_extraction_data=ocr_data,
        model_name=model,
        output_path=out_path,
        part_num=1,
        _progress=lambda m: None,
        raw_response_kind="topic_parallel",
    )
    if err:
        raise RuntimeError(err)

    with open(out_path, "r", encoding="utf-8") as f:
        e_data = json.load(f)
    e_points = e_data.get("points") or e_data if isinstance(e_data, list) else []
    if isinstance(e_data, dict):
        e_points = e_data.get("points") or []
    kept = [r for r in e_points if isinstance(r, dict) and (r.get("topic") or "").strip() != topic_name]
    kept.extend(rows)
    if isinstance(e_data, dict):
        e_data["points"] = kept
    else:
        e_data = kept
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(e_data, f, ensure_ascii=False, indent=2)

    rel = os.path.relpath(out_path, base).replace("\\", "/")
    start = cfg.get("start_pointid") or "0010010001"
    manifest = load_manifest(job_id, pair_index)
    if not manifest:
        manifest = build_manifest_from_lesson(job_id, pair_index, out_path, str(start))
    manifest["output_relpath"] = rel
    manifest.setdefault("renumber", {})["ids_provisional"] = True
    save_manifest(job_id, pair_index, manifest)
    register_artifacts_under(db, job_id, pair_index, base, os.path.dirname(rel))


def renumber_pair(db: Session, job_id: str, pair_index: int, cfg: Dict[str, Any]) -> int:
    from webapp.models import JobPair

    pair = db.query(JobPair).filter(JobPair.job_id == job_id, JobPair.pair_index == pair_index).one()
    out_path = _output_path(job_id, pair_index, pair.stage_j_relpath)
    try:
        manifest = _ensure_from_lesson(job_id, pair_index, cfg)
    except FileNotFoundError:
        start = cfg.get("start_pointid") or "0010010001"
        manifest = build_manifest_from_lesson(job_id, pair_index, out_path, str(start))
        save_manifest(job_id, pair_index, manifest)
    with open(out_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    points = data.get("points") or []
    start_id = (manifest.get("renumber") or {}).get("start_id") or cfg.get("start_pointid") or "0010010001"
    n = renumber_points_in_rows(points, manifest, str(start_id))
    if isinstance(data, dict):
        data["points"] = points
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    mark_renumber_applied(manifest)
    save_manifest(job_id, pair_index, manifest)
    register_artifacts_under(db, job_id, pair_index, job_root(job_id), os.path.dirname(manifest["output_relpath"]))
    return n
