"""Table notes (Stage TA) unit repair."""

from __future__ import annotations

import json
import os
import re
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy.orm import Session

from webapp.job_files import job_root, pair_dir, register_artifacts_under
from webapp.unit_repair.docproc import DocumentProcessingUnitHooks, hooks_for_pair
from webapp.unit_repair.manifest import load_manifest, save_manifest
from webapp.unit_repair.renumber import mark_renumber_applied, renumber_points_in_rows


def topic_units_from_points(points: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """One unit per unique topic name, in first-seen order (matches regenerate_unit)."""
    units: List[Dict[str, Any]] = []
    seen: set[str] = set()
    for row in points:
        if not isinstance(row, dict):
            continue
        topic = (row.get("topic") or "").strip()
        if not topic or topic in seen:
            continue
        seen.add(topic)
        chapter = (row.get("chapter") or "").strip()
        subchapter = (row.get("subchapter") or "").strip()
        units.append(
            {
                "unit_index": len(units) + 1,
                "label": f"{chapter} > {subchapter} > {topic}".strip(" >"),
                "chapter": chapter,
                "subchapter": subchapter,
                "topic": topic,
                "status": "pending",
            }
        )
    return units


def topic_unit_map(units: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    return {(u.get("topic") or "").strip(): u for u in units if (u.get("topic") or "").strip()}


def _stage_e_path(job_id: str, stage_e_relpath: str) -> str:
    path = os.path.join(job_root(job_id), stage_e_relpath.replace("/", os.sep))
    if not os.path.isfile(path):
        raise FileNotFoundError(f"Stage E JSON not found: {path}")
    return path


def _ta_output_path(job_id: str, stage_e_relpath: str) -> str:
    """Find ta*.json written beside Stage E (Stage TA output)."""
    out_dir = os.path.dirname(_stage_e_path(job_id, stage_e_relpath))
    ta_files = sorted(
        fn for fn in os.listdir(out_dir) if fn.startswith("ta") and fn.endswith(".json")
    )
    if ta_files:
        return os.path.join(out_dir, ta_files[-1])
    raise FileNotFoundError("Stage TA output (ta*.json) not found")


def _load_ta_file(path: str) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    with open(path, "r", encoding="utf-8") as f:
        doc = json.load(f)
    rows = doc.get("data") if isinstance(doc, dict) else None
    if rows is None:
        rows = doc.get("points") if isinstance(doc, dict) else doc
    if not isinstance(rows, list):
        rows = []
    return doc, rows


def _save_ta_file(path: str, doc: Dict[str, Any], rows: List[Dict[str, Any]]) -> None:
    if "data" in doc:
        doc["data"] = rows
    else:
        doc["points"] = rows
    with open(path, "w", encoding="utf-8") as f:
        json.dump(doc, f, ensure_ascii=False, indent=2)


def _sync_status_from_prompts(job_id: str, pair_index: int, manifest: Dict[str, Any]) -> None:
    prompts_dir = os.path.join(pair_dir(job_id, pair_index), "prompts")
    if not os.path.isdir(prompts_dir):
        return
    done: set[int] = set()
    for fn in os.listdir(prompts_dir):
        m = re.search(r"_u(\d{3})_", fn)
        if m:
            done.add(int(m.group(1)))
    for u in manifest.get("units") or []:
        idx = int(u.get("unit_index") or 0)
        if idx in done and u.get("status") in (None, "", "pending", "running"):
            u["status"] = "succeeded"


def build_manifest_from_stage_e(
    job_id: str,
    pair_index: int,
    stage_e_path: str,
    output_relpath: Optional[str] = None,
    start_pointid: str = "0010010001",
) -> Dict[str, Any]:
    with open(stage_e_path, "r", encoding="utf-8") as f:
        raw = json.load(f)
    points = raw.get("data") if isinstance(raw, dict) else None
    if points is None and isinstance(raw, dict):
        points = raw.get("points")
    if not isinstance(points, list):
        points = raw if isinstance(raw, list) else []

    units = topic_units_from_points(points)
    rel = output_relpath
    if not rel and os.path.isfile(stage_e_path):
        try:
            out_abs = _ta_output_path(job_id, os.path.relpath(stage_e_path, job_root(job_id)).replace("\\", "/"))
            rel = os.path.relpath(out_abs, job_root(job_id)).replace("\\", "/")
        except FileNotFoundError:
            rel = None

    manifest = {
        "job_type": "table_notes",
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
        raise FileNotFoundError("No Stage E input for this pair")

    stage_e = _stage_e_path(job_id, pair.stage_j_relpath)
    output_rel: Optional[str] = None
    try:
        out_abs = _ta_output_path(job_id, pair.stage_j_relpath)
        output_rel = os.path.relpath(out_abs, job_root(job_id)).replace("\\", "/")
    except FileNotFoundError:
        pass

    start = (cfg.get("start_pointid") or "0010010001").strip()
    built = build_manifest_from_stage_e(job_id, pair_index, stage_e, output_rel, start)
    save_manifest(job_id, pair_index, built)
    return built


def _tablepic_path(stage_e_path: str) -> str:
    base_dir = os.path.dirname(stage_e_path)
    base_name, _ = os.path.splitext(os.path.basename(stage_e_path))
    if base_name.startswith("e") and len(base_name) > 1 and base_name[1:4].isdigit():
        base_name = base_name[1:]
    return os.path.join(base_dir, f"{base_name}_tablepic.json")


def regenerate_unit(
    db: Session,
    job_id: str,
    pair_index: int,
    unit_index: int,
    cfg: Dict[str, Any],
    prompt_client: Any,
) -> None:
    from stage_ta_processor import StageTAProcessor
    from webapp.models import JobPair
    from webapp.system_prompt_defaults import resolve_prompt_for_job
    from webapp.unit_repair.pic_sidecar import apply_regenerate_to_merged_files

    pair = db.query(JobPair).filter(JobPair.job_id == job_id, JobPair.pair_index == pair_index).one()
    base = job_root(job_id)
    abs_e = _stage_e_path(job_id, pair.stage_j_relpath)
    abs_ocr = os.path.join(base, (pair.word_relpath or "").replace("/", os.sep))
    out_path = _ta_output_path(job_id, pair.stage_j_relpath)

    processor = StageTAProcessor(prompt_client)
    stage_e_data = processor.load_json_file(abs_e)
    ocr_data = processor.load_json_file(abs_ocr)
    if not stage_e_data or not ocr_data:
        raise FileNotFoundError("Missing Stage E or OCR JSON")

    e_points = processor.get_data_from_json(stage_e_data)
    units = topic_units_from_points(e_points)
    if unit_index < 1 or unit_index > len(units):
        raise ValueError(f"unit_index {unit_index} out of range")
    unit = units[unit_index - 1]
    topic_name = unit["topic"]
    pts = [p for p in e_points if isinstance(p, dict) and (p.get("topic") or "").strip() == topic_name]
    sub = (pts[0].get("subchapter") or "").strip() if pts else unit.get("subchapter") or ""

    prompt = resolve_prompt_for_job(db, "table_notes", cfg, "prompt")
    model = (cfg.get("model") or "z-ai/glm-5").strip()
    prompt_sub = prompt.replace("{SUBCHAPTER_NAME}", sub).replace("{Subchapter_Name}", sub)

    subchapter_tables = processor._extract_tables_from_ocr(ocr_data)
    if hasattr(prompt_client, "set_current_unit"):
        prompt_client.set_current_unit(unit_index, topic_name)

    _ti, tn, rows, err = processor._run_stage_ta_single_topic(
        unit_index - 1,
        topic_name,
        pts,
        prompt_with_subchapter=prompt_sub,
        persian_subchapter_name=sub,
        ocr_extraction_data=ocr_data,
        subchapter_tables=subchapter_tables,
        model_name=model,
        output_path=out_path,
        part_num=1,
        _progress=lambda _m: None,
        raw_response_kind="topic_parallel",
    )
    if err:
        raise RuntimeError(err)

    tablepic_path = _tablepic_path(abs_e)
    apply_regenerate_to_merged_files(
        processor,
        source_points=e_points,
        pic_path=tablepic_path,
        main_path=out_path,
        topic_name=topic_name,
        new_pic_rows=rows,
        pic_stage_label="tablepic",
        notes_count_key="table_notes_count",
        source_count_key="stage_e_total_points",
        first_note_id_key="first_table_point_id",
        pic_meta_prefix="tablepic",
    )

    rel = os.path.relpath(out_path, base).replace("\\", "/")
    manifest = ensure_manifest(db, job_id, pair_index, cfg)
    manifest["output_relpath"] = rel
    manifest.setdefault("renumber", {})["ids_provisional"] = True
    save_manifest(job_id, pair_index, manifest)
    register_artifacts_under(db, job_id, pair_index, base, os.path.dirname(rel))


def renumber_pair(db: Session, job_id: str, pair_index: int, cfg: Dict[str, Any]) -> int:
    from webapp.models import JobPair

    pair = db.query(JobPair).filter(JobPair.job_id == job_id, JobPair.pair_index == pair_index).one()
    out_path = _ta_output_path(job_id, pair.stage_j_relpath)
    manifest = ensure_manifest(db, job_id, pair_index, cfg)
    doc, points = _load_ta_file(out_path)
    start_id = (manifest.get("renumber") or {}).get("start_id") or cfg.get("start_pointid") or "0010010001"
    n = renumber_points_in_rows(points, manifest, str(start_id))
    _save_ta_file(out_path, doc, points)
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
