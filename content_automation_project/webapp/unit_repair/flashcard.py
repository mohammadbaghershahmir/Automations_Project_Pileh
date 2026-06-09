"""Flashcard (Stage H web) unit repair — per-topic regenerate."""

from __future__ import annotations

import json
import os
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy.orm import Session

from webapp.job_files import job_root, pair_dir, register_artifacts_under
from webapp.unit_repair.docproc import DocumentProcessingUnitHooks, hooks_for_pair
from webapp.unit_repair.manifest import get_unit, load_manifest, save_manifest
from webapp.unit_repair.table_notes import (
    _sync_status_from_prompts,
    filter_points_for_unit,
    topic_units_from_points,
)


def _tagged_path(job_id: str, tagged_relpath: str) -> str:
    path = os.path.join(job_root(job_id), tagged_relpath.replace("/", os.sep))
    if not os.path.isfile(path):
        raise FileNotFoundError(f"Tagged JSON not found: {path}")
    return path


def _ac_output_path(job_id: str, pair_index: int) -> str:
    """Find ac*.json in pair output directory."""
    pd = pair_dir(job_id, pair_index)
    if not os.path.isdir(pd):
        raise FileNotFoundError(f"Pair directory not found: {pd}")
    ac_files = sorted(
        fn for fn in os.listdir(pd) if fn.startswith("ac") and fn.endswith(".json")
    )
    if ac_files:
        return os.path.join(pd, ac_files[-1])
    raise FileNotFoundError("Flashcard output (ac*.json) not found")


def _rows_from_json_path(path: str) -> List[Dict[str, Any]]:
    with open(path, "r", encoding="utf-8") as f:
        raw = json.load(f)
    rows = raw.get("data") if isinstance(raw, dict) else None
    if rows is None and isinstance(raw, dict):
        rows = raw.get("points")
    if not isinstance(rows, list):
        rows = raw if isinstance(raw, list) else []
    return [r for r in rows if isinstance(r, dict)]


def _load_ac_file(path: str) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    with open(path, "r", encoding="utf-8") as f:
        doc = json.load(f)
    rows = doc.get("data") if isinstance(doc, dict) else None
    if rows is None and isinstance(doc, dict):
        rows = doc.get("points")
    if not isinstance(rows, list):
        rows = []
    return doc, rows


def _save_ac_file(path: str, doc: Dict[str, Any], rows: List[Dict[str, Any]]) -> None:
    if "data" in doc:
        doc["data"] = rows
    else:
        doc["points"] = rows
    with open(path, "w", encoding="utf-8") as f:
        json.dump(doc, f, ensure_ascii=False, indent=2)


def _apply_llm_flashcards_to_rows(
    output_rows: List[Dict[str, Any]],
    llm_rows: List[Dict[str, Any]],
    unit_point_ids: set[str],
) -> int:
    """Patch Qtext/choices on output rows for PointIds in this unit. Returns match count."""
    pid_map = {
        str(r.get("PointId", "") or ""): r
        for r in llm_rows
        if isinstance(r, dict) and (r.get("PointId") or "")
    }
    matched = 0
    for row in output_rows:
        if not isinstance(row, dict):
            continue
        pid = str(row.get("PointId", "") or "")
        if pid not in unit_point_ids:
            continue
        fc = pid_map.get(pid, {})
        if not fc:
            continue
        row["Qtext"] = fc.get("Qtext", "")
        row["Choice1"] = fc.get("Choice1", "")
        row["Choice2"] = fc.get("Choice2", "")
        row["Choice3"] = fc.get("Choice3", "")
        row["Choice4"] = fc.get("Choice4", "")
        row["Correct"] = fc.get("Correct", "")
        if row.get("Qtext") or row.get("Correct"):
            matched += 1
    return matched


def build_manifest_from_tagged(
    job_id: str,
    pair_index: int,
    tagged_path: str,
    output_relpath: Optional[str] = None,
) -> Dict[str, Any]:
    units = topic_units_from_points(_rows_from_json_path(tagged_path))
    rel = output_relpath
    if not rel:
        try:
            out_abs = _ac_output_path(job_id, pair_index)
            rel = os.path.relpath(out_abs, job_root(job_id)).replace("\\", "/")
        except FileNotFoundError:
            rel = None

    manifest = {
        "job_type": "flashcard",
        "units": units,
        "renumber": {
            "scheme": None,
            "start_id": None,
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
        raise FileNotFoundError("No tagged JSON for this pair")

    tagged = _tagged_path(job_id, pair.stage_j_relpath)
    output_rel: Optional[str] = None
    try:
        out_abs = _ac_output_path(job_id, pair_index)
        output_rel = os.path.relpath(out_abs, job_root(job_id)).replace("\\", "/")
    except FileNotFoundError:
        pass

    built = build_manifest_from_tagged(job_id, pair_index, tagged, output_rel)
    save_manifest(job_id, pair_index, built)
    return built


def regenerate_unit(
    db: Session,
    job_id: str,
    pair_index: int,
    unit_index: int,
    cfg: Dict[str, Any],
    prompt_client: Any,
) -> None:
    from stage_h_processor import StageHProcessor
    from webapp.models import JobPair
    from webapp.system_prompt_defaults import resolve_prompt_for_job

    pair = db.query(JobPair).filter(JobPair.job_id == job_id, JobPair.pair_index == pair_index).one()
    base = job_root(job_id)
    abs_tagged = _tagged_path(job_id, pair.stage_j_relpath)
    abs_catalog = os.path.join(base, (pair.word_relpath or "").replace("/", os.sep))
    out_path = _ac_output_path(job_id, pair_index)

    processor = StageHProcessor(prompt_client)
    tagged_data = processor.load_json_file(abs_tagged)
    catalog_data = processor.load_json_file(abs_catalog)
    if not tagged_data or not catalog_data:
        raise FileNotFoundError("Missing tagged JSON or image catalog")

    tagged_rows = processor.get_data_from_json(tagged_data) or []
    catalog_rows = processor.get_data_from_json(catalog_data) or []
    stage_f_json_str = json.dumps(catalog_rows, ensure_ascii=False, separators=(",", ":"))

    manifest = ensure_manifest(db, job_id, pair_index, cfg)
    unit = get_unit(manifest, unit_index)
    if not unit:
        raise ValueError(f"Unknown unit_index {unit_index}")

    unit_rows = filter_points_for_unit(tagged_rows, unit)
    ch = (unit.get("chapter") or "").strip() or (
        (unit_rows[0].get("chapter") or "").strip() if unit_rows else ""
    )
    sub = (unit.get("subchapter") or "").strip() or (
        (unit_rows[0].get("subchapter") or "").strip() if unit_rows else ""
    )
    topic_name = (unit.get("topic") or "").strip()

    if not unit_rows:
        raise ValueError(
            f"No tagged rows for unit {unit_index} ({ch} > {sub} > {topic_name})"
        )

    prompt = resolve_prompt_for_job(db, "flashcard", cfg, "prompt")
    model = (cfg.get("model") or "z-ai/glm-5").strip()

    if hasattr(prompt_client, "set_current_unit"):
        prompt_client.set_current_unit(unit_index, topic_name)

    llm_rows, err, _prompt_calls = processor._run_flashcards_for_lesson_rows(
        unit_rows,
        stage_f_json_str,
        prompt,
        model,
        cancel_check=None,
    )
    if err:
        raise RuntimeError(err)
    if not llm_rows:
        raise RuntimeError(
            f"Regenerate unit {unit_index} produced no flashcards for "
            f"«{topic_name}» in subchapter «{sub}»"
        )

    doc, output_rows = _load_ac_file(out_path)
    unit_pids = {str(r.get("PointId", "") or "") for r in unit_rows if r.get("PointId")}
    _apply_llm_flashcards_to_rows(output_rows, llm_rows, unit_pids)
    _save_ac_file(out_path, doc, output_rows)

    unit_output = filter_points_for_unit(output_rows, unit)
    hooks = hooks_for_pair(db, job_id, pair_index, "flashcard", cfg, prompt_client)
    hooks.after_unit(
        unit_index,
        ch,
        sub,
        topic_name,
        unit_output,
        int(unit.get("prompt_seq") or unit_index),
        status="succeeded",
    )

    manifest = ensure_manifest(db, job_id, pair_index, cfg)
    rel = os.path.relpath(out_path, base).replace("\\", "/")
    manifest["output_relpath"] = rel
    save_manifest(job_id, pair_index, manifest)
    register_artifacts_under(db, job_id, pair_index, base, os.path.dirname(rel))


__all__ = [
    "DocumentProcessingUnitHooks",
    "ensure_manifest",
    "hooks_for_pair",
    "regenerate_unit",
    "topic_units_from_points",
]
