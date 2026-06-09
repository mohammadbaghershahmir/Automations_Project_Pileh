"""Voice Class Step 1 unit repair — per-topic script regenerate."""

from __future__ import annotations

import json
import os
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy.orm import Session

from webapp.job_files import job_root, pair_output, register_artifacts_under
from webapp.unit_repair.docproc import DocumentProcessingUnitHooks, hooks_for_pair
from webapp.unit_repair.manifest import get_unit, load_manifest, save_manifest
from webapp.unit_repair.table_notes import (
    _sync_status_from_prompts,
    filter_points_for_unit,
    topic_units_from_points,
)
from webapp.voice_class_inputs import resolve_voice_class_pair_files, VoiceClassPairInputError
from webapp.tasks_voice_class import pair_media_entry


def _tagged_path(job_id: str, tagged_relpath: str) -> str:
    path = os.path.join(job_root(job_id), tagged_relpath.replace("/", os.sep))
    if not os.path.isfile(path):
        raise FileNotFoundError(f"Tagged JSON not found: {path}")
    return path


def _voice_script_path(job_id: str, pair_index: int) -> str:
    pd = pair_output(job_id, pair_index)
    if not os.path.isdir(pd):
        raise FileNotFoundError(f"Pair output directory not found: {pd}")
    scripts = sorted(
        fn for fn in os.listdir(pd) if fn.startswith("voice_script_") and fn.endswith(".json")
    )
    if scripts:
        return os.path.join(pd, scripts[-1])
    raise FileNotFoundError("Voice script JSON not found")


def _load_script(path: str) -> Tuple[Dict[str, Any], List[Dict[str, Any]], List[Dict[str, Any]]]:
    with open(path, "r", encoding="utf-8") as f:
        doc = json.load(f)
    paragraphs = doc.get("paragraphs") if isinstance(doc, dict) else None
    if not isinstance(paragraphs, list):
        paragraphs = []
    segments = doc.get("segments") if isinstance(doc, dict) else None
    if not isinstance(segments, list):
        segments = []
    return doc, paragraphs, segments


def _save_script(
    path: str,
    doc: Dict[str, Any],
    paragraphs: List[Dict[str, Any]],
    segments: List[Dict[str, Any]],
) -> None:
    doc["paragraphs"] = paragraphs
    doc["segments"] = segments
    meta = doc.setdefault("metadata", {})
    if isinstance(meta, dict):
        meta["total_paragraphs"] = len(paragraphs)
        meta["total_segments"] = len(segments)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(doc, f, ensure_ascii=False, indent=2)


def build_manifest_from_lesson_context(
    job_id: str,
    pair_index: int,
    lesson_context: List[Dict[str, Any]],
    output_relpath: Optional[str] = None,
) -> Dict[str, Any]:
    units = topic_units_from_points(lesson_context)
    rel = output_relpath
    if not rel:
        try:
            out_abs = _voice_script_path(job_id, pair_index)
            rel = os.path.relpath(out_abs, job_root(job_id)).replace("\\", "/")
        except FileNotFoundError:
            rel = None

    manifest = {
        "job_type": "voice_class",
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

    from stage_voice_processor import StageVoiceProcessor
    from webapp.models import JobPair

    pair = (
        db.query(JobPair)
        .filter(JobPair.job_id == job_id, JobPair.pair_index == pair_index)
        .one_or_none()
    )
    if not pair or not pair.stage_j_relpath:
        raise FileNotFoundError("No tagged JSON for this pair")

    base = job_root(job_id)
    resolved = resolve_voice_class_pair_files(
        base,
        tagged_relpath=pair.stage_j_relpath,
        pair_media=pair_media_entry(cfg, pair_index),
    )
    if isinstance(resolved, VoiceClassPairInputError):
        raise FileNotFoundError(resolved.message)

    processor = StageVoiceProcessor(None)
    records = processor._load_tagged_records(resolved.tagged_json)
    if not records:
        raise FileNotFoundError("Tagged JSON has no rows")
    lesson_context = processor._build_lesson_context(records)

    output_rel: Optional[str] = None
    try:
        out_abs = _voice_script_path(job_id, pair_index)
        output_rel = os.path.relpath(out_abs, base).replace("\\", "/")
    except FileNotFoundError:
        pass

    built = build_manifest_from_lesson_context(job_id, pair_index, lesson_context, output_rel)
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
    from stage_voice_processor import StageVoiceProcessor
    from webapp.models import JobPair
    from webapp.system_prompt_defaults import resolve_prompt_for_job

    pair = db.query(JobPair).filter(JobPair.job_id == job_id, JobPair.pair_index == pair_index).one()
    base = job_root(job_id)
    resolved = resolve_voice_class_pair_files(
        base,
        tagged_relpath=pair.stage_j_relpath,
        pair_media=pair_media_entry(cfg, pair_index),
    )
    if isinstance(resolved, VoiceClassPairInputError):
        raise FileNotFoundError(resolved.message)

    script_path = _voice_script_path(job_id, pair_index)
    manifest = ensure_manifest(db, job_id, pair_index, cfg)
    unit = get_unit(manifest, unit_index)
    if not unit:
        raise ValueError(f"Unknown unit_index {unit_index}")

    processor = StageVoiceProcessor(prompt_client)
    records = processor._load_tagged_records(resolved.tagged_json)
    if not records:
        raise FileNotFoundError("Tagged JSON has no rows")
    lesson_context = processor._build_lesson_context(records)
    topic_rows = [
        r
        for r in filter_points_for_unit(lesson_context, unit)
        if isinstance(r, dict)
    ]
    ch = (unit.get("chapter") or "").strip() or (
        (topic_rows[0].get("chapter") or "").strip() if topic_rows else ""
    )
    sub = (unit.get("subchapter") or "").strip() or (
        (topic_rows[0].get("subchapter") or "").strip() if topic_rows else ""
    )
    topic_name = (unit.get("topic") or "").strip()

    if not topic_rows:
        raise ValueError(
            f"No Imp 1–2 rows for unit {unit_index} ({ch} > {sub} > {topic_name})"
        )

    prompt = resolve_prompt_for_job(db, "voice_class", cfg, "prompt_1")
    model = (cfg.get("model_1") or cfg.get("model") or "z-ai/glm-5").strip()
    max_seg = float(cfg.get("max_segment_seconds", 60.0))
    cps = float(cfg.get("chars_per_second", 13.0))

    if hasattr(prompt_client, "set_current_unit"):
        prompt_client.set_current_unit(unit_index, topic_name)

    caption_indexes = processor._load_caption_indexes(
        resolved.filepic_json,
        resolved.tablepic_json,
    )
    units = topic_units_from_points(lesson_context)
    total_topics = len(units)

    topic_paragraphs = processor._call_llm_for_topic_script(
        prompt=prompt,
        chapter_name=ch,
        subchapter_name=sub,
        topic_name=topic_name,
        topic_rows=topic_rows,
        topic_index=unit_index,
        total_topics=total_topics,
        caption_indexes=caption_indexes,
        model_name=model,
        cancel_check=None,
    )
    if not topic_paragraphs:
        raise RuntimeError(
            f"Regenerate unit {unit_index} produced no paragraphs for "
            f"«{topic_name}» in subchapter «{sub}»"
        )

    doc, paragraphs, _segments = _load_script(script_path)
    kept = processor._remove_unit_paragraphs(paragraphs, ch, sub, topic_name)
    insert_at = processor._insert_index_for_unit(kept, ch, sub, topic_name, units, unit_index)
    renumbered_topic, _ = processor._renumber_topic_paragraphs(
        topic_paragraphs,
        chapter_name=ch,
        subchapter_name=sub,
        topic_name=topic_name,
        start_paragraph_id=1,
        chars_per_second=cps,
    )
    merged = list(kept[:insert_at]) + renumbered_topic + list(kept[insert_at:])
    merged = processor._renumber_all_paragraphs(merged, chars_per_second=cps)
    segments = processor.pack_segments(
        merged,
        max_segment_seconds=max_seg,
        chars_per_second=cps,
    )
    if not segments:
        raise RuntimeError("Segment packing produced no segments after regenerate")

    _save_script(script_path, doc, merged, segments)

    unit_output = processor._filter_paragraphs_for_unit(merged, ch, sub, topic_name)
    hooks = hooks_for_pair(db, job_id, pair_index, "voice_class", cfg, prompt_client)
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
    rel = os.path.relpath(script_path, base).replace("\\", "/")
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
