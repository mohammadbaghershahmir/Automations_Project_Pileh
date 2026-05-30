"""OCR extraction unit repair — one LLM unit per subchapter."""

from __future__ import annotations

import json
import os
from typing import Any, Dict, List, Optional

from sqlalchemy.orm import Session

from webapp.job_files import job_root, pair_output, register_artifacts_under
from webapp.unit_repair.docproc import DocumentProcessingUnitHooks, hooks_for_pair
from webapp.unit_repair.manifest import load_manifest, save_manifest, upsert_unit
from webapp.unit_repair.table_notes import _sync_status_from_prompts


def _find_ocr_output(job_id: str, pair_index: int) -> str:
    out = pair_output(job_id, pair_index)
    if not os.path.isdir(out):
        raise FileNotFoundError("OCR output directory not found")
    for fn in os.listdir(out):
        if fn.startswith("OCR Extraction") and fn.endswith(".json"):
            return os.path.join(out, fn)
    raise FileNotFoundError("OCR output JSON not found")


def _topic_path(job_id: str, topic_relpath: str) -> str:
    path = os.path.join(job_root(job_id), topic_relpath.replace("/", os.sep))
    if not os.path.isfile(path):
        raise FileNotFoundError(f"Topic JSON not found: {path}")
    return path


def _load_topic_items(topic_path: str) -> List[Dict[str, Any]]:
    with open(topic_path, "r", encoding="utf-8") as f:
        topic_data = json.load(f)
    items = topic_data.get("data") or []
    return [x for x in items if isinstance(x, dict)]


def subchapter_units_from_topic(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """One unit per subchapter row in the Pre-OCR topic file (1-based unit_index)."""
    units: List[Dict[str, Any]] = []
    for i, item in enumerate(items, 1):
        name = (item.get("Subchapter") or item.get("subchapter") or f"Subchapter_{i}").strip()
        units.append(
            {
                "unit_index": i,
                "label": name,
                "subchapter": name,
                "chapter": "",
                "topic": "",
                "status": "pending",
            }
        )
    return units


def build_manifest_from_topic(
    job_id: str,
    pair_index: int,
    topic_path: str,
    output_relpath: Optional[str] = None,
) -> Dict[str, Any]:
    units = subchapter_units_from_topic(_load_topic_items(topic_path))
    manifest = {
        "job_type": "ocr_extraction",
        "units": units,
        "renumber": {"scheme": None, "start_id": None, "last_applied_at": None, "ids_provisional": False},
        "output_relpath": output_relpath,
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
    if not pair or not pair.word_relpath:
        raise FileNotFoundError("No topic JSON paired for this pair")

    topic_path = _topic_path(job_id, pair.word_relpath)
    output_rel: Optional[str] = None
    try:
        out_abs = _find_ocr_output(job_id, pair_index)
        output_rel = os.path.relpath(out_abs, job_root(job_id)).replace("\\", "/")
    except FileNotFoundError:
        pass

    built = build_manifest_from_topic(job_id, pair_index, topic_path, output_rel)
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
    from multi_part_processor import MultiPartProcessor
    from webapp.models import JobPair
    from webapp.system_prompt_defaults import resolve_prompt_for_job

    pair = db.query(JobPair).filter(JobPair.job_id == job_id, JobPair.pair_index == pair_index).one()
    base = job_root(job_id)
    abs_pdf = os.path.join(base, pair.stage_j_relpath.replace("/", os.sep))
    abs_topic = _topic_path(job_id, pair.word_relpath or "")
    out_path = _find_ocr_output(job_id, pair_index)

    mproc = MultiPartProcessor(prompt_client, output_dir=os.path.dirname(out_path))
    prompt = resolve_prompt_for_job(db, "ocr_extraction", cfg, "prompt")
    model = (cfg.get("model") or "z-ai/glm-5").strip()

    if hasattr(prompt_client, "set_current_unit"):
        items = _load_topic_items(abs_topic)
        label = subchapter_units_from_topic(items)[unit_index - 1]["label"] if 0 < unit_index <= len(items) else f"subchapter_{unit_index}"
        prompt_client.set_current_unit(unit_index, label)

    ok = mproc.regenerate_ocr_extraction_subchapter(
        pdf_path=abs_pdf,
        topic_file_path=abs_topic,
        base_prompt=prompt,
        model_name=model,
        subchapter_index=unit_index,
        output_json_path=str(out_path),
        progress_callback=None,
    )
    if not ok:
        raise RuntimeError(f"OCR regenerate failed for subchapter unit {unit_index}")

    manifest = ensure_manifest(db, job_id, pair_index, cfg)
    rel = os.path.relpath(out_path, base).replace("\\", "/")
    manifest["output_relpath"] = rel
    unit_row = next((u for u in manifest.get("units") or [] if int(u.get("unit_index") or 0) == unit_index), None)
    upsert_unit(
        manifest,
        {
            "unit_index": unit_index,
            "label": (unit_row or {}).get("label") or f"Subchapter {unit_index}",
            "subchapter": (unit_row or {}).get("subchapter") or "",
            "chapter": "",
            "topic": "",
            "status": "succeeded",
        },
    )
    save_manifest(job_id, pair_index, manifest)
    register_artifacts_under(db, job_id, pair_index, base, os.path.dirname(rel))


__all__ = [
    "DocumentProcessingUnitHooks",
    "ensure_manifest",
    "hooks_for_pair",
    "regenerate_unit",
    "subchapter_units_from_topic",
]
