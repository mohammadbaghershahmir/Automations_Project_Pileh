"""OCR extraction unit repair — regenerate one subchapter LLM call."""

from __future__ import annotations

import json
import os
from typing import Any, Dict, List

from sqlalchemy.orm import Session

from webapp.job_files import job_root, register_artifacts_under
from webapp.unit_repair.manifest import load_manifest, save_manifest, upsert_unit


def _find_ocr_output(job_id: str, pair_index: int) -> str:
    from webapp.job_files import pair_output

    out = pair_output(job_id, pair_index)
    for fn in os.listdir(out):
        if fn.startswith("OCR Extraction") and fn.endswith(".json"):
            return os.path.join(out, fn)
    raise FileNotFoundError("OCR output JSON not found")


def _build_subchapter_manifest(job_id: str, pair_index: int, topic_path: str) -> Dict[str, Any]:
    with open(topic_path, "r", encoding="utf-8") as f:
        topic_data = json.load(f)
    items = topic_data.get("data") or []
    units = []
    for i, item in enumerate(items, 1):
        name = (item.get("Subchapter") or item.get("subchapter") or f"Subchapter_{i}").strip()
        units.append(
            {
                "unit_index": i,
                "label": name,
                "subchapter": name,
                "status": "succeeded",
            }
        )
    return {
        "job_type": "ocr_extraction",
        "units": units,
        "renumber": {"scheme": None, "start_id": None, "last_applied_at": None, "ids_provisional": False},
        "output_relpath": None,
    }


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
    abs_topic = os.path.join(base, (pair.word_relpath or "").replace("/", os.sep))
    out_path = _find_ocr_output(job_id, pair_index)

    mproc = MultiPartProcessor(prompt_client, output_dir=os.path.dirname(out_path))
    prompt = resolve_prompt_for_job(db, "ocr_extraction", cfg, "prompt")
    model = (cfg.get("model") or "z-ai/glm-5").strip()

    if hasattr(prompt_client, "set_current_unit"):
        prompt_client.set_current_unit(unit_index, f"subchapter_{unit_index}")

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

    manifest = load_manifest(job_id, pair_index) or _build_subchapter_manifest(job_id, pair_index, abs_topic)
    rel = os.path.relpath(out_path, base).replace("\\", "/")
    manifest["output_relpath"] = rel
    upsert_unit(
        manifest,
        {
            "unit_index": unit_index,
            "label": next(
                (u["label"] for u in manifest.get("units", []) if u.get("unit_index") == unit_index),
                f"Subchapter {unit_index}",
            ),
            "subchapter": next(
                (u.get("subchapter") for u in manifest.get("units", []) if u.get("unit_index") == unit_index),
                "",
            ),
            "status": "succeeded",
        },
    )
    save_manifest(job_id, pair_index, manifest)
    register_artifacts_under(db, job_id, pair_index, base, os.path.dirname(rel))
