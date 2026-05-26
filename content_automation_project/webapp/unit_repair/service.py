"""Dispatch unit regenerate / renumber by job type."""

from __future__ import annotations

import json
import logging
from typing import Any, Dict, Optional

from sqlalchemy.orm import Session

from webapp.job_files import append_log, job_root
from webapp.models import Job, JobPair
from webapp.processor_context import build_unified_api_client
from webapp.prompt_capture import wrap_prompt_capture
from webapp.system_prompt_defaults import resolve_prompt_for_job
from webapp.unit_repair.lock import pair_repair_lock
from webapp.unit_repair.manifest import load_manifest
from webapp.unit_repair.registry import job_supports_unit_repair, renumber_scheme_for_job

logger = logging.getLogger(__name__)


def _cfg(job: Job) -> Dict[str, Any]:
    return json.loads(job.config_json or "{}")


def _pair(db: Session, job_id: str, pair_index: int) -> JobPair:
    p = (
        db.query(JobPair)
        .filter(JobPair.job_id == job_id, JobPair.pair_index == pair_index)
        .one_or_none()
    )
    if not p:
        raise ValueError(f"Pair {pair_index} not found")
    return p


def run_regenerate_unit(db: Session, job_id: str, pair_index: int, unit_index: int) -> None:
    job = db.query(Job).filter(Job.id == job_id).one_or_none()
    if not job:
        raise ValueError("Job not found")
    jt = (job.type or "").strip()
    if not job_supports_unit_repair(jt):
        raise ValueError(f"Job type {jt} does not support unit repair")
    pair = _pair(db, job_id, pair_index)
    cfg = _cfg(job)
    base = job_root(job_id)

    with pair_repair_lock(job_id, pair_index):
        append_log(db, job_id, f"Regenerate unit {unit_index} started for pair {pair_index}.", pair_index)
        client, _ssm = build_unified_api_client()
        cap = wrap_prompt_capture(client, db, job_id, pair_index, jt, _pipeline_step(jt))

        if jt == "document_processing":
            from webapp.unit_repair import docproc

            ocr = __import__("os").path.join(base, pair.stage_j_relpath.replace("/", __import__("os").sep))
            docproc.regenerate_unit(
                db,
                job_id,
                pair_index,
                unit_index,
                cfg,
                ocr,
                resolve_prompt_for_job(db, jt, cfg, "prompt"),
                (cfg.get("model") or "z-ai/glm-5").strip(),
                prompt_client=cap,
            )
        elif jt in ("test_bank", "test_bank_2"):
            from webapp.unit_repair import testbank

            step1_rel = _step1_path_for_pair(cfg, pair, jt)
            abs_j = __import__("os").path.join(base, pair.stage_j_relpath.replace("/", __import__("os").sep))
            abs_w = __import__("os").path.join(base, (pair.word_relpath or "").replace("/", __import__("os").sep))
            abs_s1 = __import__("os").path.join(base, step1_rel.replace("/", __import__("os").sep))
            testbank.regenerate_unit(
                db,
                job_id,
                pair_index,
                unit_index,
                cfg,
                abs_j,
                abs_w,
                abs_s1,
                resolve_prompt_for_job(db, jt, cfg, "prompt_2"),
                (cfg.get("model_2") or cfg.get("model") or "z-ai/glm-5").strip(),
                (cfg.get("provider_2") or cfg.get("provider") or "openrouter").strip(),
                (cfg.get("model") or cfg.get("model_1") or "z-ai/glm-5").strip(),
                prompt_client=cap,
                job_type=jt,
            )
        elif jt == "image_notes":
            from webapp.unit_repair import image_notes as img_rep

            img_rep.regenerate_unit(db, job_id, pair_index, unit_index, cfg, cap)
        elif jt == "table_notes":
            from webapp.unit_repair import table_notes as tbl_rep

            tbl_rep.regenerate_unit(db, job_id, pair_index, unit_index, cfg, cap)
        elif jt == "ocr_extraction":
            from webapp.unit_repair import ocr_extraction as ocr_rep

            ocr_rep.regenerate_unit(db, job_id, pair_index, unit_index, cfg, cap)
        else:
            raise ValueError(f"No regenerate adapter for {jt}")

        append_log(db, job_id, f"Regenerate unit {unit_index} finished for pair {pair_index}.", pair_index)


def run_renumber_pair(db: Session, job_id: str, pair_index: int) -> int:
    job = db.query(Job).filter(Job.id == job_id).one_or_none()
    if not job:
        raise ValueError("Job not found")
    jt = (job.type or "").strip()
    scheme = renumber_scheme_for_job(jt)
    if not scheme:
        raise ValueError(f"Job type {jt} does not support renumber")
    pair = _pair(db, job_id, pair_index)
    cfg = _cfg(job)
    base = job_root(job_id)

    with pair_repair_lock(job_id, pair_index):
        append_log(db, job_id, f"Renumber ({scheme}) started for pair {pair_index}.", pair_index)
        n = 0
        if jt == "document_processing":
            from webapp.unit_repair import docproc

            n = docproc.renumber_pair(db, job_id, pair_index, cfg)
        elif jt in ("test_bank", "test_bank_2"):
            from webapp.unit_repair import testbank

            step1_rel = _step1_path_for_pair(cfg, pair, jt)
            abs_j = __import__("os").path.join(base, pair.stage_j_relpath.replace("/", __import__("os").sep))
            abs_w = __import__("os").path.join(base, (pair.word_relpath or "").replace("/", __import__("os").sep))
            abs_s1 = __import__("os").path.join(base, step1_rel.replace("/", __import__("os").sep))
            n = testbank.renumber_pair(db, job_id, pair_index, cfg, abs_j, abs_w, abs_s1, jt)
        elif jt in ("image_notes", "table_notes"):
            from webapp.unit_repair import image_notes as img_rep

            n = img_rep.renumber_pair(db, job_id, pair_index, cfg)
        else:
            raise ValueError(f"No renumber adapter for {jt}")
        append_log(db, job_id, f"Renumber finished: {n} row(s) for pair {pair_index}.", pair_index)
        return n


def get_units_payload(job_id: str, pair_index: int, job_type: str) -> Dict[str, Any]:
    m = load_manifest(job_id, pair_index)
    scheme = renumber_scheme_for_job(job_type)
    if not m:
        return {
            "units": [],
            "renumber": {"scheme": scheme, "ids_provisional": False, "last_applied_at": None},
            "supports_renumber": bool(scheme),
        }
    ren = m.get("renumber") or {}
    return {
        "units": m.get("units") or [],
        "renumber": {
            "scheme": ren.get("scheme") or scheme,
            "start_id": ren.get("start_id"),
            "ids_provisional": bool(ren.get("ids_provisional")),
            "last_applied_at": ren.get("last_applied_at"),
        },
        "output_relpath": m.get("output_relpath"),
        "supports_renumber": bool(scheme),
    }


def _pipeline_step(job_type: str) -> str:
    if job_type in ("test_bank", "test_bank_2"):
        return "step2"
    return "step1"


def _step1_path_for_pair(cfg: Dict[str, Any], pair: JobPair, job_type: str) -> str:
    if job_type == "test_bank_2":
        rel = (cfg.get("step1_combined_relpaths") or {}).get(str(pair.pair_index), "")
        if not rel:
            raise FileNotFoundError("Missing step1_combined_relpaths for pair")
        return rel
    from webapp.job_files import pair_output
    import os

    out = pair_output(pair.job_id, pair.pair_index)
    for fn in os.listdir(out) if os.path.isdir(out) else []:
        if fn.startswith("step1_combined") and fn.endswith(".json"):
            return os.path.relpath(os.path.join(out, fn), job_root(pair.job_id)).replace("\\", "/")
    raise FileNotFoundError("Step 1 combined JSON not found for pair")
