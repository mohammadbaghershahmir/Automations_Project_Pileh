"""Test Bank Step 2 unit repair: manifest, regenerate topic, renumber QIds."""

from __future__ import annotations

import json
import os
import re
import shutil
from typing import Any, Callable, Dict, List, Optional

from sqlalchemy.orm import Session

from webapp.job_files import job_root, pair_output, register_artifacts_under
from webapp.unit_repair.manifest import (
    abs_from_relpath,
    get_unit,
    load_manifest,
    save_manifest,
    unit_artifact_relpath,
    upsert_unit,
)
from webapp.unit_repair.renumber import mark_renumber_applied, renumber_qids_in_rows


class TestBankStep2UnitHooks:
    """Track Step 2 topics and copy artifacts into pair_N/units/."""

    def __init__(self, job_id: str, pair_index: int, job_type: str, book_id: int, chapter_id: int):
        self.job_id = job_id
        self.pair_index = pair_index
        self.job_type = job_type
        self.book_id = book_id
        self.chapter_id = chapter_id
        self._manifest: Optional[Dict[str, Any]] = None

    def _ensure(self) -> Dict[str, Any]:
        if self._manifest is None:
            self._manifest = load_manifest(self.job_id, self.pair_index) or {
                "job_type": self.job_type,
                "units": [],
                "renumber": {
                    "scheme": "qid",
                    "start_id": f"{self.book_id:03d}{self.chapter_id:03d}0001",
                    "last_applied_at": None,
                    "ids_provisional": False,
                },
                "output_relpath": None,
            }
        return self._manifest

    def on_topic_done(
        self,
        topic_idx: int,
        chapter_name: str,
        subchapter_name: str,
        topic_name: str,
        topic_json_path: str,
        prompt_seq: int,
        status: str = "succeeded",
    ) -> None:
        m = self._ensure()
        safe = re.sub(r"[^a-zA-Z0-9._-]+", "_", topic_name[:60])
        rel = unit_artifact_relpath(self.job_id, self.pair_index, topic_idx, f"step2_{safe}.json")
        abs_dest = abs_from_relpath(self.job_id, rel)
        os.makedirs(os.path.dirname(abs_dest), exist_ok=True)
        if os.path.isfile(topic_json_path):
            shutil.copy2(topic_json_path, abs_dest)
        label = f"{chapter_name} > {subchapter_name} > {topic_name}".strip(" >")
        upsert_unit(
            m,
            {
                "unit_index": topic_idx,
                "label": label,
                "chapter": chapter_name,
                "subchapter": subchapter_name,
                "topic": topic_name,
                "topic_name": topic_name,
                "chapter_name": chapter_name,
                "subchapter_name": subchapter_name,
                "prompt_seq": prompt_seq,
                "status": status,
                "artifact_relpath": rel,
            },
        )
        save_manifest(self.job_id, self.pair_index, m)

    def set_final_output(self, relpath: str) -> None:
        m = self._ensure()
        m["output_relpath"] = relpath.replace("\\", "/")
        save_manifest(self.job_id, self.pair_index, m)

    def seed_topics(self, topics_list: List[tuple]) -> None:
        """Register all topics before Step 2 runs (status pending)."""
        m = self._ensure()
        for topic_idx, (chapter_name, subchapter_name, topic_name) in enumerate(topics_list, 1):
            label = f"{chapter_name} > {subchapter_name} > {topic_name}".strip(" >")
            upsert_unit(
                m,
                {
                    "unit_index": topic_idx,
                    "label": label,
                    "chapter": chapter_name,
                    "subchapter": subchapter_name,
                    "topic": topic_name,
                    "topic_name": topic_name,
                    "chapter_name": chapter_name,
                    "subchapter_name": subchapter_name,
                    "status": "pending",
                },
            )
        save_manifest(self.job_id, self.pair_index, m)


def build_manifest_from_step2_topics(
    job_id: str,
    pair_index: int,
    output_dir: str,
    book_id: int,
    chapter_id: int,
    job_type: str,
) -> Dict[str, Any]:
    units: List[Dict[str, Any]] = []
    if not os.path.isdir(output_dir):
        return {
            "job_type": job_type,
            "units": [],
            "renumber": {"scheme": "qid", "start_id": f"{book_id:03d}{chapter_id:03d}0001", "last_applied_at": None, "ids_provisional": False},
            "output_relpath": None,
        }
    pat = re.compile(r"_stage_v_step2_topic_(\d+)_", re.I)
    for fn in sorted(os.listdir(output_dir)):
        if not fn.endswith(".json") or "step2_topic" not in fn and "_stage_v_step2_topic_" not in fn:
            continue
        m = pat.search(fn)
        if not m:
            continue
        tidx = int(m.group(1))
        path = os.path.join(output_dir, fn)
        try:
            with open(path, "r", encoding="utf-8") as f:
                payload = json.load(f)
        except Exception:
            payload = {}
        meta = payload.get("metadata") or {}
        topic = meta.get("topic") or fn
        sub = meta.get("subchapter") or ""
        ch = ""
        units.append(
            {
                "unit_index": tidx,
                "label": f"{ch} > {sub} > {topic}".strip(" >"),
                "chapter": ch,
                "subchapter": sub,
                "topic": topic,
                "topic_name": topic,
                "status": "succeeded",
                "artifact_relpath": os.path.relpath(path, job_root(job_id)).replace("\\", "/"),
            }
        )
    return {
        "job_type": job_type,
        "units": sorted(units, key=lambda u: int(u["unit_index"])),
        "renumber": {
            "scheme": "qid",
            "start_id": f"{book_id:03d}{chapter_id:03d}0001",
            "last_applied_at": None,
            "ids_provisional": False,
        },
        "output_relpath": None,
    }


def ensure_manifest(
    job_id: str,
    pair_index: int,
    job_type: str,
    book_id: int,
    chapter_id: int,
    output_dir: str,
) -> Dict[str, Any]:
    m = load_manifest(job_id, pair_index)
    if m and m.get("units"):
        return m
    built = build_manifest_from_step2_topics(job_id, pair_index, output_dir, book_id, chapter_id, job_type)
    if built.get("units"):
        save_manifest(job_id, pair_index, built)
        return built
    raise FileNotFoundError("No Step 2 unit manifest. Run Step 2 first.")


def _final_json_path(job_id: str, manifest: Dict[str, Any], output_dir: str) -> str:
    rel = manifest.get("output_relpath")
    if rel:
        p = abs_from_relpath(job_id, rel)
        if os.path.isfile(p):
            return p
    for fn in os.listdir(output_dir):
        if fn.lower().startswith("b") and fn.endswith(".json") and "+" in fn:
            return os.path.join(output_dir, fn)
    raise FileNotFoundError("Final Test Bank JSON not found")


def regenerate_unit(
    db: Session,
    job_id: str,
    pair_index: int,
    unit_index: int,
    cfg: Dict[str, Any],
    stage_j_path: str,
    word_path: str,
    step1_path: str,
    prompt_2: str,
    model_name: str,
    provider: str,
    model_name_1: str,
    progress_callback: Optional[Callable[[str], None]] = None,
    prompt_client: Any = None,
    job_type: str = "test_bank_2",
) -> None:
    from stage_v_processor import StageVProcessor
    from webapp.processor_context import build_stage_v_processor

    client, ssm, processor = build_stage_v_processor()
    if prompt_client:
        processor.api_client = prompt_client
    else:
        processor.api_client = client

    out_dir = pair_output(job_id, pair_index)
    os.makedirs(out_dir, exist_ok=True)
    ctx = processor._build_stage_v_processing_context(stage_j_path, word_path, out_dir, progress_callback)
    if not ctx:
        raise RuntimeError("Failed to build Stage V context")
    book_id = ctx.book_id
    chapter_id = ctx.chapter_id

    manifest = ensure_manifest(job_id, pair_index, job_type, book_id, chapter_id, out_dir)
    unit = get_unit(manifest, unit_index)
    if not unit:
        raise ValueError(f"Unknown unit_index {unit_index}")

    topic_idx = int(unit_index)
    topics_list = ctx.topics_list
    if topic_idx < 1 or topic_idx > len(topics_list):
        raise ValueError(f"topic index {topic_idx} out of range")

    chapter_name, subchapter_name, topic_name = topics_list[topic_idx - 1]
    step1_data = processor.load_json_file(step1_path)
    all_step1 = processor._coerce_stage_v_rows_from_any_json(step1_data) if step1_data else []
    topic_key = processor._build_topic_key(chapter_name, subchapter_name, topic_name)
    filtered_stage_j = [
        rec
        for rec in ctx.stage_j_records_for_prompt
        if processor._build_topic_key(rec.get("chapter", ""), rec.get("subchapter", ""), rec.get("topic", "")) == topic_key
    ]
    filtered_step1 = [
        q
        for q in all_step1
        if isinstance(q, dict)
        and processor._build_topic_key(
            q.get("Chapter", q.get("chapter", "")),
            q.get("Subchapter", q.get("subchapter", "")),
            q.get("Topic", q.get("topic", "")),
        )[1:]
        == topic_key[1:]
    ]

    if prompt_client and hasattr(prompt_client, "set_current_unit"):
        prompt_client.set_current_unit(topic_idx, topic_name)

    path, _n = processor._step2_refine_questions_and_add_qid(
        stage_j_path=stage_j_path,
        word_file_path=word_path,
        full_stage_j_json=json.dumps(filtered_stage_j, ensure_ascii=False, indent=2),
        current_topic_name=topic_name,
        current_topic_subchapter=subchapter_name,
        topic_step1_json=json.dumps(filtered_step1, ensure_ascii=False, indent=2),
        step1_output_path=step1_path,
        prompt=prompt_2,
        model_name=model_name,
        book_id=book_id,
        chapter_id=chapter_id,
        topic_idx=topic_idx,
        total_topics=len(topics_list),
        qid_start_counter=1,
        output_dir=out_dir,
        progress_callback=progress_callback,
        assign_qid=False,
    )
    if not path:
        raise RuntimeError(f"Step 2 regenerate failed for topic {topic_name}")

    hooks = TestBankStep2UnitHooks(job_id, pair_index, job_type, book_id, chapter_id)
    hooks.on_topic_done(topic_idx, chapter_name, subchapter_name, topic_name, path, 1)
    combine_and_save_final(
        db, job_id, pair_index, processor, ctx, out_dir, manifest, job_type, apply_qid_renumber=False
    )
    m = load_manifest(job_id, pair_index) or manifest
    m.setdefault("renumber", {})["ids_provisional"] = True
    save_manifest(job_id, pair_index, m)


def combine_and_save_final(
    db: Session,
    job_id: str,
    pair_index: int,
    processor: Any,
    ctx: Any,
    output_dir: str,
    manifest: Dict[str, Any],
    job_type: str,
    apply_qid_renumber: bool = True,
) -> str:
    """Rebuild final b*.json from unit artifacts without deleting them."""
    book_id = ctx.book_id
    chapter_id = ctx.chapter_id
    combined: List[Dict[str, Any]] = []
    units = sorted(manifest.get("units") or [], key=lambda u: int(u.get("unit_index", 0)))
    for u in units:
        rel = u.get("artifact_relpath")
        if not rel:
            continue
        path = abs_from_relpath(job_id, rel)
        if not os.path.isfile(path):
            alt = os.path.join(output_dir, os.path.basename(path))
            if os.path.isfile(alt):
                path = alt
            else:
                continue
        data = processor.load_json_file(path)
        rows = processor.get_data_from_json(data) if data else []
        combined.extend(rows)
    combined = processor._dedup_stage_v_rows(combined)
    if apply_qid_renumber:
        renumber_qids_in_rows(combined, manifest, book_id, chapter_id)

    chapter_name_out = ""
    if ctx.stage_j_records:
        first = ctx.stage_j_records[0]
        if isinstance(first, dict):
            chapter_name_out = first.get("chapter", "") or ""

    if chapter_name_out:
        import re as _re

        chapter_name_clean = _re.sub(r'[<>:"/\\|?*]', "_", chapter_name_out).replace(" ", "_")
        chapter_name_clean = _re.sub(r"_+", "_", chapter_name_clean).strip("_")
        base_filename = f"b{book_id:03d}{chapter_id:03d}+{chapter_name_clean}.json"
    else:
        from datetime import datetime

        base_filename = f"b{book_id:03d}{chapter_id:03d}+{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"

    final_path = os.path.join(output_dir, base_filename)
    processor.save_json_file(combined, final_path, {"step": "2_combined", "book_id": book_id, "chapter_id": chapter_id}, "V-Final")
    rel = os.path.relpath(final_path, job_root(job_id)).replace("\\", "/")
    manifest["output_relpath"] = rel
    save_manifest(job_id, pair_index, manifest)
    register_artifacts_under(db, job_id, pair_index, job_root(job_id), f"pair_{pair_index}/output")
    return final_path


def renumber_pair(
    db: Session,
    job_id: str,
    pair_index: int,
    cfg: Dict[str, Any],
    stage_j_path: str,
    word_path: str,
    step1_path: str,
    job_type: str = "test_bank_2",
) -> int:
    from webapp.processor_context import build_stage_v_processor

    _client, ssm, processor = build_stage_v_processor()
    ctx = processor._build_stage_v_processing_context(stage_j_path, word_path, out_dir, None)
    if not ctx:
        raise RuntimeError("Failed to build Stage V context")
    out_dir = pair_output(job_id, pair_index)
    manifest = ensure_manifest(
        job_id, pair_index, job_type, ctx.book_id, ctx.chapter_id, out_dir
    )
    combine_and_save_final(
        db, job_id, pair_index, processor, ctx, out_dir, manifest, job_type, apply_qid_renumber=True
    )
    mark_renumber_applied(manifest)
    save_manifest(job_id, pair_index, manifest)
    m = load_manifest(job_id, pair_index) or manifest
    m.setdefault("renumber", {})["ids_provisional"] = False
    save_manifest(job_id, pair_index, m)
    final_path = _final_json_path(job_id, manifest, out_dir)
    with open(final_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    rows = processor.get_data_from_json(data) if isinstance(data, dict) else (data if isinstance(data, list) else [])
    return len(rows) if isinstance(rows, list) else 0
