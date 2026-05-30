"""Build admin preview payload for a single LLM unit."""

from __future__ import annotations

import json
import os
import re
from typing import Any, Dict, List, Optional

from webapp.job_files import job_root, pair_dir
from webapp.unit_repair.manifest import abs_from_relpath, get_unit, load_manifest


def _read_text_slice(path: str, limit: int = 120_000) -> str:
    if not os.path.isfile(path):
        return ""
    with open(path, "r", encoding="utf-8", errors="replace") as f:
        return f.read(limit)


def _pretty_json_text(raw: str) -> str:
    try:
        return json.dumps(json.loads(raw), ensure_ascii=False, indent=2)
    except Exception:
        return raw


def _find_prompt_file(job_id: str, pair_index: int, unit_index: int, prompt_seq: Optional[int]) -> Optional[str]:
    prompts_dir = os.path.join(pair_dir(job_id, pair_index), "prompts")
    if not os.path.isdir(prompts_dir):
        return None
    unit_tag = f"_u{int(unit_index):03d}_"
    seq_prefix = f"{int(prompt_seq):04d}" if prompt_seq else None
    candidates: List[str] = []
    for fn in sorted(os.listdir(prompts_dir)):
        if not fn.endswith(".txt"):
            continue
        path = os.path.join(prompts_dir, fn)
        if unit_tag in fn:
            candidates.append(path)
        elif seq_prefix and fn.startswith(seq_prefix):
            candidates.append(path)
    if not candidates:
        for fn in sorted(os.listdir(prompts_dir)):
            if not fn.endswith(".txt"):
                continue
            path = os.path.join(prompts_dir, fn)
            try:
                head = _read_text_slice(path, 2000)
                if re.search(rf"unit_index:\s*{int(unit_index)}\b", head):
                    candidates.append(path)
            except OSError:
                continue
    return candidates[0] if candidates else None


def _slice_from_output(job_type: str, output_path: str, unit: Dict[str, Any]) -> Optional[str]:
    if not os.path.isfile(output_path):
        return None
    raw = _read_text_slice(output_path)
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        return raw[:80_000] if raw else None

    jt = (job_type or "").strip()
    if jt == "document_processing" or jt in ("image_notes", "table_notes"):
        points = data.get("points") if isinstance(data, dict) else None
        if not isinstance(points, list):
            return None
        ch = (unit.get("chapter") or "").strip()
        sub = (unit.get("subchapter") or "").strip()
        top = (unit.get("topic") or unit.get("paragraph") or "").strip()
        matched = [
            p
            for p in points
            if isinstance(p, dict)
            and (p.get("chapter") or "").strip() == ch
            and (p.get("subchapter") or "").strip() == sub
            and (p.get("topic") or "").strip() == top
        ]
        if matched:
            return json.dumps({"points": matched}, ensure_ascii=False, indent=2)
        return None

    if jt == "ocr_extraction":
        subs = (data.get("chapters") or [{}])[0].get("subchapters") or []
        name = (unit.get("subchapter") or "").strip()
        ui = int(unit.get("unit_index") or 0)
        for s in subs:
            if isinstance(s, dict) and (s.get("subchapter") or "").strip() == name:
                return json.dumps(s, ensure_ascii=False, indent=2)
        if 0 < ui <= len(subs):
            return json.dumps(subs[ui - 1], ensure_ascii=False, indent=2)
        return None

    if jt in ("test_bank", "test_bank_2"):
        rows = data if isinstance(data, list) else data.get("data") or data.get("questions")
        if isinstance(rows, list):
            topic = (unit.get("topic") or unit.get("topic_name") or "").strip()
            sub = (unit.get("subchapter") or unit.get("subchapter_name") or "").strip()
            matched = [
                r
                for r in rows
                if isinstance(r, dict)
                and (r.get("Topic") or r.get("topic") or "").strip() == topic
                and (not sub or (r.get("Subchapter") or r.get("subchapter") or "").strip() == sub)
            ]
            if matched:
                return json.dumps(matched, ensure_ascii=False, indent=2)
        return None

    return None


def get_unit_preview_payload(
    job_id: str,
    pair_index: int,
    unit_index: int,
    job_type: str,
) -> Dict[str, Any]:
    manifest = load_manifest(job_id, pair_index)
    if not manifest:
        raise FileNotFoundError("No manifest for this pair")
    unit = get_unit(manifest, unit_index)
    if not unit:
        raise ValueError(f"Unknown unit_index {unit_index}")

    sections: List[Dict[str, Any]] = []
    artifact_rel = (unit.get("artifact_relpath") or "").strip()
    if artifact_rel:
        abs_path = abs_from_relpath(job_id, artifact_rel)
        if os.path.isfile(abs_path):
            content = _pretty_json_text(_read_text_slice(abs_path))
            sections.append(
                {
                    "title": "Unit output (saved slice)",
                    "content": content,
                    "rel_path": artifact_rel,
                }
            )

    output_rel = (manifest.get("output_relpath") or "").strip()
    if output_rel:
        output_path = abs_from_relpath(job_id, output_rel)
        sliced = _slice_from_output(job_type, output_path, unit)
        has_saved = any(s.get("title") == "Unit output (saved slice)" for s in sections)
        if sliced and not has_saved:
            sections.append(
                {
                    "title": "Unit output (from combined file)",
                    "content": sliced,
                    "rel_path": output_rel,
                }
            )

    prompt_seq = unit.get("prompt_seq")
    prompt_path = _find_prompt_file(job_id, pair_index, unit_index, prompt_seq)
    if prompt_path:
        rel = os.path.relpath(prompt_path, job_root(job_id)).replace("\\", "/")
        sections.append(
            {
                "title": "LLM prompt sent",
                "content": _read_text_slice(prompt_path),
                "rel_path": rel,
            }
        )

    if not sections:
        sections.append(
            {
                "title": "No preview available",
                "content": "Run processing first, or wait until this unit finishes.",
                "rel_path": None,
            }
        )

    return {
        "unit_index": unit_index,
        "label": unit.get("label") or "",
        "status": unit.get("status") or "",
        "sections": sections,
    }
