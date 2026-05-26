"""Manifest JSON under pair_N/units/manifest.json — one row per LLM unit."""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from webapp.job_files import job_root, pair_dir


def units_dir(job_id: str, pair_index: int) -> str:
    return os.path.join(pair_dir(job_id, pair_index), "units")


def manifest_path(job_id: str, pair_index: int) -> str:
    return os.path.join(units_dir(job_id, pair_index), "manifest.json")


def _empty_manifest(job_type: str, renumber_scheme: Optional[str], start_id: Optional[str]) -> Dict[str, Any]:
    return {
        "job_type": job_type,
        "units": [],
        "renumber": {
            "scheme": renumber_scheme,
            "start_id": start_id,
            "last_applied_at": None,
            "ids_provisional": False,
        },
        "output_relpath": None,
        "updated_at": None,
    }


def load_manifest(job_id: str, pair_index: int) -> Optional[Dict[str, Any]]:
    path = manifest_path(job_id, pair_index)
    if not os.path.isfile(path):
        return None
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_manifest(job_id: str, pair_index: int, data: Dict[str, Any]) -> str:
    ud = units_dir(job_id, pair_index)
    os.makedirs(ud, exist_ok=True)
    data["updated_at"] = datetime.now(timezone.utc).isoformat()
    path = manifest_path(job_id, pair_index)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    return path


def get_unit(manifest: Dict[str, Any], unit_index: int) -> Optional[Dict[str, Any]]:
    for u in manifest.get("units") or []:
        if int(u.get("unit_index", -1)) == int(unit_index):
            return u
    return None


def upsert_unit(manifest: Dict[str, Any], unit: Dict[str, Any]) -> None:
    idx = int(unit["unit_index"])
    units: List[Dict[str, Any]] = list(manifest.get("units") or [])
    manifest["units"] = [u for u in units if int(u.get("unit_index", -1)) != idx]
    manifest["units"].append(unit)
    manifest["units"].sort(key=lambda u: int(u.get("unit_index", 0)))


def unit_artifact_relpath(job_id: str, pair_index: int, unit_index: int, suffix: str) -> str:
    base = job_root(job_id)
    rel = os.path.join(
        f"pair_{pair_index}",
        "units",
        f"{unit_index:03d}_{suffix}",
    ).replace("\\", "/")
    return rel


def abs_from_relpath(job_id: str, relpath: str) -> str:
    return os.path.join(job_root(job_id), relpath.replace("/", os.sep))
