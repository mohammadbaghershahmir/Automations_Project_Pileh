"""
Resolve Voice Class pair input paths from job storage.

Infrastructure layer — filesystem paths only, no LLM logic.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Dict, Optional


@dataclass(frozen=True)
class VoiceClassPairFiles:
    """Absolute paths to the three JSON inputs for one Voice Class pair."""

    tagged_json: str
    filepic_json: str
    tablepic_json: str


@dataclass(frozen=True)
class VoiceClassPairInputError:
    """Why a pair's inputs could not be resolved."""

    message: str


def pair_media_entry(cfg: Dict[str, Any], pair_index: int) -> Dict[str, Any]:
    """Read pair_media config for one pair index."""
    pair_media = cfg.get("pair_media") or {}
    entry = pair_media.get(str(pair_index))
    if isinstance(entry, dict):
        return entry
    return {}


def resolve_voice_class_pair_files(
    job_root_path: str,
    *,
    tagged_relpath: Optional[str],
    pair_media: Dict[str, Any],
) -> VoiceClassPairFiles | VoiceClassPairInputError:
    """
    Resolve and validate the three input files for one Voice Class pair.

    Returns VoiceClassPairFiles on success, or VoiceClassPairInputError with a
    human-readable message when paths are missing or files do not exist.
    """
    filepic_relpath = (pair_media.get("filepic_relpath") or "").strip()
    tablepic_relpath = (pair_media.get("tablepic_relpath") or "").strip()
    tagged_relpath_clean = (tagged_relpath or "").strip()

    if not tagged_relpath_clean or not filepic_relpath or not tablepic_relpath:
        return VoiceClassPairInputError(
            message="Missing tagged JSON, filepic, or tablepic path",
        )

    tagged_abs = _abs_path(job_root_path, tagged_relpath_clean)
    filepic_abs = _abs_path(job_root_path, filepic_relpath)
    tablepic_abs = _abs_path(job_root_path, tablepic_relpath)

    if not os.path.isfile(tagged_abs):
        return VoiceClassPairInputError(message="Tagged JSON file missing on disk")
    if not os.path.isfile(filepic_abs):
        return VoiceClassPairInputError(message="Filepic JSON file missing on disk")
    if not os.path.isfile(tablepic_abs):
        return VoiceClassPairInputError(message="Tablepic JSON file missing on disk")

    return VoiceClassPairFiles(
        tagged_json=tagged_abs,
        filepic_json=filepic_abs,
        tablepic_json=tablepic_abs,
    )


def _abs_path(job_root_path: str, relpath: str) -> str:
    return os.path.join(job_root_path, relpath.replace("/", os.sep))
