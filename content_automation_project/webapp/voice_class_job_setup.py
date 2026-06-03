"""
Voice Class job creation helpers.

Validates uploads, pairs files, and builds pair_media config for persistence.
"""

from __future__ import annotations

import os
import shutil
from dataclasses import dataclass
from typing import Any, Dict, List, Tuple

from stage_v_pairing import auto_pair_voice_class_files, validate_voice_class_upload_counts


@dataclass(frozen=True)
class VoiceClassUploadedTriplet:
    """One chapter's three uploaded JSON files (absolute temp paths)."""

    tagged_path: str
    filepic_path: str
    tablepic_path: str


@dataclass(frozen=True)
class VoiceClassPairStorage:
    """Relative paths and display names after copying inputs to job storage."""

    tagged_relpath: str
    filepic_relpath: str
    tablepic_relpath: str
    tagged_basename: str
    filepic_basename: str
    tablepic_basename: str


def prepare_voice_class_upload_triplets(
    tagged_paths: List[str],
    filepic_paths: List[str],
    tablepic_paths: List[str],
) -> Tuple[List[VoiceClassUploadedTriplet], str | None]:
    """
    Validate upload counts and zip sorted paths into triplets.

    Returns (triplets, error_message). error_message is None on success.
    """
    count_error = validate_voice_class_upload_counts(
        len(tagged_paths),
        len(filepic_paths),
        len(tablepic_paths),
    )
    if count_error:
        return [], count_error

    triplets: List[VoiceClassUploadedTriplet] = []
    pairs_spec = auto_pair_voice_class_files(tagged_paths, filepic_paths, tablepic_paths)
    for pair in pairs_spec:
        triplets.append(
            VoiceClassUploadedTriplet(
                tagged_path=pair["stage_j_path"],
                filepic_path=pair["filepic_path"],
                tablepic_path=pair["tablepic_path"],
            )
        )
    return triplets, None


def copy_voice_class_pair_inputs(
    job_root: str,
    pair_index: int,
    triplet: VoiceClassUploadedTriplet,
) -> VoiceClassPairStorage:
    """Copy one triplet into pair_N/inputs/ and return relative paths."""
    tagged_name = os.path.basename(triplet.tagged_path)
    filepic_name = os.path.basename(triplet.filepic_path)
    tablepic_name = os.path.basename(triplet.tablepic_path)

    tagged_relpath = f"pair_{pair_index}/inputs/{tagged_name}"
    filepic_relpath = f"pair_{pair_index}/inputs/{filepic_name}"
    tablepic_relpath = f"pair_{pair_index}/inputs/{tablepic_name}"

    shutil.copy2(triplet.tagged_path, os.path.join(job_root, tagged_relpath.replace("/", os.sep)))
    shutil.copy2(triplet.filepic_path, os.path.join(job_root, filepic_relpath.replace("/", os.sep)))
    shutil.copy2(triplet.tablepic_path, os.path.join(job_root, tablepic_relpath.replace("/", os.sep)))

    return VoiceClassPairStorage(
        tagged_relpath=tagged_relpath,
        filepic_relpath=filepic_relpath,
        tablepic_relpath=tablepic_relpath,
        tagged_basename=tagged_name,
        filepic_basename=filepic_name,
        tablepic_basename=tablepic_name,
    )


def build_pair_media_entry(storage: VoiceClassPairStorage) -> Dict[str, str]:
    """Build one pair_media config entry for job.config_json."""
    return {
        "filepic_relpath": storage.filepic_relpath,
        "tablepic_relpath": storage.tablepic_relpath,
        "filepic_basename": storage.filepic_basename,
        "tablepic_basename": storage.tablepic_basename,
    }
